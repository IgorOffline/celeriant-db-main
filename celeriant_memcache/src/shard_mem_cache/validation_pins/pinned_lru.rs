use celeriant_wal::precomputed_hash::{PrecomputedBuildHasher, PrecomputedMap};
use lru::LruCache;
use std::hash::Hash;
use std::num::NonZeroUsize;

/// An LRU whose keys can be reserved by in-flight requests.
///
/// Every key lives in one of two disjoint halves. `ordinary` is a plain LRU
/// bounded by `cap`. `pinned` holds reserved keys with a reference count and a
/// value that is `None` until loaded. The first `pin` moves the value out of
/// `ordinary`; the last `unpin` moves it back as most recent. Overlapping
/// requests share one value.
///
/// - Memory: at most `cap` ordinary entries plus one slot per distinct reserved
///   key. The reserved set is bounded only by admitted requests. `len()` counts
///   loaded reserved values, so it can exceed `cap()` and "full" checks trip early.
/// - After a large request releases, its reservation table shrinks back to
///   roughly `max(cap, 2 × live reservations)` slots.
/// - A reserved key without a value is unloaded, not absent. `clear` drops
///   values and keeps reservations.
/// - Releasing inserts the value as most recent and can evict ordinary entries,
///   so a large request's release can flush the ordinary half. Recency is not
///   tracked while reserved.
/// - An unbalanced `unpin` panics. A leaked reservation keeps its key forever.
/// - `clear` advances `epoch`, so a load that began before it can tell its
///   result is stale.
pub(crate) struct PinnedLru<K: Hash + Eq, V> {
    ordinary: LruCache<K, V, PrecomputedBuildHasher>,
    pinned: PrecomputedMap<K, (usize, Option<V>)>,
    loaded: usize,
    epoch: u64,
}

impl<K: Hash + Eq + Clone, V> PinnedLru<K, V> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            ordinary: LruCache::with_hasher(capacity, PrecomputedBuildHasher::default()),
            pinned: PrecomputedMap::default(),
            loaded: 0,
            epoch: 0,
        }
    }

    pub(super) fn pin(&mut self, key: K) {
        let entry = self.pinned.entry(key.clone()).or_insert_with(|| {
            let value = self.ordinary.pop(&key);
            self.loaded += usize::from(value.is_some());
            (0, value)
        });
        entry.0 += 1;
    }

    pub(super) fn unpin(&mut self, key: &K) {
        let entry = self.pinned.get_mut(key).expect("reservation must outlive its owner");
        entry.0 -= 1;
        if entry.0 == 0 {
            let (_, value) = self.pinned.remove(key).unwrap();
            if let Some(value) = value {
                self.loaded -= 1;
                self.ordinary.put(key.clone(), value);
            }
        }
        // Reclaim a large request's reservation table even while smaller
        // overlapping requests are still warming.
        let retained = self.ordinary.cap().get().max(self.pinned.len().saturating_mul(4));
        if self.pinned.capacity() > retained {
            self.pinned.shrink_to(self.ordinary.cap().get().max(self.pinned.len().saturating_mul(2)));
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some((_, value)) = self.pinned.get(key) {
            return value.as_ref();
        }
        self.ordinary.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if let Some((_, value)) = self.pinned.get_mut(key) {
            return value.as_mut();
        }
        self.ordinary.get_mut(key)
    }

    pub fn peek(&self, key: &K) -> Option<&V> {
        if let Some((_, value)) = self.pinned.get(key) {
            return value.as_ref();
        }
        self.ordinary.peek(key)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.peek(key).is_some()
    }

    pub fn put(&mut self, key: K, value: V) -> Option<V> {
        if let Some((_, slot)) = self.pinned.get_mut(&key) {
            let previous = slot.replace(value);
            self.loaded += usize::from(previous.is_none());
            return previous;
        }
        self.ordinary.put(key, value)
    }

    pub fn pop(&mut self, key: &K) -> Option<V> {
        if let Some((_, slot)) = self.pinned.get_mut(key) {
            let previous = slot.take();
            self.loaded -= usize::from(previous.is_some());
            return previous;
        }
        self.ordinary.pop(key)
    }

    pub fn clear(&mut self) {
        self.ordinary.clear();
        for (_, slot) in self.pinned.values_mut() {
            *slot = None;
        }
        self.loaded = 0;
        self.epoch = self.epoch.wrapping_add(1);
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.ordinary.iter_mut().chain(
            self.pinned
                .iter_mut()
                .filter_map(|(key, (_, value))| value.as_mut().map(|value| (key, value))),
        )
    }

    pub fn demote(&mut self, key: &K) {
        self.ordinary.demote(key);
    }
    pub fn len(&self) -> usize {
        self.ordinary.len() + self.loaded
    }
    pub fn cap(&self) -> NonZeroUsize {
        self.ordinary.cap()
    }
}

#[cfg(test)]
mod tests {
    use super::PinnedLru;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use std::collections::HashMap;
    use std::num::NonZeroUsize;

    #[test]
    fn overlapping_reservations_keep_current_values_until_the_last_owner_releases() {
        let mut cache = PinnedLru::new(NonZeroUsize::new(1).unwrap());
        cache.put(1u64, 10);
        cache.pin(1);
        cache.pin(1);
        for key in 2..64 {
            cache.put(key, key);
        }
        assert_eq!(cache.get(&1), Some(&10));
        cache.put(1, 20);
        cache.unpin(&1);
        for key in 64..128 {
            cache.put(key, key);
        }
        assert_eq!(cache.get(&1), Some(&20));
        cache.unpin(&1);
        cache.put(128, 128);
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn invalidation_drops_values_and_preserves_reservations_for_reload() {
        let mut cache = PinnedLru::new(NonZeroUsize::new(1).unwrap());
        cache.pin(1u64);
        cache.pin(2);
        assert_eq!(cache.get(&1), None);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.clear();
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.len(), 0);
        cache.put(1, 30);
        cache.put(2, 40);
        cache.put(3, 50);
        assert_eq!(cache.get(&1), Some(&30));
        assert_eq!(cache.get(&2), Some(&40));
        assert_eq!(cache.pop(&1), Some(30));
        cache.put(1, 60);
        cache.unpin(&1);
        cache.unpin(&2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn release_reinserts_as_most_recent_and_evicts_ordinary_entries() {
        let mut cache = PinnedLru::new(NonZeroUsize::new(2).unwrap());
        cache.put(1u64, 1);
        cache.put(2, 2);
        cache.pin(1);
        cache.pin(3);
        cache.put(3, 3);
        cache.put(4, 4);
        cache.unpin(&1);
        cache.unpin(&3);
        assert_eq!(cache.peek(&2), None);
        assert_eq!(cache.peek(&4), None);
        assert_eq!(cache.peek(&1), Some(&1));
        assert_eq!(cache.peek(&3), Some(&3));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn popped_reservation_returns_nothing_to_the_lru() {
        let mut cache = PinnedLru::new(NonZeroUsize::new(1).unwrap());
        cache.put(1u64, 10);
        cache.pin(1);
        assert_eq!(cache.pop(&1), Some(10));
        assert_eq!(cache.len(), 0);
        cache.put(2, 20);
        cache.unpin(&1);
        assert_eq!(cache.peek(&1), None);
        assert_eq!(cache.peek(&2), Some(&20));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn release_shrinks_a_large_reservation_table_while_others_stay_reserved() {
        let mut cache = PinnedLru::new(NonZeroUsize::new(4).unwrap());
        for key in 0..1024u64 {
            cache.pin(key);
            cache.put(key, key);
        }
        cache.pin(0);
        for key in 0..1024 {
            cache.unpin(&key);
        }
        assert_eq!(cache.get(&0), Some(&0));
        assert!(cache.pinned.capacity() < 16, "reservation table kept {} slots", cache.pinned.capacity());
    }

    #[test]
    fn only_clear_advances_the_epoch() {
        let mut cache = PinnedLru::new(NonZeroUsize::new(1).unwrap());
        cache.pin(1u64);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.pop(&1);
        cache.unpin(&1);
        assert_eq!(cache.epoch(), 0);
        cache.clear();
        assert_eq!(cache.epoch(), 1);
    }

    #[test]
    fn iter_mut_reaches_loaded_reserved_values() {
        let mut cache = PinnedLru::new(NonZeroUsize::new(1).unwrap());
        cache.put(1u64, 1);
        cache.pin(2);
        cache.put(2, 2);
        cache.pin(3);
        for (_, value) in cache.iter_mut() {
            *value += 10;
        }
        assert_eq!(cache.peek(&1), Some(&11));
        assert_eq!(cache.peek(&2), Some(&12));
        assert_eq!(cache.peek(&3), None);
    }

    #[test]
    #[should_panic(expected = "reservation must outlive its owner")]
    fn unbalanced_release_panics() {
        let mut cache = PinnedLru::<u64, u64>::new(NonZeroUsize::new(1).unwrap());
        cache.unpin(&1);
    }

    /// The model tracks every write and reservation. Unreserved keys may be
    /// evicted; the model forgets them when the cache does.
    #[test]
    fn random_operations_preserve_reservations_and_accounting() {
        const KEYS: u64 = 8;
        for seed in 0..200 {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut cache = PinnedLru::new(NonZeroUsize::new(3).unwrap());
            let mut values: HashMap<u64, u64> = HashMap::new();
            let mut refs: HashMap<u64, usize> = HashMap::new();
            for step in 0..500 {
                let key = rng.gen_range(0..KEYS);
                match rng.gen_range(0..7) {
                    0 | 1 => {
                        let value = rng.gen_range(0..1000);
                        cache.put(key, value);
                        values.insert(key, value);
                    }
                    2 => assert_eq!(cache.get(&key), values.get(&key), "seed {seed} step {step}"),
                    3 => assert_eq!(cache.pop(&key), values.remove(&key), "seed {seed} step {step}"),
                    4 => {
                        cache.pin(key);
                        *refs.entry(key).or_default() += 1;
                    }
                    5 => {
                        if let Some(count) = refs.get_mut(&key) {
                            cache.unpin(&key);
                            *count -= 1;
                            if *count == 0 {
                                refs.remove(&key);
                                assert_eq!(cache.peek(&key), values.get(&key), "seed {seed} step {step}: released value");
                            }
                        }
                    }
                    _ if rng.gen_ratio(1, 20) => {
                        cache.clear();
                        values.clear();
                    }
                    _ => cache.demote(&key),
                }
                assert_invariants(&mut cache, &mut values, &refs, &format!("seed {seed} step {step}"));
            }
            for (key, count) in refs {
                for _ in 0..count {
                    cache.unpin(&key);
                }
            }
            assert!(cache.pinned.is_empty(), "seed {seed}");
            assert!(cache.len() <= cache.cap().get(), "seed {seed}");
        }
    }

    fn assert_invariants(cache: &mut PinnedLru<u64, u64>, values: &mut HashMap<u64, u64>, refs: &HashMap<u64, usize>, context: &str) {
        for (key, _) in cache.ordinary.iter() {
            assert!(!cache.pinned.contains_key(key), "{context}: key {key} is in the LRU and reserved");
        }
        assert!(cache.ordinary.len() <= cache.cap().get(), "{context}: ordinary half exceeds cap");
        assert_eq!(cache.loaded, cache.pinned.values().filter(|(_, value)| value.is_some()).count(), "{context}: loaded count");
        assert_eq!(cache.iter_mut().count(), cache.len(), "{context}: len");
        assert_eq!(cache.pinned.len(), refs.len(), "{context}: reserved keys");
        for (key, count) in refs {
            assert_eq!(cache.pinned.get(key).map(|(refs, _)| *refs), Some(*count), "{context}: refs for {key}");
            assert_eq!(cache.peek(key), values.get(key), "{context}: reserved value for {key}");
        }
        values.retain(|key, value| match cache.peek(key) {
            Some(cached) => {
                assert_eq!(cached, value, "{context}: stale value for {key}");
                true
            }
            None => false,
        });
    }

    enum Op {
        Put(u64, u64),
        Get(u64),
        Pop(u64),
        Pin(u64),
        Unpin(u64),
        Clear,
    }

    /// Replays `ops` and renders the state after each one.
    /// `celeriant_memcache/pinned-lru-simulator.html` must produce these lines
    /// exactly; update its port and scenarios when this file changes.
    fn trace(cap: usize, ops: &[Op]) -> Vec<String> {
        let mut cache = PinnedLru::new(NonZeroUsize::new(cap).unwrap());
        ops.iter()
            .map(|op| {
                let label = match *op {
                    Op::Put(key, value) => format!("put {key}={value} -> {:?}", cache.put(key, value)),
                    Op::Get(key) => format!("get {key} -> {:?}", cache.get(&key)),
                    Op::Pop(key) => format!("pop {key} -> {:?}", cache.pop(&key)),
                    Op::Pin(key) => {
                        cache.pin(key);
                        format!("pin {key}")
                    }
                    Op::Unpin(key) => {
                        cache.unpin(&key);
                        format!("unpin {key}")
                    }
                    Op::Clear => {
                        cache.clear();
                        "clear".to_string()
                    }
                };
                let ordinary: Vec<_> = cache.ordinary.iter().map(|(key, value)| format!("{key}={value}")).collect();
                let mut pinned: Vec<_> = cache.pinned.iter().collect();
                pinned.sort_by_key(|(key, _)| **key);
                let pinned: Vec<_> = pinned.iter()
                    .map(|(key, (refs, value))| match value {
                        Some(value) => format!("{key}={value}x{refs}"),
                        None => format!("{key}=?x{refs}"),
                    })
                    .collect();
                format!("{label:<22} ordinary [{}]  pinned [{}]  len {}/{cap}", ordinary.join(" "), pinned.join(" "), cache.len())
            })
            .collect()
    }

    #[test]
    fn simulator_scenarios_end_in_their_expected_state() {
        use Op::*;
        let scenarios = [
            ("overlapping requests", 2, vec![
                Put(1, 10), Pin(1), Pin(1), Put(2, 2), Put(3, 3), Put(4, 4), Get(1), Put(1, 20), Unpin(1), Put(5, 5), Unpin(1), Put(6, 6),
            ], "ordinary [6=6 1=20]  pinned []  len 2/2"),
            ("rollback clear", 2, vec![
                Pin(1), Pin(2), Put(1, 10), Put(2, 20), Clear, Get(1), Put(1, 30), Put(3, 3), Unpin(1), Unpin(2),
            ], "ordinary [1=30 3=3]  pinned []  len 2/2"),
            ("large release flush", 3, vec![
                Put(1, 1), Put(2, 2), Put(3, 3), Pin(10), Pin(11), Pin(12), Put(10, 10), Put(11, 11), Put(12, 12), Get(1), Unpin(10), Unpin(11), Unpin(12),
            ], "ordinary [12=12 11=11 10=10]  pinned []  len 3/3"),
            ("pop while reserved", 1, vec![
                Put(1, 10), Pin(1), Pop(1), Put(2, 20), Unpin(1),
            ], "ordinary [2=20]  pinned []  len 1/1"),
        ];
        for (name, cap, ops, end) in scenarios {
            let lines = trace(cap, &ops);
            println!("# {name} (cap {cap})\n{}", lines.join("\n"));
            assert!(lines.last().unwrap().ends_with(end), "{name}");
        }
    }
}
