//! Black-box contract for `ValidationPins`: reservations survive LRU pressure,
//! sit outside capacity, release as most-recent, and `is_ready` never reports
//! readiness for an attempt that straddled a clear or rollback.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use celeriant_memcache::cache_path::CachePath;
use celeriant_memcache::cached_schema::Validate;
use celeriant_memcache::mem_snapshot_aggregate::MemSnapshotAggregate;
use celeriant_memcache::shard_mem_cache::ShardMemCache;
use celeriant_memcache::validation_pins::ValidationPins;
use celeriant_wal::aggregate_client_key::AggregateClientKey;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::schema_key::SchemaKey;

struct NoValidation;
impl Validate for NoValidation {
    fn validate(&self, _event_value: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

type Cache = Rc<RefCell<ShardMemCache<NoValidation>>>;

/// Aggregate write-snapshot capacity 2 (bytes / 112), client capacity 2
/// (bytes / 128), no-schema capacity 1 ((bytes / 2) / 80).
fn small_cache() -> Cache {
    Rc::new(RefCell::new(ShardMemCache::new(1 << 20, 224, 256, 160, 1 << 20, 1 << 20)))
}

fn agg(id: u128) -> AggregateKey {
    AggregateKey::new(1, 2, id)
}

fn client(id: u128) -> AggregateClientKey {
    AggregateClientKey::new(agg(id), 7)
}

fn schema(minor: u64) -> SchemaKey {
    SchemaKey::new(1, 2, 1, minor)
}

fn pin_aggs(cache: &Cache, ids: &[u128]) -> ValidationPins<NoValidation> {
    ValidationPins::new(cache.clone(), ids.iter().map(|&id| agg(id)).collect(), vec![], vec![])
}

fn put_agg(cache: &Cache, id: u128) {
    let snapshot = MemSnapshotAggregate::found(1, 0, id as u64, 1, 1);
    cache.borrow_mut().put_aggregate_snapshot_only(agg(id), snapshot, false, CachePath::Write);
}

fn agg_loaded(cache: &Cache, id: u128) -> bool {
    cache.borrow_mut().aggregate_load_status(&agg(id), CachePath::Write).0
}

fn put_client(cache: &Cache, id: u128) {
    cache.borrow_mut().put_aggregate_client_into_cache(client(id), 5, false);
}

fn client_loaded(cache: &Cache, id: u128) -> bool {
    let key = client(id);
    cache.borrow_mut().aggregate_client_load_status(&key.aggregate_key, &key).0
}

fn agg_len(cache: &Cache) -> usize {
    cache.borrow().aggregate_write_snapshots_len()
}

#[test]
fn reserved_aggregates_are_outside_capacity_and_len_returns_to_capacity_on_drop() {
    let cache = small_cache();
    let pins = pin_aggs(&cache, &[1, 2, 3]);
    for id in [1, 2, 3, 10, 11] {
        put_agg(&cache, id);
    }
    assert_eq!(agg_len(&cache), 5, "3 reserved + 2 ordinary at capacity 2");
    assert!(pins.is_ready(pins.begin_attempt()));
    for id in [1, 2, 3, 10, 11] {
        assert!(agg_loaded(&cache, id), "aggregate {id} evicted while reservations held");
    }

    drop(pins);
    assert_eq!(agg_len(&cache), 2, "released values must fall back under capacity");
}

#[test]
fn reserved_aggregate_survives_lru_pressure() {
    let cache = small_cache();
    let pins = pin_aggs(&cache, &[1]);
    put_agg(&cache, 1);
    for id in 10..20 {
        put_agg(&cache, id);
    }
    assert!(agg_loaded(&cache, 1));
    assert!(pins.is_ready(pins.begin_attempt()));
}

#[test]
fn value_loaded_before_the_guard_becomes_reserved() {
    let cache = small_cache();
    put_agg(&cache, 1);
    let pins = pin_aggs(&cache, &[1]);
    assert!(pins.is_ready(pins.begin_attempt()), "already-loaded key counts as loaded");
    for id in 10..20 {
        put_agg(&cache, id);
    }
    assert!(agg_loaded(&cache, 1), "pre-loaded key must be protected once reserved");
}

#[test]
fn release_returns_value_as_most_recent_and_evicts_least_recent_ordinary_entry() {
    let cache = small_cache();
    let pins = pin_aggs(&cache, &[1]);
    put_agg(&cache, 1);
    put_agg(&cache, 10);
    put_agg(&cache, 11);
    assert_eq!(agg_len(&cache), 3);

    drop(pins);
    assert_eq!(agg_len(&cache), 2);
    assert!(!agg_loaded(&cache, 10), "least recent ordinary entry evicted by release");
    assert!(agg_loaded(&cache, 11));
    assert!(agg_loaded(&cache, 1), "released value is most recent, not evicted");
}

#[test]
fn overlapping_guards_share_one_value_until_last_drop() {
    let cache = small_cache();
    let first = pin_aggs(&cache, &[1]);
    let second = pin_aggs(&cache, &[1]);
    put_agg(&cache, 1);
    assert_eq!(agg_len(&cache), 1, "one value for two reservations");
    assert!(first.is_ready(first.begin_attempt()));
    assert!(second.is_ready(second.begin_attempt()));

    drop(first);
    for id in 10..20 {
        put_agg(&cache, id);
    }
    assert!(agg_loaded(&cache, 1), "still reserved by the second guard");
    assert!(second.is_ready(second.begin_attempt()));

    drop(second);
    for id in 20..30 {
        put_agg(&cache, id);
    }
    assert!(!agg_loaded(&cache, 1), "ordinary after last guard drops");
    assert_eq!(agg_len(&cache), 2);
}

#[test]
fn unloaded_reserved_key_is_not_ready_but_not_found_counts_as_loaded() {
    let cache = small_cache();
    let pins = pin_aggs(&cache, &[1]);
    assert!(!pins.is_ready(pins.begin_attempt()), "reserved but unloaded");

    cache.borrow_mut().put_aggregate_into_cache_as_not_found(agg(1), CachePath::Write);
    assert!(pins.is_ready(pins.begin_attempt()), "not-found is a loaded answer");
    for id in 10..20 {
        put_agg(&cache, id);
    }
    assert!(pins.is_ready(pins.begin_attempt()), "not-found value is protected too");
}

#[test]
fn reserved_client_survives_pressure_outside_capacity() {
    let cache = small_cache();
    let pins = ValidationPins::new(cache.clone(), vec![], vec![client(1)], vec![]);
    assert!(!pins.is_ready(pins.begin_attempt()));
    put_client(&cache, 1);
    for id in 10..20 {
        put_client(&cache, id);
    }
    assert!(client_loaded(&cache, 1));
    assert!(pins.is_ready(pins.begin_attempt()));
    assert_eq!(cache.borrow().aggregate_write_client_snapshots_len(), 3, "1 reserved + 2 ordinary");

    drop(pins);
    assert_eq!(cache.borrow().aggregate_write_client_snapshots_len(), 2);
}

#[test]
fn reserved_schema_survives_pressure() {
    let cache = small_cache();
    let pins = ValidationPins::new(cache.clone(), vec![], vec![], vec![schema(1)]);
    assert!(!pins.is_ready(pins.begin_attempt()));
    cache.borrow_mut().no_schema_cache_insert(schema(1));
    for minor in 10..20 {
        cache.borrow_mut().no_schema_cache_insert(schema(minor));
    }
    assert!(!cache.borrow().schema_cache_is_loaded(&schema(10)), "control: ordinary schema evicted at capacity 1");
    assert!(cache.borrow().schema_cache_is_loaded(&schema(1)));
    assert!(pins.is_ready(pins.begin_attempt()));
}

/// Load every key, then assert the attempt begun before `clear` never becomes
/// ready — neither straight after the clear nor after reloading across it —
/// while a fresh attempt after reloading is ready.
fn assert_clear_invalidates_attempt(clear: impl Fn(&mut ShardMemCache<NoValidation>), what: &str) {
    let cache = small_cache();
    let pins = ValidationPins::new(cache.clone(), vec![agg(1)], vec![client(1)], vec![schema(1)]);
    let reload = || {
        put_agg(&cache, 1);
        put_client(&cache, 1);
        cache.borrow_mut().no_schema_cache_insert(schema(1));
    };
    reload();
    let before = pins.begin_attempt();
    assert!(pins.is_ready(before), "{what}: ready before clear");

    let epoch = cache.borrow().load_epoch();
    clear(&mut cache.borrow_mut());
    assert_ne!(cache.borrow().load_epoch(), epoch, "{what}: load_epoch must change");
    assert!(!pins.is_ready(before), "{what}: attempt straddles clear");

    reload();
    assert!(!pins.is_ready(before), "{what}: value loaded across clear must not ready old attempt");
    assert!(pins.is_ready(pins.begin_attempt()), "{what}: fresh attempt after reload is ready");
}

#[test]
fn clear_aggregate_write_snapshots_invalidates_attempt() {
    assert_clear_invalidates_attempt(|c| c.clear_aggregate_write_snapshots_for_test(), "write snapshots");
}

#[test]
fn clear_aggregate_read_snapshots_invalidates_attempt() {
    assert_clear_invalidates_attempt(|c| c.clear_aggregate_read_snapshots_for_test(), "read snapshots");
}

#[test]
fn clear_client_snapshots_invalidates_attempt() {
    assert_clear_invalidates_attempt(|c| c.clear_aggregate_write_client_snapshots_for_test(), "client snapshots");
}

#[test]
fn schema_cache_clear_invalidates_attempt() {
    assert_clear_invalidates_attempt(|c| c.schema_cache_clear(), "schema cache");
}

#[test]
fn fsync_rollback_invalidates_attempt() {
    assert_clear_invalidates_attempt(|c| c.execute_fsync_rollback(), "fsync rollback");
}

#[test]
fn replication_rollback_invalidates_attempt() {
    assert_clear_invalidates_attempt(|c| c.execute_replication_rollback(), "replication rollback");
}

#[test]
fn speculative_cull_invalidates_attempt() {
    assert_clear_invalidates_attempt(
        |c| {
            c.clear_speculative_write_caches_for_cull();
        },
        "speculative cull",
    );
}

#[test]
fn clear_all_caches_invalidates_attempt() {
    assert_clear_invalidates_attempt(|c| c.clear_all_caches(), "clear all");
}

#[test]
fn compaction_remap_invalidates_attempt() {
    let new_tips = HashMap::from([(agg(1), 4096)]);
    assert_clear_invalidates_attempt(|c| c.remap_compacted_positions(1, &new_tips), "compaction remap");
}

#[test]
fn clear_drops_reserved_values_but_keeps_reservations() {
    let cache = small_cache();
    let pins = ValidationPins::new(cache.clone(), vec![agg(1)], vec![client(1)], vec![schema(1)]);
    put_agg(&cache, 1);
    put_client(&cache, 1);
    cache.borrow_mut().no_schema_cache_insert(schema(1));

    cache.borrow_mut().clear_all_caches();
    assert!(!agg_loaded(&cache, 1), "reserved aggregate value dropped");
    assert!(!client_loaded(&cache, 1), "reserved client value dropped");
    assert!(!cache.borrow().schema_cache_is_loaded(&schema(1)), "reserved schema value dropped");
    assert!(!pins.is_ready(pins.begin_attempt()), "must reload after clear");

    put_agg(&cache, 1);
    put_client(&cache, 1);
    cache.borrow_mut().no_schema_cache_insert(schema(1));
    for id in 10..20 {
        put_agg(&cache, id);
        put_client(&cache, id);
        cache.borrow_mut().no_schema_cache_insert(schema(id as u64));
    }
    assert!(pins.is_ready(pins.begin_attempt()), "reloaded values still reserved after clear");
}

#[test]
fn load_epoch_is_stable_across_puts_evictions_and_reservations() {
    let cache = small_cache();
    let epoch = cache.borrow().load_epoch();

    let pins = ValidationPins::new(cache.clone(), vec![agg(1)], vec![client(1)], vec![schema(1)]);
    assert_eq!(cache.borrow().load_epoch(), epoch, "reserving");
    put_agg(&cache, 1);
    put_client(&cache, 1);
    cache.borrow_mut().no_schema_cache_insert(schema(1));
    cache.borrow_mut().put_aggregate_into_cache_as_not_found(agg(2), CachePath::Write);
    for id in 10..20 {
        put_agg(&cache, id);
        put_client(&cache, id);
        cache.borrow_mut().no_schema_cache_insert(schema(id as u64));
    }
    assert_eq!(cache.borrow().load_epoch(), epoch, "puts and evictions");

    drop(pins);
    assert_eq!(cache.borrow().load_epoch(), epoch, "releasing, including release-driven eviction");
}
