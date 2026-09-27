pub(crate) mod pinned_lru;

use super::{LoadEpoch, ShardMemCache};
use crate::cached_schema::Validate;
use celeriant_wal::{aggregate_client_key::AggregateClientKey, aggregate_key::AggregateKey, schema_key::SchemaKey};
use std::cell::RefCell;
use std::rc::Rc;

/// Keeps the live cache values needed by one asynchronous validation attempt.
/// Concurrent requests share reservations; cancellation releases only its owner.
///
/// Validation loops: `begin_attempt`, load every key (awaits allowed), then
/// validate synchronously only if `is_ready`; otherwise start a new attempt.
pub struct ValidationPins<V: Validate> {
    cache: Rc<RefCell<ShardMemCache<V>>>,
    aggregates: Vec<AggregateKey>,
    clients: Vec<AggregateClientKey>,
    schemas: Vec<SchemaKey>,
}

impl<V: Validate> ValidationPins<V> {
    pub fn new(
        cache: Rc<RefCell<ShardMemCache<V>>>,
        aggregates: Vec<AggregateKey>,
        clients: Vec<AggregateClientKey>,
        schemas: Vec<SchemaKey>,
    ) -> Self {
        {
            let mut guarded = cache.borrow_mut();
            for key in &aggregates { guarded.aggregate_write_snapshots.pin(key.clone()); }
            for key in &clients { guarded.aggregate_write_client_snapshots.pin(key.clone()); }
            for key in &schemas {
                guarded.schema_cache.pin(key.clone());
                guarded.no_schema_cache.pin(key.clone());
            }
        }
        Self {
            cache,
            aggregates,
            clients,
            schemas,
        }
    }

    /// Take before the first load of a pass.
    pub fn begin_attempt(&self) -> Attempt {
        Attempt(self.cache.borrow().load_epoch())
    }

    /// Every reserved key is loaded, and nothing was cleared or rolled back
    /// since `attempt` began. A value loaded across a clear may describe disk
    /// state that no longer exists, so a changed epoch fails even when every
    /// key looks loaded.
    pub fn is_ready(&self, attempt: Attempt) -> bool {
        let mut cache = self.cache.borrow_mut();
        cache.load_epoch() == attempt.0 && cache.validation_keys_loaded(&self.aggregates, &self.clients, &self.schemas)
    }
}

/// One loading pass over the reserved keys. Only `begin_attempt` creates it.
#[derive(Clone, Copy)]
pub struct Attempt(LoadEpoch);

impl<V: Validate> Drop for ValidationPins<V> {
    fn drop(&mut self) {
        let mut cache = self.cache.borrow_mut();
        for key in &self.aggregates { cache.aggregate_write_snapshots.unpin(key); }
        for key in &self.clients { cache.aggregate_write_client_snapshots.unpin(key); }
        for key in &self.schemas {
            cache.schema_cache.unpin(key);
            cache.no_schema_cache.unpin(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_path::CachePath;
    struct Validator;
    impl Validate for Validator {
        fn validate(&self, _: &[u8]) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn dropping_a_validation_owner_restores_the_configured_cache_bound() {
        let cache = Rc::new(RefCell::new(ShardMemCache::<Validator>::new(0, 112, 128, 200, 0, u64::MAX)));
        let keys: Vec<_> = (0..32).map(|id| AggregateKey::new(1, 1, id)).collect();
        let pins = ValidationPins::new(cache.clone(), keys.clone(), vec![], vec![]);
        let attempt = pins.begin_attempt();
        assert!(!pins.is_ready(attempt));
        for key in keys {
            cache.borrow_mut().put_aggregate_into_cache_as_not_found(key, CachePath::Write);
        }
        assert!(pins.is_ready(attempt));
        assert_eq!(cache.borrow().aggregate_write_snapshots_len(), 32);
        drop(pins);
        assert_eq!(cache.borrow().aggregate_write_snapshots_len(), 1);
    }

    #[test]
    fn an_attempt_that_crossed_a_clear_is_not_ready_even_when_reloaded() {
        let cache = Rc::new(RefCell::new(ShardMemCache::<Validator>::new(0, 112, 128, 200, 0, u64::MAX)));
        let key = AggregateKey::new(1, 1, 1);
        let pins = ValidationPins::new(cache.clone(), vec![key.clone()], vec![], vec![]);
        let crossed = pins.begin_attempt();
        cache.borrow_mut().clear_aggregate_write_snapshots_for_test();
        cache.borrow_mut().put_aggregate_into_cache_as_not_found(key, CachePath::Write);
        assert!(!pins.is_ready(crossed));
        assert!(pins.is_ready(pins.begin_attempt()));
    }
}
