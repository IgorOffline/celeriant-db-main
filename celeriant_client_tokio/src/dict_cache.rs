use std::sync::{Arc, LazyLock, Mutex};

use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;

use crate::celeriant_client::CachedDict;

/// One copy per process, shared by every cache.
static BUILTIN: LazyLock<CachedDict> = LazyLock::new(|| CachedDict {
    sha: celeriant_crypto::sha256_hex(BUILTIN_DICT_BYTES),
    bytes: Arc::from(BUILTIN_DICT_BYTES),
});

/// The dictionaries a client resolves without the server shipping them: the built-in every client links, and the one
/// cluster dictionary learned last. Clones share the slot, so a pool and all its connections learn a dictionary once.
/// Bounded at two dictionaries: learning a new cluster dictionary replaces the previous one.
#[derive(Clone)]
pub(crate) struct DictCache(Arc<Mutex<CachedDict>>);

impl Default for DictCache {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(BUILTIN.clone())))
    }
}

impl DictCache {
    /// The dictionary to advertise in Identify. Resolve the server's confirm against this snapshot with [`resolve`],
    /// since another connection may replace the slot before the response arrives.
    pub(crate) fn snapshot(&self) -> CachedDict {
        self.0.lock().unwrap().clone()
    }

    pub(crate) fn lookup(&self, sha: &str) -> Option<Arc<[u8]>> {
        resolve(&self.0.lock().unwrap(), sha)
    }

    pub(crate) fn learn(&self, dict: &CachedDict) {
        let mut learned = self.0.lock().unwrap();
        if learned.sha != dict.sha {
            *learned = dict.clone();
        }
    }
}

/// The bytes for `sha` if it is the advertised dictionary or the built-in.
pub(crate) fn resolve(advertised: &CachedDict, sha: &str) -> Option<Arc<[u8]>> {
    if advertised.sha == sha {
        return Some(Arc::clone(&advertised.bytes));
    }
    (BUILTIN.sha == sha).then(|| Arc::clone(&BUILTIN.bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict(sha: &str) -> CachedDict {
        CachedDict { sha: sha.to_string(), bytes: Arc::from(sha.as_bytes()) }
    }

    #[test]
    fn a_fresh_cache_advertises_and_resolves_the_builtin() {
        let cache = DictCache::default();
        let builtin_sha = celeriant_crypto::sha256_hex(BUILTIN_DICT_BYTES);

        assert_eq!(cache.snapshot().sha, builtin_sha);
        assert_eq!(cache.lookup(&builtin_sha).as_deref(), Some(BUILTIN_DICT_BYTES));
        assert!(cache.lookup("unknown").is_none());
    }

    #[test]
    fn every_clone_sees_a_learned_dict_without_copying_it() {
        let pool = DictCache::default();
        let connection = pool.clone();
        let cluster = dict("cluster");

        connection.learn(&cluster);

        assert_eq!(pool.snapshot().sha, "cluster");
        assert!(Arc::ptr_eq(&pool.lookup("cluster").unwrap(), &cluster.bytes));
    }

    #[test]
    fn learning_a_new_cluster_dict_evicts_the_old_one_but_never_the_builtin() {
        let cache = DictCache::default();
        cache.learn(&dict("first"));
        cache.learn(&dict("second"));

        assert!(cache.lookup("first").is_none());
        assert!(cache.lookup("second").is_some());
        assert!(cache.lookup(&BUILTIN.sha).is_some());
    }

    #[test]
    fn relearning_the_same_sha_keeps_the_first_copy() {
        let cache = DictCache::default();
        let first = dict("cluster");
        cache.learn(&first);
        cache.learn(&dict("cluster"));

        assert!(Arc::ptr_eq(&cache.lookup("cluster").unwrap(), &first.bytes));
    }
}
