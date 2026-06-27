//! Translated from PostgreSQL src/include/lib/dshash.h
//! Concurrent hash tables backed by dynamic shared memory.
//!
//! Shared memory is a non-goal under the single-process async model, so this
//! collapses to a `HashMap` behind a lock (translation-rules container table).
//! The opaque `dshash_table` becomes a generic container; the C find/enter/
//! remove functions become methods (function-mapping.md 4); the `dsa_area`,
//! handles, and per-backend function pointers fall away.

use std::collections::HashMap;
use parking_lot::Mutex;

/// The type for hash values.
pub type DsHashHash = u32;

/// Concurrent hash table. Was the opaque `dshash_table`; now a locked `HashMap`.
pub struct DsHashTable<K, V> {
    inner: Mutex<HashMap<K, V>>,
}

impl<K: std::hash::Hash + Eq, V> DsHashTable<K, V> {
    /// `dshash_create` (the `dshash_parameters`/tranche/`dsa_area` args vanish).
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// `dshash_find` (`exclusive` was the lock mode; std `Mutex` is exclusive).
    pub fn find(&self, _key: &K) -> Option<()> {
        unimplemented!()
    }

    /// `dshash_find_or_insert`; the `*found` out-param folds into the bool.
    pub fn find_or_insert(&self, _key: K) -> (/* entry */ (), /* found */ bool) {
        unimplemented!()
    }

    /// `dshash_delete_key`: true if an entry was removed.
    pub fn delete_key(&self, _key: &K) -> bool {
        unimplemented!()
    }

    /// `dshash_dump` (debugging support).
    pub fn dump(&self) {
        unimplemented!()
    }
}

impl<K: std::hash::Hash + Eq, V> Default for DsHashTable<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

// dshash_attach / dshash_detach / dshash_get_hash_table_handle / dshash_destroy:
// shared-memory attach/detach across processes -> no analog single-process.
// dshash_release_lock / dshash_delete_entry: the per-entry lock lifetime is the
// Mutex guard's scope in Rust. The seq-scan API (dshash_seq_init/next/term/
// delete_current) -> iterate the HashMap; modeled later when a caller needs it.
// The memcmp/strcmp/strcpy convenience fns are subsumed by Hash/Eq impls.
