//! Type cache. Translated from the step-39-relevant parts of
//! `src/backend/utils/cache/typcache.c` (disposition: grow).
//!
//! Step 39 PHASE A: the composite-type tuple-descriptor cache that CREATE TYPE AS
//! (composite) populates lands in PHASE B. `lookup_type_cache` is the reachable
//! entry; its body fills in with the composite-type milestone.

use crate::postgres_ext::Oid;

/// PG `lookup_type_cache`: fetch (and lazily build) the cached metadata for a type.
/// PHASE B (composite rowtype descriptors for CREATE TYPE AS).
pub fn lookup_type_cache(_type_id: Oid, _flags: i32) {
    unimplemented!("lookup_type_cache: not yet translated (step 39 phase B)")
}
