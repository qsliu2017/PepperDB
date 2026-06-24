//! Translated from PostgreSQL src/include/utils/inval.h
//!
//! STUB (foundation-rewrite: invalidation). The cache-invalidation dispatcher
//! registration API. All bodies are `// TODO(invalidation)`. Uses
//! `crate::storage::sinval` message types in Phase 2.

use crate::access::htup::HeapTuple;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::relfilelocator::RelFileLocatorBackend;
use crate::utils::relcache::Relation;

// GUC -> process-global, deferred (Phase 2).
pub static mut debug_discard_caches: i32 = 0;

// Callback typedefs. The `Datum arg` opaque context maps to a captured closure
// (function-mapping 6.3); typedefs kept as fn-pointer aliases for the skeleton.
pub type SyscacheCallbackFunction = fn(arg: Datum, cacheid: i32, hashvalue: u32);
pub type RelcacheCallbackFunction = fn(arg: Datum, relid: Oid);
pub type RelSyncCallbackFunction = fn(arg: Datum, relid: Oid);

pub fn AcceptInvalidationMessages() {
    unimplemented!() // TODO(invalidation)
}

pub fn AtEOXact_Inval(_is_commit: bool) {
    unimplemented!() // TODO(invalidation)
}

pub fn PreInplace_Inval() {
    unimplemented!() // TODO(invalidation)
}
pub fn AtInplace_Inval() {
    unimplemented!() // TODO(invalidation)
}
pub fn ForgetInplace_Inval() {
    unimplemented!() // TODO(invalidation)
}

pub fn AtEOSubXact_Inval(_is_commit: bool) {
    unimplemented!() // TODO(invalidation)
}

pub fn PostPrepare_Inval() {
    unimplemented!() // TODO(invalidation)
}

pub fn CommandEndInvalidationMessages() {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateHeapTuple(_relation: Relation, _tuple: HeapTuple, _newtuple: HeapTuple) {
    unimplemented!() // TODO(invalidation)
}
pub fn CacheInvalidateHeapTupleInplace(_relation: Relation, _key_equivalent_tuple: HeapTuple) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateCatalog(_catalog_id: Oid) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateRelcache(_relation: Relation) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateRelcacheAll() {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateRelcacheByTuple(_class_tuple: HeapTuple) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateRelcacheByRelid(_relid: Oid) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateRelSync(_relid: Oid) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateRelSyncAll() {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateSmgr(_rlocator: RelFileLocatorBackend) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheInvalidateRelmap(_database_id: Oid) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheRegisterSyscacheCallback(
    _cacheid: i32,
    _func: SyscacheCallbackFunction,
    _arg: Datum,
) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheRegisterRelcacheCallback(_func: RelcacheCallbackFunction, _arg: Datum) {
    unimplemented!() // TODO(invalidation)
}

pub fn CacheRegisterRelSyncCallback(_func: RelSyncCallbackFunction, _arg: Datum) {
    unimplemented!() // TODO(invalidation)
}

pub fn CallSyscacheCallbacks(_cacheid: i32, _hashvalue: u32) {
    unimplemented!() // TODO(invalidation)
}

pub fn CallRelSyncCallbacks(_relid: Oid) {
    unimplemented!() // TODO(invalidation)
}

pub fn InvalidateSystemCaches() {
    unimplemented!() // TODO(invalidation)
}
pub fn InvalidateSystemCachesExtended(_debug_discard: bool) {
    unimplemented!() // TODO(invalidation)
}

pub fn LogLogicalInvalidations() {
    unimplemented!() // TODO(invalidation)
}
