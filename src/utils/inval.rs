//! Translated from PostgreSQL src/include/utils/inval.h
//!
//! The cache-invalidation dispatcher registration API. The function bodies live
//! in `crate::backend::utils::cache::inval`; the header rewires to `pub use`
//! (non-type-centric global-state fns). The fn-pointer typedefs stay here.

use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// Callback typedefs. The `Datum arg` opaque context maps to a captured closure
// (function-mapping 6.3); typedefs kept as fn-pointer aliases for the skeleton.
pub type SyscacheCallbackFunction = fn(arg: Datum, cacheid: i32, hashvalue: u32);
pub type RelcacheCallbackFunction = fn(arg: Datum, relid: Oid);
pub type RelSyncCallbackFunction = fn(arg: Datum, relid: Oid);

// GUC: PG's file-global `int debug_discard_caches`. Backed by a process-global
// atomic in the backend module; read via the accessor (no `static mut`).
#[deprecated(note = "use crate::backend::utils::cache::inval::debug_discard_caches()")]
#[inline]
pub fn debug_discard_caches() -> i32 {
    crate::backend::utils::cache::inval::debug_discard_caches()
}

// inval.c function bodies live in the backend module; rewire the header stubs to
// `pub use` (non-type-centric global-state fns).
pub use crate::backend::utils::cache::inval::{
    accept_invalidation_messages as AcceptInvalidationMessages,
    at_eosubxact_inval as AtEOSubXact_Inval, at_eoxact_inval as AtEOXact_Inval,
    at_inplace_inval as AtInplace_Inval, cache_invalidate_catalog as CacheInvalidateCatalog,
    cache_invalidate_heap_tuple as CacheInvalidateHeapTuple,
    cache_invalidate_heap_tuple_inplace as CacheInvalidateHeapTupleInplace,
    cache_invalidate_rel_sync as CacheInvalidateRelSync,
    cache_invalidate_rel_sync_all as CacheInvalidateRelSyncAll,
    cache_invalidate_relcache as CacheInvalidateRelcache,
    cache_invalidate_relcache_all as CacheInvalidateRelcacheAll,
    cache_invalidate_relcache_by_relid as CacheInvalidateRelcacheByRelid,
    cache_invalidate_relcache_by_tuple as CacheInvalidateRelcacheByTuple,
    cache_invalidate_relmap as CacheInvalidateRelmap, cache_invalidate_smgr as CacheInvalidateSmgr,
    cache_register_rel_sync_callback as CacheRegisterRelSyncCallback,
    cache_register_relcache_callback as CacheRegisterRelcacheCallback,
    cache_register_syscache_callback as CacheRegisterSyscacheCallback,
    call_rel_sync_callbacks as CallRelSyncCallbacks,
    call_syscache_callbacks as CallSyscacheCallbacks,
    command_end_invalidation_messages as CommandEndInvalidationMessages,
    forget_inplace_inval as ForgetInplace_Inval, invalidate_system_caches as InvalidateSystemCaches,
    invalidate_system_caches_extended as InvalidateSystemCachesExtended,
    log_logical_invalidations as LogLogicalInvalidations, post_prepare_inval as PostPrepare_Inval,
    pre_inplace_inval as PreInplace_Inval,
};

// The following publics are declared in storage/sinval.h but defined in inval.c;
// rewire them here too.
pub use crate::backend::utils::cache::inval::{
    inplace_get_invalidation_messages as inplaceGetInvalidationMessages,
    local_execute_invalidation_message as LocalExecuteInvalidationMessage,
    process_committed_invalidation_messages as ProcessCommittedInvalidationMessages,
    xact_get_committed_invalidation_messages as xactGetCommittedInvalidationMessages,
};
