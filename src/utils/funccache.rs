//! Translated from PostgreSQL src/include/utils/funccache.h
//! Function cache definitions. In-memory.

use crate::access::htup::HeapTuple;
use crate::access::tupdesc::TupleDesc;
use crate::c::{Size, TransactionId};
use crate::fmgr::FunctionCallInfo;
use crate::nodes::nodes::Node;
use crate::pg_config_manual::FUNC_MAX_ARGS;
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointerData;

/// Callback to compile a cached function. Fills in `*function` (except the
/// CachedFunction fields) or throws on trouble.
pub type CachedFunctionCompileCallback = fn(
    fcinfo: FunctionCallInfo,
    proc_tup: HeapTuple,
    hashkey: &CachedFunctionHashKey,
    function: &mut CachedFunction,
    for_validator: bool,
);

/// Callback invoked when discarding a cache entry; frees subsidiary data of
/// `cfunc` but not the struct itself.
pub type CachedFunctionDeleteCallback = fn(cfunc: &mut CachedFunction);

/// Hash lookup key for functions. Accounts for every call aspect that might
/// lead to different types/collations within the function.
pub struct CachedFunctionHashKey {
    pub funcOid: Oid,

    /// true if called as a DML trigger
    pub isTrigger: bool,
    /// true if called as an event trigger
    pub isEventTrigger: bool,

    /// language-specific size of the function's cache entry
    pub cacheEntrySize: Size,

    /// OID of the trigger (part of key for trigger functions); 0 if not a DML trigger
    pub trigOid: Oid,

    /// input collation (affects generated Param collations)
    pub inputCollation: Oid,

    /// number of input arguments (pronargs)
    pub nargs: i32,

    /// result descriptor for a composite-returning function, if relevant
    pub callResultType: TupleDesc,

    /// input argument types, polymorphic types resolved; first `nargs` valid
    pub argtypes: [Oid; FUNC_MAX_ARGS],
}

/// A compiled function. Contains just the fields funccache.c manages; typically
/// embedded in a larger language-specific struct.
pub struct CachedFunction {
    /// back-link to hashtable entry, or None if not in hash table. TODO(ptr).
    pub fn_hashkey: Option<*mut CachedFunctionHashKey>,
    /// xmin of function's pg_proc row; used to detect invalidation
    pub fn_xmin: TransactionId,
    /// ctid of function's pg_proc row
    pub fn_tid: ItemPointerData,
    /// deletion callback
    pub dcallback: Option<CachedFunctionDeleteCallback>,
    /// changes when the function is used
    pub use_count: u64,
}

pub fn cached_function_compile(
    _fcinfo: FunctionCallInfo,
    _function: &mut CachedFunction,
    _ccallback: CachedFunctionCompileCallback,
    _dcallback: CachedFunctionDeleteCallback,
    _cache_entry_size: Size,
    _include_result_type: bool,
    _for_validator: bool,
) -> *mut CachedFunction {
    unimplemented!() // TODO(ptr)
}

/// In C, `argtypes`/`argmodes` are in/out arrays mutated in place.
pub fn cfunc_resolve_polymorphic_argtypes(
    _numargs: i32,
    _argtypes: &mut [Oid],
    _argmodes: &[u8],
    _call_expr: &Node,
    _for_validator: bool,
    _proname: &str,
) {
    unimplemented!()
}
