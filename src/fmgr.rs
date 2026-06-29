//! Translated from PostgreSQL src/include/fmgr.h
//! Function manager and function-call interface.

#![allow(clippy::similar_names, reason = "names mirror PG identifiers")]

use crate::c::{bytea, varlena};
use crate::lib::stringinfo::StringInfo;
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::Aggref;
use crate::postgres::{Datum, NullableDatum};
use crate::postgres_ext::{InvalidOid, Oid};
pub use crate::utils::palloc::MemoryContext;

// Rule-7 forward decls: "We don't want to include primnodes.h here".
// C: `typedef struct Node *fmNodePtr;` (real def nodes/nodes.h)
pub type fmNodePtr = *mut Node; // TODO(ptr)

// C: `typedef struct Aggref *fmAggrefPtr;` (real def nodes/primnodes.h)
pub type fmAggrefPtr = *mut Aggref; // TODO(ptr)

// C: `typedef struct StringInfoData *fmStringInfo;` (real def lib/stringinfo.h)
pub type fmStringInfo = *mut StringInfo; // TODO(ptr)

/// C: `typedef void (*fmExprContextCallbackFunction) (Datum arg);` (avoid execnodes.h)
pub type fmExprContextCallbackFunction = fn(arg: Datum);

/// Runtime-dispatch vtable entry: all fmgr-callable functions have this signature.
/// C: `typedef Datum (*PGFunction) (FunctionCallInfo fcinfo);`
pub type PGFunction = fn(fcinfo: &mut FunctionCallInfoBaseData) -> Datum;

/// C: `typedef struct FunctionCallInfoBaseData *FunctionCallInfo;`
pub type FunctionCallInfo<'a> = &'a mut FunctionCallInfoBaseData;

/// System-catalog info looked up before a function can be called through fmgr.
#[allow(deprecated)]
pub struct FmgrInfo {
    pub fn_addr: Option<PGFunction>, // pointer to function or handler to be called
    pub oid: Oid,                 // OID of function (NOT of handler, if any)
    pub nargs: i16,               // number of input args (0..FUNC_MAX_ARGS)
    pub strict: bool,             // function is "strict" (NULL in => NULL out)
    pub retset: bool,             // function returns a set
    pub stats: u8,                // collect stats if track_functions > this
    pub extra: usize,             // extra space for use by handler (void *) TODO(ptr)
    /// Memory context to store `extra` in. Tombstoned in this port (the C
    /// `fn_mcxt` is only consulted by the palloc that no longer exists), so it
    /// carries no handle -- a unit, keeping the struct genuinely `Send`.
    pub mcxt: (),
    /// Expression parse tree for the call, or `None`. Owned `Option<Box<Node>>`
    /// (was a raw `*mut Node`); `None` on the support/scankey path, so the
    /// struct is genuinely `Send` without an `unsafe impl`.
    pub expr: Option<Box<Node>>,
}

pub const FIELDNO_FUNCTIONCALLINFODATA_ISNULL: usize = 4;
pub const FIELDNO_FUNCTIONCALLINFODATA_ARGS: usize = 6;

/// Data actually passed to an fmgr-called function.
/// In-memory struct; the C FLEXIBLE_ARRAY_MEMBER `args[]` becomes a `Vec`.
#[allow(deprecated)]
pub struct FunctionCallInfoBaseData {
    pub flinfo: Option<Box<FmgrInfo>>, // ptr to lookup info used for this call TODO(ptr)
    /// Call-context node, or `None`. Owned `Option<Box<Node>>` (was a raw
    /// `*mut Node`); `None` on every live path (only set by trigger/agg/SRF
    /// callers that don't exist yet), so the struct stays genuinely `Send`.
    pub context: Option<Box<Node>>, // pass info about context of call
    /// Result-info node, or `None`. Same owned representation as `context`.
    pub resultinfo: Option<Box<Node>>, // pass or return extra info about result
    pub fncollation: Oid,              // collation for function to use
    pub isnull: bool,                  // function must set true if result is NULL
    pub nargs: i16,                    // # arguments actually passed
    pub args: Vec<NullableDatum>,      // C: NullableDatum args[FLEXIBLE_ARRAY_MEMBER]
}

/// C: `#define SizeForFunctionCallInfo(nargs)` - bytes for an fcinfo with `nargs` args.
pub const fn SizeForFunctionCallInfo(nargs: usize) -> usize {
    // offsetof(args) + sizeof(NullableDatum) * nargs
    core::mem::size_of::<FunctionCallInfoBaseData>()
        + core::mem::size_of::<NullableDatum>() * nargs
}

/// C: `#define InitFunctionCallInfoData(...)` - initializes all fields except args[].
pub fn InitFunctionCallInfoData(
    fcinfo: &mut FunctionCallInfoBaseData,
    flinfo: Option<Box<FmgrInfo>>,
    nargs: i16,
    collation: Oid,
    context: Option<Box<Node>>,
    resultinfo: Option<Box<Node>>,
) {
    fcinfo.flinfo = flinfo;
    fcinfo.context = context;
    fcinfo.resultinfo = resultinfo;
    fcinfo.fncollation = collation;
    fcinfo.isnull = false;
    fcinfo.nargs = nargs;
}

/// C: `#define FunctionCallInvoke(fcinfo)` - invoke via the looked-up fn_addr.
pub fn FunctionCallInvoke(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    #[allow(
        clippy::unwrap_used,
        reason = "FunctionCallInvoke is only reached after fmgr_info installs fn_addr"
    )]
    let f = fcinfo.flinfo.as_ref().and_then(|fi| fi.fn_addr).unwrap();
    f(fcinfo)
}

/// C: `#define fmgr_info_set_expr(expr, finfo)`
#[allow(deprecated)]
pub fn fmgr_info_set_expr(expr: Option<Box<Node>>, finfo: &mut FmgrInfo) {
    finfo.expr = expr;
}

// PG_GET_COLLATION / PG_NARGS / PG_ARGISNULL are caller-side accessors on fcinfo.
pub fn PG_GET_COLLATION(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo.fncollation
}
pub fn PG_NARGS(fcinfo: &FunctionCallInfoBaseData) -> i16 {
    fcinfo.nargs
}
pub fn PG_ARGISNULL(fcinfo: &FunctionCallInfoBaseData, n: usize) -> bool {
    fcinfo.args[n].isnull
}

/// Detect call convention of dynamically-loaded functions (version-1 only).
pub struct Pg_finfo_record {
    pub api_version: i32, // specifies call convention version number
}

/// C: `typedef const Pg_finfo_record *(*PGFInfoFunction) (void);`
pub type PGFInfoFunction = fn() -> &'static Pg_finfo_record;

/// Values checked to verify ABI compatibility (compared with memcmp in C).
#[repr(C)]
pub struct Pg_abi_values {
    pub version: i32,        // PostgreSQL major version
    pub funcmaxargs: i32,    // FUNC_MAX_ARGS
    pub indexmaxkeys: i32,   // INDEX_MAX_KEYS
    pub namedatalen: i32,    // NAMEDATALEN
    pub float8byval: i32,    // FLOAT8PASSBYVAL
    pub abi_extra: [u8; 32], // see pg_config_manual.h
}

/// The magic block structure for loadable modules.
pub struct Pg_magic_struct {
    pub len: i32,                 // sizeof(this struct)
    pub abi_fields: Pg_abi_values,
    pub name: Option<&'static str>,    // optional module name
    pub version: Option<&'static str>, // optional module version
}

/// C: `typedef const Pg_magic_struct *(*PGModuleMagicFunction) (void);`
pub type PGModuleMagicFunction = fn() -> &'static Pg_magic_struct;

pub const PG_MAGIC_FUNCTION_NAME_STRING: &str = "Pg_magic_func";

// AggCheckCallContext return codes (0 means neither).
pub const AGG_CONTEXT_AGGREGATE: i32 = 1; // regular aggregate
pub const AGG_CONTEXT_WINDOW: i32 = 2; // window function

/// Plugin hook event type for function entry/exit.
pub enum FmgrHookEventType {
    START,
    END,
    ABORT,
}

/// C: `typedef bool (*needs_fmgr_hook_type) (Oid oid);`
pub type needs_fmgr_hook_type = fn(oid: Oid) -> bool;

/// C: `typedef void (*fmgr_hook_type) (FmgrHookEventType event, FmgrInfo *flinfo, Datum *arg);`
pub type fmgr_hook_type = fn(event: FmgrHookEventType, flinfo: &mut FmgrInfo, arg: &mut Datum);

/// Opaque outside dfmgr.c. C: `typedef struct DynamicFileList DynamicFileList;`
pub struct DynamicFileList;

// ---- Routines in fmgr.c -----------------------------------------------------
// Bodies live in the backend definition module (crate::backend::utils::fmgr::fmgr)
// per rules.md s3; re-export them so `crate::fmgr::<name>` keeps resolving.

pub use crate::backend::utils::fmgr::fmgr::{
    fetch_finfo_record, fmgr_info, fmgr_info_copy, fmgr_info_cxt, fmgr_internal_function,
    fmgr_symbol, CheckFunctionValidatorAccess,
};
pub use crate::backend::utils::fmgr::fmgr::{
    pg_detoast_datum, pg_detoast_datum_copy, pg_detoast_datum_packed, pg_detoast_datum_slice,
};
pub use crate::backend::utils::fmgr::fmgr::{
    get_call_expr_arg_stable, get_call_expr_argtype, get_fn_expr_arg_stable, get_fn_expr_argtype,
    get_fn_expr_rettype, get_fn_expr_variadic, get_fn_opclass_options, has_fn_opclass_options,
    set_fn_opclass_options,
};

// DirectFunctionCallN: invoke a named function with a computed argument list.
pub use crate::backend::utils::fmgr::fmgr::{
    DirectFunctionCall1Coll, DirectFunctionCall2Coll, DirectFunctionCall3Coll,
    DirectFunctionCall4Coll, DirectFunctionCall5Coll, DirectFunctionCall6Coll,
    DirectFunctionCall7Coll, DirectFunctionCall8Coll, DirectFunctionCall9Coll,
};
pub use crate::backend::utils::fmgr::fmgr::{CallerFInfoFunctionCall1, CallerFInfoFunctionCall2};

// FunctionCallNColl: invoke a previously-looked-up function.
pub use crate::backend::utils::fmgr::fmgr::{
    FunctionCall0Coll, FunctionCall1Coll, FunctionCall2Coll, FunctionCall3Coll, FunctionCall4Coll,
    FunctionCall5Coll, FunctionCall6Coll, FunctionCall7Coll, FunctionCall8Coll, FunctionCall9Coll,
};

// OidFunctionCallNColl: fmgr_info() followed by FunctionCallN().
pub use crate::backend::utils::fmgr::fmgr::{
    OidFunctionCall0Coll, OidFunctionCall1Coll, OidFunctionCall2Coll, OidFunctionCall3Coll,
    OidFunctionCall4Coll, OidFunctionCall5Coll, OidFunctionCall6Coll, OidFunctionCall7Coll,
    OidFunctionCall8Coll, OidFunctionCall9Coll,
};

// Datatype I/O convenience invocations.
pub use crate::backend::utils::fmgr::fmgr::{
    DirectInputFunctionCallSafe, InputFunctionCall, InputFunctionCallSafe, OidInputFunctionCall,
    OidOutputFunctionCall, OidReceiveFunctionCall, OidSendFunctionCall, OutputFunctionCall,
    ReceiveFunctionCall, SendFunctionCall,
};

// Non-Coll convenience wrappers default collation to InvalidOid. These are C
// macros in fmgr.h (no .c body); keep them here delegating to the *Coll forms.
pub fn DirectFunctionCall1(func: PGFunction, arg1: Datum) -> Option<Datum> {
    DirectFunctionCall1Coll(func, InvalidOid, arg1)
}
pub fn DirectFunctionCall2(func: PGFunction, arg1: Datum, arg2: Datum) -> Option<Datum> {
    DirectFunctionCall2Coll(func, InvalidOid, arg1, arg2)
}
#[allow(deprecated)]
pub fn FunctionCall1(flinfo: &mut FmgrInfo, arg1: Datum) -> Option<Datum> {
    FunctionCall1Coll(flinfo, InvalidOid, arg1)
}
#[allow(deprecated)]
pub fn FunctionCall2(flinfo: &mut FmgrInfo, arg1: Datum, arg2: Datum) -> Option<Datum> {
    FunctionCall2Coll(flinfo, InvalidOid, arg1, arg2)
}
pub fn OidFunctionCall0(function_id: Oid) -> Option<Datum> {
    OidFunctionCall0Coll(function_id, InvalidOid)
}
pub fn OidFunctionCall1(function_id: Oid, arg1: Datum) -> Option<Datum> {
    OidFunctionCall1Coll(function_id, InvalidOid, arg1)
}
pub fn OidFunctionCall2(function_id: Oid, arg1: Datum, arg2: Datum) -> Option<Datum> {
    OidFunctionCall2Coll(function_id, InvalidOid, arg1, arg2)
}

// Routines in dfmgr.c.
pub fn substitute_path_macro(_str: &str, _macro: &str, _value: &str) -> String {
    unimplemented!()
}
pub fn find_in_path(_basename: &str, _path: &str, _path_param: &str, _macro: &str, _macro_val: &str) -> Option<String> {
    unimplemented!()
}
/// void* return + void** filehandle out-param -> (ptr, filehandle) tuple.
pub fn load_external_function(_filename: &str, _funcname: &str, _signal_not_found: bool) -> (usize, usize) {
    unimplemented!()
}
pub fn lookup_external_function(_filehandle: usize, _funcname: &str) -> usize {
    unimplemented!()
}
pub fn load_file(_filename: &str, _restricted: bool) {
    unimplemented!()
}
pub fn get_first_loaded_module() -> Option<Box<DynamicFileList>> {
    unimplemented!()
}
pub fn get_next_loaded_module(_dfptr: &mut DynamicFileList) -> Option<Box<DynamicFileList>> {
    unimplemented!()
}
/// Three string out-params -> (library_path, module_name, module_version).
pub fn get_loaded_module_details(_dfptr: &mut DynamicFileList) -> (String, String, String) {
    unimplemented!()
}
pub fn find_rendezvous_variable(_var_name: &str) -> usize {
    unimplemented!()
}
pub fn EstimateLibraryStateSpace() -> usize {
    unimplemented!()
}
pub fn SerializeLibraryState(_maxsize: usize, _start_address: &mut [u8]) {
    unimplemented!()
}
pub fn RestoreLibraryState(_start_address: &mut [u8]) {
    unimplemented!()
}

// Aggregate support (actually in nodeAgg.c).
/// MemoryContext out-param -> folded into Option tuple with the int code.
pub fn AggCheckCallContext(_fcinfo: &mut FunctionCallInfoBaseData) -> (i32, Option<MemoryContext>) {
    unimplemented!()
}
pub fn AggGetAggref(_fcinfo: &mut FunctionCallInfoBaseData) -> fmAggrefPtr {
    unimplemented!()
}
pub fn AggGetTempMemoryContext(_fcinfo: &mut FunctionCallInfoBaseData) -> MemoryContext {
    unimplemented!()
}
pub fn AggStateIsShared(_fcinfo: &mut FunctionCallInfoBaseData) -> bool {
    unimplemented!()
}
pub fn AggRegisterCallback(_fcinfo: &mut FunctionCallInfoBaseData, _func: fmExprContextCallbackFunction, _arg: Datum) {
    unimplemented!()
}
