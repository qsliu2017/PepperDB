//! Translated from PostgreSQL src/include/fmgr.h
//! Function manager and function-call interface.

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
    pub fn_oid: Oid,                 // OID of function (NOT of handler, if any)
    pub fn_nargs: i16,               // number of input args (0..FUNC_MAX_ARGS)
    pub fn_strict: bool,             // function is "strict" (NULL in => NULL out)
    pub fn_retset: bool,             // function returns a set
    pub fn_stats: u8,                // collect stats if track_functions > this
    pub fn_extra: usize,             // extra space for use by handler (void *) TODO(ptr)
    pub fn_mcxt: MemoryContext,      // memory context to store fn_extra in
    pub fn_expr: fmNodePtr,          // expression parse tree for call, or NULL
}

pub const FIELDNO_FUNCTIONCALLINFODATA_ISNULL: usize = 4;
pub const FIELDNO_FUNCTIONCALLINFODATA_ARGS: usize = 6;

/// Data actually passed to an fmgr-called function.
/// In-memory struct; the C FLEXIBLE_ARRAY_MEMBER `args[]` becomes a `Vec`.
#[allow(deprecated)]
pub struct FunctionCallInfoBaseData {
    pub flinfo: Option<Box<FmgrInfo>>, // ptr to lookup info used for this call TODO(ptr)
    pub context: fmNodePtr,            // pass info about context of call
    pub resultinfo: fmNodePtr,         // pass or return extra info about result
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
    context: fmNodePtr,
    resultinfo: fmNodePtr,
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
    let f = fcinfo.flinfo.as_ref().and_then(|fi| fi.fn_addr).unwrap();
    f(fcinfo)
}

/// C: `#define fmgr_info_set_expr(expr, finfo)`
pub fn fmgr_info_set_expr(expr: fmNodePtr, finfo: &mut FmgrInfo) {
    finfo.fn_expr = expr;
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
    FHET_START,
    FHET_END,
    FHET_ABORT,
}

/// C: `typedef bool (*needs_fmgr_hook_type) (Oid fn_oid);`
pub type needs_fmgr_hook_type = fn(fn_oid: Oid) -> bool;

/// C: `typedef void (*fmgr_hook_type) (FmgrHookEventType event, FmgrInfo *flinfo, Datum *arg);`
pub type fmgr_hook_type = fn(event: FmgrHookEventType, flinfo: &mut FmgrInfo, arg: &mut Datum);

/// Opaque outside dfmgr.c. C: `typedef struct DynamicFileList DynamicFileList;`
pub struct DynamicFileList;

// ---- Routines in fmgr.c / dfmgr.c / nodeAgg.c -----------------------------
// Bodies unimplemented; signatures translated per function-mapping.md.

#[allow(deprecated)]
pub fn fmgr_info(_function_id: Oid, _finfo: &mut FmgrInfo) {
    unimplemented!()
}
#[allow(deprecated)]
pub fn fmgr_info_cxt(_function_id: Oid, _finfo: &mut FmgrInfo, _mcxt: MemoryContext) {
    unimplemented!()
}
#[allow(deprecated)]
pub fn fmgr_info_copy(_dstinfo: &mut FmgrInfo, _srcinfo: &mut FmgrInfo, _destcxt: MemoryContext) {
    unimplemented!()
}
/// Two string out-params -> tuple (module, fn).
pub fn fmgr_symbol(_function_id: Oid) -> (String, String) {
    unimplemented!()
}

// Detoast returns either the input datum (borrow) or a palloc'd copy; the
// header alone does not reveal which, so ownership stays raw for now.
#[allow(deprecated)]
pub fn pg_detoast_datum(_datum: *mut varlena) -> *mut varlena {
    // TODO(ptr)
    unimplemented!()
}
#[allow(deprecated)]
pub fn pg_detoast_datum_copy(_datum: *mut varlena) -> *mut varlena {
    // TODO(ptr)
    unimplemented!()
}
#[allow(deprecated)]
pub fn pg_detoast_datum_slice(_datum: *mut varlena, _first: i32, _count: i32) -> *mut varlena {
    // TODO(ptr)
    unimplemented!()
}
#[allow(deprecated)]
pub fn pg_detoast_datum_packed(_datum: *mut varlena) -> *mut varlena {
    // TODO(ptr)
    unimplemented!()
}

// DirectFunctionCallN: invoke a named function with a computed argument list.
// Result folds fcinfo->isnull: None == SQL NULL.
pub fn DirectFunctionCall1Coll(_func: PGFunction, _collation: Oid, _arg1: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn DirectFunctionCall2Coll(_func: PGFunction, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn DirectFunctionCall3Coll(_func: PGFunction, _collation: Oid, _a1: Datum, _a2: Datum, _a3: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn DirectFunctionCall4Coll(_func: PGFunction, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn DirectFunctionCall5Coll(_func: PGFunction, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn DirectFunctionCall6Coll(_func: PGFunction, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn DirectFunctionCall7Coll(_func: PGFunction, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn DirectFunctionCall8Coll(_func: PGFunction, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum, _a8: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn DirectFunctionCall9Coll(_func: PGFunction, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum, _a8: Datum, _a9: Datum) -> Option<Datum> {
    unimplemented!()
}

pub fn CallerFInfoFunctionCall1(_func: PGFunction, _flinfo: &mut FmgrInfo, _collation: Oid, _arg1: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn CallerFInfoFunctionCall2(_func: PGFunction, _flinfo: &mut FmgrInfo, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Option<Datum> {
    unimplemented!()
}

// FunctionCallNColl: invoke a previously-looked-up function.
pub fn FunctionCall0Coll(_flinfo: &mut FmgrInfo, _collation: Oid) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall1Coll(_flinfo: &mut FmgrInfo, _collation: Oid, _arg1: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall2Coll(_flinfo: &mut FmgrInfo, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall3Coll(_flinfo: &mut FmgrInfo, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall4Coll(_flinfo: &mut FmgrInfo, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall5Coll(_flinfo: &mut FmgrInfo, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall6Coll(_flinfo: &mut FmgrInfo, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall7Coll(_flinfo: &mut FmgrInfo, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall8Coll(_flinfo: &mut FmgrInfo, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum, _a8: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn FunctionCall9Coll(_flinfo: &mut FmgrInfo, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum, _a8: Datum, _a9: Datum) -> Option<Datum> {
    unimplemented!()
}

// OidFunctionCallNColl: fmgr_info() followed by FunctionCallN().
pub fn OidFunctionCall0Coll(_function_id: Oid, _collation: Oid) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall1Coll(_function_id: Oid, _collation: Oid, _arg1: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall2Coll(_function_id: Oid, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall3Coll(_fid: Oid, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall4Coll(_fid: Oid, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall5Coll(_fid: Oid, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall6Coll(_fid: Oid, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall7Coll(_fid: Oid, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall8Coll(_fid: Oid, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum, _a8: Datum) -> Option<Datum> {
    unimplemented!()
}
pub fn OidFunctionCall9Coll(_fid: Oid, _c: Oid, _a1: Datum, _a2: Datum, _a3: Datum, _a4: Datum, _a5: Datum, _a6: Datum, _a7: Datum, _a8: Datum, _a9: Datum) -> Option<Datum> {
    unimplemented!()
}

// Non-Coll convenience wrappers default collation to InvalidOid.
pub fn DirectFunctionCall1(func: PGFunction, arg1: Datum) -> Option<Datum> {
    DirectFunctionCall1Coll(func, InvalidOid, arg1)
}
pub fn DirectFunctionCall2(func: PGFunction, arg1: Datum, arg2: Datum) -> Option<Datum> {
    DirectFunctionCall2Coll(func, InvalidOid, arg1, arg2)
}
pub fn FunctionCall1(flinfo: &mut FmgrInfo, arg1: Datum) -> Option<Datum> {
    FunctionCall1Coll(flinfo, InvalidOid, arg1)
}
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

// Datatype I/O convenience invocations.
#[allow(deprecated)]
pub fn InputFunctionCall(_flinfo: &mut FmgrInfo, _str: &str, _typioparam: Oid, _typmod: i32) -> Option<Datum> {
    unimplemented!()
}
/// Soft-error variant: bool success + Datum out-param -> Option<Datum>.
#[allow(deprecated)]
pub fn InputFunctionCallSafe(_flinfo: &mut FmgrInfo, _str: &str, _typioparam: Oid, _typmod: i32, _escontext: fmNodePtr) -> Option<Datum> {
    unimplemented!()
}
#[allow(deprecated)]
pub fn DirectInputFunctionCallSafe(_func: PGFunction, _str: &str, _typioparam: Oid, _typmod: i32, _escontext: fmNodePtr) -> Option<Datum> {
    unimplemented!()
}
#[allow(deprecated)]
pub fn OidInputFunctionCall(_function_id: Oid, _str: &str, _typioparam: Oid, _typmod: i32) -> Option<Datum> {
    unimplemented!()
}
#[allow(deprecated)]
pub fn OutputFunctionCall(_flinfo: &mut FmgrInfo, _val: Datum) -> String {
    unimplemented!()
}
pub fn OidOutputFunctionCall(_function_id: Oid, _val: Datum) -> String {
    unimplemented!()
}
#[allow(deprecated)]
pub fn ReceiveFunctionCall(_flinfo: &mut FmgrInfo, _buf: fmStringInfo, _typioparam: Oid, _typmod: i32) -> Option<Datum> {
    unimplemented!()
}
#[allow(deprecated)]
pub fn OidReceiveFunctionCall(_function_id: Oid, _buf: fmStringInfo, _typioparam: Oid, _typmod: i32) -> Option<Datum> {
    unimplemented!()
}
// Send produces a freshly palloc'd bytea; the caller owns it -> Box.
#[allow(deprecated)]
pub fn SendFunctionCall(_flinfo: &mut FmgrInfo, _val: Datum) -> Box<bytea> {
    unimplemented!()
}
#[allow(deprecated)]
pub fn OidSendFunctionCall(_function_id: Oid, _val: Datum) -> Box<bytea> {
    unimplemented!()
}

// Routines in fmgr.c (lookups -> Option<Oid> for sentinel returns).
pub fn fetch_finfo_record(_filehandle: usize, _funcname: &str) -> &'static Pg_finfo_record {
    unimplemented!()
}
pub fn fmgr_internal_function(_proname: &str) -> Option<Oid> {
    unimplemented!()
}
#[allow(deprecated)]
pub fn get_fn_expr_rettype(_flinfo: &mut FmgrInfo) -> Oid {
    unimplemented!()
}
#[allow(deprecated)]
pub fn get_fn_expr_argtype(_flinfo: &mut FmgrInfo, _argnum: i32) -> Oid {
    unimplemented!()
}
pub fn get_call_expr_argtype(_expr: fmNodePtr, _argnum: i32) -> Oid {
    unimplemented!()
}
#[allow(deprecated)]
pub fn get_fn_expr_arg_stable(_flinfo: &mut FmgrInfo, _argnum: i32) -> bool {
    unimplemented!()
}
pub fn get_call_expr_arg_stable(_expr: fmNodePtr, _argnum: i32) -> bool {
    unimplemented!()
}
#[allow(deprecated)]
pub fn get_fn_expr_variadic(_flinfo: &mut FmgrInfo) -> bool {
    unimplemented!()
}
// Returns the opclass-options bytea cached in flinfo; borrow vs palloc'd copy
// is not clear from the header.
#[allow(deprecated)]
pub fn get_fn_opclass_options(_flinfo: &mut FmgrInfo) -> *mut bytea {
    // TODO(ptr)
    unimplemented!()
}
#[allow(deprecated)]
pub fn has_fn_opclass_options(_flinfo: &mut FmgrInfo) -> bool {
    unimplemented!()
}
#[allow(deprecated)]
pub fn set_fn_opclass_options(_flinfo: &mut FmgrInfo, _options: *mut bytea) {
    // TODO(ptr)
    unimplemented!()
}
pub fn CheckFunctionValidatorAccess(_validator_oid: Oid, _function_oid: Oid) -> bool {
    unimplemented!()
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
