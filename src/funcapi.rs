//! Translated from PostgreSQL src/include/funcapi.h
//!
//! Helpers for functions returning composite types and/or sets, and for VARIADIC
//! inputs. The SRF_* macros become free functions taking the fcinfo explicitly;
//! `get_call_result_type` & siblings fold their out-params into a returned tuple
//! per function-mapping.md 5.

use bitflags::bitflags;

use crate::access::htup::{HeapTuple, HeapTupleData, HeapTupleHeaderData};
use crate::access::tupdesc::TupleDesc;
use crate::fmgr::{FmgrInfo, FunctionCallInfo, MemoryContext};
use crate::nodes::execnodes::{ExprDoneCond, ReturnSetInfo};
use crate::nodes::nodes::Node;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

/// Per-attribute metadata to build a tuple from raw C strings.
pub struct AttInMetadata {
    pub tupdesc: TupleDesc,
    /// array of attribute type input function finfo.
    pub attinfuncs: Vec<FmgrInfo>,
    /// array of attribute type i/o parameter OIDs.
    pub attioparams: Vec<Oid>,
    /// array of attribute typmod.
    pub atttypmods: Vec<i32>,
}

/// Function context for Set Returning Functions (held via `extra`).
pub struct FuncCallContext {
    /// number of times we've been called before.
    pub call_cntr: u64,
    /// OPTIONAL maximum number of calls.
    pub max_calls: u64,
    /// OPTIONAL user-provided context (`void *user_fctx`). TODO(ptr)
    pub user_fctx: usize,
    /// OPTIONAL attribute input metadata for returning composite types.
    pub attinmeta: Option<Box<AttInMetadata>>,
    /// memory context that lives across multiple calls.
    pub multi_call_memory_ctx: MemoryContext,
    /// OPTIONAL tuple description for heap_form_tuple-built tuples.
    pub tuple_desc: TupleDesc,
}

/// Type categories for `get_call_result_type` and siblings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeFuncClass {
    /// scalar result type.
    Scalar,
    /// determinable rowtype result.
    Composite,
    /// domain over determinable rowtype result.
    CompositeDomain,
    /// indeterminate rowtype result.
    Record,
    /// bogus type, eg pseudotype.
    Other,
}

/// Result of `get_call_result_type` & siblings: the class plus the two former
/// out-params (`*resultTypeId`, `*resultTupleDesc`) folded in.
pub struct ResultTypeInfo {
    pub class: TypeFuncClass,
    /// actual result datatype OID (None when not determinable).
    pub result_type_id: Option<Oid>,
    /// result TupleDesc for composite results (None for scalar/indeterminate).
    pub result_tuple_desc: Option<TupleDesc>,
}

pub fn get_call_result_type(_fcinfo: FunctionCallInfo) -> ResultTypeInfo {
    unimplemented!()
}
pub fn get_expr_result_type(_expr: &Node) -> ResultTypeInfo {
    unimplemented!()
}
pub fn get_func_result_type(_function_id: Oid) -> ResultTypeInfo {
    unimplemented!()
}

/// Convenience wrapper around `get_expr_result_type` for determinable rowtypes.
pub fn get_expr_result_tupdesc(_expr: &Node, _no_error: bool) -> Option<TupleDesc> {
    unimplemented!()
}

pub fn resolve_polymorphic_argtypes(
    _argtypes: &mut [Oid],
    _argmodes: Option<&[u8]>,
    _call_expr: Option<&Node>,
) -> bool {
    unimplemented!()
}

/// Function argument info (out-params `p_argtypes`/`p_argnames`/`p_argmodes`
/// -> a returned struct); the int return was the arg count = vec lengths.
pub struct FuncArgInfo {
    pub argtypes: Vec<Oid>,
    pub argnames: Option<Vec<String>>,
    pub argmodes: Option<Vec<u8>>,
}

pub fn get_func_arg_info(_proc_tup: HeapTuple) -> FuncArgInfo {
    unimplemented!()
}

pub fn get_func_input_arg_names(_proargnames: Datum, _proargmodes: Datum) -> Vec<String> {
    unimplemented!()
}

pub fn get_func_trftypes(_proc_tup: HeapTuple) -> Vec<Oid> {
    unimplemented!()
}
pub fn get_func_result_name(_function_id: Oid) -> Option<String> {
    unimplemented!()
}

pub fn build_function_result_tupdesc_d(
    _prokind: u8,
    _proallargtypes: Datum,
    _proargmodes: Datum,
    _proargnames: Datum,
) -> Option<TupleDesc> {
    unimplemented!()
}
pub fn build_function_result_tupdesc_t(_proc_tuple: HeapTuple) -> Option<TupleDesc> {
    unimplemented!()
}

pub fn RelationNameGetTupleDesc(_relname: &str) -> TupleDesc {
    unimplemented!()
}
pub fn TypeGetTupleDesc(_typeoid: Oid, _colaliases: &[Box<Node>]) -> TupleDesc {
    unimplemented!()
}

// from execTuples.c
pub fn BlessTupleDesc(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!()
}
pub fn TupleDescGetAttInMetadata(_tupdesc: TupleDesc) -> Box<AttInMetadata> {
    unimplemented!()
}
pub fn BuildTupleFromCStrings(_attinmeta: &AttInMetadata, _values: &[&str]) -> HeapTuple {
    unimplemented!()
}
pub fn HeapTupleHeaderGetDatum(_tuple: *mut HeapTupleHeaderData) -> Datum {
    unimplemented!()
}

/// Convert a HeapTuple to a Datum (inline in C).
pub fn HeapTupleGetDatum(tuple: &HeapTupleData) -> Datum {
    HeapTupleHeaderGetDatum(tuple.t_data)
}

/// Obsolete `TupleGetDatum(_slot, _tuple)` -> just `HeapTupleGetDatum`.
pub fn TupleGetDatum(tuple: &HeapTupleData) -> Datum {
    HeapTupleGetDatum(tuple)
}

bitflags! {
    /// Flag bits for `InitMaterializedSRF()`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MatSrf: u32 {
        /// use expectedDesc as tupdesc.
        const USE_EXPECTED_DESC = 0x01;
        /// "Bless" a tuple descriptor with BlessTupleDesc().
        const BLESS = 0x02;
    }
}

pub fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: MatSrf) {
    unimplemented!()
}

pub fn init_MultiFuncCall(_fcinfo: FunctionCallInfo) -> Box<FuncCallContext> {
    unimplemented!()
}
pub fn per_MultiFuncCall(_fcinfo: FunctionCallInfo) -> Box<FuncCallContext> {
    unimplemented!()
}
pub fn end_MultiFuncCall(_fcinfo: FunctionCallInfo, _funcctx: Box<FuncCallContext>) {
    unimplemented!()
}

/// SRF_IS_FIRSTCALL(): true when `fcinfo->flinfo->extra == NULL`.
pub fn SRF_IS_FIRSTCALL(fcinfo: &crate::fmgr::FunctionCallInfoBaseData) -> bool {
    fcinfo.flinfo.as_ref().map_or(true, |fi| fi.extra == 0)
}

/// SRF_FIRSTCALL_INIT().
pub fn SRF_FIRSTCALL_INIT(fcinfo: FunctionCallInfo) -> Box<FuncCallContext> {
    init_MultiFuncCall(fcinfo)
}

/// SRF_PERCALL_SETUP().
pub fn SRF_PERCALL_SETUP(fcinfo: FunctionCallInfo) -> Box<FuncCallContext> {
    per_MultiFuncCall(fcinfo)
}

/// SRF_RETURN_NEXT(): increments the counter, marks the rsi multi-result, and
/// hands back the result Datum (the C macro `PG_RETURN_DATUM`s out of the fn).
pub fn SRF_RETURN_NEXT(
    funcctx: &mut FuncCallContext,
    rsi: &mut ReturnSetInfo,
    result: Datum,
) -> Datum {
    funcctx.call_cntr += 1;
    rsi.is_done = Some(ExprDoneCond::ExprMultipleResult);
    result
}

/// SRF_RETURN_NEXT_NULL(): like above but the result is SQL NULL (-> None).
pub fn SRF_RETURN_NEXT_NULL(funcctx: &mut FuncCallContext, rsi: &mut ReturnSetInfo) -> Option<Datum> {
    funcctx.call_cntr += 1;
    rsi.is_done = Some(ExprDoneCond::ExprMultipleResult);
    None
}

/// SRF_RETURN_DONE(): tears down the multi-call state and signals end-of-set.
pub fn SRF_RETURN_DONE(
    fcinfo: FunctionCallInfo,
    funcctx: Box<FuncCallContext>,
    rsi: &mut ReturnSetInfo,
) -> Option<Datum> {
    end_MultiFuncCall(fcinfo, funcctx);
    rsi.is_done = Some(ExprDoneCond::ExprEndResult);
    None
}

/// Extract VARIADIC args (out-params `args`/`types`/`nulls` -> a returned
/// struct). C returns the element count, or -1 for "VARIADIC NULL" -> None.
pub struct VariadicArgs {
    pub args: Vec<Datum>,
    pub types: Vec<Oid>,
    pub nulls: Vec<bool>,
}

pub fn extract_variadic_args(
    _fcinfo: FunctionCallInfo,
    _variadic_start: i32,
    _convert_unknown: bool,
) -> Option<VariadicArgs> {
    unimplemented!()
}
