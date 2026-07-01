//! Translated from PostgreSQL src/include/funcapi.h and
//! src/backend/utils/fmgr/funcapi.c
//!
//! Helpers for functions returning composite types and/or sets, and for VARIADIC
//! inputs. The SRF_* macros become free functions taking the fcinfo explicitly;
//! `get_call_result_type` & siblings fold their out-params into a returned tuple
//! per function-mapping.md 5.
//!
//! ValuePerCall SRF protocol (rules.md s8): PG keeps the cross-call
//! [`FuncCallContext`] in `fcinfo->flinfo->fn_extra` (a palloc'd pointer in a
//! multi-call memory context) and reports set progress through
//! `fcinfo->resultinfo` (a [`ReturnSetInfo`]). This port carries the same two
//! channels: the context is a `Box<FuncCallContext>` leaked into the `usize`
//! `flinfo.extra` and reclaimed by `end_MultiFuncCall`; `is_done` flows through
//! the owned `ReturnSetInfo` on the fcinfo. The multi-call memory context is a
//! unit (this port's `MemoryContext` is tombstoned; cross-call state lives in the
//! boxed context itself).

#![allow(clippy::similar_names, reason = "fcinfo/flinfo mirror PG's identifiers")]

use bitflags::bitflags;

use crate::access::htup::{HeapTuple, HeapTupleData, HeapTupleHeaderData};
use crate::access::tupdesc::{TupleDesc, TupleDescData};
use crate::fmgr::{FmgrInfo, FunctionCallInfo, FunctionCallInfoBaseData};
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

/// Function context for Set Returning Functions (held via `flinfo.extra`).
pub struct FuncCallContext {
    /// number of times we've been called before.
    pub call_cntr: u64,
    /// OPTIONAL maximum number of calls.
    pub max_calls: u64,
    /// OPTIONAL user-provided cross-call state (`void *user_fctx`). Owned box; the
    /// SRF stashes its per-call state (e.g. `generate_series_fctx`) here.
    pub user_fctx: Option<Box<dyn core::any::Any + Send>>,
    /// OPTIONAL attribute input metadata for returning composite types.
    pub attinmeta: Option<Box<AttInMetadata>>,
    /// memory context that lives across multiple calls (tombstoned unit here --
    /// cross-call state lives in the boxed context itself, so no palloc context).
    pub multi_call_memory_ctx: (),
    /// OPTIONAL tuple description for heap_form_tuple-built tuples.
    pub tuple_desc: Option<TupleDesc>,
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

/// PG `get_call_result_type`: resolve the result type of the function being
/// called (from `fcinfo->flinfo->fn_expr`).
pub fn get_call_result_type(fcinfo: &FunctionCallInfoBaseData) -> ResultTypeInfo {
    let expr = fcinfo.flinfo.as_ref().and_then(|fi| fi.expr.as_deref());
    expr.map_or(
        ResultTypeInfo { class: TypeFuncClass::Other, result_type_id: None, result_tuple_desc: None },
        get_expr_result_type,
    )
}

/// PG `get_expr_result_type`: resolve the result type of a function/operator call
/// expression. Handles the FuncExpr case (the SRF/record path); other node kinds
/// fall through to `exprType` + `get_type_func_class`.
pub fn get_expr_result_type(expr: &Node) -> ResultTypeInfo {
    if let Node::FuncExpr(f) = expr {
        return internal_get_result_type(f.funcid, Some(expr));
    }
    // Generic expression: no chance to resolve RECORD.
    let typid = crate::backend::nodes::nodeFuncs::exprType(expr);
    let (class, base) = get_type_func_class(typid);
    let tupdesc = if matches!(class, TypeFuncClass::Composite | TypeFuncClass::CompositeDomain) {
        lookup_rowtype_tupdesc_copy(base)
    } else {
        None
    };
    ResultTypeInfo { class, result_type_id: Some(typid), result_tuple_desc: tupdesc }
}

pub fn get_func_result_type(function_id: Oid) -> ResultTypeInfo {
    internal_get_result_type(function_id, None)
}

/// PG `internal_get_result_type`: fetch the function's pg_proc row, check for OUT
/// parameters defining a RECORD result, else classify the scalar/composite
/// rettype. The OUT-param path is served from a builtin record-descriptor
/// registry (this port stages the pg_proc OUT-param arrays; the registry supplies
/// the same tupdesc the C array walk would build).
fn internal_get_result_type(funcid: Oid, _call_expr: Option<&Node>) -> ResultTypeInfo {
    use crate::backend::utils::cache::lsyscache::get_func_rettype;
    use crate::catalog::genbki::RECORDOID;

    let rettype = get_func_rettype(funcid);

    // OUT parameters defining a RECORD result.
    if rettype == RECORDOID
        && let Some(tupdesc) = build_function_result_tupdesc_t(funcid)
    {
        return ResultTypeInfo {
            class: TypeFuncClass::Composite,
            result_type_id: Some(rettype),
            result_tuple_desc: Some(tupdesc),
        };
    }

    let (class, base) = get_type_func_class(rettype);
    let tupdesc = if matches!(class, TypeFuncClass::Composite | TypeFuncClass::CompositeDomain) {
        lookup_rowtype_tupdesc_copy(base)
    } else {
        None
    };
    ResultTypeInfo { class, result_type_id: Some(rettype), result_tuple_desc: tupdesc }
}

/// PG `get_type_func_class`: classify a type OID. This port reaches base/scalar
/// and RECORD; named composite types resolve their base type unchanged (no
/// domains over rowtypes yet). Returns `(class, base_typeid)`.
fn get_type_func_class(typid: Oid) -> (TypeFuncClass, Oid) {
    use crate::catalog::genbki::RECORDOID;
    if typid == RECORDOID {
        return (TypeFuncClass::Record, typid);
    }
    // The composite-type lattice (get_typtype == TYPTYPE_COMPOSITE) is not yet
    // reachable for the step-08 SRFs (generate_series is scalar; pg_input_error_info
    // resolves via OUT params before this point), so every remaining type is scalar.
    (TypeFuncClass::Scalar, typid)
}

/// Look up a named composite type's tupdesc (a copy). Not reachable for the
/// step-08 SRFs; grows with the typcache composite-type path.
fn lookup_rowtype_tupdesc_copy(_typid: Oid) -> Option<TupleDesc> {
    unimplemented!("lookup_rowtype_tupdesc_copy: named composite result types (typcache) not yet translated")
}

pub fn get_expr_result_tupdesc(expr: &Node, no_error: bool) -> Option<TupleDesc> {
    let info = get_expr_result_type(expr);
    if matches!(info.class, TypeFuncClass::Composite | TypeFuncClass::CompositeDomain) {
        return info.result_tuple_desc;
    }
    if !no_error {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("function in FROM has unsupported return type".to_string());
        });
        unreachable!("ereport(ERROR) diverges");
    }
    None
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

/// PG `build_function_result_tupdesc_t`: build the composite result tupdesc from a
/// RECORD-returning function's OUT parameters. This port stages the pg_proc
/// OUT-param arrays (proallargtypes/proargmodes/proargnames stay NULL on the
/// seeded row), so the OUT-column layout is served from a small builtin registry
/// keyed by funcid -- the same 4-column `(message, detail, hint, sql_error_code)`
/// tupdesc the C array walk would produce for `pg_input_error_info`.
pub fn build_function_result_tupdesc_t(funcid: Oid) -> Option<TupleDesc> {
    let cols = builtin_record_out_columns(funcid)?;
    let mut desc = TupleDescData::create_template(i32::try_from(cols.len()).unwrap_or(0));
    for (i, (name, typid)) in cols.iter().enumerate() {
        let attno = (i + 1) as i16;
        desc.init_builtin_entry(attno, name, *typid, -1, 0);
        desc.init_entry_collation(attno, crate::postgres_ext::InvalidOid);
    }
    Some(std::sync::Arc::new(desc))
}

/// The OUT-parameter columns of a RECORD-returning builtin, keyed by funcid.
/// Mirrors the `proargmodes`/`proargnames`/`proallargtypes` of pg_proc.dat for
/// the record-returning functions this port supports.
fn builtin_record_out_columns(funcid: Oid) -> Option<Vec<(&'static str, Oid)>> {
    use crate::catalog::genbki::TEXTOID;
    // pg_input_error_info(text, text) RETURNS record
    //   OUT message text, OUT detail text, OUT hint text, OUT sql_error_code text
    const PG_INPUT_ERROR_INFO_OID: u32 = 6211;
    if funcid.get() == PG_INPUT_ERROR_INFO_OID {
        return Some(vec![
            ("message", TEXTOID),
            ("detail", TEXTOID),
            ("hint", TEXTOID),
            ("sql_error_code", TEXTOID),
        ]);
    }
    None
}

pub fn build_function_result_tupdesc_t_legacy(_proc_tuple: HeapTuple) -> Option<TupleDesc> {
    unimplemented!()
}

pub fn RelationNameGetTupleDesc(_relname: &str) -> TupleDesc {
    unimplemented!()
}
pub fn TypeGetTupleDesc(_typeoid: Oid, _colaliases: &[Node]) -> TupleDesc {
    unimplemented!()
}

// from execTuples.c
pub fn BlessTupleDesc(tupdesc: TupleDesc) -> TupleDesc {
    // Named rowtype/RECORD typmod assignment is not needed for the step-08 SRFs
    // (their descs are consumed directly by the FunctionScan); pass through.
    tupdesc
}
pub fn TupleDescGetAttInMetadata(_tupdesc: TupleDesc) -> Box<AttInMetadata> {
    unimplemented!()
}
pub fn BuildTupleFromCStrings(_attinmeta: &AttInMetadata, _values: &[&str]) -> HeapTuple {
    unimplemented!()
}
pub fn HeapTupleHeaderGetDatum(_tuple: *mut HeapTupleHeaderData) -> Datum {
    unimplemented!("HeapTupleHeaderGetDatum: use heap_tuple_get_datum for record results")
}

/// PG `HeapTupleGetDatum`: turn a composite HeapTuple into a rowtype Datum. This
/// port has no on-disk record Datum yet; the record is carried as a leaked
/// `Box<HeapTupleData>` pointer (freed when the FunctionScan deforms it via
/// [`datum_get_heap_tuple`]). Used by record-returning functions
/// (`pg_input_error_info`).
pub fn HeapTupleGetDatum(tuple: HeapTupleData) -> Datum {
    Datum(Box::into_raw(Box::new(tuple)) as usize)
}

/// Reclaim a record Datum built by [`HeapTupleGetDatum`], returning the owned
/// `HeapTupleData`.
///
/// SAFETY: `datum` must be a record Datum produced by `HeapTupleGetDatum` and not
/// previously reclaimed.
pub unsafe fn datum_get_heap_tuple(datum: Datum) -> Box<HeapTupleData> {
    Box::from_raw(datum.0 as *mut HeapTupleData)
}

/// Obsolete `TupleGetDatum(_slot, _tuple)` -> just `HeapTupleGetDatum`.
pub fn TupleGetDatum(tuple: HeapTupleData) -> Datum {
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

/// PG `init_MultiFuncCall` (`SRF_FIRSTCALL_INIT`): allocate and stash the
/// cross-call [`FuncCallContext`], returning a mutable handle to it. The context
/// is leaked into `flinfo.extra` (a raw pointer as `usize`) and reclaimed by
/// `end_MultiFuncCall`.
pub fn init_MultiFuncCall(fcinfo: &mut FunctionCallInfoBaseData) -> &mut FuncCallContext {
    if fcinfo.resultinfo.is_none() {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("set-valued function called in context that cannot accept a set".to_string());
        });
        unreachable!("ereport(ERROR) diverges");
    }
    let flinfo = fcinfo
        .flinfo
        .as_mut()
        .unwrap_or_else(|| unimplemented!("init_MultiFuncCall: SRF called without an flinfo"));
    crate::assert!(flinfo.extra == 0, "init_MultiFuncCall cannot be called more than once");

    let ctx = Box::new(FuncCallContext {
        call_cntr: 0,
        max_calls: 0,
        user_fctx: None,
        attinmeta: None,
        multi_call_memory_ctx: (),
        tuple_desc: None,
    });
    let raw = Box::into_raw(ctx);
    flinfo.extra = raw as usize;
    // SAFETY: `raw` is a fresh, uniquely-owned box just stored in `extra`; no other
    // alias exists, and the borrow lives only for this call.
    unsafe { &mut *raw }
}

/// PG `per_MultiFuncCall` (`SRF_PERCALL_SETUP`): return the previously-stashed
/// context.
pub fn per_MultiFuncCall(fcinfo: &mut FunctionCallInfoBaseData) -> &mut FuncCallContext {
    let extra = fcinfo
        .flinfo
        .as_ref()
        .map_or_else(|| unimplemented!("per_MultiFuncCall: SRF called without an flinfo"), |fi| fi.extra);
    crate::assert!(extra != 0, "per_MultiFuncCall: no multi-call context");
    // SAFETY: `extra` is the pointer `init_MultiFuncCall` stored; it is owned by the
    // flinfo and outlives this call.
    unsafe { &mut *(extra as *mut FuncCallContext) }
}

/// PG `end_MultiFuncCall`: reclaim the cross-call context and clear `extra`.
pub fn end_MultiFuncCall(fcinfo: &mut FunctionCallInfoBaseData) {
    if let Some(fi) = fcinfo.flinfo.as_mut()
        && fi.extra != 0
    {
        let raw = fi.extra as *mut FuncCallContext;
        fi.extra = 0;
        // SAFETY: `raw` was produced by `Box::into_raw` in `init_MultiFuncCall` and
        // has not been freed (extra was non-zero); reclaim it exactly once.
        drop(unsafe { Box::from_raw(raw) });
    }
}

/// SRF_IS_FIRSTCALL(): true when `fcinfo->flinfo->fn_extra == NULL`.
pub fn SRF_IS_FIRSTCALL(fcinfo: &crate::fmgr::FunctionCallInfoBaseData) -> bool {
    fcinfo.flinfo.as_ref().is_none_or(|fi| fi.extra == 0)
}

/// SRF_FIRSTCALL_INIT().
pub fn SRF_FIRSTCALL_INIT(fcinfo: &mut FunctionCallInfoBaseData) -> &mut FuncCallContext {
    init_MultiFuncCall(fcinfo)
}

/// SRF_PERCALL_SETUP().
pub fn SRF_PERCALL_SETUP(fcinfo: &mut FunctionCallInfoBaseData) -> &mut FuncCallContext {
    per_MultiFuncCall(fcinfo)
}

/// Mark the fcinfo's ReturnSetInfo `is_done` (the SRF_RETURN_* macros set
/// `rsi->isDone` via `fcinfo->resultinfo`).
fn set_is_done(fcinfo: &mut FunctionCallInfoBaseData, done: ExprDoneCond) {
    if let Some(rsi) = fcinfo.resultinfo.as_mut() {
        rsi.is_done = Some(done);
    }
}

/// SRF_RETURN_NEXT(): increment the counter, mark `isDone = ExprMultipleResult`,
/// and hand back the result Datum.
pub fn SRF_RETURN_NEXT(fcinfo: &mut FunctionCallInfoBaseData, result: Datum) -> Datum {
    per_MultiFuncCall(fcinfo).call_cntr += 1;
    set_is_done(fcinfo, ExprDoneCond::ExprMultipleResult);
    result
}

/// SRF_RETURN_NEXT_NULL(): like above but the result is SQL NULL.
pub fn SRF_RETURN_NEXT_NULL(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    per_MultiFuncCall(fcinfo).call_cntr += 1;
    set_is_done(fcinfo, ExprDoneCond::ExprMultipleResult);
    fcinfo.isnull = true;
    Datum(0)
}

/// SRF_RETURN_DONE(): tear down the multi-call state and signal end-of-set.
pub fn SRF_RETURN_DONE(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    end_MultiFuncCall(fcinfo);
    set_is_done(fcinfo, ExprDoneCond::ExprEndResult);
    fcinfo.isnull = true;
    Datum(0)
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
