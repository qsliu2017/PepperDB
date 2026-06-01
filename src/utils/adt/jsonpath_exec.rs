//! jsonpath_exec.rs
//!   Routines for SQL/JSON path execution.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/jsonpath_exec.c
//!
//! Copyright (c) 2019-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/jsonpath_exec.c

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unreachable_code)]
#![allow(unreachable_patterns)]
#![allow(clippy::all)]

use crate::prelude::*;
use crate::{
    PG_GETARG_BOOL, PG_GETARG_DATUM, PG_GETARG_INT32,
    PG_NARGS, PG_RETURN_BOOL, PG_RETURN_NULL,
};
use core::ffi::{c_char, c_int, c_void};
use core::ptr;

use crate::postgres_ext::Oid;
use crate::c::{int32, int64, uint32, Size};
use crate::postgres::{
    Datum, PointerGetDatum, DatumGetPointer, DatumGetInt32, DatumGetBool,
    Int32GetDatum, Int64GetDatum, Float8GetDatum,
    DatumGetCString, CStringGetDatum,
};
use crate::varatt::{VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::postgres_ext::InvalidOid;

/* type aliases matching postgres/c */
use crate::utils::adt::numeric::{
    Numeric,
    numeric_abs, numeric_uminus, numeric_floor, numeric_ceil,
    numeric_add_opt_error, numeric_sub_opt_error,
    numeric_mul_opt_error, numeric_div_opt_error, numeric_mod_opt_error,
    numeric_is_nan, numeric_is_inf,
    numeric_int4_opt_error, numeric_int8_opt_error,
    int64_to_numeric, numeric_trunc,
    numerictypmodin,
};

use crate::utils::adt::jsonb_util::{
    Jsonb, JsonbValue, JsonbContainer, JsonbIterator, JsonbParseState,
    jbvType, JsonbIteratorToken, JsonbPair,
    JsonbIteratorInit, JsonbIteratorNext, JsonbValueToJsonb, pushJsonbValue,
    getKeyJsonValueFromContainer, getIthJsonbValueFromContainer, IsAJsonbScalar,
    JsonbToJsonbValue, JsonContainerSize, JsonContainerIsArray,
    JsonContainerIsObject, JsonContainerIsScalar,
    findJsonbValueFromContainer,
    JB_FSCALAR, JB_FOBJECT, JB_FARRAY,
};
// JsonbExtractScalar is not yet ported; stub it here
unsafe fn JsonbExtractScalar(jbc: *mut JsonbContainer, res: *mut JsonbValue) -> bool {
    // TODO(pg-port): real implementation in jsonb_util
    let _ = (jbc, res);
    false
}
pub use crate::utils::adt::jsonb_util::jbvType::*;
pub use crate::utils::adt::jsonb_util::JsonbIteratorToken::*;

use crate::utils::adt::jsonb_gin::{
    JsonPath, JsonPathItem, JsonPathItemType, JsonPathItemContent,
    JsonPathItemArgs, JsonPathItemArrayElems, JsonPathItemArray,
    JsonPathItemAnyBounds, JsonPathItemValue, JsonPathItemLikeRegex,
    JSONPATH_LAX,
};
pub use crate::utils::adt::jsonb_gin::JsonPathItemType::*;

use crate::utils::adt::json::JsonEncodeDateTime;
use crate::utils::adt::varlena::{
    cstring_to_text, cstring_to_text_with_len, text_to_cstring,
};
use crate::utils::adt::date::{
    DateADT, Timestamp, date_cmp_timestamp_internal, date_cmp_timestamptz_internal,
    anytime_typmod_check, AdjustTimeForTypmod, j2date, DetermineTimeZoneOffset,
    TimeADT, TimeTzADT,
};
// TimestampTz is same type as Timestamp (both = int64)
type TimestampTz = Timestamp;
use crate::utils::adt::timestamp::{
    anytimestamp_typmod_check, AdjustTimestampForTypmod,
    timestamp_cmp_timestamptz_internal,
};
use crate::utils::adt::formatting::parse_datetime;
use crate::utils::adt::float::float8in_internal;

use crate::{DirectFunctionCall1, DirectFunctionCall2};
use crate::utils::fmgr::{
    FunctionCallInfo, FmgrInfo, PGFunction,
    DirectInputFunctionCallSafe,
};
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::mmgr::mcxt::{
    MemoryContextSwitchTo, MemoryContextResetOnly,
    CurrentMemoryContext, TopMemoryContext,
    palloc, palloc0, pfree,
};
use crate::utils::mmgr::aset::{AllocSetContextCreate, ALLOCSET_DEFAULT_SIZES};
use crate::lib::stringinfo::StringInfo;

use crate::nodes::nodes::{NodeTag, Node};
use crate::{list_make1, list_make2, forboth};
use crate::nodes::pg_list::{List, ListCell, NIL, list_length,
    lappend, linitial, list_head, lfirst, lnext, list_second_cell,
    list_delete_first, list_nth,
};
use crate::nodes::primnodes::{
    JsonWrapper, JsonExpr, Const,
    JsonTablePlan, JsonTablePathScan, JsonTableSiblingJoin,
    TableFunc,
};
pub use crate::nodes::primnodes::JsonWrapper::*;

use crate::nodes::execnodes::{
    TableFuncScanState, TableFuncRoutine, ExprState, ExprContext,
    ScanState,
};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::executor::execExpr::JsonPathVariable;

use crate::regex::regex::RE_compile_and_execute;

use crate::miscadmin::{check_stack_depth, CHECK_FOR_INTERRUPTS};
use crate::mb::mbutils::{pg_server_to_any, GetDatabaseEncoding};
use crate::utils::adt::format_type::format_type_be;
use crate::utils::builtins::{parse_bool, pg_ltoa};
use crate::utils::mmgr::mcxt::{pstrdup, pnstrdup};

/* OID constants needed below */
use crate::catalog::pg_type_d::{
    BOOLOID, NUMERICOID, INT2OID, INT4OID, INT8OID, FLOAT4OID, FLOAT8OID,
    TEXTOID, VARCHAROID, DATEOID, TIMEOID, TIMETZOID, TIMESTAMPOID,
    TIMESTAMPTZOID, JSONOID, JSONBOID, CSTRINGOID,
};

/* per-file local shims ---------------------------------------------------- */

macro_rules! PG_GETARG_JSONB_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut Jsonb
    };
}

macro_rules! PG_GETARG_JSONPATH_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut JsonPath
    };
}

macro_rules! PG_GETARG_JSONB_P_COPY {
    ($fcinfo:expr, $n:expr) => {
        /* TODO(pg-port): real copy via datumCopy; using pointer for now */
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut Jsonb
    };
}

macro_rules! PG_GETARG_JSONPATH_P_COPY {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut JsonPath
    };
}

macro_rules! PG_RETURN_JSONB_P {
    ($x:expr) => {
        return crate::postgres::PointerGetDatum($x as *const c_void)
    };
}

/* PG_FREE_IF_COPY: no-op stub; real version detoasts */
macro_rules! PG_FREE_IF_COPY {
    ($ptr:expr, $n:expr) => { let _ = $ptr; };
}

unsafe fn DatumGetJsonbP(d: Datum) -> *mut Jsonb {
    /* TODO(pg-port): real DatumGetJsonbP detoasts */
    DatumGetPointer(d) as *mut Jsonb
}

/* Datum accessor shims for types not yet in crate::postgres */
#[inline] unsafe fn DatumGetNumeric(d: Datum) -> Numeric {
    use crate::postgres::DatumGetPointer;
    DatumGetPointer(d) as Numeric
}
#[inline] fn NumericGetDatum(n: Numeric) -> Datum {
    use crate::postgres::PointerGetDatum;
    PointerGetDatum(n as *const c_void)
}
#[inline] fn DatumGetDateADT(d: Datum) -> DateADT { d as DateADT }
#[inline] unsafe fn DatumGetTimestamp(d: Datum) -> Timestamp {
    use crate::postgres::DatumGetInt64;
    DatumGetInt64(d) as Timestamp
}
#[inline] unsafe fn DatumGetTimestampTz(d: Datum) -> TimestampTz {
    use crate::postgres::DatumGetInt64;
    DatumGetInt64(d) as TimestampTz
}
#[inline] fn DatumGetTimeADT(d: Datum) -> TimeADT { d as TimeADT }
#[inline] fn DatumGetTimeTzADTP(d: Datum) -> *mut TimeTzADT {
    use crate::postgres::DatumGetPointer;
    DatumGetPointer(d) as *mut TimeTzADT
}
#[inline] fn TimeADTGetDatum(t: TimeADT) -> Datum { t as Datum }
#[inline] fn TimestampGetDatum(t: Timestamp) -> Datum { t as Datum }
#[inline] fn TimestampTzGetDatum(t: TimestampTz) -> Datum { t as Datum }
#[inline] fn TimeTzADTPGetDatum(t: *mut TimeTzADT) -> Datum {
    use crate::postgres::PointerGetDatum;
    PointerGetDatum(t as *const c_void)
}
#[allow(dead_code)]
const PG_USED_FOR_ASSERTS_ONLY: () = ();

unsafe fn JsonbPGetDatum(jb: *mut Jsonb) -> Datum {
    PointerGetDatum(jb as *const c_void)
}

unsafe fn JsonbPGetDatumConst(jb: *const Jsonb) -> Datum {
    PointerGetDatum(jb as *const c_void)
}

unsafe fn DatumGetJsonPathP(d: Datum) -> *mut JsonPath {
    /* TODO(pg-port): real detoast not yet ported */
    DatumGetPointer(d) as *mut JsonPath
}

/* json.h helpers not yet in json.rs */
unsafe fn JsonbTypeName(jb: *mut JsonbValue) -> *const c_char {
    /* TODO(pg-port): JsonbTypeName from utils/jsonb.h */
    b"unknown\0".as_ptr() as *const c_char
}

/* jspHasNext / jspGetBool / jspGetNumeric / jspGetArraySubscript / etc. */
extern "C" {
    fn jspInit(v: *mut JsonPathItem, js: *mut JsonPath);
    fn jspGetNext(v: *mut JsonPathItem, a: *mut JsonPathItem) -> bool;
    fn jspHasNext(v: *const JsonPathItem) -> bool;
    fn jspGetArg(v: *mut JsonPathItem, a: *mut JsonPathItem);
    fn jspGetLeftArg(v: *mut JsonPathItem, a: *mut JsonPathItem);
    fn jspGetRightArg(v: *mut JsonPathItem, a: *mut JsonPathItem);
    fn jspGetString(v: *mut JsonPathItem, len: *mut int32) -> *mut c_char;
    fn jspGetBool(v: *mut JsonPathItem) -> bool;
    fn jspGetNumeric(v: *mut JsonPathItem) -> Numeric;
    fn jspGetArraySubscript(v: *mut JsonPathItem, from: *mut JsonPathItem,
                            to: *mut JsonPathItem, i: c_int) -> bool;
    fn jspInitByBuffer(v: *mut JsonPathItem, base: *mut c_char, pos: int32);
    fn jspOperationName(t: JsonPathItemType) -> *const c_char;
    fn jspConvertRegexFlags(cflags: uint32, result: *mut c_int,
                            escontext: *mut c_void) -> bool;
}

/* numeric function stubs used as PGFunction callbacks */
unsafe fn numeric_out(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): numeric_out from numeric.rs */
    unimplemented!("numeric_out")
}
unsafe fn int4in(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): int4in */
    unimplemented!("int4in")
}
unsafe fn int8in(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): int8in */
    unimplemented!("int8in")
}
unsafe fn numeric_in(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): numeric_in */
    unimplemented!("numeric_in")
}
unsafe fn float8_numeric(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): float8_numeric */
    unimplemented!("float8_numeric")
}
unsafe fn int4_numeric(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): int4_numeric */
    unimplemented!("int4_numeric")
}
unsafe fn int8_numeric(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): int8_numeric */
    unimplemented!("int8_numeric")
}
unsafe fn int2_numeric(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): int2_numeric */
    unimplemented!("int2_numeric")
}
unsafe fn float4_numeric(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): float4_numeric */
    unimplemented!("float4_numeric")
}
unsafe fn jsonb_in(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): jsonb_in */
    unimplemented!("jsonb_in")
}
unsafe fn numeric_cmp(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): numeric_cmp */
    unimplemented!("numeric_cmp")
}
/* datetime conversion helpers */
unsafe fn timestamp_date(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timestamp_date */
    unimplemented!("timestamp_date")
}
unsafe fn timestamptz_date(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timestamptz_date */
    unimplemented!("timestamptz_date")
}
unsafe fn timetz_time(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timetz_time */
    unimplemented!("timetz_time")
}
unsafe fn timestamp_time(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timestamp_time */
    unimplemented!("timestamp_time")
}
unsafe fn timestamptz_time(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timestamptz_time */
    unimplemented!("timestamptz_time")
}
unsafe fn time_timetz(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): time_timetz */
    unimplemented!("time_timetz")
}
unsafe fn timestamptz_timetz(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timestamptz_timetz */
    unimplemented!("timestamptz_timetz")
}
unsafe fn date_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): date_timestamp */
    unimplemented!("date_timestamp")
}
unsafe fn timestamptz_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timestamptz_timestamp */
    unimplemented!("timestamptz_timestamp")
}
unsafe fn date_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): date_timestamptz */
    unimplemented!("date_timestamptz")
}
unsafe fn timestamp_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timestamp_timestamptz */
    unimplemented!("timestamp_timestamptz")
}
unsafe fn date_cmp(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): date_cmp */
    unimplemented!("date_cmp")
}
unsafe fn time_cmp(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): time_cmp */
    unimplemented!("time_cmp")
}
unsafe fn timetz_cmp(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timetz_cmp */
    unimplemented!("timetz_cmp")
}
unsafe fn timestamp_cmp(fcinfo: FunctionCallInfo) -> Datum {
    /* TODO(pg-port): timestamp_cmp */
    unimplemented!("timestamp_cmp")
}
/* session_timezone: global from datetime.c */
extern "C" {
    static session_timezone: *mut c_void; /* pg_tz* */
}
use crate::pgtime::pg_tm;
type fsec_t = int32;

/* construct_array_builtin: stub */
unsafe fn construct_array_builtin(
    elems: *mut Datum, nelems: c_int, elmtype: Oid,
) -> *mut ArrayType {
    /* TODO(pg-port): real ArrayType not yet ported */
    unimplemented!("construct_array_builtin")
}
#[repr(C)] struct ArrayType { _opaque: [u8; 0] }

/* ExecEvalExpr stub */
unsafe fn ExecEvalExpr(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isnull: *mut bool,
) -> Datum {
    /* TODO(pg-port): ExecEvalExpr (execExprInterp.rs) */
    unimplemented!("ExecEvalExpr")
}

/* exprType / exprTypmod stubs */
unsafe fn exprType(node: *const c_void) -> Oid {
    /* TODO(pg-port): exprType (nodes/nodeFuncs.c) */
    unimplemented!("exprType")
}
unsafe fn exprTypmod(node: *const c_void) -> int32 {
    /* TODO(pg-port): exprTypmod (nodes/nodeFuncs.c) */
    unimplemented!("exprTypmod")
}

/* SRF helpers */
#[repr(C)]
struct FuncCallContext {
    call_cntr: uint32,
    max_calls: uint32,
    user_fctx: *mut c_void,
    multi_call_memory_ctx: MemoryContext,
}
unsafe fn SRF_IS_FIRSTCALL(fcinfo: FunctionCallInfo) -> bool {
    /* TODO(pg-port): SRF_IS_FIRSTCALL (funcapi.h) */
    unimplemented!("SRF_IS_FIRSTCALL")
}
unsafe fn SRF_FIRSTCALL_INIT(fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    /* TODO(pg-port): SRF_FIRSTCALL_INIT (funcapi.h) */
    unimplemented!("SRF_FIRSTCALL_INIT")
}
unsafe fn SRF_PERCALL_SETUP(fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    /* TODO(pg-port): SRF_PERCALL_SETUP (funcapi.h) */
    unimplemented!("SRF_PERCALL_SETUP")
}
unsafe fn SRF_RETURN_NEXT(funcctx: *mut FuncCallContext, result: Datum) -> Datum {
    /* TODO(pg-port): SRF_RETURN_NEXT (funcapi.h) */
    unimplemented!("SRF_RETURN_NEXT")
}
unsafe fn SRF_RETURN_DONE(funcctx: *mut FuncCallContext) -> Datum {
    /* TODO(pg-port): SRF_RETURN_DONE (funcapi.h) */
    unimplemented!("SRF_RETURN_DONE")
}

/* MAXDATELEN from utils/datetime.h */
const MAXDATELEN: usize = 128;

/* PG_UINT32_MAX */
const PG_UINT32_MAX: uint32 = 0xFFFFFFFF;

/* DEFAULT_COLLATION_OID from pg_collation.h */
const DEFAULT_COLLATION_OID: Oid = 100;

/* INT64CONST macro */
macro_rules! INT64CONST { ($x:expr) => { $x as i64 }; }

/* text type alias */
use crate::c::text;

/* castNode macro used in JsonTableInitOpaque */
macro_rules! castNode {
    ($t:ty, $tag:expr, $e:expr) => {
        $e as *mut $t
    };
}

macro_rules! IsA {
    ($nodeptr:expr, $tag:ident) => {
        crate::IsA!($nodeptr, $tag)
    };
}

/* RETURN_ERROR macro (context-dependent: cxt must be in scope) */
macro_rules! RETURN_ERROR {
    ($cxt:expr, $throw_error:expr) => {{
        if jspThrowErrors($cxt) {
            $throw_error;
        } else {
            return jperError;
        }
    }};
}

/* -------------------------------------------------------------------------
 * Types
 * -------------------------------------------------------------------------
 */

/*
 * Represents "base object" and its "id" for .keyvalue() evaluation.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct JsonBaseObjectInfo {
    jbc: *mut JsonbContainer,
    id:  c_int,
}

/* Callbacks for executeJsonPath() */
type JsonPathGetVarCallback = unsafe fn(
    vars: *mut c_void,
    varName: *mut c_char,
    varNameLen: c_int,
    baseObject: *mut JsonbValue,
    baseObjectId: *mut c_int,
) -> *mut JsonbValue;

type JsonPathCountVarsCallback = unsafe fn(vars: *mut c_void) -> c_int;

/*
 * Context of jsonpath execution.
 */
#[repr(C)]
struct JsonPathExecContext {
    vars:    *mut c_void,               /* variables to substitute into jsonpath */
    getVar:  Option<JsonPathGetVarCallback>, /* callback to extract a given variable from 'vars' */
    root:    *mut JsonbValue,           /* for $ evaluation */
    current: *mut JsonbValue,           /* for @ evaluation */
    baseObject: JsonBaseObjectInfo,     /* "base object" for .keyvalue() evaluation */
    lastGeneratedObjectId: c_int,       /* "id" counter for .keyvalue() evaluation */
    innermostArraySize: c_int,          /* for LAST array index evaluation */
    laxMode: bool,                      /* true for "lax" mode, false for "strict" mode */
    ignoreStructuralErrors: bool,       /* with "true" structural errors such as absence
                                         * of required json item or unexpected json item
                                         * type are ignored */
    throwErrors: bool,                  /* with "false" all suppressible errors are suppressed */
    useTz: bool,
}

/* Context for LIKE_REGEX execution. */
#[repr(C)]
struct JsonLikeRegexContext {
    regex:  *mut text,
    cflags: c_int,
}

/* Result of jsonpath predicate evaluation */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum JsonPathBool {
    jpbFalse   = 0,
    jpbTrue    = 1,
    jpbUnknown = 2,
}
use JsonPathBool::*;

/* Result of jsonpath expression evaluation */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum JsonPathExecResult {
    jperOk       = 0,
    jperNotFound = 1,
    jperError    = 2,
}
use JsonPathExecResult::*;

#[inline]
fn jperIsError(jper: JsonPathExecResult) -> bool {
    jper == jperError
}

/*
 * List of jsonb values with shortcut for single-value list.
 */
#[repr(C)]
struct JsonValueList {
    singleton: *mut JsonbValue,
    list:      *mut List,
}

impl JsonValueList {
    const fn new() -> Self {
        JsonValueList { singleton: ptr::null_mut(), list: ptr::null_mut() }
    }
}

#[repr(C)]
struct JsonValueListIterator {
    value: *mut JsonbValue,
    list:  *mut List,
    next:  *mut ListCell,
}

/* Structures for JSON_TABLE execution */

/*
 * Struct holding the result of jsonpath evaluation, to be used as source row
 * for JsonTableGetValue() which in turn computes the values of individual
 * JSON_TABLE columns.
 */
#[repr(C)]
struct JsonTablePlanRowSource {
    value:  Datum,
    isnull: bool,
}

/*
 * State of evaluation of row pattern derived by applying jsonpath given in
 * a JsonTablePlan to an input document given in the parent TableFunc.
 */
#[repr(C)]
struct JsonTablePlanState {
    /* Original plan */
    plan: *mut JsonTablePlan,

    /* The following fields are only valid for JsonTablePathScan plans */

    /* jsonpath to evaluate against the input doc to get the row pattern */
    path: *mut JsonPath,

    /*
     * Memory context to use when evaluating the row pattern from the jsonpath
     */
    mcxt: MemoryContext,

    /* PASSING arguments passed to jsonpath executor */
    args: *mut List,

    /* List and iterator of jsonpath result values */
    found: JsonValueList,
    iter:  JsonValueListIterator,

    /* Currently selected row for JsonTableGetValue() to use */
    current: JsonTablePlanRowSource,

    /* Counter for ORDINAL columns */
    ordinal: c_int,

    /* Nested plan, if any */
    nested: *mut JsonTablePlanState,

    /* Left sibling, if any */
    left: *mut JsonTablePlanState,

    /* Right sibling, if any */
    right: *mut JsonTablePlanState,

    /* Parent plan, if this is a nested plan */
    parent: *mut JsonTablePlanState,
}

/* Random number to identify JsonTableExecContext for sanity checking */
const JSON_TABLE_EXEC_CONTEXT_MAGIC: c_int = 418352867;

#[repr(C)]
struct JsonTableExecContext {
    magic: c_int,

    /* State of the plan providing a row evaluated from "root" jsonpath */
    rootplanstate: *mut JsonTablePlanState,

    /*
     * Per-column JsonTablePlanStates for all columns including the nested
     * ones.
     */
    colplanstates: *mut *mut JsonTablePlanState,
}

/* strict/lax flags is decomposed into four [un]wrap/error flags */
#[inline] unsafe fn jspStrictAbsenceOfErrors(cxt: *const JsonPathExecContext) -> bool { !(*cxt).laxMode }
#[inline] unsafe fn jspAutoUnwrap(cxt: *const JsonPathExecContext) -> bool { (*cxt).laxMode }
#[inline] unsafe fn jspAutoWrap(cxt: *const JsonPathExecContext) -> bool { (*cxt).laxMode }
#[inline] unsafe fn jspIgnoreStructuralErrors(cxt: *const JsonPathExecContext) -> bool { (*cxt).ignoreStructuralErrors }
#[inline] unsafe fn jspThrowErrors(cxt: *const JsonPathExecContext) -> bool { (*cxt).throwErrors }

type JsonPathPredicateCallback = unsafe fn(
    jsp: *mut JsonPathItem,
    larg: *mut JsonbValue,
    rarg: *mut JsonbValue,
    param: *mut c_void,
) -> JsonPathBool;

type BinaryArithmFunc = unsafe fn(
    num1: Numeric,
    num2: Numeric,
    error: *mut bool,
) -> Numeric;

/* -------------------------------------------------------------------------
 * JsonbTableRoutine
 * -------------------------------------------------------------------------
 */

pub static JsonbTableRoutine: TableFuncRoutine = TableFuncRoutine {
    InitOpaque:    Some(JsonTableInitOpaque),
    SetDocument:   Some(JsonTableSetDocument),
    SetNamespace:  None,
    SetRowFilter:  None,
    SetColumnFilter: None,
    FetchRow:      Some(JsonTableFetchRow),
    GetValue:      Some(JsonTableGetValue),
    DestroyOpaque: Some(JsonTableDestroyOpaque),
};

/****************** User interface to JsonPath executor ********************/

/*
 * jsonb_path_exists
 *      Returns true if jsonpath returns at least one item for the specified
 *      jsonb value.
 */
unsafe fn jsonb_path_exists_internal(fcinfo: FunctionCallInfo, tz: bool) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let jp: *mut JsonPath = PG_GETARG_JSONPATH_P!(fcinfo, 1);
    let mut res: JsonPathExecResult;
    let mut vars: *mut Jsonb = ptr::null_mut();
    let mut silent: bool = true;

    if PG_NARGS!(fcinfo) == 4 {
        vars = PG_GETARG_JSONB_P!(fcinfo, 2);
        silent = PG_GETARG_BOOL!(fcinfo, 3);
    }

    res = executeJsonPath(jp, vars as *mut c_void,
                          Some(getJsonPathVariableFromJsonb_cb),
                          Some(countVariablesFromJsonb_cb),
                          jb, !silent, ptr::null_mut(), tz);

    PG_FREE_IF_COPY!(jb, 0);
    PG_FREE_IF_COPY!(jp, 1);

    if jperIsError(res) {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_BOOL!(res == jperOk)
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_exists(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_exists_internal(fcinfo, false)
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_exists_tz(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_exists_internal(fcinfo, true)
}

/*
 * jsonb_path_exists_opr
 *      Implementation of operator "jsonb @? jsonpath" (2-argument version of
 *      jsonb_path_exists()).
 */
#[no_mangle]
pub unsafe extern "C" fn jsonb_path_exists_opr(fcinfo: FunctionCallInfo) -> Datum {
    /* just call the other one -- it can handle both cases */
    jsonb_path_exists_internal(fcinfo, false)
}

/*
 * jsonb_path_match
 *      Returns jsonpath predicate result item for the specified jsonb value.
 */
unsafe fn jsonb_path_match_internal(fcinfo: FunctionCallInfo, tz: bool) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let jp: *mut JsonPath = PG_GETARG_JSONPATH_P!(fcinfo, 1);
    let mut found = JsonValueList::new();
    let mut vars: *mut Jsonb = ptr::null_mut();
    let mut silent: bool = true;

    if PG_NARGS!(fcinfo) == 4 {
        vars = PG_GETARG_JSONB_P!(fcinfo, 2);
        silent = PG_GETARG_BOOL!(fcinfo, 3);
    }

    let _ = executeJsonPath(jp, vars as *mut c_void,
                            Some(getJsonPathVariableFromJsonb_cb),
                            Some(countVariablesFromJsonb_cb),
                            jb, !silent, &mut found, tz);

    PG_FREE_IF_COPY!(jb, 0);
    PG_FREE_IF_COPY!(jp, 1);

    if JsonValueListLength(&found) == 1 {
        let jbv: *mut JsonbValue = JsonValueListHead(&mut found);

        if (*jbv).type_ == jbvBool {
            return PG_RETURN_BOOL!((*jbv).val.boolean);
        }

        if (*jbv).type_ == jbvNull {
            PG_RETURN_NULL!(fcinfo);
        }
    }

    if !silent {
        ereport!(ERROR,
            errmsg!("single boolean result is expected")
            /* C also: errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED) */
        );
    }

    PG_RETURN_NULL!(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_match(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_match_internal(fcinfo, false)
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_match_tz(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_match_internal(fcinfo, true)
}

/*
 * jsonb_path_match_opr
 *      Implementation of operator "jsonb @@ jsonpath" (2-argument version of
 *      jsonb_path_match()).
 */
#[no_mangle]
pub unsafe extern "C" fn jsonb_path_match_opr(fcinfo: FunctionCallInfo) -> Datum {
    /* just call the other one -- it can handle both cases */
    jsonb_path_match_internal(fcinfo, false)
}

/*
 * jsonb_path_query
 *      Executes jsonpath for given jsonb document and returns result as
 *      rowset.
 */
unsafe fn jsonb_path_query_internal(fcinfo: FunctionCallInfo, tz: bool) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let mut found: *mut List;
    let mut v: *mut JsonbValue;
    let mut c: *mut ListCell;

    if SRF_IS_FIRSTCALL(fcinfo) {
        let jp: *mut JsonPath;
        let jb: *mut Jsonb;
        let oldcontext: MemoryContext;
        let vars: *mut Jsonb;
        let silent: bool;
        let mut found_inner = JsonValueList::new();

        funcctx = SRF_FIRSTCALL_INIT(fcinfo);
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        jb = PG_GETARG_JSONB_P_COPY!(fcinfo, 0);
        jp = PG_GETARG_JSONPATH_P_COPY!(fcinfo, 1);
        vars = PG_GETARG_JSONB_P_COPY!(fcinfo, 2);
        silent = PG_GETARG_BOOL!(fcinfo, 3);

        let _ = executeJsonPath(jp, vars as *mut c_void,
                                Some(getJsonPathVariableFromJsonb_cb),
                                Some(countVariablesFromJsonb_cb),
                                jb, !silent, &mut found_inner, tz);

        (*funcctx).user_fctx = JsonValueListGetList(&mut found_inner) as *mut c_void;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP(fcinfo);
    found = (*funcctx).user_fctx as *mut List;

    c = list_head(found);

    if c.is_null() {
        return SRF_RETURN_DONE(funcctx);
    }

    v = lfirst(c) as *mut JsonbValue;
    (*funcctx).user_fctx = list_delete_first(found) as *mut c_void;

    SRF_RETURN_NEXT(funcctx, JsonbPGetDatum(JsonbValueToJsonb(v)))
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_query(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_query_internal(fcinfo, false)
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_query_tz(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_query_internal(fcinfo, true)
}

/*
 * jsonb_path_query_array
 *      Executes jsonpath for given jsonb document and returns result as
 *      jsonb array.
 */
unsafe fn jsonb_path_query_array_internal(fcinfo: FunctionCallInfo, tz: bool) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let jp: *mut JsonPath = PG_GETARG_JSONPATH_P!(fcinfo, 1);
    let mut found = JsonValueList::new();
    let vars: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 2);
    let silent: bool = PG_GETARG_BOOL!(fcinfo, 3);

    let _ = executeJsonPath(jp, vars as *mut c_void,
                            Some(getJsonPathVariableFromJsonb_cb),
                            Some(countVariablesFromJsonb_cb),
                            jb, !silent, &mut found, tz);

    PG_RETURN_JSONB_P!(JsonbValueToJsonb(wrapItemsInArray(&found)));
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_query_array(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_query_array_internal(fcinfo, false)
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_query_array_tz(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_query_array_internal(fcinfo, true)
}

/*
 * jsonb_path_query_first
 *      Executes jsonpath for given jsonb document and returns first result
 *      item.  If there are no items, NULL returned.
 */
unsafe fn jsonb_path_query_first_internal(fcinfo: FunctionCallInfo, tz: bool) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let jp: *mut JsonPath = PG_GETARG_JSONPATH_P!(fcinfo, 1);
    let mut found = JsonValueList::new();
    let vars: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 2);
    let silent: bool = PG_GETARG_BOOL!(fcinfo, 3);

    let _ = executeJsonPath(jp, vars as *mut c_void,
                            Some(getJsonPathVariableFromJsonb_cb),
                            Some(countVariablesFromJsonb_cb),
                            jb, !silent, &mut found, tz);

    if JsonValueListLength(&found) >= 1 {
        PG_RETURN_JSONB_P!(JsonbValueToJsonb(JsonValueListHead(&mut found)));
    } else {
        PG_RETURN_NULL!(fcinfo)
    }
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_query_first(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_query_first_internal(fcinfo, false)
}

#[no_mangle]
pub unsafe extern "C" fn jsonb_path_query_first_tz(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_path_query_first_internal(fcinfo, true)
}

/********************Execute functions for JsonPath**************************/

/*
 * Interface to jsonpath executor
 */
unsafe fn executeJsonPath(
    path: *mut JsonPath,
    vars: *mut c_void,
    getVar: Option<JsonPathGetVarCallback>,
    countVars: Option<JsonPathCountVarsCallback>,
    json: *mut Jsonb,
    throwErrors: bool,
    result: *mut JsonValueList,
    useTz: bool,
) -> JsonPathExecResult {
    let mut cxt = JsonPathExecContext {
        vars,
        getVar,
        root: ptr::null_mut(),
        current: ptr::null_mut(),
        baseObject: JsonBaseObjectInfo { jbc: ptr::null_mut(), id: 0 },
        lastGeneratedObjectId: 0,
        innermostArraySize: -1,
        laxMode: false,
        ignoreStructuralErrors: false,
        throwErrors,
        useTz,
    };
    let mut res: JsonPathExecResult;
    let mut jsp = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let mut jbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();

    jspInit(jsp.as_mut_ptr(), path);
    let jsp = jsp.assume_init_mut();
    let jbv_ptr = jbv.as_mut_ptr();

    if !JsonbExtractScalar(&mut (*json).root, jbv_ptr) {
        JsonbInitBinary(jbv_ptr, json);
    }

    cxt.root = jbv_ptr;
    cxt.current = jbv_ptr;
    cxt.baseObject.jbc = ptr::null_mut();
    cxt.baseObject.id = 0;
    /* 1 + number of base objects in vars */
    cxt.lastGeneratedObjectId = 1 + countVars.unwrap()(vars);
    cxt.innermostArraySize = -1;
    cxt.laxMode = ((*path).header & JSONPATH_LAX) != 0;
    cxt.ignoreStructuralErrors = cxt.laxMode;

    if jspStrictAbsenceOfErrors(&cxt) && result.is_null() {
        /*
         * In strict mode we must get a complete list of values to check that
         * there are no errors at all.
         */
        let mut vals = JsonValueList::new();

        res = executeItem(&mut cxt, jsp, jbv_ptr, &mut vals);

        if jperIsError(res) {
            return res;
        }

        return if JsonValueListIsEmpty(&mut vals) { jperNotFound } else { jperOk };
    }

    res = executeItem(&mut cxt, jsp, jbv_ptr, result);

    debug_assert!(!throwErrors || !jperIsError(res));

    res
}

/*
 * Execute jsonpath with automatic unwrapping of current item in lax mode.
 */
unsafe fn executeItem(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    found: *mut JsonValueList,
) -> JsonPathExecResult {
    executeItemOptUnwrapTarget(cxt, jsp, jb, found, jspAutoUnwrap(cxt))
}

/*
 * Main jsonpath executor function: walks on jsonpath structure, finds
 * relevant parts of jsonb and evaluates expressions over them.
 * When 'unwrap' is true current SQL/JSON item is unwrapped if it is an array.
 */
unsafe fn executeItemOptUnwrapTarget(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    mut jb: *mut JsonbValue,
    found: *mut JsonValueList,
    unwrap: bool,
) -> JsonPathExecResult {
    let mut elem = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let mut res: JsonPathExecResult = jperNotFound;
    let mut baseObject: JsonBaseObjectInfo;

    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    match (*jsp).type_ {
        jpiNull | jpiBool | jpiNumeric | jpiString | jpiVariable => {
            let mut vbuf = core::mem::MaybeUninit::<JsonbValue>::zeroed();
            let mut v: *mut JsonbValue;
            let hasNext: bool = jspGetNext(jsp, elem.as_mut_ptr());

            if !hasNext && found.is_null() && (*jsp).type_ != jpiVariable {
                /*
                 * Skip evaluation, but not for variables.  We must
                 * trigger an error for the missing variable.
                 */
                res = jperOk;
                return res;
            }

            v = if hasNext {
                vbuf.as_mut_ptr()
            } else {
                palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue
            };

            baseObject = (*cxt).baseObject;
            getJsonPathItem(cxt, jsp, v);

            res = executeNextItem(cxt, jsp, elem.as_mut_ptr(), v, found, hasNext);
            (*cxt).baseObject = baseObject;
        }

        /* all boolean item types: */
        jpiAnd | jpiOr | jpiNot | jpiIsUnknown
        | jpiEqual | jpiNotEqual | jpiLess | jpiGreater
        | jpiLessOrEqual | jpiGreaterOrEqual
        | jpiExists | jpiStartsWith | jpiLikeRegex => {
            let st: JsonPathBool = executeBoolItem(cxt, jsp, jb, true);
            res = appendBoolResult(cxt, jsp, found, st);
        }

        jpiAdd => {
            return executeBinaryArithmExpr(cxt, jsp, jb,
                                           Some(numeric_add_opt_error_cb), found);
        }

        jpiSub => {
            return executeBinaryArithmExpr(cxt, jsp, jb,
                                           Some(numeric_sub_opt_error_cb), found);
        }

        jpiMul => {
            return executeBinaryArithmExpr(cxt, jsp, jb,
                                           Some(numeric_mul_opt_error_cb), found);
        }

        jpiDiv => {
            return executeBinaryArithmExpr(cxt, jsp, jb,
                                           Some(numeric_div_opt_error_cb), found);
        }

        jpiMod => {
            return executeBinaryArithmExpr(cxt, jsp, jb,
                                           Some(numeric_mod_opt_error_cb), found);
        }

        jpiPlus => {
            return executeUnaryArithmExpr(cxt, jsp, jb, None, found);
        }

        jpiMinus => {
            return executeUnaryArithmExpr(cxt, jsp, jb, Some(numeric_uminus), found);
        }

        jpiAnyArray => {
            if JsonbType(jb) == jbvArray as c_int {
                let hasNext: bool = jspGetNext(jsp, elem.as_mut_ptr());
                res = executeItemUnwrapTargetArray(
                    cxt,
                    if hasNext { elem.as_mut_ptr() } else { ptr::null_mut() },
                    jb, found, jspAutoUnwrap(cxt));
            } else if jspAutoWrap(cxt) {
                res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, true);
            } else if !jspIgnoreStructuralErrors(cxt) {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath wildcard array accessor can only be applied to an array")
                    /* C also: errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND) */
                ));
            }
        }

        jpiAnyKey => {
            if JsonbType(jb) == jbvObject as c_int {
                let hasNext: bool = jspGetNext(jsp, elem.as_mut_ptr());

                if (*jb).type_ != jbvBinary {
                    elog!(ERROR, "invalid jsonb object type: {}", (*jb).type_ as i32);
                }

                return executeAnyItem(
                    cxt,
                    if hasNext { elem.as_mut_ptr() } else { ptr::null_mut() },
                    (*jb).val.binary.data, found, 1, 1, 1,
                    false, jspAutoUnwrap(cxt));
            } else if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            } else if !jspIgnoreStructuralErrors(cxt) {
                debug_assert!(!found.is_null());
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath wildcard member accessor can only be applied to an object")
                    /* C also: errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND) */
                ));
            }
        }

        jpiIndexArray => {
            if JsonbType(jb) == jbvArray as c_int || jspAutoWrap(cxt) {
                let innermostArraySize: c_int = (*cxt).innermostArraySize;
                let mut i: c_int;
                let size: c_int = JsonbArraySize(jb);
                let singleton: bool = size < 0;
                let hasNext: bool = jspGetNext(jsp, elem.as_mut_ptr());
                let size = if singleton { 1 } else { size };

                (*cxt).innermostArraySize = size; /* for LAST evaluation */

                i = 0;
                while i < (*jsp).content.array.nelems {
                    let mut from = core::mem::MaybeUninit::<JsonPathItem>::uninit();
                    let mut to = core::mem::MaybeUninit::<JsonPathItem>::uninit();
                    let mut index: int32 = 0;
                    let mut index_from: int32 = 0;
                    let mut index_to: int32 = 0;
                    let range: bool = jspGetArraySubscript(jsp, from.as_mut_ptr(),
                                                           to.as_mut_ptr(), i);

                    res = getArrayIndex(cxt, from.as_mut_ptr(), jb, &mut index_from);

                    if jperIsError(res) { break; }

                    if range {
                        res = getArrayIndex(cxt, to.as_mut_ptr(), jb, &mut index_to);
                        if jperIsError(res) { break; }
                    } else {
                        index_to = index_from;
                    }

                    if !jspIgnoreStructuralErrors(cxt)
                        && (index_from < 0
                            || index_from > index_to
                            || index_to >= size)
                    {
                        RETURN_ERROR!(cxt, ereport!(ERROR,
                            errmsg!("jsonpath array subscript is out of bounds")
                            /* C also: errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT) */
                        ));
                    }

                    if index_from < 0 { index_from = 0; }
                    if index_to >= size { index_to = size - 1; }

                    res = jperNotFound;

                    let mut index = index_from;
                    while index <= index_to {
                        let v: *mut JsonbValue;
                        let copy: bool;

                        if singleton {
                            v = jb;
                            copy = true;
                        } else {
                            v = getIthJsonbValueFromContainer((*jb).val.binary.data,
                                                              index as uint32);
                            if v.is_null() {
                                index += 1;
                                continue;
                            }
                            copy = false;
                        }

                        if !hasNext && found.is_null() {
                            return jperOk;
                        }

                        res = executeNextItem(cxt, jsp, elem.as_mut_ptr(), v, found, copy);

                        if jperIsError(res) { break; }

                        if res == jperOk && found.is_null() { break; }

                        index += 1;
                    }

                    if jperIsError(res) { break; }
                    if res == jperOk && found.is_null() { break; }

                    i += 1;
                }

                (*cxt).innermostArraySize = innermostArraySize;
            } else if !jspIgnoreStructuralErrors(cxt) {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath array accessor can only be applied to an array")
                    /* C also: errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND) */
                ));
            }
        }

        jpiAny => {
            let hasNext: bool = jspGetNext(jsp, elem.as_mut_ptr());

            /* first try without any intermediate steps */
            if (*jsp).content.anybounds.first == 0 {
                let savedIgnoreStructuralErrors: bool;

                savedIgnoreStructuralErrors = (*cxt).ignoreStructuralErrors;
                (*cxt).ignoreStructuralErrors = true;
                res = executeNextItem(cxt, jsp, elem.as_mut_ptr(),
                                      jb, found, true);
                (*cxt).ignoreStructuralErrors = savedIgnoreStructuralErrors;

                if res == jperOk && found.is_null() {
                    return res;
                }
            }

            if (*jb).type_ == jbvBinary {
                res = executeAnyItem(
                    cxt,
                    if hasNext { elem.as_mut_ptr() } else { ptr::null_mut() },
                    (*jb).val.binary.data, found,
                    1,
                    (*jsp).content.anybounds.first,
                    (*jsp).content.anybounds.last,
                    true, jspAutoUnwrap(cxt));
            }
        }

        jpiKey => {
            if JsonbType(jb) == jbvObject as c_int {
                let mut v: *mut JsonbValue;
                let mut key = core::mem::MaybeUninit::<JsonbValue>::zeroed();
                let key = key.assume_init_mut();

                (*key).type_ = jbvString;
                (*key).val.string.val = jspGetString(jsp, &mut (*key).val.string.len);

                v = findJsonbValueFromContainer((*jb).val.binary.data,
                                               JB_FOBJECT, key);

                if !v.is_null() {
                    res = executeNextItem(cxt, jsp, ptr::null_mut(),
                                          v, found, false);

                    /* free value if it was not added to found list */
                    if jspHasNext(jsp) || found.is_null() {
                        pfree(v as *mut c_void);
                    }
                } else if !jspIgnoreStructuralErrors(cxt) {
                    debug_assert!(!found.is_null());

                    if !jspThrowErrors(cxt) {
                        return jperError;
                    }

                    ereport!(ERROR,
                        errmsg!("JSON object does not contain key \"{}\"",
                            std::ffi::CStr::from_ptr(
                                pnstrdup((*key).val.string.val,
                                         (*key).val.string.len as usize)
                            ).to_string_lossy()
                        )
                        /* C also: errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND) */
                    );
                }
            } else if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            } else if !jspIgnoreStructuralErrors(cxt) {
                debug_assert!(!found.is_null());
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath member accessor can only be applied to an object")
                    /* C also: errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND) */
                ));
            }
        }

        jpiCurrent => {
            res = executeNextItem(cxt, jsp, ptr::null_mut(), (*cxt).current,
                                  found, true);
        }

        jpiRoot => {
            jb = (*cxt).root;
            baseObject = setBaseObject(cxt, jb, 0);
            res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, true);
            (*cxt).baseObject = baseObject;
        }

        jpiFilter => {
            let mut st: JsonPathBool;

            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }

            jspGetArg(jsp, elem.as_mut_ptr());
            st = executeNestedBoolItem(cxt, elem.as_mut_ptr(), jb);
            if st != jpbTrue {
                res = jperNotFound;
            } else {
                res = executeNextItem(cxt, jsp, ptr::null_mut(),
                                      jb, found, true);
            }
        }

        jpiType => {
            let jbv: *mut JsonbValue =
                palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;

            (*jbv).type_ = jbvString;
            (*jbv).val.string.val = pstrdup(JsonbTypeName(jb) as *mut c_char);
            (*jbv).val.string.len =
                libc_strlen((*jbv).val.string.val as *const u8) as int32;

            res = executeNextItem(cxt, jsp, ptr::null_mut(), jbv, found, false);
        }

        jpiSize => {
            let mut size: c_int = JsonbArraySize(jb);

            if size < 0 {
                if !jspAutoWrap(cxt) {
                    if !jspIgnoreStructuralErrors(cxt) {
                        RETURN_ERROR!(cxt, ereport!(ERROR,
                            errmsg!("jsonpath item method .{}() can only be applied to an array",
                                std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                            /* C also: errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND) */
                        ));
                    }
                    return res;
                }
                size = 1;
            }

            jb = palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;
            (*jb).type_ = jbvNumeric;
            (*jb).val.numeric = (int64_to_numeric(size as int64)) as crate::utils::adt::jsonb_util::Numeric;

            res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, false);
        }

        jpiAbs => {
            return executeNumericItemMethod(cxt, jsp, jb, unwrap, Some(numeric_abs), found);
        }

        jpiFloor => {
            return executeNumericItemMethod(cxt, jsp, jb, unwrap, Some(numeric_floor), found);
        }

        jpiCeiling => {
            return executeNumericItemMethod(cxt, jsp, jb, unwrap, Some(numeric_ceil), found);
        }

        jpiDouble => {
            let mut jbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();

            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }

            if (*jb).type_ == jbvNumeric {
                let tmp: *mut c_char = DatumGetCString(
                    DirectFunctionCall1!(numeric_out,
                                        NumericGetDatum(((*jb).val.numeric as Numeric))));
                let mut val: f64;
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };

                val = float8in_internal(tmp, ptr::null_mut(),
                                        b"double precision\0".as_ptr() as *mut c_char,
                                        tmp,
                                        &mut escontext as *mut ErrorSaveContext as *mut Node);

                if escontext.error_occurred {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type double precision",
                            std::ffi::CStr::from_ptr(tmp).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                        /* C also: errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM) */
                    ));
                }
                if val.is_infinite() || val.is_nan() {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("NaN or Infinity is not allowed for jsonpath item method .{}()",
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                        /* C also: errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM) */
                    ));
                }
                res = jperOk;
            } else if (*jb).type_ == jbvString {
                /* cast string as double */
                let tmp: *mut c_char = pnstrdup((*jb).val.string.val,
                                                 (*jb).val.string.len as usize);
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };
                let mut val: f64;

                val = float8in_internal(tmp, ptr::null_mut(),
                                        b"double precision\0".as_ptr() as *mut c_char,
                                        tmp,
                                        &mut escontext as *mut ErrorSaveContext as *mut Node);

                if escontext.error_occurred {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type double precision",
                            std::ffi::CStr::from_ptr(tmp).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                if val.is_infinite() || val.is_nan() {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("NaN or Infinity is not allowed for jsonpath item method .{}()",
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }

                let jbv_ptr = jbv.as_mut_ptr();
                jb = jbv_ptr;
                (*jb).type_ = jbvNumeric;
                (*jb).val.numeric = DatumGetNumeric(
                    DirectFunctionCall1!(float8_numeric, Float8GetDatum(val))) as crate::utils::adt::jsonb_util::Numeric;
                res = jperOk;
            }

            if res == jperNotFound {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath item method .{}() can only be applied to a string or numeric value",
                        std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                ));
            }

            res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, true);
        }

        jpiDatetime | jpiDate | jpiTime | jpiTimeTz | jpiTimestamp | jpiTimestampTz => {
            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }
            return executeDateTimeMethod(cxt, jsp, jb, found);
        }

        jpiKeyValue => {
            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }
            return executeKeyValueMethod(cxt, jsp, jb, found);
        }

        jpiLast => {
            let mut tmpjbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();
            let lastjbv: *mut JsonbValue;
            let last: c_int;
            let hasNext: bool = jspGetNext(jsp, elem.as_mut_ptr());

            if (*cxt).innermostArraySize < 0 {
                elog!(ERROR, "evaluating jsonpath LAST outside of array subscript");
            }

            if !hasNext && found.is_null() {
                res = jperOk;
                return res;
            }

            last = (*cxt).innermostArraySize - 1;

            lastjbv = if hasNext {
                tmpjbv.as_mut_ptr()
            } else {
                palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue
            };

            (*lastjbv).type_ = jbvNumeric;
            (*lastjbv).val.numeric = (int64_to_numeric(last as int64)) as crate::utils::adt::jsonb_util::Numeric;

            res = executeNextItem(cxt, jsp, elem.as_mut_ptr(),
                                  lastjbv, found, hasNext);
        }

        jpiBigint => {
            /* handled in part 3 */
            let mut jbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();
            let mut datum: Datum = 0;

            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }

            if (*jb).type_ == jbvNumeric {
                let mut have_error: bool = false;
                let val: int64 =
                    numeric_int8_opt_error(((*jb).val.numeric as Numeric), &mut have_error);
                if have_error {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type bigint",
                            std::ffi::CStr::from_ptr(DatumGetCString(DirectFunctionCall1!(
                                numeric_out, NumericGetDatum(((*jb).val.numeric as Numeric))))).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                datum = Int64GetDatum(val);
                res = jperOk;
            } else if (*jb).type_ == jbvString {
                /* cast string as bigint */
                let tmp: *mut c_char = pnstrdup((*jb).val.string.val,
                                                 (*jb).val.string.len as usize);
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };
                let noerr: bool = DirectInputFunctionCallSafe(
                    int8in, tmp, InvalidOid, -1,
                    &mut escontext as *mut ErrorSaveContext as *mut Node,
                    &mut datum);
                if !noerr || escontext.error_occurred {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type bigint",
                            std::ffi::CStr::from_ptr(tmp).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                res = jperOk;
            }

            if res == jperNotFound {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath item method .{}() can only be applied to a string or numeric value",
                        std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                ));
            }

            let jbv_ptr = jbv.as_mut_ptr();
            jb = jbv_ptr;
            (*jb).type_ = jbvNumeric;
            (*jb).val.numeric = DatumGetNumeric(
                DirectFunctionCall1!(int8_numeric, datum)) as crate::utils::adt::jsonb_util::Numeric;

            res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, true);
        }

        jpiBoolean => {
            let mut jbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();
            let mut bval: bool = false;

            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }

            if (*jb).type_ == jbvBool {
                bval = (*jb).val.boolean;
                res = jperOk;
            } else if (*jb).type_ == jbvNumeric {
                let mut datum: Datum = 0;
                let tmp: *mut c_char = DatumGetCString(DirectFunctionCall1!(
                    numeric_out, NumericGetDatum(((*jb).val.numeric as Numeric))));
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };
                let noerr: bool = DirectInputFunctionCallSafe(
                    int4in, tmp, InvalidOid, -1,
                    &mut escontext as *mut ErrorSaveContext as *mut Node,
                    &mut datum);
                if !noerr || escontext.error_occurred {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type boolean",
                            std::ffi::CStr::from_ptr(tmp).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                let ival: int32 = DatumGetInt32(datum);
                bval = ival != 0;
                res = jperOk;
            } else if (*jb).type_ == jbvString {
                let tmp: *mut c_char = pnstrdup((*jb).val.string.val,
                                                 (*jb).val.string.len as usize);
                if !parse_bool(tmp, &mut bval) {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type boolean",
                            std::ffi::CStr::from_ptr(tmp).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                res = jperOk;
            }

            if res == jperNotFound {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath item method .{}() can only be applied to a boolean, string, or numeric value",
                        std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                ));
            }

            let jbv_ptr = jbv.as_mut_ptr();
            jb = jbv_ptr;
            (*jb).type_ = jbvBool;
            (*jb).val.boolean = bval;

            res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, true);
        }

        jpiDecimal | jpiNumber => {
            let mut jbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();
            let mut num: Numeric = ptr::null_mut();
            let mut numstr: *mut c_char = ptr::null_mut();

            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }

            if (*jb).type_ == jbvNumeric {
                num = (*jb).val.numeric as Numeric;
                if numeric_is_nan(num) || numeric_is_inf(num) {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("NaN or Infinity is not allowed for jsonpath item method .{}()",
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                if (*jsp).type_ == jpiDecimal {
                    numstr = DatumGetCString(DirectFunctionCall1!(
                        numeric_out, NumericGetDatum(num)));
                }
                res = jperOk;
            } else if (*jb).type_ == jbvString {
                let mut datum: Datum = 0;
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };
                numstr = pnstrdup((*jb).val.string.val, (*jb).val.string.len as usize);
                let noerr: bool = DirectInputFunctionCallSafe(
                    numeric_in, numstr, InvalidOid, -1,
                    &mut escontext as *mut ErrorSaveContext as *mut Node,
                    &mut datum);
                if !noerr || escontext.error_occurred {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type numeric",
                            std::ffi::CStr::from_ptr(numstr).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                num = DatumGetNumeric(datum);
                if numeric_is_nan(num) || numeric_is_inf(num) {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("NaN or Infinity is not allowed for jsonpath item method .{}()",
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                res = jperOk;
            }

            if res == jperNotFound {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath item method .{}() can only be applied to a string or numeric value",
                        std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                ));
            }

            /* Handle .decimal(precision[, scale]) arguments */
            if (*jsp).type_ == jpiDecimal && (*jsp).content.args.left != 0 {
                let mut numdatum: Datum = 0;
                let dtypmod: Datum;
                let precision: int32;
                let mut scale: int32 = 0;
                let mut have_error: bool = false;
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };
                let mut pstr: [c_char; 12] = [0; 12];
                let mut sstr: [c_char; 12] = [0; 12];

                jspGetLeftArg(jsp, elem.as_mut_ptr());
                if (*elem.as_mut_ptr()).type_ != jpiNumeric {
                    elog!(ERROR, "invalid jsonpath item type for .decimal() precision");
                }
                precision = numeric_int4_opt_error(jspGetNumeric(elem.as_mut_ptr()), &mut have_error);
                if have_error {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("precision of jsonpath item method .{}() is out of range for type integer",
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }

                if (*jsp).content.args.right != 0 {
                    jspGetRightArg(jsp, elem.as_mut_ptr());
                    if (*elem.as_mut_ptr()).type_ != jpiNumeric {
                        elog!(ERROR, "invalid jsonpath item type for .decimal() scale");
                    }
                    scale = numeric_int4_opt_error(jspGetNumeric(elem.as_mut_ptr()), &mut have_error);
                    if have_error {
                        RETURN_ERROR!(cxt, ereport!(ERROR,
                            errmsg!("scale of jsonpath item method .{}() is out of range for type integer",
                                std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                        ));
                    }
                }

                pg_ltoa(precision, pstr.as_mut_ptr());
                pg_ltoa(scale, sstr.as_mut_ptr());
                let mut datums: [Datum; 2] = [
                    CStringGetDatum(pstr.as_ptr()),
                    CStringGetDatum(sstr.as_ptr()),
                ];
                let arrtypmod: *mut ArrayType =
                    construct_array_builtin(datums.as_mut_ptr(), 2, CSTRINGOID);

                dtypmod = DirectFunctionCall1!(numerictypmodin,
                                              PointerGetDatum(arrtypmod as *const c_void));

                let noerr: bool = DirectInputFunctionCallSafe(
                    numeric_in, numstr, InvalidOid, dtypmod as int32,
                    &mut escontext as *mut ErrorSaveContext as *mut Node,
                    &mut numdatum);

                if !noerr || escontext.error_occurred {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type numeric",
                            std::ffi::CStr::from_ptr(numstr).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }

                num = DatumGetNumeric(numdatum);
                pfree(arrtypmod as *mut c_void);
            }

            let jbv_ptr = jbv.as_mut_ptr();
            jb = jbv_ptr;
            (*jb).type_ = jbvNumeric;
            (*jb).val.numeric = (num) as crate::utils::adt::jsonb_util::Numeric;

            res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, true);
        }

        jpiInteger => {
            let mut jbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();
            let mut datum: Datum = 0;

            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }

            if (*jb).type_ == jbvNumeric {
                let mut have_error: bool = false;
                let val: int32 = numeric_int4_opt_error(((*jb).val.numeric as Numeric), &mut have_error);
                if have_error {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type integer",
                            std::ffi::CStr::from_ptr(DatumGetCString(DirectFunctionCall1!(
                                numeric_out, NumericGetDatum(((*jb).val.numeric as Numeric))))).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                datum = Int32GetDatum(val);
                res = jperOk;
            } else if (*jb).type_ == jbvString {
                let tmp: *mut c_char = pnstrdup((*jb).val.string.val,
                                                 (*jb).val.string.len as usize);
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };
                let noerr: bool = DirectInputFunctionCallSafe(
                    int4in, tmp, InvalidOid, -1,
                    &mut escontext as *mut ErrorSaveContext as *mut Node,
                    &mut datum);
                if !noerr || escontext.error_occurred {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("argument \"{}\" of jsonpath item method .{}() is invalid for type integer",
                            std::ffi::CStr::from_ptr(tmp).to_string_lossy(),
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                res = jperOk;
            }

            if res == jperNotFound {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("jsonpath item method .{}() can only be applied to a string or numeric value",
                        std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                ));
            }

            let jbv_ptr = jbv.as_mut_ptr();
            jb = jbv_ptr;
            (*jb).type_ = jbvNumeric;
            (*jb).val.numeric = DatumGetNumeric(
                DirectFunctionCall1!(int4_numeric, datum)) as crate::utils::adt::jsonb_util::Numeric;

            res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, true);
        }

        jpiStringFunc => {
            let mut jbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();
            let mut tmp: *mut c_char = ptr::null_mut();

            if unwrap && JsonbType(jb) == jbvArray as c_int {
                return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
            }

            match JsonbType(jb) as u32 {
                t if t == jbvString as u32 => {
                    tmp = pnstrdup((*jb).val.string.val, (*jb).val.string.len as usize);
                }
                t if t == jbvNumeric as u32 => {
                    tmp = DatumGetCString(DirectFunctionCall1!(
                        numeric_out, NumericGetDatum(((*jb).val.numeric as Numeric))));
                }
                t if t == jbvBool as u32 => {
                    tmp = if (*jb).val.boolean {
                        b"true\0".as_ptr() as *mut c_char
                    } else {
                        b"false\0".as_ptr() as *mut c_char
                    };
                }
                t if t == jbvDatetime as u32 => {
                    let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];
                    JsonEncodeDateTime(buf.as_mut_ptr(),
                                       (*jb).val.datetime.value,
                                       (*jb).val.datetime.typid,
                                       &(*jb).val.datetime.tz);
                    tmp = pstrdup(buf.as_ptr());
                }
                _ => {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("jsonpath item method .{}() can only be applied to a boolean, string, numeric, or datetime value",
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
            }

            let jbv_ptr = jbv.as_mut_ptr();
            jb = jbv_ptr;
            (*jb).val.string.val = tmp;
            (*jb).val.string.len = libc_strlen(tmp as *const u8) as i32;
            (*jb).type_ = jbvString;

            res = executeNextItem(cxt, jsp, ptr::null_mut(), jb, found, true);
        }

        _ => {
            elog!(ERROR, "unrecognized jsonpath item type: {}", (*jsp).type_ as i32);
        }
    }

    res
}

/* inline strlen to avoid libc dependency */
#[inline]
unsafe fn libc_strlen(s: *const u8) -> usize {
    let mut i = 0usize;
    while *s.add(i) != 0 { i += 1; }
    i
}

/* BinaryArithmFunc adapter callbacks (fn-ptr-compatible with BinaryArithmFunc) */
unsafe fn numeric_add_opt_error_cb(n1: Numeric, n2: Numeric, e: *mut bool) -> Numeric {
    numeric_add_opt_error(n1, n2, e)
}
unsafe fn numeric_sub_opt_error_cb(n1: Numeric, n2: Numeric, e: *mut bool) -> Numeric {
    numeric_sub_opt_error(n1, n2, e)
}
unsafe fn numeric_mul_opt_error_cb(n1: Numeric, n2: Numeric, e: *mut bool) -> Numeric {
    numeric_mul_opt_error(n1, n2, e)
}
unsafe fn numeric_div_opt_error_cb(n1: Numeric, n2: Numeric, e: *mut bool) -> Numeric {
    numeric_div_opt_error(n1, n2, e)
}
unsafe fn numeric_mod_opt_error_cb(n1: Numeric, n2: Numeric, e: *mut bool) -> Numeric {
    numeric_mod_opt_error(n1, n2, e)
}

/* JsonPathGetVarCallback adapters */
unsafe fn getJsonPathVariableFromJsonb_cb(
    vars: *mut c_void,
    varName: *mut c_char,
    varNameLen: c_int,
    baseObject: *mut JsonbValue,
    baseObjectId: *mut c_int,
) -> *mut JsonbValue {
    getJsonPathVariableFromJsonb(vars, varName, varNameLen, baseObject, baseObjectId)
}

unsafe fn GetJsonPathVar_cb(
    vars: *mut c_void,
    varName: *mut c_char,
    varNameLen: c_int,
    baseObject: *mut JsonbValue,
    baseObjectId: *mut c_int,
) -> *mut JsonbValue {
    GetJsonPathVar(vars, varName, varNameLen, baseObject, baseObjectId)
}

/* JsonPathCountVarsCallback adapters */
unsafe fn countVariablesFromJsonb_cb(vars: *mut c_void) -> c_int {
    countVariablesFromJsonb(vars)
}
unsafe fn CountJsonPathVars_cb(vars: *mut c_void) -> c_int {
    CountJsonPathVars(vars)
}

/*
 * Unwrap current array item and execute jsonpath for each of its elements.
 */
unsafe fn executeItemUnwrapTargetArray(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    found: *mut JsonValueList,
    unwrapElements: bool,
) -> JsonPathExecResult {
    if (*jb).type_ != jbvBinary {
        debug_assert!((*jb).type_ != jbvArray);
        elog!(ERROR, "invalid jsonb array value type: {}", (*jb).type_ as i32);
    }

    executeAnyItem(cxt, jsp, (*jb).val.binary.data, found, 1, 1, 1,
                   false, unwrapElements)
}

/*
 * Execute next jsonpath item if exists.  Otherwise put "v" to the "found"
 * list if provided.
 */
unsafe fn executeNextItem(
    cxt: *mut JsonPathExecContext,
    cur: *mut JsonPathItem,
    mut next: *mut JsonPathItem,
    v: *mut JsonbValue,
    found: *mut JsonValueList,
    copy: bool,
) -> JsonPathExecResult {
    let mut elem = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let hasNext: bool;

    if cur.is_null() {
        hasNext = !next.is_null();
    } else if !next.is_null() {
        hasNext = jspHasNext(cur);
    } else {
        next = elem.as_mut_ptr();
        hasNext = jspGetNext(cur, next);
    }

    if hasNext {
        return executeItem(cxt, next, v, found);
    }

    if !found.is_null() {
        JsonValueListAppend(found, if copy { copyJsonbValue(v) } else { v });
    }

    jperOk
}

/*
 * Same as executeItem(), but when "unwrap == true" automatically unwraps
 * each array item from the resulting sequence in lax mode.
 */
unsafe fn executeItemOptUnwrapResult(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    unwrap: bool,
    found: *mut JsonValueList,
) -> JsonPathExecResult {
    if unwrap && jspAutoUnwrap(cxt) {
        let mut seq = JsonValueList::new();
        let mut it = JsonValueListIterator {
            value: ptr::null_mut(),
            list: ptr::null_mut(),
            next: ptr::null_mut(),
        };
        let res: JsonPathExecResult = executeItem(cxt, jsp, jb, &mut seq);
        let mut item: *mut JsonbValue;

        if jperIsError(res) {
            return res;
        }

        JsonValueListInitIterator(&seq, &mut it);
        while { item = JsonValueListNext(&seq, &mut it); !item.is_null() } {
            debug_assert!((*item).type_ != jbvArray);

            if JsonbType(item) == jbvArray as c_int {
                executeItemUnwrapTargetArray(cxt, ptr::null_mut(), item, found, false);
            } else {
                JsonValueListAppend(found, item);
            }
        }

        return jperOk;
    }

    executeItem(cxt, jsp, jb, found)
}

/*
 * Same as executeItemOptUnwrapResult(), but with error suppression.
 */
unsafe fn executeItemOptUnwrapResultNoThrow(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    unwrap: bool,
    found: *mut JsonValueList,
) -> JsonPathExecResult {
    let res: JsonPathExecResult;
    let throwErrors: bool = (*cxt).throwErrors;

    (*cxt).throwErrors = false;
    res = executeItemOptUnwrapResult(cxt, jsp, jb, unwrap, found);
    (*cxt).throwErrors = throwErrors;

    res
}

/* Execute boolean-valued jsonpath expression. */
unsafe fn executeBoolItem(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    canHaveNext: bool,
) -> JsonPathBool {
    let mut larg = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let mut rarg = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let mut res: JsonPathBool;
    let mut res2: JsonPathBool;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    if !canHaveNext && jspHasNext(jsp) {
        elog!(ERROR, "boolean jsonpath item cannot have next item");
    }

    match (*jsp).type_ {
        jpiAnd => {
            jspGetLeftArg(jsp, larg.as_mut_ptr());
            res = executeBoolItem(cxt, larg.as_mut_ptr(), jb, false);

            if res == jpbFalse {
                return jpbFalse;
            }

            /*
             * SQL/JSON says that we should check second arg in case of
             * jperError
             */

            jspGetRightArg(jsp, rarg.as_mut_ptr());
            res2 = executeBoolItem(cxt, rarg.as_mut_ptr(), jb, false);

            return if res2 == jpbTrue { res } else { res2 };
        }

        jpiOr => {
            jspGetLeftArg(jsp, larg.as_mut_ptr());
            res = executeBoolItem(cxt, larg.as_mut_ptr(), jb, false);

            if res == jpbTrue {
                return jpbTrue;
            }

            jspGetRightArg(jsp, rarg.as_mut_ptr());
            res2 = executeBoolItem(cxt, rarg.as_mut_ptr(), jb, false);

            return if res2 == jpbFalse { res } else { res2 };
        }

        jpiNot => {
            jspGetArg(jsp, larg.as_mut_ptr());

            res = executeBoolItem(cxt, larg.as_mut_ptr(), jb, false);

            if res == jpbUnknown {
                return jpbUnknown;
            }

            return if res == jpbTrue { jpbFalse } else { jpbTrue };
        }

        jpiIsUnknown => {
            jspGetArg(jsp, larg.as_mut_ptr());
            res = executeBoolItem(cxt, larg.as_mut_ptr(), jb, false);
            return if res == jpbUnknown { jpbTrue } else { jpbFalse };
        }

        jpiEqual | jpiNotEqual | jpiLess | jpiGreater
        | jpiLessOrEqual | jpiGreaterOrEqual => {
            jspGetLeftArg(jsp, larg.as_mut_ptr());
            jspGetRightArg(jsp, rarg.as_mut_ptr());
            return executePredicate(cxt, jsp, larg.as_mut_ptr(), rarg.as_mut_ptr(),
                                    jb, true, Some(executeComparison_cb), cxt as *mut c_void);
        }

        jpiStartsWith => { /* 'whole STARTS WITH initial' */
            jspGetLeftArg(jsp, larg.as_mut_ptr()); /* 'whole' */
            jspGetRightArg(jsp, rarg.as_mut_ptr()); /* 'initial' */
            return executePredicate(cxt, jsp, larg.as_mut_ptr(), rarg.as_mut_ptr(),
                                    jb, false, Some(executeStartsWith_cb), ptr::null_mut());
        }

        jpiLikeRegex => { /* 'expr LIKE_REGEX pattern FLAGS flags' */
            /*
             * 'expr' is a sequence-returning expression.  'pattern' is a
             * regex string literal.  SQL/JSON standard requires XQuery
             * regexes, but we use Postgres regexes here.  'flags' is a
             * string literal converted to integer flags at compile-time.
             */
            let mut lrcxt = JsonLikeRegexContext { regex: ptr::null_mut(), cflags: 0 };

            jspInitByBuffer(larg.as_mut_ptr(), (*jsp).base,
                            (*jsp).content.like_regex.expr);

            return executePredicate(cxt, jsp, larg.as_mut_ptr(), ptr::null_mut(),
                                    jb, false, Some(executeLikeRegex_cb),
                                    &mut lrcxt as *mut JsonLikeRegexContext as *mut c_void);
        }

        jpiExists => {
            jspGetArg(jsp, larg.as_mut_ptr());

            if jspStrictAbsenceOfErrors(cxt) {
                /*
                 * In strict mode we must get a complete list of values to
                 * check that there are no errors at all.
                 */
                let mut vals = JsonValueList::new();
                let res: JsonPathExecResult =
                    executeItemOptUnwrapResultNoThrow(cxt, larg.as_mut_ptr(), jb,
                                                     false, &mut vals);

                if jperIsError(res) {
                    return jpbUnknown;
                }

                return if JsonValueListIsEmpty(&mut vals) { jpbFalse } else { jpbTrue };
            } else {
                let res: JsonPathExecResult =
                    executeItemOptUnwrapResultNoThrow(cxt, larg.as_mut_ptr(), jb,
                                                     false, ptr::null_mut());

                if jperIsError(res) {
                    return jpbUnknown;
                }

                return if res == jperOk { jpbTrue } else { jpbFalse };
            }
        }

        _ => {
            elog!(ERROR, "invalid boolean jsonpath item type: {}", (*jsp).type_ as i32);
            return jpbUnknown;
        }
    }
}

/*
 * Execute nested (filters etc.) boolean expression pushing current SQL/JSON
 * item onto the stack.
 */
unsafe fn executeNestedBoolItem(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
) -> JsonPathBool {
    let prev: *mut JsonbValue;
    let res: JsonPathBool;

    prev = (*cxt).current;
    (*cxt).current = jb;
    res = executeBoolItem(cxt, jsp, jb, false);
    (*cxt).current = prev;

    res
}

/* Predicate callback adapters */
unsafe fn executeComparison_cb(
    cmp: *mut JsonPathItem,
    lv: *mut JsonbValue,
    rv: *mut JsonbValue,
    p: *mut c_void,
) -> JsonPathBool {
    let cxt = p as *mut JsonPathExecContext;
    compareItems((*cmp).type_, lv, rv, (*cxt).useTz)
}

unsafe fn executeStartsWith_cb(
    jsp: *mut JsonPathItem,
    whole: *mut JsonbValue,
    initial: *mut JsonbValue,
    param: *mut c_void,
) -> JsonPathBool {
    executeStartsWith(jsp, whole, initial, param)
}

unsafe fn executeLikeRegex_cb(
    jsp: *mut JsonPathItem,
    str_: *mut JsonbValue,
    rarg: *mut JsonbValue,
    param: *mut c_void,
) -> JsonPathBool {
    executeLikeRegex(jsp, str_, rarg, param)
}

/*
 * Implementation of several jsonpath nodes:
 *  - jpiAny (.** accessor),
 *  - jpiAnyKey (.* accessor),
 *  - jpiAnyArray ([*] accessor)
 */
unsafe fn executeAnyItem(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jbc: *mut JsonbContainer,
    found: *mut JsonValueList,
    level: uint32,
    first: uint32,
    last: uint32,
    ignoreStructuralErrors: bool,
    unwrapNext: bool,
) -> JsonPathExecResult {
    let mut res: JsonPathExecResult = jperNotFound;
    let mut it: *mut JsonbIterator;
    let mut r: JsonbIteratorToken;
    let mut v = core::mem::MaybeUninit::<JsonbValue>::zeroed();

    check_stack_depth();

    if level > last {
        return res;
    }

    it = JsonbIteratorInit(jbc);

    /*
     * Recursively iterate over jsonb objects/arrays
     */
    loop {
        r = JsonbIteratorNext(&mut it, v.as_mut_ptr(), true);
        if r == WJB_DONE { break; }

        if r == WJB_KEY {
            r = JsonbIteratorNext(&mut it, v.as_mut_ptr(), true);
            debug_assert!(r == WJB_VALUE);
        }

        if r == WJB_VALUE || r == WJB_ELEM {
            if level >= first
                || (first == PG_UINT32_MAX && last == PG_UINT32_MAX
                    && (*v.as_ptr()).type_ != jbvBinary) /* leaves only requested */
            {
                /* check expression */
                if !jsp.is_null() {
                    if ignoreStructuralErrors {
                        let savedIgnoreStructuralErrors: bool;

                        savedIgnoreStructuralErrors = (*cxt).ignoreStructuralErrors;
                        (*cxt).ignoreStructuralErrors = true;
                        res = executeItemOptUnwrapTarget(cxt, jsp, v.as_mut_ptr(),
                                                        found, unwrapNext);
                        (*cxt).ignoreStructuralErrors = savedIgnoreStructuralErrors;
                    } else {
                        res = executeItemOptUnwrapTarget(cxt, jsp, v.as_mut_ptr(),
                                                        found, unwrapNext);
                    }

                    if jperIsError(res) { break; }
                    if res == jperOk && found.is_null() { break; }
                } else if !found.is_null() {
                    JsonValueListAppend(found, copyJsonbValue(v.as_mut_ptr()));
                } else {
                    return jperOk;
                }
            }

            if level < last && (*v.as_ptr()).type_ == jbvBinary {
                res = executeAnyItem(
                    cxt, jsp, (*v.as_ptr()).val.binary.data, found,
                    level + 1, first, last,
                    ignoreStructuralErrors, unwrapNext);

                if jperIsError(res) { break; }
                if res == jperOk && found.is_null() { break; }
            }
        }
    }

    res
}

/*
 * Execute unary or binary predicate.
 */
unsafe fn executePredicate(
    cxt: *mut JsonPathExecContext,
    pred: *mut JsonPathItem,
    larg: *mut JsonPathItem,
    rarg: *mut JsonPathItem,
    jb: *mut JsonbValue,
    unwrapRightArg: bool,
    exec: Option<JsonPathPredicateCallback>,
    param: *mut c_void,
) -> JsonPathBool {
    let mut res: JsonPathExecResult;
    let mut lseqit = JsonValueListIterator {
        value: ptr::null_mut(),
        list: ptr::null_mut(),
        next: ptr::null_mut(),
    };
    let mut lseq = JsonValueList::new();
    let mut rseq = JsonValueList::new();
    let mut lval: *mut JsonbValue;
    let mut error: bool = false;
    let mut found: bool = false;

    /* Left argument is always auto-unwrapped. */
    res = executeItemOptUnwrapResultNoThrow(cxt, larg, jb, true, &mut lseq);
    if jperIsError(res) {
        return jpbUnknown;
    }

    if !rarg.is_null() {
        /* Right argument is conditionally auto-unwrapped. */
        res = executeItemOptUnwrapResultNoThrow(cxt, rarg, jb,
                                               unwrapRightArg, &mut rseq);
        if jperIsError(res) {
            return jpbUnknown;
        }
    }

    JsonValueListInitIterator(&lseq, &mut lseqit);
    while { lval = JsonValueListNext(&lseq, &mut lseqit); !lval.is_null() } {
        let mut rseqit = JsonValueListIterator {
            value: ptr::null_mut(),
            list: ptr::null_mut(),
            next: ptr::null_mut(),
        };
        let mut rval: *mut JsonbValue;
        let mut first: bool = true;

        JsonValueListInitIterator(&rseq, &mut rseqit);
        if !rarg.is_null() {
            rval = JsonValueListNext(&rseq, &mut rseqit);
        } else {
            rval = ptr::null_mut();
        }

        /* Loop over right arg sequence or do single pass otherwise */
        loop {
            let cond = if !rarg.is_null() { !rval.is_null() } else { first };
            if !cond { break; }

            let res_inner: JsonPathBool = exec.unwrap()(pred, lval, rval, param);

            if res_inner == jpbUnknown {
                if jspStrictAbsenceOfErrors(cxt) {
                    return jpbUnknown;
                }
                error = true;
            } else if res_inner == jpbTrue {
                if !jspStrictAbsenceOfErrors(cxt) {
                    return jpbTrue;
                }
                found = true;
            }

            first = false;
            if !rarg.is_null() {
                rval = JsonValueListNext(&rseq, &mut rseqit);
            }
        }
    }

    if found { /* possible only in strict mode */
        return jpbTrue;
    }

    if error { /* possible only in lax mode */
        return jpbUnknown;
    }

    jpbFalse
}

/*
 * Execute binary arithmetic expression on singleton numeric operands.
 */
unsafe fn executeBinaryArithmExpr(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    func: Option<BinaryArithmFunc>,
    found: *mut JsonValueList,
) -> JsonPathExecResult {
    let mut jper: JsonPathExecResult;
    let mut elem = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let mut lseq = JsonValueList::new();
    let mut rseq = JsonValueList::new();
    let mut lval: *mut JsonbValue;
    let mut rval: *mut JsonbValue;
    let mut res: Numeric;

    jspGetLeftArg(jsp, elem.as_mut_ptr());

    /*
     * XXX: By standard only operands of multiplicative expressions are
     * unwrapped.  We extend it to other binary arithmetic expressions too.
     */
    jper = executeItemOptUnwrapResult(cxt, elem.as_mut_ptr(), jb, true, &mut lseq);
    if jperIsError(jper) {
        return jper;
    }

    jspGetRightArg(jsp, elem.as_mut_ptr());

    jper = executeItemOptUnwrapResult(cxt, elem.as_mut_ptr(), jb, true, &mut rseq);
    if jperIsError(jper) {
        return jper;
    }

    lval = getScalar(JsonValueListHead(&mut lseq), jbvNumeric);
    if JsonValueListLength(&lseq) != 1 || lval.is_null() {
        RETURN_ERROR!(cxt, ereport!(ERROR,
            errmsg!("left operand of jsonpath operator {} is not a single numeric value",
                std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
            /* C also: errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED) */
        ));
    }

    rval = getScalar(JsonValueListHead(&mut rseq), jbvNumeric);
    if JsonValueListLength(&rseq) != 1 || rval.is_null() {
        RETURN_ERROR!(cxt, ereport!(ERROR,
            errmsg!("right operand of jsonpath operator {} is not a single numeric value",
                std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
        ));
    }

    if jspThrowErrors(cxt) {
        res = func.unwrap()(((*lval).val.numeric as Numeric), ((*rval).val.numeric as Numeric), ptr::null_mut());
    } else {
        let mut error: bool = false;

        res = func.unwrap()(((*lval).val.numeric as Numeric), ((*rval).val.numeric as Numeric), &mut error);

        if error {
            return jperError;
        }
    }

    if !jspGetNext(jsp, elem.as_mut_ptr()) && found.is_null() {
        return jperOk;
    }

    lval = palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;
    (*lval).type_ = jbvNumeric;
    (*lval).val.numeric = (res) as crate::utils::adt::jsonb_util::Numeric;

    executeNextItem(cxt, jsp, elem.as_mut_ptr(), lval, found, false)
}

/*
 * Execute unary arithmetic expression for each numeric item in its operand's
 * sequence.
 */
unsafe fn executeUnaryArithmExpr(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    func: Option<PGFunction>,
    found: *mut JsonValueList,
) -> JsonPathExecResult {
    let mut jper: JsonPathExecResult;
    let mut jper2: JsonPathExecResult;
    let mut elem = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let mut seq = JsonValueList::new();
    let mut it = JsonValueListIterator {
        value: ptr::null_mut(),
        list: ptr::null_mut(),
        next: ptr::null_mut(),
    };
    let mut val: *mut JsonbValue;
    let mut hasNext: bool;

    jspGetArg(jsp, elem.as_mut_ptr());
    jper = executeItemOptUnwrapResult(cxt, elem.as_mut_ptr(), jb, true, &mut seq);

    if jperIsError(jper) {
        return jper;
    }

    jper = jperNotFound;

    hasNext = jspGetNext(jsp, elem.as_mut_ptr());

    JsonValueListInitIterator(&seq, &mut it);
    while { val = JsonValueListNext(&seq, &mut it); !val.is_null() } {
        val = getScalar(val, jbvNumeric);
        if !val.is_null() {
            if found.is_null() && !hasNext {
                return jperOk;
            }
        } else {
            if found.is_null() && !hasNext {
                continue; /* skip non-numerics processing */
            }

            RETURN_ERROR!(cxt, ereport!(ERROR,
                errmsg!("operand of unary jsonpath operator {} is not a numeric value",
                    std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                /* C also: errcode(ERRCODE_SQL_JSON_NUMBER_NOT_FOUND) */
            ));
        }

        if let Some(f) = func {
            (*val).val.numeric =
                DatumGetNumeric(DirectFunctionCall1!(f,
                                                    NumericGetDatum(((*val).val.numeric as Numeric)))) as crate::utils::adt::jsonb_util::Numeric;
        }

        jper2 = executeNextItem(cxt, jsp, elem.as_mut_ptr(), val, found, false);

        if jperIsError(jper2) {
            return jper2;
        }

        if jper2 == jperOk {
            if found.is_null() {
                return jperOk;
            }
            jper = jperOk;
        }
    }

    jper
}

/*
 * STARTS_WITH predicate callback.
 */
unsafe fn executeStartsWith(
    jsp: *mut JsonPathItem,
    mut whole: *mut JsonbValue,
    mut initial: *mut JsonbValue,
    param: *mut c_void,
) -> JsonPathBool {
    whole = getScalar(whole, jbvString);
    if whole.is_null() {
        return jpbUnknown; /* error */
    }

    initial = getScalar(initial, jbvString);
    if initial.is_null() {
        return jpbUnknown; /* error */
    }

    if (*whole).val.string.len >= (*initial).val.string.len
        && core::slice::from_raw_parts(
            (*whole).val.string.val as *const u8,
            (*initial).val.string.len as usize,
        ) == core::slice::from_raw_parts(
            (*initial).val.string.val as *const u8,
            (*initial).val.string.len as usize,
        )
    {
        return jpbTrue;
    }

    jpbFalse
}

/*
 * LIKE_REGEX predicate callback.
 */
unsafe fn executeLikeRegex(
    jsp: *mut JsonPathItem,
    mut str_: *mut JsonbValue,
    rarg: *mut JsonbValue,
    param: *mut c_void,
) -> JsonPathBool {
    let cxt = param as *mut JsonLikeRegexContext;

    str_ = getScalar(str_, jbvString);
    if str_.is_null() {
        return jpbUnknown;
    }

    /* Cache regex text and converted flags. */
    if (*cxt).regex.is_null() {
        (*cxt).regex = cstring_to_text_with_len(
            (*jsp).content.like_regex.pattern,
            (*jsp).content.like_regex.patternlen as c_int,
        );
        let _ = jspConvertRegexFlags((*jsp).content.like_regex.flags,
                                     &mut (*cxt).cflags, ptr::null_mut());
    }

    if RE_compile_and_execute((*cxt).regex as *mut text, (*str_).val.string.val,
                              (*str_).val.string.len as c_int,
                              (*cxt).cflags, DEFAULT_COLLATION_OID, 0, ptr::null_mut())
    {
        return jpbTrue;
    }

    jpbFalse
}

/*
 * Execute numeric item methods (.abs(), .floor(), .ceil()).
 */
unsafe fn executeNumericItemMethod(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    mut jb: *mut JsonbValue,
    unwrap: bool,
    func: Option<PGFunction>,
    found: *mut JsonValueList,
) -> JsonPathExecResult {
    let mut next = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let datum: Datum;

    if unwrap && JsonbType(jb) == jbvArray as c_int {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    }

    jb = getScalar(jb, jbvNumeric);
    if jb.is_null() {
        RETURN_ERROR!(cxt, ereport!(ERROR,
            errmsg!("jsonpath item method .{}() can only be applied to a numeric value",
                std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
            /* C also: errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM) */
        ));
    }

    datum = DirectFunctionCall1!(func.unwrap(), NumericGetDatum(((*jb).val.numeric as Numeric)));

    if !jspGetNext(jsp, next.as_mut_ptr()) && found.is_null() {
        return jperOk;
    }

    jb = palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;
    (*jb).type_ = jbvNumeric;
    (*jb).val.numeric = (DatumGetNumeric(datum)) as crate::utils::adt::jsonb_util::Numeric;

    executeNextItem(cxt, jsp, next.as_mut_ptr(), jb, found, false)
}

/*
 * Implementation of the .datetime() and related methods.
 */
unsafe fn executeDateTimeMethod(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    mut jb: *mut JsonbValue,
    found: *mut JsonValueList,
) -> JsonPathExecResult {
    let mut jbvbuf = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let mut value: Datum = 0;
    let datetime: *mut text;
    let collid: Oid;
    let mut typid: Oid = 0;
    let mut typmod: int32 = -1;
    let mut tz: c_int = 0;
    let mut hasNext: bool;
    let mut res: JsonPathExecResult = jperNotFound;
    let mut elem = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let mut time_precision: int32 = -1;

    jb = getScalar(jb, jbvString);
    if jb.is_null() {
        RETURN_ERROR!(cxt, ereport!(ERROR,
            errmsg!("jsonpath item method .{}() can only be applied to a string",
                std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
            /* C also: errcode(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION) */
        ));
    }

    datetime = cstring_to_text_with_len((*jb).val.string.val,
                                        (*jb).val.string.len as c_int);

    /*
     * At some point we might wish to have callers supply the collation to
     * use, but right now it's unclear that they'd be able to do better than
     * DEFAULT_COLLATION_OID anyway.
     */
    collid = DEFAULT_COLLATION_OID;

    /*
     * .datetime(template) has an argument, the rest of the methods don't have
     * an argument.
     */
    if (*jsp).type_ == jpiDatetime && (*jsp).content.arg != 0 {
        let template_: *mut text;
        let template_str: *mut c_char;
        let mut template_len: c_int = 0;
        let mut escontext = ErrorSaveContext {
            r#type: NodeTag::T_ErrorSaveContext,
            error_occurred: false,
            details_wanted: false,
            error_data: ptr::null_mut(),
        };

        jspGetArg(jsp, elem.as_mut_ptr());

        if (*elem.as_ptr()).type_ != jpiString {
            elog!(ERROR, "invalid jsonpath item type for .datetime() argument");
        }

        template_str = jspGetString(elem.as_mut_ptr(), &mut template_len);

        template_ = cstring_to_text_with_len(template_str, template_len as c_int);

        value = parse_datetime(datetime, template_, collid, true,
                               &mut typid, &mut typmod, &mut tz,
                               if jspThrowErrors(cxt) {
                                   ptr::null_mut()
                               } else {
                                   &mut escontext as *mut ErrorSaveContext as *mut c_void
                               });

        if escontext.error_occurred {
            res = jperError;
        } else {
            res = jperOk;
        }
    } else {
        /*
         * According to SQL/JSON standard enumerate ISO formats for: date,
         * timetz, time, timestamptz, timestamp.
         */
        static FMT_STR: [&[u8]; 13] = [
            b"yyyy-mm-dd\0",                       /* date */
            b"HH24:MI:SS.USTZ\0",                  /* timetz */
            b"HH24:MI:SSTZ\0",
            b"HH24:MI:SS.US\0",                    /* time without tz */
            b"HH24:MI:SS\0",
            b"yyyy-mm-dd HH24:MI:SS.USTZ\0",       /* timestamptz */
            b"yyyy-mm-dd HH24:MI:SSTZ\0",
            b"yyyy-mm-dd\"T\"HH24:MI:SS.USTZ\0",
            b"yyyy-mm-dd\"T\"HH24:MI:SSTZ\0",
            b"yyyy-mm-dd HH24:MI:SS.US\0",         /* timestamp without tz */
            b"yyyy-mm-dd HH24:MI:SS\0",
            b"yyyy-mm-dd\"T\"HH24:MI:SS.US\0",
            b"yyyy-mm-dd\"T\"HH24:MI:SS\0",
        ];
        /* cache for format texts */
        static mut FMT_TXT: [*mut text; 13] = [ptr::null_mut(); 13];

        /*
         * Check for optional precision for methods other than .datetime() and .date()
         */
        if (*jsp).type_ != jpiDatetime && (*jsp).type_ != jpiDate
            && (*jsp).content.arg != 0
        {
            let mut have_error: bool = false;

            jspGetArg(jsp, elem.as_mut_ptr());

            if (*elem.as_ptr()).type_ != jpiNumeric {
                elog!(ERROR, "invalid jsonpath item type for {} argument",
                    std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy());
            }

            time_precision = numeric_int4_opt_error(jspGetNumeric(elem.as_mut_ptr()),
                                                    &mut have_error);
            if have_error {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("time precision of jsonpath item method .{}() is out of range for type integer",
                        std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                ));
            }
        }

        /* loop until datetime format fits */
        let mut i: usize = 0;
        while i < FMT_STR.len() {
            let mut escontext = ErrorSaveContext {
                r#type: NodeTag::T_ErrorSaveContext,
                error_occurred: false,
                details_wanted: false,
                error_data: ptr::null_mut(),
            };

            if FMT_TXT[i].is_null() {
                let oldcxt = MemoryContextSwitchTo(TopMemoryContext);
                FMT_TXT[i] = cstring_to_text(FMT_STR[i].as_ptr() as *const c_char);
                MemoryContextSwitchTo(oldcxt);
            }

            value = parse_datetime(datetime, FMT_TXT[i], collid, true,
                                   &mut typid, &mut typmod, &mut tz,
                                   &mut escontext as *mut ErrorSaveContext as *mut c_void);

            if !escontext.error_occurred {
                res = jperOk;
                break;
            }

            i += 1;
        }

        if res == jperNotFound {
            if (*jsp).type_ == jpiDatetime {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("{} format is not recognized: \"{}\"",
                        "datetime",
                        std::ffi::CStr::from_ptr(text_to_cstring(datetime)).to_string_lossy())
                    /* C also: errhint("Use a datetime template argument to specify the input data format.") */
                ));
            } else {
                RETURN_ERROR!(cxt, ereport!(ERROR,
                    errmsg!("{} format is not recognized: \"{}\"",
                        std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy(),
                        std::ffi::CStr::from_ptr(text_to_cstring(datetime)).to_string_lossy())
                ));
            }
        }
    }

    /*
     * parse_datetime() processes the entire input string per the template or
     * ISO format and returns the Datum in best fitted datetime type.  So, if
     * this call is for a specific datatype, then we do the conversion here.
     */
    match (*jsp).type_ {
        jpiDatetime => { /* Nothing to do for DATETIME */ }

        jpiDate => {
            /* Convert result type to date */
            match typid {
                DATEOID => { /* Nothing to do for DATE */ }
                TIMEOID | TIMETZOID => {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("{} format is not recognized: \"{}\"",
                            "date",
                            std::ffi::CStr::from_ptr(text_to_cstring(datetime)).to_string_lossy())
                    ));
                }
                TIMESTAMPOID => {
                    value = DirectFunctionCall1!(timestamp_date, value);
                }
                TIMESTAMPTZOID => {
                    checkTimezoneIsUsedForCast((*cxt).useTz, b"timestamptz\0".as_ptr() as *const c_char,
                                              b"date\0".as_ptr() as *const c_char);
                    value = DirectFunctionCall1!(timestamptz_date, value);
                }
                _ => {
                    elog!(ERROR, "type with oid {} not supported", typid);
                }
            }
            typid = DATEOID;
        }

        jpiTime => {
            /* Convert result type to time without time zone */
            match typid {
                DATEOID => {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("{} format is not recognized: \"{}\"",
                            "time",
                            std::ffi::CStr::from_ptr(text_to_cstring(datetime)).to_string_lossy())
                    ));
                }
                TIMEOID => { /* Nothing to do for TIME */ }
                TIMETZOID => {
                    checkTimezoneIsUsedForCast((*cxt).useTz,
                                              b"timetz\0".as_ptr() as *const c_char,
                                              b"time\0".as_ptr() as *const c_char);
                    value = DirectFunctionCall1!(timetz_time, value);
                }
                TIMESTAMPOID => {
                    value = DirectFunctionCall1!(timestamp_time, value);
                }
                TIMESTAMPTZOID => {
                    checkTimezoneIsUsedForCast((*cxt).useTz,
                                              b"timestamptz\0".as_ptr() as *const c_char,
                                              b"time\0".as_ptr() as *const c_char);
                    value = DirectFunctionCall1!(timestamptz_time, value);
                }
                _ => { elog!(ERROR, "type with oid {} not supported", typid); }
            }

            /* Force the user-given time precision, if any */
            if time_precision != -1 {
                let mut result: TimeADT;

                /* Get a warning when precision is reduced */
                time_precision = anytime_typmod_check(false, time_precision);
                result = DatumGetTimeADT(value);
                AdjustTimeForTypmod(&mut result, time_precision);
                value = TimeADTGetDatum(result);

                /* Update the typmod value with the user-given precision */
                typmod = time_precision;
            }

            typid = TIMEOID;
        }

        jpiTimeTz => {
            /* Convert result type to time with time zone */
            match typid {
                DATEOID | TIMESTAMPOID => {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("{} format is not recognized: \"{}\"",
                            "time_tz",
                            std::ffi::CStr::from_ptr(text_to_cstring(datetime)).to_string_lossy())
                    ));
                }
                TIMEOID => {
                    checkTimezoneIsUsedForCast((*cxt).useTz,
                                              b"time\0".as_ptr() as *const c_char,
                                              b"timetz\0".as_ptr() as *const c_char);
                    value = DirectFunctionCall1!(time_timetz, value);
                }
                TIMETZOID => { /* Nothing to do for TIMETZ */ }
                TIMESTAMPTZOID => {
                    value = DirectFunctionCall1!(timestamptz_timetz, value);
                }
                _ => { elog!(ERROR, "type with oid {} not supported", typid); }
            }

            /* Force the user-given time precision, if any */
            if time_precision != -1 {
                let mut result: *mut TimeTzADT;

                /* Get a warning when precision is reduced */
                time_precision = anytime_typmod_check(true, time_precision);
                result = DatumGetTimeTzADTP(value);
                AdjustTimeForTypmod(&mut (*result).time, time_precision);
                value = TimeTzADTPGetDatum(result);

                typmod = time_precision;
            }

            typid = TIMETZOID;
        }

        jpiTimestamp => {
            /* Convert result type to timestamp without time zone */
            match typid {
                DATEOID => {
                    value = DirectFunctionCall1!(date_timestamp, value);
                }
                TIMEOID | TIMETZOID => {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("{} format is not recognized: \"{}\"",
                            "timestamp",
                            std::ffi::CStr::from_ptr(text_to_cstring(datetime)).to_string_lossy())
                    ));
                }
                TIMESTAMPOID => { /* Nothing to do for TIMESTAMP */ }
                TIMESTAMPTZOID => {
                    checkTimezoneIsUsedForCast((*cxt).useTz,
                                              b"timestamptz\0".as_ptr() as *const c_char,
                                              b"timestamp\0".as_ptr() as *const c_char);
                    value = DirectFunctionCall1!(timestamptz_timestamp, value);
                }
                _ => { elog!(ERROR, "type with oid {} not supported", typid); }
            }

            /* Force the user-given time precision, if any */
            if time_precision != -1 {
                let mut result: Timestamp;
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };

                /* Get a warning when precision is reduced */
                time_precision = anytimestamp_typmod_check(false, time_precision);
                result = DatumGetTimestamp(value);
                AdjustTimestampForTypmod(&mut result, time_precision,
                                        &mut escontext as *mut ErrorSaveContext as *mut c_void);
                if escontext.error_occurred { /* should not happen */
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("time precision of jsonpath item method .{}() is invalid",
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                value = TimestampGetDatum(result);

                typmod = time_precision;
            }

            typid = TIMESTAMPOID;
        }

        jpiTimestampTz => {
            let mut tm = core::mem::MaybeUninit::<pg_tm>::zeroed();
            let mut fsec: fsec_t = 0;

            /* Convert result type to timestamp with time zone */
            match typid {
                DATEOID => {
                    checkTimezoneIsUsedForCast((*cxt).useTz,
                                              b"date\0".as_ptr() as *const c_char,
                                              b"timestamptz\0".as_ptr() as *const c_char);

                    /*
                     * Get the timezone value explicitly since JsonbValue
                     * keeps that separate.
                     */
                    j2date(DatumGetDateADT(value) + 2451545 /* POSTGRES_EPOCH_JDATE */,
                           &mut (*tm.as_mut_ptr()).tm_year,
                           &mut (*tm.as_mut_ptr()).tm_mon,
                           &mut (*tm.as_mut_ptr()).tm_mday);
                    (*tm.as_mut_ptr()).tm_hour = 0;
                    (*tm.as_mut_ptr()).tm_min = 0;
                    (*tm.as_mut_ptr()).tm_sec = 0;
                    tz = DetermineTimeZoneOffset(tm.as_mut_ptr(),
                                                session_timezone as *mut c_void);

                    value = DirectFunctionCall1!(date_timestamptz, value);
                }
                TIMEOID | TIMETZOID => {
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("{} format is not recognized: \"{}\"",
                            "timestamp_tz",
                            std::ffi::CStr::from_ptr(text_to_cstring(datetime)).to_string_lossy())
                    ));
                }
                TIMESTAMPOID => {
                    checkTimezoneIsUsedForCast((*cxt).useTz,
                                              b"timestamp\0".as_ptr() as *const c_char,
                                              b"timestamptz\0".as_ptr() as *const c_char);

                    /*
                     * Get the timezone value explicitly since JsonbValue
                     * keeps that separate.
                     */
                    if timestamp2tm(DatumGetTimestamp(value), ptr::null_mut(),
                                    tm.as_mut_ptr(), &mut fsec,
                                    ptr::null_mut(), ptr::null_mut()) == 0 {
                        tz = DetermineTimeZoneOffset(tm.as_mut_ptr(),
                                                    session_timezone as *mut c_void);
                    }

                    value = DirectFunctionCall1!(timestamp_timestamptz, value);
                }
                TIMESTAMPTZOID => { /* Nothing to do for TIMESTAMPTZ */ }
                _ => { elog!(ERROR, "type with oid {} not supported", typid); }
            }

            /* Force the user-given time precision, if any */
            if time_precision != -1 {
                let mut result: Timestamp;
                let mut escontext = ErrorSaveContext {
                    r#type: NodeTag::T_ErrorSaveContext,
                    error_occurred: false,
                    details_wanted: false,
                    error_data: ptr::null_mut(),
                };

                /* Get a warning when precision is reduced */
                time_precision = anytimestamp_typmod_check(true, time_precision);
                result = DatumGetTimestampTz(value);
                AdjustTimestampForTypmod(&mut result, time_precision,
                                        &mut escontext as *mut ErrorSaveContext as *mut c_void);
                if escontext.error_occurred { /* should not happen */
                    RETURN_ERROR!(cxt, ereport!(ERROR,
                        errmsg!("time precision of jsonpath item method .{}() is invalid",
                            std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
                    ));
                }
                value = TimestampTzGetDatum(result);

                typmod = time_precision;
            }

            typid = TIMESTAMPTZOID;
        }

        _ => {
            elog!(ERROR, "unrecognized jsonpath item type: {}", (*jsp).type_ as i32);
        }
    }

    pfree(datetime as *mut c_void);

    if jperIsError(res) {
        return res;
    }

    hasNext = jspGetNext(jsp, elem.as_mut_ptr());

    if !hasNext && found.is_null() {
        return res;
    }

    jb = if hasNext { jbvbuf.as_mut_ptr() } else {
        palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue
    };

    (*jb).type_ = jbvDatetime;
    (*jb).val.datetime.value = value;
    (*jb).val.datetime.typid = typid;
    (*jb).val.datetime.typmod = typmod;
    (*jb).val.datetime.tz = tz;

    executeNextItem(cxt, jsp, elem.as_mut_ptr(), jb, found, hasNext)
}

/* timestamp2tm stub for use in jpiTimestampTz block */
unsafe fn timestamp2tm(
    dt: Timestamp,
    tzp: *mut c_int,
    tm: *mut pg_tm,
    fsec: *mut fsec_t,
    tzn: *mut *const c_char,
    attimezone: *mut c_void,
) -> c_int {
    /* TODO(pg-port): timestamp2tm from utils/adt/timestamp.c */
    unimplemented!("timestamp2tm")
}

/*
 * Implementation of .keyvalue() method.
 */
unsafe fn executeKeyValueMethod(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    found: *mut JsonValueList,
) -> JsonPathExecResult {
    let mut res: JsonPathExecResult = jperNotFound;
    let mut next = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let jbc: *mut JsonbContainer;
    let mut key = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let mut val = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let mut idval = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let mut keystr = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let mut valstr = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let mut idstr = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let mut it: *mut JsonbIterator;
    let mut tok: JsonbIteratorToken;
    let id: int64;
    let mut hasNext: bool;

    if JsonbType(jb) != jbvObject as c_int || (*jb).type_ != jbvBinary {
        RETURN_ERROR!(cxt, ereport!(ERROR,
            errmsg!("jsonpath item method .{}() can only be applied to an object",
                std::ffi::CStr::from_ptr(jspOperationName((*jsp).type_)).to_string_lossy())
            /* C also: errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND) */
        ));
    }

    jbc = (*jb).val.binary.data;

    if JsonContainerSize(jbc) == 0 {
        return jperNotFound; /* no key-value pairs */
    }

    hasNext = jspGetNext(jsp, next.as_mut_ptr());

    let keystr_ref = keystr.as_mut_ptr();
    (*keystr_ref).type_ = jbvString;
    (*keystr_ref).val.string.val = b"key\0".as_ptr() as *mut c_char;
    (*keystr_ref).val.string.len = 3;

    let valstr_ref = valstr.as_mut_ptr();
    (*valstr_ref).type_ = jbvString;
    (*valstr_ref).val.string.val = b"value\0".as_ptr() as *mut c_char;
    (*valstr_ref).val.string.len = 5;

    let idstr_ref = idstr.as_mut_ptr();
    (*idstr_ref).type_ = jbvString;
    (*idstr_ref).val.string.val = b"id\0".as_ptr() as *mut c_char;
    (*idstr_ref).val.string.len = 2;

    /* construct object id from its base object and offset inside that */
    id = if (*jb).type_ != jbvBinary { 0 }
         else { ((*jb).val.binary.data as *mut c_char)
                    .offset_from((*cxt).baseObject.jbc as *mut c_char) as int64 };
    let id = id + ((*cxt).baseObject.id as int64) * INT64CONST!(10000000000);

    let idval_ref = idval.as_mut_ptr();
    (*idval_ref).type_ = jbvNumeric;
    (*idval_ref).val.numeric = (int64_to_numeric(id)) as crate::utils::adt::jsonb_util::Numeric;

    it = JsonbIteratorInit(jbc);

    loop {
        tok = JsonbIteratorNext(&mut it, key.as_mut_ptr(), true);
        if tok == WJB_DONE { break; }

        let mut baseObject: JsonBaseObjectInfo;
        let mut obj = core::mem::MaybeUninit::<JsonbValue>::zeroed();
        let mut ps: *mut JsonbParseState = ptr::null_mut();
        let keyval: *mut JsonbValue;
        let jsonb: *mut Jsonb;

        if tok != WJB_KEY { continue; }

        res = jperOk;

        if !hasNext && found.is_null() { break; }

        let tok2 = JsonbIteratorNext(&mut it, val.as_mut_ptr(), true);
        debug_assert!(tok2 == WJB_VALUE);

        ps = ptr::null_mut();
        pushJsonbValue(&mut ps, WJB_BEGIN_OBJECT, ptr::null_mut());

        pushJsonbValue(&mut ps, WJB_KEY, keystr_ref);
        pushJsonbValue(&mut ps, WJB_VALUE, key.as_mut_ptr());

        pushJsonbValue(&mut ps, WJB_KEY, valstr_ref);
        pushJsonbValue(&mut ps, WJB_VALUE, val.as_mut_ptr());

        pushJsonbValue(&mut ps, WJB_KEY, idstr_ref);
        pushJsonbValue(&mut ps, WJB_VALUE, idval_ref);

        keyval = pushJsonbValue(&mut ps, WJB_END_OBJECT, ptr::null_mut());

        jsonb = JsonbValueToJsonb(keyval);

        JsonbInitBinary(obj.as_mut_ptr(), jsonb);

        baseObject = setBaseObject(cxt, obj.as_mut_ptr(),
                                   { let id = (*cxt).lastGeneratedObjectId; (*cxt).lastGeneratedObjectId += 1; id });

        res = executeNextItem(cxt, jsp, next.as_mut_ptr(), obj.as_mut_ptr(), found, true);

        (*cxt).baseObject = baseObject;

        if jperIsError(res) { return res; }
        if res == jperOk && found.is_null() { break; }
    }

    res
}

/*
 * Convert boolean execution status 'res' to a boolean JSON item and execute
 * next jsonpath.
 */
unsafe fn appendBoolResult(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    found: *mut JsonValueList,
    res: JsonPathBool,
) -> JsonPathExecResult {
    let mut next = core::mem::MaybeUninit::<JsonPathItem>::uninit();
    let mut jbv = core::mem::MaybeUninit::<JsonbValue>::zeroed();

    if !jspGetNext(jsp, next.as_mut_ptr()) && found.is_null() {
        return jperOk; /* found singleton boolean value */
    }

    if res == jpbUnknown {
        (*jbv.as_mut_ptr()).type_ = jbvNull;
    } else {
        (*jbv.as_mut_ptr()).type_ = jbvBool;
        (*jbv.as_mut_ptr()).val.boolean = res == jpbTrue;
    }

    executeNextItem(cxt, jsp, next.as_mut_ptr(), jbv.as_mut_ptr(), found, true)
}

/*
 * Convert jsonpath's scalar or variable node to actual jsonb value.
 */
unsafe fn getJsonPathItem(
    cxt: *mut JsonPathExecContext,
    item: *mut JsonPathItem,
    value: *mut JsonbValue,
) {
    match (*item).type_ {
        jpiNull => {
            (*value).type_ = jbvNull;
        }
        jpiBool => {
            (*value).type_ = jbvBool;
            (*value).val.boolean = jspGetBool(item);
        }
        jpiNumeric => {
            (*value).type_ = jbvNumeric;
            (*value).val.numeric = (jspGetNumeric(item)) as crate::utils::adt::jsonb_util::Numeric;
        }
        jpiString => {
            (*value).type_ = jbvString;
            (*value).val.string.val = jspGetString(item, &mut (*value).val.string.len);
        }
        jpiVariable => {
            getJsonPathVariable(cxt, item, value);
            return;
        }
        _ => {
            elog!(ERROR, "unexpected jsonpath item type");
        }
    }
}

/*
 * Returns the computed value of a JSON path variable with given name.
 */
unsafe fn GetJsonPathVar(
    cxt: *mut c_void,
    varName: *mut c_char,
    varNameLen: c_int,
    baseObject: *mut JsonbValue,
    baseObjectId: *mut c_int,
) -> *mut JsonbValue {
    let mut var: *mut JsonPathVariable = ptr::null_mut();
    let vars: *mut List = cxt as *mut List;
    let mut lc: *mut ListCell;
    let result: *mut JsonbValue;
    let mut id: c_int = 1;

    lc = list_head(vars);
    while !lc.is_null() {
        let curvar = lfirst(lc) as *mut JsonPathVariable;

        if (*curvar).namelen == varNameLen as usize
            && core::slice::from_raw_parts((*curvar).name as *const u8, varNameLen as usize)
               == core::slice::from_raw_parts(varName as *const u8, varNameLen as usize)
        {
            var = curvar;
            break;
        }

        id += 1;
        lc = lnext(vars, lc);
    }

    if var.is_null() {
        *baseObjectId = -1;
        return ptr::null_mut();
    }

    result = palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;
    if (*var).isnull {
        *baseObjectId = 0;
        (*result).type_ = jbvNull;
    } else {
        JsonItemFromDatum((*var).value, (*var).typid, (*var).typmod, result);
    }

    *baseObject = *result;
    *baseObjectId = id;

    result
}

unsafe fn CountJsonPathVars(cxt: *mut c_void) -> c_int {
    let vars = cxt as *mut List;
    list_length(vars)
}

/*
 * Initialize JsonbValue to pass to jsonpath executor from given
 * datum value of the specified type.
 */
unsafe fn JsonItemFromDatum(val: Datum, typid: Oid, typmod: int32, res: *mut JsonbValue) {
    match typid {
        BOOLOID => {
            (*res).type_ = jbvBool;
            (*res).val.boolean = DatumGetBool(val);
        }
        NUMERICOID => {
            JsonbValueInitNumericDatum(res, val);
        }
        INT2OID => {
            JsonbValueInitNumericDatum(res, DirectFunctionCall1!(int2_numeric, val));
        }
        INT4OID => {
            JsonbValueInitNumericDatum(res, DirectFunctionCall1!(int4_numeric, val));
        }
        INT8OID => {
            JsonbValueInitNumericDatum(res, DirectFunctionCall1!(int8_numeric, val));
        }
        FLOAT4OID => {
            JsonbValueInitNumericDatum(res, DirectFunctionCall1!(float4_numeric, val));
        }
        FLOAT8OID => {
            JsonbValueInitNumericDatum(res, DirectFunctionCall1!(float8_numeric, val));
        }
        TEXTOID | VARCHAROID => {
            (*res).type_ = jbvString;
            (*res).val.string.val = VARDATA_ANY(val as *const c_char) as *mut c_char;
            (*res).val.string.len = VARSIZE_ANY_EXHDR(val as *const c_char) as int32;
        }
        DATEOID | TIMEOID | TIMETZOID | TIMESTAMPOID | TIMESTAMPTZOID => {
            (*res).type_ = jbvDatetime;
            (*res).val.datetime.value = val;
            (*res).val.datetime.typid = typid;
            (*res).val.datetime.typmod = typmod;
            (*res).val.datetime.tz = 0;
        }
        JSONBOID => {
            let jbv = res;
            let jb: *mut Jsonb = DatumGetJsonbP(val);

            if JsonContainerIsScalar(&(*jb).root) {
                let result = JsonbExtractScalar(&(*jb).root as *const _ as *mut _, jbv);
                debug_assert!(result);
            } else {
                JsonbInitBinary(jbv, jb);
            }
        }
        JSONOID => {
            let txt = DatumGetPointer(val) as *mut text;
            let str_ = text_to_cstring(txt);
            let jb: *mut Jsonb;

            jb = DatumGetJsonbP(DirectFunctionCall1!(jsonb_in, CStringGetDatum(str_)));
            pfree(str_ as *mut c_void);

            JsonItemFromDatum(JsonbPGetDatum(jb), JSONBOID, -1, res);
        }
        _ => {
            ereport!(ERROR,
                errmsg!("could not convert value of type {} to jsonpath",
                    std::ffi::CStr::from_ptr(format_type_be(typid)).to_string_lossy())
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            );
        }
    }
}

/* Initialize numeric value from the given datum */
unsafe fn JsonbValueInitNumericDatum(jbv: *mut JsonbValue, num: Datum) {
    (*jbv).type_ = jbvNumeric;
    (*jbv).val.numeric = (DatumGetNumeric(num)) as crate::utils::adt::jsonb_util::Numeric;
}

/*
 * Get the value of variable passed to jsonpath executor
 */
unsafe fn getJsonPathVariable(
    cxt: *mut JsonPathExecContext,
    variable: *mut JsonPathItem,
    value: *mut JsonbValue,
) {
    let mut varName: *mut c_char;
    let mut varNameLength: c_int = 0;
    let mut baseObject = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let mut baseObjectId: c_int = 0;
    let mut v: *mut JsonbValue = ptr::null_mut();

    debug_assert!((*variable).type_ == jpiVariable);
    varName = jspGetString(variable, &mut varNameLength);

    if (*cxt).vars.is_null()
        || { v = (*cxt).getVar.unwrap()((*cxt).vars, varName, varNameLength,
                                        baseObject.as_mut_ptr(), &mut baseObjectId);
             v.is_null() }
    {
        ereport!(ERROR,
            errmsg!("could not find jsonpath variable \"{}\"",
                std::ffi::CStr::from_ptr(pnstrdup(varName, varNameLength as usize)).to_string_lossy())
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        );
    }

    if baseObjectId > 0 {
        *value = *v;
        setBaseObject(cxt, baseObject.as_mut_ptr(), baseObjectId);
    }
}

/*
 * Definition of JsonPathGetVarCallback for when JsonPathExecContext.vars
 * is specified as a jsonb value.
 */
unsafe fn getJsonPathVariableFromJsonb(
    varsJsonb: *mut c_void,
    varName: *mut c_char,
    varNameLength: c_int,
    baseObject: *mut JsonbValue,
    baseObjectId: *mut c_int,
) -> *mut JsonbValue {
    let vars = varsJsonb as *mut Jsonb;
    let mut tmp = core::mem::MaybeUninit::<JsonbValue>::zeroed();
    let result: *mut JsonbValue;

    (*tmp.as_mut_ptr()).type_ = jbvString;
    (*tmp.as_mut_ptr()).val.string.val = varName;
    (*tmp.as_mut_ptr()).val.string.len = varNameLength;

    result = findJsonbValueFromContainer(&mut (*vars).root, JB_FOBJECT, tmp.as_mut_ptr());

    if result.is_null() {
        *baseObjectId = -1;
        return ptr::null_mut();
    }

    *baseObjectId = 1;
    JsonbInitBinary(baseObject, vars);

    result
}

/*
 * Definition of JsonPathCountVarsCallback for when JsonPathExecContext.vars
 * is specified as a jsonb value.
 */
unsafe fn countVariablesFromJsonb(varsJsonb: *mut c_void) -> c_int {
    let vars = varsJsonb as *mut Jsonb;

    if !vars.is_null() && !JsonContainerIsObject(&(*vars).root) {
        ereport!(ERROR,
            errmsg!("\"vars\" argument is not an object")
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             *         errdetail("Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.") */
        );
    }

    /* count of base objects */
    if !vars.is_null() { 1 } else { 0 }
}

/**************** Support functions for JsonPath execution *****************/

/*
 * Returns the size of an array item, or -1 if item is not an array.
 */
unsafe fn JsonbArraySize(jb: *mut JsonbValue) -> c_int {
    debug_assert!((*jb).type_ != jbvArray);

    if (*jb).type_ == jbvBinary {
        let jbc: *mut JsonbContainer = (*jb).val.binary.data;

        if JsonContainerIsArray(jbc) && !JsonContainerIsScalar(jbc) {
            return JsonContainerSize(jbc) as c_int;
        }
    }

    -1
}

/* Comparison predicate callback. */
unsafe fn executeComparison(
    cmp: *mut JsonPathItem,
    lv: *mut JsonbValue,
    rv: *mut JsonbValue,
    p: *mut c_void,
) -> JsonPathBool {
    let cxt = p as *mut JsonPathExecContext;
    compareItems((*cmp).type_, lv, rv, (*cxt).useTz)
}

/*
 * Perform per-byte comparison of two strings.
 */
unsafe fn binaryCompareStrings(
    s1: *const c_char, len1: c_int,
    s2: *const c_char, len2: c_int,
) -> c_int {
    let cmp: c_int;

    cmp = libc_memcmp(s1 as *const u8, s2 as *const u8,
                      if len1 < len2 { len1 } else { len2 } as usize);

    if cmp != 0 { return cmp; }
    if len1 == len2 { return 0; }
    if len1 < len2 { -1 } else { 1 }
}

#[inline]
unsafe fn libc_memcmp(s1: *const u8, s2: *const u8, n: usize) -> c_int {
    let s1 = core::slice::from_raw_parts(s1, n);
    let s2 = core::slice::from_raw_parts(s2, n);
    for i in 0..n {
        if s1[i] < s2[i] { return -1; }
        if s1[i] > s2[i] { return 1; }
    }
    0
}

/*
 * Compare two strings in the current server encoding using Unicode codepoint
 * collation.
 */
unsafe fn compareStrings(
    mbstr1: *const c_char, mblen1: c_int,
    mbstr2: *const c_char, mblen2: c_int,
) -> c_int {
    const PG_SQL_ASCII: c_int = 0;
    const PG_UTF8: c_int = 6;

    if GetDatabaseEncoding() == PG_SQL_ASCII || GetDatabaseEncoding() == PG_UTF8 {
        /*
         * It's known property of UTF-8 strings that their per-byte comparison
         * result matches codepoints comparison result.
         */
        return binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
    } else {
        let utf8str1: *mut c_char;
        let utf8str2: *mut c_char;
        let cmp: c_int;
        let utf8len1: c_int;
        let utf8len2: c_int;

        /*
         * We have to convert other encodings to UTF-8 first, then compare.
         */
        utf8str1 = pg_server_to_any(mbstr1, mblen1, PG_UTF8);
        utf8str2 = pg_server_to_any(mbstr2, mblen2, PG_UTF8);
        utf8len1 = if mbstr1 == utf8str1 { mblen1 } else {
            libc_strlen(utf8str1 as *const u8) as c_int
        };
        utf8len2 = if mbstr2 == utf8str2 { mblen2 } else {
            libc_strlen(utf8str2 as *const u8) as c_int
        };

        cmp = binaryCompareStrings(utf8str1, utf8len1, utf8str2, utf8len2);

        /*
         * If pg_server_to_any() did no real conversion, then we actually
         * compared original strings.
         */
        if mbstr1 == utf8str1 && mbstr2 == utf8str2 {
            return cmp;
        }

        /* Free memory if needed */
        if mbstr1 != utf8str1 { pfree(utf8str1 as *mut c_void); }
        if mbstr2 != utf8str2 { pfree(utf8str2 as *mut c_void); }

        /*
         * When all Unicode codepoints are equal, return result of binary
         * comparison.
         */
        if cmp == 0 {
            binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2)
        } else {
            cmp
        }
    }
}

/*
 * Compare two SQL/JSON items using comparison operation 'op'.
 */
unsafe fn compareItems(
    op: JsonPathItemType,
    jb1: *mut JsonbValue,
    jb2: *mut JsonbValue,
    useTz: bool,
) -> JsonPathBool {
    let mut cmp: c_int;
    let mut res: bool;

    if (*jb1).type_ != (*jb2).type_ {
        if (*jb1).type_ == jbvNull || (*jb2).type_ == jbvNull {
            /*
             * Equality and order comparison of nulls to non-nulls returns
             * always false, but inequality comparison returns true.
             */
            return if op == jpiNotEqual { jpbTrue } else { jpbFalse };
        }

        /* Non-null items of different types are not comparable. */
        return jpbUnknown;
    }

    match (*jb1).type_ {
        jbvNull => {
            cmp = 0;
        }
        jbvBool => {
            cmp = if (*jb1).val.boolean == (*jb2).val.boolean { 0 }
                  else if (*jb1).val.boolean { 1 } else { -1 };
        }
        jbvNumeric => {
            cmp = compareNumeric(((*jb1).val.numeric as Numeric), ((*jb2).val.numeric as Numeric));
        }
        jbvString => {
            if op == jpiEqual {
                return if (*jb1).val.string.len != (*jb2).val.string.len
                    || libc_memcmp((*jb1).val.string.val as *const u8,
                                   (*jb2).val.string.val as *const u8,
                                   (*jb1).val.string.len as usize) != 0
                { jpbFalse } else { jpbTrue };
            }

            cmp = compareStrings((*jb1).val.string.val, (*jb1).val.string.len,
                                  (*jb2).val.string.val, (*jb2).val.string.len);
        }
        jbvDatetime => {
            let mut cast_error: bool = false;

            cmp = compareDatetime((*jb1).val.datetime.value,
                                  (*jb1).val.datetime.typid,
                                  (*jb2).val.datetime.value,
                                  (*jb2).val.datetime.typid,
                                  useTz,
                                  &mut cast_error);

            if cast_error {
                return jpbUnknown;
            }
        }
        jbvBinary | jbvArray | jbvObject => {
            return jpbUnknown; /* non-scalars are not comparable */
        }
        _ => {
            elog!(ERROR, "invalid jsonb value type {}", (*jb1).type_ as i32);
            return jpbUnknown;
        }
    }

    match op {
        jpiEqual        => { res = cmp == 0; }
        jpiNotEqual     => { res = cmp != 0; }
        jpiLess         => { res = cmp < 0; }
        jpiGreater      => { res = cmp > 0; }
        jpiLessOrEqual  => { res = cmp <= 0; }
        jpiGreaterOrEqual => { res = cmp >= 0; }
        _ => {
            elog!(ERROR, "unrecognized jsonpath operation: {}", op as i32);
            return jpbUnknown;
        }
    }

    if res { jpbTrue } else { jpbFalse }
}

/* Compare two numerics */
unsafe fn compareNumeric(a: Numeric, b: Numeric) -> c_int {
    DatumGetInt32(DirectFunctionCall2!(numeric_cmp,
                                      NumericGetDatum(a),
                                      NumericGetDatum(b)))
}

unsafe fn copyJsonbValue(src: *mut JsonbValue) -> *mut JsonbValue {
    let dst: *mut JsonbValue =
        palloc(core::mem::size_of::<JsonbValue>()) as *mut JsonbValue;
    *dst = *src;
    dst
}

/*
 * Execute array subscript expression and convert resulting numeric item to
 * the integer type with truncation.
 */
unsafe fn getArrayIndex(
    cxt: *mut JsonPathExecContext,
    jsp: *mut JsonPathItem,
    jb: *mut JsonbValue,
    index: *mut int32,
) -> JsonPathExecResult {
    let mut jbv: *mut JsonbValue;
    let mut found = JsonValueList::new();
    let res: JsonPathExecResult = executeItem(cxt, jsp, jb, &mut found);
    let mut numeric_index: Datum;
    let mut have_error: bool = false;

    if jperIsError(res) { return res; }

    jbv = getScalar(JsonValueListHead(&mut found), jbvNumeric);
    if JsonValueListLength(&found) != 1 || jbv.is_null() {
        RETURN_ERROR!(cxt, ereport!(ERROR,
            errmsg!("jsonpath array subscript is not a single numeric value")
            /* C also: errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT) */
        ));
    }

    numeric_index = DirectFunctionCall2!(numeric_trunc,
                                        NumericGetDatum(((*jbv).val.numeric as Numeric)),
                                        Int32GetDatum(0));

    *index = numeric_int4_opt_error(DatumGetNumeric(numeric_index), &mut have_error);

    if have_error {
        RETURN_ERROR!(cxt, ereport!(ERROR,
            errmsg!("jsonpath array subscript is out of integer range")
            /* C also: errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT) */
        ));
    }

    jperOk
}

/* Save base object and its id needed for the execution of .keyvalue(). */
unsafe fn setBaseObject(
    cxt: *mut JsonPathExecContext,
    jbv: *mut JsonbValue,
    id: int32,
) -> JsonBaseObjectInfo {
    let baseObject: JsonBaseObjectInfo = (*cxt).baseObject;

    (*cxt).baseObject.jbc = if (*jbv).type_ != jbvBinary { ptr::null_mut() }
                             else { (*jbv).val.binary.data as *mut JsonbContainer };
    (*cxt).baseObject.id = id;

    baseObject
}

unsafe fn JsonValueListClear(jvl: *mut JsonValueList) {
    (*jvl).singleton = ptr::null_mut();
    (*jvl).list = ptr::null_mut();
}

unsafe fn JsonValueListAppend(jvl: *mut JsonValueList, jbv: *mut JsonbValue) {
    if !(*jvl).singleton.is_null() {
        (*jvl).list = list_make2!((*jvl).singleton as *mut c_void,
                                  jbv as *mut c_void) as *mut List;
        (*jvl).singleton = ptr::null_mut();
    } else if (*jvl).list.is_null() {
        (*jvl).singleton = jbv;
    } else {
        (*jvl).list = lappend((*jvl).list, jbv as *mut c_void);
    }
}

unsafe fn JsonValueListLength(jvl: *const JsonValueList) -> c_int {
    if !(*jvl).singleton.is_null() { 1 } else { list_length((*jvl).list) }
}

unsafe fn JsonValueListIsEmpty(jvl: *mut JsonValueList) -> bool {
    (*jvl).singleton.is_null() && (*jvl).list.is_null()
}

unsafe fn JsonValueListHead(jvl: *mut JsonValueList) -> *mut JsonbValue {
    if !(*jvl).singleton.is_null() {
        (*jvl).singleton
    } else {
        linitial((*jvl).list) as *mut JsonbValue
    }
}

unsafe fn JsonValueListGetList(jvl: *mut JsonValueList) -> *mut List {
    if !(*jvl).singleton.is_null() {
        list_make1!((*jvl).singleton as *mut c_void) as *mut List
    } else {
        (*jvl).list
    }
}

unsafe fn JsonValueListInitIterator(jvl: *const JsonValueList, it: *mut JsonValueListIterator) {
    if !(*jvl).singleton.is_null() {
        (*it).value = (*jvl).singleton;
        (*it).list = ptr::null_mut();
        (*it).next = ptr::null_mut();
    } else if !(*jvl).list.is_null() {
        (*it).value = linitial((*jvl).list) as *mut JsonbValue;
        (*it).list = (*jvl).list;
        (*it).next = list_second_cell((*jvl).list);
    } else {
        (*it).value = ptr::null_mut();
        (*it).list = ptr::null_mut();
        (*it).next = ptr::null_mut();
    }
}

/*
 * Get the next item from the sequence advancing iterator.
 */
unsafe fn JsonValueListNext(
    jvl: *const JsonValueList,
    it: *mut JsonValueListIterator,
) -> *mut JsonbValue {
    let result: *mut JsonbValue = (*it).value;

    if !(*it).next.is_null() {
        (*it).value = lfirst((*it).next) as *mut JsonbValue;
        (*it).next = lnext((*it).list, (*it).next);
    } else {
        (*it).value = ptr::null_mut();
    }

    result
}

/*
 * Initialize a binary JsonbValue with the given jsonb container.
 */
unsafe fn JsonbInitBinary(jbv: *mut JsonbValue, jb: *mut Jsonb) -> *mut JsonbValue {
    (*jbv).type_ = jbvBinary;
    (*jbv).val.binary.data = &mut (*jb).root as *mut JsonbContainer;
    (*jbv).val.binary.len = VARSIZE_ANY_EXHDR(jb as *const c_char) as int32;
    jbv
}

/*
 * Returns jbv* type of JsonbValue. Note, it never returns jbvBinary as is.
 */
unsafe fn JsonbType(jb: *mut JsonbValue) -> c_int {
    let mut type_: c_int = (*jb).type_ as c_int;

    if (*jb).type_ == jbvBinary {
        let jbc: *mut JsonbContainer = (*jb).val.binary.data;

        /* Scalars should be always extracted during jsonpath execution. */
        debug_assert!(!JsonContainerIsScalar(jbc));

        if JsonContainerIsObject(jbc) {
            type_ = jbvObject as c_int;
        } else if JsonContainerIsArray(jbc) {
            type_ = jbvArray as c_int;
        } else {
            elog!(ERROR, "invalid jsonb container type: {:#010x}", (*jbc).header);
        }
    }

    type_
}

/* Get scalar of given type or NULL on type mismatch */
unsafe fn getScalar(scalar: *mut JsonbValue, type_: jbvType) -> *mut JsonbValue {
    /* Scalars should be always extracted during jsonpath execution. */
    debug_assert!((*scalar).type_ != jbvBinary
        || !JsonContainerIsScalar((*scalar).val.binary.data));

    if (*scalar).type_ == type_ { scalar } else { ptr::null_mut() }
}

/* Construct a JSON array from the item list */
unsafe fn wrapItemsInArray(items: *const JsonValueList) -> *mut JsonbValue {
    let mut ps: *mut JsonbParseState = ptr::null_mut();
    let mut it = JsonValueListIterator {
        value: ptr::null_mut(),
        list: ptr::null_mut(),
        next: ptr::null_mut(),
    };
    let mut jbv: *mut JsonbValue;

    pushJsonbValue(&mut ps, WJB_BEGIN_ARRAY, ptr::null_mut());

    JsonValueListInitIterator(items, &mut it);
    while { jbv = JsonValueListNext(items, &mut it); !jbv.is_null() } {
        pushJsonbValue(&mut ps, WJB_ELEM, jbv);
    }

    pushJsonbValue(&mut ps, WJB_END_ARRAY, ptr::null_mut())
}

/* Check if the timezone required for casting from type1 to type2 is used */
unsafe fn checkTimezoneIsUsedForCast(
    useTz: bool,
    type1: *const c_char,
    type2: *const c_char,
) {
    if !useTz {
        ereport!(ERROR,
            errmsg!("cannot convert value from {} to {} without time zone usage",
                std::ffi::CStr::from_ptr(type1).to_string_lossy(),
                std::ffi::CStr::from_ptr(type2).to_string_lossy())
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             *         errhint("Use *_tz() function for time zone support.") */
        );
    }
}

/* Convert time datum to timetz datum */
unsafe fn castTimeToTimeTz(time: Datum, useTz: bool) -> Datum {
    checkTimezoneIsUsedForCast(useTz, b"time\0".as_ptr() as *const c_char,
                               b"timetz\0".as_ptr() as *const c_char);
    DirectFunctionCall1!(time_timetz, time)
}

/*
 * Compare date to timestamp.
 */
unsafe fn cmpDateToTimestamp(date1: DateADT, ts2: Timestamp, useTz: bool) -> c_int {
    date_cmp_timestamp_internal(date1, ts2)
}

/*
 * Compare date to timestamptz.
 */
unsafe fn cmpDateToTimestampTz(date1: DateADT, tstz2: TimestampTz, useTz: bool) -> c_int {
    checkTimezoneIsUsedForCast(useTz, b"date\0".as_ptr() as *const c_char,
                               b"timestamptz\0".as_ptr() as *const c_char);
    date_cmp_timestamptz_internal(date1, tstz2)
}

/*
 * Compare timestamp to timestamptz.
 */
unsafe fn cmpTimestampToTimestampTz(ts1: Timestamp, tstz2: TimestampTz, useTz: bool) -> c_int {
    checkTimezoneIsUsedForCast(useTz, b"timestamp\0".as_ptr() as *const c_char,
                               b"timestamptz\0".as_ptr() as *const c_char);
    timestamp_cmp_timestamptz_internal(ts1, tstz2)
}

/*
 * Cross-type comparison of two datetime SQL/JSON items.
 */
unsafe fn compareDatetime(
    val1: Datum, typid1: Oid,
    val2: Datum, typid2: Oid,
    useTz: bool,
    cast_error: *mut bool,
) -> c_int {
    let cmpfunc: Option<PGFunction>;

    *cast_error = false;

    match typid1 {
        DATEOID => match typid2 {
            DATEOID => { cmpfunc = Some(date_cmp); }
            TIMESTAMPOID => {
                return cmpDateToTimestamp(DatumGetDateADT(val1),
                                          DatumGetTimestamp(val2), useTz);
            }
            TIMESTAMPTZOID => {
                return cmpDateToTimestampTz(DatumGetDateADT(val1),
                                             DatumGetTimestampTz(val2), useTz);
            }
            TIMEOID | TIMETZOID => {
                *cast_error = true; /* uncomparable types */
                return 0;
            }
            _ => {
                elog!(ERROR, "unrecognized SQL/JSON datetime type oid: {}", typid2);
                return 0;
            }
        },

        TIMEOID => match typid2 {
            TIMEOID => { cmpfunc = Some(time_cmp); }
            TIMETZOID => {
                let val1_tz = castTimeToTimeTz(val1, useTz);
                return DatumGetInt32(DirectFunctionCall2!(timetz_cmp, val1_tz, val2));
            }
            DATEOID | TIMESTAMPOID | TIMESTAMPTZOID => {
                *cast_error = true;
                return 0;
            }
            _ => {
                elog!(ERROR, "unrecognized SQL/JSON datetime type oid: {}", typid2);
                return 0;
            }
        },

        TIMETZOID => match typid2 {
            TIMEOID => {
                let val2_tz = castTimeToTimeTz(val2, useTz);
                return DatumGetInt32(DirectFunctionCall2!(timetz_cmp, val1, val2_tz));
            }
            TIMETZOID => { cmpfunc = Some(timetz_cmp); }
            DATEOID | TIMESTAMPOID | TIMESTAMPTZOID => {
                *cast_error = true;
                return 0;
            }
            _ => {
                elog!(ERROR, "unrecognized SQL/JSON datetime type oid: {}", typid2);
                return 0;
            }
        },

        TIMESTAMPOID => match typid2 {
            DATEOID => {
                return -cmpDateToTimestamp(DatumGetDateADT(val2),
                                           DatumGetTimestamp(val1), useTz);
            }
            TIMESTAMPOID => { cmpfunc = Some(timestamp_cmp); }
            TIMESTAMPTZOID => {
                return cmpTimestampToTimestampTz(DatumGetTimestamp(val1),
                                                 DatumGetTimestampTz(val2), useTz);
            }
            TIMEOID | TIMETZOID => {
                *cast_error = true;
                return 0;
            }
            _ => {
                elog!(ERROR, "unrecognized SQL/JSON datetime type oid: {}", typid2);
                return 0;
            }
        },

        TIMESTAMPTZOID => match typid2 {
            DATEOID => {
                return -cmpDateToTimestampTz(DatumGetDateADT(val2),
                                              DatumGetTimestampTz(val1), useTz);
            }
            TIMESTAMPOID => {
                return -cmpTimestampToTimestampTz(DatumGetTimestamp(val2),
                                                   DatumGetTimestampTz(val1), useTz);
            }
            TIMESTAMPTZOID => { cmpfunc = Some(timestamp_cmp); }
            TIMEOID | TIMETZOID => {
                *cast_error = true;
                return 0;
            }
            _ => {
                elog!(ERROR, "unrecognized SQL/JSON datetime type oid: {}", typid2);
                return 0;
            }
        },

        _ => {
            elog!(ERROR, "unrecognized SQL/JSON datetime type oid: {}", typid1);
            return 0;
        }
    }

    if *cast_error {
        return 0; /* cast error */
    }

    DatumGetInt32(DirectFunctionCall2!(cmpfunc.unwrap(), val1, val2))
}

/************************ Executor-callable functions ************************/

/*
 * Executor-callable JSON_EXISTS implementation
 */
pub unsafe fn JsonPathExists(
    jb: Datum,
    jp: *mut JsonPath,
    error: *mut bool,
    vars: *mut List,
) -> bool {
    let res: JsonPathExecResult;

    res = executeJsonPath(jp, vars as *mut c_void,
                          Some(GetJsonPathVar_cb),
                          Some(CountJsonPathVars_cb),
                          DatumGetJsonbP(jb), error.is_null(), ptr::null_mut(), true);

    debug_assert!(!error.is_null() || !jperIsError(res));

    if !error.is_null() && jperIsError(res) {
        *error = true;
    }

    res == jperOk
}

/*
 * Executor-callable JSON_QUERY implementation
 */
pub unsafe fn JsonPathQuery(
    jb: Datum,
    jp: *mut JsonPath,
    wrapper: JsonWrapper,
    empty: *mut bool,
    error: *mut bool,
    vars: *mut List,
    column_name: *const c_char,
) -> Datum {
    let mut singleton: *mut JsonbValue;
    let wrap: bool;
    let mut found = JsonValueList::new();
    let res: JsonPathExecResult;
    let count: c_int;

    res = executeJsonPath(jp, vars as *mut c_void,
                          Some(GetJsonPathVar_cb),
                          Some(CountJsonPathVars_cb),
                          DatumGetJsonbP(jb), error.is_null(), &mut found, true);
    debug_assert!(!error.is_null() || !jperIsError(res));
    if !error.is_null() && jperIsError(res) {
        *error = true;
        *empty = false;
        return 0 as Datum;
    }

    /*
     * Determine whether to wrap the result in a JSON array or not.
     */
    count = JsonValueListLength(&found);
    singleton = if count > 0 { JsonValueListHead(&mut found) } else { ptr::null_mut() };
    if singleton.is_null() {
        wrap = false;
    } else if wrapper == JSW_NONE || wrapper == JSW_UNSPEC {
        wrap = false;
    } else if wrapper == JSW_UNCONDITIONAL {
        wrap = true;
    } else if wrapper == JSW_CONDITIONAL {
        wrap = count > 1;
    } else {
        elog!(ERROR, "unrecognized json wrapper {}", wrapper as i32);
        wrap = false;
    }

    if wrap {
        return JsonbPGetDatum(JsonbValueToJsonb(wrapItemsInArray(&found)));
    }

    /* No wrapping means only one item is expected. */
    if count > 1 {
        if !error.is_null() {
            *error = true;
            return 0 as Datum;
        }

        if !column_name.is_null() {
            ereport!(ERROR,
                errmsg!("JSON path expression for column \"{}\" must return single item when no wrapper is requested",
                    std::ffi::CStr::from_ptr(column_name).to_string_lossy())
                /* C also: errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
                 *         errhint("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.") */
            );
        } else {
            ereport!(ERROR,
                errmsg!("JSON path expression in JSON_QUERY must return single item when no wrapper is requested")
                /* C also: errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
                 *         errhint("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.") */
            );
        }
    }

    if !singleton.is_null() {
        return JsonbPGetDatum(JsonbValueToJsonb(singleton));
    }

    *empty = true;
    PointerGetDatum(ptr::null())
}

/*
 * Executor-callable JSON_VALUE implementation
 */
pub unsafe fn JsonPathValue(
    jb: Datum,
    jp: *mut JsonPath,
    empty: *mut bool,
    error: *mut bool,
    vars: *mut List,
    column_name: *const c_char,
) -> *mut JsonbValue {
    let mut res: *mut JsonbValue;
    let mut found = JsonValueList::new();
    let jper: JsonPathExecResult;
    let count: c_int;

    jper = executeJsonPath(jp, vars as *mut c_void,
                           Some(GetJsonPathVar_cb),
                           Some(CountJsonPathVars_cb),
                           DatumGetJsonbP(jb), error.is_null(), &mut found, true);

    debug_assert!(!error.is_null() || !jperIsError(jper));

    if !error.is_null() && jperIsError(jper) {
        *error = true;
        *empty = false;
        return ptr::null_mut();
    }

    count = JsonValueListLength(&found);

    *empty = count == 0;

    if *empty {
        return ptr::null_mut();
    }

    /* JSON_VALUE expects to get only singletons. */
    if count > 1 {
        if !error.is_null() {
            *error = true;
            return ptr::null_mut();
        }

        if !column_name.is_null() {
            ereport!(ERROR,
                errmsg!("JSON path expression for column \"{}\" must return single scalar item",
                    std::ffi::CStr::from_ptr(column_name).to_string_lossy())
                /* C also: errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM) */
            );
        } else {
            ereport!(ERROR,
                errmsg!("JSON path expression in JSON_VALUE must return single scalar item")
            );
        }
    }

    res = JsonValueListHead(&mut found);
    if (*res).type_ == jbvBinary
        && JsonContainerIsScalar((*res).val.binary.data)
    {
        JsonbExtractScalar((*res).val.binary.data, res);
    }

    /* JSON_VALUE expects to get only scalars. */
    if !IsAJsonbScalar(res) {
        if !error.is_null() {
            *error = true;
            return ptr::null_mut();
        }

        if !column_name.is_null() {
            ereport!(ERROR,
                errmsg!("JSON path expression for column \"{}\" must return single scalar item",
                    std::ffi::CStr::from_ptr(column_name).to_string_lossy())
                /* C also: errcode(ERRCODE_SQL_JSON_SCALAR_REQUIRED) */
            );
        } else {
            ereport!(ERROR,
                errmsg!("JSON path expression in JSON_VALUE must return single scalar item")
            );
        }
    }

    if (*res).type_ == jbvNull {
        return ptr::null_mut();
    }

    res
}

/************************ JSON_TABLE functions ***************************/

/*
 * Sanity-checks and returns the opaque JsonTableExecContext from the
 * given executor state struct.
 */
#[inline]
unsafe fn GetJsonTableExecContext(
    state: *mut TableFuncScanState,
    fname: *const c_char,
) -> *mut JsonTableExecContext {
    let result: *mut JsonTableExecContext;

    if !crate::IsA!(state, T_TableFuncScanState) {
        elog!(ERROR, "{} called with invalid TableFuncScanState",
            std::ffi::CStr::from_ptr(fname).to_string_lossy());
    }
    result = (*state).opaque as *mut JsonTableExecContext;
    if (*result).magic != JSON_TABLE_EXEC_CONTEXT_MAGIC {
        elog!(ERROR, "{} called with invalid TableFuncScanState",
            std::ffi::CStr::from_ptr(fname).to_string_lossy());
    }

    result
}

/*
 * JsonTableInitOpaque
 *      Fill in TableFuncScanState->opaque for processing JSON_TABLE
 */
unsafe extern "C" fn JsonTableInitOpaque(state: *mut TableFuncScanState, natts: c_int) {
    let cxt: *mut JsonTableExecContext;
    let ps = &mut (*state).ss.ps as *mut crate::nodes::execnodes::PlanState;
    let tfs = castNode!(crate::nodes::plannodes::TableFuncScan,
                        T_TableFuncScan, (*ps).plan)
              as *mut crate::nodes::plannodes::TableFuncScan;
    let tf: *mut TableFunc = (*tfs).tablefunc;
    let rootplan = (*tf).plan as *mut JsonTablePlan;
    let je = castNode!(JsonExpr, T_JsonExpr, (*tf).docexpr)
             as *mut JsonExpr;
    let mut args: *mut List = NIL as *mut List;

    cxt = palloc0(core::mem::size_of::<JsonTableExecContext>()) as *mut JsonTableExecContext;
    (*cxt).magic = JSON_TABLE_EXEC_CONTEXT_MAGIC;

    /*
     * Evaluate JSON_TABLE() PASSING arguments to be passed to the jsonpath
     * executor via JsonPathVariables.
     */
    if !(*state).passingvalexprs.is_null() {
        let mut exprlc: *mut ListCell = list_head((*state).passingvalexprs);
        let mut namelc: *mut ListCell = list_head((*je).passing_names);

        debug_assert!(list_length((*state).passingvalexprs)
                      == list_length((*je).passing_names));
        while !exprlc.is_null() {
            let estate = lfirst(exprlc) as *mut ExprState;
            let name = lfirst(namelc) as *mut crate::nodes::value::String;
            let var: *mut JsonPathVariable =
                palloc(core::mem::size_of::<JsonPathVariable>()) as *mut JsonPathVariable;

            (*var).name = pstrdup((*name).sval);
            (*var).namelen = libc_strlen((*var).name as *const u8);
            (*var).typid = exprType((*estate).expr as *const c_void);
            (*var).typmod = exprTypmod((*estate).expr as *const c_void);

            /*
             * Evaluate the expression and save the value to be returned by
             * GetJsonPathVar().
             */
            (*var).value = ExecEvalExpr(estate, (*ps).ps_ExprContext, &mut (*var).isnull);

            args = lappend(args, var as *mut c_void);

            exprlc = lnext((*state).passingvalexprs, exprlc);
            namelc = lnext((*je).passing_names, namelc);
        }
    }

    (*cxt).colplanstates = palloc(
        core::mem::size_of::<*mut JsonTablePlanState>()
            * list_length((*tf).colvalexprs) as usize,
    ) as *mut *mut JsonTablePlanState;

    /*
     * Initialize plan for the root path and, recursively, also any child
     * plans that compute the NESTED paths.
     */
    (*cxt).rootplanstate = JsonTableInitPlan(cxt, rootplan, ptr::null_mut(),
                                              args, CurrentMemoryContext);

    (*state).opaque = cxt as *mut c_void;
}

/*
 * JsonTableDestroyOpaque
 */
unsafe extern "C" fn JsonTableDestroyOpaque(state: *mut TableFuncScanState) {
    let cxt: *mut JsonTableExecContext =
        GetJsonTableExecContext(state, b"JsonTableDestroyOpaque\0".as_ptr() as *const c_char);

    /* not valid anymore */
    (*cxt).magic = 0;

    (*state).opaque = ptr::null_mut();
}

/*
 * JsonTableInitPlan
 */
unsafe fn JsonTableInitPlan(
    cxt: *mut JsonTableExecContext,
    plan: *mut JsonTablePlan,
    parentstate: *mut JsonTablePlanState,
    args: *mut List,
    mcxt: MemoryContext,
) -> *mut JsonTablePlanState {
    let planstate: *mut JsonTablePlanState =
        palloc0(core::mem::size_of::<JsonTablePlanState>()) as *mut JsonTablePlanState;

    (*planstate).plan = plan;
    (*planstate).parent = parentstate;

    if crate::IsA!(plan, T_JsonTablePathScan) {
        let scan = plan as *mut JsonTablePathScan;
        let mut i: c_int;

        (*planstate).path = DatumGetJsonPathP((*(*(*scan).path).value).constvalue);
        (*planstate).args = args;
        (*planstate).mcxt = AllocSetContextCreate(mcxt,
                                                   b"JsonTableExecContext\0".as_ptr() as *const c_char,
                                                   ALLOCSET_DEFAULT_SIZES);

        /* No row pattern evaluated yet. */
        (*planstate).current.value = PointerGetDatum(ptr::null());
        (*planstate).current.isnull = true;

        i = (*scan).colMin;
        while i >= 0 && i <= (*scan).colMax {
            *(*cxt).colplanstates.add(i as usize) = planstate;
            i += 1;
        }

        (*planstate).nested = if !(*scan).child.is_null() {
            JsonTableInitPlan(cxt, (*scan).child, planstate, args, mcxt)
        } else {
            ptr::null_mut()
        };
    } else if crate::IsA!(plan, T_JsonTableSiblingJoin) {
        let join = plan as *mut JsonTableSiblingJoin;

        (*planstate).left = JsonTableInitPlan(cxt, (*join).lplan, parentstate,
                                               args, mcxt);
        (*planstate).right = JsonTableInitPlan(cxt, (*join).rplan, parentstate,
                                                args, mcxt);
    }

    planstate
}

/*
 * JsonTableSetDocument
 */
unsafe extern "C" fn JsonTableSetDocument(state: *mut TableFuncScanState, value: Datum) {
    let cxt: *mut JsonTableExecContext =
        GetJsonTableExecContext(state, b"JsonTableSetDocument\0".as_ptr() as *const c_char);

    JsonTableResetRowPattern((*cxt).rootplanstate, value);
}

/*
 * Evaluate a JsonTablePlan's jsonpath to get a new row pattern from
 * the given context item
 */
unsafe fn JsonTableResetRowPattern(planstate: *mut JsonTablePlanState, item: Datum) {
    let scan = castNode!(JsonTablePathScan, T_JsonTablePathScan,
                         (*planstate).plan) as *mut JsonTablePathScan;
    let oldcxt: MemoryContext;
    let mut res: JsonPathExecResult;
    let js: *mut Jsonb = DatumGetJsonbP(item);

    JsonValueListClear(&mut (*planstate).found);

    MemoryContextResetOnly((*planstate).mcxt);

    oldcxt = MemoryContextSwitchTo((*planstate).mcxt);

    res = executeJsonPath((*planstate).path, (*planstate).args as *mut c_void,
                          Some(GetJsonPathVar_cb),
                          Some(CountJsonPathVars_cb),
                          js, (*scan).errorOnError,
                          &mut (*planstate).found,
                          true);

    MemoryContextSwitchTo(oldcxt);

    if jperIsError(res) {
        debug_assert!(!(*scan).errorOnError);
        JsonValueListClear(&mut (*planstate).found);
    }

    /* Reset plan iterator to the beginning of the item list */
    JsonValueListInitIterator(&(*planstate).found, &mut (*planstate).iter);
    (*planstate).current.value = PointerGetDatum(ptr::null());
    (*planstate).current.isnull = true;
    (*planstate).ordinal = 0;
}

/*
 * Fetch next row from a JsonTablePlan.
 */
unsafe fn JsonTablePlanNextRow(planstate: *mut JsonTablePlanState) -> bool {
    if crate::IsA!((*planstate).plan, T_JsonTablePathScan) {
        return JsonTablePlanScanNextRow(planstate);
    } else if crate::IsA!((*planstate).plan, T_JsonTableSiblingJoin) {
        return JsonTablePlanJoinNextRow(planstate);
    } else {
        elog!(ERROR, "invalid JsonTablePlan {}", (*(*planstate).plan).r#type as i32);
    }

    debug_assert!(false);
    false /* appease compiler */
}

/*
 * Fetch next row from a JsonTablePlan's path evaluation result and from
 * any child nested path(s).
 */
unsafe fn JsonTablePlanScanNextRow(planstate: *mut JsonTablePlanState) -> bool {
    let mut jbv: *mut JsonbValue;
    let oldcxt: MemoryContext;

    /*
     * If planstate already has an active row and there is a nested plan,
     * check if it has an active row to join with the former.
     */
    if !(*planstate).current.isnull {
        if !(*planstate).nested.is_null()
            && JsonTablePlanNextRow((*planstate).nested)
        {
            return true;
        }
    }

    /* Fetch new row from the list of found values to set as active. */
    jbv = JsonValueListNext(&(*planstate).found, &mut (*planstate).iter);

    /* End of list? */
    if jbv.is_null() {
        (*planstate).current.value = PointerGetDatum(ptr::null());
        (*planstate).current.isnull = true;
        return false;
    }

    /*
     * Set current row item for subsequent JsonTableGetValue() calls.
     */
    oldcxt = MemoryContextSwitchTo((*planstate).mcxt);
    (*planstate).current.value = JsonbPGetDatum(JsonbValueToJsonb(jbv));
    (*planstate).current.isnull = false;
    MemoryContextSwitchTo(oldcxt);

    /* Next row! */
    (*planstate).ordinal += 1;

    /* Process nested plan(s), if any. */
    if !(*planstate).nested.is_null() {
        /* Re-evaluate the nested path using the above parent row. */
        JsonTableResetNestedPlan((*planstate).nested);

        /*
         * Now fetch the nested plan's current row to be joined against the
         * parent row.
         */
        let _ = JsonTablePlanNextRow((*planstate).nested);
    }

    true
}

/*
 * Re-evaluate the row pattern of a nested plan using the new parent row
 * pattern.
 */
unsafe fn JsonTableResetNestedPlan(planstate: *mut JsonTablePlanState) {
    /* This better be a child plan. */
    debug_assert!(!(*planstate).parent.is_null());
    if crate::IsA!((*planstate).plan, T_JsonTablePathScan) {
        let parent: *mut JsonTablePlanState = (*planstate).parent;

        if !(*parent).current.isnull {
            JsonTableResetRowPattern(planstate, (*parent).current.value);
        }

        /*
         * If this plan itself has a child nested plan, it will be reset when
         * the caller calls JsonTablePlanNextRow() on this plan.
         */
    } else if crate::IsA!((*planstate).plan, T_JsonTableSiblingJoin) {
        JsonTableResetNestedPlan((*planstate).left);
        JsonTableResetNestedPlan((*planstate).right);
    }
}

/*
 * Fetch the next row from a JsonTableSiblingJoin.
 *
 * This is essentially a UNION between the rows from left and right siblings.
 */
unsafe fn JsonTablePlanJoinNextRow(planstate: *mut JsonTablePlanState) -> bool {
    /* Fetch row from left sibling. */
    if !JsonTablePlanNextRow((*planstate).left) {
        /*
         * Left sibling ran out of rows, so start fetching from the right
         * sibling.
         */
        if !JsonTablePlanNextRow((*planstate).right) {
            /* Right sibling ran out of row, so there are more rows. */
            return false;
        }
    }

    true
}

/*
 * JsonTableFetchRow
 *      Prepare the next "current" row for upcoming GetValue calls.
 */
unsafe extern "C" fn JsonTableFetchRow(state: *mut TableFuncScanState) -> bool {
    let cxt: *mut JsonTableExecContext =
        GetJsonTableExecContext(state, b"JsonTableFetchRow\0".as_ptr() as *const c_char);

    JsonTablePlanNextRow((*cxt).rootplanstate)
}

/*
 * JsonTableGetValue
 *      Return the value for column number 'colnum' for the current row.
 *
 * This leaks memory, so be sure to reset often the context in which it's
 * called.
 */
unsafe extern "C" fn JsonTableGetValue(
    state: *mut TableFuncScanState,
    colnum: c_int,
    typid: Oid,
    typmod: int32,
    isnull: *mut bool,
) -> Datum {
    let cxt: *mut JsonTableExecContext =
        GetJsonTableExecContext(state, b"JsonTableGetValue\0".as_ptr() as *const c_char);
    let econtext: *mut ExprContext = (*state).ss.ps.ps_ExprContext;
    let estate: *mut ExprState = list_nth((*state).colvalexprs, colnum) as *mut ExprState;
    let planstate: *mut JsonTablePlanState = *(*cxt).colplanstates.add(colnum as usize);
    let current: *mut JsonTablePlanRowSource = &mut (*planstate).current;
    let mut result: Datum;

    /* Row pattern value is NULL */
    if (*current).isnull {
        result = 0 as Datum;
        *isnull = true;
    }
    /* Evaluate JsonExpr. */
    else if !estate.is_null() {
        let saved_caseValue: Datum = (*econtext).caseValue_datum;
        let saved_caseIsNull: bool = (*econtext).caseValue_isNull;

        /* Pass the row pattern value via CaseTestExpr. */
        (*econtext).caseValue_datum = (*current).value;
        (*econtext).caseValue_isNull = false;

        result = ExecEvalExpr(estate, econtext, isnull);

        (*econtext).caseValue_datum = saved_caseValue;
        (*econtext).caseValue_isNull = saved_caseIsNull;
    }
    /* ORDINAL column */
    else {
        result = Int32GetDatum((*planstate).ordinal);
        *isnull = false;
    }

    result
}
