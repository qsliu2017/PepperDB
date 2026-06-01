//! Translation of postgres/src/backend/utils/adt/jsonb.c
//!
//! I/O routines for jsonb type.
//!
//! Copyright (c) 2014-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/jsonb.c
//!
//! `#include`s mapped:
//!   postgres.h            -> crate::prelude
//!   access/htup_details.h -> HeapTupleHeader / heap_getattr (TODO stubbed below)
//!   catalog/pg_proc.h     -> PROVOLATILE_IMMUTABLE (TODO stub)
//!   catalog/pg_type.h     -> crate::catalog::pg_type_d (type OIDs)
//!   funcapi.h             -> AggCheckCallContext / extract_variadic_args (TODO stub)
//!   libpq/pqformat.h      -> crate::libpq::pqformat
//!   miscadmin.h           -> check_stack_depth()
//!   utils/builtins.h      -> crate::utils::adt::varlena (cstring_to_text family)
//!   utils/json.h          -> crate::utils::adt::json (JsonTypeCategory, JsonEncodeDateTime,
//!                            escape_json_with_len); the JSON lexer/parser hooks
//!                            (JsonLexContext/JsonSemAction/makeJsonLexContext*/
//!                            pg_parse_json_or_*/freeJsonLexContext/json_categorize_type)
//!                            are not yet ported and are stubbed locally as TODO(pg-port).
//!   utils/jsonb.h         -> crate::utils::adt::jsonb_util (Jsonb, JsonbValue, ...)
//!   utils/jsonfuncs.h     -> (only types we already have)
//!   utils/lsyscache.h     -> func_volatile / get_typlenbyvalalign (TODO stub)
//!   utils/typcache.h      -> lookup_rowtype_tupdesc (TODO stub)
//!
//! Every jsonb.c function is translated in full.  Functions living in other,
//! not-yet-ported .c files are declared here with TODO(pg-port) bodies.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_assignments)]

use crate::prelude::*;
use crate::c::{int16, int32, text};
use crate::postgres::{
    CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetPointer, Int32GetDatum,
    ObjectIdGetDatum, PointerGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::nodes::nodes::Node;
use crate::utils::fmgr::{
    get_fn_expr_argtype, DirectFunctionCall1Coll, DirectFunctionCall3Coll, DirectInputFunctionCallSafe,
    FunctionCallInfo, OidFunctionCall1Coll, OidOutputFunctionCall, PGFunction,
};
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoSpaces, appendStringInfoString, destroyStringInfo,
    enlargeStringInfo, makeStringInfo, StringInfo, StringInfoData,
};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_getmsgtext, pq_sendint8, pq_sendtext,
};
use crate::mb::mbutils::GetDatabaseEncoding;
use crate::utils::misc::stack_depth::check_stack_depth;
use crate::utils::adt::varlena::TextDatumGetCString;
use crate::utils::adt::varlena::cstring_to_text;
use crate::utils::adt::json::{
    escape_json_with_len, JsonEncodeDateTime, JsonTypeCategory,
};
use crate::utils::adt::json::JsonTypeCategory::*;
use crate::utils::adt::jsonb_util::{
    jbvType, IsAJsonbScalar, Jsonb, JsonbContainer, JsonbIterator, JsonbIteratorInit,
    JsonbIteratorNext, JsonbIteratorToken, JsonbParseState, JsonbValue, JsonbValueToJsonb,
    JsonContainerIsArray, JsonContainerIsObject, Numeric, pushJsonbValue,
};
use crate::utils::adt::jsonb_util::jbvType::*;
use crate::utils::adt::jsonb_util::JsonbIteratorToken::*;
use crate::utils::adt::jsonb_util::{JENTRY_OFFLENMASK, JB_FSCALAR};

use crate::catalog::pg_type_d::{
    DATEOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
};

use crate::{
    elog, ereport, errmsg, Assert, DirectFunctionCall1, DirectFunctionCall3, OidFunctionCall1,
    PG_GETARG_DATUM, PG_GETARG_POINTER, PG_RETURN_DATUM, PG_RETURN_NULL,
    PG_RETURN_POINTER, PG_ARGISNULL,
};
use crate::utils::elog::ERROR;

use core::ffi::{c_char, c_int, c_void, CStr};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

// ===========================================================================
// fmgr macros not exported as crate macros (numeric helpers, type-id getters).
// ===========================================================================

/* DatumGetNumeric(X): cast through Numeric pointer (utils/numeric.h). */
unsafe fn DatumGetNumeric(X: Datum) -> Numeric {
    DatumGetPointer(X) as Numeric
}
/* DatumGetNumericCopy(X): TODO(pg-port) real version flat-copies; here a plain
 * cast (numeric.c not ported, so we cannot deep-copy). */
unsafe fn DatumGetNumericCopy(X: Datum) -> Numeric {
    DatumGetPointer(X) as Numeric
}
unsafe fn NumericGetDatum(X: Numeric) -> Datum {
    PointerGetDatum(X as *const c_void)
}

/* PG_GETARG_CSTRING(n): the cstring input is a plain char*. */
macro_rules! PG_GETARG_CSTRING_local {
    ($fcinfo:expr, $n:expr) => {
        DatumGetCString(PG_GETARG_DATUM!($fcinfo, $n))
    };
}

/* PG_GETARG_JSONB_P(x): DatumGetJsonbP(PG_GETARG_DATUM(x)) (jsonb.h).
 * TODO(pg-port): real DatumGetJsonbP detoasts; helper not yet public. */
macro_rules! PG_GETARG_JSONB_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut Jsonb
    };
}

/* PG_GETARG_ARRAYTYPE_P(n) (array.h). */
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut ArrayType
    };
}

/* JsonbPGetDatum / JsonbGetDatum: Jsonb is a varlena pointer. */
unsafe fn JsonbPGetDatum(p: *mut Jsonb) -> Datum {
    PointerGetDatum(p as *const c_void)
}

/* DatumGetJsonbP(d): DatumGetJsonbPCopy without detoast (jsonb.h).
 * TODO(pg-port): real DatumGetJsonbP detoasts; helper not yet public. */
unsafe fn DatumGetJsonbP(d: Datum) -> *mut Jsonb {
    DatumGetPointer(d) as *mut Jsonb
}

/* VARSIZE(jb): varlena total length. */
unsafe fn VARSIZE(p: *const Jsonb) -> c_int {
    crate::varatt::VARSIZE(p as *const c_char) as c_int
}

/* PG_RETURN_CSTRING(x): return char* as Datum. */
macro_rules! PG_RETURN_CSTRING {
    ($x:expr) => {
        return CStringGetDatum($x)
    };
}
/* PG_RETURN_TEXT_P(x): return text* as Datum. */
macro_rules! PG_RETURN_TEXT_P {
    ($x:expr) => {
        return PointerGetDatum($x as *const c_void)
    };
}
/* PG_RETURN_BYTEA_P(x): return bytea* as Datum. */
macro_rules! PG_RETURN_BYTEA_P {
    ($x:expr) => {
        return PointerGetDatum($x as *const c_void)
    };
}
/* PG_RETURN_BOOL(x). */
macro_rules! PG_RETURN_BOOL {
    ($x:expr) => {
        return Datum::from($x as usize)
    };
}
/* PG_RETURN_NUMERIC(x): return Numeric as Datum. */
macro_rules! PG_RETURN_NUMERIC {
    ($x:expr) => {
        return NumericGetDatum($x)
    };
}
/* PG_FREE_IF_COPY(ptr, n): TODO(pg-port) detoast-copy tracking not modeled. */
macro_rules! PG_FREE_IF_COPY {
    ($ptr:expr, $n:expr) => {{
        let _ = (&$ptr, $n);
    }};
}

/* JB_ROOT_IS_SCALAR(jbp): top container is a scalar (jsonb.h). */
unsafe fn JB_ROOT_IS_SCALAR(jbp: *const Jsonb) -> bool {
    ((*jbp).root.header & JB_FSCALAR) != 0
}

// ===========================================================================
// TODO(pg-port): dependencies that live in other, not-yet-ported .c files.
// Declared here with the C signatures (PGFunction etc.) so this file is 1:1.
// ===========================================================================

/* numeric.c output/input/cast functions, used via DirectFunctionCall*. */
unsafe fn numeric_in(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("jsonb: numeric_in (utils/adt/numeric.c) not yet translated")
}
unsafe fn numeric_out(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("jsonb: numeric_out (utils/adt/numeric.c) not yet translated")
}
unsafe fn numeric_int2(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("jsonb: numeric_int2 (utils/adt/numeric.c) not yet translated")
}
unsafe fn numeric_int4(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("jsonb: numeric_int4 (utils/adt/numeric.c) not yet translated")
}
unsafe fn numeric_int8(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("jsonb: numeric_int8 (utils/adt/numeric.c) not yet translated")
}
unsafe fn numeric_float4(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("jsonb: numeric_float4 (utils/adt/numeric.c) not yet translated")
}
unsafe fn numeric_float8(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("jsonb: numeric_float8 (utils/adt/numeric.c) not yet translated")
}
unsafe fn numeric_uplus(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("jsonb: numeric_uplus (utils/adt/numeric.c) not yet translated")
}

/* lsyscache.c: function volatility, type physical properties. */
const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char;
unsafe fn func_volatile(_funcid: Oid) -> c_char {
    unimplemented!("jsonb: func_volatile (utils/cache/lsyscache.c) not yet translated")
}
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    _typlen: *mut int16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
) {
    unimplemented!("jsonb: get_typlenbyvalalign (utils/cache/lsyscache.c) not yet translated")
}

/* json.c: classify a SQL type for JSON conversion. */
unsafe fn json_categorize_type(
    _typoid: Oid,
    _is_jsonb: bool,
    _tcategory: *mut JsonTypeCategory,
    _outfuncoid: *mut Oid,
) {
    unimplemented!("jsonb: json_categorize_type (utils/adt/json.c) not yet translated")
}

/* common/jsonapi.h: parse error type, lexer context, semantic-action hooks. */
type JsonParseErrorType = c_int;
const JSON_SUCCESS: JsonParseErrorType = 0;
const JSON_SEM_ACTION_FAILED: JsonParseErrorType = 14;

type JsonTokenType = c_int;
const JSON_TOKEN_STRING: JsonTokenType = 1;
const JSON_TOKEN_NUMBER: JsonTokenType = 2;
const JSON_TOKEN_TRUE: JsonTokenType = 11;
const JSON_TOKEN_FALSE: JsonTokenType = 12;
const JSON_TOKEN_NULL: JsonTokenType = 13;

#[repr(C)]
struct JsonLexContext {
    _opaque: [u8; 0],
}

type json_struct_action = unsafe fn(state: *mut c_void) -> JsonParseErrorType;
type json_ofield_action =
    unsafe fn(state: *mut c_void, fname: *mut c_char, isnull: bool) -> JsonParseErrorType;
type json_scalar_action =
    unsafe fn(state: *mut c_void, token: *mut c_char, tokentype: JsonTokenType) -> JsonParseErrorType;

#[repr(C)]
struct JsonSemAction {
    semstate: *mut c_void,
    object_start: Option<json_struct_action>,
    object_end: Option<json_struct_action>,
    array_start: Option<json_struct_action>,
    array_end: Option<json_struct_action>,
    object_field_start: Option<json_ofield_action>,
    object_field_end: Option<json_ofield_action>,
    array_element_start: Option<json_ofield_action>,
    array_element_end: Option<json_ofield_action>,
    scalar: Option<json_scalar_action>,
}

unsafe fn makeJsonLexContextCstringLen(
    _lex: *mut JsonLexContext,
    _json: *mut c_char,
    _len: c_int,
    _encoding: c_int,
    _need_escapes: bool,
) -> *mut JsonLexContext {
    unimplemented!("jsonb: makeJsonLexContextCstringLen (common/jsonapi.c) not yet translated")
}
unsafe fn makeJsonLexContext(
    _lex: *mut JsonLexContext,
    _json: *mut text,
    _need_escapes: bool,
) -> *mut JsonLexContext {
    unimplemented!("jsonb: makeJsonLexContext (utils/adt/jsonfuncs.c) not yet translated")
}
unsafe fn pg_parse_json_or_errsave(
    _lex: *mut JsonLexContext,
    _sem: *mut JsonSemAction,
    _escontext: *mut Node,
) -> bool {
    unimplemented!("jsonb: pg_parse_json_or_errsave (common/jsonapi.c) not yet translated")
}
unsafe fn pg_parse_json_or_ereport(_lex: *mut JsonLexContext, _sem: *mut JsonSemAction) {
    unimplemented!("jsonb: pg_parse_json_or_ereport (utils/adt/jsonfuncs.c) not yet translated")
}
unsafe fn freeJsonLexContext(_lex: *mut JsonLexContext) {
    unimplemented!("jsonb: freeJsonLexContext (common/jsonapi.c) not yet translated")
}

/* funcapi.h / executor: aggregate context + variadic argument extraction. */
unsafe fn AggCheckCallContext(
    _fcinfo: FunctionCallInfo,
    _aggcontext: *mut MemoryContext,
) -> bool {
    unimplemented!("jsonb: AggCheckCallContext (executor/nodeAgg.c) not yet translated")
}
unsafe fn extract_variadic_args(
    _fcinfo: FunctionCallInfo,
    _variadic_start: c_int,
    _convert_unknown: bool,
    _args: *mut *mut Datum,
    _types: *mut *mut Oid,
    _nulls: *mut *mut bool,
) -> c_int {
    unimplemented!("jsonb: extract_variadic_args (utils/fmgr/funcapi.c) not yet translated")
}

/* utils/array.h, arrayfuncs.c: array deconstruction. */
#[repr(C)]
struct ArrayType {
    _opaque: [u8; 0],
}
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    unimplemented!("jsonb: DatumGetArrayTypeP (utils/adt/arrayfuncs.c) not yet translated")
}
unsafe fn ARR_ELEMTYPE(_a: *mut ArrayType) -> Oid {
    unimplemented!("jsonb: ARR_ELEMTYPE (utils/array.h) not yet translated")
}
unsafe fn ARR_NDIM(_a: *mut ArrayType) -> c_int {
    unimplemented!("jsonb: ARR_NDIM (utils/array.h) not yet translated")
}
unsafe fn ARR_DIMS(_a: *mut ArrayType) -> *mut c_int {
    unimplemented!("jsonb: ARR_DIMS (utils/array.h) not yet translated")
}
unsafe fn ArrayGetNItems(_ndim: c_int, _dims: *const c_int) -> c_int {
    unimplemented!("jsonb: ArrayGetNItems (utils/adt/arrayutils.c) not yet translated")
}
unsafe fn deconstruct_array(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elmlen: int16,
    _elmbyval: bool,
    _elmalign: c_char,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!("jsonb: deconstruct_array (utils/adt/arrayfuncs.c) not yet translated")
}
unsafe fn deconstruct_array_builtin(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!("jsonb: deconstruct_array_builtin (utils/adt/arrayfuncs.c) not yet translated")
}

/* access/htup_details.h + typcache.h: composite (record) deconstruction. */
type HeapTupleHeader = *mut c_void;
#[repr(C)]
struct HeapTupleData {
    t_len: u32,
    t_self: [u8; 6],
    t_tableOid: Oid,
    t_data: HeapTupleHeader,
}
type HeapTuple = *mut HeapTupleData;
#[repr(C)]
struct TupleDescData {
    natts: c_int,
    _opaque: [u8; 0],
}
type TupleDesc = *mut TupleDescData;
type Form_pg_attribute = *mut FormData_pg_attribute;
#[repr(C)]
struct FormData_pg_attribute {
    _opaque: [u8; 0],
}

unsafe fn DatumGetHeapTupleHeader(_d: Datum) -> HeapTupleHeader {
    unimplemented!("jsonb: DatumGetHeapTupleHeader (fmgr.h) not yet translated")
}
unsafe fn HeapTupleHeaderGetTypeId(_td: HeapTupleHeader) -> Oid {
    unimplemented!("jsonb: HeapTupleHeaderGetTypeId (access/htup_details.h) not yet translated")
}
unsafe fn HeapTupleHeaderGetTypMod(_td: HeapTupleHeader) -> int32 {
    unimplemented!("jsonb: HeapTupleHeaderGetTypMod (access/htup_details.h) not yet translated")
}
unsafe fn HeapTupleHeaderGetDatumLength(_td: HeapTupleHeader) -> u32 {
    unimplemented!("jsonb: HeapTupleHeaderGetDatumLength (access/htup_details.h) not yet translated")
}
unsafe fn lookup_rowtype_tupdesc(_type_id: Oid, _typmod: int32) -> TupleDesc {
    unimplemented!("jsonb: lookup_rowtype_tupdesc (utils/cache/typcache.c) not yet translated")
}
unsafe fn ReleaseTupleDesc(_tupdesc: TupleDesc) {
    unimplemented!("jsonb: ReleaseTupleDesc (utils/cache/typcache.c) not yet translated")
}
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    unimplemented!("jsonb: TupleDescAttr (access/tupdesc.h) not yet translated")
}
unsafe fn att_isdropped(_att: Form_pg_attribute) -> bool {
    unimplemented!("jsonb: attisdropped (access/tupdesc.h) not yet translated")
}
unsafe fn att_name(_att: Form_pg_attribute) -> *mut c_char {
    unimplemented!("jsonb: NameStr(att->attname) (access/tupdesc.h) not yet translated")
}
unsafe fn att_typid(_att: Form_pg_attribute) -> Oid {
    unimplemented!("jsonb: att->atttypid (access/tupdesc.h) not yet translated")
}
unsafe fn heap_getattr(
    _tup: HeapTuple,
    _attnum: c_int,
    _tupdesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!("jsonb: heap_getattr (access/htup_details.h) not yet translated")
}

// ===========================================================================
// Local types (jsonb.c)
// ===========================================================================

#[repr(C)]
struct JsonbInState {
    parseState: *mut JsonbParseState,
    res: *mut JsonbValue,
    unique_keys: bool,
    escontext: *mut Node,
}

#[repr(C)]
struct JsonbAggState {
    res: *mut JsonbInState,
    key_category: JsonTypeCategory,
    key_output_func: Oid,
    val_category: JsonTypeCategory,
    val_output_func: Oid,
}

/*
 * jsonb type input function
 */
pub unsafe fn jsonb_in(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut c_char = PG_GETARG_CSTRING_local!(fcinfo, 0);

    jsonb_from_cstring(json, strlen(json) as c_int, false, (*fcinfo).context)
}

/*
 * jsonb type recv function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
pub unsafe fn jsonb_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let version: c_int = pq_getmsgint(buf, 1) as c_int;
    let str: *mut c_char;
    let mut nbytes: c_int = 0;

    if version == 1 {
        str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &mut nbytes);
    } else {
        elog!(ERROR, "unsupported jsonb version number {}", version);
    }

    jsonb_from_cstring(str, nbytes, false, std::ptr::null_mut())
}

/*
 * jsonb type output function
 */
pub unsafe fn jsonb_out(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let out: *mut c_char;

    out = JsonbToCString(std::ptr::null_mut(), &mut (*jb).root, VARSIZE(jb));

    PG_RETURN_CSTRING!(out);
}

/*
 * jsonb type send function
 *
 * Just send jsonb as a version number, then a string of text
 */
pub unsafe fn jsonb_send(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();
    let jtext: StringInfo = makeStringInfo();
    let version: c_int = 1;

    JsonbToCString(jtext, &mut (*jb).root, VARSIZE(jb));

    pq_begintypsend(&mut buf);
    pq_sendint8(&mut buf, version as u8);
    pq_sendtext(&mut buf, (*jtext).data, (*jtext).len);
    destroyStringInfo(jtext);

    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

/*
 * jsonb_from_text
 *
 * Turns json text string into a jsonb Datum.
 */
pub unsafe fn jsonb_from_text(js: *mut text, unique_keys: bool) -> Datum {
    jsonb_from_cstring(
        crate::varatt::VARDATA_ANY(js as *mut c_char),
        crate::varatt::VARSIZE_ANY_EXHDR(js as *mut c_char) as c_int,
        unique_keys,
        std::ptr::null_mut(),
    )
}

/*
 * Get the type name of a jsonb container.
 */
unsafe fn JsonbContainerTypeName(jbc: *mut JsonbContainer) -> *const c_char {
    let mut scalar: JsonbValue = core::mem::zeroed();

    if JsonbExtractScalar(jbc, &mut scalar) {
        JsonbTypeName(&mut scalar)
    } else if JsonContainerIsArray(jbc) {
        c"array".as_ptr()
    } else if JsonContainerIsObject(jbc) {
        c"object".as_ptr()
    } else {
        elog!(
            ERROR,
            "invalid jsonb container type: 0x{:08x}",
            (*jbc).header
        );
        #[allow(unreachable_code)]
        c"unknown".as_ptr()
    }
}

/*
 * SQL function jsonb_typeof(jsonb) -> text
 *
 * This function is here because the analog json function is in json.c, since
 * it uses the json parser internals not exposed elsewhere.
 */
pub unsafe fn jsonb_typeof(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let result: *const c_char = JsonbContainerTypeName(&mut (*r#in).root);

    PG_RETURN_TEXT_P!(cstring_to_text(result));
}

/*
 * jsonb_from_cstring
 *
 * Turns json string into a jsonb Datum.
 *
 * Uses the json parser (with hooks) to construct a jsonb.
 *
 * If escontext points to an ErrorSaveContext, errors are reported there
 * instead of being thrown.
 */
unsafe fn jsonb_from_cstring(
    json: *mut c_char,
    len: c_int,
    unique_keys: bool,
    escontext: *mut Node,
) -> Datum {
    let mut lex: JsonLexContext = core::mem::zeroed();
    let mut state: JsonbInState = core::mem::zeroed();
    let mut sem: JsonSemAction = core::mem::zeroed();

    memset(
        &mut state as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );
    memset(
        &mut sem as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonSemAction>(),
    );
    makeJsonLexContextCstringLen(&mut lex, json, len, GetDatabaseEncoding(), true);

    state.unique_keys = unique_keys;
    state.escontext = escontext;
    sem.semstate = &mut state as *mut _ as *mut c_void;

    sem.object_start = Some(jsonb_in_object_start);
    sem.array_start = Some(jsonb_in_array_start);
    sem.object_end = Some(jsonb_in_object_end);
    sem.array_end = Some(jsonb_in_array_end);
    sem.scalar = Some(jsonb_in_scalar);
    sem.object_field_start = Some(jsonb_in_object_field_start);

    if !pg_parse_json_or_errsave(&mut lex, &mut sem, escontext) {
        return 0 as Datum;
    }

    /* after parsing, the item member has the composed jsonb structure */
    PG_RETURN_POINTER!(JsonbValueToJsonb(state.res));
}

unsafe fn checkStringLen(len: usize, escontext: *mut Node) -> bool {
    if len > JENTRY_OFFLENMASK as usize {
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        // errdetail("Due to an implementation restriction, jsonb strings
        // cannot exceed %d bytes.", JENTRY_OFFLENMASK)
        let _ = escontext;
        ereport!(
            ERROR,
            errmsg!("string too long to represent as jsonb string")
        );
        #[allow(unreachable_code)]
        return false;
    }

    true
}

unsafe fn jsonb_in_object_start(pstate: *mut c_void) -> JsonParseErrorType {
    let _state: *mut JsonbInState = pstate as *mut JsonbInState;

    (*_state).res = pushJsonbValue(
        &mut (*_state).parseState,
        WJB_BEGIN_OBJECT,
        std::ptr::null_mut(),
    );
    (*(*_state).parseState).unique_keys = (*_state).unique_keys;

    JSON_SUCCESS
}

unsafe fn jsonb_in_object_end(pstate: *mut c_void) -> JsonParseErrorType {
    let _state: *mut JsonbInState = pstate as *mut JsonbInState;

    (*_state).res = pushJsonbValue(
        &mut (*_state).parseState,
        WJB_END_OBJECT,
        std::ptr::null_mut(),
    );

    JSON_SUCCESS
}

unsafe fn jsonb_in_array_start(pstate: *mut c_void) -> JsonParseErrorType {
    let _state: *mut JsonbInState = pstate as *mut JsonbInState;

    (*_state).res = pushJsonbValue(
        &mut (*_state).parseState,
        WJB_BEGIN_ARRAY,
        std::ptr::null_mut(),
    );

    JSON_SUCCESS
}

unsafe fn jsonb_in_array_end(pstate: *mut c_void) -> JsonParseErrorType {
    let _state: *mut JsonbInState = pstate as *mut JsonbInState;

    (*_state).res = pushJsonbValue(
        &mut (*_state).parseState,
        WJB_END_ARRAY,
        std::ptr::null_mut(),
    );

    JSON_SUCCESS
}

unsafe fn jsonb_in_object_field_start(
    pstate: *mut c_void,
    fname: *mut c_char,
    _isnull: bool,
) -> JsonParseErrorType {
    let _state: *mut JsonbInState = pstate as *mut JsonbInState;
    let mut v: JsonbValue = core::mem::zeroed();

    Assert!(!fname.is_null());
    v.type_ = jbvString;
    v.val.string.len = strlen(fname) as c_int;
    if !checkStringLen(v.val.string.len as usize, (*_state).escontext) {
        return JSON_SEM_ACTION_FAILED;
    }
    v.val.string.val = fname;

    (*_state).res = pushJsonbValue(&mut (*_state).parseState, WJB_KEY, &mut v);

    JSON_SUCCESS
}

unsafe fn jsonb_put_escaped_value(out: StringInfo, scalarVal: *mut JsonbValue) {
    match (*scalarVal).type_ {
        jbvNull => {
            appendBinaryStringInfo(out, c"null".as_ptr() as *const c_void, 4);
        }
        jbvString => {
            escape_json_with_len(out, (*scalarVal).val.string.val, (*scalarVal).val.string.len);
        }
        jbvNumeric => {
            appendStringInfoString(
                out,
                DatumGetCString(DirectFunctionCall1!(
                    numeric_out as PGFunction,
                    PointerGetDatum((*scalarVal).val.numeric as *const c_void)
                )),
            );
        }
        jbvBool => {
            if (*scalarVal).val.boolean {
                appendBinaryStringInfo(out, c"true".as_ptr() as *const c_void, 4);
            } else {
                appendBinaryStringInfo(out, c"false".as_ptr() as *const c_void, 5);
            }
        }
        _ => {
            elog!(ERROR, "unknown jsonb scalar type");
        }
    }
}

/*
 * For jsonb we always want the de-escaped value - that's what's in token
 */
unsafe fn jsonb_in_scalar(
    pstate: *mut c_void,
    token: *mut c_char,
    tokentype: JsonTokenType,
) -> JsonParseErrorType {
    let _state: *mut JsonbInState = pstate as *mut JsonbInState;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut numd: Datum = 0;

    match tokentype {
        JSON_TOKEN_STRING => {
            Assert!(!token.is_null());
            v.type_ = jbvString;
            v.val.string.len = strlen(token) as c_int;
            if !checkStringLen(v.val.string.len as usize, (*_state).escontext) {
                return JSON_SEM_ACTION_FAILED;
            }
            v.val.string.val = token;
        }
        JSON_TOKEN_NUMBER => {
            /*
             * No need to check size of numeric values, because maximum
             * numeric size is well below the JsonbValue restriction
             */
            Assert!(!token.is_null());
            v.type_ = jbvNumeric;
            if !DirectInputFunctionCallSafe(
                numeric_in as PGFunction,
                token,
                InvalidOid,
                -1,
                (*_state).escontext,
                &mut numd,
            ) {
                return JSON_SEM_ACTION_FAILED;
            }
            v.val.numeric = DatumGetNumeric(numd);
        }
        JSON_TOKEN_TRUE => {
            v.type_ = jbvBool;
            v.val.boolean = true;
        }
        JSON_TOKEN_FALSE => {
            v.type_ = jbvBool;
            v.val.boolean = false;
        }
        JSON_TOKEN_NULL => {
            v.type_ = jbvNull;
        }
        _ => {
            /* should not be possible */
            elog!(ERROR, "invalid json token type");
        }
    }

    if (*_state).parseState.is_null() {
        /* single scalar */
        let mut va: JsonbValue = core::mem::zeroed();

        va.type_ = jbvArray;
        va.val.array.rawScalar = true;
        va.val.array.nElems = 1;

        (*_state).res = pushJsonbValue(&mut (*_state).parseState, WJB_BEGIN_ARRAY, &mut va);
        (*_state).res = pushJsonbValue(&mut (*_state).parseState, WJB_ELEM, &mut v);
        (*_state).res = pushJsonbValue(
            &mut (*_state).parseState,
            WJB_END_ARRAY,
            std::ptr::null_mut(),
        );
    } else {
        let o: *mut JsonbValue = &mut (*(*_state).parseState).contVal;

        match (*o).type_ {
            jbvArray => {
                (*_state).res = pushJsonbValue(&mut (*_state).parseState, WJB_ELEM, &mut v);
            }
            jbvObject => {
                (*_state).res = pushJsonbValue(&mut (*_state).parseState, WJB_VALUE, &mut v);
            }
            _ => {
                elog!(ERROR, "unexpected parent of nested structure");
            }
        }
    }

    JSON_SUCCESS
}

/*
 * Turn a Datum into jsonb, adding it to the result JsonbInState.  See part 2.
 */

/*
 * Get the type name of a jsonb value.
 */
pub unsafe fn JsonbTypeName(val: *mut JsonbValue) -> *const c_char {
    match (*val).type_ {
        jbvBinary => JsonbContainerTypeName((*val).val.binary.data),
        jbvObject => c"object".as_ptr(),
        jbvArray => c"array".as_ptr(),
        jbvNumeric => c"number".as_ptr(),
        jbvString => c"string".as_ptr(),
        jbvBool => c"boolean".as_ptr(),
        jbvNull => c"null".as_ptr(),
        jbvDatetime => match (*val).val.datetime.typid {
            DATEOID => c"date".as_ptr(),
            TIMEOID => c"time without time zone".as_ptr(),
            TIMETZOID => c"time with time zone".as_ptr(),
            TIMESTAMPOID => c"timestamp without time zone".as_ptr(),
            TIMESTAMPTZOID => c"timestamp with time zone".as_ptr(),
            _ => {
                elog!(
                    ERROR,
                    "unrecognized jsonb value datetime type: {}",
                    (*val).val.datetime.typid
                );
                #[allow(unreachable_code)]
                c"unknown".as_ptr()
            }
        },
        _ => {
            elog!(ERROR, "unrecognized jsonb value type: {}", (*val).type_ as c_int);
            #[allow(unreachable_code)]
            c"unknown".as_ptr()
        }
    }
}

/*
 * JsonbToCString
 *	   Converts jsonb value to a C-string.
 *
 * If 'out' argument is non-null, the resulting C-string is stored inside the
 * StringBuffer.  The resulting string is always returned.
 *
 * A typical case for passing the StringInfo in rather than NULL is where the
 * caller wants access to the len attribute without having to call strlen, e.g.
 * if they are converting it to a text* object.
 */
pub unsafe fn JsonbToCString(
    out: StringInfo,
    r#in: *mut JsonbContainer,
    estimated_len: c_int,
) -> *mut c_char {
    JsonbToCStringWorker(out, r#in, estimated_len, false)
}

/*
 * same thing but with indentation turned on
 */
pub unsafe fn JsonbToCStringIndent(
    out: StringInfo,
    r#in: *mut JsonbContainer,
    estimated_len: c_int,
) -> *mut c_char {
    JsonbToCStringWorker(out, r#in, estimated_len, true)
}

/*
 * common worker for above two functions
 */
unsafe fn JsonbToCStringWorker(
    mut out: StringInfo,
    r#in: *mut JsonbContainer,
    estimated_len: c_int,
    indent: bool,
) -> *mut c_char {
    let mut first = true;
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r#type: JsonbIteratorToken = WJB_DONE;
    let mut level: c_int = 0;
    let mut redo_switch = false;

    /* If we are indenting, don't add a space after a comma */
    let ispaces: c_int = if indent { 1 } else { 2 };

    /*
     * Don't indent the very first item. This gets set to the indent flag at
     * the bottom of the loop.
     */
    let mut use_indent = false;
    let mut raw_scalar = false;
    let mut last_was_key = false;

    if out.is_null() {
        out = makeStringInfo();
    }

    enlargeStringInfo(out, if estimated_len >= 0 { estimated_len } else { 64 });

    it = JsonbIteratorInit(r#in);

    while redo_switch || {
        r#type = JsonbIteratorNext(&mut it, &mut v, false);
        r#type != WJB_DONE
    } {
        redo_switch = false;
        match r#type {
            WJB_BEGIN_ARRAY => {
                if !first {
                    appendBinaryStringInfo(out, c", ".as_ptr() as *const c_void, ispaces);
                }

                if !v.val.array.rawScalar {
                    add_indent(out, use_indent && !last_was_key, level);
                    crate::appendStringInfoCharMacro!(out, b'[' as c_char);
                } else {
                    raw_scalar = true;
                }

                first = true;
                level += 1;
            }
            WJB_BEGIN_OBJECT => {
                if !first {
                    appendBinaryStringInfo(out, c", ".as_ptr() as *const c_void, ispaces);
                }

                add_indent(out, use_indent && !last_was_key, level);
                crate::appendStringInfoCharMacro!(out, b'{' as c_char);

                first = true;
                level += 1;
            }
            WJB_KEY => {
                if !first {
                    appendBinaryStringInfo(out, c", ".as_ptr() as *const c_void, ispaces);
                }
                first = true;

                add_indent(out, use_indent, level);

                /* json rules guarantee this is a string */
                jsonb_put_escaped_value(out, &mut v);
                appendBinaryStringInfo(out, c": ".as_ptr() as *const c_void, 2);

                r#type = JsonbIteratorNext(&mut it, &mut v, false);
                if r#type == WJB_VALUE {
                    first = false;
                    jsonb_put_escaped_value(out, &mut v);
                } else {
                    Assert!(r#type == WJB_BEGIN_OBJECT || r#type == WJB_BEGIN_ARRAY);

                    /*
                     * We need to rerun the current switch() since we need to
                     * output the object which we just got from the iterator
                     * before calling the iterator again.
                     */
                    redo_switch = true;
                }
            }
            WJB_ELEM => {
                if !first {
                    appendBinaryStringInfo(out, c", ".as_ptr() as *const c_void, ispaces);
                }
                first = false;

                if !raw_scalar {
                    add_indent(out, use_indent, level);
                }
                jsonb_put_escaped_value(out, &mut v);
            }
            WJB_END_ARRAY => {
                level -= 1;
                if !raw_scalar {
                    add_indent(out, use_indent, level);
                    crate::appendStringInfoCharMacro!(out, b']' as c_char);
                }
                first = false;
            }
            WJB_END_OBJECT => {
                level -= 1;
                add_indent(out, use_indent, level);
                crate::appendStringInfoCharMacro!(out, b'}' as c_char);
                first = false;
            }
            _ => {
                elog!(ERROR, "unknown jsonb iterator token type");
            }
        }
        use_indent = indent;
        last_was_key = redo_switch;
    }

    Assert!(level == 0);

    (*out).data
}

unsafe fn add_indent(out: StringInfo, indent: bool, level: c_int) {
    if indent {
        crate::appendStringInfoCharMacro!(out, b'\n' as c_char);
        appendStringInfoSpaces(out, level * 4);
    }
}

/*
 * Turn a Datum into jsonb, adding it to the result JsonbInState.
 *
 * tcategory and outfuncoid are from a previous call to json_categorize_type,
 * except that if is_null is true then they can be invalid.
 *
 * If key_scalar is true, the value is stored as a key, so insist
 * it's of an acceptable type, and force it to be a jbvString.
 *
 * Note: currently, we assume that result->escontext is NULL and errors
 * will be thrown.
 */
unsafe fn datum_to_jsonb_internal(
    mut val: Datum,
    is_null: bool,
    result: *mut JsonbInState,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
    key_scalar: bool,
) {
    let mut outputstr: *mut c_char;
    let numeric_error: bool;
    let mut jb: JsonbValue = core::mem::zeroed();
    let mut scalar_jsonb = false;

    check_stack_depth();

    /* Convert val to a JsonbValue in jb (in most cases) */
    if is_null {
        Assert!(!key_scalar);
        jb.type_ = jbvNull;
    } else if key_scalar
        && (tcategory == JSONTYPE_ARRAY
            || tcategory == JSONTYPE_COMPOSITE
            || tcategory == JSONTYPE_JSON
            || tcategory == JSONTYPE_JSONB
            || tcategory == JSONTYPE_CAST)
    {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        ereport!(
            ERROR,
            errmsg!("key value must be scalar, not array, composite, or json")
        );
    } else {
        if tcategory == JSONTYPE_CAST {
            val = OidFunctionCall1!(outfuncoid, val);
        }

        match tcategory {
            JSONTYPE_ARRAY => {
                array_to_jsonb_internal(val, result);
            }
            JSONTYPE_COMPOSITE => {
                composite_to_jsonb(val, result);
            }
            JSONTYPE_BOOL => {
                if key_scalar {
                    outputstr = if DatumGetBool(val) {
                        c"true".as_ptr() as *mut c_char
                    } else {
                        c"false".as_ptr() as *mut c_char
                    };
                    jb.type_ = jbvString;
                    jb.val.string.len = strlen(outputstr) as c_int;
                    jb.val.string.val = outputstr;
                } else {
                    jb.type_ = jbvBool;
                    jb.val.boolean = DatumGetBool(val);
                }
            }
            JSONTYPE_NUMERIC => {
                outputstr = OidOutputFunctionCall(outfuncoid, val);
                if key_scalar {
                    /* always quote keys */
                    jb.type_ = jbvString;
                    jb.val.string.len = strlen(outputstr) as c_int;
                    jb.val.string.val = outputstr;
                } else {
                    /*
                     * Make it numeric if it's a valid JSON number, otherwise
                     * a string. Invalid numeric output will always have an
                     * 'N' or 'n' in it (I think).
                     */
                    numeric_error = !strchr(outputstr, b'N' as c_int).is_null()
                        || !strchr(outputstr, b'n' as c_int).is_null();
                    if !numeric_error {
                        let numd: Datum;

                        jb.type_ = jbvNumeric;
                        numd = DirectFunctionCall3!(
                            numeric_in as PGFunction,
                            CStringGetDatum(outputstr),
                            ObjectIdGetDatum(InvalidOid),
                            Int32GetDatum(-1)
                        );
                        jb.val.numeric = DatumGetNumeric(numd);
                        pfree(outputstr as *mut c_void);
                    } else {
                        jb.type_ = jbvString;
                        jb.val.string.len = strlen(outputstr) as c_int;
                        jb.val.string.val = outputstr;
                    }
                }
            }
            JSONTYPE_DATE => {
                jb.type_ = jbvString;
                jb.val.string.val = JsonEncodeDateTime(
                    std::ptr::null_mut(),
                    val,
                    DATEOID,
                    core::ptr::null::<c_int>(),
                );
                jb.val.string.len = strlen(jb.val.string.val) as c_int;
            }
            JSONTYPE_TIMESTAMP => {
                jb.type_ = jbvString;
                jb.val.string.val = JsonEncodeDateTime(
                    std::ptr::null_mut(),
                    val,
                    TIMESTAMPOID,
                    core::ptr::null::<c_int>(),
                );
                jb.val.string.len = strlen(jb.val.string.val) as c_int;
            }
            JSONTYPE_TIMESTAMPTZ => {
                jb.type_ = jbvString;
                jb.val.string.val = JsonEncodeDateTime(
                    std::ptr::null_mut(),
                    val,
                    TIMESTAMPTZOID,
                    core::ptr::null::<c_int>(),
                );
                jb.val.string.len = strlen(jb.val.string.val) as c_int;
            }
            JSONTYPE_CAST | JSONTYPE_JSON => {
                /* parse the json right into the existing result object */
                let mut lex: JsonLexContext = core::mem::zeroed();
                let mut sem: JsonSemAction = core::mem::zeroed();
                let json: *mut text = crate::utils::fmgr::DatumGetTextPP!(val);

                makeJsonLexContext(&mut lex, json, true);

                memset(
                    &mut sem as *mut _ as *mut c_void,
                    0,
                    core::mem::size_of::<JsonSemAction>(),
                );

                sem.semstate = result as *mut c_void;

                sem.object_start = Some(jsonb_in_object_start);
                sem.array_start = Some(jsonb_in_array_start);
                sem.object_end = Some(jsonb_in_object_end);
                sem.array_end = Some(jsonb_in_array_end);
                sem.scalar = Some(jsonb_in_scalar);
                sem.object_field_start = Some(jsonb_in_object_field_start);

                pg_parse_json_or_ereport(&mut lex, &mut sem);
                freeJsonLexContext(&mut lex);
            }
            JSONTYPE_JSONB => {
                let jsonb: *mut Jsonb = DatumGetJsonbP(val);
                let mut it: *mut JsonbIterator;

                it = JsonbIteratorInit(&mut (*jsonb).root);

                if JB_ROOT_IS_SCALAR(jsonb) {
                    JsonbIteratorNext(&mut it, &mut jb, true);
                    Assert!(jb.type_ == jbvArray);
                    JsonbIteratorNext(&mut it, &mut jb, true);
                    scalar_jsonb = true;
                } else {
                    let mut r#type: JsonbIteratorToken;

                    while {
                        r#type = JsonbIteratorNext(&mut it, &mut jb, false);
                        r#type != WJB_DONE
                    } {
                        if r#type == WJB_END_ARRAY
                            || r#type == WJB_END_OBJECT
                            || r#type == WJB_BEGIN_ARRAY
                            || r#type == WJB_BEGIN_OBJECT
                        {
                            (*result).res = pushJsonbValue(
                                &mut (*result).parseState,
                                r#type,
                                std::ptr::null_mut(),
                            );
                        } else {
                            (*result).res =
                                pushJsonbValue(&mut (*result).parseState, r#type, &mut jb);
                        }
                    }
                }
            }
            _ => {
                outputstr = OidOutputFunctionCall(outfuncoid, val);
                jb.type_ = jbvString;
                jb.val.string.len = strlen(outputstr) as c_int;
                checkStringLen(jb.val.string.len as usize, std::ptr::null_mut());
                jb.val.string.val = outputstr;
            }
        }
    }

    /* Now insert jb into result, unless we did it recursively */
    if !is_null
        && !scalar_jsonb
        && tcategory as c_int >= JSONTYPE_JSON as c_int
        && tcategory as c_int <= JSONTYPE_CAST as c_int
    {
        /* work has been done recursively */
        return;
    } else if (*result).parseState.is_null() {
        /* single root scalar */
        let mut va: JsonbValue = core::mem::zeroed();

        va.type_ = jbvArray;
        va.val.array.rawScalar = true;
        va.val.array.nElems = 1;

        (*result).res =
            pushJsonbValue(&mut (*result).parseState, WJB_BEGIN_ARRAY, &mut va);
        (*result).res = pushJsonbValue(&mut (*result).parseState, WJB_ELEM, &mut jb);
        (*result).res = pushJsonbValue(
            &mut (*result).parseState,
            WJB_END_ARRAY,
            std::ptr::null_mut(),
        );
    } else {
        let o: *mut JsonbValue = &mut (*(*result).parseState).contVal;

        match (*o).type_ {
            jbvArray => {
                (*result).res = pushJsonbValue(&mut (*result).parseState, WJB_ELEM, &mut jb);
            }
            jbvObject => {
                (*result).res = pushJsonbValue(
                    &mut (*result).parseState,
                    if key_scalar { WJB_KEY } else { WJB_VALUE },
                    &mut jb,
                );
            }
            _ => {
                elog!(ERROR, "unexpected parent of nested structure");
            }
        }
    }
}

/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
unsafe fn array_dim_to_jsonb(
    result: *mut JsonbInState,
    dim: c_int,
    ndims: c_int,
    dims: *mut c_int,
    vals: *const Datum,
    nulls: *const bool,
    valcount: *mut c_int,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
) {
    let mut i: c_int;

    Assert!(dim < ndims);

    (*result).res = pushJsonbValue(
        &mut (*result).parseState,
        WJB_BEGIN_ARRAY,
        std::ptr::null_mut(),
    );

    i = 1;
    while i <= *dims.add(dim as usize) {
        if dim + 1 == ndims {
            datum_to_jsonb_internal(
                *vals.add(*valcount as usize),
                *nulls.add(*valcount as usize),
                result,
                tcategory,
                outfuncoid,
                false,
            );
            *valcount += 1;
        } else {
            array_dim_to_jsonb(
                result,
                dim + 1,
                ndims,
                dims,
                vals,
                nulls,
                valcount,
                tcategory,
                outfuncoid,
            );
        }
        i += 1;
    }

    (*result).res = pushJsonbValue(
        &mut (*result).parseState,
        WJB_END_ARRAY,
        std::ptr::null_mut(),
    );
}

/*
 * Turn an array into JSON.
 */
unsafe fn array_to_jsonb_internal(array: Datum, result: *mut JsonbInState) {
    let v: *mut ArrayType = DatumGetArrayTypeP(array);
    let element_type: Oid = ARR_ELEMTYPE(v);
    let dim: *mut c_int;
    let ndim: c_int;
    let mut nitems: c_int;
    let mut count: c_int = 0;
    let mut elements: *mut Datum = std::ptr::null_mut();
    let mut nulls: *mut bool = std::ptr::null_mut();
    let mut typlen: int16 = 0;
    let mut typbyval: bool = false;
    let mut typalign: c_char = 0;
    let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
    let mut outfuncoid: Oid = InvalidOid;

    ndim = ARR_NDIM(v);
    dim = ARR_DIMS(v);
    nitems = ArrayGetNItems(ndim, dim);

    if nitems <= 0 {
        (*result).res = pushJsonbValue(
            &mut (*result).parseState,
            WJB_BEGIN_ARRAY,
            std::ptr::null_mut(),
        );
        (*result).res = pushJsonbValue(
            &mut (*result).parseState,
            WJB_END_ARRAY,
            std::ptr::null_mut(),
        );
        return;
    }

    get_typlenbyvalalign(element_type, &mut typlen, &mut typbyval, &mut typalign);

    json_categorize_type(element_type, true, &mut tcategory, &mut outfuncoid);

    deconstruct_array(
        v,
        element_type,
        typlen,
        typbyval,
        typalign,
        &mut elements,
        &mut nulls,
        &mut nitems,
    );

    array_dim_to_jsonb(
        result,
        0,
        ndim,
        dim,
        elements,
        nulls,
        &mut count,
        tcategory,
        outfuncoid,
    );

    pfree(elements as *mut c_void);
    pfree(nulls as *mut c_void);
}

/*
 * Turn a composite / record into JSON.
 */
unsafe fn composite_to_jsonb(composite: Datum, result: *mut JsonbInState) {
    let td: HeapTupleHeader;
    let tupType: Oid;
    let tupTypmod: int32;
    let tupdesc: TupleDesc;
    let mut tmptup: HeapTupleData = core::mem::zeroed();
    let tuple: HeapTuple;
    let mut i: c_int;

    td = DatumGetHeapTupleHeader(composite);

    /* Extract rowtype info and find a tupdesc */
    tupType = HeapTupleHeaderGetTypeId(td);
    tupTypmod = HeapTupleHeaderGetTypMod(td);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    /* Build a temporary HeapTuple control structure */
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;
    tuple = &mut tmptup;

    (*result).res = pushJsonbValue(
        &mut (*result).parseState,
        WJB_BEGIN_OBJECT,
        std::ptr::null_mut(),
    );

    i = 0;
    while i < (*tupdesc).natts {
        let val: Datum;
        let mut isnull: bool = false;
        let attname: *mut c_char;
        let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
        let mut outfuncoid: Oid = InvalidOid;
        let mut v: JsonbValue = core::mem::zeroed();
        let att: Form_pg_attribute = TupleDescAttr(tupdesc, i);

        if att_isdropped(att) {
            i += 1;
            continue;
        }

        attname = att_name(att);

        v.type_ = jbvString;
        /* don't need checkStringLen here - can't exceed maximum name length */
        v.val.string.len = strlen(attname) as c_int;
        v.val.string.val = attname;

        (*result).res = pushJsonbValue(&mut (*result).parseState, WJB_KEY, &mut v);

        val = heap_getattr(tuple, i + 1, tupdesc, &mut isnull);

        if isnull {
            tcategory = JSONTYPE_NULL;
            outfuncoid = InvalidOid;
        } else {
            json_categorize_type(att_typid(att), true, &mut tcategory, &mut outfuncoid);
        }

        datum_to_jsonb_internal(val, isnull, result, tcategory, outfuncoid, false);

        i += 1;
    }

    (*result).res = pushJsonbValue(
        &mut (*result).parseState,
        WJB_END_OBJECT,
        std::ptr::null_mut(),
    );
    ReleaseTupleDesc(tupdesc);
}

/*
 * Append JSON text for "val" to "result".
 *
 * This is just a thin wrapper around datum_to_jsonb.  If the same type will be
 * printed many times, avoid using this; better to do the json_categorize_type
 * lookups only once.
 */
unsafe fn add_jsonb(
    val: Datum,
    is_null: bool,
    result: *mut JsonbInState,
    val_type: Oid,
    key_scalar: bool,
) {
    let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
    let mut outfuncoid: Oid = InvalidOid;

    if val_type == InvalidOid {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        ereport!(ERROR, errmsg!("could not determine input data type"));
    }

    if is_null {
        tcategory = JSONTYPE_NULL;
        outfuncoid = InvalidOid;
    } else {
        json_categorize_type(val_type, true, &mut tcategory, &mut outfuncoid);
    }

    datum_to_jsonb_internal(val, is_null, result, tcategory, outfuncoid, key_scalar);
}

/*
 * Is the given type immutable when coming out of a JSONB context?
 *
 * At present, datetimes are all considered mutable, because they
 * depend on timezone.  XXX we should also drill down into objects and
 * arrays, but do not.
 */
pub unsafe fn to_jsonb_is_immutable(typoid: Oid) -> bool {
    let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
    let mut outfuncoid: Oid = InvalidOid;

    json_categorize_type(typoid, true, &mut tcategory, &mut outfuncoid);

    match tcategory {
        JSONTYPE_NULL | JSONTYPE_BOOL | JSONTYPE_JSON | JSONTYPE_JSONB => true,

        JSONTYPE_DATE | JSONTYPE_TIMESTAMP | JSONTYPE_TIMESTAMPTZ => false,

        JSONTYPE_ARRAY => false, /* TODO recurse into elements */

        JSONTYPE_COMPOSITE => false, /* TODO recurse into fields */

        JSONTYPE_NUMERIC | JSONTYPE_CAST | JSONTYPE_OTHER => {
            func_volatile(outfuncoid) == PROVOLATILE_IMMUTABLE
        }
    }
}

/*
 * SQL function to_jsonb(anyvalue)
 */
pub unsafe fn to_jsonb(fcinfo: FunctionCallInfo) -> Datum {
    let val: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let val_type: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 0);
    let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
    let mut outfuncoid: Oid = InvalidOid;

    if val_type == InvalidOid {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        ereport!(ERROR, errmsg!("could not determine input data type"));
    }

    json_categorize_type(val_type, true, &mut tcategory, &mut outfuncoid);

    PG_RETURN_DATUM!(datum_to_jsonb(val, tcategory, outfuncoid));
}

/*
 * Turn a Datum into jsonb.
 *
 * tcategory and outfuncoid are from a previous call to json_categorize_type.
 */
pub unsafe fn datum_to_jsonb(val: Datum, tcategory: JsonTypeCategory, outfuncoid: Oid) -> Datum {
    let mut result: JsonbInState = core::mem::zeroed();

    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    datum_to_jsonb_internal(val, false, &mut result, tcategory, outfuncoid, false);

    JsonbPGetDatum(JsonbValueToJsonb(result.res))
}

pub unsafe fn jsonb_build_object_worker(
    nargs: c_int,
    args: *const Datum,
    nulls: *const bool,
    types: *const Oid,
    absent_on_null: bool,
    unique_keys: bool,
) -> Datum {
    let mut i: c_int;
    let mut result: JsonbInState = core::mem::zeroed();

    if nargs % 2 != 0 {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        // errhint("The arguments of %s must consist of alternating keys and
        // values.", "jsonb_build_object()")
        ereport!(
            ERROR,
            errmsg!("argument list must have even number of elements")
        );
    }

    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    result.res = pushJsonbValue(
        &mut result.parseState,
        WJB_BEGIN_OBJECT,
        std::ptr::null_mut(),
    );
    (*result.parseState).unique_keys = unique_keys;
    (*result.parseState).skip_nulls = absent_on_null;

    i = 0;
    while i < nargs {
        /* process key */
        let skip: bool;

        if *nulls.add(i as usize) {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(
                ERROR,
                errmsg!("argument {}: key must not be null", i + 1)
            );
        }

        /* skip null values if absent_on_null */
        skip = absent_on_null && *nulls.add((i + 1) as usize);

        /* we need to save skipped keys for the key uniqueness check */
        if skip && !unique_keys {
            i += 2;
            continue;
        }

        add_jsonb(*args.add(i as usize), false, &mut result, *types.add(i as usize), true);

        /* process value */
        add_jsonb(
            *args.add((i + 1) as usize),
            *nulls.add((i + 1) as usize),
            &mut result,
            *types.add((i + 1) as usize),
            false,
        );

        i += 2;
    }

    result.res = pushJsonbValue(&mut result.parseState, WJB_END_OBJECT, std::ptr::null_mut());

    JsonbPGetDatum(JsonbValueToJsonb(result.res))
}

/*
 * SQL function jsonb_build_object(variadic "any")
 */
pub unsafe fn jsonb_build_object(fcinfo: FunctionCallInfo) -> Datum {
    let mut args: *mut Datum = std::ptr::null_mut();
    let mut nulls: *mut bool = std::ptr::null_mut();
    let mut types: *mut Oid = std::ptr::null_mut();

    /* build argument values to build the object */
    let nargs: c_int =
        extract_variadic_args(fcinfo, 0, true, &mut args, &mut types, &mut nulls);

    if nargs < 0 {
        PG_RETURN_NULL!();
    }

    PG_RETURN_DATUM!(jsonb_build_object_worker(
        nargs, args, nulls, types, false, false
    ));
}

/*
 * degenerate case of jsonb_build_object where it gets 0 arguments.
 */
pub unsafe fn jsonb_build_object_noargs(_fcinfo: FunctionCallInfo) -> Datum {
    let mut result: JsonbInState = core::mem::zeroed();

    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    pushJsonbValue(&mut result.parseState, WJB_BEGIN_OBJECT, std::ptr::null_mut());
    result.res = pushJsonbValue(&mut result.parseState, WJB_END_OBJECT, std::ptr::null_mut());

    PG_RETURN_POINTER!(JsonbValueToJsonb(result.res));
}

pub unsafe fn jsonb_build_array_worker(
    nargs: c_int,
    args: *const Datum,
    nulls: *const bool,
    types: *const Oid,
    absent_on_null: bool,
) -> Datum {
    let mut i: c_int;
    let mut result: JsonbInState = core::mem::zeroed();

    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    result.res = pushJsonbValue(&mut result.parseState, WJB_BEGIN_ARRAY, std::ptr::null_mut());

    i = 0;
    while i < nargs {
        if absent_on_null && *nulls.add(i as usize) {
            i += 1;
            continue;
        }

        add_jsonb(
            *args.add(i as usize),
            *nulls.add(i as usize),
            &mut result,
            *types.add(i as usize),
            false,
        );

        i += 1;
    }

    result.res = pushJsonbValue(&mut result.parseState, WJB_END_ARRAY, std::ptr::null_mut());

    JsonbPGetDatum(JsonbValueToJsonb(result.res))
}

/*
 * SQL function jsonb_build_array(variadic "any")
 */
pub unsafe fn jsonb_build_array(fcinfo: FunctionCallInfo) -> Datum {
    let mut args: *mut Datum = std::ptr::null_mut();
    let mut nulls: *mut bool = std::ptr::null_mut();
    let mut types: *mut Oid = std::ptr::null_mut();

    /* build argument values to build the object */
    let nargs: c_int =
        extract_variadic_args(fcinfo, 0, true, &mut args, &mut types, &mut nulls);

    if nargs < 0 {
        PG_RETURN_NULL!();
    }

    PG_RETURN_DATUM!(jsonb_build_array_worker(nargs, args, nulls, types, false));
}

/*
 * degenerate case of jsonb_build_array where it gets 0 arguments.
 */
pub unsafe fn jsonb_build_array_noargs(_fcinfo: FunctionCallInfo) -> Datum {
    let mut result: JsonbInState = core::mem::zeroed();

    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    pushJsonbValue(&mut result.parseState, WJB_BEGIN_ARRAY, std::ptr::null_mut());
    result.res = pushJsonbValue(&mut result.parseState, WJB_END_ARRAY, std::ptr::null_mut());

    PG_RETURN_POINTER!(JsonbValueToJsonb(result.res));
}

/*
 * SQL function jsonb_object(text[])
 *
 * take a one or two dimensional array of text as name value pairs
 * for a jsonb object.
 *
 */
pub unsafe fn jsonb_object(fcinfo: FunctionCallInfo) -> Datum {
    let in_array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let ndims: c_int = ARR_NDIM(in_array);
    let mut in_datums: *mut Datum = std::ptr::null_mut();
    let mut in_nulls: *mut bool = std::ptr::null_mut();
    let mut in_count: c_int = 0;
    let count: c_int;
    let mut i: c_int;
    let mut result: JsonbInState = core::mem::zeroed();

    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    pushJsonbValue(&mut result.parseState, WJB_BEGIN_OBJECT, std::ptr::null_mut());

    'close_object: {
        match ndims {
            0 => {
                break 'close_object;
            }
            1 => {
                if (*ARR_DIMS(in_array).add(0)) % 2 != 0 {
                    // C also: errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                    ereport!(ERROR, errmsg!("array must have even number of elements"));
                }
            }
            2 => {
                if (*ARR_DIMS(in_array).add(1)) != 2 {
                    // C also: errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                    ereport!(ERROR, errmsg!("array must have two columns"));
                }
            }
            _ => {
                // C also: errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
                ereport!(ERROR, errmsg!("wrong number of array subscripts"));
            }
        }

        deconstruct_array_builtin(in_array, TEXTOID, &mut in_datums, &mut in_nulls, &mut in_count);

        count = in_count / 2;

        i = 0;
        while i < count {
            let mut v: JsonbValue = core::mem::zeroed();
            let mut str: *mut c_char;
            let mut len: c_int;

            if *in_nulls.add((i * 2) as usize) {
                // C also: errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                ereport!(ERROR, errmsg!("null value not allowed for object key"));
            }

            str = TextDatumGetCString(*in_datums.add((i * 2) as usize));
            len = strlen(str) as c_int;

            v.type_ = jbvString;

            v.val.string.len = len;
            v.val.string.val = str;

            pushJsonbValue(&mut result.parseState, WJB_KEY, &mut v);

            if *in_nulls.add((i * 2 + 1) as usize) {
                v.type_ = jbvNull;
            } else {
                str = TextDatumGetCString(*in_datums.add((i * 2 + 1) as usize));
                len = strlen(str) as c_int;

                v.type_ = jbvString;

                v.val.string.len = len;
                v.val.string.val = str;
            }

            pushJsonbValue(&mut result.parseState, WJB_VALUE, &mut v);

            i += 1;
        }

        pfree(in_datums as *mut c_void);
        pfree(in_nulls as *mut c_void);
    }

    // close_object:
    result.res = pushJsonbValue(&mut result.parseState, WJB_END_OBJECT, std::ptr::null_mut());

    PG_RETURN_POINTER!(JsonbValueToJsonb(result.res));
}

/*
 * SQL function jsonb_object(text[], text[])
 *
 * take separate name and value arrays of text to construct a jsonb object
 * pairwise.
 */
pub unsafe fn jsonb_object_two_arg(fcinfo: FunctionCallInfo) -> Datum {
    let key_array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let val_array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);
    let nkdims: c_int = ARR_NDIM(key_array);
    let nvdims: c_int = ARR_NDIM(val_array);
    let mut key_datums: *mut Datum = std::ptr::null_mut();
    let mut val_datums: *mut Datum = std::ptr::null_mut();
    let mut key_nulls: *mut bool = std::ptr::null_mut();
    let mut val_nulls: *mut bool = std::ptr::null_mut();
    let mut key_count: c_int = 0;
    let mut val_count: c_int = 0;
    let mut i: c_int;
    let mut result: JsonbInState = core::mem::zeroed();

    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    pushJsonbValue(&mut result.parseState, WJB_BEGIN_OBJECT, std::ptr::null_mut());

    'close_object: {
        if nkdims > 1 || nkdims != nvdims {
            // C also: errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            ereport!(ERROR, errmsg!("wrong number of array subscripts"));
        }

        if nkdims == 0 {
            break 'close_object;
        }

        deconstruct_array_builtin(key_array, TEXTOID, &mut key_datums, &mut key_nulls, &mut key_count);
        deconstruct_array_builtin(val_array, TEXTOID, &mut val_datums, &mut val_nulls, &mut val_count);

        if key_count != val_count {
            // C also: errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            ereport!(ERROR, errmsg!("mismatched array dimensions"));
        }

        i = 0;
        while i < key_count {
            let mut v: JsonbValue = core::mem::zeroed();
            let mut str: *mut c_char;
            let mut len: c_int;

            if *key_nulls.add(i as usize) {
                // C also: errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED)
                ereport!(ERROR, errmsg!("null value not allowed for object key"));
            }

            str = TextDatumGetCString(*key_datums.add(i as usize));
            len = strlen(str) as c_int;

            v.type_ = jbvString;

            v.val.string.len = len;
            v.val.string.val = str;

            pushJsonbValue(&mut result.parseState, WJB_KEY, &mut v);

            if *val_nulls.add(i as usize) {
                v.type_ = jbvNull;
            } else {
                str = TextDatumGetCString(*val_datums.add(i as usize));
                len = strlen(str) as c_int;

                v.type_ = jbvString;

                v.val.string.len = len;
                v.val.string.val = str;
            }

            pushJsonbValue(&mut result.parseState, WJB_VALUE, &mut v);

            i += 1;
        }

        pfree(key_datums as *mut c_void);
        pfree(key_nulls as *mut c_void);
        pfree(val_datums as *mut c_void);
        pfree(val_nulls as *mut c_void);
    }

    // close_object:
    result.res = pushJsonbValue(&mut result.parseState, WJB_END_OBJECT, std::ptr::null_mut());

    PG_RETURN_POINTER!(JsonbValueToJsonb(result.res));
}

/*
 * shallow clone of a parse state, suitable for use in aggregate
 * final functions that will only append to the values rather than
 * change them.
 */
unsafe fn clone_parse_state(state: *mut JsonbParseState) -> *mut JsonbParseState {
    let result: *mut JsonbParseState;
    let mut icursor: *mut JsonbParseState;
    let mut ocursor: *mut JsonbParseState;

    if state.is_null() {
        return std::ptr::null_mut();
    }

    result = palloc(core::mem::size_of::<JsonbParseState>()) as *mut JsonbParseState;
    icursor = state;
    ocursor = result;
    loop {
        (*ocursor).contVal = (*icursor).contVal;
        (*ocursor).size = (*icursor).size;
        (*ocursor).unique_keys = (*icursor).unique_keys;
        (*ocursor).skip_nulls = (*icursor).skip_nulls;
        icursor = (*icursor).next;
        if icursor.is_null() {
            break;
        }
        (*ocursor).next = palloc(core::mem::size_of::<JsonbParseState>()) as *mut JsonbParseState;
        ocursor = (*ocursor).next;
    }
    (*ocursor).next = std::ptr::null_mut();

    result
}

unsafe fn jsonb_agg_transfn_worker(fcinfo: FunctionCallInfo, absent_on_null: bool) -> Datum {
    let mut oldcontext: MemoryContext;
    let mut aggcontext: MemoryContext = std::ptr::null_mut();
    let state: *mut JsonbAggState;
    let mut elem: JsonbInState = core::mem::zeroed();
    let val: Datum;
    let result: *mut JsonbInState;
    let mut single_scalar = false;
    let mut it: *mut JsonbIterator;
    let jbelem: *mut Jsonb;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r#type: JsonbIteratorToken;

    if !AggCheckCallContext(fcinfo, &mut aggcontext) {
        /* cannot be called directly because of internal-type argument */
        elog!(ERROR, "jsonb_agg_transfn called in non-aggregate context");
    }

    /* set up the accumulator on the first go round */

    if PG_ARGISNULL!(fcinfo, 0) {
        let arg_type: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 1);

        if arg_type == InvalidOid {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(ERROR, errmsg!("could not determine input data type"));
        }

        oldcontext = MemoryContextSwitchTo(aggcontext);
        state = palloc(core::mem::size_of::<JsonbAggState>()) as *mut JsonbAggState;
        result = palloc0(core::mem::size_of::<JsonbInState>()) as *mut JsonbInState;
        (*state).res = result;
        (*result).res = pushJsonbValue(
            &mut (*result).parseState,
            WJB_BEGIN_ARRAY,
            std::ptr::null_mut(),
        );
        MemoryContextSwitchTo(oldcontext);

        json_categorize_type(
            arg_type,
            true,
            &mut (*state).val_category,
            &mut (*state).val_output_func,
        );
    } else {
        state = PG_GETARG_POINTER!(fcinfo, 0) as *mut JsonbAggState;
        result = (*state).res;
    }

    if absent_on_null && PG_ARGISNULL!(fcinfo, 1) {
        PG_RETURN_POINTER!(state);
    }

    /* turn the argument into jsonb in the normal function context */

    val = if PG_ARGISNULL!(fcinfo, 1) {
        0 as Datum
    } else {
        PG_GETARG_DATUM!(fcinfo, 1)
    };

    memset(
        &mut elem as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    datum_to_jsonb_internal(
        val,
        PG_ARGISNULL!(fcinfo, 1),
        &mut elem,
        (*state).val_category,
        (*state).val_output_func,
        false,
    );

    jbelem = JsonbValueToJsonb(elem.res);

    /* switch to the aggregate context for accumulation operations */

    oldcontext = MemoryContextSwitchTo(aggcontext);

    it = JsonbIteratorInit(&mut (*jbelem).root);

    while {
        r#type = JsonbIteratorNext(&mut it, &mut v, false);
        r#type != WJB_DONE
    } {
        match r#type {
            WJB_BEGIN_ARRAY => {
                if v.val.array.rawScalar {
                    single_scalar = true;
                } else {
                    (*result).res =
                        pushJsonbValue(&mut (*result).parseState, r#type, std::ptr::null_mut());
                }
            }
            WJB_END_ARRAY => {
                if !single_scalar {
                    (*result).res =
                        pushJsonbValue(&mut (*result).parseState, r#type, std::ptr::null_mut());
                }
            }
            WJB_BEGIN_OBJECT | WJB_END_OBJECT => {
                (*result).res =
                    pushJsonbValue(&mut (*result).parseState, r#type, std::ptr::null_mut());
            }
            WJB_ELEM | WJB_KEY | WJB_VALUE => {
                if v.type_ == jbvString {
                    /* copy string values in the aggregate context */
                    let buf: *mut c_char = palloc((v.val.string.len + 1) as usize) as *mut c_char;

                    snprintf(
                        buf,
                        (v.val.string.len + 1) as usize,
                        c"%s".as_ptr(),
                        v.val.string.val,
                    );
                    v.val.string.val = buf;
                } else if v.type_ == jbvNumeric {
                    /* same for numeric */
                    v.val.numeric = DatumGetNumeric(DirectFunctionCall1!(
                        numeric_uplus as PGFunction,
                        NumericGetDatum(v.val.numeric)
                    ));
                }
                (*result).res = pushJsonbValue(&mut (*result).parseState, r#type, &mut v);
            }
            _ => {
                elog!(ERROR, "unknown jsonb iterator token type");
            }
        }
    }

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER!(state);
}

/*
 * jsonb_agg aggregate function
 */
pub unsafe fn jsonb_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_agg_transfn_worker(fcinfo, false)
}

/*
 * jsonb_agg_strict aggregate function
 */
pub unsafe fn jsonb_agg_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_agg_transfn_worker(fcinfo, true)
}

pub unsafe fn jsonb_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut JsonbAggState;
    let mut result: JsonbInState = core::mem::zeroed();
    let out: *mut Jsonb;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, std::ptr::null_mut()));

    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(); /* returns null iff no input values */
    }

    arg = PG_GETARG_POINTER!(fcinfo, 0) as *mut JsonbAggState;

    /*
     * We need to do a shallow clone of the argument in case the final
     * function is called more than once, so we avoid changing the argument. A
     * shallow clone is sufficient as we aren't going to change any of the
     * values, just add the final array end marker.
     */
    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    result.parseState = clone_parse_state((*(*arg).res).parseState);

    result.res = pushJsonbValue(&mut result.parseState, WJB_END_ARRAY, std::ptr::null_mut());

    out = JsonbValueToJsonb(result.res);

    PG_RETURN_POINTER!(out);
}

unsafe fn jsonb_object_agg_transfn_worker(
    fcinfo: FunctionCallInfo,
    absent_on_null: bool,
    unique_keys: bool,
) -> Datum {
    let mut oldcontext: MemoryContext;
    let mut aggcontext: MemoryContext = std::ptr::null_mut();
    let mut elem: JsonbInState = core::mem::zeroed();
    let state: *mut JsonbAggState;
    let mut val: Datum;
    let result: *mut JsonbInState;
    let mut single_scalar: bool;
    let mut it: *mut JsonbIterator;
    let jbkey: *mut Jsonb;
    let jbval: *mut Jsonb;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r#type: JsonbIteratorToken;
    let skip: bool;

    if !AggCheckCallContext(fcinfo, &mut aggcontext) {
        /* cannot be called directly because of internal-type argument */
        elog!(ERROR, "jsonb_object_agg_transfn called in non-aggregate context");
    }

    /* set up the accumulator on the first go round */

    if PG_ARGISNULL!(fcinfo, 0) {
        let mut arg_type: Oid;

        oldcontext = MemoryContextSwitchTo(aggcontext);
        state = palloc(core::mem::size_of::<JsonbAggState>()) as *mut JsonbAggState;
        result = palloc0(core::mem::size_of::<JsonbInState>()) as *mut JsonbInState;
        (*state).res = result;
        (*result).res = pushJsonbValue(
            &mut (*result).parseState,
            WJB_BEGIN_OBJECT,
            std::ptr::null_mut(),
        );
        (*(*result).parseState).unique_keys = unique_keys;
        (*(*result).parseState).skip_nulls = absent_on_null;

        MemoryContextSwitchTo(oldcontext);

        arg_type = get_fn_expr_argtype((*fcinfo).flinfo, 1);

        if arg_type == InvalidOid {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(ERROR, errmsg!("could not determine input data type"));
        }

        json_categorize_type(
            arg_type,
            true,
            &mut (*state).key_category,
            &mut (*state).key_output_func,
        );

        arg_type = get_fn_expr_argtype((*fcinfo).flinfo, 2);

        if arg_type == InvalidOid {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            ereport!(ERROR, errmsg!("could not determine input data type"));
        }

        json_categorize_type(
            arg_type,
            true,
            &mut (*state).val_category,
            &mut (*state).val_output_func,
        );
    } else {
        state = PG_GETARG_POINTER!(fcinfo, 0) as *mut JsonbAggState;
        result = (*state).res;
    }

    /* turn the argument into jsonb in the normal function context */

    if PG_ARGISNULL!(fcinfo, 1) {
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        ereport!(ERROR, errmsg!("field name must not be null"));
    }

    /*
     * Skip null values if absent_on_null unless key uniqueness check is
     * needed (because we must save keys in this case).
     */
    skip = absent_on_null && PG_ARGISNULL!(fcinfo, 2);

    if skip && !unique_keys {
        PG_RETURN_POINTER!(state);
    }

    val = PG_GETARG_DATUM!(fcinfo, 1);

    memset(
        &mut elem as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    datum_to_jsonb_internal(
        val,
        false,
        &mut elem,
        (*state).key_category,
        (*state).key_output_func,
        true,
    );

    jbkey = JsonbValueToJsonb(elem.res);

    val = if PG_ARGISNULL!(fcinfo, 2) {
        0 as Datum
    } else {
        PG_GETARG_DATUM!(fcinfo, 2)
    };

    memset(
        &mut elem as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    datum_to_jsonb_internal(
        val,
        PG_ARGISNULL!(fcinfo, 2),
        &mut elem,
        (*state).val_category,
        (*state).val_output_func,
        false,
    );

    jbval = JsonbValueToJsonb(elem.res);

    it = JsonbIteratorInit(&mut (*jbkey).root);

    /* switch to the aggregate context for accumulation operations */

    oldcontext = MemoryContextSwitchTo(aggcontext);

    /*
     * keys should be scalar, and we should have already checked for that
     * above when calling datum_to_jsonb, so we only need to look for these
     * things.
     */

    while {
        r#type = JsonbIteratorNext(&mut it, &mut v, false);
        r#type != WJB_DONE
    } {
        match r#type {
            WJB_BEGIN_ARRAY => {
                if !v.val.array.rawScalar {
                    elog!(ERROR, "unexpected structure for key");
                }
            }
            WJB_ELEM => {
                if v.type_ == jbvString {
                    /* copy string values in the aggregate context */
                    let buf: *mut c_char = palloc((v.val.string.len + 1) as usize) as *mut c_char;

                    snprintf(
                        buf,
                        (v.val.string.len + 1) as usize,
                        c"%s".as_ptr(),
                        v.val.string.val,
                    );
                    v.val.string.val = buf;
                } else {
                    // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    ereport!(ERROR, errmsg!("object keys must be strings"));
                }
                (*result).res = pushJsonbValue(&mut (*result).parseState, WJB_KEY, &mut v);

                if skip {
                    v.type_ = jbvNull;
                    (*result).res = pushJsonbValue(&mut (*result).parseState, WJB_VALUE, &mut v);
                    MemoryContextSwitchTo(oldcontext);
                    PG_RETURN_POINTER!(state);
                }
            }
            WJB_END_ARRAY => {}
            _ => {
                elog!(ERROR, "unexpected structure for key");
            }
        }
    }

    it = JsonbIteratorInit(&mut (*jbval).root);

    single_scalar = false;

    /*
     * values can be anything, including structured and null, so we treat them
     * as in json_agg_transfn, except that single scalars are always pushed as
     * WJB_VALUE items.
     */

    while {
        r#type = JsonbIteratorNext(&mut it, &mut v, false);
        r#type != WJB_DONE
    } {
        match r#type {
            WJB_BEGIN_ARRAY => {
                if v.val.array.rawScalar {
                    single_scalar = true;
                } else {
                    (*result).res =
                        pushJsonbValue(&mut (*result).parseState, r#type, std::ptr::null_mut());
                }
            }
            WJB_END_ARRAY => {
                if !single_scalar {
                    (*result).res =
                        pushJsonbValue(&mut (*result).parseState, r#type, std::ptr::null_mut());
                }
            }
            WJB_BEGIN_OBJECT | WJB_END_OBJECT => {
                (*result).res =
                    pushJsonbValue(&mut (*result).parseState, r#type, std::ptr::null_mut());
            }
            WJB_ELEM | WJB_KEY | WJB_VALUE => {
                if v.type_ == jbvString {
                    /* copy string values in the aggregate context */
                    let buf: *mut c_char = palloc((v.val.string.len + 1) as usize) as *mut c_char;

                    snprintf(
                        buf,
                        (v.val.string.len + 1) as usize,
                        c"%s".as_ptr(),
                        v.val.string.val,
                    );
                    v.val.string.val = buf;
                } else if v.type_ == jbvNumeric {
                    /* same for numeric */
                    v.val.numeric = DatumGetNumeric(DirectFunctionCall1!(
                        numeric_uplus as PGFunction,
                        NumericGetDatum(v.val.numeric)
                    ));
                }
                (*result).res = pushJsonbValue(
                    &mut (*result).parseState,
                    if single_scalar { WJB_VALUE } else { r#type },
                    &mut v,
                );
            }
            _ => {
                elog!(ERROR, "unknown jsonb iterator token type");
            }
        }
    }

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_POINTER!(state);
}

/*
 * jsonb_object_agg aggregate function
 */
pub unsafe fn jsonb_object_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_object_agg_transfn_worker(fcinfo, false, false)
}

/*
 * jsonb_object_agg_strict aggregate function
 */
pub unsafe fn jsonb_object_agg_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_object_agg_transfn_worker(fcinfo, true, false)
}

/*
 * jsonb_object_agg_unique aggregate function
 */
pub unsafe fn jsonb_object_agg_unique_transfn(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_object_agg_transfn_worker(fcinfo, false, true)
}

/*
 * jsonb_object_agg_unique_strict aggregate function
 */
pub unsafe fn jsonb_object_agg_unique_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    jsonb_object_agg_transfn_worker(fcinfo, true, true)
}

pub unsafe fn jsonb_object_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let arg: *mut JsonbAggState;
    let mut result: JsonbInState = core::mem::zeroed();
    let out: *mut Jsonb;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, std::ptr::null_mut()));

    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(); /* returns null iff no input values */
    }

    arg = PG_GETARG_POINTER!(fcinfo, 0) as *mut JsonbAggState;

    /*
     * We need to do a shallow clone of the argument's res field in case the
     * final function is called more than once, so we avoid changing the
     * aggregate state value.  A shallow clone is sufficient as we aren't
     * going to change any of the values, just add the final object end
     * marker.
     */
    memset(
        &mut result as *mut _ as *mut c_void,
        0,
        core::mem::size_of::<JsonbInState>(),
    );

    result.parseState = clone_parse_state((*(*arg).res).parseState);

    result.res = pushJsonbValue(&mut result.parseState, WJB_END_OBJECT, std::ptr::null_mut());

    out = JsonbValueToJsonb(result.res);

    PG_RETURN_POINTER!(out);
}

/*
 * Extract scalar value from raw-scalar pseudo-array jsonb.
 */
pub unsafe fn JsonbExtractScalar(jbc: *mut JsonbContainer, res: *mut JsonbValue) -> bool {
    let mut it: *mut JsonbIterator;
    #[allow(unused_variables)]
    let mut tok: JsonbIteratorToken;
    let mut tmp: JsonbValue = core::mem::zeroed();

    if !crate::utils::adt::jsonb_util::JsonContainerIsArray(jbc)
        || !crate::utils::adt::jsonb_util::JsonContainerIsScalar(jbc)
    {
        /* inform caller about actual type of container */
        (*res).type_ = if JsonContainerIsArray(jbc) {
            jbvArray
        } else {
            jbvObject
        };
        return false;
    }

    /*
     * A root scalar is stored as an array of one element, so we get the array
     * and then its first (and only) member.
     */
    it = JsonbIteratorInit(jbc);

    tok = JsonbIteratorNext(&mut it, &mut tmp, true);
    Assert!(tok == WJB_BEGIN_ARRAY);
    Assert!(tmp.val.array.nElems == 1 && tmp.val.array.rawScalar);

    tok = JsonbIteratorNext(&mut it, res, true);
    Assert!(tok == WJB_ELEM);
    Assert!(IsAJsonbScalar(res));

    tok = JsonbIteratorNext(&mut it, &mut tmp, true);
    Assert!(tok == WJB_END_ARRAY);

    tok = JsonbIteratorNext(&mut it, &mut tmp, true);
    Assert!(tok == WJB_DONE);

    true
}

/*
 * Emit correct, translatable cast error message
 */
unsafe fn cannotCastJsonbValue(r#type: jbvType, sqltype: *const c_char) {
    struct MsgEntry {
        r#type: jbvType,
        msg: &'static str,
    }
    // C also: gettext_noop() wraps each message for translation.
    static MESSAGES: &[MsgEntry] = &[
        MsgEntry { r#type: jbvNull, msg: "cannot cast jsonb null to type {}" },
        MsgEntry { r#type: jbvString, msg: "cannot cast jsonb string to type {}" },
        MsgEntry { r#type: jbvNumeric, msg: "cannot cast jsonb numeric to type {}" },
        MsgEntry { r#type: jbvBool, msg: "cannot cast jsonb boolean to type {}" },
        MsgEntry { r#type: jbvArray, msg: "cannot cast jsonb array to type {}" },
        MsgEntry { r#type: jbvObject, msg: "cannot cast jsonb object to type {}" },
        MsgEntry { r#type: jbvBinary, msg: "cannot cast jsonb array or object to type {}" },
    ];
    let mut i: c_int;

    let sql = CStr::from_ptr(sqltype).to_string_lossy();
    i = 0;
    while (i as usize) < MESSAGES.len() {
        if MESSAGES[i as usize].r#type == r#type {
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            match MESSAGES[i as usize].r#type {
                jbvNull => ereport!(ERROR, errmsg!("cannot cast jsonb null to type {}", sql)),
                jbvString => ereport!(ERROR, errmsg!("cannot cast jsonb string to type {}", sql)),
                jbvNumeric => ereport!(ERROR, errmsg!("cannot cast jsonb numeric to type {}", sql)),
                jbvBool => ereport!(ERROR, errmsg!("cannot cast jsonb boolean to type {}", sql)),
                jbvArray => ereport!(ERROR, errmsg!("cannot cast jsonb array to type {}", sql)),
                jbvObject => ereport!(ERROR, errmsg!("cannot cast jsonb object to type {}", sql)),
                _ => ereport!(
                    ERROR,
                    errmsg!("cannot cast jsonb array or object to type {}", sql)
                ),
            }
        }
        i += 1;
    }

    /* should be unreachable */
    elog!(ERROR, "unknown jsonb type: {}", r#type as c_int);
}

pub unsafe fn jsonb_bool(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let mut v: JsonbValue = core::mem::zeroed();

    if !JsonbExtractScalar(&mut (*r#in).root, &mut v) {
        cannotCastJsonbValue(v.type_, c"boolean".as_ptr());
    }

    if v.type_ == jbvNull {
        PG_FREE_IF_COPY!(r#in, 0);
        PG_RETURN_NULL!();
    }

    if v.type_ != jbvBool {
        cannotCastJsonbValue(v.type_, c"boolean".as_ptr());
    }

    PG_FREE_IF_COPY!(r#in, 0);

    PG_RETURN_BOOL!(v.val.boolean);
}

pub unsafe fn jsonb_numeric(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let mut v: JsonbValue = core::mem::zeroed();
    let retValue: Numeric;

    if !JsonbExtractScalar(&mut (*r#in).root, &mut v) {
        cannotCastJsonbValue(v.type_, c"numeric".as_ptr());
    }

    if v.type_ == jbvNull {
        PG_FREE_IF_COPY!(r#in, 0);
        PG_RETURN_NULL!();
    }

    if v.type_ != jbvNumeric {
        cannotCastJsonbValue(v.type_, c"numeric".as_ptr());
    }

    /*
     * v.val.numeric points into jsonb body, so we need to make a copy to
     * return
     */
    retValue = DatumGetNumericCopy(NumericGetDatum(v.val.numeric));

    PG_FREE_IF_COPY!(r#in, 0);

    PG_RETURN_NUMERIC!(retValue);
}

pub unsafe fn jsonb_int2(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let mut v: JsonbValue = core::mem::zeroed();
    let retValue: Datum;

    if !JsonbExtractScalar(&mut (*r#in).root, &mut v) {
        cannotCastJsonbValue(v.type_, c"smallint".as_ptr());
    }

    if v.type_ == jbvNull {
        PG_FREE_IF_COPY!(r#in, 0);
        PG_RETURN_NULL!();
    }

    if v.type_ != jbvNumeric {
        cannotCastJsonbValue(v.type_, c"smallint".as_ptr());
    }

    retValue = DirectFunctionCall1!(numeric_int2 as PGFunction, NumericGetDatum(v.val.numeric));

    PG_FREE_IF_COPY!(r#in, 0);

    PG_RETURN_DATUM!(retValue);
}

pub unsafe fn jsonb_int4(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let mut v: JsonbValue = core::mem::zeroed();
    let retValue: Datum;

    if !JsonbExtractScalar(&mut (*r#in).root, &mut v) {
        cannotCastJsonbValue(v.type_, c"integer".as_ptr());
    }

    if v.type_ == jbvNull {
        PG_FREE_IF_COPY!(r#in, 0);
        PG_RETURN_NULL!();
    }

    if v.type_ != jbvNumeric {
        cannotCastJsonbValue(v.type_, c"integer".as_ptr());
    }

    retValue = DirectFunctionCall1!(numeric_int4 as PGFunction, NumericGetDatum(v.val.numeric));

    PG_FREE_IF_COPY!(r#in, 0);

    PG_RETURN_DATUM!(retValue);
}

pub unsafe fn jsonb_int8(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let mut v: JsonbValue = core::mem::zeroed();
    let retValue: Datum;

    if !JsonbExtractScalar(&mut (*r#in).root, &mut v) {
        cannotCastJsonbValue(v.type_, c"bigint".as_ptr());
    }

    if v.type_ == jbvNull {
        PG_FREE_IF_COPY!(r#in, 0);
        PG_RETURN_NULL!();
    }

    if v.type_ != jbvNumeric {
        cannotCastJsonbValue(v.type_, c"bigint".as_ptr());
    }

    retValue = DirectFunctionCall1!(numeric_int8 as PGFunction, NumericGetDatum(v.val.numeric));

    PG_FREE_IF_COPY!(r#in, 0);

    PG_RETURN_DATUM!(retValue);
}

pub unsafe fn jsonb_float4(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let mut v: JsonbValue = core::mem::zeroed();
    let retValue: Datum;

    if !JsonbExtractScalar(&mut (*r#in).root, &mut v) {
        cannotCastJsonbValue(v.type_, c"real".as_ptr());
    }

    if v.type_ == jbvNull {
        PG_FREE_IF_COPY!(r#in, 0);
        PG_RETURN_NULL!();
    }

    if v.type_ != jbvNumeric {
        cannotCastJsonbValue(v.type_, c"real".as_ptr());
    }

    retValue = DirectFunctionCall1!(numeric_float4 as PGFunction, NumericGetDatum(v.val.numeric));

    PG_FREE_IF_COPY!(r#in, 0);

    PG_RETURN_DATUM!(retValue);
}

pub unsafe fn jsonb_float8(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let mut v: JsonbValue = core::mem::zeroed();
    let retValue: Datum;

    if !JsonbExtractScalar(&mut (*r#in).root, &mut v) {
        cannotCastJsonbValue(v.type_, c"double precision".as_ptr());
    }

    if v.type_ == jbvNull {
        PG_FREE_IF_COPY!(r#in, 0);
        PG_RETURN_NULL!();
    }

    if v.type_ != jbvNumeric {
        cannotCastJsonbValue(v.type_, c"double precision".as_ptr());
    }

    retValue = DirectFunctionCall1!(numeric_float8 as PGFunction, NumericGetDatum(v.val.numeric));

    PG_FREE_IF_COPY!(r#in, 0);

    PG_RETURN_DATUM!(retValue);
}

/*
 * Convert jsonb to a C-string stripping quotes from scalar strings.
 */
pub unsafe fn JsonbUnquote(jb: *mut Jsonb) -> *mut c_char {
    if JB_ROOT_IS_SCALAR(jb) {
        let mut v: JsonbValue = core::mem::zeroed();

        JsonbExtractScalar(&mut (*jb).root, &mut v);

        if v.type_ == jbvString {
            pnstrdup(v.val.string.val, v.val.string.len as usize)
        } else if v.type_ == jbvBool {
            pstrdup(if v.val.boolean {
                c"true".as_ptr()
            } else {
                c"false".as_ptr()
            })
        } else if v.type_ == jbvNumeric {
            DatumGetCString(DirectFunctionCall1!(
                numeric_out as PGFunction,
                PointerGetDatum(v.val.numeric as *const c_void)
            ))
        } else if v.type_ == jbvNull {
            pstrdup(c"null".as_ptr())
        } else {
            elog!(ERROR, "unrecognized jsonb value type {}", v.type_ as c_int);
            #[allow(unreachable_code)]
            std::ptr::null_mut()
        }
    } else {
        JsonbToCString(std::ptr::null_mut(), &mut (*jb).root, VARSIZE(jb))
    }
}
