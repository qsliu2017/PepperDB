//! Translation of postgres/src/backend/utils/adt/json.c
//!
//! JSON data type support.  The SQL "json" type is stored as validated text: the
//! on-disk/in-memory representation is identical to `text`, and `json_in` /
//! `json_recv` merely run the input through the JSON lexer+parser to confirm it
//! is well-formed before storing the bytes verbatim.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h            -> crate::prelude
//!   catalog/pg_proc.h, catalog/pg_type.h, utils/fmgroids.h
//!                         -> Oid constants (F_TEXTOUT etc.); used only by the
//!                            stubbed function-library code below.
//!   common/hashfn.h       -> crate::common::hashfn (key-uniqueness hashing; only
//!                            referenced by the stubbed builder/agg code).
//!   funcapi.h             -> SRF/agg support (stubbed).
//!   libpq/pqformat.h      -> crate::libpq::pqformat (pq_begintypsend/pq_sendtext/
//!                            pq_endtypsend/pq_getmsgtext) -- used by json_send/recv.
//!   miscadmin.h           -> check_stack_depth() (local no-op).
//!   port/simd.h           -> Vector8 SIMD primitives are NOT ported; the SIMD fast
//!                            path of escape_json_with_len is replaced by an
//!                            equivalent scalar loop (see note there).
//!   utils/array.h, utils/date.h, utils/datetime.h, utils/lsyscache.h,
//!   utils/typcache.h      -> catalog/array/datetime helpers (stubbed).
//!   utils/builtins.h      -> crate::utils::adt::varlena (cstring_to_text family,
//!                            text_to_cstring, TextDatumGetCString).
//!   utils/json.h, utils/jsonfuncs.h, common/jsonapi.h
//!                         -> the JSON lexer/parser (common/jsonapi.c) is NOT yet
//!                            ported.  We declare the minimal surface we need
//!                            (JsonLexContext, JsonSemAction, JsonTokenType,
//!                            JsonParseErrorType, makeJsonLexContext, pg_parse_json,
//!                            json_lex, ...) as local TODO(pg-port) stubs so this
//!                            file compiles; the validation entry points keep their
//!                            full C structure but reach `unimplemented!()` at the
//!                            actual parse call.
//!
//! TRANSLATED FULLY (real, tested paths):
//!   json_out, json_send, escape_json, escape_json_with_len, escape_json_char,
//!   escape_json_text, catenate_stringinfo_string.
//! TRANSLATED STRUCTURALLY (compile, but parse via the unported jsonapi stub):
//!   json_in, json_recv, json_typeof, json_validate.
//! STUBBED (need unported catalog/array/datetime/funcapi machinery):
//!   datum_to_json[_internal], to_json, to_json_is_immutable, array_to_json[_pretty],
//!   row_to_json[_pretty], composite_to_json, array_dim_to_json, array_to_json_internal,
//!   add_json, json_build_object[_worker/_noargs], json_build_array[_worker/_noargs],
//!   json_object[_two_arg], json_agg[_strict]_transfn + finalfn, the json_object_agg*
//!   transfns + finalfn, JsonEncodeDateTime, and the key-uniqueness hash helpers.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;
use crate::{
    appendStringInfo, appendStringInfoCharMacro, ereport, errmsg, DatumGetTextPP, PG_ARGISNULL,
    PG_GETARG_DATUM, PG_GETARG_POINTER, PG_GETARG_TEXT_PP, PG_RETURN_BYTEA_P, PG_RETURN_CSTRING,
    PG_RETURN_DATUM, PG_RETURN_NULL, PG_RETURN_POINTER, PG_RETURN_TEXT_P,
};
use crate::catalog::pg_type_d::{
    DATEOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
};
use crate::c::{int16, text};
use crate::postgres::{DatumGetBool, DatumGetPointer, DatumGetUInt32, PointerGetDatum};
use crate::postgres_ext::Oid;
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoChar, appendStringInfoString, enlargeStringInfo,
    initStringInfo, makeStringInfo, StringInfo, StringInfoData,
};
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgtext, pq_sendtext};
use crate::mb::mbutils::GetDatabaseEncoding;
use crate::utils::adt::varlena::{
    cstring_to_text, cstring_to_text_with_len, TextDatumGetCString,
};
use core::ffi::{c_char, c_int, c_long, c_void};

// libc bindings (string.h, via postgres.h).  palloc/pfree/pstrdup are prelude.
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
}

/* PG_GETARG_BOOL(n) (fmgr.h). */
macro_rules! PG_GETARG_BOOL {
    ($fcinfo:expr, $n:expr) => {
        DatumGetBool(PG_GETARG_DATUM!($fcinfo, $n))
    };
}

/* PG_GETARG_ARRAYTYPE_P(n) (array.h). */
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut ArrayType
    };
}

/* CStringGetTextDatum(s) == DirectFunctionCall1(textin, CStringGetDatum(s)). */
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    PointerGetDatum(cstring_to_text(s) as *const c_void)
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_NULL_VALUE_NOT_ALLOWED: c_int = 0;
const ERRCODE_ARRAY_SUBSCRIPT_ERROR: c_int = 0;
const ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE: c_int = 0;
const ERRCODE_DATETIME_VALUE_OUT_OF_RANGE: c_int = 0;

/* miscadmin.h: bound recursion guard; a no-op in the port (see CONVENTIONS). */
#[inline]
unsafe fn check_stack_depth() {}

// ===========================================================================
// jsonapi surface (common/jsonapi.h) -- NOT yet ported.
//
// TODO(pg-port): common/jsonapi.c (the JSON lexer/recursive-descent parser) is
// not yet translated.  We declare just enough of its public surface here so the
// "json" type's validation entry points keep their faithful C structure and
// compile.  Every routine that would actually scan/parse input reaches
// `unimplemented!()`.  When jsonapi lands as crate::common::jsonapi, delete this
// block and import from there instead.
// ===========================================================================

/* common/jsonapi.h: enum JsonTokenType */
#[allow(non_camel_case_types, dead_code)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum JsonTokenType {
    JSON_TOKEN_INVALID,
    JSON_TOKEN_STRING,
    JSON_TOKEN_NUMBER,
    JSON_TOKEN_OBJECT_START,
    JSON_TOKEN_OBJECT_END,
    JSON_TOKEN_ARRAY_START,
    JSON_TOKEN_ARRAY_END,
    JSON_TOKEN_COMMA,
    JSON_TOKEN_COLON,
    JSON_TOKEN_TRUE,
    JSON_TOKEN_FALSE,
    JSON_TOKEN_NULL,
    JSON_TOKEN_END,
}
use JsonTokenType::*;

/* common/jsonapi.h: enum JsonParseErrorType */
#[allow(non_camel_case_types, dead_code)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum JsonParseErrorType {
    JSON_SUCCESS,
    JSON_INCOMPLETE,
    JSON_INVALID_LEXER_TYPE,
    JSON_NESTING_TOO_DEEP,
    JSON_ESCAPING_INVALID,
    JSON_ESCAPING_REQUIRED,
    JSON_EXPECTED_ARRAY_FIRST,
    JSON_EXPECTED_ARRAY_NEXT,
    JSON_EXPECTED_COLON,
    JSON_EXPECTED_END,
    JSON_EXPECTED_JSON,
    JSON_EXPECTED_MORE,
    JSON_EXPECTED_OBJECT_FIRST,
    JSON_EXPECTED_OBJECT_NEXT,
    JSON_EXPECTED_STRING,
    JSON_INVALID_TOKEN,
    JSON_OUT_OF_MEMORY,
    JSON_UNICODE_CODE_POINT_ZERO,
    JSON_UNICODE_ESCAPE_FORMAT,
    JSON_UNICODE_HIGH_ESCAPE,
    JSON_UNICODE_UNTRANSLATABLE,
    JSON_UNICODE_HIGH_SURROGATE,
    JSON_UNICODE_LOW_SURROGATE,
    JSON_SEM_ACTION_FAILED,
}
use JsonParseErrorType::*;

/*
 * common/jsonapi.h: JsonLexContext.  Only the fields json.c reaches into are
 * mirrored (token_type, used by json_typeof); the rest of the real struct is
 * elided until jsonapi is ported.
 */
#[allow(non_snake_case)]
#[repr(C)]
pub struct JsonLexContext {
    pub token_type: JsonTokenType,
    // TODO(pg-port): remaining fields (input, input_length, line_number,
    // strval, errormsg, parse_strict, ...) belong to common/jsonapi.c.
}

/*
 * common/jsonapi.h: JsonSemAction.  Callbacks fire during pg_parse_json.  We
 * keep the fields used by json_validate's uniqueness check; all are stubbed to
 * the jsonapi callback signature (void* state -> JsonParseErrorType).
 */
pub type json_struct_action = unsafe fn(state: *mut c_void) -> JsonParseErrorType;
pub type json_ofield_action =
    unsafe fn(state: *mut c_void, fname: *mut c_char, isnull: bool) -> JsonParseErrorType;

#[allow(non_snake_case)]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonSemAction {
    pub semstate: *mut c_void,
    pub object_start: Option<json_struct_action>,
    pub object_end: Option<json_struct_action>,
    pub array_start: Option<json_struct_action>,
    pub array_end: Option<json_struct_action>,
    pub object_field_start: Option<json_ofield_action>,
    pub object_field_end: Option<json_ofield_action>,
    pub array_element_start: Option<json_struct_action>,
    pub array_element_end: Option<json_struct_action>,
    pub scalar: *mut c_void,
}

impl JsonSemAction {
    /* {0} initializer used by json_validate */
    const fn zeroed() -> Self {
        JsonSemAction {
            semstate: null_mut(),
            object_start: None,
            object_end: None,
            array_start: None,
            array_end: None,
            object_field_start: None,
            object_field_end: None,
            array_element_start: None,
            array_element_end: None,
            scalar: null_mut(),
        }
    }
}

/*
 * common/jsonapi.h: nullSemAction -- the no-op semantic action used for
 * validation-only parsing (json_in / json_recv).  In C this is a file-scope
 * global ({0}-initialized); mirrored here as a zero-initialized `static mut`,
 * taken by `addr_of_mut!(nullSemAction)` to avoid a &mut to a static mut.
 */
#[allow(non_upper_case_globals)]
static mut nullSemAction: JsonSemAction = JsonSemAction::zeroed();

/*
 * makeJsonLexContext: initialize a lexer over the text value `json`.
 * TODO(pg-port): real impl lives in common/jsonapi.c.
 */
#[allow(non_snake_case)]
unsafe fn makeJsonLexContext(
    lex: *mut JsonLexContext,
    _json: *mut text,
    _need_escapes: bool,
) -> *mut JsonLexContext {
    let _ = lex;
    unimplemented!("makeJsonLexContext: common/jsonapi.c not yet translated")
}

/*
 * makeJsonLexContextCstringLen: initialize a lexer over (str,len) in `encoding`.
 * TODO(pg-port): real impl lives in common/jsonapi.c.
 */
#[allow(non_snake_case)]
unsafe fn makeJsonLexContextCstringLen(
    lex: *mut JsonLexContext,
    _str: *mut c_char,
    _len: c_int,
    _encoding: c_int,
    _need_escapes: bool,
) -> *mut JsonLexContext {
    let _ = lex;
    unimplemented!("makeJsonLexContextCstringLen: common/jsonapi.c not yet translated")
}

/*
 * pg_parse_json: drive the parser with the given semantic actions.
 * TODO(pg-port): real impl lives in common/jsonapi.c.
 */
unsafe fn pg_parse_json(
    _lex: *mut JsonLexContext,
    _sem: *mut JsonSemAction,
) -> JsonParseErrorType {
    unimplemented!("pg_parse_json: common/jsonapi.c not yet translated")
}

/*
 * pg_parse_json_or_errsave: parse; on error either soft-report (escontext) or
 * raise.  Returns true on success.  TODO(pg-port): common/jsonapi.c.
 */
unsafe fn pg_parse_json_or_errsave(
    _lex: *mut JsonLexContext,
    _sem: *mut JsonSemAction,
    _escontext: *mut c_void,
) -> bool { crate::utils::adt::jsonfuncs::pg_parse_json_or_errsave(_lex as _, _sem as _, _escontext as _) }

/*
 * pg_parse_json_or_ereport: parse; raise ERROR on any failure.
 * TODO(pg-port): common/jsonapi.c.
 */
unsafe fn pg_parse_json_or_ereport(_lex: *mut JsonLexContext, _sem: *mut JsonSemAction) {
    unimplemented!("pg_parse_json_or_ereport: common/jsonapi.c not yet translated")
}

/*
 * json_lex: lex exactly one token, leaving its kind in lex->token_type.
 * TODO(pg-port): common/jsonapi.c.
 */
unsafe fn json_lex(_lex: *mut JsonLexContext) -> JsonParseErrorType {
    unimplemented!("json_lex: common/jsonapi.c not yet translated")
}

/*
 * freeJsonLexContext: release lexer-owned memory.
 * TODO(pg-port): common/jsonapi.c.
 */
#[allow(non_snake_case)]
unsafe fn freeJsonLexContext(_lex: *mut JsonLexContext) {
    unimplemented!("freeJsonLexContext: common/jsonapi.c not yet translated")
}

/*
 * json_errsave_error: convert a JsonParseErrorType into a soft/hard error
 * (uses json_errdetail under the hood).  TODO(pg-port): utils/jsonfuncs.c.
 */
unsafe fn json_errsave_error(
    _error: JsonParseErrorType,
    _lex: *mut JsonLexContext,
    _escontext: *mut c_void,
) { crate::utils::adt::jsonfuncs::json_errsave_error(_error as _, _lex as _, _escontext as _) }

// ===========================================================================
// Forward declarations of the stubbed function-library helpers (json.c statics).
// ===========================================================================

/*
 * json_categorize_type: classify `typoid` for JSON output and fetch its output
 * function oid.  Lives in utils/adt/jsonfuncs-ish territory (catalog lookups).
 * TODO(pg-port): needs utils/lsyscache.h + utils/typcache.h.
 */
#[allow(dead_code)]
unsafe fn json_categorize_type(
    _typoid: Oid,
    _is_jsonb: bool,
    _tcategory: *mut JsonTypeCategory,
    _outfuncoid: *mut Oid,
) { crate::utils::adt::jsonfuncs::json_categorize_type(_typoid as _, _is_jsonb, _tcategory as _, _outfuncoid as _) }

/*
 * utils/jsonfuncs.h: JsonTypeCategory enum.  Declared for the stubbed builder
 * code's signatures; values mirror the C enum order.
 */
#[allow(non_camel_case_types, dead_code)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum JsonTypeCategory {
    JSONTYPE_NULL,
    JSONTYPE_BOOL,
    JSONTYPE_NUMERIC,
    JSONTYPE_DATE,
    JSONTYPE_TIMESTAMP,
    JSONTYPE_TIMESTAMPTZ,
    JSONTYPE_JSON,
    JSONTYPE_JSONB,
    JSONTYPE_ARRAY,
    JSONTYPE_COMPOSITE,
    JSONTYPE_CAST,
    JSONTYPE_OTHER,
}
use JsonTypeCategory::*;

// ===========================================================================
// Catalog / fmgr / array / composite / datetime dependencies that live in
// not-yet-ported .c files.  Declared here with TODO(pg-port) bodies (mirrors the
// sibling jsonb.rs), so json.c's value-conversion paths translate 1:1 and the
// file compiles.  Replace with real imports as those modules land.
// ===========================================================================

/* catalog/pg_proc.h: PROVOLATILE_IMMUTABLE. */
const PROVOLATILE_IMMUTABLE: c_char = b'i' as c_char;

/*
 * utils/fmgroids.h: built-in output-function OIDs special-cased by
 * datum_to_json_internal.  F_TEXTOUT lives in bootstrap; F_VARCHAROUT/F_BPCHAROUT
 * are not yet exported, so they are declared here as TODO(pg-port) constants with
 * their real catalog OIDs.
 */
const F_TEXTOUT: Oid = 47;
const F_VARCHAROUT: Oid = 1046;
const F_BPCHAROUT: Oid = 1045;

/* fmgr.h: output-function call wrappers used by the value-conversion paths. */
unsafe fn OidFunctionCall1(_functionId: Oid, _arg1: Datum) -> Datum {
    unimplemented!("json: OidFunctionCall1 (utils/fmgr/fmgr.c) not yet translated")
}

/* utils/cache/lsyscache.c: function volatility, type physical properties. */
unsafe fn func_volatile(_funcid: Oid) -> c_char { crate::utils::cache::lsyscache::func_volatile(_funcid as _) as _ }
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    _typlen: *mut int16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
) { crate::utils::cache::lsyscache::get_typlenbyvalalign(_typid as _, _typlen as _, _typbyval as _, _typalign as _) }

/* funcapi.h / executor: aggregate context + variadic argument extraction. */
unsafe fn AggCheckCallContext(
    _fcinfo: FunctionCallInfo,
    _aggcontext: *mut MemoryContext,
) -> bool { crate::executor::nodeAgg::AggCheckCallContext(_fcinfo as _, _aggcontext as _) != 0 }
unsafe fn extract_variadic_args(
    _fcinfo: FunctionCallInfo,
    _variadic_start: c_int,
    _convert_unknown: bool,
    _args: *mut *mut Datum,
    _types: *mut *mut Oid,
    _nulls: *mut *mut bool,
) -> c_int { crate::utils::fmgr::funcapi::extract_variadic_args(_fcinfo as _, _variadic_start as _, _convert_unknown, _args as _, _types as _, _nulls as _) as _ }

/* utils/array.h, arrayfuncs.c: array deconstruction. */
#[repr(C)]
struct ArrayType {
    _opaque: [u8; 0],
}
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType { crate::access::nbtree::nbtpreprocesskeys::DatumGetArrayTypeP(_d as _) as _ }
unsafe fn ARR_ELEMTYPE(_a: *mut ArrayType) -> Oid {
    unimplemented!("json: ARR_ELEMTYPE (utils/array.h) not yet translated")
}
unsafe fn ARR_NDIM(_a: *mut ArrayType) -> c_int { crate::utils::array::ARR_NDIM(_a as _) as _ }
unsafe fn ARR_DIMS(_a: *mut ArrayType) -> *mut c_int {
    unimplemented!("json: ARR_DIMS (utils/array.h) not yet translated")
}
unsafe fn ArrayGetNItems(_ndim: c_int, _dims: *const c_int) -> c_int { crate::utils::adt::arrayutils::ArrayGetNItems(_ndim as _, _dims as _) as _ }
unsafe fn deconstruct_array(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elmlen: int16,
    _elmbyval: bool,
    _elmalign: c_char,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) { crate::utils::adt::arrayfuncs::deconstruct_array(_array as _, _elmtype as _, _elmlen as _, _elmbyval, _elmalign as _, _elemsp as _, _nullsp as _, _nelemsp as _) }
unsafe fn deconstruct_array_builtin(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!("json: deconstruct_array_builtin (utils/adt/arrayfuncs.c) not yet translated")
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
    unimplemented!("json: DatumGetHeapTupleHeader (fmgr.h) not yet translated")
}
unsafe fn HeapTupleHeaderGetTypeId(_td: HeapTupleHeader) -> Oid { crate::access::htup_details::HeapTupleHeaderGetTypeId(_td as _) as _ }
unsafe fn HeapTupleHeaderGetTypMod(_td: HeapTupleHeader) -> i32 { crate::access::htup_details::HeapTupleHeaderGetTypMod(_td as _) as _ }
unsafe fn HeapTupleHeaderGetDatumLength(_td: HeapTupleHeader) -> u32 { crate::access::htup_details::HeapTupleHeaderGetDatumLength(_td as _) as _ }
unsafe fn lookup_rowtype_tupdesc(_type_id: Oid, _typmod: i32) -> TupleDesc { crate::utils::cache::typcache::lookup_rowtype_tupdesc(_type_id as _, _typmod as _) as _ }
unsafe fn ReleaseTupleDesc(_tupdesc: TupleDesc) { crate::access::common::tupdesc::ReleaseTupleDesc(_tupdesc as _) }
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    unimplemented!("json: TupleDescAttr (access/tupdesc.h) not yet translated")
}
unsafe fn att_isdropped(_att: Form_pg_attribute) -> bool {
    unimplemented!("json: attisdropped (access/tupdesc.h) not yet translated")
}
unsafe fn att_name(_att: Form_pg_attribute) -> *mut c_char {
    unimplemented!("json: NameStr(att->attname) (access/tupdesc.h) not yet translated")
}
unsafe fn att_typid(_att: Form_pg_attribute) -> Oid {
    unimplemented!("json: att->atttypid (access/tupdesc.h) not yet translated")
}
unsafe fn heap_getattr(
    _tup: HeapTuple,
    _attnum: c_int,
    _tupdesc: TupleDesc,
    _isnull: *mut bool,
) -> Datum {
    unimplemented!("json: heap_getattr (access/htup_details.h) not yet translated")
}

/* utils/datetime.h: MAXDATELEN for stack date/time buffers. */
const MAXDATELEN: usize = 128;

// ---------------------------------------------------------------------------
// utils/date.h + utils/datetime.h + pgtime.h: datetime encoding primitives used
// by JsonEncodeDateTime.  These live across several not-yet-stable modules
// (date.rs/datetime.rs/timestamp.rs/xml.rs/pgtime.rs) and several are not yet
// `pub`-exported, so they are declared here as TODO(pg-port) shims to keep the
// faithful structure of JsonEncodeDateTime while the file compiles.
// ---------------------------------------------------------------------------
type DateADT = i32;
type TimeADT = i64;
type Timestamp = i64;
type TimestampTz = i64;
type fsec_t = i32;
const POSTGRES_EPOCH_JDATE: DateADT = 2451545; /* == date2j(2000, 1, 1) */
const USE_XSD_DATES: c_int = 3;
const USECS_PER_SEC: Timestamp = 1000000;

#[repr(C)]
struct TimeTzADT {
    _opaque: [u8; 0],
}

/* pgtime.h: broken-down time.  Only the fields json.c touches are mirrored. */
#[allow(non_snake_case)]
#[repr(C)]
struct pg_tm {
    tm_sec: c_int,
    tm_min: c_int,
    tm_hour: c_int,
    tm_mday: c_int,
    tm_mon: c_int,
    tm_year: c_int,
    tm_wday: c_int,
    tm_yday: c_int,
    tm_isdst: c_int,
    tm_gmtoff: i64,
    tm_zone: *const c_char,
}

unsafe fn DatumGetDateADT(_x: Datum) -> DateADT { crate::utils::adt::date::DatumGetDateADT(_x as _) as _ }
unsafe fn DatumGetTimeADT(_x: Datum) -> TimeADT { crate::utils::adt::date::DatumGetTimeADT(_x as _) as _ }
unsafe fn DatumGetTimeTzADTP(_x: Datum) -> *mut TimeTzADT { crate::utils::adt::date::DatumGetTimeTzADTP(_x as _) as _ }
unsafe fn DatumGetTimestamp(_x: Datum) -> Timestamp { crate::utils::adt::xml::DatumGetTimestamp(_x as _) as _ }
unsafe fn DatumGetTimestampTz(_x: Datum) -> TimestampTz {
    unimplemented!("json: DatumGetTimestampTz (utils/timestamp.h) not yet translated")
}
unsafe fn DATE_NOT_FINITE(_d: DateADT) -> bool { crate::utils::adt::date::DATE_NOT_FINITE(_d as _) }
unsafe fn TIMESTAMP_NOT_FINITE(_t: Timestamp) -> bool { crate::utils::adt::date::TIMESTAMP_NOT_FINITE(_t as _) }
unsafe fn EncodeSpecialDate(_dt: DateADT, _str: *mut c_char) { crate::utils::adt::date::EncodeSpecialDate(_dt as _, _str as _) }
unsafe fn EncodeSpecialTimestamp(_dt: Timestamp, _str: *mut c_char) { crate::utils::adt::timestamp::EncodeSpecialTimestamp(_dt as _, _str as _) }
unsafe fn j2date(_jd: c_int, _year: *mut c_int, _month: *mut c_int, _day: *mut c_int) { crate::utils::adt::datetime::j2date(_jd as _, _year as _, _month as _, _day as _) }
unsafe fn EncodeDateOnly(_tm: *mut pg_tm, _style: c_int, _str: *mut c_char) { crate::utils::adt::datetime::EncodeDateOnly(_tm as _, _style as _, _str as _) }
unsafe fn EncodeTimeOnly(
    _tm: *mut pg_tm,
    _fsec: fsec_t,
    _print_tz: bool,
    _tz: c_int,
    _style: c_int,
    _str: *mut c_char,
) { crate::utils::adt::datetime::EncodeTimeOnly(_tm as _, _fsec as _, _print_tz, _tz as _, _style as _, _str as _) }
unsafe fn EncodeDateTime(
    _tm: *mut pg_tm,
    _fsec: fsec_t,
    _print_tz: bool,
    _tz: c_int,
    _tzn: *const c_char,
    _style: c_int,
    _str: *mut c_char,
) { crate::utils::adt::datetime::EncodeDateTime(_tm as _, _fsec as _, _print_tz, _tz as _, _tzn as _, _style as _, _str as _) }
unsafe fn time2tm(_time: TimeADT, _tm: *mut pg_tm, _fsec: *mut fsec_t) -> c_int { crate::utils::adt::date::time2tm(_time as _, _tm as _, _fsec as _) as _ }
unsafe fn timetz2tm(
    _time: *mut TimeTzADT,
    _tm: *mut pg_tm,
    _fsec: *mut fsec_t,
    _tzp: *mut c_int,
) -> c_int {
    unimplemented!("json: timetz2tm (utils/adt/date.c) not yet translated")
}
unsafe fn timestamp2tm(
    _dt: Timestamp,
    _tzp: *mut c_int,
    _tm: *mut pg_tm,
    _fsec: *mut fsec_t,
    _tzn: *mut *const c_char,
    _attimezone: *mut c_void,
) -> c_int { crate::utils::adt::timestamp::timestamp2tm(_dt as _, _tzp as _, _tm as _, _fsec as _, _tzn as _, _attimezone as _) as _ }

/* MemoryContextStrdup (utils/mmgr/mcxt.c). */
unsafe fn MemoryContextStrdup(_context: MemoryContext, _string: *const c_char) -> *mut c_char {
    unimplemented!("json: MemoryContextStrdup (utils/mmgr/mcxt.c) not yet translated")
}

// ===========================================================================
// Local state structs (json.c file-scope).
// ===========================================================================

/* hash table for key names (HTAB). */
type JsonUniqueCheckState = *mut c_void; // TODO(pg-port): utils/hsearch.h HTAB

/* Context struct for key uniqueness check during JSON building */
#[repr(C)]
struct JsonUniqueBuilderState {
    check: JsonUniqueCheckState, /* unique check */
    skipped_keys: StringInfoData, /* skipped keys with NULL values */
    mcxt: MemoryContext,         /* context for saving skipped keys */
}

/* State struct for JSON aggregation */
#[repr(C)]
struct JsonAggState {
    str: StringInfo,
    key_category: JsonTypeCategory,
    key_output_func: Oid,
    val_category: JsonTypeCategory,
    val_output_func: Oid,
    unique_check: JsonUniqueBuilderState,
}

// ===========================================================================
// Support for fast key uniqueness checking.
//
// We maintain a hash table of used keys in JSON objects for fast detection of
// duplicates.  The dynahash layer (utils/hsearch.h) and common/hashfn.h are not
// yet ported, so hash_create/hash_search/hash_bytes* are declared as local
// TODO(pg-port) shims; the json_unique_* logic is translated 1:1.
// ===========================================================================

/* Hash entry for JsonUniqueCheckState */
#[repr(C)]
struct JsonUniqueHashEntry {
    key: *const c_char,
    key_len: c_int,
    object_id: c_int,
}

/* utils/hsearch.h: dynahash control struct + actions (only what we use). */
#[allow(non_snake_case)]
#[repr(C)]
struct HASHCTL {
    keysize: Size,
    entrysize: Size,
    hcxt: MemoryContext,
    hash: HashValueFunc,
    r#match: HashCompareFunc,
}
type HashValueFunc = unsafe fn(key: *const c_void, keysize: Size) -> u32;
type HashCompareFunc = unsafe fn(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int;

const HASH_ELEM: c_int = 0x0008;
const HASH_CONTEXT: c_int = 0x0040;
const HASH_FUNCTION: c_int = 0x0010;
const HASH_COMPARE: c_int = 0x0400;
const HASH_ENTER: c_int = 1; /* HASHACTION */

unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> JsonUniqueCheckState { crate::utils::hash::dynahash::hash_create(_tabname as _, _nelem as _, _info as _, _flags as _) as _ }
unsafe fn hash_search(
    _hashp: JsonUniqueCheckState,
    _key: *const c_void,
    _action: c_int,
    _foundptr: *mut bool,
) -> *mut c_void { unimplemented!() }
unsafe fn hash_bytes(_k: *const u8, _keylen: c_int) -> u32 { crate::common::hashfn::hash_bytes(_k as _, _keylen as _) as _ }
unsafe fn hash_bytes_uint32(_k: u32) -> u32 { crate::common::hashfn::hash_bytes_uint32(_k as _) as _ }

/* Functions implementing hash table for key uniqueness check */
unsafe fn json_unique_hash(key: *const c_void, _keysize: Size) -> u32 {
    let entry = key as *const JsonUniqueHashEntry;
    let mut hash: u32 = hash_bytes_uint32((*entry).object_id as u32);

    hash ^= hash_bytes((*entry).key as *const u8, (*entry).key_len);

    DatumGetUInt32(hash as Datum)
}

unsafe fn json_unique_hash_match(key1: *const c_void, key2: *const c_void, _keysize: Size) -> c_int {
    let entry1 = key1 as *const JsonUniqueHashEntry;
    let entry2 = key2 as *const JsonUniqueHashEntry;

    if (*entry1).object_id != (*entry2).object_id {
        return if (*entry1).object_id > (*entry2).object_id {
            1
        } else {
            -1
        };
    }

    if (*entry1).key_len != (*entry2).key_len {
        return if (*entry1).key_len > (*entry2).key_len {
            1
        } else {
            -1
        };
    }

    strncmp((*entry1).key, (*entry2).key, (*entry1).key_len as usize)
}

// ===========================================================================
// Input.
// ===========================================================================

/*
 * json_in: validate the text and store it verbatim (json == validated text).
 */
pub unsafe fn json_in(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut c_char = PG_GETARG_DATUM!(fcinfo, 0) as *mut c_char; // PG_GETARG_CSTRING(0)
    let result: *mut text = cstring_to_text(json);
    let mut lex: JsonLexContext = core::mem::zeroed();

    /* validate it */
    makeJsonLexContext(&mut lex, result, false);
    if !pg_parse_json_or_errsave(
        &mut lex,
        core::ptr::addr_of_mut!(nullSemAction),
        (*fcinfo).context as *mut c_void,
    ) {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Internal representation is the same as text */
    PG_RETURN_TEXT_P!(result);
}

// ===========================================================================
// Output.
// ===========================================================================

/*
 * json_out: json is text, so just return the text as a cstring.
 */
pub unsafe fn json_out(fcinfo: FunctionCallInfo) -> Datum {
    /* we needn't detoast because text_to_cstring will handle that */
    let txt: Datum = PG_GETARG_DATUM!(fcinfo, 0);

    PG_RETURN_CSTRING!(TextDatumGetCString(txt));
}

/*
 * json_send: binary send -- just ship the text bytes.
 */
pub unsafe fn json_send(fcinfo: FunctionCallInfo) -> Datum {
    let t: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendtext(
        &mut buf,
        VARDATA_ANY(t as *const c_char),
        VARSIZE_ANY_EXHDR(t as *const c_char) as c_int,
    );
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

/*
 * json_recv: binary receive -- read the text bytes and validate them.
 */
pub unsafe fn json_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let str: *mut c_char;
    let mut nbytes: c_int = 0;
    let mut lex: JsonLexContext = core::mem::zeroed();

    str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &mut nbytes);

    /* Validate it. */
    makeJsonLexContextCstringLen(&mut lex, str, nbytes, GetDatabaseEncoding(), false);
    pg_parse_json_or_ereport(&mut lex, core::ptr::addr_of_mut!(nullSemAction));

    PG_RETURN_TEXT_P!(cstring_to_text_with_len(str, nbytes));
}

// ===========================================================================
// datum_to_json_internal and friends -- STUBBED (catalog/array/datetime).
// ===========================================================================

/*
 * Turn a Datum into JSON text, appending to "result".
 * TODO(pg-port): needs OidOutputFunctionCall / JsonEncodeDateTime /
 * composite_to_json / array_to_json_internal / escape_json_text dispatch.
 */
unsafe fn datum_to_json_internal(
    val: Datum,
    is_null: bool,
    result: StringInfo,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
    key_scalar: bool,
) {
    let outputstr: *mut c_char;
    let jsontext: *mut text;

    check_stack_depth();

    /* callers are expected to ensure that null keys are not passed in */
    Assert!(!(key_scalar && is_null));

    if is_null {
        appendBinaryStringInfo(result, c"null".as_ptr() as *const c_void, strlen(c"null".as_ptr()) as c_int);
        return;
    }

    if key_scalar
        && (tcategory == JSONTYPE_ARRAY
            || tcategory == JSONTYPE_COMPOSITE
            || tcategory == JSONTYPE_JSON
            || tcategory == JSONTYPE_CAST)
    {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("key value must be scalar, not array, composite, or json")
        );
    }

    match tcategory {
        JSONTYPE_ARRAY => {
            array_to_json_internal(val, result, false);
        }
        JSONTYPE_COMPOSITE => {
            composite_to_json(val, result, false);
        }
        JSONTYPE_BOOL => {
            if key_scalar {
                appendStringInfoChar(result, b'"' as c_char);
            }
            if DatumGetBool(val) {
                appendBinaryStringInfo(result, c"true".as_ptr() as *const c_void, strlen(c"true".as_ptr()) as c_int);
            } else {
                appendBinaryStringInfo(
                    result,
                    c"false".as_ptr() as *const c_void,
                    strlen(c"false".as_ptr()) as c_int,
                );
            }
            if key_scalar {
                appendStringInfoChar(result, b'"' as c_char);
            }
        }
        JSONTYPE_NUMERIC => {
            let outputstr = OidOutputFunctionCall(outfuncoid, val);

            /*
             * Don't quote a non-key if it's a valid JSON number (i.e., not
             * "Infinity", "-Infinity", or "NaN").  Since we know this is a
             * numeric data type's output, we simplify and open-code the
             * validation for better performance.
             */
            if !key_scalar
                && ((*outputstr >= b'0' as c_char && *outputstr <= b'9' as c_char)
                    || (*outputstr == b'-' as c_char
                        && (*outputstr.add(1) >= b'0' as c_char
                            && *outputstr.add(1) <= b'9' as c_char)))
            {
                appendStringInfoString(result, outputstr);
            } else {
                appendStringInfoChar(result, b'"' as c_char);
                appendStringInfoString(result, outputstr);
                appendStringInfoChar(result, b'"' as c_char);
            }
            pfree(outputstr as *mut c_void);
        }
        JSONTYPE_DATE => {
            let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

            JsonEncodeDateTime(buf.as_mut_ptr(), val, DATEOID, null());
            appendStringInfoChar(result, b'"' as c_char);
            appendStringInfoString(result, buf.as_ptr());
            appendStringInfoChar(result, b'"' as c_char);
        }
        JSONTYPE_TIMESTAMP => {
            let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

            JsonEncodeDateTime(buf.as_mut_ptr(), val, TIMESTAMPOID, null());
            appendStringInfoChar(result, b'"' as c_char);
            appendStringInfoString(result, buf.as_ptr());
            appendStringInfoChar(result, b'"' as c_char);
        }
        JSONTYPE_TIMESTAMPTZ => {
            let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

            JsonEncodeDateTime(buf.as_mut_ptr(), val, TIMESTAMPTZOID, null());
            appendStringInfoChar(result, b'"' as c_char);
            appendStringInfoString(result, buf.as_ptr());
            appendStringInfoChar(result, b'"' as c_char);
        }
        JSONTYPE_JSON => {
            /* JSON and JSONB output will already be escaped */
            outputstr = OidOutputFunctionCall(outfuncoid, val);
            appendStringInfoString(result, outputstr);
            pfree(outputstr as *mut c_void);
        }
        JSONTYPE_CAST => {
            /* outfuncoid refers to a cast function, not an output function */
            jsontext = DatumGetTextPP!(OidFunctionCall1(outfuncoid, val));
            appendBinaryStringInfo(
                result,
                VARDATA_ANY(jsontext as *const c_char) as *const c_void,
                VARSIZE_ANY_EXHDR(jsontext as *const c_char) as c_int,
            );
            pfree(jsontext as *mut c_void);
        }
        _ => {
            /* special-case text types to save useless palloc/memcpy cycles */
            if outfuncoid == F_TEXTOUT || outfuncoid == F_VARCHAROUT || outfuncoid == F_BPCHAROUT {
                escape_json_text(result, DatumGetPointer(val) as *mut text);
            } else {
                outputstr = OidOutputFunctionCall(outfuncoid, val);
                escape_json(result, outputstr);
                pfree(outputstr as *mut c_void);
            }
        }
    }
}

/*
 * JsonEncodeDateTime: encode a datetime Datum as an ISO JSON string.
 * TODO(pg-port): needs utils/date.h + utils/datetime.h (j2date/EncodeDateTime/...).
 */
pub unsafe fn JsonEncodeDateTime(
    mut buf: *mut c_char,
    value: Datum,
    typid: Oid,
    tzp: *const c_int,
) -> *mut c_char {
    if buf.is_null() {
        buf = palloc((MAXDATELEN + 1) as Size) as *mut c_char;
    }

    match typid {
        DATEOID => {
            let date: DateADT;
            let mut tm: pg_tm = core::mem::zeroed();

            date = DatumGetDateADT(value);

            /* Same as date_out(), but forcing DateStyle */
            if DATE_NOT_FINITE(date) {
                EncodeSpecialDate(date, buf);
            } else {
                j2date(
                    date + POSTGRES_EPOCH_JDATE,
                    &mut tm.tm_year,
                    &mut tm.tm_mon,
                    &mut tm.tm_mday,
                );
                EncodeDateOnly(&mut tm, USE_XSD_DATES, buf);
            }
        }
        TIMEOID => {
            let time: TimeADT = DatumGetTimeADT(value);
            let mut tt: pg_tm = core::mem::zeroed();
            let tm: *mut pg_tm = &mut tt;
            let mut fsec: fsec_t = 0;

            /* Same as time_out(), but forcing DateStyle */
            time2tm(time, tm, &mut fsec);
            EncodeTimeOnly(tm, fsec, false, 0, USE_XSD_DATES, buf);
        }
        TIMETZOID => {
            let time: *mut TimeTzADT = DatumGetTimeTzADTP(value);
            let mut tt: pg_tm = core::mem::zeroed();
            let tm: *mut pg_tm = &mut tt;
            let mut fsec: fsec_t = 0;
            let mut tz: c_int = 0;

            /* Same as timetz_out(), but forcing DateStyle */
            timetz2tm(time, tm, &mut fsec, &mut tz);
            EncodeTimeOnly(tm, fsec, true, tz, USE_XSD_DATES, buf);
        }
        TIMESTAMPOID => {
            let timestamp: Timestamp;
            let mut tm: pg_tm = core::mem::zeroed();
            let mut fsec: fsec_t = 0;

            timestamp = DatumGetTimestamp(value);
            /* Same as timestamp_out(), but forcing DateStyle */
            if TIMESTAMP_NOT_FINITE(timestamp) {
                EncodeSpecialTimestamp(timestamp, buf);
            } else if timestamp2tm(timestamp, null_mut(), &mut tm, &mut fsec, null_mut(), null_mut())
                == 0
            {
                EncodeDateTime(&mut tm, fsec, false, 0, null(), USE_XSD_DATES, buf);
            } else {
                let _ = errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
        }
        TIMESTAMPTZOID => {
            let mut timestamp: TimestampTz;
            let mut tm: pg_tm = core::mem::zeroed();
            let mut tz: c_int = 0;
            let mut fsec: fsec_t = 0;
            let mut tzn: *const c_char = null();

            timestamp = DatumGetTimestampTz(value);

            /*
             * If a time zone is specified, we apply the time-zone shift,
             * convert timestamptz to pg_tm as if it were without a time
             * zone, and then use the specified time zone for converting
             * the timestamp into a string.
             */
            if !tzp.is_null() {
                tz = *tzp;
                timestamp -= (tz as TimestampTz) * USECS_PER_SEC;
            }

            /* Same as timestamptz_out(), but forcing DateStyle */
            if TIMESTAMP_NOT_FINITE(timestamp) {
                EncodeSpecialTimestamp(timestamp, buf);
            } else if timestamp2tm(
                timestamp,
                if !tzp.is_null() { null_mut() } else { &mut tz },
                &mut tm,
                &mut fsec,
                if !tzp.is_null() { null_mut() } else { &mut tzn },
                null_mut(),
            ) == 0
            {
                if !tzp.is_null() {
                    tm.tm_isdst = 1; /* set time-zone presence flag */
                }

                EncodeDateTime(&mut tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
            } else {
                let _ = errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE);
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
        }
        _ => {
            elog!(ERROR, "unknown jsonb value datetime type oid {}", typid);
            return null_mut();
        }
    }

    buf
}

/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
unsafe fn array_dim_to_json(
    result: StringInfo,
    dim: c_int,
    ndims: c_int,
    dims: *mut c_int,
    vals: *mut Datum,
    nulls: *mut bool,
    valcount: *mut c_int,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
    use_line_feeds: bool,
) {
    let mut i: c_int;
    let sep: *const c_char;

    Assert!(dim < ndims);

    sep = if use_line_feeds {
        c",\n ".as_ptr()
    } else {
        c",".as_ptr()
    };

    appendStringInfoChar(result, b'[' as c_char);

    i = 1;
    while i <= *dims.add(dim as usize) {
        if i > 1 {
            appendStringInfoString(result, sep);
        }

        if dim + 1 == ndims {
            datum_to_json_internal(
                *vals.add(*valcount as usize),
                *nulls.add(*valcount as usize),
                result,
                tcategory,
                outfuncoid,
                false,
            );
            *valcount += 1;
        } else {
            /*
             * Do we want line feeds on inner dimensions of arrays? For now
             * we'll say no.
             */
            array_dim_to_json(
                result,
                dim + 1,
                ndims,
                dims,
                vals,
                nulls,
                valcount,
                tcategory,
                outfuncoid,
                false,
            );
        }
        i += 1;
    }

    appendStringInfoChar(result, b']' as c_char);
}

/*
 * Turn an array into JSON.
 */
unsafe fn array_to_json_internal(array: Datum, result: StringInfo, use_line_feeds: bool) {
    let v: *mut ArrayType = DatumGetArrayTypeP(array);
    let element_type: Oid = ARR_ELEMTYPE(v);
    let dim: *mut c_int;
    let ndim: c_int;
    let mut nitems: c_int;
    let mut count: c_int = 0;
    let mut elements: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();
    let mut typlen: int16 = 0;
    let mut typbyval: bool = false;
    let mut typalign: c_char = 0;
    let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
    let mut outfuncoid: Oid = InvalidOid;

    ndim = ARR_NDIM(v);
    dim = ARR_DIMS(v);
    nitems = ArrayGetNItems(ndim, dim);

    if nitems <= 0 {
        appendStringInfoString(result, c"[]".as_ptr());
        return;
    }

    get_typlenbyvalalign(element_type, &mut typlen, &mut typbyval, &mut typalign);

    json_categorize_type(element_type, false, &mut tcategory, &mut outfuncoid);

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

    array_dim_to_json(
        result,
        0,
        ndim,
        dim,
        elements,
        nulls,
        &mut count,
        tcategory,
        outfuncoid,
        use_line_feeds,
    );

    pfree(elements as *mut c_void);
    pfree(nulls as *mut c_void);
}

/*
 * Turn a composite / record into JSON.
 */
unsafe fn composite_to_json(composite: Datum, result: StringInfo, use_line_feeds: bool) {
    let td: HeapTupleHeader;
    let tupType: Oid;
    let tupTypmod: i32;
    let tupdesc: TupleDesc;
    let mut tmptup: HeapTupleData = core::mem::zeroed();
    let tuple: HeapTuple;
    let mut i: c_int;
    let mut needsep: bool = false;
    let sep: *const c_char;
    let seplen: c_int;

    /*
     * We can avoid expensive strlen() calls by precalculating the separator
     * length.
     */
    sep = if use_line_feeds {
        c",\n ".as_ptr()
    } else {
        c",".as_ptr()
    };
    seplen = if use_line_feeds {
        strlen(c",\n ".as_ptr()) as c_int
    } else {
        strlen(c",".as_ptr()) as c_int
    };

    td = DatumGetHeapTupleHeader(composite);

    /* Extract rowtype info and find a tupdesc */
    tupType = HeapTupleHeaderGetTypeId(td);
    tupTypmod = HeapTupleHeaderGetTypMod(td);
    tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

    /* Build a temporary HeapTuple control structure */
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    tmptup.t_data = td;
    tuple = &mut tmptup;

    appendStringInfoChar(result, b'{' as c_char);

    i = 0;
    while i < (*tupdesc).natts {
        let val: Datum;
        let mut isnull: bool = false;
        let attname: *mut c_char;
        let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
        let mut outfuncoid: Oid = InvalidOid;
        let att: Form_pg_attribute = TupleDescAttr(tupdesc, i);

        if att_isdropped(att) {
            i += 1;
            continue;
        }

        if needsep {
            appendBinaryStringInfo(result, sep as *const c_void, seplen);
        }
        needsep = true;

        attname = att_name(att);
        escape_json(result, attname);
        appendStringInfoChar(result, b':' as c_char);

        val = heap_getattr(tuple, i + 1, tupdesc, &mut isnull);

        if isnull {
            tcategory = JSONTYPE_NULL;
            outfuncoid = InvalidOid;
        } else {
            json_categorize_type(att_typid(att), false, &mut tcategory, &mut outfuncoid);
        }

        datum_to_json_internal(val, isnull, result, tcategory, outfuncoid, false);
        i += 1;
    }

    appendStringInfoChar(result, b'}' as c_char);
    ReleaseTupleDesc(tupdesc);
}

/*
 * Append JSON text for "val" to "result".
 *
 * This is just a thin wrapper around datum_to_json.  If the same type will be
 * printed many times, avoid using this; better to do the json_categorize_type
 * lookups only once.
 */
unsafe fn add_json(
    val: Datum,
    is_null: bool,
    result: StringInfo,
    val_type: Oid,
    key_scalar: bool,
) {
    let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
    let mut outfuncoid: Oid = InvalidOid;

    if val_type == InvalidOid {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("could not determine input data type"));
    }

    if is_null {
        tcategory = JSONTYPE_NULL;
        outfuncoid = InvalidOid;
    } else {
        json_categorize_type(val_type, false, &mut tcategory, &mut outfuncoid);
    }

    datum_to_json_internal(val, is_null, result, tcategory, outfuncoid, key_scalar);
}

/*
 * SQL function array_to_json(anyarray) / array_to_json(anyarray, pretty bool).
 * TODO(pg-port): array_to_json_internal depends on utils/array.h.
 */
pub unsafe fn array_to_json(fcinfo: FunctionCallInfo) -> Datum {
    let array: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let result: StringInfo;

    result = makeStringInfo();

    array_to_json_internal(array, result, false);

    PG_RETURN_TEXT_P!(cstring_to_text_with_len((*result).data, (*result).len));
}

/*
 * SQL function array_to_json(row, prettybool)
 */
pub unsafe fn array_to_json_pretty(fcinfo: FunctionCallInfo) -> Datum {
    let array: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let use_line_feeds: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let result: StringInfo;

    result = makeStringInfo();

    array_to_json_internal(array, result, use_line_feeds);

    PG_RETURN_TEXT_P!(cstring_to_text_with_len((*result).data, (*result).len));
}

/*
 * SQL function row_to_json(row)
 */
pub unsafe fn row_to_json(fcinfo: FunctionCallInfo) -> Datum {
    let array: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let result: StringInfo;

    result = makeStringInfo();

    composite_to_json(array, result, false);

    PG_RETURN_TEXT_P!(cstring_to_text_with_len((*result).data, (*result).len));
}

/*
 * SQL function row_to_json(row, prettybool)
 */
pub unsafe fn row_to_json_pretty(fcinfo: FunctionCallInfo) -> Datum {
    let array: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let use_line_feeds: bool = PG_GETARG_BOOL!(fcinfo, 1);
    let result: StringInfo;

    result = makeStringInfo();

    composite_to_json(array, result, use_line_feeds);

    PG_RETURN_TEXT_P!(cstring_to_text_with_len((*result).data, (*result).len));
}

/*
 * Is the given type immutable when coming out of a JSON context?
 *
 * At present, datetimes are all considered mutable, because they
 * depend on timezone.  XXX we should also drill down into objects
 * and arrays, but do not.
 */
pub unsafe fn to_json_is_immutable(typoid: Oid) -> bool {
    let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
    let mut outfuncoid: Oid = InvalidOid;

    json_categorize_type(typoid, false, &mut tcategory, &mut outfuncoid);

    match tcategory {
        JSONTYPE_BOOL | JSONTYPE_JSON | JSONTYPE_JSONB | JSONTYPE_NULL => true,

        JSONTYPE_DATE | JSONTYPE_TIMESTAMP | JSONTYPE_TIMESTAMPTZ => false,

        JSONTYPE_ARRAY => false, /* TODO recurse into elements */

        JSONTYPE_COMPOSITE => false, /* TODO recurse into fields */

        JSONTYPE_NUMERIC | JSONTYPE_CAST | JSONTYPE_OTHER => {
            func_volatile(outfuncoid) == PROVOLATILE_IMMUTABLE
        }
    }
}

/*
 * SQL function to_json(anyvalue)
 */
pub unsafe fn to_json(fcinfo: FunctionCallInfo) -> Datum {
    let val: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let val_type: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 0);
    let mut tcategory: JsonTypeCategory = JSONTYPE_NULL;
    let mut outfuncoid: Oid = InvalidOid;

    if val_type == InvalidOid {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("could not determine input data type"));
    }

    json_categorize_type(val_type, false, &mut tcategory, &mut outfuncoid);

    PG_RETURN_DATUM!(datum_to_json(val, tcategory, outfuncoid));
}

/*
 * Turn a Datum into JSON text.
 *
 * tcategory and outfuncoid are from a previous call to json_categorize_type.
 */
pub unsafe fn datum_to_json(val: Datum, tcategory: JsonTypeCategory, outfuncoid: Oid) -> Datum {
    let result: StringInfo = makeStringInfo();

    datum_to_json_internal(val, false, result, tcategory, outfuncoid, false);

    PointerGetDatum(cstring_to_text_with_len((*result).data, (*result).len) as *const c_void)
}

// ===========================================================================
// json_agg / json_object_agg transition + final functions -- STUBBED.
// All need AggCheckCallContext (executor/nodeAgg.c), get_fn_expr_argtype, and
// json_categorize_type/datum_to_json_internal.
// ===========================================================================

/*
 * json_agg transition function
 *
 * aggregate input column as a json array value.
 */
unsafe fn json_agg_transfn_worker(fcinfo: FunctionCallInfo, absent_on_null: bool) -> Datum {
    let mut aggcontext: MemoryContext = null_mut();
    let oldcontext: MemoryContext;
    let state: *mut JsonAggState;
    let val: Datum;

    if !AggCheckCallContext(fcinfo, &mut aggcontext) {
        /* cannot be called directly because of internal-type argument */
        elog!(ERROR, "json_agg_transfn called in non-aggregate context");
    }

    if PG_ARGISNULL!(fcinfo, 0) {
        let arg_type: Oid = get_fn_expr_argtype((*fcinfo).flinfo, 1);

        if arg_type == InvalidOid {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(ERROR, errmsg!("could not determine input data type"));
        }

        /*
         * Make this state object in a context where it will persist for the
         * duration of the aggregate call.  MemoryContextSwitchTo is only
         * needed the first time, as the StringInfo routines make sure they
         * use the right context to enlarge the object if necessary.
         */
        oldcontext = MemoryContextSwitchTo(aggcontext);
        state = palloc(core::mem::size_of::<JsonAggState>() as Size) as *mut JsonAggState;
        (*state).str = makeStringInfo();
        MemoryContextSwitchTo(oldcontext);

        appendStringInfoChar((*state).str, b'[' as c_char);
        json_categorize_type(
            arg_type,
            false,
            &mut (*state).val_category,
            &mut (*state).val_output_func,
        );
    } else {
        state = PG_GETARG_POINTER!(fcinfo, 0) as *mut JsonAggState;
    }

    if absent_on_null && PG_ARGISNULL!(fcinfo, 1) {
        PG_RETURN_POINTER!(state);
    }

    if (*(*state).str).len > 1 {
        appendStringInfoString((*state).str, c", ".as_ptr());
    }

    /* fast path for NULLs */
    if PG_ARGISNULL!(fcinfo, 1) {
        datum_to_json_internal(
            0 as Datum,
            true,
            (*state).str,
            JSONTYPE_NULL,
            InvalidOid,
            false,
        );
        PG_RETURN_POINTER!(state);
    }

    val = PG_GETARG_DATUM!(fcinfo, 1);

    /* add some whitespace if structured type and not first item */
    if !PG_ARGISNULL!(fcinfo, 0)
        && (*(*state).str).len > 1
        && ((*state).val_category == JSONTYPE_ARRAY
            || (*state).val_category == JSONTYPE_COMPOSITE)
    {
        appendStringInfoString((*state).str, c"\n ".as_ptr());
    }

    datum_to_json_internal(
        val,
        false,
        (*state).str,
        (*state).val_category,
        (*state).val_output_func,
        false,
    );

    /*
     * The transition type for json_agg() is declared to be "internal", which
     * is a pass-by-value type the same size as a pointer.  So we can safely
     * pass the JsonAggState pointer through nodeAgg.c's machinations.
     */
    PG_RETURN_POINTER!(state);
}

/*
 * json_agg aggregate function
 */
pub unsafe fn json_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    json_agg_transfn_worker(fcinfo, false)
}

/*
 * json_agg_strict aggregate function
 */
pub unsafe fn json_agg_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    json_agg_transfn_worker(fcinfo, true)
}

/*
 * json_agg final function
 */
pub unsafe fn json_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut JsonAggState;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, null_mut()));

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut JsonAggState
    };

    /* NULL result for no rows in, as is standard with aggregates */
    if state.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Else return state with appropriate array terminator added */
    PG_RETURN_TEXT_P!(catenate_stringinfo_string((*state).str, c"]".as_ptr()));
}

/*
 * json_object_agg transition function.
 *
 * aggregate two input columns as a single json object value.
 */
unsafe fn json_object_agg_transfn_worker(
    fcinfo: FunctionCallInfo,
    absent_on_null: bool,
    unique_keys: bool,
) -> Datum {
    let mut aggcontext: MemoryContext = null_mut();
    let oldcontext: MemoryContext;
    let state: *mut JsonAggState;
    let out: StringInfo;
    let mut arg: Datum;
    let skip: bool;
    let key_offset: c_int;

    if !AggCheckCallContext(fcinfo, &mut aggcontext) {
        /* cannot be called directly because of internal-type argument */
        elog!(ERROR, "json_object_agg_transfn called in non-aggregate context");
    }

    if PG_ARGISNULL!(fcinfo, 0) {
        let mut arg_type: Oid;

        /*
         * Make the StringInfo in a context where it will persist for the
         * duration of the aggregate call. Switching context is only needed
         * for this initial step, as the StringInfo and dynahash routines make
         * sure they use the right context to enlarge the object if necessary.
         */
        oldcontext = MemoryContextSwitchTo(aggcontext);
        state = palloc(core::mem::size_of::<JsonAggState>() as Size) as *mut JsonAggState;
        (*state).str = makeStringInfo();
        if unique_keys {
            json_unique_builder_init(&mut (*state).unique_check);
        } else {
            memset(
                &mut (*state).unique_check as *mut JsonUniqueBuilderState as *mut c_void,
                0,
                core::mem::size_of::<JsonUniqueBuilderState>(),
            );
        }
        MemoryContextSwitchTo(oldcontext);

        arg_type = get_fn_expr_argtype((*fcinfo).flinfo, 1);

        if arg_type == InvalidOid {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(ERROR, errmsg!("could not determine data type for argument {}", 1));
        }

        json_categorize_type(
            arg_type,
            false,
            &mut (*state).key_category,
            &mut (*state).key_output_func,
        );

        arg_type = get_fn_expr_argtype((*fcinfo).flinfo, 2);

        if arg_type == InvalidOid {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(ERROR, errmsg!("could not determine data type for argument {}", 2));
        }

        json_categorize_type(
            arg_type,
            false,
            &mut (*state).val_category,
            &mut (*state).val_output_func,
        );

        appendStringInfoString((*state).str, c"{ ".as_ptr());
    } else {
        state = PG_GETARG_POINTER!(fcinfo, 0) as *mut JsonAggState;
    }

    /*
     * Note: since json_object_agg() is declared as taking type "any", the
     * parser will not do any type conversion on unknown-type literals (that
     * is, undecorated strings or NULLs).  Such values will arrive here as
     * type UNKNOWN, which fortunately does not matter to us, since
     * unknownout() works fine.
     */

    if PG_ARGISNULL!(fcinfo, 1) {
        let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
        ereport!(ERROR, errmsg!("null value not allowed for object key"));
    }

    /* Skip null values if absent_on_null */
    skip = absent_on_null && PG_ARGISNULL!(fcinfo, 2);

    if skip {
        /*
         * We got a NULL value and we're not storing those; if we're not
         * testing key uniqueness, we're done.  If we are, use the throwaway
         * buffer to store the key name so that we can check it.
         */
        if !unique_keys {
            PG_RETURN_POINTER!(state);
        }

        out = json_unique_builder_get_throwawaybuf(&mut (*state).unique_check);
    } else {
        out = (*state).str;

        /*
         * Append comma delimiter only if we have already output some fields
         * after the initial string "{ ".
         */
        if (*out).len > 2 {
            appendStringInfoString(out, c", ".as_ptr());
        }
    }

    arg = PG_GETARG_DATUM!(fcinfo, 1);

    key_offset = (*out).len;

    datum_to_json_internal(
        arg,
        false,
        out,
        (*state).key_category,
        (*state).key_output_func,
        true,
    );

    if unique_keys {
        /*
         * Copy the key first, instead of pointing into the buffer. It will be
         * added to the hash table, but the buffer may get reallocated as
         * we're appending more data to it. That would invalidate pointers to
         * keys in the current buffer.
         */
        let key: *const c_char =
            MemoryContextStrdup(aggcontext, (*out).data.add(key_offset as usize));

        if !json_unique_check_key(&mut (*state).unique_check.check, key, 0) {
            let _ = errcode(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE);
            ereport!(
                ERROR,
                errmsg!(
                    "duplicate JSON object key value: {}",
                    std::ffi::CStr::from_ptr(key).to_string_lossy()
                )
            );
        }

        if skip {
            PG_RETURN_POINTER!(state);
        }
    }

    appendStringInfoString((*state).str, c" : ".as_ptr());

    if PG_ARGISNULL!(fcinfo, 2) {
        arg = 0 as Datum;
    } else {
        arg = PG_GETARG_DATUM!(fcinfo, 2);
    }

    datum_to_json_internal(
        arg,
        PG_ARGISNULL!(fcinfo, 2),
        (*state).str,
        (*state).val_category,
        (*state).val_output_func,
        false,
    );

    PG_RETURN_POINTER!(state);
}

/*
 * json_object_agg aggregate function
 */
pub unsafe fn json_object_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    json_object_agg_transfn_worker(fcinfo, false, false)
}

/*
 * json_object_agg_strict aggregate function
 */
pub unsafe fn json_object_agg_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    json_object_agg_transfn_worker(fcinfo, true, false)
}

/*
 * json_object_agg_unique aggregate function
 */
pub unsafe fn json_object_agg_unique_transfn(fcinfo: FunctionCallInfo) -> Datum {
    json_object_agg_transfn_worker(fcinfo, false, true)
}

/*
 * json_object_agg_unique_strict aggregate function
 */
pub unsafe fn json_object_agg_unique_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    json_object_agg_transfn_worker(fcinfo, true, true)
}

/*
 * json_object_agg final function.
 */
pub unsafe fn json_object_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut JsonAggState;

    /* cannot be called directly because of internal-type argument */
    Assert!(AggCheckCallContext(fcinfo, null_mut()));

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut JsonAggState
    };

    /* NULL result for no rows in, as is standard with aggregates */
    if state.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Else return state with appropriate object terminator added */
    PG_RETURN_TEXT_P!(catenate_stringinfo_string((*state).str, c" }".as_ptr()));
}

/*
 * catenate_stringinfo_string: return buffer->contents + addon as a text datum.
 * This is self-contained (no catalog deps), so it is translated fully even
 * though its only callers (the agg finalfns) are stubbed.
 *
 * # Safety
 * `buffer` is a valid StringInfo; `addon` is a NUL-terminated C string.
 */
unsafe fn catenate_stringinfo_string(buffer: StringInfo, addon: *const c_char) -> *mut text {
    /* custom version of cstring_to_text_with_len */
    let buflen: c_int = (*buffer).len;
    let addlen: c_int = strlen(addon) as c_int;
    let result: *mut text = palloc((buflen + addlen + VARHDRSZ) as Size) as *mut text;

    SET_VARSIZE(result as *mut c_char, buflen + addlen + VARHDRSZ);
    memcpy(
        VARDATA(result as *const c_char) as *mut c_void,
        (*buffer).data as *const c_void,
        buflen as usize,
    );
    memcpy(
        VARDATA(result as *const c_char).add(buflen as usize) as *mut c_void,
        addon as *const c_void,
        addlen as usize,
    );

    result
}

// ===========================================================================
// json_build_object / json_build_array / json_object -- STUBBED.
// All need add_json / escape_json_text dispatch + extract_variadic_args /
// deconstruct_array_builtin.
// ===========================================================================

pub unsafe fn json_build_object_worker(
    nargs: c_int,
    args: *const Datum,
    nulls: *const bool,
    types: *const Oid,
    absent_on_null: bool,
    unique_keys: bool,
) -> Datum {
    let mut i: c_int;
    let mut sep: *const c_char = c"".as_ptr();
    let result: StringInfo;
    let mut unique_check: JsonUniqueBuilderState = core::mem::zeroed();

    if nargs % 2 != 0 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        /* C also: errhint("The arguments of %s must consist of alternating keys and values.", "json_build_object()") */
        ereport!(
            ERROR,
            errmsg!("argument list must have even number of elements")
        );
    }

    result = makeStringInfo();

    appendStringInfoChar(result, b'{' as c_char);

    if unique_keys {
        json_unique_builder_init(&mut unique_check);
    }

    i = 0;
    while i < nargs {
        let out: StringInfo;
        let skip: bool;
        let key_offset: c_int;

        /* Skip null values if absent_on_null */
        skip = absent_on_null && *nulls.add((i + 1) as usize);

        if skip {
            /* If key uniqueness check is needed we must save skipped keys */
            if !unique_keys {
                i += 2;
                continue;
            }

            out = json_unique_builder_get_throwawaybuf(&mut unique_check);
        } else {
            appendStringInfoString(result, sep);
            sep = c", ".as_ptr();
            out = result;
        }

        /* process key */
        if *nulls.add(i as usize) {
            let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
            ereport!(ERROR, errmsg!("null value not allowed for object key"));
        }

        /* save key offset before appending it */
        key_offset = (*out).len;

        add_json(*args.add(i as usize), false, out, *types.add(i as usize), true);

        if unique_keys {
            /*
             * check key uniqueness after key appending
             *
             * Copy the key first, instead of pointing into the buffer. It
             * will be added to the hash table, but the buffer may get
             * reallocated as we're appending more data to it. That would
             * invalidate pointers to keys in the current buffer.
             */
            let key: *const c_char = pstrdup((*out).data.add(key_offset as usize));

            if !json_unique_check_key(&mut unique_check.check, key, 0) {
                let _ = errcode(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE);
                ereport!(
                    ERROR,
                    errmsg!(
                        "duplicate JSON object key value: {}",
                        std::ffi::CStr::from_ptr(key).to_string_lossy()
                    )
                );
            }

            if skip {
                i += 2;
                continue;
            }
        }

        appendStringInfoString(result, c" : ".as_ptr());

        /* process value */
        add_json(
            *args.add((i + 1) as usize),
            *nulls.add((i + 1) as usize),
            result,
            *types.add((i + 1) as usize),
            false,
        );

        i += 2;
    }

    appendStringInfoChar(result, b'}' as c_char);

    PointerGetDatum(cstring_to_text_with_len((*result).data, (*result).len) as *const c_void)
}

/*
 * SQL function json_build_object(variadic "any")
 */
pub unsafe fn json_build_object(fcinfo: FunctionCallInfo) -> Datum {
    let mut args: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();
    let mut types: *mut Oid = null_mut();

    /* build argument values to build the object */
    let nargs: c_int =
        extract_variadic_args(fcinfo, 0, true, &mut args, &mut types, &mut nulls);

    if nargs < 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(json_build_object_worker(
        nargs, args, nulls, types, false, false
    ));
}

pub unsafe fn json_build_object_noargs(_fcinfo: FunctionCallInfo) -> Datum {
    // C: PG_RETURN_TEXT_P(cstring_to_text_with_len("{}", 2));
    // Self-contained; translated fully.
    PG_RETURN_TEXT_P!(cstring_to_text_with_len(c"{}".as_ptr(), 2));
}

pub unsafe fn json_build_array_worker(
    nargs: c_int,
    args: *const Datum,
    nulls: *const bool,
    types: *const Oid,
    absent_on_null: bool,
) -> Datum {
    let mut i: c_int;
    let mut sep: *const c_char = c"".as_ptr();
    let result: StringInfo;

    result = makeStringInfo();

    appendStringInfoChar(result, b'[' as c_char);

    i = 0;
    while i < nargs {
        if absent_on_null && *nulls.add(i as usize) {
            i += 1;
            continue;
        }

        appendStringInfoString(result, sep);
        sep = c", ".as_ptr();
        add_json(
            *args.add(i as usize),
            *nulls.add(i as usize),
            result,
            *types.add(i as usize),
            false,
        );
        i += 1;
    }

    appendStringInfoChar(result, b']' as c_char);

    PointerGetDatum(cstring_to_text_with_len((*result).data, (*result).len) as *const c_void)
}

/*
 * SQL function json_build_array(variadic "any")
 */
pub unsafe fn json_build_array(fcinfo: FunctionCallInfo) -> Datum {
    let mut args: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();
    let mut types: *mut Oid = null_mut();

    /* build argument values to build the object */
    let nargs: c_int =
        extract_variadic_args(fcinfo, 0, true, &mut args, &mut types, &mut nulls);

    if nargs < 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(json_build_array_worker(nargs, args, nulls, types, false));
}

pub unsafe fn json_build_array_noargs(_fcinfo: FunctionCallInfo) -> Datum {
    // C: PG_RETURN_TEXT_P(cstring_to_text_with_len("[]", 2));
    PG_RETURN_TEXT_P!(cstring_to_text_with_len(c"[]".as_ptr(), 2));
}

/*
 * SQL function json_object(text[])
 *
 * take a one or two dimensional array of text as key/value pairs
 * for a json object.
 */
pub unsafe fn json_object(fcinfo: FunctionCallInfo) -> Datum {
    let in_array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let ndims: c_int = ARR_NDIM(in_array);
    let mut result: StringInfoData = core::mem::zeroed();
    let mut in_datums: *mut Datum = null_mut();
    let mut in_nulls: *mut bool = null_mut();
    let mut in_count: c_int = 0;
    let count: c_int;
    let mut i: c_int;
    let rval: *mut text;

    match ndims {
        0 => {
            PG_RETURN_DATUM!(CStringGetTextDatum(c"{}".as_ptr()));
        }

        1 => {
            if (*ARR_DIMS(in_array).add(0)) % 2 != 0 {
                let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
                ereport!(ERROR, errmsg!("array must have even number of elements"));
            }
        }

        2 => {
            if (*ARR_DIMS(in_array).add(1)) != 2 {
                let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
                ereport!(ERROR, errmsg!("array must have two columns"));
            }
        }

        _ => {
            let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
            ereport!(ERROR, errmsg!("wrong number of array subscripts"));
        }
    }

    deconstruct_array_builtin(in_array, TEXTOID, &mut in_datums, &mut in_nulls, &mut in_count);

    count = in_count / 2;

    initStringInfo(&mut result);

    appendStringInfoChar(&mut result, b'{' as c_char);

    i = 0;
    while i < count {
        if *in_nulls.add((i * 2) as usize) {
            let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
            ereport!(ERROR, errmsg!("null value not allowed for object key"));
        }

        if i > 0 {
            appendStringInfoString(&mut result, c", ".as_ptr());
        }
        escape_json_text(
            &mut result,
            DatumGetPointer(*in_datums.add((i * 2) as usize)) as *mut text,
        );
        appendStringInfoString(&mut result, c" : ".as_ptr());
        if *in_nulls.add((i * 2 + 1) as usize) {
            appendStringInfoString(&mut result, c"null".as_ptr());
        } else {
            escape_json_text(
                &mut result,
                DatumGetPointer(*in_datums.add((i * 2 + 1) as usize)) as *mut text,
            );
        }
        i += 1;
    }

    appendStringInfoChar(&mut result, b'}' as c_char);

    pfree(in_datums as *mut c_void);
    pfree(in_nulls as *mut c_void);

    rval = cstring_to_text_with_len(result.data, result.len);
    pfree(result.data as *mut c_void);

    PG_RETURN_TEXT_P!(rval);
}

/*
 * SQL function json_object(text[], text[])
 *
 * take separate key and value arrays of text to construct a json object
 * pairwise.
 */
pub unsafe fn json_object_two_arg(fcinfo: FunctionCallInfo) -> Datum {
    let key_array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let val_array: *mut ArrayType = PG_GETARG_ARRAYTYPE_P!(fcinfo, 1);
    let nkdims: c_int = ARR_NDIM(key_array);
    let nvdims: c_int = ARR_NDIM(val_array);
    let mut result: StringInfoData = core::mem::zeroed();
    let mut key_datums: *mut Datum = null_mut();
    let mut val_datums: *mut Datum = null_mut();
    let mut key_nulls: *mut bool = null_mut();
    let mut val_nulls: *mut bool = null_mut();
    let mut key_count: c_int = 0;
    let mut val_count: c_int = 0;
    let mut i: c_int;
    let rval: *mut text;

    if nkdims > 1 || nkdims != nvdims {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(ERROR, errmsg!("wrong number of array subscripts"));
    }

    if nkdims == 0 {
        PG_RETURN_DATUM!(CStringGetTextDatum(c"{}".as_ptr()));
    }

    deconstruct_array_builtin(key_array, TEXTOID, &mut key_datums, &mut key_nulls, &mut key_count);
    deconstruct_array_builtin(val_array, TEXTOID, &mut val_datums, &mut val_nulls, &mut val_count);

    if key_count != val_count {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(ERROR, errmsg!("mismatched array dimensions"));
    }

    initStringInfo(&mut result);

    appendStringInfoChar(&mut result, b'{' as c_char);

    i = 0;
    while i < key_count {
        if *key_nulls.add(i as usize) {
            let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
            ereport!(ERROR, errmsg!("null value not allowed for object key"));
        }

        if i > 0 {
            appendStringInfoString(&mut result, c", ".as_ptr());
        }
        escape_json_text(
            &mut result,
            DatumGetPointer(*key_datums.add(i as usize)) as *mut text,
        );
        appendStringInfoString(&mut result, c" : ".as_ptr());
        if *val_nulls.add(i as usize) {
            appendStringInfoString(&mut result, c"null".as_ptr());
        } else {
            escape_json_text(
                &mut result,
                DatumGetPointer(*val_datums.add(i as usize)) as *mut text,
            );
        }
        i += 1;
    }

    appendStringInfoChar(&mut result, b'}' as c_char);

    pfree(key_datums as *mut c_void);
    pfree(key_nulls as *mut c_void);
    pfree(val_datums as *mut c_void);
    pfree(val_nulls as *mut c_void);

    rval = cstring_to_text_with_len(result.data, result.len);
    pfree(result.data as *mut c_void);

    PG_RETURN_TEXT_P!(rval);
}

// ===========================================================================
// escape_json family -- TRANSLATED FULLY (self-contained, real path).
// ===========================================================================

/*
 * escape_json_char
 *		Inline helper for the escape_json* functions: append one byte to `buf`,
 *		emitting the appropriate JSON escape sequence.
 *
 * # Safety
 * `buf` is a writable StringInfo.
 */
#[inline(always)]
unsafe fn escape_json_char(buf: StringInfo, c: c_char) {
    match c as u8 {
        0x08 /* '\b' */ => appendStringInfoString(buf, c"\\b".as_ptr()),
        0x0c /* '\f' */ => appendStringInfoString(buf, c"\\f".as_ptr()),
        b'\n' => appendStringInfoString(buf, c"\\n".as_ptr()),
        b'\r' => appendStringInfoString(buf, c"\\r".as_ptr()),
        b'\t' => appendStringInfoString(buf, c"\\t".as_ptr()),
        b'"' => appendStringInfoString(buf, c"\\\"".as_ptr()),
        b'\\' => appendStringInfoString(buf, c"\\\\".as_ptr()),
        _ => {
            if (c as u8) < b' ' {
                // C: appendStringInfo(buf, "\\u%04x", (int) c);
                appendStringInfo!(buf, "\\u{:04x}", c as c_int);
            } else {
                appendStringInfoCharMacro!(buf, c);
            }
        }
    }
}

/*
 * escape_json
 *		Produce a JSON string literal, properly escaping the NUL-terminated
 *		cstring.
 *
 * # Safety
 * `buf` is a writable StringInfo; `str` is a valid NUL-terminated C string.
 */
pub unsafe fn escape_json(buf: StringInfo, mut str: *const c_char) {
    appendStringInfoCharMacro!(buf, b'"' as c_char);

    while *str != 0 {
        escape_json_char(buf, *str);
        str = str.add(1);
    }

    appendStringInfoCharMacro!(buf, b'"' as c_char);
}

/*
 * escape_json_with_len
 *		Produce a JSON string literal, properly escaping the possibly not
 *		NUL-terminated `len` bytes of `str`.
 *
 * The upstream code uses port/simd.h Vector8 primitives to scan sizeof(Vector8)
 * bytes at a time for characters needing escapes, falling back to a per-byte
 * loop only around hits and the tail.  port/simd.h is NOT yet ported, so we
 * implement the equivalent scalar behavior: scan forward for the next byte that
 * needs escaping, bulk-copy the clean run with appendBinaryStringInfo, then
 * escape the single offending byte.  Output is byte-for-byte identical to the
 * SIMD path; only the internal batching differs.
 *
 * # Safety
 * `buf` is a writable StringInfo; `str` is readable for `len` bytes; `len >= 0`.
 */
pub unsafe fn escape_json_with_len(buf: StringInfo, str: *const c_char, len: c_int) {
    Assert!(len >= 0);

    /*
     * Since we know the minimum length we'll need to append, enlarge the buffer
     * now rather than incrementally.  Add two extra bytes for the quotes.
     */
    enlargeStringInfo(buf, len + 2);

    appendStringInfoCharMacro!(buf, b'"' as c_char);

    /*
     * Scalar equivalent of the Vector8 loop: copypos marks the start of the
     * current clean (no-escape) run; i scans forward.  When we hit a byte that
     * needs escaping (< 0x20, '"', or '\\') we flush [copypos, i) in one go and
     * escape str[i].
     */
    let mut i: c_int = 0;
    let mut copypos: c_int = 0;
    while i < len {
        let ch = *str.add(i as usize) as u8;
        let needs_escape = ch <= 0x1F || ch == b'"' || ch == b'\\';
        if needs_escape {
            if copypos < i {
                appendBinaryStringInfo(
                    buf,
                    str.add(copypos as usize) as *const c_void,
                    i - copypos,
                );
            }
            escape_json_char(buf, ch as c_char);
            i += 1;
            copypos = i;
        } else {
            i += 1;
        }
    }

    /* flush the trailing clean run */
    if copypos < i {
        appendBinaryStringInfo(
            buf,
            str.add(copypos as usize) as *const c_void,
            i - copypos,
        );
    }

    appendStringInfoCharMacro!(buf, b'"' as c_char);
}

/*
 * escape_json_text
 *		Append the (possibly toasted) text value `txt` onto `buf`, escaped via
 *		escape_json_with_len.  More efficient than text_to_cstring + escape_json.
 *
 * # Safety
 * `buf` is a writable StringInfo; `txt` is a valid text datum.
 */
pub unsafe fn escape_json_text(buf: StringInfo, txt: *const text) {
    /* must cast away the const, unfortunately */
    let tunpacked: *mut text = pg_detoast_datum_packed(txt as *mut c_void) as *mut text;
    let len: c_int = VARSIZE_ANY_EXHDR(tunpacked as *const c_char) as c_int;
    let str: *mut c_char;

    str = VARDATA_ANY(tunpacked as *const c_char);

    escape_json_with_len(buf, str, len);

    /* pfree any detoasted values */
    if tunpacked != txt as *mut text {
        pfree(tunpacked as *mut c_void);
    }
}

// ===========================================================================
// json_validate -- semantic actions + the validator.
// The validator keeps its full C structure; the actual parse goes through the
// unported jsonapi stub (pg_parse_json), so it compiles but reaches
// unimplemented!() when a key-uniqueness/validation parse is attempted.
// ===========================================================================

/* Stack element for key uniqueness check during JSON parsing */
#[repr(C)]
struct JsonUniqueStackEntry {
    parent: *mut JsonUniqueStackEntry,
    object_id: c_int,
}

/* Context struct for key uniqueness check during JSON parsing */
#[repr(C)]
struct JsonUniqueParsingState {
    lex: *mut JsonLexContext,
    check: JsonUniqueCheckState,
    stack: *mut JsonUniqueStackEntry,
    id_counter: c_int,
    unique: bool,
}

/*
 * Uniqueness detection support.
 *
 * In order to detect uniqueness during building or parsing of a JSON
 * object, we maintain a hash table of key names already seen.
 */
unsafe fn json_unique_check_init(cxt: *mut JsonUniqueCheckState) {
    let mut ctl: HASHCTL = core::mem::zeroed();

    memset(
        &mut ctl as *mut HASHCTL as *mut c_void,
        0,
        core::mem::size_of::<HASHCTL>(),
    );
    ctl.keysize = core::mem::size_of::<JsonUniqueHashEntry>();
    ctl.entrysize = core::mem::size_of::<JsonUniqueHashEntry>();
    ctl.hcxt = CurrentMemoryContext;
    ctl.hash = json_unique_hash;
    ctl.r#match = json_unique_hash_match;

    *cxt = hash_create(
        c"json object hashtable".as_ptr(),
        32,
        &mut ctl,
        HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE,
    );
}

unsafe fn json_unique_builder_init(cxt: *mut JsonUniqueBuilderState) {
    json_unique_check_init(&mut (*cxt).check);
    (*cxt).mcxt = CurrentMemoryContext;
    (*cxt).skipped_keys.data = null_mut();
}

unsafe fn json_unique_check_key(
    cxt: *mut JsonUniqueCheckState,
    key: *const c_char,
    object_id: c_int,
) -> bool {
    let mut entry: JsonUniqueHashEntry = core::mem::zeroed();
    let mut found: bool = false;

    entry.key = key;
    entry.key_len = strlen(key) as c_int;
    entry.object_id = object_id;

    hash_search(
        *cxt,
        &mut entry as *mut JsonUniqueHashEntry as *const c_void,
        HASH_ENTER,
        &mut found,
    );

    !found
}

/*
 * On-demand initialization of a throwaway StringInfo.  This is used to
 * read a key name that we don't need to store in the output object, for
 * duplicate key detection when the value is NULL.
 */
unsafe fn json_unique_builder_get_throwawaybuf(cxt: *mut JsonUniqueBuilderState) -> StringInfo {
    let out: StringInfo = &mut (*cxt).skipped_keys;

    if (*out).data.is_null() {
        let oldcxt: MemoryContext = MemoryContextSwitchTo((*cxt).mcxt);

        initStringInfo(out);
        MemoryContextSwitchTo(oldcxt);
    } else {
        /* Just reset the string to empty */
        (*out).len = 0;
    }

    out
}

/* Semantic actions for key uniqueness check */
unsafe fn json_unique_object_start(_state: *mut c_void) -> JsonParseErrorType {
    let state = _state as *mut JsonUniqueParsingState;

    if !(*state).unique {
        return JSON_SUCCESS;
    }

    /* push object entry to stack */
    let entry =
        palloc(core::mem::size_of::<JsonUniqueStackEntry>()) as *mut JsonUniqueStackEntry;
    (*entry).object_id = (*state).id_counter;
    (*state).id_counter += 1;
    (*entry).parent = (*state).stack;
    (*state).stack = entry;

    JSON_SUCCESS
}

unsafe fn json_unique_object_end(_state: *mut c_void) -> JsonParseErrorType {
    let state = _state as *mut JsonUniqueParsingState;

    if !(*state).unique {
        return JSON_SUCCESS;
    }

    let entry = (*state).stack;
    (*state).stack = (*entry).parent; /* pop object from stack */
    pfree(entry as *mut c_void);
    JSON_SUCCESS
}

unsafe fn json_unique_object_field_start(
    _state: *mut c_void,
    field: *mut c_char,
    _isnull: bool,
) -> JsonParseErrorType {
    let state = _state as *mut JsonUniqueParsingState;

    if !(*state).unique {
        return JSON_SUCCESS;
    }

    /* find key collision in the current object */
    if json_unique_check_key(
        core::ptr::addr_of_mut!((*state).check),
        field,
        (*(*state).stack).object_id,
    ) {
        return JSON_SUCCESS;
    }

    (*state).unique = false;

    /* pop all objects entries */
    let mut entry = (*state).stack;
    while !entry.is_null() {
        (*state).stack = (*entry).parent;
        pfree(entry as *mut c_void);
        entry = (*state).stack;
    }
    JSON_SUCCESS
}

/*
 * json_validate: validate JSON text and optionally check key uniqueness.
 * Returns true if ok; on failure either raises (throw_error) or returns false.
 *
 * # Safety
 * `json` is a valid text datum.
 */
pub unsafe fn json_validate(
    json: *mut text,
    check_unique_keys: bool,
    throw_error: bool,
) -> bool {
    let mut lex: JsonLexContext = core::mem::zeroed();
    let mut unique_sem_action: JsonSemAction = JsonSemAction::zeroed();
    let mut state: JsonUniqueParsingState = core::mem::zeroed();
    let result: JsonParseErrorType;

    makeJsonLexContext(&mut lex, json, check_unique_keys);

    if check_unique_keys {
        state.lex = &mut lex as *mut JsonLexContext;
        state.stack = null_mut();
        state.id_counter = 0;
        state.unique = true;
        json_unique_check_init(core::ptr::addr_of_mut!(state.check));

        unique_sem_action.semstate = core::ptr::addr_of_mut!(state) as *mut c_void;
        unique_sem_action.object_start = Some(json_unique_object_start);
        unique_sem_action.object_field_start = Some(json_unique_object_field_start);
        unique_sem_action.object_end = Some(json_unique_object_end);
    }

    result = pg_parse_json(
        &mut lex,
        if check_unique_keys {
            core::ptr::addr_of_mut!(unique_sem_action)
        } else {
            core::ptr::addr_of_mut!(nullSemAction)
        },
    );

    if result != JSON_SUCCESS {
        if throw_error {
            json_errsave_error(result, &mut lex, null_mut());
        }

        return false; /* invalid json */
    }

    if check_unique_keys && !state.unique {
        if throw_error {
            let _ = errcode(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE);
            ereport!(ERROR, errmsg!("duplicate JSON object key value"));
        }

        return false; /* not unique keys */
    }

    if check_unique_keys {
        freeJsonLexContext(&mut lex);
    }

    true /* ok */
}

// ===========================================================================
// json_typeof -- lex the first token and map it to a type name.
// Structure translated fully; the single json_lex call goes through the
// unported jsonapi stub.
// ===========================================================================

/*
 * SQL function json_typeof(json) -> text.
 *
 * Returns the type of the outermost JSON value as text: "object", "array",
 * "string", "number", "boolean", or "null".
 */
pub unsafe fn json_typeof(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let mut lex: JsonLexContext = core::mem::zeroed();
    let type_: *const c_char;
    let result: JsonParseErrorType;

    /* Lex exactly one token from the input and check its type. */
    makeJsonLexContext(&mut lex, json, false);
    result = json_lex(&mut lex);
    if result != JSON_SUCCESS {
        json_errsave_error(result, &mut lex, null_mut());
    }

    type_ = match lex.token_type {
        JSON_TOKEN_OBJECT_START => c"object".as_ptr(),
        JSON_TOKEN_ARRAY_START => c"array".as_ptr(),
        JSON_TOKEN_STRING => c"string".as_ptr(),
        JSON_TOKEN_NUMBER => c"number".as_ptr(),
        JSON_TOKEN_TRUE | JSON_TOKEN_FALSE => c"boolean".as_ptr(),
        JSON_TOKEN_NULL => c"null".as_ptr(),
        _ => {
            // elog(ERROR, ...) is noreturn; the shim panics, but the type
            // checker needs a divergent arm here so the match yields a *const.
            elog!(ERROR, "unexpected json token: {}", lex.token_type as c_int);
            unreachable!()
        }
    };

    PG_RETURN_TEXT_P!(cstring_to_text(type_));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::stringinfo::makeStringInfo;
    use crate::utils::adt::varlena::text_to_cstring;
    use crate::utils::fmgr::DirectFunctionCall1Coll;

    /* Read the C-string contents of a StringInfo back into a Rust String. */
    unsafe fn si_str(s: StringInfo) -> String {
        let bytes = core::slice::from_raw_parts((*s).data as *const u8, (*s).len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    }

    /* Build a text datum from a Rust &str (4-byte header). */
    unsafe fn mk(s: &str) -> Datum {
        let p = cstring_to_text_with_len(s.as_ptr() as *const c_char, s.len() as c_int);
        PointerGetDatum(p as *const c_void)
    }

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    #[test]
    fn escape_json_basic_and_specials() {
        unsafe {
            let s = makeStringInfo();
            escape_json(s, c"he\"llo\n\t\\world".as_ptr());
            // " he \" llo \n \t \\ world "  ->  "he\"llo\n\t\\world"
            assert_eq!(si_str(s), "\"he\\\"llo\\n\\t\\\\world\"");
        }
    }

    #[test]
    fn escape_json_control_char_u_escape() {
        unsafe {
            // 0x01 and 0x1f must become  and ; 0x08/0x0c are \b/\f.
            let s = makeStringInfo();
            let raw = [b'a' as c_char, 0x01, 0x1f, 0x08, 0x0c, b'b' as c_char, 0];
            escape_json(s, raw.as_ptr());
            assert_eq!(si_str(s), "\"a\\u0001\\u001f\\b\\fb\"");
        }
    }

    #[test]
    fn escape_json_with_len_matches_escape_json() {
        unsafe {
            // escape_json_with_len over a non-NUL-terminated slice (no trailing NUL
            // needed) must match escape_json's output for the same content, and the
            // bulk-copy clean-run batching must round-trip embedded specials.
            let content = "plain text, then \"quote\" and \\back\\ and \n newline";
            let s1 = makeStringInfo();
            escape_json_with_len(s1, content.as_ptr() as *const c_char, content.len() as c_int);

            // Compare against escape_json on a NUL-terminated copy (same bytes).
            let mut cz: Vec<c_char> = content.bytes().map(|b| b as c_char).collect();
            cz.push(0);
            let s2 = makeStringInfo();
            escape_json(s2, cz.as_ptr());

            assert_eq!(si_str(s1), si_str(s2));
            assert_eq!(
                si_str(s1),
                "\"plain text, then \\\"quote\\\" and \\\\back\\\\ and \\n newline\""
            );
        }
    }

    #[test]
    fn escape_json_with_len_empty() {
        unsafe {
            let s = makeStringInfo();
            escape_json_with_len(s, c"".as_ptr(), 0);
            assert_eq!(si_str(s), "\"\"");
        }
    }

    #[test]
    fn escape_json_text_roundtrip() {
        unsafe {
            let s = makeStringInfo();
            let t = cstring_to_text_with_len(
                "a\tb\"c".as_ptr() as *const c_char,
                "a\tb\"c".len() as c_int,
            );
            escape_json_text(s, t);
            assert_eq!(si_str(s), "\"a\\tb\\\"c\"");
        }
    }

    #[test]
    fn json_out_returns_text_bytes() {
        unsafe {
            // json_out is just text_to_cstring of the stored text.
            let d = mk("{\"k\": 1}");
            let out = DatumGetPointer(DirectFunctionCall1Coll(json_out, InvalidOid, d))
                as *const c_char;
            assert!(cstr_eq(out, "{\"k\": 1}"));
        }
    }

    #[test]
    fn json_build_noargs_constants() {
        unsafe {
            let obj = DirectFunctionCall1Coll(json_build_object_noargs, InvalidOid, 0);
            let arr = DirectFunctionCall1Coll(json_build_array_noargs, InvalidOid, 0);
            assert!(cstr_eq(
                text_to_cstring(DatumGetPointer(obj) as *const text),
                "{}"
            ));
            assert!(cstr_eq(
                text_to_cstring(DatumGetPointer(arr) as *const text),
                "[]"
            ));
        }
    }

    #[test]
    fn catenate_appends_terminator() {
        unsafe {
            let buf = makeStringInfo();
            appendStringInfoString(buf, c"[1, 2".as_ptr());
            let t = catenate_stringinfo_string(buf, c"]".as_ptr());
            assert!(cstr_eq(text_to_cstring(t), "[1, 2]"));
        }
    }
}
