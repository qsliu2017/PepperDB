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
    appendStringInfo, appendStringInfoCharMacro, PG_GETARG_DATUM, PG_GETARG_POINTER,
    PG_GETARG_TEXT_PP, PG_RETURN_BYTEA_P, PG_RETURN_CSTRING, PG_RETURN_NULL, PG_RETURN_TEXT_P,
};
use crate::c::text;
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use crate::postgres_ext::Oid;
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoString, enlargeStringInfo, StringInfo, StringInfoData,
};
use crate::libpq::pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgtext, pq_sendtext};
use crate::mb::mbutils::GetDatabaseEncoding;
use crate::utils::adt::varlena::{
    cstring_to_text, cstring_to_text_with_len, TextDatumGetCString,
};
use core::ffi::{c_char, c_int, c_void};

// libc bindings (string.h, via postgres.h).  palloc/pfree/pstrdup are prelude.
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
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
) -> bool {
    unimplemented!("pg_parse_json_or_errsave: common/jsonapi.c not yet translated")
}

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
) {
    unimplemented!("json_errsave_error: utils/jsonfuncs.c not yet translated")
}

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
) {
    unimplemented!("json_categorize_type: catalog (lsyscache/typcache) not yet translated")
}

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
#[allow(dead_code)]
unsafe fn datum_to_json_internal(
    _val: Datum,
    _is_null: bool,
    _result: StringInfo,
    _tcategory: JsonTypeCategory,
    _outfuncoid: Oid,
    _key_scalar: bool,
) {
    check_stack_depth();
    unimplemented!("datum_to_json_internal: catalog output funcs / datetime not yet translated")
}

/*
 * JsonEncodeDateTime: encode a datetime Datum as an ISO JSON string.
 * TODO(pg-port): needs utils/date.h + utils/datetime.h (j2date/EncodeDateTime/...).
 */
#[allow(dead_code)]
pub unsafe fn JsonEncodeDateTime(
    _buf: *mut c_char,
    _value: Datum,
    _typid: Oid,
    _tzp: *const c_int,
) -> *mut c_char {
    let _ = ERRCODE_DATETIME_VALUE_OUT_OF_RANGE;
    unimplemented!("JsonEncodeDateTime: utils/date.h + utils/datetime.h not yet translated")
}

/*
 * array_dim_to_json / array_to_json_internal / composite_to_json: STUBBED.
 * TODO(pg-port): need utils/array.h (deconstruct_array, ARR_*) and the composite
 * TupleDesc machinery (lookup_rowtype_tupdesc, heap_getattr).
 */
#[allow(dead_code)]
unsafe fn array_to_json_internal(_array: Datum, _result: StringInfo, _use_line_feeds: bool) {
    unimplemented!("array_to_json_internal: utils/array.h not yet translated")
}

#[allow(dead_code)]
unsafe fn composite_to_json(_composite: Datum, _result: StringInfo, _use_line_feeds: bool) {
    unimplemented!("composite_to_json: TupleDesc/typcache not yet translated")
}

/*
 * add_json: thin wrapper around datum_to_json_internal.  STUBBED with its deps.
 */
#[allow(dead_code)]
unsafe fn add_json(
    _val: Datum,
    _is_null: bool,
    _result: StringInfo,
    _val_type: Oid,
    _key_scalar: bool,
) {
    let _ = ERRCODE_INVALID_PARAMETER_VALUE;
    unimplemented!("add_json: json_categorize_type/datum_to_json_internal not yet translated")
}

/*
 * SQL function array_to_json(anyarray) / array_to_json(anyarray, pretty bool).
 * TODO(pg-port): array_to_json_internal depends on utils/array.h.
 */
pub unsafe fn array_to_json(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("array_to_json: utils/array.h not yet translated")
}

pub unsafe fn array_to_json_pretty(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("array_to_json_pretty: utils/array.h not yet translated")
}

/*
 * SQL function row_to_json(record) / row_to_json(record, pretty bool).
 * TODO(pg-port): composite_to_json depends on TupleDesc/typcache.
 */
pub unsafe fn row_to_json(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("row_to_json: TupleDesc/typcache not yet translated")
}

pub unsafe fn row_to_json_pretty(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("row_to_json_pretty: TupleDesc/typcache not yet translated")
}

/*
 * to_json_is_immutable: planner support; needs func_volatile + json_categorize_type.
 * TODO(pg-port): utils/lsyscache.h (func_volatile) not yet translated.
 */
pub unsafe fn to_json_is_immutable(_typoid: Oid) -> bool {
    unimplemented!("to_json_is_immutable: lsyscache (func_volatile) not yet translated")
}

/*
 * SQL function to_json(anyvalue).
 * TODO(pg-port): needs get_fn_expr_argtype + json_categorize_type + datum_to_json.
 */
pub unsafe fn to_json(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("to_json: json_categorize_type/datum_to_json not yet translated")
}

/*
 * datum_to_json: turn a Datum into a json text Datum.  STUBBED with its deps.
 */
#[allow(dead_code)]
pub unsafe fn datum_to_json(
    _val: Datum,
    _tcategory: JsonTypeCategory,
    _outfuncoid: Oid,
) -> Datum {
    unimplemented!("datum_to_json: datum_to_json_internal not yet translated")
}

// ===========================================================================
// json_agg / json_object_agg transition + final functions -- STUBBED.
// All need AggCheckCallContext (executor/nodeAgg.c), get_fn_expr_argtype, and
// json_categorize_type/datum_to_json_internal.
// ===========================================================================

pub unsafe fn json_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_agg_transfn: nodeAgg/json_categorize_type not yet translated")
}

pub unsafe fn json_agg_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_agg_strict_transfn: nodeAgg/json_categorize_type not yet translated")
}

pub unsafe fn json_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_agg_finalfn: nodeAgg not yet translated")
}

pub unsafe fn json_object_agg_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_object_agg_transfn: nodeAgg/json_categorize_type not yet translated")
}

pub unsafe fn json_object_agg_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_object_agg_strict_transfn: nodeAgg not yet translated")
}

pub unsafe fn json_object_agg_unique_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_object_agg_unique_transfn: nodeAgg not yet translated")
}

pub unsafe fn json_object_agg_unique_strict_transfn(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_object_agg_unique_strict_transfn: nodeAgg not yet translated")
}

pub unsafe fn json_object_agg_finalfn(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_object_agg_finalfn: nodeAgg not yet translated")
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

#[allow(dead_code)]
pub unsafe fn json_build_object_worker(
    _nargs: c_int,
    _args: *const Datum,
    _nulls: *const bool,
    _types: *const Oid,
    _absent_on_null: bool,
    _unique_keys: bool,
) -> Datum {
    unimplemented!("json_build_object_worker: add_json/json_categorize_type not yet translated")
}

pub unsafe fn json_build_object(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_build_object: extract_variadic_args/add_json not yet translated")
}

pub unsafe fn json_build_object_noargs(_fcinfo: FunctionCallInfo) -> Datum {
    // C: PG_RETURN_TEXT_P(cstring_to_text_with_len("{}", 2));
    // Self-contained; translated fully.
    PG_RETURN_TEXT_P!(cstring_to_text_with_len(c"{}".as_ptr(), 2));
}

#[allow(dead_code)]
pub unsafe fn json_build_array_worker(
    _nargs: c_int,
    _args: *const Datum,
    _nulls: *const bool,
    _types: *const Oid,
    _absent_on_null: bool,
) -> Datum {
    unimplemented!("json_build_array_worker: add_json/json_categorize_type not yet translated")
}

pub unsafe fn json_build_array(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_build_array: extract_variadic_args/add_json not yet translated")
}

pub unsafe fn json_build_array_noargs(_fcinfo: FunctionCallInfo) -> Datum {
    // C: PG_RETURN_TEXT_P(cstring_to_text_with_len("[]", 2));
    PG_RETURN_TEXT_P!(cstring_to_text_with_len(c"[]".as_ptr(), 2));
}

pub unsafe fn json_object(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_object: deconstruct_array_builtin/escape_json_text not yet translated")
}

pub unsafe fn json_object_two_arg(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("json_object_two_arg: deconstruct_array_builtin not yet translated")
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

/* hash table for key names (HTAB) -- not yet ported. */
type JsonUniqueCheckState = *mut c_void; // TODO(pg-port): utils/hsearch.h HTAB

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
 * json_unique_check_init / json_unique_check_key: the dynahash-backed key
 * uniqueness set.  TODO(pg-port): utils/hsearch.h (hash_create/hash_search) +
 * common/hashfn.h wiring not yet translated.
 */
unsafe fn json_unique_check_init(_cxt: *mut JsonUniqueCheckState) {
    unimplemented!("json_unique_check_init: utils/hsearch.h not yet translated")
}

unsafe fn json_unique_check_key(
    _cxt: *mut JsonUniqueCheckState,
    _key: *const c_char,
    _object_id: c_int,
) -> bool {
    unimplemented!("json_unique_check_key: utils/hsearch.h not yet translated")
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
