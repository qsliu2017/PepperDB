//! common/jsonapi.h - Declarations for JSON API support.

use std::ffi::{c_char, c_int, c_void};

use crate::c::bits32;
// The default jsonapi_StrValType is StringInfoData (frontend PQExpBufferData
// variant is selected via JSONAPI_USE_PQEXPBUFFER, which we don't model here).
use crate::lib::stringinfo::StringInfoData;

/*
 * typedef enum JsonTokenType
 */
pub type JsonTokenType = c_int;
pub const JSON_TOKEN_INVALID: JsonTokenType = 0;
pub const JSON_TOKEN_STRING: JsonTokenType = 1;
pub const JSON_TOKEN_NUMBER: JsonTokenType = 2;
pub const JSON_TOKEN_OBJECT_START: JsonTokenType = 3;
pub const JSON_TOKEN_OBJECT_END: JsonTokenType = 4;
pub const JSON_TOKEN_ARRAY_START: JsonTokenType = 5;
pub const JSON_TOKEN_ARRAY_END: JsonTokenType = 6;
pub const JSON_TOKEN_COMMA: JsonTokenType = 7;
pub const JSON_TOKEN_COLON: JsonTokenType = 8;
pub const JSON_TOKEN_TRUE: JsonTokenType = 9;
pub const JSON_TOKEN_FALSE: JsonTokenType = 10;
pub const JSON_TOKEN_NULL: JsonTokenType = 11;
pub const JSON_TOKEN_END: JsonTokenType = 12;

/*
 * typedef enum JsonParseErrorType
 */
pub type JsonParseErrorType = c_int;
pub const JSON_SUCCESS: JsonParseErrorType = 0;
pub const JSON_INCOMPLETE: JsonParseErrorType = 1;
pub const JSON_INVALID_LEXER_TYPE: JsonParseErrorType = 2;
pub const JSON_NESTING_TOO_DEEP: JsonParseErrorType = 3;
pub const JSON_ESCAPING_INVALID: JsonParseErrorType = 4;
pub const JSON_ESCAPING_REQUIRED: JsonParseErrorType = 5;
pub const JSON_EXPECTED_ARRAY_FIRST: JsonParseErrorType = 6;
pub const JSON_EXPECTED_ARRAY_NEXT: JsonParseErrorType = 7;
pub const JSON_EXPECTED_COLON: JsonParseErrorType = 8;
pub const JSON_EXPECTED_END: JsonParseErrorType = 9;
pub const JSON_EXPECTED_JSON: JsonParseErrorType = 10;
pub const JSON_EXPECTED_MORE: JsonParseErrorType = 11;
pub const JSON_EXPECTED_OBJECT_FIRST: JsonParseErrorType = 12;
pub const JSON_EXPECTED_OBJECT_NEXT: JsonParseErrorType = 13;
pub const JSON_EXPECTED_STRING: JsonParseErrorType = 14;
pub const JSON_INVALID_TOKEN: JsonParseErrorType = 15;
pub const JSON_OUT_OF_MEMORY: JsonParseErrorType = 16;
pub const JSON_UNICODE_CODE_POINT_ZERO: JsonParseErrorType = 17;
pub const JSON_UNICODE_ESCAPE_FORMAT: JsonParseErrorType = 18;
pub const JSON_UNICODE_HIGH_ESCAPE: JsonParseErrorType = 19;
pub const JSON_UNICODE_UNTRANSLATABLE: JsonParseErrorType = 20;
pub const JSON_UNICODE_HIGH_SURROGATE: JsonParseErrorType = 21;
pub const JSON_UNICODE_LOW_SURROGATE: JsonParseErrorType = 22;
/* error should already be reported */
pub const JSON_SEM_ACTION_FAILED: JsonParseErrorType = 23;

/* Parser state private to jsonapi.c */
// typedef struct JsonParserStack JsonParserStack; (opaque)
// TODO: dedup - opaque forward declaration, defined in jsonapi.c
pub type JsonParserStack = c_void;
// typedef struct JsonIncrementalState JsonIncrementalState; (opaque)
// TODO: dedup - opaque forward declaration, defined in jsonapi.c
pub type JsonIncrementalState = c_void;

/*
 * Don't depend on the internal type header for strval; if callers need access
 * then they can include the appropriate header themselves.
 *
 * #ifdef JSONAPI_USE_PQEXPBUFFER -> PQExpBufferData, else StringInfoData.
 */
pub type jsonapi_StrValType = StringInfoData;

/*
 * JSONLEX_FREE_STRUCT/STRVAL are used to drive freeJsonLexContext.
 * JSONLEX_CTX_OWNS_TOKENS is used by setJsonLexContextOwnsTokens.
 */
pub const JSONLEX_FREE_STRUCT: c_int = 1 << 0;
pub const JSONLEX_FREE_STRVAL: c_int = 1 << 1;
pub const JSONLEX_CTX_OWNS_TOKENS: c_int = 1 << 2;

/*
 * typedef struct JsonLexContext
 */
#[repr(C)]
pub struct JsonLexContext {
    pub input: *const c_char,
    pub input_length: usize,
    pub input_encoding: c_int,
    pub token_start: *const c_char,
    pub token_terminator: *const c_char,
    pub prev_token_terminator: *const c_char,
    pub incremental: bool,
    pub token_type: JsonTokenType,
    pub lex_level: c_int,
    pub flags: bits32,
    pub line_number: c_int, /* line number, starting from 1 */
    pub line_start: *const c_char, /* where that line starts within input */
    pub pstack: *mut JsonParserStack,
    pub inc_state: *mut JsonIncrementalState,
    pub need_escapes: bool,
    pub strval: *mut jsonapi_StrValType, /* only used if need_escapes == true */
    pub errormsg: *mut jsonapi_StrValType,
}

/*
 * Function types for custom json parsing actions.
 */
pub type json_struct_action = Option<unsafe extern "C" fn(state: *mut c_void) -> JsonParseErrorType>;
pub type json_ofield_action =
    Option<unsafe extern "C" fn(state: *mut c_void, fname: *mut c_char, isnull: bool) -> JsonParseErrorType>;
pub type json_aelem_action =
    Option<unsafe extern "C" fn(state: *mut c_void, isnull: bool) -> JsonParseErrorType>;
pub type json_scalar_action = Option<
    unsafe extern "C" fn(state: *mut c_void, token: *mut c_char, tokentype: JsonTokenType) -> JsonParseErrorType,
>;

/*
 * Semantic Action structure for use in parsing json.
 */
#[repr(C)]
pub struct JsonSemAction {
    pub semstate: *mut c_void,
    pub object_start: json_struct_action,
    pub object_end: json_struct_action,
    pub array_start: json_struct_action,
    pub array_end: json_struct_action,
    pub object_field_start: json_ofield_action,
    pub object_field_end: json_ofield_action,
    pub array_element_start: json_aelem_action,
    pub array_element_end: json_aelem_action,
    pub scalar: json_scalar_action,
}

/*
 * pg_parse_json will parse the string in the lex calling the
 * action functions in sem at the appropriate points.
 */
pub unsafe fn pg_parse_json(
    _lex: *mut JsonLexContext,
    _sem: *const JsonSemAction,
) -> JsonParseErrorType {
    unimplemented!()
}

pub unsafe fn pg_parse_json_incremental(
    _lex: *mut JsonLexContext,
    _sem: *const JsonSemAction,
    _json: *const c_char,
    _len: usize,
    _is_last: bool,
) -> JsonParseErrorType {
    unimplemented!()
}

/* the null action object used for pure validation */
// extern PGDLLIMPORT const JsonSemAction nullSemAction;
// TODO: dedup - defined in jsonapi.c
unsafe extern "C" {
    pub static nullSemAction: JsonSemAction;
}

/*
 * json_count_array_elements performs a fast secondary parse to determine the
 * number of elements in passed array lex context.
 */
pub unsafe fn json_count_array_elements(
    _lex: *mut JsonLexContext,
    _elements: *mut c_int,
) -> JsonParseErrorType {
    unimplemented!()
}

/*
 * initializer for JsonLexContext.
 */
pub unsafe fn makeJsonLexContextCstringLen(
    _lex: *mut JsonLexContext,
    _json: *const c_char,
    _len: usize,
    _encoding: c_int,
    _need_escapes: bool,
) -> *mut JsonLexContext {
    unimplemented!()
}

/*
 * make a JsonLexContext suitable for incremental parsing.
 */
pub unsafe fn makeJsonLexContextIncremental(
    _lex: *mut JsonLexContext,
    _encoding: c_int,
    _need_escapes: bool,
) -> *mut JsonLexContext {
    unimplemented!()
}

/*
 * Sets whether tokens passed to semantic action callbacks are owned by the
 * context or by the callback.
 */
pub unsafe fn setJsonLexContextOwnsTokens(_lex: *mut JsonLexContext, _owned_by_context: bool) {
    unimplemented!()
}

pub unsafe fn freeJsonLexContext(_lex: *mut JsonLexContext) {
    unimplemented!()
}

/* lex one token */
pub unsafe fn json_lex(_lex: *mut JsonLexContext) -> JsonParseErrorType {
    unimplemented!()
}

/* construct an error detail string for a json error */
pub unsafe fn json_errdetail(
    _error: JsonParseErrorType,
    _lex: *mut JsonLexContext,
) -> *mut c_char {
    unimplemented!()
}

/*
 * Utility function to check if a string is a valid JSON number.
 */
pub unsafe fn IsValidJsonNumber(_str: *const c_char, _len: usize) -> bool {
    unimplemented!()
}
