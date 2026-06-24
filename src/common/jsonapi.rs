//! Translated from PostgreSQL src/include/common/jsonapi.h
// JSON lexer/parser API. Lexer/parser internals stay opaque; bodies stubbed.

use bitflags::bitflags;

/// JSON token kinds produced by the lexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonTokenType {
    Invalid,
    String,
    Number,
    ObjectStart,
    ObjectEnd,
    ArrayStart,
    ArrayEnd,
    Comma,
    Colon,
    True,
    False,
    Null,
    End,
}

/// Result codes from the JSON parser/lexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonParseErrorType {
    Success,
    Incomplete,
    InvalidLexerType,
    NestingTooDeep,
    EscapingInvalid,
    EscapingRequired,
    ExpectedArrayFirst,
    ExpectedArrayNext,
    ExpectedColon,
    ExpectedEnd,
    ExpectedJson,
    ExpectedMore,
    ExpectedObjectFirst,
    ExpectedObjectNext,
    ExpectedString,
    InvalidToken,
    OutOfMemory,
    UnicodeCodePointZero,
    UnicodeEscapeFormat,
    UnicodeHighEscape,
    UnicodeUntranslatable,
    UnicodeHighSurrogate,
    UnicodeLowSurrogate,
    /// Error should already be reported by the action function.
    SemActionFailed,
}

/// Parser state private to jsonapi.c (opaque).
pub struct JsonParserStack {
    _private: (),
}

/// Incremental parser state private to jsonapi.c (opaque).
pub struct JsonIncrementalState {
    _private: (),
}

bitflags! {
    /// Flags driving freeJsonLexContext / setJsonLexContextOwnsTokens.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct JsonLexFlags: u32 {
        const FREE_STRUCT     = 1 << 0;
        const FREE_STRVAL     = 1 << 1;
        const CTX_OWNS_TOKENS = 1 << 2;
    }
}

/// Lexer context. All fields are read-only to callers. `strval`/`errormsg` are
/// `StringInfoData` (the C `jsonapi_StrValType` default, not the PQExpBuffer one).
pub struct JsonLexContext {
    pub input: String,
    pub input_length: usize,
    pub input_encoding: i32,
    pub token_start: usize,
    pub token_terminator: usize,
    pub prev_token_terminator: usize,
    pub incremental: bool,
    pub token_type: JsonTokenType,
    pub lex_level: i32,
    pub flags: JsonLexFlags,
    /// Line number, starting from 1.
    pub line_number: i32,
    /// Where that line starts within input.
    pub line_start: usize,
    pub pstack: Option<Box<JsonParserStack>>,
    pub inc_state: Option<Box<JsonIncrementalState>>,
    pub need_escapes: bool,
    /// Only used if need_escapes == true.
    pub strval: Option<String>,
    pub errormsg: Option<String>,
}

// Custom action callback types. `state` is the opaque semstate threaded through.
pub type JsonStructAction = fn(state: *mut core::ffi::c_void) -> JsonParseErrorType;
pub type JsonOfieldAction =
    fn(state: *mut core::ffi::c_void, fname: Option<String>, isnull: bool) -> JsonParseErrorType;
pub type JsonAelemAction =
    fn(state: *mut core::ffi::c_void, isnull: bool) -> JsonParseErrorType;
pub type JsonScalarAction = fn(
    state: *mut core::ffi::c_void,
    token: Option<String>,
    tokentype: JsonTokenType,
) -> JsonParseErrorType;

/// Semantic action table for parsing JSON. Any action may be None (no-op).
/// An all-None table amounts to a pure validate-only parse.
pub struct JsonSemAction {
    pub semstate: *mut core::ffi::c_void,
    pub object_start: Option<JsonStructAction>,
    pub object_end: Option<JsonStructAction>,
    pub array_start: Option<JsonStructAction>,
    pub array_end: Option<JsonStructAction>,
    pub object_field_start: Option<JsonOfieldAction>,
    pub object_field_end: Option<JsonOfieldAction>,
    pub array_element_start: Option<JsonAelemAction>,
    pub array_element_end: Option<JsonAelemAction>,
    pub scalar: Option<JsonScalarAction>,
}

/// The null action object used for pure validation.
pub const NULL_SEM_ACTION: JsonSemAction = JsonSemAction {
    semstate: core::ptr::null_mut(),
    object_start: None,
    object_end: None,
    array_start: None,
    array_end: None,
    object_field_start: None,
    object_field_end: None,
    array_element_start: None,
    array_element_end: None,
    scalar: None,
};

/// Parse the string in `lex`, invoking `sem`'s actions at the right points.
pub fn pg_parse_json(lex: &mut JsonLexContext, sem: &JsonSemAction) -> JsonParseErrorType {
    let _ = (lex, sem);
    unimplemented!()
}

/// Incrementally parse a `json` chunk; `is_last` marks the final chunk.
pub fn pg_parse_json_incremental(
    lex: &mut JsonLexContext,
    sem: &JsonSemAction,
    json: &str,
    is_last: bool,
) -> JsonParseErrorType {
    let _ = (lex, sem, json, is_last);
    unimplemented!()
}

/// Fast secondary parse counting array elements; call from an array_start action.
/// Returns the count on success.
pub fn json_count_array_elements(lex: &mut JsonLexContext) -> Result<i32, JsonParseErrorType> {
    let _ = lex;
    unimplemented!()
}

/// Initialize/allocate a lexer over a cstring-with-length JSON input.
pub fn make_json_lex_context_cstring_len(
    lex: Option<JsonLexContext>,
    json: &str,
    encoding: i32,
    need_escapes: bool,
) -> JsonLexContext {
    let _ = (lex, json, encoding, need_escapes);
    unimplemented!()
}

/// Make a lexer suitable for incremental parsing.
pub fn make_json_lex_context_incremental(
    lex: Option<JsonLexContext>,
    encoding: i32,
    need_escapes: bool,
) -> JsonLexContext {
    let _ = (lex, encoding, need_escapes);
    unimplemented!()
}

/// Set whether tokens passed to actions are owned by the context.
pub fn set_json_lex_context_owns_tokens(lex: &mut JsonLexContext, owned_by_context: bool) {
    let _ = (lex, owned_by_context);
    unimplemented!()
}

/// Free a lexer context.
pub fn free_json_lex_context(lex: &mut JsonLexContext) {
    let _ = lex;
    unimplemented!()
}

/// Lex one token.
pub fn json_lex(lex: &mut JsonLexContext) -> JsonParseErrorType {
    let _ = lex;
    unimplemented!()
}

/// Construct an error-detail string for a JSON error.
pub fn json_errdetail(error: JsonParseErrorType, lex: &mut JsonLexContext) -> String {
    let _ = (error, lex);
    unimplemented!()
}

/// True if `str` is a valid JSON number (need not be nul-terminated).
pub fn is_valid_json_number(s: &str) -> bool {
    let _ = s;
    unimplemented!()
}
