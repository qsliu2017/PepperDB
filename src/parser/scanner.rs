//! scanner.h - API for the core scanner (flex machine).
//!
//! The core scanner is also used by PL/pgSQL, so this provides a public API
//! for it. The rest of the backend is only expected to use the higher-level
//! API provided by parser.h.

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int32, uint16, Size};
use crate::common::kwlookup::ScanKeywordList;

// ErrorContextCallback is defined in utils/elog.h, which has not been ported
// yet. Provide a minimal stub so ScannerCallbackState can embed it.
// TODO: dedup when elog.h lands.
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(arg: *mut c_void)>,
    pub arg: *mut c_void,
}

/// The scanner returns extra data about scanned tokens in this union type.
/// Note that this is a subset of the fields used in YYSTYPE of the bison
/// parsers built atop the scanner.
#[repr(C)]
pub union core_YYSTYPE {
    /// for integer literals
    pub ival: c_int,
    /// for identifiers and non-integer literals
    pub str: *mut c_char,
    /// canonical spelling of keywords
    pub keyword: *const c_char,
}

/// We track token locations in terms of byte offsets from the start of the
/// source string. It's sufficient to make YYLTYPE an int.
pub type YYLTYPE = c_int;

/*
 * Another important component of the scanner's API is the token code numbers.
 * However, those are not defined in this file, because bison insists on
 * defining them for itself. The token codes used by the core scanner are the
 * ASCII characters plus IDENT/UIDENT/FCONST/etc. (IDENT = 258 and so on).
 */

/// The YY_EXTRA data that a flex scanner allows us to pass around.
/// Private state needed by the core scanner goes here. Note that the actual
/// yy_extra struct may be larger and have this as its first component, thus
/// allowing the calling parser to keep some fields of its own in YY_EXTRA.
#[repr(C)]
pub struct core_yy_extra_type {
    /// The string the scanner is physically scanning. We keep this mainly so
    /// that we can cheaply compute the offset of the current token (yytext).
    pub scanbuf: *mut c_char,
    pub scanbuflen: Size,

    /// The keyword list to use,
    pub keywordlist: *const ScanKeywordList,
    /// and the associated grammar token codes.
    pub keyword_tokens: *const uint16,

    /// Scanner settings to use. These are initialized from the corresponding
    /// GUC variables by scanner_init(). Callers can modify them after
    /// scanner_init() if they don't want the scanner's behavior to follow the
    /// prevailing GUC settings.
    pub backslash_quote: c_int,
    pub escape_string_warning: bool,
    pub standard_conforming_strings: bool,

    /// literalbuf is used to accumulate literal values when multiple rules are
    /// needed to parse a single literal. Call startlit() to reset buffer to
    /// empty, addlit() to add text. NOTE: the string in literalbuf is NOT
    /// necessarily null-terminated, but there always IS room to add a trailing
    /// null at offset literallen. We store a null only when we need it.
    ///
    /// palloc'd expandable buffer
    pub literalbuf: *mut c_char,
    /// actual current string length
    pub literallen: c_int,
    /// current allocated buffer size
    pub literalalloc: c_int,

    // Random assorted scanner state.
    /// start cond. before end quote
    pub state_before_str_stop: c_int,
    /// depth of nesting in slash-star comments
    pub xcdepth: c_int,
    /// current $foo$ quote start string
    pub dolqstart: *mut c_char,
    /// one-element stack for PUSH_YYLLOC()
    pub save_yylloc: YYLTYPE,

    /// first part of UTF16 surrogate pair for Unicode escapes
    pub utf16_first_part: int32,

    // state variables for literal-lexing warnings
    pub warn_on_first_escape: bool,
    pub saw_non_ascii: bool,
}

/// The type of yyscanner is opaque outside scan.l.
pub type core_yyscan_t = *mut c_void;

/// Support for scanner_errposition_callback function
#[repr(C)]
pub struct ScannerCallbackState {
    pub yyscanner: core_yyscan_t,
    pub location: c_int,
    pub errcallback: ErrorContextCallback,
}

// Constant data exported from parser/scan.l
//
// extern PGDLLIMPORT const uint16 ScanKeywordTokens[];
// (Flexible array; declared in the corresponding .rs translation of scan.l.)

// Entry points in parser/scan.l

pub unsafe fn scanner_init(
    str: *const c_char,
    yyext: *mut core_yy_extra_type,
    keywordlist: *const ScanKeywordList,
    keyword_tokens: *const uint16,
) -> core_yyscan_t {
    unimplemented!()
}

pub unsafe fn scanner_finish(yyscanner: core_yyscan_t) {
    unimplemented!()
}

pub unsafe fn core_yylex(
    yylval_param: *mut core_YYSTYPE,
    yylloc_param: *mut YYLTYPE,
    yyscanner: core_yyscan_t,
) -> c_int {
    unimplemented!()
}

pub unsafe fn scanner_errposition(location: c_int, yyscanner: core_yyscan_t) -> c_int {
    unimplemented!()
}

pub unsafe fn setup_scanner_errposition_callback(
    scbstate: *mut ScannerCallbackState,
    yyscanner: core_yyscan_t,
    location: c_int,
) {
    unimplemented!()
}

pub unsafe fn cancel_scanner_errposition_callback(scbstate: *mut ScannerCallbackState) {
    unimplemented!()
}

/// pg_noreturn: this function never returns.
pub unsafe fn scanner_yyerror(message: *const c_char, yyscanner: core_yyscan_t) -> ! {
    unimplemented!()
}
