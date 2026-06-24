//! Translated from PostgreSQL src/include/parser/scanner.h
//
// API for the core scanner (flex machine). The flex internals stay opaque
// (core_yyscan_t). In-memory types only. core_YYSTYPE union -> enum.

use crate::common::kwlookup::ScanKeywordList;
use crate::c::Size;
use crate::utils::elog::ErrorContextCallback;

/// Extra data returned about a scanned token. C union -> Rust enum.
#[derive(Debug, Clone)]
pub enum core_YYSTYPE {
    Ival(i32),           // integer literals
    Str(String),         // identifiers and non-integer literals
    Keyword(&'static str), // canonical spelling of keywords
}

/// Token locations are byte offsets from the start of the source string.
pub type YYLTYPE = i32;

/// The YY_EXTRA data the core scanner threads around (private scanner state).
pub struct core_yy_extra_type {
    /// String being scanned (kept to compute the offset of the current token).
    pub scanbuf: *mut u8, // TODO(ptr): &mut [u8] view over the source buffer
    pub scanbuflen: Size,

    /// Keyword list to use, and the associated grammar token codes.
    pub keywordlist: *const ScanKeywordList, // TODO(ptr): borrow once lifetimes land
    pub keyword_tokens: *const u16,

    // Scanner settings (initialized from GUCs by scanner_init).
    pub backslash_quote: i32,
    pub escape_string_warning: bool,
    pub standard_conforming_strings: bool,

    // literalbuf accumulates multi-rule literal values.
    pub literalbuf: *mut u8, // palloc'd expandable buffer; TODO(ptr): String/Vec
    pub literallen: i32,
    pub literalalloc: i32,

    // Random assorted scanner state.
    pub state_before_str_stop: i32,
    pub xcdepth: i32,
    pub dolqstart: *mut u8, // current $foo$ quote start string; TODO(ptr)
    pub save_yylloc: YYLTYPE,

    pub utf16_first_part: i32,

    pub warn_on_first_escape: bool,
    pub saw_non_ascii: bool,
}

/// The yyscanner type is opaque outside scan.l.
pub type core_yyscan_t = *mut core::ffi::c_void; // opaque flex state (FFI boundary)

/// Support for the scanner_errposition callback.
pub struct ScannerCallbackState {
    pub yyscanner: core_yyscan_t,
    pub location: i32,
    pub errcallback: ErrorContextCallback,
}

// Constant data exported from parser/scan.l (built by build.rs / scan.l later).
// TODO(generated): emit ScanKeywordTokens.
pub fn ScanKeywordTokens() -> &'static [u16] {
    unimplemented!()
}

// Entry points in parser/scan.l.
pub fn scanner_init(
    _str: &str,
    _yyext: &mut core_yy_extra_type,
    _keywordlist: &ScanKeywordList,
    _keyword_tokens: &[u16],
) -> core_yyscan_t {
    unimplemented!()
}

pub fn scanner_finish(_yyscanner: core_yyscan_t) {
    unimplemented!()
}

/// Returns the token code; fills yylval/yylloc out-params.
pub fn core_yylex(
    _yylval_param: &mut core_YYSTYPE,
    _yylloc_param: &mut YYLTYPE,
    _yyscanner: core_yyscan_t,
) -> i32 {
    unimplemented!()
}

pub fn scanner_errposition(_location: i32, _yyscanner: core_yyscan_t) -> i32 {
    unimplemented!()
}

pub fn setup_scanner_errposition_callback(
    _scbstate: &mut ScannerCallbackState,
    _yyscanner: core_yyscan_t,
    _location: i32,
) {
    unimplemented!()
}

pub fn cancel_scanner_errposition_callback(_scbstate: &mut ScannerCallbackState) {
    unimplemented!()
}

/// pg_noreturn in C. TODO(panic): currently raises an error.
pub fn scanner_yyerror(_message: &str, _yyscanner: core_yyscan_t) -> ! {
    unimplemented!()
}
