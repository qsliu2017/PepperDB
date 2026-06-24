//! Translated from PostgreSQL src/include/tsearch/ts_locale.h
//
// Locale compatibility layer for tsearch. Server encoding is UTF-8 only.
// StringInfo (lib/stringinfo.h) is tombstoned -> `String`.

use crate::mb::pg_wchar::pg_mblen_cstr;
use crate::utils::elog::ErrorContextCallback;

/// Working state for tsearch_readline (a local var in the caller). C: the FILE*
/// becomes a std reader; the StringInfo input line is a UTF-8 `String`.
pub struct TsearchReadlineState {
    pub fp: Option<std::fs::File>,
    pub filename: String,
    pub lineno: i32,
    pub buf: String,            // current input line, in UTF-8 (was StringInfoData)
    pub curline: Option<String>, // current input line, in DB's encoding
    pub cb: ErrorContextCallback,
}

/// C: `t_iseq(x, c)` - byte at `x` equals plain-ASCII char `c`.
pub fn t_iseq(x: &[u8], c: u8) -> bool {
    !x.is_empty() && x[0] == c
}

/// Copy a multibyte character of known byte length; return byte length.
pub fn ts_copychar_with_len(dest: &mut [u8], src: &[u8], length: usize) -> usize {
    dest[..length].copy_from_slice(&src[..length]);
    length
}

/// Copy a multibyte char from a NUL-terminated string; return byte length.
pub fn ts_copychar_cstr(dest: &mut [u8], src: *const u8) -> usize {
    let len = unsafe { pg_mblen_cstr(src) } as usize;
    let s = unsafe { core::slice::from_raw_parts(src, len) };
    ts_copychar_with_len(dest, s, len)
}

// C GENERATE_T_ISCLASS_DECL(alnum) / (alpha): the *_with_len / *_cstr /
// *_unbounded variants plus the deprecated bare form. Return int (PG's tri-ish
// bool); kept as bool here.
pub fn t_isalnum_with_len(ptr: &str, len: i32) -> bool {
    unimplemented!()
}
pub fn t_isalnum_cstr(ptr: *const u8) -> bool {
    unimplemented!()
}
pub fn t_isalnum_unbounded(ptr: *const u8) -> bool {
    unimplemented!()
}
#[deprecated(note = "PG-deprecated bare t_isalnum")]
pub fn t_isalnum(ptr: *const u8) -> bool {
    unimplemented!()
}

pub fn t_isalpha_with_len(ptr: &str, len: i32) -> bool {
    unimplemented!()
}
pub fn t_isalpha_cstr(ptr: *const u8) -> bool {
    unimplemented!()
}
pub fn t_isalpha_unbounded(ptr: *const u8) -> bool {
    unimplemented!()
}
#[deprecated(note = "PG-deprecated bare t_isalpha")]
pub fn t_isalpha(ptr: *const u8) -> bool {
    unimplemented!()
}

pub fn tsearch_readline_begin(stp: &mut TsearchReadlineState, filename: &str) -> bool {
    unimplemented!()
}

/// Read one line; None at EOF. (C returns NULL at end.)
pub fn tsearch_readline(stp: &mut TsearchReadlineState) -> Option<String> {
    unimplemented!()
}

pub fn tsearch_readline_end(stp: &mut TsearchReadlineState) {
    unimplemented!()
}
