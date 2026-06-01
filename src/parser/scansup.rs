//! Translation of postgres/src/backend/parser/scansup.c
//!                (+ postgres/src/include/parser/scansup.h declarations)
//!
//! Scanner support routines used by the core lexer: identifier downcasing /
//! truncation and the lexer's whitespace test.

use crate::prelude::*;
use crate::pg_config::NAMEDATALEN;
use core::ffi::{c_char, c_int};

// <ctype.h> locale-aware case routines, used only for the high-bit (non-ASCII)
// single-byte range, exactly as scansup.c does.
extern "C" {
    fn isupper(ch: c_int) -> c_int;
    fn tolower(ch: c_int) -> c_int;
}

// Backend current-encoding queries live in utils/mb/mbutils.c (not yet translated).
// TODO(pg-port): wire these to the real database-encoding GUC + wchar dispatch.
/// `pg_database_encoding_max_length()`: max bytes per char in the current DB
/// encoding. Defaulting to 4 (UTF-8) is conservative: the multi-byte path skips
/// locale downcasing of high-bit bytes, matching the safe behavior.
fn pg_database_encoding_max_length() -> c_int {
    4 // TODO(pg-port): real value from the current database encoding (mbutils.c)
}

/// `pg_mbcliplen(mbstr, len, limit)`: longest prefix of `mbstr` (<= `len` bytes)
/// that fits in `limit` bytes without splitting a multibyte char. Single-byte
/// approximation for now.
///
/// # Safety
/// `mbstr` must be valid for `len` bytes.
unsafe fn pg_mbcliplen(_mbstr: *const c_char, len: c_int, limit: c_int) -> c_int {
    // TODO(pg-port): real multibyte-boundary clip via pg_encoding_mbliplen (mbutils.c).
    Min(len, limit)
}

/*
 * downcase_truncate_identifier() --- do appropriate downcasing and truncation
 * of an unquoted identifier.  Optionally warn of truncation.
 *
 * Returns a palloc'd string containing the adjusted identifier.
 *
 * # Safety
 * `ident` must be valid for `len` bytes (it need not be NUL-terminated).
 */
pub unsafe fn downcase_truncate_identifier(
    ident: *const c_char,
    len: c_int,
    warn: bool,
) -> *mut c_char {
    downcase_identifier(ident, len, warn, true)
}

/*
 * a workhorse for downcase_truncate_identifier
 *
 * # Safety
 * `ident` must be valid for `len` bytes.
 */
pub unsafe fn downcase_identifier(
    ident: *const c_char,
    len: c_int,
    warn: bool,
    truncate: bool,
) -> *mut c_char {
    let result: *mut c_char;
    let mut i: c_int;
    let enc_is_single_byte: bool;

    result = palloc(len as Size + 1) as *mut c_char;
    enc_is_single_byte = pg_database_encoding_max_length() == 1;

    /*
     * SQL99 specifies Unicode-aware case normalization, which we don't yet have
     * the infrastructure for.  Instead we use tolower() for high-bit single-byte
     * characters and an ASCII-only downcasing for 7-bit characters.
     */
    i = 0;
    while i < len {
        let mut ch: u8 = *ident.add(i as usize) as u8;

        if ch >= b'A' && ch <= b'Z' {
            ch += b'a' - b'A';
        } else if enc_is_single_byte && IS_HIGHBIT_SET(ch) && isupper(ch as c_int) != 0 {
            ch = tolower(ch as c_int) as u8;
        }
        *result.add(i as usize) = ch as c_char;
        i += 1;
    }
    *result.add(i as usize) = 0;

    if i as usize >= NAMEDATALEN && truncate {
        truncate_identifier(result, i, warn);
    }

    result
}

/*
 * truncate_identifier() --- truncate an identifier to NAMEDATALEN-1 bytes.
 * The given string is modified in-place, if necessary.
 *
 * # Safety
 * `ident` must be a writable buffer of at least `len + 1` bytes.
 */
pub unsafe fn truncate_identifier(ident: *mut c_char, mut len: c_int, warn: bool) {
    if len as usize >= NAMEDATALEN {
        len = pg_mbcliplen(ident, len, (NAMEDATALEN - 1) as c_int);
        if warn {
            ereport!(
                NOTICE,
                errmsg!(
                    "identifier will be truncated to {} bytes",
                    len
                )
            );
        }
        *ident.add(len as usize) = 0;
    }
}

/*
 * scanner_isspace() --- return true if the flex scanner considers `ch` whitespace.
 * Use instead of the locale-dependent isspace() when matching the lexer matters.
 */
pub fn scanner_isspace(ch: c_char) -> bool {
    // This must match scan.l's list of {space} characters.
    let ch = ch as u8;
    ch == b' ' || ch == b'\t' || ch == b'\n' || ch == b'\r' || ch == 0x0b /*\v*/ || ch == 0x0c /*\f*/
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downcase_and_isspace() {
        unsafe {
            let r = downcase_identifier(c"FooBar_123".as_ptr(), 10, false, true);
            let bytes = core::slice::from_raw_parts(r as *const u8, 10);
            assert_eq!(bytes, b"foobar_123");
            assert_eq!(*r.add(10), 0); // NUL-terminated
            pfree(r as *mut core::ffi::c_void);
        }
        assert!(scanner_isspace(b' ' as c_char));
        assert!(scanner_isspace(b'\t' as c_char));
        assert!(scanner_isspace(b'\n' as c_char));
        assert!(!scanner_isspace(b'x' as c_char));
        assert!(!scanner_isspace(b'_' as c_char));
    }
}
