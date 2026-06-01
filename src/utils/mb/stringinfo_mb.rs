//! Translation of postgres/src/backend/utils/mb/stringinfo_mb.c
//!                + postgres/src/include/mb/stringinfo_mb.h
//!
//! Multibyte encoding-aware additional StringInfo facilities. This is separate
//! from common/stringinfo.c so that frontend users of that file need not pull in
//! unnecessary multibyte-encoding support code.

use crate::appendStringInfo;
use crate::lib::stringinfo::{
    appendBinaryStringInfoNT, appendStringInfoChar, StringInfo,
};
use crate::mb::mbutils::pg_mbcliplen;
use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
}

/// `appendStringInfoStringQuoted`
///
/// Append up to `maxlen` bytes from `s` to `str`, or the whole input string if
/// `maxlen < 0`, adding single quotes around it and doubling all single quotes.
/// Add an ellipsis if the copy is incomplete.
///
/// # Safety
/// `str` must be a writable StringInfo; `s` must be a valid NUL-terminated C
/// string that is valid in the current database encoding.
pub unsafe fn appendStringInfoStringQuoted(str: StringInfo, s: *const c_char, maxlen: c_int) {
    let copy: *mut c_char;
    let mut chunk_search_start: *const c_char;
    let mut chunk_copy_start: *const c_char;
    let ellipsis: bool;

    Assert!(!str.is_null());

    let slen = strlen(s) as c_int;
    if maxlen >= 0 && maxlen < slen {
        let finallen = pg_mbcliplen(s, slen, maxlen);

        copy = pnstrdup(s, finallen as Size);
        chunk_search_start = copy;
        chunk_copy_start = copy;

        ellipsis = true;
    } else {
        copy = null_mut();
        chunk_search_start = s;
        chunk_copy_start = s;

        ellipsis = false;
    }

    appendStringInfoChar(str, b'\'' as c_char);

    loop {
        let chunk_end = strchr(chunk_search_start, b'\'' as c_int);
        if chunk_end.is_null() {
            break;
        }

        // copy including the found delimiting '
        appendBinaryStringInfoNT(
            str,
            chunk_copy_start as *const c_void,
            (chunk_end as isize - chunk_copy_start as isize + 1) as c_int,
        );

        // in order to double it, include this ' into the next chunk as well
        chunk_copy_start = chunk_end;
        chunk_search_start = chunk_end.add(1);
    }

    // copy the last chunk and terminate
    if ellipsis {
        appendStringInfo!(str, "{}...'", CStr(chunk_copy_start));
    } else {
        appendStringInfo!(str, "{}'", CStr(chunk_copy_start));
    }

    if !copy.is_null() {
        pfree(copy as *mut c_void);
    }
}

/// Helper wrapper rendering a borrowed NUL-terminated C string via `{}` so the
/// `appendStringInfo!` macro (which uses Rust `format!`) can interpolate the
/// C-string chunk the way the original `%s` placeholder did. Bytes are emitted
/// 1:1 (lossy only for display of invalid UTF-8, which does not affect the
/// appended bytes since the macro formats from this Display impl).
struct CStr(*const c_char);

impl core::fmt::Display for CStr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        unsafe {
            let len = strlen(self.0);
            let bytes = core::slice::from_raw_parts(self.0 as *const u8, len);
            // Preserve raw bytes exactly; write each as a char in 0..=255 range.
            for &b in bytes {
                f.write_str(core::str::from_utf8(core::slice::from_ref(&b)).unwrap_or("\u{FFFD}"))?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::stringinfo::{initStringInfo, StringInfoData};

    unsafe fn as_str(s: StringInfo) -> String {
        let bytes = core::slice::from_raw_parts((*s).data as *const u8, (*s).len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    }

    fn fresh() -> StringInfoData {
        StringInfoData {
            data: null_mut(),
            len: 0,
            maxlen: 0,
            cursor: 0,
        }
    }

    #[test]
    fn quotes_short_ascii() {
        unsafe {
            let mut sd = fresh();
            initStringInfo(&mut sd);
            appendStringInfoStringQuoted(&mut sd, c"hello".as_ptr(), -1);
            assert_eq!(as_str(&mut sd), "'hello'");
            pfree(sd.data as *mut c_void);
        }
    }

    #[test]
    fn doubles_embedded_quote() {
        unsafe {
            let mut sd = fresh();
            initStringInfo(&mut sd);
            // it's -> 'it''s'
            appendStringInfoStringQuoted(&mut sd, c"it's".as_ptr(), -1);
            assert_eq!(as_str(&mut sd), "'it''s'");
            pfree(sd.data as *mut c_void);
        }
    }

    #[test]
    fn truncation_appends_ellipsis() {
        unsafe {
            let mut sd = fresh();
            initStringInfo(&mut sd);
            // maxlen 3 over a 7-byte ASCII string -> clip to 3 chars + ellipsis
            appendStringInfoStringQuoted(&mut sd, c"abcdefg".as_ptr(), 3);
            assert_eq!(as_str(&mut sd), "'abc...'");
            pfree(sd.data as *mut c_void);
        }
    }
}
