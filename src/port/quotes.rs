//! Translation of postgres/src/port/quotes.c
//!   (its declaration lives in src/include/port.h:
//!    `extern char *escape_single_quotes_ascii(const char *src);` -- only the
//!    function defined in quotes.c is translated here, not all of port.h).
//!
//! string quoting and escaping functions
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does `#include "c.h"`.  From `c.h` we use the `SQL_STR_DOUBLE`
//! macro, reproduced inline below, and (in the backend) the palloc-based
//! allocator in place of the bare `malloc` the original uses.

use crate::prelude::*;

/*
 * Support macro for escaping strings.  escape_backslash should be true
 * if generating a non-standard-conforming string.
 *
 * #define SQL_STR_DOUBLE(ch, escape_backslash)	\
 *	((ch) == '\'' || ((ch) == '\\' && (escape_backslash)))
 */
#[inline]
fn SQL_STR_DOUBLE(ch: c_char, escape_backslash: bool) -> bool {
    ch == b'\'' as c_char || (ch == b'\\' as c_char && escape_backslash)
}

// TODO(pg-port): the prelude does not export libc `strlen`; provide a private
// NUL-scanning helper matching C's `strlen` over a `const char *`.
//
// # Safety
// `s` must point to a valid NUL-terminated C string.
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * Escape (by doubling) any single quotes or backslashes in given string
 *
 * Note: this is used to process postgresql.conf entries and to quote
 * string literals in pg_basebackup for writing the recovery configuration.
 * Since postgresql.conf strings are defined to treat backslashes as escapes,
 * we have to double backslashes here.
 *
 * Since this function is only used for parsing or creating configuration
 * files, we do not care about encoding considerations.
 *
 * Returns a malloced() string that it's the responsibility of the caller
 * to free.
 */
//
// # Safety
// `src` must point to a valid NUL-terminated C string.  The returned buffer is
// allocated in the current memory context (palloc) and is owned by the caller.
pub unsafe fn escape_single_quotes_ascii(src: *const c_char) -> *mut c_char {
    // int len = strlen(src), i, j;
    let len: c_int = strlen(src) as c_int;
    // char *result = malloc(len * 2 + 1);
    let result: *mut c_char = palloc((len * 2 + 1) as Size) as *mut c_char;

    if result.is_null() {
        return core::ptr::null_mut();
    }

    let mut i = 0;
    let mut j = 0;
    while i < len {
        if SQL_STR_DOUBLE(*src.add(i as usize), true) {
            *result.add(j as usize) = *src.add(i as usize);
            j += 1;
        }
        *result.add(j as usize) = *src.add(i as usize);
        j += 1;
        i += 1;
    }
    *result.add(j as usize) = b'\0' as c_char;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_quotes_and_backslashes() {
        unsafe {
            let out = escape_single_quotes_ascii(c"a'b\\c".as_ptr());
            assert!(!out.is_null());
            let n = strlen(out);
            let bytes: &[u8] =
                core::slice::from_raw_parts(out as *const u8, n);
            assert_eq!(bytes, b"a''b\\\\c");
            pfree(out as *mut core::ffi::c_void);
        }
    }
}
