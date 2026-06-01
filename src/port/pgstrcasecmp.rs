//! Translation of postgres/src/port/pgstrcasecmp.c
//!   (its declarations live in src/include/port.h; only the functions defined
//!    in pgstrcasecmp.c are translated here, not all of port.h).
//!
//! Portable SQL-like case-independent comparisons and conversions.
//!
//! SQL99 specifies Unicode-aware case normalization, which we don't yet
//! have the infrastructure for.  Instead we use tolower() to provide a
//! locale-aware translation.  However, there are some locales where this
//! is not right either (eg, Turkish may do strange things with 'i' and
//! 'I').  Our current compromise is to use tolower() for characters with
//! the high bit set, and use an ASCII-only downcasing for 7-bit
//! characters.
//!
//! NB: this code should match downcase_truncate_identifier() in scansup.c.
//!
//! We also provide strict ASCII-only case conversion functions, which can
//! be used to implement C/POSIX case folding semantics no matter what the
//! C library thinks the locale is.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! The .c does `#include "c.h"` then `#include <ctype.h>`.  `c.h` gives us
//! `IS_HIGHBIT_SET` (via the prelude / crate::c).  The high-bit branch calls
//! libc's locale-aware `<ctype.h>` routines (`isupper`/`islower`/`toupper`/
//! `tolower`), which we bind directly via `extern "C"` so the behavior on the
//! non-ASCII range matches the C 1:1.

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_uchar};

// ----------------------------------------------------------------
//   <ctype.h> bindings
// ----------------------------------------------------------------
//
// The C source only reaches these for the high-bit-set (non-ASCII) range; the
// 7-bit letters are handled inline.  libc's isupper/islower/toupper/tolower
// take and return `int`, operating on a value representable as `unsigned char`
// (or EOF), exactly as the C passes `(unsigned char)` values into them.
extern "C" {
    fn isupper(ch: c_int) -> c_int;
    fn islower(ch: c_int) -> c_int;
    fn toupper(ch: c_int) -> c_int;
    fn tolower(ch: c_int) -> c_int;
}

/*
 * Case-independent comparison of two null-terminated strings.
 */
//
// # Safety
// `s1` and `s2` must point to valid NUL-terminated C strings.
pub unsafe fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int {
    let mut s1 = s1;
    let mut s2 = s2;
    loop {
        // unsigned char ch1 = (unsigned char) *s1++;
        let mut ch1: c_uchar = *s1 as c_uchar;
        s1 = s1.add(1);
        // unsigned char ch2 = (unsigned char) *s2++;
        let mut ch2: c_uchar = *s2 as c_uchar;
        s2 = s2.add(1);

        if ch1 != ch2 {
            if ch1 >= b'A' && ch1 <= b'Z' {
                ch1 += b'a' - b'A';
            } else if IS_HIGHBIT_SET(ch1) && isupper(ch1 as c_int) != 0 {
                ch1 = tolower(ch1 as c_int) as c_uchar;
            }

            if ch2 >= b'A' && ch2 <= b'Z' {
                ch2 += b'a' - b'A';
            } else if IS_HIGHBIT_SET(ch2) && isupper(ch2 as c_int) != 0 {
                ch2 = tolower(ch2 as c_int) as c_uchar;
            }

            if ch1 != ch2 {
                return ch1 as c_int - ch2 as c_int;
            }
        }
        if ch1 == 0 {
            break;
        }
    }
    0
}

/*
 * Case-independent comparison of two not-necessarily-null-terminated strings.
 * At most n bytes will be examined from each string.
 */
//
// # Safety
// `s1` and `s2` must each point to at least `n` readable bytes (or fewer if a
// NUL is encountered first).
pub unsafe fn pg_strncasecmp(s1: *const c_char, s2: *const c_char, n: Size) -> c_int {
    let mut s1 = s1;
    let mut s2 = s2;
    let mut n = n;
    // while (n-- > 0)
    while n > 0 {
        n -= 1;
        // unsigned char ch1 = (unsigned char) *s1++;
        let mut ch1: c_uchar = *s1 as c_uchar;
        s1 = s1.add(1);
        // unsigned char ch2 = (unsigned char) *s2++;
        let mut ch2: c_uchar = *s2 as c_uchar;
        s2 = s2.add(1);

        if ch1 != ch2 {
            if ch1 >= b'A' && ch1 <= b'Z' {
                ch1 += b'a' - b'A';
            } else if IS_HIGHBIT_SET(ch1) && isupper(ch1 as c_int) != 0 {
                ch1 = tolower(ch1 as c_int) as c_uchar;
            }

            if ch2 >= b'A' && ch2 <= b'Z' {
                ch2 += b'a' - b'A';
            } else if IS_HIGHBIT_SET(ch2) && isupper(ch2 as c_int) != 0 {
                ch2 = tolower(ch2 as c_int) as c_uchar;
            }

            if ch1 != ch2 {
                return ch1 as c_int - ch2 as c_int;
            }
        }
        if ch1 == 0 {
            break;
        }
    }
    0
}

/*
 * Fold a character to upper case.
 *
 * Unlike some versions of toupper(), this is safe to apply to characters
 * that aren't lower case letters.  Note however that the whole thing is
 * a bit bogus for multibyte character sets.
 */
pub fn pg_toupper(mut ch: c_uchar) -> c_uchar {
    if ch >= b'a' && ch <= b'z' {
        // C: `ch += 'A' - 'a'` (= -32) on an unsigned char; fold down by 32.
        ch -= b'a' - b'A';
    } else if IS_HIGHBIT_SET(ch) && unsafe { islower(ch as c_int) } != 0 {
        ch = unsafe { toupper(ch as c_int) } as c_uchar;
    }
    ch
}

/*
 * Fold a character to lower case.
 *
 * Unlike some versions of tolower(), this is safe to apply to characters
 * that aren't upper case letters.  Note however that the whole thing is
 * a bit bogus for multibyte character sets.
 */
pub fn pg_tolower(mut ch: c_uchar) -> c_uchar {
    if ch >= b'A' && ch <= b'Z' {
        ch += b'a' - b'A';
    } else if IS_HIGHBIT_SET(ch) && unsafe { isupper(ch as c_int) } != 0 {
        ch = unsafe { tolower(ch as c_int) } as c_uchar;
    }
    ch
}

/*
 * Fold a character to upper case, following C/POSIX locale rules.
 */
pub fn pg_ascii_toupper(mut ch: c_uchar) -> c_uchar {
    if ch >= b'a' && ch <= b'z' {
        // C: `ch += 'A' - 'a'` (= -32) on an unsigned char; fold down by 32.
        ch -= b'a' - b'A';
    }
    ch
}

/*
 * Fold a character to lower case, following C/POSIX locale rules.
 */
pub fn pg_ascii_tolower(mut ch: c_uchar) -> c_uchar {
    if ch >= b'A' && ch <= b'Z' {
        ch += b'a' - b'A';
    }
    ch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_fold_and_compare() {
        assert_eq!(pg_toupper(b'a'), b'A');
        assert_eq!(pg_toupper(b'A'), b'A');
        assert_eq!(pg_toupper(b'1'), b'1');
        assert_eq!(pg_tolower(b'Z'), b'z');
        assert_eq!(pg_ascii_toupper(b'z'), b'Z');
        assert_eq!(pg_ascii_tolower(b'Q'), b'q');
        unsafe {
            assert_eq!(pg_strcasecmp(c"Hello".as_ptr(), c"HELLO".as_ptr()), 0);
            assert!(pg_strcasecmp(c"abc".as_ptr(), c"abd".as_ptr()) < 0);
            assert!(pg_strcasecmp(c"abd".as_ptr(), c"abc".as_ptr()) > 0);
            assert_eq!(pg_strncasecmp(c"FooBar".as_ptr(), c"foobaZ".as_ptr(), 5), 0);
            assert!(pg_strncasecmp(c"FooBar".as_ptr(), c"foobaZ".as_ptr(), 6) != 0);
        }
    }
}
