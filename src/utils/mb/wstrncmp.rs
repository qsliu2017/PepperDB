//! src/backend/utils/mb/wstrncmp.c
//!
//! #include "postgres_fe.h"   -> crate::prelude (c-types, c_char/c_int, Size via size_t)
//! #include "mb/pg_wchar.h"   -> crate::mb::wchar (pg_wchar)
//!
//! Bounded comparisons over pg_wchar arrays, plus pg_wchar_strlen. Faithful
//! 1:1 port of the FreeBSD-derived do/while loops, preserving the post-increment
//! semantics (returns reference s2-1 after the increment) and the
//! unsigned-char cast in the char/wchar variant.

use crate::prelude::*;
use crate::mb::wchar::pg_wchar;

// size_t maps to usize here (matches C size_t for the platforms we target).
#[allow(non_camel_case_types)]
type size_t = usize;

// int pg_wchar_strncmp(const pg_wchar *s1, const pg_wchar *s2, size_t n)
#[no_mangle]
pub unsafe fn pg_wchar_strncmp(mut s1: *const pg_wchar, mut s2: *const pg_wchar, mut n: size_t) -> c_int {
    if n == 0 {
        return 0;
    }
    loop {
        // if (*s1 != *s2++) return (*s1 - *(s2 - 1));
        let r = { let v = *s2; s2 = s2.add(1); v };
        if *s1 != r {
            return (*s1 as c_int) - (*(s2.sub(1)) as c_int);
        }
        // if (*s1++ == 0) break;
        if { let v = *s1; s1 = s1.add(1); v } == 0 {
            break;
        }
        // } while (--n != 0);
        n -= 1;
        if n == 0 {
            break;
        }
    }
    0
}

// int pg_char_and_wchar_strncmp(const char *s1, const pg_wchar *s2, size_t n)
#[no_mangle]
pub unsafe fn pg_char_and_wchar_strncmp(mut s1: *const c_char, mut s2: *const pg_wchar, mut n: size_t) -> c_int {
    if n == 0 {
        return 0;
    }
    loop {
        // if ((pg_wchar)((unsigned char) *s1) != *s2++)
        //     return ((pg_wchar)((unsigned char) *s1) - *(s2 - 1));
        let lhs = (*(s1 as *const c_uchar)) as pg_wchar;
        let r = { let v = *s2; s2 = s2.add(1); v };
        if lhs != r {
            return (lhs as c_int) - (*(s2.sub(1)) as c_int);
        }
        // if (*s1++ == 0) break;
        if { let v = *s1; s1 = s1.add(1); v } == 0 {
            break;
        }
        // } while (--n != 0);
        n -= 1;
        if n == 0 {
            break;
        }
    }
    0
}

// size_t pg_wchar_strlen(const pg_wchar *str)
#[no_mangle]
pub unsafe fn pg_wchar_strlen(str: *const pg_wchar) -> size_t {
    // for (s = str; *s; ++s) ; return (s - str);
    let mut s = str;
    while *s != 0 {
        s = s.add(1);
    }
    s.offset_from(str) as size_t
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wbuf(bytes: &[u8]) -> Vec<pg_wchar> {
        let mut v: Vec<pg_wchar> = bytes.iter().map(|&b| b as pg_wchar).collect();
        v.push(0);
        v
    }

    #[test]
    fn wchar_strncmp_n_zero() {
        let a = wbuf(b"abc");
        let b = wbuf(b"xyz");
        unsafe {
            assert_eq!(pg_wchar_strncmp(a.as_ptr(), b.as_ptr(), 0), 0);
        }
    }

    #[test]
    fn wchar_strncmp_equal_and_order() {
        let a = wbuf(b"hello");
        let b = wbuf(b"hello");
        let c = wbuf(b"hellp"); // differs at last char
        unsafe {
            assert_eq!(pg_wchar_strncmp(a.as_ptr(), b.as_ptr(), 5), 0);
            assert!(pg_wchar_strncmp(a.as_ptr(), c.as_ptr(), 5) < 0);
            assert!(pg_wchar_strncmp(c.as_ptr(), a.as_ptr(), 5) > 0);
            // bounded: only first char compared, both 'h'
            assert_eq!(pg_wchar_strncmp(a.as_ptr(), c.as_ptr(), 1), 0);
        }
    }

    #[test]
    fn char_and_wchar_strncmp_n_zero() {
        let s1 = b"abc\0";
        let w = wbuf(b"xyz");
        unsafe {
            assert_eq!(pg_char_and_wchar_strncmp(s1.as_ptr() as *const c_char, w.as_ptr(), 0), 0);
        }
    }

    #[test]
    fn char_and_wchar_strncmp_equal_and_order() {
        let s1 = b"hello\0";
        let eq = wbuf(b"hello");
        let gt = wbuf(b"hellp");
        unsafe {
            assert_eq!(pg_char_and_wchar_strncmp(s1.as_ptr() as *const c_char, eq.as_ptr(), 5), 0);
            assert!(pg_char_and_wchar_strncmp(s1.as_ptr() as *const c_char, gt.as_ptr(), 5) < 0);
            assert!(pg_char_and_wchar_strncmp(s1.as_ptr() as *const c_char, gt.as_ptr(), 4) == 0);
        }
    }

    #[test]
    fn wchar_strlen_counts() {
        let empty = wbuf(b"");
        let five = wbuf(b"hello");
        unsafe {
            assert_eq!(pg_wchar_strlen(empty.as_ptr()), 0);
            assert_eq!(pg_wchar_strlen(five.as_ptr()), 5);
        }
    }
}
