//! src/backend/utils/mb/wstrcmp.c
//!
//! #include "postgres_fe.h"   -> crate::prelude (c-types, c_char/c_int)
//! #include "mb/pg_wchar.h"   -> crate::mb::wchar (pg_wchar)
//!
//! Compare a C string against a pg_wchar array. Faithful 1:1 pointer-walk
//! port, preserving the C post-increment ordering and the unsigned-char cast.

use crate::prelude::*;
use crate::mb::wchar::pg_wchar;

// int pg_char_and_wchar_strcmp(const char *s1, const pg_wchar *s2)
#[no_mangle]
pub unsafe fn pg_char_and_wchar_strcmp(mut s1: *const c_char, mut s2: *const pg_wchar) -> c_int {
    // while ((pg_wchar) *s1 == *s2++)
    //     if (*s1++ == 0)
    //         return 0;
    while (*s1 as pg_wchar) == { let v = *s2; s2 = s2.add(1); v } {
        if { let v = *s1; s1 = s1.add(1); v } == 0 {
            return 0;
        }
    }
    // return *(const unsigned char *) s1 - *(const pg_wchar *) (s2 - 1);
    (*(s1 as *const c_uchar) as c_int) - (*(s2.sub(1)) as c_int)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a NUL-terminated pg_wchar buffer from a byte string.
    fn wbuf(bytes: &[u8]) -> Vec<pg_wchar> {
        let mut v: Vec<pg_wchar> = bytes.iter().map(|&b| b as pg_wchar).collect();
        v.push(0);
        v
    }

    #[test]
    fn equal_strings_zero() {
        let s1 = b"hello\0";
        let w = wbuf(b"hello");
        unsafe {
            assert_eq!(pg_char_and_wchar_strcmp(s1.as_ptr() as *const c_char, w.as_ptr()), 0);
        }
    }

    #[test]
    fn ordering() {
        let s1 = b"abc\0";
        let lo = wbuf(b"abb"); // s1 > s2 -> positive
        let hi = wbuf(b"abd"); // s1 < s2 -> negative
        unsafe {
            assert!(pg_char_and_wchar_strcmp(s1.as_ptr() as *const c_char, lo.as_ptr()) > 0);
            assert!(pg_char_and_wchar_strcmp(s1.as_ptr() as *const c_char, hi.as_ptr()) < 0);
        }
    }

    #[test]
    fn prefix_difference() {
        // "ab" vs "abc": at the NUL on s1, *s1(0) != 'c', so 0 - 'c' < 0
        let s1 = b"ab\0";
        let w = wbuf(b"abc");
        unsafe {
            assert!(pg_char_and_wchar_strcmp(s1.as_ptr() as *const c_char, w.as_ptr()) < 0);
        }
    }
}
