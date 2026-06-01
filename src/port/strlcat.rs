//! Translation of postgres/src/port/strlcat.c
//!
//! Taken from OpenBSD; used on platforms that don't provide strlcat().
//! (OpenBSD copyright: Todd C. Miller, 1998.)

use crate::prelude::*;
use core::ffi::c_char;

/// Append `src` to string `dst` of size `siz` (unlike strncat, `siz` is the full
/// size of `dst`, not the space left). At most `siz-1` characters will be copied.
/// Always NUL terminates (unless `siz <= strlen(dst)`). Returns
/// `strlen(src) + MIN(siz, strlen(initial dst))`. A return value >= `siz` means
/// truncation occurred.
///
/// # Safety
/// `dst` must be a NUL-terminated C string valid for `siz` bytes; `src` a
/// NUL-terminated C string.
pub unsafe fn strlcat(dst: *mut c_char, src: *const c_char, siz: Size) -> Size {
    let mut d = dst;
    let mut s = src;
    let mut n = siz;
    let dlen: Size;

    /* Find the end of dst and adjust bytes left but don't go past end */
    // C: while (n-- != 0 && *d != '\0') d++;
    while {
        let old = n;
        n = n.wrapping_sub(1); // size_t post-decrement; wraps when n == 0 (unused then)
        old != 0
    } && *d != 0
    {
        d = d.add(1);
    }
    dlen = d.offset_from(dst) as Size;
    n = siz - dlen;

    if n == 0 {
        return dlen + strlen(s);
    }
    while *s != 0 {
        if n != 1 {
            *d = *s;
            d = d.add(1);
            n -= 1;
        }
        s = s.add(1);
    }
    *d = 0;

    dlen + (s.offset_from(src) as Size) /* count does not include NUL */
}

/// Minimal `strlen` over a C string (mirrors libc strlen).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
#[inline]
unsafe fn strlen(s: *const c_char) -> Size {
    let mut n: Size = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_and_truncates() {
        unsafe {
            let mut buf = [0i8; 8];
            strlcpy_into(&mut buf, b"ab\0");
            let r = strlcat(buf.as_mut_ptr(), c"cdef".as_ptr(), buf.len());
            assert_eq!(r, 6); // strlen("ab")+strlen("cdef")
            assert_eq!(buf[0], b'a' as i8);
            assert_eq!(buf[5], b'f' as i8);
            assert_eq!(buf[6], 0);

            // truncation: dst already "ab", append long src
            let mut buf2 = [0i8; 5];
            strlcpy_into(&mut buf2, b"ab\0");
            let r = strlcat(buf2.as_mut_ptr(), c"xyz123".as_ptr(), buf2.len());
            assert_eq!(r, 8); // 2 + 6, >= siz(5) => truncated
            assert_eq!(buf2[4], 0); // NUL terminated within siz
        }
    }

    // helper: seed a fixed buffer with bytes (incl. trailing NUL byte already present)
    fn strlcpy_into(buf: &mut [i8], bytes: &[u8]) {
        for (i, &b) in bytes.iter().enumerate() {
            buf[i] = b as i8;
        }
    }
}
