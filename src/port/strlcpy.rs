//! Translation of postgres/src/port/strlcpy.c
//!
//! strncpy done right. Taken from OpenBSD; used on platforms that don't provide
//! strlcpy(). (OpenBSD copyright: Todd C. Miller, 1998.)

use crate::prelude::*;
use core::ffi::c_char;

/// Copy `src` to string `dst` of size `siz`. At most `siz-1` characters will be
/// copied. Always NUL terminates (unless `siz == 0`). Returns `strlen(src)`; if
/// the return value >= `siz`, truncation occurred.
///
/// # Safety
/// `dst` must be valid for `siz` bytes; `src` must be a NUL-terminated C string.
pub unsafe fn strlcpy(dst: *mut c_char, src: *const c_char, siz: Size) -> Size {
    let mut d = dst;
    let mut s = src;
    let mut n = siz;

    /* Copy as many bytes as will fit */
    if n != 0 {
        // C: while (--n != 0) { if ((*d++ = *s++) == '\0') break; }
        loop {
            n -= 1;
            if n == 0 {
                break;
            }
            let c = *s;
            s = s.add(1);
            *d = c;
            d = d.add(1);
            if c == 0 {
                break;
            }
        }
    }

    /* Not enough room in dst, add NUL and traverse rest of src */
    if n == 0 {
        if siz != 0 {
            *d = 0; /* NUL-terminate dst */
        }
        // C: while (*s++) ;
        loop {
            let c = *s;
            s = s.add(1);
            if c == 0 {
                break;
            }
        }
    }

    (s.offset_from(src) - 1) as Size /* count does not include NUL */
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copies_and_truncates() {
        unsafe {
            let mut buf = [0i8; 8];
            // fits
            let r = strlcpy(buf.as_mut_ptr(), c"hello".as_ptr(), buf.len());
            assert_eq!(r, 5);
            assert_eq!(&buf[..6], &[b'h' as i8, b'e' as i8, b'l' as i8, b'l' as i8, b'o' as i8, 0]);
            // truncates: src len 11 > 8-1
            let r = strlcpy(buf.as_mut_ptr(), c"abcdefghijk".as_ptr(), buf.len());
            assert_eq!(r, 11); // returns full src length
            assert_eq!(buf[7], 0); // always NUL-terminated
            assert_eq!(buf[0], b'a' as i8);
            assert_eq!(buf[6], b'g' as i8);
        }
    }
}
