/*-------------------------------------------------------------------------
 *
 * base64.c
 *	  Encoding and decoding routines for base64 without whitespace.
 *
 * Copyright (c) 2001-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/common/base64.c
 *
 *-------------------------------------------------------------------------
 */

// Combined translation of:
//   HEADER: src/include/common/base64.h
//   IMPL:   src/common/base64.c
//
// base64.h
//	  Encoding and decoding routines for base64 without whitespace
//	  support.
//
// Portions Copyright (c) 2001-2025, PostgreSQL Global Development Group
//
// src/include/common/base64.h

// #ifndef FRONTEND
// #include "postgres.h"
// #else
// #include "postgres_fe.h"
// #endif
//
// We translate the BACKEND path (postgres.h): error reporting uses elog!(ERROR, ...).
// TODO(pg-port): the FRONTEND branch (postgres_fe.h) is not yet ported; PostgreSQL
// frontend builds route elog through a frontend-specific implementation. When the
// frontend is ported, gate the error path accordingly.
//
// #include "common/base64.h"

#![allow(clippy::missing_safety_doc)]

use crate::prelude::*;

/*
 * BASE64
 */

static _base64: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static b64lookup: [int8; 128] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1,
    -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
    -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
];

/*
 * pg_b64_encode
 *
 * Encode the 'src' byte array into base64.  Returns the length of the encoded
 * string, and -1 in the event of an error with the result buffer zeroed for
 * safety.
 */
#[must_use]
pub unsafe fn pg_b64_encode(src: *const uint8, len: c_int, dst: *mut c_char, dstlen: c_int) -> c_int {
    let mut s: *const uint8;
    let end: *const uint8 = src.add(len as usize);
    let mut pos: c_int = 2;
    let mut buf: uint32 = 0;

    s = src;
    // C: `p = dst;` (p declared above with `char *p`)
    let mut p: *mut c_char = dst;

    while s < end {
        buf |= (*s as uint32) << (pos << 3);
        pos -= 1;
        s = s.add(1);

        /* write it out */
        if pos < 0 {
            /*
             * Leave if there is an overflow in the area allocated for the
             * encoded string.
             */
            if (p.offset_from(dst) as c_int + 4) > dstlen {
                /* error */
                core::ptr::write_bytes(dst, 0, dstlen as usize);
                return -1;
            }

            *p = _base64[((buf >> 18) & 0x3f) as usize] as c_char;
            p = p.add(1);
            *p = _base64[((buf >> 12) & 0x3f) as usize] as c_char;
            p = p.add(1);
            *p = _base64[((buf >> 6) & 0x3f) as usize] as c_char;
            p = p.add(1);
            *p = _base64[(buf & 0x3f) as usize] as c_char;
            p = p.add(1);

            pos = 2;
            buf = 0;
        }
    }
    if pos != 2 {
        /*
         * Leave if there is an overflow in the area allocated for the encoded
         * string.
         */
        if (p.offset_from(dst) as c_int + 4) > dstlen {
            /* error */
            core::ptr::write_bytes(dst, 0, dstlen as usize);
            return -1;
        }

        *p = _base64[((buf >> 18) & 0x3f) as usize] as c_char;
        p = p.add(1);
        *p = _base64[((buf >> 12) & 0x3f) as usize] as c_char;
        p = p.add(1);
        *p = if pos == 0 {
            _base64[((buf >> 6) & 0x3f) as usize] as c_char
        } else {
            b'=' as c_char
        };
        p = p.add(1);
        *p = b'=' as c_char;
        p = p.add(1);
    }

    Assert!((p.offset_from(dst) as c_int) <= dstlen);
    p.offset_from(dst) as c_int

    /*
     * error:
     *   memset(dst, 0, dstlen); return -1;
     * The C `goto error` targets above are inlined at each goto site
     * (Rust has no goto), so this label has no remaining body.
     */
}

/*
 * pg_b64_decode
 *
 * Decode the given base64 string.  Returns the length of the decoded
 * string on success, and -1 in the event of an error with the result
 * buffer zeroed for safety.
 */
#[must_use]
pub unsafe fn pg_b64_decode(src: *const c_char, len: c_int, dst: *mut uint8, dstlen: c_int) -> c_int {
    let srcend: *const c_char = src.add(len as usize);
    let mut s: *const c_char = src;
    let mut p: *mut uint8 = dst;
    let mut c: c_char;
    let mut b: c_int = 0;
    let mut buf: uint32 = 0;
    let mut pos: c_int = 0;
    let mut end: c_int = 0;

    while s < srcend {
        c = *s;
        s = s.add(1);

        /* Leave if a whitespace is found */
        if c == b' ' as c_char || c == b'\t' as c_char || c == b'\n' as c_char || c == b'\r' as c_char {
            /* error */
            core::ptr::write_bytes(dst, 0, dstlen as usize);
            return -1;
        }

        if c == b'=' as c_char {
            /* end sequence */
            if end == 0 {
                if pos == 2 {
                    end = 1;
                } else if pos == 3 {
                    end = 2;
                } else {
                    /*
                     * Unexpected "=" character found while decoding base64
                     * sequence.
                     */
                    /* error */
                    core::ptr::write_bytes(dst, 0, dstlen as usize);
                    return -1;
                }
            }
            b = 0;
        } else {
            b = -1;
            if c > 0 && c < 127 {
                b = b64lookup[(c as uint8) as usize] as c_int;
            }
            if b < 0 {
                /* invalid symbol found */
                /* error */
                core::ptr::write_bytes(dst, 0, dstlen as usize);
                return -1;
            }
        }
        /* add it to buffer */
        buf = (buf << 6) + (b as uint32);
        pos += 1;
        if pos == 4 {
            /*
             * Leave if there is an overflow in the area allocated for the
             * decoded string.
             */
            if (p.offset_from(dst) as c_int + 1) > dstlen {
                /* error */
                core::ptr::write_bytes(dst, 0, dstlen as usize);
                return -1;
            }
            *p = ((buf >> 16) & 255) as uint8;
            p = p.add(1);

            if end == 0 || end > 1 {
                /* overflow check */
                if (p.offset_from(dst) as c_int + 1) > dstlen {
                    /* error */
                    core::ptr::write_bytes(dst, 0, dstlen as usize);
                    return -1;
                }
                *p = ((buf >> 8) & 255) as uint8;
                p = p.add(1);
            }
            if end == 0 || end > 2 {
                /* overflow check */
                if (p.offset_from(dst) as c_int + 1) > dstlen {
                    /* error */
                    core::ptr::write_bytes(dst, 0, dstlen as usize);
                    return -1;
                }
                *p = (buf & 255) as uint8;
                p = p.add(1);
            }
            buf = 0;
            pos = 0;
        }
    }

    if pos != 0 {
        /*
         * base64 end sequence is invalid.  Input data is missing padding, is
         * truncated or is otherwise corrupted.
         */
        /* error */
        core::ptr::write_bytes(dst, 0, dstlen as usize);
        return -1;
    }

    Assert!((p.offset_from(dst) as c_int) <= dstlen);
    p.offset_from(dst) as c_int

    /*
     * error:
     *   memset(dst, 0, dstlen); return -1;
     * Inlined at each goto site above (Rust has no goto).
     */
}

/*
 * pg_b64_enc_len
 *
 * Returns to caller the length of the string if it were encoded with
 * base64 based on the length provided by caller.  This is useful to
 * estimate how large a buffer allocation needs to be done before doing
 * the actual encoding.
 */
pub fn pg_b64_enc_len(srclen: c_int) -> c_int {
    /* 3 bytes will be converted to 4 */
    (srclen + 2) / 3 * 4
}

/*
 * pg_b64_dec_len
 *
 * Returns to caller the length of the string if it were to be decoded
 * with base64, based on the length given by caller.  This is useful to
 * estimate how large a buffer allocation needs to be done before doing
 * the actual decoding.
 */
pub fn pg_b64_dec_len(srclen: c_int) -> c_int {
    (srclen * 3) >> 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        unsafe {
            let input = b"Hello, PostgreSQL!";
            let enc_cap = pg_b64_enc_len(input.len() as c_int) + 1;
            let mut enc = vec![0i8; enc_cap as usize];
            let elen = pg_b64_encode(input.as_ptr(), input.len() as c_int, enc.as_mut_ptr(), enc_cap);
            assert!(elen > 0);
            // decode back
            let dec_cap = pg_b64_dec_len(elen) + 1;
            let mut dec = vec![0u8; dec_cap as usize];
            let dlen = pg_b64_decode(enc.as_ptr(), elen, dec.as_mut_ptr(), dec_cap);
            assert_eq!(dlen as usize, input.len());
            assert_eq!(&dec[..dlen as usize], input);
        }
    }
}
