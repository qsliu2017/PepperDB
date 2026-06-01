//! Translation of postgres/src/backend/utils/adt/encode.c
//!
//! Various data encoding/decoding things: the bytea<->text encoders behind the
//! SQL functions `encode()`/`decode()` (binary_encode / binary_decode), plus the
//! three self-contained codecs they dispatch to (hex, base64, escape).
//!
//! Copyright (c) 2001-2025, PostgreSQL Global Development Group
//!
//! `#include`s mapped:
//!   - postgres.h, varatt.h          -> crate::prelude / crate::varatt (VAR* helpers)
//!   - utils/builtins.h              -> crate::utils::adt::varlena (TextDatumGetCString)
//!   - utils/memutils.h (MaxAllocSize) -> crate::prelude (MaxAllocSize)
//!   - <ctype.h>                     -> not needed (codecs use their own tables)
//!   - port (pg_strcasecmp)          -> crate::port::pgstrcasecmp::pg_strcasecmp
//! STUBBED:
//!   - mb/pg_wchar.h (pg_mblen_range, used only to pretty-print the offending byte
//!     in an error message): no multibyte support ported yet, so the error messages
//!     emit a single byte (single-byte-encoding assumption).  See pg_mblen_range below.
//!   - hex_decode_safe's `Node *escontext` soft-error path (nodes/miscnodes.h ereturn):
//!     not ported -> we raise a hard ERROR like uuid.rs / bool.rs do.

#![allow(clippy::missing_safety_doc)]

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::{PG_GETARG_DATUM, PG_RETURN_BYTEA_P, PG_RETURN_TEXT_P};
use crate::c::{bytea, int8, text, uint32};
use crate::nodes::nodes::Node;
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::utils::adt::varlena::TextDatumGetCString;
use core::ffi::{c_char, c_int};

// libc string.h: memcpy.  Bound here for fidelity with the C `memcpy` call in
// hex_encode (the byte loops elsewhere use Rust slice writes).
extern "C" {
    fn memcpy(dst: *mut core::ffi::c_void, src: *const core::ffi::c_void, n: usize) -> *mut core::ffi::c_void;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_INVALID_TEXT_REPRESENTATION: c_int = 0;

/*
 * Encoding conversion API.
 * encode_len() and decode_len() compute the amount of space needed, while
 * encode() and decode() perform the actual conversions.  It is okay for
 * the _len functions to return an overestimate, but not an underestimate.
 * (Having said that, large overestimates could cause unnecessary errors,
 * so it's better to get it right.)  The conversion routines write to the
 * buffer at *res and return the true length of their output.
 */
struct pg_encoding {
    encode_len: unsafe fn(data: *const c_char, dlen: usize) -> u64,
    decode_len: unsafe fn(data: *const c_char, dlen: usize) -> u64,
    encode: unsafe fn(data: *const c_char, dlen: usize, res: *mut c_char) -> u64,
    decode: unsafe fn(data: *const c_char, dlen: usize, res: *mut c_char) -> u64,
}

/*
 * SQL functions.
 */

pub unsafe fn binary_encode(fcinfo: FunctionCallInfo) -> Datum {
    // bytea *data = PG_GETARG_BYTEA_PP(0);
    let data: *mut bytea = crate::varatt::pg_detoast_datum_packed(
        crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut core::ffi::c_void,
    ) as *mut bytea;
    let name: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let result: *mut text;
    let namebuf: *mut c_char;
    let dataptr: *mut c_char;
    let datalen: usize;
    let resultlen: u64;
    let res: u64;
    let enc: *const pg_encoding;

    namebuf = TextDatumGetCString(name);

    enc = pg_find_encoding(namebuf);
    if enc.is_null() {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("unrecognized encoding: \"{}\"", cstr(namebuf))
        );
    }

    dataptr = VARDATA_ANY(data as *const c_char);
    datalen = VARSIZE_ANY_EXHDR(data as *const c_char) as usize;

    resultlen = ((*enc).encode_len)(dataptr, datalen);

    /*
     * resultlen possibly overflows uint32, therefore on 32-bit machines it's
     * unsafe to rely on palloc's internal check.
     */
    if resultlen > (MaxAllocSize - VARHDRSZ as usize) as u64 {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!("result of encoding conversion is too large")
        );
    }

    result = palloc(VARHDRSZ as usize + resultlen as usize) as *mut text;

    res = ((*enc).encode)(dataptr, datalen, VARDATA(result as *const c_char));

    /* Make this FATAL 'cause we've trodden on memory ... */
    if res > resultlen {
        elog!(FATAL, "overflow - encode estimate too small");
    }

    SET_VARSIZE(result as *mut c_char, VARHDRSZ + res as c_int);

    PG_RETURN_TEXT_P!(result);
}

pub unsafe fn binary_decode(fcinfo: FunctionCallInfo) -> Datum {
    // text *data = PG_GETARG_TEXT_PP(0);
    let data: *mut text = crate::varatt::pg_detoast_datum_packed(
        crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut core::ffi::c_void,
    ) as *mut text;
    let name: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let result: *mut bytea;
    let namebuf: *mut c_char;
    let dataptr: *mut c_char;
    let datalen: usize;
    let resultlen: u64;
    let res: u64;
    let enc: *const pg_encoding;

    namebuf = TextDatumGetCString(name);

    enc = pg_find_encoding(namebuf);
    if enc.is_null() {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("unrecognized encoding: \"{}\"", cstr(namebuf))
        );
    }

    dataptr = VARDATA_ANY(data as *const c_char);
    datalen = VARSIZE_ANY_EXHDR(data as *const c_char) as usize;

    resultlen = ((*enc).decode_len)(dataptr, datalen);

    /*
     * resultlen possibly overflows uint32, therefore on 32-bit machines it's
     * unsafe to rely on palloc's internal check.
     */
    if resultlen > (MaxAllocSize - VARHDRSZ as usize) as u64 {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!("result of decoding conversion is too large")
        );
    }

    result = palloc(VARHDRSZ as usize + resultlen as usize) as *mut bytea;

    res = ((*enc).decode)(dataptr, datalen, VARDATA(result as *const c_char));

    /* Make this FATAL 'cause we've trodden on memory ... */
    if res > resultlen {
        elog!(FATAL, "overflow - decode estimate too small");
    }

    SET_VARSIZE(result as *mut c_char, VARHDRSZ + res as c_int);

    PG_RETURN_BYTEA_P!(result);
}

/*
 * HEX
 */

/*
 * The hex expansion of each possible byte value (two chars per value).
 */
static hextbl: &[u8; 512] = b"\
000102030405060708090a0b0c0d0e0f\
101112131415161718191a1b1c1d1e1f\
202122232425262728292a2b2c2d2e2f\
303132333435363738393a3b3c3d3e3f\
404142434445464748494a4b4c4d4e4f\
505152535455565758595a5b5c5d5e5f\
606162636465666768696a6b6c6d6e6f\
707172737475767778797a7b7c7d7e7f\
808182838485868788898a8b8c8d8e8f\
909192939495969798999a9b9c9d9e9f\
a0a1a2a3a4a5a6a7a8a9aaabacadaeaf\
b0b1b2b3b4b5b6b7b8b9babbbcbdbebf\
c0c1c2c3c4c5c6c7c8c9cacbcccdcecf\
d0d1d2d3d4d5d6d7d8d9dadbdcdddedf\
e0e1e2e3e4e5e6e7e8e9eaebecedeeef\
f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

static hexlookup: [int8; 128] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
];

pub unsafe fn hex_encode(src: *const c_char, len: usize, dst: *mut c_char) -> u64 {
    let end: *const c_char = src.add(len);
    let mut src = src;
    let mut dst = dst;

    while src < end {
        let usrc = *(src as *const u8);

        memcpy(
            dst as *mut core::ffi::c_void,
            hextbl.as_ptr().add(2 * usrc as usize) as *const core::ffi::c_void,
            2,
        );
        src = src.add(1);
        dst = dst.add(2);
    }
    len as u64 * 2
}

#[inline]
unsafe fn get_hex(cp: *const c_char, out: *mut c_char) -> bool {
    let c = *(cp as *const u8);
    let mut res: c_int = -1;

    if c < 127 {
        res = hexlookup[c as usize] as c_int;
    }

    *out = res as c_char;

    res >= 0
}

pub unsafe fn hex_decode(src: *const c_char, len: usize, dst: *mut c_char) -> u64 {
    hex_decode_safe(src, len, dst, null_mut())
}

pub unsafe fn hex_decode_safe(src: *const c_char, len: usize, dst: *mut c_char, escontext: *mut Node) -> u64 {
    let srcend: *const c_char;
    let mut s: *const c_char;
    let mut v1: c_char = 0;
    let mut v2: c_char = 0;
    let mut p: *mut c_char;
    let _ = escontext; // TODO(pg-port): soft-error (ereturn) path not yet ported -> hard ERROR.

    srcend = src.add(len);
    s = src;
    p = dst;
    while s < srcend {
        if *s == b' ' as c_char || *s == b'\n' as c_char || *s == b'\t' as c_char || *s == b'\r' as c_char {
            s = s.add(1);
            continue;
        }
        if !get_hex(s, &mut v1) {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(
                ERROR,
                errmsg!(
                    "invalid hexadecimal digit: \"{}\"",
                    mblen_str(s, srcend)
                )
            );
        }
        s = s.add(1);
        if s >= srcend {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(
                ERROR,
                errmsg!("invalid hexadecimal data: odd number of digits")
            );
        }
        if !get_hex(s, &mut v2) {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(
                ERROR,
                errmsg!(
                    "invalid hexadecimal digit: \"{}\"",
                    mblen_str(s, srcend)
                )
            );
        }
        s = s.add(1);
        *p = ((v1 << 4) | v2) as c_char;
        p = p.add(1);
    }

    p.offset_from(dst) as u64
}

unsafe fn hex_enc_len(_src: *const c_char, srclen: usize) -> u64 {
    (srclen as u64) << 1
}

unsafe fn hex_dec_len(_src: *const c_char, srclen: usize) -> u64 {
    (srclen as u64) >> 1
}

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

unsafe fn pg_base64_encode(src: *const c_char, len: usize, dst: *mut c_char) -> u64 {
    let mut p: *mut c_char;
    let mut lend: *mut c_char = dst.add(76);
    let mut s: *const c_char;
    let end: *const c_char = src.add(len);
    let mut pos: c_int = 2;
    let mut buf: uint32 = 0;

    s = src;
    p = dst;

    while s < end {
        buf |= (*(s as *const u8) as uint32) << (pos << 3);
        pos -= 1;
        s = s.add(1);

        /* write it out */
        if pos < 0 {
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
        if p >= lend {
            *p = b'\n' as c_char;
            p = p.add(1);
            lend = p.add(76);
        }
    }
    if pos != 2 {
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

    p.offset_from(dst) as u64
}

unsafe fn pg_base64_decode(src: *const c_char, len: usize, dst: *mut c_char) -> u64 {
    let srcend: *const c_char = src.add(len);
    let mut s: *const c_char = src;
    let mut p: *mut c_char = dst;
    let mut c: c_char;
    let mut b: c_int = 0;
    let mut buf: uint32 = 0;
    let mut pos: c_int = 0;
    let mut end: c_int = 0;

    while s < srcend {
        c = *s;
        s = s.add(1);

        if c == b' ' as c_char || c == b'\t' as c_char || c == b'\n' as c_char || c == b'\r' as c_char {
            continue;
        }

        if c == b'=' as c_char {
            /* end sequence */
            if end == 0 {
                if pos == 2 {
                    end = 1;
                } else if pos == 3 {
                    end = 2;
                } else {
                    let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                    ereport!(
                        ERROR,
                        errmsg!("unexpected \"=\" while decoding base64 sequence")
                    );
                }
            }
            b = 0;
        } else {
            b = -1;
            if c > 0 && c < 127 {
                b = b64lookup[(c as u8) as usize] as c_int;
            }
            if b < 0 {
                let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                ereport!(
                    ERROR,
                    errmsg!(
                        "invalid symbol \"{}\" found while decoding base64 sequence",
                        mblen_str(s.sub(1), srcend)
                    )
                );
            }
        }
        /* add it to buffer */
        buf = (buf << 6) + b as uint32;
        pos += 1;
        if pos == 4 {
            *p = ((buf >> 16) & 255) as c_char;
            p = p.add(1);
            if end == 0 || end > 1 {
                *p = ((buf >> 8) & 255) as c_char;
                p = p.add(1);
            }
            if end == 0 || end > 2 {
                *p = (buf & 255) as c_char;
                p = p.add(1);
            }
            buf = 0;
            pos = 0;
        }
    }

    if pos != 0 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            // C also attaches errhint("Input data is missing padding, is
            // truncated, or is otherwise corrupted.") - the errhint shim is a no-op.
            errmsg!("invalid base64 end sequence")
        );
    }

    p.offset_from(dst) as u64
}

unsafe fn pg_base64_enc_len(_src: *const c_char, srclen: usize) -> u64 {
    /* 3 bytes will be converted to 4, linefeed after 76 chars */
    (srclen as u64 + 2) / 3 * 4 + srclen as u64 / (76 * 3 / 4)
}

unsafe fn pg_base64_dec_len(_src: *const c_char, srclen: usize) -> u64 {
    (srclen as u64 * 3) >> 2
}

/*
 * Escape
 * Minimally escape bytea to text.
 * De-escape text to bytea.
 *
 * We must escape zero bytes and high-bit-set bytes to avoid generating
 * text that might be invalid in the current encoding, or that might
 * change to something else if passed through an encoding conversion
 * (leading to failing to de-escape to the original bytea value).
 * Also of course backslash itself has to be escaped.
 *
 * De-escaping processes \\ and any \### octal
 */

// #define VAL(CH)  ((CH) - '0')
#[inline]
fn VAL(ch: c_char) -> c_int {
    (ch as c_int) - ('0' as c_int)
}
// #define DIG(VAL) ((VAL) + '0')
#[inline]
fn DIG(val: c_int) -> c_char {
    (val + ('0' as c_int)) as c_char
}

unsafe fn esc_encode(src: *const c_char, srclen: usize, dst: *mut c_char) -> u64 {
    let end: *const c_char = src.add(srclen);
    let mut src = src;
    let mut rp: *mut c_char = dst;
    let mut len: u64 = 0;

    while src < end {
        let c = *(src as *const u8);

        if c == b'\0' || IS_HIGHBIT_SET(c) {
            *rp.add(0) = b'\\' as c_char;
            *rp.add(1) = DIG((c >> 6) as c_int);
            *rp.add(2) = DIG(((c >> 3) & 7) as c_int);
            *rp.add(3) = DIG((c & 7) as c_int);
            rp = rp.add(4);
            len += 4;
        } else if c == b'\\' {
            *rp.add(0) = b'\\' as c_char;
            *rp.add(1) = b'\\' as c_char;
            rp = rp.add(2);
            len += 2;
        } else {
            *rp = c as c_char;
            rp = rp.add(1);
            len += 1;
        }

        src = src.add(1);
    }

    len
}

unsafe fn esc_decode(src: *const c_char, srclen: usize, dst: *mut c_char) -> u64 {
    let end: *const c_char = src.add(srclen);
    let mut src = src;
    let mut rp: *mut c_char = dst;
    let mut len: u64 = 0;

    while src < end {
        if *src.add(0) != b'\\' as c_char {
            *rp = *src;
            rp = rp.add(1);
            src = src.add(1);
        } else if src.add(3) < end
            && (*src.add(1) >= b'0' as c_char && *src.add(1) <= b'3' as c_char)
            && (*src.add(2) >= b'0' as c_char && *src.add(2) <= b'7' as c_char)
            && (*src.add(3) >= b'0' as c_char && *src.add(3) <= b'7' as c_char)
        {
            let mut val: c_int;

            val = VAL(*src.add(1));
            val <<= 3;
            val += VAL(*src.add(2));
            val <<= 3;
            *rp = (val + VAL(*src.add(3))) as c_char;
            rp = rp.add(1);
            src = src.add(4);
        } else if src.add(1) < end && (*src.add(1) == b'\\' as c_char) {
            *rp = b'\\' as c_char;
            rp = rp.add(1);
            src = src.add(2);
        } else {
            /*
             * One backslash, not followed by ### valid octal. Should never
             * get here, since esc_dec_len does same check.
             */
            let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
            ereport!(
                ERROR,
                errmsg!("invalid input syntax for type {}", "bytea")
            );
        }

        len += 1;
    }

    len
}

unsafe fn esc_enc_len(src: *const c_char, srclen: usize) -> u64 {
    let end: *const c_char = src.add(srclen);
    let mut src = src;
    let mut len: u64 = 0;

    while src < end {
        if *src == b'\0' as c_char || IS_HIGHBIT_SET(*(src as *const u8)) {
            len += 4;
        } else if *src == b'\\' as c_char {
            len += 2;
        } else {
            len += 1;
        }

        src = src.add(1);
    }

    len
}

unsafe fn esc_dec_len(src: *const c_char, srclen: usize) -> u64 {
    let end: *const c_char = src.add(srclen);
    let mut src = src;
    let mut len: u64 = 0;

    while src < end {
        if *src.add(0) != b'\\' as c_char {
            src = src.add(1);
        } else if src.add(3) < end
            && (*src.add(1) >= b'0' as c_char && *src.add(1) <= b'3' as c_char)
            && (*src.add(2) >= b'0' as c_char && *src.add(2) <= b'7' as c_char)
            && (*src.add(3) >= b'0' as c_char && *src.add(3) <= b'7' as c_char)
        {
            /*
             * backslash + valid octal
             */
            src = src.add(4);
        } else if src.add(1) < end && (*src.add(1) == b'\\' as c_char) {
            /*
             * two backslashes = backslash
             */
            src = src.add(2);
        } else {
            /*
             * one backslash, not followed by ### valid octal
             */
            let _ = errcode(ERRCODE_INVALID_TEXT_REPRESENTATION);
            ereport!(
                ERROR,
                errmsg!("invalid input syntax for type {}", "bytea")
            );
        }

        len += 1;
    }
    len
}

/*
 * Common
 */

// enclist[] entry: the C struct pairs a NUL-terminated name with a pg_encoding.
// `name` is stored as a NUL-terminated byte literal (its `.as_ptr()` is the
// `const char *` the C literal would give); the NULL terminator entry from C is
// represented by simply ending the array.
struct enclist_entry {
    name: &'static [u8],
    enc: pg_encoding,
}

/*
 * static const struct { const char *name; struct pg_encoding enc; } enclist[] =
 * {
 *   {"hex",    {hex_enc_len, hex_dec_len, hex_encode, hex_decode}},
 *   {"base64", {pg_base64_enc_len, pg_base64_dec_len, pg_base64_encode, pg_base64_decode}},
 *   {"escape", {esc_enc_len, esc_dec_len, esc_encode, esc_decode}},
 *   {NULL,     {NULL, NULL, NULL, NULL}}
 * };
 */
static enclist: [enclist_entry; 3] = [
    enclist_entry {
        name: b"hex\0",
        enc: pg_encoding {
            encode_len: hex_enc_len,
            decode_len: hex_dec_len,
            encode: hex_encode,
            decode: hex_decode,
        },
    },
    enclist_entry {
        name: b"base64\0",
        enc: pg_encoding {
            encode_len: pg_base64_enc_len,
            decode_len: pg_base64_dec_len,
            encode: pg_base64_encode,
            decode: pg_base64_decode,
        },
    },
    enclist_entry {
        name: b"escape\0",
        enc: pg_encoding {
            encode_len: esc_enc_len,
            decode_len: esc_dec_len,
            encode: esc_encode,
            decode: esc_decode,
        },
    },
];

unsafe fn pg_find_encoding(name: *const c_char) -> *const pg_encoding {
    for entry in enclist.iter() {
        if pg_strcasecmp(entry.name.as_ptr() as *const c_char, name) == 0 {
            return &entry.enc as *const pg_encoding;
        }
    }

    null_mut()
}

/*
 * pg_mblen_range stub: in C this returns the byte length of the (possibly
 * multibyte) character starting at `s`, clamped to the remaining bytes, so the
 * error message can show one whole character.  Multibyte support (mb/pg_wchar.h)
 * is not ported yet; assuming a single-byte encoding, emit just the one byte.
 *
 * # Safety
 * `s` < `srcend`, both valid pointers into the same buffer.
 */
unsafe fn mblen_str(s: *const c_char, srcend: *const c_char) -> std::string::String {
    // TODO(pg-port): pg_mblen_range needs mb/pg_wchar.h; single-byte assumption.
    let _ = srcend;
    let b = *(s as *const u8);
    std::string::String::from_utf8_lossy(&[b]).into_owned()
}

/*
 * Format a C string for an error message via Rust `{}` (lossy).
 *
 * # Safety
 * `s` must be a valid NUL-terminated C string.
 */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, n);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{DatumGetPointer, PointerGetDatum};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::adt::varlena::cstring_to_text_with_len;
    use crate::utils::fmgr::DirectFunctionCall2Coll;

    // Build a bytea Datum from raw bytes.
    unsafe fn bytea_datum(bytes: &[u8]) -> Datum {
        let p = cstring_to_text_with_len(bytes.as_ptr() as *const c_char, bytes.len() as c_int);
        PointerGetDatum(p as *const core::ffi::c_void)
    }
    // Build a text Datum from a &str.
    unsafe fn text_datum(s: &str) -> Datum {
        let p = cstring_to_text_with_len(s.as_ptr() as *const c_char, s.len() as c_int);
        PointerGetDatum(p as *const core::ffi::c_void)
    }
    // Read a varlena Datum's bytes into a Vec.
    unsafe fn datum_bytes(d: Datum) -> std::vec::Vec<u8> {
        let p = DatumGetPointer(d) as *const c_char;
        let len = VARSIZE_ANY_EXHDR(p) as usize;
        let data = VARDATA_ANY(p);
        core::slice::from_raw_parts(data as *const u8, len).to_vec()
    }
    unsafe fn datum_str(d: Datum) -> std::string::String {
        std::string::String::from_utf8(datum_bytes(d)).unwrap()
    }

    #[test]
    fn hex_round_trip() {
        unsafe {
            let raw: &[u8] = &[0x00, 0x01, 0xff, 0xab, 0x10, 0x7f, 0x80];
            // encode(raw, 'hex')
            let enc = DirectFunctionCall2Coll(
                binary_encode,
                InvalidOid,
                bytea_datum(raw),
                text_datum("hex"),
            );
            assert_eq!(datum_str(enc), "0001ffab107f80");
            // decode(hexstr, 'hex') -> original
            let dec = DirectFunctionCall2Coll(
                binary_decode,
                InvalidOid,
                text_datum("0001ffab107f80"),
                text_datum("hex"),
            );
            assert_eq!(datum_bytes(dec), raw);

            // case-insensitive encoding name + whitespace + uppercase digits
            let dec2 = DirectFunctionCall2Coll(
                binary_decode,
                InvalidOid,
                text_datum("DE AD\nBE\tEF"),
                text_datum("HEX"),
            );
            assert_eq!(datum_bytes(dec2), &[0xde, 0xad, 0xbe, 0xef]);
        }
    }

    #[test]
    fn base64_round_trip() {
        unsafe {
            let raw: &[u8] = b"Hello, PepperDB! base64 roundtrip.";
            let enc = DirectFunctionCall2Coll(
                binary_encode,
                InvalidOid,
                bytea_datum(raw),
                text_datum("base64"),
            );
            let dec = DirectFunctionCall2Coll(
                binary_decode,
                InvalidOid,
                enc,
                text_datum("base64"),
            );
            assert_eq!(datum_bytes(dec), raw);

            // known vector: "foobar" -> "Zm9vYmFy"
            let enc2 = DirectFunctionCall2Coll(
                binary_encode,
                InvalidOid,
                bytea_datum(b"foobar"),
                text_datum("base64"),
            );
            assert_eq!(datum_str(enc2), "Zm9vYmFy");
            // padding cases
            let enc3 = DirectFunctionCall2Coll(
                binary_encode,
                InvalidOid,
                bytea_datum(b"f"),
                text_datum("base64"),
            );
            assert_eq!(datum_str(enc3), "Zg==");
        }
    }

    #[test]
    fn escape_round_trip() {
        unsafe {
            let raw: &[u8] = &[0x00, b'a', b'\\', 0xff, b'z', 0x7f];
            let enc = DirectFunctionCall2Coll(
                binary_encode,
                InvalidOid,
                bytea_datum(raw),
                text_datum("escape"),
            );
            // 0x00 -> \000, '\\' -> \\, 0xff -> \377; printable left as-is
            assert_eq!(datum_str(enc), "\\000a\\\\\\377z\u{7f}");
            let dec = DirectFunctionCall2Coll(
                binary_decode,
                InvalidOid,
                enc,
                text_datum("escape"),
            );
            assert_eq!(datum_bytes(dec), raw);
        }
    }

    #[test]
    #[should_panic]
    fn hex_decode_rejects_bad_digit() {
        unsafe {
            DirectFunctionCall2Coll(
                binary_decode,
                InvalidOid,
                text_datum("zz"),
                text_datum("hex"),
            );
        }
    }

    #[test]
    #[should_panic]
    fn unrecognized_encoding_errors() {
        unsafe {
            DirectFunctionCall2Coll(
                binary_encode,
                InvalidOid,
                bytea_datum(b"abc"),
                text_datum("rot13"),
            );
        }
    }
}
