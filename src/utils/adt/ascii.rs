//! Translation of postgres/src/backend/utils/adt/ascii.c
//!
//! The PostgreSQL routine for string to ascii conversion.
//!
//! Portions Copyright (c) 1999-2025, PostgreSQL Global Development Group
//!
//! `#include`s mapped:
//!   mb/pg_wchar.h        -> crate::mb::wchar (pg_enc constants, PG_VALID_ENCODING,
//!                           pg_encoding_max_length, pg_utf8_islegal) and
//!                           crate::common::encnames (pg_char_to_encoding,
//!                           pg_encoding_to_char).
//!   utils/ascii.h        -> declares ascii_safe_strlcpy (translated below) and the
//!                           SIMD inline `is_valid_ascii` (port/simd.h Vector8) which
//!                           is NOT translated here (header-inline, needs port/simd).
//!   utils/fmgrprotos.h   -> the fmgr prototypes; the fmgr machinery is crate::utils::fmgr.
//!   varatt.h             -> crate::varatt (VARDATA/VARSIZE/SET_VARSIZE/VARDATA_ANY/
//!                           VARSIZE_ANY_EXHDR).
//!
//! In addition to ascii.c proper, the closely-related fmgr functions `ascii(text)`
//! and `chr(int4)` (which physically live in oracle_compat.c) are translated here
//! per the port plan, since they are the public face of the "ascii" family.  Their
//! C origin is noted at each definition.
//!
//! STUBBED (deps not yet ported):
//!   GetDatabaseEncoding (utils/mb/mbutils.c, needs the GUC/SetDatabaseEncoding
//!     state) -> a local stub assumes the database encoding is PG_UTF8, matching the
//!     port-plan assumption.  This affects ascii()/chr()/to_ascii_default().
//!   to_ascii_encname / to_ascii_enc / to_ascii_default ultimately call the fully
//!     translated pg_to_ascii(); they are translated, but note pg_to_ascii() only
//!     supports the four 8-bit encodings (LATIN1/LATIN2/LATIN9/WIN1250) and raises
//!     ERROR otherwise (e.g. for the UTF8 default), exactly as upstream.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::{
    pg_detoast_datum_packed, SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE, VARSIZE_ANY_EXHDR,
};
use crate::{PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_NAME, PG_RETURN_INT32};
use crate::c::{int32, text, uint32};
use crate::common::encnames::{pg_char_to_encoding, pg_encoding_to_char};
use crate::mb::wchar::{
    pg_encoding_max_length, pg_utf8_islegal, PG_VALID_ENCODING, PG_LATIN1, PG_LATIN2, PG_LATIN9,
    PG_UTF8, PG_WIN1250,
};
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use core::ffi::{c_char, c_int, c_uchar, c_void};

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

/*
 * GetDatabaseEncoding (utils/mb/mbutils.c) returns the current database
 * encoding.  That subsystem (the GUC-driven DatabaseEncoding state) is not yet
 * ported; the port plan assumes a UTF8 database, so this stub returns PG_UTF8.
 *
 * TODO(pg-port): real GetDatabaseEncoding needs utils/mb/mbutils.c + GUC state.
 */
#[inline]
unsafe fn GetDatabaseEncoding() -> c_int {
    PG_UTF8 as c_int
}

/*
 * PG_GETARG_TEXT_P_COPY(n): a writable, fully-detoasted (4-byte header) copy of
 * argument n.  The fmgr `PG_DETOAST_DATUM_COPY!` macro routes through the
 * not-yet-ported toast detoaster (crate::utils::fmgr::pg_detoast_datum_copy is
 * unimplemented!), so we spell the common in-line case here: detoast (identity
 * for plain/short datums via crate::varatt::pg_detoast_datum_packed) then palloc
 * a fresh 4-byte-header copy that pg_to_ascii() may overwrite in place.
 *
 * # Safety
 * `datum` holds a valid (in-line) text pointer.
 */
unsafe fn pg_getarg_text_p_copy(datum: Datum) -> *mut text {
    let src = pg_detoast_datum_packed(DatumGetPointer(datum) as *mut c_void) as *const c_char;
    let len = VARSIZE_ANY_EXHDR(src) as usize; /* data length, header excluded */
    let total = len + VARHDRSZ as usize;
    let result = palloc(total) as *mut text;
    SET_VARSIZE(result as *mut c_char, total as int32);
    core::ptr::copy_nonoverlapping(
        VARDATA_ANY(src),
        VARDATA(result as *const c_char),
        len,
    );
    result
}

/* ----------
 * to_ascii
 * ----------
 */
/*
 * # Safety
 * `src`..`src_end` is a readable byte range; `dest` is writable for the same
 * number of bytes.  `dest` may alias `src` (in-place conversion).
 */
unsafe fn pg_to_ascii(
    src: *mut c_uchar,
    src_end: *mut c_uchar,
    dest: *mut c_uchar,
    enc: c_int,
) {
    let mut x: *mut c_uchar;
    let mut dest = dest;
    let ascii: &[u8];
    let range: c_int;

    /*
     * relevant start for an encoding
     */
    const RANGE_128: c_int = 128;
    const RANGE_160: c_int = 160;

    if enc == PG_LATIN1 as c_int {
        /*
         * ISO-8859-1 <range: 160 -- 255>
         */
        ascii = b"  cL Y  \"Ca  -R     'u .,      ?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
        range = RANGE_160;
    } else if enc == PG_LATIN2 as c_int {
        /*
         * ISO-8859-2 <range: 160 -- 255>
         */
        ascii = b" A L LS \"SSTZ-ZZ a,l'ls ,sstz\"zzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt.";
        range = RANGE_160;
    } else if enc == PG_LATIN9 as c_int {
        /*
         * ISO-8859-15 <range: 160 -- 255>
         */
        ascii = b"  cL YS sCa  -R     Zu .z   EeY?AAAAAAACEEEEIIII NOOOOOxOUUUUYTBaaaaaaaceeeeiiii nooooo/ouuuuyty";
        range = RANGE_160;
    } else if enc == PG_WIN1250 as c_int {
        /*
         * Window CP1250 <range: 128 -- 255>
         */
        ascii = b"  ' \"    %S<STZZ `'\"\".--  s>stzz   L A  \"CS  -RZ  ,l'u .,as L\"lzRAAAALCCCEEEEIIDDNNOOOOxRUUUUYTBraaaalccceeeeiiddnnoooo/ruuuuyt ";
        range = RANGE_128;
    } else {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!(
                "encoding conversion from {} to ASCII not supported",
                cstr(pg_encoding_to_char(enc))
            )
        );
        return; /* keep compiler quiet */
    }

    /*
     * Encode
     */
    x = src;
    while x < src_end {
        let c = *x as c_int;
        if c < 128 {
            *dest = *x;
            dest = dest.add(1);
        } else if c < range {
            *dest = b' '; /* bogus 128 to 'range' */
            dest = dest.add(1);
        } else {
            *dest = ascii[(c - range) as usize];
            dest = dest.add(1);
        }
        x = x.add(1);
    }
}

/* ----------
 * encode text
 *
 * The text datum is overwritten in-place, therefore this coding method
 * cannot support conversions that change the string length!
 * ----------
 */
/*
 * # Safety
 * `data` is a fully-detoasted (4-byte header) text value, writable in place.
 */
unsafe fn encode_to_ascii(data: *mut text, enc: c_int) -> *mut text {
    pg_to_ascii(
        VARDATA(data as *const c_char) as *mut c_uchar, /* src */
        (data as *mut c_uchar).add(VARSIZE(data as *const c_char) as usize), /* src end */
        VARDATA(data as *const c_char) as *mut c_uchar, /* dest */
        enc,
    ); /* encoding */

    data
}

/* ----------
 * convert to ASCII - enc is set as 'name' arg.
 * ----------
 */
pub unsafe fn to_ascii_encname(fcinfo: FunctionCallInfo) -> Datum {
    let data: *mut text = pg_getarg_text_p_copy(PG_GETARG_DATUM!(fcinfo, 0));
    let encname: *const c_char = NameStr(&*PG_GETARG_NAME!(fcinfo, 1));
    let enc: c_int = pg_char_to_encoding(encname);

    if enc < 0 {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("{} is not a valid encoding name", cstr(encname))
        );
    }

    return PointerGetDatum(encode_to_ascii(data, enc) as *const c_void); /* PG_RETURN_TEXT_P */
}

/* ----------
 * convert to ASCII - enc is set as int4
 * ----------
 */
pub unsafe fn to_ascii_enc(fcinfo: FunctionCallInfo) -> Datum {
    let data: *mut text = pg_getarg_text_p_copy(PG_GETARG_DATUM!(fcinfo, 0));
    let enc: c_int = PG_GETARG_INT32!(fcinfo, 1);

    if !PG_VALID_ENCODING(enc) {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(ERROR, errmsg!("{} is not a valid encoding code", enc));
    }

    return PointerGetDatum(encode_to_ascii(data, enc) as *const c_void); /* PG_RETURN_TEXT_P */
}

/* ----------
 * convert to ASCII - current enc is DatabaseEncoding
 * ----------
 */
pub unsafe fn to_ascii_default(fcinfo: FunctionCallInfo) -> Datum {
    let data: *mut text = pg_getarg_text_p_copy(PG_GETARG_DATUM!(fcinfo, 0));
    let enc: c_int = GetDatabaseEncoding();

    return PointerGetDatum(encode_to_ascii(data, enc) as *const c_void); /* PG_RETURN_TEXT_P */
}

/* ----------
 * Copy a string in an arbitrary backend-safe encoding, converting it to a
 * valid ASCII string by replacing non-ASCII bytes with '?'.  Otherwise the
 * behavior is identical to strlcpy(), except that we don't bother with a
 * return value.
 *
 * This must not trigger ereport(ERROR), as it is called in postmaster.
 * ----------
 */
/*
 * # Safety
 * `src` is a NUL-terminated C string; `dest` is writable for `destsiz` bytes.
 */
pub unsafe fn ascii_safe_strlcpy(dest: *mut c_char, src: *const c_char, destsiz: usize) {
    let mut dest = dest;
    let mut src = src;
    let mut destsiz = destsiz;

    if destsiz == 0 {
        /* corner case: no room for trailing nul */
        return;
    }

    destsiz -= 1;
    while destsiz > 0 {
        /* use unsigned char here to avoid compiler warning */
        let ch = *src as c_uchar;
        src = src.add(1);

        if ch == b'\0' {
            break;
        }
        /* Keep printable ASCII characters */
        if (32..=127).contains(&ch) {
            *dest = ch as c_char;
        }
        /* White-space is also OK */
        else if ch == b'\n' || ch == b'\r' || ch == b'\t' {
            *dest = ch as c_char;
        }
        /* Everything else is replaced with '?' */
        else {
            *dest = b'?' as c_char;
        }
        dest = dest.add(1);
        destsiz -= 1;
    }

    *dest = b'\0' as c_char;
}

/********************************************************************
 *
 * ascii
 *
 * (Physically defined in oracle_compat.c; translated here with the rest of the
 * ascii family.)
 *
 * Syntax:
 *
 *	 int ascii(text string)
 *
 * Purpose:
 *
 *	 Returns the decimal representation of the first character from
 *	 string.
 *	 If the string is empty we return 0.
 *	 If the database encoding is UTF8, we return the Unicode codepoint.
 *	 If the database encoding is any other multi-byte encoding, we
 *	 return the value of the first byte if it is an ASCII character
 *	 (range 1 .. 127), or raise an error.
 *	 For all other encodings we return the value of the first byte,
 *	 (range 1..255).
 *
 ********************************************************************/
pub unsafe fn ascii(fcinfo: FunctionCallInfo) -> Datum {
    /* PG_GETARG_TEXT_PP(0): pg_detoast_datum_packed is identity for plain datums. */
    let string: *mut text =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void)
            as *mut text;
    let encoding: c_int = GetDatabaseEncoding();
    let data: *mut c_uchar;

    if (VARSIZE_ANY_EXHDR(string as *const c_char) as i32) <= 0 {
        PG_RETURN_INT32!(0);
    }

    data = VARDATA_ANY(string as *const c_char) as *mut c_uchar;

    if encoding == PG_UTF8 as c_int && *data > 127 {
        /* return the code point for Unicode */

        let mut result: c_int;
        let tbytes: c_int;
        let mut i: c_int;

        if *data >= 0xF0 {
            result = (*data & 0x07) as c_int;
            tbytes = 3;
        } else if *data >= 0xE0 {
            result = (*data & 0x0F) as c_int;
            tbytes = 2;
        } else {
            Assert!(*data > 0xC0);
            result = (*data & 0x1f) as c_int;
            tbytes = 1;
        }

        Assert!(tbytes > 0);

        i = 1;
        while i <= tbytes {
            Assert!((*data.add(i as usize) & 0xC0) == 0x80);
            result = (result << 6) + (*data.add(i as usize) & 0x3f) as c_int;
            i += 1;
        }

        PG_RETURN_INT32!(result);
    } else {
        if pg_encoding_max_length(encoding) > 1 && *data > 127 {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(ERROR, errmsg!("requested character too large"));
        }

        PG_RETURN_INT32!(*data as int32);
    }
}

/********************************************************************
 *
 * chr
 *
 * (Physically defined in oracle_compat.c; translated here with the rest of the
 * ascii family.)
 *
 * Syntax:
 *
 *	 text chr(int val)
 *
 * Purpose:
 *
 *	Returns the character having the binary equivalent to val.
 *
 * For UTF8 we treat the argument as a Unicode code point.
 * For other multi-byte encodings we raise an error for arguments
 * outside the strict ASCII range (1..127).
 *
 * It's important that we don't ever return a value that is not valid
 * in the database encoding, so that this doesn't become a way for
 * invalid data to enter the database.
 *
 ********************************************************************/
pub unsafe fn chr(fcinfo: FunctionCallInfo) -> Datum {
    let arg: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let cvalue: uint32;
    let result: *mut text;
    let encoding: c_int = GetDatabaseEncoding();

    /*
     * Error out on arguments that make no sense or that we can't validly
     * represent in the encoding.
     */
    if arg < 0 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("character number must be positive"));
    } else if arg == 0 {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(ERROR, errmsg!("null character not permitted"));
    }

    cvalue = arg as uint32;

    if encoding == PG_UTF8 as c_int && cvalue > 127 {
        /* for Unicode we treat the argument as a code point */
        let bytes: c_int;
        let wch: *mut c_uchar;

        /*
         * We only allow valid Unicode code points; per RFC3629 that stops at
         * U+10FFFF, even though 4-byte UTF8 sequences can hold values up to
         * U+1FFFFF.
         */
        if cvalue > 0x0010ffff {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!("requested character too large for encoding: {}", cvalue)
            );
        }

        if cvalue > 0xffff {
            bytes = 4;
        } else if cvalue > 0x07ff {
            bytes = 3;
        } else {
            bytes = 2;
        }

        result = palloc((VARHDRSZ + bytes) as Size) as *mut text;
        SET_VARSIZE(result as *mut c_char, VARHDRSZ + bytes);
        wch = VARDATA(result as *const c_char) as *mut c_uchar;

        if bytes == 2 {
            *wch.add(0) = 0xC0 | ((cvalue >> 6) & 0x1F) as c_uchar;
            *wch.add(1) = 0x80 | (cvalue & 0x3F) as c_uchar;
        } else if bytes == 3 {
            *wch.add(0) = 0xE0 | ((cvalue >> 12) & 0x0F) as c_uchar;
            *wch.add(1) = 0x80 | ((cvalue >> 6) & 0x3F) as c_uchar;
            *wch.add(2) = 0x80 | (cvalue & 0x3F) as c_uchar;
        } else {
            *wch.add(0) = 0xF0 | ((cvalue >> 18) & 0x07) as c_uchar;
            *wch.add(1) = 0x80 | ((cvalue >> 12) & 0x3F) as c_uchar;
            *wch.add(2) = 0x80 | ((cvalue >> 6) & 0x3F) as c_uchar;
            *wch.add(3) = 0x80 | (cvalue & 0x3F) as c_uchar;
        }

        /*
         * The preceding range check isn't sufficient, because UTF8 excludes
         * Unicode "surrogate pair" codes.  Make sure what we created is valid
         * UTF8.
         */
        if !pg_utf8_islegal(wch, bytes) {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!("requested character not valid for encoding: {}", cvalue)
            );
        }
    } else {
        let is_mb: bool;

        is_mb = pg_encoding_max_length(encoding) > 1;

        if (is_mb && (cvalue > 127)) || (!is_mb && (cvalue > 255)) {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!("requested character too large for encoding: {}", cvalue)
            );
        }

        result = palloc((VARHDRSZ + 1) as Size) as *mut text;
        SET_VARSIZE(result as *mut c_char, VARHDRSZ + 1);
        *(VARDATA(result as *const c_char)) = cvalue as c_char;
    }

    return PointerGetDatum(result as *const c_void); /* PG_RETURN_TEXT_P */
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
    use crate::postgres::{DatumGetInt32, Int32GetDatum};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::adt::varlena::{
        cstring_to_text, cstring_to_text_with_len, text_to_cstring,
    };
    use crate::utils::fmgr::DirectFunctionCall1Coll;

    unsafe fn cstr_eq(p: *const c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    /* ascii(text) on the single-byte/ASCII path: first byte's codepoint. */
    #[test]
    fn ascii_first_char() {
        unsafe {
            // 'x' == 120
            let t = cstring_to_text(c"xyz".as_ptr());
            let d = DirectFunctionCall1Coll(ascii, InvalidOid, PointerGetDatum(t as *const c_void));
            assert_eq!(DatumGetInt32(d), 120);

            // empty string -> 0
            let e = cstring_to_text(c"".as_ptr());
            let d0 = DirectFunctionCall1Coll(ascii, InvalidOid, PointerGetDatum(e as *const c_void));
            assert_eq!(DatumGetInt32(d0), 0);
        }
    }

    /* UTF8 codepoint decode in ascii() via a hand-built two-byte sequence. */
    #[test]
    fn ascii_utf8_codepoint() {
        unsafe {
            // U+00E9 LATIN SMALL LETTER E WITH ACUTE -> bytes C3 A9
            let bytes = [0xC3u8, 0xA9u8];
            let t = cstring_to_text_bytes(&bytes);
            let d = DirectFunctionCall1Coll(ascii, InvalidOid, PointerGetDatum(t as *const c_void));
            assert_eq!(DatumGetInt32(d), 0x00E9);
        }
    }

    /* chr(int4) round-trips through ascii() for both ASCII and UTF8 ranges. */
    #[test]
    fn chr_roundtrip() {
        unsafe {
            // ASCII: chr(65) == "A"
            let a = DirectFunctionCall1Coll(chr, InvalidOid, Int32GetDatum(65));
            let s = text_to_cstring(DatumGetPointer(a) as *const text);
            assert!(cstr_eq(s, "A"));

            // UTF8 codepoint U+00E9 -> 2-byte sequence; ascii() decodes it back.
            let c = DirectFunctionCall1Coll(chr, InvalidOid, Int32GetDatum(0x00E9));
            let back = DirectFunctionCall1Coll(ascii, InvalidOid, c);
            assert_eq!(DatumGetInt32(back), 0x00E9);

            // UTF8 codepoint U+1F600 (emoji) -> 4-byte sequence; round-trips.
            let g = DirectFunctionCall1Coll(chr, InvalidOid, Int32GetDatum(0x1F600));
            let backg = DirectFunctionCall1Coll(ascii, InvalidOid, g);
            assert_eq!(DatumGetInt32(backg), 0x1F600);
        }
    }

    /* chr(0) is rejected ("null character not permitted"). */
    #[test]
    #[should_panic]
    fn chr_rejects_zero() {
        unsafe {
            DirectFunctionCall1Coll(chr, InvalidOid, Int32GetDatum(0));
        }
    }

    /* chr() rejects code points beyond U+10FFFF for UTF8. */
    #[test]
    #[should_panic]
    fn chr_rejects_too_large() {
        unsafe {
            DirectFunctionCall1Coll(chr, InvalidOid, Int32GetDatum(0x110000));
        }
    }

    /* pg_to_ascii folds Latin-1 high bytes to their 7-bit lookalikes in place. */
    #[test]
    fn pg_to_ascii_latin1_fold() {
        unsafe {
            // 0xC0 (A grave) -> 'A', 0xE9 (e acute) -> 'e', plus a plain ASCII byte.
            let mut buf = [b'X' as c_uchar, 0xC0u8, 0xE9u8, b'!' as c_uchar];
            let p = buf.as_mut_ptr();
            pg_to_ascii(p, p.add(buf.len()), p, PG_LATIN1 as c_int);
            assert_eq!(&buf, &[b'X', b'A', b'e', b'!']);
        }
    }

    /* ascii_safe_strlcpy keeps printable ASCII, maps others to '?', NUL-terminates. */
    #[test]
    fn safe_strlcpy_replaces() {
        unsafe {
            let src = [b'a' as c_char, 0x01, b'b' as c_char, b'\n' as c_char, 0];
            let mut dst = [0i8; 8];
            ascii_safe_strlcpy(dst.as_mut_ptr(), src.as_ptr(), dst.len());
            // 0x01 -> '?', '\n' kept
            assert!(cstr_eq(dst.as_ptr(), "a?b\n"));

            // destsiz truncation: only 2 chars + NUL fit
            let mut dst2 = [0i8; 3];
            ascii_safe_strlcpy(dst2.as_mut_ptr(), c"hello".as_ptr(), dst2.len());
            assert!(cstr_eq(dst2.as_ptr(), "he"));
        }
    }

    // Test-only helper: build a text from a raw byte slice (no NUL needed).
    unsafe fn cstring_to_text_bytes(b: &[u8]) -> *mut text {
        cstring_to_text_with_len(b.as_ptr() as *const c_char, b.len() as c_int)
    }
}
