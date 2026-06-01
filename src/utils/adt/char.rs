//! Translation of postgres/src/backend/utils/adt/char.c
//!
//! Functions for the built-in type "char" (not to be confused with bpchar,
//! which is the SQL CHAR(n) type).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does:
//!   #include "postgres.h"
//!   #include <limits.h>
//!   #include "libpq/pqformat.h"
//!   #include "utils/fmgrprotos.h"
//!   #include "varatt.h"
//!
//! `postgres.h` -> crate::prelude.  `<limits.h>`'s SCHAR_MIN/SCHAR_MAX are the C
//! `signed char` bounds; here the SQL "char" type is a 1-byte int8, so we use
//! `i8::MIN`/`i8::MAX` (-128..127) as the exact analogues.  The
//! `libpq/pqformat.h` StringInfo serializers (pq_getmsgbyte / pq_begintypsend /
//! pq_sendbyte / pq_endtypsend) are NOT yet translated, so charrecv/charsend are
//! stubbed (mirroring bool.rs boolrecv/boolsend).  `varatt.h`'s
//! VARDATA_ANY/VARSIZE_ANY_EXHDR/VARDATA/SET_VARSIZE plus the `text` varlena
//! constructors live in utils/adt/varlena.c (not yet translated), so text_char /
//! char_text are stubbed (mirroring bool.rs booltext).

use crate::prelude::*; // Datum, palloc, ereport!/errmsg!, c_char/c_int, etc.
use crate::utils::fmgr::*; // FunctionCallInfo (and the rest of the fmgr.h interface)
// The PG_GETARG_*!/PG_RETURN_*! helpers are #[macro_export] macro_rules! in
// utils/fmgr.rs, so they live at the crate root and must be imported by name
// (a glob `use crate::utils::fmgr::*` does NOT bring exported macros into scope).
use crate::{
    PG_GETARG_CHAR, PG_GETARG_CSTRING, PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_POINTER,
    PG_RETURN_BOOL, PG_RETURN_CHAR, PG_RETURN_CSTRING, PG_RETURN_INT32,
};
use crate::lib::stringinfo::StringInfo; // libpq/pqformat.h passes a StringInfo
use crate::c::text; // char_text/text_char build/read a text varlena
use crate::varatt::{pg_detoast_datum_packed, SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::postgres::{DatumGetPointer, PointerGetDatum};
use core::ffi::{c_char, c_int, c_void};

/* errcodes.h classification (errcode() shim ignores the value) */
// TODO(pg-port): ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE from utils/errcodes.h.
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;

/*
 * The C uses <limits.h> SCHAR_MIN / SCHAR_MAX (the bounds of `signed char`).
 * The SQL "char" type stores one signed byte (int8), so these are i8's range.
 */
const SCHAR_MIN: int32 = i8::MIN as int32; // -128
const SCHAR_MAX: int32 = i8::MAX as int32; //  127

/*
 * C: #define ISOCTAL(c)   (((c) >= '0') && ((c) <= '7'))
 *    #define TOOCTAL(c)   ((c) + '0')
 *    #define FROMOCTAL(c) ((unsigned char) (c) - '0')
 *
 * The macros operate on `char` values.  ISOCTAL takes a `c_char` and tests the
 * '0'..'7' range; TOOCTAL maps a 0..7 digit value to its ASCII char; FROMOCTAL
 * maps an ASCII octal digit char back to its 0..7 value (via unsigned char).
 */
#[inline]
fn ISOCTAL(c: c_char) -> bool {
    (c >= b'0' as c_char) && (c <= b'7' as c_char)
}
#[inline]
fn TOOCTAL(c: u8) -> c_char {
    (c + b'0') as c_char
}
#[inline]
fn FROMOCTAL(c: c_char) -> u8 {
    (c as u8).wrapping_sub(b'0')
}

/*
 * Private strlen for the `*const c_char` C strings handled here (C uses libc's
 * strlen via string.h, included by postgres.h).  Counts bytes up to the NUL.
 *
 * # Safety
 * `s` must point to a valid NUL-terminated C string.
 */
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/*
 *		charin			- converts "x" to 'x'
 *
 * This accepts the formats charout produces.  If we have multibyte input
 * that is not in the form '\ooo', then we take its first byte as the value
 * and silently discard the rest; this is a backwards-compatibility provision.
 */
pub unsafe fn charin(fcinfo: FunctionCallInfo) -> Datum {
    let ch: *const c_char = PG_GETARG_CSTRING!(fcinfo, 0);

    if strlen(ch) == 4
        && *ch.add(0) == b'\\' as c_char
        && ISOCTAL(*ch.add(1))
        && ISOCTAL(*ch.add(2))
        && ISOCTAL(*ch.add(3))
    {
        // C computes this in `int` (integer promotion of the FROMOCTAL bytes),
        // then narrows to `char`.  We widen to c_int so the shifts/adds cannot
        // overflow (e.g. "\777" => 511), matching C's truncation on the cast.
        PG_RETURN_CHAR!((((FROMOCTAL(*ch.add(1)) as c_int) << 6)
            + ((FROMOCTAL(*ch.add(2)) as c_int) << 3)
            + (FROMOCTAL(*ch.add(3)) as c_int)) as c_char);
    }
    /* This will do the right thing for a zero-length input string */
    PG_RETURN_CHAR!(*ch.add(0));
}

/*
 *		charout			- converts 'x' to "x"
 *
 * The possible output formats are:
 * 1. 0x00 is represented as an empty string.
 * 2. 0x01..0x7F are represented as a single ASCII byte.
 * 3. 0x80..0xFF are represented as \ooo (backslash and 3 octal digits).
 * Case 3 is meant to match the traditional "escape" format of bytea.
 */
pub unsafe fn charout(fcinfo: FunctionCallInfo) -> Datum {
    let ch: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let result: *mut c_char = palloc(5) as *mut c_char;

    if IS_HIGHBIT_SET(ch as u8) {
        *result.add(0) = b'\\' as c_char;
        *result.add(1) = TOOCTAL((ch as u8) >> 6);
        *result.add(2) = TOOCTAL(((ch as u8) >> 3) & 0o7);
        *result.add(3) = TOOCTAL((ch as u8) & 0o7);
        *result.add(4) = b'\0' as c_char;
    } else {
        /* This produces acceptable results for 0x00 as well */
        *result.add(0) = ch;
        *result.add(1) = b'\0' as c_char;
    }
    PG_RETURN_CSTRING!(result);
}

/*
 *		charrecv			- converts external binary format to char
 *
 * The external representation is one byte, with no character set
 * conversion.  This is somewhat dubious, perhaps, but in many
 * cases people use char for a 1-byte binary type.
 */
pub unsafe fn charrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    // C body:
    //   PG_RETURN_CHAR(pq_getmsgbyte(buf));
    // TODO(pg-port): libpq pqformat (pq_getmsgbyte) not yet translated.
    let _ = buf;
    unimplemented!("charrecv: libpq/pqformat (pq_getmsgbyte) not yet translated")
}

/*
 *		charsend			- converts char to binary format
 */
pub unsafe fn charsend(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);

    // C body:
    //   StringInfoData buf;
    //   pq_begintypsend(&buf);
    //   pq_sendbyte(&buf, arg1);
    //   PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
    // TODO(pg-port): libpq pqformat (pq_begintypsend / pq_sendbyte /
    // pq_endtypsend) not yet translated.
    let _ = arg1;
    unimplemented!("charsend: libpq/pqformat (pq_sendbyte) not yet translated")
}

/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/

/*
 * NOTE: comparisons are done as though char is unsigned (uint8).
 * Conversions to and from integer are done as though char is signed (int8).
 *
 * You wanted consistency?
 */

pub unsafe fn chareq(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let arg2: c_char = PG_GETARG_CHAR!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 == arg2);
}

pub unsafe fn charne(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let arg2: c_char = PG_GETARG_CHAR!(fcinfo, 1);

    PG_RETURN_BOOL!(arg1 != arg2);
}

pub unsafe fn charlt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let arg2: c_char = PG_GETARG_CHAR!(fcinfo, 1);

    PG_RETURN_BOOL!((arg1 as uint8) < (arg2 as uint8));
}

pub unsafe fn charle(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let arg2: c_char = PG_GETARG_CHAR!(fcinfo, 1);

    PG_RETURN_BOOL!((arg1 as uint8) <= (arg2 as uint8));
}

pub unsafe fn chargt(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let arg2: c_char = PG_GETARG_CHAR!(fcinfo, 1);

    PG_RETURN_BOOL!((arg1 as uint8) > (arg2 as uint8));
}

pub unsafe fn charge(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let arg2: c_char = PG_GETARG_CHAR!(fcinfo, 1);

    PG_RETURN_BOOL!((arg1 as uint8) >= (arg2 as uint8));
}

pub unsafe fn chartoi4(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);

    // C: PG_RETURN_INT32((int32) ((int8) arg1));
    // Sign-extend the byte through int8 -> int32 (so 0x80..0xFF become negative).
    PG_RETURN_INT32!((arg1 as int8) as int32);
}

pub unsafe fn i4tochar(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: int32 = PG_GETARG_INT32!(fcinfo, 0);

    if arg1 < SCHAR_MIN || arg1 > SCHAR_MAX {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(ERROR, errmsg!("\"char\" out of range"));
    }

    // C: PG_RETURN_CHAR((int8) arg1);
    PG_RETURN_CHAR!((arg1 as int8) as c_char);
}

pub unsafe fn text_char(fcinfo: FunctionCallInfo) -> Datum {
    // PG_GETARG_TEXT_PP(0): detoast (identity for plain datums)
    let arg1: *mut text =
        pg_detoast_datum_packed(DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut c_void)
            as *mut text;
    let ch: *const c_char = VARDATA_ANY(arg1 as *const c_char);
    let exhdr: u32 = VARSIZE_ANY_EXHDR(arg1 as *const c_char);
    let result: c_char;

    /* Conversion rules are the same as in charin(), but here we need to handle
     * the empty-string case honestly. */
    if exhdr == 4
        && *ch.add(0) == b'\\' as c_char
        && ISOCTAL(*ch.add(1))
        && ISOCTAL(*ch.add(2))
        && ISOCTAL(*ch.add(3))
    {
        result = ((FROMOCTAL(*ch.add(1)) << 6) + (FROMOCTAL(*ch.add(2)) << 3) + FROMOCTAL(*ch.add(3)))
            as c_char;
    } else if exhdr > 0 {
        result = *ch.add(0);
    } else {
        result = b'\0' as c_char;
    }
    PG_RETURN_CHAR!(result);
}

pub unsafe fn char_text(fcinfo: FunctionCallInfo) -> Datum {
    let arg1: c_char = PG_GETARG_CHAR!(fcinfo, 0);
    let result: *mut text = palloc((VARHDRSZ + 4) as Size) as *mut text;
    let rp = result as *mut c_char;

    /* Conversion rules are the same as in charout(), but here we need to be
     * honest about converting 0x00 to an empty string. */
    if IS_HIGHBIT_SET(arg1 as u8) {
        SET_VARSIZE(rp, VARHDRSZ + 4);
        let d = VARDATA(rp as *const c_char);
        *d.add(0) = b'\\' as c_char;
        *d.add(1) = TOOCTAL((arg1 as u8) >> 6);
        *d.add(2) = TOOCTAL(((arg1 as u8) >> 3) & 0o7);
        *d.add(3) = TOOCTAL((arg1 as u8) & 0o7);
    } else if arg1 != b'\0' as c_char {
        SET_VARSIZE(rp, VARHDRSZ + 1);
        *VARDATA(rp as *const c_char) = arg1;
    } else {
        SET_VARSIZE(rp, VARHDRSZ);
    }
    return PointerGetDatum(result as *const c_void); // PG_RETURN_TEXT_P
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{CStringGetDatum, CharGetDatum, DatumGetBool, DatumGetChar, DatumGetCString, DatumGetInt32, Int32GetDatum};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::{DirectFunctionCall1Coll, DirectFunctionCall2Coll};

    // Drive each SQL function through the real fmgr call path so the
    // fcinfo-threaded PG_GETARG_*!/PG_RETURN_*! macros are exercised end-to-end.
    #[test]
    fn char_io_conversions_and_operators() {
        unsafe {
            // charin: single ASCII byte round-trips to its char value.
            let d = DirectFunctionCall1Coll(charin, InvalidOid, CStringGetDatum(c"A".as_ptr()));
            assert_eq!(DatumGetChar(d), b'A' as c_char);

            // charin: empty string -> NUL (0x00).
            let d = DirectFunctionCall1Coll(charin, InvalidOid, CStringGetDatum(c"".as_ptr()));
            assert_eq!(DatumGetChar(d), 0 as c_char);

            // charin: the '\ooo' high-bit escape form. \377 == 0xFF.
            let d = DirectFunctionCall1Coll(charin, InvalidOid, CStringGetDatum(c"\\377".as_ptr()));
            assert_eq!(DatumGetChar(d) as u8, 0xFFu8);
            // \200 == 0x80.
            let d = DirectFunctionCall1Coll(charin, InvalidOid, CStringGetDatum(c"\\200".as_ptr()));
            assert_eq!(DatumGetChar(d) as u8, 0x80u8);
            // Multibyte non-escape: take the first byte, discard the rest.
            let d = DirectFunctionCall1Coll(charin, InvalidOid, CStringGetDatum(c"xyz".as_ptr()));
            assert_eq!(DatumGetChar(d), b'x' as c_char);

            // charout: low ASCII -> single byte then NUL.
            let s = DatumGetCString(DirectFunctionCall1Coll(charout, InvalidOid, CharGetDatum(b'A' as c_char)));
            assert_eq!(*s.add(0) as u8, b'A');
            assert_eq!(*s.add(1) as u8, 0);
            // charout: 0x00 -> empty string.
            let s = DatumGetCString(DirectFunctionCall1Coll(charout, InvalidOid, CharGetDatum(0 as c_char)));
            assert_eq!(*s.add(0) as u8, 0);
            // charout: high-bit -> "\ooo".  0xFF -> "\377".
            let s = DatumGetCString(DirectFunctionCall1Coll(charout, InvalidOid, CharGetDatum(0xFFu8 as c_char)));
            assert_eq!(*s.add(0) as u8, b'\\');
            assert_eq!(*s.add(1) as u8, b'3');
            assert_eq!(*s.add(2) as u8, b'7');
            assert_eq!(*s.add(3) as u8, b'7');
            assert_eq!(*s.add(4) as u8, 0);

            // charin <-> charout round-trip across the whole byte range.
            for b in 0u16..=255 {
                let c = b as u8 as c_char;
                let out = DatumGetCString(DirectFunctionCall1Coll(charout, InvalidOid, CharGetDatum(c)));
                let back = DatumGetChar(DirectFunctionCall1Coll(charin, InvalidOid, CStringGetDatum(out)));
                assert_eq!(back, c, "round-trip failed for byte {:#x}", b);
            }

            // Comparison operators are UNSIGNED: 0xFF (255) > 'A' (65).
            let lt = |a: u8, b: u8| DatumGetBool(DirectFunctionCall2Coll(charlt, InvalidOid, CharGetDatum(a as c_char), CharGetDatum(b as c_char)));
            let gt = |a: u8, b: u8| DatumGetBool(DirectFunctionCall2Coll(chargt, InvalidOid, CharGetDatum(a as c_char), CharGetDatum(b as c_char)));
            let le = |a: u8, b: u8| DatumGetBool(DirectFunctionCall2Coll(charle, InvalidOid, CharGetDatum(a as c_char), CharGetDatum(b as c_char)));
            let ge = |a: u8, b: u8| DatumGetBool(DirectFunctionCall2Coll(charge, InvalidOid, CharGetDatum(a as c_char), CharGetDatum(b as c_char)));
            let eq = |a: u8, b: u8| DatumGetBool(DirectFunctionCall2Coll(chareq, InvalidOid, CharGetDatum(a as c_char), CharGetDatum(b as c_char)));
            let ne = |a: u8, b: u8| DatumGetBool(DirectFunctionCall2Coll(charne, InvalidOid, CharGetDatum(a as c_char), CharGetDatum(b as c_char)));
            assert!(gt(0xFF, b'A') && !lt(0xFF, b'A')); // unsigned: 255 > 65
            assert!(lt(b'A', b'B') && le(b'A', b'A') && ge(b'B', b'A'));
            assert!(eq(0x80, 0x80) && ne(0x80, 0x7F));

            // chartoi4: SIGNED conversion. 0xFF -> -1, 0x80 -> -128, 0x7F -> 127.
            let toi4 = |b: u8| DatumGetInt32(DirectFunctionCall1Coll(chartoi4, InvalidOid, CharGetDatum(b as c_char)));
            assert_eq!(toi4(0xFF), -1);
            assert_eq!(toi4(0x80), -128);
            assert_eq!(toi4(0x7F), 127);
            assert_eq!(toi4(0x00), 0);

            // i4tochar: in-range values round-trip; SIGNED so -1 -> 0xFF.
            let toc = |i: i32| DatumGetChar(DirectFunctionCall1Coll(i4tochar, InvalidOid, Int32GetDatum(i)));
            assert_eq!(toc(-1) as u8, 0xFF);
            assert_eq!(toc(-128) as u8, 0x80);
            assert_eq!(toc(127) as u8, 0x7F);
            assert_eq!(toc(65) as u8, b'A');
        }
    }

    // i4tochar raises an out-of-range error (ereport!(ERROR) panics under the
    // elog shim) for values outside SCHAR_MIN..SCHAR_MAX.
    #[test]
    #[should_panic]
    fn i4tochar_out_of_range() {
        unsafe {
            let _ = DirectFunctionCall1Coll(i4tochar, InvalidOid, Int32GetDatum(128));
        }
    }

    // char_text <-> text_char cascade (now that varatt + varlena exist).
    #[test]
    fn char_text_roundtrip() {
        unsafe {
            // ASCII 'A' -> text "A" -> 'A'
            let t = DirectFunctionCall1Coll(char_text, InvalidOid, CharGetDatum(b'A' as c_char));
            assert_eq!(DatumGetChar(DirectFunctionCall1Coll(text_char, InvalidOid, t)), b'A' as c_char);

            // 0x00 -> empty text -> 0x00
            let t0 = DirectFunctionCall1Coll(char_text, InvalidOid, CharGetDatum(0 as c_char));
            assert_eq!(DatumGetChar(DirectFunctionCall1Coll(text_char, InvalidOid, t0)), 0 as c_char);

            // high-bit 0xFF -> text "\377" (4 chars) -> 0xFF
            let th = DirectFunctionCall1Coll(char_text, InvalidOid, CharGetDatum(0xFFu8 as c_char));
            let s = DatumGetCString(DirectFunctionCall1Coll(
                crate::utils::adt::varlena::textout,
                InvalidOid,
                th,
            ));
            assert_eq!(
                core::slice::from_raw_parts(s as *const u8, 4),
                b"\\377"
            );
            assert_eq!(DatumGetChar(DirectFunctionCall1Coll(text_char, InvalidOid, th)) as u8, 0xFFu8);
        }
    }
}
