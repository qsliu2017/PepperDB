/*-------------------------------------------------------------------------
 *
 *    GB18030 <--> UTF8
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::mb::conv::{LocalToUtf, UtfToLocal};
use crate::mb::pg_wchar::check_encoding_conversion_args;
use crate::mb::wchar::{pg_enc, pg_mb_radix_tree};
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};
use crate::c::uint32;
use core::ptr::null;

// TODO(pg-port): generated radix map from src/backend/utils/mb/Unicode/gb18030_to_utf8.map - tables not transcribed
const GB18030_TO_UNICODE_TREE: pg_mb_radix_tree = pg_mb_radix_tree {
    chars16: null(),
    chars32: null(),
    b1root: 0,
    b1_lower: 0,
    b1_upper: 0,
    b2root: 0,
    b2_1_lower: 0,
    b2_1_upper: 0,
    b2_2_lower: 0,
    b2_2_upper: 0,
    b3root: 0,
    b3_1_lower: 0,
    b3_1_upper: 0,
    b3_2_lower: 0,
    b3_2_upper: 0,
    b3_3_lower: 0,
    b3_3_upper: 0,
    b4root: 0,
    b4_1_lower: 0,
    b4_1_upper: 0,
    b4_2_lower: 0,
    b4_2_upper: 0,
    b4_3_lower: 0,
    b4_3_upper: 0,
    b4_4_lower: 0,
    b4_4_upper: 0,
};

// TODO(pg-port): generated radix map from src/backend/utils/mb/Unicode/utf8_to_gb18030.map - tables not transcribed
const GB18030_FROM_UNICODE_TREE: pg_mb_radix_tree = pg_mb_radix_tree {
    chars16: null(),
    chars32: null(),
    b1root: 0,
    b1_lower: 0,
    b1_upper: 0,
    b2root: 0,
    b2_1_lower: 0,
    b2_1_upper: 0,
    b2_2_lower: 0,
    b2_2_upper: 0,
    b3root: 0,
    b3_1_lower: 0,
    b3_1_upper: 0,
    b3_2_lower: 0,
    b3_2_upper: 0,
    b3_3_lower: 0,
    b3_3_upper: 0,
    b4root: 0,
    b4_1_lower: 0,
    b4_1_upper: 0,
    b4_2_lower: 0,
    b4_2_upper: 0,
    b4_3_lower: 0,
    b4_3_upper: 0,
    b4_4_lower: 0,
    b4_4_upper: 0,
};

/*
 * Convert 4-byte GB18030 characters to and from a linear code space
 *
 * The first and third bytes can range from 0x81 to 0xfe (126 values),
 * while the second and fourth bytes can range from 0x30 to 0x39 (10 values).
 */
#[inline]
fn gb_linear(gb: uint32) -> uint32 {
    let b0 = (gb & 0xff000000) >> 24;
    let b1 = (gb & 0x00ff0000) >> 16;
    let b2 = (gb & 0x0000ff00) >> 8;
    let b3 =  gb & 0x000000ff;

    b0.wrapping_mul(12600)
        .wrapping_add(b1.wrapping_mul(1260))
        .wrapping_add(b2.wrapping_mul(10))
        .wrapping_add(b3)
        .wrapping_sub(0x81u32.wrapping_mul(12600)
            .wrapping_add(0x30u32.wrapping_mul(1260))
            .wrapping_add(0x81u32.wrapping_mul(10))
            .wrapping_add(0x30))
}

#[inline]
fn gb_unlinear(lin: uint32) -> uint32 {
    let r0 = 0x81 + lin / 12600;
    let r1 = 0x30 + (lin / 1260) % 10;
    let r2 = 0x81 + (lin / 10) % 126;
    let r3 = 0x30 + lin % 10;

    (r0 << 24) | (r1 << 16) | (r2 << 8) | r3
}

/*
 * Convert word-formatted UTF8 to and from Unicode code points
 */
#[inline]
fn unicode_to_utf8word(c: uint32) -> uint32 {
    if c <= 0x7F {
        c
    } else if c <= 0x7FF {
        let word = (0xC0 | ((c >> 6) & 0x1F)) << 8;
        word | (0x80 | (c & 0x3F))
    } else if c <= 0xFFFF {
        let word = (0xE0 | ((c >> 12) & 0x0F)) << 16;
        let word = word | ((0x80 | ((c >> 6) & 0x3F)) << 8);
        word | (0x80 | (c & 0x3F))
    } else {
        let word = (0xF0 | ((c >> 18) & 0x07)) << 24;
        let word = word | ((0x80 | ((c >> 12) & 0x3F)) << 16);
        let word = word | ((0x80 | ((c >> 6) & 0x3F)) << 8);
        word | (0x80 | (c & 0x3F))
    }
}

#[inline]
fn utf8word_to_unicode(c: uint32) -> uint32 {
    if c <= 0x7F {
        c
    } else if c <= 0xFFFF {
        let ucs = ((c >> 8) & 0x1F) << 6;
        ucs | (c & 0x3F)
    } else if c <= 0xFFFFFF {
        let ucs = ((c >> 16) & 0x0F) << 12;
        let ucs = ucs | (((c >> 8) & 0x3F) << 6);
        ucs | (c & 0x3F)
    } else {
        let ucs = ((c >> 24) & 0x07) << 18;
        let ucs = ucs | (((c >> 16) & 0x3F) << 12);
        let ucs = ucs | (((c >> 8) & 0x3F) << 6);
        ucs | (c & 0x3F)
    }
}

/*
 * Perform mapping of GB18030 ranges to UTF8
 *
 * The ranges we need to convert are specified in gb-18030-2000.xml.
 * All are ranges of 4-byte GB18030 codes.
 */
unsafe extern "C" fn conv_18030_to_utf8(code: uint32) -> uint32 {
    macro_rules! conv18030 {
        ($minunicode:expr, $mincode:expr, $maxcode:expr) => {
            if code >= $mincode && code <= $maxcode {
                return unicode_to_utf8word(gb_linear(code) - gb_linear($mincode) + $minunicode);
            }
        };
    }

    conv18030!(0x0452, 0x8130D330, 0x8136A531);
    conv18030!(0x2643, 0x8137A839, 0x8138FD38);
    conv18030!(0x361B, 0x8230A633, 0x8230F237);
    conv18030!(0x3CE1, 0x8231D438, 0x8232AF32);
    conv18030!(0x4160, 0x8232C937, 0x8232F837);
    conv18030!(0x44D7, 0x8233A339, 0x8233C931);
    conv18030!(0x478E, 0x8233E838, 0x82349638);
    conv18030!(0x49B8, 0x8234A131, 0x8234E733);
    conv18030!(0x9FA6, 0x82358F33, 0x8336C738);
    conv18030!(0xE865, 0x8336D030, 0x84308534);
    conv18030!(0xFA2A, 0x84309C38, 0x84318537);
    conv18030!(0xFFE6, 0x8431A234, 0x8431A439);
    conv18030!(0x10000, 0x90308130, 0xE3329A35);
    /* No mapping exists */
    0
}

/*
 * Perform mapping of UTF8 ranges to GB18030
 */
unsafe extern "C" fn conv_utf8_to_18030(code: uint32) -> uint32 {
    let ucs = utf8word_to_unicode(code);

    macro_rules! convutf8 {
        ($minunicode:expr, $maxunicode:expr, $mincode:expr) => {
            if ucs >= $minunicode && ucs <= $maxunicode {
                return gb_unlinear(ucs - $minunicode + gb_linear($mincode));
            }
        };
    }

    convutf8!(0x0452, 0x200F, 0x8130D330);
    convutf8!(0x2643, 0x2E80, 0x8137A839);
    convutf8!(0x361B, 0x3917, 0x8230A633);
    convutf8!(0x3CE1, 0x4055, 0x8231D438);
    convutf8!(0x4160, 0x4336, 0x8232C937);
    convutf8!(0x44D7, 0x464B, 0x8233A339);
    convutf8!(0x478E, 0x4946, 0x8233E838);
    convutf8!(0x49B8, 0x4C76, 0x8234A131);
    convutf8!(0x9FA6, 0xD7FF, 0x82358F33);
    convutf8!(0xE865, 0xF92B, 0x8336D030);
    convutf8!(0xFA2A, 0xFE2F, 0x84309C38);
    convutf8!(0xFFE6, 0xFFFF, 0x8431A234);
    convutf8!(0x10000, 0x10FFFF, 0x90308130);
    /* No mapping exists */
    0
}

/* ----------
 * conv_proc(
 *      INTEGER,    -- source encoding id
 *      INTEGER,    -- destination encoding id
 *      CSTRING,    -- source string (null terminated C string)
 *      CSTRING,    -- destination string (null terminated C string)
 *      INTEGER,    -- source string length
 *      BOOL        -- if true, don't throw an error if conversion fails
 * ) returns INTEGER;
 *
 * Returns the number of bytes successfully converted.
 * ----------
 */
pub unsafe fn gb18030_to_utf8(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_GB18030 as c_int,
        pg_enc::PG_UTF8 as c_int,
    );

    converted = LocalToUtf(
        src,
        len,
        dest,
        &GB18030_TO_UNICODE_TREE,
        null(),
        0,
        Some(conv_18030_to_utf8),
        pg_enc::PG_GB18030 as c_int,
        no_error,
    );

    PG_RETURN_INT32!(converted);
}

pub unsafe fn utf8_to_gb18030(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_UTF8 as c_int,
        pg_enc::PG_GB18030 as c_int,
    );

    converted = UtfToLocal(
        src,
        len,
        dest,
        &GB18030_FROM_UNICODE_TREE,
        null(),
        0,
        Some(conv_utf8_to_18030),
        pg_enc::PG_GB18030 as c_int,
        no_error,
    );

    PG_RETURN_INT32!(converted);
}
