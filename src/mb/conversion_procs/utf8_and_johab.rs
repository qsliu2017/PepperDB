/*-------------------------------------------------------------------------
 *
 *    JOHAB <--> UTF8
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/utils/mb/conversion_procs/utf8_and_johab/utf8_and_johab.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::mb::conv::{LocalToUtf, UtfToLocal};
use crate::mb::pg_wchar::check_encoding_conversion_args;
use crate::mb::wchar::{pg_enc, pg_mb_radix_tree};
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};
use core::ptr::null;

// TODO(pg-port): generated radix map from src/backend/utils/mb/Unicode/johab_to_utf8.map - tables not transcribed
const JOHAB_TO_UNICODE_TREE: pg_mb_radix_tree = pg_mb_radix_tree {
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

// TODO(pg-port): generated radix map from src/backend/utils/mb/Unicode/utf8_to_johab.map - tables not transcribed
const JOHAB_FROM_UNICODE_TREE: pg_mb_radix_tree = pg_mb_radix_tree {
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
pub unsafe fn johab_to_utf8(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_JOHAB as c_int,
        pg_enc::PG_UTF8 as c_int,
    );

    converted = LocalToUtf(
        src,
        len,
        dest,
        &JOHAB_TO_UNICODE_TREE,
        null(),
        0,
        None,
        pg_enc::PG_JOHAB as c_int,
        no_error,
    );

    PG_RETURN_INT32!(converted);
}

pub unsafe fn utf8_to_johab(fcinfo: FunctionCallInfo) -> Datum {
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
        pg_enc::PG_JOHAB as c_int,
    );

    converted = UtfToLocal(
        src,
        len,
        dest,
        &JOHAB_FROM_UNICODE_TREE,
        null(),
        0,
        None,
        pg_enc::PG_JOHAB as c_int,
        no_error,
    );

    PG_RETURN_INT32!(converted);
}
