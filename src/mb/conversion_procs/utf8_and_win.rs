/*-------------------------------------------------------------------------
 *
 *    WIN <--> UTF8
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/utils/mb/conversion_procs/utf8_and_win/utf8_and_win.c
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

// TODO(pg-port): per-encoding radix maps from src/backend/utils/mb/Unicode/win*_to_utf8.map / utf8_to_win*.map - tables not transcribed
const NULL_TREE: pg_mb_radix_tree = pg_mb_radix_tree {
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

struct PgConvMap {
    encoding: pg_enc,
    map1: *const pg_mb_radix_tree, /* to UTF8 */
    map2: *const pg_mb_radix_tree, /* from UTF8 */
}

// SAFETY: raw pointers point to 'static const data only
unsafe impl Sync for PgConvMap {}

// TODO(pg-port): map1/map2 point to NULL_TREE stubs until radix tables are transcribed
const MAPS: [PgConvMap; 11] = [
    PgConvMap { encoding: pg_enc::PG_WIN866,   map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN874,   map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1250,  map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1251,  map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1252,  map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1253,  map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1254,  map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1255,  map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1256,  map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1257,  map1: &NULL_TREE, map2: &NULL_TREE },
    PgConvMap { encoding: pg_enc::PG_WIN1258,  map1: &NULL_TREE, map2: &NULL_TREE },
];

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
pub unsafe fn win_to_utf8(fcinfo: FunctionCallInfo) -> Datum {
    let encoding: c_int = PG_GETARG_INT32!(fcinfo, 0);
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        -1,
        pg_enc::PG_UTF8 as c_int,
    );

    for entry in &MAPS {
        if encoding == entry.encoding as c_int {
            let converted = LocalToUtf(
                src,
                len,
                dest,
                entry.map1,
                null(),
                0,
                None,
                encoding,
                no_error,
            );
            PG_RETURN_INT32!(converted);
        }
    }

    ereport!(ERROR, errmsg!("unexpected encoding ID {} for WIN character sets", encoding));

    PG_RETURN_INT32!(0);
}

pub unsafe fn utf8_to_win(fcinfo: FunctionCallInfo) -> Datum {
    let encoding: c_int = PG_GETARG_INT32!(fcinfo, 1);
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_UTF8 as c_int,
        -1,
    );

    for entry in &MAPS {
        if encoding == entry.encoding as c_int {
            let converted = UtfToLocal(
                src,
                len,
                dest,
                entry.map2,
                null(),
                0,
                None,
                encoding,
                no_error,
            );
            PG_RETURN_INT32!(converted);
        }
    }

    ereport!(ERROR, errmsg!("unexpected encoding ID {} for WIN character sets", encoding));

    PG_RETURN_INT32!(0);
}
