/*-------------------------------------------------------------------------
 *
 *    ISO8859_1 <--> UTF8
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/utils/mb/conversion_procs/utf8_and_iso8859_1/utf8_and_iso8859_1.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::mb::pg_wchar::{
    check_encoding_conversion_args, pg_utf_mblen, pg_utf8_islegal,
    report_invalid_encoding, report_untranslatable_char,
};
use crate::mb::wchar::pg_enc;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};
use crate::c::{HIGHBIT, IS_HIGHBIT_SET};

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
pub unsafe fn iso8859_1_to_utf8(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let start = src;
    let mut src = src;
    let mut dest = dest;
    let mut len = len;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_LATIN1 as c_int,
        pg_enc::PG_UTF8 as c_int,
    );

    while len > 0 {
        let c: c_uint = *src as c_uint;
        if c == 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_LATIN1 as c_int, src as *const c_char, len);
        }
        if !IS_HIGHBIT_SET(*src) {
            *dest = *src;
            dest = dest.add(1);
        } else {
            *dest = ((c >> 6) | 0xc0) as c_uchar;
            dest = dest.add(1);
            *dest = ((c & 0x003f) | HIGHBIT as c_uint) as c_uchar;
            dest = dest.add(1);
        }
        src = src.add(1);
        len -= 1;
    }
    *dest = b'\0';

    PG_RETURN_INT32!(src.offset_from(start) as c_int);
}

pub unsafe fn utf8_to_iso8859_1(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let start = src;
    let mut src = src;
    let mut dest = dest;
    let mut len = len;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_UTF8 as c_int,
        pg_enc::PG_LATIN1 as c_int,
    );

    while len > 0 {
        let c: c_uint = *src as c_uint;
        if c == 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_UTF8 as c_int, src as *const c_char, len);
        }
        /* fast path for ASCII-subset characters */
        if !IS_HIGHBIT_SET(*src) {
            *dest = *src;
            dest = dest.add(1);
            src = src.add(1);
            len -= 1;
        } else {
            let l = pg_utf_mblen(src);
            if l > len || !pg_utf8_islegal(src, l) {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_UTF8 as c_int, src as *const c_char, len);
            }
            if l != 2 {
                if no_error {
                    break;
                }
                report_untranslatable_char(
                    pg_enc::PG_UTF8 as c_int,
                    pg_enc::PG_LATIN1 as c_int,
                    src as *const c_char,
                    len,
                );
            }
            let c1 = (*src.add(1) & 0x3f) as c_uint;
            let c2 = ((c & 0x1f) << 6) | c1;
            if c2 >= 0x80 && c2 <= 0xff {
                *dest = c2 as c_uchar;
                dest = dest.add(1);
                src = src.add(2);
                len -= 2;
            } else {
                if no_error {
                    break;
                }
                report_untranslatable_char(
                    pg_enc::PG_UTF8 as c_int,
                    pg_enc::PG_LATIN1 as c_int,
                    src as *const c_char,
                    len,
                );
            }
        }
    }
    *dest = b'\0';

    PG_RETURN_INT32!(src.offset_from(start) as c_int);
}
