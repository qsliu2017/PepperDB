/*-------------------------------------------------------------------------
 *
 *    EUC_JIS_2004, SHIFT_JIS_2004
 *
 * Copyright (c) 2007-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::c::IS_HIGHBIT_SET;
use crate::mb::mbutils::report_invalid_encoding;
use crate::mb::pg_wchar::{check_encoding_conversion_args, SS2, SS3};
use crate::mb::wchar::{pg_enc, pg_encoding_verifymbchar};
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};

// PG_MODULE_MAGIC_EXT(.name = "euc2004_sjis2004", .version = PG_VERSION)
// (module magic handled by the loader; not represented here)

// PG_FUNCTION_INFO_V1(euc_jis_2004_to_shift_jis_2004);
// PG_FUNCTION_INFO_V1(shift_jis_2004_to_euc_jis_2004);

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

pub unsafe fn euc_jis_2004_to_shift_jis_2004(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_EUC_JIS_2004 as c_int,
        pg_enc::PG_SHIFT_JIS_2004 as c_int,
    );

    converted = euc_jis_20042shift_jis_2004(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn shift_jis_2004_to_euc_jis_2004(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_SHIFT_JIS_2004 as c_int,
        pg_enc::PG_EUC_JIS_2004 as c_int,
    );

    converted = shift_jis_20042euc_jis_2004(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

/*
 * EUC_JIS_2004 -> SHIFT_JIS_2004
 */
unsafe fn euc_jis_20042shift_jis_2004(
    euc: *const c_uchar,
    mut p: *mut c_uchar,
    mut len: c_int,
    no_error: bool,
) -> c_int {
    let start: *const c_uchar = euc;
    let mut euc = euc;
    let mut c1: c_int;
    let mut ku: c_int;
    let mut ten: c_int;
    let mut l: c_int;

    while len > 0 {
        c1 = *euc as c_int;
        if !IS_HIGHBIT_SET(c1 as u8) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(
                    pg_enc::PG_EUC_JIS_2004 as c_int,
                    euc as *const c_char,
                    len,
                );
            }
            *p = c1 as c_uchar;
            p = p.add(1);
            euc = euc.add(1);
            len -= 1;
            continue;
        }

        l = pg_encoding_verifymbchar(
            pg_enc::PG_EUC_JIS_2004 as c_int,
            euc as *const c_char,
            len,
        );

        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(
                pg_enc::PG_EUC_JIS_2004 as c_int,
                euc as *const c_char,
                len,
            );
        }

        if c1 == SS2 && l == 2 {
            /* JIS X 0201 kana? */
            *p = *euc.add(1);
            p = p.add(1);
        } else if c1 == SS3 && l == 3 {
            /* JIS X 0213 plane 2? */
            ku = *euc.add(1) as c_int - 0xa0;
            ten = *euc.add(2) as c_int - 0xa0;

            match ku {
                1 | 3 | 4 | 5 | 8 | 12 | 13 | 14 | 15 => {
                    *p = (((ku + 0x1df) >> 1) - (ku >> 3) * 3) as c_uchar;
                    p = p.add(1);
                }
                _ => {
                    if ku >= 78 && ku <= 94 {
                        *p = ((ku + 0x19b) >> 1) as c_uchar;
                        p = p.add(1);
                    } else {
                        if no_error {
                            break;
                        }
                        report_invalid_encoding(
                            pg_enc::PG_EUC_JIS_2004 as c_int,
                            euc as *const c_char,
                            len,
                        );
                    }
                }
            }

            if ku % 2 != 0 {
                if ten >= 1 && ten <= 63 {
                    *p = (ten + 0x3f) as c_uchar;
                    p = p.add(1);
                } else if ten >= 64 && ten <= 94 {
                    *p = (ten + 0x40) as c_uchar;
                    p = p.add(1);
                } else {
                    if no_error {
                        break;
                    }
                    report_invalid_encoding(
                        pg_enc::PG_EUC_JIS_2004 as c_int,
                        euc as *const c_char,
                        len,
                    );
                }
            } else {
                *p = (ten + 0x9e) as c_uchar;
                p = p.add(1);
            }
        } else if l == 2 {
            /* JIS X 0213 plane 1? */
            ku = c1 - 0xa0;
            ten = *euc.add(1) as c_int - 0xa0;

            if ku >= 1 && ku <= 62 {
                *p = ((ku + 0x101) >> 1) as c_uchar;
                p = p.add(1);
            } else if ku >= 63 && ku <= 94 {
                *p = ((ku + 0x181) >> 1) as c_uchar;
                p = p.add(1);
            } else {
                if no_error {
                    break;
                }
                report_invalid_encoding(
                    pg_enc::PG_EUC_JIS_2004 as c_int,
                    euc as *const c_char,
                    len,
                );
            }

            if ku % 2 != 0 {
                if ten >= 1 && ten <= 63 {
                    *p = (ten + 0x3f) as c_uchar;
                    p = p.add(1);
                } else if ten >= 64 && ten <= 94 {
                    *p = (ten + 0x40) as c_uchar;
                    p = p.add(1);
                } else {
                    if no_error {
                        break;
                    }
                    report_invalid_encoding(
                        pg_enc::PG_EUC_JIS_2004 as c_int,
                        euc as *const c_char,
                        len,
                    );
                }
            } else {
                *p = (ten + 0x9e) as c_uchar;
                p = p.add(1);
            }
        } else {
            if no_error {
                break;
            }
            report_invalid_encoding(
                pg_enc::PG_EUC_JIS_2004 as c_int,
                euc as *const c_char,
                len,
            );
        }

        euc = euc.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    (euc as isize - start as isize) as c_int
}
