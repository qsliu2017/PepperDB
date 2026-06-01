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
use crate::mb::pg_wchar::{
    check_encoding_conversion_args, pg_encoding_verifymbchar, report_invalid_encoding,
};
use crate::mb::wchar::{pg_enc, SS2, SS3};
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};
use crate::c::IS_HIGHBIT_SET;

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
    p: *mut c_uchar,
    len: c_int,
    no_error: bool,
) -> c_int {
    let start = euc;
    let mut euc = euc;
    let mut p = p;
    let mut len = len;

    while len > 0 {
        let c1 = *euc as c_int;
        if !IS_HIGHBIT_SET(*euc) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_EUC_JIS_2004 as c_int, euc as *const c_char, len);
            }
            *p = c1 as c_uchar;
            p = p.add(1);
            euc = euc.add(1);
            len -= 1;
            continue;
        }

        let l = pg_encoding_verifymbchar(pg_enc::PG_EUC_JIS_2004 as c_int, euc as *const c_char, len);

        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_EUC_JIS_2004 as c_int, euc as *const c_char, len);
        }

        if c1 == SS2 as c_int && l == 2 {
            /* JIS X 0201 kana? */
            *p = *euc.add(1);
            p = p.add(1);
        } else if c1 == SS3 as c_int && l == 3 {
            /* JIS X 0213 plane 2? */
            let ku = *euc.add(1) as c_int - 0xa0;
            let ten = *euc.add(2) as c_int - 0xa0;

            let first_byte: c_int = match ku {
                1 | 3 | 4 | 5 | 8 | 12 | 13 | 14 | 15 => {
                    ((ku + 0x1df) >> 1) - (ku >> 3) * 3
                }
                _ => {
                    if ku >= 78 && ku <= 94 {
                        (ku + 0x19b) >> 1
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
            };
            *p = first_byte as c_uchar;
            p = p.add(1);

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
            let ku = c1 - 0xa0;
            let ten = *euc.add(1) as c_int - 0xa0;

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
                report_invalid_encoding(pg_enc::PG_EUC_JIS_2004 as c_int, euc as *const c_char, len);
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
            report_invalid_encoding(pg_enc::PG_EUC_JIS_2004 as c_int, euc as *const c_char, len);
        }

        euc = euc.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    euc.offset_from(start) as c_int
}

/*
 * returns SHIFT_JIS_2004 "ku" code indicated by second byte
 * *ku = 0: "ku" = even
 * *ku = 1: "ku" = odd
 */
fn get_ten(b: c_int, ku: &mut c_int) -> c_int {
    if b >= 0x40 && b <= 0x7e {
        *ku = 1;
        b - 0x3f
    } else if b >= 0x80 && b <= 0x9e {
        *ku = 1;
        b - 0x40
    } else if b >= 0x9f && b <= 0xfc {
        *ku = 0;
        b - 0x9e
    } else {
        *ku = 0; /* keep compiler quiet */
        -1       /* error */
    }
}

/*
 * SHIFT_JIS_2004 ---> EUC_JIS_2004
 */
unsafe fn shift_jis_20042euc_jis_2004(
    sjis: *const c_uchar,
    p: *mut c_uchar,
    len: c_int,
    no_error: bool,
) -> c_int {
    let start = sjis;
    let mut sjis = sjis;
    let mut p = p;
    let mut len = len;

    while len > 0 {
        let c1 = *sjis as c_int;

        if !IS_HIGHBIT_SET(*sjis) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(
                    pg_enc::PG_SHIFT_JIS_2004 as c_int,
                    sjis as *const c_char,
                    len,
                );
            }
            *p = c1 as c_uchar;
            p = p.add(1);
            sjis = sjis.add(1);
            len -= 1;
            continue;
        }

        let l = pg_encoding_verifymbchar(
            pg_enc::PG_SHIFT_JIS_2004 as c_int,
            sjis as *const c_char,
            len,
        );

        if l < 0 || l > len {
            if no_error {
                break;
            }
            report_invalid_encoding(
                pg_enc::PG_SHIFT_JIS_2004 as c_int,
                sjis as *const c_char,
                len,
            );
        }

        if c1 >= 0xa1 && c1 <= 0xdf && l == 1 {
            /* JIS X0201 (1 byte kana) */
            *p = SS2;
            p = p.add(1);
            *p = c1 as c_uchar;
            p = p.add(1);
        } else if l == 2 {
            let c2 = *sjis.add(1) as c_int;
            let mut plane: c_int = 1;
            let mut ku: c_int = 1;
            let mut ten: c_int = 0;
            let mut kubun: c_int = 0;

            /* JIS X 0213 */
            if c1 >= 0x81 && c1 <= 0x9f {
                /* plane 1 1ku-62ku */
                ku = (c1 << 1) - 0x100;
                ten = get_ten(c2, &mut kubun);
                if ten < 0 {
                    if no_error {
                        break;
                    }
                    report_invalid_encoding(
                        pg_enc::PG_SHIFT_JIS_2004 as c_int,
                        sjis as *const c_char,
                        len,
                    );
                }
                ku -= kubun;
            } else if c1 >= 0xe0 && c1 <= 0xef {
                /* plane 1 62ku-94ku */
                ku = (c1 << 1) - 0x180;
                ten = get_ten(c2, &mut kubun);
                if ten < 0 {
                    if no_error {
                        break;
                    }
                    report_invalid_encoding(
                        pg_enc::PG_SHIFT_JIS_2004 as c_int,
                        sjis as *const c_char,
                        len,
                    );
                }
                ku -= kubun;
            } else if c1 >= 0xf0 && c1 <= 0xf3 {
                /* plane 2: 1,3,4,5,8,12,13,14,15 ku */
                plane = 2;
                ten = get_ten(c2, &mut kubun);
                if ten < 0 {
                    if no_error {
                        break;
                    }
                    report_invalid_encoding(
                        pg_enc::PG_SHIFT_JIS_2004 as c_int,
                        sjis as *const c_char,
                        len,
                    );
                }
                ku = match c1 {
                    0xf0 => if kubun == 0 { 8 } else { 1 },
                    0xf1 => if kubun == 0 { 4 } else { 3 },
                    0xf2 => if kubun == 0 { 12 } else { 5 },
                    _    => if kubun == 0 { 14 } else { 13 },
                };
            } else if c1 >= 0xf4 && c1 <= 0xfc {
                /* plane 2 78-94ku */
                plane = 2;
                ten = get_ten(c2, &mut kubun);
                if ten < 0 {
                    if no_error {
                        break;
                    }
                    report_invalid_encoding(
                        pg_enc::PG_SHIFT_JIS_2004 as c_int,
                        sjis as *const c_char,
                        len,
                    );
                }
                if c1 == 0xf4 && kubun == 1 {
                    ku = 15;
                } else {
                    ku = (c1 << 1) - 0x19a - kubun;
                }
            } else {
                if no_error {
                    break;
                }
                report_invalid_encoding(
                    pg_enc::PG_SHIFT_JIS_2004 as c_int,
                    sjis as *const c_char,
                    len,
                );
            }

            if plane == 2 {
                *p = SS3;
                p = p.add(1);
            }

            *p = (ku + 0xa0) as c_uchar;
            p = p.add(1);
            *p = (ten + 0xa0) as c_uchar;
            p = p.add(1);
        }

        sjis = sjis.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    sjis.offset_from(start) as c_int
}
