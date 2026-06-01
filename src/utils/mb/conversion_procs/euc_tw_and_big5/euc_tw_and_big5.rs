/*-------------------------------------------------------------------------
 *
 *	  EUC_TW, BIG5 and MULE_INTERNAL
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::mb::pg_wchar::{
    check_encoding_conversion_args, pg_encoding_verifymbchar, report_invalid_encoding,
    report_untranslatable_char, BIG5toCNS, CNStoBIG5, LCPRV2_B, LC_CNS11643_1, LC_CNS11643_2,
    LC_CNS11643_3, LC_CNS11643_4, LC_CNS11643_7, SS2,
};
use crate::mb::wchar::pg_enc;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};
use crate::c::IS_HIGHBIT_SET;

// PG_MODULE_MAGIC_EXT(.name = "euc_tw_and_big5", .version = PG_VERSION)

/* ----------
 * conv_proc(
 *		INTEGER,	-- source encoding id
 *		INTEGER,	-- destination encoding id
 *		CSTRING,	-- source string (null terminated C string)
 *		CSTRING,	-- destination string (null terminated C string)
 *		INTEGER,	-- source string length
 *		BOOL		-- if true, don't throw an error if conversion fails
 * ) returns INTEGER;
 *
 * Returns the number of bytes successfully converted.
 * ----------
 */

pub unsafe fn euc_tw_to_big5(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_EUC_TW as c_int,
        pg_enc::PG_BIG5 as c_int,
    );

    converted = euc_tw2big5(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn big5_to_euc_tw(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_BIG5 as c_int,
        pg_enc::PG_EUC_TW as c_int,
    );

    converted = big52euc_tw(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn euc_tw_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_EUC_TW as c_int,
        pg_enc::PG_MULE_INTERNAL as c_int,
    );

    converted = euc_tw2mic(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn mic_to_euc_tw(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_MULE_INTERNAL as c_int,
        pg_enc::PG_EUC_TW as c_int,
    );

    converted = mic2euc_tw(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn big5_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_BIG5 as c_int,
        pg_enc::PG_MULE_INTERNAL as c_int,
    );

    converted = big52mic(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn mic_to_big5(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_MULE_INTERNAL as c_int,
        pg_enc::PG_BIG5 as c_int,
    );

    converted = mic2big5(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

/*
 * EUC_TW ---> Big5
 */
unsafe fn euc_tw2big5(
    euc: *const c_uchar,
    p: *mut c_uchar,
    len: c_int,
    no_error: bool,
) -> c_int {
    let start = euc;
    let mut euc = euc;
    let mut p = p;
    let mut len = len;
    let mut c1: c_uchar;
    let mut big5buf: c_ushort;
    let mut cns_buf: c_ushort;
    let mut lc: c_uchar;
    let mut l: c_int;

    while len > 0 {
        c1 = *euc;
        if IS_HIGHBIT_SET(c1) {
            /* Verify and decode the next EUC_TW input character */
            l = pg_encoding_verifymbchar(pg_enc::PG_EUC_TW as c_int, euc as *const c_char, len);
            if l < 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_EUC_TW as c_int, euc as *const c_char, len);
            }
            if c1 as c_int == SS2 {
                c1 = *euc.add(1); /* plane No. */
                if c1 == 0xa1 {
                    lc = LC_CNS11643_1 as c_uchar;
                } else if c1 == 0xa2 {
                    lc = LC_CNS11643_2 as c_uchar;
                } else {
                    lc = (c1 as c_int - 0xa3 + LC_CNS11643_3) as c_uchar;
                }
                cns_buf = (((*euc.add(2) as c_int) << 8) | (*euc.add(3) as c_int)) as c_ushort;
            } else {
                /* CNS11643-1 */
                lc = LC_CNS11643_1 as c_uchar;
                cns_buf = (((c1 as c_int) << 8) | (*euc.add(1) as c_int)) as c_ushort;
            }

            /* Write it out in Big5 */
            big5buf = CNStoBIG5(cns_buf, lc);
            if big5buf == 0 {
                if no_error {
                    break;
                }
                report_untranslatable_char(
                    pg_enc::PG_EUC_TW as c_int,
                    pg_enc::PG_BIG5 as c_int,
                    euc as *const c_char,
                    len,
                );
            }
            *p = ((big5buf as c_int >> 8) & 0x00ff) as c_uchar;
            p = p.add(1);
            *p = (big5buf as c_int & 0x00ff) as c_uchar;
            p = p.add(1);

            euc = euc.add(l as usize);
            len -= l;
        } else {
            /* should be ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_EUC_TW as c_int, euc as *const c_char, len);
            }
            *p = c1;
            p = p.add(1);
            euc = euc.add(1);
            len -= 1;
        }
    }
    *p = b'\0';

    euc.offset_from(start) as c_int
}

/*
 * Big5 ---> EUC_TW
 */
unsafe fn big52euc_tw(
    big5: *const c_uchar,
    p: *mut c_uchar,
    len: c_int,
    no_error: bool,
) -> c_int {
    let start = big5;
    let mut big5 = big5;
    let mut p = p;
    let mut len = len;
    let mut c1: c_ushort;
    let mut big5buf: c_ushort;
    let mut cns_buf: c_ushort;
    let mut lc: c_uchar = 0;
    let mut l: c_int;

    while len > 0 {
        /* Verify and decode the next Big5 input character */
        c1 = *big5 as c_ushort;
        if IS_HIGHBIT_SET(c1 as c_uchar) {
            l = pg_encoding_verifymbchar(pg_enc::PG_BIG5 as c_int, big5 as *const c_char, len);
            if l < 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_BIG5 as c_int, big5 as *const c_char, len);
            }
            big5buf = (((c1 as c_int) << 8) | (*big5.add(1) as c_int)) as c_ushort;
            cns_buf = BIG5toCNS(big5buf, &mut lc);

            if lc as c_int == LC_CNS11643_1 {
                *p = ((cns_buf as c_int >> 8) & 0x00ff) as c_uchar;
                p = p.add(1);
                *p = (cns_buf as c_int & 0x00ff) as c_uchar;
                p = p.add(1);
            } else if lc as c_int == LC_CNS11643_2 {
                *p = SS2 as c_uchar;
                p = p.add(1);
                *p = 0xa2;
                p = p.add(1);
                *p = ((cns_buf as c_int >> 8) & 0x00ff) as c_uchar;
                p = p.add(1);
                *p = (cns_buf as c_int & 0x00ff) as c_uchar;
                p = p.add(1);
            } else if lc as c_int >= LC_CNS11643_3 && lc as c_int <= LC_CNS11643_7 {
                *p = SS2 as c_uchar;
                p = p.add(1);
                *p = (lc as c_int - LC_CNS11643_3 + 0xa3) as c_uchar;
                p = p.add(1);
                *p = ((cns_buf as c_int >> 8) & 0x00ff) as c_uchar;
                p = p.add(1);
                *p = (cns_buf as c_int & 0x00ff) as c_uchar;
                p = p.add(1);
            } else {
                if no_error {
                    break;
                }
                report_untranslatable_char(
                    pg_enc::PG_BIG5 as c_int,
                    pg_enc::PG_EUC_TW as c_int,
                    big5 as *const c_char,
                    len,
                );
            }

            big5 = big5.add(l as usize);
            len -= l;
        } else {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_BIG5 as c_int, big5 as *const c_char, len);
            }
            *p = c1 as c_uchar;
            p = p.add(1);
            big5 = big5.add(1);
            len -= 1;
            continue;
        }
    }
    *p = b'\0';

    big5.offset_from(start) as c_int
}

/*
 * EUC_TW ---> MIC
 */
unsafe fn euc_tw2mic(
    euc: *const c_uchar,
    p: *mut c_uchar,
    len: c_int,
    no_error: bool,
) -> c_int {
    let start = euc;
    let mut euc = euc;
    let mut p = p;
    let mut len = len;
    let mut c1: c_int;
    let mut l: c_int;

    while len > 0 {
        c1 = *euc as c_int;
        if IS_HIGHBIT_SET(c1 as c_uchar) {
            l = pg_encoding_verifymbchar(pg_enc::PG_EUC_TW as c_int, euc as *const c_char, len);
            if l < 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_EUC_TW as c_int, euc as *const c_char, len);
            }
            if c1 == SS2 {
                c1 = *euc.add(1) as c_int; /* plane No. */
                if c1 == 0xa1 {
                    *p = LC_CNS11643_1 as c_uchar;
                    p = p.add(1);
                } else if c1 == 0xa2 {
                    *p = LC_CNS11643_2 as c_uchar;
                    p = p.add(1);
                } else {
                    /* other planes are MULE private charsets */
                    *p = LCPRV2_B as c_uchar;
                    p = p.add(1);
                    *p = (c1 - 0xa3 + LC_CNS11643_3) as c_uchar;
                    p = p.add(1);
                }
                *p = *euc.add(2);
                p = p.add(1);
                *p = *euc.add(3);
                p = p.add(1);
            } else {
                /* CNS11643-1 */
                *p = LC_CNS11643_1 as c_uchar;
                p = p.add(1);
                *p = c1 as c_uchar;
                p = p.add(1);
                *p = *euc.add(1);
                p = p.add(1);
            }
            euc = euc.add(l as usize);
            len -= l;
        } else {
            /* should be ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_EUC_TW as c_int, euc as *const c_char, len);
            }
            *p = c1 as c_uchar;
            p = p.add(1);
            euc = euc.add(1);
            len -= 1;
        }
    }
    *p = b'\0';

    euc.offset_from(start) as c_int
}

/*
 * MIC ---> EUC_TW
 */
unsafe fn mic2euc_tw(
    mic: *const c_uchar,
    p: *mut c_uchar,
    len: c_int,
    no_error: bool,
) -> c_int {
    let start = mic;
    let mut mic = mic;
    let mut p = p;
    let mut len = len;
    let mut c1: c_int;
    let mut l: c_int;

    while len > 0 {
        c1 = *mic as c_int;
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(
                    pg_enc::PG_MULE_INTERNAL as c_int,
                    mic as *const c_char,
                    len,
                );
            }
            *p = c1 as c_uchar;
            p = p.add(1);
            mic = mic.add(1);
            len -= 1;
            continue;
        }
        l = pg_encoding_verifymbchar(pg_enc::PG_MULE_INTERNAL as c_int, mic as *const c_char, len);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_MULE_INTERNAL as c_int, mic as *const c_char, len);
        }
        if c1 == LC_CNS11643_1 {
            *p = *mic.add(1);
            p = p.add(1);
            *p = *mic.add(2);
            p = p.add(1);
        } else if c1 == LC_CNS11643_2 {
            *p = SS2 as c_uchar;
            p = p.add(1);
            *p = 0xa2;
            p = p.add(1);
            *p = *mic.add(1);
            p = p.add(1);
            *p = *mic.add(2);
            p = p.add(1);
        } else if c1 == LCPRV2_B
            && *mic.add(1) as c_int >= LC_CNS11643_3
            && *mic.add(1) as c_int <= LC_CNS11643_7
        {
            *p = SS2 as c_uchar;
            p = p.add(1);
            *p = (*mic.add(1) as c_int - LC_CNS11643_3 + 0xa3) as c_uchar;
            p = p.add(1);
            *p = *mic.add(2);
            p = p.add(1);
            *p = *mic.add(3);
            p = p.add(1);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(
                pg_enc::PG_MULE_INTERNAL as c_int,
                pg_enc::PG_EUC_TW as c_int,
                mic as *const c_char,
                len,
            );
        }
        mic = mic.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    mic.offset_from(start) as c_int
}

/*
 * Big5 ---> MIC
 */
unsafe fn big52mic(
    big5: *const c_uchar,
    p: *mut c_uchar,
    len: c_int,
    no_error: bool,
) -> c_int {
    let start = big5;
    let mut big5 = big5;
    let mut p = p;
    let mut len = len;
    let mut c1: c_ushort;
    let mut big5buf: c_ushort;
    let mut cns_buf: c_ushort;
    let mut lc: c_uchar = 0;
    let mut l: c_int;

    while len > 0 {
        c1 = *big5 as c_ushort;
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_BIG5 as c_int, big5 as *const c_char, len);
            }
            *p = c1 as c_uchar;
            p = p.add(1);
            big5 = big5.add(1);
            len -= 1;
            continue;
        }
        l = pg_encoding_verifymbchar(pg_enc::PG_BIG5 as c_int, big5 as *const c_char, len);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_BIG5 as c_int, big5 as *const c_char, len);
        }
        big5buf = (((c1 as c_int) << 8) | (*big5.add(1) as c_int)) as c_ushort;
        cns_buf = BIG5toCNS(big5buf, &mut lc);
        if lc != 0 {
            /* Planes 3 and 4 are MULE private charsets */
            if lc as c_int == LC_CNS11643_3 || lc as c_int == LC_CNS11643_4 {
                *p = LCPRV2_B as c_uchar;
                p = p.add(1);
            }
            *p = lc; /* Plane No. */
            p = p.add(1);
            *p = ((cns_buf as c_int >> 8) & 0x00ff) as c_uchar;
            p = p.add(1);
            *p = (cns_buf as c_int & 0x00ff) as c_uchar;
            p = p.add(1);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(
                pg_enc::PG_BIG5 as c_int,
                pg_enc::PG_MULE_INTERNAL as c_int,
                big5 as *const c_char,
                len,
            );
        }
        big5 = big5.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    big5.offset_from(start) as c_int
}

/*
 * MIC ---> Big5
 */
unsafe fn mic2big5(
    mic: *const c_uchar,
    p: *mut c_uchar,
    len: c_int,
    no_error: bool,
) -> c_int {
    let start = mic;
    let mut mic = mic;
    let mut p = p;
    let mut len = len;
    let mut c1: c_ushort;
    let mut big5buf: c_ushort;
    let mut cns_buf: c_ushort;
    let mut l: c_int;

    while len > 0 {
        c1 = *mic as c_ushort;
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(
                    pg_enc::PG_MULE_INTERNAL as c_int,
                    mic as *const c_char,
                    len,
                );
            }
            *p = c1 as c_uchar;
            p = p.add(1);
            mic = mic.add(1);
            len -= 1;
            continue;
        }
        l = pg_encoding_verifymbchar(pg_enc::PG_MULE_INTERNAL as c_int, mic as *const c_char, len);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_MULE_INTERNAL as c_int, mic as *const c_char, len);
        }
        if c1 as c_int == LC_CNS11643_1
            || c1 as c_int == LC_CNS11643_2
            || c1 as c_int == LCPRV2_B
        {
            if c1 as c_int == LCPRV2_B {
                c1 = *mic.add(1) as c_ushort; /* get plane no. */
                cns_buf = (((*mic.add(2) as c_int) << 8) | (*mic.add(3) as c_int)) as c_ushort;
            } else {
                cns_buf = (((*mic.add(1) as c_int) << 8) | (*mic.add(2) as c_int)) as c_ushort;
            }
            big5buf = CNStoBIG5(cns_buf, c1 as c_uchar);
            if big5buf == 0 {
                if no_error {
                    break;
                }
                report_untranslatable_char(
                    pg_enc::PG_MULE_INTERNAL as c_int,
                    pg_enc::PG_BIG5 as c_int,
                    mic as *const c_char,
                    len,
                );
            }
            *p = ((big5buf as c_int >> 8) & 0x00ff) as c_uchar;
            p = p.add(1);
            *p = (big5buf as c_int & 0x00ff) as c_uchar;
            p = p.add(1);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(
                pg_enc::PG_MULE_INTERNAL as c_int,
                pg_enc::PG_BIG5 as c_int,
                mic as *const c_char,
                len,
            );
        }
        mic = mic.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    mic.offset_from(start) as c_int
}
