/*-------------------------------------------------------------------------
 *
 *	  EUC_JP, SJIS and MULE_INTERNAL
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::mb::pg_wchar::{
    check_encoding_conversion_args, pg_encoding_verifymbchar, report_invalid_encoding,
    report_untranslatable_char, ISSJISHEAD, ISSJISTAIL,
};
use crate::mb::wchar::{pg_enc, LC_JISX0201K, LC_JISX0208, LC_JISX0212, SS2, SS3};
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};
use crate::c::IS_HIGHBIT_SET;

/*
 * SJIS alternative code.
 * this code is used if a mapping EUC -> SJIS is not defined.
 */
const PGSJISALTCODE: c_int = 0x81ac;
const PGEUCALTCODE: c_int = 0xa2ae;

/*
 * conversion table between SJIS UDC (IBM kanji) and EUC_JP
 *
 * C: #include "sjis.map"
 */
struct IbmKanji {
    nec: u16,  /* SJIS UDC (NEC selection IBM kanji) */
    sjis: u16, /* SJIS UDC (IBM kanji) */
    euc: c_int, /* EUC_JP */
}

static IBMKANJI: &[IbmKanji] = &[
RSEOF_PLACEHOLDER
];

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

pub unsafe fn euc_jp_to_sjis(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_EUC_JP as c_int,
        pg_enc::PG_SJIS as c_int,
    );

    converted = euc_jp2sjis(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn sjis_to_euc_jp(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_SJIS as c_int,
        pg_enc::PG_EUC_JP as c_int,
    );

    converted = sjis2euc_jp(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn euc_jp_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_EUC_JP as c_int,
        pg_enc::PG_MULE_INTERNAL as c_int,
    );

    converted = euc_jp2mic(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn mic_to_euc_jp(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_MULE_INTERNAL as c_int,
        pg_enc::PG_EUC_JP as c_int,
    );

    converted = mic2euc_jp(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn sjis_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_SJIS as c_int,
        pg_enc::PG_MULE_INTERNAL as c_int,
    );

    converted = sjis2mic(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

pub unsafe fn mic_to_sjis(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *const c_uchar;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut c_uchar;
    let len: c_int = PG_GETARG_INT32!(fcinfo, 4);
    let no_error: bool = PG_GETARG_BOOL!(fcinfo, 5);
    let converted: c_int;

    check_encoding_conversion_args(
        PG_GETARG_INT32!(fcinfo, 0),
        PG_GETARG_INT32!(fcinfo, 1),
        len,
        pg_enc::PG_MULE_INTERNAL as c_int,
        pg_enc::PG_SJIS as c_int,
    );

    converted = mic2sjis(src, dest, len, no_error);

    PG_RETURN_INT32!(converted);
}

/*
 * SJIS ---> MIC
 */
unsafe fn sjis2mic(sjis: *const c_uchar, mut p: *mut c_uchar, mut len: c_int, no_error: bool) -> c_int {
    let start = sjis;
    let mut sjis = sjis;
    let mut c1: c_int;
    let mut c2: c_int;
    let mut i: usize;
    let mut k: c_int;
    let mut k2: c_int;

    while len > 0 {
        c1 = *sjis as c_int;
        if c1 >= 0xa1 && c1 <= 0xdf {
            /* JIS X0201 (1 byte kana) */
            *p = LC_JISX0201K as c_uchar; p = p.add(1);
            *p = c1 as c_uchar; p = p.add(1);
            sjis = sjis.add(1);
            len -= 1;
        } else if IS_HIGHBIT_SET(c1 as c_uchar) {
            /*
             * JIS X0208, X0212, user defined extended characters
             */
            if len < 2 || !ISSJISHEAD(c1) || !ISSJISTAIL(*sjis.add(1) as c_int) {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_SJIS as c_int, sjis as *const c_char, len);
            }
            c2 = *sjis.add(1) as c_int;
            k = (c1 << 8) + c2;
            if k >= 0xed40 && k < 0xf040 {
                /* NEC selection IBM kanji */
                i = 0;
                loop {
                    k2 = IBMKANJI[i].nec as c_int;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        k = IBMKANJI[i].sjis as c_int;
                        c1 = (k >> 8) & 0xff;
                        c2 = k & 0xff;
                    }
                    i += 1;
                }
            }

            if k < 0xeb3f {
                /* JIS X0208 */
                *p = LC_JISX0208 as c_uchar; p = p.add(1);
                *p = (((c1 & 0x3f) << 1) + 0x9f + (c2 > 0x9e) as c_int) as c_uchar; p = p.add(1);
                *p = (c2 + (if c2 > 0x9e { 2 } else { 0x60 }) + (c2 < 0x80) as c_int) as c_uchar; p = p.add(1);
            } else if (k >= 0xeb40 && k < 0xf040) || (k >= 0xfc4c && k <= 0xfcfc) {
                /* NEC selection IBM kanji - Other undecided justice */
                *p = LC_JISX0208 as c_uchar; p = p.add(1);
                *p = (PGEUCALTCODE >> 8) as c_uchar; p = p.add(1);
                *p = (PGEUCALTCODE & 0xff) as c_uchar; p = p.add(1);
            } else if k >= 0xf040 && k < 0xf540 {
                /*
                 * UDC1 mapping to X0208 85 ku - 94 ku JIS code 0x7521 -
                 * 0x7e7e EUC 0xf5a1 - 0xfefe
                 */
                *p = LC_JISX0208 as c_uchar; p = p.add(1);
                c1 -= 0x6f;
                *p = (((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e) as c_int) as c_uchar; p = p.add(1);
                *p = (c2 + (if c2 > 0x9e { 2 } else { 0x60 }) + (c2 < 0x80) as c_int) as c_uchar; p = p.add(1);
            } else if k >= 0xf540 && k < 0xfa40 {
                /*
                 * UDC2 mapping to X0212 85 ku - 94 ku JIS code 0x7521 -
                 * 0x7e7e EUC 0x8ff5a1 - 0x8ffefe
                 */
                *p = LC_JISX0212 as c_uchar; p = p.add(1);
                c1 -= 0x74;
                *p = (((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e) as c_int) as c_uchar; p = p.add(1);
                *p = (c2 + (if c2 > 0x9e { 2 } else { 0x60 }) + (c2 < 0x80) as c_int) as c_uchar; p = p.add(1);
            } else if k >= 0xfa40 {
                /*
                 * mapping IBM kanji to X0208 and X0212
                 */
                i = 0;
                loop {
                    k2 = IBMKANJI[i].sjis as c_int;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        k = IBMKANJI[i].euc;
                        if k >= 0x8f0000 {
                            *p = LC_JISX0212 as c_uchar; p = p.add(1);
                            *p = (0x80 | ((k & 0xff00) >> 8)) as c_uchar; p = p.add(1);
                            *p = (0x80 | (k & 0xff)) as c_uchar; p = p.add(1);
                        } else {
                            *p = LC_JISX0208 as c_uchar; p = p.add(1);
                            *p = (0x80 | (k >> 8)) as c_uchar; p = p.add(1);
                            *p = (0x80 | (k & 0xff)) as c_uchar; p = p.add(1);
                        }
                    }
                    i += 1;
                }
            }
            sjis = sjis.add(2);
            len -= 2;
        } else {
            /* should be ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_SJIS as c_int, sjis as *const c_char, len);
            }
            *p = c1 as c_uchar; p = p.add(1);
            sjis = sjis.add(1);
            len -= 1;
        }
    }
    *p = b'\0';

    sjis.offset_from(start) as c_int
}

/*
 * MIC ---> SJIS
 */
unsafe fn mic2sjis(mic: *const c_uchar, mut p: *mut c_uchar, mut len: c_int, no_error: bool) -> c_int {
    let start = mic;
    let mut mic = mic;
    let mut c1: c_int;
    let mut c2: c_int;
    let mut k: c_int;
    let l: c_int;

    while len > 0 {
        c1 = *mic as c_int;
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_MULE_INTERNAL as c_int, mic as *const c_char, len);
            }
            *p = c1 as c_uchar; p = p.add(1);
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
        if c1 == LC_JISX0201K as c_int {
            *p = *mic.add(1); p = p.add(1);
        } else if c1 == LC_JISX0208 as c_int {
            c1 = *mic.add(1) as c_int;
            c2 = *mic.add(2) as c_int;
            k = (c1 << 8) | (c2 & 0xff);
            if k >= 0xf5a1 {
                /* UDC1 */
                c1 -= 0x54;
                *p = (((c1 - 0xa1) >> 1) + (if c1 < 0xdf { 0x81 } else { 0xc1 }) + 0x6f) as c_uchar; p = p.add(1);
            } else {
                *p = (((c1 - 0xa1) >> 1) + (if c1 < 0xdf { 0x81 } else { 0xc1 })) as c_uchar; p = p.add(1);
            }
            *p = (c2 - (if (c1 & 1) != 0 { if c2 < 0xe0 { 0x61 } else { 0x60 } } else { 2 })) as c_uchar; p = p.add(1);
        } else if c1 == LC_JISX0212 as c_int {
            let mut i: usize;
            let mut k2: c_int;

            c1 = *mic.add(1) as c_int;
            c2 = *mic.add(2) as c_int;
            k = c1 << 8 | c2;
            if k >= 0xf5a1 {
                /* UDC2 */
                c1 -= 0x54;
                *p = (((c1 - 0xa1) >> 1) + (if c1 < 0xdf { 0x81 } else { 0xc1 }) + 0x74) as c_uchar; p = p.add(1);
                *p = (c2 - (if (c1 & 1) != 0 { if c2 < 0xe0 { 0x61 } else { 0x60 } } else { 2 })) as c_uchar; p = p.add(1);
            } else {
                /* IBM kanji */
                i = 0;
                loop {
                    k2 = IBMKANJI[i].euc & 0xffff;
                    if k2 == 0xffff {
                        *p = (PGSJISALTCODE >> 8) as c_uchar; p = p.add(1);
                        *p = (PGSJISALTCODE & 0xff) as c_uchar; p = p.add(1);
                        break;
                    }
                    if k2 == k {
                        k = IBMKANJI[i].sjis as c_int;
                        *p = (k >> 8) as c_uchar; p = p.add(1);
                        *p = (k & 0xff) as c_uchar; p = p.add(1);
                        break;
                    }
                    i += 1;
                }
            }
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(
                pg_enc::PG_MULE_INTERNAL as c_int,
                pg_enc::PG_SJIS as c_int,
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
 * EUC_JP ---> MIC
 */
unsafe fn euc_jp2mic(euc: *const c_uchar, mut p: *mut c_uchar, mut len: c_int, no_error: bool) -> c_int {
    let start = euc;
    let mut euc = euc;

    while len > 0 {
        let c1 = *euc as c_int;
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_EUC_JP as c_int, euc as *const c_char, len);
            }
            *p = c1 as c_uchar; p = p.add(1);
            euc = euc.add(1);
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(pg_enc::PG_EUC_JP as c_int, euc as *const c_char, len);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_EUC_JP as c_int, euc as *const c_char, len);
        }
        if c1 == SS2 as c_int {
            /* 1 byte kana? */
            *p = LC_JISX0201K as c_uchar; p = p.add(1);
            *p = *euc.add(1); p = p.add(1);
        } else if c1 == SS3 as c_int {
            /* JIS X0212 kanji? */
            *p = LC_JISX0212 as c_uchar; p = p.add(1);
            *p = *euc.add(1); p = p.add(1);
            *p = *euc.add(2); p = p.add(1);
        } else {
            /* kanji? */
            *p = LC_JISX0208 as c_uchar; p = p.add(1);
            *p = c1 as c_uchar; p = p.add(1);
            *p = *euc.add(1); p = p.add(1);
        }
        euc = euc.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    euc.offset_from(start) as c_int
}

/*
 * MIC ---> EUC_JP
 */
unsafe fn mic2euc_jp(mic: *const c_uchar, mut p: *mut c_uchar, mut len: c_int, no_error: bool) -> c_int {
    let start = mic;
    let mut mic = mic;

    while len > 0 {
        let c1 = *mic as c_int;
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_MULE_INTERNAL as c_int, mic as *const c_char, len);
            }
            *p = c1 as c_uchar; p = p.add(1);
            mic = mic.add(1);
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(pg_enc::PG_MULE_INTERNAL as c_int, mic as *const c_char, len);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_MULE_INTERNAL as c_int, mic as *const c_char, len);
        }
        if c1 == LC_JISX0201K as c_int {
            *p = SS2 as c_uchar; p = p.add(1);
            *p = *mic.add(1); p = p.add(1);
        } else if c1 == LC_JISX0212 as c_int {
            *p = SS3 as c_uchar; p = p.add(1);
            *p = *mic.add(1); p = p.add(1);
            *p = *mic.add(2); p = p.add(1);
        } else if c1 == LC_JISX0208 as c_int {
            *p = *mic.add(1); p = p.add(1);
            *p = *mic.add(2); p = p.add(1);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(
                pg_enc::PG_MULE_INTERNAL as c_int,
                pg_enc::PG_EUC_JP as c_int,
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
 * EUC_JP -> SJIS
 */
unsafe fn euc_jp2sjis(euc: *const c_uchar, mut p: *mut c_uchar, mut len: c_int, no_error: bool) -> c_int {
    let start = euc;
    let mut euc = euc;
    let mut c1: c_int;
    let mut c2: c_int;
    let mut k: c_int;

    while len > 0 {
        c1 = *euc as c_int;
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_EUC_JP as c_int, euc as *const c_char, len);
            }
            *p = c1 as c_uchar; p = p.add(1);
            euc = euc.add(1);
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(pg_enc::PG_EUC_JP as c_int, euc as *const c_char, len);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_EUC_JP as c_int, euc as *const c_char, len);
        }
        if c1 == SS2 as c_int {
            /* hankaku kana? */
            *p = *euc.add(1); p = p.add(1);
        } else if c1 == SS3 as c_int {
            /* JIS X0212 kanji? */
            c1 = *euc.add(1) as c_int;
            c2 = *euc.add(2) as c_int;
            k = c1 << 8 | c2;
            if k >= 0xf5a1 {
                /* UDC2 */
                c1 -= 0x54;
                *p = (((c1 - 0xa1) >> 1) + (if c1 < 0xdf { 0x81 } else { 0xc1 }) + 0x74) as c_uchar; p = p.add(1);
                *p = (c2 - (if (c1 & 1) != 0 { if c2 < 0xe0 { 0x61 } else { 0x60 } } else { 2 })) as c_uchar; p = p.add(1);
            } else {
                let mut i: usize;
                let mut k2: c_int;

                /* IBM kanji */
                i = 0;
                loop {
                    k2 = IBMKANJI[i].euc & 0xffff;
                    if k2 == 0xffff {
                        *p = (PGSJISALTCODE >> 8) as c_uchar; p = p.add(1);
                        *p = (PGSJISALTCODE & 0xff) as c_uchar; p = p.add(1);
                        break;
                    }
                    if k2 == k {
                        k = IBMKANJI[i].sjis as c_int;
                        *p = (k >> 8) as c_uchar; p = p.add(1);
                        *p = (k & 0xff) as c_uchar; p = p.add(1);
                        break;
                    }
                    i += 1;
                }
            }
        } else {
            /* JIS X0208 kanji? */
            c2 = *euc.add(1) as c_int;
            k = (c1 << 8) | (c2 & 0xff);
            if k >= 0xf5a1 {
                /* UDC1 */
                c1 -= 0x54;
                *p = (((c1 - 0xa1) >> 1) + (if c1 < 0xdf { 0x81 } else { 0xc1 }) + 0x6f) as c_uchar; p = p.add(1);
            } else {
                *p = (((c1 - 0xa1) >> 1) + (if c1 < 0xdf { 0x81 } else { 0xc1 })) as c_uchar; p = p.add(1);
            }
            *p = (c2 - (if (c1 & 1) != 0 { if c2 < 0xe0 { 0x61 } else { 0x60 } } else { 2 })) as c_uchar; p = p.add(1);
        }
        euc = euc.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    euc.offset_from(start) as c_int
}

/*
 * SJIS ---> EUC_JP
 */
unsafe fn sjis2euc_jp(sjis: *const c_uchar, mut p: *mut c_uchar, mut len: c_int, no_error: bool) -> c_int {
    let start = sjis;
    let mut sjis = sjis;
    let mut c1: c_int;
    let mut c2: c_int;
    let mut i: usize;
    let mut k: c_int;
    let mut k2: c_int;

    while len > 0 {
        c1 = *sjis as c_int;
        if !IS_HIGHBIT_SET(c1 as c_uchar) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(pg_enc::PG_SJIS as c_int, sjis as *const c_char, len);
            }
            *p = c1 as c_uchar; p = p.add(1);
            sjis = sjis.add(1);
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(pg_enc::PG_SJIS as c_int, sjis as *const c_char, len);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(pg_enc::PG_SJIS as c_int, sjis as *const c_char, len);
        }
        if c1 >= 0xa1 && c1 <= 0xdf {
            /* JIS X0201 (1 byte kana) */
            *p = SS2 as c_uchar; p = p.add(1);
            *p = c1 as c_uchar; p = p.add(1);
        } else {
            /*
             * JIS X0208, X0212, user defined extended characters
             */
            c2 = *sjis.add(1) as c_int;
            k = (c1 << 8) + c2;
            if k >= 0xed40 && k < 0xf040 {
                /* NEC selection IBM kanji */
                i = 0;
                loop {
                    k2 = IBMKANJI[i].nec as c_int;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        k = IBMKANJI[i].sjis as c_int;
                        c1 = (k >> 8) & 0xff;
                        c2 = k & 0xff;
                    }
                    i += 1;
                }
            }

            if k < 0xeb3f {
                /* JIS X0208 */
                *p = (((c1 & 0x3f) << 1) + 0x9f + (c2 > 0x9e) as c_int) as c_uchar; p = p.add(1);
                *p = (c2 + (if c2 > 0x9e { 2 } else { 0x60 }) + (c2 < 0x80) as c_int) as c_uchar; p = p.add(1);
            } else if (k >= 0xeb40 && k < 0xf040) || (k >= 0xfc4c && k <= 0xfcfc) {
                /* NEC selection IBM kanji - Other undecided justice */
                *p = (PGEUCALTCODE >> 8) as c_uchar; p = p.add(1);
                *p = (PGEUCALTCODE & 0xff) as c_uchar; p = p.add(1);
            } else if k >= 0xf040 && k < 0xf540 {
                /*
                 * UDC1 mapping to X0208 85 ku - 94 ku JIS code 0x7521 -
                 * 0x7e7e EUC 0xf5a1 - 0xfefe
                 */
                c1 -= 0x6f;
                *p = (((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e) as c_int) as c_uchar; p = p.add(1);
                *p = (c2 + (if c2 > 0x9e { 2 } else { 0x60 }) + (c2 < 0x80) as c_int) as c_uchar; p = p.add(1);
            } else if k >= 0xf540 && k < 0xfa40 {
                /*
                 * UDC2 mapping to X0212 85 ku - 94 ku JIS code 0x7521 -
                 * 0x7e7e EUC 0x8ff5a1 - 0x8ffefe
                 */
                *p = SS3 as c_uchar; p = p.add(1);
                c1 -= 0x74;
                *p = (((c1 & 0x3f) << 1) + 0xf3 + (c2 > 0x9e) as c_int) as c_uchar; p = p.add(1);
                *p = (c2 + (if c2 > 0x9e { 2 } else { 0x60 }) + (c2 < 0x80) as c_int) as c_uchar; p = p.add(1);
            } else if k >= 0xfa40 {
                /*
                 * mapping IBM kanji to X0208 and X0212
                 *
                 */
                i = 0;
                loop {
                    k2 = IBMKANJI[i].sjis as c_int;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        k = IBMKANJI[i].euc;
                        if k >= 0x8f0000 {
                            *p = SS3 as c_uchar; p = p.add(1);
                            *p = (0x80 | ((k & 0xff00) >> 8)) as c_uchar; p = p.add(1);
                            *p = (0x80 | (k & 0xff)) as c_uchar; p = p.add(1);
                        } else {
                            *p = (0x80 | (k >> 8)) as c_uchar; p = p.add(1);
                            *p = (0x80 | (k & 0xff)) as c_uchar; p = p.add(1);
                        }
                    }
                    i += 1;
                }
            }
        }
        sjis = sjis.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    sjis.offset_from(start) as c_int
}
