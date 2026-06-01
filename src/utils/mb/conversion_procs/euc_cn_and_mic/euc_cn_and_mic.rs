//! EUC_CN <--> MIC (utils/mb/conversion_procs/euc_cn_and_mic/euc_cn_and_mic.c).
//! 1:1 translation. PG_MODULE_MAGIC_EXT / PG_FUNCTION_INFO_V1 handled at wiring.

#![allow(non_upper_case_globals)]
use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};
use core::ffi::c_char;

// Encoding ids and the MULE leading byte (mb/pg_wchar.h).
use crate::mb::pg_wchar::{PG_EUC_CN, PG_MULE_INTERNAL, LC_GB2312_80};

// IS_HIGHBIT_SET(c): high bit set test (c.h).
#[inline]
fn IS_HIGHBIT_SET(c: u8) -> bool {
    (c & 0x80) != 0
}

// Error reporters (utils/mb/mbutils.c).
use crate::utils::mb::mbutils::{report_invalid_encoding, report_untranslatable_char};

// CHECK_ENCODING_CONVERSION_ARGS validates the conv_proc fixed args at the
// boundary; modeled by check_encoding_conversion_args.
use crate::utils::mb::mbutils::check_encoding_conversion_args;

pub unsafe fn euc_cn_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);

    check_encoding_conversion_args(PG_EUC_CN as i32, PG_MULE_INTERNAL as i32, len);

    let converted = euc_cn2mic(src, dest, len, no_error);

    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn mic_to_euc_cn(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);

    check_encoding_conversion_args(PG_MULE_INTERNAL as i32, PG_EUC_CN as i32, len);

    let converted = mic2euc_cn(src, dest, len, no_error);

    PG_RETURN_INT32!(fcinfo, converted)
}

/*
 * EUC_CN ---> MIC
 */
unsafe fn euc_cn2mic(mut euc: *const u8, mut p: *mut u8, mut len: i32, no_error: bool) -> i32 {
    let start = euc;
    let mut c1: u8;

    while len > 0 {
        c1 = *euc;
        if IS_HIGHBIT_SET(c1) {
            if len < 2 || !IS_HIGHBIT_SET(*euc.add(1)) {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_CN as i32, euc as *const c_char, len);
            }
            *p = LC_GB2312_80 as u8;
            p = p.add(1);
            *p = c1;
            p = p.add(1);
            *p = *euc.add(1);
            p = p.add(1);
            euc = euc.add(2);
            len -= 2;
        } else {
            /* should be ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_CN as i32, euc as *const c_char, len);
            }
            *p = c1;
            p = p.add(1);
            euc = euc.add(1);
            len -= 1;
        }
    }
    *p = b'\0';

    euc.offset_from(start) as i32
}

/*
 * MIC ---> EUC_CN
 */
unsafe fn mic2euc_cn(mut mic: *const u8, mut p: *mut u8, mut len: i32, no_error: bool) -> i32 {
    let start = mic;
    let mut c1: u8;

    while len > 0 {
        c1 = *mic;
        if IS_HIGHBIT_SET(c1) {
            if c1 != LC_GB2312_80 as u8 {
                if no_error {
                    break;
                }
                report_untranslatable_char(
                    PG_MULE_INTERNAL as i32,
                    PG_EUC_CN as i32,
                    mic as *const c_char,
                    len,
                );
            }
            if len < 3 || !IS_HIGHBIT_SET(*mic.add(1)) || !IS_HIGHBIT_SET(*mic.add(2)) {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL as i32, mic as *const c_char, len);
            }
            mic = mic.add(1);
            *p = *mic;
            p = p.add(1);
            mic = mic.add(1);
            *p = *mic;
            p = p.add(1);
            mic = mic.add(1);
            len -= 3;
        } else {
            /* should be ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL as i32, mic as *const c_char, len);
            }
            *p = c1;
            p = p.add(1);
            mic = mic.add(1);
            len -= 1;
        }
    }
    *p = b'\0';

    mic.offset_from(start) as i32
}
