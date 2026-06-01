//! EUC_KR <--> MIC (utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c).
//! 1:1 translation. PG_MODULE_MAGIC_EXT / PG_FUNCTION_INFO_V1 handled at wiring.

#![allow(non_upper_case_globals)]
use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};
use core::ffi::c_char;

use crate::mb::pg_wchar::{PG_EUC_KR, PG_MULE_INTERNAL, LC_KS5601};
use crate::utils::mb::mbutils::{
    check_encoding_conversion_args, pg_encoding_verifymbchar, report_invalid_encoding,
    report_untranslatable_char,
};

#[inline]
fn IS_HIGHBIT_SET(c: u8) -> bool {
    (c & 0x80) != 0
}

pub unsafe fn euc_kr_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);

    check_encoding_conversion_args(PG_EUC_KR as i32, PG_MULE_INTERNAL as i32, len);

    let converted = euc_kr2mic(src, dest, len, no_error);

    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn mic_to_euc_kr(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);

    check_encoding_conversion_args(PG_MULE_INTERNAL as i32, PG_EUC_KR as i32, len);

    let converted = mic2euc_kr(src, dest, len, no_error);

    PG_RETURN_INT32!(fcinfo, converted)
}

/*
 * EUC_KR ---> MIC
 */
unsafe fn euc_kr2mic(mut euc: *const u8, mut p: *mut u8, mut len: i32, no_error: bool) -> i32 {
    let start = euc;
    let mut c1: u8;
    let mut l: i32;

    while len > 0 {
        c1 = *euc;
        if IS_HIGHBIT_SET(c1) {
            l = pg_encoding_verifymbchar(PG_EUC_KR as i32, euc as *const c_char, len);
            if l != 2 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_KR as i32, euc as *const c_char, len);
            }
            *p = LC_KS5601 as u8;
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
                report_invalid_encoding(PG_EUC_KR as i32, euc as *const c_char, len);
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
 * MIC ---> EUC_KR
 */
unsafe fn mic2euc_kr(mut mic: *const u8, mut p: *mut u8, mut len: i32, no_error: bool) -> i32 {
    let start = mic;
    let mut c1: u8;
    let mut l: i32;

    while len > 0 {
        c1 = *mic;
        if !IS_HIGHBIT_SET(c1) {
            /* ASCII */
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
            continue;
        }
        l = pg_encoding_verifymbchar(PG_MULE_INTERNAL as i32, mic as *const c_char, len);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_MULE_INTERNAL as i32, mic as *const c_char, len);
        }
        if c1 == LC_KS5601 as u8 {
            *p = *mic.add(1);
            p = p.add(1);
            *p = *mic.add(2);
            p = p.add(1);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(
                PG_MULE_INTERNAL as i32,
                PG_EUC_KR as i32,
                mic as *const c_char,
                len,
            );
        }
        mic = mic.add(l as usize);
        len -= l;
    }
    *p = b'\0';

    mic.offset_from(start) as i32
}
