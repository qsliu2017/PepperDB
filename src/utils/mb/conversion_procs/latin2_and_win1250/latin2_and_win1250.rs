//! LATIN2 / WIN1250 <--> MIC (utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c).
//! 1:1 translation. PG_MODULE_MAGIC_EXT / PG_FUNCTION_INFO_V1 handled at wiring.

#![allow(non_upper_case_globals)]
use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};

use crate::mb::pg_wchar::{PG_LATIN2, PG_MULE_INTERNAL, PG_WIN1250, LC_ISO8859_2};
use crate::utils::mb::conv::{
    latin2mic, latin2mic_with_table, local2local, mic2latin, mic2latin_with_table,
};
use crate::utils::mb::mbutils::check_encoding_conversion_args;

/* WIN1250 to ISO-8859-2 */
static win1250_2_iso88592: [u8; 128] = [
    0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0xA9, 0x8B, 0xA6, 0xAB, 0xAE, 0xAC,
    0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0xB9, 0x9B, 0xB6, 0xBB, 0xBE, 0xBC,
    0xA0, 0xB7, 0xA2, 0xA3, 0xA4, 0xA1, 0x00, 0xA7, 0xA8, 0x00, 0xAA, 0x00, 0x00, 0xAD, 0x00, 0xAF,
    0xB0, 0x00, 0xB2, 0xB3, 0xB4, 0x00, 0x00, 0x00, 0xB8, 0xB1, 0xBA, 0x00, 0xA5, 0xBD, 0xB5, 0xBF,
    0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF,
    0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF,
    0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF,
    0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF,
];

/* ISO-8859-2 to WIN1250 */
static iso88592_2_win1250: [u8; 128] = [
    0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x00, 0x8B, 0x00, 0x00, 0x00, 0x00,
    0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x00, 0x9B, 0x00, 0x00, 0x00, 0x00,
    0xA0, 0xA5, 0xA2, 0xA3, 0xA4, 0xBC, 0x8C, 0xA7, 0xA8, 0x8A, 0xAA, 0x8D, 0x8F, 0xAD, 0x8E, 0xAF,
    0xB0, 0xB9, 0xB2, 0xB3, 0xB4, 0xBE, 0x9C, 0xA1, 0xB8, 0x9A, 0xBA, 0x9D, 0x9F, 0xBD, 0x9E, 0xBF,
    0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF,
    0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF,
    0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF,
    0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF,
];

pub unsafe fn latin2_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_LATIN2 as i32, PG_MULE_INTERNAL as i32, len);
    let converted = latin2mic(src, dest, len, LC_ISO8859_2 as i32, PG_LATIN2 as i32, no_error);
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn mic_to_latin2(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_MULE_INTERNAL as i32, PG_LATIN2 as i32, len);
    let converted = mic2latin(src, dest, len, LC_ISO8859_2 as i32, PG_LATIN2 as i32, no_error);
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn win1250_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_WIN1250 as i32, PG_MULE_INTERNAL as i32, len);
    let converted = latin2mic_with_table(
        src, dest, len, LC_ISO8859_2 as i32, PG_WIN1250 as i32,
        win1250_2_iso88592.as_ptr(), no_error,
    );
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn mic_to_win1250(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_MULE_INTERNAL as i32, PG_WIN1250 as i32, len);
    let converted = mic2latin_with_table(
        src, dest, len, LC_ISO8859_2 as i32, PG_WIN1250 as i32,
        iso88592_2_win1250.as_ptr(), no_error,
    );
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn latin2_to_win1250(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_LATIN2 as i32, PG_WIN1250 as i32, len);
    let converted = local2local(
        src, dest, len, PG_LATIN2 as i32, PG_WIN1250 as i32,
        iso88592_2_win1250.as_ptr(), no_error,
    );
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn win1250_to_latin2(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_WIN1250 as i32, PG_LATIN2 as i32, len);
    let converted = local2local(
        src, dest, len, PG_WIN1250 as i32, PG_LATIN2 as i32,
        win1250_2_iso88592.as_ptr(), no_error,
    );
    PG_RETURN_INT32!(fcinfo, converted)
}
