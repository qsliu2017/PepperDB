//! LATIN1/3/4 <--> MIC (utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c).
//! 1:1 translation. PG_MODULE_MAGIC_EXT / PG_FUNCTION_INFO_V1 handled at wiring.

#![allow(non_upper_case_globals)]
use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_INT32, PG_RETURN_INT32};

use crate::mb::pg_wchar::{
    PG_LATIN1, PG_LATIN3, PG_LATIN4, PG_MULE_INTERNAL, LC_ISO8859_1, LC_ISO8859_3, LC_ISO8859_4,
};
use crate::utils::mb::conv::{latin2mic, mic2latin};
use crate::utils::mb::mbutils::check_encoding_conversion_args;

pub unsafe fn latin1_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_LATIN1 as i32, PG_MULE_INTERNAL as i32, len);
    let converted = latin2mic(src, dest, len, LC_ISO8859_1 as i32, PG_LATIN1 as i32, no_error);
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn mic_to_latin1(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_MULE_INTERNAL as i32, PG_LATIN1 as i32, len);
    let converted = mic2latin(src, dest, len, LC_ISO8859_1 as i32, PG_LATIN1 as i32, no_error);
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn latin3_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_LATIN3 as i32, PG_MULE_INTERNAL as i32, len);
    let converted = latin2mic(src, dest, len, LC_ISO8859_3 as i32, PG_LATIN3 as i32, no_error);
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn mic_to_latin3(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_MULE_INTERNAL as i32, PG_LATIN3 as i32, len);
    let converted = mic2latin(src, dest, len, LC_ISO8859_3 as i32, PG_LATIN3 as i32, no_error);
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn latin4_to_mic(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_LATIN4 as i32, PG_MULE_INTERNAL as i32, len);
    let converted = latin2mic(src, dest, len, LC_ISO8859_4 as i32, PG_LATIN4 as i32, no_error);
    PG_RETURN_INT32!(fcinfo, converted)
}

pub unsafe fn mic_to_latin4(fcinfo: FunctionCallInfo) -> Datum {
    let src = PG_GETARG_CSTRING!(fcinfo, 2) as *mut u8;
    let dest = PG_GETARG_CSTRING!(fcinfo, 3) as *mut u8;
    let len = PG_GETARG_INT32!(fcinfo, 4);
    let no_error = PG_GETARG_BOOL!(fcinfo, 5);
    check_encoding_conversion_args(PG_MULE_INTERNAL as i32, PG_LATIN4 as i32, len);
    let converted = mic2latin(src, dest, len, LC_ISO8859_4 as i32, PG_LATIN4 as i32, no_error);
    PG_RETURN_INT32!(fcinfo, converted)
}
