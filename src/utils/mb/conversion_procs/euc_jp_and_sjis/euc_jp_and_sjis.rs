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
    report_untranslatable_char,
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
