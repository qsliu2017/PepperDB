//! formatting.rs
//!   Formatting and conversion routines (to_char/to_date/to_number).
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/formatting.c
//!
//!   TO_CHAR(); TO_TIMESTAMP(); TO_DATE(); TO_NUMBER();
//!
//!   The PostgreSQL routines for a timestamp/int/float/numeric formatting,
//!   inspired by the Oracle TO_CHAR() / TO_DATE() / TO_NUMBER() routines.
//!
//!   Karel Zak
//!
//! Portions Copyright (c) 1999-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994-5, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h               -> crate::prelude::*
//!   <ctype.h>/<math.h>/...   -> libc/libm via extern "C" + local consts
//!   catalog/pg_collation.h   -> stubbed below (DEFAULT_COLLATION_OID unused here)
//!   catalog/pg_type.h        -> crate::catalog::pg_type_d
//!   common/int.h             -> crate::common::int (pg_*_s32_overflow)
//!   mb/pg_wchar.h            -> crate::mb::{pg_wchar, mbutils} (MAX_MULTIBYTE_CHAR_LEN/PG_UTF8/pg_mblen/...)
//!   nodes/miscnodes.h        -> stubbed below (Node/ErrorSaveContext/SOFT_ERROR_OCCURRED)
//!   parser/scansup.h         -> scanner_isspace (stubbed below)
//!   utils/builtins.h         -> cstring_to_text/text_to_cstring + int*out/dtoi8 stubs
//!   utils/date.h             -> crate::utils::adt::date (DateADT/TimeADT/TimeTzADT + consts)
//!   utils/datetime.h         -> crate::utils::adt::datetime + stubs (date2j/j2date/ValidateDate/...)
//!   utils/formatting.h       -> THIS FILE (str_tolower/str_toupper/str_initcap/asc_* exports)
//!   utils/memutils.h         -> TopMemoryContext (prelude)
//!   utils/numeric.h          -> stubbed below (Numeric/numeric_in/numeric_out/... -- numeric.c not ported)
//!   utils/pg_locale.h        -> stubbed below (pg_locale_t/pg_newlocale_from_collation/pg_str*/PGLC_localeconv/cache_locale_time/localized_*)
//!   varatt.h                 -> crate::varatt (VARSIZE_ANY_EXHDR/VARDATA_ANY/VARDATA/SET_VARSIZE)
//!
//! NOTE: numeric.c, large parts of datetime.c/timestamp.c, and pg_locale.c are
//! not yet ported, so the broken-down-time decode/encode helpers, the
//! Numeric/NumericVar API, the locale plumbing, and the localized day/month
//! caches are declared here as minimal local stubs marked `// TODO(pg-port)`.
//! Each stub names the file where the real symbol will eventually live.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::prelude::*;

use crate::catalog::pg_type_d::{
    DATEOID, INTERVALOID, NUMERICOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
};
use crate::common::int::{
    pg_add_s32_overflow, pg_mul_s32_overflow, pg_sub_s32_overflow,
};
use crate::mb::mbutils::{
    pg_mblen, pg_mblen_cstr, pg_mblen_range, pg_mbstrlen, GetDatabaseEncoding,
};
use crate::mb::pg_wchar::{MAX_MULTIBYTE_CHAR_LEN, PG_UTF8};
use crate::pgtime::{pg_tm, pg_tz, session_timezone};
use crate::port::pgstrcasecmp::{pg_ascii_tolower, pg_ascii_toupper};
use crate::utils::adt::date::{
    fsec_t, DateADT, Interval, TimeADT, TimeTzADT, Timestamp, TimestampTz, HOURS_PER_DAY,
    MAX_TZDISP_HOUR, MINS_PER_HOUR, POSTGRES_EPOCH_JDATE, SECS_PER_HOUR, SECS_PER_MINUTE,
    USECS_PER_SEC,
};
use crate::utils::adt::datetime::{
    days, isleap, months, DAYS_PER_MONTH, DTERR_FIELD_OVERFLOW, DTERR_TZDISP_OVERFLOW, MONTHS_PER_YEAR,
};
use crate::utils::adt::varlena::{cstring_to_text, text_to_cstring};
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR, VARHDRSZ};

use crate::utils::fmgr::FunctionCallInfo;

use crate::{
    PG_RETURN_NULL,
    DirectFunctionCall1, DirectFunctionCall2, DirectFunctionCall3, PG_GETARG_DATUM,
};

use std::ffi::{c_char, c_int, c_uchar, c_void};

// ===========================================================================
// libc / libm bindings used pervasively below.
// ===========================================================================

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strcat(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strtol(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_long;
    fn isspace(c: c_int) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn isalpha(c: c_int) -> c_int;
    /* port/strlcpy.c */
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;
    /* errno access (macOS) */
    #[link_name = "__error"]
    fn __error() -> *mut c_int;
    /* math */
    fn pow(x: f64, y: f64) -> f64;
    fn rint(x: f64) -> f64;
    fn fabs(x: f64) -> f64;
    fn isnan(x: f64) -> c_int;
    fn isinf(x: f64) -> c_int;
}

const ERANGE: c_int = 34;
const INT_MIN_C: c_long = i32::MIN as c_long;
const INT_MAX_C: c_long = i32::MAX as c_long;
const INT_MAX: c_int = c_int::MAX;
const PG_INT32_MAX: int32 = i32::MAX;
const PG_INT32_MIN: int32 = i32::MIN;
const FLT_DIG: c_int = 6;
const DBL_DIG: c_int = 15;

#[inline]
unsafe fn get_errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    *__error() = v;
}

/* cstr: render a C string for elog!/ereport! formatting */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

/* abs() helper matching C int abs semantics */
#[inline]
fn abs_i32(x: c_int) -> c_int {
    x.unsigned_abs() as c_int
}

/* OidIsValid (c.h) */
#[inline]
fn OidIsValid(o: Oid) -> bool {
    o != InvalidOid
}

/* Min/Max (c.h) */
#[inline]
fn min_i32(a: c_int, b: c_int) -> c_int {
    if a < b { a } else { b }
}

// ===========================================================================
// TODO(pg-port) stubs for symbols whose real home is elsewhere.
// ===========================================================================

// --- nodes/miscnodes.h: soft-error infrastructure (not yet ported) ----------
// TODO(pg-port): real Node/ErrorSaveContext live in crate::nodes; soft-error
// reporting is approximated by always throwing (ereturn/errsave -> ereport ERROR).
pub type Node = c_void;

#[inline]
unsafe fn SOFT_ERROR_OCCURRED(_escontext: *mut Node) -> bool {
    false
}

// --- parser/scansup.h -------------------------------------------------------
// TODO(pg-port): real scanner_isspace lives in crate::parser::scansup.
#[inline]
unsafe fn scanner_isspace(ch: c_char) -> bool {
    matches!(ch as u8, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

// --- utils/numeric.h: numeric.c is NOT ported. Minimal Numeric API stubs ----
// TODO(pg-port): real Numeric/numeric_* live in crate::utils::adt::numeric.
pub type Numeric = *mut c_void; /* opaque varlena numeric */

#[inline]
unsafe fn DatumGetNumeric(d: Datum) -> Numeric {
    DatumGetPointer(d) as Numeric
}
#[inline]
unsafe fn NumericGetDatum(n: Numeric) -> Datum {
    PointerGetDatum(n as *const c_void)
}

// numeric_in/out/out_sci/power/mul/round/int4_opt_error: real homes in numeric.c.
unsafe fn numeric_in(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("numeric_in: numeric.c not yet ported")
}
unsafe fn numeric_out(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("numeric_out: numeric.c not yet ported")
}
unsafe fn numeric_power(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("numeric_power: numeric.c not yet ported")
}
unsafe fn numeric_mul(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("numeric_mul: numeric.c not yet ported")
}
unsafe fn numeric_round(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("numeric_round: numeric.c not yet ported")
}
unsafe fn numeric_out_sci(_value: Numeric, _scale: c_int) -> *mut c_char {
    unimplemented!("numeric_out_sci: numeric.c not yet ported")
}
unsafe fn numeric_int4_opt_error(_value: Numeric, _have_error: *mut bool) -> int32 {
    unimplemented!("numeric_int4_opt_error: numeric.c not yet ported")
}
unsafe fn int64_to_numeric(_val: int64) -> Numeric {
    unimplemented!("int64_to_numeric: numeric.c not yet ported")
}

// --- utils/builtins.h int/float output + dtoi8 (int.c/int8.c/float.c) -------
// TODO(pg-port): real int4out/int8out/int8mul/dtoi8 live in crate::utils::adt::{int,int8,float}.
unsafe fn int4out(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("int4out: int.c not yet ported")
}
unsafe fn int8out(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("int8out: int8.c not yet ported")
}
unsafe fn int8mul(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("int8mul: int8.c not yet ported")
}
unsafe fn dtoi8(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("dtoi8: int8.c not yet ported")
}

// --- psprintf (utils/mb/stringinfo / common) --------------------------------
// TODO(pg-port): real psprintf lives in crate::common (psprintf.c).  We only
// use it with the format strings "%+.*e", "%.0f", "%.*f"; implement minimally.
unsafe fn psprintf_e(scale: c_int, val: f64) -> *mut c_char {
    let s = format!("{:+.*e}", scale as usize, val);
    // Rust's {:e} yields e.g. "1.5e2"; C's %e yields "1.5e+02".  Normalize.
    let s = normalize_exp(&s);
    cstring_pstrdup(&s)
}
unsafe fn psprintf_f0(val: f64) -> *mut c_char {
    cstring_pstrdup(&format!("{:.0}", val))
}
unsafe fn psprintf_f(scale: c_int, val: f64) -> *mut c_char {
    cstring_pstrdup(&format!("{:.*}", scale as usize, val))
}

/* Turn Rust "1.5e2"/"1.5e-2" into C printf "%e"-style "1.5e+02". */
fn normalize_exp(s: &str) -> std::string::String {
    if let Some(epos) = s.find(['e', 'E']) {
        let (mantissa, exp) = s.split_at(epos);
        let exp = &exp[1..];
        let (sign, digits) = if let Some(rest) = exp.strip_prefix('-') {
            ("-", rest)
        } else if let Some(rest) = exp.strip_prefix('+') {
            ("+", rest)
        } else {
            ("+", exp)
        };
        let digits = if digits.len() < 2 {
            format!("{:0>2}", digits)
        } else {
            digits.to_string()
        };
        format!("{}e{}{}", mantissa, sign, digits)
    } else {
        s.to_string()
    }
}

/* palloc a NUL-terminated copy of a Rust string */
unsafe fn cstring_pstrdup(s: &str) -> *mut c_char {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let dst = palloc(len + 1) as *mut c_char;
    if len > 0 {
        memcpy(dst as *mut c_void, bytes.as_ptr() as *const c_void, len);
    }
    *dst.add(len) = 0;
    dst
}

// --- pg_locale.h: locale plumbing (pg_locale.c not yet ported) --------------
// TODO(pg-port): real pg_locale_t/pg_newlocale_from_collation/pg_str*/lconv/
// PGLC_localeconv live in crate::utils::adt::pg_locale.
#[repr(C)]
pub struct pg_locale_struct {
    pub ctype_is_c: bool,
}
pub type pg_locale_t = *mut pg_locale_struct;

unsafe fn pg_newlocale_from_collation(_collid: Oid) -> pg_locale_t {
    unimplemented!("pg_newlocale_from_collation: pg_locale.c not yet ported")
}
unsafe fn pg_strlower(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: usize,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!("pg_strlower: pg_locale.c not yet ported")
}
unsafe fn pg_strupper(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: usize,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!("pg_strupper: pg_locale.c not yet ported")
}
unsafe fn pg_strtitle(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: usize,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!("pg_strtitle: pg_locale.c not yet ported")
}
unsafe fn pg_strfold(
    _dst: *mut c_char,
    _dstsize: usize,
    _src: *const c_char,
    _srclen: usize,
    _locale: pg_locale_t,
) -> usize {
    unimplemented!("pg_strfold: pg_locale.c not yet ported")
}

#[repr(C)]
pub struct lconv {
    pub decimal_point: *const c_char,
    pub thousands_sep: *const c_char,
    pub currency_symbol: *const c_char,
    pub positive_sign: *const c_char,
    pub negative_sign: *const c_char,
}
unsafe fn PGLC_localeconv() -> *mut lconv {
    unimplemented!("PGLC_localeconv: pg_locale.c not yet ported")
}

// cache_locale_time + localized day/month caches (pg_locale.c).
unsafe fn cache_locale_time() {
    // TODO(pg-port): real cache_locale_time lives in crate::utils::adt::pg_locale.
}
// These are filled by cache_locale_time(); declared as static-mut arrays of
// NUL-terminated C strings (one per weekday / month).
static mut localized_full_days: [*mut c_char; 8] = [null_mut(); 8];
static mut localized_abbrev_days: [*mut c_char; 8] = [null_mut(); 8];
static mut localized_full_months: [*mut c_char; 13] = [null_mut(); 13];
static mut localized_abbrev_months: [*mut c_char; 13] = [null_mut(); 13];

// --- utils/datetime.h + timestamp.c date/time helpers (not fully ported) ----
// TODO(pg-port): real homes are crate::utils::adt::{datetime, timestamp, date}.
unsafe fn date2j(_y: c_int, _m: c_int, _d: c_int) -> c_int {
    unimplemented!("date2j: datetime.c helper not yet ported")
}
unsafe fn j2date(_jd: c_int, _year: *mut c_int, _month: *mut c_int, _day: *mut c_int) {
    unimplemented!("j2date: datetime.c helper not yet ported")
}
unsafe fn date2isoyear(_year: c_int, _mon: c_int, _mday: c_int) -> c_int {
    unimplemented!("date2isoyear: timestamp.c helper not yet ported")
}
unsafe fn date2isoweek(_year: c_int, _mon: c_int, _mday: c_int) -> c_int {
    unimplemented!("date2isoweek: timestamp.c helper not yet ported")
}
unsafe fn date2isoyearday(_year: c_int, _mon: c_int, _mday: c_int) -> c_int {
    unimplemented!("date2isoyearday: timestamp.c helper not yet ported")
}
unsafe fn isoweek2date(_woy: c_int, _year: *mut c_int, _mon: *mut c_int, _mday: *mut c_int) {
    unimplemented!("isoweek2date: timestamp.c helper not yet ported")
}
unsafe fn isoweek2j(_year: c_int, _week: c_int) -> c_int {
    unimplemented!("isoweek2j: timestamp.c helper not yet ported")
}
unsafe fn isoweekdate2date(
    _isoweek: c_int,
    _wday: c_int,
    _year: *mut c_int,
    _mon: *mut c_int,
    _mday: *mut c_int,
) {
    unimplemented!("isoweekdate2date: timestamp.c helper not yet ported")
}
unsafe fn ValidateDate(
    _fmask: c_int,
    _isjulian: bool,
    _is2digits: bool,
    _bc: bool,
    _tm: *mut pg_tm,
) -> c_int {
    unimplemented!("ValidateDate: datetime.c helper not yet ported")
}
unsafe fn DateTimeParseError(
    _dterr: c_int,
    _extra: *mut c_void,
    _str: *const c_char,
    _datatype: *const c_char,
    _escontext: *mut Node,
) {
    unimplemented!("DateTimeParseError: datetime.c helper not yet ported")
}
unsafe fn DecodeTimezoneAbbrevPrefix(
    _str: *const c_char,
    _offset: *mut c_int,
    _tz: *mut *mut pg_tz,
) -> c_int {
    unimplemented!("DecodeTimezoneAbbrevPrefix: datetime.c helper not yet ported")
}
unsafe fn DetermineTimeZoneOffset(_tm: *mut pg_tm, _tzp: *mut pg_tz) -> c_int {
    unimplemented!("DetermineTimeZoneOffset: datetime.c helper not yet ported")
}
unsafe fn DetermineTimeZoneAbbrevOffset(
    _tm: *mut pg_tm,
    _abbr: *const c_char,
    _tzp: *mut pg_tz,
) -> c_int {
    unimplemented!("DetermineTimeZoneAbbrevOffset: datetime.c helper not yet ported")
}

// timestamp.c: pg_itm (interval broken-down time) + interval helpers.
#[repr(C)]
pub struct pg_itm {
    pub tm_usec: c_int,
    pub tm_sec: c_int,
    pub tm_min: c_int,
    pub tm_hour: int64,
    pub tm_mday: c_int,
    pub tm_mon: c_int,
    pub tm_year: c_int,
}
unsafe fn interval2itm(_span: Interval, _itm: *mut pg_itm) {
    unimplemented!("interval2itm: timestamp.c helper not yet ported")
}
unsafe fn timestamp2tm(
    _dt: Timestamp,
    _tzp: *mut c_int,
    _tm: *mut pg_tm,
    _fsec: *mut fsec_t,
    _tzn: *mut *const c_char,
    _attimezone: *mut pg_tz,
) -> c_int {
    unimplemented!("timestamp2tm: timestamp.c helper not yet ported")
}
unsafe fn tm2timestamp(
    _tm: *mut pg_tm,
    _fsec: fsec_t,
    _tzp: *mut c_int,
    _result: *mut Timestamp,
) -> c_int {
    unimplemented!("tm2timestamp: timestamp.c helper not yet ported")
}
unsafe fn tm2time(_tm: *mut pg_tm, _fsec: fsec_t, _result: *mut TimeADT) -> c_int {
    unimplemented!("tm2time: date.c helper not yet ported")
}
unsafe fn tm2timetz(_tm: *mut pg_tm, _fsec: fsec_t, _tz: c_int, _result: *mut TimeTzADT) -> c_int {
    unimplemented!("tm2timetz: date.c helper not yet ported")
}
unsafe fn AdjustTimestampForTypmod(_time: *mut Timestamp, _typmod: int32, _escontext: *mut Node) {
    unimplemented!("AdjustTimestampForTypmod: timestamp.c helper not yet ported")
}
unsafe fn AdjustTimeForTypmod(_time: *mut TimeADT, _typmod: int32) {
    unimplemented!("AdjustTimeForTypmod: date.c helper not yet ported")
}

/* TIMESTAMP_NOT_FINITE / INTERVAL_NOT_FINITE (timestamp.h) */
const DT_NOBEGIN: int64 = i64::MIN;
const DT_NOEND: int64 = i64::MAX;
#[inline]
fn TIMESTAMP_NOT_FINITE(t: Timestamp) -> bool {
    t == DT_NOBEGIN || t == DT_NOEND
}
#[inline]
unsafe fn INTERVAL_NOT_FINITE(_i: *mut Interval) -> bool {
    // TODO(pg-port): real INTERVAL_NOT_FINITE checks the time/day/month fields;
    // exact field layout lives in crate::utils::adt::date::Interval.
    false
}

/* IS_VALID_JULIAN / IS_VALID_DATE (datetime.h) */
const JULIAN_MINYEAR: c_int = -4713;
const JULIAN_MINMONTH: c_int = 11;
const JULIAN_MINDAY: c_int = 24;
const JULIAN_MAXYEAR: c_int = 5874898;
const JULIAN_MAXMONTH: c_int = 6;
const JULIAN_MAXDAY: c_int = 3;
#[inline]
fn IS_VALID_JULIAN(y: c_int, m: c_int, d: c_int) -> bool {
    (y > JULIAN_MINYEAR
        || (y == JULIAN_MINYEAR
            && (m > JULIAN_MINMONTH || (m == JULIAN_MINMONTH && d >= JULIAN_MINDAY))))
        && (y < JULIAN_MAXYEAR
            || (y == JULIAN_MAXYEAR
                && (m < JULIAN_MAXMONTH || (m == JULIAN_MAXMONTH && d < JULIAN_MAXDAY))))
}
const DATEVAL_NOBEGIN: DateADT = i32::MIN;
const DATEVAL_NOEND: DateADT = i32::MAX;
#[inline]
fn IS_VALID_DATE(d: DateADT) -> bool {
    POSTGRES_EPOCH_JDATE.wrapping_add(DATEVAL_NOBEGIN) <= d && d < POSTGRES_EPOCH_JDATE.wrapping_add(DATEVAL_NOEND)
}

/* DTK_M / DTK_DATE_M and field codes (datetime.h) */
const YEAR: c_int = 1;
const MONTH: c_int = 3;
const DAY: c_int = 4;
#[inline]
fn DTK_M(t: c_int) -> c_int {
    1 << t
}
#[inline]
fn DTK_DATE_M() -> c_int {
    DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY)
}

/* FLOAT*_FITS_IN_INT32 (float.h) */
#[inline]
fn FLOAT4_FITS_IN_INT32(x: f32) -> bool {
    x >= -2147483648.0_f32 && x < 2147483648.0_f32
}
#[inline]
fn FLOAT8_FITS_IN_INT32(x: f64) -> bool {
    x >= -2147483648.0_f64 && x < 2147483648.0_f64
}

// ===========================================================================
// Local Datum-getter / -return macros (per-file, matching builtins.h).
// ===========================================================================
macro_rules! PG_GET_COLLATION {
    ($fcinfo:expr) => {
        crate::PG_GET_COLLATION!($fcinfo)
    };
}
macro_rules! PG_GETARG_TEXT_PP {
    ($fcinfo:expr, $n:expr) => {
        crate::PG_GETARG_DATUM!($fcinfo, $n) as *mut text
    };
}
macro_rules! PG_GETARG_INT32 {
    ($fcinfo:expr, $n:expr) => {
        DatumGetInt32(crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_INT64 {
    ($fcinfo:expr, $n:expr) => {
        DatumGetInt64(crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_FLOAT4 {
    ($fcinfo:expr, $n:expr) => {
        DatumGetFloat4(crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_FLOAT8 {
    ($fcinfo:expr, $n:expr) => {
        DatumGetFloat8(crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_NUMERIC {
    ($fcinfo:expr, $n:expr) => {
        DatumGetNumeric(crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_TIMESTAMP {
    ($fcinfo:expr, $n:expr) => {
        DatumGetInt64(crate::PG_GETARG_DATUM!($fcinfo, $n)) as Timestamp
    };
}
macro_rules! PG_GETARG_INTERVAL_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(crate::PG_GETARG_DATUM!($fcinfo, $n)) as *mut Interval
    };
}
macro_rules! PG_RETURN_TEXT_P {
    ($fcinfo:expr, $x:expr) => {
        return PointerGetDatum($x as *const c_void)
    };
}
macro_rules! PG_RETURN_TIMESTAMP {
    ($x:expr) => {
        return Int64GetDatum($x as int64)
    };
}
macro_rules! PG_RETURN_DATEADT {
    ($x:expr) => {
        return Int32GetDatum($x as int32)
    };
}

/* TimestampTzGetDatum / TimestampGetDatum / DateADTGetDatum / TimeADTGetDatum /
 * TimeTzADTPGetDatum (postgres.h/date.h convenience wrappers). */
#[inline]
fn TimestampTzGetDatum(x: TimestampTz) -> Datum {
    Int64GetDatum(x)
}
#[inline]
fn TimestampGetDatum(x: Timestamp) -> Datum {
    Int64GetDatum(x)
}
#[inline]
fn DateADTGetDatum(x: DateADT) -> Datum {
    Int32GetDatum(x)
}
#[inline]
fn TimeADTGetDatum(x: TimeADT) -> Datum {
    Int64GetDatum(x)
}
#[inline]
unsafe fn TimeTzADTPGetDatum(x: *const TimeTzADT) -> Datum {
    PointerGetDatum(x as *const c_void)
}

/* DatumGetFloat4/Float8 (postgres.h) -- not in prelude exports here. */
#[inline]
fn DatumGetFloat4(d: Datum) -> float4 {
    f32::from_bits(d as u32)
}
#[inline]
fn DatumGetFloat8(d: Datum) -> float8 {
    f64::from_bits(d as u64)
}

// ===========================================================================
// Routines flags
// ===========================================================================
const DCH_FLAG: uint32 = 0x1; /* DATE-TIME flag */
const NUM_FLAG: uint32 = 0x2; /* NUMBER flag */
const STD_FLAG: uint32 = 0x4; /* STANDARD flag */

/* KeyWord Index (ascii from position 32 (' ') to 126 (~)) */
const KeyWord_INDEX_SIZE: usize = (b'~' - b' ') as usize;
#[inline]
fn KeyWord_INDEX_FILTER(c: c_char) -> bool {
    !(c <= b' ' as c_char || c >= b'~' as c_char)
}

/* Maximal length of one node */
const DCH_MAX_ITEM_SIZ: usize = 12; /* max localized day name */
const NUM_MAX_ITEM_SIZ: usize = 8; /* roman number (RN has 15 chars) */

// ===========================================================================
// Format parser structs
// ===========================================================================
#[repr(C)]
pub struct KeySuffix {
    pub name: *const c_char, /* suffix string */
    pub len: c_int,          /* suffix length */
    pub id: c_int,           /* used in node->suffix */
    pub r#type: c_int,       /* prefix / postfix */
}

/*
 * FromCharDateMode
 *
 * This value is used to nominate one of several distinct (and mutually
 * exclusive) date conventions that a keyword can belong to.
 */
type FromCharDateMode = c_int;
const FROM_CHAR_DATE_NONE: FromCharDateMode = 0; /* Value does not affect date mode. */
const FROM_CHAR_DATE_GREGORIAN: FromCharDateMode = 1; /* Gregorian (day, month, year) style date */
const FROM_CHAR_DATE_ISOWEEK: FromCharDateMode = 2; /* ISO 8601 week date */

#[repr(C)]
pub struct KeyWord {
    pub name: *const c_char,
    pub len: c_int,
    pub id: c_int,
    pub is_digit: bool,
    pub date_mode: FromCharDateMode,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormatNode {
    pub r#type: uint8,                            /* NODE_TYPE_XXX, see below */
    pub character: [c_char; MAX_MULTIBYTE_CHAR_LEN as usize + 1], /* if type is CHAR */
    pub suffix: uint8,                            /* keyword prefix/suffix code, if any */
    pub key: *const KeyWord,                      /* if type is ACTION */
}

const NODE_TYPE_END: uint8 = 1;
const NODE_TYPE_ACTION: uint8 = 2;
const NODE_TYPE_CHAR: uint8 = 3;
const NODE_TYPE_SEPARATOR: uint8 = 4;
const NODE_TYPE_SPACE: uint8 = 5;

const SUFFTYPE_PREFIX: c_int = 1;
const SUFFTYPE_POSTFIX: c_int = 2;

const CLOCK_24_HOUR: c_int = 0;
const CLOCK_12_HOUR: c_int = 1;

// ===========================================================================
// Full months
// ===========================================================================
static mut months_full: [*const c_char; 13] = [
    c"January".as_ptr(),
    c"February".as_ptr(),
    c"March".as_ptr(),
    c"April".as_ptr(),
    c"May".as_ptr(),
    c"June".as_ptr(),
    c"July".as_ptr(),
    c"August".as_ptr(),
    c"September".as_ptr(),
    c"October".as_ptr(),
    c"November".as_ptr(),
    c"December".as_ptr(),
    null(),
];

static mut days_short: [*const c_char; 8] = [
    c"Sun".as_ptr(),
    c"Mon".as_ptr(),
    c"Tue".as_ptr(),
    c"Wed".as_ptr(),
    c"Thu".as_ptr(),
    c"Fri".as_ptr(),
    c"Sat".as_ptr(),
    null(),
];

/*
 * AD / BC
 *  There is no 0 AD.  Years go from 1 BC to 1 AD, so we make it positive and
 *  map year == -1 to year zero, and shift all negative years up one.  For
 *  interval years, we just return the year.
 */
#[inline]
fn ADJUST_YEAR(year: c_int, is_interval: bool) -> c_int {
    if is_interval {
        year
    } else if year <= 0 {
        -(year - 1)
    } else {
        year
    }
}

const A_D_STR: &std::ffi::CStr = c"A.D.";
const a_d_STR: &std::ffi::CStr = c"a.d.";
const AD_STR: &std::ffi::CStr = c"AD";
const ad_STR: &std::ffi::CStr = c"ad";

const B_C_STR: &std::ffi::CStr = c"B.C.";
const b_c_STR: &std::ffi::CStr = c"b.c.";
const BC_STR: &std::ffi::CStr = c"BC";
const bc_STR: &std::ffi::CStr = c"bc";

/* AD / BC strings for seq_search (see comment in C source). */
static mut adbc_strings: [*const c_char; 5] = [
    ad_STR.as_ptr(),
    bc_STR.as_ptr(),
    AD_STR.as_ptr(),
    BC_STR.as_ptr(),
    null(),
];
static mut adbc_strings_long: [*const c_char; 5] = [
    a_d_STR.as_ptr(),
    b_c_STR.as_ptr(),
    A_D_STR.as_ptr(),
    B_C_STR.as_ptr(),
    null(),
];

/* AM / PM */
const A_M_STR: &std::ffi::CStr = c"A.M.";
const a_m_STR: &std::ffi::CStr = c"a.m.";
const AM_STR: &std::ffi::CStr = c"AM";
const am_STR: &std::ffi::CStr = c"am";

const P_M_STR: &std::ffi::CStr = c"P.M.";
const p_m_STR: &std::ffi::CStr = c"p.m.";
const PM_STR: &std::ffi::CStr = c"PM";
const pm_STR: &std::ffi::CStr = c"pm";

static mut ampm_strings: [*const c_char; 5] = [
    am_STR.as_ptr(),
    pm_STR.as_ptr(),
    AM_STR.as_ptr(),
    PM_STR.as_ptr(),
    null(),
];
static mut ampm_strings_long: [*const c_char; 5] = [
    a_m_STR.as_ptr(),
    p_m_STR.as_ptr(),
    A_M_STR.as_ptr(),
    P_M_STR.as_ptr(),
    null(),
];

/*
 * Months in roman-numeral
 * (Must be in reverse order for seq_search (in FROM_CHAR), because
 *  'VIII' must have higher precedence than 'V')
 */
static mut rm_months_upper: [*const c_char; 13] = [
    c"XII".as_ptr(),
    c"XI".as_ptr(),
    c"X".as_ptr(),
    c"IX".as_ptr(),
    c"VIII".as_ptr(),
    c"VII".as_ptr(),
    c"VI".as_ptr(),
    c"V".as_ptr(),
    c"IV".as_ptr(),
    c"III".as_ptr(),
    c"II".as_ptr(),
    c"I".as_ptr(),
    null(),
];
static mut rm_months_lower: [*const c_char; 13] = [
    c"xii".as_ptr(),
    c"xi".as_ptr(),
    c"x".as_ptr(),
    c"ix".as_ptr(),
    c"viii".as_ptr(),
    c"vii".as_ptr(),
    c"vi".as_ptr(),
    c"v".as_ptr(),
    c"iv".as_ptr(),
    c"iii".as_ptr(),
    c"ii".as_ptr(),
    c"i".as_ptr(),
    null(),
];

/* Roman numerals */
static mut rm1: [*const c_char; 10] = [
    c"I".as_ptr(),
    c"II".as_ptr(),
    c"III".as_ptr(),
    c"IV".as_ptr(),
    c"V".as_ptr(),
    c"VI".as_ptr(),
    c"VII".as_ptr(),
    c"VIII".as_ptr(),
    c"IX".as_ptr(),
    null(),
];
static mut rm10: [*const c_char; 10] = [
    c"X".as_ptr(),
    c"XX".as_ptr(),
    c"XXX".as_ptr(),
    c"XL".as_ptr(),
    c"L".as_ptr(),
    c"LX".as_ptr(),
    c"LXX".as_ptr(),
    c"LXXX".as_ptr(),
    c"XC".as_ptr(),
    null(),
];
static mut rm100: [*const c_char; 10] = [
    c"C".as_ptr(),
    c"CC".as_ptr(),
    c"CCC".as_ptr(),
    c"CD".as_ptr(),
    c"D".as_ptr(),
    c"DC".as_ptr(),
    c"DCC".as_ptr(),
    c"DCCC".as_ptr(),
    c"CM".as_ptr(),
    null(),
];

/*
 * Check if the current and next characters form a valid subtraction
 * combination for roman numerals.
 */
#[inline]
fn IS_VALID_SUB_COMB(curr: c_char, next: c_char) -> bool {
    let c = curr as u8;
    let n = next as u8;
    (c == b'I' && (n == b'V' || n == b'X'))
        || (c == b'X' && (n == b'L' || n == b'C'))
        || (c == b'C' && (n == b'D' || n == b'M'))
}

/* Roman numeral value, or 0 if character isn't a roman numeral. */
#[inline]
fn ROMAN_VAL(r: c_char) -> c_int {
    match r as u8 {
        b'I' => 1,
        b'V' => 5,
        b'X' => 10,
        b'L' => 50,
        b'C' => 100,
        b'D' => 500,
        b'M' => 1000,
        _ => 0,
    }
}

/*
 * 'MMMDCCCLXXXVIII' (3888) is the longest valid roman numeral (15 characters).
 */
const MAX_ROMAN_LEN: usize = 15;

/* Ordinal postfixes */
static mut numTH: [*const c_char; 5] = [
    c"ST".as_ptr(),
    c"ND".as_ptr(),
    c"RD".as_ptr(),
    c"TH".as_ptr(),
    null(),
];
static mut numth: [*const c_char; 5] = [
    c"st".as_ptr(),
    c"nd".as_ptr(),
    c"rd".as_ptr(),
    c"th".as_ptr(),
    null(),
];

/* Flags & Options */
const TH_UPPER: c_int = 1;
const TH_LOWER: c_int = 2;

/* Number description struct */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NUMDesc {
    pub pre: c_int,           /* (count) numbers before decimal */
    pub post: c_int,          /* (count) numbers after decimal  */
    pub lsign: c_int,         /* want locales sign */
    pub flag: c_int,          /* number parameters */
    pub pre_lsign_num: c_int, /* tmp value for lsign */
    pub multi: c_int,         /* multiplier for 'V' */
    pub zero_start: c_int,    /* position of first zero */
    pub zero_end: c_int,      /* position of last zero */
    pub need_locale: c_int,   /* needs it locale */
}

/* Flags for NUMBER version */
const NUM_F_DECIMAL: c_int = 1 << 1;
const NUM_F_LDECIMAL: c_int = 1 << 2;
const NUM_F_ZERO: c_int = 1 << 3;
const NUM_F_BLANK: c_int = 1 << 4;
const NUM_F_FILLMODE: c_int = 1 << 5;
const NUM_F_LSIGN: c_int = 1 << 6;
const NUM_F_BRACKET: c_int = 1 << 7;
const NUM_F_MINUS: c_int = 1 << 8;
const NUM_F_PLUS: c_int = 1 << 9;
const NUM_F_ROMAN: c_int = 1 << 10;
const NUM_F_MULTI: c_int = 1 << 11;
const NUM_F_PLUS_POST: c_int = 1 << 12;
const NUM_F_MINUS_POST: c_int = 1 << 13;
const NUM_F_EEEE: c_int = 1 << 14;

const NUM_LSIGN_PRE: c_int = -1;
const NUM_LSIGN_POST: c_int = 1;
const NUM_LSIGN_NONE: c_int = 0;

/* Tests */
#[inline]
unsafe fn IS_DECIMAL(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_DECIMAL != 0
}
#[inline]
unsafe fn IS_LDECIMAL(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_LDECIMAL != 0
}
#[inline]
unsafe fn IS_ZERO(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_ZERO != 0
}
#[inline]
unsafe fn IS_BLANK(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_BLANK != 0
}
#[inline]
unsafe fn IS_FILLMODE(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_FILLMODE != 0
}
#[inline]
unsafe fn IS_BRACKET(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_BRACKET != 0
}
#[inline]
unsafe fn IS_MINUS(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_MINUS != 0
}
#[inline]
unsafe fn IS_LSIGN(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_LSIGN != 0
}
#[inline]
unsafe fn IS_PLUS(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_PLUS != 0
}
#[inline]
unsafe fn IS_ROMAN(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_ROMAN != 0
}
#[inline]
unsafe fn IS_MULTI(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_MULTI != 0
}
#[inline]
unsafe fn IS_EEEE(f: *const NUMDesc) -> bool {
    (*f).flag & NUM_F_EEEE != 0
}

/*
 * Format picture cache.  See comment in the C source for the size derivation.
 */
const DCH_CACHE_SIZE: usize = 136; /* (2048 - overhead) / (sizeof(FormatNode)+1) - 1, see C */
const NUM_CACHE_SIZE: usize = 64; /* (1024 - overhead) / (sizeof(FormatNode)+1) - 1, see C */

const DCH_CACHE_ENTRIES: usize = 20;
const NUM_CACHE_ENTRIES: usize = 20;

#[repr(C)]
pub struct DCHCacheEntry {
    pub format: [FormatNode; DCH_CACHE_SIZE + 1],
    pub str: [c_char; DCH_CACHE_SIZE + 1],
    pub std: bool,
    pub valid: bool,
    pub age: c_int,
}

#[repr(C)]
pub struct NUMCacheEntry {
    pub format: [FormatNode; NUM_CACHE_SIZE + 1],
    pub str: [c_char; NUM_CACHE_SIZE + 1],
    pub valid: bool,
    pub age: c_int,
    pub Num: NUMDesc,
}

/* global cache for date/time format pictures */
static mut DCHCache: [*mut DCHCacheEntry; DCH_CACHE_ENTRIES] = [null_mut(); DCH_CACHE_ENTRIES];
static mut n_DCHCache: c_int = 0; /* current number of entries */
static mut DCHCounter: c_int = 0; /* aging-event counter */

/* global cache for number format pictures */
static mut NUMCache: [*mut NUMCacheEntry; NUM_CACHE_ENTRIES] = [null_mut(); NUM_CACHE_ENTRIES];
static mut n_NUMCache: c_int = 0; /* current number of entries */
static mut NUMCounter: c_int = 0; /* aging-event counter */

/* For char->date/time conversion */
#[repr(C)]
pub struct TmFromChar {
    pub mode: FromCharDateMode,
    pub hh: c_int,
    pub pm: c_int,
    pub mi: c_int,
    pub ss: c_int,
    pub ssss: c_int,
    pub d: c_int, /* stored as 1-7, Sunday = 1, 0 means missing */
    pub dd: c_int,
    pub ddd: c_int,
    pub mm: c_int,
    pub ms: c_int,
    pub year: c_int,
    pub bc: c_int,
    pub ww: c_int,
    pub w: c_int,
    pub cc: c_int,
    pub j: c_int,
    pub us: c_int,
    pub yysz: c_int,   /* is it YY or YYYY ? */
    pub clock: c_int,  /* 12 or 24 hour clock? */
    pub tzsign: c_int, /* +1, -1, or 0 if no TZH/TZM fields */
    pub tzh: c_int,
    pub tzm: c_int,
    pub ff: c_int,        /* fractional precision */
    pub has_tz: bool,     /* was there a TZ field? */
    pub gmtoffset: c_int, /* GMT offset of fixed-offset zone abbrev */
    pub tzp: *mut pg_tz,  /* pg_tz for dynamic abbrev */
    pub abbrev: *mut c_char, /* dynamic abbrev */
}

#[inline]
unsafe fn ZERO_tmfc(x: *mut TmFromChar) {
    memset(x as *mut c_void, 0, core::mem::size_of::<TmFromChar>());
}

/* do_to_timestamp's timezone info output */
#[repr(C)]
pub struct fmt_tz {
    pub has_tz: bool,     /* was there any TZ/TZH/TZM field? */
    pub gmtoffset: c_int, /* GMT offset in seconds */
}

/*
 * Datetime to char conversion
 *
 * To support intervals as well as timestamps, we use a custom "tm" struct
 * that is almost like struct pg_tm, but has a 64-bit tm_hour field.
 */
#[repr(C)]
pub struct fmt_tm {
    pub tm_sec: c_int,
    pub tm_min: c_int,
    pub tm_hour: int64,
    pub tm_mday: c_int,
    pub tm_mon: c_int,
    pub tm_year: c_int,
    pub tm_wday: c_int,
    pub tm_yday: c_int,
    pub tm_gmtoff: c_long,
}

#[repr(C)]
pub struct TmToChar {
    pub tm: fmt_tm,         /* almost the classic 'tm' struct */
    pub fsec: fsec_t,       /* fractional seconds */
    pub tzn: *const c_char, /* timezone */
}

#[inline]
unsafe fn tmtcTm(x: *mut TmToChar) -> *mut fmt_tm {
    &raw mut (*x).tm
}
#[inline]
unsafe fn tmtcTzn(x: *mut TmToChar) -> *const c_char {
    (*x).tzn
}

#[inline]
unsafe fn ZERO_tm(x: *mut fmt_tm) {
    memset(x as *mut c_void, 0, core::mem::size_of::<fmt_tm>());
    (*x).tm_mday = 1;
    (*x).tm_mon = 1;
}
#[inline]
unsafe fn ZERO_tm_pg(x: *mut pg_tm) {
    memset(x as *mut c_void, 0, core::mem::size_of::<pg_tm>());
    (*x).tm_mday = 1;
    (*x).tm_mon = 1;
}
#[inline]
unsafe fn ZERO_tmtc(x: *mut TmToChar) {
    ZERO_tm(tmtcTm(x));
    (*x).fsec = 0;
    (*x).tzn = null();
}

/* COPY_tm: copy pg_tm fields into fmt_tm */
#[inline]
unsafe fn COPY_tm(dst: *mut fmt_tm, src: *const pg_tm) {
    (*dst).tm_sec = (*src).tm_sec;
    (*dst).tm_min = (*src).tm_min;
    (*dst).tm_hour = (*src).tm_hour as int64;
    (*dst).tm_mday = (*src).tm_mday;
    (*dst).tm_mon = (*src).tm_mon;
    (*dst).tm_year = (*src).tm_year;
    (*dst).tm_wday = (*src).tm_wday;
    (*dst).tm_yday = (*src).tm_yday;
    (*dst).tm_gmtoff = (*src).tm_gmtoff;
}

/*
 *  to_char(time) appears to to_char() as an interval, so this check is really
 *  for interval and time data types.
 */
macro_rules! INVALID_FOR_INTERVAL {
    ($is_interval:expr) => {
        if $is_interval {
            ereport!(
                ERROR,
                errmsg!("invalid format specification for an interval value")
            );
        }
    };
}

// ===========================================================================
// KeyWord definitions: DCH/NUM enums + keyword tables + index tables.
// ===========================================================================

/* Suffixes (FormatNode.suffix is an OR of these codes) */
const DCH_S_FM: uint8 = 0x01;
const DCH_S_TH: uint8 = 0x02;
const DCH_S_th: uint8 = 0x04;
const DCH_S_SP: uint8 = 0x08;
const DCH_S_TM: uint8 = 0x10;

/* Suffix tests */
#[inline]
fn S_THth(s: uint8) -> c_int {
    if (s & DCH_S_TH) != 0 || (s & DCH_S_th) != 0 { 1 } else { 0 }
}
#[inline]
fn S_TH(s: uint8) -> c_int {
    if (s & DCH_S_TH) != 0 { 1 } else { 0 }
}
#[inline]
fn S_th(s: uint8) -> c_int {
    if (s & DCH_S_th) != 0 { 1 } else { 0 }
}
#[inline]
fn S_TH_TYPE(s: uint8) -> c_int {
    if (s & DCH_S_TH) != 0 { TH_UPPER } else { TH_LOWER }
}
/* Oracle toggles FM behavior, we don't; see docs. */
#[inline]
fn S_FM(s: uint8) -> c_int {
    if (s & DCH_S_FM) != 0 { 1 } else { 0 }
}
#[inline]
fn S_SP(s: uint8) -> c_int {
    if (s & DCH_S_SP) != 0 { 1 } else { 0 }
}
#[inline]
fn S_TM(s: uint8) -> c_int {
    if (s & DCH_S_TM) != 0 { 1 } else { 0 }
}

/* Suffixes definition for DATE-TIME TO/FROM CHAR */
const TM_SUFFIX_LEN: c_int = 2;

static mut DCH_suff: [KeySuffix; 8] = [
    KeySuffix { name: c"FM".as_ptr(), len: 2, id: DCH_S_FM as c_int, r#type: SUFFTYPE_PREFIX },
    KeySuffix { name: c"fm".as_ptr(), len: 2, id: DCH_S_FM as c_int, r#type: SUFFTYPE_PREFIX },
    KeySuffix { name: c"TM".as_ptr(), len: TM_SUFFIX_LEN, id: DCH_S_TM as c_int, r#type: SUFFTYPE_PREFIX },
    KeySuffix { name: c"tm".as_ptr(), len: 2, id: DCH_S_TM as c_int, r#type: SUFFTYPE_PREFIX },
    KeySuffix { name: c"TH".as_ptr(), len: 2, id: DCH_S_TH as c_int, r#type: SUFFTYPE_POSTFIX },
    KeySuffix { name: c"th".as_ptr(), len: 2, id: DCH_S_th as c_int, r#type: SUFFTYPE_POSTFIX },
    KeySuffix { name: c"SP".as_ptr(), len: 2, id: DCH_S_SP as c_int, r#type: SUFFTYPE_POSTFIX },
    /* last */
    KeySuffix { name: null(), len: 0, id: 0, r#type: 0 },
];

/* DCH_poz enum */
const DCH_A_D: c_int = 0;
const DCH_A_M: c_int = 1;
const DCH_AD: c_int = 2;
const DCH_AM: c_int = 3;
const DCH_B_C: c_int = 4;
const DCH_BC: c_int = 5;
const DCH_CC: c_int = 6;
const DCH_DAY: c_int = 7;
const DCH_DDD: c_int = 8;
const DCH_DD: c_int = 9;
const DCH_DY: c_int = 10;
const DCH_Day: c_int = 11;
const DCH_Dy: c_int = 12;
const DCH_D: c_int = 13;
const DCH_FF1: c_int = 14; /* FFn codes must be consecutive */
const DCH_FF2: c_int = 15;
const DCH_FF3: c_int = 16;
const DCH_FF4: c_int = 17;
const DCH_FF5: c_int = 18;
const DCH_FF6: c_int = 19;
const DCH_FX: c_int = 20; /* global suffix */
const DCH_HH24: c_int = 21;
const DCH_HH12: c_int = 22;
const DCH_HH: c_int = 23;
const DCH_IDDD: c_int = 24;
const DCH_ID: c_int = 25;
const DCH_IW: c_int = 26;
const DCH_IYYY: c_int = 27;
const DCH_IYY: c_int = 28;
const DCH_IY: c_int = 29;
const DCH_I: c_int = 30;
const DCH_J: c_int = 31;
const DCH_MI: c_int = 32;
const DCH_MM: c_int = 33;
const DCH_MONTH: c_int = 34;
const DCH_MON: c_int = 35;
const DCH_MS: c_int = 36;
const DCH_Month: c_int = 37;
const DCH_Mon: c_int = 38;
const DCH_OF: c_int = 39;
const DCH_P_M: c_int = 40;
const DCH_PM: c_int = 41;
const DCH_Q: c_int = 42;
const DCH_RM: c_int = 43;
const DCH_SSSSS: c_int = 44;
const DCH_SSSS: c_int = 45;
const DCH_SS: c_int = 46;
const DCH_TZH: c_int = 47;
const DCH_TZM: c_int = 48;
const DCH_TZ: c_int = 49;
const DCH_US: c_int = 50;
const DCH_WW: c_int = 51;
const DCH_W: c_int = 52;
const DCH_Y_YYY: c_int = 53;
const DCH_YYYY: c_int = 54;
const DCH_YYY: c_int = 55;
const DCH_YY: c_int = 56;
const DCH_Y: c_int = 57;
const DCH_a_d: c_int = 58;
const DCH_a_m: c_int = 59;
const DCH_ad: c_int = 60;
const DCH_am: c_int = 61;
const DCH_b_c: c_int = 62;
const DCH_bc: c_int = 63;
const DCH_cc: c_int = 64;
const DCH_day: c_int = 65;
const DCH_ddd: c_int = 66;
const DCH_dd: c_int = 67;
const DCH_dy: c_int = 68;
const DCH_d: c_int = 69;
const DCH_ff1: c_int = 70;
const DCH_ff2: c_int = 71;
const DCH_ff3: c_int = 72;
const DCH_ff4: c_int = 73;
const DCH_ff5: c_int = 74;
const DCH_ff6: c_int = 75;
const DCH_fx: c_int = 76;
const DCH_hh24: c_int = 77;
const DCH_hh12: c_int = 78;
const DCH_hh: c_int = 79;
const DCH_iddd: c_int = 80;
const DCH_id: c_int = 81;
const DCH_iw: c_int = 82;
const DCH_iyyy: c_int = 83;
const DCH_iyy: c_int = 84;
const DCH_iy: c_int = 85;
const DCH_i: c_int = 86;
const DCH_j: c_int = 87;
const DCH_mi: c_int = 88;
const DCH_mm: c_int = 89;
const DCH_month: c_int = 90;
const DCH_mon: c_int = 91;
const DCH_ms: c_int = 92;
const DCH_of: c_int = 93;
const DCH_p_m: c_int = 94;
const DCH_pm: c_int = 95;
const DCH_q: c_int = 96;
const DCH_rm: c_int = 97;
const DCH_sssss: c_int = 98;
const DCH_ssss: c_int = 99;
const DCH_ss: c_int = 100;
const DCH_tzh: c_int = 101;
const DCH_tzm: c_int = 102;
const DCH_tz: c_int = 103;
const DCH_us: c_int = 104;
const DCH_ww: c_int = 105;
const DCH_w: c_int = 106;
const DCH_y_yyy: c_int = 107;
const DCH_yyyy: c_int = 108;
const DCH_yyy: c_int = 109;
const DCH_yy: c_int = 110;
const DCH_y: c_int = 111;
const _DCH_last_: c_int = 112;

/* NUM_poz enum */
const NUM_COMMA: c_int = 0;
const NUM_DEC: c_int = 1;
const NUM_0: c_int = 2;
const NUM_9: c_int = 3;
const NUM_B: c_int = 4;
const NUM_C: c_int = 5;
const NUM_D: c_int = 6;
const NUM_E: c_int = 7;
const NUM_FM: c_int = 8;
const NUM_G: c_int = 9;
const NUM_L: c_int = 10;
const NUM_MI: c_int = 11;
const NUM_PL: c_int = 12;
const NUM_PR: c_int = 13;
const NUM_RN: c_int = 14;
const NUM_SG: c_int = 15;
const NUM_SP: c_int = 16;
const NUM_S: c_int = 17;
const NUM_TH: c_int = 18;
const NUM_V: c_int = 19;
const NUM_b: c_int = 20;
const NUM_c: c_int = 21;
const NUM_d: c_int = 22;
const NUM_e: c_int = 23;
const NUM_fm: c_int = 24;
const NUM_g: c_int = 25;
const NUM_l: c_int = 26;
const NUM_mi: c_int = 27;
const NUM_pl: c_int = 28;
const NUM_pr: c_int = 29;
const NUM_rn: c_int = 30;
const NUM_sg: c_int = 31;
const NUM_sp: c_int = 32;
const NUM_s: c_int = 33;
const NUM_th: c_int = 34;
const NUM_v: c_int = 35;
const _NUM_last_: c_int = 36;

/* KeyWords for DATE-TIME version */
macro_rules! kw {
    ($name:expr, $len:expr, $id:expr, $is_digit:expr, $date_mode:expr) => {
        KeyWord { name: $name.as_ptr(), len: $len, id: $id, is_digit: $is_digit, date_mode: $date_mode }
    };
}

static mut DCH_keywords: [KeyWord; 113] = [
    /* name, len, id, is_digit, date_mode */
    kw!(c"A.D.", 4, DCH_A_D, false, FROM_CHAR_DATE_NONE), /* A */
    kw!(c"A.M.", 4, DCH_A_M, false, FROM_CHAR_DATE_NONE),
    kw!(c"AD", 2, DCH_AD, false, FROM_CHAR_DATE_NONE),
    kw!(c"AM", 2, DCH_AM, false, FROM_CHAR_DATE_NONE),
    kw!(c"B.C.", 4, DCH_B_C, false, FROM_CHAR_DATE_NONE), /* B */
    kw!(c"BC", 2, DCH_BC, false, FROM_CHAR_DATE_NONE),
    kw!(c"CC", 2, DCH_CC, true, FROM_CHAR_DATE_NONE), /* C */
    kw!(c"DAY", 3, DCH_DAY, false, FROM_CHAR_DATE_NONE), /* D */
    kw!(c"DDD", 3, DCH_DDD, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"DD", 2, DCH_DD, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"DY", 2, DCH_DY, false, FROM_CHAR_DATE_NONE),
    kw!(c"Day", 3, DCH_Day, false, FROM_CHAR_DATE_NONE),
    kw!(c"Dy", 2, DCH_Dy, false, FROM_CHAR_DATE_NONE),
    kw!(c"D", 1, DCH_D, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"FF1", 3, DCH_FF1, true, FROM_CHAR_DATE_NONE), /* F */
    kw!(c"FF2", 3, DCH_FF2, true, FROM_CHAR_DATE_NONE),
    kw!(c"FF3", 3, DCH_FF3, true, FROM_CHAR_DATE_NONE),
    kw!(c"FF4", 3, DCH_FF4, true, FROM_CHAR_DATE_NONE),
    kw!(c"FF5", 3, DCH_FF5, true, FROM_CHAR_DATE_NONE),
    kw!(c"FF6", 3, DCH_FF6, true, FROM_CHAR_DATE_NONE),
    kw!(c"FX", 2, DCH_FX, false, FROM_CHAR_DATE_NONE),
    kw!(c"HH24", 4, DCH_HH24, true, FROM_CHAR_DATE_NONE), /* H */
    kw!(c"HH12", 4, DCH_HH12, true, FROM_CHAR_DATE_NONE),
    kw!(c"HH", 2, DCH_HH, true, FROM_CHAR_DATE_NONE),
    kw!(c"IDDD", 4, DCH_IDDD, true, FROM_CHAR_DATE_ISOWEEK), /* I */
    kw!(c"ID", 2, DCH_ID, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"IW", 2, DCH_IW, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"IYYY", 4, DCH_IYYY, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"IYY", 3, DCH_IYY, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"IY", 2, DCH_IY, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"I", 1, DCH_I, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"J", 1, DCH_J, true, FROM_CHAR_DATE_NONE), /* J */
    kw!(c"MI", 2, DCH_MI, true, FROM_CHAR_DATE_NONE), /* M */
    kw!(c"MM", 2, DCH_MM, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"MONTH", 5, DCH_MONTH, false, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"MON", 3, DCH_MON, false, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"MS", 2, DCH_MS, true, FROM_CHAR_DATE_NONE),
    kw!(c"Month", 5, DCH_Month, false, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"Mon", 3, DCH_Mon, false, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"OF", 2, DCH_OF, false, FROM_CHAR_DATE_NONE), /* O */
    kw!(c"P.M.", 4, DCH_P_M, false, FROM_CHAR_DATE_NONE), /* P */
    kw!(c"PM", 2, DCH_PM, false, FROM_CHAR_DATE_NONE),
    kw!(c"Q", 1, DCH_Q, true, FROM_CHAR_DATE_NONE), /* Q */
    kw!(c"RM", 2, DCH_RM, false, FROM_CHAR_DATE_GREGORIAN), /* R */
    kw!(c"SSSSS", 5, DCH_SSSS, true, FROM_CHAR_DATE_NONE), /* S */
    kw!(c"SSSS", 4, DCH_SSSS, true, FROM_CHAR_DATE_NONE),
    kw!(c"SS", 2, DCH_SS, true, FROM_CHAR_DATE_NONE),
    kw!(c"TZH", 3, DCH_TZH, false, FROM_CHAR_DATE_NONE), /* T */
    kw!(c"TZM", 3, DCH_TZM, true, FROM_CHAR_DATE_NONE),
    kw!(c"TZ", 2, DCH_TZ, false, FROM_CHAR_DATE_NONE),
    kw!(c"US", 2, DCH_US, true, FROM_CHAR_DATE_NONE), /* U */
    kw!(c"WW", 2, DCH_WW, true, FROM_CHAR_DATE_GREGORIAN), /* W */
    kw!(c"W", 1, DCH_W, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"Y,YYY", 5, DCH_Y_YYY, true, FROM_CHAR_DATE_GREGORIAN), /* Y */
    kw!(c"YYYY", 4, DCH_YYYY, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"YYY", 3, DCH_YYY, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"YY", 2, DCH_YY, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"Y", 1, DCH_Y, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"a.d.", 4, DCH_a_d, false, FROM_CHAR_DATE_NONE), /* a */
    kw!(c"a.m.", 4, DCH_a_m, false, FROM_CHAR_DATE_NONE),
    kw!(c"ad", 2, DCH_ad, false, FROM_CHAR_DATE_NONE),
    kw!(c"am", 2, DCH_am, false, FROM_CHAR_DATE_NONE),
    kw!(c"b.c.", 4, DCH_b_c, false, FROM_CHAR_DATE_NONE), /* b */
    kw!(c"bc", 2, DCH_bc, false, FROM_CHAR_DATE_NONE),
    kw!(c"cc", 2, DCH_CC, true, FROM_CHAR_DATE_NONE), /* c */
    kw!(c"day", 3, DCH_day, false, FROM_CHAR_DATE_NONE), /* d */
    kw!(c"ddd", 3, DCH_DDD, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"dd", 2, DCH_DD, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"dy", 2, DCH_dy, false, FROM_CHAR_DATE_NONE),
    kw!(c"d", 1, DCH_D, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"ff1", 3, DCH_FF1, true, FROM_CHAR_DATE_NONE), /* f */
    kw!(c"ff2", 3, DCH_FF2, true, FROM_CHAR_DATE_NONE),
    kw!(c"ff3", 3, DCH_FF3, true, FROM_CHAR_DATE_NONE),
    kw!(c"ff4", 3, DCH_FF4, true, FROM_CHAR_DATE_NONE),
    kw!(c"ff5", 3, DCH_FF5, true, FROM_CHAR_DATE_NONE),
    kw!(c"ff6", 3, DCH_FF6, true, FROM_CHAR_DATE_NONE),
    kw!(c"fx", 2, DCH_FX, false, FROM_CHAR_DATE_NONE),
    kw!(c"hh24", 4, DCH_HH24, true, FROM_CHAR_DATE_NONE), /* h */
    kw!(c"hh12", 4, DCH_HH12, true, FROM_CHAR_DATE_NONE),
    kw!(c"hh", 2, DCH_HH, true, FROM_CHAR_DATE_NONE),
    kw!(c"iddd", 4, DCH_IDDD, true, FROM_CHAR_DATE_ISOWEEK), /* i */
    kw!(c"id", 2, DCH_ID, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"iw", 2, DCH_IW, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"iyyy", 4, DCH_IYYY, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"iyy", 3, DCH_IYY, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"iy", 2, DCH_IY, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"i", 1, DCH_I, true, FROM_CHAR_DATE_ISOWEEK),
    kw!(c"j", 1, DCH_J, true, FROM_CHAR_DATE_NONE), /* j */
    kw!(c"mi", 2, DCH_MI, true, FROM_CHAR_DATE_NONE), /* m */
    kw!(c"mm", 2, DCH_MM, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"month", 5, DCH_month, false, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"mon", 3, DCH_mon, false, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"ms", 2, DCH_MS, true, FROM_CHAR_DATE_NONE),
    kw!(c"of", 2, DCH_OF, false, FROM_CHAR_DATE_NONE), /* o */
    kw!(c"p.m.", 4, DCH_p_m, false, FROM_CHAR_DATE_NONE), /* p */
    kw!(c"pm", 2, DCH_pm, false, FROM_CHAR_DATE_NONE),
    kw!(c"q", 1, DCH_Q, true, FROM_CHAR_DATE_NONE), /* q */
    kw!(c"rm", 2, DCH_rm, false, FROM_CHAR_DATE_GREGORIAN), /* r */
    kw!(c"sssss", 5, DCH_SSSS, true, FROM_CHAR_DATE_NONE), /* s */
    kw!(c"ssss", 4, DCH_SSSS, true, FROM_CHAR_DATE_NONE),
    kw!(c"ss", 2, DCH_SS, true, FROM_CHAR_DATE_NONE),
    kw!(c"tzh", 3, DCH_TZH, false, FROM_CHAR_DATE_NONE), /* t */
    kw!(c"tzm", 3, DCH_TZM, true, FROM_CHAR_DATE_NONE),
    kw!(c"tz", 2, DCH_tz, false, FROM_CHAR_DATE_NONE),
    kw!(c"us", 2, DCH_US, true, FROM_CHAR_DATE_NONE), /* u */
    kw!(c"ww", 2, DCH_WW, true, FROM_CHAR_DATE_GREGORIAN), /* w */
    kw!(c"w", 1, DCH_W, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"y,yyy", 5, DCH_Y_YYY, true, FROM_CHAR_DATE_GREGORIAN), /* y */
    kw!(c"yyyy", 4, DCH_YYYY, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"yyy", 3, DCH_YYY, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"yy", 2, DCH_YY, true, FROM_CHAR_DATE_GREGORIAN),
    kw!(c"y", 1, DCH_Y, true, FROM_CHAR_DATE_GREGORIAN),
    /* last */
    KeyWord { name: null(), len: 0, id: 0, is_digit: false, date_mode: 0 },
];

/* KeyWords for NUMBER version (is_digit and date_mode fields not relevant) */
macro_rules! nkw {
    ($name:expr, $len:expr, $id:expr) => {
        KeyWord { name: $name.as_ptr(), len: $len, id: $id, is_digit: false, date_mode: 0 }
    };
}
static mut NUM_keywords: [KeyWord; 37] = [
    /* name, len, id        is in Index */
    nkw!(c",", 1, NUM_COMMA), /* , */
    nkw!(c".", 1, NUM_DEC),   /* . */
    nkw!(c"0", 1, NUM_0),     /* 0 */
    nkw!(c"9", 1, NUM_9),     /* 9 */
    nkw!(c"B", 1, NUM_B),     /* B */
    nkw!(c"C", 1, NUM_C),     /* C */
    nkw!(c"D", 1, NUM_D),     /* D */
    nkw!(c"EEEE", 4, NUM_E),  /* E */
    nkw!(c"FM", 2, NUM_FM),   /* F */
    nkw!(c"G", 1, NUM_G),     /* G */
    nkw!(c"L", 1, NUM_L),     /* L */
    nkw!(c"MI", 2, NUM_MI),   /* M */
    nkw!(c"PL", 2, NUM_PL),   /* P */
    nkw!(c"PR", 2, NUM_PR),
    nkw!(c"RN", 2, NUM_RN), /* R */
    nkw!(c"SG", 2, NUM_SG), /* S */
    nkw!(c"SP", 2, NUM_SP),
    nkw!(c"S", 1, NUM_S),
    nkw!(c"TH", 2, NUM_TH), /* T */
    nkw!(c"V", 1, NUM_V),   /* V */
    nkw!(c"b", 1, NUM_B),   /* b */
    nkw!(c"c", 1, NUM_C),   /* c */
    nkw!(c"d", 1, NUM_D),   /* d */
    nkw!(c"eeee", 4, NUM_E), /* e */
    nkw!(c"fm", 2, NUM_FM), /* f */
    nkw!(c"g", 1, NUM_G),   /* g */
    nkw!(c"l", 1, NUM_L),   /* l */
    nkw!(c"mi", 2, NUM_MI), /* m */
    nkw!(c"pl", 2, NUM_PL), /* p */
    nkw!(c"pr", 2, NUM_PR),
    nkw!(c"rn", 2, NUM_rn), /* r */
    nkw!(c"sg", 2, NUM_SG), /* s */
    nkw!(c"sp", 2, NUM_SP),
    nkw!(c"s", 1, NUM_S),
    nkw!(c"th", 2, NUM_th), /* t */
    nkw!(c"v", 1, NUM_V),   /* v */
    /* last */
    KeyWord { name: null(), len: 0, id: 0, is_digit: false, date_mode: 0 },
];

/* KeyWords index for DATE-TIME version */
static DCH_index: [c_int; KeyWord_INDEX_SIZE] = [
    /*---- first 0..31 chars are skipped ----*/
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, DCH_A_D, DCH_B_C, DCH_CC, DCH_DAY, -1, DCH_FF1, -1, DCH_HH24,
    DCH_IDDD, DCH_J, -1, -1, DCH_MI, -1, DCH_OF, DCH_P_M, DCH_Q, DCH_RM, DCH_SSSSS, DCH_TZH, DCH_US,
    -1, DCH_WW, -1, DCH_Y_YYY, -1, -1, -1, -1, -1, -1, -1, DCH_a_d, DCH_b_c, DCH_cc, DCH_day, -1,
    DCH_ff1, -1, DCH_hh24, DCH_iddd, DCH_j, -1, -1, DCH_mi, -1, DCH_of, DCH_p_m, DCH_q, DCH_rm,
    DCH_sssss, DCH_tzh, DCH_us, -1, DCH_ww, -1, DCH_y_yyy, -1, -1, -1, -1,
    /*---- chars over 126 are skipped ----*/
];

/* KeyWords index for NUMBER version */
static NUM_index: [c_int; 92] = [
    /*---- first 0..31 chars are skipped ----*/
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, NUM_COMMA, -1, NUM_DEC, -1, NUM_0, -1, -1, -1,
    -1, -1, -1, -1, -1, NUM_9, -1, -1, -1, -1, -1, -1, -1, -1, NUM_B, NUM_C, NUM_D, NUM_E, NUM_FM,
    NUM_G, -1, -1, -1, -1, NUM_L, NUM_MI, -1, -1, NUM_PL, -1, NUM_RN, NUM_SG, NUM_TH, -1, NUM_V, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, NUM_b, NUM_c, NUM_d, NUM_e, NUM_fm, NUM_g, -1, -1, -1, -1,
    NUM_l, NUM_mi, -1, -1, NUM_pl, -1, NUM_rn, NUM_sg, NUM_th, -1, NUM_v, -1, -1, -1, -1, -1, -1,
    /*---- chars over 126 are skipped ----*/
];

/* Number processor struct */
#[repr(C)]
pub struct NUMProc {
    pub is_to_char: bool,
    pub Num: *mut NUMDesc, /* number description */

    pub sign: c_int,           /* '-' or '+' */
    pub sign_wrote: c_int,     /* was sign write */
    pub num_count: c_int,      /* number of write digits */
    pub num_in: c_int,         /* is inside number */
    pub num_curr: c_int,       /* current position in number */
    pub out_pre_spaces: c_int, /* spaces before first digit */

    pub read_dec: c_int,  /* to_number - was read dec. point */
    pub read_post: c_int, /* to_number - number of dec. digit */
    pub read_pre: c_int,  /* to_number - number non-dec. digit */

    pub number: *mut c_char,   /* string with number */
    pub number_p: *mut c_char, /* pointer to current number position */
    pub inout: *mut c_char,    /* in / out buffer */
    pub inout_p: *mut c_char,  /* pointer to current inout position */

    pub last_relevant: *const c_char, /* last relevant number after decimal point */

    pub L_negative_sign: *const c_char, /* Locale */
    pub L_positive_sign: *const c_char,
    pub decimal: *const c_char,
    pub L_thousands_sep: *const c_char,
    pub L_currency_symbol: *const c_char,
}

/* Return flags for DCH_from_char() */
const DCH_DATED: c_int = 0x01;
const DCH_TIMED: c_int = 0x02;
const DCH_ZONED: c_int = 0x04;

/*
 * These macros are used in NUM_processor() and its subsidiary routines.
 * OVERLOAD_TEST: true if we've reached end of input string
 * AMOUNT_TEST(s): true if at least s bytes remain in string
 */
macro_rules! OVERLOAD_TEST {
    ($Np:expr, $input_len:expr) => {
        (*$Np).inout_p >= (*$Np).inout.add($input_len as usize)
    };
}
macro_rules! AMOUNT_TEST {
    ($Np:expr, $input_len:expr, $s:expr) => {
        (*$Np).inout_p <= (*$Np).inout.add(($input_len - ($s)) as usize)
    };
}

/*
 * write_cstr: copy a Rust string (as bytes + NUL) into the C buffer at `s`.
 * This is the workhorse for the many sprintf() calls in DCH_to_char that write
 * a formatted token and then advance with `s += strlen(s)`.
 */
unsafe fn write_cstr(s: *mut c_char, val: &str) {
    let bytes = val.as_bytes();
    let len = bytes.len();
    if len > 0 {
        memcpy(s as *mut c_void, bytes.as_ptr() as *const c_void, len);
    }
    *s.add(len) = 0;
}

/* sprintf("%0*d", width, v): zero-padded signed int of minimum field width. */
fn fmt_0d(width: c_int, v: c_int) -> std::string::String {
    if width <= 0 {
        format!("{}", v)
    } else if v < 0 {
        format!("-{:0>width$}", -(v as i64), width = (width as usize).saturating_sub(1).max(1))
    } else {
        format!("{:0>width$}", v, width = width as usize)
    }
}

/* sprintf("%0*lld", width, v): zero-padded signed 64-bit. */
fn fmt_0lld(width: c_int, v: int64) -> std::string::String {
    if width <= 0 {
        format!("{}", v)
    } else if v < 0 {
        format!("-{:0>width$}", (v as i128).unsigned_abs(), width = (width as usize).saturating_sub(1).max(1))
    } else {
        format!("{:0>width$}", v, width = width as usize)
    }
}

/* sprintf("%*s", width, s): right-justify if width>0, left-justify if width<0. */
unsafe fn fmt_pad_str(width: c_int, s: *const c_char) -> std::string::String {
    let val = cstr(s);
    if width == 0 {
        val
    } else if width > 0 {
        format!("{:>width$}", val, width = width as usize)
    } else {
        format!("{:<width$}", val, width = (-width) as usize)
    }
}

// ===========================================================================
// Functions
// ===========================================================================

/*
 * Fast sequential search, use index for data selection which go to seq. cycle
 * (it is very fast for unwanted strings)
 */
unsafe fn index_seq_search(
    str: *const c_char,
    kw: *const KeyWord,
    index: *const c_int,
) -> *const KeyWord {
    let poz: c_int;

    if !KeyWord_INDEX_FILTER(*str) {
        return null();
    }

    poz = *index.offset((*str - b' ' as c_char) as isize);
    if poz > -1 {
        let mut k = kw.offset(poz as isize);

        loop {
            if strncmp(str, (*k).name, (*k).len as usize) == 0 {
                return k;
            }
            k = k.add(1);
            if (*k).name.is_null() {
                return null();
            }
            if *str != *(*k).name {
                break;
            }
        }
    }
    null()
}

unsafe fn suff_search(
    str: *const c_char,
    suf: *const KeySuffix,
    r#type: c_int,
) -> *const KeySuffix {
    let mut s = suf;
    while !(*s).name.is_null() {
        if (*s).r#type != r#type {
            s = s.add(1);
            continue;
        }

        if strncmp(str, (*s).name, (*s).len as usize) == 0 {
            return s;
        }
        s = s.add(1);
    }
    null()
}

unsafe fn is_separator_char(str: *const c_char) -> bool {
    /* ASCII printable character, but not letter or digit */
    let c = *str as u8 as c_int;
    c > 0x20
        && c < 0x7F
        && !(*str >= b'A' as c_char && *str <= b'Z' as c_char)
        && !(*str >= b'a' as c_char && *str <= b'z' as c_char)
        && !(*str >= b'0' as c_char && *str <= b'9' as c_char)
}

/* Prepare NUMDesc (number description struct) via FormatNode struct */
unsafe fn NUMDesc_prepare(num: *mut NUMDesc, n: *mut FormatNode) {
    if (*n).r#type != NODE_TYPE_ACTION {
        return;
    }

    if IS_EEEE(num) && (*(*n).key).id != NUM_E {
        ereport!(ERROR, errmsg!("\"EEEE\" must be the last pattern used"));
    }

    match (*(*n).key).id {
        x if x == NUM_9 => {
            if IS_BRACKET(num) {
                ereport!(ERROR, errmsg!("\"9\" must be ahead of \"PR\""));
            }
            if IS_MULTI(num) {
                (*num).multi += 1;
            } else if IS_DECIMAL(num) {
                (*num).post += 1;
            } else {
                (*num).pre += 1;
            }
        }

        x if x == NUM_0 => {
            if IS_BRACKET(num) {
                ereport!(ERROR, errmsg!("\"0\" must be ahead of \"PR\""));
            }
            if !IS_ZERO(num) && !IS_DECIMAL(num) {
                (*num).flag |= NUM_F_ZERO;
                (*num).zero_start = (*num).pre + 1;
            }
            if !IS_DECIMAL(num) {
                (*num).pre += 1;
            } else {
                (*num).post += 1;
            }

            (*num).zero_end = (*num).pre + (*num).post;
        }

        x if x == NUM_B => {
            if (*num).pre == 0 && (*num).post == 0 && (!IS_ZERO(num)) {
                (*num).flag |= NUM_F_BLANK;
            }
        }

        x if x == NUM_D || x == NUM_DEC => {
            if x == NUM_D {
                (*num).flag |= NUM_F_LDECIMAL;
                (*num).need_locale = true as c_int;
            }
            /* FALLTHROUGH to NUM_DEC handling */
            if IS_DECIMAL(num) {
                ereport!(ERROR, errmsg!("multiple decimal points"));
            }
            if IS_MULTI(num) {
                ereport!(
                    ERROR,
                    errmsg!("cannot use \"V\" and decimal point together")
                );
            }
            (*num).flag |= NUM_F_DECIMAL;
        }

        x if x == NUM_FM => {
            (*num).flag |= NUM_F_FILLMODE;
        }

        x if x == NUM_S => {
            if IS_LSIGN(num) {
                ereport!(ERROR, errmsg!("cannot use \"S\" twice"));
            }
            if IS_PLUS(num) || IS_MINUS(num) || IS_BRACKET(num) {
                ereport!(
                    ERROR,
                    errmsg!("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together")
                );
            }
            if !IS_DECIMAL(num) {
                (*num).lsign = NUM_LSIGN_PRE;
                (*num).pre_lsign_num = (*num).pre;
                (*num).need_locale = true as c_int;
                (*num).flag |= NUM_F_LSIGN;
            } else if (*num).lsign == NUM_LSIGN_NONE {
                (*num).lsign = NUM_LSIGN_POST;
                (*num).need_locale = true as c_int;
                (*num).flag |= NUM_F_LSIGN;
            }
        }

        x if x == NUM_MI => {
            if IS_LSIGN(num) {
                ereport!(ERROR, errmsg!("cannot use \"S\" and \"MI\" together"));
            }
            (*num).flag |= NUM_F_MINUS;
            if IS_DECIMAL(num) {
                (*num).flag |= NUM_F_MINUS_POST;
            }
        }

        x if x == NUM_PL => {
            if IS_LSIGN(num) {
                ereport!(ERROR, errmsg!("cannot use \"S\" and \"PL\" together"));
            }
            (*num).flag |= NUM_F_PLUS;
            if IS_DECIMAL(num) {
                (*num).flag |= NUM_F_PLUS_POST;
            }
        }

        x if x == NUM_SG => {
            if IS_LSIGN(num) {
                ereport!(ERROR, errmsg!("cannot use \"S\" and \"SG\" together"));
            }
            (*num).flag |= NUM_F_MINUS;
            (*num).flag |= NUM_F_PLUS;
        }

        x if x == NUM_PR => {
            if IS_LSIGN(num) || IS_PLUS(num) || IS_MINUS(num) {
                ereport!(
                    ERROR,
                    errmsg!("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together")
                );
            }
            (*num).flag |= NUM_F_BRACKET;
        }

        x if x == NUM_rn || x == NUM_RN => {
            if IS_ROMAN(num) {
                ereport!(ERROR, errmsg!("cannot use \"RN\" twice"));
            }
            (*num).flag |= NUM_F_ROMAN;
        }

        x if x == NUM_L || x == NUM_G => {
            (*num).need_locale = true as c_int;
        }

        x if x == NUM_V => {
            if IS_DECIMAL(num) {
                ereport!(
                    ERROR,
                    errmsg!("cannot use \"V\" and decimal point together")
                );
            }
            (*num).flag |= NUM_F_MULTI;
        }

        x if x == NUM_E => {
            if IS_EEEE(num) {
                ereport!(ERROR, errmsg!("cannot use \"EEEE\" twice"));
            }
            if IS_BLANK(num)
                || IS_FILLMODE(num)
                || IS_LSIGN(num)
                || IS_BRACKET(num)
                || IS_MINUS(num)
                || IS_PLUS(num)
                || IS_ROMAN(num)
                || IS_MULTI(num)
            {
                ereport!(ERROR, errmsg!("\"EEEE\" is incompatible with other formats"));
            }
            (*num).flag |= NUM_F_EEEE;
        }

        _ => {}
    }

    if IS_ROMAN(num) && ((*num).flag & !(NUM_F_ROMAN | NUM_F_FILLMODE)) != 0 {
        ereport!(ERROR, errmsg!("\"RN\" is incompatible with other formats"));
    }
}

/*
 * Format parser, search small keywords and keyword's suffixes, and make
 * format-node tree.
 */
unsafe fn parse_format(
    node: *mut FormatNode,
    mut str: *const c_char,
    kw: *const KeyWord,
    suf: *const KeySuffix,
    index: *const c_int,
    flags: uint32,
    Num: *mut NUMDesc,
) {
    let mut n = node;

    while *str != 0 {
        let mut suffix: c_int = 0;
        let mut s: *const KeySuffix;

        /*
         * Prefix
         */
        if (flags & DCH_FLAG) != 0 && {
            s = suff_search(str, suf, SUFFTYPE_PREFIX);
            !s.is_null()
        } {
            suffix |= (*s).id;
            if (*s).len != 0 {
                str = str.offset((*s).len as isize);
            }
        }

        /*
         * Keyword
         */
        if *str != 0 && {
            (*n).key = index_seq_search(str, kw, index);
            !(*n).key.is_null()
        } {
            (*n).r#type = NODE_TYPE_ACTION;
            (*n).suffix = suffix as uint8;
            if (*(*n).key).len != 0 {
                str = str.offset((*(*n).key).len as isize);
            }

            /*
             * NUM version: Prepare global NUMDesc struct
             */
            if (flags & NUM_FLAG) != 0 {
                NUMDesc_prepare(Num, n);
            }

            /*
             * Postfix
             */
            if (flags & DCH_FLAG) != 0 && *str != 0 && {
                s = suff_search(str, suf, SUFFTYPE_POSTFIX);
                !s.is_null()
            } {
                (*n).suffix |= (*s).id as uint8;
                if (*s).len != 0 {
                    str = str.offset((*s).len as isize);
                }
            }

            n = n.add(1);
        } else if *str != 0 {
            let mut chlen: c_int;

            if (flags & STD_FLAG) != 0 && *str != b'"' as c_char {
                /*
                 * Standard mode, allow only following separators: "-./,':; ".
                 * However, we support double quotes even in standard mode
                 * (see below).  This is our extension of standard mode.
                 */
                if strchr(c"-./,':; ".as_ptr(), *str as c_int).is_null() {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "invalid datetime format separator: \"{}\"",
                            cstr_n(str, pg_mblen_cstr(str))
                        )
                    );
                }

                if *str == b' ' as c_char {
                    (*n).r#type = NODE_TYPE_SPACE;
                } else {
                    (*n).r#type = NODE_TYPE_SEPARATOR;
                }

                (*n).character[0] = *str;
                (*n).character[1] = b'\0' as c_char;
                (*n).key = null();
                (*n).suffix = 0;
                n = n.add(1);
                str = str.add(1);
            } else if *str == b'"' as c_char {
                /*
                 * Process double-quoted literal string, if any
                 */
                str = str.add(1);
                while *str != 0 {
                    if *str == b'"' as c_char {
                        str = str.add(1);
                        break;
                    }
                    /* backslash quotes the next character, if any */
                    if *str == b'\\' as c_char && *str.add(1) != 0 {
                        str = str.add(1);
                    }
                    chlen = pg_mblen_cstr(str);
                    (*n).r#type = NODE_TYPE_CHAR;
                    memcpy(
                        (*n).character.as_mut_ptr() as *mut c_void,
                        str as *const c_void,
                        chlen as usize,
                    );
                    (*n).character[chlen as usize] = b'\0' as c_char;
                    (*n).key = null();
                    (*n).suffix = 0;
                    n = n.add(1);
                    str = str.offset(chlen as isize);
                }
            } else {
                /*
                 * Outside double-quoted strings, backslash is only special if
                 * it immediately precedes a double quote.
                 */
                if *str == b'\\' as c_char && *str.add(1) == b'"' as c_char {
                    str = str.add(1);
                }
                chlen = pg_mblen_cstr(str);

                if (flags & DCH_FLAG) != 0 && is_separator_char(str) {
                    (*n).r#type = NODE_TYPE_SEPARATOR;
                } else if isspace(*str as c_uchar as c_int) != 0 {
                    (*n).r#type = NODE_TYPE_SPACE;
                } else {
                    (*n).r#type = NODE_TYPE_CHAR;
                }

                memcpy(
                    (*n).character.as_mut_ptr() as *mut c_void,
                    str as *const c_void,
                    chlen as usize,
                );
                (*n).character[chlen as usize] = b'\0' as c_char;
                (*n).key = null();
                (*n).suffix = 0;
                n = n.add(1);
                str = str.offset(chlen as isize);
            }
        }
    }

    (*n).r#type = NODE_TYPE_END;
    (*n).suffix = 0;
}

/* pnstrdup(str, len) rendered to a Rust String for error messages */
unsafe fn cstr_n(s: *const c_char, len: c_int) -> std::string::String {
    if s.is_null() || len <= 0 {
        return std::string::String::new();
    }
    let slice = core::slice::from_raw_parts(s as *const u8, len as usize);
    std::string::String::from_utf8_lossy(slice).into_owned()
}

// ===========================================================================
// Private utils
// ===========================================================================

/*
 * Return ST/ND/RD/TH for simple (1..9) numbers
 * type --> 0 upper, 1 lower
 */
unsafe fn get_th(num: *mut c_char, r#type: c_int) -> *const c_char {
    let len = strlen(num) as c_int;
    let mut last: c_int;

    last = *num.offset((len - 1) as isize) as c_int;
    if isdigit(last) == 0 {
        ereport!(ERROR, errmsg!("\"{}\" is not a number", cstr(num)));
    }

    /*
     * All "teens" (<x>1[0-9]) get 'TH/th', while <x>[02-9][123] still get
     * 'ST/st', 'ND/nd', 'RD/rd', respectively
     */
    if (len > 1) && (*num.offset((len - 2) as isize) == b'1' as c_char) {
        last = 0;
    }

    match last as u8 {
        b'1' => {
            if r#type == TH_UPPER {
                numTH[0]
            } else {
                numth[0]
            }
        }
        b'2' => {
            if r#type == TH_UPPER {
                numTH[1]
            } else {
                numth[1]
            }
        }
        b'3' => {
            if r#type == TH_UPPER {
                numTH[2]
            } else {
                numth[2]
            }
        }
        _ => {
            if r#type == TH_UPPER {
                numTH[3]
            } else {
                numth[3]
            }
        }
    }
}

/*
 * Convert string-number to ordinal string-number
 * type --> 0 upper, 1 lower
 */
unsafe fn str_numth(dest: *mut c_char, num: *mut c_char, r#type: c_int) -> *mut c_char {
    if dest != num {
        strcpy(dest, num);
    }
    strcat(dest, get_th(num, r#type));
    dest
}

// ===========================================================================
// upper/lower/initcap functions
// ===========================================================================

/*
 * collation-aware, wide-character-aware lower function
 *
 * The result is a palloc'd, null-terminated string.
 */
pub unsafe fn str_tolower(buff: *const c_char, nbytes: usize, collid: Oid) -> *mut c_char {
    let result: *mut c_char;
    let mylocale: pg_locale_t;

    if buff.is_null() {
        return null_mut();
    }

    if !OidIsValid(collid) {
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for {} function", "lower()")
        );
    }

    mylocale = pg_newlocale_from_collation(collid);

    /* C/POSIX collations use this path regardless of database encoding */
    if (*mylocale).ctype_is_c {
        result = asc_tolower(buff, nbytes);
    } else {
        let src = buff;
        let srclen = nbytes;
        let mut dstsize: usize;
        let mut dst: *mut c_char;
        let mut needed: usize;

        /* first try buffer of equal size plus terminating NUL */
        dstsize = srclen + 1;
        dst = palloc(dstsize) as *mut c_char;

        needed = pg_strlower(dst, dstsize, src, srclen, mylocale);
        if needed + 1 > dstsize {
            /* grow buffer if needed and retry */
            dstsize = needed + 1;
            dst = repalloc(dst as *mut c_void, dstsize) as *mut c_char;
            needed = pg_strlower(dst, dstsize, src, srclen, mylocale);
            Assert!(needed + 1 <= dstsize);
        }

        Assert!(*dst.add(needed) == b'\0' as c_char);
        result = dst;
    }

    result
}

/*
 * collation-aware, wide-character-aware upper function
 */
pub unsafe fn str_toupper(buff: *const c_char, nbytes: usize, collid: Oid) -> *mut c_char {
    let result: *mut c_char;
    let mylocale: pg_locale_t;

    if buff.is_null() {
        return null_mut();
    }

    if !OidIsValid(collid) {
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for {} function", "upper()")
        );
    }

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).ctype_is_c {
        result = asc_toupper(buff, nbytes);
    } else {
        let src = buff;
        let srclen = nbytes;
        let mut dstsize: usize;
        let mut dst: *mut c_char;
        let mut needed: usize;

        dstsize = srclen + 1;
        dst = palloc(dstsize) as *mut c_char;

        needed = pg_strupper(dst, dstsize, src, srclen, mylocale);
        if needed + 1 > dstsize {
            dstsize = needed + 1;
            dst = repalloc(dst as *mut c_void, dstsize) as *mut c_char;
            needed = pg_strupper(dst, dstsize, src, srclen, mylocale);
            Assert!(needed + 1 <= dstsize);
        }

        Assert!(*dst.add(needed) == b'\0' as c_char);
        result = dst;
    }

    result
}

/*
 * collation-aware, wide-character-aware initcap function
 */
pub unsafe fn str_initcap(buff: *const c_char, nbytes: usize, collid: Oid) -> *mut c_char {
    let result: *mut c_char;
    let mylocale: pg_locale_t;

    if buff.is_null() {
        return null_mut();
    }

    if !OidIsValid(collid) {
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for {} function", "initcap()")
        );
    }

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).ctype_is_c {
        result = asc_initcap(buff, nbytes);
    } else {
        let src = buff;
        let srclen = nbytes;
        let mut dstsize: usize;
        let mut dst: *mut c_char;
        let mut needed: usize;

        dstsize = srclen + 1;
        dst = palloc(dstsize) as *mut c_char;

        needed = pg_strtitle(dst, dstsize, src, srclen, mylocale);
        if needed + 1 > dstsize {
            dstsize = needed + 1;
            dst = repalloc(dst as *mut c_void, dstsize) as *mut c_char;
            needed = pg_strtitle(dst, dstsize, src, srclen, mylocale);
            Assert!(needed + 1 <= dstsize);
        }

        Assert!(*dst.add(needed) == b'\0' as c_char);
        result = dst;
    }

    result
}

/*
 * collation-aware, wide-character-aware case folding
 */
pub unsafe fn str_casefold(buff: *const c_char, nbytes: usize, collid: Oid) -> *mut c_char {
    let result: *mut c_char;
    let mylocale: pg_locale_t;

    if buff.is_null() {
        return null_mut();
    }

    if !OidIsValid(collid) {
        ereport!(
            ERROR,
            errmsg!("could not determine which collation to use for {} function", "lower()")
        );
    }

    if GetDatabaseEncoding() != PG_UTF8 {
        ereport!(
            ERROR,
            errmsg!("Unicode case folding can only be performed if server encoding is UTF8")
        );
    }

    mylocale = pg_newlocale_from_collation(collid);

    if (*mylocale).ctype_is_c {
        result = asc_tolower(buff, nbytes);
    } else {
        let src = buff;
        let srclen = nbytes;
        let mut dstsize: usize;
        let mut dst: *mut c_char;
        let mut needed: usize;

        dstsize = srclen + 1;
        dst = palloc(dstsize) as *mut c_char;

        needed = pg_strfold(dst, dstsize, src, srclen, mylocale);
        if needed + 1 > dstsize {
            dstsize = needed + 1;
            dst = repalloc(dst as *mut c_void, dstsize) as *mut c_char;
            needed = pg_strfold(dst, dstsize, src, srclen, mylocale);
            Assert!(needed + 1 <= dstsize);
        }

        Assert!(*dst.add(needed) == b'\0' as c_char);
        result = dst;
    }

    result
}

/*
 * ASCII-only lower function
 */
pub unsafe fn asc_tolower(buff: *const c_char, nbytes: usize) -> *mut c_char {
    let result: *mut c_char;
    let mut p: *mut c_char;

    if buff.is_null() {
        return null_mut();
    }

    result = pnstrdup(buff, nbytes);

    p = result;
    while *p != 0 {
        *p = pg_ascii_tolower(*p as c_uchar) as c_char;
        p = p.add(1);
    }

    result
}

/*
 * ASCII-only upper function
 */
pub unsafe fn asc_toupper(buff: *const c_char, nbytes: usize) -> *mut c_char {
    let result: *mut c_char;
    let mut p: *mut c_char;

    if buff.is_null() {
        return null_mut();
    }

    result = pnstrdup(buff, nbytes);

    p = result;
    while *p != 0 {
        *p = pg_ascii_toupper(*p as c_uchar) as c_char;
        p = p.add(1);
    }

    result
}

/*
 * ASCII-only initcap function
 */
pub unsafe fn asc_initcap(buff: *const c_char, nbytes: usize) -> *mut c_char {
    let result: *mut c_char;
    let mut p: *mut c_char;
    let mut wasalnum: c_int = false as c_int;

    if buff.is_null() {
        return null_mut();
    }

    result = pnstrdup(buff, nbytes);

    p = result;
    while *p != 0 {
        let c: c_char;

        if wasalnum != 0 {
            *p = pg_ascii_tolower(*p as c_uchar) as c_char;
            c = *p;
        } else {
            *p = pg_ascii_toupper(*p as c_uchar) as c_char;
            c = *p;
        }
        /* we don't trust isalnum() here */
        wasalnum = ((c >= b'A' as c_char && c <= b'Z' as c_char)
            || (c >= b'a' as c_char && c <= b'z' as c_char)
            || (c >= b'0' as c_char && c <= b'9' as c_char)) as c_int;
        p = p.add(1);
    }

    result
}

/* convenience routines for when the input is null-terminated */

unsafe fn str_tolower_z(buff: *const c_char, collid: Oid) -> *mut c_char {
    str_tolower(buff, strlen(buff), collid)
}

unsafe fn str_toupper_z(buff: *const c_char, collid: Oid) -> *mut c_char {
    str_toupper(buff, strlen(buff), collid)
}

unsafe fn str_initcap_z(buff: *const c_char, collid: Oid) -> *mut c_char {
    str_initcap(buff, strlen(buff), collid)
}

unsafe fn asc_tolower_z(buff: *const c_char) -> *mut c_char {
    asc_tolower(buff, strlen(buff))
}

unsafe fn asc_toupper_z(buff: *const c_char) -> *mut c_char {
    asc_toupper(buff, strlen(buff))
}

/* asc_initcap_z is not currently needed */

/*
 * Skip TM / th in FROM_CHAR
 *
 * If S_THth is on, skip two chars, assuming there are two available
 */
macro_rules! SKIP_THth {
    ($ptr:expr, $suf:expr) => {
        if S_THth($suf) != 0 {
            if *($ptr) != 0 {
                $ptr = $ptr.offset(pg_mblen_cstr($ptr) as isize);
            }
            if *($ptr) != 0 {
                $ptr = $ptr.offset(pg_mblen_cstr($ptr) as isize);
            }
        }
    };
}

/* Return true if next format picture is not digit value */
unsafe fn is_next_separator(mut n: *mut FormatNode) -> bool {
    if (*n).r#type == NODE_TYPE_END {
        return false;
    }

    if (*n).r#type == NODE_TYPE_ACTION && S_THth((*n).suffix) != 0 {
        return true;
    }

    /*
     * Next node
     */
    n = n.add(1);

    /* end of format string is treated like a non-digit separator */
    if (*n).r#type == NODE_TYPE_END {
        return true;
    }

    if (*n).r#type == NODE_TYPE_ACTION {
        if (*(*n).key).is_digit {
            return false;
        }

        return true;
    } else if (*n).character[1] == b'\0' as c_char
        && isdigit((*n).character[0] as c_uchar as c_int) != 0
    {
        return false;
    }

    true /* some non-digit input (separator) */
}

fn adjust_partial_year_to_2020(year: c_int) -> c_int {
    /*
     * Adjust all dates toward 2020; this is effectively what happens when we
     * assume '70' is 1970 and '69' is 2069.
     */
    if year < 70 {
        year + 2000
    } else if year < 100 {
        year + 1900
    } else if year < 520 {
        year + 2000
    } else if year < 1000 {
        year + 1000
    } else {
        year
    }
}

unsafe fn strspace_len(mut str: *const c_char) -> c_int {
    let mut len: c_int = 0;

    while *str != 0 && isspace(*str as c_uchar as c_int) != 0 {
        str = str.add(1);
        len += 1;
    }
    len
}

/*
 * Set the date mode of a from-char conversion.
 */
unsafe fn from_char_set_mode(
    tmfc: *mut TmFromChar,
    mode: FromCharDateMode,
    _escontext: *mut Node,
) -> bool {
    if mode != FROM_CHAR_DATE_NONE {
        if (*tmfc).mode == FROM_CHAR_DATE_NONE {
            (*tmfc).mode = mode;
        } else if (*tmfc).mode != mode {
            ereport!(ERROR, errmsg!("invalid combination of date conventions"));
        }
    }
    true
}

/*
 * Set the integer pointed to by 'dest' to the given value.
 */
unsafe fn from_char_set_int(
    dest: *mut c_int,
    value: c_int,
    node: *const FormatNode,
    _escontext: *mut Node,
) -> bool {
    if *dest != 0 && *dest != value {
        ereport!(
            ERROR,
            errmsg!(
                "conflicting values for \"{}\" field in formatting string",
                cstr((*(*node).key).name)
            )
        );
    }
    *dest = value;
    true
}

/*
 * Read a single integer from the source string, into the int pointed to by
 * 'dest'. If 'dest' is NULL, the result is discarded.
 */
unsafe fn from_char_parse_int_len(
    dest: *mut c_int,
    src: *mut *const c_char,
    len: c_int,
    node: *mut FormatNode,
    escontext: *mut Node,
) -> c_int {
    let result: c_long;
    let mut copy: [c_char; DCH_MAX_ITEM_SIZ + 1] = [0; DCH_MAX_ITEM_SIZ + 1];
    let init = *src;
    let mut used: c_int;

    /*
     * Skip any whitespace before parsing the integer.
     */
    *src = (*src).offset(strspace_len(*src) as isize);

    Assert!(len <= DCH_MAX_ITEM_SIZ as c_int);
    used = strlcpy(copy.as_mut_ptr(), *src, (len + 1) as usize) as c_int;

    if S_FM((*node).suffix) != 0 || is_next_separator(node) {
        /*
         * This node is in Fill Mode, or the next node is known to be a
         * non-digit value, so we just slurp as many characters as we can get.
         */
        let mut endptr: *mut c_char = null_mut();

        set_errno(0);
        result = strtol(init, &raw mut endptr, 10);
        *src = endptr;
    } else {
        /*
         * We need to pull exactly the number of characters given in 'len' out
         * of the string, and convert those.
         */
        let mut last: *mut c_char = null_mut();

        if used < len {
            ereport!(
                ERROR,
                errmsg!(
                    "source string too short for \"{}\" formatting field",
                    cstr((*(*node).key).name)
                )
            );
        }

        set_errno(0);
        result = strtol(copy.as_ptr(), &raw mut last, 10);
        used = (last as usize - copy.as_ptr() as usize) as c_int;

        if used > 0 && used < len {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid value \"{}\" for \"{}\"",
                    cstr(copy.as_ptr()),
                    cstr((*(*node).key).name)
                )
            );
        }

        *src = (*src).offset(used as isize);
    }

    if *src == init {
        ereport!(
            ERROR,
            errmsg!(
                "invalid value \"{}\" for \"{}\"",
                cstr(copy.as_ptr()),
                cstr((*(*node).key).name)
            )
        );
    }

    if get_errno() == ERANGE || result < INT_MIN_C || result > INT_MAX_C {
        ereport!(
            ERROR,
            errmsg!(
                "value for \"{}\" in source string is out of range",
                cstr((*(*node).key).name)
            )
        );
    }

    if !dest.is_null() {
        if !from_char_set_int(dest, result as c_int, node, escontext) {
            return -1;
        }
    }

    (*src as usize - init as usize) as c_int
}

/*
 * Call from_char_parse_int_len(), using the length of the format keyword as
 * the expected length of the field.
 */
unsafe fn from_char_parse_int(
    dest: *mut c_int,
    src: *mut *const c_char,
    node: *mut FormatNode,
    escontext: *mut Node,
) -> c_int {
    from_char_parse_int_len(dest, src, (*(*node).key).len, node, escontext)
}

/*
 * Sequentially search null-terminated "array" for a case-insensitive match
 * to the initial character(s) of "name".
 */
unsafe fn seq_search_ascii(
    name: *const c_char,
    array: *const *const c_char,
    len: *mut c_int,
) -> c_int {
    let firstc: c_uchar;
    let mut a: *const *const c_char;

    *len = 0;

    /* empty string can't match anything */
    if *name == 0 {
        return -1;
    }

    /* we handle first char specially to gain some speed */
    firstc = pg_ascii_tolower(*name as c_uchar);

    a = array;
    while !(*a).is_null() {
        let mut p: *const c_char;
        let mut n: *const c_char;

        /* compare first chars */
        if pg_ascii_tolower(**a as c_uchar) != firstc {
            a = a.add(1);
            continue;
        }

        /* compare rest of string */
        p = (*a).add(1);
        n = name.add(1);
        loop {
            /* return success if we matched whole array entry */
            if *p == b'\0' as c_char {
                *len = (n as usize - name as usize) as c_int;
                return (a as usize - array as usize) as c_int
                    / core::mem::size_of::<*const c_char>() as c_int;
            }
            /* else, must have another character in "name" ... */
            if *n == b'\0' as c_char {
                break;
            }
            /* ... and it must match */
            if pg_ascii_tolower(*p as c_uchar) != pg_ascii_tolower(*n as c_uchar) {
                break;
            }
            p = p.add(1);
            n = n.add(1);
        }
        a = a.add(1);
    }

    -1
}

/*
 * Sequentially search an array of possibly non-English words for a
 * case-insensitive match to the initial character(s) of "name".
 */
unsafe fn seq_search_localized(
    name: *const c_char,
    array: *mut *mut c_char,
    len: *mut c_int,
    collid: Oid,
) -> c_int {
    let mut a: *mut *mut c_char;
    let upper_name: *mut c_char;
    let lower_name: *mut c_char;

    *len = 0;

    /* empty string can't match anything */
    if *name == 0 {
        return -1;
    }

    /*
     * The case-folding processing done below is fairly expensive, so before
     * doing that, make a quick pass to see if there is an exact match.
     */
    a = array;
    while !(*a).is_null() {
        let element_len = strlen(*a) as c_int;

        if strncmp(name, *a, element_len as usize) == 0 {
            *len = element_len;
            return (a as usize - array as usize) as c_int
                / core::mem::size_of::<*mut c_char>() as c_int;
        }
        a = a.add(1);
    }

    /*
     * Fold to upper case, then to lower case, so that we can match reliably
     * even in languages in which case conversions are not injective.
     */
    upper_name = str_toupper(name, strlen(name), collid);
    lower_name = str_tolower(upper_name, strlen(upper_name), collid);
    pfree(upper_name as *mut c_void);

    a = array;
    while !(*a).is_null() {
        let upper_element: *mut c_char;
        let lower_element: *mut c_char;
        let element_len: c_int;

        /* Likewise upper/lower-case array element */
        upper_element = str_toupper(*a, strlen(*a), collid);
        lower_element = str_tolower(upper_element, strlen(upper_element), collid);
        pfree(upper_element as *mut c_void);
        element_len = strlen(lower_element) as c_int;

        /* Match? */
        if strncmp(lower_name, lower_element, element_len as usize) == 0 {
            *len = element_len;
            pfree(lower_element as *mut c_void);
            pfree(lower_name as *mut c_void);
            return (a as usize - array as usize) as c_int
                / core::mem::size_of::<*mut c_char>() as c_int;
        }
        pfree(lower_element as *mut c_void);
        a = a.add(1);
    }

    pfree(lower_name as *mut c_void);
    -1
}

/*
 * Perform a sequential search in 'array' (or 'localized_array', if that's not
 * NULL) for an entry matching the first character(s) of the 'src' string
 * case-insensitively.
 */
unsafe fn from_char_seq_search(
    dest: *mut c_int,
    src: *mut *const c_char,
    array: *const *const c_char,
    localized_array: *mut *mut c_char,
    collid: Oid,
    node: *mut FormatNode,
    _escontext: *mut Node,
) -> bool {
    let len: c_int;

    if localized_array.is_null() {
        let mut l: c_int = 0;
        *dest = seq_search_ascii(*src, array, &raw mut l);
        len = l;
    } else {
        let mut l: c_int = 0;
        *dest = seq_search_localized(*src, localized_array, &raw mut l, collid);
        len = l;
    }

    if len <= 0 {
        /*
         * In the error report, truncate the string at the next whitespace (if
         * any) to avoid including irrelevant data.
         */
        let copy = pstrdup(*src);
        let mut c: *mut c_char;

        c = copy;
        while *c != 0 {
            if scanner_isspace(*c) {
                *c = b'\0' as c_char;
                break;
            }
            c = c.add(1);
        }

        ereport!(
            ERROR,
            errmsg!(
                "invalid value \"{}\" for \"{}\"",
                cstr(copy),
                cstr((*(*node).key).name)
            )
        );
    }
    *src = (*src).offset(len as isize);
    true
}

/*
 * Process a TmToChar struct as denoted by a list of FormatNodes.
 * The formatted data is written to the string pointed to by 'out'.
 */
unsafe fn DCH_to_char(
    node: *mut FormatNode,
    is_interval: bool,
    inp: *mut TmToChar,
    out: *mut c_char,
    collid: Oid,
) {
    let mut n: *mut FormatNode;
    let mut s: *mut c_char;
    let tm: *mut fmt_tm = &raw mut (*inp).tm;
    let mut i: c_int;

    /* cache localized days and months */
    cache_locale_time();

    /* localized day/month caches used in S_TM branches */
    let localized_full_months_p = &raw mut localized_full_months as *mut *mut c_char;
    let localized_abbrev_months_p = &raw mut localized_abbrev_months as *mut *mut c_char;
    let localized_full_days_p = &raw mut localized_full_days as *mut *mut c_char;
    let localized_abbrev_days_p = &raw mut localized_abbrev_days as *mut *mut c_char;

    s = out;
    n = node;
    while (*n).r#type != NODE_TYPE_END {
        if (*n).r#type != NODE_TYPE_ACTION {
            strcpy(s, (*n).character.as_ptr());
            s = s.add(strlen(s));
            n = n.add(1);
            continue;
        }

        match (*(*n).key).id {
            x if x == DCH_A_M || x == DCH_P_M => {
                let v = if (*tm).tm_hour % HOURS_PER_DAY as int64 >= (HOURS_PER_DAY / 2) as int64 {
                    P_M_STR
                } else {
                    A_M_STR
                };
                strcpy(s, v.as_ptr());
                s = s.add(strlen(s));
            }
            x if x == DCH_AM || x == DCH_PM => {
                let v = if (*tm).tm_hour % HOURS_PER_DAY as int64 >= (HOURS_PER_DAY / 2) as int64 {
                    PM_STR
                } else {
                    AM_STR
                };
                strcpy(s, v.as_ptr());
                s = s.add(strlen(s));
            }
            x if x == DCH_a_m || x == DCH_p_m => {
                let v = if (*tm).tm_hour % HOURS_PER_DAY as int64 >= (HOURS_PER_DAY / 2) as int64 {
                    p_m_STR
                } else {
                    a_m_STR
                };
                strcpy(s, v.as_ptr());
                s = s.add(strlen(s));
            }
            x if x == DCH_am || x == DCH_pm => {
                let v = if (*tm).tm_hour % HOURS_PER_DAY as int64 >= (HOURS_PER_DAY / 2) as int64 {
                    pm_STR
                } else {
                    am_STR
                };
                strcpy(s, v.as_ptr());
                s = s.add(strlen(s));
            }
            x if x == DCH_HH || x == DCH_HH12 => {
                /* display time as shown on a 12-hour clock, even for intervals */
                let width = if S_FM((*n).suffix) != 0 {
                    0
                } else if (*tm).tm_hour >= 0 {
                    2
                } else {
                    3
                };
                let v = if (*tm).tm_hour % (HOURS_PER_DAY / 2) as int64 == 0 {
                    (HOURS_PER_DAY / 2) as int64
                } else {
                    (*tm).tm_hour % (HOURS_PER_DAY / 2) as int64
                };
                write_cstr(s, &fmt_0lld(width, v));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_HH24 => {
                let width = if S_FM((*n).suffix) != 0 {
                    0
                } else if (*tm).tm_hour >= 0 {
                    2
                } else {
                    3
                };
                write_cstr(s, &fmt_0lld(width, (*tm).tm_hour));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_MI => {
                let width = if S_FM((*n).suffix) != 0 {
                    0
                } else if (*tm).tm_min >= 0 {
                    2
                } else {
                    3
                };
                write_cstr(s, &fmt_0d(width, (*tm).tm_min));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_SS => {
                let width = if S_FM((*n).suffix) != 0 {
                    0
                } else if (*tm).tm_sec >= 0 {
                    2
                } else {
                    3
                };
                write_cstr(s, &fmt_0d(width, (*tm).tm_sec));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_FF1 => {
                write_cstr(s, &format!("{:01}", (*inp).fsec / 100000));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_FF2 => {
                write_cstr(s, &format!("{:02}", (*inp).fsec / 10000));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_FF3 || x == DCH_MS => {
                write_cstr(s, &format!("{:03}", (*inp).fsec / 1000));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_FF4 => {
                write_cstr(s, &format!("{:04}", (*inp).fsec / 100));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_FF5 => {
                write_cstr(s, &format!("{:05}", (*inp).fsec / 10));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_FF6 || x == DCH_US => {
                write_cstr(s, &format!("{:06}", (*inp).fsec));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_SSSS => {
                write_cstr(
                    s,
                    &format!(
                        "{}",
                        (*tm).tm_hour * SECS_PER_HOUR as int64
                            + ((*tm).tm_min * SECS_PER_MINUTE) as int64
                            + (*tm).tm_sec as int64
                    ),
                );
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_tz => {
                INVALID_FOR_INTERVAL!(is_interval);
                if !tmtcTzn(inp).is_null() {
                    /* We assume here that timezone names aren't localized */
                    let p = asc_tolower_z(tmtcTzn(inp));

                    strcpy(s, p);
                    pfree(p as *mut c_void);
                    s = s.add(strlen(s));
                }
            }
            x if x == DCH_TZ => {
                INVALID_FOR_INTERVAL!(is_interval);
                if !tmtcTzn(inp).is_null() {
                    strcpy(s, tmtcTzn(inp));
                    s = s.add(strlen(s));
                }
            }
            x if x == DCH_TZH => {
                INVALID_FOR_INTERVAL!(is_interval);
                write_cstr(
                    s,
                    &format!(
                        "{}{:02}",
                        if (*tm).tm_gmtoff >= 0 { '+' } else { '-' },
                        abs_i32((*tm).tm_gmtoff as c_int) / SECS_PER_HOUR
                    ),
                );
                s = s.add(strlen(s));
            }
            x if x == DCH_TZM => {
                INVALID_FOR_INTERVAL!(is_interval);
                write_cstr(
                    s,
                    &format!(
                        "{:02}",
                        (abs_i32((*tm).tm_gmtoff as c_int) % SECS_PER_HOUR) / SECS_PER_MINUTE
                    ),
                );
                s = s.add(strlen(s));
            }
            x if x == DCH_OF => {
                INVALID_FOR_INTERVAL!(is_interval);
                let w = if S_FM((*n).suffix) != 0 { 0 } else { 2 };
                write_cstr(
                    s,
                    &format!(
                        "{}{}",
                        if (*tm).tm_gmtoff >= 0 { '+' } else { '-' },
                        fmt_0d(w, abs_i32((*tm).tm_gmtoff as c_int) / SECS_PER_HOUR)
                    ),
                );
                s = s.add(strlen(s));
                if abs_i32((*tm).tm_gmtoff as c_int) % SECS_PER_HOUR != 0 {
                    write_cstr(
                        s,
                        &format!(
                            ":{:02}",
                            (abs_i32((*tm).tm_gmtoff as c_int) % SECS_PER_HOUR) / SECS_PER_MINUTE
                        ),
                    );
                    s = s.add(strlen(s));
                }
            }
            x if x == DCH_A_D || x == DCH_B_C => {
                INVALID_FOR_INTERVAL!(is_interval);
                strcpy(s, if (*tm).tm_year <= 0 { B_C_STR } else { A_D_STR }.as_ptr());
                s = s.add(strlen(s));
            }
            x if x == DCH_AD || x == DCH_BC => {
                INVALID_FOR_INTERVAL!(is_interval);
                strcpy(s, if (*tm).tm_year <= 0 { BC_STR } else { AD_STR }.as_ptr());
                s = s.add(strlen(s));
            }
            x if x == DCH_a_d || x == DCH_b_c => {
                INVALID_FOR_INTERVAL!(is_interval);
                strcpy(s, if (*tm).tm_year <= 0 { b_c_STR } else { a_d_STR }.as_ptr());
                s = s.add(strlen(s));
            }
            x if x == DCH_ad || x == DCH_bc => {
                INVALID_FOR_INTERVAL!(is_interval);
                strcpy(s, if (*tm).tm_year <= 0 { bc_STR } else { ad_STR }.as_ptr());
                s = s.add(strlen(s));
            }
            x if x == DCH_MONTH => {
                INVALID_FOR_INTERVAL!(is_interval);
                if (*tm).tm_mon == 0 {
                    n = n.add(1);
                    continue;
                }
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_toupper_z(*localized_full_months_p.offset(((*tm).tm_mon - 1) as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    write_cstr(
                        s,
                        &fmt_pad_str(
                            if S_FM((*n).suffix) != 0 { 0 } else { -9 },
                            asc_toupper_z(months_full[((*tm).tm_mon - 1) as usize]),
                        ),
                    );
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_Month => {
                INVALID_FOR_INTERVAL!(is_interval);
                if (*tm).tm_mon == 0 {
                    n = n.add(1);
                    continue;
                }
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_initcap_z(*localized_full_months_p.offset(((*tm).tm_mon - 1) as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    write_cstr(
                        s,
                        &fmt_pad_str(
                            if S_FM((*n).suffix) != 0 { 0 } else { -9 },
                            months_full[((*tm).tm_mon - 1) as usize],
                        ),
                    );
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_month => {
                INVALID_FOR_INTERVAL!(is_interval);
                if (*tm).tm_mon == 0 {
                    n = n.add(1);
                    continue;
                }
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_tolower_z(*localized_full_months_p.offset(((*tm).tm_mon - 1) as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    write_cstr(
                        s,
                        &fmt_pad_str(
                            if S_FM((*n).suffix) != 0 { 0 } else { -9 },
                            asc_tolower_z(months_full[((*tm).tm_mon - 1) as usize]),
                        ),
                    );
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_MON => {
                INVALID_FOR_INTERVAL!(is_interval);
                if (*tm).tm_mon == 0 {
                    n = n.add(1);
                    continue;
                }
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_toupper_z(*localized_abbrev_months_p.offset(((*tm).tm_mon - 1) as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    strcpy(s, asc_toupper_z(months[((*tm).tm_mon - 1) as usize]));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_Mon => {
                INVALID_FOR_INTERVAL!(is_interval);
                if (*tm).tm_mon == 0 {
                    n = n.add(1);
                    continue;
                }
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_initcap_z(*localized_abbrev_months_p.offset(((*tm).tm_mon - 1) as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    strcpy(s, months[((*tm).tm_mon - 1) as usize]);
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_mon => {
                INVALID_FOR_INTERVAL!(is_interval);
                if (*tm).tm_mon == 0 {
                    n = n.add(1);
                    continue;
                }
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_tolower_z(*localized_abbrev_months_p.offset(((*tm).tm_mon - 1) as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    strcpy(s, asc_tolower_z(months[((*tm).tm_mon - 1) as usize]));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_MM => {
                let width = if S_FM((*n).suffix) != 0 {
                    0
                } else if (*tm).tm_mon >= 0 {
                    2
                } else {
                    3
                };
                write_cstr(s, &fmt_0d(width, (*tm).tm_mon));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_DAY => {
                INVALID_FOR_INTERVAL!(is_interval);
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_toupper_z(*localized_full_days_p.offset((*tm).tm_wday as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    write_cstr(
                        s,
                        &fmt_pad_str(
                            if S_FM((*n).suffix) != 0 { 0 } else { -9 },
                            asc_toupper_z(days[(*tm).tm_wday as usize]),
                        ),
                    );
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_Day => {
                INVALID_FOR_INTERVAL!(is_interval);
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_initcap_z(*localized_full_days_p.offset((*tm).tm_wday as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    write_cstr(
                        s,
                        &fmt_pad_str(
                            if S_FM((*n).suffix) != 0 { 0 } else { -9 },
                            days[(*tm).tm_wday as usize],
                        ),
                    );
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_day => {
                INVALID_FOR_INTERVAL!(is_interval);
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_tolower_z(*localized_full_days_p.offset((*tm).tm_wday as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    write_cstr(
                        s,
                        &fmt_pad_str(
                            if S_FM((*n).suffix) != 0 { 0 } else { -9 },
                            asc_tolower_z(days[(*tm).tm_wday as usize]),
                        ),
                    );
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_DY => {
                INVALID_FOR_INTERVAL!(is_interval);
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_toupper_z(*localized_abbrev_days_p.offset((*tm).tm_wday as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    strcpy(s, asc_toupper_z(days_short[(*tm).tm_wday as usize]));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_Dy => {
                INVALID_FOR_INTERVAL!(is_interval);
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_initcap_z(*localized_abbrev_days_p.offset((*tm).tm_wday as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    strcpy(s, days_short[(*tm).tm_wday as usize]);
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_dy => {
                INVALID_FOR_INTERVAL!(is_interval);
                if S_TM((*n).suffix) != 0 {
                    let str =
                        str_tolower_z(*localized_abbrev_days_p.offset((*tm).tm_wday as isize), collid);
                    DCH_check_localized(s, str, (*n).key);
                } else {
                    strcpy(s, asc_tolower_z(days_short[(*tm).tm_wday as usize]));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_DDD || x == DCH_IDDD => {
                let v = if (*(*n).key).id == DCH_DDD {
                    (*tm).tm_yday
                } else {
                    date2isoyearday((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday)
                };
                write_cstr(s, &fmt_0d(if S_FM((*n).suffix) != 0 { 0 } else { 3 }, v));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_DD => {
                write_cstr(s, &fmt_0d(if S_FM((*n).suffix) != 0 { 0 } else { 2 }, (*tm).tm_mday));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_D => {
                INVALID_FOR_INTERVAL!(is_interval);
                write_cstr(s, &format!("{}", (*tm).tm_wday + 1));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_ID => {
                INVALID_FOR_INTERVAL!(is_interval);
                write_cstr(s, &format!("{}", if (*tm).tm_wday == 0 { 7 } else { (*tm).tm_wday }));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_WW => {
                write_cstr(
                    s,
                    &fmt_0d(if S_FM((*n).suffix) != 0 { 0 } else { 2 }, ((*tm).tm_yday - 1) / 7 + 1),
                );
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_IW => {
                write_cstr(
                    s,
                    &fmt_0d(
                        if S_FM((*n).suffix) != 0 { 0 } else { 2 },
                        date2isoweek((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday),
                    ),
                );
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_Q => {
                if (*tm).tm_mon == 0 {
                    n = n.add(1);
                    continue;
                }
                write_cstr(s, &format!("{}", ((*tm).tm_mon - 1) / 3 + 1));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_CC => {
                if is_interval {
                    /* straight calculation */
                    i = (*tm).tm_year / 100;
                } else if (*tm).tm_year > 0 {
                    /* Century 20 == 1901 - 2000 */
                    i = ((*tm).tm_year - 1) / 100 + 1;
                } else {
                    /* Century 6BC == 600BC - 501BC */
                    i = (*tm).tm_year / 100 - 1;
                }
                if i <= 99 && i >= -99 {
                    write_cstr(
                        s,
                        &fmt_0d(if S_FM((*n).suffix) != 0 { 0 } else if i >= 0 { 2 } else { 3 }, i),
                    );
                } else {
                    write_cstr(s, &format!("{}", i));
                }
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_Y_YYY => {
                i = ADJUST_YEAR((*tm).tm_year, is_interval) / 1000;
                write_cstr(
                    s,
                    &format!(
                        "{},{:03}",
                        i,
                        ADJUST_YEAR((*tm).tm_year, is_interval) - (i * 1000)
                    ),
                );
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_YYYY || x == DCH_IYYY => {
                let yr = if (*(*n).key).id == DCH_YYYY {
                    ADJUST_YEAR((*tm).tm_year, is_interval)
                } else {
                    ADJUST_YEAR(
                        date2isoyear((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday),
                        is_interval,
                    )
                };
                let w = if S_FM((*n).suffix) != 0 {
                    0
                } else if ADJUST_YEAR((*tm).tm_year, is_interval) >= 0 {
                    4
                } else {
                    5
                };
                write_cstr(s, &fmt_0d(w, yr));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_YYY || x == DCH_IYY => {
                let yr = if (*(*n).key).id == DCH_YYY {
                    ADJUST_YEAR((*tm).tm_year, is_interval)
                } else {
                    ADJUST_YEAR(
                        date2isoyear((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday),
                        is_interval,
                    )
                } % 1000;
                let w = if S_FM((*n).suffix) != 0 {
                    0
                } else if ADJUST_YEAR((*tm).tm_year, is_interval) >= 0 {
                    3
                } else {
                    4
                };
                write_cstr(s, &fmt_0d(w, yr));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_YY || x == DCH_IY => {
                let yr = if (*(*n).key).id == DCH_YY {
                    ADJUST_YEAR((*tm).tm_year, is_interval)
                } else {
                    ADJUST_YEAR(
                        date2isoyear((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday),
                        is_interval,
                    )
                } % 100;
                let w = if S_FM((*n).suffix) != 0 {
                    0
                } else if ADJUST_YEAR((*tm).tm_year, is_interval) >= 0 {
                    2
                } else {
                    3
                };
                write_cstr(s, &fmt_0d(w, yr));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_Y || x == DCH_I => {
                let yr = if (*(*n).key).id == DCH_Y {
                    ADJUST_YEAR((*tm).tm_year, is_interval)
                } else {
                    ADJUST_YEAR(
                        date2isoyear((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday),
                        is_interval,
                    )
                } % 10;
                write_cstr(s, &format!("{}", yr));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_RM || x == DCH_rm => {
                /*
                 * For intervals, values like '12 month' will be reduced to 0
                 * month and some years.  These should be processed.
                 */
                if (*tm).tm_mon == 0 && (*tm).tm_year == 0 {
                    n = n.add(1);
                    continue;
                } else {
                    let mon: c_int;
                    let months_r: *const *const c_char;

                    if (*(*n).key).id == DCH_RM {
                        months_r = &raw const rm_months_upper as *const *const c_char;
                    } else {
                        months_r = &raw const rm_months_lower as *const *const c_char;
                    }

                    /*
                     * Compute the position in the roman-numeral array.  Note that
                     * the contents of the array are reversed, December being
                     * first and January last.
                     */
                    if (*tm).tm_mon == 0 {
                        mon = if (*tm).tm_year >= 0 { 0 } else { MONTHS_PER_YEAR - 1 };
                    } else if (*tm).tm_mon < 0 {
                        mon = -1 * ((*tm).tm_mon + 1);
                    } else {
                        mon = MONTHS_PER_YEAR - (*tm).tm_mon;
                    }

                    write_cstr(
                        s,
                        &fmt_pad_str(
                            if S_FM((*n).suffix) != 0 { 0 } else { -4 },
                            *months_r.offset(mon as isize),
                        ),
                    );
                    s = s.add(strlen(s));
                }
            }
            x if x == DCH_W => {
                write_cstr(s, &format!("{}", ((*tm).tm_mday - 1) / 7 + 1));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            x if x == DCH_J => {
                write_cstr(s, &format!("{}", date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday)));
                if S_THth((*n).suffix) != 0 {
                    str_numth(s, s, S_TH_TYPE((*n).suffix));
                }
                s = s.add(strlen(s));
            }
            _ => {}
        }
        n = n.add(1);
    }

    *s = b'\0' as c_char;
}

/*
 * Helper for the S_TM branches in DCH_to_char: copy a localized string into
 * the output buffer, or error out if it is too long.  Mirrors the inline
 * `if (strlen(str) <= (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ)` checks.
 */
unsafe fn DCH_check_localized(s: *mut c_char, str: *mut c_char, key: *const KeyWord) {
    if strlen(str) <= (((*key).len + TM_SUFFIX_LEN) as usize) * DCH_MAX_ITEM_SIZ {
        strcpy(s, str);
    } else {
        ereport!(ERROR, errmsg!("localized string format value too long"));
    }
}

/*
 * Process the string 'in' as denoted by the array of FormatNodes 'node[]'.
 * The TmFromChar struct pointed to by 'out' is populated with the results.
 */
unsafe fn DCH_from_char(
    node: *mut FormatNode,
    inp: *const c_char,
    out: *mut TmFromChar,
    collid: Oid,
    std: bool,
    escontext: *mut Node,
) {
    let mut n: *mut FormatNode;
    let mut s: *const c_char;
    let mut len: c_int;
    let mut value: c_int = 0;
    let mut fx_mode = std;

    /* number of extra skipped characters (more than given in format string) */
    let mut extra_skip: c_int = 0;

    /* cache localized days and months */
    cache_locale_time();

    let localized_full_months_p = &raw mut localized_full_months as *mut *mut c_char;
    let localized_abbrev_months_p = &raw mut localized_abbrev_months as *mut *mut c_char;
    let localized_full_days_p = &raw mut localized_full_days as *mut *mut c_char;
    let localized_abbrev_days_p = &raw mut localized_abbrev_days as *mut *mut c_char;

    n = node;
    s = inp;
    while (*n).r#type != NODE_TYPE_END && *s != b'\0' as c_char {
        /*
         * Ignore spaces at the beginning of the string and before fields when
         * not in FX (fixed width) mode.
         */
        if !fx_mode
            && ((*n).r#type != NODE_TYPE_ACTION || (*(*n).key).id != DCH_FX)
            && ((*n).r#type == NODE_TYPE_ACTION || n == node)
        {
            while *s != b'\0' as c_char && isspace(*s as c_uchar as c_int) != 0 {
                s = s.add(1);
                extra_skip += 1;
            }
        }

        if (*n).r#type == NODE_TYPE_SPACE || (*n).r#type == NODE_TYPE_SEPARATOR {
            if std {
                /*
                 * Standard mode requires strict matching between format string
                 * separators/spaces and input string.
                 */
                Assert!((*n).character[0] != 0 && (*n).character[1] == 0);

                if *s == (*n).character[0] {
                    s = s.add(1);
                } else {
                    ereport!(
                        ERROR,
                        errmsg!("unmatched format separator \"{}\"", (*n).character[0] as u8 as char)
                    );
                }
            } else if !fx_mode {
                /*
                 * In non FX (fixed format) mode one format string space or
                 * separator match to one space or separator in input string.
                 */
                extra_skip -= 1;
                if isspace(*s as c_uchar as c_int) != 0 || is_separator_char(s) {
                    s = s.add(1);
                    extra_skip += 1;
                }
            } else {
                /*
                 * In FX mode, on format string space or separator we consume
                 * exactly one character from input string.
                 */
                s = s.offset(pg_mblen_cstr(s) as isize);
            }
            n = n.add(1);
            continue;
        } else if (*n).r#type != NODE_TYPE_ACTION {
            /*
             * Text character, so consume one character from input string.
             */
            if !fx_mode {
                if extra_skip > 0 {
                    extra_skip -= 1;
                } else {
                    s = s.offset(pg_mblen_cstr(s) as isize);
                }
            } else {
                let chlen = pg_mblen_cstr(s);

                /*
                 * Standard mode requires strict match of format characters.
                 */
                if std
                    && (*n).r#type == NODE_TYPE_CHAR
                    && strncmp(s, (*n).character.as_ptr(), chlen as usize) != 0
                {
                    ereport!(
                        ERROR,
                        errmsg!("unmatched format character \"{}\"", cstr((*n).character.as_ptr()))
                    );
                }

                s = s.offset(chlen as isize);
            }
            n = n.add(1);
            continue;
        }

        if !from_char_set_mode(out, (*(*n).key).date_mode, escontext) {
            return;
        }

        match (*(*n).key).id {
            x if x == DCH_FX => {
                fx_mode = true;
            }
            x if x == DCH_A_M || x == DCH_P_M || x == DCH_a_m || x == DCH_p_m => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut ampm_strings_long as *const *const c_char,
                    null_mut(),
                    InvalidOid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).pm, value % 2, n, escontext) {
                    return;
                }
                (*out).clock = CLOCK_12_HOUR;
            }
            x if x == DCH_AM || x == DCH_PM || x == DCH_am || x == DCH_pm => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut ampm_strings as *const *const c_char,
                    null_mut(),
                    InvalidOid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).pm, value % 2, n, escontext) {
                    return;
                }
                (*out).clock = CLOCK_12_HOUR;
            }
            x if x == DCH_HH || x == DCH_HH12 => {
                if from_char_parse_int_len(&raw mut (*out).hh, &raw mut s, 2, n, escontext) < 0 {
                    return;
                }
                (*out).clock = CLOCK_12_HOUR;
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_HH24 => {
                if from_char_parse_int_len(&raw mut (*out).hh, &raw mut s, 2, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_MI => {
                if from_char_parse_int(&raw mut (*out).mi, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_SS => {
                if from_char_parse_int(&raw mut (*out).ss, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_MS => {
                /* millisecond */
                len = from_char_parse_int_len(&raw mut (*out).ms, &raw mut s, 3, n, escontext);
                if len < 0 {
                    return;
                }

                /* 25 is 0.25 and 250 is 0.25 too; 025 is 0.025 and not 0.25 */
                (*out).ms *= if len == 1 {
                    100
                } else if len == 2 {
                    10
                } else {
                    1
                };

                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_FF1
                || x == DCH_FF2
                || x == DCH_FF3
                || x == DCH_FF4
                || x == DCH_FF5
                || x == DCH_FF6
                || x == DCH_US =>
            {
                if x != DCH_US {
                    (*out).ff = (*(*n).key).id - DCH_FF1 + 1;
                }
                /* microsecond / fractional, fall through to shared handling */
                len = from_char_parse_int_len(
                    &raw mut (*out).us,
                    &raw mut s,
                    if (*(*n).key).id == DCH_US { 6 } else { (*out).ff },
                    n,
                    escontext,
                );
                if len < 0 {
                    return;
                }

                (*out).us *= if len == 1 {
                    100000
                } else if len == 2 {
                    10000
                } else if len == 3 {
                    1000
                } else if len == 4 {
                    100
                } else if len == 5 {
                    10
                } else {
                    1
                };

                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_SSSS => {
                if from_char_parse_int(&raw mut (*out).ssss, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_tz || x == DCH_TZ || x == DCH_OF => {
                let mut handled_of = false;
                if x == DCH_tz || x == DCH_TZ {
                    let tzlen = DecodeTimezoneAbbrevPrefix(
                        s,
                        &raw mut (*out).gmtoffset,
                        &raw mut (*out).tzp,
                    );
                    if tzlen > 0 {
                        (*out).has_tz = true;
                        /* we only need the zone abbrev for DYNTZ case */
                        if !(*out).tzp.is_null() {
                            (*out).abbrev = pnstrdup(s, tzlen as usize);
                        }
                        (*out).tzsign = 0; /* drop any earlier TZH/TZM info */
                        s = s.offset(tzlen as isize);
                        handled_of = true;
                    } else if isalpha(*s as c_uchar as c_int) != 0 {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "invalid value \"{}\" for \"{}\"",
                                cstr(s),
                                cstr((*(*n).key).name)
                            )
                        );
                    }
                    /* otherwise parse it like OF */
                }
                if !handled_of {
                    /* OF is equivalent to TZH or TZH:TZM */
                    if *s == b'+' as c_char || *s == b'-' as c_char || *s == b' ' as c_char {
                        (*out).tzsign = if *s == b'-' as c_char { -1 } else { 1 };
                        s = s.add(1);
                    } else if extra_skip > 0 && *s.offset(-1) == b'-' as c_char {
                        (*out).tzsign = -1;
                    } else {
                        (*out).tzsign = 1;
                    }
                    if from_char_parse_int_len(&raw mut (*out).tzh, &raw mut s, 2, n, escontext) < 0
                    {
                        return;
                    }
                    if *s == b':' as c_char {
                        s = s.add(1);
                        if from_char_parse_int_len(
                            &raw mut (*out).tzm,
                            &raw mut s,
                            2,
                            n,
                            escontext,
                        ) < 0
                        {
                            return;
                        }
                    }
                }
            }
            x if x == DCH_TZH => {
                /*
                 * Value of TZH might be negative.  And the issue is that we
                 * might swallow minus sign as the separator.
                 */
                if *s == b'+' as c_char || *s == b'-' as c_char || *s == b' ' as c_char {
                    (*out).tzsign = if *s == b'-' as c_char { -1 } else { 1 };
                    s = s.add(1);
                } else if extra_skip > 0 && *s.offset(-1) == b'-' as c_char {
                    (*out).tzsign = -1;
                } else {
                    (*out).tzsign = 1;
                }

                if from_char_parse_int_len(&raw mut (*out).tzh, &raw mut s, 2, n, escontext) < 0 {
                    return;
                }
            }
            x if x == DCH_TZM => {
                /* assign positive timezone sign if TZH was not seen before */
                if (*out).tzsign == 0 {
                    (*out).tzsign = 1;
                }
                if from_char_parse_int_len(&raw mut (*out).tzm, &raw mut s, 2, n, escontext) < 0 {
                    return;
                }
            }
            x if x == DCH_A_D || x == DCH_B_C || x == DCH_a_d || x == DCH_b_c => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut adbc_strings_long as *const *const c_char,
                    null_mut(),
                    InvalidOid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).bc, value % 2, n, escontext) {
                    return;
                }
            }
            x if x == DCH_AD || x == DCH_BC || x == DCH_ad || x == DCH_bc => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut adbc_strings as *const *const c_char,
                    null_mut(),
                    InvalidOid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).bc, value % 2, n, escontext) {
                    return;
                }
            }
            x if x == DCH_MONTH || x == DCH_Month || x == DCH_month => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut months_full as *const *const c_char,
                    if S_TM((*n).suffix) != 0 {
                        localized_full_months_p
                    } else {
                        null_mut()
                    },
                    collid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).mm, value + 1, n, escontext) {
                    return;
                }
            }
            x if x == DCH_MON || x == DCH_Mon || x == DCH_mon => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut months as *const *const c_char,
                    if S_TM((*n).suffix) != 0 {
                        localized_abbrev_months_p
                    } else {
                        null_mut()
                    },
                    collid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).mm, value + 1, n, escontext) {
                    return;
                }
            }
            x if x == DCH_MM => {
                if from_char_parse_int(&raw mut (*out).mm, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_DAY || x == DCH_Day || x == DCH_day => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut days as *const *const c_char,
                    if S_TM((*n).suffix) != 0 {
                        localized_full_days_p
                    } else {
                        null_mut()
                    },
                    collid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).d, value, n, escontext) {
                    return;
                }
                (*out).d += 1;
            }
            x if x == DCH_DY || x == DCH_Dy || x == DCH_dy => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut days_short as *const *const c_char,
                    if S_TM((*n).suffix) != 0 {
                        localized_abbrev_days_p
                    } else {
                        null_mut()
                    },
                    collid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).d, value, n, escontext) {
                    return;
                }
                (*out).d += 1;
            }
            x if x == DCH_DDD => {
                if from_char_parse_int(&raw mut (*out).ddd, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_IDDD => {
                if from_char_parse_int_len(&raw mut (*out).ddd, &raw mut s, 3, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_DD => {
                if from_char_parse_int(&raw mut (*out).dd, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_D => {
                if from_char_parse_int(&raw mut (*out).d, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_ID => {
                if from_char_parse_int_len(&raw mut (*out).d, &raw mut s, 1, n, escontext) < 0 {
                    return;
                }
                /* Shift numbering to match Gregorian where Sunday = 1 */
                (*out).d += 1;
                if (*out).d > 7 {
                    (*out).d = 1;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_WW || x == DCH_IW => {
                if from_char_parse_int(&raw mut (*out).ww, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_Q => {
                /*
                 * We ignore 'Q' when converting to date because it is unclear
                 * which date in the quarter to use.
                 */
                if from_char_parse_int(null_mut(), &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_CC => {
                if from_char_parse_int(&raw mut (*out).cc, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_Y_YYY => {
                let mut millennia: c_int = 0;
                let mut years: c_int = 0;
                let mut nch: c_int = 0;

                let matched = sscanf_y_yyy(s, &raw mut millennia, &raw mut years, &raw mut nch);
                if matched < 2 {
                    ereport!(
                        ERROR,
                        errmsg!("invalid value \"{}\" for \"{}\"", cstr(s), "Y,YYY")
                    );
                }

                /* years += (millennia * 1000); */
                if pg_mul_s32_overflow(millennia, 1000, &raw mut millennia)
                    || pg_add_s32_overflow(years, millennia, &raw mut years)
                {
                    ereport!(
                        ERROR,
                        errmsg!("value for \"{}\" in source string is out of range", "Y,YYY")
                    );
                }

                if !from_char_set_int(&raw mut (*out).year, years, n, escontext) {
                    return;
                }
                (*out).yysz = 4;
                s = s.offset(nch as isize);
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_YYYY || x == DCH_IYYY => {
                if from_char_parse_int(&raw mut (*out).year, &raw mut s, n, escontext) < 0 {
                    return;
                }
                (*out).yysz = 4;
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_YYY || x == DCH_IYY => {
                len = from_char_parse_int(&raw mut (*out).year, &raw mut s, n, escontext);
                if len < 0 {
                    return;
                }
                if len < 4 {
                    (*out).year = adjust_partial_year_to_2020((*out).year);
                }
                (*out).yysz = 3;
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_YY || x == DCH_IY => {
                len = from_char_parse_int(&raw mut (*out).year, &raw mut s, n, escontext);
                if len < 0 {
                    return;
                }
                if len < 4 {
                    (*out).year = adjust_partial_year_to_2020((*out).year);
                }
                (*out).yysz = 2;
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_Y || x == DCH_I => {
                len = from_char_parse_int(&raw mut (*out).year, &raw mut s, n, escontext);
                if len < 0 {
                    return;
                }
                if len < 4 {
                    (*out).year = adjust_partial_year_to_2020((*out).year);
                }
                (*out).yysz = 1;
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_RM || x == DCH_rm => {
                if !from_char_seq_search(
                    &raw mut value,
                    &raw mut s,
                    &raw mut rm_months_lower as *const *const c_char,
                    null_mut(),
                    InvalidOid,
                    n,
                    escontext,
                ) {
                    return;
                }
                if !from_char_set_int(&raw mut (*out).mm, MONTHS_PER_YEAR - value, n, escontext) {
                    return;
                }
            }
            x if x == DCH_W => {
                if from_char_parse_int(&raw mut (*out).w, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            x if x == DCH_J => {
                if from_char_parse_int(&raw mut (*out).j, &raw mut s, n, escontext) < 0 {
                    return;
                }
                SKIP_THth!(s, (*n).suffix);
            }
            _ => {}
        }

        /* Ignore all spaces after fields */
        if !fx_mode {
            extra_skip = 0;
            while *s != b'\0' as c_char && isspace(*s as c_uchar as c_int) != 0 {
                s = s.add(1);
                extra_skip += 1;
            }
        }
        n = n.add(1);
    }

    /*
     * Standard parsing mode doesn't allow unmatched format patterns or trailing
     * characters in the input string.
     */
    if std {
        if (*n).r#type != NODE_TYPE_END {
            ereport!(ERROR, errmsg!("input string is too short for datetime format"));
        }

        while *s != b'\0' as c_char && isspace(*s as c_uchar as c_int) != 0 {
            s = s.add(1);
        }

        if *s != b'\0' as c_char {
            ereport!(
                ERROR,
                errmsg!("trailing characters remain in input string after datetime format")
            );
        }
    }
}

/* sscanf(s, "%d,%03d%n", &millennia, &years, &nch) helper for DCH_Y_YYY. */
unsafe fn sscanf_y_yyy(
    s: *const c_char,
    millennia: *mut c_int,
    years: *mut c_int,
    nch: *mut c_int,
) -> c_int {
    let bytes = std::ffi::CStr::from_ptr(s).to_bytes();
    let text = String::from_utf8_lossy(bytes);
    let mut chars = 0usize;
    let mut matched = 0;

    /* %d for millennia: optional sign + digits, leading whitespace skipped */
    let trimmed_lead = text.len() - text.trim_start().len();
    chars += trimmed_lead;
    let rest = &text[chars..];
    let (mlen, mval) = scan_signed_int(rest);
    if mlen == 0 {
        return 0;
    }
    *millennia = mval;
    chars += mlen;
    matched += 1;

    /* literal comma */
    if chars >= text.len() || text.as_bytes()[chars] != b',' {
        return matched;
    }
    chars += 1;

    /* %03d for years: width-limited (3) but %d ignores width on input width,
     * so scan up to 3 digits (matching glibc behavior for "%03d"). */
    let rest = &text[chars..];
    let (ylen, yval) = scan_signed_int_maxwidth(rest, 3);
    if ylen == 0 {
        return matched;
    }
    *years = yval;
    chars += ylen;
    matched += 1;

    /* %n */
    *nch = chars as c_int;
    matched
}

fn scan_signed_int(s: &str) -> (usize, c_int) {
    let bytes = s.as_bytes();
    let mut i = 0;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    let start_digits = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == start_digits {
        return (0, 0);
    }
    match s[..i].parse::<c_int>() {
        Ok(v) => (i, v),
        Err(_) => (0, 0),
    }
}

fn scan_signed_int_maxwidth(s: &str, maxw: usize) -> (usize, c_int) {
    let bytes = s.as_bytes();
    let mut i = 0;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    let start_digits = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() && (i - start_digits) < maxw {
        i += 1;
    }
    if i == start_digits {
        return (0, 0);
    }
    match s[..i].parse::<c_int>() {
        Ok(v) => (i, v),
        Err(_) => (0, 0),
    }
}

/*
 * The invariant for DCH cache entry management is that DCHCounter is equal to
 * the maximum age value among the existing entries.
 */
#[inline]
unsafe fn DCH_prevent_counter_overflow() {
    if DCHCounter >= (INT_MAX - 1) {
        for i in 0..n_DCHCache {
            (*DCHCache[i as usize]).age >>= 1;
        }
        DCHCounter >>= 1;
    }
}

/* Get mask of date/time/zone components present in format nodes. */
unsafe fn DCH_datetime_type(node: *mut FormatNode) -> c_int {
    let mut n: *mut FormatNode;
    let mut flags: c_int = 0;

    n = node;
    while (*n).r#type != NODE_TYPE_END {
        if (*n).r#type != NODE_TYPE_ACTION {
            n = n.add(1);
            continue;
        }

        match (*(*n).key).id {
            x if x == DCH_FX => {}
            x if x == DCH_A_M
                || x == DCH_P_M
                || x == DCH_a_m
                || x == DCH_p_m
                || x == DCH_AM
                || x == DCH_PM
                || x == DCH_am
                || x == DCH_pm
                || x == DCH_HH
                || x == DCH_HH12
                || x == DCH_HH24
                || x == DCH_MI
                || x == DCH_SS
                || x == DCH_MS
                || x == DCH_US
                || x == DCH_FF1
                || x == DCH_FF2
                || x == DCH_FF3
                || x == DCH_FF4
                || x == DCH_FF5
                || x == DCH_FF6
                || x == DCH_SSSS =>
            {
                flags |= DCH_TIMED;
            }
            x if x == DCH_tz || x == DCH_TZ || x == DCH_OF || x == DCH_TZH || x == DCH_TZM => {
                flags |= DCH_ZONED;
            }
            x if x == DCH_A_D
                || x == DCH_B_C
                || x == DCH_a_d
                || x == DCH_b_c
                || x == DCH_AD
                || x == DCH_BC
                || x == DCH_ad
                || x == DCH_bc
                || x == DCH_MONTH
                || x == DCH_Month
                || x == DCH_month
                || x == DCH_MON
                || x == DCH_Mon
                || x == DCH_mon
                || x == DCH_MM
                || x == DCH_DAY
                || x == DCH_Day
                || x == DCH_day
                || x == DCH_DY
                || x == DCH_Dy
                || x == DCH_dy
                || x == DCH_DDD
                || x == DCH_IDDD
                || x == DCH_DD
                || x == DCH_D
                || x == DCH_ID
                || x == DCH_WW
                || x == DCH_Q
                || x == DCH_CC
                || x == DCH_Y_YYY
                || x == DCH_YYYY
                || x == DCH_IYYY
                || x == DCH_YYY
                || x == DCH_IYY
                || x == DCH_YY
                || x == DCH_IY
                || x == DCH_Y
                || x == DCH_I
                || x == DCH_RM
                || x == DCH_rm
                || x == DCH_W
                || x == DCH_J =>
            {
                flags |= DCH_DATED;
            }
            _ => {}
        }
        n = n.add(1);
    }

    flags
}

/* select a DCHCacheEntry to hold the given format picture */
unsafe fn DCH_cache_getnew(str: *const c_char, std: bool) -> *mut DCHCacheEntry {
    let mut ent: *mut DCHCacheEntry;

    /* Ensure we can advance DCHCounter below */
    DCH_prevent_counter_overflow();

    /*
     * If cache is full, remove oldest entry (or recycle first not-valid one)
     */
    if n_DCHCache >= DCH_CACHE_ENTRIES as c_int {
        let mut old = DCHCache[0];

        if (*old).valid {
            for i in 1..DCH_CACHE_ENTRIES {
                ent = DCHCache[i];
                if !(*ent).valid {
                    old = ent;
                    break;
                }
                if (*ent).age < (*old).age {
                    old = ent;
                }
            }
        }
        (*old).valid = false;
        strlcpy((*old).str.as_mut_ptr(), str, DCH_CACHE_SIZE + 1);
        DCHCounter += 1;
        (*old).age = DCHCounter;
        /* caller is expected to fill format, then set valid */
        old
    } else {
        Assert!(DCHCache[n_DCHCache as usize].is_null());
        ent = MemoryContextAllocZero(TopMemoryContext, core::mem::size_of::<DCHCacheEntry>())
            as *mut DCHCacheEntry;
        DCHCache[n_DCHCache as usize] = ent;
        (*ent).valid = false;
        strlcpy((*ent).str.as_mut_ptr(), str, DCH_CACHE_SIZE + 1);
        (*ent).std = std;
        DCHCounter += 1;
        (*ent).age = DCHCounter;
        n_DCHCache += 1;
        ent
    }
}

/* look for an existing DCHCacheEntry matching the given format picture */
unsafe fn DCH_cache_search(str: *const c_char, std: bool) -> *mut DCHCacheEntry {
    /* Ensure we can advance DCHCounter below */
    DCH_prevent_counter_overflow();

    for i in 0..n_DCHCache {
        let ent = DCHCache[i as usize];

        if (*ent).valid && strcmp((*ent).str.as_ptr(), str) == 0 && (*ent).std == std {
            DCHCounter += 1;
            (*ent).age = DCHCounter;
            return ent;
        }
    }

    null_mut()
}

/* Find or create a DCHCacheEntry for the given format picture */
unsafe fn DCH_cache_fetch(str: *const c_char, std: bool) -> *mut DCHCacheEntry {
    let mut ent: *mut DCHCacheEntry;

    ent = DCH_cache_search(str, std);
    if ent.is_null() {
        /*
         * Not in the cache, must run parser and save a new format-picture to
         * the cache.  Do not mark the cache entry valid until parsing succeeds.
         */
        ent = DCH_cache_getnew(str, std);

        parse_format(
            (*ent).format.as_mut_ptr(),
            str,
            &raw const DCH_keywords as *const KeyWord,
            &raw const DCH_suff as *const KeySuffix,
            DCH_index.as_ptr(),
            DCH_FLAG | (if std { STD_FLAG } else { 0 }),
            null_mut(),
        );

        (*ent).valid = true;
    }
    ent
}

/*
 * Format a date/time or interval into a string according to fmt.
 */
unsafe fn datetime_to_char_body(
    tmtc: *mut TmToChar,
    fmt: *mut text,
    is_interval: bool,
    collid: Oid,
) -> *mut text {
    let format: *mut FormatNode;
    let fmt_str: *mut c_char;
    let result: *mut c_char;
    let incache: bool;
    let fmt_len: c_int;
    let res: *mut text;

    /* Convert fmt to C string */
    fmt_str = text_to_cstring(fmt);
    fmt_len = strlen(fmt_str) as c_int;

    /* Allocate workspace for result as C string */
    result = palloc((fmt_len as usize * DCH_MAX_ITEM_SIZ) + 1) as *mut c_char;
    *result = b'\0' as c_char;

    if fmt_len > DCH_CACHE_SIZE as c_int {
        /*
         * Allocate new memory if format picture is bigger than static cache and
         * do not use cache (call parser always)
         */
        incache = false;

        format = palloc((fmt_len as usize + 1) * core::mem::size_of::<FormatNode>())
            as *mut FormatNode;

        parse_format(
            format,
            fmt_str,
            &raw const DCH_keywords as *const KeyWord,
            &raw const DCH_suff as *const KeySuffix,
            DCH_index.as_ptr(),
            DCH_FLAG,
            null_mut(),
        );
    } else {
        /* Use cache buffers */
        let ent = DCH_cache_fetch(fmt_str, false);

        incache = true;
        format = (*ent).format.as_mut_ptr();
    }

    /* The real work is here */
    DCH_to_char(format, is_interval, tmtc, result, collid);

    if !incache {
        pfree(format as *mut c_void);
    }

    pfree(fmt_str as *mut c_void);

    /* convert C-string result to TEXT format */
    res = cstring_to_text(result);

    pfree(result as *mut c_void);
    res
}

// ===========================================================================
//              Public routines
// ===========================================================================

/* TIMESTAMP to_char() */
pub unsafe fn timestamp_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let dt: Timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let res: *mut text;
    let mut tmtc: TmToChar = core::mem::zeroed();
    let mut tt: pg_tm = core::mem::zeroed();
    let tm: *mut fmt_tm;
    let thisdate: c_int;

    if VARSIZE_ANY_EXHDR(fmt as *const c_char) as i64 <= 0 || TIMESTAMP_NOT_FINITE(dt) {
        PG_RETURN_NULL!(fcinfo);
    }

    ZERO_tmtc(&raw mut tmtc);
    tm = tmtcTm(&raw mut tmtc);

    if timestamp2tm(
        dt,
        null_mut(),
        &raw mut tt,
        &raw mut tmtc.fsec,
        null_mut(),
        null_mut(),
    ) != 0
    {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    /* calculate wday and yday, because timestamp2tm doesn't */
    thisdate = date2j(tt.tm_year, tt.tm_mon, tt.tm_mday);
    tt.tm_wday = (thisdate + 1) % 7;
    tt.tm_yday = thisdate - date2j(tt.tm_year, 1, 1) + 1;

    COPY_tm(tm, &raw const tt);

    res = datetime_to_char_body(&raw mut tmtc, fmt, false, PG_GET_COLLATION!(fcinfo));
    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(fcinfo, res);
}

pub unsafe fn timestamptz_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let dt: TimestampTz = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let res: *mut text;
    let mut tmtc: TmToChar = core::mem::zeroed();
    let mut tz: c_int = 0;
    let mut tt: pg_tm = core::mem::zeroed();
    let tm: *mut fmt_tm;
    let thisdate: c_int;

    if VARSIZE_ANY_EXHDR(fmt as *const c_char) as i64 <= 0 || TIMESTAMP_NOT_FINITE(dt) {
        PG_RETURN_NULL!(fcinfo);
    }

    ZERO_tmtc(&raw mut tmtc);
    tm = tmtcTm(&raw mut tmtc);

    if timestamp2tm(
        dt,
        &raw mut tz,
        &raw mut tt,
        &raw mut tmtc.fsec,
        &raw mut tmtc.tzn,
        null_mut(),
    ) != 0
    {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    /* calculate wday and yday, because timestamp2tm doesn't */
    thisdate = date2j(tt.tm_year, tt.tm_mon, tt.tm_mday);
    tt.tm_wday = (thisdate + 1) % 7;
    tt.tm_yday = thisdate - date2j(tt.tm_year, 1, 1) + 1;

    COPY_tm(tm, &raw const tt);

    res = datetime_to_char_body(&raw mut tmtc, fmt, false, PG_GET_COLLATION!(fcinfo));
    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(fcinfo, res);
}

/* INTERVAL to_char() */
pub unsafe fn interval_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let it: *mut Interval = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let res: *mut text;
    let mut tmtc: TmToChar = core::mem::zeroed();
    let tm: *mut fmt_tm;
    let mut tt: pg_itm = core::mem::zeroed();
    let itm: *mut pg_itm = &raw mut tt;

    if VARSIZE_ANY_EXHDR(fmt as *const c_char) as i64 <= 0 || INTERVAL_NOT_FINITE(it) {
        PG_RETURN_NULL!(fcinfo);
    }

    ZERO_tmtc(&raw mut tmtc);
    tm = tmtcTm(&raw mut tmtc);

    interval2itm(core::ptr::read(it), itm);
    tmtc.fsec = (*itm).tm_usec;
    (*tm).tm_sec = (*itm).tm_sec;
    (*tm).tm_min = (*itm).tm_min;
    (*tm).tm_hour = (*itm).tm_hour;
    (*tm).tm_mday = (*itm).tm_mday;
    (*tm).tm_mon = (*itm).tm_mon;
    (*tm).tm_year = (*itm).tm_year;

    /* wday is meaningless, yday approximates the total span in days */
    (*tm).tm_yday =
        ((*tm).tm_year * MONTHS_PER_YEAR + (*tm).tm_mon) * DAYS_PER_MONTH + (*tm).tm_mday;

    res = datetime_to_char_body(&raw mut tmtc, fmt, true, PG_GET_COLLATION!(fcinfo));
    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_TEXT_P!(fcinfo, res);
}

/*
 * TO_TIMESTAMP()
 * Make Timestamp from date_str which is formatted at argument 'fmt'
 * ( to_timestamp is reverse to_char() )
 */
pub unsafe fn to_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let date_txt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let mut result: Timestamp = 0;
    let tz: c_int;
    let mut tm: pg_tm = core::mem::zeroed();
    let mut ftz: fmt_tz = core::mem::zeroed();
    let mut fsec: fsec_t = 0;
    let mut fprec: c_int = 0;

    do_to_timestamp(
        date_txt,
        fmt,
        collid,
        false,
        &raw mut tm,
        &raw mut fsec,
        &raw mut ftz,
        &raw mut fprec,
        null_mut(),
        null_mut(),
    );

    /* Use the specified time zone, if any. */
    if ftz.has_tz {
        tz = ftz.gmtoffset;
    } else {
        tz = DetermineTimeZoneOffset(&raw mut tm, session_timezone);
    }

    let mut tzv = tz;
    if tm2timestamp(&raw mut tm, fsec, &raw mut tzv, &raw mut result) != 0 {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    /* Use the specified fractional precision, if any. */
    if fprec != 0 {
        AdjustTimestampForTypmod(&raw mut result, fprec, null_mut());
    }

    PG_RETURN_TIMESTAMP!(result);
}

/*
 * TO_DATE
 *  Make Date from date_str which is formatted at argument 'fmt'
 */
pub unsafe fn to_date(fcinfo: FunctionCallInfo) -> Datum {
    let date_txt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let collid: Oid = PG_GET_COLLATION!(fcinfo);
    let result: DateADT;
    let mut tm: pg_tm = core::mem::zeroed();
    let mut ftz: fmt_tz = core::mem::zeroed();
    let mut fsec: fsec_t = 0;

    do_to_timestamp(
        date_txt,
        fmt,
        collid,
        false,
        &raw mut tm,
        &raw mut fsec,
        &raw mut ftz,
        null_mut(),
        null_mut(),
        null_mut(),
    );

    /* Prevent overflow in Julian-day routines */
    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        ereport!(
            ERROR,
            errmsg!("date out of range: \"{}\"", cstr(text_to_cstring(date_txt)))
        );
    }

    result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    /* Now check for just-out-of-range dates */
    if !IS_VALID_DATE(result) {
        ereport!(
            ERROR,
            errmsg!("date out of range: \"{}\"", cstr(text_to_cstring(date_txt)))
        );
    }

    PG_RETURN_DATEADT!(result);
}

/*
 * Convert the 'date_txt' input to a datetime type using argument 'fmt' as a
 * format string.
 */
pub unsafe fn parse_datetime(
    date_txt: *mut text,
    fmt: *mut text,
    collid: Oid,
    strict: bool,
    typid: *mut Oid,
    typmod: *mut int32,
    tz: *mut c_int,
    escontext: *mut Node,
) -> Datum {
    let mut tm: pg_tm = core::mem::zeroed();
    let mut ftz: fmt_tz = core::mem::zeroed();
    let mut fsec: fsec_t = 0;
    let mut fprec: c_int = 0;
    let mut flags: uint32 = 0;

    if !do_to_timestamp(
        date_txt,
        fmt,
        collid,
        strict,
        &raw mut tm,
        &raw mut fsec,
        &raw mut ftz,
        &raw mut fprec,
        &raw mut flags,
        escontext,
    ) {
        return 0 as Datum;
    }

    *typmod = if fprec != 0 { fprec } else { -1 }; /* fractional part precision */

    if flags & DCH_DATED as uint32 != 0 {
        if flags & DCH_TIMED as uint32 != 0 {
            if flags & DCH_ZONED as uint32 != 0 {
                let mut result: TimestampTz = 0;

                if ftz.has_tz {
                    *tz = ftz.gmtoffset;
                } else {
                    Assert!(!strict);
                    ereport!(
                        ERROR,
                        errmsg!("missing time zone in input string for type timestamptz")
                    );
                }

                if tm2timestamp(&raw mut tm, fsec, tz, &raw mut result) != 0 {
                    ereport!(ERROR, errmsg!("timestamptz out of range"));
                }

                AdjustTimestampForTypmod(&raw mut result, *typmod, escontext);

                *typid = TIMESTAMPTZOID;
                return TimestampTzGetDatum(result);
            } else {
                let mut result: Timestamp = 0;

                if tm2timestamp(&raw mut tm, fsec, null_mut(), &raw mut result) != 0 {
                    ereport!(ERROR, errmsg!("timestamp out of range"));
                }

                AdjustTimestampForTypmod(&raw mut result, *typmod, escontext);

                *typid = TIMESTAMPOID;
                return TimestampGetDatum(result);
            }
        } else if flags & DCH_ZONED as uint32 != 0 {
            ereport!(ERROR, errmsg!("datetime format is zoned but not timed"));
            unreachable!()
        } else {
            let result: DateADT;

            /* Prevent overflow in Julian-day routines */
            if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
                ereport!(
                    ERROR,
                    errmsg!("date out of range: \"{}\"", cstr(text_to_cstring(date_txt)))
                );
            }

            result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

            /* Now check for just-out-of-range dates */
            if !IS_VALID_DATE(result) {
                ereport!(
                    ERROR,
                    errmsg!("date out of range: \"{}\"", cstr(text_to_cstring(date_txt)))
                );
            }

            *typid = DATEOID;
            return DateADTGetDatum(result);
        }
    } else if flags & DCH_TIMED as uint32 != 0 {
        if flags & DCH_ZONED as uint32 != 0 {
            let result: *mut TimeTzADT = palloc(core::mem::size_of::<TimeTzADT>()) as *mut TimeTzADT;

            if ftz.has_tz {
                *tz = ftz.gmtoffset;
            } else {
                Assert!(!strict);
                ereport!(
                    ERROR,
                    errmsg!("missing time zone in input string for type timetz")
                );
            }

            if tm2timetz(&raw mut tm, fsec, *tz, result) != 0 {
                ereport!(ERROR, errmsg!("timetz out of range"));
            }

            AdjustTimeForTypmod(&raw mut (*result).time, *typmod);

            *typid = TIMETZOID;
            return TimeTzADTPGetDatum(result);
        } else {
            let mut result: TimeADT = 0;

            if tm2time(&raw mut tm, fsec, &raw mut result) != 0 {
                ereport!(ERROR, errmsg!("time out of range"));
            }

            AdjustTimeForTypmod(&raw mut result, *typmod);

            *typid = TIMEOID;
            return TimeADTGetDatum(result);
        }
    } else {
        ereport!(ERROR, errmsg!("datetime format is not dated and not timed"));
        unreachable!()
    }
}

/*
 * Parses the datetime format string in 'fmt_str' and returns true if it
 * contains a timezone specifier, false if not.
 */
pub unsafe fn datetime_format_has_tz(fmt_str: *const c_char) -> bool {
    let incache: bool;
    let fmt_len = strlen(fmt_str) as c_int;
    let result: c_int;
    let format: *mut FormatNode;

    if fmt_len > DCH_CACHE_SIZE as c_int {
        incache = false;

        format = palloc((fmt_len as usize + 1) * core::mem::size_of::<FormatNode>())
            as *mut FormatNode;

        parse_format(
            format,
            fmt_str,
            &raw const DCH_keywords as *const KeyWord,
            &raw const DCH_suff as *const KeySuffix,
            DCH_index.as_ptr(),
            DCH_FLAG,
            null_mut(),
        );
    } else {
        let ent = DCH_cache_fetch(fmt_str, false);

        incache = true;
        format = (*ent).format.as_mut_ptr();
    }

    result = DCH_datetime_type(format);

    if !incache {
        pfree(format as *mut c_void);
    }

    result & DCH_ZONED != 0
}

/*
 * Workhorse of the do_to_timestamp() family.
 *
 * Convert the textual representation 'date_txt' (with the format string 'fmt')
 * into the broken-down time '*tm' (plus '*fsec', '*tz', '*fprec', '*flags').
 *
 * Translated 1:1 from do_to_timestamp() in formatting.c (C4430).
 */
unsafe fn do_to_timestamp(
    date_txt: *mut text,
    fmt: *mut text,
    collid: Oid,
    std: bool,
    tm: *mut pg_tm,
    fsec: *mut fsec_t,
    tz: *mut fmt_tz,
    fprec: *mut c_int,
    flags: *mut uint32,
    escontext: *mut Node,
) -> bool {
    let mut format: *mut FormatNode = null_mut();
    let mut tmfc: TmFromChar = core::mem::zeroed();
    let fmt_len: c_int;
    let date_str: *mut c_char;
    let mut fmask: c_int;
    let mut incache: bool = false;

    Assert!(!tm.is_null());
    Assert!(!fsec.is_null());

    date_str = text_to_cstring(date_txt);

    ZERO_tmfc(&raw mut tmfc);
    ZERO_tm_pg(tm);
    *fsec = 0;
    (*tz).has_tz = false;
    if !fprec.is_null() {
        *fprec = 0;
    }
    if !flags.is_null() {
        *flags = 0;
    }
    fmask = 0; /* bit mask for ValidateDate() */

    fmt_len = VARSIZE_ANY_EXHDR(fmt as *const c_char) as c_int;

    if fmt_len != 0 {
        let fmt_str: *mut c_char;

        fmt_str = text_to_cstring(fmt);

        if fmt_len > DCH_CACHE_SIZE as c_int {
            /*
             * Allocate new memory if format picture is bigger than static
             * cache and do not use cache (call parser always)
             */
            format = palloc((fmt_len as usize + 1) * core::mem::size_of::<FormatNode>())
                as *mut FormatNode;

            parse_format(
                format,
                fmt_str,
                &raw const DCH_keywords as *const KeyWord,
                &raw const DCH_suff as *const KeySuffix,
                DCH_index.as_ptr(),
                DCH_FLAG | (if std { STD_FLAG } else { 0 }),
                null_mut(),
            );
        } else {
            /*
             * Use cache buffers
             */
            let ent = DCH_cache_fetch(fmt_str, std);

            incache = true;
            format = (*ent).format.as_mut_ptr();
        }

        DCH_from_char(format, date_str, &raw mut tmfc, collid, std, escontext);
        pfree(fmt_str as *mut c_void);
        if SOFT_ERROR_OCCURRED(escontext) {
            return do_to_timestamp_fail(format, incache, date_str);
        }

        if !flags.is_null() {
            *flags = DCH_datetime_type(format) as uint32;
        }

        if !incache {
            pfree(format as *mut c_void);
            format = null_mut();
        }
    }

    /*
     * Convert to_date/to_timestamp input fields to standard 'tm'
     */
    if tmfc.ssss != 0 {
        let mut x = tmfc.ssss;

        (*tm).tm_hour = x / SECS_PER_HOUR;
        x %= SECS_PER_HOUR;
        (*tm).tm_min = x / SECS_PER_MINUTE;
        x %= SECS_PER_MINUTE;
        (*tm).tm_sec = x;
    }

    if tmfc.ss != 0 {
        (*tm).tm_sec = tmfc.ss;
    }
    if tmfc.mi != 0 {
        (*tm).tm_min = tmfc.mi;
    }
    if tmfc.hh != 0 {
        (*tm).tm_hour = tmfc.hh;
    }

    if tmfc.clock == CLOCK_12_HOUR {
        if (*tm).tm_hour < 1 || (*tm).tm_hour > HOURS_PER_DAY / 2 {
            let _ = escontext;
            ereport!(
                ERROR,
                errmsg!(
                    "hour \"{}\" is invalid for the 12-hour clock",
                    (*tm).tm_hour
                )
            );
            #[allow(unreachable_code)]
            return do_to_timestamp_fail(format, incache, date_str);
        }

        if tmfc.pm != 0 && (*tm).tm_hour < HOURS_PER_DAY / 2 {
            (*tm).tm_hour += HOURS_PER_DAY / 2;
        } else if tmfc.pm == 0 && (*tm).tm_hour == HOURS_PER_DAY / 2 {
            (*tm).tm_hour = 0;
        }
    }

    if tmfc.year != 0 {
        /*
         * If CC and YY (or Y) are provided, use YY as 2 low-order digits for
         * the year in the given century.  Keep in mind that the 21st century
         * AD runs from 2001-2100, not 2000-2099; 6th century BC runs from
         * 600BC to 501BC.
         */
        if tmfc.cc != 0 && tmfc.yysz <= 2 {
            if tmfc.bc != 0 {
                tmfc.cc = -tmfc.cc;
            }
            (*tm).tm_year = tmfc.year % 100;
            if (*tm).tm_year != 0 {
                let mut tmp: c_int;

                if tmfc.cc >= 0 {
                    /* tm->tm_year += (tmfc.cc - 1) * 100; */
                    tmp = tmfc.cc - 1;
                    if pg_mul_s32_overflow(tmp, 100, &raw mut tmp)
                        || pg_add_s32_overflow((*tm).tm_year, tmp, &raw mut (*tm).tm_year)
                    {
                        DateTimeParseError(
                            DTERR_FIELD_OVERFLOW,
                            null_mut(),
                            text_to_cstring(date_txt),
                            c"timestamp".as_ptr(),
                            escontext,
                        );
                        return do_to_timestamp_fail(format, incache, date_str);
                    }
                } else {
                    /* tm->tm_year = (tmfc.cc + 1) * 100 - tm->tm_year + 1; */
                    tmp = tmfc.cc + 1;
                    if pg_mul_s32_overflow(tmp, 100, &raw mut tmp)
                        || pg_sub_s32_overflow(tmp, (*tm).tm_year, &raw mut tmp)
                        || pg_add_s32_overflow(tmp, 1, &raw mut (*tm).tm_year)
                    {
                        DateTimeParseError(
                            DTERR_FIELD_OVERFLOW,
                            null_mut(),
                            text_to_cstring(date_txt),
                            c"timestamp".as_ptr(),
                            escontext,
                        );
                        return do_to_timestamp_fail(format, incache, date_str);
                    }
                }
            } else {
                /* find century year for dates ending in "00" */
                (*tm).tm_year = tmfc.cc * 100 + (if tmfc.cc >= 0 { 0 } else { 1 });
            }
        } else {
            /* If a 4-digit year is provided, we use that and ignore CC. */
            (*tm).tm_year = tmfc.year;
            if tmfc.bc != 0 {
                (*tm).tm_year = -(*tm).tm_year;
            }
            /* correct for our representation of BC years */
            if (*tm).tm_year < 0 {
                (*tm).tm_year += 1;
            }
        }
        fmask |= DTK_M(YEAR);
    } else if tmfc.cc != 0 {
        /* use first year of century */
        if tmfc.bc != 0 {
            tmfc.cc = -tmfc.cc;
        }
        if tmfc.cc >= 0 {
            /* +1 because 21st century started in 2001 */
            /* tm->tm_year = (tmfc.cc - 1) * 100 + 1; */
            if pg_mul_s32_overflow(tmfc.cc - 1, 100, &raw mut (*tm).tm_year)
                || pg_add_s32_overflow((*tm).tm_year, 1, &raw mut (*tm).tm_year)
            {
                DateTimeParseError(
                    DTERR_FIELD_OVERFLOW,
                    null_mut(),
                    text_to_cstring(date_txt),
                    c"timestamp".as_ptr(),
                    escontext,
                );
                return do_to_timestamp_fail(format, incache, date_str);
            }
        } else {
            /* +1 because year == 599 is 600 BC */
            /* tm->tm_year = tmfc.cc * 100 + 1; */
            if pg_mul_s32_overflow(tmfc.cc, 100, &raw mut (*tm).tm_year)
                || pg_add_s32_overflow((*tm).tm_year, 1, &raw mut (*tm).tm_year)
            {
                DateTimeParseError(
                    DTERR_FIELD_OVERFLOW,
                    null_mut(),
                    text_to_cstring(date_txt),
                    c"timestamp".as_ptr(),
                    escontext,
                );
                return do_to_timestamp_fail(format, incache, date_str);
            }
        }
        fmask |= DTK_M(YEAR);
    }

    if tmfc.j != 0 {
        j2date(
            tmfc.j,
            &raw mut (*tm).tm_year,
            &raw mut (*tm).tm_mon,
            &raw mut (*tm).tm_mday,
        );
        fmask |= DTK_DATE_M();
    }

    if tmfc.ww != 0 {
        if tmfc.mode == FROM_CHAR_DATE_ISOWEEK {
            /*
             * If tmfc.d is not set, then the date is left at the beginning of
             * the ISO week (Monday).
             */
            if tmfc.d != 0 {
                isoweekdate2date(
                    tmfc.ww,
                    tmfc.d,
                    &raw mut (*tm).tm_year,
                    &raw mut (*tm).tm_mon,
                    &raw mut (*tm).tm_mday,
                );
            } else {
                isoweek2date(
                    tmfc.ww,
                    &raw mut (*tm).tm_year,
                    &raw mut (*tm).tm_mon,
                    &raw mut (*tm).tm_mday,
                );
            }
            fmask |= DTK_DATE_M();
        } else {
            /* tmfc.ddd = (tmfc.ww - 1) * 7 + 1; */
            if pg_sub_s32_overflow(tmfc.ww, 1, &raw mut tmfc.ddd)
                || pg_mul_s32_overflow(tmfc.ddd, 7, &raw mut tmfc.ddd)
                || pg_add_s32_overflow(tmfc.ddd, 1, &raw mut tmfc.ddd)
            {
                DateTimeParseError(
                    DTERR_FIELD_OVERFLOW,
                    null_mut(),
                    date_str,
                    c"timestamp".as_ptr(),
                    escontext,
                );
                return do_to_timestamp_fail(format, incache, date_str);
            }
        }
    }

    if tmfc.w != 0 {
        /* tmfc.dd = (tmfc.w - 1) * 7 + 1; */
        if pg_sub_s32_overflow(tmfc.w, 1, &raw mut tmfc.dd)
            || pg_mul_s32_overflow(tmfc.dd, 7, &raw mut tmfc.dd)
            || pg_add_s32_overflow(tmfc.dd, 1, &raw mut tmfc.dd)
        {
            DateTimeParseError(
                DTERR_FIELD_OVERFLOW,
                null_mut(),
                date_str,
                c"timestamp".as_ptr(),
                escontext,
            );
            return do_to_timestamp_fail(format, incache, date_str);
        }
    }
    if tmfc.dd != 0 {
        (*tm).tm_mday = tmfc.dd;
        fmask |= DTK_M(DAY);
    }
    if tmfc.mm != 0 {
        (*tm).tm_mon = tmfc.mm;
        fmask |= DTK_M(MONTH);
    }

    if tmfc.ddd != 0 && ((*tm).tm_mon <= 1 || (*tm).tm_mday <= 1) {
        /*
         * The month and day field have not been set, so we use the
         * day-of-year field to populate them.  Depending on the date mode,
         * this field may be interpreted as a Gregorian day-of-year, or an ISO
         * week date day-of-year.
         */

        if (*tm).tm_year == 0 && tmfc.bc == 0 {
            ereport!(
                ERROR,
                errmsg!("cannot calculate day of year without year information")
            );
            #[allow(unreachable_code)]
            return do_to_timestamp_fail(format, incache, date_str);
        }

        if tmfc.mode == FROM_CHAR_DATE_ISOWEEK {
            let j0: c_int; /* zeroth day of the ISO year, in Julian */

            j0 = isoweek2j((*tm).tm_year, 1) - 1;

            j2date(
                j0 + tmfc.ddd,
                &raw mut (*tm).tm_year,
                &raw mut (*tm).tm_mon,
                &raw mut (*tm).tm_mday,
            );
            fmask |= DTK_DATE_M();
        } else {
            let y: *const c_int;
            let mut i: c_int;

            static YSUM: [[c_int; 13]; 2] = [
                [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
                [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
            ];

            y = YSUM[isleap((*tm).tm_year) as usize].as_ptr();

            i = 1;
            while i <= MONTHS_PER_YEAR {
                if tmfc.ddd <= *y.add(i as usize) {
                    break;
                }
                i += 1;
            }
            if (*tm).tm_mon <= 1 {
                (*tm).tm_mon = i;
            }

            if (*tm).tm_mday <= 1 {
                (*tm).tm_mday = tmfc.ddd - *y.add((i - 1) as usize);
            }

            fmask |= DTK_M(MONTH) | DTK_M(DAY);
        }
    }

    if tmfc.ms != 0 {
        let mut tmp: c_int = 0;

        /* *fsec += tmfc.ms * 1000; */
        if pg_mul_s32_overflow(tmfc.ms, 1000, &raw mut tmp)
            || pg_add_s32_overflow(*fsec, tmp, fsec)
        {
            DateTimeParseError(
                DTERR_FIELD_OVERFLOW,
                null_mut(),
                date_str,
                c"timestamp".as_ptr(),
                escontext,
            );
            return do_to_timestamp_fail(format, incache, date_str);
        }
    }
    if tmfc.us != 0 {
        *fsec += tmfc.us;
    }
    if !fprec.is_null() {
        *fprec = tmfc.ff; /* fractional precision, if specified */
    }

    /* Range-check date fields according to bit mask computed above */
    if fmask != 0 {
        /* We already dealt with AD/BC, so pass isjulian = true */
        let dterr = ValidateDate(fmask, true, false, false, tm);

        if dterr != 0 {
            /*
             * Force the error to be DTERR_FIELD_OVERFLOW even if ValidateDate
             * said DTERR_MD_FIELD_OVERFLOW, because we don't want to print an
             * irrelevant hint about datestyle.
             */
            DateTimeParseError(
                DTERR_FIELD_OVERFLOW,
                null_mut(),
                date_str,
                c"timestamp".as_ptr(),
                escontext,
            );
            return do_to_timestamp_fail(format, incache, date_str);
        }
    }

    /* Range-check time fields too */
    if (*tm).tm_hour < 0
        || (*tm).tm_hour >= HOURS_PER_DAY
        || (*tm).tm_min < 0
        || (*tm).tm_min >= MINS_PER_HOUR
        || (*tm).tm_sec < 0
        || (*tm).tm_sec >= SECS_PER_MINUTE
        || *fsec < 0
        || *fsec >= USECS_PER_SEC as fsec_t
    {
        DateTimeParseError(
            DTERR_FIELD_OVERFLOW,
            null_mut(),
            date_str,
            c"timestamp".as_ptr(),
            escontext,
        );
        return do_to_timestamp_fail(format, incache, date_str);
    }

    /*
     * If timezone info was present, reduce it to a GMT offset.  (We cannot do
     * this until we've filled all of the tm struct, since the zone's offset
     * might be time-varying.)
     */
    if tmfc.tzsign != 0 {
        /* TZH and/or TZM fields */
        if tmfc.tzh < 0
            || tmfc.tzh > MAX_TZDISP_HOUR
            || tmfc.tzm < 0
            || tmfc.tzm >= MINS_PER_HOUR
        {
            DateTimeParseError(
                DTERR_TZDISP_OVERFLOW,
                null_mut(),
                date_str,
                c"timestamp".as_ptr(),
                escontext,
            );
            return do_to_timestamp_fail(format, incache, date_str);
        }

        (*tz).has_tz = true;
        (*tz).gmtoffset = (tmfc.tzh * MINS_PER_HOUR + tmfc.tzm) * SECS_PER_MINUTE;
        /* note we are flipping the sign convention here */
        if tmfc.tzsign > 0 {
            (*tz).gmtoffset = -(*tz).gmtoffset;
        }
    } else if tmfc.has_tz {
        /* TZ field */
        (*tz).has_tz = true;
        if tmfc.tzp.is_null() {
            /* fixed-offset abbreviation; flip the sign convention */
            (*tz).gmtoffset = -tmfc.gmtoffset;
        } else {
            /* dynamic-offset abbreviation, resolve using specified time */
            (*tz).gmtoffset = DetermineTimeZoneAbbrevOffset(tm, tmfc.abbrev, tmfc.tzp);
        }
    }

    if !format.is_null() && !incache {
        pfree(format as *mut c_void);
    }
    pfree(date_str as *mut c_void);

    true
}

/*
 * Common cleanup for do_to_timestamp()'s "fail" label, factored out so that
 * the various `goto fail;` sites can become early returns.
 */
unsafe fn do_to_timestamp_fail(
    format: *mut FormatNode,
    incache: bool,
    date_str: *mut c_char,
) -> bool {
    if !format.is_null() && !incache {
        pfree(format as *mut c_void);
    }
    pfree(date_str as *mut c_void);

    false
}

/* **********************************************************************
 *	the NUMBER version part
 * ********************************************************************* */

unsafe fn fill_str(str: *mut c_char, c: c_int, max: c_int) -> *mut c_char {
    memset(str as *mut c_void, c, max as usize);
    *str.add(max as usize) = b'\0' as c_char;
    str
}

/*
 * zeroize_NUM(_n): reset a NUMDesc.  C uses a macro; we use a fn.
 */
unsafe fn zeroize_NUM(n: *mut NUMDesc) {
    (*n).flag = 0;
    (*n).lsign = 0;
    (*n).pre = 0;
    (*n).post = 0;
    (*n).pre_lsign_num = 0;
    (*n).need_locale = 0;
    (*n).multi = 0;
    (*n).zero_start = 0;
    (*n).zero_end = 0;
}

/* This works the same as DCH_prevent_counter_overflow */
#[inline]
unsafe fn NUM_prevent_counter_overflow() {
    if NUMCounter >= (i32::MAX - 1) {
        for i in 0..n_NUMCache {
            (*NUMCache[i as usize]).age >>= 1;
        }
        NUMCounter >>= 1;
    }
}

/* select a NUMCacheEntry to hold the given format picture */
unsafe fn NUM_cache_getnew(str: *const c_char) -> *mut NUMCacheEntry {
    let ent: *mut NUMCacheEntry;

    /* Ensure we can advance NUMCounter below */
    NUM_prevent_counter_overflow();

    /*
     * If cache is full, remove oldest entry (or recycle first not-valid one)
     */
    if n_NUMCache >= NUM_CACHE_ENTRIES as c_int {
        let mut old = NUMCache[0];

        if (*old).valid {
            for i in 1..NUM_CACHE_ENTRIES as c_int {
                let e = NUMCache[i as usize];
                if !(*e).valid {
                    old = e;
                    break;
                }
                if (*e).age < (*old).age {
                    old = e;
                }
            }
        }
        (*old).valid = false;
        strlcpy(
            (*old).str.as_mut_ptr(),
            str,
            NUM_CACHE_SIZE + 1,
        );
        NUMCounter += 1;
        (*old).age = NUMCounter;
        /* caller is expected to fill format and Num, then set valid */
        old
    } else {
        Assert!(NUMCache[n_NUMCache as usize].is_null());
        let e = MemoryContextAllocZero(TopMemoryContext, core::mem::size_of::<NUMCacheEntry>())
            as *mut NUMCacheEntry;
        NUMCache[n_NUMCache as usize] = e;
        ent = e;
        (*ent).valid = false;
        strlcpy(
            (*ent).str.as_mut_ptr(),
            str,
            NUM_CACHE_SIZE + 1,
        );
        NUMCounter += 1;
        (*ent).age = NUMCounter;
        /* caller is expected to fill format and Num, then set valid */
        n_NUMCache += 1;
        ent
    }
}

/* look for an existing NUMCacheEntry matching the given format picture */
unsafe fn NUM_cache_search(str: *const c_char) -> *mut NUMCacheEntry {
    /* Ensure we can advance NUMCounter below */
    NUM_prevent_counter_overflow();

    for i in 0..n_NUMCache {
        let ent = NUMCache[i as usize];

        if (*ent).valid && strcmp((*ent).str.as_ptr(), str) == 0 {
            NUMCounter += 1;
            (*ent).age = NUMCounter;
            return ent;
        }
    }

    null_mut()
}

/* Find or create a NUMCacheEntry for the given format picture */
unsafe fn NUM_cache_fetch(str: *const c_char) -> *mut NUMCacheEntry {
    let mut ent: *mut NUMCacheEntry;

    ent = NUM_cache_search(str);
    if ent.is_null() {
        /*
         * Not in the cache, must run parser and save a new format-picture to
         * the cache.  Do not mark the cache entry valid until parsing
         * succeeds.
         */
        ent = NUM_cache_getnew(str);

        zeroize_NUM(&raw mut (*ent).Num);

        parse_format(
            (*ent).format.as_mut_ptr(),
            str,
            &raw const NUM_keywords as *const KeyWord,
            null(),
            NUM_index.as_ptr(),
            NUM_FLAG,
            &raw mut (*ent).Num,
        );

        (*ent).valid = true;
    }
    ent
}

/* ----------
 * Cache routine for NUM to_char version
 * ----------
 */
unsafe fn NUM_cache(
    len: c_int,
    Num: *mut NUMDesc,
    pars_str: *mut text,
    shouldFree: *mut bool,
) -> *mut FormatNode {
    let mut format: *mut FormatNode = null_mut();
    let str: *mut c_char;

    str = text_to_cstring(pars_str);

    if len > NUM_CACHE_SIZE as c_int {
        /*
         * Allocate new memory if format picture is bigger than static cache
         * and do not use cache (call parser always)
         */
        format = palloc((len as usize + 1) * core::mem::size_of::<FormatNode>())
            as *mut FormatNode;

        *shouldFree = true;

        zeroize_NUM(Num);

        parse_format(
            format,
            str,
            &raw const NUM_keywords as *const KeyWord,
            null(),
            NUM_index.as_ptr(),
            NUM_FLAG,
            Num,
        );
    } else {
        /*
         * Use cache buffers
         */
        let ent = NUM_cache_fetch(str);

        *shouldFree = false;

        format = (*ent).format.as_mut_ptr();

        /*
         * Copy cache to used struct
         */
        (*Num).flag = (*ent).Num.flag;
        (*Num).lsign = (*ent).Num.lsign;
        (*Num).pre = (*ent).Num.pre;
        (*Num).post = (*ent).Num.post;
        (*Num).pre_lsign_num = (*ent).Num.pre_lsign_num;
        (*Num).need_locale = (*ent).Num.need_locale;
        (*Num).multi = (*ent).Num.multi;
        (*Num).zero_start = (*ent).Num.zero_start;
        (*Num).zero_end = (*ent).Num.zero_end;
    }

    pfree(str as *mut c_void);
    format
}

/*
 * Convert integer to Roman numerals
 * Result is upper-case and not blank-padded (NUM_processor converts as needed)
 * If input is out-of-range, produce '###############'
 */
unsafe fn int_to_roman(number: c_int) -> *mut c_char {
    let mut len: c_int;
    let mut num: c_int;
    let result: *mut c_char;

    result = palloc(MAX_ROMAN_LEN + 1) as *mut c_char;
    *result = b'\0' as c_char;

    /*
     * This range limit is the same as in Oracle(TM).  The difficulty with
     * handling 4000 or more is that we'd need to use more than 3 "M"'s, and
     * more than 3 of the same digit isn't considered a valid Roman string.
     */
    if number > 3999 || number < 1 {
        fill_str(result, b'#' as c_int, MAX_ROMAN_LEN as c_int);
        return result;
    }

    /* Convert to decimal, then examine each digit */
    let numstr_s = format!("{}", number);
    let numstr = numstr_s.as_bytes();
    len = numstr.len() as c_int;
    Assert!(len > 0 && len <= 4);

    let mut pi: usize = 0;
    while pi < numstr.len() {
        let p = numstr[pi];
        num = p as c_int - (b'0' as c_int + 1);
        if num < 0 {
            pi += 1;
            len -= 1;
            continue; /* ignore zeroes */
        }
        /* switch on current column position */
        match len {
            4 => {
                while {
                    let cur = num;
                    num -= 1;
                    cur >= 0
                } {
                    strcat(result, c"M".as_ptr());
                }
            }
            3 => {
                strcat(result, rm100[num as usize]);
            }
            2 => {
                strcat(result, rm10[num as usize]);
            }
            1 => {
                strcat(result, rm1[num as usize]);
            }
            _ => {}
        }
        pi += 1;
        len -= 1;
    }
    result
}

/*
 * Convert a roman numeral (standard form) to an integer.
 * Result is an integer between 1 and 3999.
 * Np->inout_p is advanced past the characters consumed.
 *
 * If input is invalid, return -1.
 */
unsafe fn roman_to_int(Np: *mut NUMProc, input_len: c_int) -> c_int {
    let mut result: c_int = 0;
    let mut len: usize;
    let mut romanChars: [c_char; MAX_ROMAN_LEN] = [0; MAX_ROMAN_LEN];
    let mut romanValues: [c_int; MAX_ROMAN_LEN] = [0; MAX_ROMAN_LEN];
    let mut repeatCount: c_int = 1;
    let mut vCount: c_int = 0;
    let mut lCount: c_int = 0;
    let mut dCount: c_int = 0;
    let mut subtractionEncountered: bool = false;
    let mut lastSubtractedValue: c_int = 0;

    /*
     * Skip any leading whitespace.  Perhaps we should limit the amount of
     * space skipped to MAX_ROMAN_LEN, but that seems unnecessarily picky.
     */
    while !OVERLOAD_TEST!(Np, input_len) && isspace(*(*Np).inout_p as c_uchar as c_int) != 0 {
        (*Np).inout_p = (*Np).inout_p.add(1);
    }

    /*
     * Collect and decode valid roman numerals, consuming at most
     * MAX_ROMAN_LEN characters.  We do this in a separate loop to avoid
     * repeated decoding and because the main loop needs to know when it's at
     * the last numeral.
     */
    len = 0;
    while len < MAX_ROMAN_LEN && !OVERLOAD_TEST!(Np, input_len) {
        let currChar = pg_ascii_toupper(*(*Np).inout_p as c_uchar) as c_char;
        let currValue = ROMAN_VAL(currChar);

        if currValue == 0 {
            break; /* Not a valid roman numeral. */
        }
        romanChars[len] = currChar;
        romanValues[len] = currValue;
        (*Np).inout_p = (*Np).inout_p.add(1);
        len += 1;
    }

    if len == 0 {
        return -1; /* No valid roman numerals. */
    }

    /* Check for valid combinations and compute the represented value. */
    let mut i: usize = 0;
    while i < len {
        let currChar = romanChars[i];
        let currValue = romanValues[i];

        /*
         * Ensure no numeral greater than or equal to the subtracted numeral
         * appears after a subtraction.
         */
        if subtractionEncountered && currValue >= lastSubtractedValue {
            return -1;
        }

        /*
         * V, L, and D should not appear before a larger numeral, nor should
         * they be repeated.
         */
        if (vCount != 0 && currValue >= ROMAN_VAL(b'V' as c_char))
            || (lCount != 0 && currValue >= ROMAN_VAL(b'L' as c_char))
            || (dCount != 0 && currValue >= ROMAN_VAL(b'D' as c_char))
        {
            return -1;
        }
        if currChar == b'V' as c_char {
            vCount += 1;
        } else if currChar == b'L' as c_char {
            lCount += 1;
        } else if currChar == b'D' as c_char {
            dCount += 1;
        }

        if i < len - 1 {
            /* Compare current numeral to next numeral. */
            let nextChar = romanChars[i + 1];
            let nextValue = romanValues[i + 1];

            /*
             * If the current value is less than the next value, handle
             * subtraction. Verify valid subtractive combinations and update
             * the result accordingly.
             */
            if currValue < nextValue {
                if !IS_VALID_SUB_COMB(currChar, nextChar) {
                    return -1;
                }

                /*
                 * Reject cases where same numeral is repeated with
                 * subtraction (e.g. 'MCCM' or 'DCCCD').
                 */
                if repeatCount > 1 {
                    return -1;
                }

                /*
                 * We are going to skip nextChar, so first make checks needed
                 * for V, L, and D.  These are the same as we'd have applied
                 * if we reached nextChar without a subtraction.
                 */
                if (vCount != 0 && nextValue >= ROMAN_VAL(b'V' as c_char))
                    || (lCount != 0 && nextValue >= ROMAN_VAL(b'L' as c_char))
                    || (dCount != 0 && nextValue >= ROMAN_VAL(b'D' as c_char))
                {
                    return -1;
                }
                if nextChar == b'V' as c_char {
                    vCount += 1;
                } else if nextChar == b'L' as c_char {
                    lCount += 1;
                } else if nextChar == b'D' as c_char {
                    dCount += 1;
                }

                /*
                 * Skip the next numeral as it is part of the subtractive
                 * combination.
                 */
                i += 1;

                /* Update state. */
                repeatCount = 1;
                subtractionEncountered = true;
                lastSubtractedValue = currValue;
                result += nextValue - currValue;
            } else {
                /* For same numerals, check for repetition. */
                if currChar == nextChar {
                    repeatCount += 1;
                    if repeatCount > 3 {
                        return -1;
                    }
                } else {
                    repeatCount = 1;
                }
                result += currValue;
            }
        } else {
            /* This is the last numeral; just add it to the result. */
            result += currValue;
        }
        i += 1;
    }

    result
}

/* ----------
 * Locale
 * ----------
 */
unsafe fn NUM_prepare_locale(Np: *mut NUMProc) {
    if (*(*Np).Num).need_locale != 0 {
        let lconv: *mut lconv;

        /*
         * Get locales
         */
        lconv = PGLC_localeconv();

        /*
         * Positive / Negative number sign
         */
        if !(*lconv).negative_sign.is_null() && *(*lconv).negative_sign != 0 {
            (*Np).L_negative_sign = (*lconv).negative_sign;
        } else {
            (*Np).L_negative_sign = c"-".as_ptr();
        }

        if !(*lconv).positive_sign.is_null() && *(*lconv).positive_sign != 0 {
            (*Np).L_positive_sign = (*lconv).positive_sign;
        } else {
            (*Np).L_positive_sign = c"+".as_ptr();
        }

        /*
         * Number decimal point
         */
        if !(*lconv).decimal_point.is_null() && *(*lconv).decimal_point != 0 {
            (*Np).decimal = (*lconv).decimal_point;
        } else {
            (*Np).decimal = c".".as_ptr();
        }

        if !IS_LDECIMAL((*Np).Num) {
            (*Np).decimal = c".".as_ptr();
        }

        /*
         * Number thousands separator
         *
         * Some locales (e.g. broken glibc pt_BR), have a comma for decimal,
         * but "" for thousands_sep, so we set the thousands_sep too.
         * http://archives.postgresql.org/pgsql-hackers/2007-11/msg00772.php
         */
        if !(*lconv).thousands_sep.is_null() && *(*lconv).thousands_sep != 0 {
            (*Np).L_thousands_sep = (*lconv).thousands_sep;
        }
        /* Make sure thousands separator doesn't match decimal point symbol. */
        else if strcmp((*Np).decimal, c",".as_ptr()) != 0 {
            (*Np).L_thousands_sep = c",".as_ptr();
        } else {
            (*Np).L_thousands_sep = c".".as_ptr();
        }

        /*
         * Currency symbol
         */
        if !(*lconv).currency_symbol.is_null() && *(*lconv).currency_symbol != 0 {
            (*Np).L_currency_symbol = (*lconv).currency_symbol;
        } else {
            (*Np).L_currency_symbol = c" ".as_ptr();
        }
    } else {
        /*
         * Default values
         */
        (*Np).L_negative_sign = c"-".as_ptr();
        (*Np).L_positive_sign = c"+".as_ptr();
        (*Np).decimal = c".".as_ptr();

        (*Np).L_thousands_sep = c",".as_ptr();
        (*Np).L_currency_symbol = c" ".as_ptr();
    }
}

/* ----------
 * Return pointer of last relevant number after decimal point
 *	12.0500 --> last relevant is '5'
 *	12.0000 --> last relevant is '.'
 * If there is no decimal point, return NULL (which will result in same
 * behavior as if FM hadn't been specified).
 * ----------
 */
unsafe fn get_last_relevant_decnum(num: *mut c_char) -> *mut c_char {
    let mut result: *mut c_char;
    let mut p: *mut c_char = strchr(num, b'.' as c_int);

    if p.is_null() {
        return null_mut();
    }

    result = p;

    loop {
        p = p.add(1);
        if *p == 0 {
            break;
        }
        if *p != b'0' as c_char {
            result = p;
        }
    }

    result
}

/* ----------
 * Number extraction for TO_NUMBER()
 * ----------
 */
unsafe fn NUM_numpart_from_char(Np: *mut NUMProc, id: c_int, input_len: c_int) {
    let mut isread: bool = false;

    if OVERLOAD_TEST!(Np, input_len) {
        return;
    }

    if *(*Np).inout_p == b' ' as c_char {
        (*Np).inout_p = (*Np).inout_p.add(1);
    }

    if OVERLOAD_TEST!(Np, input_len) {
        return;
    }

    /*
     * read sign before number
     */
    if *(*Np).number == b' ' as c_char
        && (id == NUM_0 || id == NUM_9)
        && ((*Np).read_pre + (*Np).read_post) == 0
    {
        /*
         * locale sign
         */
        if IS_LSIGN((*Np).Num) && (*(*Np).Num).lsign == NUM_LSIGN_PRE {
            let mut x: c_int;

            x = strlen((*Np).L_negative_sign) as c_int;
            if x != 0
                && AMOUNT_TEST!(Np, input_len, x)
                && strncmp((*Np).inout_p, (*Np).L_negative_sign, x as usize) == 0
            {
                (*Np).inout_p = (*Np).inout_p.add(x as usize);
                *(*Np).number = b'-' as c_char;
            } else {
                x = strlen((*Np).L_positive_sign) as c_int;
                if x != 0
                    && AMOUNT_TEST!(Np, input_len, x)
                    && strncmp((*Np).inout_p, (*Np).L_positive_sign, x as usize) == 0
                {
                    (*Np).inout_p = (*Np).inout_p.add(x as usize);
                    *(*Np).number = b'+' as c_char;
                }
            }
        } else {
            /*
             * simple + - < >
             */
            if *(*Np).inout_p == b'-' as c_char
                || (IS_BRACKET((*Np).Num) && *(*Np).inout_p == b'<' as c_char)
            {
                *(*Np).number = b'-' as c_char; /* set - */
                (*Np).inout_p = (*Np).inout_p.add(1);
            } else if *(*Np).inout_p == b'+' as c_char {
                *(*Np).number = b'+' as c_char; /* set + */
                (*Np).inout_p = (*Np).inout_p.add(1);
            }
        }
    }

    if OVERLOAD_TEST!(Np, input_len) {
        return;
    }

    /*
     * read digit or decimal point
     */
    if isdigit(*(*Np).inout_p as c_uchar as c_int) != 0 {
        if (*Np).read_dec != 0 && (*Np).read_post == (*(*Np).Num).post {
            return;
        }

        *(*Np).number_p = *(*Np).inout_p;
        (*Np).number_p = (*Np).number_p.add(1);

        if (*Np).read_dec != 0 {
            (*Np).read_post += 1;
        } else {
            (*Np).read_pre += 1;
        }

        isread = true;
    } else if IS_DECIMAL((*Np).Num) && (*Np).read_dec == 0 {
        /*
         * We need not test IS_LDECIMAL(Np->Num) explicitly here, because
         * Np->decimal is always just "." if we don't have a D format token.
         * So we just unconditionally match to Np->decimal.
         */
        let x = strlen((*Np).decimal) as c_int;

        if x != 0
            && AMOUNT_TEST!(Np, input_len, x)
            && strncmp((*Np).inout_p, (*Np).decimal, x as usize) == 0
        {
            (*Np).inout_p = (*Np).inout_p.add((x - 1) as usize);
            *(*Np).number_p = b'.' as c_char;
            (*Np).number_p = (*Np).number_p.add(1);
            (*Np).read_dec = 1;
            isread = true;
        }
    }

    if OVERLOAD_TEST!(Np, input_len) {
        return;
    }

    /*
     * Read sign behind "last" number
     *
     * We need sign detection because determine exact position of post-sign is
     * difficult:
     *
     * FM9999.9999999S	   -> 123.001- 9.9S			   -> .5- FM9.999999MI ->
     * 5.01-
     */
    if *(*Np).number == b' ' as c_char && (*Np).read_pre + (*Np).read_post > 0 {
        /*
         * locale sign (NUM_S) is always anchored behind a last number, if: -
         * locale sign expected - last read char was NUM_0/9 or NUM_DEC - and
         * next char is not digit
         */
        if IS_LSIGN((*Np).Num)
            && isread
            && (*Np).inout_p.add(1) < (*Np).inout.add(input_len as usize)
            && isdigit(*(*Np).inout_p.add(1) as c_uchar as c_int) == 0
        {
            let mut x: c_int;
            let tmp = (*Np).inout_p;
            (*Np).inout_p = (*Np).inout_p.add(1);

            x = strlen((*Np).L_negative_sign) as c_int;
            if x != 0
                && AMOUNT_TEST!(Np, input_len, x)
                && strncmp((*Np).inout_p, (*Np).L_negative_sign, x as usize) == 0
            {
                (*Np).inout_p = (*Np).inout_p.add((x - 1) as usize); /* -1 .. NUM_processor() do inout_p++ */
                *(*Np).number = b'-' as c_char;
            } else {
                x = strlen((*Np).L_positive_sign) as c_int;
                if x != 0
                    && AMOUNT_TEST!(Np, input_len, x)
                    && strncmp((*Np).inout_p, (*Np).L_positive_sign, x as usize) == 0
                {
                    (*Np).inout_p = (*Np).inout_p.add((x - 1) as usize); /* -1 .. NUM_processor() do inout_p++ */
                    *(*Np).number = b'+' as c_char;
                }
            }
            if *(*Np).number == b' ' as c_char {
                /* no sign read */
                (*Np).inout_p = tmp;
            }
        }
        /*
         * try read non-locale sign, which happens only if format is not exact
         * and we cannot determine sign position of MI/PL/SG, an example:
         *
         * FM9.999999MI			   -> 5.01-
         *
         * if (.... && IS_LSIGN(Np->Num)==false) prevents read wrong formats
         * like to_number('1 -', '9S') where sign is not anchored to last
         * number.
         */
        else if !isread && !IS_LSIGN((*Np).Num) && (IS_PLUS((*Np).Num) || IS_MINUS((*Np).Num)) {
            /*
             * simple + -
             */
            if *(*Np).inout_p == b'-' as c_char || *(*Np).inout_p == b'+' as c_char {
                /* NUM_processor() do inout_p++ */
                *(*Np).number = *(*Np).inout_p;
            }
        }
    }
}

/*
 * IS_PREDEC_SPACE(_n): handle "9.9" --> " .1" output case.
 */
unsafe fn IS_PREDEC_SPACE(n: *mut NUMProc) -> bool {
    !IS_ZERO((*n).Num)
        && (*n).number == (*n).number_p
        && *(*n).number == b'0' as c_char
        && (*(*n).Num).post != 0
}

/* ----------
 * Add digit or sign to number-string
 * ----------
 */
unsafe fn NUM_numpart_to_char(Np: *mut NUMProc, id: c_int) {
    let mut end: c_int;

    if IS_ROMAN((*Np).Num) {
        return;
    }

    /* Note: in this elog() output not set '\0' in 'inout' */

    (*Np).num_in = false as c_int;

    /*
     * Write sign if real number will write to output Note: IS_PREDEC_SPACE()
     * handle "9.9" --> " .1"
     */
    if (*Np).sign_wrote == false as c_int
        && ((*Np).num_curr >= (*Np).out_pre_spaces
            || (IS_ZERO((*Np).Num) && (*(*Np).Num).zero_start == (*Np).num_curr))
        && (!IS_PREDEC_SPACE(Np)
            || (!(*Np).last_relevant.is_null() && *(*Np).last_relevant == b'.' as c_char))
    {
        if IS_LSIGN((*Np).Num) {
            if (*(*Np).Num).lsign == NUM_LSIGN_PRE {
                if (*Np).sign == b'-' as c_int {
                    strcpy((*Np).inout_p, (*Np).L_negative_sign);
                } else {
                    strcpy((*Np).inout_p, (*Np).L_positive_sign);
                }
                (*Np).inout_p = (*Np).inout_p.add(strlen((*Np).inout_p));
                (*Np).sign_wrote = true as c_int;
            }
        } else if IS_BRACKET((*Np).Num) {
            *(*Np).inout_p = if (*Np).sign == b'+' as c_int {
                b' ' as c_char
            } else {
                b'<' as c_char
            };
            (*Np).inout_p = (*Np).inout_p.add(1);
            (*Np).sign_wrote = true as c_int;
        } else if (*Np).sign == b'+' as c_int {
            if !IS_FILLMODE((*Np).Num) {
                *(*Np).inout_p = b' ' as c_char; /* Write + */
                (*Np).inout_p = (*Np).inout_p.add(1);
            }
            (*Np).sign_wrote = true as c_int;
        } else if (*Np).sign == b'-' as c_int {
            /* Write - */
            *(*Np).inout_p = b'-' as c_char;
            (*Np).inout_p = (*Np).inout_p.add(1);
            (*Np).sign_wrote = true as c_int;
        }
    }

    /*
     * digits / FM / Zero / Dec. point
     */
    if id == NUM_9 || id == NUM_0 || id == NUM_D || id == NUM_DEC {
        if (*Np).num_curr < (*Np).out_pre_spaces
            && ((*(*Np).Num).zero_start > (*Np).num_curr || !IS_ZERO((*Np).Num))
        {
            /*
             * Write blank space
             */
            if !IS_FILLMODE((*Np).Num) {
                *(*Np).inout_p = b' ' as c_char; /* Write ' ' */
                (*Np).inout_p = (*Np).inout_p.add(1);
            }
        } else if IS_ZERO((*Np).Num)
            && (*Np).num_curr < (*Np).out_pre_spaces
            && (*(*Np).Num).zero_start <= (*Np).num_curr
        {
            /*
             * Write ZERO
             */
            *(*Np).inout_p = b'0' as c_char; /* Write '0' */
            (*Np).inout_p = (*Np).inout_p.add(1);
            (*Np).num_in = true as c_int;
        } else {
            /*
             * Write Decimal point
             */
            if *(*Np).number_p == b'.' as c_char {
                if (*Np).last_relevant.is_null() || *(*Np).last_relevant != b'.' as c_char {
                    strcpy((*Np).inout_p, (*Np).decimal); /* Write DEC/D */
                    (*Np).inout_p = (*Np).inout_p.add(strlen((*Np).inout_p));
                }
                /*
                 * Ora 'n' -- FM9.9 --> 'n.'
                 */
                else if IS_FILLMODE((*Np).Num)
                    && !(*Np).last_relevant.is_null()
                    && *(*Np).last_relevant == b'.' as c_char
                {
                    strcpy((*Np).inout_p, (*Np).decimal); /* Write DEC/D */
                    (*Np).inout_p = (*Np).inout_p.add(strlen((*Np).inout_p));
                }
            } else {
                /*
                 * Write Digits
                 */
                if !(*Np).last_relevant.is_null()
                    && (*Np).number_p > (*Np).last_relevant as *mut c_char
                    && id != NUM_0
                {
                    /* do nothing */
                }
                /*
                 * '0.1' -- 9.9 --> '  .1'
                 */
                else if IS_PREDEC_SPACE(Np) {
                    if !IS_FILLMODE((*Np).Num) {
                        *(*Np).inout_p = b' ' as c_char;
                        (*Np).inout_p = (*Np).inout_p.add(1);
                    }
                    /*
                     * '0' -- FM9.9 --> '0.'
                     */
                    else if !(*Np).last_relevant.is_null()
                        && *(*Np).last_relevant == b'.' as c_char
                    {
                        *(*Np).inout_p = b'0' as c_char;
                        (*Np).inout_p = (*Np).inout_p.add(1);
                    }
                } else {
                    *(*Np).inout_p = *(*Np).number_p; /* Write DIGIT */
                    (*Np).inout_p = (*Np).inout_p.add(1);
                    (*Np).num_in = true as c_int;
                }
            }
            /* do no exceed string length */
            if *(*Np).number_p != 0 {
                (*Np).number_p = (*Np).number_p.add(1);
            }
        }

        end = (*Np).num_count
            + (if (*Np).out_pre_spaces != 0 { 1 } else { 0 })
            + (if IS_DECIMAL((*Np).Num) { 1 } else { 0 });

        if !(*Np).last_relevant.is_null()
            && (*Np).last_relevant == (*Np).number_p as *const c_char
        {
            end = (*Np).num_curr;
        }

        if (*Np).num_curr + 1 == end {
            if (*Np).sign_wrote == true as c_int && IS_BRACKET((*Np).Num) {
                *(*Np).inout_p = if (*Np).sign == b'+' as c_int {
                    b' ' as c_char
                } else {
                    b'>' as c_char
                };
                (*Np).inout_p = (*Np).inout_p.add(1);
            } else if IS_LSIGN((*Np).Num) && (*(*Np).Num).lsign == NUM_LSIGN_POST {
                if (*Np).sign == b'-' as c_int {
                    strcpy((*Np).inout_p, (*Np).L_negative_sign);
                } else {
                    strcpy((*Np).inout_p, (*Np).L_positive_sign);
                }
                (*Np).inout_p = (*Np).inout_p.add(strlen((*Np).inout_p));
            }
        }
    }

    (*Np).num_curr += 1;
}

/*
 * Skip over "n" input characters, but only if they aren't numeric data
 */
unsafe fn NUM_eat_non_data_chars(Np: *mut NUMProc, mut n: c_int, input_len: c_int) {
    let end = (*Np).inout.add(input_len as usize);

    while {
        let cur = n;
        n -= 1;
        cur > 0
    } {
        if OVERLOAD_TEST!(Np, input_len) {
            break; /* end of input */
        }
        if !strchr(c"0123456789.,+-".as_ptr(), *(*Np).inout_p as c_int).is_null() {
            break; /* it's a data character */
        }
        (*Np).inout_p = (*Np).inout_p.add(pg_mblen_range((*Np).inout_p, end) as usize);
    }
}

unsafe fn NUM_processor(
    node: *mut FormatNode,
    Num: *mut NUMDesc,
    inout: *mut c_char,
    number: *mut c_char,
    input_len: c_int,
    to_char_out_pre_spaces: c_int,
    sign: c_int,
    is_to_char: bool,
    _collid: Oid,
) -> *mut c_char {
    let mut n: *mut FormatNode;
    let mut _Np: NUMProc = core::mem::zeroed();
    let Np: *mut NUMProc = &raw mut _Np;
    let mut pattern: *const c_char;
    let mut pattern_len: c_int;

    memset(Np as *mut c_void, 0, core::mem::size_of::<NUMProc>());

    (*Np).Num = Num;
    (*Np).is_to_char = is_to_char;
    (*Np).number = number;
    (*Np).inout = inout;
    (*Np).last_relevant = null();
    (*Np).read_post = 0;
    (*Np).read_pre = 0;
    (*Np).read_dec = false as c_int;

    if (*(*Np).Num).zero_start != 0 {
        (*(*Np).Num).zero_start -= 1;
    }

    if IS_EEEE((*Np).Num) {
        if !(*Np).is_to_char {
            ereport!(
                ERROR,
                errmsg!("\"EEEE\" not supported for input")
            );
        }
        return strcpy(inout, number);
    }

    /*
     * Sign
     */
    if is_to_char {
        (*Np).sign = sign;

        /* MI/PL/SG - write sign itself and not in number */
        if IS_PLUS((*Np).Num) || IS_MINUS((*Np).Num) {
            if IS_PLUS((*Np).Num) && !IS_MINUS((*Np).Num) {
                (*Np).sign_wrote = false as c_int; /* need sign */
            } else {
                (*Np).sign_wrote = true as c_int; /* needn't sign */
            }
        } else {
            if (*Np).sign != b'-' as c_int {
                if IS_FILLMODE((*Np).Num) {
                    (*(*Np).Num).flag &= !NUM_F_BRACKET;
                }
            }

            if (*Np).sign == b'+' as c_int && IS_FILLMODE((*Np).Num) && !IS_LSIGN((*Np).Num) {
                (*Np).sign_wrote = true as c_int; /* needn't sign */
            } else {
                (*Np).sign_wrote = false as c_int; /* need sign */
            }

            if (*(*Np).Num).lsign == NUM_LSIGN_PRE
                && (*(*Np).Num).pre == (*(*Np).Num).pre_lsign_num
            {
                (*(*Np).Num).lsign = NUM_LSIGN_POST;
            }
        }
    } else {
        (*Np).sign = false as c_int;
    }

    /*
     * Count
     */
    (*Np).num_count = (*(*Np).Num).post + (*(*Np).Num).pre - 1;

    if is_to_char {
        (*Np).out_pre_spaces = to_char_out_pre_spaces;

        if IS_FILLMODE((*Np).Num) && IS_DECIMAL((*Np).Num) {
            (*Np).last_relevant = get_last_relevant_decnum((*Np).number);

            /*
             * If any '0' specifiers are present, make sure we don't strip
             * those digits.  But don't advance last_relevant beyond the last
             * character of the Np->number string, which is a hazard if the
             * number got shortened due to precision limitations.
             */
            if !(*Np).last_relevant.is_null() && (*(*Np).Num).zero_end > (*Np).out_pre_spaces {
                let mut last_zero_pos: c_int;
                let last_zero: *mut c_char;

                /* note that Np->number cannot be zero-length here */
                last_zero_pos = strlen((*Np).number) as c_int - 1;
                last_zero_pos = min_i32(last_zero_pos, (*(*Np).Num).zero_end - (*Np).out_pre_spaces);
                last_zero = (*Np).number.add(last_zero_pos as usize);
                if (*Np).last_relevant < last_zero as *const c_char {
                    (*Np).last_relevant = last_zero;
                }
            }
        }

        if (*Np).sign_wrote == false as c_int && (*Np).out_pre_spaces == 0 {
            (*Np).num_count += 1;
        }
    } else {
        (*Np).out_pre_spaces = 0;
        *(*Np).number = b' ' as c_char; /* sign space */
        *(*Np).number.add(1) = b'\0' as c_char;
    }

    (*Np).num_in = 0;
    (*Np).num_curr = 0;

    /*
     * Locale
     */
    NUM_prepare_locale(Np);

    /*
     * Processor direct cycle
     */
    if (*Np).is_to_char {
        (*Np).number_p = (*Np).number;
    } else {
        (*Np).number_p = (*Np).number.add(1); /* first char is space for sign */
    }

    n = node;
    (*Np).inout_p = (*Np).inout;
    while (*n).r#type != NODE_TYPE_END {
        if !(*Np).is_to_char {
            /*
             * Check at least one byte remains to be scanned.  (In actions
             * below, must use AMOUNT_TEST if we want to read more bytes than
             * that.)
             */
            if OVERLOAD_TEST!(Np, input_len) {
                break;
            }
        }

        /*
         * Format pictures actions
         */
        if (*n).r#type == NODE_TYPE_ACTION {
            /*
             * Create/read digit/zero/blank/sign/special-case
             *
             * 'NUM_S' note: The locale sign is anchored to number and we
             * read/write it when we work with first or last number
             * (NUM_0/NUM_9).  This is why NUM_S is missing in switch().
             *
             * Notice the "Np->inout_p++" at the bottom of the loop.  This is
             * why most of the actions advance inout_p one less than you might
             * expect.  In cases where we don't want that increment to happen,
             * a switch case ends with "continue" not "break".
             */
            let key_id = (*(*n).key).id;
            if key_id == NUM_9 || key_id == NUM_0 || key_id == NUM_DEC || key_id == NUM_D {
                if (*Np).is_to_char {
                    NUM_numpart_to_char(Np, key_id);
                    n = n.add(1);
                    continue; /* for() */
                } else {
                    NUM_numpart_from_char(Np, key_id, input_len);
                    /* break; switch() case: */
                }
            } else if key_id == NUM_COMMA {
                if (*Np).is_to_char {
                    if (*Np).num_in == 0 {
                        if IS_FILLMODE((*Np).Num) {
                            n = n.add(1);
                            continue;
                        } else {
                            *(*Np).inout_p = b' ' as c_char;
                        }
                    } else {
                        *(*Np).inout_p = b',' as c_char;
                    }
                } else {
                    if (*Np).num_in == 0 {
                        if IS_FILLMODE((*Np).Num) {
                            n = n.add(1);
                            continue;
                        }
                    }
                    if *(*Np).inout_p != b',' as c_char {
                        n = n.add(1);
                        continue;
                    }
                }
            } else if key_id == NUM_G {
                pattern = (*Np).L_thousands_sep;
                pattern_len = strlen(pattern) as c_int;
                if (*Np).is_to_char {
                    if (*Np).num_in == 0 {
                        if IS_FILLMODE((*Np).Num) {
                            n = n.add(1);
                            continue;
                        } else {
                            /* just in case there are MB chars */
                            pattern_len = pg_mbstrlen(pattern);
                            memset((*Np).inout_p as *mut c_void, b' ' as c_int, pattern_len as usize);
                            (*Np).inout_p = (*Np).inout_p.add((pattern_len - 1) as usize);
                        }
                    } else {
                        strcpy((*Np).inout_p, pattern);
                        (*Np).inout_p = (*Np).inout_p.add((pattern_len - 1) as usize);
                    }
                } else {
                    if (*Np).num_in == 0 {
                        if IS_FILLMODE((*Np).Num) {
                            n = n.add(1);
                            continue;
                        }
                    }

                    /*
                     * Because L_thousands_sep typically contains data
                     * characters (either '.' or ','), we can't use
                     * NUM_eat_non_data_chars here.  Instead skip only if
                     * the input matches L_thousands_sep.
                     */
                    if AMOUNT_TEST!(Np, input_len, pattern_len)
                        && strncmp((*Np).inout_p, pattern, pattern_len as usize) == 0
                    {
                        (*Np).inout_p = (*Np).inout_p.add((pattern_len - 1) as usize);
                    } else {
                        n = n.add(1);
                        continue;
                    }
                }
            } else if key_id == NUM_L {
                pattern = (*Np).L_currency_symbol;
                if (*Np).is_to_char {
                    strcpy((*Np).inout_p, pattern);
                    (*Np).inout_p = (*Np).inout_p.add(strlen(pattern) - 1);
                } else {
                    NUM_eat_non_data_chars(Np, pg_mbstrlen(pattern), input_len);
                    n = n.add(1);
                    continue;
                }
            } else if key_id == NUM_RN || key_id == NUM_rn {
                if (*Np).is_to_char {
                    let number_p: *const c_char;

                    if key_id == NUM_rn {
                        number_p = asc_tolower_z((*Np).number_p);
                    } else {
                        number_p = (*Np).number_p;
                    }
                    if IS_FILLMODE((*Np).Num) {
                        strcpy((*Np).inout_p, number_p);
                    } else {
                        write_cstr((*Np).inout_p, &format!("{:>15}", cstr(number_p)));
                    }
                    (*Np).inout_p = (*Np).inout_p.add(strlen((*Np).inout_p) - 1);
                } else {
                    let roman_result = roman_to_int(Np, input_len);
                    let numlen: c_int;

                    if roman_result < 0 {
                        ereport!(
                            ERROR,
                            errmsg!("invalid Roman numeral")
                        );
                    }
                    let s = format!("{}", roman_result);
                    write_cstr((*Np).number_p, &s);
                    numlen = s.len() as c_int;
                    (*Np).number_p = (*Np).number_p.add(numlen as usize);
                    (*(*Np).Num).pre = numlen;
                    (*(*Np).Num).post = 0;
                    n = n.add(1);
                    continue; /* roman_to_int ate all the chars */
                }
            } else if key_id == NUM_th {
                if IS_ROMAN((*Np).Num)
                    || *(*Np).number == b'#' as c_char
                    || (*Np).sign == b'-' as c_int
                    || IS_DECIMAL((*Np).Num)
                {
                    n = n.add(1);
                    continue;
                }

                if (*Np).is_to_char {
                    strcpy((*Np).inout_p, get_th((*Np).number, TH_LOWER));
                    (*Np).inout_p = (*Np).inout_p.add(1);
                } else {
                    /* All variants of 'th' occupy 2 characters */
                    NUM_eat_non_data_chars(Np, 2, input_len);
                    n = n.add(1);
                    continue;
                }
            } else if key_id == NUM_TH {
                if IS_ROMAN((*Np).Num)
                    || *(*Np).number == b'#' as c_char
                    || (*Np).sign == b'-' as c_int
                    || IS_DECIMAL((*Np).Num)
                {
                    n = n.add(1);
                    continue;
                }

                if (*Np).is_to_char {
                    strcpy((*Np).inout_p, get_th((*Np).number, TH_UPPER));
                    (*Np).inout_p = (*Np).inout_p.add(1);
                } else {
                    /* All variants of 'TH' occupy 2 characters */
                    NUM_eat_non_data_chars(Np, 2, input_len);
                    n = n.add(1);
                    continue;
                }
            } else if key_id == NUM_MI {
                if (*Np).is_to_char {
                    if (*Np).sign == b'-' as c_int {
                        *(*Np).inout_p = b'-' as c_char;
                    } else if IS_FILLMODE((*Np).Num) {
                        n = n.add(1);
                        continue;
                    } else {
                        *(*Np).inout_p = b' ' as c_char;
                    }
                } else {
                    if *(*Np).inout_p == b'-' as c_char {
                        *(*Np).number = b'-' as c_char;
                    } else {
                        NUM_eat_non_data_chars(Np, 1, input_len);
                        n = n.add(1);
                        continue;
                    }
                }
            } else if key_id == NUM_PL {
                if (*Np).is_to_char {
                    if (*Np).sign == b'+' as c_int {
                        *(*Np).inout_p = b'+' as c_char;
                    } else if IS_FILLMODE((*Np).Num) {
                        n = n.add(1);
                        continue;
                    } else {
                        *(*Np).inout_p = b' ' as c_char;
                    }
                } else {
                    if *(*Np).inout_p == b'+' as c_char {
                        *(*Np).number = b'+' as c_char;
                    } else {
                        NUM_eat_non_data_chars(Np, 1, input_len);
                        n = n.add(1);
                        continue;
                    }
                }
            } else if key_id == NUM_SG {
                if (*Np).is_to_char {
                    *(*Np).inout_p = (*Np).sign as c_char;
                } else {
                    if *(*Np).inout_p == b'-' as c_char {
                        *(*Np).number = b'-' as c_char;
                    } else if *(*Np).inout_p == b'+' as c_char {
                        *(*Np).number = b'+' as c_char;
                    } else {
                        NUM_eat_non_data_chars(Np, 1, input_len);
                        n = n.add(1);
                        continue;
                    }
                }
            } else {
                n = n.add(1);
                continue;
            }
        } else {
            /*
             * In TO_CHAR, non-pattern characters in the format are copied to
             * the output.  In TO_NUMBER, we skip one input character for each
             * non-pattern format character, whether or not it matches the
             * format character.
             */
            if (*Np).is_to_char {
                strcpy((*Np).inout_p, (*n).character.as_ptr());
                (*Np).inout_p = (*Np).inout_p.add(strlen((*Np).inout_p));
            } else {
                (*Np).inout_p = (*Np)
                    .inout_p
                    .add(pg_mblen_range((*Np).inout_p, (*Np).inout.add(input_len as usize)) as usize);
            }
            n = n.add(1);
            continue;
        }
        (*Np).inout_p = (*Np).inout_p.add(1);
        n = n.add(1);
    }

    if (*Np).is_to_char {
        *(*Np).inout_p = b'\0' as c_char;
        (*Np).inout
    } else {
        if *(*Np).number_p.sub(1) == b'.' as c_char {
            *(*Np).number_p.sub(1) = b'\0' as c_char;
        } else {
            *(*Np).number_p = b'\0' as c_char;
        }

        /*
         * Correction - precision of dec. number
         */
        (*(*Np).Num).post = (*Np).read_post;

        (*Np).number
    }
}

/* ----------
 * MACRO: Start part of NUM - for all NUM's to_char variants
 *	(sorry, but I hate copy same code - macro is better..)
 * ----------
 */
macro_rules! NUM_TOCHAR_prepare {
    ($fcinfo:expr, $fmt:expr, $result:expr, $format:expr, $Num:expr, $shouldFree:expr) => {{
        let len: c_int = VARSIZE_ANY_EXHDR($fmt as *const c_char) as c_int;
        if len <= 0 || len >= (i32::MAX - VARHDRSZ as c_int) / NUM_MAX_ITEM_SIZ as c_int {
            PG_RETURN_TEXT_P!($fcinfo, cstring_to_text(c"".as_ptr()));
        }
        $result = palloc0((len as usize * NUM_MAX_ITEM_SIZ) + 1 + VARHDRSZ as usize) as *mut text;
        $format = NUM_cache(len, &raw mut $Num, $fmt, &raw mut $shouldFree);
    }};
}

/* ----------
 * MACRO: Finish part of NUM
 * ----------
 */
macro_rules! NUM_TOCHAR_finish {
    ($fmt:expr, $result:expr, $format:expr, $Num:expr, $shouldFree:expr, $numstr:expr, $out_pre_spaces:expr, $sign:expr, $fcinfo:expr) => {{
        let len: c_int;

        NUM_processor(
            $format,
            &raw mut $Num,
            VARDATA($result as *const c_char) as *mut c_char,
            $numstr,
            0,
            $out_pre_spaces,
            $sign,
            true,
            PG_GET_COLLATION!($fcinfo),
        );

        if $shouldFree {
            pfree($format as *mut c_void);
        }

        /*
         * Convert null-terminated representation of result to standard text.
         * The result is usually much bigger than it needs to be, but there
         * seems little point in realloc'ing it smaller.
         */
        len = strlen(VARDATA($result as *const c_char) as *const c_char) as c_int;
        SET_VARSIZE($result as *mut c_char, len + VARHDRSZ as c_int);
    }};
}

/* -------------------
 * NUMERIC to_number() (convert string to numeric)
 * -------------------
 */
pub unsafe fn numeric_to_number(fcinfo: FunctionCallInfo) -> Datum {
    let value: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut Num: NUMDesc = core::mem::zeroed();
    let mut result: Datum;
    let format: *mut FormatNode;
    let numstr: *mut c_char;
    let mut shouldFree: bool = false;
    let mut len: c_int = 0;
    let scale: c_int;
    let precision: c_int;

    len = VARSIZE_ANY_EXHDR(fmt as *const c_char) as c_int;

    if len <= 0 || len >= i32::MAX / NUM_MAX_ITEM_SIZ as c_int {
        PG_RETURN_NULL!(fcinfo);
    }

    format = NUM_cache(len, &raw mut Num, fmt, &raw mut shouldFree);

    numstr = palloc((len as usize * NUM_MAX_ITEM_SIZ) + 1) as *mut c_char;

    NUM_processor(
        format,
        &raw mut Num,
        VARDATA_ANY(value as *const c_char) as *mut c_char,
        numstr,
        VARSIZE_ANY_EXHDR(value as *const c_char) as c_int,
        0,
        0,
        false,
        PG_GET_COLLATION!(fcinfo),
    );

    scale = Num.post;
    precision = Num.pre + Num.multi + scale;

    if shouldFree {
        pfree(format as *mut c_void);
    }

    result = DirectFunctionCall3!(
        numeric_in,
        CStringGetDatum(numstr),
        ObjectIdGetDatum(InvalidOid),
        Int32GetDatum(((precision << 16) | scale) + VARHDRSZ as c_int)
    );

    if IS_MULTI(&raw const Num) {
        let x: Numeric;
        let a: Numeric = int64_to_numeric(10);
        let b: Numeric = int64_to_numeric(-(Num.multi as int64));

        x = DatumGetNumeric(DirectFunctionCall2!(
            numeric_power,
            NumericGetDatum(a),
            NumericGetDatum(b)
        ));
        result = DirectFunctionCall2!(numeric_mul, result, NumericGetDatum(x));
    }

    pfree(numstr as *mut c_void);
    result
}

/* ------------------
 * NUMERIC to_char()
 * ------------------
 */
pub unsafe fn numeric_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let value: Numeric = PG_GETARG_NUMERIC!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut Num: NUMDesc = core::mem::zeroed();
    let format: *mut FormatNode;
    let mut result: *mut text = null_mut();
    let mut shouldFree: bool = false;
    let mut out_pre_spaces: c_int = 0;
    let mut sign: c_int = 0;
    let mut numstr: *mut c_char;
    let orgnum: *mut c_char;
    let p: *mut c_char;

    NUM_TOCHAR_prepare!(fcinfo, fmt, result, format, Num, shouldFree);

    /*
     * On DateType depend part (numeric)
     */
    if IS_ROMAN(&raw const Num) {
        let mut intvalue: int32;
        let mut err: bool = false;

        /* Round and convert to int */
        intvalue = numeric_int4_opt_error(value, &raw mut err);
        /* On overflow, just use PG_INT32_MAX; int_to_roman will cope */
        if err {
            intvalue = PG_INT32_MAX;
        }
        numstr = int_to_roman(intvalue);
    } else if IS_EEEE(&raw const Num) {
        orgnum = numeric_out_sci(value, Num.post);

        /*
         * numeric_out_sci() does not emit a sign for positive numbers.  We
         * need to add a space in this case so that positive and negative
         * numbers are aligned.  Also must check for NaN/infinity cases, which
         * we handle the same way as in float8_to_char.
         */
        if strcmp(orgnum, c"NaN".as_ptr()) == 0
            || strcmp(orgnum, c"Infinity".as_ptr()) == 0
            || strcmp(orgnum, c"-Infinity".as_ptr()) == 0
        {
            /*
             * Allow 6 characters for the leading sign, the decimal point,
             * "e", the exponent's sign and two exponent digits.
             */
            numstr = palloc((Num.pre + Num.post + 7) as usize) as *mut c_char;
            fill_str(numstr, b'#' as c_int, Num.pre + Num.post + 6);
            *numstr = b' ' as c_char;
            *numstr.add((Num.pre + 1) as usize) = b'.' as c_char;
        } else if *orgnum != b'-' as c_char {
            numstr = palloc(strlen(orgnum) + 2) as *mut c_char;
            *numstr = b' ' as c_char;
            strcpy(numstr.add(1), orgnum);
        } else {
            numstr = orgnum;
        }
    } else {
        let numstr_pre_len: c_int;
        let mut val: Numeric = value;
        let mut x: Numeric;

        if IS_MULTI(&raw const Num) {
            let a: Numeric = int64_to_numeric(10);
            let b: Numeric = int64_to_numeric(Num.multi as int64);

            x = DatumGetNumeric(DirectFunctionCall2!(
                numeric_power,
                NumericGetDatum(a),
                NumericGetDatum(b)
            ));
            val = DatumGetNumeric(DirectFunctionCall2!(
                numeric_mul,
                NumericGetDatum(value),
                NumericGetDatum(x)
            ));
            Num.pre += Num.multi;
        }

        x = DatumGetNumeric(DirectFunctionCall2!(
            numeric_round,
            NumericGetDatum(val),
            Int32GetDatum(Num.post)
        ));
        orgnum = DatumGetCString(DirectFunctionCall1!(numeric_out, NumericGetDatum(x)));

        if *orgnum == b'-' as c_char {
            sign = b'-' as c_int;
            numstr = orgnum.add(1);
        } else {
            sign = b'+' as c_int;
            numstr = orgnum;
        }

        p = strchr(numstr, b'.' as c_int);
        if !p.is_null() {
            numstr_pre_len = p.offset_from(numstr) as c_int;
        } else {
            numstr_pre_len = strlen(numstr) as c_int;
        }

        /* needs padding? */
        if numstr_pre_len < Num.pre {
            out_pre_spaces = Num.pre - numstr_pre_len;
        }
        /* overflowed prefix digit format? */
        else if numstr_pre_len > Num.pre {
            numstr = palloc((Num.pre + Num.post + 2) as usize) as *mut c_char;
            fill_str(numstr, b'#' as c_int, Num.pre + Num.post + 1);
            *numstr.add(Num.pre as usize) = b'.' as c_char;
        }
    }

    NUM_TOCHAR_finish!(
        fmt,
        result,
        format,
        Num,
        shouldFree,
        numstr,
        out_pre_spaces,
        sign,
        fcinfo
    );
    PG_RETURN_TEXT_P!(fcinfo, result)
}

/* ---------------
 * INT4 to_char()
 * ---------------
 */
pub unsafe fn int4_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let value: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut Num: NUMDesc = core::mem::zeroed();
    let format: *mut FormatNode;
    let mut result: *mut text = null_mut();
    let mut shouldFree: bool = false;
    let mut out_pre_spaces: c_int = 0;
    let mut sign: c_int = 0;
    let mut numstr: *mut c_char;
    let mut orgnum: *mut c_char;

    NUM_TOCHAR_prepare!(fcinfo, fmt, result, format, Num, shouldFree);

    /*
     * On DateType depend part (int32)
     */
    if IS_ROMAN(&raw const Num) {
        numstr = int_to_roman(value);
    } else if IS_EEEE(&raw const Num) {
        /* we can do it easily because float8 won't lose any precision */
        let val: float8 = value as float8;

        orgnum = psprintf_e(Num.post, val);

        /*
         * Swap a leading positive sign for a space.
         */
        if *orgnum == b'+' as c_char {
            *orgnum = b' ' as c_char;
        }

        numstr = orgnum;
    } else {
        let numstr_pre_len: c_int;

        if IS_MULTI(&raw const Num) {
            orgnum = DatumGetCString(DirectFunctionCall1!(
                int4out,
                Int32GetDatum(value * (pow(10.0, Num.multi as f64) as int32))
            ));
            Num.pre += Num.multi;
        } else {
            orgnum = DatumGetCString(DirectFunctionCall1!(int4out, Int32GetDatum(value)));
        }

        if *orgnum == b'-' as c_char {
            sign = b'-' as c_int;
            orgnum = orgnum.add(1);
        } else {
            sign = b'+' as c_int;
        }

        numstr_pre_len = strlen(orgnum) as c_int;

        /* post-decimal digits?  Pad out with zeros. */
        if Num.post != 0 {
            numstr = palloc((numstr_pre_len + Num.post + 2) as usize) as *mut c_char;
            strcpy(numstr, orgnum);
            *numstr.add(numstr_pre_len as usize) = b'.' as c_char;
            memset(
                numstr.add((numstr_pre_len + 1) as usize) as *mut c_void,
                b'0' as c_int,
                Num.post as usize,
            );
            *numstr.add((numstr_pre_len + Num.post + 1) as usize) = b'\0' as c_char;
        } else {
            numstr = orgnum;
        }

        /* needs padding? */
        if numstr_pre_len < Num.pre {
            out_pre_spaces = Num.pre - numstr_pre_len;
        }
        /* overflowed prefix digit format? */
        else if numstr_pre_len > Num.pre {
            numstr = palloc((Num.pre + Num.post + 2) as usize) as *mut c_char;
            fill_str(numstr, b'#' as c_int, Num.pre + Num.post + 1);
            *numstr.add(Num.pre as usize) = b'.' as c_char;
        }
    }

    NUM_TOCHAR_finish!(
        fmt,
        result,
        format,
        Num,
        shouldFree,
        numstr,
        out_pre_spaces,
        sign,
        fcinfo
    );
    PG_RETURN_TEXT_P!(fcinfo, result)
}

/* ---------------
 * INT8 to_char()
 * ---------------
 */
pub unsafe fn int8_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let mut value: int64 = PG_GETARG_INT64!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut Num: NUMDesc = core::mem::zeroed();
    let format: *mut FormatNode;
    let mut result: *mut text = null_mut();
    let mut shouldFree: bool = false;
    let mut out_pre_spaces: c_int = 0;
    let mut sign: c_int = 0;
    let mut numstr: *mut c_char;
    let mut orgnum: *mut c_char;

    NUM_TOCHAR_prepare!(fcinfo, fmt, result, format, Num, shouldFree);

    /*
     * On DateType depend part (int64)
     */
    if IS_ROMAN(&raw const Num) {
        let intvalue: int32;

        /* On overflow, just use PG_INT32_MAX; int_to_roman will cope */
        if value <= PG_INT32_MAX as int64 && value >= PG_INT32_MIN as int64 {
            intvalue = value as int32;
        } else {
            intvalue = PG_INT32_MAX;
        }
        numstr = int_to_roman(intvalue);
    } else if IS_EEEE(&raw const Num) {
        /* to avoid loss of precision, must go via numeric not float8 */
        orgnum = numeric_out_sci(int64_to_numeric(value), Num.post);

        /*
         * numeric_out_sci() does not emit a sign for positive numbers.  We
         * need to add a space in this case so that positive and negative
         * numbers are aligned.  We don't have to worry about NaN/inf here.
         */
        if *orgnum != b'-' as c_char {
            numstr = palloc(strlen(orgnum) + 2) as *mut c_char;
            *numstr = b' ' as c_char;
            strcpy(numstr.add(1), orgnum);
        } else {
            numstr = orgnum;
        }
    } else {
        let numstr_pre_len: c_int;

        if IS_MULTI(&raw const Num) {
            let multi: f64 = pow(10.0, Num.multi as f64);

            value = DatumGetInt64(DirectFunctionCall2!(
                int8mul,
                Int64GetDatum(value),
                DirectFunctionCall1!(dtoi8, Float8GetDatum(multi))
            ));
            Num.pre += Num.multi;
        }

        orgnum = DatumGetCString(DirectFunctionCall1!(int8out, Int64GetDatum(value)));

        if *orgnum == b'-' as c_char {
            sign = b'-' as c_int;
            orgnum = orgnum.add(1);
        } else {
            sign = b'+' as c_int;
        }

        numstr_pre_len = strlen(orgnum) as c_int;

        /* post-decimal digits?  Pad out with zeros. */
        if Num.post != 0 {
            numstr = palloc((numstr_pre_len + Num.post + 2) as usize) as *mut c_char;
            strcpy(numstr, orgnum);
            *numstr.add(numstr_pre_len as usize) = b'.' as c_char;
            memset(
                numstr.add((numstr_pre_len + 1) as usize) as *mut c_void,
                b'0' as c_int,
                Num.post as usize,
            );
            *numstr.add((numstr_pre_len + Num.post + 1) as usize) = b'\0' as c_char;
        } else {
            numstr = orgnum;
        }

        /* needs padding? */
        if numstr_pre_len < Num.pre {
            out_pre_spaces = Num.pre - numstr_pre_len;
        }
        /* overflowed prefix digit format? */
        else if numstr_pre_len > Num.pre {
            numstr = palloc((Num.pre + Num.post + 2) as usize) as *mut c_char;
            fill_str(numstr, b'#' as c_int, Num.pre + Num.post + 1);
            *numstr.add(Num.pre as usize) = b'.' as c_char;
        }
    }

    NUM_TOCHAR_finish!(
        fmt,
        result,
        format,
        Num,
        shouldFree,
        numstr,
        out_pre_spaces,
        sign,
        fcinfo
    );
    PG_RETURN_TEXT_P!(fcinfo, result)
}

/* -----------------
 * FLOAT4 to_char()
 * -----------------
 */
pub unsafe fn float4_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let mut value: float4 = PG_GETARG_FLOAT4!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut Num: NUMDesc = core::mem::zeroed();
    let format: *mut FormatNode;
    let mut result: *mut text = null_mut();
    let mut shouldFree: bool = false;
    let mut out_pre_spaces: c_int = 0;
    let mut sign: c_int = 0;
    let mut numstr: *mut c_char;
    let p: *mut c_char;

    NUM_TOCHAR_prepare!(fcinfo, fmt, result, format, Num, shouldFree);

    if IS_ROMAN(&raw const Num) {
        let intvalue: int32;

        /* See notes in ftoi4() */
        value = rint(value as f64) as float4;
        /* On overflow, just use PG_INT32_MAX; int_to_roman will cope */
        if isnan(value as f64) == 0 && FLOAT4_FITS_IN_INT32(value) {
            intvalue = value as int32;
        } else {
            intvalue = PG_INT32_MAX;
        }
        numstr = int_to_roman(intvalue);
    } else if IS_EEEE(&raw const Num) {
        if isnan(value as f64) != 0 || isinf(value as f64) != 0 {
            /*
             * Allow 6 characters for the leading sign, the decimal point,
             * "e", the exponent's sign and two exponent digits.
             */
            numstr = palloc((Num.pre + Num.post + 7) as usize) as *mut c_char;
            fill_str(numstr, b'#' as c_int, Num.pre + Num.post + 6);
            *numstr = b' ' as c_char;
            *numstr.add((Num.pre + 1) as usize) = b'.' as c_char;
        } else {
            numstr = psprintf_e(Num.post, value as f64);

            /*
             * Swap a leading positive sign for a space.
             */
            if *numstr == b'+' as c_char {
                *numstr = b' ' as c_char;
            }
        }
    } else {
        let mut val: float4 = value;
        let mut orgnum: *mut c_char;
        let mut numstr_pre_len: c_int;

        if IS_MULTI(&raw const Num) {
            let multi: f32 = pow(10.0, Num.multi as f64) as f32;

            val = value * multi;
            Num.pre += Num.multi;
        }

        orgnum = psprintf_f0(fabs(val as f64));
        numstr_pre_len = strlen(orgnum) as c_int;

        /* adjust post digits to fit max float digits */
        if numstr_pre_len >= FLT_DIG {
            Num.post = 0;
        } else if numstr_pre_len + Num.post > FLT_DIG {
            Num.post = FLT_DIG - numstr_pre_len;
        }
        orgnum = psprintf_f(Num.post, val as f64);

        if *orgnum == b'-' as c_char {
            /* < 0 */
            sign = b'-' as c_int;
            numstr = orgnum.add(1);
        } else {
            sign = b'+' as c_int;
            numstr = orgnum;
        }

        p = strchr(numstr, b'.' as c_int);
        if !p.is_null() {
            numstr_pre_len = p.offset_from(numstr) as c_int;
        } else {
            numstr_pre_len = strlen(numstr) as c_int;
        }

        /* needs padding? */
        if numstr_pre_len < Num.pre {
            out_pre_spaces = Num.pre - numstr_pre_len;
        }
        /* overflowed prefix digit format? */
        else if numstr_pre_len > Num.pre {
            numstr = palloc((Num.pre + Num.post + 2) as usize) as *mut c_char;
            fill_str(numstr, b'#' as c_int, Num.pre + Num.post + 1);
            *numstr.add(Num.pre as usize) = b'.' as c_char;
        }
    }

    NUM_TOCHAR_finish!(
        fmt,
        result,
        format,
        Num,
        shouldFree,
        numstr,
        out_pre_spaces,
        sign,
        fcinfo
    );
    PG_RETURN_TEXT_P!(fcinfo, result)
}

/* -----------------
 * FLOAT8 to_char()
 * -----------------
 */
pub unsafe fn float8_to_char(fcinfo: FunctionCallInfo) -> Datum {
    let mut value: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let fmt: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut Num: NUMDesc = core::mem::zeroed();
    let format: *mut FormatNode;
    let mut result: *mut text = null_mut();
    let mut shouldFree: bool = false;
    let mut out_pre_spaces: c_int = 0;
    let mut sign: c_int = 0;
    let mut numstr: *mut c_char;
    let p: *mut c_char;

    NUM_TOCHAR_prepare!(fcinfo, fmt, result, format, Num, shouldFree);

    if IS_ROMAN(&raw const Num) {
        let intvalue: int32;

        /* See notes in dtoi4() */
        value = rint(value);
        /* On overflow, just use PG_INT32_MAX; int_to_roman will cope */
        if isnan(value) == 0 && FLOAT8_FITS_IN_INT32(value) {
            intvalue = value as int32;
        } else {
            intvalue = PG_INT32_MAX;
        }
        numstr = int_to_roman(intvalue);
    } else if IS_EEEE(&raw const Num) {
        if isnan(value) != 0 || isinf(value) != 0 {
            /*
             * Allow 6 characters for the leading sign, the decimal point,
             * "e", the exponent's sign and two exponent digits.
             */
            numstr = palloc((Num.pre + Num.post + 7) as usize) as *mut c_char;
            fill_str(numstr, b'#' as c_int, Num.pre + Num.post + 6);
            *numstr = b' ' as c_char;
            *numstr.add((Num.pre + 1) as usize) = b'.' as c_char;
        } else {
            numstr = psprintf_e(Num.post, value);

            /*
             * Swap a leading positive sign for a space.
             */
            if *numstr == b'+' as c_char {
                *numstr = b' ' as c_char;
            }
        }
    } else {
        let mut val: float8 = value;
        let mut orgnum: *mut c_char;
        let mut numstr_pre_len: c_int;

        if IS_MULTI(&raw const Num) {
            let multi: f64 = pow(10.0, Num.multi as f64);

            val = value * multi;
            Num.pre += Num.multi;
        }

        orgnum = psprintf_f0(fabs(val));
        numstr_pre_len = strlen(orgnum) as c_int;

        /* adjust post digits to fit max double digits */
        if numstr_pre_len >= DBL_DIG {
            Num.post = 0;
        } else if numstr_pre_len + Num.post > DBL_DIG {
            Num.post = DBL_DIG - numstr_pre_len;
        }
        orgnum = psprintf_f(Num.post, val);

        if *orgnum == b'-' as c_char {
            /* < 0 */
            sign = b'-' as c_int;
            numstr = orgnum.add(1);
        } else {
            sign = b'+' as c_int;
            numstr = orgnum;
        }

        p = strchr(numstr, b'.' as c_int);
        if !p.is_null() {
            numstr_pre_len = p.offset_from(numstr) as c_int;
        } else {
            numstr_pre_len = strlen(numstr) as c_int;
        }

        /* needs padding? */
        if numstr_pre_len < Num.pre {
            out_pre_spaces = Num.pre - numstr_pre_len;
        }
        /* overflowed prefix digit format? */
        else if numstr_pre_len > Num.pre {
            numstr = palloc((Num.pre + Num.post + 2) as usize) as *mut c_char;
            fill_str(numstr, b'#' as c_int, Num.pre + Num.post + 1);
            *numstr.add(Num.pre as usize) = b'.' as c_char;
        }
    }

    NUM_TOCHAR_finish!(
        fmt,
        result,
        format,
        Num,
        shouldFree,
        numstr,
        out_pre_spaces,
        sign,
        fcinfo
    );
    PG_RETURN_TEXT_P!(fcinfo, result)
}
