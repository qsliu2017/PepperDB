//! timestamp.rs
//!   Functions for the built-in SQL types "timestamp" and "interval".
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/timestamp.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h               -> crate::prelude::* (Datum, *GetDatum, palloc, elog!/ereport!/Assert!)
//!   <ctype.h>/<math.h>/<limits.h>/<sys/time.h> -> Rust libm/intrinsics + libc gettimeofday
//!   access/xact.h            -> GetCurrentTransactionStartTimestamp / GetCurrentStatementStartTimestamp (stubbed)
//!   catalog/pg_type.h        -> crate::catalog::pg_type_d (TIMESTAMPOID/TIMESTAMPTZOID/INTERVALOID)
//!   common/int.h             -> crate::common::int (pg_add/sub/mul/neg_s32/s64_overflow)
//!   common/int128.h          -> crate::common::int128 (INT128/int64_to_int128/...)
//!   funcapi.h                -> SRF/FuncCallContext/AggCheckCallContext (stubbed below)
//!   libpq/pqformat.h         -> crate::libpq::pqformat (pq_*)
//!   miscadmin.h              -> crate::miscadmin
//!   nodes/nodeFuncs.h        -> exprTypmod (stubbed below)
//!   nodes/supportnodes.h     -> SupportRequest* (stubbed below)
//!   optimizer/optimizer.h    -> estimate_expression_value/is_funcclause/relabel_to_typmod (stubbed)
//!   parser/scansup.h         -> downcase_truncate_identifier (stubbed below)
//!   utils/array.h            -> ArrayType/ArrayGetIntegerTypmods (stubbed below)
//!   utils/builtins.h         -> cstring_to_text/text helpers
//!   utils/date.h             -> crate::utils::adt::date (DateADT/TimeADT + INTERVAL_* etc.)
//!   utils/datetime.h         -> crate::utils::adt::datetime & date (Decode*/Encode*/j2date/date2j/DTK_*/consts)
//!   utils/float.h            -> crate::utils::adt::float (float8_mul/get_float8_infinity/FLOAT8_FITS_*)
//!   utils/numeric.h          -> crate::utils::adt::numeric (int64_to_numeric/...; stubbed below until ported)
//!   utils/skipsupport.h      -> SkipSupport (stubbed below)
//!   utils/sortsupport.h      -> SortSupport (stubbed below)
//!
//! This file is the real home for timestamp2tm/tm2timestamp/timestamp_cmp_internal/
//! dt2time/EncodeSpecialTimestamp/AdjustTimestampForTypmod/etc.  date.rs has local
//! `unimplemented!` stubs for these that will eventually call into this module.

use crate::prelude::*;

use crate::utils::fmgr::*;
use crate::c::{float8, int32, int64, text, uint64, Size};
use crate::common::int::{
    pg_add_s32_overflow, pg_add_s64_overflow, pg_mul_s32_overflow, pg_mul_s64_overflow,
    pg_sub_s32_overflow, pg_sub_s64_overflow,
};
use crate::common::int128::{
    int128_add_int64_mul_int64, int128_compare, int128_to_int64, int64_to_int128, INT128,
};
use crate::lib::stringinfo::{initReadOnlyStringInfo, StringInfo, StringInfoData};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgend, pq_getmsgint, pq_getmsgint64, pq_sendint32,
    pq_sendint64,
};
use crate::pgtime::{pg_tm, pg_tz, TZ_STRLEN_MAX};
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt32, DatumGetInt64,
    DatumGetPointer, Float8GetDatum, Int32GetDatum, Int64GetDatum, ObjectIdGetDatum,
    PointerGetDatum,
};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::adt::datetime::{
    j2date, pg_itm, pg_itm_in, AppendSeconds, ClearPgItmIn, DecodeISO8601Interval, DecodeInterval, DecodeTimezone,
    DecodeTimezoneName, EncodeInterval, EncodeTimezone, GetCurrentTransactionStartTimestamp,
    DAYS_PER_MONTH, DAYS_PER_WEEK, DTERR_FIELD_OVERFLOW, DTERR_INTERVAL_OVERFLOW,
    DTERR_TZDISP_OVERFLOW, DTK_AGO, DTK_DELTA, MAX_INTERVAL_PRECISION, MAX_TIMESTAMP_PRECISION,
    MONTHS_PER_YEAR,
};
use crate::utils::adt::date::{
    float_time_overflows, DateTimeErrorExtra, DTERR_BAD_FORMAT, Interval, Timestamp, TimestampTz,
    TimeOffset, fsec_t, DTK_DATE, DTK_DATE_M, DTK_EARLY, DTK_EPOCH, DTK_LATE,
    DATEVAL_NOBEGIN, DATEVAL_NOEND, DT_NOBEGIN, DT_NOEND, DATETIME_MIN_JULIAN, EARLY,
    HOURS_PER_DAY, INTERVAL_IS_NOBEGIN, INTERVAL_IS_NOEND, INTERVAL_NOT_FINITE, IS_VALID_JULIAN,
    IS_VALID_TIMESTAMP, LATE, MAXDATELEN, MAXDATEFIELDS, MINS_PER_HOUR, MIN_TIMESTAMP,
    POSTGRES_EPOCH_JDATE, SECS_PER_DAY, SECS_PER_HOUR, SECS_PER_MINUTE, TIMESTAMP_END_JULIAN,
    TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND,
    TIMESTAMP_NOT_FINITE, UNIX_EPOCH_JDATE, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE,
    USECS_PER_SEC,
};
use crate::utils::adt::float::{float8_mul, get_float8_infinity};
use crate::utils::adt::varlena::cstring_to_text;
use crate::varatt::{VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::{
    DirectFunctionCall1, DirectFunctionCall2, DirectFunctionCall3, IsA, PG_ARGISNULL,
    PG_GETARG_BOOL, PG_GETARG_BYTEA_PP, PG_GETARG_CSTRING, PG_GETARG_DATUM, PG_GETARG_FLOAT8,
    PG_GETARG_INT32, PG_GETARG_POINTER, PG_GETARG_TEXT_PP, PG_NARGS, PG_RETURN_BOOL,
    PG_RETURN_BYTEA_P, PG_RETURN_CSTRING, PG_RETURN_FLOAT8, PG_RETURN_INT32, PG_RETURN_NULL,
    PG_RETURN_POINTER, PG_RETURN_TEXT_P, PG_RETURN_VOID,
};

use std::ffi::{c_char, c_int, c_void};
use std::ptr::{null, null_mut};

// crate root only exports DirectFunctionCall1/2/3; DirectFunctionCall5Coll exists as a fn.
macro_rules! DirectFunctionCall5 {
    ($func:expr, $a1:expr, $a2:expr, $a3:expr, $a4:expr, $a5:expr) => {
        crate::utils::fmgr::DirectFunctionCall5Coll(
            $func,
            crate::postgres_ext::InvalidOid,
            $a1,
            $a2,
            $a3,
            $a4,
            $a5,
        )
    };
}

extern "C" {
    fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: Size) -> Size;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: Size) -> *mut c_void;
    fn snprintf(s: *mut c_char, n: Size, format: *const c_char, ...) -> c_int;
    fn rint(x: f64) -> f64;
    fn floor(x: f64) -> f64;
    fn fabs(x: f64) -> f64;
    fn isnan(x: f64) -> c_int;
    fn isinf(x: f64) -> c_int;
}

// ===========================================================================
// Local helpers/macros mirroring the conventions established in date.rs.
// ===========================================================================

const INT_MAX: c_int = i32::MAX;
const INT_MIN: c_int = i32::MIN;
const PG_INT64_MIN: int64 = i64::MIN;
const PG_INT64_MAX: int64 = i64::MAX;

/* SAMESIGN(a,b) is defined in C but unused after macro expansion below; kept for parity */
#[allow(unused_macros)]
macro_rules! SAMESIGN {
    ($a:expr, $b:expr) => {
        (($a) < 0) == (($b) < 0)
    };
}

#[allow(non_snake_case)]
unsafe fn DatumGetTimestamp(X: Datum) -> Timestamp {
    DatumGetInt64(X) as Timestamp
}
#[allow(non_snake_case)]
fn TimestampGetDatum(X: Timestamp) -> Datum {
    Int64GetDatum(X)
}
#[allow(non_snake_case)]
fn TimestampTzGetDatum(X: TimestampTz) -> Datum {
    Int64GetDatum(X)
}
#[allow(non_snake_case)]
unsafe fn DatumGetIntervalP(X: Datum) -> *mut Interval {
    DatumGetPointer(X) as *mut Interval
}
#[allow(non_snake_case)]
fn IntervalPGetDatum(X: *const Interval) -> Datum {
    PointerGetDatum(X as *const c_void)
}
macro_rules! PG_GETARG_TIMESTAMP {
    ($fcinfo:expr, $n:expr) => {
        DatumGetTimestamp(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_TIMESTAMPTZ {
    ($fcinfo:expr, $n:expr) => {
        DatumGetTimestamp(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_INTERVAL_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut Interval
    };
}
macro_rules! PG_RETURN_TIMESTAMP {
    ($x:expr) => {
        return TimestampGetDatum($x)
    };
}
macro_rules! PG_RETURN_TIMESTAMPTZ {
    ($x:expr) => {
        return TimestampTzGetDatum($x)
    };
}
macro_rules! PG_RETURN_INTERVAL_P {
    ($x:expr) => {
        return IntervalPGetDatum($x)
    };
}

// INTERVAL_NOBEGIN/INTERVAL_NOEND macros set all three fields to the reserved
// infinity markers.  TODO(pg-port): real macros in datatype/timestamp.h.
#[inline]
unsafe fn INTERVAL_NOBEGIN(i: *mut Interval) {
    (*i).time = DT_NOBEGIN;
    (*i).day = INT_MIN;
    (*i).month = INT_MIN;
}
#[inline]
unsafe fn INTERVAL_NOEND(i: *mut Interval) {
    (*i).time = DT_NOEND;
    (*i).day = INT_MAX;
    (*i).month = INT_MAX;
}

// TMODULO(t, q, u): q = t / u (truncating toward zero); t = t % u.
#[inline]
fn TMODULO(t: &mut Timestamp, q: &mut Timestamp, u: Timestamp) {
    *q = *t / u;
    if *q != 0 {
        *t -= *q * u;
    }
}

// TSROUND: round a double to MAX_TIMESTAMP_PRECISION fractional digits.
const TS_PREC_INV: f64 = 1000000.0;
#[inline]
unsafe fn TSROUND(j: f64) -> f64 {
    rint(j * TS_PREC_INV) / TS_PREC_INV
}

// FLOAT8_FITS_IN_INT32 / FLOAT8_FITS_IN_INT64 from utils/float.h.
#[inline]
fn FLOAT8_FITS_IN_INT32(num: f64) -> bool {
    num >= (i32::MIN as f64) && num < -(i32::MIN as f64)
}
#[inline]
fn FLOAT8_FITS_IN_INT64(num: f64) -> bool {
    num >= (i64::MIN as f64) && num < -(i64::MIN as f64)
}

// day_tab / isleap from datetime.h (number of days in each month).
static DAY_TAB: [[c_int; 13]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 365],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 366],
];
#[allow(non_snake_case)]
#[inline]
fn day_tab(leap: usize, mon: usize) -> c_int {
    DAY_TAB[leap][mon]
}
#[inline]
fn isleap(y: c_int) -> usize {
    (((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0)) as usize)
}

// DateStyle / IntervalStyle GUCs and date encoding style constants.
// TODO(pg-port): real GUCs live in crate::utils::adt::datetime.
#[allow(non_upper_case_globals)]
static mut DateStyle: c_int = 0;
#[allow(non_upper_case_globals)]
static mut IntervalStyle: c_int = 0;
pub const USE_ISO_DATES: c_int = 1;

// Field-type tokens used by Decode/Encode (utils/datetime.h).
pub const RESERV: c_int = 0;
pub const UNITS: c_int = 17;
pub const UNKNOWN_FIELD: c_int = 31;

pub const TZNAME_FIXED_OFFSET: c_int = 0;
pub const TZNAME_DYNTZ: c_int = 1;

pub const INTERVAL_FULL_RANGE: c_int = 0x7FFF;
pub const INTERVAL_FULL_PRECISION: c_int = 0xFFFF;

// DTK_* / DAYS_PER_YEAR not re-exported from datetime/date go here.
pub const DTK_WEEK: c_int = 22;
pub const DTK_MILLENNIUM: c_int = 28;
pub const DTK_CENTURY: c_int = 27;
pub const DTK_DECADE: c_int = 26;
pub const DTK_YEAR: c_int = 25;
pub const DTK_QUARTER: c_int = 24;
pub const DTK_MONTH: c_int = 23;
pub const DTK_DAY: c_int = 21;
pub const DTK_HOUR: c_int = 20;
pub const DTK_MINUTE: c_int = 19;
pub const DTK_SECOND: c_int = 18;
pub const DTK_MILLISEC: c_int = 29;
pub const DTK_MICROSEC: c_int = 30;
pub const DTK_JULIAN: c_int = 31;
pub const DTK_ISOYEAR: c_int = 36;
pub const DTK_ISODOW: c_int = 37;
pub const DTK_DOW: c_int = 32;
pub const DTK_DOY: c_int = 33;
pub const DTK_TZ: c_int = 4;
pub const DTK_TZ_HOUR: c_int = 34;
pub const DTK_TZ_MINUTE: c_int = 35;
pub const DAYS_PER_YEAR: f64 = 365.25;

// catalog/pg_type.h OIDs.
pub const TIMESTAMPOID: Oid = 1114;
pub const TIMESTAMPTZOID: Oid = 1184;
pub const INTERVALOID: Oid = 1186;

// ---------------------------------------------------------------------------
// INTERVAL typmod macros (utils/timestamp.h):
//   typmod = ((precision) | ((range) << 16))
// TODO(pg-port): canonical macros live in datatype/timestamp.h.
// ---------------------------------------------------------------------------
const INTERVAL_PRECISION_MASK: int32 = 0xFFFF;
#[allow(non_snake_case)]
#[inline]
fn INTERVAL_MASK(b: c_int) -> c_int {
    1 << b
}
#[allow(non_snake_case)]
#[inline]
fn INTERVAL_TYPMOD(p: int32, r: int32) -> int32 {
    (p & INTERVAL_PRECISION_MASK) | (r << 16)
}
#[allow(non_snake_case)]
#[inline]
fn INTERVAL_RANGE(t: int32) -> int32 {
    (t >> 16) & INTERVAL_FULL_RANGE
}
#[allow(non_snake_case)]
#[inline]
fn INTERVAL_PRECISION(t: int32) -> int32 {
    t & INTERVAL_PRECISION_MASK
}

// dt.h field codes for INTERVAL_MASK; only used as macro arguments.
const YEAR: c_int = 25; /* DTK_YEAR */
const MONTH: c_int = 23; /* DTK_MONTH */
const DAY: c_int = 21; /* DTK_DAY */
const HOUR: c_int = 20; /* DTK_HOUR */
const MINUTE: c_int = 19; /* DTK_MINUTE */
const SECOND: c_int = 18; /* DTK_SECOND */

// ===========================================================================
// TODO(pg-port) stubs for symbols whose real homes are not yet ported.
// ===========================================================================

#[allow(non_upper_case_globals)]
static mut session_timezone: *mut pg_tz = std::ptr::null_mut();

// --- numeric (TODO(pg-port): crate::utils::adt::numeric) ---
pub type Numeric = *mut c_void;
#[allow(non_snake_case)]
unsafe fn DatumGetNumeric(X: Datum) -> Numeric {
    DatumGetPointer(X) as Numeric
}
#[allow(non_snake_case)]
fn NumericGetDatum(X: Numeric) -> Datum {
    PointerGetDatum(X as *const c_void)
}
macro_rules! PG_RETURN_NUMERIC {
    ($x:expr) => {
        return NumericGetDatum($x)
    };
}
unsafe fn int64_to_numeric(_val: int64) -> Numeric {
    unimplemented!("int64_to_numeric: crate::utils::adt::numeric")
}
unsafe fn int64_div_fast_to_numeric(_val1: int64, _log10val2: c_int) -> Numeric {
    unimplemented!("int64_div_fast_to_numeric: crate::utils::adt::numeric")
}
unsafe fn numeric_in(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("numeric_in: crate::utils::adt::numeric")
}
unsafe fn numeric_round(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("numeric_round: crate::utils::adt::numeric")
}
unsafe fn numeric_add_opt_error(_n1: Numeric, _n2: Numeric, _have_error: *mut bool) -> Numeric {
    unimplemented!("numeric_add_opt_error: crate::utils::adt::numeric")
}
unsafe fn numeric_sub_opt_error(_n1: Numeric, _n2: Numeric, _have_error: *mut bool) -> Numeric {
    unimplemented!("numeric_sub_opt_error: crate::utils::adt::numeric")
}
unsafe fn numeric_div_opt_error(_n1: Numeric, _n2: Numeric, _have_error: *mut bool) -> Numeric {
    unimplemented!("numeric_div_opt_error: crate::utils::adt::numeric")
}

// --- int8 hashing (TODO(pg-port): crate::utils::adt::int8) ---
unsafe fn hashint8(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("hashint8: crate::utils::adt::int8")
}
unsafe fn hashint8extended(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!("hashint8extended: crate::utils::adt::int8")
}
#[allow(non_snake_case)]
fn Int64GetDatumFast(X: int64) -> Datum {
    Int64GetDatum(X)
}

// --- datetime.h helpers not yet exported from datetime.rs ---
unsafe fn DecodeUnits(_field: c_int, _lowtoken: *const c_char, _val: *mut c_int) -> c_int {
    unimplemented!("DecodeUnits: crate::utils::adt::datetime")
}
unsafe fn DecodeSpecial(_field: c_int, _lowtoken: *const c_char, _val: *mut c_int) -> c_int {
    unimplemented!("DecodeSpecial: crate::utils::adt::datetime")
}
unsafe fn date2j(_year: c_int, _month: c_int, _day: c_int) -> c_int {
    unimplemented!("date2j: crate::utils::adt::datetime")
}
unsafe fn j2day(_date: c_int) -> c_int {
    unimplemented!("j2day: crate::utils::adt::datetime")
}
unsafe fn ValidateDate(
    _fmask: c_int,
    _isjulian: bool,
    _is2digits: bool,
    _bc: bool,
    _tm: *mut pg_tm,
) -> c_int {
    unimplemented!("ValidateDate: crate::utils::adt::datetime")
}
unsafe fn EncodeDateTime(
    _tm: *mut pg_tm,
    _fsec: fsec_t,
    _print_tz: bool,
    _tz: c_int,
    _tzn: *const c_char,
    _style: c_int,
    _str: *mut c_char,
) {
    unimplemented!("EncodeDateTime: crate::utils::adt::datetime")
}
unsafe fn DetermineTimeZoneOffset(_tm: *mut pg_tm, _tzp: *mut pg_tz) -> c_int {
    unimplemented!("DetermineTimeZoneOffset: crate::utils::adt::datetime")
}
unsafe fn DetermineTimeZoneAbbrevOffset(
    _tm: *mut pg_tm,
    _abbr: *const c_char,
    _tzp: *mut pg_tz,
) -> c_int {
    unimplemented!("DetermineTimeZoneAbbrevOffset: crate::utils::adt::datetime")
}
unsafe fn DetermineTimeZoneAbbrevOffsetTS(
    _ts: TimestampTz,
    _abbr: *const c_char,
    _tzp: *mut pg_tz,
    _isdst: *mut c_int,
) -> c_int {
    unimplemented!("DetermineTimeZoneAbbrevOffsetTS: crate::utils::adt::datetime")
}
unsafe fn DecodeTimezoneNameToTz(_tzname: *const c_char) -> *mut pg_tz {
    unimplemented!("DecodeTimezoneNameToTz: crate::utils::adt::datetime")
}
unsafe fn itmin2interval(itm_in: *const pg_itm_in, span: *mut Interval) -> c_int {
    // forward to the real impl defined in this file
    itmin2interval_impl(itm_in, span)
}
unsafe fn ParseDateTime(
    _str: *const c_char,
    _workbuf: *mut c_char,
    _buflen: Size,
    _field: *mut *mut c_char,
    _ftype: *mut c_int,
    _maxfields: c_int,
    _numfields: *mut c_int,
) -> c_int {
    unimplemented!("ParseDateTime: crate::utils::adt::datetime")
}
unsafe fn DecodeDateTime(
    _field: *mut *mut c_char,
    _ftype: *const c_int,
    _nf: c_int,
    _dtype: *mut c_int,
    _tm: *mut pg_tm,
    _fsec: *mut fsec_t,
    _tzp: *mut c_int,
    _extra: *mut DateTimeErrorExtra,
) -> c_int {
    unimplemented!("DecodeDateTime: crate::utils::adt::datetime")
}
unsafe fn DateTimeParseError(
    _dterr: c_int,
    _extra: *const DateTimeErrorExtra,
    _str: *const c_char,
    _datatype: *const c_char,
    _escontext: *mut c_void,
) {
    unimplemented!("DateTimeParseError: crate::utils::adt::datetime")
}

// --- parser/scansup.h ---
unsafe fn downcase_truncate_identifier(
    _ident: *const c_char,
    _len: c_int,
    _warn: bool,
) -> *mut c_char {
    unimplemented!("downcase_truncate_identifier: crate::parser::scansup")
}

// --- varlena/format helpers ---
unsafe fn text_to_cstring_buffer(_src: *const text, _dst: *mut c_char, _dstlen: Size) {
    unimplemented!("text_to_cstring_buffer: crate::utils::adt::varlena")
}
unsafe fn format_type_be(_typid: Oid) -> *mut c_char {
    unimplemented!("format_type_be: crate::utils::adt::format_type")
}
unsafe fn pg_get_timezone_name(_tz: *mut pg_tz) -> *const c_char {
    unimplemented!("pg_get_timezone_name: crate::utils::adt::datetime")
}
unsafe fn pg_get_timezone_offset(_tz: *mut pg_tz, _offset: *mut std::os::raw::c_long) -> bool {
    unimplemented!("pg_get_timezone_offset: crate::pgtime")
}
// pstrdup is provided by crate::prelude (crate::utils::palloc).

// --- pgtime helpers (TODO(pg-port): crate::pgtime / crate::pgtz) ---
pub type pg_time_t = i64;
unsafe fn pg_localtime(_timep: *const pg_time_t, _tz: *mut pg_tz) -> *mut pg_tm {
    unimplemented!("pg_localtime: crate::pgtime")
}
unsafe fn pg_gmtime(_timep: *const pg_time_t) -> *mut pg_tm {
    unimplemented!("pg_gmtime: crate::pgtime")
}
unsafe fn pg_strftime(
    _s: *mut c_char,
    _maxsize: Size,
    _format: *const c_char,
    _tm: *mut pg_tm,
) -> Size {
    unimplemented!("pg_strftime: crate::pgtime")
}

#[repr(C)]
struct timeval {
    tv_sec: i64,
    tv_usec: i64,
}
unsafe fn gettimeofday(_tp: *mut timeval, _tz: *mut c_void) -> c_int {
    unimplemented!("gettimeofday: libc")
}

// --- access/xact.h (TODO(pg-port): crate::access::transam::xact) ---
pub unsafe fn GetCurrentStatementStartTimestamp() -> TimestampTz {
    unimplemented!("GetCurrentStatementStartTimestamp: crate::access::transam::xact")
}

// --- funcapi.h SRF / aggregate plumbing (TODO(pg-port): crate::funcapi) ---
#[repr(C)]
pub struct FuncCallContext {
    pub user_fctx: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
}
unsafe fn SRF_IS_FIRSTCALL() -> bool {
    unimplemented!("SRF_IS_FIRSTCALL: crate::funcapi")
}
unsafe fn SRF_FIRSTCALL_INIT() -> *mut FuncCallContext {
    unimplemented!("SRF_FIRSTCALL_INIT: crate::funcapi")
}
unsafe fn SRF_PERCALL_SETUP() -> *mut FuncCallContext {
    unimplemented!("SRF_PERCALL_SETUP: crate::funcapi")
}
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!("SRF_RETURN_NEXT: crate::funcapi")
}
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!("SRF_RETURN_DONE: crate::funcapi")
}
unsafe fn AggCheckCallContext(
    _fcinfo: FunctionCallInfo,
    _aggcontext: *mut MemoryContext,
) -> bool {
    unimplemented!("AggCheckCallContext: crate::executor::nodeAgg")
}

// --- utils/array.h (TODO(pg-port): crate::utils::adt::array) ---
pub type ArrayType = c_void;
unsafe fn ArrayGetIntegerTypmods(_arr: *mut ArrayType, _n: *mut c_int) -> *mut int32 {
    unimplemented!("ArrayGetIntegerTypmods: crate::utils::adt::arrayutils")
}
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut ArrayType
    };
}

// --- nodes/supportnodes.h & optimizer (TODO(pg-port)) ---
pub type Node = c_void;
pub type List = c_void;
#[repr(C)]
pub struct SupportRequestSimplify {
    pub fcall: *mut c_void, /* FuncExpr* */
}
#[repr(C)]
pub struct SupportRequestRows {
    pub root: *mut c_void,
    pub node: *mut Node,
    pub rows: f64,
}
unsafe fn TemporalSimplify(_max_precis: int32, _node: *mut Node) -> *mut Node {
    unimplemented!("TemporalSimplify: crate::utils::adt::date")
}
unsafe fn exprTypmod(_expr: *mut Node) -> int32 {
    unimplemented!("exprTypmod: crate::nodes::nodeFuncs")
}
unsafe fn relabel_to_typmod(_expr: *mut Node, _typmod: int32) -> *mut Node {
    unimplemented!("relabel_to_typmod: crate::optimizer")
}
unsafe fn estimate_expression_value(_root: *mut c_void, _node: *mut Node) -> *mut Node {
    unimplemented!("estimate_expression_value: crate::optimizer")
}
unsafe fn is_funcclause(_node: *mut Node) -> bool {
    unimplemented!("is_funcclause: crate::nodes::nodeFuncs")
}
#[repr(C)]
pub struct FuncExpr {
    pub args: *mut List,
}
#[repr(C)]
pub struct Const {
    pub constvalue: Datum,
    pub constisnull: bool,
}
unsafe fn list_length(_l: *mut List) -> c_int {
    unimplemented!("list_length: crate::nodes::pg_list")
}
unsafe fn linitial(_l: *mut List) -> *mut c_void {
    unimplemented!("linitial: crate::nodes::pg_list")
}
unsafe fn lsecond(_l: *mut List) -> *mut c_void {
    unimplemented!("lsecond: crate::nodes::pg_list")
}
unsafe fn lthird(_l: *mut List) -> *mut c_void {
    unimplemented!("lthird: crate::nodes::pg_list")
}

// --- utils/sortsupport.h & skipsupport.h (TODO(pg-port)) ---
pub type Relation = *mut c_void;
#[repr(C)]
pub struct SortSupportData {
    pub comparator: Option<unsafe extern "C" fn(Datum, Datum, *mut SortSupportData) -> c_int>,
}
pub type SortSupport = *mut SortSupportData;
unsafe extern "C" fn ssup_datum_signed_cmp(
    _x: Datum,
    _y: Datum,
    _ssup: *mut SortSupportData,
) -> c_int {
    unimplemented!("ssup_datum_signed_cmp: crate::utils::sortsupport")
}
#[repr(C)]
pub struct SkipSupportData {
    pub decrement: Option<unsafe fn(Relation, Datum, *mut bool) -> Datum>,
    pub increment: Option<unsafe fn(Relation, Datum, *mut bool) -> Datum>,
    pub low_elem: Datum,
    pub high_elem: Datum,
}
pub type SkipSupport = *mut SkipSupportData;

pub type bytea = crate::c::bytea;

// ===========================================================================
//                  end of stubs; faithful translation follows
// ===========================================================================

/*
 * gcc's -ffast-math switch breaks routines that expect exact results from
 * expressions like timeval / SECS_PER_HOUR, where timeval is double.
 */

/* Set at postmaster start */
#[allow(non_upper_case_globals)]
pub static mut PgStartTime: TimestampTz = 0;

/* Set at configuration reload */
#[allow(non_upper_case_globals)]
pub static mut PgReloadTime: TimestampTz = 0;

#[repr(C)]
struct generate_series_timestamp_fctx {
    current: Timestamp,
    finish: Timestamp,
    step: Interval,
    step_sign: c_int,
}

#[repr(C)]
struct generate_series_timestamptz_fctx {
    current: TimestampTz,
    finish: TimestampTz,
    step: Interval,
    step_sign: c_int,
    attimezone: *mut pg_tz,
}

/*
 * The transition datatype for interval aggregates is declared as internal.
 * It's a pointer to an IntervalAggState allocated in the aggregate context.
 */
#[repr(C)]
struct IntervalAggState {
    N: int64,        /* count of finite intervals processed */
    sumX: Interval,  /* sum of finite intervals processed */
    /* These counts are *not* included in N!  Use IA_TOTAL_COUNT() as needed */
    pInfcount: int64, /* count of +infinity intervals */
    nInfcount: int64, /* count of -infinity intervals */
}

#[inline]
unsafe fn IA_TOTAL_COUNT(ia: *const IntervalAggState) -> int64 {
    (*ia).N + (*ia).pInfcount + (*ia).nInfcount
}

/* common code for timestamptypmodin and timestamptztypmodin */
unsafe fn anytimestamp_typmodin(istz: bool, ta: *mut ArrayType) -> int32 {
    let tl: *mut int32;
    let mut n: c_int = 0;

    tl = ArrayGetIntegerTypmods(ta, &mut n);

    /*
     * we're not too tense about good error message here because grammar
     * shouldn't allow wrong number of modifiers for TIMESTAMP
     */
    if n != 1 {
        ereport!(ERROR, errmsg!("invalid type modifier"));
    }

    anytimestamp_typmod_check(istz, *tl)
}

/* exported so parse_expr.c can use it */
pub unsafe fn anytimestamp_typmod_check(istz: bool, mut typmod: int32) -> int32 {
    if typmod < 0 {
        ereport!(
            ERROR,
            errmsg!(
                "TIMESTAMP({}){} precision must not be negative",
                typmod,
                if istz { " WITH TIME ZONE" } else { "" }
            )
        );
    }
    if typmod > MAX_TIMESTAMP_PRECISION {
        ereport!(
            WARNING,
            errmsg!(
                "TIMESTAMP({}){} precision reduced to maximum allowed, {}",
                typmod,
                if istz { " WITH TIME ZONE" } else { "" },
                MAX_TIMESTAMP_PRECISION
            )
        );
        typmod = MAX_TIMESTAMP_PRECISION;
    }

    typmod
}

/* common code for timestamptypmodout and timestamptztypmodout */
unsafe fn anytimestamp_typmodout(istz: bool, typmod: int32) -> *mut c_char {
    let tz = if istz {
        c" with time zone".as_ptr()
    } else {
        c" without time zone".as_ptr()
    };

    if typmod >= 0 {
        psprintf_paren(typmod, tz)
    } else {
        pstrdup(tz)
    }
}

// psprintf("(%d)%s", (int) typmod, tz) helper (psprintf is variadic in C).
unsafe fn psprintf_paren(typmod: int32, tz: *const c_char) -> *mut c_char {
    let tzs = std::ffi::CStr::from_ptr(tz).to_string_lossy();
    let s = format!("({}){}\0", typmod, tzs);
    let bytes = s.into_bytes();
    let p = palloc(bytes.len() as Size) as *mut u8;
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), p, bytes.len());
    p as *mut c_char
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/

/* timestamp_in()
 * Convert a string to internal form.
 */
pub unsafe fn timestamp_in(fcinfo: FunctionCallInfo) -> Datum {
    let str = PG_GETARG_CSTRING!(fcinfo, 0);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let escontext = (*fcinfo).context;
    let mut result: Timestamp = 0;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut tz: c_int = 0;
    let mut dtype: c_int = 0;
    let mut nf: c_int = 0;
    let mut dterr: c_int;
    let mut field: [*mut c_char; MAXDATEFIELDS] = [null_mut(); MAXDATEFIELDS];
    let mut ftype: [c_int; MAXDATEFIELDS] = [0; MAXDATEFIELDS];
    let mut workbuf: [c_char; MAXDATELEN + MAXDATEFIELDS] = [0; MAXDATELEN + MAXDATEFIELDS];
    let mut extra: DateTimeErrorExtra = std::mem::zeroed();

    dterr = ParseDateTime(
        str,
        workbuf.as_mut_ptr(),
        std::mem::size_of_val(&workbuf) as Size,
        field.as_mut_ptr(),
        ftype.as_mut_ptr(),
        MAXDATEFIELDS as c_int,
        &mut nf,
    );
    if dterr == 0 {
        dterr = DecodeDateTime(
            field.as_mut_ptr(),
            ftype.as_ptr(),
            nf,
            &mut dtype,
            tm,
            &mut fsec,
            &mut tz,
            &mut extra,
        );
    }
    if dterr != 0 {
        DateTimeParseError(dterr, &extra, str, c"timestamp".as_ptr(), escontext as *mut c_void);
        PG_RETURN_NULL!(fcinfo);
    }

    match dtype {
        d if d == DTK_DATE => {
            if tm2timestamp(tm, fsec, null_mut(), &mut result) != 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "timestamp out of range: \"{}\"",
                        std::ffi::CStr::from_ptr(str).to_string_lossy()
                    )
                );
                return 0 as Datum;
            }
        }
        d if d == DTK_EPOCH => {
            result = SetEpochTimestamp();
        }
        d if d == DTK_LATE => {
            TIMESTAMP_NOEND(&mut result);
        }
        d if d == DTK_EARLY => {
            TIMESTAMP_NOBEGIN(&mut result);
        }
        _ => {
            elog!(
                ERROR,
                "unexpected dtype {} while parsing timestamp \"{}\"",
                dtype,
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            );
            TIMESTAMP_NOEND(&mut result);
        }
    }

    AdjustTimestampForTypmod(&mut result, typmod, escontext as *mut Node);

    PG_RETURN_TIMESTAMP!(result);
}

/* timestamp_out()
 * Convert a timestamp to external form.
 */
pub unsafe fn timestamp_out(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let result: *mut c_char;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut fsec: fsec_t = 0;
    let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

    if TIMESTAMP_NOT_FINITE(timestamp) {
        EncodeSpecialTimestamp(timestamp, buf.as_mut_ptr());
    } else if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) == 0 {
        EncodeDateTime(tm, fsec, false, 0, null(), DateStyle, buf.as_mut_ptr());
    } else {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    result = pstrdup(buf.as_ptr());
    PG_RETURN_CSTRING!(result);
}

/*
 *		timestamp_recv			- converts external binary format to timestamp
 */
pub unsafe fn timestamp_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let mut timestamp: Timestamp;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut fsec: fsec_t = 0;

    timestamp = pq_getmsgint64(buf) as Timestamp;

    /* range check: see if timestamp_out would like it */
    if TIMESTAMP_NOT_FINITE(timestamp) {
        /* ok */
    } else if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) != 0
        || !IS_VALID_TIMESTAMP(timestamp)
    {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    AdjustTimestampForTypmod(&mut timestamp, typmod, null_mut());

    PG_RETURN_TIMESTAMP!(timestamp);
}

/*
 *		timestamp_send			- converts timestamp to binary format
 */
pub unsafe fn timestamp_send(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint64(&mut buf, timestamp as uint64);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

pub unsafe fn timestamptypmodin(fcinfo: FunctionCallInfo) -> Datum {
    let ta = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    PG_RETURN_INT32!(anytimestamp_typmodin(false, ta));
}

pub unsafe fn timestamptypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_CSTRING!(anytimestamp_typmodout(false, typmod));
}

/*
 * timestamp_support()
 *
 * Planner support function for the timestamp_scale() and timestamptz_scale()
 * length coercion functions (we need not distinguish them here).
 */
pub unsafe fn timestamp_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = null_mut();

    if IsA!(rawreq, T_SupportRequestSimplify) {
        let req = rawreq as *mut SupportRequestSimplify;

        ret = TemporalSimplify(MAX_TIMESTAMP_PRECISION, (*req).fcall as *mut Node);
    }

    PG_RETURN_POINTER!(ret);
}

/* timestamp_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
pub unsafe fn timestamp_scale(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: Timestamp;

    result = timestamp;

    AdjustTimestampForTypmod(&mut result, typmod, null_mut());

    PG_RETURN_TIMESTAMP!(result);
}

/*
 * AdjustTimestampForTypmod --- round off a timestamp to suit given typmod
 * Works for either timestamp or timestamptz.
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
pub unsafe fn AdjustTimestampForTypmod(
    time: *mut Timestamp,
    typmod: int32,
    escontext: *mut Node,
) -> bool {
    static TIMESTAMP_SCALES: [int64; (MAX_TIMESTAMP_PRECISION + 1) as usize] =
        [1000000, 100000, 10000, 1000, 100, 10, 1];

    static TIMESTAMP_OFFSETS: [int64; (MAX_TIMESTAMP_PRECISION + 1) as usize] =
        [500000, 50000, 5000, 500, 50, 5, 0];

    if !TIMESTAMP_NOT_FINITE(*time) && (typmod != -1) && (typmod != MAX_TIMESTAMP_PRECISION) {
        if typmod < 0 || typmod > MAX_TIMESTAMP_PRECISION {
            ereport!(
                ERROR,
                errmsg!(
                    "timestamp({}) precision must be between {} and {}",
                    typmod,
                    0,
                    MAX_TIMESTAMP_PRECISION
                )
            );
            return false;
        }

        if *time >= 0 {
            *time = ((*time + TIMESTAMP_OFFSETS[typmod as usize]) / TIMESTAMP_SCALES[typmod as usize])
                * TIMESTAMP_SCALES[typmod as usize];
        } else {
            *time = -((((-*time) + TIMESTAMP_OFFSETS[typmod as usize])
                / TIMESTAMP_SCALES[typmod as usize])
                * TIMESTAMP_SCALES[typmod as usize]);
        }
    }

    true
}

/* timestamptz_in()
 * Convert a string to internal form.
 */
pub unsafe fn timestamptz_in(fcinfo: FunctionCallInfo) -> Datum {
    let str = PG_GETARG_CSTRING!(fcinfo, 0);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let escontext = (*fcinfo).context;
    let mut result: TimestampTz = 0;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut tz: c_int = 0;
    let mut dtype: c_int = 0;
    let mut nf: c_int = 0;
    let mut dterr: c_int;
    let mut field: [*mut c_char; MAXDATEFIELDS] = [null_mut(); MAXDATEFIELDS];
    let mut ftype: [c_int; MAXDATEFIELDS] = [0; MAXDATEFIELDS];
    let mut workbuf: [c_char; MAXDATELEN + MAXDATEFIELDS] = [0; MAXDATELEN + MAXDATEFIELDS];
    let mut extra: DateTimeErrorExtra = std::mem::zeroed();

    dterr = ParseDateTime(
        str,
        workbuf.as_mut_ptr(),
        std::mem::size_of_val(&workbuf) as Size,
        field.as_mut_ptr(),
        ftype.as_mut_ptr(),
        MAXDATEFIELDS as c_int,
        &mut nf,
    );
    if dterr == 0 {
        dterr = DecodeDateTime(
            field.as_mut_ptr(),
            ftype.as_ptr(),
            nf,
            &mut dtype,
            tm,
            &mut fsec,
            &mut tz,
            &mut extra,
        );
    }
    if dterr != 0 {
        DateTimeParseError(
            dterr,
            &extra,
            str,
            c"timestamp with time zone".as_ptr(),
            escontext as *mut c_void,
        );
        PG_RETURN_NULL!(fcinfo);
    }

    match dtype {
        d if d == DTK_DATE => {
            if tm2timestamp(tm, fsec, &mut tz, &mut result) != 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "timestamp out of range: \"{}\"",
                        std::ffi::CStr::from_ptr(str).to_string_lossy()
                    )
                );
                return 0 as Datum;
            }
        }
        d if d == DTK_EPOCH => {
            result = SetEpochTimestamp();
        }
        d if d == DTK_LATE => {
            TIMESTAMP_NOEND(&mut result);
        }
        d if d == DTK_EARLY => {
            TIMESTAMP_NOBEGIN(&mut result);
        }
        _ => {
            elog!(
                ERROR,
                "unexpected dtype {} while parsing timestamptz \"{}\"",
                dtype,
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            );
            TIMESTAMP_NOEND(&mut result);
        }
    }

    AdjustTimestampForTypmod(&mut result, typmod, escontext as *mut Node);

    PG_RETURN_TIMESTAMPTZ!(result);
}

/*
 * Try to parse a timezone specification, and return its timezone offset value
 * if it's acceptable.  Otherwise, an error is thrown.
 *
 * Note: some code paths update tm->tm_isdst, and some don't; current callers
 * don't care, so we don't bother being consistent.
 */
unsafe fn parse_sane_timezone(tm: *mut pg_tm, zone: *mut text) -> c_int {
    let mut tzname: [c_char; TZ_STRLEN_MAX + 1] = [0; TZ_STRLEN_MAX + 1];
    let dterr: c_int;
    let mut tz: c_int = 0;

    text_to_cstring_buffer(zone, tzname.as_mut_ptr(), std::mem::size_of_val(&tzname) as Size);

    /*
     * Look up the requested timezone.  First we try to interpret it as a
     * numeric timezone specification; if DecodeTimezone decides it doesn't
     * like the format, we try timezone abbreviations and names.
     *
     * Note pg_tzset happily parses numeric input that DecodeTimezone would
     * reject.  To avoid having it accept input that would otherwise be seen
     * as invalid, it's enough to disallow having a digit in the first
     * position of our input string.
     */
    if (tzname[0] as u8 as char).is_ascii_digit() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                "numeric time zone",
                std::ffi::CStr::from_ptr(tzname.as_ptr()).to_string_lossy()
            )
        );
    }

    dterr = DecodeTimezone(tzname.as_ptr(), &mut tz);
    if dterr != 0 {
        let r#type: c_int;
        let mut val: c_int = 0;
        let mut tzp: *mut pg_tz = null_mut();

        if dterr == DTERR_TZDISP_OVERFLOW {
            ereport!(
                ERROR,
                errmsg!(
                    "numeric time zone \"{}\" out of range",
                    std::ffi::CStr::from_ptr(tzname.as_ptr()).to_string_lossy()
                )
            );
        } else if dterr != DTERR_BAD_FORMAT {
            ereport!(
                ERROR,
                errmsg!(
                    "time zone \"{}\" not recognized",
                    std::ffi::CStr::from_ptr(tzname.as_ptr()).to_string_lossy()
                )
            );
        }

        r#type = DecodeTimezoneName(tzname.as_ptr(), &mut val, &mut tzp);

        if r#type == TZNAME_FIXED_OFFSET {
            /* fixed-offset abbreviation */
            tz = -val;
        } else if r#type == TZNAME_DYNTZ {
            /* dynamic-offset abbreviation, resolve using specified time */
            tz = DetermineTimeZoneAbbrevOffset(tm, tzname.as_ptr(), tzp);
        } else {
            /* full zone name */
            tz = DetermineTimeZoneOffset(tm, tzp);
        }
    }

    tz
}

/*
 * Look up the requested timezone, returning a pg_tz struct.
 *
 * This is the same as DecodeTimezoneNameToTz, but starting with a text Datum.
 */
unsafe fn lookup_timezone(zone: *mut text) -> *mut pg_tz {
    let mut tzname: [c_char; TZ_STRLEN_MAX + 1] = [0; TZ_STRLEN_MAX + 1];

    text_to_cstring_buffer(zone, tzname.as_mut_ptr(), std::mem::size_of_val(&tzname) as Size);

    DecodeTimezoneNameToTz(tzname.as_ptr())
}

/*
 * make_timestamp_internal
 *		workhorse for make_timestamp and make_timestamptz
 */
unsafe fn make_timestamp_internal(
    year: c_int,
    month: c_int,
    day: c_int,
    hour: c_int,
    min: c_int,
    sec: f64,
) -> Timestamp {
    let mut tm: pg_tm = std::mem::zeroed();
    let date: TimeOffset;
    let time: TimeOffset;
    let dterr: c_int;
    let mut bc: bool = false;
    let mut result: Timestamp = 0;

    tm.tm_year = year;
    tm.tm_mon = month;
    tm.tm_mday = day;

    /* Handle negative years as BC */
    if tm.tm_year < 0 {
        bc = true;
        tm.tm_year = -tm.tm_year;
    }

    dterr = ValidateDate(DTK_DATE_M, false, false, bc, &mut tm);

    if dterr != 0 {
        ereport!(
            ERROR,
            errmsg!("date field value out of range: {}-{:02}-{:02}", year, month, day)
        );
    }

    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        ereport!(
            ERROR,
            errmsg!("date out of range: {}-{:02}-{:02}", year, month, day)
        );
    }

    date = (date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE) as TimeOffset;

    /* Check for time overflow */
    if float_time_overflows(hour, min, sec) {
        ereport!(
            ERROR,
            errmsg!("time field value out of range: {}:{:02}:{:02}", hour, min, sec)
        );
    }

    /* This should match tm2time */
    time = (((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE) as int64 * USECS_PER_SEC)
        + rint(sec * USECS_PER_SEC as f64) as int64;

    if pg_mul_s64_overflow(date, USECS_PER_DAY, &mut result)
        || pg_add_s64_overflow(result, time, &mut result)
    {
        ereport!(
            ERROR,
            errmsg!(
                "timestamp out of range: {}-{:02}-{:02} {}:{:02}:{:02}",
                year, month, day, hour, min, sec
            )
        );
    }

    /* final range check catches just-out-of-range timestamps */
    if !IS_VALID_TIMESTAMP(result) {
        ereport!(
            ERROR,
            errmsg!(
                "timestamp out of range: {}-{:02}-{:02} {}:{:02}:{:02}",
                year, month, day, hour, min, sec
            )
        );
    }

    result
}

/*
 * make_timestamp() - timestamp constructor
 */
pub unsafe fn make_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let year: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let month: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mday: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let hour: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let min: int32 = PG_GETARG_INT32!(fcinfo, 4);
    let sec: float8 = PG_GETARG_FLOAT8!(fcinfo, 5);
    let result: Timestamp;

    result = make_timestamp_internal(year, month, mday, hour, min, sec);

    PG_RETURN_TIMESTAMP!(result);
}

/*
 * make_timestamptz() - timestamp with time zone constructor
 */
pub unsafe fn make_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let year: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let month: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mday: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let hour: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let min: int32 = PG_GETARG_INT32!(fcinfo, 4);
    let sec: float8 = PG_GETARG_FLOAT8!(fcinfo, 5);
    let result: Timestamp;

    result = make_timestamp_internal(year, month, mday, hour, min, sec);

    PG_RETURN_TIMESTAMPTZ!(timestamp2timestamptz(result));
}

/*
 * Construct a timestamp with time zone.
 *		As above, but the time zone is specified as seventh argument.
 */
pub unsafe fn make_timestamptz_at_timezone(fcinfo: FunctionCallInfo) -> Datum {
    let year: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let month: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mday: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let hour: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let min: int32 = PG_GETARG_INT32!(fcinfo, 4);
    let sec: float8 = PG_GETARG_FLOAT8!(fcinfo, 5);
    let zone = PG_GETARG_TEXT_PP!(fcinfo, 6);
    let result: TimestampTz;
    let timestamp: Timestamp;
    let mut tt: pg_tm = std::mem::zeroed();
    let tz: c_int;
    let mut fsec: fsec_t = 0;

    timestamp = make_timestamp_internal(year, month, mday, hour, min, sec);

    if timestamp2tm(timestamp, null_mut(), &mut tt, &mut fsec, null_mut(), null_mut()) != 0 {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    tz = parse_sane_timezone(&mut tt, zone);

    result = dt2local(timestamp, -tz);

    if !IS_VALID_TIMESTAMP(result) {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    PG_RETURN_TIMESTAMPTZ!(result);
}

/*
 * to_timestamp(double precision)
 * Convert UNIX epoch to timestamptz.
 */
pub unsafe fn float8_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let mut seconds: float8 = PG_GETARG_FLOAT8!(fcinfo, 0);
    let mut result: TimestampTz = 0;

    /* Deal with NaN and infinite inputs ... */
    if isnan(seconds) != 0 {
        ereport!(ERROR, errmsg!("timestamp cannot be NaN"));
    }

    if isinf(seconds) != 0 {
        if seconds < 0.0 {
            TIMESTAMP_NOBEGIN(&mut result);
        } else {
            TIMESTAMP_NOEND(&mut result);
        }
    } else {
        /* Out of range? */
        if seconds < (SECS_PER_DAY as float8) * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE) as float8
            || seconds
                >= (SECS_PER_DAY as float8) * (TIMESTAMP_END_JULIAN - UNIX_EPOCH_JDATE) as float8
        {
            ereport!(ERROR, errmsg!("timestamp out of range: \"{}\"", seconds));
        }

        /* Convert UNIX epoch to Postgres epoch */
        seconds -= ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) as float8;

        seconds = rint(seconds * USECS_PER_SEC as f64);
        result = seconds as int64;

        /* Recheck in case roundoff produces something just out of range */
        if !IS_VALID_TIMESTAMP(result) {
            ereport!(
                ERROR,
                errmsg!("timestamp out of range: \"{}\"", PG_GETARG_FLOAT8!(fcinfo, 0))
            );
        }
    }

    PG_RETURN_TIMESTAMP!(result);
}

/* timestamptz_out()
 * Convert a timestamp to external form.
 */
pub unsafe fn timestamptz_out(fcinfo: FunctionCallInfo) -> Datum {
    let dt = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let result: *mut c_char;
    let mut tz: c_int = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut fsec: fsec_t = 0;
    let mut tzn: *const c_char = null();
    let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

    if TIMESTAMP_NOT_FINITE(dt) {
        EncodeSpecialTimestamp(dt, buf.as_mut_ptr());
    } else if timestamp2tm(dt, &mut tz, tm, &mut fsec, &mut tzn, null_mut()) == 0 {
        EncodeDateTime(tm, fsec, true, tz, tzn, DateStyle, buf.as_mut_ptr());
    } else {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    result = pstrdup(buf.as_ptr());
    PG_RETURN_CSTRING!(result);
}

/*
 *		timestamptz_recv			- converts external binary format to timestamptz
 */
pub unsafe fn timestamptz_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let mut timestamp: TimestampTz;
    let mut tz: c_int = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut fsec: fsec_t = 0;

    timestamp = pq_getmsgint64(buf) as TimestampTz;

    /* range check: see if timestamptz_out would like it */
    if TIMESTAMP_NOT_FINITE(timestamp) {
        /* ok */
    } else if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), null_mut()) != 0
        || !IS_VALID_TIMESTAMP(timestamp)
    {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    AdjustTimestampForTypmod(&mut timestamp, typmod, null_mut());

    PG_RETURN_TIMESTAMPTZ!(timestamp);
}

/*
 *		timestamptz_send			- converts timestamptz to binary format
 */
pub unsafe fn timestamptz_send(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint64(&mut buf, timestamp as uint64);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

pub unsafe fn timestamptztypmodin(fcinfo: FunctionCallInfo) -> Datum {
    let ta = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    PG_RETURN_INT32!(anytimestamp_typmodin(true, ta));
}

pub unsafe fn timestamptztypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_CSTRING!(anytimestamp_typmodout(true, typmod));
}

/* timestamptz_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
pub unsafe fn timestamptz_scale(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: TimestampTz;

    result = timestamp;

    AdjustTimestampForTypmod(&mut result, typmod, null_mut());

    PG_RETURN_TIMESTAMPTZ!(result);
}

/* interval_in()
 * Convert a string to internal form.
 *
 * External format(s):
 *	Uses the generic date/time parsing and decoding routines.
 */
pub unsafe fn interval_in(fcinfo: FunctionCallInfo) -> Datum {
    let str = PG_GETARG_CSTRING!(fcinfo, 0);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let escontext = (*fcinfo).context;
    let result: *mut Interval;
    let mut tt: pg_itm_in = std::mem::zeroed();
    let itm_in = &mut tt as *mut pg_itm_in;
    let mut dtype: c_int = 0;
    let mut nf: c_int = 0;
    let range: c_int;
    let mut dterr: c_int;
    let mut field: [*mut c_char; MAXDATEFIELDS] = [null_mut(); MAXDATEFIELDS];
    let mut ftype: [c_int; MAXDATEFIELDS] = [0; MAXDATEFIELDS];
    let mut workbuf: [c_char; 256] = [0; 256];
    let extra: DateTimeErrorExtra = std::mem::zeroed();

    (*itm_in).tm_year = 0;
    (*itm_in).tm_mon = 0;
    (*itm_in).tm_mday = 0;
    (*itm_in).tm_usec = 0;

    if typmod >= 0 {
        range = INTERVAL_RANGE(typmod);
    } else {
        range = INTERVAL_FULL_RANGE;
    }

    dterr = ParseDateTime(
        str,
        workbuf.as_mut_ptr(),
        std::mem::size_of_val(&workbuf) as Size,
        field.as_mut_ptr(),
        ftype.as_mut_ptr(),
        MAXDATEFIELDS as c_int,
        &mut nf,
    );
    if dterr == 0 {
        dterr = DecodeInterval(field.as_mut_ptr(), ftype.as_ptr(), nf, range, &mut dtype, itm_in);
    }

    /* if those functions think it's a bad format, try ISO8601 style */
    if dterr == DTERR_BAD_FORMAT {
        dterr = DecodeISO8601Interval(str, &mut dtype, itm_in);
    }

    if dterr != 0 {
        if dterr == DTERR_FIELD_OVERFLOW {
            dterr = DTERR_INTERVAL_OVERFLOW;
        }
        DateTimeParseError(dterr, &extra, str, c"interval".as_ptr(), escontext as *mut c_void);
        PG_RETURN_NULL!(fcinfo);
    }

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    match dtype {
        d if d == DTK_DELTA => {
            if itmin2interval(itm_in, result) != 0 {
                ereport!(ERROR, errmsg!("interval out of range"));
                return 0 as Datum;
            }
        }
        d if d == DTK_LATE => {
            INTERVAL_NOEND(result);
        }
        d if d == DTK_EARLY => {
            INTERVAL_NOBEGIN(result);
        }
        _ => {
            elog!(
                ERROR,
                "unexpected dtype {} while parsing interval \"{}\"",
                dtype,
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            );
        }
    }

    AdjustIntervalForTypmod(result, typmod, escontext as *mut Node);

    PG_RETURN_INTERVAL_P!(result);
}

/* interval_out()
 * Convert a time span to external form.
 */
pub unsafe fn interval_out(fcinfo: FunctionCallInfo) -> Datum {
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let result: *mut c_char;
    let mut tt: pg_itm = std::mem::zeroed();
    let itm = &mut tt as *mut pg_itm;
    let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

    if INTERVAL_NOT_FINITE(span) {
        EncodeSpecialInterval(span, buf.as_mut_ptr());
    } else {
        interval2itm(core::ptr::read(span), itm);
        EncodeInterval(itm, IntervalStyle, buf.as_mut_ptr());
    }

    result = pstrdup(buf.as_ptr());
    PG_RETURN_CSTRING!(result);
}

/*
 *		interval_recv			- converts external binary format to interval
 */
pub unsafe fn interval_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let interval: *mut Interval;

    interval = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    (*interval).time = pq_getmsgint64(buf);
    (*interval).day = pq_getmsgint(buf, std::mem::size_of_val(&(*interval).day) as c_int) as int32;
    (*interval).month =
        pq_getmsgint(buf, std::mem::size_of_val(&(*interval).month) as c_int) as int32;

    AdjustIntervalForTypmod(interval, typmod, null_mut());

    PG_RETURN_INTERVAL_P!(interval);
}

/*
 *		interval_send			- converts interval to binary format
 */
pub unsafe fn interval_send(fcinfo: FunctionCallInfo) -> Datum {
    let interval = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint64(&mut buf, (*interval).time as uint64);
    pq_sendint32(&mut buf, (*interval).day as u32);
    pq_sendint32(&mut buf, (*interval).month as u32);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

/*
 * The interval typmod stores a "range" in its high 16 bits and a "precision"
 * in its low 16 bits.  Both contribute to defining the resolution of the
 * type.  Range addresses resolution granules larger than one second, and
 * precision specifies resolution below one second.  This representation can
 * express all SQL standard resolutions, but we implement them all in terms of
 * truncating rightward from some position.  Range is a bitmap of permitted
 * fields, but only the temporally-smallest such field is significant to our
 * calculations.  Precision is a count of sub-second decimal places to retain.
 * Setting all bits (INTERVAL_FULL_PRECISION) gives the same truncation
 * semantics as choosing MAX_INTERVAL_PRECISION.
 */
pub unsafe fn intervaltypmodin(fcinfo: FunctionCallInfo) -> Datum {
    let ta = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);
    let tl: *mut int32;
    let mut n: c_int = 0;
    let typmod: int32;

    tl = ArrayGetIntegerTypmods(ta, &mut n);

    /*
     * tl[0] - interval range (fields bitmask)	tl[1] - precision (optional)
     *
     * Note we must validate tl[0] even though it's normally guaranteed
     * correct by the grammar --- consider SELECT 'foo'::"interval"(1000).
     */
    if n > 0 {
        let r = *tl;
        if r == INTERVAL_MASK(YEAR)
            || r == INTERVAL_MASK(MONTH)
            || r == INTERVAL_MASK(DAY)
            || r == INTERVAL_MASK(HOUR)
            || r == INTERVAL_MASK(MINUTE)
            || r == INTERVAL_MASK(SECOND)
            || r == INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)
            || r == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR)
            || r == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)
            || r == INTERVAL_MASK(DAY)
                | INTERVAL_MASK(HOUR)
                | INTERVAL_MASK(MINUTE)
                | INTERVAL_MASK(SECOND)
            || r == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)
            || r == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)
            || r == INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)
            || r == INTERVAL_FULL_RANGE
        {
            /* all OK */
        } else {
            ereport!(ERROR, errmsg!("invalid INTERVAL type modifier"));
        }
    }

    if n == 1 {
        if *tl != INTERVAL_FULL_RANGE {
            typmod = INTERVAL_TYPMOD(INTERVAL_FULL_PRECISION, *tl);
        } else {
            typmod = -1;
        }
    } else if n == 2 {
        if *tl.add(1) < 0 {
            ereport!(
                ERROR,
                errmsg!("INTERVAL({}) precision must not be negative", *tl.add(1))
            );
        }
        if *tl.add(1) > MAX_INTERVAL_PRECISION {
            ereport!(
                WARNING,
                errmsg!(
                    "INTERVAL({}) precision reduced to maximum allowed, {}",
                    *tl.add(1),
                    MAX_INTERVAL_PRECISION
                )
            );
            typmod = INTERVAL_TYPMOD(MAX_INTERVAL_PRECISION, *tl);
        } else {
            typmod = INTERVAL_TYPMOD(*tl.add(1), *tl);
        }
    } else {
        ereport!(ERROR, errmsg!("invalid INTERVAL type modifier"));
        typmod = 0; /* keep compiler quiet */
    }

    PG_RETURN_INT32!(typmod);
}

pub unsafe fn intervaltypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let res = palloc(64) as *mut c_char;
    let fields: c_int;
    let precision: c_int;
    let fieldstr: *const c_char;

    if typmod < 0 {
        *res = b'\0' as c_char;
        PG_RETURN_CSTRING!(res);
    }

    fields = INTERVAL_RANGE(typmod);
    precision = INTERVAL_PRECISION(typmod);

    fieldstr = if fields == INTERVAL_MASK(YEAR) {
        c" year".as_ptr()
    } else if fields == INTERVAL_MASK(MONTH) {
        c" month".as_ptr()
    } else if fields == INTERVAL_MASK(DAY) {
        c" day".as_ptr()
    } else if fields == INTERVAL_MASK(HOUR) {
        c" hour".as_ptr()
    } else if fields == INTERVAL_MASK(MINUTE) {
        c" minute".as_ptr()
    } else if fields == INTERVAL_MASK(SECOND) {
        c" second".as_ptr()
    } else if fields == INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH) {
        c" year to month".as_ptr()
    } else if fields == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) {
        c" day to hour".as_ptr()
    } else if fields == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) {
        c" day to minute".as_ptr()
    } else if fields
        == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)
    {
        c" day to second".as_ptr()
    } else if fields == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) {
        c" hour to minute".as_ptr()
    } else if fields == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND) {
        c" hour to second".as_ptr()
    } else if fields == INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND) {
        c" minute to second".as_ptr()
    } else if fields == INTERVAL_FULL_RANGE {
        c"".as_ptr()
    } else {
        elog!(ERROR, "invalid INTERVAL typmod: 0x{:x}", typmod);
        c"".as_ptr()
    };

    if precision != INTERVAL_FULL_PRECISION {
        snprintf(res, 64, c"%s(%d)".as_ptr(), fieldstr, precision);
    } else {
        snprintf(res, 64, c"%s".as_ptr(), fieldstr);
    }

    PG_RETURN_CSTRING!(res);
}

/*
 * Given an interval typmod value, return a code for the least-significant
 * field that the typmod allows to be nonzero, for instance given
 * INTERVAL DAY TO HOUR we want to identify "hour".
 *
 * The results should be ordered by field significance, which means
 * we can't use the dt.h macros YEAR etc, because for some odd reason
 * they aren't ordered that way.  Instead, arbitrarily represent
 * SECOND = 0, MINUTE = 1, HOUR = 2, DAY = 3, MONTH = 4, YEAR = 5.
 */
unsafe fn intervaltypmodleastfield(typmod: int32) -> c_int {
    if typmod < 0 {
        return 0; /* SECOND */
    }

    let r = INTERVAL_RANGE(typmod);
    if r == INTERVAL_MASK(YEAR) {
        5 /* YEAR */
    } else if r == INTERVAL_MASK(MONTH) {
        4 /* MONTH */
    } else if r == INTERVAL_MASK(DAY) {
        3 /* DAY */
    } else if r == INTERVAL_MASK(HOUR) {
        2 /* HOUR */
    } else if r == INTERVAL_MASK(MINUTE) {
        1 /* MINUTE */
    } else if r == INTERVAL_MASK(SECOND) {
        0 /* SECOND */
    } else if r == INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH) {
        4 /* MONTH */
    } else if r == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) {
        2 /* HOUR */
    } else if r == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) {
        1 /* MINUTE */
    } else if r
        == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)
    {
        0 /* SECOND */
    } else if r == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) {
        1 /* MINUTE */
    } else if r == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND) {
        0 /* SECOND */
    } else if r == INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND) {
        0 /* SECOND */
    } else if r == INTERVAL_FULL_RANGE {
        0 /* SECOND */
    } else {
        elog!(ERROR, "invalid INTERVAL typmod: 0x{:x}", typmod);
        0 /* can't get here, but keep compiler quiet */
    }
}

/*
 * interval_support()
 *
 * Planner support function for interval_scale().
 *
 * Flatten superfluous calls to interval_scale().  The interval typmod is
 * complex to permit accepting and regurgitating all SQL standard variations.
 * For truncation purposes, it boils down to a single, simple granularity.
 */
pub unsafe fn interval_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = null_mut();

    if IsA!(rawreq, T_SupportRequestSimplify) {
        let req = rawreq as *mut SupportRequestSimplify;
        let expr = (*req).fcall as *mut FuncExpr;
        let typmod: *mut Node;

        Assert!(list_length((*expr).args) >= 2);

        typmod = lsecond((*expr).args) as *mut Node;

        if IsA!(typmod, T_Const) && !(*(typmod as *mut Const)).constisnull {
            let source = linitial((*expr).args) as *mut Node;
            let new_typmod: int32 = DatumGetInt32((*(typmod as *mut Const)).constvalue);
            let noop: bool;

            if new_typmod < 0 {
                noop = true;
            } else {
                let old_typmod: int32 = exprTypmod(source);
                let old_least_field: c_int;
                let new_least_field: c_int;
                let old_precis: c_int;
                let new_precis: c_int;

                old_least_field = intervaltypmodleastfield(old_typmod);
                new_least_field = intervaltypmodleastfield(new_typmod);
                if old_typmod < 0 {
                    old_precis = INTERVAL_FULL_PRECISION;
                } else {
                    old_precis = INTERVAL_PRECISION(old_typmod);
                }
                new_precis = INTERVAL_PRECISION(new_typmod);

                /*
                 * Cast is a no-op if least field stays the same or decreases
                 * while precision stays the same or increases.  But
                 * precision, which is to say, sub-second precision, only
                 * affects ranges that include SECOND.
                 */
                noop = (new_least_field <= old_least_field)
                    && (old_least_field > 0 /* SECOND */
                        || new_precis >= MAX_INTERVAL_PRECISION
                        || new_precis >= old_precis);
            }
            if noop {
                ret = relabel_to_typmod(source, new_typmod);
            }
        }
    }

    PG_RETURN_POINTER!(ret);
}

/* interval_scale()
 * Adjust interval type for specified fields.
 * Used by PostgreSQL type system to stuff columns.
 */
pub unsafe fn interval_scale(fcinfo: FunctionCallInfo) -> Datum {
    let interval = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let typmod: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;
    *result = core::ptr::read(interval);

    AdjustIntervalForTypmod(result, typmod, null_mut());

    PG_RETURN_INTERVAL_P!(result);
}

/*
 *	Adjust interval for specified precision, in both YEAR to SECOND
 *	range and sub-second precision.
 *
 * Returns true on success, false on failure (if escontext points to an
 * ErrorSaveContext; otherwise errors are thrown).
 */
unsafe fn AdjustIntervalForTypmod(
    interval: *mut Interval,
    typmod: int32,
    escontext: *mut Node,
) -> bool {
    static INTERVAL_SCALES: [int64; (MAX_INTERVAL_PRECISION + 1) as usize] =
        [1000000, 100000, 10000, 1000, 100, 10, 1];

    static INTERVAL_OFFSETS: [int64; (MAX_INTERVAL_PRECISION + 1) as usize] =
        [500000, 50000, 5000, 500, 50, 5, 0];

    /* Typmod has no effect on infinite intervals */
    if INTERVAL_NOT_FINITE(interval) {
        return true;
    }

    /*
     * Unspecified range and precision? Then not necessary to adjust. Setting
     * typmod to -1 is the convention for all data types.
     */
    if typmod >= 0 {
        let range = INTERVAL_RANGE(typmod);
        let precision = INTERVAL_PRECISION(typmod);

        if range == INTERVAL_FULL_RANGE {
            /* Do nothing... */
        } else if range == INTERVAL_MASK(YEAR) {
            (*interval).month = ((*interval).month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
            (*interval).day = 0;
            (*interval).time = 0;
        } else if range == INTERVAL_MASK(MONTH) {
            (*interval).day = 0;
            (*interval).time = 0;
        }
        /* YEAR TO MONTH */
        else if range == (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)) {
            (*interval).day = 0;
            (*interval).time = 0;
        } else if range == INTERVAL_MASK(DAY) {
            (*interval).time = 0;
        } else if range == INTERVAL_MASK(HOUR) {
            (*interval).time = ((*interval).time / USECS_PER_HOUR) * USECS_PER_HOUR;
        } else if range == INTERVAL_MASK(MINUTE) {
            (*interval).time = ((*interval).time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        } else if range == INTERVAL_MASK(SECOND) {
            /* fractional-second rounding will be dealt with below */
        }
        /* DAY TO HOUR */
        else if range == (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR)) {
            (*interval).time = ((*interval).time / USECS_PER_HOUR) * USECS_PER_HOUR;
        }
        /* DAY TO MINUTE */
        else if range == (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)) {
            (*interval).time = ((*interval).time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        }
        /* DAY TO SECOND */
        else if range
            == (INTERVAL_MASK(DAY)
                | INTERVAL_MASK(HOUR)
                | INTERVAL_MASK(MINUTE)
                | INTERVAL_MASK(SECOND))
        {
            /* fractional-second rounding will be dealt with below */
        }
        /* HOUR TO MINUTE */
        else if range == (INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)) {
            (*interval).time = ((*interval).time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        }
        /* HOUR TO SECOND */
        else if range == (INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) {
            /* fractional-second rounding will be dealt with below */
        }
        /* MINUTE TO SECOND */
        else if range == (INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) {
            /* fractional-second rounding will be dealt with below */
        } else {
            elog!(ERROR, "unrecognized interval typmod: {}", typmod);
        }

        /* Need to adjust sub-second precision? */
        if precision != INTERVAL_FULL_PRECISION {
            if precision < 0 || precision > MAX_INTERVAL_PRECISION {
                ereport!(
                    ERROR,
                    errmsg!(
                        "interval({}) precision must be between {} and {}",
                        precision, 0, MAX_INTERVAL_PRECISION
                    )
                );
                return false;
            }

            if (*interval).time >= 0 {
                if pg_add_s64_overflow(
                    (*interval).time,
                    INTERVAL_OFFSETS[precision as usize],
                    &mut (*interval).time,
                ) {
                    ereport!(ERROR, errmsg!("interval out of range"));
                    return false;
                }
                (*interval).time -= (*interval).time % INTERVAL_SCALES[precision as usize];
            } else {
                if pg_sub_s64_overflow(
                    (*interval).time,
                    INTERVAL_OFFSETS[precision as usize],
                    &mut (*interval).time,
                ) {
                    ereport!(ERROR, errmsg!("interval out of range"));
                    return false;
                }
                (*interval).time -= (*interval).time % INTERVAL_SCALES[precision as usize];
            }
        }
    }

    true
}

/*
 * make_interval - numeric Interval constructor
 */
pub unsafe fn make_interval(fcinfo: FunctionCallInfo) -> Datum {
    let years: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let months: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let weeks: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let days: int32 = PG_GETARG_INT32!(fcinfo, 3);
    let hours: int32 = PG_GETARG_INT32!(fcinfo, 4);
    let mins: int32 = PG_GETARG_INT32!(fcinfo, 5);
    let mut secs: f64 = PG_GETARG_FLOAT8!(fcinfo, 6);
    let result: *mut Interval;

    /*
     * Reject out-of-range inputs.  We reject any input values that cause
     * integer overflow of the corresponding interval fields.
     */
    'out_of_range: {
        if isinf(secs) != 0 || isnan(secs) != 0 {
            break 'out_of_range;
        }

        result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

        /* years and months -> months */
        if pg_mul_s32_overflow(years, MONTHS_PER_YEAR, &mut (*result).month)
            || pg_add_s32_overflow((*result).month, months, &mut (*result).month)
        {
            break 'out_of_range;
        }

        /* weeks and days -> days */
        if pg_mul_s32_overflow(weeks, DAYS_PER_WEEK, &mut (*result).day)
            || pg_add_s32_overflow((*result).day, days, &mut (*result).day)
        {
            break 'out_of_range;
        }

        /* hours and mins -> usecs (cannot overflow 64-bit) */
        (*result).time = hours as int64 * USECS_PER_HOUR + mins as int64 * USECS_PER_MINUTE;

        /* secs -> usecs */
        secs = rint(float8_mul(secs, USECS_PER_SEC as float8));
        if !FLOAT8_FITS_IN_INT64(secs)
            || pg_add_s64_overflow((*result).time, secs as int64, &mut (*result).time)
        {
            break 'out_of_range;
        }

        /* make sure that the result is finite */
        if INTERVAL_NOT_FINITE(result) {
            break 'out_of_range;
        }

        PG_RETURN_INTERVAL_P!(result);
    }

    // out_of_range:
    ereport!(ERROR, errmsg!("interval out of range"));

    PG_RETURN_NULL!(fcinfo); /* keep compiler quiet */
}

/* EncodeSpecialTimestamp()
 * Convert reserved timestamp data type to string.
 */
pub unsafe fn EncodeSpecialTimestamp(dt: Timestamp, str: *mut c_char) {
    if TIMESTAMP_IS_NOBEGIN(dt) {
        strcpy(str, EARLY.as_ptr() as *const c_char);
    } else if TIMESTAMP_IS_NOEND(dt) {
        strcpy(str, LATE.as_ptr() as *const c_char);
    } else
    /* shouldn't happen */
    {
        elog!(ERROR, "invalid argument for EncodeSpecialTimestamp");
    }
}

unsafe fn EncodeSpecialInterval(interval: *const Interval, str: *mut c_char) {
    if INTERVAL_IS_NOBEGIN(interval) {
        strcpy(str, EARLY.as_ptr() as *const c_char);
    } else if INTERVAL_IS_NOEND(interval) {
        strcpy(str, LATE.as_ptr() as *const c_char);
    } else
    /* shouldn't happen */
    {
        elog!(ERROR, "invalid argument for EncodeSpecialInterval");
    }
}

pub unsafe fn now(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_TIMESTAMPTZ!(GetCurrentTransactionStartTimestamp());
}

pub unsafe fn statement_timestamp(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_TIMESTAMPTZ!(GetCurrentStatementStartTimestamp());
}

pub unsafe fn clock_timestamp(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_TIMESTAMPTZ!(GetCurrentTimestamp());
}

pub unsafe fn pg_postmaster_start_time(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_TIMESTAMPTZ!(PgStartTime);
}

pub unsafe fn pg_conf_load_time(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_TIMESTAMPTZ!(PgReloadTime);
}

/*
 * GetCurrentTimestamp -- get the current operating system time
 *
 * Result is in the form of a TimestampTz value, and is expressed to the
 * full precision of the gettimeofday() syscall
 */
pub unsafe fn GetCurrentTimestamp() -> TimestampTz {
    let mut result: TimestampTz;
    let mut tp: timeval = std::mem::zeroed();

    gettimeofday(&mut tp, null_mut());

    result = tp.tv_sec as TimestampTz
        - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) as TimestampTz;
    result = (result * USECS_PER_SEC) + tp.tv_usec;

    result
}

/*
 * GetSQLCurrentTimestamp -- implements CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(n)
 */
pub unsafe fn GetSQLCurrentTimestamp(typmod: int32) -> TimestampTz {
    let mut ts: TimestampTz;

    ts = GetCurrentTransactionStartTimestamp();
    if typmod >= 0 {
        AdjustTimestampForTypmod(&mut ts, typmod, null_mut());
    }
    ts
}

/*
 * GetSQLLocalTimestamp -- implements LOCALTIMESTAMP, LOCALTIMESTAMP(n)
 */
pub unsafe fn GetSQLLocalTimestamp(typmod: int32) -> Timestamp {
    let mut ts: Timestamp;

    ts = timestamptz2timestamp(GetCurrentTransactionStartTimestamp());
    if typmod >= 0 {
        AdjustTimestampForTypmod(&mut ts, typmod, null_mut());
    }
    ts
}

/*
 * timeofday(*) -- returns the current time as a text.
 */
pub unsafe fn timeofday(_fcinfo: FunctionCallInfo) -> Datum {
    let mut tp: timeval = std::mem::zeroed();
    let mut templ: [c_char; 128] = [0; 128];
    let mut buf: [c_char; 128] = [0; 128];
    let tt: pg_time_t;

    gettimeofday(&mut tp, null_mut());
    tt = tp.tv_sec as pg_time_t;
    pg_strftime(
        templ.as_mut_ptr(),
        std::mem::size_of_val(&templ) as Size,
        c"%a %b %d %H:%M:%S.%%06d %Y %Z".as_ptr(),
        pg_localtime(&tt, session_timezone),
    );
    snprintf(
        buf.as_mut_ptr(),
        std::mem::size_of_val(&buf) as Size,
        templ.as_ptr(),
        tp.tv_usec,
    );

    PG_RETURN_TEXT_P!(cstring_to_text(buf.as_ptr()));
}

/*
 * TimestampDifference -- convert the difference between two timestamps
 *		into integer seconds and microseconds
 *
 * This is typically used to calculate a wait timeout for select(2),
 * which explains the otherwise-odd choice of output format.
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GetCurrentTimestamp()).
 *
 * We expect start_time <= stop_time.  If not, we return zeros,
 * since then we're already past the previously determined stop_time.
 */
pub unsafe fn TimestampDifference(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    secs: *mut std::os::raw::c_long,
    microsecs: *mut c_int,
) {
    let diff: TimestampTz = stop_time - start_time;

    if diff <= 0 {
        *secs = 0;
        *microsecs = 0;
    } else {
        *secs = (diff / USECS_PER_SEC) as std::os::raw::c_long;
        *microsecs = (diff % USECS_PER_SEC) as c_int;
    }
}

/*
 * TimestampDifferenceMilliseconds -- convert the difference between two
 * 		timestamps into integer milliseconds
 *
 * This is typically used to calculate a wait timeout for WaitLatch()
 * or a related function.  The choice of "long" as the result type
 * is to harmonize with that; furthermore, we clamp the result to at most
 * INT_MAX milliseconds, because that's all that WaitLatch() allows.
 *
 * We expect start_time <= stop_time.  If not, we return zero,
 * since then we're already past the previously determined stop_time.
 *
 * Subtracting finite and infinite timestamps works correctly, returning
 * zero or INT_MAX as appropriate.
 *
 * Note we round up any fractional millisecond, since waiting for just
 * less than the intended timeout is undesirable.
 */
pub unsafe fn TimestampDifferenceMilliseconds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
) -> std::os::raw::c_long {
    let mut diff: TimestampTz = 0;

    /* Deal with zero or negative elapsed time quickly. */
    if start_time >= stop_time {
        return 0;
    }
    /* To not fail with timestamp infinities, we must detect overflow. */
    if pg_sub_s64_overflow(stop_time, start_time, &mut diff) {
        return INT_MAX as std::os::raw::c_long;
    }
    if diff >= (INT_MAX as int64 * 1000 - 999) {
        INT_MAX as std::os::raw::c_long
    } else {
        ((diff + 999) / 1000) as std::os::raw::c_long
    }
}

/*
 * TimestampDifferenceExceeds -- report whether the difference between two
 *		timestamps is >= a threshold (expressed in milliseconds)
 *
 * Both inputs must be ordinary finite timestamps (in current usage,
 * they'll be results from GetCurrentTimestamp()).
 */
pub unsafe fn TimestampDifferenceExceeds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    msec: c_int,
) -> bool {
    let diff: TimestampTz = stop_time - start_time;

    diff >= msec as int64 * 1000
}

/*
 * Check if the difference between two timestamps is >= a given
 * threshold (expressed in seconds).
 */
pub unsafe fn TimestampDifferenceExceedsSeconds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    threshold_sec: c_int,
) -> bool {
    let mut secs: std::os::raw::c_long = 0;
    let mut usecs: c_int = 0;

    /* Calculate the difference in seconds */
    TimestampDifference(start_time, stop_time, &mut secs, &mut usecs);

    secs >= threshold_sec as std::os::raw::c_long
}

/*
 * Convert a time_t to TimestampTz.
 *
 * We do not use time_t internally in Postgres, but this is provided for use
 * by functions that need to interpret, say, a stat(2) result.
 *
 * To avoid having the function's ABI vary depending on the width of time_t,
 * we declare the argument as pg_time_t, which is cast-compatible with
 * time_t but always 64 bits wide (unless the platform has no 64-bit type).
 * This detail should be invisible to callers, at least at source code level.
 */
pub unsafe fn time_t_to_timestamptz(tm: pg_time_t) -> TimestampTz {
    let mut result: TimestampTz;

    result = tm as TimestampTz
        - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) as TimestampTz;
    result *= USECS_PER_SEC;

    result
}

/*
 * Convert a TimestampTz to time_t.
 *
 * This too is just marginally useful, but some places need it.
 *
 * To avoid having the function's ABI vary depending on the width of time_t,
 * we declare the result as pg_time_t, which is cast-compatible with
 * time_t but always 64 bits wide (unless the platform has no 64-bit type).
 * This detail should be invisible to callers, at least at source code level.
 */
pub unsafe fn timestamptz_to_time_t(t: TimestampTz) -> pg_time_t {
    let result: pg_time_t;

    result = (t / USECS_PER_SEC
        + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) as int64) as pg_time_t;

    result
}

/*
 * Produce a C-string representation of a TimestampTz.
 *
 * This is mostly for use in emitting messages.  The primary difference
 * from timestamptz_out is that we force the output format to ISO.  Note
 * also that the result is in a static buffer, not pstrdup'd.
 *
 * See also pg_strftime.
 */
pub unsafe fn timestamptz_to_str(t: TimestampTz) -> *const c_char {
    static mut BUF: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];
    let buf = &raw mut BUF as *mut c_char;
    let mut tz: c_int = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut fsec: fsec_t = 0;
    let mut tzn: *const c_char = null();

    if TIMESTAMP_NOT_FINITE(t) {
        EncodeSpecialTimestamp(t, buf);
    } else if timestamp2tm(t, &mut tz, tm, &mut fsec, &mut tzn, null_mut()) == 0 {
        EncodeDateTime(tm, fsec, true, tz, tzn, USE_ISO_DATES, buf);
    } else {
        strlcpy(buf, c"(timestamp out of range)".as_ptr(), (MAXDATELEN + 1) as Size);
    }

    buf as *const c_char
}

pub unsafe fn dt2time(
    jd: Timestamp,
    hour: *mut c_int,
    min: *mut c_int,
    sec: *mut c_int,
    fsec: *mut fsec_t,
) {
    let mut time: TimeOffset;

    time = jd;

    *hour = (time / USECS_PER_HOUR) as c_int;
    time -= (*hour) as int64 * USECS_PER_HOUR;
    *min = (time / USECS_PER_MINUTE) as c_int;
    time -= (*min) as int64 * USECS_PER_MINUTE;
    *sec = (time / USECS_PER_SEC) as c_int;
    *fsec = (time - (*sec as int64 * USECS_PER_SEC)) as fsec_t;
} /* dt2time() */

/*
 * timestamp2tm() - Convert timestamp data type to POSIX time structure.
 *
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 * Returns:
 *	 0 on success
 *	-1 on out of range
 *
 * If attimezone is NULL, the global timezone setting will be used.
 */
pub unsafe fn timestamp2tm(
    mut dt: Timestamp,
    tzp: *mut c_int,
    tm: *mut pg_tm,
    fsec: *mut fsec_t,
    tzn: *mut *const c_char,
    mut attimezone: *mut pg_tz,
) -> c_int {
    let mut date: Timestamp = 0;
    let mut time: Timestamp;
    let utime: pg_time_t;

    /* Use session timezone if caller asks for default */
    if attimezone.is_null() {
        attimezone = session_timezone;
    }

    time = dt;
    TMODULO(&mut time, &mut date, USECS_PER_DAY);

    if time < 0 {
        time += USECS_PER_DAY;
        date -= 1;
    }

    /* add offset to go from J2000 back to standard Julian date */
    date += POSTGRES_EPOCH_JDATE as Timestamp;

    /* Julian day routine does not work for negative Julian days */
    if date < 0 || date > INT_MAX as Timestamp {
        return -1;
    }

    j2date(
        date as c_int,
        &mut (*tm).tm_year,
        &mut (*tm).tm_mon,
        &mut (*tm).tm_mday,
    );
    dt2time(
        time,
        &mut (*tm).tm_hour,
        &mut (*tm).tm_min,
        &mut (*tm).tm_sec,
        fsec,
    );

    /* Done if no TZ conversion wanted */
    if tzp.is_null() {
        (*tm).tm_isdst = -1;
        (*tm).tm_gmtoff = 0;
        (*tm).tm_zone = null();
        if !tzn.is_null() {
            *tzn = null();
        }
        return 0;
    }

    /*
     * If the time falls within the range of pg_time_t, use pg_localtime() to
     * rotate to the local time zone.
     *
     * First, convert to an integral timestamp, avoiding possibly
     * platform-specific roundoff-in-wrong-direction errors, and adjust to
     * Unix epoch.  Then see if we can convert to pg_time_t without loss. This
     * coding avoids hardwiring any assumptions about the width of pg_time_t,
     * so it should behave sanely on machines without int64.
     */
    dt = (dt - *fsec as Timestamp) / USECS_PER_SEC
        + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY) as Timestamp;
    utime = dt as pg_time_t;
    if utime as Timestamp == dt {
        let tx: *mut pg_tm = pg_localtime(&utime, attimezone);

        (*tm).tm_year = (*tx).tm_year + 1900;
        (*tm).tm_mon = (*tx).tm_mon + 1;
        (*tm).tm_mday = (*tx).tm_mday;
        (*tm).tm_hour = (*tx).tm_hour;
        (*tm).tm_min = (*tx).tm_min;
        (*tm).tm_sec = (*tx).tm_sec;
        (*tm).tm_isdst = (*tx).tm_isdst;
        (*tm).tm_gmtoff = (*tx).tm_gmtoff;
        (*tm).tm_zone = (*tx).tm_zone;
        *tzp = -(*tm).tm_gmtoff as c_int;
        if !tzn.is_null() {
            *tzn = (*tm).tm_zone;
        }
    } else {
        /*
         * When out of range of pg_time_t, treat as GMT
         */
        *tzp = 0;
        /* Mark this as *no* time zone available */
        (*tm).tm_isdst = -1;
        (*tm).tm_gmtoff = 0;
        (*tm).tm_zone = null();
        if !tzn.is_null() {
            *tzn = null();
        }
    }

    0
}

/* tm2timestamp()
 * Convert a tm structure to a timestamp data type.
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 *
 * Returns -1 on failure (value out of range).
 */
pub unsafe fn tm2timestamp(
    tm: *mut pg_tm,
    fsec: fsec_t,
    tzp: *mut c_int,
    result: *mut Timestamp,
) -> c_int {
    let date: TimeOffset;
    let time: TimeOffset;

    /* Prevent overflow in Julian-day routines */
    if !IS_VALID_JULIAN((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) {
        *result = 0; /* keep compiler quiet */
        return -1;
    }

    date = (date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) - POSTGRES_EPOCH_JDATE) as TimeOffset;
    time = time2t((*tm).tm_hour, (*tm).tm_min, (*tm).tm_sec, fsec);

    if pg_mul_s64_overflow(date, USECS_PER_DAY, result)
        || pg_add_s64_overflow(*result, time, result)
    {
        *result = 0; /* keep compiler quiet */
        return -1;
    }
    if !tzp.is_null() {
        *result = dt2local(*result, -(*tzp));
    }

    /* final range check catches just-out-of-range timestamps */
    if !IS_VALID_TIMESTAMP(*result) {
        *result = 0; /* keep compiler quiet */
        return -1;
    }

    0
}

/* interval2itm()
 * Convert an Interval to a pg_itm structure.
 * Note: overflow is not possible, because the pg_itm fields are
 * wide enough for all possible conversion results.
 */
pub unsafe fn interval2itm(span: Interval, itm: *mut pg_itm) {
    let mut time: TimeOffset;
    let mut tfrac: TimeOffset;

    (*itm).tm_year = span.month / MONTHS_PER_YEAR;
    (*itm).tm_mon = span.month % MONTHS_PER_YEAR;
    (*itm).tm_mday = span.day;
    time = span.time;

    tfrac = time / USECS_PER_HOUR;
    time -= tfrac * USECS_PER_HOUR;
    (*itm).tm_hour = tfrac;
    tfrac = time / USECS_PER_MINUTE;
    time -= tfrac * USECS_PER_MINUTE;
    (*itm).tm_min = tfrac as c_int;
    tfrac = time / USECS_PER_SEC;
    time -= tfrac * USECS_PER_SEC;
    (*itm).tm_sec = tfrac as c_int;
    (*itm).tm_usec = time as c_int;
}

/* itm2interval()
 * Convert a pg_itm structure to an Interval.
 * Returns 0 if OK, -1 on overflow.
 *
 * This is for use in computations expected to produce finite results.  Any
 * inputs that lead to infinite results are treated as overflows.
 */
pub unsafe fn itm2interval(itm: *mut pg_itm, span: *mut Interval) -> c_int {
    let total_months: int64 = (*itm).tm_year as int64 * MONTHS_PER_YEAR as int64 + (*itm).tm_mon as int64;

    if total_months > INT_MAX as int64 || total_months < INT_MIN as int64 {
        return -1;
    }
    (*span).month = total_months as int32;
    (*span).day = (*itm).tm_mday;
    if pg_mul_s64_overflow((*itm).tm_hour, USECS_PER_HOUR, &mut (*span).time) {
        return -1;
    }
    /* tm_min, tm_sec are 32 bits, so intermediate products can't overflow */
    if pg_add_s64_overflow((*span).time, (*itm).tm_min as int64 * USECS_PER_MINUTE, &mut (*span).time) {
        return -1;
    }
    if pg_add_s64_overflow((*span).time, (*itm).tm_sec as int64 * USECS_PER_SEC, &mut (*span).time) {
        return -1;
    }
    if pg_add_s64_overflow((*span).time, (*itm).tm_usec as int64, &mut (*span).time) {
        return -1;
    }
    if INTERVAL_NOT_FINITE(span) {
        return -1;
    }
    0
}

/* itmin2interval()
 * Convert a pg_itm_in structure to an Interval.
 * Returns 0 if OK, -1 on overflow.
 *
 * Note: if the result is infinite, it is not treated as an overflow.  This
 * avoids any dump/reload hazards from pre-17 databases that do not support
 * infinite intervals, but do allow finite intervals with all fields set to
 * INT_MIN/INT_MAX (outside the documented range).  Such intervals will be
 * silently converted to +/-infinity.  This may not be ideal, but seems
 * preferable to failure, and ought to be pretty unlikely in practice.
 */
unsafe fn itmin2interval_impl(itm_in: *const pg_itm_in, span: *mut Interval) -> c_int {
    let total_months: int64 =
        (*itm_in).tm_year as int64 * MONTHS_PER_YEAR as int64 + (*itm_in).tm_mon as int64;

    if total_months > INT_MAX as int64 || total_months < INT_MIN as int64 {
        return -1;
    }
    (*span).month = total_months as int32;
    (*span).day = (*itm_in).tm_mday;
    (*span).time = (*itm_in).tm_usec;
    0
}

unsafe fn time2t(hour: c_int, min: c_int, sec: c_int, fsec: fsec_t) -> TimeOffset {
    (((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec) as int64 * USECS_PER_SEC)
        + fsec as int64
}

unsafe fn dt2local(mut dt: Timestamp, timezone: c_int) -> Timestamp {
    dt -= timezone as int64 * USECS_PER_SEC;
    dt
}

/*****************************************************************************
 *	 PUBLIC ROUTINES														 *
 *****************************************************************************/

pub unsafe fn timestamp_finite(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);

    PG_RETURN_BOOL!(!TIMESTAMP_NOT_FINITE(timestamp));
}

pub unsafe fn interval_finite(fcinfo: FunctionCallInfo) -> Datum {
    let interval = PG_GETARG_INTERVAL_P!(fcinfo, 0);

    PG_RETURN_BOOL!(!INTERVAL_NOT_FINITE(interval));
}

/*----------------------------------------------------------
 *	Relational operators for timestamp.
 *---------------------------------------------------------*/

pub unsafe fn GetEpochTime(tm: *mut pg_tm) {
    let t0: *mut pg_tm;
    let epoch: pg_time_t = 0;

    t0 = pg_gmtime(&epoch);

    if t0.is_null() {
        elog!(ERROR, "could not convert epoch to timestamp: %m");
    }

    (*tm).tm_year = (*t0).tm_year;
    (*tm).tm_mon = (*t0).tm_mon;
    (*tm).tm_mday = (*t0).tm_mday;
    (*tm).tm_hour = (*t0).tm_hour;
    (*tm).tm_min = (*t0).tm_min;
    (*tm).tm_sec = (*t0).tm_sec;

    (*tm).tm_year += 1900;
    (*tm).tm_mon += 1;
}

pub unsafe fn SetEpochTimestamp() -> Timestamp {
    let mut dt: Timestamp = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;

    GetEpochTime(tm);
    /* we don't bother to test for failure ... */
    tm2timestamp(tm, 0, null_mut(), &mut dt);

    dt
} /* SetEpochTimestamp() */

/*
 * We are currently sharing some code between timestamp and timestamptz.
 * The comparison functions are among them. - thomas 2001-09-25
 *
 *		timestamp_relop - is timestamp1 relop timestamp2
 */
pub unsafe fn timestamp_cmp_internal(dt1: Timestamp, dt2: Timestamp) -> int32 {
    if dt1 < dt2 {
        -1
    } else if dt1 > dt2 {
        1
    } else {
        0
    }
}

/* timestamptz_cmp_internal is just timestamp_cmp_internal applied to TimestampTz */
#[inline]
pub unsafe fn timestamptz_cmp_internal(dt1: TimestampTz, dt2: TimestampTz) -> int32 {
    timestamp_cmp_internal(dt1, dt2)
}

pub unsafe fn timestamp_eq(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_internal(dt1, dt2) == 0);
}

pub unsafe fn timestamp_ne(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_internal(dt1, dt2) != 0);
}

pub unsafe fn timestamp_lt(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_internal(dt1, dt2) < 0);
}

pub unsafe fn timestamp_gt(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_internal(dt1, dt2) > 0);
}

pub unsafe fn timestamp_le(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_internal(dt1, dt2) <= 0);
}

pub unsafe fn timestamp_ge(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_internal(dt1, dt2) >= 0);
}

pub unsafe fn timestamp_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_INT32!(timestamp_cmp_internal(dt1, dt2));
}

/* note: this comparator is used for timestamptz also (SIZEOF_DATUM >= 8 on our target) */

/* note: this is used for timestamptz also */
#[cfg(target_pointer_width = "32")]
unsafe extern "C" fn timestamp_fastcmp(x: Datum, y: Datum, _ssup: *mut SortSupportData) -> c_int {
    let a: Timestamp = DatumGetTimestamp(x);
    let b: Timestamp = DatumGetTimestamp(y);

    timestamp_cmp_internal(a, b) as c_int
}

pub unsafe fn timestamp_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    /*
     * If this build has pass-by-value timestamps, then we can use a standard
     * comparator function.
     */
    (*ssup).comparator = Some(ssup_datum_signed_cmp);
    PG_RETURN_VOID!();
}

/* note: this is used for timestamptz also */
unsafe fn timestamp_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    let texisting: Timestamp = DatumGetTimestamp(existing);

    if texisting == PG_INT64_MIN {
        /* return value is undefined */
        *underflow = true;
        return 0 as Datum;
    }

    *underflow = false;
    TimestampGetDatum(texisting - 1)
}

/* note: this is used for timestamptz also */
unsafe fn timestamp_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    let texisting: Timestamp = DatumGetTimestamp(existing);

    if texisting == PG_INT64_MAX {
        /* return value is undefined */
        *overflow = true;
        return 0 as Datum;
    }

    *overflow = false;
    TimestampGetDatum(texisting + 1)
}

pub unsafe fn timestamp_skipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;

    (*sksup).decrement = Some(timestamp_decrement);
    (*sksup).increment = Some(timestamp_increment);
    (*sksup).low_elem = TimestampGetDatum(PG_INT64_MIN);
    (*sksup).high_elem = TimestampGetDatum(PG_INT64_MAX);

    PG_RETURN_VOID!();
}

pub unsafe fn timestamp_hash(fcinfo: FunctionCallInfo) -> Datum {
    hashint8(fcinfo)
}

pub unsafe fn timestamp_hash_extended(fcinfo: FunctionCallInfo) -> Datum {
    hashint8extended(fcinfo)
}

pub unsafe fn timestamptz_hash(fcinfo: FunctionCallInfo) -> Datum {
    hashint8(fcinfo)
}

pub unsafe fn timestamptz_hash_extended(fcinfo: FunctionCallInfo) -> Datum {
    hashint8extended(fcinfo)
}

/*
 * Cross-type comparison functions for timestamp vs timestamptz
 */

pub unsafe fn timestamp_cmp_timestamptz_internal(
    timestampVal: Timestamp,
    dt2: TimestampTz,
) -> int32 {
    let dt1: TimestampTz;
    let mut overflow: c_int = 0;

    dt1 = timestamp2timestamptz_opt_overflow(timestampVal, &mut overflow);
    if overflow > 0 {
        /* dt1 is larger than any finite timestamp, but less than infinity */
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    if overflow < 0 {
        /* dt1 is less than any finite timestamp, but more than -infinity */
        return if TIMESTAMP_IS_NOBEGIN(dt2) { 1 } else { -1 };
    }

    timestamptz_cmp_internal(dt1, dt2)
}

pub unsafe fn timestamp_eq_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt2) == 0);
}

pub unsafe fn timestamp_ne_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt2) != 0);
}

pub unsafe fn timestamp_lt_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt2) < 0);
}

pub unsafe fn timestamp_gt_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt2) > 0);
}

pub unsafe fn timestamp_le_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt2) <= 0);
}

pub unsafe fn timestamp_ge_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt2) >= 0);
}

pub unsafe fn timestamp_cmp_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_INT32!(timestamp_cmp_timestamptz_internal(timestampVal, dt2));
}

pub unsafe fn timestamptz_eq_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt1) == 0);
}

pub unsafe fn timestamptz_ne_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt1) != 0);
}

pub unsafe fn timestamptz_lt_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt1) > 0);
}

pub unsafe fn timestamptz_gt_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt1) < 0);
}

pub unsafe fn timestamptz_le_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt1) >= 0);
}

pub unsafe fn timestamptz_ge_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(timestamp_cmp_timestamptz_internal(timestampVal, dt1) <= 0);
}

pub unsafe fn timestamptz_cmp_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let timestampVal = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_INT32!(-timestamp_cmp_timestamptz_internal(timestampVal, dt1));
}

/*
 *		interval_relop	- is interval1 relop interval2
 *
 * Interval comparison is based on converting interval values to a linear
 * representation expressed in the units of the time field (microseconds,
 * in the case of integer timestamps) with days assumed to be always 24 hours
 * and months assumed to be always 30 days.  To avoid overflow, we need a
 * wider-than-int64 datatype for the linear representation, so use INT128.
 */

#[inline]
unsafe fn interval_cmp_value(interval: *const Interval) -> INT128 {
    let mut span: INT128;
    let mut days: int64;

    /*
     * Combine the month and day fields into an integral number of days.
     * Because the inputs are int32, int64 arithmetic suffices here.
     */
    days = (*interval).month as int64 * 30;
    days += (*interval).day as int64;

    /* Widen time field to 128 bits */
    span = int64_to_int128((*interval).time);

    /* Scale up days to microseconds, forming a 128-bit product */
    int128_add_int64_mul_int64(&mut span, days, USECS_PER_DAY);

    span
}

unsafe fn interval_cmp_internal(interval1: *const Interval, interval2: *const Interval) -> c_int {
    let span1: INT128 = interval_cmp_value(interval1);
    let span2: INT128 = interval_cmp_value(interval2);

    int128_compare(span1, span2)
}

unsafe fn interval_sign(interval: *const Interval) -> c_int {
    let span: INT128 = interval_cmp_value(interval);
    let zero: INT128 = int64_to_int128(0);

    int128_compare(span, zero)
}

pub unsafe fn interval_eq(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_BOOL!(interval_cmp_internal(interval1, interval2) == 0);
}

pub unsafe fn interval_ne(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_BOOL!(interval_cmp_internal(interval1, interval2) != 0);
}

pub unsafe fn interval_lt(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_BOOL!(interval_cmp_internal(interval1, interval2) < 0);
}

pub unsafe fn interval_gt(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_BOOL!(interval_cmp_internal(interval1, interval2) > 0);
}

pub unsafe fn interval_le(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_BOOL!(interval_cmp_internal(interval1, interval2) <= 0);
}

pub unsafe fn interval_ge(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_BOOL!(interval_cmp_internal(interval1, interval2) >= 0);
}

pub unsafe fn interval_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_INT32!(interval_cmp_internal(interval1, interval2));
}

/*
 * Hashing for intervals
 *
 * We must produce equal hashvals for values that interval_cmp_internal()
 * considers equal.  So, compute the net span the same way it does,
 * and then hash that.
 */
pub unsafe fn interval_hash(fcinfo: FunctionCallInfo) -> Datum {
    let interval = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let span: INT128 = interval_cmp_value(interval);
    let span64: int64;

    /*
     * Use only the least significant 64 bits for hashing.  The upper 64 bits
     * seldom add any useful information, and besides we must do it like this
     * for compatibility with hashes calculated before use of INT128 was
     * introduced.
     */
    span64 = int128_to_int64(span);

    DirectFunctionCall1!(hashint8, Int64GetDatumFast(span64))
}

pub unsafe fn interval_hash_extended(fcinfo: FunctionCallInfo) -> Datum {
    let interval = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let span: INT128 = interval_cmp_value(interval);
    let span64: int64;

    /* Same approach as interval_hash */
    span64 = int128_to_int64(span);

    DirectFunctionCall2!(
        hashint8extended,
        Int64GetDatumFast(span64),
        PG_GETARG_DATUM!(fcinfo, 1)
    )
}

/* overlaps_timestamp() --- implements the SQL OVERLAPS operator.
 *
 * Algorithm is per SQL spec.  This is much harder than you'd think
 * because the spec requires us to deliver a non-null answer in some cases
 * where some of the inputs are null.
 */
pub unsafe fn overlaps_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    /*
     * The arguments are Timestamps, but we leave them as generic Datums to
     * avoid unnecessary conversions between value and reference forms --- not
     * to mention possible dereferences of null pointers.
     */
    let mut ts1: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let mut te1: Datum = PG_GETARG_DATUM!(fcinfo, 1);
    let mut ts2: Datum = PG_GETARG_DATUM!(fcinfo, 2);
    let mut te2: Datum = PG_GETARG_DATUM!(fcinfo, 3);
    let ts1IsNull: bool = PG_ARGISNULL!(fcinfo, 0);
    let mut te1IsNull: bool = PG_ARGISNULL!(fcinfo, 1);
    let ts2IsNull: bool = PG_ARGISNULL!(fcinfo, 2);
    let mut te2IsNull: bool = PG_ARGISNULL!(fcinfo, 3);

    macro_rules! TIMESTAMP_GT {
        ($t1:expr, $t2:expr) => {
            DatumGetBool(DirectFunctionCall2!(timestamp_gt, $t1, $t2))
        };
    }
    macro_rules! TIMESTAMP_LT {
        ($t1:expr, $t2:expr) => {
            DatumGetBool(DirectFunctionCall2!(timestamp_lt, $t1, $t2))
        };
    }

    /*
     * If both endpoints of interval 1 are null, the result is null (unknown).
     * If just one endpoint is null, take ts1 as the non-null one. Otherwise,
     * take ts1 as the lesser endpoint.
     */
    if ts1IsNull {
        if te1IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        /* swap null for non-null */
        ts1 = te1;
        te1IsNull = true;
    } else if !te1IsNull {
        if TIMESTAMP_GT!(ts1, te1) {
            let tt: Datum = ts1;

            ts1 = te1;
            te1 = tt;
        }
    }

    /* Likewise for interval 2. */
    if ts2IsNull {
        if te2IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        /* swap null for non-null */
        ts2 = te2;
        te2IsNull = true;
    } else if !te2IsNull {
        if TIMESTAMP_GT!(ts2, te2) {
            let tt: Datum = ts2;

            ts2 = te2;
            te2 = tt;
        }
    }

    /*
     * At this point neither ts1 nor ts2 is null, so we can consider three
     * cases: ts1 > ts2, ts1 < ts2, ts1 = ts2
     */
    if TIMESTAMP_GT!(ts1, ts2) {
        /*
         * This case is ts1 < te2 OR te1 < te2, which may look redundant but
         * in the presence of nulls it's not quite completely so.
         */
        if te2IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        if TIMESTAMP_LT!(ts1, te2) {
            PG_RETURN_BOOL!(true);
        }
        if te1IsNull {
            PG_RETURN_NULL!(fcinfo);
        }

        /*
         * If te1 is not null then we had ts1 <= te1 above, and we just found
         * ts1 >= te2, hence te1 >= te2.
         */
        PG_RETURN_BOOL!(false);
    } else if TIMESTAMP_LT!(ts1, ts2) {
        /* This case is ts2 < te1 OR te2 < te1 */
        if te1IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        if TIMESTAMP_LT!(ts2, te1) {
            PG_RETURN_BOOL!(true);
        }
        if te2IsNull {
            PG_RETURN_NULL!(fcinfo);
        }

        /*
         * If te2 is not null then we had ts2 <= te2 above, and we just found
         * ts2 >= te1, hence te2 >= te1.
         */
        PG_RETURN_BOOL!(false);
    } else {
        /*
         * For ts1 = ts2 the spec says te1 <> te2 OR te1 = te2, which is a
         * rather silly way of saying "true if both are non-null, else null".
         */
        if te1IsNull || te2IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        PG_RETURN_BOOL!(true);
    }
}

/*----------------------------------------------------------
 *	"Arithmetic" operators on date/times.
 *---------------------------------------------------------*/

pub unsafe fn timestamp_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let result: Timestamp;

    /* use timestamp_cmp_internal to be sure this agrees with comparisons */
    if timestamp_cmp_internal(dt1, dt2) < 0 {
        result = dt1;
    } else {
        result = dt2;
    }
    PG_RETURN_TIMESTAMP!(result);
}

pub unsafe fn timestamp_larger(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let result: Timestamp;

    if timestamp_cmp_internal(dt1, dt2) > 0 {
        result = dt1;
    } else {
        result = dt2;
    }
    PG_RETURN_TIMESTAMP!(result);
}

pub unsafe fn timestamp_mi(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let mut result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    /*
     * Handle infinities.
     *
     * We treat anything that amounts to "infinity - infinity" as an error,
     * since the interval type has nothing equivalent to NaN.
     */
    if TIMESTAMP_NOT_FINITE(dt1) || TIMESTAMP_NOT_FINITE(dt2) {
        if TIMESTAMP_IS_NOBEGIN(dt1) {
            if TIMESTAMP_IS_NOBEGIN(dt2) {
                ereport!(ERROR, errmsg!("interval out of range"));
            } else {
                INTERVAL_NOBEGIN(result);
            }
        } else if TIMESTAMP_IS_NOEND(dt1) {
            if TIMESTAMP_IS_NOEND(dt2) {
                ereport!(ERROR, errmsg!("interval out of range"));
            } else {
                INTERVAL_NOEND(result);
            }
        } else if TIMESTAMP_IS_NOBEGIN(dt2) {
            INTERVAL_NOEND(result);
        } else
        /* TIMESTAMP_IS_NOEND(dt2) */
        {
            INTERVAL_NOBEGIN(result);
        }

        PG_RETURN_INTERVAL_P!(result);
    }

    if pg_sub_s64_overflow(dt1, dt2, &mut (*result).time) {
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    (*result).month = 0;
    (*result).day = 0;

    /*----------
     *	This is wrong, but removing it breaks a lot of regression tests.
     *	For example:
     *
     *	test=> SET timezone = 'EST5EDT';
     *	test=> SELECT
     *	test-> ('2005-10-30 13:22:00-05'::timestamptz -
     *	test(>	'2005-10-29 13:22:00-04'::timestamptz);
     *	?column?
     *	----------------
     *	 1 day 01:00:00
     *	 (1 row)
     *
     *	so adding that to the first timestamp gets:
     *
     *	 test=> SELECT
     *	 test-> ('2005-10-29 13:22:00-04'::timestamptz +
     *	 test(> ('2005-10-30 13:22:00-05'::timestamptz -
     *	 test(>  '2005-10-29 13:22:00-04'::timestamptz)) at time zone 'EST';
     *		timezone
     *	--------------------
     *	2005-10-30 14:22:00
     *	(1 row)
     *----------
     */
    result = DatumGetIntervalP(DirectFunctionCall1!(
        interval_justify_hours,
        IntervalPGetDatum(result)
    ));

    PG_RETURN_INTERVAL_P!(result);
}

/*
 *	interval_justify_interval()
 *
 *	Adjust interval so 'month', 'day', and 'time' portions are within
 *	customary bounds.  Specifically:
 *
 *		0 <= abs(time) < 24 hours
 *		0 <= abs(day)  < 30 days
 *
 *	Also, the sign bit on all three fields is made equal, so either
 *	all three fields are negative or all are positive.
 */
pub unsafe fn interval_justify_interval(fcinfo: FunctionCallInfo) -> Datum {
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let result: *mut Interval;
    let mut wholeday: TimeOffset = 0;
    let mut wholemonth: int32;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;
    (*result).month = (*span).month;
    (*result).day = (*span).day;
    (*result).time = (*span).time;

    /* do nothing for infinite intervals */
    if INTERVAL_NOT_FINITE(result) {
        PG_RETURN_INTERVAL_P!(result);
    }

    /* pre-justify days if it might prevent overflow */
    if ((*result).day > 0 && (*result).time > 0) || ((*result).day < 0 && (*result).time < 0) {
        wholemonth = (*result).day / DAYS_PER_MONTH;
        (*result).day -= wholemonth * DAYS_PER_MONTH;
        if pg_add_s32_overflow((*result).month, wholemonth, &mut (*result).month) {
            ereport!(ERROR, errmsg!("interval out of range"));
        }
    }

    /*
     * Since TimeOffset is int64, abs(wholeday) can't exceed about 1.07e8.  If
     * we pre-justified then abs(result->day) is less than DAYS_PER_MONTH, so
     * this addition can't overflow.  If we didn't pre-justify, then day and
     * time are of different signs, so it still can't overflow.
     */
    TMODULO(&mut (*result).time, &mut wholeday, USECS_PER_DAY);
    (*result).day += wholeday as int32;

    wholemonth = (*result).day / DAYS_PER_MONTH;
    (*result).day -= wholemonth * DAYS_PER_MONTH;
    if pg_add_s32_overflow((*result).month, wholemonth, &mut (*result).month) {
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    if (*result).month > 0
        && ((*result).day < 0 || ((*result).day == 0 && (*result).time < 0))
    {
        (*result).day += DAYS_PER_MONTH;
        (*result).month -= 1;
    } else if (*result).month < 0
        && ((*result).day > 0 || ((*result).day == 0 && (*result).time > 0))
    {
        (*result).day -= DAYS_PER_MONTH;
        (*result).month += 1;
    }

    if (*result).day > 0 && (*result).time < 0 {
        (*result).time += USECS_PER_DAY;
        (*result).day -= 1;
    } else if (*result).day < 0 && (*result).time > 0 {
        (*result).time -= USECS_PER_DAY;
        (*result).day += 1;
    }

    PG_RETURN_INTERVAL_P!(result);
}

/*
 *	interval_justify_hours()
 *
 *	Adjust interval so 'time' contains less than a whole day, adding
 *	the excess to 'day'.  This is useful for
 *	situations (such as non-TZ) where '1 day' = '24 hours' is valid,
 *	e.g. interval subtraction and division.
 */
pub unsafe fn interval_justify_hours(fcinfo: FunctionCallInfo) -> Datum {
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let result: *mut Interval;
    let mut wholeday: TimeOffset = 0;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;
    (*result).month = (*span).month;
    (*result).day = (*span).day;
    (*result).time = (*span).time;

    /* do nothing for infinite intervals */
    if INTERVAL_NOT_FINITE(result) {
        PG_RETURN_INTERVAL_P!(result);
    }

    TMODULO(&mut (*result).time, &mut wholeday, USECS_PER_DAY);
    if pg_add_s32_overflow((*result).day, wholeday as int32, &mut (*result).day) {
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    if (*result).day > 0 && (*result).time < 0 {
        (*result).time += USECS_PER_DAY;
        (*result).day -= 1;
    } else if (*result).day < 0 && (*result).time > 0 {
        (*result).time -= USECS_PER_DAY;
        (*result).day += 1;
    }

    PG_RETURN_INTERVAL_P!(result);
}

/*
 *	interval_justify_days()
 *
 *	Adjust interval so 'day' contains less than 30 days, adding
 *	the excess to 'month'.
 */
pub unsafe fn interval_justify_days(fcinfo: FunctionCallInfo) -> Datum {
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let result: *mut Interval;
    let wholemonth: int32;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;
    (*result).month = (*span).month;
    (*result).day = (*span).day;
    (*result).time = (*span).time;

    /* do nothing for infinite intervals */
    if INTERVAL_NOT_FINITE(result) {
        PG_RETURN_INTERVAL_P!(result);
    }

    wholemonth = (*result).day / DAYS_PER_MONTH;
    (*result).day -= wholemonth * DAYS_PER_MONTH;
    if pg_add_s32_overflow((*result).month, wholemonth, &mut (*result).month) {
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    if (*result).month > 0 && (*result).day < 0 {
        (*result).day += DAYS_PER_MONTH;
        (*result).month -= 1;
    } else if (*result).month < 0 && (*result).day > 0 {
        (*result).day -= DAYS_PER_MONTH;
        (*result).month += 1;
    }

    PG_RETURN_INTERVAL_P!(result);
}

/* timestamp_pl_interval()
 * Add an interval to a timestamp data type.
 * Note that interval has provisions for qualitative year/month and day
 *	units, so try to do the right thing with them.
 * To add a month, increment the month, and use the same day of month.
 * Then, if the next month has fewer days, set the day of month
 *	to the last day of month.
 * To add a day, increment the mday, and use the same time of day.
 * Lastly, add in the "quantitative time".
 */
pub unsafe fn timestamp_pl_interval(fcinfo: FunctionCallInfo) -> Datum {
    let mut timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let mut result: Timestamp = 0;

    /*
     * Handle infinities.
     *
     * We treat anything that amounts to "infinity - infinity" as an error,
     * since the timestamp type has nothing equivalent to NaN.
     */
    if INTERVAL_IS_NOBEGIN(span) {
        if TIMESTAMP_IS_NOEND(timestamp) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        } else {
            TIMESTAMP_NOBEGIN(&mut result);
        }
    } else if INTERVAL_IS_NOEND(span) {
        if TIMESTAMP_IS_NOBEGIN(timestamp) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        } else {
            TIMESTAMP_NOEND(&mut result);
        }
    } else if TIMESTAMP_NOT_FINITE(timestamp) {
        result = timestamp;
    } else {
        if (*span).month != 0 {
            let mut tt: pg_tm = std::mem::zeroed();
            let tm = &mut tt as *mut pg_tm;
            let mut fsec: fsec_t = 0;

            if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) != 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }

            if pg_add_s32_overflow((*tm).tm_mon, (*span).month, &mut (*tm).tm_mon) {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
            if (*tm).tm_mon > MONTHS_PER_YEAR {
                (*tm).tm_year += ((*tm).tm_mon - 1) / MONTHS_PER_YEAR;
                (*tm).tm_mon = (((*tm).tm_mon - 1) % MONTHS_PER_YEAR) + 1;
            } else if (*tm).tm_mon < 1 {
                (*tm).tm_year += (*tm).tm_mon / MONTHS_PER_YEAR - 1;
                (*tm).tm_mon = (*tm).tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
            }

            /* adjust for end of month boundary problems... */
            if (*tm).tm_mday > day_tab(isleap((*tm).tm_year), ((*tm).tm_mon - 1) as usize) {
                (*tm).tm_mday = day_tab(isleap((*tm).tm_year), ((*tm).tm_mon - 1) as usize);
            }

            if tm2timestamp(tm, fsec, null_mut(), &mut timestamp) != 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
        }

        if (*span).day != 0 {
            let mut tt: pg_tm = std::mem::zeroed();
            let tm = &mut tt as *mut pg_tm;
            let mut fsec: fsec_t = 0;
            let mut julian: c_int;

            if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) != 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }

            /*
             * Add days by converting to and from Julian.  We need an overflow
             * check here since j2date expects a non-negative integer input.
             */
            julian = date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday);
            if pg_add_s32_overflow(julian, (*span).day, &mut julian) || julian < 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
            j2date(julian, &mut (*tm).tm_year, &mut (*tm).tm_mon, &mut (*tm).tm_mday);

            if tm2timestamp(tm, fsec, null_mut(), &mut timestamp) != 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
        }

        if pg_add_s64_overflow(timestamp, (*span).time, &mut timestamp) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        if !IS_VALID_TIMESTAMP(timestamp) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        result = timestamp;
    }

    PG_RETURN_TIMESTAMP!(result);
}

pub unsafe fn timestamp_mi_interval(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let mut tspan: Interval = std::mem::zeroed();

    interval_um_internal(span, &mut tspan);

    DirectFunctionCall2!(
        timestamp_pl_interval,
        TimestampGetDatum(timestamp),
        PointerGetDatum(&tspan as *const Interval as *const c_void)
    )
}

/* timestamptz_pl_interval_internal()
 * Add an interval to a timestamptz, in the given (or session) timezone.
 *
 * Note that interval has provisions for qualitative year/month and day
 *	units, so try to do the right thing with them.
 * To add a month, increment the month, and use the same day of month.
 * Then, if the next month has fewer days, set the day of month
 *	to the last day of month.
 * To add a day, increment the mday, and use the same time of day.
 * Lastly, add in the "quantitative time".
 */
unsafe fn timestamptz_pl_interval_internal(
    mut timestamp: TimestampTz,
    span: *mut Interval,
    mut attimezone: *mut pg_tz,
) -> TimestampTz {
    let mut result: TimestampTz = 0;
    let mut tz: c_int = 0;

    /*
     * Handle infinities.
     *
     * We treat anything that amounts to "infinity - infinity" as an error,
     * since the timestamptz type has nothing equivalent to NaN.
     */
    if INTERVAL_IS_NOBEGIN(span) {
        if TIMESTAMP_IS_NOEND(timestamp) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        } else {
            TIMESTAMP_NOBEGIN(&mut result);
        }
    } else if INTERVAL_IS_NOEND(span) {
        if TIMESTAMP_IS_NOBEGIN(timestamp) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        } else {
            TIMESTAMP_NOEND(&mut result);
        }
    } else if TIMESTAMP_NOT_FINITE(timestamp) {
        result = timestamp;
    } else {
        /* Use session timezone if caller asks for default */
        if attimezone.is_null() {
            attimezone = session_timezone;
        }

        if (*span).month != 0 {
            let mut tt: pg_tm = std::mem::zeroed();
            let tm = &mut tt as *mut pg_tm;
            let mut fsec: fsec_t = 0;

            if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), attimezone) != 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }

            if pg_add_s32_overflow((*tm).tm_mon, (*span).month, &mut (*tm).tm_mon) {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
            if (*tm).tm_mon > MONTHS_PER_YEAR {
                (*tm).tm_year += ((*tm).tm_mon - 1) / MONTHS_PER_YEAR;
                (*tm).tm_mon = (((*tm).tm_mon - 1) % MONTHS_PER_YEAR) + 1;
            } else if (*tm).tm_mon < 1 {
                (*tm).tm_year += (*tm).tm_mon / MONTHS_PER_YEAR - 1;
                (*tm).tm_mon = (*tm).tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
            }

            /* adjust for end of month boundary problems... */
            if (*tm).tm_mday > day_tab(isleap((*tm).tm_year), ((*tm).tm_mon - 1) as usize) {
                (*tm).tm_mday = day_tab(isleap((*tm).tm_year), ((*tm).tm_mon - 1) as usize);
            }

            tz = DetermineTimeZoneOffset(tm, attimezone);

            if tm2timestamp(tm, fsec, &mut tz, &mut timestamp) != 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
        }

        if (*span).day != 0 {
            let mut tt: pg_tm = std::mem::zeroed();
            let tm = &mut tt as *mut pg_tm;
            let mut fsec: fsec_t = 0;
            let mut julian: c_int;

            if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), attimezone) != 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }

            /*
             * Add days by converting to and from Julian.  We need an overflow
             * check here since j2date expects a non-negative integer input.
             * In practice though, it will give correct answers for small
             * negative Julian dates; we should allow -1 to avoid
             * timezone-dependent failures, as discussed in timestamp.h.
             */
            julian = date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday);
            if pg_add_s32_overflow(julian, (*span).day, &mut julian) || julian < -1 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
            j2date(julian, &mut (*tm).tm_year, &mut (*tm).tm_mon, &mut (*tm).tm_mday);

            tz = DetermineTimeZoneOffset(tm, attimezone);

            if tm2timestamp(tm, fsec, &mut tz, &mut timestamp) != 0 {
                ereport!(ERROR, errmsg!("timestamp out of range"));
            }
        }

        if pg_add_s64_overflow(timestamp, (*span).time, &mut timestamp) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        if !IS_VALID_TIMESTAMP(timestamp) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        result = timestamp;
    }

    result
}

/* timestamptz_mi_interval_internal()
 * As above, but subtract the interval.
 */
unsafe fn timestamptz_mi_interval_internal(
    timestamp: TimestampTz,
    span: *mut Interval,
    attimezone: *mut pg_tz,
) -> TimestampTz {
    let mut tspan: Interval = std::mem::zeroed();

    interval_um_internal(span, &mut tspan);

    timestamptz_pl_interval_internal(timestamp, &mut tspan, attimezone)
}

/* timestamptz_pl_interval()
 * Add an interval to a timestamptz, in the session timezone.
 */
pub unsafe fn timestamptz_pl_interval(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_TIMESTAMP!(timestamptz_pl_interval_internal(timestamp, span, null_mut()));
}

pub unsafe fn timestamptz_mi_interval(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);

    PG_RETURN_TIMESTAMP!(timestamptz_mi_interval_internal(timestamp, span, null_mut()));
}

/* timestamptz_pl_interval_at_zone()
 * Add an interval to a timestamptz, in the specified timezone.
 */
pub unsafe fn timestamptz_pl_interval_at_zone(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let zone = PG_GETARG_TEXT_PP!(fcinfo, 2);
    let attimezone = lookup_timezone(zone);

    PG_RETURN_TIMESTAMP!(timestamptz_pl_interval_internal(timestamp, span, attimezone));
}

pub unsafe fn timestamptz_mi_interval_at_zone(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let zone = PG_GETARG_TEXT_PP!(fcinfo, 2);
    let attimezone = lookup_timezone(zone);

    PG_RETURN_TIMESTAMP!(timestamptz_mi_interval_internal(timestamp, span, attimezone));
}

/* interval_um_internal()
 * Negate an interval.
 */
unsafe fn interval_um_internal(interval: *const Interval, result: *mut Interval) {
    if INTERVAL_IS_NOBEGIN(interval) {
        INTERVAL_NOEND(result);
    } else if INTERVAL_IS_NOEND(interval) {
        INTERVAL_NOBEGIN(result);
    } else {
        /* Negate each field, guarding against overflow */
        if pg_sub_s64_overflow(0, (*interval).time, &mut (*result).time)
            || pg_sub_s32_overflow(0, (*interval).day, &mut (*result).day)
            || pg_sub_s32_overflow(0, (*interval).month, &mut (*result).month)
            || INTERVAL_NOT_FINITE(result)
        {
            ereport!(ERROR, errmsg!("interval out of range"));
        }
    }
}

pub unsafe fn interval_um(fcinfo: FunctionCallInfo) -> Datum {
    let interval = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;
    interval_um_internal(interval, result);

    PG_RETURN_INTERVAL_P!(result);
}

pub unsafe fn interval_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let result: *mut Interval;

    /* use interval_cmp_internal to be sure this agrees with comparisons */
    if interval_cmp_internal(interval1, interval2) < 0 {
        result = interval1;
    } else {
        result = interval2;
    }
    PG_RETURN_INTERVAL_P!(result);
}

pub unsafe fn interval_larger(fcinfo: FunctionCallInfo) -> Datum {
    let interval1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let interval2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let result: *mut Interval;

    if interval_cmp_internal(interval1, interval2) > 0 {
        result = interval1;
    } else {
        result = interval2;
    }
    PG_RETURN_INTERVAL_P!(result);
}

unsafe fn finite_interval_pl(span1: *const Interval, span2: *const Interval, result: *mut Interval) {
    Assert!(!INTERVAL_NOT_FINITE(span1));
    Assert!(!INTERVAL_NOT_FINITE(span2));

    if pg_add_s32_overflow((*span1).month, (*span2).month, &mut (*result).month)
        || pg_add_s32_overflow((*span1).day, (*span2).day, &mut (*result).day)
        || pg_add_s64_overflow((*span1).time, (*span2).time, &mut (*result).time)
        || INTERVAL_NOT_FINITE(result)
    {
        ereport!(ERROR, errmsg!("interval out of range"));
    }
}

pub unsafe fn interval_pl(fcinfo: FunctionCallInfo) -> Datum {
    let span1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let span2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    /*
     * Handle infinities.
     *
     * We treat anything that amounts to "infinity - infinity" as an error,
     * since the interval type has nothing equivalent to NaN.
     */
    if INTERVAL_IS_NOBEGIN(span1) {
        if INTERVAL_IS_NOEND(span2) {
            ereport!(ERROR, errmsg!("interval out of range"));
        } else {
            INTERVAL_NOBEGIN(result);
        }
    } else if INTERVAL_IS_NOEND(span1) {
        if INTERVAL_IS_NOBEGIN(span2) {
            ereport!(ERROR, errmsg!("interval out of range"));
        } else {
            INTERVAL_NOEND(result);
        }
    } else if INTERVAL_NOT_FINITE(span2) {
        memcpy(
            result as *mut c_void,
            span2 as *const c_void,
            std::mem::size_of::<Interval>() as Size,
        );
    } else {
        finite_interval_pl(span1, span2, result);
    }

    PG_RETURN_INTERVAL_P!(result);
}

unsafe fn finite_interval_mi(span1: *const Interval, span2: *const Interval, result: *mut Interval) {
    Assert!(!INTERVAL_NOT_FINITE(span1));
    Assert!(!INTERVAL_NOT_FINITE(span2));

    if pg_sub_s32_overflow((*span1).month, (*span2).month, &mut (*result).month)
        || pg_sub_s32_overflow((*span1).day, (*span2).day, &mut (*result).day)
        || pg_sub_s64_overflow((*span1).time, (*span2).time, &mut (*result).time)
        || INTERVAL_NOT_FINITE(result)
    {
        ereport!(ERROR, errmsg!("interval out of range"));
    }
}

pub unsafe fn interval_mi(fcinfo: FunctionCallInfo) -> Datum {
    let span1 = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let span2 = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    /*
     * Handle infinities.
     *
     * We treat anything that amounts to "infinity - infinity" as an error,
     * since the interval type has nothing equivalent to NaN.
     */
    if INTERVAL_IS_NOBEGIN(span1) {
        if INTERVAL_IS_NOBEGIN(span2) {
            ereport!(ERROR, errmsg!("interval out of range"));
        } else {
            INTERVAL_NOBEGIN(result);
        }
    } else if INTERVAL_IS_NOEND(span1) {
        if INTERVAL_IS_NOEND(span2) {
            ereport!(ERROR, errmsg!("interval out of range"));
        } else {
            INTERVAL_NOEND(result);
        }
    } else if INTERVAL_IS_NOBEGIN(span2) {
        INTERVAL_NOEND(result);
    } else if INTERVAL_IS_NOEND(span2) {
        INTERVAL_NOBEGIN(result);
    } else {
        finite_interval_mi(span1, span2, result);
    }

    PG_RETURN_INTERVAL_P!(result);
}

/*
 *	There is no interval_abs():  it is unclear what value to return:
 *	  http://archives.postgresql.org/pgsql-general/2009-10/msg01031.php
 *	  http://archives.postgresql.org/pgsql-general/2009-11/msg00041.php
 */

pub unsafe fn interval_mul(fcinfo: FunctionCallInfo) -> Datum {
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let factor: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let mut month_remainder_days: f64;
    let mut sec_remainder: f64;
    let mut result_double: f64;
    let orig_month: int32 = (*span).month;
    let orig_day: int32 = (*span).day;
    let result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    'out_of_range: {
        /*
         * Handle NaN and infinities.
         *
         * We treat "0 * infinity" and "infinity * 0" as errors, since the
         * interval type has nothing equivalent to NaN.
         */
        if isnan(factor) != 0 {
            break 'out_of_range;
        }

        if INTERVAL_NOT_FINITE(span) {
            if factor == 0.0 {
                break 'out_of_range;
            }

            if factor < 0.0 {
                interval_um_internal(span, result);
            } else {
                memcpy(
                    result as *mut c_void,
                    span as *const c_void,
                    std::mem::size_of::<Interval>() as Size,
                );
            }

            PG_RETURN_INTERVAL_P!(result);
        }
        if isinf(factor) != 0 {
            let isign: c_int = interval_sign(span);

            if isign == 0 {
                break 'out_of_range;
            }

            if (factor * isign as f64) < 0.0 {
                INTERVAL_NOBEGIN(result);
            } else {
                INTERVAL_NOEND(result);
            }

            PG_RETURN_INTERVAL_P!(result);
        }

        result_double = (*span).month as f64 * factor;
        if isnan(result_double) != 0 || !FLOAT8_FITS_IN_INT32(result_double) {
            break 'out_of_range;
        }
        (*result).month = result_double as int32;

        result_double = (*span).day as f64 * factor;
        if isnan(result_double) != 0 || !FLOAT8_FITS_IN_INT32(result_double) {
            break 'out_of_range;
        }
        (*result).day = result_double as int32;

        /*
         * The above correctly handles the whole-number part of the month and day
         * products, but we have to do something with any fractional part
         * resulting when the factor is non-integral.  We cascade the fractions
         * down to lower units using the conversion factors DAYS_PER_MONTH and
         * SECS_PER_DAY.  Note we do NOT cascade up, since we are not forced to do
         * so by the representation.  The user can choose to cascade up later,
         * using justify_hours and/or justify_days.
         */

        /*
         * Fractional months full days into days.
         *
         * Floating point calculation are inherently imprecise, so these
         * calculations are crafted to produce the most reliable result possible.
         * TSROUND() is needed to more accurately produce whole numbers where
         * appropriate.
         */
        month_remainder_days =
            (orig_month as f64 * factor - (*result).month as f64) * DAYS_PER_MONTH as f64;
        month_remainder_days = TSROUND(month_remainder_days);
        sec_remainder = (orig_day as f64 * factor - (*result).day as f64 + month_remainder_days
            - month_remainder_days as c_int as f64)
            * SECS_PER_DAY as f64;
        sec_remainder = TSROUND(sec_remainder);

        /*
         * Might have 24:00:00 hours due to rounding, or >24 hours because of time
         * cascade from months and days.  It might still be >24 if the combination
         * of cascade and the seconds factor operation itself.
         */
        if fabs(sec_remainder) >= SECS_PER_DAY as f64 {
            if pg_add_s32_overflow(
                (*result).day,
                (sec_remainder / SECS_PER_DAY as f64) as c_int,
                &mut (*result).day,
            ) {
                break 'out_of_range;
            }
            sec_remainder -= (sec_remainder / SECS_PER_DAY as f64) as c_int as f64 * SECS_PER_DAY as f64;
        }

        /* cascade units down */
        if pg_add_s32_overflow((*result).day, month_remainder_days as int32, &mut (*result).day) {
            break 'out_of_range;
        }
        result_double = rint((*span).time as f64 * factor + sec_remainder * USECS_PER_SEC as f64);
        if isnan(result_double) != 0 || !FLOAT8_FITS_IN_INT64(result_double) {
            break 'out_of_range;
        }
        (*result).time = result_double as int64;

        if INTERVAL_NOT_FINITE(result) {
            break 'out_of_range;
        }

        PG_RETURN_INTERVAL_P!(result);
    }

    // out_of_range:
    ereport!(ERROR, errmsg!("interval out of range"));

    PG_RETURN_NULL!(fcinfo); /* keep compiler quiet */
}

pub unsafe fn mul_d_interval(fcinfo: FunctionCallInfo) -> Datum {
    /* Args are float8 and Interval *, but leave them as generic Datum */
    let factor: Datum = PG_GETARG_DATUM!(fcinfo, 0);
    let span: Datum = PG_GETARG_DATUM!(fcinfo, 1);

    DirectFunctionCall2!(interval_mul, span, factor)
}

pub unsafe fn interval_div(fcinfo: FunctionCallInfo) -> Datum {
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let factor: float8 = PG_GETARG_FLOAT8!(fcinfo, 1);
    let mut month_remainder_days: f64;
    let mut sec_remainder: f64;
    let mut result_double: f64;
    let orig_month: int32 = (*span).month;
    let orig_day: int32 = (*span).day;
    let result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    if factor == 0.0 {
        ereport!(ERROR, errmsg!("division by zero"));
    }

    'out_of_range: {
        /*
         * Handle NaN and infinities.
         *
         * We treat "infinity / infinity" as an error, since the interval type has
         * nothing equivalent to NaN.  Otherwise, dividing by infinity is handled
         * by the regular division code, causing all fields to be set to zero.
         */
        if isnan(factor) != 0 {
            break 'out_of_range;
        }

        if INTERVAL_NOT_FINITE(span) {
            if isinf(factor) != 0 {
                break 'out_of_range;
            }

            if factor < 0.0 {
                interval_um_internal(span, result);
            } else {
                memcpy(
                    result as *mut c_void,
                    span as *const c_void,
                    std::mem::size_of::<Interval>() as Size,
                );
            }

            PG_RETURN_INTERVAL_P!(result);
        }

        result_double = (*span).month as f64 / factor;
        if isnan(result_double) != 0 || !FLOAT8_FITS_IN_INT32(result_double) {
            break 'out_of_range;
        }
        (*result).month = result_double as int32;

        result_double = (*span).day as f64 / factor;
        if isnan(result_double) != 0 || !FLOAT8_FITS_IN_INT32(result_double) {
            break 'out_of_range;
        }
        (*result).day = result_double as int32;

        /*
         * Fractional months full days into days.  See comment in interval_mul().
         */
        month_remainder_days =
            (orig_month as f64 / factor - (*result).month as f64) * DAYS_PER_MONTH as f64;
        month_remainder_days = TSROUND(month_remainder_days);
        sec_remainder = (orig_day as f64 / factor - (*result).day as f64 + month_remainder_days
            - month_remainder_days as c_int as f64)
            * SECS_PER_DAY as f64;
        sec_remainder = TSROUND(sec_remainder);
        if fabs(sec_remainder) >= SECS_PER_DAY as f64 {
            if pg_add_s32_overflow(
                (*result).day,
                (sec_remainder / SECS_PER_DAY as f64) as c_int,
                &mut (*result).day,
            ) {
                break 'out_of_range;
            }
            sec_remainder -= (sec_remainder / SECS_PER_DAY as f64) as c_int as f64 * SECS_PER_DAY as f64;
        }

        /* cascade units down */
        if pg_add_s32_overflow((*result).day, month_remainder_days as int32, &mut (*result).day) {
            break 'out_of_range;
        }
        result_double = rint((*span).time as f64 / factor + sec_remainder * USECS_PER_SEC as f64);
        if isnan(result_double) != 0 || !FLOAT8_FITS_IN_INT64(result_double) {
            break 'out_of_range;
        }
        (*result).time = result_double as int64;

        if INTERVAL_NOT_FINITE(result) {
            break 'out_of_range;
        }

        PG_RETURN_INTERVAL_P!(result);
    }

    // out_of_range:
    ereport!(ERROR, errmsg!("interval out of range"));

    PG_RETURN_NULL!(fcinfo); /* keep compiler quiet */
}

/*
 * in_range support functions for timestamps and intervals.
 *
 * Per SQL spec, we support these with interval as the offset type.
 * The spec's restriction that the offset not be negative is a bit hard to
 * decipher for intervals, but we choose to interpret it the same as our
 * interval comparison operators would.
 */

pub unsafe fn in_range_timestamptz_interval(fcinfo: FunctionCallInfo) -> Datum {
    let val = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let base = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
    let offset = PG_GETARG_INTERVAL_P!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let sum: TimestampTz;

    if interval_sign(offset) < 0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }

    /*
     * Deal with cases where both base and offset are infinite, and computing
     * base +/- offset would cause an error.  As for float and numeric types,
     * we assume that all values infinitely precede +infinity and infinitely
     * follow -infinity.  See in_range_float8_float8() for reasoning.
     */
    if INTERVAL_IS_NOEND(offset)
        && (if sub {
            TIMESTAMP_IS_NOEND(base)
        } else {
            TIMESTAMP_IS_NOBEGIN(base)
        })
    {
        PG_RETURN_BOOL!(true);
    }

    /* We don't currently bother to avoid overflow hazards here */
    if sub {
        sum = timestamptz_mi_interval_internal(base, offset, null_mut());
    } else {
        sum = timestamptz_pl_interval_internal(base, offset, null_mut());
    }

    if less {
        PG_RETURN_BOOL!(val <= sum);
    } else {
        PG_RETURN_BOOL!(val >= sum);
    }
}

pub unsafe fn in_range_timestamp_interval(fcinfo: FunctionCallInfo) -> Datum {
    let val = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let base = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let offset = PG_GETARG_INTERVAL_P!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let sum: Timestamp;

    if interval_sign(offset) < 0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }

    /*
     * Deal with cases where both base and offset are infinite, and computing
     * base +/- offset would cause an error.  As for float and numeric types,
     * we assume that all values infinitely precede +infinity and infinitely
     * follow -infinity.  See in_range_float8_float8() for reasoning.
     */
    if INTERVAL_IS_NOEND(offset)
        && (if sub {
            TIMESTAMP_IS_NOEND(base)
        } else {
            TIMESTAMP_IS_NOBEGIN(base)
        })
    {
        PG_RETURN_BOOL!(true);
    }

    /* We don't currently bother to avoid overflow hazards here */
    if sub {
        sum = DatumGetTimestamp(DirectFunctionCall2!(
            timestamp_mi_interval,
            TimestampGetDatum(base),
            IntervalPGetDatum(offset)
        ));
    } else {
        sum = DatumGetTimestamp(DirectFunctionCall2!(
            timestamp_pl_interval,
            TimestampGetDatum(base),
            IntervalPGetDatum(offset)
        ));
    }

    if less {
        PG_RETURN_BOOL!(val <= sum);
    } else {
        PG_RETURN_BOOL!(val >= sum);
    }
}

pub unsafe fn in_range_interval_interval(fcinfo: FunctionCallInfo) -> Datum {
    let val = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let base = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let offset = PG_GETARG_INTERVAL_P!(fcinfo, 2);
    let sub: bool = PG_GETARG_BOOL!(fcinfo, 3);
    let less: bool = PG_GETARG_BOOL!(fcinfo, 4);
    let sum: *mut Interval;

    if interval_sign(offset) < 0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }

    /*
     * Deal with cases where both base and offset are infinite, and computing
     * base +/- offset would cause an error.  As for float and numeric types,
     * we assume that all values infinitely precede +infinity and infinitely
     * follow -infinity.  See in_range_float8_float8() for reasoning.
     */
    if INTERVAL_IS_NOEND(offset)
        && (if sub {
            INTERVAL_IS_NOEND(base)
        } else {
            INTERVAL_IS_NOBEGIN(base)
        })
    {
        PG_RETURN_BOOL!(true);
    }

    /* We don't currently bother to avoid overflow hazards here */
    if sub {
        sum = DatumGetIntervalP(DirectFunctionCall2!(
            interval_mi,
            IntervalPGetDatum(base),
            IntervalPGetDatum(offset)
        ));
    } else {
        sum = DatumGetIntervalP(DirectFunctionCall2!(
            interval_pl,
            IntervalPGetDatum(base),
            IntervalPGetDatum(offset)
        ));
    }

    if less {
        PG_RETURN_BOOL!(interval_cmp_internal(val, sum) <= 0);
    } else {
        PG_RETURN_BOOL!(interval_cmp_internal(val, sum) >= 0);
    }
}

/*
 * Prepare state data for an interval aggregate function, that needs to compute
 * sum and count, in the aggregate's memory context.
 *
 * The function is used when the state data needs to be allocated in aggregate's
 * context. When the state data needs to be allocated in the current memory
 * context, we use palloc0 directly e.g. interval_avg_deserialize().
 */
unsafe fn makeIntervalAggState(fcinfo: FunctionCallInfo) -> *mut IntervalAggState {
    let state: *mut IntervalAggState;
    let mut agg_context: MemoryContext = null_mut();
    let old_context: MemoryContext;

    if !AggCheckCallContext(fcinfo, &mut agg_context) {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    old_context = MemoryContextSwitchTo(agg_context);

    state = palloc0(std::mem::size_of::<IntervalAggState>() as Size) as *mut IntervalAggState;

    MemoryContextSwitchTo(old_context);

    state
}

/*
 * Accumulate a new input value for interval aggregate functions.
 */
unsafe fn do_interval_accum(state: *mut IntervalAggState, newval: *mut Interval) {
    /* Infinite inputs are counted separately, and do not affect "N" */
    if INTERVAL_IS_NOBEGIN(newval) {
        (*state).nInfcount += 1;
        return;
    }

    if INTERVAL_IS_NOEND(newval) {
        (*state).pInfcount += 1;
        return;
    }

    finite_interval_pl(&(*state).sumX, newval, &mut (*state).sumX);
    (*state).N += 1;
}

/*
 * Remove the given interval value from the aggregated state.
 */
unsafe fn do_interval_discard(state: *mut IntervalAggState, newval: *mut Interval) {
    /* Infinite inputs are counted separately, and do not affect "N" */
    if INTERVAL_IS_NOBEGIN(newval) {
        (*state).nInfcount -= 1;
        return;
    }

    if INTERVAL_IS_NOEND(newval) {
        (*state).pInfcount -= 1;
        return;
    }

    /* Handle the to-be-discarded finite value. */
    (*state).N -= 1;
    if (*state).N > 0 {
        finite_interval_mi(&(*state).sumX, newval, &mut (*state).sumX);
    } else {
        /* All values discarded, reset the state */
        Assert!((*state).N == 0);
        memset(
            &mut (*state).sumX as *mut Interval as *mut c_void,
            0,
            std::mem::size_of::<Interval>() as Size,
        );
    }
}

/*
 * Transition function for sum() and avg() interval aggregates.
 */
pub unsafe fn interval_avg_accum(fcinfo: FunctionCallInfo) -> Datum {
    let mut state: *mut IntervalAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut IntervalAggState
    };

    /* Create the state data on the first call */
    if state.is_null() {
        state = makeIntervalAggState(fcinfo);
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_interval_accum(state, PG_GETARG_INTERVAL_P!(fcinfo, 1));
    }

    PG_RETURN_POINTER!(state);
}

/*
 * Combine function for sum() and avg() interval aggregates.
 *
 * Combine the given internal aggregate states and place the combination in
 * the first argument.
 */
pub unsafe fn interval_avg_combine(fcinfo: FunctionCallInfo) -> Datum {
    let mut state1: *mut IntervalAggState;
    let state2: *mut IntervalAggState;

    state1 = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut IntervalAggState
    };
    state2 = if PG_ARGISNULL!(fcinfo, 1) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 1) as *mut IntervalAggState
    };

    if state2.is_null() {
        PG_RETURN_POINTER!(state1);
    }

    if state1.is_null() {
        /* manually copy all fields from state2 to state1 */
        state1 = makeIntervalAggState(fcinfo);

        (*state1).N = (*state2).N;
        (*state1).pInfcount = (*state2).pInfcount;
        (*state1).nInfcount = (*state2).nInfcount;

        (*state1).sumX.day = (*state2).sumX.day;
        (*state1).sumX.month = (*state2).sumX.month;
        (*state1).sumX.time = (*state2).sumX.time;

        PG_RETURN_POINTER!(state1);
    }

    (*state1).N += (*state2).N;
    (*state1).pInfcount += (*state2).pInfcount;
    (*state1).nInfcount += (*state2).nInfcount;

    /* Accumulate finite interval values, if any. */
    if (*state2).N > 0 {
        finite_interval_pl(&(*state1).sumX, &(*state2).sumX, &mut (*state1).sumX);
    }

    PG_RETURN_POINTER!(state1);
}

/*
 * interval_avg_serialize
 *		Serialize IntervalAggState for interval aggregates.
 */
pub unsafe fn interval_avg_serialize(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut IntervalAggState;
    let mut buf: StringInfoData = std::mem::zeroed();
    let result: *mut bytea;

    /* Ensure we disallow calling when not in aggregate context */
    if !AggCheckCallContext(fcinfo, null_mut()) {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    state = PG_GETARG_POINTER!(fcinfo, 0) as *mut IntervalAggState;

    pq_begintypsend(&mut buf);

    /* N */
    pq_sendint64(&mut buf, (*state).N as uint64);

    /* sumX */
    pq_sendint64(&mut buf, (*state).sumX.time as uint64);
    pq_sendint32(&mut buf, (*state).sumX.day as u32);
    pq_sendint32(&mut buf, (*state).sumX.month as u32);

    /* pInfcount */
    pq_sendint64(&mut buf, (*state).pInfcount as uint64);

    /* nInfcount */
    pq_sendint64(&mut buf, (*state).nInfcount as uint64);

    result = pq_endtypsend(&mut buf);

    PG_RETURN_BYTEA_P!(result);
}

/*
 * interval_avg_deserialize
 *		Deserialize bytea into IntervalAggState for interval aggregates.
 */
pub unsafe fn interval_avg_deserialize(fcinfo: FunctionCallInfo) -> Datum {
    let sstate: *mut bytea;
    let result: *mut IntervalAggState;
    let mut buf: StringInfoData = std::mem::zeroed();

    if !AggCheckCallContext(fcinfo, null_mut()) {
        elog!(ERROR, "aggregate function called in non-aggregate context");
    }

    sstate = PG_GETARG_BYTEA_PP!(fcinfo, 0);

    /*
     * Initialize a StringInfo so that we can "receive" it using the standard
     * recv-function infrastructure.
     */
    initReadOnlyStringInfo(
        &mut buf,
        VARDATA_ANY(sstate as *const c_char) as *mut c_char,
        VARSIZE_ANY_EXHDR(sstate as *const c_char) as c_int,
    );

    result = palloc0(std::mem::size_of::<IntervalAggState>() as Size) as *mut IntervalAggState;

    /* N */
    (*result).N = pq_getmsgint64(&mut buf);

    /* sumX */
    (*result).sumX.time = pq_getmsgint64(&mut buf);
    (*result).sumX.day = pq_getmsgint(&mut buf, 4) as int32;
    (*result).sumX.month = pq_getmsgint(&mut buf, 4) as int32;

    /* pInfcount */
    (*result).pInfcount = pq_getmsgint64(&mut buf);

    /* nInfcount */
    (*result).nInfcount = pq_getmsgint64(&mut buf);

    pq_getmsgend(&mut buf);

    PG_RETURN_POINTER!(result);
}

/*
 * Inverse transition function for sum() and avg() interval aggregates.
 */
pub unsafe fn interval_avg_accum_inv(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut IntervalAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut IntervalAggState
    };

    /* Should not get here with no state */
    if state.is_null() {
        elog!(ERROR, "interval_avg_accum_inv called with NULL state");
    }

    if !PG_ARGISNULL!(fcinfo, 1) {
        do_interval_discard(state, PG_GETARG_INTERVAL_P!(fcinfo, 1));
    }

    PG_RETURN_POINTER!(state);
}

/* avg(interval) aggregate final function */
pub unsafe fn interval_avg(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut IntervalAggState;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut IntervalAggState
    };

    /* If there were no non-null inputs, return NULL */
    if state.is_null() || IA_TOTAL_COUNT(state) == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    /*
     * Aggregating infinities that all have the same sign produces infinity
     * with that sign.  Aggregating infinities with different signs results in
     * an error.
     */
    if (*state).pInfcount > 0 || (*state).nInfcount > 0 {
        let result: *mut Interval;

        if (*state).pInfcount > 0 && (*state).nInfcount > 0 {
            ereport!(ERROR, errmsg!("interval out of range"));
        }

        result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;
        if (*state).pInfcount > 0 {
            INTERVAL_NOEND(result);
        } else {
            INTERVAL_NOBEGIN(result);
        }

        PG_RETURN_INTERVAL_P!(result);
    }

    DirectFunctionCall2!(
        interval_div,
        IntervalPGetDatum(&(*state).sumX),
        Float8GetDatum((*state).N as f64)
    )
}

/* sum(interval) aggregate final function */
pub unsafe fn interval_sum(fcinfo: FunctionCallInfo) -> Datum {
    let state: *mut IntervalAggState;
    let result: *mut Interval;

    state = if PG_ARGISNULL!(fcinfo, 0) {
        null_mut()
    } else {
        PG_GETARG_POINTER!(fcinfo, 0) as *mut IntervalAggState
    };

    /* If there were no non-null inputs, return NULL */
    if state.is_null() || IA_TOTAL_COUNT(state) == 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    /*
     * Aggregating infinities that all have the same sign produces infinity
     * with that sign.  Aggregating infinities with different signs results in
     * an error.
     */
    if (*state).pInfcount > 0 && (*state).nInfcount > 0 {
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    if (*state).pInfcount > 0 {
        INTERVAL_NOEND(result);
    } else if (*state).nInfcount > 0 {
        INTERVAL_NOBEGIN(result);
    } else {
        memcpy(
            result as *mut c_void,
            &(*state).sumX as *const Interval as *const c_void,
            std::mem::size_of::<Interval>() as Size,
        );
    }

    PG_RETURN_INTERVAL_P!(result);
}

/* timestamp_age()
 * Calculate time difference while retaining year/month fields.
 * Note that this does not result in an accurate absolute time span
 *	since year and month are out of context once the arithmetic
 *	is done.
 */
pub unsafe fn timestamp_age(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let result: *mut Interval;
    let mut fsec1: fsec_t = 0;
    let mut fsec2: fsec_t = 0;
    let mut tt: pg_itm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_itm;
    let mut tt1: pg_tm = std::mem::zeroed();
    let tm1 = &mut tt1 as *mut pg_tm;
    let mut tt2: pg_tm = std::mem::zeroed();
    let tm2 = &mut tt2 as *mut pg_tm;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    /*
     * Handle infinities.
     *
     * We treat anything that amounts to "infinity - infinity" as an error,
     * since the interval type has nothing equivalent to NaN.
     */
    if TIMESTAMP_IS_NOBEGIN(dt1) {
        if TIMESTAMP_IS_NOBEGIN(dt2) {
            ereport!(ERROR, errmsg!("interval out of range"));
        } else {
            INTERVAL_NOBEGIN(result);
        }
    } else if TIMESTAMP_IS_NOEND(dt1) {
        if TIMESTAMP_IS_NOEND(dt2) {
            ereport!(ERROR, errmsg!("interval out of range"));
        } else {
            INTERVAL_NOEND(result);
        }
    } else if TIMESTAMP_IS_NOBEGIN(dt2) {
        INTERVAL_NOEND(result);
    } else if TIMESTAMP_IS_NOEND(dt2) {
        INTERVAL_NOBEGIN(result);
    } else if timestamp2tm(dt1, null_mut(), tm1, &mut fsec1, null_mut(), null_mut()) == 0
        && timestamp2tm(dt2, null_mut(), tm2, &mut fsec2, null_mut(), null_mut()) == 0
    {
        /* form the symbolic difference */
        (*tm).tm_usec = (fsec1 - fsec2) as c_int;
        (*tm).tm_sec = (*tm1).tm_sec - (*tm2).tm_sec;
        (*tm).tm_min = (*tm1).tm_min - (*tm2).tm_min;
        (*tm).tm_hour = ((*tm1).tm_hour - (*tm2).tm_hour) as int64;
        (*tm).tm_mday = (*tm1).tm_mday - (*tm2).tm_mday;
        (*tm).tm_mon = (*tm1).tm_mon - (*tm2).tm_mon;
        (*tm).tm_year = (*tm1).tm_year - (*tm2).tm_year;

        /* flip sign if necessary... */
        if dt1 < dt2 {
            (*tm).tm_usec = -(*tm).tm_usec;
            (*tm).tm_sec = -(*tm).tm_sec;
            (*tm).tm_min = -(*tm).tm_min;
            (*tm).tm_hour = -(*tm).tm_hour;
            (*tm).tm_mday = -(*tm).tm_mday;
            (*tm).tm_mon = -(*tm).tm_mon;
            (*tm).tm_year = -(*tm).tm_year;
        }

        /* propagate any negative fields into the next higher field */
        while (*tm).tm_usec < 0 {
            (*tm).tm_usec += USECS_PER_SEC as c_int;
            (*tm).tm_sec -= 1;
        }

        while (*tm).tm_sec < 0 {
            (*tm).tm_sec += SECS_PER_MINUTE;
            (*tm).tm_min -= 1;
        }

        while (*tm).tm_min < 0 {
            (*tm).tm_min += MINS_PER_HOUR;
            (*tm).tm_hour -= 1;
        }

        while (*tm).tm_hour < 0 {
            (*tm).tm_hour += HOURS_PER_DAY as int64;
            (*tm).tm_mday -= 1;
        }

        while (*tm).tm_mday < 0 {
            if dt1 < dt2 {
                (*tm).tm_mday += day_tab(isleap((*tm1).tm_year), ((*tm1).tm_mon - 1) as usize);
                (*tm).tm_mon -= 1;
            } else {
                (*tm).tm_mday += day_tab(isleap((*tm2).tm_year), ((*tm2).tm_mon - 1) as usize);
                (*tm).tm_mon -= 1;
            }
        }

        while (*tm).tm_mon < 0 {
            (*tm).tm_mon += MONTHS_PER_YEAR;
            (*tm).tm_year -= 1;
        }

        /* recover sign if necessary... */
        if dt1 < dt2 {
            (*tm).tm_usec = -(*tm).tm_usec;
            (*tm).tm_sec = -(*tm).tm_sec;
            (*tm).tm_min = -(*tm).tm_min;
            (*tm).tm_hour = -(*tm).tm_hour;
            (*tm).tm_mday = -(*tm).tm_mday;
            (*tm).tm_mon = -(*tm).tm_mon;
            (*tm).tm_year = -(*tm).tm_year;
        }

        if itm2interval(tm, result) != 0 {
            ereport!(ERROR, errmsg!("interval out of range"));
        }
    } else {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    PG_RETURN_INTERVAL_P!(result);
}

/* timestamptz_age()
 * Calculate time difference while retaining year/month fields.
 * Note that this does not result in an accurate absolute time span
 *	since year and month are out of context once the arithmetic
 *	is done.
 */
pub unsafe fn timestamptz_age(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
    let result: *mut Interval;
    let mut fsec1: fsec_t = 0;
    let mut fsec2: fsec_t = 0;
    let mut tt: pg_itm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_itm;
    let mut tt1: pg_tm = std::mem::zeroed();
    let tm1 = &mut tt1 as *mut pg_tm;
    let mut tt2: pg_tm = std::mem::zeroed();
    let tm2 = &mut tt2 as *mut pg_tm;
    let mut tz1: c_int = 0;
    let mut tz2: c_int = 0;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    /*
     * Handle infinities.
     *
     * We treat anything that amounts to "infinity - infinity" as an error,
     * since the interval type has nothing equivalent to NaN.
     */
    if TIMESTAMP_IS_NOBEGIN(dt1) {
        if TIMESTAMP_IS_NOBEGIN(dt2) {
            ereport!(ERROR, errmsg!("interval out of range"));
        } else {
            INTERVAL_NOBEGIN(result);
        }
    } else if TIMESTAMP_IS_NOEND(dt1) {
        if TIMESTAMP_IS_NOEND(dt2) {
            ereport!(ERROR, errmsg!("interval out of range"));
        } else {
            INTERVAL_NOEND(result);
        }
    } else if TIMESTAMP_IS_NOBEGIN(dt2) {
        INTERVAL_NOEND(result);
    } else if TIMESTAMP_IS_NOEND(dt2) {
        INTERVAL_NOBEGIN(result);
    } else if timestamp2tm(dt1, &mut tz1, tm1, &mut fsec1, null_mut(), null_mut()) == 0
        && timestamp2tm(dt2, &mut tz2, tm2, &mut fsec2, null_mut(), null_mut()) == 0
    {
        /* form the symbolic difference */
        (*tm).tm_usec = (fsec1 - fsec2) as c_int;
        (*tm).tm_sec = (*tm1).tm_sec - (*tm2).tm_sec;
        (*tm).tm_min = (*tm1).tm_min - (*tm2).tm_min;
        (*tm).tm_hour = ((*tm1).tm_hour - (*tm2).tm_hour) as int64;
        (*tm).tm_mday = (*tm1).tm_mday - (*tm2).tm_mday;
        (*tm).tm_mon = (*tm1).tm_mon - (*tm2).tm_mon;
        (*tm).tm_year = (*tm1).tm_year - (*tm2).tm_year;

        /* flip sign if necessary... */
        if dt1 < dt2 {
            (*tm).tm_usec = -(*tm).tm_usec;
            (*tm).tm_sec = -(*tm).tm_sec;
            (*tm).tm_min = -(*tm).tm_min;
            (*tm).tm_hour = -(*tm).tm_hour;
            (*tm).tm_mday = -(*tm).tm_mday;
            (*tm).tm_mon = -(*tm).tm_mon;
            (*tm).tm_year = -(*tm).tm_year;
        }

        /* propagate any negative fields into the next higher field */
        while (*tm).tm_usec < 0 {
            (*tm).tm_usec += USECS_PER_SEC as c_int;
            (*tm).tm_sec -= 1;
        }

        while (*tm).tm_sec < 0 {
            (*tm).tm_sec += SECS_PER_MINUTE;
            (*tm).tm_min -= 1;
        }

        while (*tm).tm_min < 0 {
            (*tm).tm_min += MINS_PER_HOUR;
            (*tm).tm_hour -= 1;
        }

        while (*tm).tm_hour < 0 {
            (*tm).tm_hour += HOURS_PER_DAY as int64;
            (*tm).tm_mday -= 1;
        }

        while (*tm).tm_mday < 0 {
            if dt1 < dt2 {
                (*tm).tm_mday += day_tab(isleap((*tm1).tm_year), ((*tm1).tm_mon - 1) as usize);
                (*tm).tm_mon -= 1;
            } else {
                (*tm).tm_mday += day_tab(isleap((*tm2).tm_year), ((*tm2).tm_mon - 1) as usize);
                (*tm).tm_mon -= 1;
            }
        }

        while (*tm).tm_mon < 0 {
            (*tm).tm_mon += MONTHS_PER_YEAR;
            (*tm).tm_year -= 1;
        }

        /*
         * Note: we deliberately ignore any difference between tz1 and tz2.
         */

        /* recover sign if necessary... */
        if dt1 < dt2 {
            (*tm).tm_usec = -(*tm).tm_usec;
            (*tm).tm_sec = -(*tm).tm_sec;
            (*tm).tm_min = -(*tm).tm_min;
            (*tm).tm_hour = -(*tm).tm_hour;
            (*tm).tm_mday = -(*tm).tm_mday;
            (*tm).tm_mon = -(*tm).tm_mon;
            (*tm).tm_year = -(*tm).tm_year;
        }

        if itm2interval(tm, result) != 0 {
            ereport!(ERROR, errmsg!("interval out of range"));
        }
    } else {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    PG_RETURN_INTERVAL_P!(result);
}

/* timestamp_bin()
 * Bin timestamp into specified interval using specified origin.
 */
pub unsafe fn timestamp_bin(fcinfo: FunctionCallInfo) -> Datum {
    let stride = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let origin = PG_GETARG_TIMESTAMP!(fcinfo, 2);
    let mut result: Timestamp;
    let mut stride_usecs: Timestamp = 0;
    let mut tm_diff: Timestamp = 0;
    let tm_modulo: Timestamp;
    let tm_delta: Timestamp;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_TIMESTAMP!(timestamp);
    }

    if TIMESTAMP_NOT_FINITE(origin) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("origin out of range"));
    }

    if INTERVAL_NOT_FINITE(stride) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("timestamps cannot be binned into infinite intervals"));
    }

    if (*stride).month != 0 {
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        ereport!(ERROR, errmsg!("timestamps cannot be binned into intervals containing months or years"));
    }

    if pg_mul_s64_overflow((*stride).day as int64, USECS_PER_DAY, &mut stride_usecs)
        || pg_add_s64_overflow(stride_usecs, (*stride).time, &mut stride_usecs)
    {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    if stride_usecs <= 0 {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("stride must be greater than zero"));
    }

    if pg_sub_s64_overflow(timestamp, origin, &mut tm_diff) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    /* These calculations cannot overflow */
    tm_modulo = tm_diff % stride_usecs;
    tm_delta = tm_diff - tm_modulo;
    result = origin + tm_delta;

    /*
     * We want to round towards -infinity, not 0, when tm_diff is negative and
     * not a multiple of stride_usecs.  This adjustment *can* cause overflow,
     * since the result might now be out of the range origin .. timestamp.
     */
    if tm_modulo < 0 {
        if pg_sub_s64_overflow(result, stride_usecs, &mut result) || !IS_VALID_TIMESTAMP(result) {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
    }

    PG_RETURN_TIMESTAMP!(result);
}

/* timestamp_trunc()
 * Truncate timestamp to specified units.
 */
pub unsafe fn timestamp_trunc(fcinfo: FunctionCallInfo) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let mut result: Timestamp = 0;
    let r#type: c_int;
    let mut val: c_int = 0;
    let lowunits: *mut c_char;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;

    lowunits = downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units) as c_int, false);

    r#type = DecodeUnits(0, lowunits, &mut val);

    if r#type == UNITS {
        if TIMESTAMP_NOT_FINITE(timestamp) {
            /*
             * Errors thrown here for invalid units should exactly match those
             * below, else there will be unexpected discrepancies between
             * finite- and infinite-input cases.
             */
            match val {
                DTK_WEEK | DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
                | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND | DTK_MILLISEC
                | DTK_MICROSEC => {
                    PG_RETURN_TIMESTAMP!(timestamp);
                }
                _ => {
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPOID)).to_string_lossy()));
                }
            }
        }

        if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        /* fall-through cascade as in C */
        let mut matched = true;
        if val == DTK_WEEK {
            let mut woy: c_int = date2isoweek((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday);
            /*
             * If it is week 52/53 and the month is January, then the week
             * must belong to the previous year. Also, some December dates
             * belong to the next year.
             */
            if woy >= 52 && (*tm).tm_mon == 1 {
                (*tm).tm_year -= 1;
            }
            if woy <= 1 && (*tm).tm_mon == MONTHS_PER_YEAR {
                (*tm).tm_year += 1;
            }
            isoweek2date(woy, &mut (*tm).tm_year, &mut (*tm).tm_mon, &mut (*tm).tm_mday);
            let _ = &mut woy;
            (*tm).tm_hour = 0;
            (*tm).tm_min = 0;
            (*tm).tm_sec = 0;
            fsec = 0;
        } else if val == DTK_MICROSEC {
            /* nothing */
        } else if val == DTK_MILLISEC {
            fsec = (fsec / 1000) * 1000;
        } else if val >= DTK_SECOND
            && matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
                | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND)
        {
            if val == DTK_MILLENNIUM {
                /* see comments in timestamptz_trunc */
                if (*tm).tm_year > 0 {
                    (*tm).tm_year = (((*tm).tm_year + 999) / 1000) * 1000 - 999;
                } else {
                    (*tm).tm_year = -((999 - ((*tm).tm_year - 1)) / 1000) * 1000 + 1;
                }
            }
            if val == DTK_MILLENNIUM || val == DTK_CENTURY {
                /* see comments in timestamptz_trunc */
                if (*tm).tm_year > 0 {
                    (*tm).tm_year = (((*tm).tm_year + 99) / 100) * 100 - 99;
                } else {
                    (*tm).tm_year = -((99 - ((*tm).tm_year - 1)) / 100) * 100 + 1;
                }
            }
            if val == DTK_DECADE {
                /* see comments in timestamptz_trunc */
                if val != DTK_MILLENNIUM && val != DTK_CENTURY {
                    if (*tm).tm_year > 0 {
                        (*tm).tm_year = ((*tm).tm_year / 10) * 10;
                    } else {
                        (*tm).tm_year = -((8 - ((*tm).tm_year - 1)) / 10) * 10;
                    }
                }
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR) {
                (*tm).tm_mon = 1;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER) {
                (*tm).tm_mon = (3 * (((*tm).tm_mon - 1) / 3)) + 1;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH) {
                (*tm).tm_mday = 1;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY) {
                (*tm).tm_hour = 0;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY | DTK_HOUR) {
                (*tm).tm_min = 0;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE) {
                (*tm).tm_sec = 0;
            }
            fsec = 0;
        } else {
            matched = false;
        }

        if !matched {
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPOID)).to_string_lossy()));
        }

        if tm2timestamp(tm, fsec, null_mut(), &mut result) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
    } else {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("unit \"{}\" not recognized for type {}",
            std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPOID)).to_string_lossy()));
    }

    PG_RETURN_TIMESTAMP!(result);
}

/* timestamptz_bin()
 * Bin timestamptz into specified interval using specified origin.
 */
pub unsafe fn timestamptz_bin(fcinfo: FunctionCallInfo) -> Datum {
    let stride = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
    let origin = PG_GETARG_TIMESTAMPTZ!(fcinfo, 2);
    let mut result: TimestampTz;
    let mut stride_usecs: TimestampTz = 0;
    let mut tm_diff: TimestampTz = 0;
    let tm_modulo: TimestampTz;
    let tm_delta: TimestampTz;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_TIMESTAMPTZ!(timestamp);
    }

    if TIMESTAMP_NOT_FINITE(origin) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("origin out of range"));
    }

    if INTERVAL_NOT_FINITE(stride) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("timestamps cannot be binned into infinite intervals"));
    }

    if (*stride).month != 0 {
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        ereport!(ERROR, errmsg!("timestamps cannot be binned into intervals containing months or years"));
    }

    if pg_mul_s64_overflow((*stride).day as int64, USECS_PER_DAY, &mut stride_usecs)
        || pg_add_s64_overflow(stride_usecs, (*stride).time, &mut stride_usecs)
    {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    if stride_usecs <= 0 {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("stride must be greater than zero"));
    }

    if pg_sub_s64_overflow(timestamp, origin, &mut tm_diff) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("interval out of range"));
    }

    /* These calculations cannot overflow */
    tm_modulo = tm_diff % stride_usecs;
    tm_delta = tm_diff - tm_modulo;
    result = origin + tm_delta;

    /*
     * We want to round towards -infinity, not 0, when tm_diff is negative and
     * not a multiple of stride_usecs.  This adjustment *can* cause overflow,
     * since the result might now be out of the range origin .. timestamp.
     */
    if tm_modulo < 0 {
        if pg_sub_s64_overflow(result, stride_usecs, &mut result) || !IS_VALID_TIMESTAMP(result) {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
    }

    PG_RETURN_TIMESTAMPTZ!(result);
}

/*
 * Common code for timestamptz_trunc() and timestamptz_trunc_zone().
 *
 * tzp identifies the zone to truncate with respect to.  We assume
 * infinite timestamps have already been rejected.
 */
unsafe fn timestamptz_trunc_internal(units: *mut text, timestamp: TimestampTz, tzp: *mut pg_tz) -> TimestampTz {
    let mut result: TimestampTz = 0;
    let mut tz: c_int = 0;
    let r#type: c_int;
    let mut val: c_int = 0;
    let mut redotz: bool = false;
    let lowunits: *mut c_char;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;

    lowunits = downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units) as c_int, false);

    r#type = DecodeUnits(0, lowunits, &mut val);

    if r#type == UNITS {
        if TIMESTAMP_NOT_FINITE(timestamp) {
            /*
             * Errors thrown here for invalid units should exactly match those
             * below, else there will be unexpected discrepancies between
             * finite- and infinite-input cases.
             */
            match val {
                DTK_WEEK | DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
                | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND | DTK_MILLISEC
                | DTK_MICROSEC => {
                    return timestamp;
                }
                _ => {
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                    ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPTZOID)).to_string_lossy()));
                }
            }
        }

        if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), tzp) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        let mut matched = true;
        if val == DTK_WEEK {
            let woy: c_int = date2isoweek((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday);
            /*
             * If it is week 52/53 and the month is January, then the week
             * must belong to the previous year. Also, some December dates
             * belong to the next year.
             */
            if woy >= 52 && (*tm).tm_mon == 1 {
                (*tm).tm_year -= 1;
            }
            if woy <= 1 && (*tm).tm_mon == MONTHS_PER_YEAR {
                (*tm).tm_year += 1;
            }
            isoweek2date(woy, &mut (*tm).tm_year, &mut (*tm).tm_mon, &mut (*tm).tm_mday);
            (*tm).tm_hour = 0;
            (*tm).tm_min = 0;
            (*tm).tm_sec = 0;
            fsec = 0;
            redotz = true;
        } else if val == DTK_MICROSEC {
            /* nothing */
        } else if val == DTK_MILLISEC {
            fsec = (fsec / 1000) * 1000;
        } else if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
            | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND)
        {
            if val == DTK_MILLENNIUM {
                /*
                 * truncating to the millennium? what is this supposed to
                 * mean? let us put the first year of the millennium... i.e.
                 * -1000, 1, 1001, 2001...
                 */
                if (*tm).tm_year > 0 {
                    (*tm).tm_year = (((*tm).tm_year + 999) / 1000) * 1000 - 999;
                } else {
                    (*tm).tm_year = -((999 - ((*tm).tm_year - 1)) / 1000) * 1000 + 1;
                }
            }
            if val == DTK_MILLENNIUM || val == DTK_CENTURY {
                /* truncating to the century? as above: -100, 1, 101... */
                if (*tm).tm_year > 0 {
                    (*tm).tm_year = (((*tm).tm_year + 99) / 100) * 100 - 99;
                } else {
                    (*tm).tm_year = -((99 - ((*tm).tm_year - 1)) / 100) * 100 + 1;
                }
            }
            if val == DTK_DECADE {
                /*
                 * truncating to the decade? first year of the decade. must
                 * not be applied if year was truncated before!
                 */
                if val != DTK_MILLENNIUM && val != DTK_CENTURY {
                    if (*tm).tm_year > 0 {
                        (*tm).tm_year = ((*tm).tm_year / 10) * 10;
                    } else {
                        (*tm).tm_year = -((8 - ((*tm).tm_year - 1)) / 10) * 10;
                    }
                }
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR) {
                (*tm).tm_mon = 1;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER) {
                (*tm).tm_mon = (3 * (((*tm).tm_mon - 1) / 3)) + 1;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH) {
                (*tm).tm_mday = 1;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY) {
                (*tm).tm_hour = 0;
                redotz = true; /* for all cases >= DAY */
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY | DTK_HOUR) {
                (*tm).tm_min = 0;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE) {
                (*tm).tm_sec = 0;
            }
            fsec = 0;
        } else {
            matched = false;
        }

        if !matched {
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPTZOID)).to_string_lossy()));
        }

        if redotz {
            tz = DetermineTimeZoneOffset(tm, tzp);
        }

        if tm2timestamp(tm, fsec, &mut tz, &mut result) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
    } else {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("unit \"{}\" not recognized for type {}",
            std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPTZOID)).to_string_lossy()));
    }

    result
}

/* timestamptz_trunc()
 * Truncate timestamptz to specified units in session timezone.
 */
pub unsafe fn timestamptz_trunc(fcinfo: FunctionCallInfo) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
    let result: TimestampTz = timestamptz_trunc_internal(units, timestamp, session_timezone);

    PG_RETURN_TIMESTAMPTZ!(result);
}

/* timestamptz_trunc_zone()
 * Truncate timestamptz to specified units in specified timezone.
 */
pub unsafe fn timestamptz_trunc_zone(fcinfo: FunctionCallInfo) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
    let zone = PG_GETARG_TEXT_PP!(fcinfo, 2);
    let result: TimestampTz;
    let tzp: *mut pg_tz;

    /*
     * Look up the requested timezone.
     */
    tzp = lookup_timezone(zone);

    result = timestamptz_trunc_internal(units, timestamp, tzp);

    PG_RETURN_TIMESTAMPTZ!(result);
}

/* interval_trunc()
 * Extract specified field from interval.
 */
pub unsafe fn interval_trunc(fcinfo: FunctionCallInfo) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let interval = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let result: *mut Interval;
    let r#type: c_int;
    let mut val: c_int = 0;
    let lowunits: *mut c_char;
    let mut tt: pg_itm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_itm;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    lowunits = downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units) as c_int, false);

    r#type = DecodeUnits(0, lowunits, &mut val);

    if r#type == UNITS {
        if INTERVAL_NOT_FINITE(interval) {
            /*
             * Errors thrown here for invalid units should exactly match those
             * below, else there will be unexpected discrepancies between
             * finite- and infinite-input cases.
             */
            match val {
                DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH
                | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND | DTK_MILLISEC | DTK_MICROSEC => {
                    memcpy(result as *mut c_void, interval as *const c_void, std::mem::size_of::<Interval>() as Size);
                    PG_RETURN_INTERVAL_P!(result);
                }
                _ => {
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED); */
                    /* C also: (val == DTK_WEEK) ? errdetail("Months usually have fractional weeks.") : 0 */
                    ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(INTERVALOID)).to_string_lossy()));
                }
            }
        }

        interval2itm(*interval, tm);

        let mut matched = true;
        if val == DTK_MICROSEC {
            /* nothing */
        } else if val == DTK_MILLISEC {
            (*tm).tm_usec = ((*tm).tm_usec / 1000) * 1000;
        } else if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER
            | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE | DTK_SECOND)
        {
            if val == DTK_MILLENNIUM {
                /* caution: C division may have negative remainder */
                (*tm).tm_year = ((*tm).tm_year / 1000) * 1000;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY) {
                /* caution: C division may have negative remainder */
                (*tm).tm_year = ((*tm).tm_year / 100) * 100;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE) {
                /* caution: C division may have negative remainder */
                (*tm).tm_year = ((*tm).tm_year / 10) * 10;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR) {
                (*tm).tm_mon = 0;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER) {
                (*tm).tm_mon = 3 * ((*tm).tm_mon / 3);
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH) {
                (*tm).tm_mday = 0;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY) {
                (*tm).tm_hour = 0;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY | DTK_HOUR) {
                (*tm).tm_min = 0;
            }
            if matches!(val, DTK_MILLENNIUM | DTK_CENTURY | DTK_DECADE | DTK_YEAR | DTK_QUARTER | DTK_MONTH | DTK_DAY | DTK_HOUR | DTK_MINUTE) {
                (*tm).tm_sec = 0;
            }
            (*tm).tm_usec = 0;
        } else {
            matched = false;
        }

        if !matched {
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED); */
            /* C also: (val == DTK_WEEK) ? errdetail("Months usually have fractional weeks.") : 0 */
            ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(INTERVALOID)).to_string_lossy()));
        }

        if itm2interval(tm, result) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("interval out of range"));
        }
    } else {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("unit \"{}\" not recognized for type {}",
            std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(INTERVALOID)).to_string_lossy()));
    }

    PG_RETURN_INTERVAL_P!(result);
}
/* isoweek2j()
 *
 *	Return the Julian day which corresponds to the first day (Monday) of the given ISO 8601 year and week.
 *	Julian days are used to convert between ISO week dates and Gregorian dates.
 *
 *	XXX: This function has integer overflow hazards, but restructuring it to
 *	work with the soft-error handling that its callers do is likely more
 *	trouble than it's worth.
 */
pub unsafe fn isoweek2j(year: c_int, week: c_int) -> c_int {
    let day0: c_int;
    let day4: c_int;

    /* fourth day of current year */
    day4 = date2j(year, 1, 4);

    /* day0 == offset to first day of week (Monday) */
    day0 = j2day(day4 - 1);

    ((week - 1) * 7) + (day4 - day0)
}

/* isoweek2date()
 * Convert ISO week of year number to date.
 * The year field must be specified with the ISO year!
 * karel 2000/08/07
 */
pub unsafe fn isoweek2date(woy: c_int, year: *mut c_int, mon: *mut c_int, mday: *mut c_int) {
    j2date(isoweek2j(*year, woy), year, mon, mday);
}

/* isoweekdate2date()
 *
 *	Convert an ISO 8601 week date (ISO year, ISO week) into a Gregorian date.
 *	Gregorian day of week sent so weekday strings can be supplied.
 *	Populates year, mon, and mday with the correct Gregorian values.
 *	year must be passed in as the ISO year.
 */
pub unsafe fn isoweekdate2date(
    isoweek: c_int,
    wday: c_int,
    year: *mut c_int,
    mon: *mut c_int,
    mday: *mut c_int,
) {
    let mut jday: c_int;

    jday = isoweek2j(*year, isoweek);
    /* convert Gregorian week start (Sunday=1) to ISO week start (Monday=1) */
    if wday > 1 {
        jday += wday - 2;
    } else {
        jday += 6;
    }
    j2date(jday, year, mon, mday);
}

/* date2isoweek()
 *
 *	Returns ISO week number of year.
 */
pub unsafe fn date2isoweek(year: c_int, mon: c_int, mday: c_int) -> c_int {
    let mut result: float8;
    let mut day0: c_int;
    let mut day4: c_int;
    let dayn: c_int;

    /* current day */
    dayn = date2j(year, mon, mday);

    /* fourth day of current year */
    day4 = date2j(year, 1, 4);

    /* day0 == offset to first day of week (Monday) */
    day0 = j2day(day4 - 1);

    /*
     * We need the first week containing a Thursday, otherwise this day falls
     * into the previous year for purposes of counting weeks
     */
    if dayn < day4 - day0 {
        day4 = date2j(year - 1, 1, 4);

        /* day0 == offset to first day of week (Monday) */
        day0 = j2day(day4 - 1);
    }

    result = ((dayn - (day4 - day0)) / 7 + 1) as float8;

    /*
     * Sometimes the last few days in a year will fall into the first week of
     * the next year, so check for this.
     */
    if result >= 52.0 {
        day4 = date2j(year + 1, 1, 4);

        /* day0 == offset to first day of week (Monday) */
        day0 = j2day(day4 - 1);

        if dayn >= day4 - day0 {
            result = ((dayn - (day4 - day0)) / 7 + 1) as float8;
        }
    }

    result as c_int
}

/* date2isoyear()
 *
 *	Returns ISO 8601 year number.
 *	Note: zero or negative results follow the year-zero-exists convention.
 */
pub unsafe fn date2isoyear(mut year: c_int, mon: c_int, mday: c_int) -> c_int {
    let result: float8;
    let mut day0: c_int;
    let mut day4: c_int;
    let dayn: c_int;

    /* current day */
    dayn = date2j(year, mon, mday);

    /* fourth day of current year */
    day4 = date2j(year, 1, 4);

    /* day0 == offset to first day of week (Monday) */
    day0 = j2day(day4 - 1);

    /*
     * We need the first week containing a Thursday, otherwise this day falls
     * into the previous year for purposes of counting weeks
     */
    if dayn < day4 - day0 {
        day4 = date2j(year - 1, 1, 4);

        /* day0 == offset to first day of week (Monday) */
        day0 = j2day(day4 - 1);

        year -= 1;
    }

    result = ((dayn - (day4 - day0)) / 7 + 1) as float8;

    /*
     * Sometimes the last few days in a year will fall into the first week of
     * the next year, so check for this.
     */
    if result >= 52.0 {
        day4 = date2j(year + 1, 1, 4);

        /* day0 == offset to first day of week (Monday) */
        day0 = j2day(day4 - 1);

        if dayn >= day4 - day0 {
            year += 1;
        }
    }

    year
}

/* date2isoyearday()
 *
 *	Returns the ISO 8601 day-of-year, given a Gregorian year, month and day.
 *	Possible return values are 1 through 371 (364 in non-leap years).
 */
pub unsafe fn date2isoyearday(year: c_int, mon: c_int, mday: c_int) -> c_int {
    date2j(year, mon, mday) - isoweek2j(date2isoyear(year, mon, mday), 1) + 1
}

/*
 * NonFiniteTimestampTzPart
 *
 *	Used by timestamp_part and timestamptz_part when extracting from infinite
 *	timestamp[tz].  Returns +/-Infinity if that is the appropriate result,
 *	otherwise returns zero (which should be taken as meaning to return NULL).
 *
 *	Errors thrown here for invalid units should exactly match those that
 *	would be thrown in the calling functions, else there will be unexpected
 *	discrepancies between finite- and infinite-input cases.
 */
unsafe fn NonFiniteTimestampTzPart(
    r#type: c_int,
    unit: c_int,
    lowunits: *mut c_char,
    isNegative: bool,
    isTz: bool,
) -> float8 {
    if (r#type != UNITS) && (r#type != RESERV) {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("unit \"{}\" not recognized for type {}",
            std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(if isTz { TIMESTAMPTZOID } else { TIMESTAMPOID })).to_string_lossy()));
    }

    match unit {
        /* Oscillating units */
        DTK_MICROSEC | DTK_MILLISEC | DTK_SECOND | DTK_MINUTE | DTK_HOUR | DTK_DAY | DTK_MONTH
        | DTK_QUARTER | DTK_WEEK | DTK_DOW | DTK_ISODOW | DTK_DOY | DTK_TZ | DTK_TZ_MINUTE
        | DTK_TZ_HOUR => 0.0,

        /* Monotonically-increasing units */
        DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_JULIAN | DTK_ISOYEAR
        | DTK_EPOCH => {
            if isNegative {
                -get_float8_infinity()
            } else {
                get_float8_infinity()
            }
        }

        _ => {
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(if isTz { TIMESTAMPTZOID } else { TIMESTAMPOID })).to_string_lossy()));
        }
    }
}

/* timestamp_part() and extract_timestamp()
 * Extract specified field from timestamp.
 */
unsafe fn timestamp_part_common(fcinfo: FunctionCallInfo, retnumeric: bool) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let mut intresult: int64 = 0;
    let epoch: Timestamp;
    let mut r#type: c_int;
    let mut val: c_int = 0;
    let lowunits: *mut c_char;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;

    lowunits = downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units) as c_int, false);

    r#type = DecodeUnits(0, lowunits, &mut val);
    if r#type == UNKNOWN_FIELD {
        r#type = DecodeSpecial(0, lowunits, &mut val);
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        let r: f64 = NonFiniteTimestampTzPart(r#type, val, lowunits, TIMESTAMP_IS_NOBEGIN(timestamp), false);

        if r != 0.0 {
            if retnumeric {
                if r < 0.0 {
                    return DirectFunctionCall3!(numeric_in,
                        CStringGetDatum(c"-Infinity".as_ptr()),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1));
                } else if r > 0.0 {
                    return DirectFunctionCall3!(numeric_in,
                        CStringGetDatum(c"Infinity".as_ptr()),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1));
                }
            } else {
                PG_RETURN_FLOAT8!(r);
            }
        } else {
            PG_RETURN_NULL!(fcinfo);
        }
    }

    if r#type == UNITS {
        if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        match val {
            DTK_MICROSEC => {
                intresult = (*tm).tm_sec as int64 * 1000000 + fsec as int64;
            }
            DTK_MILLISEC => {
                if retnumeric {
                    /*---
                     * tm->tm_sec * 1000 + fsec / 1000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric((*tm).tm_sec as int64 * 1000000 + fsec as int64, 3));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 * 1000.0 + fsec as f64 / 1000.0);
                }
            }
            DTK_SECOND => {
                if retnumeric {
                    /*---
                     * tm->tm_sec + fsec / 1'000'000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric((*tm).tm_sec as int64 * 1000000 + fsec as int64, 6));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 + fsec as f64 / 1000000.0);
                }
            }
            DTK_MINUTE => {
                intresult = (*tm).tm_min as int64;
            }
            DTK_HOUR => {
                intresult = (*tm).tm_hour as int64;
            }
            DTK_DAY => {
                intresult = (*tm).tm_mday as int64;
            }
            DTK_MONTH => {
                intresult = (*tm).tm_mon as int64;
            }
            DTK_QUARTER => {
                intresult = (((*tm).tm_mon - 1) / 3 + 1) as int64;
            }
            DTK_WEEK => {
                intresult = date2isoweek((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) as int64;
            }
            DTK_YEAR => {
                if (*tm).tm_year > 0 {
                    intresult = (*tm).tm_year as int64;
                } else {
                    /* there is no year 0, just 1 BC and 1 AD */
                    intresult = ((*tm).tm_year - 1) as int64;
                }
            }
            DTK_DECADE => {
                /*
                 * what is a decade wrt dates? let us assume that decade 199
                 * is 1990 thru 1999... decade 0 starts on year 1 BC, and -1
                 * is 11 BC thru 2 BC...
                 */
                if (*tm).tm_year >= 0 {
                    intresult = ((*tm).tm_year / 10) as int64;
                } else {
                    intresult = -(((8 - ((*tm).tm_year - 1)) / 10) as int64);
                }
            }
            DTK_CENTURY => {
                /* ----
                 * centuries AD, c>0: year in [ (c-1)* 100 + 1 : c*100 ]
                 * centuries BC, c<0: year in [ c*100 : (c+1) * 100 - 1]
                 * there is no number 0 century.
                 * ----
                 */
                if (*tm).tm_year > 0 {
                    intresult = (((*tm).tm_year + 99) / 100) as int64;
                } else {
                    /* caution: C division may have negative remainder */
                    intresult = -(((99 - ((*tm).tm_year - 1)) / 100) as int64);
                }
            }
            DTK_MILLENNIUM => {
                /* see comments above. */
                if (*tm).tm_year > 0 {
                    intresult = (((*tm).tm_year + 999) / 1000) as int64;
                } else {
                    intresult = -(((999 - ((*tm).tm_year - 1)) / 1000) as int64);
                }
            }
            DTK_JULIAN => {
                if retnumeric {
                    PG_RETURN_NUMERIC!(numeric_add_opt_error(
                        int64_to_numeric(date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) as int64),
                        numeric_div_opt_error(
                            int64_to_numeric(((((*tm).tm_hour as int64 * MINS_PER_HOUR as int64) + (*tm).tm_min as int64) * SECS_PER_MINUTE as int64 + (*tm).tm_sec as int64) * 1000000 + fsec as int64),
                            int64_to_numeric(SECS_PER_DAY as int64 * 1000000),
                            null_mut()),
                        null_mut()));
                } else {
                    PG_RETURN_FLOAT8!(date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) as f64
                        + (((((*tm).tm_hour * MINS_PER_HOUR) + (*tm).tm_min) * SECS_PER_MINUTE) as f64
                            + (*tm).tm_sec as f64 + (fsec as f64 / 1000000.0)) / SECS_PER_DAY as f64);
                }
            }
            DTK_ISOYEAR => {
                intresult = date2isoyear((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) as int64;
                /* Adjust BC years */
                if intresult <= 0 {
                    intresult -= 1;
                }
            }
            DTK_DOW | DTK_ISODOW => {
                intresult = j2day(date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday)) as int64;
                if val == DTK_ISODOW && intresult == 0 {
                    intresult = 7;
                }
            }
            DTK_DOY => {
                intresult = (date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday)
                    - date2j((*tm).tm_year, 1, 1) + 1) as int64;
            }
            _ => {
                /* DTK_TZ, DTK_TZ_MINUTE, DTK_TZ_HOUR, default */
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                    std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPOID)).to_string_lossy()));
            }
        }
    } else if r#type == RESERV {
        match val {
            DTK_EPOCH => {
                epoch = SetEpochTimestamp();
                /* (timestamp - epoch) / 1000000 */
                if retnumeric {
                    let mut result: Numeric;

                    if timestamp < PG_INT64_MAX.wrapping_add(epoch) {
                        result = int64_div_fast_to_numeric(timestamp - epoch, 6);
                    } else {
                        result = numeric_div_opt_error(
                            numeric_sub_opt_error(int64_to_numeric(timestamp), int64_to_numeric(epoch), null_mut()),
                            int64_to_numeric(1000000),
                            null_mut());
                        result = DatumGetNumeric(DirectFunctionCall2!(numeric_round,
                            NumericGetDatum(result), Int32GetDatum(6)));
                    }
                    PG_RETURN_NUMERIC!(result);
                } else {
                    let result: float8;

                    /* try to avoid precision loss in subtraction */
                    if timestamp < PG_INT64_MAX.wrapping_add(epoch) {
                        result = (timestamp - epoch) as f64 / 1000000.0;
                    } else {
                        result = (timestamp as f64 - epoch as f64) / 1000000.0;
                    }
                    PG_RETURN_FLOAT8!(result);
                }
            }
            _ => {
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                    std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPOID)).to_string_lossy()));
            }
        }
    } else {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("unit \"{}\" not recognized for type {}",
            std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPOID)).to_string_lossy()));
    }

    if retnumeric {
        PG_RETURN_NUMERIC!(int64_to_numeric(intresult));
    } else {
        PG_RETURN_FLOAT8!(intresult as f64);
    }
}

pub unsafe fn timestamp_part(fcinfo: FunctionCallInfo) -> Datum {
    timestamp_part_common(fcinfo, false)
}

pub unsafe fn extract_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    timestamp_part_common(fcinfo, true)
}
/* timestamptz_part() and extract_timestamptz()
 * Extract specified field from timestamp with time zone.
 */
unsafe fn timestamptz_part_common(fcinfo: FunctionCallInfo, retnumeric: bool) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
    let mut intresult: int64 = 0;
    let epoch: Timestamp;
    let mut tz: c_int = 0;
    let mut r#type: c_int;
    let mut val: c_int = 0;
    let lowunits: *mut c_char;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;

    lowunits = downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units) as c_int, false);

    r#type = DecodeUnits(0, lowunits, &mut val);
    if r#type == UNKNOWN_FIELD {
        r#type = DecodeSpecial(0, lowunits, &mut val);
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        let r: f64 = NonFiniteTimestampTzPart(r#type, val, lowunits, TIMESTAMP_IS_NOBEGIN(timestamp), true);

        if r != 0.0 {
            if retnumeric {
                if r < 0.0 {
                    return DirectFunctionCall3!(numeric_in,
                        CStringGetDatum(c"-Infinity".as_ptr()),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1));
                } else if r > 0.0 {
                    return DirectFunctionCall3!(numeric_in,
                        CStringGetDatum(c"Infinity".as_ptr()),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1));
                }
            } else {
                PG_RETURN_FLOAT8!(r);
            }
        } else {
            PG_RETURN_NULL!(fcinfo);
        }
    }

    if r#type == UNITS {
        if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), null_mut()) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        match val {
            DTK_TZ => {
                intresult = -tz as int64;
            }
            DTK_TZ_MINUTE => {
                intresult = ((-tz / SECS_PER_MINUTE) % MINS_PER_HOUR) as int64;
            }
            DTK_TZ_HOUR => {
                intresult = (-tz / SECS_PER_HOUR) as int64;
            }
            DTK_MICROSEC => {
                intresult = (*tm).tm_sec as int64 * 1000000 + fsec as int64;
            }
            DTK_MILLISEC => {
                if retnumeric {
                    /*---
                     * tm->tm_sec * 1000 + fsec / 1000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric((*tm).tm_sec as int64 * 1000000 + fsec as int64, 3));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 * 1000.0 + fsec as f64 / 1000.0);
                }
            }
            DTK_SECOND => {
                if retnumeric {
                    /*---
                     * tm->tm_sec + fsec / 1'000'000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric((*tm).tm_sec as int64 * 1000000 + fsec as int64, 6));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 + fsec as f64 / 1000000.0);
                }
            }
            DTK_MINUTE => {
                intresult = (*tm).tm_min as int64;
            }
            DTK_HOUR => {
                intresult = (*tm).tm_hour as int64;
            }
            DTK_DAY => {
                intresult = (*tm).tm_mday as int64;
            }
            DTK_MONTH => {
                intresult = (*tm).tm_mon as int64;
            }
            DTK_QUARTER => {
                intresult = (((*tm).tm_mon - 1) / 3 + 1) as int64;
            }
            DTK_WEEK => {
                intresult = date2isoweek((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) as int64;
            }
            DTK_YEAR => {
                if (*tm).tm_year > 0 {
                    intresult = (*tm).tm_year as int64;
                } else {
                    /* there is no year 0, just 1 BC and 1 AD */
                    intresult = ((*tm).tm_year - 1) as int64;
                }
            }
            DTK_DECADE => {
                /* see comments in timestamp_part */
                if (*tm).tm_year > 0 {
                    intresult = ((*tm).tm_year / 10) as int64;
                } else {
                    intresult = -(((8 - ((*tm).tm_year - 1)) / 10) as int64);
                }
            }
            DTK_CENTURY => {
                /* see comments in timestamp_part */
                if (*tm).tm_year > 0 {
                    intresult = (((*tm).tm_year + 99) / 100) as int64;
                } else {
                    intresult = -(((99 - ((*tm).tm_year - 1)) / 100) as int64);
                }
            }
            DTK_MILLENNIUM => {
                /* see comments in timestamp_part */
                if (*tm).tm_year > 0 {
                    intresult = (((*tm).tm_year + 999) / 1000) as int64;
                } else {
                    intresult = -(((999 - ((*tm).tm_year - 1)) / 1000) as int64);
                }
            }
            DTK_JULIAN => {
                if retnumeric {
                    PG_RETURN_NUMERIC!(numeric_add_opt_error(
                        int64_to_numeric(date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) as int64),
                        numeric_div_opt_error(
                            int64_to_numeric(((((*tm).tm_hour as int64 * MINS_PER_HOUR as int64) + (*tm).tm_min as int64) * SECS_PER_MINUTE as int64 + (*tm).tm_sec as int64) * 1000000 + fsec as int64),
                            int64_to_numeric(SECS_PER_DAY as int64 * 1000000),
                            null_mut()),
                        null_mut()));
                } else {
                    PG_RETURN_FLOAT8!(date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) as f64
                        + (((((*tm).tm_hour * MINS_PER_HOUR) + (*tm).tm_min) * SECS_PER_MINUTE) as f64
                            + (*tm).tm_sec as f64 + (fsec as f64 / 1000000.0)) / SECS_PER_DAY as f64);
                }
            }
            DTK_ISOYEAR => {
                intresult = date2isoyear((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) as int64;
                /* Adjust BC years */
                if intresult <= 0 {
                    intresult -= 1;
                }
            }
            DTK_DOW | DTK_ISODOW => {
                intresult = j2day(date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday)) as int64;
                if val == DTK_ISODOW && intresult == 0 {
                    intresult = 7;
                }
            }
            DTK_DOY => {
                intresult = (date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday)
                    - date2j((*tm).tm_year, 1, 1) + 1) as int64;
            }
            _ => {
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                    std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPTZOID)).to_string_lossy()));
            }
        }
    } else if r#type == RESERV {
        match val {
            DTK_EPOCH => {
                epoch = SetEpochTimestamp();
                /* (timestamp - epoch) / 1000000 */
                if retnumeric {
                    let mut result: Numeric;

                    if timestamp < PG_INT64_MAX.wrapping_add(epoch) {
                        result = int64_div_fast_to_numeric(timestamp - epoch, 6);
                    } else {
                        result = numeric_div_opt_error(
                            numeric_sub_opt_error(int64_to_numeric(timestamp), int64_to_numeric(epoch), null_mut()),
                            int64_to_numeric(1000000),
                            null_mut());
                        result = DatumGetNumeric(DirectFunctionCall2!(numeric_round,
                            NumericGetDatum(result), Int32GetDatum(6)));
                    }
                    PG_RETURN_NUMERIC!(result);
                } else {
                    let result: float8;

                    /* try to avoid precision loss in subtraction */
                    if timestamp < PG_INT64_MAX.wrapping_add(epoch) {
                        result = (timestamp - epoch) as f64 / 1000000.0;
                    } else {
                        result = (timestamp as f64 - epoch as f64) / 1000000.0;
                    }
                    PG_RETURN_FLOAT8!(result);
                }
            }
            _ => {
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                    std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPTZOID)).to_string_lossy()));
            }
        }
    } else {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("unit \"{}\" not recognized for type {}",
            std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(TIMESTAMPTZOID)).to_string_lossy()));
    }

    if retnumeric {
        PG_RETURN_NUMERIC!(int64_to_numeric(intresult));
    } else {
        PG_RETURN_FLOAT8!(intresult as f64);
    }
}

pub unsafe fn timestamptz_part(fcinfo: FunctionCallInfo) -> Datum {
    timestamptz_part_common(fcinfo, false)
}

pub unsafe fn extract_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    timestamptz_part_common(fcinfo, true)
}

/*
 * NonFiniteIntervalPart
 *
 *	Used by interval_part when extracting from infinite interval.  Returns
 *	+/-Infinity if that is the appropriate result, otherwise returns zero
 *	(which should be taken as meaning to return NULL).
 *
 *	Errors thrown here for invalid units should exactly match those that
 *	would be thrown in the calling functions, else there will be unexpected
 *	discrepancies between finite- and infinite-input cases.
 */
unsafe fn NonFiniteIntervalPart(r#type: c_int, unit: c_int, lowunits: *mut c_char, isNegative: bool) -> float8 {
    if (r#type != UNITS) && (r#type != RESERV) {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("unit \"{}\" not recognized for type {}",
            std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(INTERVALOID)).to_string_lossy()));
    }

    match unit {
        /* Oscillating units */
        DTK_MICROSEC | DTK_MILLISEC | DTK_SECOND | DTK_MINUTE | DTK_WEEK | DTK_MONTH | DTK_QUARTER => 0.0,

        /* Monotonically-increasing units */
        DTK_HOUR | DTK_DAY | DTK_YEAR | DTK_DECADE | DTK_CENTURY | DTK_MILLENNIUM | DTK_EPOCH => {
            if isNegative {
                -get_float8_infinity()
            } else {
                get_float8_infinity()
            }
        }

        _ => {
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(INTERVALOID)).to_string_lossy()));
        }
    }
}

/* interval_part() and extract_interval()
 * Extract specified field from interval.
 */
unsafe fn interval_part_common(fcinfo: FunctionCallInfo, retnumeric: bool) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let interval = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let mut intresult: int64 = 0;
    let mut r#type: c_int;
    let mut val: c_int = 0;
    let lowunits: *mut c_char;
    let mut tt: pg_itm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_itm;

    lowunits = downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units) as c_int, false);

    r#type = DecodeUnits(0, lowunits, &mut val);
    if r#type == UNKNOWN_FIELD {
        r#type = DecodeSpecial(0, lowunits, &mut val);
    }

    if INTERVAL_NOT_FINITE(interval) {
        let r: f64 = NonFiniteIntervalPart(r#type, val, lowunits, INTERVAL_IS_NOBEGIN(interval));

        if r != 0.0 {
            if retnumeric {
                if r < 0.0 {
                    return DirectFunctionCall3!(numeric_in,
                        CStringGetDatum(c"-Infinity".as_ptr()),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1));
                } else if r > 0.0 {
                    return DirectFunctionCall3!(numeric_in,
                        CStringGetDatum(c"Infinity".as_ptr()),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1));
                }
            } else {
                PG_RETURN_FLOAT8!(r);
            }
        } else {
            PG_RETURN_NULL!(fcinfo);
        }
    }

    if r#type == UNITS {
        interval2itm(*interval, tm);
        match val {
            DTK_MICROSEC => {
                intresult = (*tm).tm_sec as int64 * 1000000 + (*tm).tm_usec as int64;
            }
            DTK_MILLISEC => {
                if retnumeric {
                    /*---
                     * tm->tm_sec * 1000 + fsec / 1000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric((*tm).tm_sec as int64 * 1000000 + (*tm).tm_usec as int64, 3));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 * 1000.0 + (*tm).tm_usec as f64 / 1000.0);
                }
            }
            DTK_SECOND => {
                if retnumeric {
                    /*---
                     * tm->tm_sec + fsec / 1'000'000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric((*tm).tm_sec as int64 * 1000000 + (*tm).tm_usec as int64, 6));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 + (*tm).tm_usec as f64 / 1000000.0);
                }
            }
            DTK_MINUTE => {
                intresult = (*tm).tm_min as int64;
            }
            DTK_HOUR => {
                intresult = (*tm).tm_hour;
            }
            DTK_DAY => {
                intresult = (*tm).tm_mday as int64;
            }
            DTK_WEEK => {
                intresult = ((*tm).tm_mday / 7) as int64;
            }
            DTK_MONTH => {
                intresult = (*tm).tm_mon as int64;
            }
            DTK_QUARTER => {
                /*
                 * We want to maintain the rule that a field extracted from a
                 * negative interval is the negative of the field's value for
                 * the sign-reversed interval.  The broken-down tm_year and
                 * tm_mon aren't very helpful for that, so work from
                 * interval->month.
                 */
                if (*interval).month >= 0 {
                    intresult = (((*tm).tm_mon / 3) + 1) as int64;
                } else {
                    intresult = -((((-(*interval).month % MONTHS_PER_YEAR) / 3) + 1) as int64);
                }
            }
            DTK_YEAR => {
                intresult = (*tm).tm_year as int64;
            }
            DTK_DECADE => {
                /* caution: C division may have negative remainder */
                intresult = ((*tm).tm_year / 10) as int64;
            }
            DTK_CENTURY => {
                /* caution: C division may have negative remainder */
                intresult = ((*tm).tm_year / 100) as int64;
            }
            DTK_MILLENNIUM => {
                /* caution: C division may have negative remainder */
                intresult = ((*tm).tm_year / 1000) as int64;
            }
            _ => {
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                ereport!(ERROR, errmsg!("unit \"{}\" not supported for type {}",
                    std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(INTERVALOID)).to_string_lossy()));
            }
        }
    } else if r#type == RESERV && val == DTK_EPOCH {
        if retnumeric {
            let result: Numeric;
            let secs_from_day_month: int64;
            let mut valv: int64 = 0;

            /*
             * To do this calculation in integer arithmetic even though
             * DAYS_PER_YEAR is fractional, multiply everything by 4 and then
             * divide by 4 again at the end.  This relies on DAYS_PER_YEAR
             * being a multiple of 0.25 and on SECS_PER_DAY being a multiple
             * of 4.
             */
            secs_from_day_month = ((4.0 * DAYS_PER_YEAR) as int64 * ((*interval).month / MONTHS_PER_YEAR) as int64
                + (4 * DAYS_PER_MONTH) as int64 * ((*interval).month % MONTHS_PER_YEAR) as int64
                + 4 * (*interval).day as int64) * (SECS_PER_DAY / 4) as int64;

            /*---
             * result = secs_from_day_month + interval->time / 1'000'000
             * = (secs_from_day_month * 1'000'000 + interval->time) / 1'000'000
             */

            /*
             * Try the computation inside int64; if it overflows, do it in
             * numeric (slower).  This overflow happens around 10^9 days, so
             * not common in practice.
             */
            if !pg_mul_s64_overflow(secs_from_day_month, 1000000, &mut valv)
                && !pg_add_s64_overflow(valv, (*interval).time, &mut valv)
            {
                result = int64_div_fast_to_numeric(valv, 6);
            } else {
                result = numeric_add_opt_error(
                    int64_div_fast_to_numeric((*interval).time, 6),
                    int64_to_numeric(secs_from_day_month),
                    null_mut());
            }

            PG_RETURN_NUMERIC!(result);
        } else {
            let mut result: float8;

            result = (*interval).time as f64 / 1000000.0;
            result += (DAYS_PER_YEAR * SECS_PER_DAY as f64) * ((*interval).month / MONTHS_PER_YEAR) as f64;
            result += (DAYS_PER_MONTH as f64 * SECS_PER_DAY as f64) * ((*interval).month % MONTHS_PER_YEAR) as f64;
            result += (SECS_PER_DAY as f64) * (*interval).day as f64;

            PG_RETURN_FLOAT8!(result);
        }
    } else {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("unit \"{}\" not recognized for type {}",
            std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
            std::ffi::CStr::from_ptr(format_type_be(INTERVALOID)).to_string_lossy()));
    }

    if retnumeric {
        PG_RETURN_NUMERIC!(int64_to_numeric(intresult));
    } else {
        PG_RETURN_FLOAT8!(intresult as f64);
    }
}

pub unsafe fn interval_part(fcinfo: FunctionCallInfo) -> Datum {
    interval_part_common(fcinfo, false)
}

pub unsafe fn extract_interval(fcinfo: FunctionCallInfo) -> Datum {
    interval_part_common(fcinfo, true)
}
/*	timestamp_zone()
 *	Encode timestamp type with specified time zone.
 *	This function is just timestamp2timestamptz() except instead of
 *	shifting to the global timezone, we shift to the specified timezone.
 *	This is different from the other AT TIME ZONE cases because instead
 *	of shifting _to_ a new time zone, it sets the time to _be_ the
 *	specified timezone.
 */
pub unsafe fn timestamp_zone(fcinfo: FunctionCallInfo) -> Datum {
    let zone = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let mut result: TimestampTz = 0;
    let tz: c_int;
    let mut tzname: [c_char; TZ_STRLEN_MAX as usize + 1] = [0; TZ_STRLEN_MAX as usize + 1];
    let r#type: c_int;
    let mut val: c_int = 0;
    let mut tzp: *mut pg_tz = null_mut();
    let mut tm: pg_tm = std::mem::zeroed();
    let mut fsec: fsec_t = 0;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_TIMESTAMPTZ!(timestamp);
    }

    /*
     * Look up the requested timezone.
     */
    text_to_cstring_buffer(zone, tzname.as_mut_ptr(), std::mem::size_of_val(&tzname) as Size);

    r#type = DecodeTimezoneName(tzname.as_ptr(), &mut val, &mut tzp);

    if r#type == TZNAME_FIXED_OFFSET {
        /* fixed-offset abbreviation */
        tz = val;
        result = dt2local(timestamp, tz);
    } else if r#type == TZNAME_DYNTZ {
        /* dynamic-offset abbreviation, resolve using specified time */
        if timestamp2tm(timestamp, null_mut(), &mut tm, &mut fsec, null_mut(), tzp) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
        tz = -DetermineTimeZoneAbbrevOffset(&mut tm, tzname.as_ptr(), tzp);
        result = dt2local(timestamp, tz);
    } else {
        /* full zone name, rotate to that zone */
        if timestamp2tm(timestamp, null_mut(), &mut tm, &mut fsec, null_mut(), tzp) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
        tz = DetermineTimeZoneOffset(&mut tm, tzp);
        let mut tzv = tz;
        if tm2timestamp(&mut tm, fsec, &mut tzv, &mut result) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
    }

    if !IS_VALID_TIMESTAMP(result) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    PG_RETURN_TIMESTAMPTZ!(result);
}

/* timestamp_izone()
 * Encode timestamp type with specified time interval as time zone.
 */
pub unsafe fn timestamp_izone(fcinfo: FunctionCallInfo) -> Datum {
    let zone = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 1);
    let result: TimestampTz;
    let tz: c_int;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_TIMESTAMPTZ!(timestamp);
    }

    if INTERVAL_NOT_FINITE(zone) {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("interval time zone \"{}\" must be finite",
            std::ffi::CStr::from_ptr(DatumGetCString(DirectFunctionCall1!(interval_out, PointerGetDatum(zone as *const c_void)))).to_string_lossy()));
    }

    if (*zone).month != 0 || (*zone).day != 0 {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("interval time zone \"{}\" must not include months or days",
            std::ffi::CStr::from_ptr(DatumGetCString(DirectFunctionCall1!(interval_out, PointerGetDatum(zone as *const c_void)))).to_string_lossy()));
    }

    tz = ((*zone).time / USECS_PER_SEC) as c_int;

    result = dt2local(timestamp, tz);

    if !IS_VALID_TIMESTAMP(result) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    PG_RETURN_TIMESTAMPTZ!(result);
} /* timestamp_izone() */

/* TimestampTimestampTzRequiresRewrite()
 *
 * Returns false if the TimeZone GUC setting causes timestamp_timestamptz and
 * timestamptz_timestamp to be no-ops, where the return value has the same
 * bits as the argument.  Since project convention is to assume a GUC changes
 * no more often than STABLE functions change, the answer is valid that long.
 */
pub unsafe fn TimestampTimestampTzRequiresRewrite() -> bool {
    let mut offset: std::os::raw::c_long = 0;

    if pg_get_timezone_offset(session_timezone, &mut offset) && offset == 0 {
        return false;
    }
    true
}

/* timestamp_timestamptz()
 * Convert local timestamp to timestamp at GMT
 */
pub unsafe fn timestamp_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);

    PG_RETURN_TIMESTAMPTZ!(timestamp2timestamptz(timestamp));
}

/*
 * Convert timestamp to timestamp with time zone.
 *
 * On successful conversion, *overflow is set to zero if it's not NULL.
 *
 * If the timestamp is finite but out of the valid range for timestamptz, then:
 * if overflow is NULL, we throw an out-of-range error.
 * if overflow is not NULL, we store +1 or -1 there to indicate the sign
 * of the overflow, and return the appropriate timestamptz infinity.
 */
pub unsafe fn timestamp2timestamptz_opt_overflow(timestamp: Timestamp, overflow: *mut c_int) -> TimestampTz {
    let mut result: TimestampTz;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut fsec: fsec_t = 0;
    let tz: c_int;

    if !overflow.is_null() {
        *overflow = 0;
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        return timestamp;
    }

    /* We don't expect this to fail, but check it pro forma */
    if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) == 0 {
        tz = DetermineTimeZoneOffset(tm, session_timezone);

        result = dt2local(timestamp, -tz);

        if IS_VALID_TIMESTAMP(result) {
            return result;
        } else if !overflow.is_null() {
            if result < MIN_TIMESTAMP {
                *overflow = -1;
                TIMESTAMP_NOBEGIN(&mut result);
            } else {
                *overflow = 1;
                TIMESTAMP_NOEND(&mut result);
            }
            return result;
        }
    }

    /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
    ereport!(ERROR, errmsg!("timestamp out of range"));

    #[allow(unreachable_code)]
    {
        0
    }
}

/*
 * Promote timestamp to timestamptz, throwing error for overflow.
 */
unsafe fn timestamp2timestamptz(timestamp: Timestamp) -> TimestampTz {
    timestamp2timestamptz_opt_overflow(timestamp, null_mut())
}

/* timestamptz_timestamp()
 * Convert timestamp at GMT to local timestamp
 */
pub unsafe fn timestamptz_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);

    PG_RETURN_TIMESTAMP!(timestamptz2timestamp(timestamp));
}

unsafe fn timestamptz2timestamp(timestamp: TimestampTz) -> Timestamp {
    let mut result: Timestamp = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm = &mut tt as *mut pg_tm;
    let mut fsec: fsec_t = 0;
    let mut tz: c_int = 0;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        result = timestamp;
    } else {
        if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), null_mut()) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
        if tm2timestamp(tm, fsec, null_mut(), &mut result) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
    }
    result
}

/* timestamptz_zone()
 * Evaluate timestamp with time zone type at the specified time zone.
 * Returns a timestamp without time zone.
 */
pub unsafe fn timestamptz_zone(fcinfo: FunctionCallInfo) -> Datum {
    let zone = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
    let mut result: Timestamp = 0;
    let tz: c_int;
    let mut tzname: [c_char; TZ_STRLEN_MAX as usize + 1] = [0; TZ_STRLEN_MAX as usize + 1];
    let r#type: c_int;
    let mut val: c_int = 0;
    let mut tzp: *mut pg_tz = null_mut();

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_TIMESTAMP!(timestamp);
    }

    /*
     * Look up the requested timezone.
     */
    text_to_cstring_buffer(zone, tzname.as_mut_ptr(), std::mem::size_of_val(&tzname) as Size);

    r#type = DecodeTimezoneName(tzname.as_ptr(), &mut val, &mut tzp);

    if r#type == TZNAME_FIXED_OFFSET {
        /* fixed-offset abbreviation */
        tz = -val;
        result = dt2local(timestamp, tz);
    } else if r#type == TZNAME_DYNTZ {
        /* dynamic-offset abbreviation, resolve using specified time */
        let mut isdst: c_int = 0;

        tz = DetermineTimeZoneAbbrevOffsetTS(timestamp, tzname.as_ptr(), tzp, &mut isdst);
        result = dt2local(timestamp, tz);
    } else {
        /* full zone name, rotate from that zone */
        let mut tm: pg_tm = std::mem::zeroed();
        let mut fsec: fsec_t = 0;
        let mut tz2: c_int = 0;

        if timestamp2tm(timestamp, &mut tz2, &mut tm, &mut fsec, null_mut(), tzp) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
        if tm2timestamp(&mut tm, fsec, null_mut(), &mut result) != 0 {
            /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
    }

    if !IS_VALID_TIMESTAMP(result) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    PG_RETURN_TIMESTAMP!(result);
}

/* timestamptz_izone()
 * Encode timestamp with time zone type with specified time interval as time zone.
 * Returns a timestamp without time zone.
 */
pub unsafe fn timestamptz_izone(fcinfo: FunctionCallInfo) -> Datum {
    let zone = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let timestamp = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
    let result: Timestamp;
    let tz: c_int;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_TIMESTAMP!(timestamp);
    }

    if INTERVAL_NOT_FINITE(zone) {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("interval time zone \"{}\" must be finite",
            std::ffi::CStr::from_ptr(DatumGetCString(DirectFunctionCall1!(interval_out, PointerGetDatum(zone as *const c_void)))).to_string_lossy()));
    }

    if (*zone).month != 0 || (*zone).day != 0 {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        ereport!(ERROR, errmsg!("interval time zone \"{}\" must not include months or days",
            std::ffi::CStr::from_ptr(DatumGetCString(DirectFunctionCall1!(interval_out, PointerGetDatum(zone as *const c_void)))).to_string_lossy()));
    }

    tz = -(((*zone).time / USECS_PER_SEC) as c_int);

    result = dt2local(timestamp, tz);

    if !IS_VALID_TIMESTAMP(result) {
        /* C also: errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE) */
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    PG_RETURN_TIMESTAMP!(result);
}

/* generate_series_timestamp()
 * Generate the set of timestamps from start to finish by step
 */
pub unsafe fn generate_series_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let funcctx: *mut FuncCallContext;
    let fctx: *mut generate_series_timestamp_fctx;
    let result: Timestamp;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL() {
        let start = PG_GETARG_TIMESTAMP!(fcinfo, 0);
        let finish = PG_GETARG_TIMESTAMP!(fcinfo, 1);
        let step = PG_GETARG_INTERVAL_P!(fcinfo, 2);
        let oldcontext: MemoryContext;

        /* create a function context for cross-call persistence */
        let funcctx0 = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo((*funcctx0).multi_call_memory_ctx);

        /* allocate memory for user context */
        let fctx0 = palloc(std::mem::size_of::<generate_series_timestamp_fctx>() as Size)
            as *mut generate_series_timestamp_fctx;

        /*
         * Use fctx to keep state from call to call. Seed current with the
         * original start value
         */
        (*fctx0).current = start;
        (*fctx0).finish = finish;
        (*fctx0).step = *step;

        /* Determine sign of the interval */
        (*fctx0).step_sign = interval_sign(&(*fctx0).step);

        if (*fctx0).step_sign == 0 {
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            ereport!(ERROR, errmsg!("step size cannot equal zero"));
        }

        if INTERVAL_NOT_FINITE(&(*fctx0).step) {
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            ereport!(ERROR, errmsg!("step size cannot be infinite"));
        }

        (*funcctx0).user_fctx = fctx0 as *mut c_void;
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    /*
     * get the saved state and use current as the result for this iteration
     */
    fctx = (*funcctx).user_fctx as *mut generate_series_timestamp_fctx;
    result = (*fctx).current;

    if if (*fctx).step_sign > 0 {
        timestamp_cmp_internal(result, (*fctx).finish) <= 0
    } else {
        timestamp_cmp_internal(result, (*fctx).finish) >= 0
    } {
        /* increment current in preparation for next iteration */
        (*fctx).current = DatumGetTimestamp(DirectFunctionCall2!(timestamp_pl_interval,
            TimestampGetDatum((*fctx).current),
            PointerGetDatum(&(*fctx).step as *const Interval as *const c_void)));

        /* do when there is more left to send */
        SRF_RETURN_NEXT(funcctx, TimestampGetDatum(result))
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx)
    }
}

/* generate_series_timestamptz()
 * Generate the set of timestamps from start to finish by step,
 * doing arithmetic in the specified or session timezone.
 */
unsafe fn generate_series_timestamptz_internal(fcinfo: FunctionCallInfo) -> Datum {
    let funcctx: *mut FuncCallContext;
    let fctx: *mut generate_series_timestamptz_fctx;
    let result: TimestampTz;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL() {
        let start = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
        let finish = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);
        let step = PG_GETARG_INTERVAL_P!(fcinfo, 2);
        let zone: *mut text = if PG_NARGS!(fcinfo) == 4 { PG_GETARG_TEXT_PP!(fcinfo, 3) } else { null_mut() };
        let oldcontext: MemoryContext;

        /* create a function context for cross-call persistence */
        let funcctx0 = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo((*funcctx0).multi_call_memory_ctx);

        /* allocate memory for user context */
        let fctx0 = palloc(std::mem::size_of::<generate_series_timestamptz_fctx>() as Size)
            as *mut generate_series_timestamptz_fctx;

        /*
         * Use fctx to keep state from call to call. Seed current with the
         * original start value
         */
        (*fctx0).current = start;
        (*fctx0).finish = finish;
        (*fctx0).step = *step;
        (*fctx0).attimezone = if !zone.is_null() { lookup_timezone(zone) } else { session_timezone };

        /* Determine sign of the interval */
        (*fctx0).step_sign = interval_sign(&(*fctx0).step);

        if (*fctx0).step_sign == 0 {
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            ereport!(ERROR, errmsg!("step size cannot equal zero"));
        }

        if INTERVAL_NOT_FINITE(&(*fctx0).step) {
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            ereport!(ERROR, errmsg!("step size cannot be infinite"));
        }

        (*funcctx0).user_fctx = fctx0 as *mut c_void;
        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    /*
     * get the saved state and use current as the result for this iteration
     */
    fctx = (*funcctx).user_fctx as *mut generate_series_timestamptz_fctx;
    result = (*fctx).current;

    if if (*fctx).step_sign > 0 {
        timestamp_cmp_internal(result, (*fctx).finish) <= 0
    } else {
        timestamp_cmp_internal(result, (*fctx).finish) >= 0
    } {
        /* increment current in preparation for next iteration */
        (*fctx).current = timestamptz_pl_interval_internal((*fctx).current, &mut (*fctx).step, (*fctx).attimezone);

        /* do when there is more left to send */
        SRF_RETURN_NEXT(funcctx, TimestampTzGetDatum(result))
    } else {
        /* do when there is no more left */
        SRF_RETURN_DONE(funcctx)
    }
}

pub unsafe fn generate_series_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    generate_series_timestamptz_internal(fcinfo)
}

pub unsafe fn generate_series_timestamptz_at_zone(fcinfo: FunctionCallInfo) -> Datum {
    generate_series_timestamptz_internal(fcinfo)
}

/*
 * Planner support function for generate_series(timestamp, timestamp, interval)
 */
pub unsafe fn generate_series_timestamp_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = null_mut();

    if IsA!(rawreq, T_SupportRequestRows) {
        /* Try to estimate the number of rows returned */
        let req = rawreq as *mut SupportRequestRows;

        if is_funcclause((*req).node) {
            /* be paranoid */
            let args: *mut List = (*((*req).node as *mut FuncExpr)).args;
            let arg1: *mut Node;
            let arg2: *mut Node;
            let arg3: *mut Node;

            /* We can use estimated argument values here */
            arg1 = estimate_expression_value((*req).root, linitial(args) as *mut Node);
            arg2 = estimate_expression_value((*req).root, lsecond(args) as *mut Node);
            arg3 = estimate_expression_value((*req).root, lthird(args) as *mut Node);

            /*
             * If any argument is constant NULL, we can safely assume that zero
             * rows are returned.  Otherwise, if they're all non-NULL
             * constants, we can calculate the number of rows that will be
             * returned.
             */
            if (IsA!(arg1, T_Const) && (*(arg1 as *mut Const)).constisnull)
                || (IsA!(arg2, T_Const) && (*(arg2 as *mut Const)).constisnull)
                || (IsA!(arg3, T_Const) && (*(arg3 as *mut Const)).constisnull)
            {
                (*req).rows = 0.0;
                ret = req as *mut Node;
            } else if IsA!(arg1, T_Const) && IsA!(arg2, T_Const) && IsA!(arg3, T_Const) {
                let start: Timestamp;
                let finish: Timestamp;
                let step: *mut Interval;
                let diff: Datum;
                let dstep: f64;
                let mut dummy: int64 = 0;

                start = DatumGetTimestamp((*(arg1 as *mut Const)).constvalue);
                finish = DatumGetTimestamp((*(arg2 as *mut Const)).constvalue);
                step = DatumGetIntervalP((*(arg3 as *mut Const)).constvalue);

                /*
                 * Perform some prechecks which could cause timestamp_mi to
                 * raise an ERROR.  It's much better to just return some
                 * default estimate than error out in a support function.
                 */
                if !TIMESTAMP_NOT_FINITE(start)
                    && !TIMESTAMP_NOT_FINITE(finish)
                    && !pg_sub_s64_overflow(finish, start, &mut dummy)
                {
                    diff = DirectFunctionCall2!(timestamp_mi, TimestampGetDatum(finish), TimestampGetDatum(start));

                    /* INTERVAL_TO_MICROSECONDS(i) macro inlined below */
                    dstep = (((*step).month as f64 * DAYS_PER_MONTH as f64 + (*step).day as f64))
                        * USECS_PER_DAY as f64 + (*step).time as f64;

                    /* This equation works for either sign of step */
                    if dstep != 0.0 {
                        let idiff: *mut Interval = DatumGetIntervalP(diff);
                        let ddiff: f64 = (((*idiff).month as f64 * DAYS_PER_MONTH as f64 + (*idiff).day as f64))
                            * USECS_PER_DAY as f64 + (*idiff).time as f64;

                        (*req).rows = floor(ddiff / dstep + 1.0);
                        ret = req as *mut Node;
                    }
                }
            }
        }
    }

    PG_RETURN_POINTER!(ret)
}

/* timestamp_at_local()
 * timestamptz_at_local()
 *
 * The regression tests do not like two functions with the same proargs and
 * prosrc but different proname, but the grammar for AT LOCAL needs an
 * overloaded name to handle both types of timestamp, so we make simple
 * wrappers for it.
 */
pub unsafe fn timestamp_at_local(fcinfo: FunctionCallInfo) -> Datum {
    timestamp_timestamptz(fcinfo)
}

pub unsafe fn timestamptz_at_local(fcinfo: FunctionCallInfo) -> Datum {
    timestamptz_timestamp(fcinfo)
}
