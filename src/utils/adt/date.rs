//! date.rs
//!   implements DATE and TIME data types specified in SQL standard
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/date.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994-5, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h               -> crate::prelude::* (Datum, *GetDatum, palloc, elog!/ereport!/Assert!)
//!   <ctype.h>/<limits.h>/<float.h>/<math.h>/<time.h> -> Rust libm/intrinsics + local consts
//!   access/xact.h            -> GetCurrentTransactionStartTimestamp (stubbed below)
//!   catalog/pg_type.h        -> crate::catalog::pg_type_d (DATEOID/TIMEOID/TIMETZOID)
//!   common/hashfn.h          -> crate::common::hashfn (hash_uint32/hash_uint32_extended)
//!   common/int.h             -> crate::common::int (pg_add_s64_overflow/pg_neg_s32_overflow)
//!   libpq/pqformat.h         -> crate::libpq::pqformat (pq_*)
//!   miscadmin.h              -> crate::miscadmin
//!   nodes/supportnodes.h     -> stubbed below (SupportRequestSimplify/IsA/TemporalSimplify)
//!   parser/scansup.h         -> downcase_truncate_identifier (stubbed below)
//!   utils/array.h            -> ArrayType/ArrayGetIntegerTypmods (stubbed below)
//!   utils/builtins.h         -> cstring_to_text/text helpers
//!   utils/date.h             -> THIS FILE (DateADT/TimeADT/TimeTzADT + macros below)
//!   utils/datetime.h         -> stubbed below (Decode*/Encode*/j2date/date2j/DTK_*/consts)
//!   utils/numeric.h          -> stubbed below (int64_to_numeric/int64_div_fast_to_numeric)
//!   utils/skipsupport.h      -> stubbed below (SkipSupport)
//!   utils/sortsupport.h      -> stubbed below (SortSupport/ssup_datum_int32_cmp)
//!
//! NOTE: timestamp.c/datetime.c are not yet ported, so the broken-down-time
//! decode/encode helpers, the Timestamp/Interval datatypes, the numeric
//! conversions, and the sort/skip/support-node plumbing are defined here as
//! minimal local stubs marked `// TODO(pg-port)`.  All such stubs name the file
//! where the real symbol will eventually live.

use crate::prelude::*;

// crate root only exports DirectFunctionCall1/2/3; DirectFunctionCall5Coll exists as a fn.
macro_rules! DirectFunctionCall5 {
    ($func:expr, $a1:expr, $a2:expr, $a3:expr, $a4:expr, $a5:expr) => {
        crate::utils::fmgr::DirectFunctionCall5Coll(
            $func, crate::postgres_ext::InvalidOid, $a1, $a2, $a3, $a4, $a5,
        )
    };
}

use crate::utils::fmgr::*;
use crate::common::hashfn::{hash_uint32, hash_uint32_extended};
use crate::common::int::{pg_add_s64_overflow, pg_neg_s32_overflow};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_getmsgint64, pq_sendint32, pq_sendint64,
};
use crate::pgtime::{pg_tz, pg_tm, TZ_STRLEN_MAX};
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, DatumGetBool, DatumGetCString, DatumGetInt32, DatumGetInt64,
    DatumGetPointer, DatumGetUInt32, DatumGetUInt64, Float8GetDatum, Int32GetDatum, Int64GetDatum,
    ObjectIdGetDatum, PointerGetDatum, UInt32GetDatum, UInt64GetDatum,
};
use crate::utils::adt::varlena::cstring_to_text;
use crate::varatt::{VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::{
    DirectFunctionCall1, DirectFunctionCall2, DirectFunctionCall3,
    PG_GETARG_BOOL, PG_GETARG_CSTRING, PG_GETARG_DATUM, PG_GETARG_FLOAT8, PG_GETARG_INT32,
    PG_GETARG_INT64, PG_GETARG_POINTER, PG_GETARG_TEXT_PP, PG_ARGISNULL, PG_RETURN_BOOL,
    PG_RETURN_CSTRING, PG_RETURN_FLOAT8, PG_RETURN_INT32, PG_RETURN_NULL, PG_RETURN_UINT32,
    PG_RETURN_UINT64, PG_RETURN_VOID,
};

use std::ffi::{c_char, c_int};

// ===========================================================================
// Local type aliases / macros from utils/date.h (typedef int32 DateADT; etc.)
// ===========================================================================

pub type DateADT = int32;
pub type TimeADT = int64;

#[repr(C)]
pub struct TimeTzADT {
    pub time: TimeADT, /* all time units other than months and years */
    pub zone: int32,   /* numeric time zone, in seconds */
}

/*
 * Infinity and minus infinity must be the max and min values of DateADT.
 */
pub const DATEVAL_NOBEGIN: DateADT = PG_INT32_MIN;
pub const DATEVAL_NOEND: DateADT = PG_INT32_MAX;

#[inline]
pub fn DATE_IS_NOBEGIN(j: DateADT) -> bool {
    j == DATEVAL_NOBEGIN
}
#[inline]
pub fn DATE_IS_NOEND(j: DateADT) -> bool {
    j == DATEVAL_NOEND
}
#[inline]
pub fn DATE_NOT_FINITE(j: DateADT) -> bool {
    DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j)
}

pub const MAX_TIME_PRECISION: int32 = 6;

/* Functions for fmgr-callable functions (utils/date.h) */
#[inline]
pub unsafe fn DatumGetDateADT(X: Datum) -> DateADT {
    DatumGetInt32(X) as DateADT
}
#[inline]
pub unsafe fn DatumGetTimeADT(X: Datum) -> TimeADT {
    DatumGetInt64(X) as TimeADT
}
#[inline]
pub unsafe fn DatumGetTimeTzADTP(X: Datum) -> *mut TimeTzADT {
    DatumGetPointer(X) as *mut TimeTzADT
}
#[inline]
pub fn DateADTGetDatum(X: DateADT) -> Datum {
    Int32GetDatum(X)
}
#[inline]
pub fn TimeADTGetDatum(X: TimeADT) -> Datum {
    Int64GetDatum(X)
}
#[inline]
pub fn TimeTzADTPGetDatum(X: *const TimeTzADT) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// PG_GETARG_*/PG_RETURN_* helpers for date.h types (inline; no crate-root macro).
macro_rules! PG_GETARG_DATEADT {
    ($fcinfo:expr, $n:expr) => {
        DatumGetDateADT(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_TIMEADT {
    ($fcinfo:expr, $n:expr) => {
        DatumGetTimeADT(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_GETARG_TIMETZADT_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetTimeTzADTP(PG_GETARG_DATUM!($fcinfo, $n))
    };
}
macro_rules! PG_RETURN_DATEADT {
    ($x:expr) => {
        return DateADTGetDatum($x)
    };
}
macro_rules! PG_RETURN_TIMEADT {
    ($x:expr) => {
        return TimeADTGetDatum($x)
    };
}
macro_rules! PG_RETURN_TIMETZADT_P {
    ($x:expr) => {
        return TimeTzADTPGetDatum($x)
    };
}

// ===========================================================================
// TODO(pg-port): the following are minimal local stubs for symbols that will
// live in their real homes once those C files are ported.  Each is annotated.
// ===========================================================================

// --- datatype/timestamp.h types ---
// TODO(pg-port): real Timestamp/TimestampTz/TimeOffset/fsec_t/Interval live in
// crate::utils::adt::timestamp & crate::utils::adt::datetime.
pub type Timestamp = int64;
pub type TimestampTz = int64;
pub type TimeOffset = int64;
pub type fsec_t = int32; /* fractional seconds (in microseconds) */

#[repr(C)]
pub struct Interval {
    pub time: TimeOffset, /* all time units other than days, months and years */
    pub day: int32,       /* days, after time for alignment */
    pub month: int32,     /* months and years, after time for alignment */
}

// TODO(pg-port): DateTimeErrorExtra lives in crate::utils::adt::datetime.
#[repr(C)]
pub struct DateTimeErrorExtra {
    /* Needed for DTERR_BAD_TIMEZONE and DTERR_BAD_ZONE_ABBREV: */
    pub dtee_timezone: *const c_char,
    /* Needed for DTERR_BAD_ZONE_ABBREV: */
    pub dtee_abbrev: *const c_char,
}

// --- datatype/timestamp.h constants ---
pub const HOURS_PER_DAY: c_int = 24;
pub const SECS_PER_DAY: c_int = 86400;
pub const SECS_PER_HOUR: c_int = 3600;
pub const SECS_PER_MINUTE: c_int = 60;
pub const MINS_PER_HOUR: c_int = 60;
pub const USECS_PER_DAY: int64 = 86400000000;
pub const USECS_PER_HOUR: int64 = 3600000000;
pub const USECS_PER_MINUTE: int64 = 60000000;
pub const USECS_PER_SEC: int64 = 1000000;

pub const MAX_TZDISP_HOUR: c_int = 15;
pub const TZDISP_LIMIT: c_int = (MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR;

pub const UNIX_EPOCH_JDATE: int32 = 2440588; /* == date2j(1970, 1, 1) */
pub const POSTGRES_EPOCH_JDATE: int32 = 2451545; /* == date2j(2000, 1, 1) */

pub const JULIAN_MINYEAR: c_int = -4713;
pub const JULIAN_MINMONTH: c_int = 11;
pub const JULIAN_MAXYEAR: c_int = 5874898;
pub const JULIAN_MAXMONTH: c_int = 6;

pub const DATETIME_MIN_JULIAN: int32 = 0;
pub const DATE_END_JULIAN: int32 = 2147483494; /* == date2j(JULIAN_MAXYEAR, 1, 1) */
pub const TIMESTAMP_END_JULIAN: int32 = 109203528; /* == date2j(294277, 1, 1) */

pub const MIN_TIMESTAMP: int64 = -211813488000000000;
pub const END_TIMESTAMP: int64 = 9223371331200000000;

pub const DT_NOBEGIN: int64 = PG_INT64_MIN;
pub const DT_NOEND: int64 = PG_INT64_MAX;

#[inline]
pub fn DATE_NOBEGIN(j: &mut DateADT) {
    *j = DATEVAL_NOBEGIN;
}
#[inline]
pub fn DATE_NOEND(j: &mut DateADT) {
    *j = DATEVAL_NOEND;
}

#[inline]
pub fn TIMESTAMP_NOBEGIN(j: &mut Timestamp) {
    *j = DT_NOBEGIN;
}
#[inline]
pub fn TIMESTAMP_NOEND(j: &mut Timestamp) {
    *j = DT_NOEND;
}
#[inline]
pub fn TIMESTAMP_IS_NOBEGIN(j: Timestamp) -> bool {
    j == DT_NOBEGIN
}
#[inline]
pub fn TIMESTAMP_IS_NOEND(j: Timestamp) -> bool {
    j == DT_NOEND
}
#[inline]
pub fn TIMESTAMP_NOT_FINITE(j: Timestamp) -> bool {
    TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j)
}

#[inline]
pub fn IS_VALID_JULIAN(y: c_int, m: c_int, _d: c_int) -> bool {
    (y > JULIAN_MINYEAR || (y == JULIAN_MINYEAR && m >= JULIAN_MINMONTH))
        && (y < JULIAN_MAXYEAR || (y == JULIAN_MAXYEAR && m < JULIAN_MAXMONTH))
}
#[inline]
pub fn IS_VALID_DATE(d: DateADT) -> bool {
    (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= d && d < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE)
}
#[inline]
pub fn IS_VALID_TIMESTAMP(t: Timestamp) -> bool {
    MIN_TIMESTAMP <= t && t < END_TIMESTAMP
}

// INTERVAL_NOT_FINITE: an interval is non-finite iff month/day/time are at the
// reserved infinity markers. TODO(pg-port): real macros in datatype/timestamp.h.
pub const INT_MIN_C: int32 = i32::MIN;
pub const INT_MAX_C: int32 = i32::MAX;
#[inline]
pub unsafe fn INTERVAL_IS_NOBEGIN(i: *const Interval) -> bool {
    (*i).month == INT_MIN_C && (*i).day == INT_MIN_C && (*i).time == DT_NOBEGIN
}
#[inline]
pub unsafe fn INTERVAL_IS_NOEND(i: *const Interval) -> bool {
    (*i).month == INT_MAX_C && (*i).day == INT_MAX_C && (*i).time == DT_NOEND
}
#[inline]
pub unsafe fn INTERVAL_NOT_FINITE(i: *const Interval) -> bool {
    INTERVAL_IS_NOBEGIN(i) || INTERVAL_IS_NOEND(i)
}

// --- utils/datetime.h constants ---
pub const EARLY: &[u8] = b"-infinity\0";
pub const LATE: &[u8] = b"infinity\0";
pub const MAXDATELEN: usize = 128;
pub const MAXDATEFIELDS: usize = 25;

pub const RESERV: c_int = 0;
pub const UNITS: c_int = 17;
pub const UNKNOWN_FIELD: c_int = 31;

pub const DTK_DATE: c_int = 2;
pub const DTK_TZ: c_int = 4;
pub const DTK_EARLY: c_int = 9;
pub const DTK_LATE: c_int = 10;
pub const DTK_EPOCH: c_int = 11;
pub const DTK_SECOND: c_int = 18;
pub const DTK_MINUTE: c_int = 19;
pub const DTK_HOUR: c_int = 20;
pub const DTK_DAY: c_int = 21;
pub const DTK_WEEK: c_int = 22;
pub const DTK_MONTH: c_int = 23;
pub const DTK_QUARTER: c_int = 24;
pub const DTK_YEAR: c_int = 25;
pub const DTK_DECADE: c_int = 26;
pub const DTK_CENTURY: c_int = 27;
pub const DTK_MILLENNIUM: c_int = 28;
pub const DTK_MILLISEC: c_int = 29;
pub const DTK_MICROSEC: c_int = 30;
pub const DTK_JULIAN: c_int = 31;
pub const DTK_DOW: c_int = 32;
pub const DTK_DOY: c_int = 33;
pub const DTK_TZ_HOUR: c_int = 34;
pub const DTK_TZ_MINUTE: c_int = 35;
pub const DTK_ISOYEAR: c_int = 36;
pub const DTK_ISODOW: c_int = 37;

/* DTK_DATE_M == DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY) == (1<<2)|(1<<1)|(1<<3) */
pub const DTK_DATE_M: c_int = 14;

pub const TZNAME_FIXED_OFFSET: c_int = 0;
pub const TZNAME_DYNTZ: c_int = 1;

pub const DTERR_BAD_FORMAT: c_int = -1;

// --- catalog/pg_type.h OIDs ---
// TODO(pg-port): real OIDs in crate::catalog::pg_type_d.
pub const DATEOID: Oid = 1082;
pub const TIMEOID: Oid = 1083;
pub const TIMETZOID: Oid = 1266;

const DBL_MAX: f64 = f64::MAX;

extern "C" {
    fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn rint(x: f64) -> f64;
}

// --- utils/datetime.h functions (TODO(pg-port): crate::utils::adt::datetime) ---
extern "C" {
    fn isnan(x: f64) -> c_int;
}

#[allow(non_upper_case_globals)]
static mut session_timezone: *mut pg_tz = std::ptr::null_mut();

// TODO(pg-port): real implementations live in crate::utils::adt::datetime.
unsafe fn ParseDateTime(
    _str: *const c_char,
    _workbuf: *mut c_char,
    _buflen: Size,
    _field: *mut *mut c_char,
    _ftype: *mut c_int,
    _maxfields: c_int,
    _numfields: *mut c_int,
) -> c_int { crate::utils::adt::datetime::ParseDateTime(_str as _, _workbuf as _, _buflen as _, _field as _, _ftype as _, _maxfields as _, _numfields as _) as _ }
unsafe fn DecodeDateTime(
    _field: *mut *mut c_char,
    _ftype: *const c_int,
    _nf: c_int,
    _dtype: *mut c_int,
    _tm: *mut pg_tm,
    _fsec: *mut fsec_t,
    _tzp: *mut c_int,
    _extra: *mut DateTimeErrorExtra,
) -> c_int { crate::utils::adt::datetime::DecodeDateTime(_field as _, _ftype as _, _nf as _, _dtype as _, _tm as _, _fsec as _, _tzp as _, _extra as _) as _ }
unsafe fn DecodeTimeOnly(
    _field: *mut *mut c_char,
    _ftype: *const c_int,
    _nf: c_int,
    _dtype: *mut c_int,
    _tm: *mut pg_tm,
    _fsec: *mut fsec_t,
    _tzp: *mut c_int,
    _extra: *mut DateTimeErrorExtra,
) -> c_int { crate::utils::adt::datetime::DecodeTimeOnly(_field as _, _ftype as _, _nf as _, _dtype as _, _tm as _, _fsec as _, _tzp as _, _extra as _) as _ }
unsafe fn DateTimeParseError(
    _dterr: c_int,
    _extra: *const DateTimeErrorExtra,
    _str: *const c_char,
    _datatype: *const c_char,
    _escontext: *mut c_void,
) { crate::utils::adt::datetime::DateTimeParseError(_dterr as _, _extra as _, _str as _, _datatype as _, _escontext as _) }
unsafe fn ValidateDate(
    _fmask: c_int,
    _isjulian: bool,
    _is2digits: bool,
    _bc: bool,
    _tm: *mut pg_tm,
) -> c_int { crate::utils::adt::datetime::ValidateDate(_fmask as _, _isjulian, _is2digits, _bc, _tm as _) as _ }
unsafe fn GetEpochTime(_tm: *mut pg_tm) { crate::utils::adt::timestamp::GetEpochTime(_tm as _) }
unsafe fn GetCurrentDateTime(_tm: *mut pg_tm) { crate::utils::adt::datetime::GetCurrentDateTime(_tm as _) }
unsafe fn GetCurrentTimeUsec(_tm: *mut pg_tm, _fsec: *mut fsec_t, _tzp: *mut c_int) { crate::utils::adt::datetime::GetCurrentTimeUsec(_tm as _, _fsec as _, _tzp as _) }
pub unsafe fn j2date(_jd: c_int, _year: *mut c_int, _month: *mut c_int, _day: *mut c_int) { crate::utils::adt::datetime::j2date(_jd as _, _year as _, _month as _, _day as _) }
unsafe fn date2j(_year: c_int, _month: c_int, _day: c_int) -> c_int { crate::utils::adt::datetime::date2j(_year as _, _month as _, _day as _) as _ }
unsafe fn j2day(_date: c_int) -> c_int { crate::utils::adt::datetime::j2day(_date as _) as _ }
unsafe fn date2isoweek(_year: c_int, _mon: c_int, _mday: c_int) -> c_int { crate::utils::adt::timestamp::date2isoweek(_year as _, _mon as _, _mday as _) as _ }
unsafe fn date2isoyear(_year: c_int, _mon: c_int, _mday: c_int) -> c_int { crate::utils::adt::timestamp::date2isoyear(_year as _, _mon as _, _mday as _) as _ }
unsafe fn EncodeDateOnly(_tm: *mut pg_tm, _style: c_int, _str: *mut c_char) { crate::utils::adt::datetime::EncodeDateOnly(_tm as _, _style as _, _str as _) }
unsafe fn EncodeTimeOnly(
    _tm: *mut pg_tm,
    _fsec: fsec_t,
    _print_tz: bool,
    _tz: c_int,
    _style: c_int,
    _str: *mut c_char,
) { crate::utils::adt::datetime::EncodeTimeOnly(_tm as _, _fsec as _, _print_tz, _tz as _, _style as _, _str as _) }
unsafe fn DecodeUnits(_field: c_int, _lowtoken: *const c_char, _val: *mut c_int) -> c_int { crate::utils::adt::datetime::DecodeUnits(_field as _, _lowtoken as _, _val as _) as _ }
unsafe fn DecodeSpecial(_field: c_int, _lowtoken: *const c_char, _val: *mut c_int) -> c_int { crate::utils::adt::datetime::DecodeSpecial(_field as _, _lowtoken as _, _val as _) as _ }
unsafe fn DecodeTimezoneName(_str: *const c_char, _offset: *mut c_int, _tz: *mut *mut pg_tz) -> c_int {
    unimplemented!("DecodeTimezoneName: crate::utils::adt::datetime")
}
pub unsafe fn DetermineTimeZoneOffset(_tm: *mut pg_tm, _tzp: *mut pg_tz) -> c_int { crate::utils::adt::datetime::DetermineTimeZoneOffset(_tm as _, _tzp as _) as _ }
unsafe fn DetermineTimeZoneAbbrevOffsetTS(
    _ts: TimestampTz,
    _abbr: *const c_char,
    _tzp: *mut pg_tz,
    _isdst: *mut c_int,
) -> c_int { crate::utils::adt::datetime::DetermineTimeZoneAbbrevOffsetTS(_ts as _, _abbr as _, _tzp as _, _isdst as _) as _ }

#[allow(non_upper_case_globals)]
static mut DateStyle: c_int = 0; /* TODO(pg-port): GUC in crate::utils::adt::datetime */

// --- utils/timestamp.h functions (TODO(pg-port): crate::utils::adt::timestamp) ---
unsafe fn timestamp_cmp_internal(_dt1: Timestamp, _dt2: Timestamp) -> int32 { crate::utils::adt::timestamp::timestamp_cmp_internal(_dt1 as _, _dt2 as _) as _ }
unsafe fn timestamptz_cmp_internal(_dt1: TimestampTz, _dt2: TimestampTz) -> int32 { crate::utils::adt::timestamp::timestamptz_cmp_internal(_dt1 as _, _dt2 as _) as _ }
unsafe fn timestamp2tm(
    _dt: Timestamp,
    _tzp: *mut c_int,
    _tm: *mut pg_tm,
    _fsec: *mut fsec_t,
    _tzn: *mut *const c_char,
    _attimezone: *mut pg_tz,
) -> c_int { crate::utils::adt::timestamp::timestamp2tm(_dt as _, _tzp as _, _tm as _, _fsec as _, _tzn as _, _attimezone as _) as _ }
#[allow(non_snake_case)]
unsafe fn DatumGetTimestamp(X: Datum) -> Timestamp {
    DatumGetInt64(X) as Timestamp
}
#[allow(non_snake_case)]
fn TimestampGetDatum(X: Timestamp) -> Datum {
    Int64GetDatum(X)
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
macro_rules! PG_RETURN_TIMESTAMP {
    ($x:expr) => {
        return TimestampGetDatum($x)
    };
}
#[allow(non_snake_case)]
fn IntervalPGetDatum(X: *const Interval) -> Datum {
    PointerGetDatum(X as *const c_void)
}
macro_rules! PG_GETARG_INTERVAL_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut Interval
    };
}
macro_rules! PG_RETURN_INTERVAL_P {
    ($x:expr) => {
        return IntervalPGetDatum($x)
    };
}
// TODO(pg-port): real fmgr targets live in crate::utils::adt::timestamp / numeric.
unsafe fn in_range_timestamp_interval(_fcinfo: FunctionCallInfo) -> Datum { crate::utils::adt::timestamp::in_range_timestamp_interval(_fcinfo as _) as _ }
unsafe fn timestamp_pl_interval(_fcinfo: FunctionCallInfo) -> Datum { crate::utils::adt::timestamp::timestamp_pl_interval(_fcinfo as _) as _ }
unsafe fn timestamp_mi_interval(_fcinfo: FunctionCallInfo) -> Datum { crate::utils::adt::timestamp::timestamp_mi_interval(_fcinfo as _) as _ }
unsafe fn interval_out(_fcinfo: FunctionCallInfo) -> Datum { crate::utils::adt::timestamp::interval_out(_fcinfo as _) as _ }

// --- utils/numeric.h (TODO(pg-port): crate::utils::adt::numeric) ---
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
unsafe fn int64_to_numeric(_val: int64) -> Numeric { crate::utils::adt::numeric::int64_to_numeric(_val as _) as _ }
unsafe fn int64_div_fast_to_numeric(_val1: int64, _log10val2: c_int) -> Numeric { crate::utils::adt::numeric::int64_div_fast_to_numeric(_val1 as _, _log10val2 as _) as _ }
unsafe fn numeric_in(_fcinfo: FunctionCallInfo) -> Datum { crate::utils::adt::numeric::numeric_in(_fcinfo as _) as _ }

// --- utils/int8.h hashing (TODO(pg-port): crate::utils::adt::int8) ---
unsafe fn hashint8(_fcinfo: FunctionCallInfo) -> Datum { crate::access::hash::hashfunc::hashint8(_fcinfo as _) as _ }
unsafe fn hashint8extended(_fcinfo: FunctionCallInfo) -> Datum { crate::access::hash::hashfunc::hashint8extended(_fcinfo as _) as _ }
#[allow(non_snake_case)]
fn Int64GetDatumFast(X: int64) -> Datum {
    Int64GetDatum(X)
}

// --- utils/sortsupport.h & skipsupport.h (TODO(pg-port)) ---
#[repr(C)]
pub struct SortSupportData {
    pub comparator: Option<unsafe extern "C" fn(Datum, Datum, *mut SortSupportData) -> c_int>,
}
pub type SortSupport = *mut SortSupportData;
unsafe extern "C" fn ssup_datum_int32_cmp(_x: Datum, _y: Datum, _ssup: *mut SortSupportData) -> c_int { crate::utils::sort::tuplesort::ssup_datum_int32_cmp(_x as _, _y as _, _ssup as _) as _ }

pub type Relation = *mut c_void;
#[repr(C)]
pub struct SkipSupportData {
    pub decrement: Option<unsafe fn(Relation, Datum, *mut bool) -> Datum>,
    pub increment: Option<unsafe fn(Relation, Datum, *mut bool) -> Datum>,
    pub low_elem: Datum,
    pub high_elem: Datum,
}
pub type SkipSupport = *mut SkipSupportData;

// --- nodes/supportnodes.h (TODO(pg-port): crate::nodes::supportnodes) ---
pub type Node = c_void;
#[repr(C)]
pub struct SupportRequestSimplify {
    pub fcall: *mut c_void, /* FuncExpr* */
}
unsafe fn IsA_SupportRequestSimplify(_node: *mut Node) -> bool {
    unimplemented!("IsA(SupportRequestSimplify): crate::nodes::supportnodes")
}
unsafe fn TemporalSimplify(_max_precis: int32, _node: *mut Node) -> *mut Node { crate::utils::adt::datetime::TemporalSimplify(_max_precis as _, _node as _) as _ }

// --- parser/scansup.h & utils helpers (TODO(pg-port)) ---
unsafe fn downcase_truncate_identifier(_ident: *const c_char, _len: c_int, _warn: bool) -> *mut c_char { crate::parser_link_shims::downcase_truncate_identifier(_ident as _, _len as _, _warn) as _ }
unsafe fn text_to_cstring_buffer(_src: *const text, _dst: *mut c_char, _dstlen: Size) {
    unimplemented!("text_to_cstring_buffer: crate::utils::adt::varlena")
}
unsafe fn format_type_be(_typid: Oid) -> *mut c_char {
    unimplemented!("format_type_be: crate::utils::adt::format_type")
}
unsafe fn pg_get_timezone_name(_tz: *mut pg_tz) -> *const c_char {
    unimplemented!("pg_get_timezone_name: crate::utils::adt::datetime")
}

// --- access/xact.h (TODO(pg-port): crate::access::transam::xact) ---
unsafe fn GetCurrentTransactionStartTimestamp() -> TimestampTz {
    unimplemented!("GetCurrentTransactionStartTimestamp: crate::access::transam::xact")
}

// --- utils/array.h (TODO(pg-port): crate::utils::adt::array) ---
pub type ArrayType = c_void;
unsafe fn ArrayGetIntegerTypmods(_arr: *mut ArrayType, _n: *mut c_int) -> *mut int32 { crate::utils::adt::arrayutils::ArrayGetIntegerTypmods(_arr as _, _n as _) as _ }
macro_rules! PG_GETARG_ARRAYTYPE_P {
    ($fcinfo:expr, $n:expr) => {
        DatumGetPointer(PG_GETARG_DATUM!($fcinfo, $n)) as *mut ArrayType
    };
}

// --- misc string helpers ---
unsafe fn psprintf_paren_tz(typmod: c_int, tz: *const c_char) -> *mut c_char {
    // psprintf("(%d)%s", typmod, tz)
    let tzs = std::ffi::CStr::from_ptr(tz).to_string_lossy();
    let s = format!("({}){}\0", typmod, tzs);
    let bytes = s.into_bytes();
    let p = palloc(bytes.len() as Size) as *mut u8;
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), p, bytes.len());
    p as *mut c_char
}

// ===========================================================================
//                  end of stubs; faithful translation follows
// ===========================================================================

/*
 * gcc's -ffast-math switch breaks routines that expect exact results from
 * expressions like timeval / SECS_PER_HOUR, where timeval is double.
 */

/* common code for timetypmodin and timetztypmodin */
unsafe fn anytime_typmodin(istz: bool, ta: *mut ArrayType) -> int32 {
    let tl: *mut int32;
    let mut n: c_int = 0;

    tl = ArrayGetIntegerTypmods(ta, &mut n);

    /*
     * we're not too tense about good error message here because grammar
     * shouldn't allow wrong number of modifiers for TIME
     */
    if n != 1 {
        ereport!(ERROR, errmsg!("invalid type modifier"));
    }

    anytime_typmod_check(istz, *tl)
}

/* exported so parse_expr.c can use it */
pub unsafe fn anytime_typmod_check(istz: bool, mut typmod: int32) -> int32 {
    if typmod < 0 {
        ereport!(
            ERROR,
            errmsg!(
                "TIME({}){} precision must not be negative",
                typmod,
                if istz { " WITH TIME ZONE" } else { "" }
            )
        );
    }
    if typmod > MAX_TIME_PRECISION {
        ereport!(
            ERROR,
            errmsg!(
                "TIME({}){} precision reduced to maximum allowed, {}",
                typmod,
                if istz { " WITH TIME ZONE" } else { "" },
                MAX_TIME_PRECISION
            )
        );
        typmod = MAX_TIME_PRECISION;
    }

    typmod
}

/* common code for timetypmodout and timetztypmodout */
unsafe fn anytime_typmodout(istz: bool, typmod: int32) -> *mut c_char {
    let tz: &[u8] = if istz {
        b" with time zone\0"
    } else {
        b" without time zone\0"
    };

    if typmod >= 0 {
        psprintf_paren_tz(typmod as c_int, tz.as_ptr() as *const c_char)
    } else {
        pstrdup(tz.as_ptr() as *const c_char)
    }
}


/*****************************************************************************
 *	 Date ADT
 *****************************************************************************/


/* date_in()
 * Given date text string, convert to internal date format.
 */
pub unsafe fn date_in(fcinfo: FunctionCallInfo) -> Datum {
    let str = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext = (*fcinfo).context;
    let mut date: DateADT = 0;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut tzp: c_int = 0;
    let mut dtype: c_int = 0;
    let mut nf: c_int = 0;
    let mut dterr: c_int;
    let mut field: [*mut c_char; MAXDATEFIELDS] = [null_mut(); MAXDATEFIELDS];
    let mut ftype: [c_int; MAXDATEFIELDS] = [0; MAXDATEFIELDS];
    let mut workbuf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];
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
            &mut tzp,
            &mut extra,
        );
    }
    if dterr != 0 {
        DateTimeParseError(dterr, &extra, str, c"date".as_ptr(), escontext as *mut c_void);
        PG_RETURN_NULL!(fcinfo);
    }

    match dtype {
        x if x == DTK_DATE => {}

        x if x == DTK_EPOCH => {
            GetEpochTime(tm);
        }

        x if x == DTK_LATE => {
            DATE_NOEND(&mut date);
            PG_RETURN_DATEADT!(date);
        }

        x if x == DTK_EARLY => {
            DATE_NOBEGIN(&mut date);
            PG_RETURN_DATEADT!(date);
        }

        _ => {
            DateTimeParseError(
                DTERR_BAD_FORMAT,
                &extra,
                str,
                c"date".as_ptr(),
                escontext as *mut c_void,
            );
            PG_RETURN_NULL!(fcinfo);
        }
    }

    /* Prevent overflow in Julian-day routines */
    if !IS_VALID_JULIAN((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) {
        ereport!(
            ERROR,
            errmsg!(
                "date out of range: \"{}\"",
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            )
        );
        return 0;
    }

    date = date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) - POSTGRES_EPOCH_JDATE;

    /* Now check for just-out-of-range dates */
    if !IS_VALID_DATE(date) {
        ereport!(
            ERROR,
            errmsg!(
                "date out of range: \"{}\"",
                std::ffi::CStr::from_ptr(str).to_string_lossy()
            )
        );
        return 0;
    }

    PG_RETURN_DATEADT!(date);
}

/* date_out()
 * Given internal format date, convert to text string.
 */
pub unsafe fn date_out(fcinfo: FunctionCallInfo) -> Datum {
    let date = PG_GETARG_DATEADT!(fcinfo, 0);
    let result: *mut c_char;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

    if DATE_NOT_FINITE(date) {
        EncodeSpecialDate(date, buf.as_mut_ptr());
    } else {
        j2date(
            date + POSTGRES_EPOCH_JDATE,
            &raw mut (*tm).tm_year,
            &raw mut (*tm).tm_mon,
            &raw mut (*tm).tm_mday,
        );
        EncodeDateOnly(tm, DateStyle, buf.as_mut_ptr());
    }

    result = pstrdup(buf.as_ptr());
    PG_RETURN_CSTRING!(result);
}

/*
 *		date_recv			- converts external binary format to date
 */
pub unsafe fn date_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let result: DateADT;

    result = pq_getmsgint(buf, std::mem::size_of::<DateADT>() as c_int) as DateADT;

    /* Limit to the same range that date_in() accepts. */
    if DATE_NOT_FINITE(result) {
        /* ok */
    } else if !IS_VALID_DATE(result) {
        ereport!(ERROR, errmsg!("date out of range"));
    }

    PG_RETURN_DATEADT!(result);
}

/*
 *		date_send			- converts date to binary format
 */
pub unsafe fn date_send(fcinfo: FunctionCallInfo) -> Datum {
    let date = PG_GETARG_DATEADT!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint32(&mut buf, date as uint32);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void);
}

/*
 *		make_date			- date constructor
 */
pub unsafe fn make_date(fcinfo: FunctionCallInfo) -> Datum {
    let mut tm: pg_tm = std::mem::zeroed();
    let date: DateADT;
    let dterr: c_int;
    let mut bc: bool = false;

    tm.tm_year = PG_GETARG_INT32!(fcinfo, 0);
    tm.tm_mon = PG_GETARG_INT32!(fcinfo, 1);
    tm.tm_mday = PG_GETARG_INT32!(fcinfo, 2);

    /* Handle negative years as BC */
    if tm.tm_year < 0 {
        let mut year: c_int = tm.tm_year;

        bc = true;
        if pg_neg_s32_overflow(year, &mut year) {
            ereport!(
                ERROR,
                errmsg!(
                    "date field value out of range: {}-{:02}-{:02}",
                    tm.tm_year,
                    tm.tm_mon,
                    tm.tm_mday
                )
            );
        }
        tm.tm_year = year;
    }

    dterr = ValidateDate(DTK_DATE_M, false, false, bc, &mut tm);

    if dterr != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "date field value out of range: {}-{:02}-{:02}",
                tm.tm_year,
                tm.tm_mon,
                tm.tm_mday
            )
        );
    }

    /* Prevent overflow in Julian-day routines */
    if !IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday) {
        ereport!(
            ERROR,
            errmsg!(
                "date out of range: {}-{:02}-{:02}",
                tm.tm_year,
                tm.tm_mon,
                tm.tm_mday
            )
        );
    }

    date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    /* Now check for just-out-of-range dates */
    if !IS_VALID_DATE(date) {
        ereport!(
            ERROR,
            errmsg!(
                "date out of range: {}-{:02}-{:02}",
                tm.tm_year,
                tm.tm_mon,
                tm.tm_mday
            )
        );
    }

    PG_RETURN_DATEADT!(date);
}

/*
 * Convert reserved date values to string.
 */
pub unsafe fn EncodeSpecialDate(dt: DateADT, str: *mut c_char) {
    if DATE_IS_NOBEGIN(dt) {
        strcpy(str, EARLY.as_ptr() as *const c_char);
    } else if DATE_IS_NOEND(dt) {
        strcpy(str, LATE.as_ptr() as *const c_char);
    } else {
        /* shouldn't happen */
        elog!(ERROR, "invalid argument for EncodeSpecialDate");
    }
}


/*
 * GetSQLCurrentDate -- implements CURRENT_DATE
 */
pub unsafe fn GetSQLCurrentDate() -> DateADT {
    let mut tm: pg_tm = std::mem::zeroed();

    static mut cache_year: c_int = 0;
    static mut cache_mon: c_int = 0;
    static mut cache_mday: c_int = 0;
    static mut cache_date: DateADT = 0;

    GetCurrentDateTime(&mut tm);

    /*
     * date2j involves several integer divisions; moreover, unless our session
     * lives across local midnight, we don't really have to do it more than
     * once.  So it seems worth having a separate cache here.
     */
    if tm.tm_year != cache_year || tm.tm_mon != cache_mon || tm.tm_mday != cache_mday {
        cache_date = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;
        cache_year = tm.tm_year;
        cache_mon = tm.tm_mon;
        cache_mday = tm.tm_mday;
    }

    cache_date
}

/*
 * GetSQLCurrentTime -- implements CURRENT_TIME, CURRENT_TIME(n)
 */
pub unsafe fn GetSQLCurrentTime(typmod: int32) -> *mut TimeTzADT {
    let result: *mut TimeTzADT;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut fsec: fsec_t = 0;
    let mut tz: c_int = 0;

    GetCurrentTimeUsec(tm, &mut fsec, &mut tz);

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;
    tm2timetz(tm, fsec, tz, result);
    AdjustTimeForTypmod(&raw mut (*result).time, typmod);
    result
}

/*
 * GetSQLLocalTime -- implements LOCALTIME, LOCALTIME(n)
 */
pub unsafe fn GetSQLLocalTime(typmod: int32) -> TimeADT {
    let mut result: TimeADT = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut fsec: fsec_t = 0;
    let mut tz: c_int = 0;

    GetCurrentTimeUsec(tm, &mut fsec, &mut tz);

    tm2time(tm, fsec, &mut result);
    AdjustTimeForTypmod(&mut result, typmod);
    result
}


/*
 * Comparison functions for dates
 */

pub unsafe fn date_eq(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(dateVal1 == dateVal2);
}

pub unsafe fn date_ne(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(dateVal1 != dateVal2);
}

pub unsafe fn date_lt(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(dateVal1 < dateVal2);
}

pub unsafe fn date_le(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(dateVal1 <= dateVal2);
}

pub unsafe fn date_gt(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(dateVal1 > dateVal2);
}

pub unsafe fn date_ge(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(dateVal1 >= dateVal2);
}

pub unsafe fn date_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    if dateVal1 < dateVal2 {
        PG_RETURN_INT32!(-1);
    } else if dateVal1 > dateVal2 {
        PG_RETURN_INT32!(1);
    }
    PG_RETURN_INT32!(0);
}

pub unsafe fn date_sortsupport(fcinfo: FunctionCallInfo) -> Datum {
    let ssup = PG_GETARG_POINTER!(fcinfo, 0) as SortSupport;

    (*ssup).comparator = Some(ssup_datum_int32_cmp);
    PG_RETURN_VOID!();
}

unsafe fn date_decrement(_rel: Relation, existing: Datum, underflow: *mut bool) -> Datum {
    let dexisting = DatumGetDateADT(existing);

    if dexisting == DATEVAL_NOBEGIN {
        /* return value is undefined */
        *underflow = true;
        return 0;
    }

    *underflow = false;
    DateADTGetDatum(dexisting - 1)
}

unsafe fn date_increment(_rel: Relation, existing: Datum, overflow: *mut bool) -> Datum {
    let dexisting = DatumGetDateADT(existing);

    if dexisting == DATEVAL_NOEND {
        /* return value is undefined */
        *overflow = true;
        return 0;
    }

    *overflow = false;
    DateADTGetDatum(dexisting + 1)
}

pub unsafe fn date_skipsupport(fcinfo: FunctionCallInfo) -> Datum {
    let sksup = PG_GETARG_POINTER!(fcinfo, 0) as SkipSupport;

    (*sksup).decrement = Some(date_decrement);
    (*sksup).increment = Some(date_increment);
    (*sksup).low_elem = DateADTGetDatum(DATEVAL_NOBEGIN);
    (*sksup).high_elem = DateADTGetDatum(DATEVAL_NOEND);

    PG_RETURN_VOID!();
}

pub unsafe fn hashdate(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32(PG_GETARG_DATEADT!(fcinfo, 0) as uint32);
}

pub unsafe fn hashdateextended(fcinfo: FunctionCallInfo) -> Datum {
    return hash_uint32_extended(
        PG_GETARG_DATEADT!(fcinfo, 0) as uint32,
        PG_GETARG_INT64!(fcinfo, 1) as uint64,
    );
}

pub unsafe fn date_finite(fcinfo: FunctionCallInfo) -> Datum {
    let date = PG_GETARG_DATEADT!(fcinfo, 0);

    PG_RETURN_BOOL!(!DATE_NOT_FINITE(date));
}

pub unsafe fn date_larger(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_DATEADT!(if dateVal1 > dateVal2 { dateVal1 } else { dateVal2 });
}

pub unsafe fn date_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_DATEADT!(if dateVal1 < dateVal2 { dateVal1 } else { dateVal2 });
}

/* Compute difference between two dates in days.
 */
pub unsafe fn date_mi(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal1 = PG_GETARG_DATEADT!(fcinfo, 0);
    let dateVal2 = PG_GETARG_DATEADT!(fcinfo, 1);

    if DATE_NOT_FINITE(dateVal1) || DATE_NOT_FINITE(dateVal2) {
        ereport!(ERROR, errmsg!("cannot subtract infinite dates"));
    }

    PG_RETURN_INT32!((dateVal1 - dateVal2) as int32);
}

/* Add a number of days to a date, giving a new date.
 * Must handle both positive and negative numbers of days.
 */
pub unsafe fn date_pli(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let days = PG_GETARG_INT32!(fcinfo, 1);
    let result: DateADT;

    if DATE_NOT_FINITE(dateVal) {
        PG_RETURN_DATEADT!(dateVal); /* can't change infinity */
    }

    result = dateVal.wrapping_add(days);

    /* Check for integer overflow and out-of-allowed-range */
    if (if days >= 0 {
        result < dateVal
    } else {
        result > dateVal
    }) || !IS_VALID_DATE(result)
    {
        ereport!(ERROR, errmsg!("date out of range"));
    }

    PG_RETURN_DATEADT!(result);
}

/* Subtract a number of days from a date, giving a new date.
 */
pub unsafe fn date_mii(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let days = PG_GETARG_INT32!(fcinfo, 1);
    let result: DateADT;

    if DATE_NOT_FINITE(dateVal) {
        PG_RETURN_DATEADT!(dateVal); /* can't change infinity */
    }

    result = dateVal.wrapping_sub(days);

    /* Check for integer overflow and out-of-allowed-range */
    if (if days >= 0 {
        result > dateVal
    } else {
        result < dateVal
    }) || !IS_VALID_DATE(result)
    {
        ereport!(ERROR, errmsg!("date out of range"));
    }

    PG_RETURN_DATEADT!(result);
}


/*
 * Promote date to timestamp.
 *
 * On successful conversion, *overflow is set to zero if it's not NULL.
 *
 * If the date is finite but out of the valid range for timestamp, then:
 * if overflow is NULL, we throw an out-of-range error.
 * if overflow is not NULL, we store +1 or -1 there to indicate the sign
 * of the overflow, and return the appropriate timestamp infinity.
 *
 * Note: *overflow = -1 is actually not possible currently, since both
 * datatypes have the same lower bound, Julian day zero.
 */
pub unsafe fn date2timestamp_opt_overflow(dateVal: DateADT, overflow: *mut c_int) -> Timestamp {
    let mut result: Timestamp = 0;

    if !overflow.is_null() {
        *overflow = 0;
    }

    if DATE_IS_NOBEGIN(dateVal) {
        TIMESTAMP_NOBEGIN(&mut result);
    } else if DATE_IS_NOEND(dateVal) {
        TIMESTAMP_NOEND(&mut result);
    } else {
        /*
         * Since dates have the same minimum values as timestamps, only upper
         * boundary need be checked for overflow.
         */
        if dateVal >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) {
            if !overflow.is_null() {
                *overflow = 1;
                TIMESTAMP_NOEND(&mut result);
                return result;
            } else {
                ereport!(ERROR, errmsg!("date out of range for timestamp"));
            }
        }

        /* date is days since 2000, timestamp is microseconds since same... */
        result = dateVal as int64 * USECS_PER_DAY;
    }

    result
}

/*
 * Promote date to timestamp, throwing error for overflow.
 */
unsafe fn date2timestamp(dateVal: DateADT) -> Timestamp {
    date2timestamp_opt_overflow(dateVal, null_mut())
}

/*
 * Promote date to timestamp with time zone.
 *
 * On successful conversion, *overflow is set to zero if it's not NULL.
 *
 * If the date is finite but out of the valid range for timestamptz, then:
 * if overflow is NULL, we throw an out-of-range error.
 * if overflow is not NULL, we store +1 or -1 there to indicate the sign
 * of the overflow, and return the appropriate timestamptz infinity.
 */
pub unsafe fn date2timestamptz_opt_overflow(dateVal: DateADT, overflow: *mut c_int) -> TimestampTz {
    let mut result: TimestampTz = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let tz: c_int;

    if !overflow.is_null() {
        *overflow = 0;
    }

    if DATE_IS_NOBEGIN(dateVal) {
        TIMESTAMP_NOBEGIN(&mut result);
    } else if DATE_IS_NOEND(dateVal) {
        TIMESTAMP_NOEND(&mut result);
    } else {
        /*
         * Since dates have the same minimum values as timestamps, only upper
         * boundary need be checked for overflow.
         */
        if dateVal >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) {
            if !overflow.is_null() {
                *overflow = 1;
                TIMESTAMP_NOEND(&mut result);
                return result;
            } else {
                ereport!(ERROR, errmsg!("date out of range for timestamp"));
            }
        }

        j2date(
            dateVal + POSTGRES_EPOCH_JDATE,
            &raw mut (*tm).tm_year,
            &raw mut (*tm).tm_mon,
            &raw mut (*tm).tm_mday,
        );
        (*tm).tm_hour = 0;
        (*tm).tm_min = 0;
        (*tm).tm_sec = 0;
        tz = DetermineTimeZoneOffset(tm, session_timezone);

        result = dateVal as int64 * USECS_PER_DAY + tz as int64 * USECS_PER_SEC;

        /*
         * Since it is possible to go beyond allowed timestamptz range because
         * of time zone, check for allowed timestamp range after adding tz.
         */
        if !IS_VALID_TIMESTAMP(result) {
            if !overflow.is_null() {
                if result < MIN_TIMESTAMP {
                    *overflow = -1;
                    TIMESTAMP_NOBEGIN(&mut result);
                } else {
                    *overflow = 1;
                    TIMESTAMP_NOEND(&mut result);
                }
            } else {
                ereport!(ERROR, errmsg!("date out of range for timestamp"));
            }
        }
    }

    result
}

/*
 * Promote date to timestamptz, throwing error for overflow.
 */
unsafe fn date2timestamptz(dateVal: DateADT) -> TimestampTz {
    date2timestamptz_opt_overflow(dateVal, null_mut())
}

/*
 * date2timestamp_no_overflow
 *
 * This is chartered to produce a double value that is numerically
 * equivalent to the corresponding Timestamp value, if the date is in the
 * valid range of Timestamps, but in any case not throw an overflow error.
 * We can do this since the numerical range of double is greater than
 * that of non-erroneous timestamps.  The results are currently only
 * used for statistical estimation purposes.
 */
pub unsafe fn date2timestamp_no_overflow(dateVal: DateADT) -> f64 {
    let result: f64;

    if DATE_IS_NOBEGIN(dateVal) {
        result = -DBL_MAX;
    } else if DATE_IS_NOEND(dateVal) {
        result = DBL_MAX;
    } else {
        /* date is days since 2000, timestamp is microseconds since same... */
        result = dateVal as f64 * USECS_PER_DAY as f64;
    }

    result
}


/*
 * Crosstype comparison functions for dates
 */

pub unsafe fn date_cmp_timestamp_internal(dateVal: DateADT, dt2: Timestamp) -> int32 {
    let dt1: Timestamp;
    let mut overflow: c_int = 0;

    dt1 = date2timestamp_opt_overflow(dateVal, &mut overflow);
    if overflow > 0 {
        /* dt1 is larger than any finite timestamp, but less than infinity */
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    Assert!(overflow == 0); /* -1 case cannot occur */

    timestamp_cmp_internal(dt1, dt2)
}

pub unsafe fn date_eq_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt2) == 0);
}

pub unsafe fn date_ne_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt2) != 0);
}

pub unsafe fn date_lt_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt2) < 0);
}

pub unsafe fn date_gt_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt2) > 0);
}

pub unsafe fn date_le_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt2) <= 0);
}

pub unsafe fn date_ge_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt2) >= 0);
}

pub unsafe fn date_cmp_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMP!(fcinfo, 1);

    PG_RETURN_INT32!(date_cmp_timestamp_internal(dateVal, dt2));
}

pub unsafe fn date_cmp_timestamptz_internal(dateVal: DateADT, dt2: TimestampTz) -> int32 {
    let dt1: TimestampTz;
    let mut overflow: c_int = 0;

    dt1 = date2timestamptz_opt_overflow(dateVal, &mut overflow);
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

pub unsafe fn date_eq_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt2) == 0);
}

pub unsafe fn date_ne_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt2) != 0);
}

pub unsafe fn date_lt_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt2) < 0);
}

pub unsafe fn date_gt_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt2) > 0);
}

pub unsafe fn date_le_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt2) <= 0);
}

pub unsafe fn date_ge_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt2) >= 0);
}

pub unsafe fn date_cmp_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let dt2 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 1);

    PG_RETURN_INT32!(date_cmp_timestamptz_internal(dateVal, dt2));
}

pub unsafe fn timestamp_eq_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt1) == 0);
}

pub unsafe fn timestamp_ne_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt1) != 0);
}

pub unsafe fn timestamp_lt_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt1) > 0);
}

pub unsafe fn timestamp_gt_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt1) < 0);
}

pub unsafe fn timestamp_le_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt1) >= 0);
}

pub unsafe fn timestamp_ge_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamp_internal(dateVal, dt1) <= 0);
}

pub unsafe fn timestamp_cmp_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_INT32!(-date_cmp_timestamp_internal(dateVal, dt1));
}

pub unsafe fn timestamptz_eq_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt1) == 0);
}

pub unsafe fn timestamptz_ne_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt1) != 0);
}

pub unsafe fn timestamptz_lt_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt1) > 0);
}

pub unsafe fn timestamptz_gt_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt1) < 0);
}

pub unsafe fn timestamptz_le_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt1) >= 0);
}

pub unsafe fn timestamptz_ge_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(date_cmp_timestamptz_internal(dateVal, dt1) <= 0);
}

pub unsafe fn timestamptz_cmp_date(fcinfo: FunctionCallInfo) -> Datum {
    let dt1 = PG_GETARG_TIMESTAMPTZ!(fcinfo, 0);
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 1);

    PG_RETURN_INT32!(-date_cmp_timestamptz_internal(dateVal, dt1));
}

/*
 * in_range support function for date.
 *
 * We implement this by promoting the dates to timestamp (without time zone)
 * and then using the timestamp-and-interval in_range function.
 */
pub unsafe fn in_range_date_interval(fcinfo: FunctionCallInfo) -> Datum {
    let val = PG_GETARG_DATEADT!(fcinfo, 0);
    let base = PG_GETARG_DATEADT!(fcinfo, 1);
    let offset = PG_GETARG_INTERVAL_P!(fcinfo, 2);
    let sub = PG_GETARG_BOOL!(fcinfo, 3);
    let less = PG_GETARG_BOOL!(fcinfo, 4);
    let valStamp: Timestamp;
    let baseStamp: Timestamp;

    /* XXX we could support out-of-range cases here, perhaps */
    valStamp = date2timestamp(val);
    baseStamp = date2timestamp(base);

    return DirectFunctionCall5!(
        in_range_timestamp_interval,
        TimestampGetDatum(valStamp),
        TimestampGetDatum(baseStamp),
        IntervalPGetDatum(offset),
        BoolGetDatum(sub),
        BoolGetDatum(less)
    );
}


/* extract_date()
 * Extract specified field from date type.
 */
pub unsafe fn extract_date(fcinfo: FunctionCallInfo) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let date = PG_GETARG_DATEADT!(fcinfo, 1);
    let mut intresult: int64 = 0;
    let r#type: c_int;
    let mut val: c_int = 0;
    let lowunits: *mut c_char;
    let mut year: c_int = 0;
    let mut mon: c_int = 0;
    let mut mday: c_int = 0;

    lowunits = downcase_truncate_identifier(
        VARDATA_ANY(units as *const c_char),
        VARSIZE_ANY_EXHDR(units as *const c_char) as c_int,
        false,
    );

    r#type = DecodeUnits(0, lowunits, &mut val);
    let r#type = if r#type == UNKNOWN_FIELD {
        DecodeSpecial(0, lowunits, &mut val)
    } else {
        r#type
    };

    if DATE_NOT_FINITE(date) && (r#type == UNITS || r#type == RESERV) {
        match val {
            /* Oscillating units */
            x if x == DTK_DAY
                || x == DTK_MONTH
                || x == DTK_QUARTER
                || x == DTK_WEEK
                || x == DTK_DOW
                || x == DTK_ISODOW
                || x == DTK_DOY =>
            {
                PG_RETURN_NULL!(fcinfo);
            }

            /* Monotonically-increasing units */
            x if x == DTK_YEAR
                || x == DTK_DECADE
                || x == DTK_CENTURY
                || x == DTK_MILLENNIUM
                || x == DTK_JULIAN
                || x == DTK_ISOYEAR
                || x == DTK_EPOCH =>
            {
                if DATE_IS_NOBEGIN(date) {
                    PG_RETURN_NUMERIC!(DatumGetNumeric(DirectFunctionCall3!(
                        numeric_in,
                        CStringGetDatum(c"-Infinity".as_ptr()),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1)
                    )));
                } else {
                    PG_RETURN_NUMERIC!(DatumGetNumeric(DirectFunctionCall3!(
                        numeric_in,
                        CStringGetDatum(c"Infinity".as_ptr()),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(-1)
                    )));
                }
            }
            _ => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(DATEOID)).to_string_lossy()
                    )
                );
                return 0;
            }
        }
    } else if r#type == UNITS {
        j2date(date + POSTGRES_EPOCH_JDATE, &mut year, &mut mon, &mut mday);

        match val {
            x if x == DTK_DAY => {
                intresult = mday as int64;
            }

            x if x == DTK_MONTH => {
                intresult = mon as int64;
            }

            x if x == DTK_QUARTER => {
                intresult = ((mon - 1) / 3 + 1) as int64;
            }

            x if x == DTK_WEEK => {
                intresult = date2isoweek(year, mon, mday) as int64;
            }

            x if x == DTK_YEAR => {
                if year > 0 {
                    intresult = year as int64;
                } else {
                    /* there is no year 0, just 1 BC and 1 AD */
                    intresult = (year - 1) as int64;
                }
            }

            x if x == DTK_DECADE => {
                /* see comments in timestamp_part */
                if year >= 0 {
                    intresult = (year / 10) as int64;
                } else {
                    intresult = -(((8 - (year - 1)) / 10) as int64);
                }
            }

            x if x == DTK_CENTURY => {
                /* see comments in timestamp_part */
                if year > 0 {
                    intresult = ((year + 99) / 100) as int64;
                } else {
                    intresult = -(((99 - (year - 1)) / 100) as int64);
                }
            }

            x if x == DTK_MILLENNIUM => {
                /* see comments in timestamp_part */
                if year > 0 {
                    intresult = ((year + 999) / 1000) as int64;
                } else {
                    intresult = -(((999 - (year - 1)) / 1000) as int64);
                }
            }

            x if x == DTK_JULIAN => {
                intresult = (date + POSTGRES_EPOCH_JDATE) as int64;
            }

            x if x == DTK_ISOYEAR => {
                intresult = date2isoyear(year, mon, mday) as int64;
                /* Adjust BC years */
                if intresult <= 0 {
                    intresult -= 1;
                }
            }

            x if x == DTK_DOW || x == DTK_ISODOW => {
                intresult = j2day(date + POSTGRES_EPOCH_JDATE) as int64;
                if val == DTK_ISODOW && intresult == 0 {
                    intresult = 7;
                }
            }

            x if x == DTK_DOY => {
                intresult = (date2j(year, mon, mday) - date2j(year, 1, 1) + 1) as int64;
            }

            _ => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(DATEOID)).to_string_lossy()
                    )
                );
                intresult = 0;
            }
        }
    } else if r#type == RESERV {
        match val {
            x if x == DTK_EPOCH => {
                intresult = (date as int64 + POSTGRES_EPOCH_JDATE as int64
                    - UNIX_EPOCH_JDATE as int64)
                    * SECS_PER_DAY as int64;
            }

            _ => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(DATEOID)).to_string_lossy()
                    )
                );
                intresult = 0;
            }
        }
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "unit \"{}\" not recognized for type {}",
                std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(DATEOID)).to_string_lossy()
            )
        );
        intresult = 0;
    }

    PG_RETURN_NUMERIC!(int64_to_numeric(intresult));
}


/* Add an interval to a date, giving a new date.
 * Must handle both positive and negative intervals.
 *
 * We implement this by promoting the date to timestamp (without time zone)
 * and then using the timestamp plus interval function.
 */
pub unsafe fn date_pl_interval(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let dateStamp: Timestamp;

    dateStamp = date2timestamp(dateVal);

    return DirectFunctionCall2!(
        timestamp_pl_interval,
        TimestampGetDatum(dateStamp),
        PointerGetDatum(span as *const c_void)
    );
}

/* Subtract an interval from a date, giving a new date.
 * Must handle both positive and negative intervals.
 *
 * We implement this by promoting the date to timestamp (without time zone)
 * and then using the timestamp minus interval function.
 */
pub unsafe fn date_mi_interval(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let dateStamp: Timestamp;

    dateStamp = date2timestamp(dateVal);

    return DirectFunctionCall2!(
        timestamp_mi_interval,
        TimestampGetDatum(dateStamp),
        PointerGetDatum(span as *const c_void)
    );
}

/* date_timestamp()
 * Convert date to timestamp data type.
 */
pub unsafe fn date_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let result: Timestamp;

    result = date2timestamp(dateVal);

    PG_RETURN_TIMESTAMP!(result);
}

/* timestamp_date()
 * Convert timestamp to date data type.
 */
pub unsafe fn timestamp_date(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let mut result: DateADT = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut fsec: fsec_t = 0;

    if TIMESTAMP_IS_NOBEGIN(timestamp) {
        DATE_NOBEGIN(&mut result);
    } else if TIMESTAMP_IS_NOEND(timestamp) {
        DATE_NOEND(&mut result);
    } else {
        if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) != 0 {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        result = date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) - POSTGRES_EPOCH_JDATE;
    }

    PG_RETURN_DATEADT!(result);
}


/* date_timestamptz()
 * Convert date to timestamp with time zone data type.
 */
pub unsafe fn date_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let dateVal = PG_GETARG_DATEADT!(fcinfo, 0);
    let result: TimestampTz;

    result = date2timestamptz(dateVal);

    PG_RETURN_TIMESTAMP!(result);
}


/* timestamptz_date()
 * Convert timestamp with time zone to date data type.
 */
pub unsafe fn timestamptz_date(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let mut result: DateADT = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut fsec: fsec_t = 0;
    let mut tz: c_int = 0;

    if TIMESTAMP_IS_NOBEGIN(timestamp) {
        DATE_NOBEGIN(&mut result);
    } else if TIMESTAMP_IS_NOEND(timestamp) {
        DATE_NOEND(&mut result);
    } else {
        if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), null_mut()) != 0 {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        result = date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) - POSTGRES_EPOCH_JDATE;
    }

    PG_RETURN_DATEADT!(result);
}


/*****************************************************************************
 *	 Time ADT
 *****************************************************************************/

pub unsafe fn time_in(fcinfo: FunctionCallInfo) -> Datum {
    let str = PG_GETARG_CSTRING!(fcinfo, 0);
    let typmod = PG_GETARG_INT32!(fcinfo, 2);
    let escontext = (*fcinfo).context;
    let mut result: TimeADT = 0;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut tz: c_int = 0;
    let mut nf: c_int = 0;
    let mut dterr: c_int;
    let mut workbuf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];
    let mut field: [*mut c_char; MAXDATEFIELDS] = [null_mut(); MAXDATEFIELDS];
    let mut dtype: c_int = 0;
    let mut ftype: [c_int; MAXDATEFIELDS] = [0; MAXDATEFIELDS];
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
        dterr = DecodeTimeOnly(
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
        DateTimeParseError(dterr, &extra, str, c"time".as_ptr(), escontext as *mut c_void);
        PG_RETURN_NULL!(fcinfo);
    }

    tm2time(tm, fsec, &mut result);
    AdjustTimeForTypmod(&mut result, typmod);

    PG_RETURN_TIMEADT!(result);
}

/* tm2time()
 * Convert a tm structure to a time data type.
 */
pub unsafe fn tm2time(tm: *mut pg_tm, fsec: fsec_t, result: *mut TimeADT) -> c_int {
    *result = ((((*tm).tm_hour as int64 * MINS_PER_HOUR as int64 + (*tm).tm_min as int64)
        * SECS_PER_MINUTE as int64)
        + (*tm).tm_sec as int64)
        * USECS_PER_SEC
        + fsec as int64;
    0
}

/* time_overflows()
 * Check to see if a broken-down time-of-day is out of range.
 */
pub unsafe fn time_overflows(hour: c_int, min: c_int, sec: c_int, fsec: fsec_t) -> bool {
    /* Range-check the fields individually. */
    if hour < 0
        || hour > HOURS_PER_DAY
        || min < 0
        || min >= MINS_PER_HOUR
        || sec < 0
        || sec > SECS_PER_MINUTE
        || fsec < 0
        || fsec as int64 > USECS_PER_SEC
    {
        return true;
    }

    /*
     * Because we allow, eg, hour = 24 or sec = 60, we must check separately
     * that the total time value doesn't exceed 24:00:00.
     */
    if (((((hour as int64 * MINS_PER_HOUR as int64 + min as int64) * SECS_PER_MINUTE as int64)
        + sec as int64)
        * USECS_PER_SEC)
        + fsec as int64)
        > USECS_PER_DAY
    {
        return true;
    }

    false
}

/* float_time_overflows()
 * Same, when we have seconds + fractional seconds as one "double" value.
 */
pub unsafe fn float_time_overflows(hour: c_int, min: c_int, mut sec: f64) -> bool {
    /* Range-check the fields individually. */
    if hour < 0 || hour > HOURS_PER_DAY || min < 0 || min >= MINS_PER_HOUR {
        return true;
    }

    /*
     * "sec", being double, requires extra care.  Cope with NaN, and round off
     * before applying the range check to avoid unexpected errors due to
     * imprecise input.  (We assume rint() behaves sanely with infinities.)
     */
    if isnan(sec) != 0 {
        return true;
    }
    sec = rint(sec * USECS_PER_SEC as f64);
    if sec < 0.0 || sec > (SECS_PER_MINUTE as int64 * USECS_PER_SEC) as f64 {
        return true;
    }

    /*
     * Because we allow, eg, hour = 24 or sec = 60, we must check separately
     * that the total time value doesn't exceed 24:00:00.  This must match the
     * way that callers will convert the fields to a time.
     */
    if ((((hour as int64 * MINS_PER_HOUR as int64 + min as int64) * SECS_PER_MINUTE as int64)
        * USECS_PER_SEC)
        + sec as int64)
        > USECS_PER_DAY
    {
        return true;
    }

    false
}


/* time2tm()
 * Convert time data type to POSIX time structure.
 *
 * Note that only the hour/min/sec/fractional-sec fields are filled in.
 */
pub unsafe fn time2tm(mut time: TimeADT, tm: *mut pg_tm, fsec: *mut fsec_t) -> c_int {
    (*tm).tm_hour = (time / USECS_PER_HOUR) as c_int;
    time -= (*tm).tm_hour as int64 * USECS_PER_HOUR;
    (*tm).tm_min = (time / USECS_PER_MINUTE) as c_int;
    time -= (*tm).tm_min as int64 * USECS_PER_MINUTE;
    (*tm).tm_sec = (time / USECS_PER_SEC) as c_int;
    time -= (*tm).tm_sec as int64 * USECS_PER_SEC;
    *fsec = time as fsec_t;
    0
}

pub unsafe fn time_out(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMEADT!(fcinfo, 0);
    let result: *mut c_char;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut fsec: fsec_t = 0;
    let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

    time2tm(time, tm, &mut fsec);
    EncodeTimeOnly(tm, fsec, false, 0, DateStyle, buf.as_mut_ptr());

    result = pstrdup(buf.as_ptr());
    PG_RETURN_CSTRING!(result);
}

/*
 *		time_recv			- converts external binary format to time
 */
pub unsafe fn time_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    let typmod = PG_GETARG_INT32!(fcinfo, 2);
    let mut result: TimeADT;

    result = pq_getmsgint64(buf);

    if result < 0 || result > USECS_PER_DAY {
        ereport!(ERROR, errmsg!("time out of range"));
    }

    AdjustTimeForTypmod(&mut result, typmod);

    PG_RETURN_TIMEADT!(result);
}

/*
 *		time_send			- converts time to binary format
 */
pub unsafe fn time_send(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMEADT!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint64(&mut buf, time as uint64);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void);
}

pub unsafe fn timetypmodin(fcinfo: FunctionCallInfo) -> Datum {
    let ta = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    PG_RETURN_INT32!(anytime_typmodin(false, ta));
}

pub unsafe fn timetypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_CSTRING!(anytime_typmodout(false, typmod));
}

/*
 *		make_time			- time constructor
 */
pub unsafe fn make_time(fcinfo: FunctionCallInfo) -> Datum {
    let tm_hour = PG_GETARG_INT32!(fcinfo, 0);
    let tm_min = PG_GETARG_INT32!(fcinfo, 1);
    let sec = PG_GETARG_FLOAT8!(fcinfo, 2);
    let time: TimeADT;

    /* Check for time overflow */
    if float_time_overflows(tm_hour, tm_min, sec) {
        ereport!(
            ERROR,
            errmsg!(
                "time field value out of range: {}:{:02}:{:02}",
                tm_hour,
                tm_min,
                sec
            )
        );
    }

    /* This should match tm2time */
    time = ((tm_hour as int64 * MINS_PER_HOUR as int64 + tm_min as int64)
        * SECS_PER_MINUTE as int64)
        * USECS_PER_SEC
        + rint(sec * USECS_PER_SEC as f64) as int64;

    PG_RETURN_TIMEADT!(time);
}


/* time_support()
 *
 * Planner support function for the time_scale() and timetz_scale()
 * length coercion functions (we need not distinguish them here).
 */
pub unsafe fn time_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = null_mut();

    if IsA_SupportRequestSimplify(rawreq) {
        let req = rawreq as *mut SupportRequestSimplify;

        ret = TemporalSimplify(MAX_TIME_PRECISION, (*req).fcall as *mut Node);
    }

    return PointerGetDatum(ret as *const c_void);
}

/* time_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
pub unsafe fn time_scale(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMEADT!(fcinfo, 0);
    let typmod = PG_GETARG_INT32!(fcinfo, 1);
    let mut result: TimeADT;

    result = time;
    AdjustTimeForTypmod(&mut result, typmod);

    PG_RETURN_TIMEADT!(result);
}

/* AdjustTimeForTypmod()
 * Force the precision of the time value to a specified value.
 * Uses *exactly* the same code as in AdjustTimestampForTypmod()
 * but we make a separate copy because those types do not
 * have a fundamental tie together but rather a coincidence of
 * implementation. - thomas
 */
pub unsafe fn AdjustTimeForTypmod(time: *mut TimeADT, typmod: int32) {
    static TimeScales: [int64; (MAX_TIME_PRECISION + 1) as usize] =
        [1000000, 100000, 10000, 1000, 100, 10, 1];

    static TimeOffsets: [int64; (MAX_TIME_PRECISION + 1) as usize] =
        [500000, 50000, 5000, 500, 50, 5, 0];

    if typmod >= 0 && typmod <= MAX_TIME_PRECISION {
        if *time >= 0 {
            *time = ((*time + TimeOffsets[typmod as usize]) / TimeScales[typmod as usize])
                * TimeScales[typmod as usize];
        } else {
            *time = -((((-*time) + TimeOffsets[typmod as usize]) / TimeScales[typmod as usize])
                * TimeScales[typmod as usize]);
        }
    }
}


pub unsafe fn time_eq(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(time1 == time2);
}

pub unsafe fn time_ne(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(time1 != time2);
}

pub unsafe fn time_lt(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(time1 < time2);
}

pub unsafe fn time_le(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(time1 <= time2);
}

pub unsafe fn time_gt(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(time1 > time2);
}

pub unsafe fn time_ge(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    PG_RETURN_BOOL!(time1 >= time2);
}

pub unsafe fn time_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    if time1 < time2 {
        PG_RETURN_INT32!(-1);
    }
    if time1 > time2 {
        PG_RETURN_INT32!(1);
    }
    PG_RETURN_INT32!(0);
}

pub unsafe fn time_hash(fcinfo: FunctionCallInfo) -> Datum {
    return hashint8(fcinfo);
}

pub unsafe fn time_hash_extended(fcinfo: FunctionCallInfo) -> Datum {
    return hashint8extended(fcinfo);
}

pub unsafe fn time_larger(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    PG_RETURN_TIMEADT!(if time1 > time2 { time1 } else { time2 });
}

pub unsafe fn time_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);

    PG_RETURN_TIMEADT!(if time1 < time2 { time1 } else { time2 });
}

/* overlaps_time() --- implements the SQL OVERLAPS operator.
 *
 * Algorithm is per SQL spec.  This is much harder than you'd think
 * because the spec requires us to deliver a non-null answer in some cases
 * where some of the inputs are null.
 */
pub unsafe fn overlaps_time(fcinfo: FunctionCallInfo) -> Datum {
    /*
     * The arguments are TimeADT, but we leave them as generic Datums to avoid
     * dereferencing nulls (TimeADT is pass-by-reference!)
     */
    let mut ts1 = PG_GETARG_DATUM!(fcinfo, 0);
    let mut te1 = PG_GETARG_DATUM!(fcinfo, 1);
    let mut ts2 = PG_GETARG_DATUM!(fcinfo, 2);
    let mut te2 = PG_GETARG_DATUM!(fcinfo, 3);
    let ts1IsNull = PG_ARGISNULL!(fcinfo, 0);
    let mut te1IsNull = PG_ARGISNULL!(fcinfo, 1);
    let ts2IsNull = PG_ARGISNULL!(fcinfo, 2);
    let mut te2IsNull = PG_ARGISNULL!(fcinfo, 3);

    macro_rules! TIMEADT_GT {
        ($t1:expr, $t2:expr) => {
            DatumGetTimeADT($t1) > DatumGetTimeADT($t2)
        };
    }
    macro_rules! TIMEADT_LT {
        ($t1:expr, $t2:expr) => {
            DatumGetTimeADT($t1) < DatumGetTimeADT($t2)
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
        if TIMEADT_GT!(ts1, te1) {
            let tt = ts1;

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
        if TIMEADT_GT!(ts2, te2) {
            let tt = ts2;

            ts2 = te2;
            te2 = tt;
        }
    }

    /*
     * At this point neither ts1 nor ts2 is null, so we can consider three
     * cases: ts1 > ts2, ts1 < ts2, ts1 = ts2
     */
    if TIMEADT_GT!(ts1, ts2) {
        /*
         * This case is ts1 < te2 OR te1 < te2, which may look redundant but
         * in the presence of nulls it's not quite completely so.
         */
        if te2IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        if TIMEADT_LT!(ts1, te2) {
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
    } else if TIMEADT_LT!(ts1, ts2) {
        /* This case is ts2 < te1 OR te2 < te1 */
        if te1IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        if TIMEADT_LT!(ts2, te1) {
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
         * rather silly way of saying "true if both are nonnull, else null".
         */
        if te1IsNull || te2IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        PG_RETURN_BOOL!(true);
    }
}

/* timestamp_time()
 * Convert timestamp to time data type.
 */
pub unsafe fn timestamp_time(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let result: TimeADT;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut fsec: fsec_t = 0;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_NULL!(fcinfo);
    }

    if timestamp2tm(timestamp, null_mut(), tm, &mut fsec, null_mut(), null_mut()) != 0 {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    /*
     * Could also do this with time = (timestamp / USECS_PER_DAY *
     * USECS_PER_DAY) - timestamp;
     */
    result = ((((*tm).tm_hour as int64 * MINS_PER_HOUR as int64 + (*tm).tm_min as int64)
        * SECS_PER_MINUTE as int64)
        + (*tm).tm_sec as int64)
        * USECS_PER_SEC
        + fsec as int64;

    PG_RETURN_TIMEADT!(result);
}

/* timestamptz_time()
 * Convert timestamptz to time data type.
 */
pub unsafe fn timestamptz_time(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let result: TimeADT;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut tz: c_int = 0;
    let mut fsec: fsec_t = 0;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_NULL!(fcinfo);
    }

    if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), null_mut()) != 0 {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    /*
     * Could also do this with time = (timestamp / USECS_PER_DAY *
     * USECS_PER_DAY) - timestamp;
     */
    result = ((((*tm).tm_hour as int64 * MINS_PER_HOUR as int64 + (*tm).tm_min as int64)
        * SECS_PER_MINUTE as int64)
        + (*tm).tm_sec as int64)
        * USECS_PER_SEC
        + fsec as int64;

    PG_RETURN_TIMEADT!(result);
}

/* datetime_timestamp()
 * Convert date and time to timestamp data type.
 */
pub unsafe fn datetime_timestamp(fcinfo: FunctionCallInfo) -> Datum {
    let date = PG_GETARG_DATEADT!(fcinfo, 0);
    let time = PG_GETARG_TIMEADT!(fcinfo, 1);
    let mut result: Timestamp;

    result = date2timestamp(date);
    if !TIMESTAMP_NOT_FINITE(result) {
        result += time;
        if !IS_VALID_TIMESTAMP(result) {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
    }

    PG_RETURN_TIMESTAMP!(result);
}

/* time_interval()
 * Convert time to interval data type.
 */
pub unsafe fn time_interval(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMEADT!(fcinfo, 0);
    let result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    (*result).time = time;
    (*result).day = 0;
    (*result).month = 0;

    PG_RETURN_INTERVAL_P!(result);
}

/* interval_time()
 * Convert interval to time data type.
 *
 * This is defined as producing the fractional-day portion of the interval.
 * Therefore, we can just ignore the months field.  It is not real clear
 * what to do with negative intervals, but we choose to subtract the floor,
 * so that, say, '-2 hours' becomes '22:00:00'.
 */
pub unsafe fn interval_time(fcinfo: FunctionCallInfo) -> Datum {
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let mut result: TimeADT;

    if INTERVAL_NOT_FINITE(span) {
        ereport!(ERROR, errmsg!("cannot convert infinite interval to time"));
    }

    result = (*span).time % USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }

    PG_RETURN_TIMEADT!(result);
}

/* time_mi_time()
 * Subtract two times to produce an interval.
 */
pub unsafe fn time_mi_time(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMEADT!(fcinfo, 0);
    let time2 = PG_GETARG_TIMEADT!(fcinfo, 1);
    let result: *mut Interval;

    result = palloc(std::mem::size_of::<Interval>() as Size) as *mut Interval;

    (*result).month = 0;
    (*result).day = 0;
    (*result).time = time1 - time2;

    PG_RETURN_INTERVAL_P!(result);
}

/* time_pl_interval()
 * Add interval to time.
 */
pub unsafe fn time_pl_interval(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMEADT!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let mut result: TimeADT;

    if INTERVAL_NOT_FINITE(span) {
        ereport!(ERROR, errmsg!("cannot add infinite interval to time"));
    }

    result = time + (*span).time;
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }

    PG_RETURN_TIMEADT!(result);
}

/* time_mi_interval()
 * Subtract interval from time.
 */
pub unsafe fn time_mi_interval(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMEADT!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let mut result: TimeADT;

    if INTERVAL_NOT_FINITE(span) {
        ereport!(ERROR, errmsg!("cannot subtract infinite interval from time"));
    }

    result = time - (*span).time;
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }

    PG_RETURN_TIMEADT!(result);
}

/*
 * in_range support function for time.
 */
pub unsafe fn in_range_time_interval(fcinfo: FunctionCallInfo) -> Datum {
    let val = PG_GETARG_TIMEADT!(fcinfo, 0);
    let base = PG_GETARG_TIMEADT!(fcinfo, 1);
    let offset = PG_GETARG_INTERVAL_P!(fcinfo, 2);
    let sub = PG_GETARG_BOOL!(fcinfo, 3);
    let less = PG_GETARG_BOOL!(fcinfo, 4);
    let mut sum: TimeADT = 0;

    /*
     * Like time_pl_interval/time_mi_interval, we disregard the month and day
     * fields of the offset.  So our test for negative should too.  This also
     * catches -infinity, so we only need worry about +infinity below.
     */
    if (*offset).time < 0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }

    /*
     * We can't use time_pl_interval/time_mi_interval here, because their
     * wraparound behavior would give wrong (or at least undesirable) answers.
     * Fortunately the equivalent non-wrapping behavior is trivial, except
     * that adding an infinite (or very large) interval might cause integer
     * overflow.  Subtraction cannot overflow here.
     */
    if sub {
        sum = base - (*offset).time;
    } else if pg_add_s64_overflow(base, (*offset).time, &mut sum) {
        PG_RETURN_BOOL!(less);
    }

    if less {
        PG_RETURN_BOOL!(val <= sum);
    } else {
        PG_RETURN_BOOL!(val >= sum);
    }
}


/* time_part() and extract_time()
 * Extract specified field from time type.
 */
unsafe fn time_part_common(fcinfo: FunctionCallInfo, retnumeric: bool) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let time = PG_GETARG_TIMEADT!(fcinfo, 1);
    let mut intresult: int64 = 0;
    let r#type: c_int;
    let mut val: c_int = 0;
    let lowunits: *mut c_char;

    lowunits = downcase_truncate_identifier(
        VARDATA_ANY(units as *const c_char),
        VARSIZE_ANY_EXHDR(units as *const c_char) as c_int,
        false,
    );

    r#type = DecodeUnits(0, lowunits, &mut val);
    let r#type = if r#type == UNKNOWN_FIELD {
        DecodeSpecial(0, lowunits, &mut val)
    } else {
        r#type
    };

    if r#type == UNITS {
        let mut fsec: fsec_t = 0;
        let mut tt: pg_tm = std::mem::zeroed();
        let tm: *mut pg_tm = &raw mut tt;

        time2tm(time, tm, &mut fsec);

        match val {
            x if x == DTK_MICROSEC => {
                intresult = (*tm).tm_sec as int64 * 1000000 + fsec as int64;
            }

            x if x == DTK_MILLISEC => {
                if retnumeric {
                    /*---
                     * tm->tm_sec * 1000 + fsec / 1000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric(
                        (*tm).tm_sec as int64 * 1000000 + fsec as int64,
                        3
                    ));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 * 1000.0 + fsec as f64 / 1000.0);
                }
            }

            x if x == DTK_SECOND => {
                if retnumeric {
                    /*---
                     * tm->tm_sec + fsec / 1'000'000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric(
                        (*tm).tm_sec as int64 * 1000000 + fsec as int64,
                        6
                    ));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 + fsec as f64 / 1000000.0);
                }
            }

            x if x == DTK_MINUTE => {
                intresult = (*tm).tm_min as int64;
            }

            x if x == DTK_HOUR => {
                intresult = (*tm).tm_hour as int64;
            }

            x if x == DTK_TZ
                || x == DTK_TZ_MINUTE
                || x == DTK_TZ_HOUR
                || x == DTK_DAY
                || x == DTK_MONTH
                || x == DTK_QUARTER
                || x == DTK_YEAR
                || x == DTK_DECADE
                || x == DTK_CENTURY
                || x == DTK_MILLENNIUM
                || x == DTK_ISOYEAR =>
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(TIMEOID)).to_string_lossy()
                    )
                );
                intresult = 0;
            }
            _ => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(TIMEOID)).to_string_lossy()
                    )
                );
                intresult = 0;
            }
        }
    } else if r#type == RESERV && val == DTK_EPOCH {
        if retnumeric {
            PG_RETURN_NUMERIC!(int64_div_fast_to_numeric(time, 6));
        } else {
            PG_RETURN_FLOAT8!(time as f64 / 1000000.0);
        }
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "unit \"{}\" not recognized for type {}",
                std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(TIMEOID)).to_string_lossy()
            )
        );
        intresult = 0;
    }

    if retnumeric {
        PG_RETURN_NUMERIC!(int64_to_numeric(intresult));
    } else {
        PG_RETURN_FLOAT8!(intresult as f64);
    }
}

pub unsafe fn time_part(fcinfo: FunctionCallInfo) -> Datum {
    return time_part_common(fcinfo, false);
}

pub unsafe fn extract_time(fcinfo: FunctionCallInfo) -> Datum {
    return time_part_common(fcinfo, true);
}


/*****************************************************************************
 *	 Time With Time Zone ADT
 *****************************************************************************/

/* tm2timetz()
 * Convert a tm structure to a time data type.
 */
pub unsafe fn tm2timetz(tm: *mut pg_tm, fsec: fsec_t, tz: c_int, result: *mut TimeTzADT) -> c_int {
    (*result).time = ((((*tm).tm_hour as int64 * MINS_PER_HOUR as int64 + (*tm).tm_min as int64)
        * SECS_PER_MINUTE as int64)
        + (*tm).tm_sec as int64)
        * USECS_PER_SEC
        + fsec as int64;
    (*result).zone = tz;

    0
}

pub unsafe fn timetz_in(fcinfo: FunctionCallInfo) -> Datum {
    let str = PG_GETARG_CSTRING!(fcinfo, 0);
    let typmod = PG_GETARG_INT32!(fcinfo, 2);
    let escontext = (*fcinfo).context;
    let result: *mut TimeTzADT;
    let mut fsec: fsec_t = 0;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut tz: c_int = 0;
    let mut nf: c_int = 0;
    let mut dterr: c_int;
    let mut workbuf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];
    let mut field: [*mut c_char; MAXDATEFIELDS] = [null_mut(); MAXDATEFIELDS];
    let mut dtype: c_int = 0;
    let mut ftype: [c_int; MAXDATEFIELDS] = [0; MAXDATEFIELDS];
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
        dterr = DecodeTimeOnly(
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
            c"time with time zone".as_ptr(),
            escontext as *mut c_void,
        );
        PG_RETURN_NULL!(fcinfo);
    }

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;
    tm2timetz(tm, fsec, tz, result);
    AdjustTimeForTypmod(&raw mut (*result).time, typmod);

    PG_RETURN_TIMETZADT_P!(result);
}

pub unsafe fn timetz_out(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let result: *mut c_char;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut fsec: fsec_t = 0;
    let mut tz: c_int = 0;
    let mut buf: [c_char; MAXDATELEN + 1] = [0; MAXDATELEN + 1];

    timetz2tm(time, tm, &mut fsec, &mut tz);
    EncodeTimeOnly(tm, fsec, true, tz, DateStyle, buf.as_mut_ptr());

    result = pstrdup(buf.as_ptr());
    PG_RETURN_CSTRING!(result);
}

/*
 *		timetz_recv			- converts external binary format to timetz
 */
pub unsafe fn timetz_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;

    let typmod = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut TimeTzADT;

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;

    (*result).time = pq_getmsgint64(buf);

    if (*result).time < 0 || (*result).time > USECS_PER_DAY {
        ereport!(ERROR, errmsg!("time out of range"));
    }

    (*result).zone = pq_getmsgint(buf, std::mem::size_of_val(&(*result).zone) as c_int) as int32;

    /* Check for sane GMT displacement; see notes in datatype/timestamp.h */
    if (*result).zone <= -TZDISP_LIMIT || (*result).zone >= TZDISP_LIMIT {
        ereport!(ERROR, errmsg!("time zone displacement out of range"));
    }

    AdjustTimeForTypmod(&raw mut (*result).time, typmod);

    PG_RETURN_TIMETZADT_P!(result);
}

/*
 *		timetz_send			- converts timetz to binary format
 */
pub unsafe fn timetz_send(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendint64(&mut buf, (*time).time as uint64);
    pq_sendint32(&mut buf, (*time).zone as uint32);
    return PointerGetDatum(pq_endtypsend(&mut buf) as *const c_void);
}

pub unsafe fn timetztypmodin(fcinfo: FunctionCallInfo) -> Datum {
    let ta = PG_GETARG_ARRAYTYPE_P!(fcinfo, 0);

    PG_RETURN_INT32!(anytime_typmodin(true, ta));
}

pub unsafe fn timetztypmodout(fcinfo: FunctionCallInfo) -> Datum {
    let typmod = PG_GETARG_INT32!(fcinfo, 0);

    PG_RETURN_CSTRING!(anytime_typmodout(true, typmod));
}


/* timetz2tm()
 * Convert TIME WITH TIME ZONE data type to POSIX time structure.
 */
pub unsafe fn timetz2tm(time: *mut TimeTzADT, tm: *mut pg_tm, fsec: *mut fsec_t, tzp: *mut c_int) -> c_int {
    let mut trem: TimeOffset = (*time).time;

    (*tm).tm_hour = (trem / USECS_PER_HOUR) as c_int;
    trem -= (*tm).tm_hour as int64 * USECS_PER_HOUR;
    (*tm).tm_min = (trem / USECS_PER_MINUTE) as c_int;
    trem -= (*tm).tm_min as int64 * USECS_PER_MINUTE;
    (*tm).tm_sec = (trem / USECS_PER_SEC) as c_int;
    *fsec = (trem - (*tm).tm_sec as int64 * USECS_PER_SEC) as fsec_t;

    if !tzp.is_null() {
        *tzp = (*time).zone;
    }

    0
}

/* timetz_scale()
 * Adjust time type for specified scale factor.
 * Used by PostgreSQL type system to stuff columns.
 */
pub unsafe fn timetz_scale(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let typmod = PG_GETARG_INT32!(fcinfo, 1);
    let result: *mut TimeTzADT;

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;

    (*result).time = (*time).time;
    (*result).zone = (*time).zone;

    AdjustTimeForTypmod(&raw mut (*result).time, typmod);

    PG_RETURN_TIMETZADT_P!(result);
}


unsafe fn timetz_cmp_internal(time1: *mut TimeTzADT, time2: *mut TimeTzADT) -> c_int {
    let t1: TimeOffset;
    let t2: TimeOffset;

    /* Primary sort is by true (GMT-equivalent) time */
    t1 = (*time1).time + ((*time1).zone as int64 * USECS_PER_SEC);
    t2 = (*time2).time + ((*time2).zone as int64 * USECS_PER_SEC);

    if t1 > t2 {
        return 1;
    }
    if t1 < t2 {
        return -1;
    }

    /*
     * If same GMT time, sort by timezone; we only want to say that two
     * timetz's are equal if both the time and zone parts are equal.
     */
    if (*time1).zone > (*time2).zone {
        return 1;
    }
    if (*time1).zone < (*time2).zone {
        return -1;
    }

    0
}

pub unsafe fn timetz_eq(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(timetz_cmp_internal(time1, time2) == 0);
}

pub unsafe fn timetz_ne(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(timetz_cmp_internal(time1, time2) != 0);
}

pub unsafe fn timetz_lt(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(timetz_cmp_internal(time1, time2) < 0);
}

pub unsafe fn timetz_le(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(timetz_cmp_internal(time1, time2) <= 0);
}

pub unsafe fn timetz_gt(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(timetz_cmp_internal(time1, time2) > 0);
}

pub unsafe fn timetz_ge(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);

    PG_RETURN_BOOL!(timetz_cmp_internal(time1, time2) >= 0);
}

pub unsafe fn timetz_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);

    PG_RETURN_INT32!(timetz_cmp_internal(time1, time2));
}

pub unsafe fn timetz_hash(fcinfo: FunctionCallInfo) -> Datum {
    let key = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let mut thash: uint32;

    /*
     * To avoid any problems with padding bytes in the struct, we figure the
     * field hashes separately and XOR them.
     */
    thash = DatumGetUInt32(DirectFunctionCall1!(hashint8, Int64GetDatumFast((*key).time)));
    thash ^= DatumGetUInt32(hash_uint32((*key).zone as uint32));
    PG_RETURN_UINT32!(thash);
}

pub unsafe fn timetz_hash_extended(fcinfo: FunctionCallInfo) -> Datum {
    let key = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let seed = PG_GETARG_DATUM!(fcinfo, 1);
    let mut thash: uint64;

    /* Same approach as timetz_hash */
    thash = DatumGetUInt64(DirectFunctionCall2!(
        hashint8extended,
        Int64GetDatumFast((*key).time),
        seed
    ));
    thash ^= DatumGetUInt64(hash_uint32_extended((*key).zone as uint32, DatumGetInt64(seed) as uint64));
    PG_RETURN_UINT64!(thash);
}

pub unsafe fn timetz_larger(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);
    let result: *mut TimeTzADT;

    if timetz_cmp_internal(time1, time2) > 0 {
        result = time1;
    } else {
        result = time2;
    }
    PG_RETURN_TIMETZADT_P!(result);
}

pub unsafe fn timetz_smaller(fcinfo: FunctionCallInfo) -> Datum {
    let time1 = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let time2 = PG_GETARG_TIMETZADT_P!(fcinfo, 1);
    let result: *mut TimeTzADT;

    if timetz_cmp_internal(time1, time2) < 0 {
        result = time1;
    } else {
        result = time2;
    }
    PG_RETURN_TIMETZADT_P!(result);
}

/* timetz_pl_interval()
 * Add interval to timetz.
 */
pub unsafe fn timetz_pl_interval(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let result: *mut TimeTzADT;

    if INTERVAL_NOT_FINITE(span) {
        ereport!(ERROR, errmsg!("cannot add infinite interval to time"));
    }

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;

    (*result).time = (*time).time + (*span).time;
    (*result).time -= (*result).time / USECS_PER_DAY * USECS_PER_DAY;
    if (*result).time < 0 {
        (*result).time += USECS_PER_DAY;
    }

    (*result).zone = (*time).zone;

    PG_RETURN_TIMETZADT_P!(result);
}

/* timetz_mi_interval()
 * Subtract interval from timetz.
 */
pub unsafe fn timetz_mi_interval(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let span = PG_GETARG_INTERVAL_P!(fcinfo, 1);
    let result: *mut TimeTzADT;

    if INTERVAL_NOT_FINITE(span) {
        ereport!(ERROR, errmsg!("cannot subtract infinite interval from time"));
    }

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;

    (*result).time = (*time).time - (*span).time;
    (*result).time -= (*result).time / USECS_PER_DAY * USECS_PER_DAY;
    if (*result).time < 0 {
        (*result).time += USECS_PER_DAY;
    }

    (*result).zone = (*time).zone;

    PG_RETURN_TIMETZADT_P!(result);
}

/*
 * in_range support function for timetz.
 */
pub unsafe fn in_range_timetz_interval(fcinfo: FunctionCallInfo) -> Datum {
    let val = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let base = PG_GETARG_TIMETZADT_P!(fcinfo, 1);
    let offset = PG_GETARG_INTERVAL_P!(fcinfo, 2);
    let sub = PG_GETARG_BOOL!(fcinfo, 3);
    let less = PG_GETARG_BOOL!(fcinfo, 4);
    let mut sum: TimeTzADT = TimeTzADT { time: 0, zone: 0 };

    /*
     * Like timetz_pl_interval/timetz_mi_interval, we disregard the month and
     * day fields of the offset.  So our test for negative should too. This
     * also catches -infinity, so we only need worry about +infinity below.
     */
    if (*offset).time < 0 {
        ereport!(
            ERROR,
            errmsg!("invalid preceding or following size in window function")
        );
    }

    /*
     * We can't use timetz_pl_interval/timetz_mi_interval here, because their
     * wraparound behavior would give wrong (or at least undesirable) answers.
     * Fortunately the equivalent non-wrapping behavior is trivial, except
     * that adding an infinite (or very large) interval might cause integer
     * overflow.  Subtraction cannot overflow here.
     */
    if sub {
        sum.time = (*base).time - (*offset).time;
    } else if pg_add_s64_overflow((*base).time, (*offset).time, &raw mut sum.time) {
        PG_RETURN_BOOL!(less);
    }
    sum.zone = (*base).zone;

    if less {
        PG_RETURN_BOOL!(timetz_cmp_internal(val, &mut sum) <= 0);
    } else {
        PG_RETURN_BOOL!(timetz_cmp_internal(val, &mut sum) >= 0);
    }
}

/* overlaps_timetz() --- implements the SQL OVERLAPS operator.
 *
 * Algorithm is per SQL spec.  This is much harder than you'd think
 * because the spec requires us to deliver a non-null answer in some cases
 * where some of the inputs are null.
 */
pub unsafe fn overlaps_timetz(fcinfo: FunctionCallInfo) -> Datum {
    /*
     * The arguments are TimeTzADT *, but we leave them as generic Datums for
     * convenience of notation --- and to avoid dereferencing nulls.
     */
    let mut ts1 = PG_GETARG_DATUM!(fcinfo, 0);
    let mut te1 = PG_GETARG_DATUM!(fcinfo, 1);
    let mut ts2 = PG_GETARG_DATUM!(fcinfo, 2);
    let mut te2 = PG_GETARG_DATUM!(fcinfo, 3);
    let ts1IsNull = PG_ARGISNULL!(fcinfo, 0);
    let mut te1IsNull = PG_ARGISNULL!(fcinfo, 1);
    let ts2IsNull = PG_ARGISNULL!(fcinfo, 2);
    let mut te2IsNull = PG_ARGISNULL!(fcinfo, 3);

    macro_rules! TIMETZ_GT {
        ($t1:expr, $t2:expr) => {
            DatumGetBool(DirectFunctionCall2!(timetz_gt, $t1, $t2))
        };
    }
    macro_rules! TIMETZ_LT {
        ($t1:expr, $t2:expr) => {
            DatumGetBool(DirectFunctionCall2!(timetz_lt, $t1, $t2))
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
        if TIMETZ_GT!(ts1, te1) {
            let tt = ts1;

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
        if TIMETZ_GT!(ts2, te2) {
            let tt = ts2;

            ts2 = te2;
            te2 = tt;
        }
    }

    /*
     * At this point neither ts1 nor ts2 is null, so we can consider three
     * cases: ts1 > ts2, ts1 < ts2, ts1 = ts2
     */
    if TIMETZ_GT!(ts1, ts2) {
        /*
         * This case is ts1 < te2 OR te1 < te2, which may look redundant but
         * in the presence of nulls it's not quite completely so.
         */
        if te2IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        if TIMETZ_LT!(ts1, te2) {
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
    } else if TIMETZ_LT!(ts1, ts2) {
        /* This case is ts2 < te1 OR te2 < te1 */
        if te1IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        if TIMETZ_LT!(ts2, te1) {
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
         * rather silly way of saying "true if both are nonnull, else null".
         */
        if te1IsNull || te2IsNull {
            PG_RETURN_NULL!(fcinfo);
        }
        PG_RETURN_BOOL!(true);
    }
}


pub unsafe fn timetz_time(fcinfo: FunctionCallInfo) -> Datum {
    let timetz = PG_GETARG_TIMETZADT_P!(fcinfo, 0);
    let result: TimeADT;

    /* swallow the time zone and just return the time */
    result = (*timetz).time;

    PG_RETURN_TIMEADT!(result);
}


pub unsafe fn time_timetz(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_TIMEADT!(fcinfo, 0);
    let result: *mut TimeTzADT;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut fsec: fsec_t = 0;
    let tz: c_int;

    GetCurrentDateTime(tm);
    time2tm(time, tm, &mut fsec);
    tz = DetermineTimeZoneOffset(tm, session_timezone);

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;

    (*result).time = time;
    (*result).zone = tz;

    PG_RETURN_TIMETZADT_P!(result);
}


/* timestamptz_timetz()
 * Convert timestamp to timetz data type.
 */
pub unsafe fn timestamptz_timetz(fcinfo: FunctionCallInfo) -> Datum {
    let timestamp = PG_GETARG_TIMESTAMP!(fcinfo, 0);
    let result: *mut TimeTzADT;
    let mut tt: pg_tm = std::mem::zeroed();
    let tm: *mut pg_tm = &raw mut tt;
    let mut tz: c_int = 0;
    let mut fsec: fsec_t = 0;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        PG_RETURN_NULL!(fcinfo);
    }

    if timestamp2tm(timestamp, &mut tz, tm, &mut fsec, null_mut(), null_mut()) != 0 {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;

    tm2timetz(tm, fsec, tz, result);

    PG_RETURN_TIMETZADT_P!(result);
}


/* datetimetz_timestamptz()
 * Convert date and timetz to timestamp with time zone data type.
 * Timestamp is stored in GMT, so add the time zone
 * stored with the timetz to the result.
 * - thomas 2000-03-10
 */
pub unsafe fn datetimetz_timestamptz(fcinfo: FunctionCallInfo) -> Datum {
    let date = PG_GETARG_DATEADT!(fcinfo, 0);
    let time = PG_GETARG_TIMETZADT_P!(fcinfo, 1);
    let mut result: TimestampTz = 0;

    if DATE_IS_NOBEGIN(date) {
        TIMESTAMP_NOBEGIN(&mut result);
    } else if DATE_IS_NOEND(date) {
        TIMESTAMP_NOEND(&mut result);
    } else {
        /*
         * Date's range is wider than timestamp's, so check for boundaries.
         * Since dates have the same minimum values as timestamps, only upper
         * boundary need be checked for overflow.
         */
        if date >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE) {
            ereport!(ERROR, errmsg!("date out of range for timestamp"));
        }
        result = date as int64 * USECS_PER_DAY + (*time).time + (*time).zone as int64 * USECS_PER_SEC;

        /*
         * Since it is possible to go beyond allowed timestamptz range because
         * of time zone, check for allowed timestamp range after adding tz.
         */
        if !IS_VALID_TIMESTAMP(result) {
            ereport!(ERROR, errmsg!("date out of range for timestamp"));
        }
    }

    PG_RETURN_TIMESTAMP!(result);
}


/* timetz_part() and extract_timetz()
 * Extract specified field from time type.
 */
unsafe fn timetz_part_common(fcinfo: FunctionCallInfo, retnumeric: bool) -> Datum {
    let units = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let time = PG_GETARG_TIMETZADT_P!(fcinfo, 1);
    let mut intresult: int64 = 0;
    let r#type: c_int;
    let mut val: c_int = 0;
    let lowunits: *mut c_char;

    lowunits = downcase_truncate_identifier(
        VARDATA_ANY(units as *const c_char),
        VARSIZE_ANY_EXHDR(units as *const c_char) as c_int,
        false,
    );

    r#type = DecodeUnits(0, lowunits, &mut val);
    let r#type = if r#type == UNKNOWN_FIELD {
        DecodeSpecial(0, lowunits, &mut val)
    } else {
        r#type
    };

    if r#type == UNITS {
        let mut tz: c_int = 0;
        let mut fsec: fsec_t = 0;
        let mut tt: pg_tm = std::mem::zeroed();
        let tm: *mut pg_tm = &raw mut tt;

        timetz2tm(time, tm, &mut fsec, &mut tz);

        match val {
            x if x == DTK_TZ => {
                intresult = -tz as int64;
            }

            x if x == DTK_TZ_MINUTE => {
                intresult = ((-tz / SECS_PER_MINUTE) % MINS_PER_HOUR) as int64;
            }

            x if x == DTK_TZ_HOUR => {
                intresult = (-tz / SECS_PER_HOUR) as int64;
            }

            x if x == DTK_MICROSEC => {
                intresult = (*tm).tm_sec as int64 * 1000000 + fsec as int64;
            }

            x if x == DTK_MILLISEC => {
                if retnumeric {
                    /*---
                     * tm->tm_sec * 1000 + fsec / 1000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric(
                        (*tm).tm_sec as int64 * 1000000 + fsec as int64,
                        3
                    ));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 * 1000.0 + fsec as f64 / 1000.0);
                }
            }

            x if x == DTK_SECOND => {
                if retnumeric {
                    /*---
                     * tm->tm_sec + fsec / 1'000'000
                     * = (tm->tm_sec * 1'000'000 + fsec) / 1'000'000
                     */
                    PG_RETURN_NUMERIC!(int64_div_fast_to_numeric(
                        (*tm).tm_sec as int64 * 1000000 + fsec as int64,
                        6
                    ));
                } else {
                    PG_RETURN_FLOAT8!((*tm).tm_sec as f64 + fsec as f64 / 1000000.0);
                }
            }

            x if x == DTK_MINUTE => {
                intresult = (*tm).tm_min as int64;
            }

            x if x == DTK_HOUR => {
                intresult = (*tm).tm_hour as int64;
            }

            x if x == DTK_DAY
                || x == DTK_MONTH
                || x == DTK_QUARTER
                || x == DTK_YEAR
                || x == DTK_DECADE
                || x == DTK_CENTURY
                || x == DTK_MILLENNIUM =>
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(TIMETZOID)).to_string_lossy()
                    )
                );
                intresult = 0;
            }
            _ => {
                ereport!(
                    ERROR,
                    errmsg!(
                        "unit \"{}\" not supported for type {}",
                        std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(TIMETZOID)).to_string_lossy()
                    )
                );
                intresult = 0;
            }
        }
    } else if r#type == RESERV && val == DTK_EPOCH {
        if retnumeric {
            /*---
             * time->time / 1'000'000 + time->zone
             * = (time->time + time->zone * 1'000'000) / 1'000'000
             */
            PG_RETURN_NUMERIC!(int64_div_fast_to_numeric(
                (*time).time + (*time).zone as int64 * 1000000,
                6
            ));
        } else {
            PG_RETURN_FLOAT8!((*time).time as f64 / 1000000.0 + (*time).zone as f64);
        }
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "unit \"{}\" not recognized for type {}",
                std::ffi::CStr::from_ptr(lowunits).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(TIMETZOID)).to_string_lossy()
            )
        );
        intresult = 0;
    }

    if retnumeric {
        PG_RETURN_NUMERIC!(int64_to_numeric(intresult));
    } else {
        PG_RETURN_FLOAT8!(intresult as f64);
    }
}


pub unsafe fn timetz_part(fcinfo: FunctionCallInfo) -> Datum {
    return timetz_part_common(fcinfo, false);
}

pub unsafe fn extract_timetz(fcinfo: FunctionCallInfo) -> Datum {
    return timetz_part_common(fcinfo, true);
}

/* timetz_zone()
 * Encode time with time zone type with specified time zone.
 * Applies DST rules as of the transaction start time.
 */
pub unsafe fn timetz_zone(fcinfo: FunctionCallInfo) -> Datum {
    let zone = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let t = PG_GETARG_TIMETZADT_P!(fcinfo, 1);
    let result: *mut TimeTzADT;
    let tz: c_int;
    let mut tzname: [c_char; TZ_STRLEN_MAX + 1] = [0; TZ_STRLEN_MAX + 1];
    let r#type: c_int;
    let mut val: c_int = 0;
    let mut tzp: *mut pg_tz = null_mut();

    /*
     * Look up the requested timezone.
     */
    text_to_cstring_buffer(zone, tzname.as_mut_ptr(), std::mem::size_of_val(&tzname) as Size);

    r#type = DecodeTimezoneName(tzname.as_ptr(), &mut val, &mut tzp);

    if r#type == TZNAME_FIXED_OFFSET {
        /* fixed-offset abbreviation */
        tz = -val;
    } else if r#type == TZNAME_DYNTZ {
        /* dynamic-offset abbreviation, resolve using transaction start time */
        let now: TimestampTz = GetCurrentTransactionStartTimestamp();
        let mut isdst: c_int = 0;

        tz = DetermineTimeZoneAbbrevOffsetTS(now, tzname.as_ptr(), tzp, &mut isdst);
    } else {
        /* Get the offset-from-GMT that is valid now for the zone name */
        let now: TimestampTz = GetCurrentTransactionStartTimestamp();
        let mut tm: pg_tm = std::mem::zeroed();
        let mut fsec: fsec_t = 0;
        let mut tz_local: c_int = 0;

        if timestamp2tm(now, &mut tz_local, &mut tm, &mut fsec, null_mut(), tzp) != 0 {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }
        tz = tz_local;
    }

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;

    (*result).time = (*t).time + ((*t).zone - tz) as int64 * USECS_PER_SEC;
    /* C99 modulo has the wrong sign convention for negative input */
    while (*result).time < 0 {
        (*result).time += USECS_PER_DAY;
    }
    if (*result).time >= USECS_PER_DAY {
        (*result).time %= USECS_PER_DAY;
    }

    (*result).zone = tz;

    PG_RETURN_TIMETZADT_P!(result);
}

/* timetz_izone()
 * Encode time with time zone type with specified time interval as time zone.
 */
pub unsafe fn timetz_izone(fcinfo: FunctionCallInfo) -> Datum {
    let zone = PG_GETARG_INTERVAL_P!(fcinfo, 0);
    let time = PG_GETARG_TIMETZADT_P!(fcinfo, 1);
    let result: *mut TimeTzADT;
    let tz: c_int;

    if INTERVAL_NOT_FINITE(zone) {
        ereport!(
            ERROR,
            errmsg!(
                "interval time zone \"{}\" must be finite",
                std::ffi::CStr::from_ptr(DatumGetCString(DirectFunctionCall1!(
                    interval_out,
                    PointerGetDatum(zone as *const c_void)
                )))
                .to_string_lossy()
            )
        );
    }

    if (*zone).month != 0 || (*zone).day != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "interval time zone \"{}\" must not include months or days",
                std::ffi::CStr::from_ptr(DatumGetCString(DirectFunctionCall1!(
                    interval_out,
                    PointerGetDatum(zone as *const c_void)
                )))
                .to_string_lossy()
            )
        );
    }

    tz = (-((*zone).time / USECS_PER_SEC)) as c_int;

    result = palloc(std::mem::size_of::<TimeTzADT>() as Size) as *mut TimeTzADT;

    (*result).time = (*time).time + ((*time).zone - tz) as int64 * USECS_PER_SEC;
    /* C99 modulo has the wrong sign convention for negative input */
    while (*result).time < 0 {
        (*result).time += USECS_PER_DAY;
    }
    if (*result).time >= USECS_PER_DAY {
        (*result).time %= USECS_PER_DAY;
    }

    (*result).zone = tz;

    PG_RETURN_TIMETZADT_P!(result);
}

/* timetz_at_local()
 *
 * Unlike for timestamp[tz]_at_local, the type for timetz does not flip between
 * time with/without time zone, so we cannot just call the conversion function.
 */
pub unsafe fn timetz_at_local(fcinfo: FunctionCallInfo) -> Datum {
    let time = PG_GETARG_DATUM!(fcinfo, 0);
    let tzn = pg_get_timezone_name(session_timezone);
    let zone = PointerGetDatum(cstring_to_text(tzn) as *const c_void);

    return DirectFunctionCall2!(timetz_zone, zone, time);
}
