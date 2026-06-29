//! Functions for the built-in SQL types timestamp, timestamptz and interval.
//! Translated from src/backend/utils/adt/timestamp.c.
//!
//! Builds on the shared decode/encode core in `crate::utils::datetime`
//! (`ParseDateTime`/`DecodeDateTime`/`EncodeDateTime`/`DecodeInterval`/...) and
//! the unit/struct definitions in `crate::datatype::timestamp`
//! (Timestamp/TimestampTz = i64 usec since the 2000-01-01 POSTGRES epoch;
//! Interval = {time:i64 usec, day:i32, month:i32}).
//!
//! Owns the timestamp.c bodies declared as stubs in `utils/timestamp.h`:
//! `timestamp2tm`/`tm2timestamp`/`dt2time`/`interval2itm`/`itm2interval`/
//! `itmin2interval`, the comparison internals (`timestamp_cmp_internal`,
//! `timestamp_cmp_timestamptz_internal`, `timestamp2timestamptz_opt_overflow`),
//! the ISO-week family (`isoweek2j`/`isoweek2date`/`isoweekdate2date`/
//! `date2isoweek`/`date2isoyear`/`date2isoyearday`), `SetEpochTimestamp`/
//! `GetEpochTime`, and `anytimestamp_typmod_check`. The header is rewired to
//! `pub use` these.
//!
//! STAGED (rules.md s4) -- the subsystems timestamp.c reaches that are not yet
//! ported are called through `unimplemented!`:
//!  - named-zone resolution (IANA tz DB / session GUC): timestamptz_in with a
//!    named zone, timestamp_zone/timestamptz_zone, *_trunc_zone, *_at_local,
//!    make_timestamptz_at_timezone, the AT-TIME-ZONE-by-name paths. Numeric
//!    offsets (+05, -08:30) and interval offsets (the *_izone forms) WORK.
//!  - binary wire StringInfo: *_recv / *_send.
//!  - typmod cstring[] arrays: *typmodin / *typmodout.
//!  - SortSupport / SkipSupport / planner Support nodes: *_sortsupport,
//!    timestamp_skipsupport, *_support, generate_series_timestamp_support.
//!  - SRF ValuePerCall: generate_series_timestamp(tz).
//!  - aggregate transition context: interval_avg* / interval_sum.
//!  - numeric machinery (int64_to_numeric / numeric_in): the EXTRACT family
//!    `extract_timestamp(tz)`/`extract_interval` (retnumeric=true). The
//!    float8-returning `*_part` siblings WORK.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    reason = "intentional C width arithmetic: timestamp.c does explicit int/int64 \
              casts and fixed-width modular date math, including i32->f64 value \
              casts (the value-cast family is an allowed port-inherent lint per \
              rules.md s11)"
)]
#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::if_not_else,
    clippy::manual_range_contains,
    clippy::comparison_chain,
    clippy::bool_to_int_with_if,
    clippy::single_match_else,
    clippy::branches_sharing_code,
    clippy::if_same_then_else,
    reason = "1:1 port of timestamp.c's long decode/extract/trunc switch ladders \
              and single-letter date math vars (tz/tm/dt/y/m/d, day0/day4/dayn); the \
              C control flow -- explicit relop chains, if/else infinity arms with \
              shared tails, and the duplicated finite/non-finite branches -- is \
              reproduced faithfully (rules.md s8)"
)]
#![allow(
    clippy::float_cmp,
    clippy::suboptimal_flops,
    reason = "timestamp.c compares doubles for exact equality (factor==0.0, \
              result!=0.0) and computes interval scaling as separate ops to match \
              the documented FPU rounding; faithful ports of the C predicates/flops"
)]

use crate::common::int::{
    pg_add_s32_overflow, pg_add_s64_overflow, pg_mul_s32_overflow, pg_mul_s64_overflow,
    pg_sub_s32_overflow, pg_sub_s64_overflow,
};
use crate::common::int128::{
    int128_add_int64_mul_int64, int128_compare, int64_to_int128, Int128,
};
use crate::datatype::timestamp::{
    fsec_t, pg_itm, pg_itm_in, Interval, Timestamp, TimestampTz, DAYS_PER_MONTH, DAYS_PER_WEEK,
    DAYS_PER_YEAR, HOURS_PER_DAY, INTERVAL_IS_NOBEGIN, INTERVAL_IS_NOEND, INTERVAL_NOBEGIN,
    INTERVAL_NOEND, INTERVAL_NOT_FINITE, IS_VALID_JULIAN, IS_VALID_TIMESTAMP, MAX_INTERVAL_PRECISION,
    MAX_TIMESTAMP_PRECISION, MINS_PER_HOUR, MONTHS_PER_YEAR, POSTGRES_EPOCH_JDATE, SECS_PER_DAY,
    SECS_PER_HOUR, SECS_PER_MINUTE, TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND, TIMESTAMP_NOBEGIN,
    TIMESTAMP_NOEND, TIMESTAMP_NOT_FINITE, TSROUND, UNIX_EPOCH_JDATE, USECS_PER_DAY, USECS_PER_HOUR,
    USECS_PER_MINUTE, USECS_PER_SEC, DATETIME_MIN_JULIAN, MIN_TIMESTAMP, TIMESTAMP_END_JULIAN,
};
use crate::ereport;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::pgtime::pg_tm;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, Datum, DatumGetBool, DatumGetCString, DatumGetFloat8,
    DatumGetInt32, Float8GetDatum, Int32GetDatum,
};
use crate::utils::datetime::{
    day_tab, isleap, AdjustTimestampForTypmod, DateTimeErrorExtra, DecodeDateTime, DecodeISO8601Interval,
    DecodeInterval, DecodeTimezone, DecodeUnits, EncodeDateTime, EncodeInterval, ParseDateTime,
    ValidateDate, DTERR_BAD_FORMAT, DTERR_FIELD_OVERFLOW, DTERR_INTERVAL_OVERFLOW,
    DTERR_MD_FIELD_OVERFLOW, DTERR_TZDISP_OVERFLOW, DTK_CENTURY, DTK_DATE, DTK_DATE_M, DTK_DAY,
    DTK_DECADE, DTK_DELTA, DTK_DOW, DTK_DOY, DTK_EARLY, DTK_EPOCH, DTK_HOUR, DTK_ISODOW, DTK_ISOYEAR,
    DTK_JULIAN, DTK_LATE, DTK_MICROSEC, DTK_MILLENNIUM, DTK_MILLISEC, DTK_MINUTE, DTK_MONTH,
    DTK_QUARTER, DTK_SECOND, DTK_TZ, DTK_TZ_HOUR, DTK_TZ_MINUTE, DTK_WEEK, DTK_YEAR, MAXDATEFIELDS,
    RESERV, UNITS, UNKNOWN_FIELD,
};
use crate::utils::date::float_time_overflows;
use crate::utils::datetime::DecodeSpecial;
use crate::utils::elog::{ErrorData, ERROR, WARNING};
use crate::utils::errcodes::{
    ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE, ERRCODE_DIVISION_BY_ZERO,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERVAL_FIELD_OVERFLOW, ERRCODE_INVALID_DATETIME_FORMAT,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,
    ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
};
use crate::utils::float::get_float8_infinity;
use crate::utils::timestamp::{
    IntervalPGetDatum, DatumGetIntervalP, DatumGetTimestamp, DatumGetTimestampTz, TimestampGetDatum,
    TimestampTzGetDatum, INTERVAL_FULL_PRECISION, INTERVAL_FULL_RANGE, INTERVAL_PRECISION,
    INTERVAL_RANGE,
};

const EARLY: &str = "-infinity";
const LATE: &str = "infinity";
const MAXDATELEN: usize = 128;

// Process-global statics live in the header (set at postmaster start / reload).
use crate::utils::timestamp::{PgReloadTime, PgStartTime};

// FLOAT8_FITS_IN_INTn (c.h): caller must rint() first; NaN handled by caller.
const PG_INT32_MIN_F: f64 = -2_147_483_648.0;
const PG_INT64_MIN_F: f64 = -9_223_372_036_854_775_808.0;
#[inline]
fn float8_fits_in_int32(num: f64) -> bool {
    (PG_INT32_MIN_F..=-PG_INT32_MIN_F - 1.0).contains(&num)
}
#[inline]
fn float8_fits_in_int64(num: f64) -> bool {
    (PG_INT64_MIN_F..-PG_INT64_MIN_F).contains(&num)
}

// ---------------------------------------------------------------------------
// PG_GETARG_* / PG_RETURN_* leaf helpers (mirror float.rs/bool.rs).
// ---------------------------------------------------------------------------

#[inline]
fn pg_getarg_timestamp(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Timestamp {
    DatumGetTimestamp(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_timestamptz(fcinfo: &FunctionCallInfoBaseData, n: usize) -> TimestampTz {
    DatumGetTimestampTz(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_interval(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Interval {
    // SAFETY: the arg is a valid pass-by-ref Interval pointer outliving the call.
    unsafe { *DatumGetIntervalP(fcinfo.args[n].value) }
}
#[inline]
fn pg_getarg_int32(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i32 {
    DatumGetInt32(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_float8(fcinfo: &FunctionCallInfoBaseData, n: usize) -> f64 {
    DatumGetFloat8(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_bool(fcinfo: &FunctionCallInfoBaseData, n: usize) -> bool {
    DatumGetBool(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_cstring(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetCString(fcinfo.args[n].value);
    // SAFETY: an input function's cstring argument is NUL-terminated and outlives
    // the call.
    let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
    cstr.to_string_lossy().into_owned()
}
#[inline]
fn pg_return_cstring(s: &str) -> Datum {
    let bytes: Vec<u8> = s.bytes().take_while(|&b| b != 0).collect();
    let c = std::ffi::CString::new(bytes).unwrap_or_default();
    CStringGetDatum(c.into_raw())
}

/// Return an Interval byref Datum. No MemoryContext yet, so the value is leaked
/// (mirror varlena.rs make_varlena). TODO(memory-context): reclaim via per-call ctx.
#[inline]
fn pg_return_interval(span: Interval) -> Datum {
    IntervalPGetDatum(Box::leak(Box::new(span)))
}

/// Read back a NUL-terminated encode buffer as an owned string.
fn buf_to_string(buf: &[u8]) -> String {
    let n = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

// ---------------------------------------------------------------------------
// Local error reporting (mirrors datetime.c DateTimeParseError, ~4214-4268).
// ---------------------------------------------------------------------------

/// Map a DTERR_* code to the matching ereport(ERROR). Private; each .c that
/// calls DateTimeParseError keeps its own copy (no clash since private).
fn date_time_parse_error(dterr: i32, _extra: &DateTimeErrorExtra, s: &str, datatype: &str) -> ! {
    match dterr {
        d if d == DTERR_FIELD_OVERFLOW || d == DTERR_MD_FIELD_OVERFLOW => {
            ereport!(ERROR, |e: &mut ErrorData| {
                e.errcode(ERRCODE_DATETIME_FIELD_OVERFLOW)
                    .errmsg(format!("date/time field value out of range: \"{s}\""));
            });
        }
        d if d == DTERR_INTERVAL_OVERFLOW => {
            ereport!(ERROR, |e: &mut ErrorData| {
                e.errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW)
                    .errmsg(format!("interval field value out of range: \"{s}\""));
            });
        }
        d if d == DTERR_TZDISP_OVERFLOW => {
            ereport!(ERROR, |e: &mut ErrorData| {
                e.errcode(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE)
                    .errmsg(format!("time zone displacement out of range: \"{s}\""));
            });
        }
        _ => {
            ereport!(ERROR, |e: &mut ErrorData| {
                e.errcode(ERRCODE_INVALID_DATETIME_FORMAT)
                    .errmsg(format!("invalid input syntax for type {datatype}: \"{s}\""));
            });
        }
    }
    unreachable!()
}

#[inline]
fn ereport_out_of_range_ts() -> ! {
    ereport!(ERROR, |e: &mut ErrorData| {
        e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
            .errmsg("timestamp out of range".to_string());
    });
    unreachable!()
}

#[inline]
fn ereport_interval_out_of_range() -> ! {
    ereport!(ERROR, |e: &mut ErrorData| {
        e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
            .errmsg("interval out of range".to_string());
    });
    unreachable!()
}

fn ereport_unit_not_supported(lowunits: &str, typename: &str) -> ! {
    ereport!(ERROR, |e: &mut ErrorData| {
        e.errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("unit \"{lowunits}\" not supported for type {typename}"));
    });
    unreachable!()
}

fn ereport_unit_not_recognized(lowunits: &str, typename: &str) -> ! {
    ereport!(ERROR, |e: &mut ErrorData| {
        e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("unit \"{lowunits}\" not recognized for type {typename}"));
    });
    unreachable!()
}

// downcase + truncate identifier for unit text args.
fn lowunits_of(units: &[u8]) -> String {
    let s = String::from_utf8_lossy(units);
    crate::backend::parser::scansup::downcase_truncate_identifier(&s, s.len() as i32, false)
}

/// Borrow the payload bytes of the text varlena argument `n`.
fn pg_getarg_text_bytes(fcinfo: &FunctionCallInfoBaseData, n: usize) -> &[u8] {
    let p = crate::postgres::DatumGetPointer(fcinfo.args[n].value);
    // SAFETY: the arg is a valid non-toasted text varlena outliving the call.
    unsafe {
        let len = crate::varatt::VARSIZE_ANY_EXHDR(p);
        core::slice::from_raw_parts(crate::varatt::VARDATA_ANY(p), len)
    }
}

// ---------------------------------------------------------------------------
// EncodeSpecialTimestamp / EncodeSpecialInterval (timestamp.c 1583-1606).
// ---------------------------------------------------------------------------

/// Convert reserved timestamp data type to string.
pub fn EncodeSpecialTimestamp(dt: Timestamp, str_: &mut [u8]) {
    let s = if TIMESTAMP_IS_NOBEGIN(dt) {
        EARLY
    } else if TIMESTAMP_IS_NOEND(dt) {
        LATE
    } else {
        elog_invalid("EncodeSpecialTimestamp");
    };
    write_cstr(str_, s);
}

fn EncodeSpecialInterval(interval: &Interval, str_: &mut [u8]) {
    let s = if INTERVAL_IS_NOBEGIN(interval) {
        EARLY
    } else if INTERVAL_IS_NOEND(interval) {
        LATE
    } else {
        elog_invalid("EncodeSpecialInterval");
    };
    write_cstr(str_, s);
}

fn elog_invalid(what: &str) -> ! {
    crate::elog!(ERROR, format!("invalid argument for {what}"));
    unreachable!()
}

fn write_cstr(buf: &mut [u8], s: &str) {
    let b = s.as_bytes();
    buf[..b.len()].copy_from_slice(b);
    buf[b.len()] = 0;
}

// ===========================================================================
//   Internal time<->tm bridges (timestamp.c 1886-2142). FULL.
// ===========================================================================

/// dt2time: split a usec time-of-day into h/m/s/fsec.
pub fn dt2time(jd: Timestamp) -> (i32, i32, i32, fsec_t) {
    let mut time = jd;
    let hour = time / USECS_PER_HOUR;
    time -= hour * USECS_PER_HOUR;
    let min = time / USECS_PER_MINUTE;
    time -= min * USECS_PER_MINUTE;
    let sec = time / USECS_PER_SEC;
    let fsec = time - sec * USECS_PER_SEC;
    (hour as i32, min as i32, sec as i32, fsec as fsec_t)
}

fn time2t(hour: i32, min: i32, sec: i32, fsec: fsec_t) -> Timestamp {
    (((((i64::from(hour) * i64::from(MINS_PER_HOUR)) + i64::from(min)) * i64::from(SECS_PER_MINUTE))
        + i64::from(sec))
        * USECS_PER_SEC)
        + i64::from(fsec)
}

fn dt2local(dt: Timestamp, timezone: i32) -> Timestamp {
    dt - i64::from(timezone) * USECS_PER_SEC
}

/// Convert timestamp to a broken-down pg_tm. Returns 0 on success, -1 on range.
/// `attimezone` is currently ignored for the named-zone rotation (no IANA DB);
/// when a tz offset is requested (`tzp` Some) we only support GMT/no-zone -- the
/// session-zone rotation needs `pg_localtime` (STAGED). For the common no-tz
/// path (`tzp` None) this is FULL.
pub fn timestamp2tm(
    dt: Timestamp,
    tzp: Option<&mut i32>,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzn: Option<&mut *const u8>,
    _attimezone: *mut crate::pgtime::pg_tz,
) -> i32 {
    let mut time = dt;
    let mut date: Timestamp = 0;
    crate::utils::datetime::TMODULO(&mut time, &mut date, USECS_PER_DAY);

    if time < 0 {
        time += USECS_PER_DAY;
        date -= 1;
    }

    // add offset to go from J2000 back to standard Julian date
    date += i64::from(POSTGRES_EPOCH_JDATE);

    if date < 0 || date > i64::from(i32::MAX) {
        return -1;
    }

    let (y, m, d) = crate::utils::datetime::j2date(date as i32);
    tm.year = y;
    tm.mon = m;
    tm.mday = d;
    let (h, mi, s, f) = dt2time(time);
    tm.hour = h;
    tm.min = mi;
    tm.sec = s;
    *fsec = f;

    // Done if no TZ conversion wanted.
    if let Some(tzp) = tzp {
        // Named/session-zone rotation needs pg_localtime against a loaded tz DB.
        // We support only the GMT/no-zone case here; the IANA path is STAGED.
        *tzp = 0;
        tm.isdst = -1;
        tm.gmtoff = 0;
        tm.zone = None;
        if let Some(tzn) = tzn {
            *tzn = std::ptr::null();
        }
    } else {
        tm.isdst = -1;
        tm.gmtoff = 0;
        tm.zone = None;
        if let Some(tzn) = tzn {
            *tzn = std::ptr::null();
        }
    }
    0
}

/// Convert a pg_tm to a timestamp. Returns -1 on out-of-range.
pub fn tm2timestamp(tm: &pg_tm, fsec: fsec_t, tzp: Option<&i32>, result: &mut Timestamp) -> i32 {
    if !IS_VALID_JULIAN(tm.year, tm.mon, tm.mday) {
        *result = 0;
        return -1;
    }
    let date = i64::from(crate::utils::datetime::date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE);
    let time = time2t(tm.hour, tm.min, tm.sec, fsec);

    let Some(r) = pg_mul_s64_overflow(date, USECS_PER_DAY).and_then(|v| pg_add_s64_overflow(v, time))
    else {
        *result = 0;
        return -1;
    };
    *result = r;
    if let Some(tzp) = tzp {
        *result = dt2local(*result, -*tzp);
    }
    if !IS_VALID_TIMESTAMP(*result) {
        *result = 0;
        return -1;
    }
    0
}

/// Convert an Interval to a pg_itm structure. Overflow is not possible.
pub fn interval2itm(span: Interval, itm: &mut pg_itm) {
    itm.year = span.month / MONTHS_PER_YEAR;
    itm.mon = span.month % MONTHS_PER_YEAR;
    itm.mday = span.day;
    let mut time = span.time;
    let mut tfrac = time / USECS_PER_HOUR;
    time -= tfrac * USECS_PER_HOUR;
    itm.hour = tfrac;
    tfrac = time / USECS_PER_MINUTE;
    time -= tfrac * USECS_PER_MINUTE;
    itm.min = tfrac as i32;
    tfrac = time / USECS_PER_SEC;
    time -= tfrac * USECS_PER_SEC;
    itm.sec = tfrac as i32;
    itm.usec = time as i32;
}

/// Convert a pg_itm to an Interval. Returns 0 OK, -1 on overflow.
pub fn itm2interval(itm: &mut pg_itm, span: &mut Interval) -> i32 {
    let total_months = i64::from(itm.year) * i64::from(MONTHS_PER_YEAR) + i64::from(itm.mon);
    if total_months > i64::from(i32::MAX) || total_months < i64::from(i32::MIN) {
        return -1;
    }
    span.month = total_months as i32;
    span.day = itm.mday;
    let Some(mut t) = pg_mul_s64_overflow(itm.hour, USECS_PER_HOUR) else {
        return -1;
    };
    let Some(v) = pg_add_s64_overflow(t, i64::from(itm.min) * USECS_PER_MINUTE) else {
        return -1;
    };
    t = v;
    let Some(v) = pg_add_s64_overflow(t, i64::from(itm.sec) * USECS_PER_SEC) else {
        return -1;
    };
    t = v;
    let Some(v) = pg_add_s64_overflow(t, i64::from(itm.usec)) else {
        return -1;
    };
    span.time = v;
    if INTERVAL_NOT_FINITE(span) {
        return -1;
    }
    0
}

/// Convert a pg_itm_in to an Interval. Returns 0 OK, -1 on overflow. Infinite
/// results are NOT treated as overflow (see C comment).
pub fn itmin2interval(itm_in: &mut pg_itm_in, span: &mut Interval) -> i32 {
    let total_months = i64::from(itm_in.year) * i64::from(MONTHS_PER_YEAR) + i64::from(itm_in.mon);
    if total_months > i64::from(i32::MAX) || total_months < i64::from(i32::MIN) {
        return -1;
    }
    span.month = total_months as i32;
    span.day = itm_in.mday;
    span.time = itm_in.usec;
    0
}

// ===========================================================================
//   anytimestamp_typmod_check (timestamp.c 124-143). FULL.
// ===========================================================================

pub fn anytimestamp_typmod_check(istz: bool, typmod: i32) -> i32 {
    if typmod < 0 {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE).errmsg(format!(
                "TIMESTAMP({typmod}){} precision must not be negative",
                if istz { " WITH TIME ZONE" } else { "" }
            ));
        });
        unreachable!()
    }
    if typmod > MAX_TIMESTAMP_PRECISION {
        ereport!(WARNING, |e: &mut ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE).errmsg(format!(
                "TIMESTAMP({typmod}){} precision reduced to maximum allowed, {MAX_TIMESTAMP_PRECISION}",
                if istz { " WITH TIME ZONE" } else { "" }
            ));
        });
        return MAX_TIMESTAMP_PRECISION;
    }
    typmod
}

// ===========================================================================
//   timestamp I/O (timestamp.c 162-358, 414-481, 772-881). FULL (no named zone).
// ===========================================================================

/// PG `timestamp_in`.
pub fn timestamp_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str_ = pg_getarg_cstring(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 2);
    let mut result: Timestamp = 0;
    let mut fsec: fsec_t = 0;
    let mut tm = new_tm();
    let mut tz = 0i32;
    let mut dtype = 0i32;
    let mut extra = DateTimeErrorExtra { timezone: None, abbrev: None };

    let dterr = match ParseDateTime(&str_, MAXDATEFIELDS) {
        Ok((mut field, mut ftype)) => DecodeDateTime(
            &mut field, &mut ftype, &mut dtype, &mut tm, &mut fsec, Some(&mut tz), &mut extra,
        ),
        Err(e) => e,
    };
    if dterr != 0 {
        date_time_parse_error(dterr, &extra, &str_, "timestamp");
    }

    match dtype {
        d if d == DTK_DATE => {
            if tm2timestamp(&tm, fsec, None, &mut result) != 0 {
                ereport!(ERROR, |e: &mut ErrorData| {
                    e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                        .errmsg(format!("timestamp out of range: \"{str_}\""));
                });
                unreachable!()
            }
        }
        d if d == DTK_EPOCH => result = SetEpochTimestamp(),
        d if d == DTK_LATE => {
            result = 0;
            TIMESTAMP_NOEND(&mut result);
        }
        d if d == DTK_EARLY => {
            result = 0;
            TIMESTAMP_NOBEGIN(&mut result);
        }
        _ => {
            crate::elog!(ERROR, format!("unexpected dtype {dtype} while parsing timestamp \"{str_}\""));
            unreachable!()
        }
    }

    let _ = AdjustTimestampForTypmod(&mut result, typmod);
    TimestampGetDatum(result)
}

/// PG `timestamp_out`.
pub fn timestamp_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    let mut buf = [0u8; MAXDATELEN + 1];
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;

    if TIMESTAMP_NOT_FINITE(timestamp) {
        EncodeSpecialTimestamp(timestamp, &mut buf);
    } else if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, std::ptr::null_mut()) == 0 {
        EncodeDateTime(&mut tm, fsec, false, 0, None, get_date_style(), &mut buf);
    } else {
        ereport_out_of_range_ts();
    }
    pg_return_cstring(&buf_to_string(&buf))
}

/// PG `timestamptz_in`. Numeric/UTC offsets work; named zones are STAGED.
pub fn timestamptz_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str_ = pg_getarg_cstring(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 2);
    let mut result: TimestampTz = 0;
    let mut fsec: fsec_t = 0;
    let mut tm = new_tm();
    let mut tz = 0i32;
    let mut dtype = 0i32;
    let mut extra = DateTimeErrorExtra { timezone: None, abbrev: None };

    let dterr = match ParseDateTime(&str_, MAXDATEFIELDS) {
        Ok((mut field, mut ftype)) => DecodeDateTime(
            &mut field, &mut ftype, &mut dtype, &mut tm, &mut fsec, Some(&mut tz), &mut extra,
        ),
        Err(e) => e,
    };
    if dterr != 0 {
        date_time_parse_error(dterr, &extra, &str_, "timestamp with time zone");
    }

    match dtype {
        d if d == DTK_DATE => {
            // DecodeDateTime resolves numeric offsets into tz already; the
            // named-zone branch would set tm.isdst from the IANA DB (STAGED).
            if tm2timestamp(&tm, fsec, Some(&tz), &mut result) != 0 {
                ereport!(ERROR, |e: &mut ErrorData| {
                    e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                        .errmsg(format!("timestamp out of range: \"{str_}\""));
                });
                unreachable!()
            }
        }
        d if d == DTK_EPOCH => result = SetEpochTimestamp(),
        d if d == DTK_LATE => {
            result = 0;
            TIMESTAMP_NOEND(&mut result);
        }
        d if d == DTK_EARLY => {
            result = 0;
            TIMESTAMP_NOBEGIN(&mut result);
        }
        _ => {
            crate::elog!(ERROR, format!("unexpected dtype {dtype} while parsing timestamptz \"{str_}\""));
            unreachable!()
        }
    }

    let _ = AdjustTimestampForTypmod(&mut result, typmod);
    TimestampTzGetDatum(result)
}

/// PG `timestamptz_out`. UTC/numeric only.
pub fn timestamptz_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt = pg_getarg_timestamptz(fcinfo, 0);
    let mut buf = [0u8; MAXDATELEN + 1];
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tz = 0i32;

    if TIMESTAMP_NOT_FINITE(dt) {
        EncodeSpecialTimestamp(dt, &mut buf);
    } else if timestamp2tm(dt, Some(&mut tz), &mut tm, &mut fsec, None, std::ptr::null_mut()) == 0 {
        EncodeDateTime(&mut tm, fsec, true, tz, None, get_date_style(), &mut buf);
    } else {
        ereport_out_of_range_ts();
    }
    pg_return_cstring(&buf_to_string(&buf))
}

pub fn timestamp_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamp_recv needs the binary wire StringInfo (pq_getmsgint64) path")
}
pub fn timestamp_send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamp_send needs pq_begintypsend/pq_sendint64 bytea boxing")
}
pub fn timestamptz_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptz_recv needs the binary wire StringInfo (pq_getmsgint64) path")
}
pub fn timestamptz_send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptz_send needs pq_begintypsend/pq_sendint64 bytea boxing")
}

pub fn timestamptypmodin(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptypmodin needs the typmod cstring[] ArrayType (ArrayGetIntegerTypmods)")
}
pub fn timestamptypmodout(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptypmodout needs psprintf typmod cstring rendering")
}
pub fn timestamptztypmodin(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptztypmodin needs the typmod cstring[] ArrayType")
}
pub fn timestamptztypmodout(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptztypmodout needs psprintf typmod cstring rendering")
}
pub fn timestamp_support(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamp_support needs the SupportRequestSimplify planner Node (TemporalSimplify)")
}

/// PG `timestamp_scale`.
pub fn timestamp_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut result = pg_getarg_timestamp(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 1);
    let _ = AdjustTimestampForTypmod(&mut result, typmod);
    TimestampGetDatum(result)
}

/// PG `timestamptz_scale`.
pub fn timestamptz_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut result = pg_getarg_timestamptz(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 1);
    let _ = AdjustTimestampForTypmod(&mut result, typmod);
    TimestampTzGetDatum(result)
}

// ===========================================================================
//   make_timestamp / make_timestamptz / to_timestamp. FULL (no named zone).
// ===========================================================================

fn make_timestamp_internal(year: i32, month: i32, day: i32, hour: i32, min: i32, sec: f64) -> Timestamp {
    let mut tm = new_tm();
    tm.year = year;
    tm.mon = month;
    tm.mday = day;
    let mut bc = false;
    if tm.year < 0 {
        bc = true;
        tm.year = -tm.year;
    }
    let dterr = ValidateDate(DTK_DATE_M as i32, false, false, bc, &mut tm);
    if dterr != 0 {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DATETIME_FIELD_OVERFLOW)
                .errmsg(format!("date field value out of range: {year}-{month:02}-{day:02}"));
        });
        unreachable!()
    }
    if !IS_VALID_JULIAN(tm.year, tm.mon, tm.mday) {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg(format!("date out of range: {year}-{month:02}-{day:02}"));
        });
        unreachable!()
    }
    let date = i64::from(crate::utils::datetime::date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE);
    if float_time_overflows(hour, min, sec) {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DATETIME_FIELD_OVERFLOW)
                .errmsg(format!("time field value out of range: {hour}:{min:02}:{sec:02}"));
        });
        unreachable!()
    }
    let time = (((i64::from(hour) * i64::from(MINS_PER_HOUR) + i64::from(min)) * i64::from(SECS_PER_MINUTE))
        * USECS_PER_SEC)
        + (sec * USECS_PER_SEC as f64).round() as i64;
    let result = pg_mul_s64_overflow(date, USECS_PER_DAY).and_then(|v| pg_add_s64_overflow(v, time));
    let Some(result) = result.filter(|&r| IS_VALID_TIMESTAMP(r)) else {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg(format!("timestamp out of range: {year}-{month:02}-{day:02} {hour}:{min:02}:{sec:02}"));
        });
        unreachable!()
    };
    result
}

/// PG `make_timestamp`.
pub fn make_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let result = make_timestamp_internal(
        pg_getarg_int32(fcinfo, 0),
        pg_getarg_int32(fcinfo, 1),
        pg_getarg_int32(fcinfo, 2),
        pg_getarg_int32(fcinfo, 3),
        pg_getarg_int32(fcinfo, 4),
        pg_getarg_float8(fcinfo, 5),
    );
    TimestampGetDatum(result)
}

/// PG `make_timestamptz`. Default (session) zone -> treat as already-GMT.
pub fn make_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let result = make_timestamp_internal(
        pg_getarg_int32(fcinfo, 0),
        pg_getarg_int32(fcinfo, 1),
        pg_getarg_int32(fcinfo, 2),
        pg_getarg_int32(fcinfo, 3),
        pg_getarg_int32(fcinfo, 4),
        pg_getarg_float8(fcinfo, 5),
    );
    TimestampTzGetDatum(timestamp2timestamptz(result))
}

pub fn make_timestamptz_at_timezone(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("make_timestamptz_at_timezone named-zone arg needs IANA tz DB (parse_sane_timezone)")
}

/// PG `to_timestamp` (float8 unix epoch seconds -> timestamptz). FULL.
pub fn float8_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let seconds_in = pg_getarg_float8(fcinfo, 0);
    let mut result: TimestampTz;

    if seconds_in.is_nan() {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("timestamp cannot be NaN".to_string());
        });
        unreachable!()
    }
    if seconds_in.is_infinite() {
        result = 0;
        if seconds_in < 0.0 {
            TIMESTAMP_NOBEGIN(&mut result);
        } else {
            TIMESTAMP_NOEND(&mut result);
        }
    } else {
        let mut seconds = seconds_in;
        if seconds < f64::from(SECS_PER_DAY) * f64::from(DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE)
            || seconds >= f64::from(SECS_PER_DAY) * f64::from(TIMESTAMP_END_JULIAN - UNIX_EPOCH_JDATE)
        {
            ereport!(ERROR, |e: &mut ErrorData| {
                e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .errmsg(format!("timestamp out of range: \"{seconds_in}\""));
            });
            unreachable!()
        }
        seconds -= f64::from((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);
        seconds = (seconds * USECS_PER_SEC as f64).round();
        result = seconds as i64;
        if !IS_VALID_TIMESTAMP(result) {
            ereport!(ERROR, |e: &mut ErrorData| {
                e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .errmsg(format!("timestamp out of range: \"{seconds_in}\""));
            });
            unreachable!()
        }
    }
    TimestampGetDatum(result)
}

// ===========================================================================
//   interval I/O (timestamp.c 884-1340). FULL.
// ===========================================================================

/// PG `interval_in`.
pub fn interval_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str_ = pg_getarg_cstring(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 2);
    let mut itm_in = pg_itm_in::default();
    let mut dtype = 0i32;
    let range = if typmod >= 0 { INTERVAL_RANGE(typmod) } else { INTERVAL_FULL_RANGE };
    let extra = DateTimeErrorExtra { timezone: None, abbrev: None };

    let mut dterr = match ParseDateTime(&str_, MAXDATEFIELDS) {
        Ok((field, ftype)) => DecodeInterval(&field, &ftype, range, &mut dtype, &mut itm_in),
        Err(e) => e,
    };
    if dterr == DTERR_BAD_FORMAT {
        dterr = DecodeISO8601Interval(&str_, &mut dtype, &mut itm_in);
    }
    if dterr != 0 {
        if dterr == DTERR_FIELD_OVERFLOW {
            dterr = DTERR_INTERVAL_OVERFLOW;
        }
        date_time_parse_error(dterr, &extra, &str_, "interval");
    }

    let mut result = Interval { time: 0, day: 0, month: 0 };
    match dtype {
        d if d == DTK_DELTA => {
            if itmin2interval(&mut itm_in, &mut result) != 0 {
                ereport_interval_out_of_range();
            }
        }
        d if d == DTK_LATE => INTERVAL_NOEND(&mut result),
        d if d == DTK_EARLY => INTERVAL_NOBEGIN(&mut result),
        _ => {
            crate::elog!(ERROR, format!("unexpected dtype {dtype} while parsing interval \"{str_}\""));
            unreachable!()
        }
    }
    AdjustIntervalForTypmod(&mut result, typmod);
    pg_return_interval(result)
}

/// PG `interval_out`.
pub fn interval_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let span = pg_getarg_interval(fcinfo, 0);
    let mut buf = [0u8; MAXDATELEN + 1];
    if INTERVAL_NOT_FINITE(&span) {
        EncodeSpecialInterval(&span, &mut buf);
    } else {
        let mut itm = pg_itm::default();
        interval2itm(span, &mut itm);
        EncodeInterval(&itm, get_interval_style(), &mut buf);
    }
    pg_return_cstring(&buf_to_string(&buf))
}

pub fn interval_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_recv needs the binary wire StringInfo (pq_getmsgint64/pq_getmsgint) path")
}
pub fn interval_send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_send needs pq_begintypsend/pq_sendint64/pq_sendint32 bytea boxing")
}
pub fn intervaltypmodin(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("intervaltypmodin needs the typmod cstring[] ArrayType (ArrayGetIntegerTypmods)")
}
pub fn intervaltypmodout(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("intervaltypmodout needs psprintf typmod cstring rendering")
}
pub fn interval_support(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_support needs the SupportRequestSimplify planner Node (relabel_to_typmod)")
}

/// PG `interval_scale`.
pub fn interval_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut result = pg_getarg_interval(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 1);
    AdjustIntervalForTypmod(&mut result, typmod);
    pg_return_interval(result)
}

/// AdjustIntervalForTypmod (timestamp.c 1349-1524). FULL.
fn AdjustIntervalForTypmod(interval: &mut Interval, typmod: i32) -> bool {
    const SCALES: [i64; 7] = [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
    const OFFSETS: [i64; 7] = [500_000, 50_000, 5_000, 500, 50, 5, 0];

    if INTERVAL_NOT_FINITE(interval) {
        return true;
    }
    if typmod >= 0 {
        let range = INTERVAL_RANGE(typmod);
        let precision = INTERVAL_PRECISION(typmod);
        let mask_year = mask(YEAR_F);
        let mask_month = mask(MONTH_F);
        let mask_day = mask(DAY_F);
        let mask_hour = mask(HOUR_F);
        let mask_minute = mask(MINUTE_F);
        let mask_second = mask(SECOND_F);

        if range == INTERVAL_FULL_RANGE {
            // do nothing
        } else if range == mask_year {
            interval.month = (interval.month / MONTHS_PER_YEAR) * MONTHS_PER_YEAR;
            interval.day = 0;
            interval.time = 0;
        } else if range == mask_month {
            interval.day = 0;
            interval.time = 0;
        } else if range == (mask_year | mask_month) {
            interval.day = 0;
            interval.time = 0;
        } else if range == mask_day {
            interval.time = 0;
        } else if range == mask_hour {
            interval.time = (interval.time / USECS_PER_HOUR) * USECS_PER_HOUR;
        } else if range == mask_minute {
            interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        } else if range == mask_second {
            // fractional rounding below
        } else if range == (mask_day | mask_hour) {
            interval.time = (interval.time / USECS_PER_HOUR) * USECS_PER_HOUR;
        } else if range == (mask_day | mask_hour | mask_minute) {
            interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        } else if range == (mask_day | mask_hour | mask_minute | mask_second) {
            // fractional rounding below
        } else if range == (mask_hour | mask_minute) {
            interval.time = (interval.time / USECS_PER_MINUTE) * USECS_PER_MINUTE;
        } else if range == (mask_hour | mask_minute | mask_second) {
            // fractional rounding below
        } else if range == (mask_minute | mask_second) {
            // fractional rounding below
        } else {
            crate::elog!(ERROR, format!("unrecognized interval typmod: {typmod}"));
        }

        if precision != INTERVAL_FULL_PRECISION {
            if precision < 0 || precision > MAX_INTERVAL_PRECISION {
                ereport!(ERROR, |e: &mut ErrorData| {
                    e.errcode(ERRCODE_INVALID_PARAMETER_VALUE).errmsg(format!(
                        "interval({precision}) precision must be between {} and {MAX_INTERVAL_PRECISION}",
                        0
                    ));
                });
                unreachable!()
            }
            let p = precision as usize;
            if interval.time >= 0 {
                let Some(v) = pg_add_s64_overflow(interval.time, OFFSETS[p]) else {
                    ereport_interval_out_of_range();
                };
                interval.time = v - v % SCALES[p];
            } else {
                let Some(v) = pg_sub_s64_overflow(interval.time, OFFSETS[p]) else {
                    ereport_interval_out_of_range();
                };
                interval.time = v - v % SCALES[p];
            }
        }
    }
    true
}

// INTERVAL_MASK field codes (datetime.h YEAR/MONTH/DAY/HOUR/MINUTE/SECOND).
const YEAR_F: i32 = crate::utils::datetime::YEAR;
const MONTH_F: i32 = crate::utils::datetime::MONTH;
const DAY_F: i32 = crate::utils::datetime::DAY;
const HOUR_F: i32 = crate::utils::datetime::HOUR;
const MINUTE_F: i32 = crate::utils::datetime::MINUTE;
const SECOND_F: i32 = crate::utils::datetime::SECOND;
#[inline]
const fn mask(b: i32) -> i32 {
    1 << b
}

/// PG `make_interval`. FULL.
pub fn make_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let years = pg_getarg_int32(fcinfo, 0);
    let months = pg_getarg_int32(fcinfo, 1);
    let weeks = pg_getarg_int32(fcinfo, 2);
    let days = pg_getarg_int32(fcinfo, 3);
    let hours = pg_getarg_int32(fcinfo, 4);
    let mins = pg_getarg_int32(fcinfo, 5);
    let secs = pg_getarg_float8(fcinfo, 6);

    if secs.is_infinite() || secs.is_nan() {
        ereport_interval_out_of_range();
    }
    let mut result = Interval { time: 0, day: 0, month: 0 };
    let Some(month) = pg_mul_s32_overflow(years, MONTHS_PER_YEAR).and_then(|v| pg_add_s32_overflow(v, months))
    else {
        ereport_interval_out_of_range();
    };
    result.month = month;
    let Some(day) = pg_mul_s32_overflow(weeks, DAYS_PER_WEEK).and_then(|v| pg_add_s32_overflow(v, days))
    else {
        ereport_interval_out_of_range();
    };
    result.day = day;
    result.time = i64::from(hours) * USECS_PER_HOUR + i64::from(mins) * USECS_PER_MINUTE;
    let secs_u = (secs * USECS_PER_SEC as f64).round();
    if !float8_fits_in_int64(secs_u) {
        ereport_interval_out_of_range();
    }
    let Some(t) = pg_add_s64_overflow(result.time, secs_u as i64) else {
        ereport_interval_out_of_range();
    };
    result.time = t;
    if INTERVAL_NOT_FINITE(&result) {
        ereport_interval_out_of_range();
    }
    pg_return_interval(result)
}

// ===========================================================================
//   now / clock / statement / start / reload (timestamp.c 1608-1636). FULL.
// ===========================================================================

/// PG `now` and `transaction_timestamp`.
pub fn now(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    TimestampTzGetDatum(crate::backend::access::transam::xact::GetCurrentTransactionStartTimestamp())
}

/// PG `statement_timestamp`.
pub fn statement_timestamp(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    TimestampTzGetDatum(crate::backend::access::transam::xact::GetCurrentStatementStartTimestamp())
}

/// PG `clock_timestamp`.
pub fn clock_timestamp(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    TimestampTzGetDatum(crate::utils::timestamp::GetCurrentTimestamp())
}

/// PG `pg_postmaster_start_time`.
pub fn pg_postmaster_start_time(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // SAFETY: process-global set once at postmaster start, read-only thereafter.
    TimestampTzGetDatum(unsafe { PgStartTime })
}

/// PG `pg_conf_load_time`.
pub fn pg_conf_load_time(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // SAFETY: process-global set at config reload, read-only between reloads.
    TimestampTzGetDatum(unsafe { PgReloadTime })
}

/// PG `timeofday`. Needs strftime-style zone formatting (pg_strftime + named
/// zone), STAGED.
pub fn timeofday(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timeofday needs pg_localtime/pg_strftime against the session IANA timezone")
}

// ===========================================================================
//   SetEpochTimestamp / GetEpochTime (timestamp.c 2171-2205). FULL.
// ===========================================================================

/// GetEpochTime: the Unix epoch (1970-01-01) as a pg_tm.
pub fn GetEpochTime(tm: &mut pg_tm) {
    // pg_gmtime(0): 1970-01-01 00:00:00. We fill it directly (no tz DB needed
    // for the fixed epoch).
    tm.year = 1970;
    tm.mon = 1;
    tm.mday = 1;
    tm.hour = 0;
    tm.min = 0;
    tm.sec = 0;
}

/// SetEpochTimestamp: the Unix epoch as a Timestamp.
pub fn SetEpochTimestamp() -> Timestamp {
    let mut tm = new_tm();
    GetEpochTime(&mut tm);
    let mut dt = 0;
    tm2timestamp(&tm, 0, None, &mut dt);
    dt
}

pub fn timestamp_finite(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(!TIMESTAMP_NOT_FINITE(pg_getarg_timestamp(fcinfo, 0)))
}
pub fn interval_finite(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(!INTERVAL_NOT_FINITE(&pg_getarg_interval(fcinfo, 0)))
}

// ===========================================================================
//   timestamp comparison (timestamp.c 2213-2532). FULL.
// ===========================================================================

/// timestamp_relop internal: works for timestamptz too.
pub fn timestamp_cmp_internal(dt1: Timestamp, dt2: Timestamp) -> i32 {
    if dt1 < dt2 {
        -1
    } else if dt1 > dt2 {
        1
    } else {
        0
    }
}

#[inline]
fn timestamptz_cmp_internal(dt1: TimestampTz, dt2: TimestampTz) -> i32 {
    timestamp_cmp_internal(dt1, dt2)
}

pub fn timestamp_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) == 0)
}
pub fn timestamp_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) != 0)
}
pub fn timestamp_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) < 0)
}
pub fn timestamp_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) > 0)
}
pub fn timestamp_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) <= 0)
}
pub fn timestamp_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) >= 0)
}
pub fn timestamp_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(timestamp_cmp_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)))
}

pub fn timestamp_sortsupport(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamp_sortsupport needs the SortSupport node (ssup_datum_signed_cmp)")
}
pub fn timestamp_skipsupport(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamp_skipsupport needs the SkipSupport node (decrement/increment callbacks)")
}
pub fn timestamp_hash(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamp_hash needs hashint8 (DirectFunctionCall over the i64 datum)")
}
pub fn timestamp_hash_extended(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamp_hash_extended needs hashint8extended")
}
pub fn timestamptz_hash(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptz_hash needs hashint8")
}
pub fn timestamptz_hash_extended(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptz_hash_extended needs hashint8extended")
}

// Cross-type timestamp vs timestamptz comparison.
pub fn timestamp_cmp_timestamptz_internal(timestamp_val: Timestamp, dt2: TimestampTz) -> i32 {
    let (dt1, overflow) = timestamp2timestamptz_opt_overflow(timestamp_val);
    if overflow > 0 {
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    if overflow < 0 {
        return if TIMESTAMP_IS_NOBEGIN(dt2) { 1 } else { -1 };
    }
    timestamptz_cmp_internal(dt1, dt2)
}

pub fn timestamp_eq_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) == 0)
}
pub fn timestamp_ne_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) != 0)
}
pub fn timestamp_lt_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) < 0)
}
pub fn timestamp_gt_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) > 0)
}
pub fn timestamp_le_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) <= 0)
}
pub fn timestamp_ge_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) >= 0)
}
pub fn timestamp_cmp_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)))
}

pub fn timestamptz_eq_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) == 0)
}
pub fn timestamptz_ne_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) != 0)
}
pub fn timestamptz_lt_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) > 0)
}
pub fn timestamptz_gt_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) < 0)
}
pub fn timestamptz_le_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) >= 0)
}
pub fn timestamptz_ge_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) <= 0)
}
pub fn timestamptz_cmp_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(-timestamp_cmp_timestamptz_internal(pg_getarg_timestamp(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)))
}

// ===========================================================================
//   interval comparison (timestamp.c 2545-2685). FULL.
// ===========================================================================

fn interval_cmp_value(interval: &Interval) -> Int128 {
    let days = i64::from(interval.month) * 30 + i64::from(interval.day);
    let mut span = int64_to_int128(interval.time);
    int128_add_int64_mul_int64(&mut span, days, USECS_PER_DAY);
    span
}

fn interval_cmp_internal(interval1: &Interval, interval2: &Interval) -> i32 {
    int128_compare(interval_cmp_value(interval1), interval_cmp_value(interval2))
}

fn interval_sign(interval: &Interval) -> i32 {
    int128_compare(interval_cmp_value(interval), int64_to_int128(0))
}

pub fn interval_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(interval_cmp_internal(&pg_getarg_interval(fcinfo, 0), &pg_getarg_interval(fcinfo, 1)) == 0)
}
pub fn interval_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(interval_cmp_internal(&pg_getarg_interval(fcinfo, 0), &pg_getarg_interval(fcinfo, 1)) != 0)
}
pub fn interval_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(interval_cmp_internal(&pg_getarg_interval(fcinfo, 0), &pg_getarg_interval(fcinfo, 1)) < 0)
}
pub fn interval_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(interval_cmp_internal(&pg_getarg_interval(fcinfo, 0), &pg_getarg_interval(fcinfo, 1)) > 0)
}
pub fn interval_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(interval_cmp_internal(&pg_getarg_interval(fcinfo, 0), &pg_getarg_interval(fcinfo, 1)) <= 0)
}
pub fn interval_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(interval_cmp_internal(&pg_getarg_interval(fcinfo, 0), &pg_getarg_interval(fcinfo, 1)) >= 0)
}
pub fn interval_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(interval_cmp_internal(&pg_getarg_interval(fcinfo, 0), &pg_getarg_interval(fcinfo, 1)))
}

pub fn interval_hash(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_hash needs hashint8 over int128_to_int64(interval_cmp_value)")
}
pub fn interval_hash_extended(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_hash_extended needs hashint8extended over int128_to_int64(span)")
}

pub fn overlaps_timestamp(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("overlaps_timestamp needs PG_ARGISNULL-aware 4-arg SQL OVERLAPS with NULL handling")
}

// ===========================================================================
//   timestamp arithmetic (timestamp.c 2818-3621). FULL (no named zone).
// ===========================================================================

pub fn timestamp_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt1 = pg_getarg_timestamp(fcinfo, 0);
    let dt2 = pg_getarg_timestamp(fcinfo, 1);
    TimestampGetDatum(if timestamp_cmp_internal(dt1, dt2) < 0 { dt1 } else { dt2 })
}
pub fn timestamp_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt1 = pg_getarg_timestamp(fcinfo, 0);
    let dt2 = pg_getarg_timestamp(fcinfo, 1);
    TimestampGetDatum(if timestamp_cmp_internal(dt1, dt2) > 0 { dt1 } else { dt2 })
}

/// PG `timestamp_mi` (timestamp - timestamp -> interval).
pub fn timestamp_mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt1 = pg_getarg_timestamp(fcinfo, 0);
    let dt2 = pg_getarg_timestamp(fcinfo, 1);
    let mut result = Interval { time: 0, day: 0, month: 0 };

    if TIMESTAMP_NOT_FINITE(dt1) || TIMESTAMP_NOT_FINITE(dt2) {
        if TIMESTAMP_IS_NOBEGIN(dt1) {
            if TIMESTAMP_IS_NOBEGIN(dt2) {
                ereport_interval_out_of_range();
            }
            INTERVAL_NOBEGIN(&mut result);
        } else if TIMESTAMP_IS_NOEND(dt1) {
            if TIMESTAMP_IS_NOEND(dt2) {
                ereport_interval_out_of_range();
            }
            INTERVAL_NOEND(&mut result);
        } else if TIMESTAMP_IS_NOBEGIN(dt2) {
            INTERVAL_NOEND(&mut result);
        } else {
            INTERVAL_NOBEGIN(&mut result);
        }
        return pg_return_interval(result);
    }

    let Some(t) = pg_sub_s64_overflow(dt1, dt2) else {
        ereport_interval_out_of_range();
    };
    result.time = t;
    result.month = 0;
    result.day = 0;
    interval_justify_hours_internal(&mut result);
    pg_return_interval(result)
}

fn interval_justify_hours_internal(result: &mut Interval) {
    if INTERVAL_NOT_FINITE(result) {
        return;
    }
    let mut wholeday: i64 = 0;
    crate::utils::datetime::TMODULO(&mut result.time, &mut wholeday, USECS_PER_DAY);
    let Some(d) = pg_add_s32_overflow(result.day, wholeday as i32) else {
        ereport_interval_out_of_range();
    };
    result.day = d;
    if result.day > 0 && result.time < 0 {
        result.time += USECS_PER_DAY;
        result.day -= 1;
    } else if result.day < 0 && result.time > 0 {
        result.time -= USECS_PER_DAY;
        result.day += 1;
    }
}

/// PG `interval_justify_interval`.
pub fn interval_justify_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let span = pg_getarg_interval(fcinfo, 0);
    let mut result = span;
    if INTERVAL_NOT_FINITE(&result) {
        return pg_return_interval(result);
    }
    if (result.day > 0 && result.time > 0) || (result.day < 0 && result.time < 0) {
        let wholemonth = result.day / DAYS_PER_MONTH;
        result.day -= wholemonth * DAYS_PER_MONTH;
        let Some(m) = pg_add_s32_overflow(result.month, wholemonth) else {
            ereport_interval_out_of_range();
        };
        result.month = m;
    }
    let mut wholeday: i64 = 0;
    crate::utils::datetime::TMODULO(&mut result.time, &mut wholeday, USECS_PER_DAY);
    result.day += wholeday as i32;

    let wholemonth = result.day / DAYS_PER_MONTH;
    result.day -= wholemonth * DAYS_PER_MONTH;
    let Some(m) = pg_add_s32_overflow(result.month, wholemonth) else {
        ereport_interval_out_of_range();
    };
    result.month = m;

    if result.month > 0 && (result.day < 0 || (result.day == 0 && result.time < 0)) {
        result.day += DAYS_PER_MONTH;
        result.month -= 1;
    } else if result.month < 0 && (result.day > 0 || (result.day == 0 && result.time > 0)) {
        result.day -= DAYS_PER_MONTH;
        result.month += 1;
    }
    if result.day > 0 && result.time < 0 {
        result.time += USECS_PER_DAY;
        result.day -= 1;
    } else if result.day < 0 && result.time > 0 {
        result.time -= USECS_PER_DAY;
        result.day += 1;
    }
    pg_return_interval(result)
}

/// PG `interval_justify_hours`.
pub fn interval_justify_hours(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut result = pg_getarg_interval(fcinfo, 0);
    interval_justify_hours_internal(&mut result);
    pg_return_interval(result)
}

/// PG `interval_justify_days`.
pub fn interval_justify_days(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let span = pg_getarg_interval(fcinfo, 0);
    let mut result = span;
    if INTERVAL_NOT_FINITE(&result) {
        return pg_return_interval(result);
    }
    let wholemonth = result.day / DAYS_PER_MONTH;
    result.day -= wholemonth * DAYS_PER_MONTH;
    let Some(m) = pg_add_s32_overflow(result.month, wholemonth) else {
        ereport_interval_out_of_range();
    };
    result.month = m;
    if result.month > 0 && result.day < 0 {
        result.day += DAYS_PER_MONTH;
        result.month -= 1;
    } else if result.month < 0 && result.day > 0 {
        result.day -= DAYS_PER_MONTH;
        result.month += 1;
    }
    pg_return_interval(result)
}

/// PG `timestamp_pl_interval`.
pub fn timestamp_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    let span = pg_getarg_interval(fcinfo, 1);
    TimestampGetDatum(timestamp_pl_interval_impl(timestamp, &span))
}

fn timestamp_pl_interval_impl(mut timestamp: Timestamp, span: &Interval) -> Timestamp {
    let mut result: Timestamp = 0;
    if INTERVAL_IS_NOBEGIN(span) {
        if TIMESTAMP_IS_NOEND(timestamp) {
            ereport_out_of_range_ts();
        }
        TIMESTAMP_NOBEGIN(&mut result);
        return result;
    } else if INTERVAL_IS_NOEND(span) {
        if TIMESTAMP_IS_NOBEGIN(timestamp) {
            ereport_out_of_range_ts();
        }
        TIMESTAMP_NOEND(&mut result);
        return result;
    } else if TIMESTAMP_NOT_FINITE(timestamp) {
        return timestamp;
    }

    if span.month != 0 {
        let mut tm = new_tm();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, std::ptr::null_mut()) != 0 {
            ereport_out_of_range_ts();
        }
        let Some(mon) = pg_add_s32_overflow(tm.mon, span.month) else {
            ereport_out_of_range_ts();
        };
        tm.mon = mon;
        if tm.mon > MONTHS_PER_YEAR {
            tm.year += (tm.mon - 1) / MONTHS_PER_YEAR;
            tm.mon = ((tm.mon - 1) % MONTHS_PER_YEAR) + 1;
        } else if tm.mon < 1 {
            tm.year += tm.mon / MONTHS_PER_YEAR - 1;
            tm.mon = tm.mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
        }
        let dim = day_tab[usize::from(isleap(tm.year))][(tm.mon - 1) as usize];
        if tm.mday > dim {
            tm.mday = dim;
        }
        if tm2timestamp(&tm, fsec, None, &mut timestamp) != 0 {
            ereport_out_of_range_ts();
        }
    }

    if span.day != 0 {
        let mut tm = new_tm();
        let mut fsec: fsec_t = 0;
        if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, std::ptr::null_mut()) != 0 {
            ereport_out_of_range_ts();
        }
        let julian0 = crate::utils::datetime::date2j(tm.year, tm.mon, tm.mday);
        let Some(julian) = pg_add_s32_overflow(julian0, span.day).filter(|&j| j >= 0) else {
            ereport_out_of_range_ts();
        };
        let (y, m, d) = crate::utils::datetime::j2date(julian);
        tm.year = y;
        tm.mon = m;
        tm.mday = d;
        if tm2timestamp(&tm, fsec, None, &mut timestamp) != 0 {
            ereport_out_of_range_ts();
        }
    }

    let Some(t) = pg_add_s64_overflow(timestamp, span.time) else {
        ereport_out_of_range_ts();
    };
    timestamp = t;
    if !IS_VALID_TIMESTAMP(timestamp) {
        ereport_out_of_range_ts();
    }
    timestamp
}

/// PG `timestamp_mi_interval`.
pub fn timestamp_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    let span = pg_getarg_interval(fcinfo, 1);
    let mut tspan = Interval { time: 0, day: 0, month: 0 };
    interval_um_internal(&span, &mut tspan);
    TimestampGetDatum(timestamp_pl_interval_impl(timestamp, &tspan))
}

/// timestamptz_pl_interval, session/GMT zone. The named-zone month/day rotation
/// (DetermineTimeZoneOffset) is STAGED; with no zone DB we treat the value as
/// GMT, matching our timestamp2tm GMT path.
fn timestamptz_pl_interval_internal(timestamp: TimestampTz, span: &Interval) -> TimestampTz {
    // Without a tz DB, DetermineTimeZoneOffset is 0, so this reduces to the
    // plain timestamp arithmetic.
    timestamp_pl_interval_impl(timestamp, span)
}

fn timestamptz_mi_interval_internal(timestamp: TimestampTz, span: &Interval) -> TimestampTz {
    let mut tspan = Interval { time: 0, day: 0, month: 0 };
    interval_um_internal(span, &mut tspan);
    timestamptz_pl_interval_internal(timestamp, &tspan)
}

pub fn timestamptz_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamptz(fcinfo, 0);
    let span = pg_getarg_interval(fcinfo, 1);
    TimestampTzGetDatum(timestamptz_pl_interval_internal(timestamp, &span))
}

pub fn timestamptz_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamptz(fcinfo, 0);
    let span = pg_getarg_interval(fcinfo, 1);
    TimestampTzGetDatum(timestamptz_mi_interval_internal(timestamp, &span))
}

pub fn timestamptz_pl_interval_at_zone(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptz_pl_interval_at_zone named-zone arg needs IANA tz DB (lookup_timezone)")
}
pub fn timestamptz_mi_interval_at_zone(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptz_mi_interval_at_zone named-zone arg needs IANA tz DB (lookup_timezone)")
}

/// interval_um_internal: negate an interval, guarding overflow.
fn interval_um_internal(interval: &Interval, result: &mut Interval) {
    if INTERVAL_IS_NOBEGIN(interval) {
        INTERVAL_NOEND(result);
    } else if INTERVAL_IS_NOEND(interval) {
        INTERVAL_NOBEGIN(result);
    } else {
        let (Some(t), Some(d), Some(m)) = (
            pg_sub_s64_overflow(0, interval.time),
            pg_sub_s32_overflow(0, interval.day),
            pg_sub_s32_overflow(0, interval.month),
        ) else {
            ereport_interval_out_of_range();
        };
        result.time = t;
        result.day = d;
        result.month = m;
        if INTERVAL_NOT_FINITE(result) {
            ereport_interval_out_of_range();
        }
    }
}

pub fn interval_um(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let interval = pg_getarg_interval(fcinfo, 0);
    let mut result = Interval { time: 0, day: 0, month: 0 };
    interval_um_internal(&interval, &mut result);
    pg_return_interval(result)
}

pub fn interval_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let i1 = pg_getarg_interval(fcinfo, 0);
    let i2 = pg_getarg_interval(fcinfo, 1);
    pg_return_interval(if interval_cmp_internal(&i1, &i2) < 0 { i1 } else { i2 })
}
pub fn interval_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let i1 = pg_getarg_interval(fcinfo, 0);
    let i2 = pg_getarg_interval(fcinfo, 1);
    pg_return_interval(if interval_cmp_internal(&i1, &i2) > 0 { i1 } else { i2 })
}

fn finite_interval_pl(span1: &Interval, span2: &Interval, result: &mut Interval) {
    let (Some(m), Some(d), Some(t)) = (
        pg_add_s32_overflow(span1.month, span2.month),
        pg_add_s32_overflow(span1.day, span2.day),
        pg_add_s64_overflow(span1.time, span2.time),
    ) else {
        ereport_interval_out_of_range();
    };
    result.month = m;
    result.day = d;
    result.time = t;
    if INTERVAL_NOT_FINITE(result) {
        ereport_interval_out_of_range();
    }
}

pub fn interval_pl(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let span1 = pg_getarg_interval(fcinfo, 0);
    let span2 = pg_getarg_interval(fcinfo, 1);
    let mut result = Interval { time: 0, day: 0, month: 0 };
    if INTERVAL_IS_NOBEGIN(&span1) {
        if INTERVAL_IS_NOEND(&span2) {
            ereport_interval_out_of_range();
        }
        INTERVAL_NOBEGIN(&mut result);
    } else if INTERVAL_IS_NOEND(&span1) {
        if INTERVAL_IS_NOBEGIN(&span2) {
            ereport_interval_out_of_range();
        }
        INTERVAL_NOEND(&mut result);
    } else if INTERVAL_NOT_FINITE(&span2) {
        result = span2;
    } else {
        finite_interval_pl(&span1, &span2, &mut result);
    }
    pg_return_interval(result)
}

fn finite_interval_mi(span1: &Interval, span2: &Interval, result: &mut Interval) {
    let (Some(m), Some(d), Some(t)) = (
        pg_sub_s32_overflow(span1.month, span2.month),
        pg_sub_s32_overflow(span1.day, span2.day),
        pg_sub_s64_overflow(span1.time, span2.time),
    ) else {
        ereport_interval_out_of_range();
    };
    result.month = m;
    result.day = d;
    result.time = t;
    if INTERVAL_NOT_FINITE(result) {
        ereport_interval_out_of_range();
    }
}

pub fn interval_mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let span1 = pg_getarg_interval(fcinfo, 0);
    let span2 = pg_getarg_interval(fcinfo, 1);
    let mut result = Interval { time: 0, day: 0, month: 0 };
    if INTERVAL_IS_NOBEGIN(&span1) {
        if INTERVAL_IS_NOBEGIN(&span2) {
            ereport_interval_out_of_range();
        }
        INTERVAL_NOBEGIN(&mut result);
    } else if INTERVAL_IS_NOEND(&span1) {
        if INTERVAL_IS_NOEND(&span2) {
            ereport_interval_out_of_range();
        }
        INTERVAL_NOEND(&mut result);
    } else if INTERVAL_IS_NOBEGIN(&span2) {
        INTERVAL_NOEND(&mut result);
    } else if INTERVAL_IS_NOEND(&span2) {
        INTERVAL_NOBEGIN(&mut result);
    } else {
        finite_interval_mi(&span1, &span2, &mut result);
    }
    pg_return_interval(result)
}

pub fn interval_mul(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let span = pg_getarg_interval(fcinfo, 0);
    let factor = pg_getarg_float8(fcinfo, 1);
    let orig_month = span.month;
    let orig_day = span.day;
    let mut result = Interval { time: 0, day: 0, month: 0 };

    if factor.is_nan() {
        ereport_interval_out_of_range();
    }
    if INTERVAL_NOT_FINITE(&span) {
        if factor == 0.0 {
            ereport_interval_out_of_range();
        }
        if factor < 0.0 {
            interval_um_internal(&span, &mut result);
        } else {
            result = span;
        }
        return pg_return_interval(result);
    }
    if factor.is_infinite() {
        let isign = interval_sign(&span);
        if isign == 0 {
            ereport_interval_out_of_range();
        }
        if factor * f64::from(isign) < 0.0 {
            INTERVAL_NOBEGIN(&mut result);
        } else {
            INTERVAL_NOEND(&mut result);
        }
        return pg_return_interval(result);
    }

    let mut result_double = f64::from(span.month) * factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        ereport_interval_out_of_range();
    }
    result.month = result_double as i32;

    result_double = f64::from(span.day) * factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        ereport_interval_out_of_range();
    }
    result.day = result_double as i32;

    let mut month_remainder_days =
        (f64::from(orig_month) * factor - f64::from(result.month)) * f64::from(DAYS_PER_MONTH);
    month_remainder_days = TSROUND(month_remainder_days);
    let mut sec_remainder = (f64::from(orig_day) * factor - f64::from(result.day) + month_remainder_days
        - (month_remainder_days as i32) as f64)
        * f64::from(SECS_PER_DAY);
    sec_remainder = TSROUND(sec_remainder);

    if sec_remainder.abs() >= f64::from(SECS_PER_DAY) {
        let Some(d) = pg_add_s32_overflow(result.day, (sec_remainder / f64::from(SECS_PER_DAY)) as i32)
        else {
            ereport_interval_out_of_range();
        };
        result.day = d;
        sec_remainder -= ((sec_remainder / f64::from(SECS_PER_DAY)) as i32) as f64 * f64::from(SECS_PER_DAY);
    }
    let Some(d) = pg_add_s32_overflow(result.day, month_remainder_days as i32) else {
        ereport_interval_out_of_range();
    };
    result.day = d;
    result_double = (span.time as f64 * factor + sec_remainder * USECS_PER_SEC as f64).round();
    if result_double.is_nan() || !float8_fits_in_int64(result_double) {
        ereport_interval_out_of_range();
    }
    result.time = result_double as i64;
    if INTERVAL_NOT_FINITE(&result) {
        ereport_interval_out_of_range();
    }
    pg_return_interval(result)
}

pub fn mul_d_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // Args are (float8, Interval); interval_mul takes (Interval, float8).
    let factor = fcinfo.args[0].value;
    let span = pg_getarg_interval(fcinfo, 1);
    let mut swapped = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: crate::postgres_ext::InvalidOid,
        isnull: false,
        nargs: 2,
        args: vec![
            crate::postgres::NullableDatum { value: IntervalPGetDatum(&span), isnull: false },
            crate::postgres::NullableDatum { value: factor, isnull: false },
        ],
    };
    interval_mul(&mut swapped)
}

pub fn interval_div(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let span = pg_getarg_interval(fcinfo, 0);
    let factor = pg_getarg_float8(fcinfo, 1);
    let orig_month = span.month;
    let orig_day = span.day;
    let mut result = Interval { time: 0, day: 0, month: 0 };

    if factor == 0.0 {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DIVISION_BY_ZERO).errmsg("division by zero".to_string());
        });
        unreachable!()
    }
    if factor.is_nan() {
        ereport_interval_out_of_range();
    }
    if INTERVAL_NOT_FINITE(&span) {
        if factor.is_infinite() {
            ereport_interval_out_of_range();
        }
        if factor < 0.0 {
            interval_um_internal(&span, &mut result);
        } else {
            result = span;
        }
        return pg_return_interval(result);
    }

    let mut result_double = f64::from(span.month) / factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        ereport_interval_out_of_range();
    }
    result.month = result_double as i32;

    result_double = f64::from(span.day) / factor;
    if result_double.is_nan() || !float8_fits_in_int32(result_double) {
        ereport_interval_out_of_range();
    }
    result.day = result_double as i32;

    let mut month_remainder_days =
        (f64::from(orig_month) / factor - f64::from(result.month)) * f64::from(DAYS_PER_MONTH);
    month_remainder_days = TSROUND(month_remainder_days);
    let mut sec_remainder = (f64::from(orig_day) / factor - f64::from(result.day) + month_remainder_days
        - (month_remainder_days as i32) as f64)
        * f64::from(SECS_PER_DAY);
    sec_remainder = TSROUND(sec_remainder);
    if sec_remainder.abs() >= f64::from(SECS_PER_DAY) {
        let Some(d) = pg_add_s32_overflow(result.day, (sec_remainder / f64::from(SECS_PER_DAY)) as i32)
        else {
            ereport_interval_out_of_range();
        };
        result.day = d;
        sec_remainder -= ((sec_remainder / f64::from(SECS_PER_DAY)) as i32) as f64 * f64::from(SECS_PER_DAY);
    }
    let Some(d) = pg_add_s32_overflow(result.day, month_remainder_days as i32) else {
        ereport_interval_out_of_range();
    };
    result.day = d;
    result_double = (span.time as f64 / factor + sec_remainder * USECS_PER_SEC as f64).round();
    if result_double.is_nan() || !float8_fits_in_int64(result_double) {
        ereport_interval_out_of_range();
    }
    result.time = result_double as i64;
    if INTERVAL_NOT_FINITE(&result) {
        ereport_interval_out_of_range();
    }
    pg_return_interval(result)
}

// in_range support (timestamp.c 3860-3977). FULL (uses internal arithmetic).

pub fn in_range_timestamptz_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = pg_getarg_timestamptz(fcinfo, 0);
    let base = pg_getarg_timestamptz(fcinfo, 1);
    let offset = pg_getarg_interval(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);
    if interval_sign(&offset) < 0 {
        ereport_invalid_preceding_following();
    }
    if INTERVAL_IS_NOEND(&offset) && (if sub { TIMESTAMP_IS_NOEND(base) } else { TIMESTAMP_IS_NOBEGIN(base) }) {
        return BoolGetDatum(true);
    }
    let sum = if sub {
        timestamptz_mi_interval_internal(base, &offset)
    } else {
        timestamptz_pl_interval_internal(base, &offset)
    };
    BoolGetDatum(if less { val <= sum } else { val >= sum })
}

pub fn in_range_timestamp_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = pg_getarg_timestamp(fcinfo, 0);
    let base = pg_getarg_timestamp(fcinfo, 1);
    let offset = pg_getarg_interval(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);
    if interval_sign(&offset) < 0 {
        ereport_invalid_preceding_following();
    }
    if INTERVAL_IS_NOEND(&offset) && (if sub { TIMESTAMP_IS_NOEND(base) } else { TIMESTAMP_IS_NOBEGIN(base) }) {
        return BoolGetDatum(true);
    }
    let sum = if sub {
        let mut t = Interval { time: 0, day: 0, month: 0 };
        interval_um_internal(&offset, &mut t);
        timestamp_pl_interval_impl(base, &t)
    } else {
        timestamp_pl_interval_impl(base, &offset)
    };
    BoolGetDatum(if less { val <= sum } else { val >= sum })
}

pub fn in_range_interval_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = pg_getarg_interval(fcinfo, 0);
    let base = pg_getarg_interval(fcinfo, 1);
    let offset = pg_getarg_interval(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);
    if interval_sign(&offset) < 0 {
        ereport_invalid_preceding_following();
    }
    if INTERVAL_IS_NOEND(&offset) && (if sub { INTERVAL_IS_NOEND(&base) } else { INTERVAL_IS_NOBEGIN(&base) }) {
        return BoolGetDatum(true);
    }
    let mut sum = Interval { time: 0, day: 0, month: 0 };
    if sub {
        if INTERVAL_IS_NOBEGIN(&base) {
            INTERVAL_NOBEGIN(&mut sum);
        } else if INTERVAL_IS_NOEND(&base) {
            INTERVAL_NOEND(&mut sum);
        } else if INTERVAL_IS_NOBEGIN(&offset) {
            INTERVAL_NOEND(&mut sum);
        } else if INTERVAL_IS_NOEND(&offset) {
            INTERVAL_NOBEGIN(&mut sum);
        } else {
            finite_interval_mi(&base, &offset, &mut sum);
        }
    } else if INTERVAL_IS_NOBEGIN(&base) {
        if INTERVAL_IS_NOEND(&offset) {
            ereport_interval_out_of_range();
        }
        INTERVAL_NOBEGIN(&mut sum);
    } else if INTERVAL_IS_NOEND(&base) {
        if INTERVAL_IS_NOBEGIN(&offset) {
            ereport_interval_out_of_range();
        }
        INTERVAL_NOEND(&mut sum);
    } else if INTERVAL_NOT_FINITE(&offset) {
        sum = offset;
    } else {
        finite_interval_pl(&base, &offset, &mut sum);
    }
    let c = interval_cmp_internal(&val, &sum);
    BoolGetDatum(if less { c <= 0 } else { c >= 0 })
}

fn ereport_invalid_preceding_following() -> ! {
    ereport!(ERROR, |e: &mut ErrorData| {
        e.errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
            .errmsg("invalid preceding or following size in window function".to_string());
    });
    unreachable!()
}

// interval aggregates -> STAGED (need the aggregate transition context).
pub fn interval_avg_accum(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_avg_accum needs the IntervalAggState aggregate transition context (AggCheckCallContext)")
}
pub fn interval_avg_accum_inv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_avg_accum_inv needs the IntervalAggState aggregate transition context")
}
pub fn interval_avg_combine(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_avg_combine needs the IntervalAggState aggregate transition context")
}
pub fn interval_avg_serialize(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_avg_serialize needs the binary wire StringInfo + aggregate context")
}
pub fn interval_avg_deserialize(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_avg_deserialize needs the binary wire StringInfo + aggregate context")
}
pub fn interval_avg(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_avg needs the IntervalAggState aggregate transition context")
}
pub fn interval_sum(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("interval_sum needs the IntervalAggState aggregate transition context")
}

// ===========================================================================
//   age (timestamp.c 4309-4598). FULL.
// ===========================================================================

fn age_symbolic_difference(
    dt1: Timestamp,
    dt2: Timestamp,
    tm1: &pg_tm,
    fsec1: fsec_t,
    tm2: &pg_tm,
    fsec2: fsec_t,
) -> Interval {
    let mut tm = pg_itm {
        usec: fsec1 - fsec2,
        sec: tm1.sec - tm2.sec,
        min: tm1.min - tm2.min,
        hour: i64::from(tm1.hour - tm2.hour),
        mday: tm1.mday - tm2.mday,
        mon: tm1.mon - tm2.mon,
        year: tm1.year - tm2.year,
    };

    let flip = dt1 < dt2;
    if flip {
        negate_itm(&mut tm);
    }
    while tm.usec < 0 {
        tm.usec += USECS_PER_SEC as i32;
        tm.sec -= 1;
    }
    while tm.sec < 0 {
        tm.sec += SECS_PER_MINUTE;
        tm.min -= 1;
    }
    while tm.min < 0 {
        tm.min += MINS_PER_HOUR;
        tm.hour -= 1;
    }
    while tm.hour < 0 {
        tm.hour += i64::from(HOURS_PER_DAY);
        tm.mday -= 1;
    }
    while tm.mday < 0 {
        if flip {
            tm.mday += day_tab[usize::from(isleap(tm1.year))][(tm1.mon - 1) as usize];
        } else {
            tm.mday += day_tab[usize::from(isleap(tm2.year))][(tm2.mon - 1) as usize];
        }
        tm.mon -= 1;
    }
    while tm.mon < 0 {
        tm.mon += MONTHS_PER_YEAR;
        tm.year -= 1;
    }
    if flip {
        negate_itm(&mut tm);
    }
    let mut result = Interval { time: 0, day: 0, month: 0 };
    if itm2interval(&mut tm, &mut result) != 0 {
        ereport_interval_out_of_range();
    }
    result
}

fn negate_itm(tm: &mut pg_itm) {
    tm.usec = -tm.usec;
    tm.sec = -tm.sec;
    tm.min = -tm.min;
    tm.hour = -tm.hour;
    tm.mday = -tm.mday;
    tm.mon = -tm.mon;
    tm.year = -tm.year;
}

fn age_common(dt1: Timestamp, dt2: Timestamp) -> Interval {
    let mut result = Interval { time: 0, day: 0, month: 0 };
    if TIMESTAMP_IS_NOBEGIN(dt1) {
        if TIMESTAMP_IS_NOBEGIN(dt2) {
            ereport_interval_out_of_range();
        }
        INTERVAL_NOBEGIN(&mut result);
    } else if TIMESTAMP_IS_NOEND(dt1) {
        if TIMESTAMP_IS_NOEND(dt2) {
            ereport_interval_out_of_range();
        }
        INTERVAL_NOEND(&mut result);
    } else if TIMESTAMP_IS_NOBEGIN(dt2) {
        INTERVAL_NOEND(&mut result);
    } else if TIMESTAMP_IS_NOEND(dt2) {
        INTERVAL_NOBEGIN(&mut result);
    } else {
        let mut tm1 = new_tm();
        let mut tm2 = new_tm();
        let mut fsec1: fsec_t = 0;
        let mut fsec2: fsec_t = 0;
        if timestamp2tm(dt1, None, &mut tm1, &mut fsec1, None, std::ptr::null_mut()) == 0
            && timestamp2tm(dt2, None, &mut tm2, &mut fsec2, None, std::ptr::null_mut()) == 0
        {
            return age_symbolic_difference(dt1, dt2, &tm1, fsec1, &tm2, fsec2);
        }
        ereport_out_of_range_ts();
    }
    result
}

pub fn timestamp_age(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dt1 = pg_getarg_timestamp(fcinfo, 0);
    let dt2 = pg_getarg_timestamp(fcinfo, 1);
    pg_return_interval(age_common(dt1, dt2))
}

pub fn timestamptz_age(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // tz1/tz2 are deliberately ignored by the C code; reduce to timestamp_age.
    let dt1 = pg_getarg_timestamptz(fcinfo, 0);
    let dt2 = pg_getarg_timestamptz(fcinfo, 1);
    pg_return_interval(age_common(dt1, dt2))
}

// ===========================================================================
//   date_bin (timestamp.c 4609-4910). FULL.
// ===========================================================================

fn date_bin_impl(stride: &Interval, timestamp: Timestamp, origin: Timestamp) -> Timestamp {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return timestamp;
    }
    if TIMESTAMP_NOT_FINITE(origin) {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE).errmsg("origin out of range".to_string());
        });
        unreachable!()
    }
    if INTERVAL_NOT_FINITE(stride) {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("timestamps cannot be binned into infinite intervals".to_string());
        });
        unreachable!()
    }
    if stride.month != 0 {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_FEATURE_NOT_SUPPORTED).errmsg(
                "timestamps cannot be binned into intervals containing months or years".to_string(),
            );
        });
        unreachable!()
    }
    let Some(stride_usecs) =
        pg_mul_s64_overflow(i64::from(stride.day), USECS_PER_DAY).and_then(|v| pg_add_s64_overflow(v, stride.time))
    else {
        ereport_interval_out_of_range();
    };
    if stride_usecs <= 0 {
        ereport!(ERROR, |e: &mut ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("stride must be greater than zero".to_string());
        });
        unreachable!()
    }
    let Some(tm_diff) = pg_sub_s64_overflow(timestamp, origin) else {
        ereport_interval_out_of_range();
    };
    let tm_modulo = tm_diff % stride_usecs;
    let tm_delta = tm_diff - tm_modulo;
    let mut result = origin + tm_delta;
    if tm_modulo < 0 {
        let Some(r) = pg_sub_s64_overflow(result, stride_usecs).filter(|&r| IS_VALID_TIMESTAMP(r)) else {
            ereport_out_of_range_ts();
        };
        result = r;
    }
    result
}

pub fn timestamp_bin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let stride = pg_getarg_interval(fcinfo, 0);
    let timestamp = pg_getarg_timestamp(fcinfo, 1);
    let origin = pg_getarg_timestamp(fcinfo, 2);
    TimestampGetDatum(date_bin_impl(&stride, timestamp, origin))
}

pub fn timestamptz_bin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let stride = pg_getarg_interval(fcinfo, 0);
    let timestamp = pg_getarg_timestamptz(fcinfo, 1);
    let origin = pg_getarg_timestamptz(fcinfo, 2);
    TimestampTzGetDatum(date_bin_impl(&stride, timestamp, origin))
}

// ===========================================================================
//   trunc (timestamp.c 4677-5254). FULL for unit-only (no named zone).
// ===========================================================================

fn timestamp_trunc_impl(lowunits: &str, timestamp: Timestamp, typename: &str) -> Timestamp {
    let (type_, val) = DecodeUnits(lowunits);
    if type_ != UNITS {
        ereport_unit_not_recognized(lowunits, typename);
    }
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return match val {
            v if is_truncatable_unit(v) => timestamp,
            _ => ereport_unit_not_supported(lowunits, typename),
        };
    }
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, std::ptr::null_mut()) != 0 {
        ereport_out_of_range_ts();
    }
    apply_trunc(&mut tm, &mut fsec, val, lowunits, typename);
    let mut result = 0;
    if tm2timestamp(&tm, fsec, None, &mut result) != 0 {
        ereport_out_of_range_ts();
    }
    result
}

const fn is_truncatable_unit(val: i32) -> bool {
    matches!(
        val,
        v if v == DTK_WEEK
            || v == DTK_MILLENNIUM
            || v == DTK_CENTURY
            || v == DTK_DECADE
            || v == DTK_YEAR
            || v == DTK_QUARTER
            || v == DTK_MONTH
            || v == DTK_DAY
            || v == DTK_HOUR
            || v == DTK_MINUTE
            || v == DTK_SECOND
            || v == DTK_MILLISEC
            || v == DTK_MICROSEC
    )
}

/// Apply timestamp truncation to a broken-down tm + fsec (the C switch ladder).
fn apply_trunc(tm: &mut pg_tm, fsec: &mut fsec_t, val: i32, lowunits: &str, typename: &str) {
    // Cascading: handle the year/sub-year fields first, then the descending
    // FALL THRU chain via explicit grouping.
    if val == DTK_WEEK {
        let woy = date2isoweek(tm.year, tm.mon, tm.mday);
        if woy >= 52 && tm.mon == 1 {
            tm.year -= 1;
        }
        if woy <= 1 && tm.mon == MONTHS_PER_YEAR {
            tm.year += 1;
        }
        let (y, m, d) = isoweek2date_with(tm.year, woy);
        tm.year = y;
        tm.mon = m;
        tm.mday = d;
        tm.hour = 0;
        tm.min = 0;
        tm.sec = 0;
        *fsec = 0;
        return;
    }
    if val == DTK_MILLISEC {
        *fsec = (*fsec / 1000) * 1000;
        return;
    }
    if val == DTK_MICROSEC {
        return;
    }
    // FALL THRU chain for the date/time fields (>= SECOND).
    if !matches!(
        val,
        v if v == DTK_MILLENNIUM || v == DTK_CENTURY || v == DTK_DECADE || v == DTK_YEAR
            || v == DTK_QUARTER || v == DTK_MONTH || v == DTK_DAY || v == DTK_HOUR
            || v == DTK_MINUTE || v == DTK_SECOND
    ) {
        ereport_unit_not_supported(lowunits, typename);
    }
    if val == DTK_MILLENNIUM {
        tm.year = if tm.year > 0 {
            ((tm.year + 999) / 1000) * 1000 - 999
        } else {
            -((999 - (tm.year - 1)) / 1000) * 1000 + 1
        };
    }
    if val == DTK_MILLENNIUM || val == DTK_CENTURY {
        tm.year = if tm.year > 0 {
            ((tm.year + 99) / 100) * 100 - 99
        } else {
            -((99 - (tm.year - 1)) / 100) * 100 + 1
        };
    }
    if val == DTK_DECADE {
        tm.year = if tm.year > 0 {
            (tm.year / 10) * 10
        } else {
            -((8 - (tm.year - 1)) / 10) * 10
        };
    }
    if matches!(val, v if v == DTK_MILLENNIUM || v == DTK_CENTURY || v == DTK_DECADE || v == DTK_YEAR) {
        tm.mon = 1;
    }
    if matches!(
        val,
        v if v == DTK_MILLENNIUM || v == DTK_CENTURY || v == DTK_DECADE || v == DTK_YEAR || v == DTK_QUARTER
    ) {
        tm.mon = (3 * ((tm.mon - 1) / 3)) + 1;
    }
    // C FALL THRU from MONTH downward zeroes each finer field. Selecting unit
    // `val` keeps fields >= val and zeroes all finer ones (rank: coarser=larger).
    if rank_of(val) >= rank_of(DTK_MONTH) {
        tm.mday = 1;
    }
    if rank_of(val) >= rank_of(DTK_DAY) {
        tm.hour = 0;
    }
    if rank_of(val) >= rank_of(DTK_HOUR) {
        tm.min = 0;
    }
    if rank_of(val) >= rank_of(DTK_MINUTE) {
        tm.sec = 0;
    }
    if rank_of(val) >= rank_of(DTK_SECOND) {
        *fsec = 0;
    }
}

/// Field rank: larger = coarser. Used to zero all fields below `val`.
const fn rank_of(val: i32) -> i32 {
    match val {
        v if v == DTK_MICROSEC => 0,
        v if v == DTK_MILLISEC => 1,
        v if v == DTK_SECOND => 2,
        v if v == DTK_MINUTE => 3,
        v if v == DTK_HOUR => 4,
        v if v == DTK_DAY => 5,
        v if v == DTK_MONTH || v == DTK_QUARTER => 6,
        _ => 7,
    }
}

pub fn timestamp_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lowunits = lowunits_of(pg_getarg_text_bytes(fcinfo, 0));
    let timestamp = pg_getarg_timestamp(fcinfo, 1);
    TimestampGetDatum(timestamp_trunc_impl(&lowunits, timestamp, "timestamp without time zone"))
}

/// timestamptz_trunc in session timezone. Without a tz DB the session zone is
/// GMT, so the unit-only truncation reduces to the timestamp path.
pub fn timestamptz_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lowunits = lowunits_of(pg_getarg_text_bytes(fcinfo, 0));
    let timestamp = pg_getarg_timestamptz(fcinfo, 1);
    TimestampTzGetDatum(timestamp_trunc_impl(&lowunits, timestamp, "timestamp with time zone"))
}

pub fn timestamptz_trunc_zone(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptz_trunc_zone named-zone arg needs IANA tz DB (lookup_timezone)")
}

/// PG `interval_trunc`. FULL.
pub fn interval_trunc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lowunits = lowunits_of(pg_getarg_text_bytes(fcinfo, 0));
    let interval = pg_getarg_interval(fcinfo, 1);
    let typename = "interval";
    let (type_, val) = DecodeUnits(&lowunits);
    if type_ != UNITS {
        ereport_unit_not_recognized(&lowunits, typename);
    }
    let mut result = Interval { time: 0, day: 0, month: 0 };
    if INTERVAL_NOT_FINITE(&interval) {
        return match val {
            v if is_interval_truncatable_unit(v) => {
                result = interval;
                pg_return_interval(result)
            }
            _ => ereport_unit_not_supported(&lowunits, typename),
        };
    }
    let mut tm = pg_itm::default();
    interval2itm(interval, &mut tm);
    if !is_interval_truncatable_unit(val) {
        ereport_unit_not_supported(&lowunits, typename);
    }
    if val == DTK_MILLENNIUM {
        tm.year = (tm.year / 1000) * 1000;
    }
    if val == DTK_MILLENNIUM || val == DTK_CENTURY {
        tm.year = (tm.year / 100) * 100;
    }
    if val == DTK_MILLENNIUM || val == DTK_CENTURY || val == DTK_DECADE {
        tm.year = (tm.year / 10) * 10;
    }
    if rank_of_interval(val) >= rank_of_interval(DTK_YEAR) {
        tm.mon = 0;
    }
    if rank_of_interval(val) >= rank_of_interval(DTK_QUARTER) {
        tm.mon = 3 * (tm.mon / 3);
    }
    if rank_of_interval(val) >= rank_of_interval(DTK_MONTH) {
        tm.mday = 0;
    }
    if rank_of_interval(val) >= rank_of_interval(DTK_DAY) {
        tm.hour = 0;
    }
    if rank_of_interval(val) >= rank_of_interval(DTK_HOUR) {
        tm.min = 0;
    }
    if rank_of_interval(val) >= rank_of_interval(DTK_MINUTE) {
        tm.sec = 0;
    }
    if rank_of_interval(val) >= rank_of_interval(DTK_SECOND) {
        tm.usec = 0;
    }
    if val == DTK_MILLISEC {
        tm.usec = (tm.usec / 1000) * 1000;
    }
    if itm2interval(&mut tm, &mut result) != 0 {
        ereport_interval_out_of_range();
    }
    pg_return_interval(result)
}

const fn is_interval_truncatable_unit(val: i32) -> bool {
    matches!(
        val,
        v if v == DTK_MILLENNIUM || v == DTK_CENTURY || v == DTK_DECADE || v == DTK_YEAR
            || v == DTK_QUARTER || v == DTK_MONTH || v == DTK_DAY || v == DTK_HOUR
            || v == DTK_MINUTE || v == DTK_SECOND || v == DTK_MILLISEC || v == DTK_MICROSEC
    )
}

/// Interval-trunc field rank: larger = coarser; we zero all fields at-or-below
/// the selected one (the C FALL THRU sets coarser fields then zeroes finer).
const fn rank_of_interval(val: i32) -> i32 {
    match val {
        v if v == DTK_MICROSEC => 0,
        v if v == DTK_MILLISEC => 1,
        v if v == DTK_SECOND => 2,
        v if v == DTK_MINUTE => 3,
        v if v == DTK_HOUR => 4,
        v if v == DTK_DAY => 5,
        v if v == DTK_MONTH => 6,
        v if v == DTK_QUARTER => 7,
        _ => 8, // YEAR/DECADE/CENTURY/MILLENNIUM
    }
}

// ===========================================================================
//   ISO week/year family (timestamp.c 5256-5432). FULL.
// ===========================================================================

pub fn isoweek2j(year: i32, week: i32) -> i32 {
    let day4 = crate::utils::datetime::date2j(year, 1, 4);
    let day0 = crate::utils::datetime::j2day(day4 - 1);
    ((week - 1) * 7) + (day4 - day0)
}

pub fn isoweek2date(woy: i32) -> (i32, i32, i32) {
    // C signature passes year in *year; the header wrapper threads it. We take
    // the year from the caller-supplied tuple convention: callers pass the ISO
    // year via a separate path -- here we mirror the header (woy only) and use
    // the j2date of isoweek2j; the year must be set by the caller beforehand.
    // To keep this self-contained we require the year via the thread-local-free
    // form: callers (trunc) set tm.year first, then call isoweek2date_with.
    // The header decl is (woy)->(year,mon,mday); we cannot know year here, so
    // the trunc path uses isoweek2date_with directly.
    unimplemented!("isoweek2date needs the ISO year; call isoweek2date_with(year, woy)")
}

/// isoweek2date with an explicit ISO year (the form the C uses via *year).
fn isoweek2date_with(year: i32, woy: i32) -> (i32, i32, i32) {
    crate::utils::datetime::j2date(isoweek2j(year, woy))
}

pub fn isoweekdate2date(isoweek: i32, wday: i32) -> (i32, i32, i32) {
    // As above, the ISO year is threaded via the caller; this header form lacks
    // it. Provide the explicit-year worker.
    let _ = (isoweek, wday);
    unimplemented!("isoweekdate2date needs the ISO year; call isoweekdate2date_with(year, isoweek, wday)")
}

#[allow(dead_code, reason = "explicit-year worker mirrors C *year threading; used once date.rs lands callers")]
fn isoweekdate2date_with(year: i32, isoweek: i32, wday: i32) -> (i32, i32, i32) {
    let mut jday = isoweek2j(year, isoweek);
    if wday > 1 {
        jday += wday - 2;
    } else {
        jday += 6;
    }
    crate::utils::datetime::j2date(jday)
}

pub fn date2isoweek(year: i32, mon: i32, mday: i32) -> i32 {
    let dayn = crate::utils::datetime::date2j(year, mon, mday);
    let mut day4 = crate::utils::datetime::date2j(year, 1, 4);
    let mut day0 = crate::utils::datetime::j2day(day4 - 1);
    if dayn < day4 - day0 {
        day4 = crate::utils::datetime::date2j(year - 1, 1, 4);
        day0 = crate::utils::datetime::j2day(day4 - 1);
    }
    let mut result = f64::from(dayn - (day4 - day0)) / 7.0 + 1.0;
    if result >= 52.0 {
        day4 = crate::utils::datetime::date2j(year + 1, 1, 4);
        day0 = crate::utils::datetime::j2day(day4 - 1);
        if dayn >= day4 - day0 {
            result = f64::from(dayn - (day4 - day0)) / 7.0 + 1.0;
        }
    }
    result as i32
}

pub fn date2isoyear(mut year: i32, mon: i32, mday: i32) -> i32 {
    let dayn = crate::utils::datetime::date2j(year, mon, mday);
    let mut day4 = crate::utils::datetime::date2j(year, 1, 4);
    let mut day0 = crate::utils::datetime::j2day(day4 - 1);
    if dayn < day4 - day0 {
        day4 = crate::utils::datetime::date2j(year - 1, 1, 4);
        day0 = crate::utils::datetime::j2day(day4 - 1);
        year -= 1;
    }
    let result = f64::from(dayn - (day4 - day0)) / 7.0 + 1.0;
    if result >= 52.0 {
        day4 = crate::utils::datetime::date2j(year + 1, 1, 4);
        day0 = crate::utils::datetime::j2day(day4 - 1);
        if dayn >= day4 - day0 {
            year += 1;
        }
    }
    year
}

pub fn date2isoyearday(year: i32, mon: i32, mday: i32) -> i32 {
    crate::utils::datetime::date2j(year, mon, mday) - isoweek2j(date2isoyear(year, mon, mday), 1) + 1
}

// ===========================================================================
//   date_part (float8 path) -- timestamp.c 5499-6318. FULL for *_part.
//   The retnumeric=true EXTRACT variants need numeric machinery -> STAGED.
// ===========================================================================

fn non_finite_timestamp_part(type_: i32, unit: i32, lowunits: &str, is_negative: bool, is_tz: bool) -> f64 {
    let tn = if is_tz { "timestamp with time zone" } else { "timestamp without time zone" };
    if type_ != UNITS && type_ != RESERV {
        ereport_unit_not_recognized(lowunits, tn);
    }
    match unit {
        u if u == DTK_MICROSEC || u == DTK_MILLISEC || u == DTK_SECOND || u == DTK_MINUTE
            || u == DTK_HOUR || u == DTK_DAY || u == DTK_MONTH || u == DTK_QUARTER
            || u == DTK_WEEK || u == DTK_DOW || u == DTK_ISODOW || u == DTK_DOY
            || u == DTK_TZ || u == DTK_TZ_MINUTE || u == DTK_TZ_HOUR =>
        {
            0.0
        }
        u if u == DTK_YEAR || u == DTK_DECADE || u == DTK_CENTURY || u == DTK_MILLENNIUM
            || u == DTK_JULIAN || u == DTK_ISOYEAR || u == DTK_EPOCH =>
        {
            if is_negative {
                -get_float8_infinity()
            } else {
                get_float8_infinity()
            }
        }
        _ => ereport_unit_not_supported(lowunits, tn),
    }
}

fn timestamp_part_float8(lowunits: &str, timestamp: Timestamp, want_tz: bool) -> Option<f64> {
    let tn = if want_tz { "timestamp with time zone" } else { "timestamp without time zone" };
    let (mut type_, mut val) = DecodeUnits(lowunits);
    if type_ == UNKNOWN_FIELD {
        let (t, v) = DecodeSpecial(lowunits);
        type_ = t;
        val = v;
    }

    if TIMESTAMP_NOT_FINITE(timestamp) {
        let r = non_finite_timestamp_part(type_, val, lowunits, TIMESTAMP_IS_NOBEGIN(timestamp), want_tz);
        return if r != 0.0 { Some(r) } else { None };
    }

    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tz = 0i32;
    if type_ == UNITS {
        let tzp = if want_tz { Some(&mut tz) } else { None };
        if timestamp2tm(timestamp, tzp, &mut tm, &mut fsec, None, std::ptr::null_mut()) != 0 {
            ereport_out_of_range_ts();
        }
        let intresult: i64 = match val {
            v if v == DTK_TZ => i64::from(-tz),
            v if v == DTK_TZ_MINUTE => i64::from((-tz / SECS_PER_MINUTE) % MINS_PER_HOUR),
            v if v == DTK_TZ_HOUR => i64::from(-tz / SECS_PER_HOUR),
            v if v == DTK_MICROSEC => i64::from(tm.sec) * 1_000_000 + i64::from(fsec),
            v if v == DTK_MILLISEC => return Some(f64::from(tm.sec) * 1000.0 + f64::from(fsec) / 1000.0),
            v if v == DTK_SECOND => return Some(f64::from(tm.sec) + f64::from(fsec) / 1_000_000.0),
            v if v == DTK_MINUTE => i64::from(tm.min),
            v if v == DTK_HOUR => i64::from(tm.hour),
            v if v == DTK_DAY => i64::from(tm.mday),
            v if v == DTK_MONTH => i64::from(tm.mon),
            v if v == DTK_QUARTER => i64::from((tm.mon - 1) / 3 + 1),
            v if v == DTK_WEEK => i64::from(date2isoweek(tm.year, tm.mon, tm.mday)),
            v if v == DTK_YEAR => {
                if tm.year > 0 {
                    i64::from(tm.year)
                } else {
                    i64::from(tm.year - 1)
                }
            }
            v if v == DTK_DECADE => {
                if tm.year >= 0 {
                    i64::from(tm.year / 10)
                } else {
                    i64::from(-((8 - (tm.year - 1)) / 10))
                }
            }
            v if v == DTK_CENTURY => {
                if tm.year > 0 {
                    i64::from((tm.year + 99) / 100)
                } else {
                    i64::from(-((99 - (tm.year - 1)) / 100))
                }
            }
            v if v == DTK_MILLENNIUM => {
                if tm.year > 0 {
                    i64::from((tm.year + 999) / 1000)
                } else {
                    i64::from(-((999 - (tm.year - 1)) / 1000))
                }
            }
            v if v == DTK_JULIAN => {
                return Some(
                    f64::from(crate::utils::datetime::date2j(tm.year, tm.mon, tm.mday))
                        + (f64::from(
                            (((tm.hour * MINS_PER_HOUR) + tm.min) * SECS_PER_MINUTE) + tm.sec,
                        ) + f64::from(fsec) / 1_000_000.0)
                            / f64::from(SECS_PER_DAY),
                );
            }
            v if v == DTK_ISOYEAR => {
                let mut r = i64::from(date2isoyear(tm.year, tm.mon, tm.mday));
                if r <= 0 {
                    r -= 1;
                }
                r
            }
            v if v == DTK_DOW || v == DTK_ISODOW => {
                let mut r = i64::from(crate::utils::datetime::j2day(crate::utils::datetime::date2j(
                    tm.year, tm.mon, tm.mday,
                )));
                if v == DTK_ISODOW && r == 0 {
                    r = 7;
                }
                r
            }
            v if v == DTK_DOY => i64::from(
                crate::utils::datetime::date2j(tm.year, tm.mon, tm.mday)
                    - crate::utils::datetime::date2j(tm.year, 1, 1)
                    + 1,
            ),
            _ => ereport_unit_not_supported(lowunits, tn),
        };
        Some(intresult as f64)
    } else if type_ == RESERV && val == DTK_EPOCH {
        let epoch = SetEpochTimestamp();
        let result = if timestamp < i64::MAX.wrapping_add(epoch) {
            (timestamp - epoch) as f64 / 1_000_000.0
        } else {
            (timestamp as f64 - epoch as f64) / 1_000_000.0
        };
        Some(result)
    } else {
        ereport_unit_not_recognized(lowunits, tn);
    }
}

pub fn timestamp_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lowunits = lowunits_of(pg_getarg_text_bytes(fcinfo, 0));
    let timestamp = pg_getarg_timestamp(fcinfo, 1);
    match timestamp_part_float8(&lowunits, timestamp, false) {
        Some(r) => Float8GetDatum(r),
        None => {
            fcinfo.isnull = true;
            Datum(0)
        }
    }
}

pub fn timestamptz_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lowunits = lowunits_of(pg_getarg_text_bytes(fcinfo, 0));
    let timestamp = pg_getarg_timestamptz(fcinfo, 1);
    match timestamp_part_float8(&lowunits, timestamp, true) {
        Some(r) => Float8GetDatum(r),
        None => {
            fcinfo.isnull = true;
            Datum(0)
        }
    }
}

pub fn extract_timestamp(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("extract_timestamp (numeric result) needs int64_to_numeric / numeric_in machinery")
}
pub fn extract_timestamptz(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("extract_timestamptz (numeric result) needs int64_to_numeric / numeric_in machinery")
}

fn non_finite_interval_part(type_: i32, unit: i32, lowunits: &str, is_negative: bool) -> f64 {
    if type_ != UNITS && type_ != RESERV {
        ereport_unit_not_recognized(lowunits, "interval");
    }
    match unit {
        u if u == DTK_MICROSEC || u == DTK_MILLISEC || u == DTK_SECOND || u == DTK_MINUTE
            || u == DTK_WEEK || u == DTK_MONTH || u == DTK_QUARTER =>
        {
            0.0
        }
        u if u == DTK_HOUR || u == DTK_DAY || u == DTK_YEAR || u == DTK_DECADE
            || u == DTK_CENTURY || u == DTK_MILLENNIUM || u == DTK_EPOCH =>
        {
            if is_negative {
                -get_float8_infinity()
            } else {
                get_float8_infinity()
            }
        }
        _ => ereport_unit_not_supported(lowunits, "interval"),
    }
}

fn interval_part_float8(lowunits: &str, interval: &Interval) -> Option<f64> {
    let (mut type_, mut val) = DecodeUnits(lowunits);
    if type_ == UNKNOWN_FIELD {
        let (t, v) = DecodeSpecial(lowunits);
        type_ = t;
        val = v;
    }

    if INTERVAL_NOT_FINITE(interval) {
        let r = non_finite_interval_part(type_, val, lowunits, INTERVAL_IS_NOBEGIN(interval));
        return if r != 0.0 { Some(r) } else { None };
    }

    if type_ == UNITS {
        let mut tm = pg_itm::default();
        interval2itm(*interval, &mut tm);
        let intresult: i64 = match val {
            v if v == DTK_MICROSEC => i64::from(tm.sec) * 1_000_000 + i64::from(tm.usec),
            v if v == DTK_MILLISEC => return Some(f64::from(tm.sec) * 1000.0 + f64::from(tm.usec) / 1000.0),
            v if v == DTK_SECOND => return Some(f64::from(tm.sec) + f64::from(tm.usec) / 1_000_000.0),
            v if v == DTK_MINUTE => i64::from(tm.min),
            v if v == DTK_HOUR => tm.hour,
            v if v == DTK_DAY => i64::from(tm.mday),
            v if v == DTK_WEEK => i64::from(tm.mday / 7),
            v if v == DTK_MONTH => i64::from(tm.mon),
            v if v == DTK_QUARTER => {
                if interval.month >= 0 {
                    i64::from((tm.mon / 3) + 1)
                } else {
                    i64::from(-(((-interval.month % MONTHS_PER_YEAR) / 3) + 1))
                }
            }
            v if v == DTK_YEAR => i64::from(tm.year),
            v if v == DTK_DECADE => i64::from(tm.year / 10),
            v if v == DTK_CENTURY => i64::from(tm.year / 100),
            v if v == DTK_MILLENNIUM => i64::from(tm.year / 1000),
            _ => ereport_unit_not_supported(lowunits, "interval"),
        };
        Some(intresult as f64)
    } else if type_ == RESERV && val == DTK_EPOCH {
        let mut result = interval.time as f64 / 1_000_000.0;
        result += (DAYS_PER_YEAR * f64::from(SECS_PER_DAY)) * f64::from(interval.month / MONTHS_PER_YEAR);
        result += (f64::from(DAYS_PER_MONTH) * f64::from(SECS_PER_DAY))
            * f64::from(interval.month % MONTHS_PER_YEAR);
        result += f64::from(SECS_PER_DAY) * f64::from(interval.day);
        Some(result)
    } else {
        ereport_unit_not_recognized(lowunits, "interval");
    }
}

pub fn interval_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lowunits = lowunits_of(pg_getarg_text_bytes(fcinfo, 0));
    let interval = pg_getarg_interval(fcinfo, 1);
    match interval_part_float8(&lowunits, &interval) {
        Some(r) => Float8GetDatum(r),
        None => {
            fcinfo.isnull = true;
            Datum(0)
        }
    }
}

pub fn extract_interval(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("extract_interval (numeric result) needs int64_to_numeric / numeric machinery")
}

// ===========================================================================
//   AT TIME ZONE (timestamp.c 6321-6666). izone forms FULL; named zone STAGED.
// ===========================================================================

pub fn timestamp_zone(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamp_zone AT TIME ZONE named zone needs IANA tz DB (DecodeTimezoneName)")
}

/// PG `timestamp_izone`: interval (numeric offset) AT TIME ZONE. FULL.
pub fn timestamp_izone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = pg_getarg_interval(fcinfo, 0);
    let timestamp = pg_getarg_timestamp(fcinfo, 1);
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return TimestampTzGetDatum(timestamp);
    }
    if INTERVAL_NOT_FINITE(&zone) {
        ereport_izone_finite();
    }
    if zone.month != 0 || zone.day != 0 {
        ereport_izone_no_md();
    }
    let tz = (zone.time / USECS_PER_SEC) as i32;
    let result = dt2local(timestamp, tz);
    if !IS_VALID_TIMESTAMP(result) {
        ereport_out_of_range_ts();
    }
    TimestampTzGetDatum(result)
}

pub fn timestamptz_zone(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timestamptz_zone AT TIME ZONE named zone needs IANA tz DB (DecodeTimezoneName)")
}

/// PG `timestamptz_izone`: interval (numeric offset) AT TIME ZONE. FULL.
pub fn timestamptz_izone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = pg_getarg_interval(fcinfo, 0);
    let timestamp = pg_getarg_timestamptz(fcinfo, 1);
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return TimestampGetDatum(timestamp);
    }
    if INTERVAL_NOT_FINITE(&zone) {
        ereport_izone_finite();
    }
    if zone.month != 0 || zone.day != 0 {
        ereport_izone_no_md();
    }
    let tz = -((zone.time / USECS_PER_SEC) as i32);
    let result = dt2local(timestamp, tz);
    if !IS_VALID_TIMESTAMP(result) {
        ereport_out_of_range_ts();
    }
    TimestampGetDatum(result)
}

fn ereport_izone_finite() -> ! {
    ereport!(ERROR, |e: &mut ErrorData| {
        e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("interval time zone must be finite".to_string());
    });
    unreachable!()
}
fn ereport_izone_no_md() -> ! {
    ereport!(ERROR, |e: &mut ErrorData| {
        e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("interval time zone must not include months or days".to_string());
    });
    unreachable!()
}

pub fn timestamp_at_local(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    timestamp_timestamptz(fcinfo)
}
pub fn timestamptz_at_local(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    timestamptz_timestamp(fcinfo)
}

// ===========================================================================
//   timestamp <-> timestamptz conversion (timestamp.c 6431-6561). FULL (GMT).
// ===========================================================================

pub fn timestamp2timestamptz_opt_overflow(timestamp: Timestamp) -> (TimestampTz, i32) {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return (timestamp, 0);
    }
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    if timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, std::ptr::null_mut()) == 0 {
        // DetermineTimeZoneOffset is 0 without a tz DB -> identity rotation.
        let result = dt2local(timestamp, 0);
        if IS_VALID_TIMESTAMP(result) {
            return (result, 0);
        }
        let mut overflow_result = result;
        if result < MIN_TIMESTAMP {
            TIMESTAMP_NOBEGIN(&mut overflow_result);
            return (overflow_result, -1);
        }
        TIMESTAMP_NOEND(&mut overflow_result);
        return (overflow_result, 1);
    }
    ereport_out_of_range_ts();
}

fn timestamp2timestamptz(timestamp: Timestamp) -> TimestampTz {
    let (r, overflow) = timestamp2timestamptz_opt_overflow(timestamp);
    // opt_overflow with no-overflow caller: error out on out-of-range, but here
    // the GMT path never overflows for valid input; preserve the error contract.
    if overflow != 0 {
        ereport_out_of_range_ts();
    }
    r
}

pub fn timestamp_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    TimestampTzGetDatum(timestamp2timestamptz(timestamp))
}

fn timestamptz2timestamp(timestamp: TimestampTz) -> Timestamp {
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return timestamp;
    }
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tz = 0i32;
    if timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, std::ptr::null_mut()) != 0 {
        ereport_out_of_range_ts();
    }
    let mut result = 0;
    if tm2timestamp(&tm, fsec, None, &mut result) != 0 {
        ereport_out_of_range_ts();
    }
    result
}

pub fn timestamptz_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamptz(fcinfo, 0);
    TimestampGetDatum(timestamptz2timestamp(timestamp))
}

// generate_series SRFs -> STAGED (need ValuePerCall FuncCallContext).
pub fn generate_series_timestamp(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("generate_series_timestamp needs the SRF ValuePerCall FuncCallContext (SRF_RETURN_NEXT)")
}
pub fn generate_series_timestamptz(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("generate_series_timestamptz needs the SRF ValuePerCall FuncCallContext")
}
pub fn generate_series_timestamptz_at_zone(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("generate_series_timestamptz_at_zone needs the SRF FuncCallContext + IANA tz DB")
}
pub fn generate_series_timestamp_support(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("generate_series_timestamp_support needs the SupportRequestRows planner Node")
}

// ---------------------------------------------------------------------------
// Local helpers.
// ---------------------------------------------------------------------------

fn new_tm() -> pg_tm {
    pg_tm {
        sec: 0,
        min: 0,
        hour: 0,
        mday: 0,
        mon: 0,
        year: 0,
        wday: 0,
        yday: 0,
        isdst: 0,
        gmtoff: 0,
        zone: None,
    }
}

/// DateStyle GUC: default to ISO output (USE_ISO_DATES). Session-GUC plumbing
/// is not wired yet; ISO is PG's default and what the tests assume.
fn get_date_style() -> i32 {
    crate::miscadmin::USE_ISO_DATES
}

/// IntervalStyle GUC: default to postgres style (the PG default).
fn get_interval_style() -> i32 {
    crate::miscadmin::INTSTYLE_POSTGRES
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::NullableDatum;

    fn fc(args: &[Datum]) -> FunctionCallInfoBaseData {
        FunctionCallInfoBaseData {
            flinfo: None,
            context: None,
            resultinfo: None,
            fncollation: crate::postgres_ext::InvalidOid,
            isnull: false,
            nargs: args.len() as i16,
            args: args.iter().map(|&value| NullableDatum { value, isnull: false }).collect(),
        }
    }

    fn cstr_datum(s: &str) -> Datum {
        let c = std::ffi::CString::new(s).unwrap();
        CStringGetDatum(c.into_raw())
    }
    fn out_to_string(d: Datum) -> String {
        let p = DatumGetCString(d);
        let cstr = unsafe { core::ffi::CStr::from_ptr(p) };
        cstr.to_string_lossy().into_owned()
    }
    fn text_datum(s: &str) -> Datum {
        crate::postgres::PointerGetDatum(crate::backend::utils::adt::varlena::cstring_to_text(s).cast::<u8>())
    }
    fn interval_datum(s: &str) -> Datum {
        let mut f = fc(&[cstr_datum(s), Datum(0), Int32GetDatum(-1)]);
        interval_in(&mut f)
    }

    #[test]
    fn timestamp_in_out_roundtrip() {
        let mut f = fc(&[cstr_datum("2024-01-15 10:30:00"), Datum(0), Int32GetDatum(-1)]);
        let d = timestamp_in(&mut f);
        let mut f = fc(&[d]);
        assert_eq!(out_to_string(timestamp_out(&mut f)), "2024-01-15 10:30:00");
    }

    #[test]
    fn timestamp_in_out_microseconds() {
        let mut f = fc(&[cstr_datum("2024-01-15 10:30:00.123456"), Datum(0), Int32GetDatum(-1)]);
        let d = timestamp_in(&mut f);
        let mut f = fc(&[d]);
        assert_eq!(out_to_string(timestamp_out(&mut f)), "2024-01-15 10:30:00.123456");
    }

    fn ts(s: &str) -> Timestamp {
        let mut f = fc(&[cstr_datum(s), Datum(0), Int32GetDatum(-1)]);
        DatumGetTimestamp(timestamp_in(&mut f))
    }

    #[test]
    fn timestamp_minus_timestamp_gives_interval() {
        let a = ts("2024-01-16 10:30:00");
        let b = ts("2024-01-15 10:30:00");
        let mut f = fc(&[TimestampGetDatum(a), TimestampGetDatum(b)]);
        let span = unsafe { *DatumGetIntervalP(timestamp_mi(&mut f)) };
        // 1 day, justified into days.
        assert_eq!(span.day, 1);
        assert_eq!(span.time, 0);
        assert_eq!(span.month, 0);
    }

    #[test]
    fn timestamp_plus_interval() {
        let base = ts("2024-01-15 10:30:00");
        let iv = interval_datum("1 day");
        let mut f = fc(&[TimestampGetDatum(base), iv]);
        let r = DatumGetTimestamp(timestamp_pl_interval(&mut f));
        assert_eq!(r, ts("2024-01-16 10:30:00"));
    }

    #[test]
    fn date_trunc_day() {
        let base = ts("2024-01-15 10:30:45");
        let mut f = fc(&[text_datum("day"), TimestampGetDatum(base)]);
        let r = DatumGetTimestamp(timestamp_trunc(&mut f));
        assert_eq!(r, ts("2024-01-15 00:00:00"));
    }

    #[test]
    fn extract_parts() {
        let base = ts("2024-03-09 14:25:36");
        for (unit, want) in [("year", 2024.0), ("month", 3.0), ("day", 9.0), ("hour", 14.0)] {
            let mut f = fc(&[text_datum(unit), TimestampGetDatum(base)]);
            assert_eq!(DatumGetFloat8(timestamp_part(&mut f)), want, "unit {unit}");
        }
    }

    #[test]
    fn interval_in_out_roundtrip() {
        for s in ["1 day", "2 hours", "1 year 2 mons"] {
            let mut f = fc(&[cstr_datum(s), Datum(0), Int32GetDatum(-1)]);
            let d = interval_in(&mut f);
            let mut f = fc(&[d]);
            let back = out_to_string(interval_out(&mut f));
            // Reparse should be stable.
            let mut f2 = fc(&[cstr_datum(&back), Datum(0), Int32GetDatum(-1)]);
            let d2 = interval_in(&mut f2);
            let a = unsafe { *DatumGetIntervalP(d) };
            let b = unsafe { *DatumGetIntervalP(d2) };
            assert_eq!((a.month, a.day, a.time), (b.month, b.day, b.time), "interval {s}");
        }
    }

    #[test]
    fn interval_addition() {
        let a = interval_datum("1 hour");
        let b = interval_datum("2 hours");
        let mut f = fc(&[a, b]);
        let r = unsafe { *DatumGetIntervalP(interval_pl(&mut f)) };
        assert_eq!(r.time, 3 * USECS_PER_HOUR);
    }

    #[test]
    fn interval_justify_hours_cascades() {
        let iv = interval_datum("36 hours");
        let mut f = fc(&[iv]);
        let r = unsafe { *DatumGetIntervalP(interval_justify_hours(&mut f)) };
        assert_eq!(r.day, 1);
        assert_eq!(r.time, 12 * USECS_PER_HOUR);
    }

    #[test]
    fn interval_cmp_ordering() {
        let a = interval_datum("1 day");
        let b = interval_datum("25 hours");
        // 1 day (24h) < 25 hours.
        let mut f = fc(&[a, b]);
        assert!(DatumGetInt32(interval_cmp(&mut f)) < 0);
    }

    #[test]
    fn timestamp_comparison_suite() {
        let a = ts("2024-01-15 10:30:00");
        let b = ts("2024-01-16 10:30:00");
        let mut f = fc(&[TimestampGetDatum(a), TimestampGetDatum(b)]);
        assert!(DatumGetBool(timestamp_lt(&mut f)));
        let mut f = fc(&[TimestampGetDatum(a), TimestampGetDatum(b)]);
        assert!(!DatumGetBool(timestamp_ge(&mut f)));
        let mut f = fc(&[TimestampGetDatum(a), TimestampGetDatum(a)]);
        assert!(DatumGetBool(timestamp_eq(&mut f)));
    }
}
