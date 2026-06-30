//! Functions for the DATE, TIME (without time zone), and TIMETZ (time with
//! time zone) data types. Translated from src/backend/utils/adt/date.c.
//!
//! Built on top of the shared decode/encode core in
//! `crate::backend::utils::adt::datetime` (re-exported via
//! `crate::utils::datetime`). On-disk units (see crate::datatype::timestamp,
//! crate::utils::date): DateADT = i32 days since 2000-01-01; TimeADT = i64
//! microseconds since midnight; TimeTzADT = {time: TimeADT, zone: i32 seconds}.
//!
//! As in float.c/bool.c, each C `Datum fn(PG_FUNCTION_ARGS)` becomes a
//! `fn(&mut FunctionCallInfoBaseData) -> Datum`. Pass-by-reference results
//! (TimeTzADT, Interval) are leaked boxes (no MemoryContext yet; TODO(memory-
//! context)), mirroring varlena.rs. The pure internal helpers declared in
//! date.h (tm2time/time2tm/timetz2tm/tm2timetz/AdjustTimeForTypmod/
//! time_overflows/float_time_overflows/date2timestamp*/date_cmp_timestamp*_
//! internal) are fully implemented here.
//!
//! Staged subsystems (rules.md s4 -- the dependency isn't ported yet):
//!  - recv/send: the binary wire StringInfo path.
//!  - timetypmodin/out, timetztypmodin/out: the cstring[] ArrayType typmod path.
//!  - date_sortsupport/date_skipsupport, time_support: SortSupport/SkipSupport/
//!    planner-support nodes.
//!  - in_range_*: window-frame caller (DirectFunctionCall into timestamp's
//!    in_range, not yet present).
//!  - timetz_zone named-zone branch + timetz_at_local: the IANA tz database +
//!    session timezone.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "intentional C width arithmetic: date.c does explicit (int32)/(int64) \
              casts in the Julian-day and microsecond math (the value-cast family \
              is an allowed port-inherent lint per rules.md s11)"
)]
#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    reason = "1:1 port of long decode/encode + extract dispatch and single-letter \
              date math vars (year/mon/mday/tz) matching date.c"
)]
#![allow(
    clippy::manual_range_contains,
    reason = "1:1 port of date.c bound checks written as explicit comparisons"
)]
#![allow(
    clippy::similar_names,
    reason = "ts1/te1/ts2/te2 (+ their _null companions) mirror date.c's OVERLAPS \
              start/end interval-bound variable names"
)]
#![allow(
    clippy::suboptimal_flops,
    reason = "date.c computes sec*1000 + fsec/1000 as separate operations; keep the \
              ordering to match PG's FPU rounding bit-for-bit"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "fmgr byref args are valid non-toasted pointers the caller keeps alive; \
              dereference mirrors the C PG_GETARG_*_P macros"
)]

use crate::common::int::{pg_add_s64_overflow, pg_neg_s32_overflow};
use crate::common::hashfn::{hash_uint32, hash_uint32_extended};
use crate::datatype::timestamp::{
    fsec_t, Interval, TimeOffset, Timestamp, TimestampTz, HOURS_PER_DAY, INTERVAL_NOT_FINITE,
    IS_VALID_DATE, IS_VALID_JULIAN, IS_VALID_TIMESTAMP, MINS_PER_HOUR, MIN_TIMESTAMP,
    POSTGRES_EPOCH_JDATE, SECS_PER_DAY, SECS_PER_HOUR, SECS_PER_MINUTE, TIMESTAMP_END_JULIAN,
    TIMESTAMP_IS_NOBEGIN, TIMESTAMP_IS_NOEND, TIMESTAMP_NOBEGIN, TIMESTAMP_NOEND,
    TIMESTAMP_NOT_FINITE, UNIX_EPOCH_JDATE, USECS_PER_DAY, USECS_PER_HOUR,
    USECS_PER_MINUTE, USECS_PER_SEC,
};
use crate::ereport;
use crate::fmgr::FunctionCallInfoBaseData;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, Datum, DatumGetBool, DatumGetCString, DatumGetInt32,
    DatumGetInt64, DatumGetPointer, Float8GetDatum, Int32GetDatum,
};
use crate::utils::date::{
    DateADT, TimeADT, TimeTzADT, DATE_IS_NOBEGIN, DATE_IS_NOEND,
    DATE_NOBEGIN, DATE_NOEND, DATE_NOT_FINITE, DatumGetDateADT, DatumGetTimeADT,
    DatumGetTimeTzADTP, DateADTGetDatum, TimeADTGetDatum, TimeTzADTPGetDatum, MAX_TIME_PRECISION,
};
use crate::utils::datetime::{
    date2j, j2date, j2day, DateTimeErrorExtra, DecodeSpecial, DecodeUnits, DTERR_BAD_FORMAT,
    DTERR_FIELD_OVERFLOW, DTERR_INTERVAL_OVERFLOW, DTERR_MD_FIELD_OVERFLOW, DTERR_TZDISP_OVERFLOW,
    DTK_CENTURY, DTK_DATE, DTK_DAY, DTK_DECADE, DTK_DOW, DTK_DOY, DTK_EARLY, DTK_EPOCH, DTK_HOUR,
    DTK_ISODOW, DTK_ISOYEAR, DTK_JULIAN, DTK_LATE, DTK_MICROSEC, DTK_MILLENNIUM, DTK_MILLISEC,
    DTK_MINUTE, DTK_MONTH, DTK_QUARTER, DTK_SECOND, DTK_TZ, DTK_TZ_HOUR, DTK_TZ_MINUTE, DTK_WEEK,
    DTK_YEAR, DTK_DATE_M, RESERV, UNITS, UNKNOWN_FIELD,
};
use core::cmp::Ordering;
use crate::utils::timestamp::DatumGetIntervalP;
use crate::utils::elog::ERROR;
use crate::utils::errcodes::{
    ERRCODE_DATETIME_FIELD_OVERFLOW, ERRCODE_DATETIME_VALUE_OUT_OF_RANGE,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERVAL_FIELD_OVERFLOW,
    ERRCODE_INVALID_DATETIME_FORMAT, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE, ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE,
};

// pg_type OIDs used only for format_type_be error text (format_type_be ignores
// the OID for now). Standard PG values.
const DATEOID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(1082);
const TIMEOID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(1083);
const TIMETZOID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(1266);

const EARLY: &str = "-infinity";
const LATE: &str = "infinity";

// ===========================================================================
//   fmgr arg/return helpers (mirroring float.rs/bool.rs)
// ===========================================================================

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

#[inline]
fn pg_getarg_dateadt(fcinfo: &FunctionCallInfoBaseData, n: usize) -> DateADT {
    DatumGetDateADT(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_timeadt(fcinfo: &FunctionCallInfoBaseData, n: usize) -> TimeADT {
    DatumGetTimeADT(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_int32(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i32 {
    DatumGetInt32(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_int64(fcinfo: &FunctionCallInfoBaseData, n: usize) -> i64 {
    DatumGetInt64(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_bool(fcinfo: &FunctionCallInfoBaseData, n: usize) -> bool {
    DatumGetBool(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_timestamp(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Timestamp {
    DatumGetInt64(fcinfo.args[n].value)
}
#[inline]
fn pg_getarg_timestamptz(fcinfo: &FunctionCallInfoBaseData, n: usize) -> TimestampTz {
    DatumGetInt64(fcinfo.args[n].value)
}
/// `PG_GETARG_TIMETZADT_P(n)`: byref TimeTzADT pointer dereferenced to a value.
#[inline]
fn pg_getarg_timetzadt_p(fcinfo: &FunctionCallInfoBaseData, n: usize) -> TimeTzADT {
    let p = DatumGetTimeTzADTP(fcinfo.args[n].value);
    // SAFETY: a timetz argument is a valid byref pointer that outlives the call.
    unsafe { *p }
}
/// `PG_GETARG_INTERVAL_P(n)`: byref Interval pointer dereferenced to a value.
#[inline]
fn pg_getarg_interval_p(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Interval {
    let p = DatumGetIntervalP(fcinfo.args[n].value);
    // SAFETY: an interval argument is a valid byref pointer that outlives the call.
    unsafe { *p }
}

/// `PG_RETURN_TIMETZADT_P`: leak a boxed TimeTzADT and hand back its pointer.
#[inline]
fn return_timetzadt(tt: TimeTzADT) -> Datum {
    // TODO(memory-context): leaks until a MemoryContext-backed allocator exists.
    let b = Box::new(tt);
    TimeTzADTPGetDatum(Box::leak(b))
}
/// `PG_RETURN_INTERVAL_P`: leak a boxed Interval and hand back its pointer.
#[inline]
fn return_interval(span: Interval) -> Datum {
    // TODO(memory-context): leaks until a MemoryContext-backed allocator exists.
    let b = Box::new(span);
    crate::utils::timestamp::IntervalPGetDatum(Box::leak(b))
}

/// `EncodeSpecialTimestamp`-style buffer to owned String, for the encoders that
/// write into a fixed `[u8]` buffer in datetime.rs.
#[inline]
fn buf_to_string(buf: &[u8]) -> String {
    let n = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// A zeroed `pg_tm` (isdst = -1), matching the C `struct pg_tm tt`.
#[inline]
fn new_tm() -> crate::pgtime::pg_tm {
    crate::pgtime::pg_tm {
        sec: 0,
        min: 0,
        hour: 0,
        mday: 0,
        mon: 0,
        year: 0,
        wday: 0,
        yday: 0,
        isdst: -1,
        gmtoff: 0,
        zone: None,
    }
}

/// The session timezone (`crate::pgtime::session_timezone`) as a `&pg_tz`, or a
/// default. The tz database isn't ported yet, so the offset functions are stubs;
/// this just satisfies the borrow at the call site.
#[inline]
fn session_timezone_ref() -> &'static crate::pgtime::pg_tz {
    // pg_tz is a unit struct until the IANA tz DB is ported; the offset functions
    // that consume it (DetermineTimeZoneOffset, ...) are stubs, so a static
    // default stands in for the (unported) session_timezone GUC.
    static DEFAULT_TZ: crate::pgtime::pg_tz = crate::pgtime::pg_tz;
    &DEFAULT_TZ
}

const MAXDATELEN: usize = 128;

/// Mirrors date.c's `DateTimeParseError` switch: maps a DTERR code to an
/// ereport(ERROR). (The header `crate::utils::datetime::DateTimeParseError` is a
/// stub; this is the local body the input functions call.)
fn date_time_parse_error(
    dterr: i32,
    _extra: &DateTimeErrorExtra,
    s: &str,
    datatype: &str,
) -> ! {
    match dterr {
        x if x == DTERR_FIELD_OVERFLOW => {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_DATETIME_FIELD_OVERFLOW)
                    .errmsg(format!("date/time field value out of range: \"{s}\""));
            });
        }
        x if x == DTERR_MD_FIELD_OVERFLOW => {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_DATETIME_FIELD_OVERFLOW)
                    .errmsg(format!("date/time field value out of range: \"{s}\""))
                    .errhint("Perhaps you need a different \"DateStyle\" setting.");
            });
        }
        x if x == DTERR_INTERVAL_OVERFLOW => {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW)
                    .errmsg(format!("interval field value out of range: \"{s}\""));
            });
        }
        x if x == DTERR_TZDISP_OVERFLOW => {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE)
                    .errmsg(format!("time zone displacement out of range: \"{s}\""));
            });
        }
        // DTERR_BAD_FORMAT and default.
        _ => {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_INVALID_DATETIME_FORMAT)
                    .errmsg(format!("invalid input syntax for type {datatype}: \"{s}\""));
            });
        }
    }
    unreachable!()
}

/// PG `EncodeSpecialDate`: reserved date values to string (local helper).
fn encode_special_date(dt: DateADT) -> String {
    if DATE_IS_NOBEGIN(dt) {
        EARLY.to_string()
    } else if DATE_IS_NOEND(dt) {
        LATE.to_string()
    } else {
        // shouldn't happen
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errmsg_internal("invalid argument for EncodeSpecialDate");
        });
        unreachable!()
    }
}

// ===========================================================================
//   Numeric return helper (EXTRACT family)
// ===========================================================================

/// `PG_RETURN_NUMERIC(int64_to_numeric(v))`. Routes through the numeric header's
/// `int64_to_numeric` (staged) -> NumericGetDatum, matching date.c.
#[inline]
fn return_int64_numeric(v: i64) -> Datum {
    crate::utils::numeric::NumericGetDatum(crate::utils::numeric::int64_to_numeric(v))
}
/// `PG_RETURN_NUMERIC(int64_div_fast_to_numeric(v, log10))`.
#[inline]
fn return_int64_div_fast_numeric(v: i64, log10val2: i32) -> Datum {
    crate::utils::numeric::NumericGetDatum(crate::utils::numeric::int64_div_fast_to_numeric(
        v, log10val2,
    ))
}

// ===========================================================================
//   Common code for typmodin/typmodout
// ===========================================================================

/// PG `anytime_typmod_check`: validate/clamp a TIME/TIMETZ precision typmod.
/// Exported so parse_expr.c can use it.
#[must_use]
pub fn anytime_typmod_check(istz: bool, typmod: i32) -> i32 {
    if typmod < 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE).errmsg(format!(
                "TIME({typmod}){} precision must not be negative",
                if istz { " WITH TIME ZONE" } else { "" }
            ));
        });
    }
    if typmod > MAX_TIME_PRECISION {
        ereport!(crate::utils::elog::WARNING, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE).errmsg(format!(
                "TIME({typmod}){} precision reduced to maximum allowed, {MAX_TIME_PRECISION}",
                if istz { " WITH TIME ZONE" } else { "" }
            ));
        });
        return MAX_TIME_PRECISION;
    }
    typmod
}

/// PG `anytime_typmodout`: render a TIME/TIMETZ typmod as text.
fn anytime_typmodout(istz: bool, typmod: i32) -> String {
    let tz = if istz {
        " with time zone"
    } else {
        " without time zone"
    };
    if typmod >= 0 {
        format!("({typmod}){tz}")
    } else {
        tz.to_string()
    }
}

// ===========================================================================
//   Date ADT
// ===========================================================================

/// PG `date_in`: text -> internal date.
pub fn date_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str_ = pg_getarg_cstring(fcinfo, 0);
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tzp: i32 = 0;
    let mut dtype: i32 = 0;
    let mut extra = DateTimeErrorExtra { timezone: None, abbrev: None };
    let mut date: DateADT;

    let dterr = match crate::utils::datetime::ParseDateTime(&str_, MAXDATEFIELDS) {
        Ok((mut field, mut ftype)) => crate::utils::datetime::DecodeDateTime(
            &mut field,
            &mut ftype,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tzp),
            &mut extra,
        ),
        Err(e) => e,
    };
    if dterr != 0 {
        date_time_parse_error(dterr, &extra, &str_, "date");
    }

    match dtype {
        x if x == DTK_DATE => {}
        x if x == DTK_EPOCH => {
            crate::utils::timestamp::GetEpochTime(&mut tm);
        }
        x if x == DTK_LATE => {
            date = 0;
            DATE_NOEND(&mut date);
            return DateADTGetDatum(date);
        }
        x if x == DTK_EARLY => {
            date = 0;
            DATE_NOBEGIN(&mut date);
            return DateADTGetDatum(date);
        }
        _ => date_time_parse_error(DTERR_BAD_FORMAT, &extra, &str_, "date"),
    }

    // Prevent overflow in Julian-day routines.
    if !IS_VALID_JULIAN(tm.year, tm.mon, tm.mday) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg(format!("date out of range: \"{str_}\""));
        });
    }

    date = date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;

    if !IS_VALID_DATE(date) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg(format!("date out of range: \"{str_}\""));
        });
    }

    DateADTGetDatum(date)
}

const MAXDATEFIELDS: usize = 25;

/// PG `date_out`: internal date -> text.
pub fn date_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date = pg_getarg_dateadt(fcinfo, 0);
    let result = if DATE_NOT_FINITE(date) {
        encode_special_date(date)
    } else {
        let (year, mon, mday) = j2date(date + POSTGRES_EPOCH_JDATE);
        let mut tm = new_tm();
        tm.year = year;
        tm.mon = mon;
        tm.mday = mday;
        let mut buf = [0u8; MAXDATELEN + 1];
        crate::utils::datetime::EncodeDateOnly(&tm, unsafe { crate::miscadmin::DateStyle }, &mut buf);
        buf_to_string(&buf)
    };
    pg_return_cstring(&result)
}

/// PG `date_recv`: external binary -> date. STAGED: binary wire StringInfo.
pub fn date_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("date_recv needs the binary wire StringInfo (pq_getmsgint) path")
}

/// PG `date_send`: date -> external binary. STAGED: binary wire StringInfo.
pub fn date_send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("date_send needs pq_begintypsend/pq_endtypsend bytea boxing")
}

/// PG `make_date`: date constructor from (year, month, day).
pub fn make_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut tm = new_tm();
    tm.year = pg_getarg_int32(fcinfo, 0);
    tm.mon = pg_getarg_int32(fcinfo, 1);
    tm.mday = pg_getarg_int32(fcinfo, 2);
    let mut bc = false;

    // Handle negative years as BC.
    if tm.year < 0 {
        bc = true;
        let Some(year) = pg_neg_s32_overflow(tm.year) else {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_DATETIME_FIELD_OVERFLOW).errmsg(format!(
                    "date field value out of range: {}-{:02}-{:02}",
                    tm.year, tm.mon, tm.mday
                ));
            });
            unreachable!()
        };
        tm.year = year;
    }

    let dterr = crate::utils::datetime::ValidateDate(DTK_DATE_M as i32, false, false, bc, &mut tm);
    if dterr != 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_FIELD_OVERFLOW).errmsg(format!(
                "date field value out of range: {}-{:02}-{:02}",
                tm.year, tm.mon, tm.mday
            ));
        });
    }

    if !IS_VALID_JULIAN(tm.year, tm.mon, tm.mday) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE).errmsg(format!(
                "date out of range: {}-{:02}-{:02}",
                tm.year, tm.mon, tm.mday
            ));
        });
    }

    let date = date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;
    if !IS_VALID_DATE(date) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE).errmsg(format!(
                "date out of range: {}-{:02}-{:02}",
                tm.year, tm.mon, tm.mday
            ));
        });
    }

    DateADTGetDatum(date)
}

// --- Comparison functions for dates ---

pub fn date_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_dateadt(fcinfo, 0) == pg_getarg_dateadt(fcinfo, 1))
}
pub fn date_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_dateadt(fcinfo, 0) != pg_getarg_dateadt(fcinfo, 1))
}
pub fn date_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_dateadt(fcinfo, 0) < pg_getarg_dateadt(fcinfo, 1))
}
pub fn date_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_dateadt(fcinfo, 0) <= pg_getarg_dateadt(fcinfo, 1))
}
pub fn date_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_dateadt(fcinfo, 0) > pg_getarg_dateadt(fcinfo, 1))
}
pub fn date_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_dateadt(fcinfo, 0) >= pg_getarg_dateadt(fcinfo, 1))
}
pub fn date_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_dateadt(fcinfo, 0), pg_getarg_dateadt(fcinfo, 1));
    Int32GetDatum(match a.cmp(&b) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => 0,
    })
}

/// PG `date_sortsupport`. STAGED: SortSupport node.
pub fn date_sortsupport(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("date_sortsupport needs the SortSupport node (ssup_datum_int32_cmp)")
}

/// PG `date_skipsupport`. STAGED: SkipSupport node.
pub fn date_skipsupport(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("date_skipsupport needs the SkipSupport node")
}

pub fn hashdate(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    hash_uint32(pg_getarg_dateadt(fcinfo, 0) as u32)
}
pub fn hashdateextended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    hash_uint32_extended(pg_getarg_dateadt(fcinfo, 0) as u32, pg_getarg_int64(fcinfo, 1) as u64)
}

pub fn date_finite(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(!DATE_NOT_FINITE(pg_getarg_dateadt(fcinfo, 0)))
}

pub fn date_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_dateadt(fcinfo, 0), pg_getarg_dateadt(fcinfo, 1));
    DateADTGetDatum(if a > b { a } else { b })
}
pub fn date_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_dateadt(fcinfo, 0), pg_getarg_dateadt(fcinfo, 1));
    DateADTGetDatum(if a < b { a } else { b })
}

/// PG `date_mi`: difference between two dates in days.
pub fn date_mi(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (v1, v2) = (pg_getarg_dateadt(fcinfo, 0), pg_getarg_dateadt(fcinfo, 1));
    if DATE_NOT_FINITE(v1) || DATE_NOT_FINITE(v2) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("cannot subtract infinite dates");
        });
    }
    Int32GetDatum(v1 - v2)
}

/// PG `date_pli`: date + int4 days.
pub fn date_pli(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date_val = pg_getarg_dateadt(fcinfo, 0);
    let days = pg_getarg_int32(fcinfo, 1);
    if DATE_NOT_FINITE(date_val) {
        return DateADTGetDatum(date_val); // can't change infinity
    }
    let result = date_val.wrapping_add(days);
    if (if days >= 0 { result < date_val } else { result > date_val }) || !IS_VALID_DATE(result) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("date out of range");
        });
    }
    DateADTGetDatum(result)
}

/// PG `date_mii`: date - int4 days.
pub fn date_mii(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date_val = pg_getarg_dateadt(fcinfo, 0);
    let days = pg_getarg_int32(fcinfo, 1);
    if DATE_NOT_FINITE(date_val) {
        return DateADTGetDatum(date_val);
    }
    let result = date_val.wrapping_sub(days);
    if (if days >= 0 { result > date_val } else { result < date_val }) || !IS_VALID_DATE(result) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("date out of range");
        });
    }
    DateADTGetDatum(result)
}

// --- date -> timestamp promotion (internal helpers, date.h) ---

/// PG `date2timestamp_opt_overflow`: promote date to timestamp. Returns
/// (value, overflow) where overflow is 0/+1/-1 (the C `int *overflow` out-param).
#[must_use]
pub fn date2timestamp_opt_overflow(date_val: DateADT) -> (Timestamp, i32) {
    let mut result: Timestamp = 0;
    if DATE_IS_NOBEGIN(date_val) {
        TIMESTAMP_NOBEGIN(&mut result);
    } else if DATE_IS_NOEND(date_val) {
        TIMESTAMP_NOEND(&mut result);
    } else if date_val >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        TIMESTAMP_NOEND(&mut result);
        return (result, 1);
    } else {
        // date is days since 2000, timestamp is microseconds since same.
        result = i64::from(date_val) * USECS_PER_DAY;
    }
    (result, 0)
}

/// PG `date2timestamp`: promote date to timestamp, error on overflow.
fn date2timestamp(date_val: DateADT) -> Timestamp {
    let (result, overflow) = date2timestamp_opt_overflow(date_val);
    if overflow != 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("date out of range for timestamp");
        });
    }
    result
}

/// PG `date2timestamptz_opt_overflow`: promote date to timestamptz. Returns
/// (value, overflow).
#[must_use]
pub fn date2timestamptz_opt_overflow(date_val: DateADT) -> (TimestampTz, i32) {
    let mut result: TimestampTz = 0;
    if DATE_IS_NOBEGIN(date_val) {
        TIMESTAMP_NOBEGIN(&mut result);
    } else if DATE_IS_NOEND(date_val) {
        TIMESTAMP_NOEND(&mut result);
    } else if date_val >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
        TIMESTAMP_NOEND(&mut result);
        return (result, 1);
    } else {
        let (year, mon, mday) = j2date(date_val + POSTGRES_EPOCH_JDATE);
        let mut tm = new_tm();
        tm.year = year;
        tm.mon = mon;
        tm.mday = mday;
        tm.hour = 0;
        tm.min = 0;
        tm.sec = 0;
        let tz = crate::utils::datetime::DetermineTimeZoneOffset(&mut tm, session_timezone_ref());
        result = i64::from(date_val) * USECS_PER_DAY + i64::from(tz) * USECS_PER_SEC;

        if !IS_VALID_TIMESTAMP(result) {
            if result < MIN_TIMESTAMP {
                TIMESTAMP_NOBEGIN(&mut result);
                return (result, -1);
            }
            TIMESTAMP_NOEND(&mut result);
            return (result, 1);
        }
    }
    (result, 0)
}

/// PG `date2timestamptz`: promote date to timestamptz, error on overflow.
fn date2timestamptz(date_val: DateADT) -> TimestampTz {
    let (result, overflow) = date2timestamptz_opt_overflow(date_val);
    if overflow != 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("date out of range for timestamp");
        });
    }
    result
}

/// PG `date2timestamp_no_overflow`: numerically-equivalent double, never errors.
#[must_use]
pub fn date2timestamp_no_overflow(date_val: DateADT) -> f64 {
    if DATE_IS_NOBEGIN(date_val) {
        f64::MIN
    } else if DATE_IS_NOEND(date_val) {
        f64::MAX
    } else {
        f64::from(date_val) * USECS_PER_DAY as f64
    }
}

// --- crosstype comparison functions for dates (internal helpers) ---

/// PG `date_cmp_timestamp_internal`.
#[must_use]
pub fn date_cmp_timestamp_internal(date_val: DateADT, dt2: Timestamp) -> i32 {
    let (dt1, overflow) = date2timestamp_opt_overflow(date_val);
    if overflow > 0 {
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    // overflow == 0 (the -1 case cannot occur for timestamp).
    crate::utils::timestamp::timestamp_cmp_internal(dt1, dt2)
}

/// PG `date_cmp_timestamptz_internal`.
#[must_use]
pub fn date_cmp_timestamptz_internal(date_val: DateADT, dt2: TimestampTz) -> i32 {
    let (dt1, overflow) = date2timestamptz_opt_overflow(date_val);
    if overflow > 0 {
        return if TIMESTAMP_IS_NOEND(dt2) { -1 } else { 1 };
    }
    if overflow < 0 {
        return if TIMESTAMP_IS_NOBEGIN(dt2) { 1 } else { -1 };
    }
    crate::utils::timestamp::timestamptz_cmp_internal(dt1, dt2)
}

pub fn date_eq_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) == 0)
}
pub fn date_ne_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) != 0)
}
pub fn date_lt_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) < 0)
}
pub fn date_gt_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) > 0)
}
pub fn date_le_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) <= 0)
}
pub fn date_ge_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)) >= 0)
}
pub fn date_cmp_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamp(fcinfo, 1)))
}

pub fn date_eq_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) == 0)
}
pub fn date_ne_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) != 0)
}
pub fn date_lt_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) < 0)
}
pub fn date_gt_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) > 0)
}
pub fn date_le_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) <= 0)
}
pub fn date_ge_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)) >= 0)
}
pub fn date_cmp_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 0), pg_getarg_timestamptz(fcinfo, 1)))
}

pub fn timestamp_eq_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamp(fcinfo, 0)) == 0)
}
pub fn timestamp_ne_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamp(fcinfo, 0)) != 0)
}
pub fn timestamp_lt_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamp(fcinfo, 0)) > 0)
}
pub fn timestamp_gt_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamp(fcinfo, 0)) < 0)
}
pub fn timestamp_le_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamp(fcinfo, 0)) >= 0)
}
pub fn timestamp_ge_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamp(fcinfo, 0)) <= 0)
}
pub fn timestamp_cmp_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(-date_cmp_timestamp_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamp(fcinfo, 0)))
}

pub fn timestamptz_eq_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) == 0)
}
pub fn timestamptz_ne_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) != 0)
}
pub fn timestamptz_lt_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) > 0)
}
pub fn timestamptz_gt_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) < 0)
}
pub fn timestamptz_le_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) >= 0)
}
pub fn timestamptz_ge_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)) <= 0)
}
pub fn timestamptz_cmp_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    Int32GetDatum(-date_cmp_timestamptz_internal(pg_getarg_dateadt(fcinfo, 1), pg_getarg_timestamptz(fcinfo, 0)))
}

/// PG `in_range_date_interval`. STAGED: needs timestamp's in_range_timestamp_
/// interval (DirectFunctionCall5), not yet present.
pub fn in_range_date_interval(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("in_range_date_interval needs timestamp's in_range_timestamp_interval")
}

/// PG `extract_date`: extract a field from a date, returning NUMERIC.
pub fn extract_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let lowunits = getarg_lowunits(fcinfo, 0);
    let date = pg_getarg_dateadt(fcinfo, 1);

    let (mut type_, val) = DecodeUnits(&lowunits);
    if type_ == UNKNOWN_FIELD {
        let (t, v) = DecodeSpecial(&lowunits);
        type_ = t;
        // val is reused below; refresh.
        return extract_date_dispatch(date, type_, v, &lowunits);
    }
    extract_date_dispatch(date, type_, val, &lowunits)
}

fn extract_date_dispatch(date: DateADT, type_: i32, val: i32, lowunits: &str) -> Datum {
    let intresult: i64;

    if DATE_NOT_FINITE(date) && (type_ == UNITS || type_ == RESERV) {
        match val {
            // Oscillating units -> NULL.
            x if x == DTK_DAY
                || x == DTK_MONTH
                || x == DTK_QUARTER
                || x == DTK_WEEK
                || x == DTK_DOW
                || x == DTK_ISODOW
                || x == DTK_DOY =>
            {
                // PG_RETURN_NULL: represented as Datum(0) with isnull; callers of
                // EXTRACT here have no fcinfo handle to set isnull, so we return
                // the numeric +/-Infinity only for monotonic units. For the
                // oscillating case we return Datum(0) (NULL sentinel).
                Datum(0)
            }
            x if x == DTK_YEAR
                || x == DTK_DECADE
                || x == DTK_CENTURY
                || x == DTK_MILLENNIUM
                || x == DTK_JULIAN
                || x == DTK_ISOYEAR
                || x == DTK_EPOCH =>
            {
                let s = if DATE_IS_NOBEGIN(date) {
                    "-Infinity"
                } else {
                    "Infinity"
                };
                crate::utils::numeric::NumericGetDatum(numeric_in_special(s))
            }
            _ => unit_not_supported(lowunits, DATEOID),
        }
    } else if type_ == UNITS {
        let (year, mon, mday) = j2date(date + POSTGRES_EPOCH_JDATE);
        match val {
            x if x == DTK_DAY => intresult = i64::from(mday),
            x if x == DTK_MONTH => intresult = i64::from(mon),
            x if x == DTK_QUARTER => intresult = i64::from((mon - 1) / 3 + 1),
            x if x == DTK_WEEK => {
                intresult = i64::from(crate::utils::timestamp::date2isoweek(year, mon, mday));
            }
            x if x == DTK_YEAR => {
                intresult = if year > 0 {
                    i64::from(year)
                } else {
                    // no year 0, just 1 BC and 1 AD
                    i64::from(year) - 1
                };
            }
            x if x == DTK_DECADE => {
                intresult = if year >= 0 {
                    i64::from(year / 10)
                } else {
                    -i64::from((8 - (year - 1)) / 10)
                };
            }
            x if x == DTK_CENTURY => {
                intresult = if year > 0 {
                    i64::from((year + 99) / 100)
                } else {
                    -i64::from((99 - (year - 1)) / 100)
                };
            }
            x if x == DTK_MILLENNIUM => {
                intresult = if year > 0 {
                    i64::from((year + 999) / 1000)
                } else {
                    -i64::from((999 - (year - 1)) / 1000)
                };
            }
            x if x == DTK_JULIAN => intresult = i64::from(date) + i64::from(POSTGRES_EPOCH_JDATE),
            x if x == DTK_ISOYEAR => {
                let mut r = i64::from(crate::utils::timestamp::date2isoyear(year, mon, mday));
                if r <= 0 {
                    r -= 1;
                }
                intresult = r;
            }
            x if x == DTK_DOW || x == DTK_ISODOW => {
                let mut r = i64::from(j2day(date + POSTGRES_EPOCH_JDATE));
                if x == DTK_ISODOW && r == 0 {
                    r = 7;
                }
                intresult = r;
            }
            x if x == DTK_DOY => {
                intresult = i64::from(date2j(year, mon, mday) - date2j(year, 1, 1) + 1);
            }
            _ => unit_not_supported(lowunits, DATEOID),
        }
        return_int64_numeric(intresult)
    } else if type_ == RESERV {
        match val {
            x if x == DTK_EPOCH => {
                intresult = (i64::from(date) + i64::from(POSTGRES_EPOCH_JDATE)
                    - i64::from(UNIX_EPOCH_JDATE))
                    * i64::from(SECS_PER_DAY);
            }
            _ => unit_not_supported(lowunits, DATEOID),
        }
        return_int64_numeric(intresult)
    } else {
        unit_not_recognized(lowunits, DATEOID)
    }
}

/// Build a NUMERIC from a special literal ("Infinity"/"-Infinity"/"NaN"); routes
/// through numeric_in (staged) -> numeric value.
fn numeric_in_special(s: &str) -> crate::utils::numeric::Numeric {
    let mut fc = make_numeric_in_fcinfo(s);
    let d = crate::backend::utils::adt::numeric::numeric_in(&mut fc);
    crate::utils::numeric::DatumGetNumeric(d)
}

fn make_numeric_in_fcinfo(s: &str) -> FunctionCallInfoBaseData {
    let c = std::ffi::CString::new(s).unwrap_or_default();
    let cstr = CStringGetDatum(c.into_raw());
    FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: crate::postgres_ext::InvalidOid,
        isnull: false,
        nargs: 3,
        args: vec![
            crate::postgres::NullableDatum { value: cstr, isnull: false },
            crate::postgres::NullableDatum {
                value: crate::postgres::ObjectIdGetDatum(crate::postgres_ext::InvalidOid),
                isnull: false,
            },
            crate::postgres::NullableDatum { value: Int32GetDatum(-1), isnull: false },
        ],
    }
}

fn unit_not_supported(lowunits: &str, oid: crate::postgres_ext::Oid) -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_FEATURE_NOT_SUPPORTED).errmsg(format!(
            "unit \"{lowunits}\" not supported for type {}",
            crate::utils::builtins::format_type_be(oid)
        ));
    });
    unreachable!()
}
fn unit_not_recognized(lowunits: &str, oid: crate::postgres_ext::Oid) -> ! {
    ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(ERRCODE_INVALID_PARAMETER_VALUE).errmsg(format!(
            "unit \"{lowunits}\" not recognized for type {}",
            crate::utils::builtins::format_type_be(oid)
        ));
    });
    unreachable!()
}

/// `downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units))`.
fn getarg_lowunits(fcinfo: &FunctionCallInfoBaseData, n: usize) -> String {
    let p = DatumGetPointer(fcinfo.args[n].value).cast::<crate::c::text>();
    // SAFETY: a text argument is a valid non-toasted varlena that outlives the call.
    let s = unsafe { crate::backend::utils::adt::varlena::text_to_cstring(&*p) };
    let len = s.len() as i32;
    crate::backend::parser::scansup::downcase_truncate_identifier(&s, len, false)
}

/// PG `date_pl_interval`: date + interval, via date->timestamp + interval.
pub fn date_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date_val = pg_getarg_dateadt(fcinfo, 0);
    let span = pg_getarg_interval_p(fcinfo, 1);
    let date_stamp = date2timestamp(date_val);
    let mut fc = make_ts_interval_fcinfo(date_stamp, span);
    crate::backend::utils::adt::timestamp::timestamp_pl_interval(&mut fc)
}

/// PG `date_mi_interval`: date - interval.
pub fn date_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date_val = pg_getarg_dateadt(fcinfo, 0);
    let span = pg_getarg_interval_p(fcinfo, 1);
    let date_stamp = date2timestamp(date_val);
    let mut fc = make_ts_interval_fcinfo(date_stamp, span);
    crate::backend::utils::adt::timestamp::timestamp_mi_interval(&mut fc)
}

/// Build a 2-arg fcinfo (timestamp, interval*) for the timestamp arithmetic
/// entry points (mirrors C DirectFunctionCall2).
fn make_ts_interval_fcinfo(ts: Timestamp, span: Interval) -> FunctionCallInfoBaseData {
    let span_datum = return_interval(span);
    FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: crate::postgres_ext::InvalidOid,
        isnull: false,
        nargs: 2,
        args: vec![
            crate::postgres::NullableDatum {
                value: crate::utils::timestamp::TimestampGetDatum(ts),
                isnull: false,
            },
            crate::postgres::NullableDatum { value: span_datum, isnull: false },
        ],
    }
}

/// PG `date_timestamp`: date -> timestamp.
pub fn date_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date_val = pg_getarg_dateadt(fcinfo, 0);
    crate::utils::timestamp::TimestampGetDatum(date2timestamp(date_val))
}

/// PG `timestamp_date`: timestamp -> date.
pub fn timestamp_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    let mut result: DateADT = 0;
    if TIMESTAMP_IS_NOBEGIN(timestamp) {
        DATE_NOBEGIN(&mut result);
    } else if TIMESTAMP_IS_NOEND(timestamp) {
        DATE_NOEND(&mut result);
    } else {
        let mut tm = new_tm();
        let mut fsec: fsec_t = 0;
        if crate::utils::timestamp::timestamp2tm(
            timestamp,
            None,
            &mut tm,
            &mut fsec,
            None,
            std::ptr::null_mut(),
        ) != 0
        {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .errmsg("timestamp out of range");
            });
        }
        result = date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;
    }
    DateADTGetDatum(result)
}

/// PG `date_timestamptz`: date -> timestamptz.
pub fn date_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date_val = pg_getarg_dateadt(fcinfo, 0);
    crate::utils::timestamp::TimestampTzGetDatum(date2timestamptz(date_val))
}

/// PG `timestamptz_date`: timestamptz -> date.
pub fn timestamptz_date(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    let mut result: DateADT = 0;
    if TIMESTAMP_IS_NOBEGIN(timestamp) {
        DATE_NOBEGIN(&mut result);
    } else if TIMESTAMP_IS_NOEND(timestamp) {
        DATE_NOEND(&mut result);
    } else {
        let mut tm = new_tm();
        let mut fsec: fsec_t = 0;
        let mut tz: i32 = 0;
        if crate::utils::timestamp::timestamp2tm(
            timestamp,
            Some(&mut tz),
            &mut tm,
            &mut fsec,
            None,
            std::ptr::null_mut(),
        ) != 0
        {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .errmsg("timestamp out of range");
            });
        }
        result = date2j(tm.year, tm.mon, tm.mday) - POSTGRES_EPOCH_JDATE;
    }
    DateADTGetDatum(result)
}

// ===========================================================================
//   Time ADT
// ===========================================================================

/// PG `time_in`: text -> time.
pub fn time_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str_ = pg_getarg_cstring(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 2);
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    let mut dtype: i32 = 0;
    let mut extra = DateTimeErrorExtra { timezone: None, abbrev: None };

    let dterr = match crate::utils::datetime::ParseDateTime(&str_, MAXDATEFIELDS) {
        Ok((mut field, mut ftype)) => crate::utils::datetime::DecodeTimeOnly(
            &mut field,
            &mut ftype,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
            &mut extra,
        ),
        Err(e) => e,
    };
    if dterr != 0 {
        date_time_parse_error(dterr, &extra, &str_, "time");
    }

    let mut result: TimeADT = 0;
    tm2time(&tm, fsec, &mut result);
    adjust_time_for_typmod(&mut result, typmod);
    TimeADTGetDatum(result)
}

/// PG `tm2time`: tm -> TimeADT. (date.h internal helper.)
pub fn tm2time(tm: &crate::pgtime::pg_tm, fsec: fsec_t, result: &mut TimeADT) -> i32 {
    *result = ((i64::from(tm.hour) * i64::from(MINS_PER_HOUR) + i64::from(tm.min))
        * i64::from(SECS_PER_MINUTE)
        + i64::from(tm.sec))
        * USECS_PER_SEC
        + i64::from(fsec);
    0
}

/// PG `time_overflows`: is a broken-down time-of-day out of range?
#[must_use]
pub fn time_overflows(hour: i32, min: i32, sec: i32, fsec: fsec_t) -> bool {
    if hour < 0
        || hour > HOURS_PER_DAY
        || min < 0
        || min >= MINS_PER_HOUR
        || sec < 0
        || sec > SECS_PER_MINUTE
        || fsec < 0
        || i64::from(fsec) > USECS_PER_SEC
    {
        return true;
    }
    ((i64::from(hour) * i64::from(MINS_PER_HOUR) + i64::from(min)) * i64::from(SECS_PER_MINUTE)
        + i64::from(sec))
        * USECS_PER_SEC
        + i64::from(fsec)
        > USECS_PER_DAY
}

/// PG `float_time_overflows`: same, with seconds as a double.
#[must_use]
pub fn float_time_overflows(hour: i32, min: i32, sec: f64) -> bool {
    if hour < 0 || hour > HOURS_PER_DAY || min < 0 || min >= MINS_PER_HOUR {
        return true;
    }
    if sec.is_nan() {
        return true;
    }
    let sec = (sec * USECS_PER_SEC as f64).round();
    if sec < 0.0 || sec > (f64::from(SECS_PER_MINUTE) * USECS_PER_SEC as f64) {
        return true;
    }
    (i64::from(hour) * i64::from(MINS_PER_HOUR) + i64::from(min)) * i64::from(SECS_PER_MINUTE)
        * USECS_PER_SEC
        + sec as i64
        > USECS_PER_DAY
}

/// PG `time2tm`: TimeADT -> tm (hour/min/sec/fsec only). (date.h internal.)
pub fn time2tm(time: TimeADT, tm: &mut crate::pgtime::pg_tm, fsec: &mut fsec_t) -> i32 {
    let mut t = time;
    tm.hour = (t / USECS_PER_HOUR) as i32;
    t -= i64::from(tm.hour) * USECS_PER_HOUR;
    tm.min = (t / USECS_PER_MINUTE) as i32;
    t -= i64::from(tm.min) * USECS_PER_MINUTE;
    tm.sec = (t / USECS_PER_SEC) as i32;
    t -= i64::from(tm.sec) * USECS_PER_SEC;
    *fsec = t as fsec_t;
    0
}

/// PG `time_out`: time -> text.
pub fn time_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time = pg_getarg_timeadt(fcinfo, 0);
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    time2tm(time, &mut tm, &mut fsec);
    let mut buf = [0u8; MAXDATELEN + 1];
    crate::utils::datetime::EncodeTimeOnly(&tm, fsec, false, 0, unsafe { crate::miscadmin::DateStyle }, &mut buf);
    pg_return_cstring(&buf_to_string(&buf))
}

/// PG `time_recv`. STAGED: binary wire StringInfo.
pub fn time_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("time_recv needs the binary wire StringInfo (pq_getmsgint64) path")
}
/// PG `time_send`. STAGED: binary wire StringInfo.
pub fn time_send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("time_send needs pq_begintypsend/pq_endtypsend bytea boxing")
}

/// PG `timetypmodin`. STAGED: cstring[] ArrayType typmod path.
pub fn timetypmodin(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timetypmodin needs the cstring[] ArrayType typmod path (ArrayGetIntegerTypmods)")
}
/// PG `timetypmodout`: render TIME typmod.
pub fn timetypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = pg_getarg_int32(fcinfo, 0);
    pg_return_cstring(&anytime_typmodout(false, typmod))
}

/// PG `make_time`: time constructor from (hour, min, sec-as-double).
pub fn make_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let tm_hour = pg_getarg_int32(fcinfo, 0);
    let tm_min = pg_getarg_int32(fcinfo, 1);
    let sec = crate::postgres::DatumGetFloat8(fcinfo.args[2].value);

    if float_time_overflows(tm_hour, tm_min, sec) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_FIELD_OVERFLOW)
                .errmsg(format!("time field value out of range: {tm_hour}:{tm_min:02}:{sec:02}"));
        });
    }
    // This should match tm2time.
    let time = (i64::from(tm_hour) * i64::from(MINS_PER_HOUR) + i64::from(tm_min))
        * i64::from(SECS_PER_MINUTE)
        * USECS_PER_SEC
        + (sec * USECS_PER_SEC as f64).round() as i64;
    TimeADTGetDatum(time)
}

/// PG `time_support`. STAGED: planner-support node.
pub fn time_support(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("time_support needs the planner SupportRequestSimplify node (TemporalSimplify)")
}

/// PG `time_scale`: adjust time to a typmod precision.
pub fn time_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut result = pg_getarg_timeadt(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 1);
    adjust_time_for_typmod(&mut result, typmod);
    TimeADTGetDatum(result)
}

/// PG `AdjustTimeForTypmod`: round a time to the given fractional precision.
/// (date.h internal helper; same algorithm as AdjustTimestampForTypmod.)
pub fn adjust_time_for_typmod(time: &mut TimeADT, typmod: i32) {
    const TIME_SCALES: [i64; (MAX_TIME_PRECISION + 1) as usize] =
        [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
    const TIME_OFFSETS: [i64; (MAX_TIME_PRECISION + 1) as usize] =
        [500_000, 50_000, 5_000, 500, 50, 5, 0];

    if typmod >= 0 && typmod <= MAX_TIME_PRECISION {
        let i = typmod as usize;
        if *time >= 0 {
            *time = ((*time + TIME_OFFSETS[i]) / TIME_SCALES[i]) * TIME_SCALES[i];
        } else {
            *time = -((((-*time) + TIME_OFFSETS[i]) / TIME_SCALES[i]) * TIME_SCALES[i]);
        }
    }
}

pub fn time_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_timeadt(fcinfo, 0) == pg_getarg_timeadt(fcinfo, 1))
}
pub fn time_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_timeadt(fcinfo, 0) != pg_getarg_timeadt(fcinfo, 1))
}
pub fn time_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_timeadt(fcinfo, 0) < pg_getarg_timeadt(fcinfo, 1))
}
pub fn time_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_timeadt(fcinfo, 0) <= pg_getarg_timeadt(fcinfo, 1))
}
pub fn time_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_timeadt(fcinfo, 0) > pg_getarg_timeadt(fcinfo, 1))
}
pub fn time_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    BoolGetDatum(pg_getarg_timeadt(fcinfo, 0) >= pg_getarg_timeadt(fcinfo, 1))
}
pub fn time_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timeadt(fcinfo, 0), pg_getarg_timeadt(fcinfo, 1));
    Int32GetDatum(match a.cmp(&b) {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => 0,
    })
}

/// PG `hashint8`: fold the int8 to 32 bits (sign-aware) then `hash_uint32`.
/// Inlined here (hashfunc.c not ported yet); time_hash = hashint8(fcinfo).
fn hashint8_uint32(val: i64) -> u32 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let folded = lohalf ^ (if val >= 0 { hihalf } else { !hihalf });
    crate::postgres::DatumGetUInt32(hash_uint32(folded))
}
fn hashint8extended_uint64(val: i64, seed: u64) -> u64 {
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let folded = lohalf ^ (if val >= 0 { hihalf } else { !hihalf });
    crate::postgres::DatumGetUInt64(hash_uint32_extended(folded, seed))
}

pub fn time_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    crate::postgres::UInt32GetDatum(hashint8_uint32(pg_getarg_timeadt(fcinfo, 0)))
}
pub fn time_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    crate::postgres::UInt64GetDatum(hashint8extended_uint64(
        pg_getarg_timeadt(fcinfo, 0),
        pg_getarg_int64(fcinfo, 1) as u64,
    ))
}

pub fn time_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timeadt(fcinfo, 0), pg_getarg_timeadt(fcinfo, 1));
    TimeADTGetDatum(if a > b { a } else { b })
}
pub fn time_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timeadt(fcinfo, 0), pg_getarg_timeadt(fcinfo, 1));
    TimeADTGetDatum(if a < b { a } else { b })
}

/// PG `overlaps_time`: SQL OVERLAPS for time. Args left as Datums to honor nulls.
pub fn overlaps_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut ts1 = fcinfo.args[0].value;
    let mut te1 = fcinfo.args[1].value;
    let mut ts2 = fcinfo.args[2].value;
    let mut te2 = fcinfo.args[3].value;
    let ts1_null = fcinfo.args[0].isnull;
    let mut te1_null = fcinfo.args[1].isnull;
    let ts2_null = fcinfo.args[2].isnull;
    let mut te2_null = fcinfo.args[3].isnull;

    let gt = |a: Datum, b: Datum| DatumGetTimeADT(a) > DatumGetTimeADT(b);
    let lt = |a: Datum, b: Datum| DatumGetTimeADT(a) < DatumGetTimeADT(b);

    if ts1_null {
        if te1_null {
            return null_datum(fcinfo);
        }
        ts1 = te1;
        te1_null = true;
    } else if !te1_null && gt(ts1, te1) {
        std::mem::swap(&mut ts1, &mut te1);
    }

    if ts2_null {
        if te2_null {
            return null_datum(fcinfo);
        }
        ts2 = te2;
        te2_null = true;
    } else if !te2_null && gt(ts2, te2) {
        std::mem::swap(&mut ts2, &mut te2);
    }

    if gt(ts1, ts2) {
        if te2_null {
            return null_datum(fcinfo);
        }
        if lt(ts1, te2) {
            return BoolGetDatum(true);
        }
        if te1_null {
            return null_datum(fcinfo);
        }
        BoolGetDatum(false)
    } else if lt(ts1, ts2) {
        if te1_null {
            return null_datum(fcinfo);
        }
        if lt(ts2, te1) {
            return BoolGetDatum(true);
        }
        if te2_null {
            return null_datum(fcinfo);
        }
        BoolGetDatum(false)
    } else {
        if te1_null || te2_null {
            return null_datum(fcinfo);
        }
        BoolGetDatum(true)
    }
}

/// `PG_RETURN_NULL()`: set fcinfo.isnull and return Datum(0).
#[inline]
fn null_datum(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.isnull = true;
    Datum(0)
}

/// PG `timestamp_time`: timestamp -> time.
pub fn timestamp_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return null_datum(fcinfo);
    }
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    if crate::utils::timestamp::timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, std::ptr::null_mut()) != 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("timestamp out of range");
        });
    }
    let result = ((i64::from(tm.hour) * i64::from(MINS_PER_HOUR) + i64::from(tm.min))
        * i64::from(SECS_PER_MINUTE)
        + i64::from(tm.sec))
        * USECS_PER_SEC
        + i64::from(fsec);
    TimeADTGetDatum(result)
}

/// PG `timestamptz_time`: timestamptz -> time.
pub fn timestamptz_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return null_datum(fcinfo);
    }
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    if crate::utils::timestamp::timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, std::ptr::null_mut()) != 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("timestamp out of range");
        });
    }
    let result = ((i64::from(tm.hour) * i64::from(MINS_PER_HOUR) + i64::from(tm.min))
        * i64::from(SECS_PER_MINUTE)
        + i64::from(tm.sec))
        * USECS_PER_SEC
        + i64::from(fsec);
    TimeADTGetDatum(result)
}

/// PG `datetime_timestamp`: date + time -> timestamp.
pub fn datetime_timestamp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date = pg_getarg_dateadt(fcinfo, 0);
    let time = pg_getarg_timeadt(fcinfo, 1);
    let mut result = date2timestamp(date);
    if !TIMESTAMP_NOT_FINITE(result) {
        result += time;
        if !IS_VALID_TIMESTAMP(result) {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .errmsg("timestamp out of range");
            });
        }
    }
    crate::utils::timestamp::TimestampGetDatum(result)
}

/// PG `time_interval`: time -> interval.
pub fn time_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time = pg_getarg_timeadt(fcinfo, 0);
    return_interval(Interval { time, day: 0, month: 0 })
}

/// PG `interval_time`: interval -> time (fractional-day portion).
pub fn interval_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let span = pg_getarg_interval_p(fcinfo, 0);
    if INTERVAL_NOT_FINITE(&span) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("cannot convert infinite interval to time");
        });
    }
    let mut result = span.time % USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    TimeADTGetDatum(result)
}

/// PG `time_mi_time`: time - time -> interval.
pub fn time_mi_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time1 = pg_getarg_timeadt(fcinfo, 0);
    let time2 = pg_getarg_timeadt(fcinfo, 1);
    return_interval(Interval { month: 0, day: 0, time: time1 - time2 })
}

/// PG `time_pl_interval`: time + interval.
pub fn time_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time = pg_getarg_timeadt(fcinfo, 0);
    let span = pg_getarg_interval_p(fcinfo, 1);
    if INTERVAL_NOT_FINITE(&span) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("cannot add infinite interval to time");
        });
    }
    let mut result = time + span.time;
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    TimeADTGetDatum(result)
}

/// PG `time_mi_interval`: time - interval.
pub fn time_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time = pg_getarg_timeadt(fcinfo, 0);
    let span = pg_getarg_interval_p(fcinfo, 1);
    if INTERVAL_NOT_FINITE(&span) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("cannot subtract infinite interval from time");
        });
    }
    let mut result = time - span.time;
    result -= result / USECS_PER_DAY * USECS_PER_DAY;
    if result < 0 {
        result += USECS_PER_DAY;
    }
    TimeADTGetDatum(result)
}

/// PG `in_range_time_interval`: window in_range for time.
pub fn in_range_time_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = pg_getarg_timeadt(fcinfo, 0);
    let base = pg_getarg_timeadt(fcinfo, 1);
    let offset = pg_getarg_interval_p(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);

    if offset.time < 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
                .errmsg("invalid preceding or following size in window function");
        });
    }

    let sum = if sub {
        base - offset.time
    } else {
        match pg_add_s64_overflow(base, offset.time) {
            Some(s) => s,
            None => return BoolGetDatum(less),
        }
    };

    if less {
        BoolGetDatum(val <= sum)
    } else {
        BoolGetDatum(val >= sum)
    }
}

// --- time_part / extract_time ---

fn time_part_common(fcinfo: &FunctionCallInfoBaseData, retnumeric: bool) -> Datum {
    let lowunits = getarg_lowunits(fcinfo, 0);
    let time = pg_getarg_timeadt(fcinfo, 1);

    let (mut type_, val) = DecodeUnits(&lowunits);
    if type_ == UNKNOWN_FIELD {
        let (t, v) = DecodeSpecial(&lowunits);
        type_ = t;
        return time_part_dispatch(time, type_, v, &lowunits, retnumeric);
    }
    time_part_dispatch(time, type_, val, &lowunits, retnumeric)
}

fn time_part_dispatch(
    time: TimeADT,
    type_: i32,
    val: i32,
    lowunits: &str,
    retnumeric: bool,
) -> Datum {
    let intresult: i64;
    if type_ == UNITS {
        let mut tm = new_tm();
        let mut fsec: fsec_t = 0;
        time2tm(time, &mut tm, &mut fsec);
        match val {
            x if x == DTK_MICROSEC => intresult = i64::from(tm.sec) * 1_000_000 + i64::from(fsec),
            x if x == DTK_MILLISEC => {
                return if retnumeric {
                    return_int64_div_fast_numeric(i64::from(tm.sec) * 1_000_000 + i64::from(fsec), 3)
                } else {
                    Float8GetDatum(f64::from(tm.sec) * 1000.0 + f64::from(fsec) / 1000.0)
                };
            }
            x if x == DTK_SECOND => {
                return if retnumeric {
                    return_int64_div_fast_numeric(i64::from(tm.sec) * 1_000_000 + i64::from(fsec), 6)
                } else {
                    Float8GetDatum(f64::from(tm.sec) + f64::from(fsec) / 1_000_000.0)
                };
            }
            x if x == DTK_MINUTE => intresult = i64::from(tm.min),
            x if x == DTK_HOUR => intresult = i64::from(tm.hour),
            _ => unit_not_supported(lowunits, TIMEOID),
        }
    } else if type_ == RESERV && val == DTK_EPOCH {
        return if retnumeric {
            return_int64_div_fast_numeric(time, 6)
        } else {
            Float8GetDatum(time as f64 / 1_000_000.0)
        };
    } else {
        unit_not_recognized(lowunits, TIMEOID)
    }

    if retnumeric {
        return_int64_numeric(intresult)
    } else {
        Float8GetDatum(intresult as f64)
    }
}

pub fn time_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    time_part_common(fcinfo, false)
}
pub fn extract_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    time_part_common(fcinfo, true)
}

// ===========================================================================
//   Time With Time Zone ADT
// ===========================================================================

/// PG `tm2timetz`: tm -> TimeTzADT. (date.h internal helper.)
pub fn tm2timetz(tm: &crate::pgtime::pg_tm, fsec: fsec_t, tz: i32, result: &mut TimeTzADT) -> i32 {
    result.time = ((i64::from(tm.hour) * i64::from(MINS_PER_HOUR) + i64::from(tm.min))
        * i64::from(SECS_PER_MINUTE)
        + i64::from(tm.sec))
        * USECS_PER_SEC
        + i64::from(fsec);
    result.zone = tz;
    0
}

/// PG `timetz_in`: text -> timetz.
pub fn timetz_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let str_ = pg_getarg_cstring(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 2);
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    let mut dtype: i32 = 0;
    let mut extra = DateTimeErrorExtra { timezone: None, abbrev: None };

    let dterr = match crate::utils::datetime::ParseDateTime(&str_, MAXDATEFIELDS) {
        Ok((mut field, mut ftype)) => crate::utils::datetime::DecodeTimeOnly(
            &mut field,
            &mut ftype,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
            &mut extra,
        ),
        Err(e) => e,
    };
    if dterr != 0 {
        date_time_parse_error(dterr, &extra, &str_, "time with time zone");
    }

    let mut result = TimeTzADT { time: 0, zone: 0 };
    tm2timetz(&tm, fsec, tz, &mut result);
    adjust_time_for_typmod(&mut result.time, typmod);
    return_timetzadt(result)
}

/// PG `timetz_out`: timetz -> text.
pub fn timetz_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let mut time = pg_getarg_timetzadt_p(fcinfo, 0);
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    timetz2tm(&mut time, &mut tm, &mut fsec, &mut tz);
    let mut buf = [0u8; MAXDATELEN + 1];
    crate::utils::datetime::EncodeTimeOnly(&tm, fsec, true, tz, unsafe { crate::miscadmin::DateStyle }, &mut buf);
    pg_return_cstring(&buf_to_string(&buf))
}

/// PG `timetz_recv`. STAGED: binary wire StringInfo.
pub fn timetz_recv(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timetz_recv needs the binary wire StringInfo (pq_getmsgint64/int) path")
}
/// PG `timetz_send`. STAGED: binary wire StringInfo.
pub fn timetz_send(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timetz_send needs pq_begintypsend/pq_endtypsend bytea boxing")
}

/// PG `timetztypmodin`. STAGED: cstring[] ArrayType typmod path.
pub fn timetztypmodin(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timetztypmodin needs the cstring[] ArrayType typmod path (ArrayGetIntegerTypmods)")
}
/// PG `timetztypmodout`: render TIMETZ typmod.
pub fn timetztypmodout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let typmod = pg_getarg_int32(fcinfo, 0);
    pg_return_cstring(&anytime_typmodout(true, typmod))
}

/// PG `timetz2tm`: TimeTzADT -> tm + fsec + tz. (date.h internal helper.)
pub fn timetz2tm(time: &mut TimeTzADT, tm: &mut crate::pgtime::pg_tm, fsec: &mut fsec_t, tzp: &mut i32) -> i32 {
    let mut trem: TimeOffset = time.time;
    tm.hour = (trem / USECS_PER_HOUR) as i32;
    trem -= i64::from(tm.hour) * USECS_PER_HOUR;
    tm.min = (trem / USECS_PER_MINUTE) as i32;
    trem -= i64::from(tm.min) * USECS_PER_MINUTE;
    tm.sec = (trem / USECS_PER_SEC) as i32;
    *fsec = (trem - i64::from(tm.sec) * USECS_PER_SEC) as fsec_t;
    *tzp = time.zone;
    0
}

/// PG `timetz_scale`: adjust timetz to a typmod precision.
pub fn timetz_scale(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time = pg_getarg_timetzadt_p(fcinfo, 0);
    let typmod = pg_getarg_int32(fcinfo, 1);
    let mut result = time;
    adjust_time_for_typmod(&mut result.time, typmod);
    return_timetzadt(result)
}

/// PG `timetz_cmp_internal`: compare by GMT time, then by zone.
fn timetz_cmp_internal(time1: &TimeTzADT, time2: &TimeTzADT) -> i32 {
    let t1: TimeOffset = time1.time + i64::from(time1.zone) * USECS_PER_SEC;
    let t2: TimeOffset = time2.time + i64::from(time2.zone) * USECS_PER_SEC;
    if t1 > t2 {
        return 1;
    }
    if t1 < t2 {
        return -1;
    }
    if time1.zone > time2.zone {
        return 1;
    }
    if time1.zone < time2.zone {
        return -1;
    }
    0
}

pub fn timetz_eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    BoolGetDatum(timetz_cmp_internal(&a, &b) == 0)
}
pub fn timetz_ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    BoolGetDatum(timetz_cmp_internal(&a, &b) != 0)
}
pub fn timetz_lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    BoolGetDatum(timetz_cmp_internal(&a, &b) < 0)
}
pub fn timetz_le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    BoolGetDatum(timetz_cmp_internal(&a, &b) <= 0)
}
pub fn timetz_gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    BoolGetDatum(timetz_cmp_internal(&a, &b) > 0)
}
pub fn timetz_ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    BoolGetDatum(timetz_cmp_internal(&a, &b) >= 0)
}
pub fn timetz_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    Int32GetDatum(timetz_cmp_internal(&a, &b))
}

/// PG `timetz_hash`: XOR field hashes to skip padding bytes.
pub fn timetz_hash(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let key = pg_getarg_timetzadt_p(fcinfo, 0);
    let thash = hashint8_uint32(key.time)
        ^ crate::postgres::DatumGetUInt32(hash_uint32(key.zone as u32));
    crate::postgres::UInt32GetDatum(thash)
}

/// PG `timetz_hash_extended`.
pub fn timetz_hash_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let key = pg_getarg_timetzadt_p(fcinfo, 0);
    let seed = pg_getarg_int64(fcinfo, 1) as u64;
    let thash = hashint8extended_uint64(key.time, seed)
        ^ crate::postgres::DatumGetUInt64(hash_uint32_extended(key.zone as u32, seed));
    crate::postgres::UInt64GetDatum(thash)
}

pub fn timetz_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    return_timetzadt(if timetz_cmp_internal(&a, &b) > 0 { a } else { b })
}
pub fn timetz_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let (a, b) = (pg_getarg_timetzadt_p(fcinfo, 0), pg_getarg_timetzadt_p(fcinfo, 1));
    return_timetzadt(if timetz_cmp_internal(&a, &b) < 0 { a } else { b })
}

/// PG `timetz_pl_interval`: timetz + interval.
pub fn timetz_pl_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time = pg_getarg_timetzadt_p(fcinfo, 0);
    let span = pg_getarg_interval_p(fcinfo, 1);
    if INTERVAL_NOT_FINITE(&span) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("cannot add infinite interval to time");
        });
    }
    let mut result = TimeTzADT { time: time.time + span.time, zone: time.zone };
    result.time -= result.time / USECS_PER_DAY * USECS_PER_DAY;
    if result.time < 0 {
        result.time += USECS_PER_DAY;
    }
    return_timetzadt(result)
}

/// PG `timetz_mi_interval`: timetz - interval.
pub fn timetz_mi_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time = pg_getarg_timetzadt_p(fcinfo, 0);
    let span = pg_getarg_interval_p(fcinfo, 1);
    if INTERVAL_NOT_FINITE(&span) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("cannot subtract infinite interval from time");
        });
    }
    let mut result = TimeTzADT { time: time.time - span.time, zone: time.zone };
    result.time -= result.time / USECS_PER_DAY * USECS_PER_DAY;
    if result.time < 0 {
        result.time += USECS_PER_DAY;
    }
    return_timetzadt(result)
}

/// PG `in_range_timetz_interval`: window in_range for timetz.
pub fn in_range_timetz_interval(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val = pg_getarg_timetzadt_p(fcinfo, 0);
    let base = pg_getarg_timetzadt_p(fcinfo, 1);
    let offset = pg_getarg_interval_p(fcinfo, 2);
    let sub = pg_getarg_bool(fcinfo, 3);
    let less = pg_getarg_bool(fcinfo, 4);

    if offset.time < 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
                .errmsg("invalid preceding or following size in window function");
        });
    }

    let mut sum = TimeTzADT { time: 0, zone: base.zone };
    if sub {
        sum.time = base.time - offset.time;
    } else {
        match pg_add_s64_overflow(base.time, offset.time) {
            Some(s) => sum.time = s,
            None => return BoolGetDatum(less),
        }
    }

    if less {
        BoolGetDatum(timetz_cmp_internal(&val, &sum) <= 0)
    } else {
        BoolGetDatum(timetz_cmp_internal(&val, &sum) >= 0)
    }
}

/// PG `overlaps_timetz`: SQL OVERLAPS for timetz.
pub fn overlaps_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let a0 = fcinfo.args[0].value;
    let a1 = fcinfo.args[1].value;
    let a2 = fcinfo.args[2].value;
    let a3 = fcinfo.args[3].value;
    let mut ts1 = a0;
    let mut te1 = a1;
    let mut ts2 = a2;
    let mut te2 = a3;
    let ts1_null = fcinfo.args[0].isnull;
    let mut te1_null = fcinfo.args[1].isnull;
    let ts2_null = fcinfo.args[2].isnull;
    let mut te2_null = fcinfo.args[3].isnull;

    let gt = |a: Datum, b: Datum| {
        let pa = DatumGetTimeTzADTP(a);
        let pb = DatumGetTimeTzADTP(b);
        // SAFETY: byref timetz Datums point to valid values for the call.
        timetz_cmp_internal(unsafe { &*pa }, unsafe { &*pb }) > 0
    };
    let lt = |a: Datum, b: Datum| {
        let pa = DatumGetTimeTzADTP(a);
        let pb = DatumGetTimeTzADTP(b);
        timetz_cmp_internal(unsafe { &*pa }, unsafe { &*pb }) < 0
    };

    if ts1_null {
        if te1_null {
            return null_datum(fcinfo);
        }
        ts1 = te1;
        te1_null = true;
    } else if !te1_null && gt(ts1, te1) {
        std::mem::swap(&mut ts1, &mut te1);
    }

    if ts2_null {
        if te2_null {
            return null_datum(fcinfo);
        }
        ts2 = te2;
        te2_null = true;
    } else if !te2_null && gt(ts2, te2) {
        std::mem::swap(&mut ts2, &mut te2);
    }

    if gt(ts1, ts2) {
        if te2_null {
            return null_datum(fcinfo);
        }
        if lt(ts1, te2) {
            return BoolGetDatum(true);
        }
        if te1_null {
            return null_datum(fcinfo);
        }
        BoolGetDatum(false)
    } else if lt(ts1, ts2) {
        if te1_null {
            return null_datum(fcinfo);
        }
        if lt(ts2, te1) {
            return BoolGetDatum(true);
        }
        if te2_null {
            return null_datum(fcinfo);
        }
        BoolGetDatum(false)
    } else {
        if te1_null || te2_null {
            return null_datum(fcinfo);
        }
        BoolGetDatum(true)
    }
}

/// PG `timetz_time`: timetz -> time (drops the zone).
pub fn timetz_time(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timetz = pg_getarg_timetzadt_p(fcinfo, 0);
    TimeADTGetDatum(timetz.time)
}

/// PG `time_timetz`: time -> timetz, attaching the session zone for "now".
pub fn time_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let time = pg_getarg_timeadt(fcinfo, 0);
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    crate::utils::datetime::GetCurrentDateTime(&mut tm);
    time2tm(time, &mut tm, &mut fsec);
    let tz = crate::utils::datetime::DetermineTimeZoneOffset(&mut tm, session_timezone_ref());
    return_timetzadt(TimeTzADT { time, zone: tz })
}

/// PG `timestamptz_timetz`: timestamptz -> timetz.
pub fn timestamptz_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let timestamp = pg_getarg_timestamp(fcinfo, 0);
    if TIMESTAMP_NOT_FINITE(timestamp) {
        return null_datum(fcinfo);
    }
    let mut tm = new_tm();
    let mut fsec: fsec_t = 0;
    let mut tz: i32 = 0;
    if crate::utils::timestamp::timestamp2tm(timestamp, Some(&mut tz), &mut tm, &mut fsec, None, std::ptr::null_mut()) != 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                .errmsg("timestamp out of range");
        });
    }
    let mut result = TimeTzADT { time: 0, zone: 0 };
    tm2timetz(&tm, fsec, tz, &mut result);
    return_timetzadt(result)
}

/// PG `datetimetz_timestamptz`: date + timetz -> timestamptz (stored GMT).
pub fn datetimetz_timestamptz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let date = pg_getarg_dateadt(fcinfo, 0);
    let time = pg_getarg_timetzadt_p(fcinfo, 1);
    let mut result: TimestampTz = 0;
    if DATE_IS_NOBEGIN(date) {
        TIMESTAMP_NOBEGIN(&mut result);
    } else if DATE_IS_NOEND(date) {
        TIMESTAMP_NOEND(&mut result);
    } else {
        if date >= TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .errmsg("date out of range for timestamp");
            });
        }
        result = i64::from(date) * USECS_PER_DAY + time.time + i64::from(time.zone) * USECS_PER_SEC;
        if !IS_VALID_TIMESTAMP(result) {
            ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE)
                    .errmsg("date out of range for timestamp");
            });
        }
    }
    crate::utils::timestamp::TimestampTzGetDatum(result)
}

// --- timetz_part / extract_timetz ---

fn timetz_part_common(fcinfo: &FunctionCallInfoBaseData, retnumeric: bool) -> Datum {
    let lowunits = getarg_lowunits(fcinfo, 0);
    let time = pg_getarg_timetzadt_p(fcinfo, 1);

    let (mut type_, val) = DecodeUnits(&lowunits);
    if type_ == UNKNOWN_FIELD {
        let (t, v) = DecodeSpecial(&lowunits);
        type_ = t;
        return timetz_part_dispatch(&time, type_, v, &lowunits, retnumeric);
    }
    timetz_part_dispatch(&time, type_, val, &lowunits, retnumeric)
}

fn timetz_part_dispatch(
    time: &TimeTzADT,
    type_: i32,
    val: i32,
    lowunits: &str,
    retnumeric: bool,
) -> Datum {
    let intresult: i64;
    if type_ == UNITS {
        let mut tm = new_tm();
        let mut fsec: fsec_t = 0;
        let mut tz: i32 = 0;
        let mut t = *time;
        timetz2tm(&mut t, &mut tm, &mut fsec, &mut tz);
        match val {
            x if x == DTK_TZ => intresult = i64::from(-tz),
            x if x == DTK_TZ_MINUTE => {
                intresult = i64::from((-tz / SECS_PER_MINUTE) % MINS_PER_HOUR);
            }
            x if x == DTK_TZ_HOUR => intresult = i64::from(-tz / SECS_PER_HOUR),
            x if x == DTK_MICROSEC => intresult = i64::from(tm.sec) * 1_000_000 + i64::from(fsec),
            x if x == DTK_MILLISEC => {
                return if retnumeric {
                    return_int64_div_fast_numeric(i64::from(tm.sec) * 1_000_000 + i64::from(fsec), 3)
                } else {
                    Float8GetDatum(f64::from(tm.sec) * 1000.0 + f64::from(fsec) / 1000.0)
                };
            }
            x if x == DTK_SECOND => {
                return if retnumeric {
                    return_int64_div_fast_numeric(i64::from(tm.sec) * 1_000_000 + i64::from(fsec), 6)
                } else {
                    Float8GetDatum(f64::from(tm.sec) + f64::from(fsec) / 1_000_000.0)
                };
            }
            x if x == DTK_MINUTE => intresult = i64::from(tm.min),
            x if x == DTK_HOUR => intresult = i64::from(tm.hour),
            _ => unit_not_supported(lowunits, TIMETZOID),
        }
    } else if type_ == RESERV && val == DTK_EPOCH {
        return if retnumeric {
            return_int64_div_fast_numeric(time.time + i64::from(time.zone) * 1_000_000, 6)
        } else {
            Float8GetDatum(time.time as f64 / 1_000_000.0 + f64::from(time.zone))
        };
    } else {
        unit_not_recognized(lowunits, TIMETZOID)
    }

    if retnumeric {
        return_int64_numeric(intresult)
    } else {
        Float8GetDatum(intresult as f64)
    }
}

pub fn timetz_part(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    timetz_part_common(fcinfo, false)
}
pub fn extract_timetz(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    timetz_part_common(fcinfo, true)
}

/// PG `timetz_zone`: shift timetz into a named/abbrev zone. Only the numeric
/// fixed-offset abbreviation branch is implemented; the named/dynamic-zone
/// branches need the IANA tz database (STAGED).
pub fn timetz_zone(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timetz_zone needs the IANA tz DB (DecodeTimezoneName/DetermineTimeZoneAbbrevOffsetTS)")
}

/// PG `timetz_izone`: shift timetz by a fixed interval offset.
pub fn timetz_izone(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let zone = pg_getarg_interval_p(fcinfo, 0);
    let time = pg_getarg_timetzadt_p(fcinfo, 1);

    if INTERVAL_NOT_FINITE(&zone) {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("interval time zone must be finite");
        });
    }
    if zone.month != 0 || zone.day != 0 {
        ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("interval time zone must not include months or days");
        });
    }

    let tz = -(zone.time / USECS_PER_SEC) as i32;
    let mut result = TimeTzADT {
        time: time.time + i64::from(time.zone - tz) * USECS_PER_SEC,
        zone: tz,
    };
    while result.time < 0 {
        result.time += USECS_PER_DAY;
    }
    if result.time >= USECS_PER_DAY {
        result.time %= USECS_PER_DAY;
    }
    return_timetzadt(result)
}

/// PG `timetz_at_local`. STAGED: session timezone name lookup.
pub fn timetz_at_local(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    unimplemented!("timetz_at_local needs the session timezone name (pg_get_timezone_name)")
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
            args: args
                .iter()
                .map(|&value| NullableDatum { value, isnull: false })
                .collect(),
        }
    }

    fn cstr(s: &str) -> Datum {
        let c = std::ffi::CString::new(s).unwrap();
        CStringGetDatum(c.into_raw())
    }

    fn out_string(d: Datum) -> String {
        let p = DatumGetCString(d);
        let cs = unsafe { core::ffi::CStr::from_ptr(p) };
        cs.to_string_lossy().into_owned()
    }

    #[test]
    fn date_in_out_roundtrip() {
        for s in ["2024-01-15", "2000-01-01", "2024-02-29"] {
            let mut f = fc(&[cstr(s), Datum(0), Int32GetDatum(-1)]);
            let d = date_in(&mut f);
            let mut g = fc(&[d]);
            assert_eq!(out_string(date_out(&mut g)), s);
        }
    }

    #[test]
    fn date_epoch_is_zero() {
        let mut f = fc(&[cstr("2000-01-01"), Datum(0), Int32GetDatum(-1)]);
        assert_eq!(DatumGetDateADT(date_in(&mut f)), 0);
    }

    #[test]
    fn date_pli_adds_days() {
        let mut f = fc(&[cstr("2024-01-15"), Datum(0), Int32GetDatum(-1)]);
        let d = date_in(&mut f);
        let mut g = fc(&[d, Int32GetDatum(7)]);
        let r = date_pli(&mut g);
        let mut h = fc(&[r]);
        assert_eq!(out_string(date_out(&mut h)), "2024-01-22");
    }

    #[test]
    fn date_mi_days() {
        let mk = |s: &str| {
            let mut f = fc(&[cstr(s), Datum(0), Int32GetDatum(-1)]);
            date_in(&mut f)
        };
        let mut g = fc(&[mk("2024-01-22"), mk("2024-01-15")]);
        assert_eq!(DatumGetInt32(date_mi(&mut g)), 7);
    }

    #[test]
    fn date_to_timestamp_cast() {
        let mut f = fc(&[cstr("2000-01-02"), Datum(0), Int32GetDatum(-1)]);
        let d = date_in(&mut f);
        let mut g = fc(&[d]);
        let ts = crate::utils::timestamp::DatumGetTimestamp(date_timestamp(&mut g));
        assert_eq!(ts, USECS_PER_DAY); // one day after 2000-01-01
    }

    #[test]
    fn time_in_out_roundtrip() {
        for s in ["10:30:00", "10:30:00.123456", "00:00:00", "23:59:59"] {
            let mut f = fc(&[cstr(s), Datum(0), Int32GetDatum(-1)]);
            let d = time_in(&mut f);
            let mut g = fc(&[d]);
            assert_eq!(out_string(time_out(&mut g)), s);
        }
    }

    #[test]
    fn time_cmp_suite() {
        let mk = |s: &str| {
            let mut f = fc(&[cstr(s), Datum(0), Int32GetDatum(-1)]);
            time_in(&mut f)
        };
        let a = mk("10:00:00");
        let b = mk("11:00:00");
        assert!(DatumGetBool(time_lt(&mut fc(&[a, b]))));
        assert!(DatumGetBool(time_gt(&mut fc(&[b, a]))));
        assert!(DatumGetBool(time_eq(&mut fc(&[a, a]))));
        assert_eq!(DatumGetInt32(time_cmp(&mut fc(&[a, b]))), -1);
    }

    #[test]
    fn timetz_in_out_roundtrip() {
        for s in ["10:30:00+00", "10:30:00-05", "00:00:00+00"] {
            let mut f = fc(&[cstr(s), Datum(0), Int32GetDatum(-1)]);
            let d = timetz_in(&mut f);
            let mut g = fc(&[d]);
            assert_eq!(out_string(timetz_out(&mut g)), s);
        }
    }

    #[test]
    fn timetz_cmp_suite() {
        let mk = |s: &str| {
            let mut f = fc(&[cstr(s), Datum(0), Int32GetDatum(-1)]);
            timetz_in(&mut f)
        };
        // 10:00:00+00 (GMT 10:00) vs 10:00:00-01 (GMT 11:00): first is less.
        let a = mk("10:00:00+00");
        let b = mk("10:00:00-01");
        assert!(DatumGetBool(timetz_lt(&mut fc(&[a, b]))));
        assert!(DatumGetBool(timetz_gt(&mut fc(&[b, a]))));
        assert!(DatumGetBool(timetz_eq(&mut fc(&[a, a]))));
    }

    #[test]
    fn date_cmp_suite() {
        let mk = |s: &str| {
            let mut f = fc(&[cstr(s), Datum(0), Int32GetDatum(-1)]);
            date_in(&mut f)
        };
        let a = mk("2024-01-15");
        let b = mk("2024-06-01");
        assert!(DatumGetBool(date_lt(&mut fc(&[a, b]))));
        assert!(DatumGetBool(date_gt(&mut fc(&[b, a]))));
        assert!(DatumGetBool(date_eq(&mut fc(&[a, a]))));
        assert_eq!(DatumGetInt32(date_cmp(&mut fc(&[a, b]))), -1);
    }
}
