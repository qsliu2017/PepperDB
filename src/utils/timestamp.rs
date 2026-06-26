//! Translated from PostgreSQL src/include/utils/timestamp.h
//!
//! The on-disk types (Timestamp/TimestampTz/Interval/fsec_t) live in
//! crate::datatype::timestamp; this header adds the fmgr glue + utility API.

use crate::datatype::timestamp::fsec_t;
use crate::datatype::timestamp::pg_itm;
use crate::datatype::timestamp::pg_itm_in;
use crate::datatype::timestamp::Interval;
use crate::datatype::timestamp::Timestamp;
use crate::datatype::timestamp::TimestampTz;
use crate::pgtime::pg_time_t;
use crate::pgtime::pg_tm;
use crate::pgtime::pg_tz;
use crate::postgres::Datum;

// fmgr interface
#[inline]
pub fn DatumGetTimestamp(x: Datum) -> Timestamp {
    x.0 as i64
}
#[inline]
pub fn DatumGetTimestampTz(x: Datum) -> TimestampTz {
    x.0 as i64
}
#[inline]
pub fn DatumGetIntervalP(x: Datum) -> *mut Interval {
    x.0 as *mut Interval // TODO(ptr)
}
#[inline]
pub fn TimestampGetDatum(x: Timestamp) -> Datum {
    Datum(x as usize)
}
#[inline]
pub fn TimestampTzGetDatum(x: TimestampTz) -> Datum {
    Datum(x as usize)
}
#[inline]
pub fn IntervalPGetDatum(x: &Interval) -> Datum {
    Datum(x as *const Interval as usize) // TODO(ptr)
}

#[inline]
pub const fn TIMESTAMP_MASK(b: i32) -> i32 {
    1 << b
}
#[inline]
pub const fn INTERVAL_MASK(b: i32) -> i32 {
    1 << b
}

// Packing/unpacking the typmod field for intervals.
pub const INTERVAL_FULL_RANGE: i32 = 0x7FFF;
pub const INTERVAL_RANGE_MASK: i32 = 0x7FFF;
pub const INTERVAL_FULL_PRECISION: i32 = 0xFFFF;
pub const INTERVAL_PRECISION_MASK: i32 = 0xFFFF;

#[inline]
pub const fn INTERVAL_TYPMOD(p: i32, r: i32) -> i32 {
    ((r & INTERVAL_RANGE_MASK) << 16) | (p & INTERVAL_PRECISION_MASK)
}
#[inline]
pub const fn INTERVAL_PRECISION(t: i32) -> i32 {
    t & INTERVAL_PRECISION_MASK
}
#[inline]
pub const fn INTERVAL_RANGE(t: i32) -> i32 {
    (t >> 16) & INTERVAL_RANGE_MASK
}

#[inline]
pub const fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: i64) -> TimestampTz {
    tz + ms * 1000
}
#[inline]
pub const fn TimestampTzPlusSeconds(tz: TimestampTz, s: i64) -> TimestampTz {
    tz + s * 1000000
}

#[inline]
pub fn TimestampDifferenceMicroseconds(start_time: TimestampTz, stop_time: TimestampTz) -> u64 {
    if start_time >= stop_time {
        0
    } else {
        (stop_time as u64).wrapping_sub(start_time as u64)
    }
}

// Globals (set at postmaster start / config reload) -> task/session state later.
pub static mut PgStartTime: TimestampTz = 0;
pub static mut PgReloadTime: TimestampTz = 0;

// Internal routines (not fmgr-callable)
pub fn anytimestamp_typmod_check(istz: bool, typmod: i32) -> i32 {
    unimplemented!()
}
pub fn GetCurrentTimestamp() -> TimestampTz {
    // PG `GetCurrentTimestamp`: microseconds since the PostgreSQL epoch
    // (2000-01-01 UTC). Computed from the system clock.
    const PG_EPOCH_UNIX_SECS: i64 = 946_684_800; // 2000-01-01 - 1970-01-01
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (dur.as_micros() as i64) - PG_EPOCH_UNIX_SECS * crate::datatype::timestamp::USECS_PER_SEC
}
pub fn GetSQLCurrentTimestamp(typmod: i32) -> TimestampTz {
    unimplemented!()
}
pub fn GetSQLLocalTimestamp(typmod: i32) -> Timestamp {
    unimplemented!()
}
/// out-params (secs, microsecs) -> tuple.
pub fn TimestampDifference(start_time: TimestampTz, stop_time: TimestampTz) -> (i64, i32) {
    unimplemented!()
}
pub fn TimestampDifferenceMilliseconds(start_time: TimestampTz, stop_time: TimestampTz) -> i64 {
    unimplemented!()
}
pub fn TimestampDifferenceExceeds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    msec: i32,
) -> bool {
    unimplemented!()
}
pub fn TimestampDifferenceExceedsSeconds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    threshold_sec: i32,
) -> bool {
    unimplemented!()
}

pub fn time_t_to_timestamptz(tm: pg_time_t) -> TimestampTz {
    unimplemented!()
}
pub fn timestamptz_to_time_t(t: TimestampTz) -> pg_time_t {
    unimplemented!()
}

pub fn timestamptz_to_str(t: TimestampTz) -> String {
    unimplemented!()
}

/// `int *tzp` skippable out-param -> Option; (tm, fsec) folded into the tuple.
pub fn tm2timestamp(
    tm: &pg_tm,
    fsec: fsec_t,
    tzp: Option<&i32>,
    result: &mut Timestamp,
) -> i32 {
    unimplemented!()
}
pub fn timestamp2tm(
    dt: Timestamp,
    tzp: Option<&mut i32>,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzn: Option<&mut *const u8>,
    attimezone: *mut pg_tz,
) -> i32 {
    unimplemented!() // TODO(ptr)
}
/// out-params (hour, min, sec, fsec) -> tuple.
pub fn dt2time(jd: Timestamp) -> (i32, i32, i32, fsec_t) {
    unimplemented!()
}

pub fn interval2itm(span: Interval, itm: &mut pg_itm) {
    unimplemented!()
}
pub fn itm2interval(itm: &mut pg_itm, span: &mut Interval) -> i32 {
    unimplemented!()
}
pub fn itmin2interval(itm_in: &mut pg_itm_in, span: &mut Interval) -> i32 {
    unimplemented!()
}

pub fn SetEpochTimestamp() -> Timestamp {
    unimplemented!()
}
pub fn GetEpochTime(tm: &mut pg_tm) {
    unimplemented!()
}

pub fn timestamp_cmp_internal(dt1: Timestamp, dt2: Timestamp) -> i32 {
    unimplemented!()
}

// timestamp comparison works for timestamptz too.
#[inline]
pub fn timestamptz_cmp_internal(dt1: TimestampTz, dt2: TimestampTz) -> i32 {
    timestamp_cmp_internal(dt1, dt2)
}

/// `int *overflow` out-param -> tuple.
pub fn timestamp2timestamptz_opt_overflow(timestamp: Timestamp) -> (TimestampTz, i32) {
    unimplemented!()
}
pub fn timestamp_cmp_timestamptz_internal(timestamp_val: Timestamp, dt2: TimestampTz) -> i32 {
    unimplemented!()
}

pub fn isoweek2j(year: i32, week: i32) -> i32 {
    unimplemented!()
}
/// out-params (year, mon, mday) -> tuple.
pub fn isoweek2date(woy: i32) -> (i32, i32, i32) {
    unimplemented!()
}
pub fn isoweekdate2date(isoweek: i32, wday: i32) -> (i32, i32, i32) {
    unimplemented!()
}
pub fn date2isoweek(year: i32, mon: i32, mday: i32) -> i32 {
    unimplemented!()
}
pub fn date2isoyear(year: i32, mon: i32, mday: i32) -> i32 {
    unimplemented!()
}
pub fn date2isoyearday(year: i32, mon: i32, mday: i32) -> i32 {
    unimplemented!()
}

pub fn TimestampTimestampTzRequiresRewrite() -> bool {
    unimplemented!()
}
