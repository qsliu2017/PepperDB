//! Translated from PostgreSQL src/include/datatype/timestamp.h

// Timestamps and the h/m/s fields of intervals are i64 microseconds.
pub type Timestamp = i64;
pub type TimestampTz = i64;
pub type TimeOffset = i64;
pub type fsec_t = i32; // fractional seconds (in microseconds)

/// On-disk storage format for type interval. Must not be reordered.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub time: TimeOffset, // all time units other than days, months and years
    pub day: i32,         // days, after time for alignment
    pub month: i32,       // months and years, after time for alignment
}
const _: () = assert!(core::mem::size_of::<Interval>() == 16);
const _: () = assert!(core::mem::offset_of!(Interval, time) == 0);
const _: () = assert!(core::mem::offset_of!(Interval, day) == 8);
const _: () = assert!(core::mem::offset_of!(Interval, month) == 12);

/// Broken-down interval (modeled on struct pg_tm).
#[derive(Debug, Clone, Copy, Default)]
pub struct pg_itm {
    pub tm_usec: i32,
    pub tm_sec: i32,
    pub tm_min: i32,
    pub tm_hour: i64, // needs to be wide
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
}

/// Data structure for decoding intervals.
#[derive(Debug, Clone, Copy, Default)]
pub struct pg_itm_in {
    pub tm_usec: i64, // needs to be wide
    pub tm_mday: i32,
    pub tm_mon: i32,
    pub tm_year: i32,
}

// Limits on the "precision" option (typmod).
pub const MAX_TIMESTAMP_PRECISION: i32 = 6;
pub const MAX_INTERVAL_PRECISION: i32 = 6;

// Round off to MAX_TIMESTAMP_PRECISION decimal places.
pub const TS_PREC_INV: f64 = 1000000.0;

#[inline]
pub fn TSROUND(j: f64) -> f64 {
    (j * TS_PREC_INV).round() / TS_PREC_INV
}

// Assorted constants for datetime-related calculations.
pub const DAYS_PER_YEAR: f64 = 365.25; // assumes leap year every four years
pub const MONTHS_PER_YEAR: i32 = 12;
pub const DAYS_PER_MONTH: i32 = 30; // assumes exactly 30 days per month
pub const DAYS_PER_WEEK: i32 = 7;
pub const HOURS_PER_DAY: i32 = 24;

pub const SECS_PER_YEAR: i32 = 36525 * 864; // avoid floating-point computation
pub const SECS_PER_DAY: i32 = 86400;
pub const SECS_PER_HOUR: i32 = 3600;
pub const SECS_PER_MINUTE: i32 = 60;
pub const MINS_PER_HOUR: i32 = 60;

pub const USECS_PER_DAY: i64 = 86400000000;
pub const USECS_PER_HOUR: i64 = 3600000000;
pub const USECS_PER_MINUTE: i64 = 60000000;
pub const USECS_PER_SEC: i64 = 1000000;

pub const MAX_TZDISP_HOUR: i32 = 15; // maximum allowed hour part
pub const TZDISP_LIMIT: i32 = (MAX_TZDISP_HOUR + 1) * SECS_PER_HOUR;

// Reserved min/max integers represent timestamp -infinity and +infinity.
pub const TIMESTAMP_MINUS_INFINITY: Timestamp = i64::MIN;
pub const TIMESTAMP_INFINITY: Timestamp = i64::MAX;

pub const DT_NOBEGIN: Timestamp = TIMESTAMP_MINUS_INFINITY;
pub const DT_NOEND: Timestamp = TIMESTAMP_INFINITY;

#[inline]
pub fn TIMESTAMP_NOBEGIN(j: &mut Timestamp) {
    *j = DT_NOBEGIN;
}

#[inline]
pub const fn TIMESTAMP_IS_NOBEGIN(j: Timestamp) -> bool {
    j == DT_NOBEGIN
}

#[inline]
pub fn TIMESTAMP_NOEND(j: &mut Timestamp) {
    *j = DT_NOEND;
}

#[inline]
pub const fn TIMESTAMP_IS_NOEND(j: Timestamp) -> bool {
    j == DT_NOEND
}

#[inline]
pub const fn TIMESTAMP_NOT_FINITE(j: Timestamp) -> bool {
    TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j)
}

#[inline]
pub fn INTERVAL_NOBEGIN(i: &mut Interval) {
    i.time = i64::MIN;
    i.day = i32::MIN;
    i.month = i32::MIN;
}

#[inline]
pub const fn INTERVAL_IS_NOBEGIN(i: &Interval) -> bool {
    i.month == i32::MIN && i.day == i32::MIN && i.time == i64::MIN
}

#[inline]
pub fn INTERVAL_NOEND(i: &mut Interval) {
    i.time = i64::MAX;
    i.day = i32::MAX;
    i.month = i32::MAX;
}

#[inline]
pub const fn INTERVAL_IS_NOEND(i: &Interval) -> bool {
    i.month == i32::MAX && i.day == i32::MAX && i.time == i64::MAX
}

#[inline]
pub const fn INTERVAL_NOT_FINITE(i: &Interval) -> bool {
    INTERVAL_IS_NOBEGIN(i) || INTERVAL_IS_NOEND(i)
}

// Julian date support.
pub const JULIAN_MINYEAR: i32 = -4713;
pub const JULIAN_MINMONTH: i32 = 11;
pub const JULIAN_MINDAY: i32 = 24;
pub const JULIAN_MAXYEAR: i32 = 5874898;
pub const JULIAN_MAXMONTH: i32 = 6;
pub const JULIAN_MAXDAY: i32 = 3;

#[inline]
pub const fn IS_VALID_JULIAN(y: i32, m: i32, _d: i32) -> bool {
    (y > JULIAN_MINYEAR || (y == JULIAN_MINYEAR && m >= JULIAN_MINMONTH))
        && (y < JULIAN_MAXYEAR || (y == JULIAN_MAXYEAR && m < JULIAN_MAXMONTH))
}

// Julian-date equivalents of Day 0 in Unix and Postgres reckoning.
pub const UNIX_EPOCH_JDATE: i32 = 2440588; // == date2j(1970, 1, 1)
pub const POSTGRES_EPOCH_JDATE: i32 = 2451545; // == date2j(2000, 1, 1)

// Range limits for dates and timestamps.
pub const DATETIME_MIN_JULIAN: i32 = 0;
pub const DATE_END_JULIAN: i32 = 2147483494; // == date2j(JULIAN_MAXYEAR, 1, 1)
pub const TIMESTAMP_END_JULIAN: i32 = 109203528; // == date2j(294277, 1, 1)

pub const MIN_TIMESTAMP: i64 = -211813488000000000;
pub const END_TIMESTAMP: i64 = 9223371331200000000;

/// Range-check a date (given in Postgres, not Julian, numbering).
#[inline]
pub const fn IS_VALID_DATE(d: i32) -> bool {
    (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE) <= d && d < (DATE_END_JULIAN - POSTGRES_EPOCH_JDATE)
}

/// Range-check a timestamp.
#[inline]
pub const fn IS_VALID_TIMESTAMP(t: Timestamp) -> bool {
    MIN_TIMESTAMP <= t && t < END_TIMESTAMP
}
