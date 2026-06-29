//! Translated from PostgreSQL src/include/utils/datetime.h

use crate::c::Size;
use crate::datatype::timestamp::{fsec_t, pg_itm, pg_itm_in, Timestamp};
use crate::pgtime::{pg_tm, pg_tz};

// String definitions for standard time quantities.
pub const DAGO: &str = "ago";
pub const DCURRENT: &str = "current";
pub const EPOCH: &str = "epoch";
pub const INVALID: &str = "invalid";
pub const EARLY: &str = "-infinity";
pub const LATE: &str = "infinity";
pub const NOW: &str = "now";
pub const TODAY: &str = "today";
pub const TOMORROW: &str = "tomorrow";
pub const YESTERDAY: &str = "yesterday";
pub const ZULU: &str = "zulu";

pub const DMICROSEC: &str = "usecond";
pub const DMILLISEC: &str = "msecond";
pub const DSECOND: &str = "second";
pub const DMINUTE: &str = "minute";
pub const DHOUR: &str = "hour";
pub const DDAY: &str = "day";
pub const DWEEK: &str = "week";
pub const DMONTH: &str = "month";
pub const DQUARTER: &str = "quarter";
pub const DYEAR: &str = "year";
pub const DDECADE: &str = "decade";
pub const DCENTURY: &str = "century";
pub const DMILLENNIUM: &str = "millennium";
pub const DA_D: &str = "ad";
pub const DB_C: &str = "bc";
pub const DTIMEZONE: &str = "timezone";

// Meridian: am, pm, or 24-hour style.
pub const AM: i32 = 0;
pub const PM: i32 = 1;
pub const HR24: i32 = 2;

// Millennium: ad, bc.
pub const AD: i32 = 0;
pub const BC: i32 = 1;

// Field types for time decoding (turned into bit masks during parsing).
pub const RESERV: i32 = 0;
pub const MONTH: i32 = 1;
pub const YEAR: i32 = 2;
pub const DAY: i32 = 3;
pub const JULIAN: i32 = 4;
pub const TZ: i32 = 5; // fixed-offset timezone abbreviation
pub const DTZ: i32 = 6; // fixed-offset timezone abbrev, DST
pub const DYNTZ: i32 = 7; // dynamic timezone abbreviation
pub const IGNORE_DTF: i32 = 8;
pub const AMPM: i32 = 9;
pub const HOUR: i32 = 10;
pub const MINUTE: i32 = 11;
pub const SECOND: i32 = 12;
pub const MILLISECOND: i32 = 13;
pub const MICROSECOND: i32 = 14;
pub const DOY: i32 = 15;
pub const DOW: i32 = 16;
pub const UNITS: i32 = 17;
pub const ADBC: i32 = 18;
pub const AGO: i32 = 19;
pub const ABS_BEFORE: i32 = 20;
pub const ABS_AFTER: i32 = 21;
pub const ISODATE: i32 = 22;
pub const ISOTIME: i32 = 23;
pub const WEEK: i32 = 24;
pub const DECADE: i32 = 25;
pub const CENTURY: i32 = 26;
pub const MILLENNIUM: i32 = 27;
pub const DTZMOD: i32 = 28; // "DST" as a separate word
pub const UNKNOWN_FIELD: i32 = 31;

// Token field definitions for time parsing and decoding.
pub const DTK_NUMBER: i32 = 0;
pub const DTK_STRING: i32 = 1;
pub const DTK_DATE: i32 = 2;
pub const DTK_TIME: i32 = 3;
pub const DTK_TZ: i32 = 4;
pub const DTK_AGO: i32 = 5;
pub const DTK_SPECIAL: i32 = 6;
pub const DTK_EARLY: i32 = 9;
pub const DTK_LATE: i32 = 10;
pub const DTK_EPOCH: i32 = 11;
pub const DTK_NOW: i32 = 12;
pub const DTK_YESTERDAY: i32 = 13;
pub const DTK_TODAY: i32 = 14;
pub const DTK_TOMORROW: i32 = 15;
pub const DTK_ZULU: i32 = 16;
pub const DTK_DELTA: i32 = 17;
pub const DTK_SECOND: i32 = 18;
pub const DTK_MINUTE: i32 = 19;
pub const DTK_HOUR: i32 = 20;
pub const DTK_DAY: i32 = 21;
pub const DTK_WEEK: i32 = 22;
pub const DTK_MONTH: i32 = 23;
pub const DTK_QUARTER: i32 = 24;
pub const DTK_YEAR: i32 = 25;
pub const DTK_DECADE: i32 = 26;
pub const DTK_CENTURY: i32 = 27;
pub const DTK_MILLENNIUM: i32 = 28;
pub const DTK_MILLISEC: i32 = 29;
pub const DTK_MICROSEC: i32 = 30;
pub const DTK_JULIAN: i32 = 31;
pub const DTK_DOW: i32 = 32;
pub const DTK_DOY: i32 = 33;
pub const DTK_TZ_HOUR: i32 = 34;
pub const DTK_TZ_MINUTE: i32 = 35;
pub const DTK_ISOYEAR: i32 = 36;
pub const DTK_ISODOW: i32 = 37;

/// Bit mask for a time field type.
pub const fn DTK_M(t: i32) -> u32 {
    0x01 << t
}

pub const DTK_ALL_SECS_M: u32 = DTK_M(SECOND) | DTK_M(MILLISECOND) | DTK_M(MICROSECOND);
pub const DTK_DATE_M: u32 = DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY);
pub const DTK_TIME_M: u32 = DTK_M(HOUR) | DTK_M(MINUTE) | DTK_ALL_SECS_M;

pub const MAXDATELEN: usize = 128;
pub const MAXDATEFIELDS: usize = 25;
pub const TOKMAXLEN: usize = 10;

/// A datetime keyword-table entry. token is NUL-terminated, <= TOKMAXLEN chars.
pub struct datetkn {
    pub token: [u8; TOKMAXLEN + 1],
    pub type_: i8, // field type code (see consts above)
    pub value: i32,
}

/// Table of time zone abbreviations. C FAM `abbrevs[]` + trailing
/// DynamicZoneAbbrev(s) -> owned Vecs (in-memory cache, not on-disk).
pub struct TimeZoneAbbrevTable {
    pub tblsize: Size,
    pub abbrevs: Vec<datetkn>,
    pub dynamic: Vec<DynamicZoneAbbrev>,
}

/// Auxiliary data for a dynamic (non-fixed-offset) abbreviation.
pub struct DynamicZoneAbbrev {
    pub tz: Option<Box<pg_tz>>, // None if not yet looked up
    pub zone: String,           // zone name
}

/// FMODULO: split t into integer part q (towards +/-inf) and remainder t.
pub fn FMODULO(t: &mut f64, q: &mut f64, u: f64) {
    *q = if *t < 0.0 { (*t / u).ceil() } else { (*t / u).floor() };
    if *q != 0.0 {
        *t -= (*q * u).round();
    }
}

/// TMODULO: integer-division split (C99 truncate-toward-zero) on int64 timestamps.
pub fn TMODULO(t: &mut i64, q: &mut i64, u: i64) {
    *q = *t / u;
    if *q != 0 {
        *t -= *q * u;
    }
}

// Date/time names and month-length tables (from utils/adt/datetime.c). The C
// arrays carry a trailing NULL sentinel; here callers index by ordinal instead.
pub static months: &[&str] = &[
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
pub static days: &[&str] = &[
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
];
/// Days per month; row 0 = common year, row 1 = leap year (trailing 0 is the
/// C sentinel for a 1-based month index).
pub static day_tab: [[i32; 13]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
];

/// Gregorian leap-year test (used for all years per the SQL standard).
pub const fn isleap(y: i32) -> bool {
    (y % 4) == 0 && ((y % 100) != 0 || (y % 400) == 0)
}

// DateTimeParseError negative codes (returned by parsing routines on failure).
pub const DTERR_BAD_FORMAT: i32 = -1;
pub const DTERR_FIELD_OVERFLOW: i32 = -2;
pub const DTERR_MD_FIELD_OVERFLOW: i32 = -3; // triggers hint about DateStyle
pub const DTERR_INTERVAL_OVERFLOW: i32 = -4;
pub const DTERR_TZDISP_OVERFLOW: i32 = -5;
pub const DTERR_BAD_TIMEZONE: i32 = -6;
pub const DTERR_BAD_ZONE_ABBREV: i32 = -7;

/// Extra context for DateTimeParseError.
pub struct DateTimeErrorExtra {
    pub timezone: Option<String>, // incorrect time zone name
    pub abbrev: Option<String>,   // relevant time zone abbreviation
}

// Result codes for DecodeTimezoneName().
pub const TZNAME_FIXED_OFFSET: i32 = 0;
pub const TZNAME_DYNTZ: i32 = 1;
pub const TZNAME_ZONE: i32 = 2;

// =========================================================================
// Function bodies. Decode/encode core lives in
// src/backend/utils/adt/datetime.rs (plan-002 .c-defs invariant); re-exported
// here so callers keep using `crate::utils::datetime::*`.
// =========================================================================

pub use crate::backend::utils::adt::datetime::{
    date2j, j2date, j2day, AdjustTimestampForTypmod, DecodeDateTime, DecodeISO8601Interval,
    DecodeInterval, DecodeSpecial, DecodeTimeOnly, DecodeTimezone, DecodeUnits, EncodeDateOnly,
    EncodeDateTime, EncodeInterval, EncodeTimeOnly, ParseDateTime, ValidateDate, DATETKTBL,
    DAYS, DAY_TAB, DELTATKTBL, MONTHS,
};

// --- STAGED: paths that need the unported IANA timezone database / session
// timezone GUC, or planner nodes. TODO(timezone-db). ---

pub fn GetCurrentDateTime(_tm: &mut pg_tm) {
    unimplemented!()
}

/// out-params (fsec, tzp) -> tuple.
pub fn GetCurrentTimeUsec(_tm: &mut pg_tm) -> (fsec_t, i32) {
    unimplemented!()
}

pub fn DateTimeParseError(
    _dterr: i32,
    _extra: &DateTimeErrorExtra,
    _str: &str,
    _datatype: &str,
) {
    unimplemented!()
}

pub fn DetermineTimeZoneOffset(_tm: &mut pg_tm, _tzp: &pg_tz) -> i32 {
    unimplemented!()
}

pub fn DetermineTimeZoneAbbrevOffset(_tm: &mut pg_tm, _abbr: &str, _tzp: &pg_tz) -> i32 {
    unimplemented!()
}

/// out-param isdst -> tuple.
pub fn DetermineTimeZoneAbbrevOffsetTS(_ts: Timestamp, _abbr: &str, _tzp: &pg_tz) -> (i32, i32) {
    unimplemented!()
}

pub fn EncodeSpecialTimestamp(_dt: Timestamp, _str: &mut [u8]) {
    unimplemented!()
}

/// out-params (ftype, offset, tz) -> tuple, plus the int status.
pub fn DecodeTimezoneAbbrev(
    _field: i32,
    _lowtoken: &str,
    _extra: &mut DateTimeErrorExtra,
) -> (i32, i32, i32, Option<Box<pg_tz>>) {
    unimplemented!()
}

/// out-params (offset, tz) -> tuple with the int status.
pub fn DecodeTimezoneName(_tzname: &str) -> (i32, i32, Option<Box<pg_tz>>) {
    unimplemented!()
}

pub fn DecodeTimezoneNameToTz(_tzname: &str) -> Option<Box<pg_tz>> {
    unimplemented!()
}

/// out-params (offset, tz) -> tuple with the int status.
pub fn DecodeTimezoneAbbrevPrefix(_str: &str) -> (i32, i32, Option<Box<pg_tz>>) {
    unimplemented!()
}

pub fn ClearTimeZoneAbbrevCache() {
    unimplemented!()
}

// TemporalSimplify takes/returns a planner Node; out of scope for the skeleton.
pub fn TemporalSimplify(_max_precis: i32 /* node: &mut Node */) {
    unimplemented!()
}

pub fn CheckDateTokenTables() -> bool {
    unimplemented!()
}

// ConvertTimeZoneAbbrevs: struct tzEntry from utils/tzparser.h (out of this list).
pub fn ConvertTimeZoneAbbrevs(/* abbrevs: &[tzEntry] */) -> TimeZoneAbbrevTable {
    unimplemented!()
}

pub fn InstallTimeZoneAbbrevs(_tbl: &TimeZoneAbbrevTable) {
    unimplemented!()
}
