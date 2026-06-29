//! Shared date/time decode/encode core. Translated from
//! src/backend/utils/adt/datetime.c.
//!
//! This is the format-independent infrastructure that date.c/timestamp.c build
//! on: the keyword tables (`DATETKTBL`/`DELTATKTBL`), the Julian-date math
//! (`date2j`/`j2date`/`j2day`), the field-splitting state machine
//! (`ParseDateTime`), the decoders (`DecodeDateTime`/`DecodeTimeOnly`/
//! `DecodeInterval`/`DecodeISO8601Interval` and their numeric helpers), the
//! validators (`ValidateDate`, `DecodeTimezone`), and the encoders
//! (`EncodeDateOnly`/`EncodeTimeOnly`/`EncodeDateTime`/`EncodeInterval`).
//!
//! Representation matches PG exactly: Timestamp is i64 microseconds since
//! 2000-01-01 (POSTGRES epoch); Date is i32 days since 2000-01-01; Interval is
//! {time:i64 usec, day:i32, month:i32}. The `pg_tm`/`pg_itm`/`pg_itm_in`
//! broken-down forms are in-memory intermediates.
//!
//! Pointer-to-ownership (rules.md s10): the C `ParseDateTime` writes lowercased
//! NUL-terminated field substrings into a caller workbuf and returns `char **`
//! plus an `int *` type array. Here it returns owned `Vec<String>` fields and a
//! `Vec<i32>` of field types; the decoders take `&mut [String]` so they can do
//! the in-place truncations (`*cp = '\0'`) the C code relies on, against owned
//! buffers rather than raw pointers. Encoders write into a caller `&mut [u8]`
//! through an internal cursor instead of `pg_ultostr` pointer arithmetic.
//!
//! STAGED (needs the unported IANA timezone database / session GUCs): the named
//! pg_tz lookups (`pg_tzset`), dynamic-abbreviation resolution against a loaded
//! tz DB, the DST-boundary probing in `DetermineTimeZoneOffset`, the
//! `pg_timezone_names`/`pg_timezone_abbrevs_*` SRFs, and `TemporalSimplify`
//! (planner Node). Decode/encode of dates, times, timestamps, and intervals
//! WITHOUT a named-zone lookup is complete. See `// TODO(timezone-db)` markers.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "intentional C width arithmetic: datetime.c does explicit int/int64 \
              casts and fixed-width modular date math (the value-cast family is an \
              allowed port-inherent lint per rules.md s11)"
)]
#![allow(
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::branches_sharing_code,
    clippy::if_not_else,
    clippy::if_same_then_else,
    clippy::nonminimal_bool,
    reason = "1:1 port of datetime.c's long field decode/encode state machines: \
              the C control flow, single-letter date math vars (hr/min/sec/y/m/d), \
              duplicated decode-arm bodies, and explicit boolean predicates are \
              reproduced faithfully (rules.md s8)"
)]
#![allow(
    clippy::manual_range_contains,
    clippy::option_if_let_else,
    reason = "explicit lower/upper bound checks and match-on-Result mirror the C \
              range guards / strtoX error handling more legibly than the lint's \
              rewrites; faithful-port readability (rules.md s10)"
)]

use crate::common::int::{
    pg_add_s32_overflow, pg_add_s64_overflow, pg_mul_s32_overflow, pg_mul_s64_overflow,
};
use crate::datatype::timestamp::{
    fsec_t, pg_itm, pg_itm_in, Timestamp, DAYS_PER_MONTH, MAX_INTERVAL_PRECISION,
    MAX_TIMESTAMP_PRECISION, MONTHS_PER_YEAR, SECS_PER_DAY, SECS_PER_HOUR, SECS_PER_MINUTE,
    TIMESTAMP_NOT_FINITE, UNIX_EPOCH_JDATE, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE,
    USECS_PER_SEC,
};
use crate::miscadmin::{
    DateOrder, IntervalStyle, DATEORDER_DMY, DATEORDER_YMD, INTSTYLE_ISO_8601, INTSTYLE_POSTGRES,
    INTSTYLE_SQL_STANDARD, MAXTZLEN, USE_GERMAN_DATES, USE_ISO_DATES, USE_SQL_DATES, USE_XSD_DATES,
};
use crate::pgtime::pg_tm;
use crate::utils::datetime::{
    datetkn, DateTimeErrorExtra, ADBC, AGO, AMPM, DOW, DTERR_BAD_FORMAT, DTERR_FIELD_OVERFLOW,
    DTERR_MD_FIELD_OVERFLOW, DTERR_TZDISP_OVERFLOW, DTK_ALL_SECS_M, DTK_CENTURY, DTK_DATE,
    DTK_DATE_M, DTK_DAY, DTK_DECADE, DTK_DELTA, DTK_EARLY, DTK_EPOCH, DTK_HOUR, DTK_ISODOW,
    DTK_ISOYEAR, DTK_JULIAN, DTK_LATE, DTK_MICROSEC, DTK_MILLENNIUM, DTK_MILLISEC, DTK_MINUTE,
    DTK_MONTH, DTK_NOW, DTK_NUMBER, DTK_QUARTER, DTK_SECOND, DTK_SPECIAL, DTK_STRING, DTK_TIME,
    DTK_TIME_M, DTK_TODAY, DTK_TOMORROW, DTK_TZ, DTK_TZ_HOUR, DTK_TZ_MINUTE, DTK_WEEK, DTK_YEAR,
    DTK_ZULU, DTK_DOW as DTK_DOW_V, DTK_DOY as DTK_DOY_V, DTK_YESTERDAY, DTK_M, DAY, DECADE, DOY,
    DTZ, DTZMOD, DYNTZ, HOUR, IGNORE_DTF, ISODATE, ISOTIME, JULIAN, MAXDATEFIELDS, MICROSECOND,
    MILLENNIUM, MILLISECOND, MINUTE, MONTH, RESERV, SECOND, TOKMAXLEN, TZ, UNITS, UNKNOWN_FIELD,
    WEEK, YEAR, AM, PM, CENTURY, AD, BC, HR24,
};
use crate::utils::timestamp::{INTERVAL_FULL_RANGE, INTERVAL_MASK};

// --- Day/month-length helpers (re-exported through the header) ---

/// Days per month; row 0 = common year, row 1 = leap year.
pub static DAY_TAB: [[i32; 13]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
];

pub static MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

pub static DAYS: [&str; 7] = [
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
];

#[inline]
const fn isleap(y: i32) -> bool {
    (y % 4) == 0 && ((y % 100) != 0 || (y % 400) == 0)
}

const MINS_PER_HOUR: i32 = 60;
const HOURS_PER_DAY: i32 = 24;
const MAX_TZDISP_HOUR: i32 = 15;
const MAX_TIME_PRECISION: i32 = 6;

// Construct one keyword-table entry from a (token, type, value) triple.
const fn tk(token: &str, type_: i32, value: i32) -> datetkn {
    let b = token.as_bytes();
    let mut t = [0u8; TOKMAXLEN + 1];
    let mut i = 0;
    while i < b.len() && i < TOKMAXLEN {
        t[i] = b[i];
        i += 1;
    }
    datetkn {
        token: t,
        type_: type_ as i8,
        value,
    }
}

/// datetktbl holds date/time keywords. Must be strictly alphabetically ordered
/// (binary search). Contains no TZ/DTZ/DYNTZ entries (those load from config).
pub static DATETKTBL: &[datetkn] = &[
    tk("+infinity", RESERV, DTK_LATE),
    tk("-infinity", RESERV, DTK_EARLY),
    tk("ad", ADBC, AD),
    tk("allballs", RESERV, DTK_ZULU),
    tk("am", AMPM, AM),
    tk("apr", MONTH, 4),
    tk("april", MONTH, 4),
    tk("at", IGNORE_DTF, 0),
    tk("aug", MONTH, 8),
    tk("august", MONTH, 8),
    tk("bc", ADBC, BC),
    tk("d", UNITS, DTK_DAY),
    tk("dec", MONTH, 12),
    tk("december", MONTH, 12),
    tk("dow", UNITS, DTK_DOW_V),
    tk("doy", UNITS, DTK_DOY_V),
    tk("dst", DTZMOD, SECS_PER_HOUR),
    tk("epoch", RESERV, DTK_EPOCH),
    tk("feb", MONTH, 2),
    tk("february", MONTH, 2),
    tk("fri", DOW, 5),
    tk("friday", DOW, 5),
    tk("h", UNITS, DTK_HOUR),
    tk("infinity", RESERV, DTK_LATE),
    tk("isodow", UNITS, DTK_ISODOW),
    tk("isoyear", UNITS, DTK_ISOYEAR),
    tk("j", UNITS, DTK_JULIAN),
    tk("jan", MONTH, 1),
    tk("january", MONTH, 1),
    tk("jd", UNITS, DTK_JULIAN),
    tk("jul", MONTH, 7),
    tk("julian", UNITS, DTK_JULIAN),
    tk("july", MONTH, 7),
    tk("jun", MONTH, 6),
    tk("june", MONTH, 6),
    tk("m", UNITS, DTK_MONTH),
    tk("mar", MONTH, 3),
    tk("march", MONTH, 3),
    tk("may", MONTH, 5),
    tk("mm", UNITS, DTK_MINUTE),
    tk("mon", DOW, 1),
    tk("monday", DOW, 1),
    tk("nov", MONTH, 11),
    tk("november", MONTH, 11),
    tk("now", RESERV, DTK_NOW),
    tk("oct", MONTH, 10),
    tk("october", MONTH, 10),
    tk("on", IGNORE_DTF, 0),
    tk("pm", AMPM, PM),
    tk("s", UNITS, DTK_SECOND),
    tk("sat", DOW, 6),
    tk("saturday", DOW, 6),
    tk("sep", MONTH, 9),
    tk("sept", MONTH, 9),
    tk("september", MONTH, 9),
    tk("sun", DOW, 0),
    tk("sunday", DOW, 0),
    tk("t", ISOTIME, DTK_TIME),
    tk("thu", DOW, 4),
    tk("thur", DOW, 4),
    tk("thurs", DOW, 4),
    tk("thursday", DOW, 4),
    tk("today", RESERV, DTK_TODAY),
    tk("tomorrow", RESERV, DTK_TOMORROW),
    tk("tue", DOW, 2),
    tk("tues", DOW, 2),
    tk("tuesday", DOW, 2),
    tk("wed", DOW, 3),
    tk("wednesday", DOW, 3),
    tk("weds", DOW, 3),
    tk("y", UNITS, DTK_YEAR),
    tk("yesterday", RESERV, DTK_YESTERDAY),
];

/// deltatktbl: same format as DATETKTBL, but for time-unit keywords (intervals,
/// EXTRACT). Strictly alphabetically ordered.
pub static DELTATKTBL: &[datetkn] = &[
    tk("@", IGNORE_DTF, 0),
    tk("ago", AGO, 0),
    tk("c", UNITS, DTK_CENTURY),
    tk("cent", UNITS, DTK_CENTURY),
    tk("centuries", UNITS, DTK_CENTURY),
    tk("century", UNITS, DTK_CENTURY),
    tk("d", UNITS, DTK_DAY),
    tk("day", UNITS, DTK_DAY),
    tk("days", UNITS, DTK_DAY),
    tk("dec", UNITS, DTK_DECADE),
    tk("decade", UNITS, DTK_DECADE),
    tk("decades", UNITS, DTK_DECADE),
    tk("decs", UNITS, DTK_DECADE),
    tk("h", UNITS, DTK_HOUR),
    tk("hour", UNITS, DTK_HOUR),
    tk("hours", UNITS, DTK_HOUR),
    tk("hr", UNITS, DTK_HOUR),
    tk("hrs", UNITS, DTK_HOUR),
    tk("m", UNITS, DTK_MINUTE),
    tk("microsecon", UNITS, DTK_MICROSEC),
    tk("mil", UNITS, DTK_MILLENNIUM),
    tk("millennia", UNITS, DTK_MILLENNIUM),
    tk("millennium", UNITS, DTK_MILLENNIUM),
    tk("millisecon", UNITS, DTK_MILLISEC),
    tk("mils", UNITS, DTK_MILLENNIUM),
    tk("min", UNITS, DTK_MINUTE),
    tk("mins", UNITS, DTK_MINUTE),
    tk("minute", UNITS, DTK_MINUTE),
    tk("minutes", UNITS, DTK_MINUTE),
    tk("mon", UNITS, DTK_MONTH),
    tk("mons", UNITS, DTK_MONTH),
    tk("month", UNITS, DTK_MONTH),
    tk("months", UNITS, DTK_MONTH),
    tk("ms", UNITS, DTK_MILLISEC),
    tk("msec", UNITS, DTK_MILLISEC),
    tk("msecond", UNITS, DTK_MILLISEC),
    tk("mseconds", UNITS, DTK_MILLISEC),
    tk("msecs", UNITS, DTK_MILLISEC),
    tk("qtr", UNITS, DTK_QUARTER),
    tk("quarter", UNITS, DTK_QUARTER),
    tk("s", UNITS, DTK_SECOND),
    tk("sec", UNITS, DTK_SECOND),
    tk("second", UNITS, DTK_SECOND),
    tk("seconds", UNITS, DTK_SECOND),
    tk("secs", UNITS, DTK_SECOND),
    tk("timezone", UNITS, DTK_TZ),
    tk("timezone_h", UNITS, DTK_TZ_HOUR),
    tk("timezone_m", UNITS, DTK_TZ_MINUTE),
    tk("us", UNITS, DTK_MICROSEC),
    tk("usec", UNITS, DTK_MICROSEC),
    tk("usecond", UNITS, DTK_MICROSEC),
    tk("useconds", UNITS, DTK_MICROSEC),
    tk("usecs", UNITS, DTK_MICROSEC),
    tk("w", UNITS, DTK_WEEK),
    tk("week", UNITS, DTK_WEEK),
    tk("weeks", UNITS, DTK_WEEK),
    tk("y", UNITS, DTK_YEAR),
    tk("year", UNITS, DTK_YEAR),
    tk("years", UNITS, DTK_YEAR),
    tk("yr", UNITS, DTK_YEAR),
    tk("yrs", UNITS, DTK_YEAR),
];

// =========================================================================
// Julian date <-> calendar conversions
// =========================================================================

/// Calendar date -> Julian day. Accurate for all Julian days 0..i32::MAX.
#[must_use]
pub fn date2j(year: i32, month: i32, day: i32) -> i32 {
    let (mut month, mut year) = (month, year);
    if month > 2 {
        month += 1;
        year += 4800;
    } else {
        month += 13;
        year += 4799;
    }

    let century = year / 100;
    let mut julian = year * 365 - 32167;
    julian += year / 4 - century + century / 4;
    julian += 7834 * month / 256 + day;
    julian
}

/// Julian day -> (year, month, day).
#[must_use]
pub fn j2date(jd: i32) -> (i32, i32, i32) {
    let mut julian = jd as u32;
    julian = julian.wrapping_add(32044);
    let mut quad = julian / 146097;
    let extra = (julian - quad * 146097) * 4 + 3;
    julian = julian
        .wrapping_add(60)
        .wrapping_add(quad * 3)
        .wrapping_add(extra / 146097);
    quad = julian / 1461;
    julian -= quad * 1461;
    let mut y = julian * 4 / 1461;
    julian = (if y != 0 {
        (julian + 305) % 365
    } else {
        (julian + 306) % 366
    }) + 123;
    y += quad * 4;
    let year = y as i32 - 4800;
    quad = julian * 2141 / 65536;
    let day = (julian - 7834 * quad / 256) as i32;
    let month = ((quad + 10) % MONTHS_PER_YEAR as u32 + 1) as i32;
    (year, month, day)
}

/// Julian date -> day-of-week (0..6 == Sun..Sat).
#[must_use]
pub fn j2day(date: i32) -> i32 {
    let mut date = date + 1;
    date %= 7;
    if date < 0 {
        date += 7;
    }
    date
}

// Local copies of the tiny shared time helpers that canonically live in
// timestamp.c / date.c (not yet translated by the sibling agents); kept private
// so there's no clash when those land.

fn dt2time(jd: Timestamp) -> (i32, i32, i32, fsec_t) {
    let mut time = jd;
    let hour = (time / USECS_PER_HOUR) as i32;
    time -= i64::from(hour) * USECS_PER_HOUR;
    let min = (time / USECS_PER_MINUTE) as i32;
    time -= i64::from(min) * USECS_PER_MINUTE;
    let sec = (time / USECS_PER_SEC) as i32;
    let fsec = (time - i64::from(sec) * USECS_PER_SEC) as fsec_t;
    (hour, min, sec, fsec)
}

fn time_overflows(hour: i32, min: i32, sec: i32, fsec: fsec_t) -> bool {
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
    ((((i64::from(hour) * i64::from(MINS_PER_HOUR) + i64::from(min)) * i64::from(SECS_PER_MINUTE))
        + i64::from(sec))
        * USECS_PER_SEC
        + i64::from(fsec))
        > USECS_PER_DAY
}

// =========================================================================
// Field-string parsing (ParseDateTime)
// =========================================================================

const fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}
const fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}
const fn is_alpha(c: u8) -> bool {
    c.is_ascii_alphabetic()
}
const fn is_alnum(c: u8) -> bool {
    c.is_ascii_alphanumeric()
}
const fn is_punct(c: u8) -> bool {
    c.is_ascii_punctuation()
}
const fn to_lower(c: u8) -> u8 {
    c.to_ascii_lowercase()
}

/// ParseDateTime: break string into tokens based on a date/time context.
/// Returns the lowercased field substrings and their `DTK_*` field types, or a
/// `DTERR_*` code. (C's `field[]`/`ftype[]` out-params + workbuf -> owned Vecs.)
///
/// # Errors
/// Returns `DTERR_BAD_FORMAT` on malformed input or if more than `maxfields`
/// fields are present.
pub fn ParseDateTime(timestr: &str, maxfields: usize) -> Result<(Vec<String>, Vec<i32>), i32> {
    let s = timestr.as_bytes();
    let mut i = 0usize;
    let mut fields: Vec<String> = Vec::new();
    let mut ftype: Vec<i32> = Vec::new();

    while i < s.len() {
        // Ignore spaces between fields.
        if is_space(s[i]) {
            i += 1;
            continue;
        }
        if fields.len() >= maxfields {
            return Err(DTERR_BAD_FORMAT);
        }

        let mut buf: Vec<u8> = Vec::new();
        let cur_ft;

        if is_digit(s[i]) {
            // leading digit -> date or time
            buf.push(s[i]);
            i += 1;
            while i < s.len() && is_digit(s[i]) {
                buf.push(s[i]);
                i += 1;
            }

            if i < s.len() && s[i] == b':' {
                cur_ft = DTK_TIME;
                buf.push(s[i]);
                i += 1;
                while i < s.len() && (is_digit(s[i]) || s[i] == b':' || s[i] == b'.') {
                    buf.push(s[i]);
                    i += 1;
                }
            } else if i < s.len() && (s[i] == b'-' || s[i] == b'/' || s[i] == b'.') {
                let delim = s[i];
                buf.push(s[i]);
                i += 1;
                if i < s.len() && is_digit(s[i]) {
                    let mut ft = if delim == b'.' { DTK_NUMBER } else { DTK_DATE };
                    while i < s.len() && is_digit(s[i]) {
                        buf.push(s[i]);
                        i += 1;
                    }
                    if i < s.len() && s[i] == delim {
                        ft = DTK_DATE;
                        buf.push(s[i]);
                        i += 1;
                        while i < s.len() && (is_digit(s[i]) || s[i] == delim) {
                            buf.push(s[i]);
                            i += 1;
                        }
                    }
                    cur_ft = ft;
                } else {
                    cur_ft = DTK_DATE;
                    while i < s.len() && (is_alnum(s[i]) || s[i] == delim) {
                        buf.push(to_lower(s[i]));
                        i += 1;
                    }
                }
            } else {
                cur_ft = DTK_NUMBER;
            }
        } else if s[i] == b'.' {
            // leading decimal point -> fractional seconds
            buf.push(s[i]);
            i += 1;
            while i < s.len() && is_digit(s[i]) {
                buf.push(s[i]);
                i += 1;
            }
            cur_ft = DTK_NUMBER;
        } else if is_alpha(s[i]) {
            // text -> date string, month, dow, special, or timezone
            let mut ft = DTK_STRING;
            buf.push(to_lower(s[i]));
            i += 1;
            while i < s.len() && is_alpha(s[i]) {
                buf.push(to_lower(s[i]));
                i += 1;
            }

            let mut is_date = false;
            if i < s.len() && (s[i] == b'-' || s[i] == b'/' || s[i] == b'.') {
                is_date = true;
            } else if i < s.len() && (s[i] == b'+' || is_digit(s[i])) {
                // only the core token table, not TZ names
                let token = String::from_utf8_lossy(&buf).into_owned();
                if datebsearch(&token, DATETKTBL).is_none() {
                    is_date = true;
                }
            }
            if is_date {
                ft = DTK_DATE;
                loop {
                    if i >= s.len() {
                        break;
                    }
                    buf.push(to_lower(s[i]));
                    i += 1;
                    if !(i < s.len()
                        && (s[i] == b'+'
                            || s[i] == b'-'
                            || s[i] == b'/'
                            || s[i] == b'_'
                            || s[i] == b'.'
                            || s[i] == b':'
                            || is_alnum(s[i])))
                    {
                        break;
                    }
                }
            }
            cur_ft = ft;
        } else if s[i] == b'+' || s[i] == b'-' {
            // sign -> special or numeric timezone
            buf.push(s[i]);
            i += 1;
            while i < s.len() && is_space(s[i]) {
                i += 1;
            }
            if i < s.len() && is_digit(s[i]) {
                cur_ft = DTK_TZ;
                buf.push(s[i]);
                i += 1;
                while i < s.len()
                    && (is_digit(s[i]) || s[i] == b':' || s[i] == b'.' || s[i] == b'-')
                {
                    buf.push(s[i]);
                    i += 1;
                }
            } else if i < s.len() && is_alpha(s[i]) {
                cur_ft = DTK_SPECIAL;
                buf.push(to_lower(s[i]));
                i += 1;
                while i < s.len() && is_alpha(s[i]) {
                    buf.push(to_lower(s[i]));
                    i += 1;
                }
            } else {
                return Err(DTERR_BAD_FORMAT);
            }
        } else if is_punct(s[i]) {
            // ignore other punctuation but use as delimiter
            i += 1;
            continue;
        } else {
            return Err(DTERR_BAD_FORMAT);
        }

        fields.push(String::from_utf8_lossy(&buf).into_owned());
        ftype.push(cur_ft);
    }

    Ok((fields, ftype))
}

// =========================================================================
// Numeric parse helpers (strtoint-style)
// =========================================================================

/// Parse a leading (optionally signed) base-10 integer; returns the value and
/// the index of the first unconsumed byte. Mirrors C strtoi64/strtoint: an
/// empty parse yields cp == start.
fn strtoi64_at(b: &[u8], start: usize) -> Result<(i64, usize), i32> {
    let mut i = start;
    let neg = if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        let n = b[i] == b'-';
        i += 1;
        n
    } else {
        false
    };
    let digit_start = i;
    let mut acc: i64 = 0;
    let mut overflow = false;
    while i < b.len() && is_digit(b[i]) {
        let d = i64::from(b[i] - b'0');
        match acc.checked_mul(10).and_then(|v| v.checked_add(d)) {
            Some(v) => acc = v,
            None => overflow = true,
        }
        i += 1;
    }
    if i == digit_start {
        // no digits: cp stays at start (matches strtoint returning str)
        return Ok((0, start));
    }
    if overflow {
        return Err(DTERR_FIELD_OVERFLOW);
    }
    Ok((if neg { -acc } else { acc }, i))
}

fn strtoint_at(b: &[u8], start: usize) -> Result<(i32, usize), i32> {
    let (v, end) = strtoi64_at(b, start)?;
    if v > i64::from(i32::MAX) || v < i64::from(i32::MIN) {
        return Err(DTERR_FIELD_OVERFLOW);
    }
    Ok((v as i32, end))
}

/// Parse the fractional part ('.' + optional digits) at the start of `cp`,
/// returning the value in 0..1. Rejects non-digit trailing content.
fn parse_fraction(cp: &str) -> Result<f64, i32> {
    let b = cp.as_bytes();
    debug_assert_eq!(b[0], b'.');
    if b.len() == 1 {
        return Ok(0.0);
    }
    for &c in &b[1..] {
        if !is_digit(c) {
            return Err(DTERR_BAD_FORMAT);
        }
    }
    cp.parse::<f64>().map_err(|_| DTERR_BAD_FORMAT)
}

fn parse_fractional_second(cp: &str) -> Result<fsec_t, i32> {
    let frac = parse_fraction(cp)?;
    Ok((frac * 1_000_000.0).round() as fsec_t)
}

// =========================================================================
// Interval accumulation helpers (operate on pg_itm_in)
// =========================================================================

fn int64_multiply_add(val: i64, multiplier: i64, sum: &mut i64) -> bool {
    let Some(product) = pg_mul_s64_overflow(val, multiplier) else {
        return false;
    };
    let Some(s) = pg_add_s64_overflow(*sum, product) else {
        return false;
    };
    *sum = s;
    true
}

fn adjust_fract_microseconds(mut frac: f64, scale: i64, itm_in: &mut pg_itm_in) -> bool {
    if frac == 0.0 {
        return true;
    }
    frac *= scale as f64;
    let mut usec = frac as i64;
    frac -= usec as f64;
    if frac > 0.5 {
        usec += 1;
    } else if frac < -0.5 {
        usec -= 1;
    }
    match pg_add_s64_overflow(itm_in.usec, usec) {
        Some(v) => {
            itm_in.usec = v;
            true
        }
        None => false,
    }
}

fn adjust_fract_days(mut frac: f64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    if frac == 0.0 {
        return true;
    }
    frac *= f64::from(scale);
    let extra_days = frac as i32;
    match pg_add_s32_overflow(itm_in.mday, extra_days) {
        Some(v) => itm_in.mday = v,
        None => return false,
    }
    frac -= f64::from(extra_days);
    adjust_fract_microseconds(frac, USECS_PER_DAY, itm_in)
}

fn adjust_fract_years(frac: f64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    let extra_months = (frac * f64::from(scale) * f64::from(MONTHS_PER_YEAR)).round() as i32;
    match pg_add_s32_overflow(itm_in.mon, extra_months) {
        Some(v) => {
            itm_in.mon = v;
            true
        }
        None => false,
    }
}

fn adjust_microseconds(val: i64, fval: f64, scale: i64, itm_in: &mut pg_itm_in) -> bool {
    if !int64_multiply_add(val, scale, &mut itm_in.usec) {
        return false;
    }
    adjust_fract_microseconds(fval, scale, itm_in)
}

fn adjust_days(val: i64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    if val < i64::from(i32::MIN) || val > i64::from(i32::MAX) {
        return false;
    }
    let Some(days) = pg_mul_s32_overflow(val as i32, scale) else {
        return false;
    };
    match pg_add_s32_overflow(itm_in.mday, days) {
        Some(v) => {
            itm_in.mday = v;
            true
        }
        None => false,
    }
}

fn adjust_months(val: i64, itm_in: &mut pg_itm_in) -> bool {
    if val < i64::from(i32::MIN) || val > i64::from(i32::MAX) {
        return false;
    }
    match pg_add_s32_overflow(itm_in.mon, val as i32) {
        Some(v) => {
            itm_in.mon = v;
            true
        }
        None => false,
    }
}

fn adjust_years(val: i64, scale: i32, itm_in: &mut pg_itm_in) -> bool {
    if val < i64::from(i32::MIN) || val > i64::from(i32::MAX) {
        return false;
    }
    let Some(years) = pg_mul_s32_overflow(val as i32, scale) else {
        return false;
    };
    match pg_add_s32_overflow(itm_in.year, years) {
        Some(v) => {
            itm_in.year = v;
            true
        }
        None => false,
    }
}

fn clear_pg_itm_in(itm_in: &mut pg_itm_in) {
    itm_in.usec = 0;
    itm_in.mday = 0;
    itm_in.mon = 0;
    itm_in.year = 0;
}

// =========================================================================
// Binary search over the keyword tables
// =========================================================================

fn token_str(t: &datetkn) -> &str {
    let end = t.token.iter().position(|&c| c == 0).unwrap_or(t.token.len());
    // Tokens are ASCII by construction.
    core::str::from_utf8(&t.token[..end]).unwrap_or("")
}

/// Binary search; matches on the first TOKMAXLEN bytes (truncated tokens).
fn datebsearch<'a>(key: &str, base: &'a [datetkn]) -> Option<&'a datetkn> {
    if base.is_empty() {
        return None;
    }
    let kb = key.as_bytes();
    let (mut lo, mut hi) = (0i64, base.len() as i64 - 1);
    while hi >= lo {
        let pos = lo + ((hi - lo) >> 1);
        let tp = &base[pos as usize];
        let tb = token_str(tp).as_bytes();
        // strncmp(key, token, TOKMAXLEN)
        let mut result = 0i32;
        let mut n = 0usize;
        while n < TOKMAXLEN {
            let kc = kb.get(n).copied().unwrap_or(0);
            let tc = tb.get(n).copied().unwrap_or(0);
            result = i32::from(kc) - i32::from(tc);
            if result != 0 || kc == 0 {
                break;
            }
            n += 1;
        }
        if result == 0 {
            return Some(tp);
        }
        if result < 0 {
            hi = pos - 1;
        } else {
            lo = pos + 1;
        }
    }
    None
}

/// DecodeSpecial: look up a token in DATETKTBL. Returns (type, value).
#[must_use]
pub fn DecodeSpecial(lowtoken: &str) -> (i32, i32) {
    match datebsearch(lowtoken, DATETKTBL) {
        Some(tp) => (i32::from(tp.type_), tp.value),
        None => (UNKNOWN_FIELD, 0),
    }
}

/// DecodeUnits: look up a time-unit keyword in DELTATKTBL. Returns (type,value).
#[must_use]
pub fn DecodeUnits(lowtoken: &str) -> (i32, i32) {
    match datebsearch(lowtoken, DELTATKTBL) {
        Some(tp) => (i32::from(tp.type_), tp.value),
        None => (UNKNOWN_FIELD, 0),
    }
}

// =========================================================================
// Date / number / time field decoders
// =========================================================================

/// DecodeNumberField: interpret a numeric string as a concatenated date/time
/// field. Returns a `DTK_*` token (>=0) on success, a `DTERR_*` code (<0)
/// otherwise. Mutates `field` (truncating the fraction) like the C version.
fn decode_number_field(
    field: &mut String,
    fmask: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    is2digits: &mut bool,
) -> i32 {
    // Reject anything not digits and '.'.
    if !field.bytes().all(|c| is_digit(c) || c == b'.') {
        return DTERR_BAD_FORMAT;
    }

    if let Some(dot) = field.find('.') {
        match parse_fractional_second(&field[dot..]) {
            Ok(v) => *fsec = v,
            Err(e) => return e,
        }
        field.truncate(dot);
    } else if (fmask & DTK_DATE_M as i32) != DTK_DATE_M as i32 {
        let len = field.len();
        if len >= 6 {
            *tmask = DTK_DATE_M as i32;
            let b = field.as_bytes();
            tm.mday = atoi(&b[len - 2..]);
            tm.mon = atoi(&b[len - 4..len - 2]);
            tm.year = atoi(&b[..len - 4]);
            if (len - 4) == 2 {
                *is2digits = true;
            }
            return DTK_DATE;
        }
    }

    let len = field.len();
    if (fmask & DTK_TIME_M as i32) != DTK_TIME_M as i32 {
        let b = field.as_bytes();
        if len == 6 {
            *tmask = DTK_TIME_M as i32;
            tm.sec = atoi(&b[4..]);
            tm.min = atoi(&b[2..4]);
            tm.hour = atoi(&b[..2]);
            return DTK_TIME;
        } else if len == 4 {
            *tmask = DTK_TIME_M as i32;
            tm.sec = 0;
            tm.min = atoi(&b[2..]);
            tm.hour = atoi(&b[..2]);
            return DTK_TIME;
        }
    }

    DTERR_BAD_FORMAT
}

fn atoi(b: &[u8]) -> i32 {
    let mut acc = 0i32;
    for &c in b {
        if !is_digit(c) {
            break;
        }
        acc = acc.wrapping_mul(10).wrapping_add(i32::from(c - b'0'));
    }
    acc
}

/// DecodeNumber: interpret a plain numeric field as a date value in context.
fn decode_number(
    flen: usize,
    field: &mut String,
    have_text_month: bool,
    fmask: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    is2digits: &mut bool,
) -> i32 {
    *tmask = 0;
    let b = field.as_bytes();
    let (val, cp) = match strtoint_at(b, 0) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if cp == 0 {
        return DTERR_BAD_FORMAT;
    }

    if cp < b.len() && b[cp] == b'.' {
        if cp > 2 {
            let r = decode_number_field(
                field,
                fmask | DTK_DATE_M as i32,
                tmask,
                tm,
                fsec,
                is2digits,
            );
            if r < 0 {
                return r;
            }
            return 0;
        }
        let frac = &field[cp..];
        match parse_fractional_second(frac) {
            Ok(v) => *fsec = v,
            Err(e) => return e,
        }
    } else if cp != b.len() {
        return DTERR_BAD_FORMAT;
    }

    // Special case for day of year.
    if flen == 3
        && (fmask & DTK_DATE_M as i32) == DTK_M(YEAR) as i32
        && (1..=366).contains(&val)
    {
        *tmask = (DTK_M(DOY) | DTK_M(MONTH) | DTK_M(DAY)) as i32;
        tm.yday = val;
        return 0;
    }

    match fmask & DTK_DATE_M as i32 {
        0 => {
            if flen >= 3 || unsafe { DateOrder } == DATEORDER_YMD {
                *tmask = DTK_M(YEAR) as i32;
                tm.year = val;
            } else if unsafe { DateOrder } == DATEORDER_DMY {
                *tmask = DTK_M(DAY) as i32;
                tm.mday = val;
            } else {
                *tmask = DTK_M(MONTH) as i32;
                tm.mon = val;
            }
        }
        x if x == DTK_M(YEAR) as i32 => {
            *tmask = DTK_M(MONTH) as i32;
            tm.mon = val;
        }
        x if x == DTK_M(MONTH) as i32 => {
            if have_text_month {
                if flen >= 3 || unsafe { DateOrder } == DATEORDER_YMD {
                    *tmask = DTK_M(YEAR) as i32;
                    tm.year = val;
                } else {
                    *tmask = DTK_M(DAY) as i32;
                    tm.mday = val;
                }
            } else {
                *tmask = DTK_M(DAY) as i32;
                tm.mday = val;
            }
        }
        x if x == (DTK_M(YEAR) | DTK_M(MONTH)) as i32 => {
            if have_text_month {
                if flen >= 3 && *is2digits {
                    *tmask = DTK_M(DAY) as i32;
                    tm.mday = tm.year;
                    tm.year = val;
                    *is2digits = false;
                } else {
                    *tmask = DTK_M(DAY) as i32;
                    tm.mday = val;
                }
            } else {
                *tmask = DTK_M(DAY) as i32;
                tm.mday = val;
            }
        }
        x if x == DTK_M(DAY) as i32 => {
            *tmask = DTK_M(MONTH) as i32;
            tm.mon = val;
        }
        x if x == (DTK_M(MONTH) | DTK_M(DAY)) as i32 => {
            *tmask = DTK_M(YEAR) as i32;
            tm.year = val;
        }
        x if x == (DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY)) as i32 => {
            let r = decode_number_field(field, fmask, tmask, tm, fsec, is2digits);
            if r < 0 {
                return r;
            }
            return 0;
        }
        _ => return DTERR_BAD_FORMAT,
    }

    if *tmask == DTK_M(YEAR) as i32 {
        *is2digits = flen <= 2;
    }

    0
}

/// DecodeDate: decode a delimited date string into `tm`. Returns 0 or DTERR.
fn decode_date(
    str_: &str,
    mut fmask: i32,
    tmask: &mut i32,
    is2digits: &mut bool,
    tm: &mut pg_tm,
) -> i32 {
    let mut fsec: fsec_t = 0;
    let mut fields: Vec<String> = Vec::new();
    let mut have_text_month = false;
    *tmask = 0;

    let b = str_.as_bytes();
    let mut i = 0usize;
    while i < b.len() && fields.len() < MAXDATEFIELDS {
        while i < b.len() && !is_alnum(b[i]) {
            i += 1;
        }
        if i >= b.len() {
            return DTERR_BAD_FORMAT;
        }
        let start = i;
        if is_digit(b[i]) {
            while i < b.len() && is_digit(b[i]) {
                i += 1;
            }
        } else if is_alpha(b[i]) {
            while i < b.len() && is_alpha(b[i]) {
                i += 1;
            }
        }
        fields.push(String::from_utf8_lossy(&b[start..i]).into_owned());
        if i < b.len() {
            i += 1;
        }
    }

    let mut consumed = vec![false; fields.len()];

    // Text fields first (unambiguous month).
    for idx in 0..fields.len() {
        if fields[idx].as_bytes().first().is_some_and(|&c| is_alpha(c)) {
            let (type_, val) = DecodeSpecial(&fields[idx]);
            if type_ == IGNORE_DTF {
                continue;
            }
            let dmask = DTK_M(MONTH) as i32;
            if type_ == MONTH {
                tm.mon = val;
                have_text_month = true;
            } else {
                return DTERR_BAD_FORMAT;
            }
            if fmask & dmask != 0 {
                return DTERR_BAD_FORMAT;
            }
            fmask |= dmask;
            *tmask |= dmask;
            consumed[idx] = true;
        }
    }

    // Remaining numeric fields.
    for idx in 0..fields.len() {
        if consumed[idx] {
            continue;
        }
        let len = fields[idx].len();
        if len == 0 {
            return DTERR_BAD_FORMAT;
        }
        let mut dmask = 0i32;
        let r = decode_number(
            len,
            &mut fields[idx],
            have_text_month,
            fmask,
            &mut dmask,
            tm,
            &mut fsec,
            is2digits,
        );
        if r != 0 {
            return r;
        }
        if fmask & dmask != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= dmask;
        *tmask |= dmask;
    }

    if (fmask & !((DTK_M(DOY) | DTK_M(TZ)) as i32)) != DTK_DATE_M as i32 {
        return DTERR_BAD_FORMAT;
    }
    0
}

/// ValidateDate: range-check year/month/day, handle BC and DOY.
///
/// # Errors
/// Returns a `DTERR_*` code if any field is out of range.
pub fn ValidateDate(
    fmask: i32,
    isjulian: bool,
    is2digits: bool,
    bc: bool,
    tm: &mut pg_tm,
) -> i32 {
    if fmask & DTK_M(YEAR) as i32 != 0 {
        if isjulian {
            // tm.year correct as-is
        } else if bc {
            if tm.year <= 0 {
                return DTERR_FIELD_OVERFLOW;
            }
            tm.year = -(tm.year - 1);
        } else if is2digits {
            if tm.year < 0 {
                return DTERR_FIELD_OVERFLOW;
            }
            if tm.year < 70 {
                tm.year += 2000;
            } else if tm.year < 100 {
                tm.year += 1900;
            }
        } else if tm.year <= 0 {
            return DTERR_FIELD_OVERFLOW;
        }
    }

    if fmask & DTK_M(DOY) as i32 != 0 {
        let (y, m, d) = j2date(date2j(tm.year, 1, 1) + tm.yday - 1);
        tm.year = y;
        tm.mon = m;
        tm.mday = d;
    }

    if fmask & DTK_M(MONTH) as i32 != 0 && (tm.mon < 1 || tm.mon > MONTHS_PER_YEAR) {
        return DTERR_MD_FIELD_OVERFLOW;
    }

    if fmask & DTK_M(DAY) as i32 != 0 && (tm.mday < 1 || tm.mday > 31) {
        return DTERR_MD_FIELD_OVERFLOW;
    }

    if (fmask & DTK_DATE_M as i32) == DTK_DATE_M as i32
        && tm.mday > DAY_TAB[usize::from(isleap(tm.year))][(tm.mon - 1) as usize]
    {
        return DTERR_FIELD_OVERFLOW;
    }

    0
}

/// DecodeTimeCommon: decode a delimited time string into a pg_itm. Shared by the
/// timestamp and interval decoders. Returns 0 or DTERR.
fn decode_time_common(str_: &str, range: i32, tmask: &mut i32, itm: &mut pg_itm) -> i32 {
    let b = str_.as_bytes();
    let mut fsec: fsec_t = 0;
    *tmask = DTK_TIME_M as i32;

    let (hour, mut cp) = match strtoi64_at(b, 0) {
        Ok(v) => v,
        Err(e) => return e,
    };
    itm.hour = hour;
    if cp >= b.len() || b[cp] != b':' {
        return DTERR_BAD_FORMAT;
    }
    let (min, ncp) = match strtoint_at(b, cp + 1) {
        Ok(v) => v,
        Err(e) => return e,
    };
    itm.min = min;
    cp = ncp;

    if cp >= b.len() {
        itm.sec = 0;
        if range == (INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)) {
            if itm.hour > i64::from(i32::MAX) || itm.hour < i64::from(i32::MIN) {
                return DTERR_FIELD_OVERFLOW;
            }
            itm.sec = itm.min;
            itm.min = itm.hour as i32;
            itm.hour = 0;
        }
    } else if b[cp] == b'.' {
        match parse_fractional_second(&str_[cp..]) {
            Ok(v) => fsec = v,
            Err(e) => return e,
        }
        if itm.hour > i64::from(i32::MAX) || itm.hour < i64::from(i32::MIN) {
            return DTERR_FIELD_OVERFLOW;
        }
        itm.sec = itm.min;
        itm.min = itm.hour as i32;
        itm.hour = 0;
    } else if b[cp] == b':' {
        let (sec, ncp) = match strtoint_at(b, cp + 1) {
            Ok(v) => v,
            Err(e) => return e,
        };
        itm.sec = sec;
        cp = ncp;
        if cp < b.len() && b[cp] == b'.' {
            match parse_fractional_second(&str_[cp..]) {
                Ok(v) => fsec = v,
                Err(e) => return e,
            }
        } else if cp != b.len() {
            return DTERR_BAD_FORMAT;
        }
    } else {
        return DTERR_BAD_FORMAT;
    }

    if itm.hour < 0
        || itm.min < 0
        || itm.min > MINS_PER_HOUR - 1
        || itm.sec < 0
        || itm.sec > SECS_PER_MINUTE
        || fsec < 0
        || i64::from(fsec) > USECS_PER_SEC
    {
        return DTERR_FIELD_OVERFLOW;
    }
    itm.usec = fsec;
    0
}

fn decode_time(
    str_: &str,
    range: i32,
    tmask: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
) -> i32 {
    let mut itm = pg_itm::default();
    let r = decode_time_common(str_, range, tmask, &mut itm);
    if r != 0 {
        return r;
    }
    if itm.hour > i64::from(i32::MAX) {
        return DTERR_FIELD_OVERFLOW;
    }
    tm.hour = itm.hour as i32;
    tm.min = itm.min;
    tm.sec = itm.sec;
    *fsec = itm.usec;
    0
}

fn decode_time_for_interval(str_: &str, range: i32, tmask: &mut i32, itm_in: &mut pg_itm_in) -> i32 {
    let mut itm = pg_itm::default();
    let r = decode_time_common(str_, range, tmask, &mut itm);
    if r != 0 {
        return r;
    }
    itm_in.usec = i64::from(itm.usec);
    if !int64_multiply_add(itm.hour, USECS_PER_HOUR, &mut itm_in.usec)
        || !int64_multiply_add(i64::from(itm.min), USECS_PER_MINUTE, &mut itm_in.usec)
        || !int64_multiply_add(i64::from(itm.sec), USECS_PER_SEC, &mut itm_in.usec)
    {
        return DTERR_FIELD_OVERFLOW;
    }
    0
}

/// DecodeTimezone: interpret a string as a numeric timezone. Returns 0 (and
/// sets `*tzp`) or a `DTERR_*` code.
///
/// # Errors
/// Returns a `DTERR_*` code on malformed input or out-of-range displacement.
pub fn DecodeTimezone(str_: &str, tzp: &mut i32) -> i32 {
    let b = str_.as_bytes();
    if b.is_empty() || (b[0] != b'+' && b[0] != b'-') {
        return DTERR_BAD_FORMAT;
    }
    let Ok((mut hr, mut cp)) = strtoint_at(b, 1) else {
        return DTERR_TZDISP_OVERFLOW;
    };
    let mut min;
    let mut sec = 0i32;

    if cp < b.len() && b[cp] == b':' {
        let Ok((m, ncp)) = strtoint_at(b, cp + 1) else {
            return DTERR_TZDISP_OVERFLOW;
        };
        min = m;
        cp = ncp;
        if cp < b.len() && b[cp] == b':' {
            let Ok((s, ncp2)) = strtoint_at(b, cp + 1) else {
                return DTERR_TZDISP_OVERFLOW;
            };
            sec = s;
            cp = ncp2;
        }
    } else if cp >= b.len() && b.len() > 3 {
        min = hr % 100;
        hr /= 100;
    } else {
        min = 0;
    }

    if hr < 0 || hr > MAX_TZDISP_HOUR {
        return DTERR_TZDISP_OVERFLOW;
    }
    if min < 0 || min >= MINS_PER_HOUR {
        return DTERR_TZDISP_OVERFLOW;
    }
    if sec < 0 || sec >= SECS_PER_MINUTE {
        return DTERR_TZDISP_OVERFLOW;
    }

    let mut tz = (hr * MINS_PER_HOUR + min) * SECS_PER_MINUTE + sec;
    if b[0] == b'-' {
        tz = -tz;
    }
    *tzp = -tz;
    let _ = &mut min;
    if cp != b.len() {
        return DTERR_BAD_FORMAT;
    }
    0
}

// =========================================================================
// DecodeDateTime / DecodeTimeOnly
// =========================================================================

/// DecodeDateTime: interpret parsed fields for a general date and time.
/// Returns 0 (full date), 1 (time only), or a negative `DTERR_*` code.
/// `field`/`ftype` come from [`ParseDateTime`]; the date/time goes into `tm`,
/// fractional seconds into `fsec`, and the numeric tz offset into `tzp`.
///
/// STAGED: named-zone (`pg_tzset`) and dynamic-abbreviation handling default to
/// returning `DTERR_BAD_TIMEZONE`/UTC since the IANA tz DB isn't ported.
///
/// # Errors
/// Returns a negative `DTERR_*` code on malformed or out-of-range input.
#[allow(clippy::too_many_lines, reason = "1:1 port of the C state machine")]
pub fn DecodeDateTime(
    field: &mut [String],
    ftype: &mut [i32],
    dtype: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzp: Option<&mut i32>,
    _extra: &mut DateTimeErrorExtra,
) -> i32 {
    let nf = field.len();
    let mut fmask = 0i32;
    let mut ptype = 0i32;
    let mut mer = HR24;
    let mut have_text_month = false;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut have_tz = false;
    let mut tz_val = 0i32;
    let want_tz = tzp.is_some();

    *dtype = DTK_DATE;
    tm.hour = 0;
    tm.min = 0;
    tm.sec = 0;
    *fsec = 0;
    tm.isdst = -1;

    for i in 0..nf {
        let mut tmask = 0i32;
        match ftype[i] {
            x if x == DTK_DATE => {
                if ptype == DTK_JULIAN {
                    if !want_tz {
                        return DTERR_BAD_FORMAT;
                    }
                    let b = field[i].as_bytes();
                    let Ok((jday, cp)) = strtoint_at(b, 0) else {
                        return DTERR_FIELD_OVERFLOW;
                    };
                    if jday < 0 {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    let (y, m, d) = j2date(jday);
                    tm.year = y;
                    tm.mon = m;
                    tm.mday = d;
                    isjulian = true;
                    let r = DecodeTimezone(&field[i][cp..], &mut tz_val);
                    if r != 0 {
                        return r;
                    }
                    have_tz = true;
                    tmask = (DTK_DATE_M | DTK_TIME_M | DTK_M(TZ)) as i32;
                    ptype = 0;
                } else if ptype != 0
                    || (fmask & (DTK_M(MONTH) | DTK_M(DAY)) as i32)
                        == (DTK_M(MONTH) | DTK_M(DAY)) as i32
                {
                    if !want_tz {
                        return DTERR_BAD_FORMAT;
                    }
                    let first = field[i].as_bytes()[0];
                    if is_digit(first) || ptype != 0 {
                        if ptype != 0 {
                            if ptype != DTK_TIME {
                                return DTERR_BAD_FORMAT;
                            }
                            ptype = 0;
                        }
                        if (fmask & DTK_TIME_M as i32) == DTK_TIME_M as i32 {
                            return DTERR_BAD_FORMAT;
                        }
                        let Some(dashpos) = field[i].find('-') else {
                            return DTERR_BAD_FORMAT;
                        };
                        let r = DecodeTimezone(&field[i][dashpos..], &mut tz_val);
                        if r != 0 {
                            return r;
                        }
                        have_tz = true;
                        field[i].truncate(dashpos);
                        let mut head = std::mem::take(&mut field[i]);
                        let r = decode_number_field(
                            &mut head, fmask, &mut tmask, tm, fsec, &mut is2digits,
                        );
                        field[i] = head;
                        if r < 0 {
                            return r;
                        }
                        tmask |= DTK_M(TZ) as i32;
                    } else {
                        // STAGED: named timezone lookup needs the tz DB.
                        // TODO(timezone-db): pg_tzset(field[i]); resolve below.
                        return DTERR_BAD_FORMAT;
                    }
                } else {
                    let r = decode_date(&field[i], fmask, &mut tmask, &mut is2digits, tm);
                    if r != 0 {
                        return r;
                    }
                }
            }
            x if x == DTK_TIME => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = 0;
                }
                let r = decode_time(&field[i], INTERVAL_FULL_RANGE, &mut tmask, tm, fsec);
                if r != 0 {
                    return r;
                }
                if time_overflows(tm.hour, tm.min, tm.sec, *fsec) {
                    return DTERR_FIELD_OVERFLOW;
                }
            }
            x if x == DTK_TZ => {
                if !want_tz {
                    return DTERR_BAD_FORMAT;
                }
                let mut tz = 0i32;
                let r = DecodeTimezone(&field[i], &mut tz);
                if r != 0 {
                    return r;
                }
                tz_val = tz;
                have_tz = true;
                tmask = DTK_M(TZ) as i32;
            }
            x if x == DTK_NUMBER => {
                if ptype != 0 {
                    let b = field[i].as_bytes();
                    let Ok((value, cp)) = strtoint_at(b, 0) else {
                        return DTERR_FIELD_OVERFLOW;
                    };
                    if cp < b.len() && b[cp] != b'.' {
                        return DTERR_BAD_FORMAT;
                    }
                    match ptype {
                        p if p == DTK_JULIAN => {
                            if value < 0 {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            tmask = DTK_DATE_M as i32;
                            let (y, m, d) = j2date(value);
                            tm.year = y;
                            tm.mon = m;
                            tm.mday = d;
                            isjulian = true;
                            if cp < b.len() && b[cp] == b'.' {
                                let t = match parse_fraction(&field[i][cp..]) {
                                    Ok(v) => v,
                                    Err(e) => return e,
                                };
                                let usec = (t * USECS_PER_DAY as f64) as i64;
                                let (h, mi, s, fs) = dt2time(usec);
                                tm.hour = h;
                                tm.min = mi;
                                tm.sec = s;
                                *fsec = fs;
                                tmask |= DTK_TIME_M as i32;
                            }
                        }
                        p if p == DTK_TIME => {
                            let mut head = std::mem::take(&mut field[i]);
                            let r = decode_number_field(
                                &mut head,
                                fmask | DTK_DATE_M as i32,
                                &mut tmask,
                                tm,
                                fsec,
                                &mut is2digits,
                            );
                            field[i] = head;
                            if r < 0 {
                                return r;
                            }
                            if tmask != DTK_TIME_M as i32 {
                                return DTERR_BAD_FORMAT;
                            }
                        }
                        _ => return DTERR_BAD_FORMAT,
                    }
                    ptype = 0;
                    *dtype = DTK_DATE;
                } else {
                    let flen = field[i].len();
                    let dotpos = field[i].find('.');
                    if dotpos.is_some() && (fmask & DTK_DATE_M as i32) == 0 {
                        let r = decode_date(&field[i], fmask, &mut tmask, &mut is2digits, tm);
                        if r != 0 {
                            return r;
                        }
                    } else if dotpos.is_some_and(|d| flen - (flen - d) > 2) {
                        let mut head = std::mem::take(&mut field[i]);
                        let r = decode_number_field(
                            &mut head, fmask, &mut tmask, tm, fsec, &mut is2digits,
                        );
                        field[i] = head;
                        if r < 0 {
                            return r;
                        }
                    } else if flen >= 6
                        && ((fmask & DTK_DATE_M as i32) == 0
                            || (fmask & DTK_TIME_M as i32) == 0)
                    {
                        let mut head = std::mem::take(&mut field[i]);
                        let r = decode_number_field(
                            &mut head, fmask, &mut tmask, tm, fsec, &mut is2digits,
                        );
                        field[i] = head;
                        if r < 0 {
                            return r;
                        }
                    } else {
                        let r = decode_number(
                            flen,
                            &mut field[i],
                            have_text_month,
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if r != 0 {
                            return r;
                        }
                    }
                }
            }
            x if x == DTK_STRING || x == DTK_SPECIAL => {
                // STAGED: timezone-abbrev lookup needs the tz DB; fall straight
                // through to the built-in keyword table.
                // TODO(timezone-db): DecodeTimezoneAbbrev first.
                let (type_, val) = DecodeSpecial(&field[i]);
                if type_ == IGNORE_DTF {
                    continue;
                }
                tmask = DTK_M(type_) as i32;
                match type_ {
                    t if t == RESERV => match val {
                        v if v == DTK_NOW => {
                            // STAGED: now() needs txn start time + session tz.
                            // TODO(timezone-db): GetCurrentTimeUsec.
                            return DTERR_BAD_FORMAT;
                        }
                        v if v == DTK_YESTERDAY || v == DTK_TODAY || v == DTK_TOMORROW => {
                            // STAGED: needs GetCurrentDateTime.
                            // TODO(timezone-db)
                            return DTERR_BAD_FORMAT;
                        }
                        v if v == DTK_ZULU => {
                            tmask = (DTK_TIME_M | DTK_M(TZ)) as i32;
                            *dtype = DTK_DATE;
                            tm.hour = 0;
                            tm.min = 0;
                            tm.sec = 0;
                            if want_tz {
                                tz_val = 0;
                                have_tz = true;
                            }
                        }
                        v if v == DTK_EPOCH || v == DTK_LATE || v == DTK_EARLY => {
                            tmask = (DTK_DATE_M | DTK_TIME_M | DTK_M(TZ)) as i32;
                            *dtype = val;
                        }
                        _ => return DTERR_BAD_FORMAT,
                    },
                    t if t == MONTH => {
                        if (fmask & DTK_M(MONTH) as i32) != 0
                            && !have_text_month
                            && (fmask & DTK_M(DAY) as i32) == 0
                            && tm.mon >= 1
                            && tm.mon <= 31
                        {
                            tm.mday = tm.mon;
                            tmask = DTK_M(DAY) as i32;
                        }
                        have_text_month = true;
                        tm.mon = val;
                    }
                    t if t == DTZMOD => {
                        tmask |= DTK_M(DTZ) as i32;
                        tm.isdst = 1;
                        if !want_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tz_val -= val;
                        have_tz = true;
                    }
                    t if t == DTZ => {
                        tmask |= DTK_M(TZ) as i32;
                        tm.isdst = 1;
                        if !want_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tz_val = -val;
                        have_tz = true;
                    }
                    t if t == TZ => {
                        tm.isdst = 0;
                        if !want_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tz_val = -val;
                        have_tz = true;
                    }
                    t if t == AMPM => mer = val,
                    t if t == ADBC => bc = val == BC,
                    t if t == DOW => tm.wday = val,
                    t if t == UNITS => {
                        tmask = 0;
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }
                    t if t == ISOTIME => {
                        tmask = 0;
                        if (fmask & DTK_DATE_M as i32) != DTK_DATE_M as i32 {
                            return DTERR_BAD_FORMAT;
                        }
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }
                    t if t == UNKNOWN_FIELD => {
                        // STAGED: could be a named zone; needs tz DB.
                        // TODO(timezone-db): pg_tzset(field[i]).
                        return DTERR_BAD_FORMAT;
                    }
                    _ => return DTERR_BAD_FORMAT,
                }
            }
            _ => return DTERR_BAD_FORMAT,
        }

        if tmask & fmask != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= tmask;
    }

    if ptype != 0 {
        return DTERR_BAD_FORMAT;
    }

    if *dtype == DTK_DATE {
        let r = ValidateDate(fmask, isjulian, is2digits, bc, tm);
        if r != 0 {
            return r;
        }
        if mer != HR24 && tm.hour > HOURS_PER_DAY / 2 {
            return DTERR_FIELD_OVERFLOW;
        }
        if mer == AM && tm.hour == HOURS_PER_DAY / 2 {
            tm.hour = 0;
        } else if mer == PM && tm.hour != HOURS_PER_DAY / 2 {
            tm.hour += HOURS_PER_DAY / 2;
        }

        if (fmask & DTK_DATE_M as i32) != DTK_DATE_M as i32 {
            if (fmask & DTK_TIME_M as i32) == DTK_TIME_M as i32 {
                return 1;
            }
            return DTERR_BAD_FORMAT;
        }

        // STAGED: when no tz was given, PG resolves the session tz here. Without
        // the tz DB we leave *tzp at 0 (UTC). TODO(timezone-db).
    }

    if let Some(out) = tzp {
        *out = if have_tz { tz_val } else { 0 };
    }
    let _ = is2digits;
    0
}

/// DecodeTimeOnly: interpret parsed fields as time (+optional tz) only.
/// Returns 0 on success or a negative `DTERR_*` code.
///
/// STAGED: same tz-DB limitations as [`DecodeDateTime`].
///
/// # Errors
/// Returns a negative `DTERR_*` code on malformed or out-of-range input.
#[allow(clippy::too_many_lines, reason = "1:1 port of the C state machine")]
pub fn DecodeTimeOnly(
    field: &mut [String],
    ftype: &mut [i32],
    dtype: &mut i32,
    tm: &mut pg_tm,
    fsec: &mut fsec_t,
    tzp: Option<&mut i32>,
    _extra: &mut DateTimeErrorExtra,
) -> i32 {
    let nf = field.len();
    let mut fmask = 0i32;
    let mut ptype = 0i32;
    let mut mer = HR24;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut have_tz = false;
    let mut tz_val = 0i32;
    let want_tz = tzp.is_some();

    *dtype = DTK_TIME;
    tm.hour = 0;
    tm.min = 0;
    tm.sec = 0;
    *fsec = 0;
    tm.isdst = -1;

    for i in 0..nf {
        let mut tmask = 0i32;
        match ftype[i] {
            x if x == DTK_DATE => {
                if !want_tz {
                    return DTERR_BAD_FORMAT;
                }
                if i == 0 && nf >= 2 && (ftype[nf - 1] == DTK_DATE || ftype[1] == DTK_TIME) {
                    let r = decode_date(&field[i], fmask, &mut tmask, &mut is2digits, tm);
                    if r != 0 {
                        return r;
                    }
                } else if is_digit(field[i].as_bytes()[0]) {
                    if (fmask & DTK_TIME_M as i32) == DTK_TIME_M as i32 {
                        return DTERR_BAD_FORMAT;
                    }
                    let Some(dashpos) = field[i].find('-') else {
                        return DTERR_BAD_FORMAT;
                    };
                    let r = DecodeTimezone(&field[i][dashpos..], &mut tz_val);
                    if r != 0 {
                        return r;
                    }
                    have_tz = true;
                    field[i].truncate(dashpos);
                    let mut head = std::mem::take(&mut field[i]);
                    let r = decode_number_field(
                        &mut head,
                        fmask | DTK_DATE_M as i32,
                        &mut tmask,
                        tm,
                        fsec,
                        &mut is2digits,
                    );
                    if r < 0 {
                        field[i] = head;
                        return r;
                    }
                    ftype[i] = r;
                    field[i] = head;
                    tmask |= DTK_M(TZ) as i32;
                } else {
                    // STAGED: named timezone. TODO(timezone-db).
                    return DTERR_BAD_FORMAT;
                }
            }
            x if x == DTK_TIME => {
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = 0;
                }
                let r = decode_time(
                    &field[i],
                    INTERVAL_FULL_RANGE,
                    &mut tmask,
                    tm,
                    fsec,
                );
                if r != 0 {
                    return r;
                }
            }
            x if x == DTK_TZ => {
                if !want_tz {
                    return DTERR_BAD_FORMAT;
                }
                let mut tz = 0i32;
                let r = DecodeTimezone(&field[i], &mut tz);
                if r != 0 {
                    return r;
                }
                tz_val = tz;
                have_tz = true;
                tmask = DTK_M(TZ) as i32;
            }
            x if x == DTK_NUMBER => {
                if ptype != 0 {
                    let b = field[i].as_bytes();
                    let Ok((value, cp)) = strtoint_at(b, 0) else {
                        return DTERR_FIELD_OVERFLOW;
                    };
                    if cp < b.len() && b[cp] != b'.' {
                        return DTERR_BAD_FORMAT;
                    }
                    match ptype {
                        p if p == DTK_JULIAN => {
                            if !want_tz {
                                return DTERR_BAD_FORMAT;
                            }
                            if value < 0 {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            tmask = DTK_DATE_M as i32;
                            let (y, m, d) = j2date(value);
                            tm.year = y;
                            tm.mon = m;
                            tm.mday = d;
                            isjulian = true;
                            if cp < b.len() && b[cp] == b'.' {
                                let t = match parse_fraction(&field[i][cp..]) {
                                    Ok(v) => v,
                                    Err(e) => return e,
                                };
                                let usec = (t * USECS_PER_DAY as f64) as i64;
                                let (h, mi, s, fs) = dt2time(usec);
                                tm.hour = h;
                                tm.min = mi;
                                tm.sec = s;
                                *fsec = fs;
                                tmask |= DTK_TIME_M as i32;
                            }
                        }
                        p if p == DTK_TIME => {
                            let mut head = std::mem::take(&mut field[i]);
                            let r = decode_number_field(
                                &mut head,
                                fmask | DTK_DATE_M as i32,
                                &mut tmask,
                                tm,
                                fsec,
                                &mut is2digits,
                            );
                            field[i] = head;
                            if r < 0 {
                                return r;
                            }
                            ftype[i] = r;
                            if tmask != DTK_TIME_M as i32 {
                                return DTERR_BAD_FORMAT;
                            }
                        }
                        _ => return DTERR_BAD_FORMAT,
                    }
                    ptype = 0;
                    *dtype = DTK_DATE;
                } else {
                    let flen = field[i].len();
                    let dotpos = field[i].find('.');
                    if let Some(d) = dotpos {
                        if i == 0 && nf >= 2 && ftype[nf - 1] == DTK_DATE {
                            let r =
                                decode_date(&field[i], fmask, &mut tmask, &mut is2digits, tm);
                            if r != 0 {
                                return r;
                            }
                        } else if flen - (flen - d) > 2 {
                            let mut head = std::mem::take(&mut field[i]);
                            let r = decode_number_field(
                                &mut head,
                                fmask | DTK_DATE_M as i32,
                                &mut tmask,
                                tm,
                                fsec,
                                &mut is2digits,
                            );
                            field[i] = head;
                            if r < 0 {
                                return r;
                            }
                            ftype[i] = r;
                        } else {
                            return DTERR_BAD_FORMAT;
                        }
                    } else if flen > 4 {
                        let mut head = std::mem::take(&mut field[i]);
                        let r = decode_number_field(
                            &mut head,
                            fmask | DTK_DATE_M as i32,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        field[i] = head;
                        if r < 0 {
                            return r;
                        }
                        ftype[i] = r;
                    } else {
                        let r = decode_number(
                            flen,
                            &mut field[i],
                            false,
                            fmask | DTK_DATE_M as i32,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if r != 0 {
                            return r;
                        }
                    }
                }
            }
            x if x == DTK_STRING || x == DTK_SPECIAL => {
                // STAGED: tz-abbrev lookup. TODO(timezone-db).
                let (type_, val) = DecodeSpecial(&field[i]);
                if type_ == IGNORE_DTF {
                    continue;
                }
                tmask = DTK_M(type_) as i32;
                match type_ {
                    t if t == RESERV => match val {
                        v if v == DTK_NOW => {
                            // STAGED: needs now(). TODO(timezone-db).
                            return DTERR_BAD_FORMAT;
                        }
                        v if v == DTK_ZULU => {
                            tmask = (DTK_TIME_M | DTK_M(TZ)) as i32;
                            *dtype = DTK_TIME;
                            tm.hour = 0;
                            tm.min = 0;
                            tm.sec = 0;
                            tm.isdst = 0;
                            if want_tz {
                                have_tz = true;
                            }
                        }
                        _ => return DTERR_BAD_FORMAT,
                    },
                    t if t == DTZMOD => {
                        tmask |= DTK_M(DTZ) as i32;
                        tm.isdst = 1;
                        if !want_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tz_val -= val;
                        have_tz = true;
                    }
                    t if t == DTZ => {
                        tmask |= DTK_M(TZ) as i32;
                        tm.isdst = 1;
                        if !want_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tz_val = -val;
                        have_tz = true;
                        ftype[i] = DTK_TZ;
                    }
                    t if t == TZ => {
                        tm.isdst = 0;
                        if !want_tz {
                            return DTERR_BAD_FORMAT;
                        }
                        tz_val = -val;
                        have_tz = true;
                        ftype[i] = DTK_TZ;
                    }
                    t if t == AMPM => mer = val,
                    t if t == ADBC => bc = val == BC,
                    t if t == UNITS => {
                        tmask = 0;
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }
                    t if t == ISOTIME => {
                        tmask = 0;
                        if ptype != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        ptype = val;
                    }
                    t if t == UNKNOWN_FIELD => {
                        // STAGED: named zone. TODO(timezone-db).
                        return DTERR_BAD_FORMAT;
                    }
                    _ => return DTERR_BAD_FORMAT,
                }
            }
            _ => return DTERR_BAD_FORMAT,
        }

        if tmask & fmask != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= tmask;
    }

    if ptype != 0 {
        return DTERR_BAD_FORMAT;
    }

    let r = ValidateDate(fmask, isjulian, is2digits, bc, tm);
    if r != 0 {
        return r;
    }

    if mer != HR24 && tm.hour > HOURS_PER_DAY / 2 {
        return DTERR_FIELD_OVERFLOW;
    }
    if mer == AM && tm.hour == HOURS_PER_DAY / 2 {
        tm.hour = 0;
    } else if mer == PM && tm.hour != HOURS_PER_DAY / 2 {
        tm.hour += HOURS_PER_DAY / 2;
    }

    if time_overflows(tm.hour, tm.min, tm.sec, *fsec) {
        return DTERR_FIELD_OVERFLOW;
    }

    if (fmask & DTK_TIME_M as i32) != DTK_TIME_M as i32 {
        return DTERR_BAD_FORMAT;
    }

    // STAGED: session-tz resolution for the no-tz case. TODO(timezone-db).
    if let Some(out) = tzp {
        *out = if have_tz { tz_val } else { 0 };
    }
    0
}

// =========================================================================
// DecodeInterval / DecodeISO8601Interval
// =========================================================================

/// DecodeInterval: interpret parsed fields for a general time interval.
/// Returns 0 on success or a `DTERR_*` code; output goes into `dtype`/`itm_in`.
///
/// # Errors
/// Returns a `DTERR_*` code on malformed or out-of-range input.
#[allow(clippy::too_many_lines, reason = "1:1 port of the C state machine")]
pub fn DecodeInterval(
    field: &[String],
    ftype: &[i32],
    range: i32,
    dtype: &mut i32,
    itm_in: &mut pg_itm_in,
) -> i32 {
    let nf = field.len();
    let mut force_negative = false;
    let mut is_before = false;
    let mut parsing_unit_val = false;
    let mut fmask = 0i32;
    let mut type_ = IGNORE_DTF;

    *dtype = DTK_DELTA;
    clear_pg_itm_in(itm_in);

    if unsafe { IntervalStyle } == INTSTYLE_SQL_STANDARD
        && nf > 0
        && field[0].as_bytes().first() == Some(&b'-')
    {
        force_negative = true;
        for f in field.iter().skip(1) {
            if matches!(f.as_bytes().first(), Some(&(b'-' | b'+'))) {
                force_negative = false;
                break;
            }
        }
    }

    // Read backwards to pick up units before values.
    for i in (0..nf).rev() {
        let mut tmask = 0i32;
        match ftype[i] {
            x if x == DTK_TIME => {
                let r = decode_time_for_interval(&field[i], range, &mut tmask, itm_in);
                if r != 0 {
                    return r;
                }
                if force_negative && itm_in.usec > 0 {
                    itm_in.usec = -itm_in.usec;
                }
                type_ = DTK_DAY;
                parsing_unit_val = false;
            }
            x if x == DTK_TZ || x == DTK_DATE || x == DTK_NUMBER => {
                // DTK_TZ: a signed token possibly hh:mm[:ss]; try as time first.
                if x == DTK_TZ {
                    let tail = &field[i][1..];
                    if tail.contains(':') {
                        let mut tmask2 = 0i32;
                        if decode_time_for_interval(tail, range, &mut tmask2, itm_in) == 0 {
                            if field[i].as_bytes()[0] == b'-' {
                                if itm_in.usec == i64::MIN {
                                    return DTERR_FIELD_OVERFLOW;
                                }
                                itm_in.usec = -itm_in.usec;
                            }
                            if force_negative && itm_in.usec > 0 {
                                itm_in.usec = -itm_in.usec;
                            }
                            type_ = DTK_DAY;
                            parsing_unit_val = false;
                            tmask = tmask2;
                            if tmask & fmask != 0 {
                                return DTERR_BAD_FORMAT;
                            }
                            fmask |= tmask;
                            continue;
                        }
                    }
                }

                if type_ == IGNORE_DTF {
                    type_ = match range {
                        r if r == INTERVAL_MASK(YEAR) => DTK_YEAR,
                        r if r == INTERVAL_MASK(MONTH)
                            || r == INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH) =>
                        {
                            DTK_MONTH
                        }
                        r if r == INTERVAL_MASK(DAY) => DTK_DAY,
                        r if r == INTERVAL_MASK(HOUR)
                            || r == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) =>
                        {
                            DTK_HOUR
                        }
                        r if r == INTERVAL_MASK(MINUTE)
                            || r == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)
                            || r == INTERVAL_MASK(DAY)
                                | INTERVAL_MASK(HOUR)
                                | INTERVAL_MASK(MINUTE) =>
                        {
                            DTK_MINUTE
                        }
                        _ => DTK_SECOND,
                    };
                }

                let b = field[i].as_bytes();
                let Ok((mut val, mut cp)) = strtoi64_at(b, 0) else {
                    return DTERR_FIELD_OVERFLOW;
                };
                let mut fval: f64;
                if cp < b.len() && b[cp] == b'-' {
                    let Ok((val2_raw, ncp)) = strtoint_at(b, cp + 1) else {
                        return DTERR_FIELD_OVERFLOW;
                    };
                    if val2_raw < 0 || val2_raw >= MONTHS_PER_YEAR {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    cp = ncp;
                    if cp != b.len() {
                        return DTERR_BAD_FORMAT;
                    }
                    type_ = DTK_MONTH;
                    let val2 = if b[0] == b'-' { -val2_raw } else { val2_raw };
                    let Some(m) = pg_mul_s64_overflow(val, i64::from(MONTHS_PER_YEAR)) else {
                        return DTERR_FIELD_OVERFLOW;
                    };
                    let Some(m2) = pg_add_s64_overflow(m, i64::from(val2)) else {
                        return DTERR_FIELD_OVERFLOW;
                    };
                    val = m2;
                    fval = 0.0;
                } else if cp < b.len() && b[cp] == b'.' {
                    fval = match parse_fraction(&field[i][cp..]) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    if b[0] == b'-' {
                        fval = -fval;
                    }
                } else if cp == b.len() {
                    fval = 0.0;
                } else {
                    return DTERR_BAD_FORMAT;
                }

                if force_negative {
                    if val > 0 {
                        val = -val;
                    }
                    if fval > 0.0 {
                        fval = -fval;
                    }
                }

                match type_ {
                    t if t == DTK_MICROSEC => {
                        if !adjust_microseconds(val, fval, 1, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(MICROSECOND) as i32;
                    }
                    t if t == DTK_MILLISEC => {
                        if !adjust_microseconds(val, fval, 1000, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(MILLISECOND) as i32;
                    }
                    t if t == DTK_SECOND => {
                        if !adjust_microseconds(val, fval, USECS_PER_SEC, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = if fval == 0.0 {
                            DTK_M(SECOND) as i32
                        } else {
                            DTK_ALL_SECS_M as i32
                        };
                    }
                    t if t == DTK_MINUTE => {
                        if !adjust_microseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(MINUTE) as i32;
                    }
                    t if t == DTK_HOUR => {
                        if !adjust_microseconds(val, fval, USECS_PER_HOUR, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(HOUR) as i32;
                        type_ = DTK_DAY;
                    }
                    t if t == DTK_DAY => {
                        if !adjust_days(val, 1, itm_in)
                            || !adjust_fract_microseconds(fval, USECS_PER_DAY, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(DAY) as i32;
                    }
                    t if t == DTK_WEEK => {
                        if !adjust_days(val, 7, itm_in) || !adjust_fract_days(fval, 7, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(WEEK) as i32;
                    }
                    t if t == DTK_MONTH => {
                        if !adjust_months(val, itm_in)
                            || !adjust_fract_days(fval, DAYS_PER_MONTH, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(MONTH) as i32;
                    }
                    t if t == DTK_YEAR => {
                        if !adjust_years(val, 1, itm_in) || !adjust_fract_years(fval, 1, itm_in) {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(YEAR) as i32;
                    }
                    t if t == DTK_DECADE => {
                        if !adjust_years(val, 10, itm_in) || !adjust_fract_years(fval, 10, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(DECADE) as i32;
                    }
                    t if t == DTK_CENTURY => {
                        if !adjust_years(val, 100, itm_in)
                            || !adjust_fract_years(fval, 100, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(CENTURY) as i32;
                    }
                    t if t == DTK_MILLENNIUM => {
                        if !adjust_years(val, 1000, itm_in)
                            || !adjust_fract_years(fval, 1000, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_M(MILLENNIUM) as i32;
                    }
                    _ => return DTERR_BAD_FORMAT,
                }
                parsing_unit_val = false;
            }
            x if x == DTK_STRING || x == DTK_SPECIAL => {
                if parsing_unit_val {
                    return DTERR_BAD_FORMAT;
                }
                let (mut t, mut uval) = DecodeUnits(&field[i]);
                if t == UNKNOWN_FIELD {
                    let (t2, u2) = DecodeSpecial(&field[i]);
                    t = t2;
                    uval = u2;
                }
                match special_unit_result(
                    t, uval, i, nf, &mut type_, &mut parsing_unit_val, &mut is_before, dtype,
                    &mut tmask,
                ) {
                    None => continue, // IGNORE_DTF
                    Some(code) if code != 0 => return code,
                    Some(_) => {}
                }
            }
            _ => return DTERR_BAD_FORMAT,
        }

        if tmask & fmask != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= tmask;
    }

    if fmask == 0 {
        return DTERR_BAD_FORMAT;
    }
    if parsing_unit_val {
        return DTERR_BAD_FORMAT;
    }

    if is_before {
        if itm_in.usec == i64::MIN
            || itm_in.mday == i32::MIN
            || itm_in.mon == i32::MIN
            || itm_in.year == i32::MIN
        {
            return DTERR_FIELD_OVERFLOW;
        }
        itm_in.usec = -itm_in.usec;
        itm_in.mday = -itm_in.mday;
        itm_in.mon = -itm_in.mon;
        itm_in.year = -itm_in.year;
    }

    0
}

/// Apply the result of a UNITS/AGO/RESERV keyword in an interval. Returns
/// `Some(code)` where `code` is 0 on success or a `DTERR_*` on error; `None`
/// for IGNORE_DTF (caller should `continue`).
#[allow(clippy::too_many_arguments, reason = "shared state of the C switch arm")]
fn special_unit_result(
    t: i32,
    uval: i32,
    i: usize,
    nf: usize,
    type_: &mut i32,
    parsing_unit_val: &mut bool,
    is_before: &mut bool,
    dtype: &mut i32,
    tmask: &mut i32,
) -> Option<i32> {
    if t == IGNORE_DTF {
        return None;
    }
    *tmask = 0;
    match t {
        x if x == UNITS => {
            *type_ = uval;
            *parsing_unit_val = true;
            Some(0)
        }
        x if x == AGO => {
            if i != nf - 1 {
                return Some(DTERR_BAD_FORMAT);
            }
            *is_before = true;
            *type_ = uval;
            Some(0)
        }
        x if x == RESERV => {
            *tmask = (DTK_DATE_M | DTK_TIME_M) as i32;
            if uval != DTK_LATE && uval != DTK_EARLY {
                return Some(DTERR_BAD_FORMAT);
            }
            if i != nf - 1 {
                return Some(DTERR_BAD_FORMAT);
            }
            *dtype = uval;
            Some(0)
        }
        _ => Some(DTERR_BAD_FORMAT),
    }
}

fn parse_iso8601_number(b: &[u8], pos: &mut usize) -> Result<(i64, f64), i32> {
    let start = *pos;
    if start >= b.len() || !(is_digit(b[start]) || b[start] == b'-' || b[start] == b'.') {
        return Err(DTERR_BAD_FORMAT);
    }
    // strtod-style scan: optional sign, digits, '.', digits, exponent.
    let mut i = start;
    if b[i] == b'-' || b[i] == b'+' {
        i += 1;
    }
    while i < b.len() && is_digit(b[i]) {
        i += 1;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && is_digit(b[i]) {
            i += 1;
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        let mut j = i + 1;
        if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
            j += 1;
        }
        if j < b.len() && is_digit(b[j]) {
            i = j;
            while i < b.len() && is_digit(b[i]) {
                i += 1;
            }
        }
    }
    if i == start {
        return Err(DTERR_BAD_FORMAT);
    }
    let s = core::str::from_utf8(&b[start..i]).map_err(|_| DTERR_BAD_FORMAT)?;
    let val: f64 = s.parse().map_err(|_| DTERR_BAD_FORMAT)?;
    if val.is_nan() || val < -1.0e15 || val > 1.0e15 {
        return Err(DTERR_FIELD_OVERFLOW);
    }
    let ipart = if val >= 0.0 {
        val.floor() as i64
    } else {
        -((-val).floor() as i64)
    };
    let fpart = val - ipart as f64;
    *pos = i;
    Ok((ipart, fpart))
}

fn iso8601_integer_width(b: &[u8], mut start: usize) -> usize {
    if start < b.len() && b[start] == b'-' {
        start += 1;
    }
    let mut n = 0;
    while start + n < b.len() && is_digit(b[start + n]) {
        n += 1;
    }
    n
}

/// DecodeISO8601Interval: decode an ISO 8601 duration ("P..." form).
/// Returns 0 on success or a `DTERR_*` code.
///
/// # Errors
/// Returns a `DTERR_*` code on malformed or out-of-range input.
#[allow(clippy::too_many_lines, reason = "1:1 port of the C state machine")]
pub fn DecodeISO8601Interval(str_: &str, dtype: &mut i32, itm_in: &mut pg_itm_in) -> i32 {
    let b = str_.as_bytes();
    let mut datepart = true;
    let mut havefield = false;

    *dtype = DTK_DELTA;
    clear_pg_itm_in(itm_in);

    if b.len() < 2 || b[0] != b'P' {
        return DTERR_BAD_FORMAT;
    }
    let mut pos = 1usize;

    while pos < b.len() {
        if b[pos] == b'T' {
            datepart = false;
            havefield = false;
            pos += 1;
            continue;
        }
        let fieldstart = pos;
        let (val, fval) = match parse_iso8601_number(b, &mut pos) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let unit = if pos < b.len() { b[pos] } else { 0 };
        pos += 1;

        if datepart {
            match unit {
                b'Y' => {
                    if !adjust_years(val, 1, itm_in) || !adjust_fract_years(fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'M' => {
                    if !adjust_months(val, itm_in)
                        || !adjust_fract_days(fval, DAYS_PER_MONTH, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'W' => {
                    if !adjust_days(val, 7, itm_in) || !adjust_fract_days(fval, 7, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'D' => {
                    if !adjust_days(val, 1, itm_in)
                        || !adjust_fract_microseconds(fval, USECS_PER_DAY, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'T' | 0 | b'-' => {
                    if (unit == b'T' || unit == 0)
                        && iso8601_integer_width(b, fieldstart) == 8
                        && !havefield
                    {
                        if !adjust_years(val / 10000, 1, itm_in)
                            || !adjust_months((val / 100) % 100, itm_in)
                            || !adjust_days(val % 100, 1, itm_in)
                            || !adjust_fract_microseconds(fval, USECS_PER_DAY, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        if unit == 0 {
                            return 0;
                        }
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    // Extended alternative format.
                    if havefield {
                        return DTERR_BAD_FORMAT;
                    }
                    if !adjust_years(val, 1, itm_in) || !adjust_fract_years(fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if unit == 0 {
                        return 0;
                    }
                    if unit == b'T' {
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    let (v2, f2) = match parse_iso8601_number(b, &mut pos) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    if !adjust_months(v2, itm_in)
                        || !adjust_fract_days(f2, DAYS_PER_MONTH, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pos >= b.len() {
                        return 0;
                    }
                    if b[pos] == b'T' {
                        pos += 1;
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    if b[pos] != b'-' {
                        return DTERR_BAD_FORMAT;
                    }
                    pos += 1;
                    let (v3, f3) = match parse_iso8601_number(b, &mut pos) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    if !adjust_days(v3, 1, itm_in)
                        || !adjust_fract_microseconds(f3, USECS_PER_DAY, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pos >= b.len() {
                        return 0;
                    }
                    if b[pos] == b'T' {
                        pos += 1;
                        datepart = false;
                        havefield = false;
                        continue;
                    }
                    return DTERR_BAD_FORMAT;
                }
                _ => return DTERR_BAD_FORMAT,
            }
        } else {
            match unit {
                b'H' => {
                    if !adjust_microseconds(val, fval, USECS_PER_HOUR, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'M' => {
                    if !adjust_microseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                b'S' => {
                    if !adjust_microseconds(val, fval, USECS_PER_SEC, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                }
                0 | b':' => {
                    if unit == 0
                        && iso8601_integer_width(b, fieldstart) == 6
                        && !havefield
                    {
                        if !adjust_microseconds(val / 10000, 0.0, USECS_PER_HOUR, itm_in)
                            || !adjust_microseconds(
                                (val / 100) % 100,
                                0.0,
                                USECS_PER_MINUTE,
                                itm_in,
                            )
                            || !adjust_microseconds(val % 100, 0.0, USECS_PER_SEC, itm_in)
                            || !adjust_fract_microseconds(fval, 1, itm_in)
                        {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        return 0;
                    }
                    if havefield {
                        return DTERR_BAD_FORMAT;
                    }
                    if !adjust_microseconds(val, fval, USECS_PER_HOUR, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if unit == 0 {
                        return 0;
                    }
                    let (v2, f2) = match parse_iso8601_number(b, &mut pos) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    if !adjust_microseconds(v2, f2, USECS_PER_MINUTE, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pos >= b.len() {
                        return 0;
                    }
                    if b[pos] != b':' {
                        return DTERR_BAD_FORMAT;
                    }
                    pos += 1;
                    let (v3, f3) = match parse_iso8601_number(b, &mut pos) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    if !adjust_microseconds(v3, f3, USECS_PER_SEC, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pos >= b.len() {
                        return 0;
                    }
                    return DTERR_BAD_FORMAT;
                }
                _ => return DTERR_BAD_FORMAT,
            }
        }
        havefield = true;
    }
    0
}

// =========================================================================
// Encoding (write into a caller &mut [u8] via a cursor)
// =========================================================================

struct Cursor<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn push(&mut self, c: u8) {
        self.buf[self.pos] = c;
        self.pos += 1;
    }
    fn push_str(&mut self, s: &str) {
        for &c in s.as_bytes() {
            self.push(c);
        }
    }
    // Zero-padded unsigned decimal of at least `minwidth` digits.
    fn push_u_zeropad(&mut self, value: u32, minwidth: usize) {
        let mut tmp = [0u8; 10];
        let mut n = 0usize;
        let mut v = value;
        loop {
            tmp[n] = b'0' + (v % 10) as u8;
            v /= 10;
            n += 1;
            if v == 0 {
                break;
            }
        }
        for _ in n..minwidth {
            self.push(b'0');
        }
        for k in (0..n).rev() {
            self.push(tmp[k]);
        }
    }
    fn push_i64(&mut self, value: i64) {
        if value < 0 {
            self.push(b'-');
        }
        self.push_u64(value.unsigned_abs());
    }
    fn push_u64(&mut self, value: u64) {
        let mut tmp = [0u8; 20];
        let mut n = 0usize;
        let mut v = value;
        loop {
            tmp[n] = b'0' + (v % 10) as u8;
            v /= 10;
            n += 1;
            if v == 0 {
                break;
            }
        }
        for k in (0..n).rev() {
            self.push(tmp[k]);
        }
    }
    fn finish(&mut self) {
        self.buf[self.pos] = 0; // NUL-terminate like the C output
    }
}

// Append seconds and fractional seconds, stripping sign.
fn append_seconds(cp: &mut Cursor, sec: i32, fsec: fsec_t, precision: i32, fillzeros: bool) {
    let asec = sec.unsigned_abs();
    if fillzeros {
        cp.push_u_zeropad(asec, 2);
    } else {
        cp.push_u64(u64::from(asec));
    }
    if fsec != 0 {
        let mut value = fsec.unsigned_abs();
        cp.push(b'.');
        // Build digits MSB-first, dropping trailing zeros.
        let mut digits = [0u8; 8];
        let p = precision.max(0) as usize;
        let scale_len = p;
        // Compute the p fractional digits.
        let mut tmp = value;
        for k in (0..scale_len).rev() {
            digits[k] = b'0' + (tmp % 10) as u8;
            tmp /= 10;
        }
        if tmp != 0 {
            // precision insufficient: emit full value
            cp.push_u64(u64::from(value));
            return;
        }
        // Drop trailing zeros.
        let mut end = scale_len;
        while end > 0 && digits[end - 1] == b'0' {
            end -= 1;
        }
        for &d in &digits[..end] {
            cp.push(d);
        }
        let _ = &mut value;
    }
}

fn append_timestamp_seconds(cp: &mut Cursor, tm: &pg_tm, fsec: fsec_t) {
    append_seconds(cp, tm.sec, fsec, MAX_TIMESTAMP_PRECISION, true);
}

fn year_for_display(year: i32) -> u32 {
    if year > 0 {
        year as u32
    } else {
        (-(year - 1)) as u32
    }
}

fn encode_timezone(cp: &mut Cursor, tz: i32, style: i32) {
    let mut sec = tz.abs();
    let min = sec / SECS_PER_MINUTE;
    sec -= min * SECS_PER_MINUTE;
    let hour = min / MINS_PER_HOUR;
    let min = min - hour * MINS_PER_HOUR;

    cp.push(if tz <= 0 { b'+' } else { b'-' });
    if sec != 0 {
        cp.push_u_zeropad(hour as u32, 2);
        cp.push(b':');
        cp.push_u_zeropad(min as u32, 2);
        cp.push(b':');
        cp.push_u_zeropad(sec as u32, 2);
    } else if min != 0 || style == USE_XSD_DATES {
        cp.push_u_zeropad(hour as u32, 2);
        cp.push(b':');
        cp.push_u_zeropad(min as u32, 2);
    } else {
        cp.push_u_zeropad(hour as u32, 2);
    }
}

/// EncodeDateOnly: encode a date as local time into `str_` (NUL-terminated).
pub fn EncodeDateOnly(tm: &pg_tm, style: i32, str_: &mut [u8]) {
    let mut cp = Cursor { buf: str_, pos: 0 };
    let y = year_for_display(tm.year);
    match style {
        s if s == USE_ISO_DATES || s == USE_XSD_DATES => {
            cp.push_u_zeropad(y, 4);
            cp.push(b'-');
            cp.push_u_zeropad(tm.mon as u32, 2);
            cp.push(b'-');
            cp.push_u_zeropad(tm.mday as u32, 2);
        }
        s if s == USE_SQL_DATES => {
            if unsafe { DateOrder } == DATEORDER_DMY {
                cp.push_u_zeropad(tm.mday as u32, 2);
                cp.push(b'/');
                cp.push_u_zeropad(tm.mon as u32, 2);
            } else {
                cp.push_u_zeropad(tm.mon as u32, 2);
                cp.push(b'/');
                cp.push_u_zeropad(tm.mday as u32, 2);
            }
            cp.push(b'/');
            cp.push_u_zeropad(y, 4);
        }
        s if s == USE_GERMAN_DATES => {
            cp.push_u_zeropad(tm.mday as u32, 2);
            cp.push(b'.');
            cp.push_u_zeropad(tm.mon as u32, 2);
            cp.push(b'.');
            cp.push_u_zeropad(y, 4);
        }
        _ => {
            if unsafe { DateOrder } == DATEORDER_DMY {
                cp.push_u_zeropad(tm.mday as u32, 2);
                cp.push(b'-');
                cp.push_u_zeropad(tm.mon as u32, 2);
            } else {
                cp.push_u_zeropad(tm.mon as u32, 2);
                cp.push(b'-');
                cp.push_u_zeropad(tm.mday as u32, 2);
            }
            cp.push(b'-');
            cp.push_u_zeropad(y, 4);
        }
    }
    if tm.year <= 0 {
        cp.push_str(" BC");
    }
    cp.finish();
}

/// EncodeTimeOnly: encode time fields (+optional tz) into `str_`.
pub fn EncodeTimeOnly(tm: &pg_tm, fsec: fsec_t, print_tz: bool, tz: i32, style: i32, str_: &mut [u8]) {
    let mut cp = Cursor { buf: str_, pos: 0 };
    cp.push_u_zeropad(tm.hour as u32, 2);
    cp.push(b':');
    cp.push_u_zeropad(tm.min as u32, 2);
    cp.push(b':');
    append_seconds(&mut cp, tm.sec, fsec, MAX_TIME_PRECISION, true);
    if print_tz {
        encode_timezone(&mut cp, tz, style);
    }
    cp.finish();
}

/// EncodeDateTime: encode date+time (interpreted as local time) into `str_`.
pub fn EncodeDateTime(
    tm: &mut pg_tm,
    fsec: fsec_t,
    print_tz: bool,
    tz: i32,
    tzn: Option<&str>,
    style: i32,
    str_: &mut [u8],
) {
    // Negative tm_isdst means no valid tz translation.
    let print_tz = if tm.isdst < 0 { false } else { print_tz };
    let y = year_for_display(tm.year);
    let mut cp = Cursor { buf: str_, pos: 0 };

    match style {
        s if s == USE_ISO_DATES || s == USE_XSD_DATES => {
            cp.push_u_zeropad(y, 4);
            cp.push(b'-');
            cp.push_u_zeropad(tm.mon as u32, 2);
            cp.push(b'-');
            cp.push_u_zeropad(tm.mday as u32, 2);
            cp.push(if style == USE_ISO_DATES { b' ' } else { b'T' });
            cp.push_u_zeropad(tm.hour as u32, 2);
            cp.push(b':');
            cp.push_u_zeropad(tm.min as u32, 2);
            cp.push(b':');
            append_timestamp_seconds(&mut cp, tm, fsec);
            if print_tz {
                encode_timezone(&mut cp, tz, style);
            }
        }
        s if s == USE_SQL_DATES => {
            if unsafe { DateOrder } == DATEORDER_DMY {
                cp.push_u_zeropad(tm.mday as u32, 2);
                cp.push(b'/');
                cp.push_u_zeropad(tm.mon as u32, 2);
            } else {
                cp.push_u_zeropad(tm.mon as u32, 2);
                cp.push(b'/');
                cp.push_u_zeropad(tm.mday as u32, 2);
            }
            cp.push(b'/');
            cp.push_u_zeropad(y, 4);
            cp.push(b' ');
            cp.push_u_zeropad(tm.hour as u32, 2);
            cp.push(b':');
            cp.push_u_zeropad(tm.min as u32, 2);
            cp.push(b':');
            append_timestamp_seconds(&mut cp, tm, fsec);
            if print_tz {
                if let Some(name) = tzn {
                    cp.push(b' ');
                    push_tzn(&mut cp, name);
                } else {
                    encode_timezone(&mut cp, tz, style);
                }
            }
        }
        s if s == USE_GERMAN_DATES => {
            cp.push_u_zeropad(tm.mday as u32, 2);
            cp.push(b'.');
            cp.push_u_zeropad(tm.mon as u32, 2);
            cp.push(b'.');
            cp.push_u_zeropad(y, 4);
            cp.push(b' ');
            cp.push_u_zeropad(tm.hour as u32, 2);
            cp.push(b':');
            cp.push_u_zeropad(tm.min as u32, 2);
            cp.push(b':');
            append_timestamp_seconds(&mut cp, tm, fsec);
            if print_tz {
                if let Some(name) = tzn {
                    cp.push(b' ');
                    push_tzn(&mut cp, name);
                } else {
                    encode_timezone(&mut cp, tz, style);
                }
            }
        }
        _ => {
            let day = date2j(tm.year, tm.mon, tm.mday);
            tm.wday = j2day(day);
            cp.push_str(DAYS[tm.wday as usize].get(..3).unwrap_or(DAYS[tm.wday as usize]));
            cp.push(b' ');
            if unsafe { DateOrder } == DATEORDER_DMY {
                cp.push_u_zeropad(tm.mday as u32, 2);
                cp.push(b' ');
                cp.push_str(MONTHS[(tm.mon - 1) as usize]);
            } else {
                cp.push_str(MONTHS[(tm.mon - 1) as usize]);
                cp.push(b' ');
                cp.push_u_zeropad(tm.mday as u32, 2);
            }
            cp.push(b' ');
            cp.push_u_zeropad(tm.hour as u32, 2);
            cp.push(b':');
            cp.push_u_zeropad(tm.min as u32, 2);
            cp.push(b':');
            append_timestamp_seconds(&mut cp, tm, fsec);
            cp.push(b' ');
            cp.push_u_zeropad(y, 4);
            if print_tz {
                if let Some(name) = tzn {
                    cp.push(b' ');
                    push_tzn(&mut cp, name);
                } else {
                    cp.push(b' ');
                    encode_timezone(&mut cp, tz, style);
                }
            }
        }
    }
    if tm.year <= 0 {
        cp.push_str(" BC");
    }
    cp.finish();
}

fn push_tzn(cp: &mut Cursor, tzn: &str) {
    // Clip to MAXTZLEN (ASCII abbreviations only, like the C %.*s).
    for &c in tzn.as_bytes().iter().take(MAXTZLEN) {
        cp.push(c);
    }
}

// EncodeInterval helpers.
fn add_iso8601_int_part(cp: &mut Cursor, value: i64, units: u8) {
    if value == 0 {
        return;
    }
    cp.push_i64(value);
    cp.push(units);
}

fn add_postgres_int_part(
    cp: &mut Cursor,
    value: i64,
    units: &str,
    is_zero: &mut bool,
    is_before: &mut bool,
) {
    if value == 0 {
        return;
    }
    if !*is_zero {
        cp.push(b' ');
    }
    if *is_before && value > 0 {
        cp.push(b'+');
    }
    cp.push_i64(value);
    cp.push(b' ');
    cp.push_str(units);
    if value != 1 {
        cp.push(b's');
    }
    *is_before = value < 0;
    *is_zero = false;
}

fn add_verbose_int_part(
    cp: &mut Cursor,
    value: i64,
    units: &str,
    is_zero: &mut bool,
    is_before: &mut bool,
) {
    if value == 0 {
        return;
    }
    let mut v = value;
    if *is_zero {
        *is_before = v < 0;
        v = v.abs();
    } else if *is_before {
        v = -v;
    }
    cp.push(b' ');
    cp.push_i64(v);
    cp.push(b' ');
    cp.push_str(units);
    if v != 1 {
        cp.push(b's');
    }
    *is_zero = false;
}

/// EncodeInterval: render a broken-down interval (`pg_itm`) into `str_`.
#[allow(clippy::too_many_lines, reason = "1:1 port of the four interval styles")]
pub fn EncodeInterval(itm: &pg_itm, style: i32, str_: &mut [u8]) {
    let year = itm.year;
    let mon = itm.mon;
    let mday = i64::from(itm.mday);
    let hour = itm.hour;
    let min = itm.min;
    let sec = itm.sec;
    let fsec = itm.usec;
    let mut is_before = false;
    let mut is_zero = true;
    let mut cp = Cursor { buf: str_, pos: 0 };

    match style {
        s if s == INTSTYLE_SQL_STANDARD => {
            let has_negative =
                year < 0 || mon < 0 || mday < 0 || hour < 0 || min < 0 || sec < 0 || fsec < 0;
            let has_positive =
                year > 0 || mon > 0 || mday > 0 || hour > 0 || min > 0 || sec > 0 || fsec > 0;
            let has_year_month = year != 0 || mon != 0;
            let has_day_time = mday != 0 || hour != 0 || min != 0 || sec != 0 || fsec != 0;
            let has_day = mday != 0;
            let sql_standard_value =
                !(has_negative && has_positive) && !(has_year_month && has_day_time);

            let (mut year, mut mon, mut mday, mut hour, mut min, mut sec, mut fsec) =
                (year, mon, mday, hour, min, sec, fsec);
            if has_negative && sql_standard_value {
                cp.push(b'-');
                year = -year;
                mon = -mon;
                mday = -mday;
                hour = -hour;
                min = -min;
                sec = -sec;
                fsec = -fsec;
            }

            if !has_negative && !has_positive {
                cp.push(b'0');
            } else if !sql_standard_value {
                let year_sign = if year < 0 || mon < 0 { b'-' } else { b'+' };
                let day_sign = if mday < 0 { b'-' } else { b'+' };
                let sec_sign = if hour < 0 || min < 0 || sec < 0 || fsec < 0 {
                    b'-'
                } else {
                    b'+'
                };
                cp.push(year_sign);
                cp.push_u64(u64::from(year.unsigned_abs()));
                cp.push(b'-');
                cp.push_u64(u64::from(mon.unsigned_abs()));
                cp.push(b' ');
                cp.push(day_sign);
                cp.push_u64(mday.unsigned_abs());
                cp.push(b' ');
                cp.push(sec_sign);
                cp.push_u64(hour.unsigned_abs());
                cp.push(b':');
                cp.push_u_zeropad(min.unsigned_abs(), 2);
                cp.push(b':');
                append_seconds(&mut cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            } else if has_year_month {
                cp.push_i64(i64::from(year));
                cp.push(b'-');
                cp.push_i64(i64::from(mon));
            } else if has_day {
                cp.push_i64(mday);
                cp.push(b' ');
                cp.push_i64(hour);
                cp.push(b':');
                cp.push_u_zeropad(min.unsigned_abs(), 2);
                cp.push(b':');
                append_seconds(&mut cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            } else {
                cp.push_i64(hour);
                cp.push(b':');
                cp.push_u_zeropad(min.unsigned_abs(), 2);
                cp.push(b':');
                append_seconds(&mut cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            }
        }
        s if s == INTSTYLE_ISO_8601 => {
            if year == 0
                && mon == 0
                && mday == 0
                && hour == 0
                && min == 0
                && sec == 0
                && fsec == 0
            {
                cp.push_str("PT0S");
            } else {
                cp.push(b'P');
                add_iso8601_int_part(&mut cp, i64::from(year), b'Y');
                add_iso8601_int_part(&mut cp, i64::from(mon), b'M');
                add_iso8601_int_part(&mut cp, mday, b'D');
                if hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                    cp.push(b'T');
                }
                add_iso8601_int_part(&mut cp, hour, b'H');
                add_iso8601_int_part(&mut cp, i64::from(min), b'M');
                if sec != 0 || fsec != 0 {
                    if sec < 0 || fsec < 0 {
                        cp.push(b'-');
                    }
                    append_seconds(&mut cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
                    cp.push(b'S');
                }
            }
        }
        s if s == INTSTYLE_POSTGRES => {
            add_postgres_int_part(&mut cp, i64::from(year), "year", &mut is_zero, &mut is_before);
            add_postgres_int_part(&mut cp, i64::from(mon), "mon", &mut is_zero, &mut is_before);
            add_postgres_int_part(&mut cp, mday, "day", &mut is_zero, &mut is_before);
            if is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0 {
                let minus = hour < 0 || min < 0 || sec < 0 || fsec < 0;
                if !is_zero {
                    cp.push(b' ');
                }
                if minus {
                    cp.push(b'-');
                } else if is_before {
                    cp.push(b'+');
                }
                cp.push_u_zeropad_u64(hour.unsigned_abs(), 2);
                cp.push(b':');
                cp.push_u_zeropad(min.unsigned_abs(), 2);
                cp.push(b':');
                append_seconds(&mut cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            }
        }
        _ => {
            // INTSTYLE_POSTGRES_VERBOSE
            cp.push(b'@');
            add_verbose_int_part(&mut cp, i64::from(year), "year", &mut is_zero, &mut is_before);
            add_verbose_int_part(&mut cp, i64::from(mon), "mon", &mut is_zero, &mut is_before);
            add_verbose_int_part(&mut cp, mday, "day", &mut is_zero, &mut is_before);
            add_verbose_int_part(&mut cp, hour, "hour", &mut is_zero, &mut is_before);
            add_verbose_int_part(&mut cp, i64::from(min), "min", &mut is_zero, &mut is_before);
            if sec != 0 || fsec != 0 {
                cp.push(b' ');
                if sec < 0 || (sec == 0 && fsec < 0) {
                    if is_zero {
                        is_before = true;
                    } else if !is_before {
                        cp.push(b'-');
                    }
                } else if is_before {
                    cp.push(b'-');
                }
                append_seconds(&mut cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
                cp.push_str(" sec");
                if sec.abs() != 1 || fsec != 0 {
                    cp.push(b's');
                }
                is_zero = false;
            }
            if is_zero {
                cp.push_str(" 0");
            }
            if is_before {
                cp.push_str(" ago");
            }
        }
    }
    cp.finish();
}

impl Cursor<'_> {
    // i64 zero-padded (used by POSTGRES interval hour field).
    fn push_u_zeropad_u64(&mut self, value: u64, minwidth: usize) {
        let mut tmp = [0u8; 20];
        let mut n = 0usize;
        let mut v = value;
        loop {
            tmp[n] = b'0' + (v % 10) as u8;
            v /= 10;
            n += 1;
            if v == 0 {
                break;
            }
        }
        for _ in n..minwidth {
            self.push(b'0');
        }
        for k in (0..n).rev() {
            self.push(tmp[k]);
        }
    }
}

// =========================================================================
// Typmod rounding
// =========================================================================

/// AdjustTimestampForTypmod: round a timestamp to suit the given typmod.
/// Returns `false` if the typmod is out of range (caller raises), `true`
/// otherwise. (1:1 port of the timestamp.c helper, kept here per the header.)
#[must_use]
pub fn AdjustTimestampForTypmod(time: &mut Timestamp, typmod: i32) -> bool {
    const SCALES: [i64; (MAX_TIMESTAMP_PRECISION + 1) as usize] =
        [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];
    const OFFSETS: [i64; (MAX_TIMESTAMP_PRECISION + 1) as usize] =
        [500_000, 50_000, 5_000, 500, 50, 5, 0];

    if !TIMESTAMP_NOT_FINITE(*time) && typmod != -1 && typmod != MAX_TIMESTAMP_PRECISION {
        if !(0..=MAX_TIMESTAMP_PRECISION).contains(&typmod) {
            return false;
        }
        let scale = SCALES[typmod as usize];
        let offset = OFFSETS[typmod as usize];
        if *time >= 0 {
            *time = ((*time + offset) / scale) * scale;
        } else {
            *time = -((((-*time) + offset) / scale) * scale);
        }
    }
    true
}

// =========================================================================
// Misc constants exposed for callers
// =========================================================================

/// j2date sanity: the POSTGRES/UNIX epochs match their documented Julian days.
const _: () = {
    assert!(UNIX_EPOCH_JDATE == 2_440_588);
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::datetime::{
        DTK_DATE as DTYPE_DATE, DTK_DELTA as DTYPE_DELTA, DTK_TIME as DTYPE_TIME,
    };

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
            isdst: -1,
            gmtoff: 0,
            zone: None,
        }
    }

    #[test]
    fn j2date_date2j_roundtrip() {
        // POSTGRES epoch 2000-01-01 is Julian day 2451545.
        assert_eq!(date2j(2000, 1, 1), 2_451_545);
        // UNIX epoch.
        assert_eq!(date2j(1970, 1, 1), 2_440_588);
        for &(y, m, d) in &[
            (2000, 1, 1),
            (1970, 1, 1),
            (2024, 2, 29),
            (1, 1, 1),
            (1999, 12, 31),
            (2400, 6, 15),
        ] {
            let jd = date2j(y, m, d);
            let (y2, m2, d2) = j2date(jd);
            assert_eq!((y, m, d), (y2, m2, d2), "roundtrip {y}-{m}-{d}");
        }
    }

    #[test]
    fn j2date_known_days() {
        // 2000-01-01 is a Saturday: j2day(date2j-...) Sat==6.
        let jd = date2j(2000, 1, 1);
        assert_eq!(j2day(jd), 6);
        // 2024-02-29 leap.
        let (y, m, d) = j2date(date2j(2024, 2, 29));
        assert_eq!((y, m, d), (2024, 2, 29));
    }

    fn decode_dt(s: &str) -> (i32, pg_tm, fsec_t, i32) {
        let (mut field, mut ftype) = ParseDateTime(s, crate::utils::datetime::MAXDATEFIELDS)
            .expect("parse");
        let mut dtype = 0;
        let mut tm = new_tm();
        let mut fsec = 0;
        let mut tz = 0;
        let mut extra = DateTimeErrorExtra {
            timezone: None,
            abbrev: None,
        };
        let r = DecodeDateTime(
            &mut field,
            &mut ftype,
            &mut dtype,
            &mut tm,
            &mut fsec,
            Some(&mut tz),
            &mut extra,
        );
        assert_eq!(r, 0, "decode {s}");
        (dtype, tm, fsec, tz)
    }

    #[test]
    fn decode_iso_date() {
        let (dtype, tm, fsec, _) = decode_dt("2024-01-15");
        assert_eq!(dtype, DTYPE_DATE);
        assert_eq!((tm.year, tm.mon, tm.mday), (2024, 1, 15));
        assert_eq!((tm.hour, tm.min, tm.sec, fsec), (0, 0, 0, 0));
    }

    #[test]
    fn decode_iso_timestamp() {
        let (dtype, tm, fsec, _) = decode_dt("2024-01-15 10:30:00");
        assert_eq!(dtype, DTYPE_DATE);
        assert_eq!((tm.year, tm.mon, tm.mday), (2024, 1, 15));
        assert_eq!((tm.hour, tm.min, tm.sec, fsec), (10, 30, 0, 0));
    }

    #[test]
    fn decode_timestamp_fsec() {
        let (_, tm, fsec, _) = decode_dt("2024-01-15 10:30:45.5");
        assert_eq!((tm.hour, tm.min, tm.sec), (10, 30, 45));
        assert_eq!(fsec, 500_000);
    }

    #[test]
    fn decode_numeric_tz() {
        let (_, tm, _, tz) = decode_dt("2024-01-15 10:30:00+05");
        assert_eq!((tm.hour, tm.min), (10, 30));
        // +05 -> stored as -(5*3600)
        assert_eq!(tz, -5 * 3600);
    }

    #[test]
    fn encode_iso_timestamp_roundtrip() {
        let (_, mut tm, fsec, _) = decode_dt("2024-01-15 10:30:00");
        let mut buf = [0u8; 128];
        EncodeDateTime(&mut tm, fsec, false, 0, None, USE_ISO_DATES, &mut buf);
        let end = buf.iter().position(|&c| c == 0).unwrap();
        assert_eq!(&buf[..end], b"2024-01-15 10:30:00");
    }

    #[test]
    fn encode_date_only_iso() {
        let mut tm = new_tm();
        tm.year = 2024;
        tm.mon = 3;
        tm.mday = 7;
        let mut buf = [0u8; 128];
        EncodeDateOnly(&tm, USE_ISO_DATES, &mut buf);
        let end = buf.iter().position(|&c| c == 0).unwrap();
        assert_eq!(&buf[..end], b"2024-03-07");
    }

    #[test]
    fn validate_date_rejects_feb30() {
        let mut tm = new_tm();
        tm.year = 2024;
        tm.mon = 2;
        tm.mday = 30;
        let fmask = (DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY)) as i32;
        let r = ValidateDate(fmask, false, false, false, &mut tm);
        assert_eq!(r, DTERR_FIELD_OVERFLOW);
    }

    #[test]
    fn validate_date_accepts_feb29_leap() {
        let mut tm = new_tm();
        tm.year = 2024;
        tm.mon = 2;
        tm.mday = 29;
        let fmask = (DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY)) as i32;
        assert_eq!(ValidateDate(fmask, false, false, false, &mut tm), 0);
    }

    fn decode_interval(s: &str) -> pg_itm_in {
        let (field, ftype) = ParseDateTime(s, crate::utils::datetime::MAXDATEFIELDS).expect("parse");
        let mut dtype = 0;
        let mut itm = pg_itm_in::default();
        let r = DecodeInterval(&field, &ftype, INTERVAL_FULL_RANGE, &mut dtype, &mut itm);
        assert_eq!(r, 0, "decode interval {s}");
        assert_eq!(dtype, DTYPE_DELTA);
        itm
    }

    #[test]
    fn decode_interval_one_day() {
        let itm = decode_interval("1 day");
        assert_eq!((itm.year, itm.mon, itm.mday, itm.usec), (0, 0, 1, 0));
    }

    #[test]
    fn decode_interval_two_hours() {
        let itm = decode_interval("2 hours");
        assert_eq!(itm.usec, 2 * USECS_PER_HOUR);
        assert_eq!((itm.year, itm.mon, itm.mday), (0, 0, 0));
    }

    #[test]
    fn decode_interval_year_month() {
        let itm = decode_interval("1 year 2 months");
        assert_eq!((itm.year, itm.mon, itm.mday, itm.usec), (1, 2, 0, 0));
    }

    #[test]
    fn decode_interval_ago() {
        let itm = decode_interval("1 day ago");
        assert_eq!(itm.mday, -1);
    }

    #[test]
    fn decode_iso8601_interval_basic() {
        let mut dtype = 0;
        let mut itm = pg_itm_in::default();
        let r = DecodeISO8601Interval("P2Y6M7DT1H30M", &mut dtype, &mut itm);
        assert_eq!(r, 0);
        assert_eq!((itm.year, itm.mon, itm.mday), (2, 6, 7));
        assert_eq!(itm.usec, USECS_PER_HOUR + 30 * USECS_PER_MINUTE);
    }

    #[test]
    fn encode_interval_postgres() {
        let itm = pg_itm {
            usec: 0,
            sec: 0,
            min: 0,
            hour: 0,
            mday: 1,
            mon: 0,
            year: 1,
        };
        let mut buf = [0u8; 128];
        EncodeInterval(&itm, INTSTYLE_POSTGRES, &mut buf);
        let end = buf.iter().position(|&c| c == 0).unwrap();
        assert_eq!(&buf[..end], b"1 year 1 day");
    }

    #[test]
    fn encode_interval_iso8601() {
        let itm = pg_itm {
            usec: 0,
            sec: 0,
            min: 30,
            hour: 1,
            mday: 7,
            mon: 6,
            year: 2,
        };
        let mut buf = [0u8; 128];
        EncodeInterval(&itm, INTSTYLE_ISO_8601, &mut buf);
        let end = buf.iter().position(|&c| c == 0).unwrap();
        assert_eq!(&buf[..end], b"P2Y6M7DT1H30M");
    }

    #[test]
    fn adjust_timestamp_typmod_rounds() {
        // 1.5s past epoch, round to seconds (typmod 0): 1_500_000 -> 2_000_000.
        let mut t: Timestamp = 1_500_000;
        assert!(AdjustTimestampForTypmod(&mut t, 0));
        assert_eq!(t, 2_000_000);
        // round to milliseconds (typmod 3): 1_500_499 -> 1_500_000.
        let mut t2: Timestamp = 1_500_499;
        assert!(AdjustTimestampForTypmod(&mut t2, 3));
        assert_eq!(t2, 1_500_000);
        // out-of-range typmod returns false.
        let mut t3: Timestamp = 0;
        assert!(!AdjustTimestampForTypmod(&mut t3, 99));
    }

    #[test]
    fn time_only_decode() {
        let (mut field, mut ftype) =
            ParseDateTime("10:30:45", crate::utils::datetime::MAXDATEFIELDS).expect("parse");
        let mut dtype = 0;
        let mut tm = new_tm();
        let mut fsec = 0;
        let mut extra = DateTimeErrorExtra {
            timezone: None,
            abbrev: None,
        };
        let r = DecodeTimeOnly(&mut field, &mut ftype, &mut dtype, &mut tm, &mut fsec, None, &mut extra);
        assert_eq!(r, 0);
        assert_eq!(dtype, DTYPE_TIME);
        assert_eq!((tm.hour, tm.min, tm.sec), (10, 30, 45));
    }

    #[test]
    fn keyword_tables_sorted() {
        for w in DATETKTBL.windows(2) {
            assert!(token_str(&w[0]) < token_str(&w[1]), "datetktbl order");
        }
        for w in DELTATKTBL.windows(2) {
            assert!(token_str(&w[0]) < token_str(&w[1]), "deltatktbl order");
        }
    }
}
