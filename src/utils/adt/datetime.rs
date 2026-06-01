//! datetime.rs
//!   Support functions for date/time types.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/datetime.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h               -> crate::prelude::* (Datum, *GetDatum, palloc, elog!/ereport!/Assert!)
//!   <ctype.h>/<limits.h>/<math.h> -> Rust intrinsics + libm via extern "C"
//!   access/htup_details.h    -> heap_form_tuple/HeapTupleGetDatum (stubbed below)
//!   access/xact.h            -> GetCurrentTransactionStartTimestamp (stubbed below)
//!   common/int.h             -> crate::common::int (pg_add/mul/neg_*_overflow)
//!   common/string.h          -> strtoint (extern "C")
//!   funcapi.h                -> SRF machinery (stubbed below)
//!   miscadmin.h              -> crate::miscadmin (DateStyle/DateOrder/IntervalStyle/USE_*_DATES/DATEORDER_*/INTSTYLE_*/MAXTZLEN)
//!   nodes/nodeFuncs.h        -> exprTypmod/relabel_to_typmod (stubbed below)
//!   parser/scansup.h         -> downcase_truncate_identifier (stubbed below)
//!   utils/builtins.h         -> cstring_to_text/CStringGetTextDatum/pg_strtoint*
//!   utils/date.h             -> crate::utils::adt::date (DateADT/TimeADT/Timestamp/Interval/consts)
//!   utils/datetime.h         -> THIS FILE (canonical home of DTK_*/DTERR_*/field+token consts/datetkn)
//!   utils/guc.h              -> guc_malloc (stubbed below)
//!   utils/tzparser.h         -> tzEntry (stubbed below)
//!
//! Most fundamental types and constants (Timestamp/Interval/fsec_t/USECS_PER_*/
//! DTK_* tokens/DTERR_*/MAXDATELEN/...) already have a home in
//! crate::utils::adt::date and crate::pgtime; this file re-exports / reuses
//! those and DEFINES the parsing field-type and token constants
//! (RESERV/MONTH/.../DTK_NUMBER/...) plus the datetkn table type, which are the
//! canonical residents of datetime.h.
//!
//! The deeply set-returning SQL functions (pg_timezone_abbrevs_*,
//! pg_timezone_names) and the timezone-DB plumbing (pg_tzset, the abbrev tables)
//! depend on funcapi/heap/tuplestore and the timezone library, none of which is
//! ported yet; those are translated as faithfully as possible with the missing
//! leaf symbols carried as local `// TODO(pg-port)` stubs that name the file
//! where the real symbol will eventually live.

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_long, c_void};

use crate::common::int::{
    pg_add_s32_overflow, pg_add_s64_overflow, pg_mul_s32_overflow, pg_mul_s64_overflow,
};
use crate::miscadmin::{
    DateOrder, DateStyle, IntervalStyle, DATEORDER_DMY, DATEORDER_YMD, INTSTYLE_ISO_8601,
    INTSTYLE_POSTGRES, INTSTYLE_POSTGRES_VERBOSE, INTSTYLE_SQL_STANDARD, MAXTZLEN, USE_GERMAN_DATES,
    USE_ISO_DATES, USE_POSTGRES_DATES, USE_SQL_DATES, USE_XSD_DATES,
};
use crate::pgtime::{
    pg_get_next_timezone_abbrev, pg_get_timezone_name, pg_get_timezone_offset,
    pg_interpret_timezone_abbrev, pg_next_dst_boundary, pg_time_t, pg_timezone_abbrev_is_known,
    pg_tm, pg_tz, pg_tzenum, pg_tzenumerate_end, pg_tzenumerate_next, pg_tzenumerate_start, pg_tzset,
    pg_tzset_offset, session_timezone, TZ_STRLEN_MAX,
};
use crate::port::pgstrcasecmp::{pg_tolower, pg_toupper};
use crate::utils::adt::date::{
    fsec_t, DateTimeErrorExtra, Interval, Timestamp, TimestampTz, DTERR_BAD_FORMAT, DTK_DATE,
    DTK_EARLY, DTK_EPOCH, DTK_HOUR, DTK_JULIAN, DTK_LATE, DTK_MICROSEC, DTK_MILLENNIUM,
    DTK_MILLISEC, DTK_MINUTE, DTK_MONTH, DTK_QUARTER, DTK_SECOND, DTK_TZ, DTK_TZ_HOUR,
    DTK_TZ_MINUTE, DTK_WEEK, DTK_YEAR, HOURS_PER_DAY, IS_VALID_JULIAN, MAX_TIME_PRECISION,
    MINS_PER_HOUR, SECS_PER_DAY, SECS_PER_HOUR, SECS_PER_MINUTE, TZNAME_DYNTZ, TZNAME_FIXED_OFFSET,
    UNIX_EPOCH_JDATE, USECS_PER_DAY, USECS_PER_HOUR, USECS_PER_MINUTE, USECS_PER_SEC, MAXDATEFIELDS,
};
use crate::utils::adt::numutils::{pg_ultostr, pg_ultostr_zeropad};
use crate::utils::fmgr::FunctionCallInfo;

// ===========================================================================
// Constants that are canonical to datetime.h / datatype/timestamp.h and not yet
// homed elsewhere.  (Those already pub in crate::utils::adt::date are imported
// above; the parsing field types and token types below live here.)
// ===========================================================================

/*
 * String definitions for standard time quantities (datetime.h).
 *
 * These strings are the defaults used to form output time strings.
 */
const DAGO: &[u8] = b"ago\0";
const DCURRENT: &[u8] = b"current\0";
const EPOCH: &[u8] = b"epoch\0";
const INVALID: &[u8] = b"invalid\0";
const EARLY: &[u8] = b"-infinity\0";
const LATE: &[u8] = b"infinity\0";
const NOW: &[u8] = b"now\0";
const TODAY: &[u8] = b"today\0";
const TOMORROW: &[u8] = b"tomorrow\0";
const YESTERDAY: &[u8] = b"yesterday\0";
const ZULU: &[u8] = b"zulu\0";

const DMICROSEC: &[u8] = b"usecond\0";
const DMILLISEC: &[u8] = b"msecond\0";
const DSECOND: &[u8] = b"second\0";
const DMINUTE: &[u8] = b"minute\0";
const DHOUR: &[u8] = b"hour\0";
const DDAY: &[u8] = b"day\0";
const DWEEK: &[u8] = b"week\0";
const DMONTH: &[u8] = b"month\0";
const DQUARTER: &[u8] = b"quarter\0";
const DYEAR: &[u8] = b"year\0";
const DDECADE: &[u8] = b"decade\0";
const DCENTURY: &[u8] = b"century\0";
const DMILLENNIUM: &[u8] = b"millennium\0";
const DA_D: &[u8] = b"ad\0";
const DB_C: &[u8] = b"bc\0";
const DTIMEZONE: &[u8] = b"timezone\0";

/*
 * Fundamental time field definitions for parsing.
 *  Meridian:  am, pm, or 24-hour style.
 *  Millennium: ad, bc
 */
pub const AM: c_int = 0;
pub const PM: c_int = 1;
pub const HR24: c_int = 2;

pub const AD: c_int = 0;
pub const BC: c_int = 1;

/*
 * Field types for time decoding.
 */
pub const RESERV: c_int = 0;
pub const MONTH: c_int = 1;
pub const YEAR: c_int = 2;
pub const DAY: c_int = 3;
pub const JULIAN: c_int = 4;
pub const TZ: c_int = 5; /* fixed-offset timezone abbreviation */
pub const DTZ: c_int = 6; /* fixed-offset timezone abbrev, DST */
pub const DYNTZ: c_int = 7; /* dynamic timezone abbreviation */
pub const IGNORE_DTF: c_int = 8;
pub const AMPM: c_int = 9;
pub const HOUR: c_int = 10;
pub const MINUTE: c_int = 11;
pub const SECOND: c_int = 12;
pub const MILLISECOND: c_int = 13;
pub const MICROSECOND: c_int = 14;
pub const DOY: c_int = 15;
pub const DOW: c_int = 16;
pub const UNITS: c_int = 17;
pub const ADBC: c_int = 18;
/* these are only for relative dates */
pub const AGO: c_int = 19;
pub const ABS_BEFORE: c_int = 20;
pub const ABS_AFTER: c_int = 21;
/* generic fields to help with parsing */
pub const ISODATE: c_int = 22;
pub const ISOTIME: c_int = 23;
/* these are only for parsing intervals */
pub const WEEK: c_int = 24;
pub const DECADE: c_int = 25;
pub const CENTURY: c_int = 26;
pub const MILLENNIUM: c_int = 27;
/* hack for parsing two-word timezone specs "MET DST" etc */
pub const DTZMOD: c_int = 28; /* "DST" as a separate word */
/* reserved for unrecognized string values */
pub const UNKNOWN_FIELD: c_int = 31;

/*
 * Token field definitions for time parsing and decoding.
 */
pub const DTK_NUMBER: c_int = 0;
pub const DTK_STRING: c_int = 1;

/* DTK_DATE (== 2) is imported from crate::utils::adt::date */
pub const DTK_TIME: c_int = 3;
/* DTK_TZ already in crate::utils::adt::date == 4 (re-exported as DTK_TZ above) */
pub const DTK_AGO: c_int = 5;

pub const DTK_SPECIAL: c_int = 6;
/* DTK_EARLY/LATE/EPOCH imported from date.rs */
pub const DTK_NOW: c_int = 12;
pub const DTK_YESTERDAY: c_int = 13;
pub const DTK_TODAY: c_int = 14;
pub const DTK_TOMORROW: c_int = 15;
pub const DTK_ZULU: c_int = 16;

pub const DTK_DELTA: c_int = 17;
pub const DTK_CENTURY: c_int = 27;
pub const DTK_DECADE: c_int = 26;
pub const DTK_ISODOW: c_int = 37;
pub const DTK_ISOYEAR: c_int = 36;
pub const DTK_DAY: c_int = 21;
pub const DTK_DOW: c_int = 32;
pub const DTK_DOY: c_int = 33;

/*
 * Bit mask definitions for time parsing.
 */
#[inline]
pub fn DTK_M(t: c_int) -> c_int {
    0x01 << t
}

/* Convenience: a second, plus any fractional component */
#[inline]
pub fn DTK_ALL_SECS_M() -> c_int {
    DTK_M(SECOND) | DTK_M(MILLISECOND) | DTK_M(MICROSECOND)
}
#[inline]
pub fn DTK_DATE_M() -> c_int {
    DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY)
}
#[inline]
pub fn DTK_TIME_M() -> c_int {
    DTK_M(HOUR) | DTK_M(MINUTE) | DTK_ALL_SECS_M()
}

/*
 * Working buffer size for input and output of interval, timestamp, etc.
 */
pub const MAXDATELEN: usize = 128;
/* only this many chars are stored in datetktbl */
pub const TOKMAXLEN: usize = 10;

/* keep this struct small; it gets used a lot */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct datetkn {
    pub token: [c_char; TOKMAXLEN + 1], /* always NUL-terminated */
    pub r#type: c_char,                 /* see field type codes above */
    pub value: int32,                   /* meaning depends on type */
}

/* one of its uses is in tables of time zone abbreviations */
#[repr(C)]
pub struct TimeZoneAbbrevTable {
    pub tblsize: Size,    /* size in bytes of TimeZoneAbbrevTable */
    pub numabbrevs: c_int, /* number of entries in abbrevs[] array */
    pub abbrevs: [datetkn; 0], /* FLEXIBLE_ARRAY_MEMBER */
    /* DynamicZoneAbbrev(s) may follow the abbrevs[] array */
}

/* auxiliary data for a dynamic time zone abbreviation (non-fixed-offset) */
#[repr(C)]
pub struct DynamicZoneAbbrev {
    pub tz: *mut pg_tz,    /* NULL if not yet looked up */
    pub zone: [c_char; 0], /* FLEXIBLE_ARRAY_MEMBER, NUL-terminated zone name */
}

/* Result codes for DecodeTimezoneName() */
pub const TZNAME_ZONE: c_int = 2;

/*
 * Datetime input parsing routines return one of these negative codes on
 * failure (datetime.h).
 */
pub const DTERR_FIELD_OVERFLOW: c_int = -2;
pub const DTERR_MD_FIELD_OVERFLOW: c_int = -3; /* triggers hint about DateStyle */
pub const DTERR_INTERVAL_OVERFLOW: c_int = -4;
pub const DTERR_TZDISP_OVERFLOW: c_int = -5;
pub const DTERR_BAD_TIMEZONE: c_int = -6;
pub const DTERR_BAD_ZONE_ABBREV: c_int = -7;

/* misc constants from datatype/timestamp.h */
pub const MAX_TIMESTAMP_PRECISION: c_int = 6;
pub const MAX_INTERVAL_PRECISION: c_int = 6;
pub const MONTHS_PER_YEAR: c_int = 12;
pub const DAYS_PER_MONTH: c_int = 30; /* assumes exactly 30 days per month */
pub const DAYS_PER_WEEK: c_int = 7;
pub const INTERVAL_FULL_RANGE: c_int = 0x7FFF;

#[inline]
pub fn INTERVAL_MASK(b: c_int) -> c_int {
    1 << b
}

/*
 * isleap(y): Gregorian leap-year test (datetime.h).
 */
#[inline]
pub fn isleap(y: c_int) -> bool {
    (y % 4) == 0 && ((y % 100) != 0 || (y % 400) == 0)
}

// ===========================================================================
// pg_itm / pg_itm_in (datatype/timestamp.h).  Not yet homed; canonical here
// for use by the interval decode/encode paths.
// ===========================================================================

#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_itm {
    pub tm_usec: c_int,
    pub tm_sec: c_int,
    pub tm_min: c_int,
    pub tm_hour: int64, /* needs to be wide */
    pub tm_mday: c_int,
    pub tm_mon: c_int,
    pub tm_year: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_itm_in {
    pub tm_usec: int64, /* needs to be wide */
    pub tm_mday: c_int,
    pub tm_mon: c_int,
    pub tm_year: c_int,
}

// ===========================================================================
// libc / common bindings used pervasively below.
// ===========================================================================

extern "C" {
    fn strtod(s: *const c_char, endptr: *mut *mut c_char) -> f64;
    fn strtol(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_long;
    fn atoi(s: *const c_char) -> c_int;
    fn rint(x: f64) -> f64;
    fn floor(x: f64) -> f64;
    fn ceil(x: f64) -> f64;
    fn isnan(x: f64) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn isalpha(c: c_int) -> c_int;
    fn isalnum(c: c_int) -> c_int;
    fn ispunct(c: c_int) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strcat(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    /* common/string.h */
    fn strtoint(str: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_int;
    /* port/strlcpy.c */
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;
    /* errno access */
    #[link_name = "__error"]
    fn __error() -> *mut c_int;
}

/* strtoi64(str, endptr, base) == (int64) strtol(str, endptr, base) (c.h) */
#[inline]
unsafe fn strtoi64(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> int64 {
    strtol(s, endptr, base) as int64
}

#[inline]
unsafe fn get_errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    *__error() = v;
}

const ERANGE: c_int = 34;
const INT_MIN_C: int64 = i32::MIN as int64;
const INT_MAX_C: int64 = i32::MAX as int64;

/* i64abs() from c.h */
#[inline]
fn i64abs(i: int64) -> int64 {
    i.unsigned_abs() as int64
}

/* abs() helpers matching C int abs/INT semantics used in this file */
#[inline]
fn abs_i32(x: c_int) -> c_int {
    x.unsigned_abs() as c_int
}

/* cstr: render a C string for elog!/ereport! formatting */
unsafe fn cstr(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

// ===========================================================================
// TODO(pg-port) stubs for symbols whose real home is elsewhere.
// ===========================================================================

// time_overflows: real Rust symbol in crate::utils::adt::date
use crate::utils::adt::date::time_overflows;

// dt2time: utils/adt/timestamp.c -- break a usec count into h/m/s/fsec
unsafe fn dt2time(
    _jd: int64,
    _hour: *mut c_int,
    _min: *mut c_int,
    _sec: *mut c_int,
    _fsec: *mut fsec_t,
) {
    // TODO(pg-port): real symbol lives in crate::utils::adt::timestamp
    unimplemented!("dt2time: crate::utils::adt::timestamp")
}

// timestamptz_to_time_t: utils/adt/timestamp.c
unsafe fn timestamptz_to_time_t(_t: TimestampTz) -> pg_time_t {
    // TODO(pg-port): real symbol lives in crate::utils::adt::timestamp
    unimplemented!("timestamptz_to_time_t: crate::utils::adt::timestamp")
}

// crate::access::transam (xact.h)
pub unsafe fn GetCurrentTransactionStartTimestamp() -> TimestampTz {
    // TODO(pg-port): real symbol lives in crate::access::transam::xact
    unimplemented!("GetCurrentTransactionStartTimestamp: crate::access::transam::xact")
}

// crate::utils::adt::timestamp (timestamp2tm and itmin2interval)
unsafe fn timestamp2tm(
    _dt: Timestamp,
    _tzp: *mut c_int,
    _tm: *mut pg_tm,
    _fsec: *mut fsec_t,
    _tzn: *mut *const c_char,
    _attimezone: *mut pg_tz,
) -> c_int {
    // TODO(pg-port): real symbol lives in crate::utils::adt::timestamp
    unimplemented!("timestamp2tm: crate::utils::adt::timestamp")
}
unsafe fn itmin2interval(_itm_in: *const pg_itm_in, _span: *mut Interval) -> c_int {
    // TODO(pg-port): real symbol lives in crate::utils::adt::timestamp
    unimplemented!("itmin2interval: crate::utils::adt::timestamp")
}

// crate::parser::scansup (downcase_truncate_identifier)
unsafe fn downcase_truncate_identifier(
    _ident: *const c_char,
    _len: c_int,
    _warn: bool,
) -> *mut c_char {
    // TODO(pg-port): real symbol lives in crate::parser::scansup
    unimplemented!("downcase_truncate_identifier: crate::parser::scansup")
}

// crate::utils::adt::varlena / builtins (CStringGetTextDatum)
#[allow(non_snake_case)]
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum {
    // TODO(pg-port): real symbol lives in crate::utils::adt::varlena
    PointerGetDatum(crate::utils::adt::varlena::cstring_to_text(s) as *const c_void)
}

// crate::utils::adt::date (IntervalPGetDatum)
#[allow(non_snake_case)]
unsafe fn IntervalPGetDatum(x: *const Interval) -> Datum {
    PointerGetDatum(x as *const c_void)
}

// crate::utils::guc (guc_malloc)
unsafe fn guc_malloc(_elevel: c_int, _size: Size) -> *mut c_void {
    // TODO(pg-port): real symbol lives in crate::utils::misc::guc
    unimplemented!("guc_malloc: crate::utils::misc::guc")
}

// nodes/parsenodes + nodeFuncs (TemporalSimplify support)
pub type Node = c_void;

// tzparser.h
#[repr(C)]
pub struct tzEntry {
    pub abbrev: *mut c_char,
    pub zone: *mut c_char,
    pub offset: c_int,
    pub is_dst: bool,
    /* the remaining fields are used only for error reporting */
    pub lineno: c_int,
    pub filename: *const c_char,
}

// MAXALIGN (c.h)
const MAXIMUM_ALIGNOF: usize = 8;
#[inline]
fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

// ===========================================================================
// Static forward-declared functions are inlined below as `unsafe fn`.
// ===========================================================================

#[rustfmt::skip]
pub static day_tab: [[c_int; 13]; 2] = [
    [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
    [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0],
];

pub static mut months: [*const c_char; 13] = [
    b"Jan\0".as_ptr() as *const c_char,
    b"Feb\0".as_ptr() as *const c_char,
    b"Mar\0".as_ptr() as *const c_char,
    b"Apr\0".as_ptr() as *const c_char,
    b"May\0".as_ptr() as *const c_char,
    b"Jun\0".as_ptr() as *const c_char,
    b"Jul\0".as_ptr() as *const c_char,
    b"Aug\0".as_ptr() as *const c_char,
    b"Sep\0".as_ptr() as *const c_char,
    b"Oct\0".as_ptr() as *const c_char,
    b"Nov\0".as_ptr() as *const c_char,
    b"Dec\0".as_ptr() as *const c_char,
    std::ptr::null(),
];

pub static mut days: [*const c_char; 8] = [
    b"Sunday\0".as_ptr() as *const c_char,
    b"Monday\0".as_ptr() as *const c_char,
    b"Tuesday\0".as_ptr() as *const c_char,
    b"Wednesday\0".as_ptr() as *const c_char,
    b"Thursday\0".as_ptr() as *const c_char,
    b"Friday\0".as_ptr() as *const c_char,
    b"Saturday\0".as_ptr() as *const c_char,
    std::ptr::null(),
];

// ---------------------------------------------------------------------------
// datetkn table construction.  In C these are brace-initialized string/type/value
// triples; here we build the fixed [c_char; TOKMAXLEN+1] token field at compile
// time from a byte-string literal (NUL-padded, truncated to TOKMAXLEN).
// ---------------------------------------------------------------------------

const fn tok(s: &[u8]) -> [c_char; TOKMAXLEN + 1] {
    let mut out = [0 as c_char; TOKMAXLEN + 1];
    let mut i = 0;
    while i < s.len() && i < TOKMAXLEN {
        // skip trailing NUL of byte-string literals
        if s[i] == 0 {
            break;
        }
        out[i] = s[i] as c_char;
        i += 1;
    }
    out
}

const fn dtk(token: &[u8], r#type: c_int, value: c_int) -> datetkn {
    datetkn {
        token: tok(token),
        r#type: r#type as c_char,
        value: value as int32,
    }
}

/*
 * datetktbl holds date/time keywords.
 *
 * Note that this table must be strictly alphabetically ordered to allow an
 * O(ln(N)) search algorithm to be used.
 *
 * The token field must be NUL-terminated; we truncate entries to TOKMAXLEN
 * characters to fit.
 *
 * The static table contains no TZ, DTZ, or DYNTZ entries; rather those
 * are loaded from configuration files and stored in zoneabbrevtbl, whose
 * abbrevs[] field has the same format as the static datetktbl.
 */
#[rustfmt::skip]
static datetktbl: [datetkn; 72] = [
    /* token, type, value */
    dtk(b"+infinity", RESERV, DTK_LATE),    /* same as "infinity" */
    dtk(EARLY, RESERV, DTK_EARLY),          /* "-infinity" reserved for "early time" */
    dtk(DA_D, ADBC, AD),                    /* "ad" for years > 0 */
    dtk(b"allballs", RESERV, DTK_ZULU),     /* 00:00:00 */
    dtk(b"am", AMPM, AM),
    dtk(b"apr", MONTH, 4),
    dtk(b"april", MONTH, 4),
    dtk(b"at", IGNORE_DTF, 0),              /* "at" (throwaway) */
    dtk(b"aug", MONTH, 8),
    dtk(b"august", MONTH, 8),
    dtk(DB_C, ADBC, BC),                    /* "bc" for years <= 0 */
    dtk(b"d", UNITS, DTK_DAY),              /* "day of month" for ISO input */
    dtk(b"dec", MONTH, 12),
    dtk(b"december", MONTH, 12),
    dtk(b"dow", UNITS, DTK_DOW),            /* day of week */
    dtk(b"doy", UNITS, DTK_DOY),            /* day of year */
    dtk(b"dst", DTZMOD, SECS_PER_HOUR),
    dtk(EPOCH, RESERV, DTK_EPOCH),          /* "epoch" reserved for system epoch time */
    dtk(b"feb", MONTH, 2),
    dtk(b"february", MONTH, 2),
    dtk(b"fri", DOW, 5),
    dtk(b"friday", DOW, 5),
    dtk(b"h", UNITS, DTK_HOUR),             /* "hour" */
    dtk(LATE, RESERV, DTK_LATE),            /* "infinity" reserved for "late time" */
    dtk(b"isodow", UNITS, DTK_ISODOW),      /* ISO day of week, Sunday == 7 */
    dtk(b"isoyear", UNITS, DTK_ISOYEAR),    /* year in terms of the ISO week date */
    dtk(b"j", UNITS, DTK_JULIAN),
    dtk(b"jan", MONTH, 1),
    dtk(b"january", MONTH, 1),
    dtk(b"jd", UNITS, DTK_JULIAN),
    dtk(b"jul", MONTH, 7),
    dtk(b"julian", UNITS, DTK_JULIAN),
    dtk(b"july", MONTH, 7),
    dtk(b"jun", MONTH, 6),
    dtk(b"june", MONTH, 6),
    dtk(b"m", UNITS, DTK_MONTH),            /* "month" for ISO input */
    dtk(b"mar", MONTH, 3),
    dtk(b"march", MONTH, 3),
    dtk(b"may", MONTH, 5),
    dtk(b"mm", UNITS, DTK_MINUTE),          /* "minute" for ISO input */
    dtk(b"mon", DOW, 1),
    dtk(b"monday", DOW, 1),
    dtk(b"nov", MONTH, 11),
    dtk(b"november", MONTH, 11),
    dtk(NOW, RESERV, DTK_NOW),              /* current transaction time */
    dtk(b"oct", MONTH, 10),
    dtk(b"october", MONTH, 10),
    dtk(b"on", IGNORE_DTF, 0),              /* "on" (throwaway) */
    dtk(b"pm", AMPM, PM),
    dtk(b"s", UNITS, DTK_SECOND),           /* "seconds" for ISO input */
    dtk(b"sat", DOW, 6),
    dtk(b"saturday", DOW, 6),
    dtk(b"sep", MONTH, 9),
    dtk(b"sept", MONTH, 9),
    dtk(b"september", MONTH, 9),
    dtk(b"sun", DOW, 0),
    dtk(b"sunday", DOW, 0),
    dtk(b"t", ISOTIME, DTK_TIME),           /* Filler for ISO time fields */
    dtk(b"thu", DOW, 4),
    dtk(b"thur", DOW, 4),
    dtk(b"thurs", DOW, 4),
    dtk(b"thursday", DOW, 4),
    dtk(TODAY, RESERV, DTK_TODAY),          /* midnight */
    dtk(TOMORROW, RESERV, DTK_TOMORROW),    /* tomorrow midnight */
    dtk(b"tue", DOW, 2),
    dtk(b"tues", DOW, 2),
    dtk(b"tuesday", DOW, 2),
    dtk(b"wed", DOW, 3),
    dtk(b"wednesday", DOW, 3),
    dtk(b"weds", DOW, 3),
    dtk(b"y", UNITS, DTK_YEAR),             /* "year" for ISO input */
    dtk(YESTERDAY, RESERV, DTK_YESTERDAY),  /* yesterday midnight */
];

static szdatetktbl: c_int = datetktbl.len() as c_int;

/*
 * deltatktbl: same format as datetktbl, but holds keywords used to represent
 * time units (eg, for intervals, and for EXTRACT).
 */
#[rustfmt::skip]
static deltatktbl: [datetkn; 61] = [
    /* token, type, value */
    dtk(b"@", IGNORE_DTF, 0),               /* postgres relative prefix */
    dtk(DAGO, AGO, 0),                      /* "ago" indicates negative time offset */
    dtk(b"c", UNITS, DTK_CENTURY),          /* "century" relative */
    dtk(b"cent", UNITS, DTK_CENTURY),       /* "century" relative */
    dtk(b"centuries", UNITS, DTK_CENTURY),  /* "centuries" relative */
    dtk(DCENTURY, UNITS, DTK_CENTURY),      /* "century" relative */
    dtk(b"d", UNITS, DTK_DAY),              /* "day" relative */
    dtk(DDAY, UNITS, DTK_DAY),              /* "day" relative */
    dtk(b"days", UNITS, DTK_DAY),           /* "days" relative */
    dtk(b"dec", UNITS, DTK_DECADE),         /* "decade" relative */
    dtk(DDECADE, UNITS, DTK_DECADE),        /* "decade" relative */
    dtk(b"decades", UNITS, DTK_DECADE),     /* "decades" relative */
    dtk(b"decs", UNITS, DTK_DECADE),        /* "decades" relative */
    dtk(b"h", UNITS, DTK_HOUR),             /* "hour" relative */
    dtk(DHOUR, UNITS, DTK_HOUR),            /* "hour" relative */
    dtk(b"hours", UNITS, DTK_HOUR),         /* "hours" relative */
    dtk(b"hr", UNITS, DTK_HOUR),            /* "hour" relative */
    dtk(b"hrs", UNITS, DTK_HOUR),           /* "hours" relative */
    dtk(b"m", UNITS, DTK_MINUTE),           /* "minute" relative */
    dtk(b"microsecon", UNITS, DTK_MICROSEC), /* "microsecond" relative */
    dtk(b"mil", UNITS, DTK_MILLENNIUM),     /* "millennium" relative */
    dtk(b"millennia", UNITS, DTK_MILLENNIUM), /* "millennia" relative */
    dtk(DMILLENNIUM, UNITS, DTK_MILLENNIUM), /* "millennium" relative */
    dtk(b"millisecon", UNITS, DTK_MILLISEC), /* relative */
    dtk(b"mils", UNITS, DTK_MILLENNIUM),    /* "millennia" relative */
    dtk(b"min", UNITS, DTK_MINUTE),         /* "minute" relative */
    dtk(b"mins", UNITS, DTK_MINUTE),        /* "minutes" relative */
    dtk(DMINUTE, UNITS, DTK_MINUTE),        /* "minute" relative */
    dtk(b"minutes", UNITS, DTK_MINUTE),     /* "minutes" relative */
    dtk(b"mon", UNITS, DTK_MONTH),          /* "months" relative */
    dtk(b"mons", UNITS, DTK_MONTH),         /* "months" relative */
    dtk(DMONTH, UNITS, DTK_MONTH),          /* "month" relative */
    dtk(b"months", UNITS, DTK_MONTH),
    dtk(b"ms", UNITS, DTK_MILLISEC),
    dtk(b"msec", UNITS, DTK_MILLISEC),
    dtk(DMILLISEC, UNITS, DTK_MILLISEC),
    dtk(b"mseconds", UNITS, DTK_MILLISEC),
    dtk(b"msecs", UNITS, DTK_MILLISEC),
    dtk(b"qtr", UNITS, DTK_QUARTER),        /* "quarter" relative */
    dtk(DQUARTER, UNITS, DTK_QUARTER),      /* "quarter" relative */
    dtk(b"s", UNITS, DTK_SECOND),
    dtk(b"sec", UNITS, DTK_SECOND),
    dtk(DSECOND, UNITS, DTK_SECOND),
    dtk(b"seconds", UNITS, DTK_SECOND),
    dtk(b"secs", UNITS, DTK_SECOND),
    dtk(DTIMEZONE, UNITS, DTK_TZ),          /* "timezone" time offset */
    dtk(b"timezone_h", UNITS, DTK_TZ_HOUR), /* timezone hour units */
    dtk(b"timezone_m", UNITS, DTK_TZ_MINUTE), /* timezone minutes units */
    dtk(b"us", UNITS, DTK_MICROSEC),        /* "microsecond" relative */
    dtk(b"usec", UNITS, DTK_MICROSEC),      /* "microsecond" relative */
    dtk(DMICROSEC, UNITS, DTK_MICROSEC),    /* "microsecond" relative */
    dtk(b"useconds", UNITS, DTK_MICROSEC),  /* "microseconds" relative */
    dtk(b"usecs", UNITS, DTK_MICROSEC),     /* "microseconds" relative */
    dtk(b"w", UNITS, DTK_WEEK),             /* "week" relative */
    dtk(DWEEK, UNITS, DTK_WEEK),            /* "week" relative */
    dtk(b"weeks", UNITS, DTK_WEEK),         /* "weeks" relative */
    dtk(b"y", UNITS, DTK_YEAR),             /* "year" relative */
    dtk(DYEAR, UNITS, DTK_YEAR),            /* "year" relative */
    dtk(b"years", UNITS, DTK_YEAR),         /* "years" relative */
    dtk(b"yr", UNITS, DTK_YEAR),            /* "year" relative */
    dtk(b"yrs", UNITS, DTK_YEAR),           /* "years" relative */
];

static szdeltatktbl: c_int = deltatktbl.len() as c_int;

static mut zoneabbrevtbl: *mut TimeZoneAbbrevTable = std::ptr::null_mut();

/* Caches of recent lookup results in the above tables */
static mut datecache: [*const datetkn; MAXDATEFIELDS] = [std::ptr::null(); MAXDATEFIELDS];
static mut deltacache: [*const datetkn; MAXDATEFIELDS] = [std::ptr::null(); MAXDATEFIELDS];

/* Cache for results of timezone abbreviation lookups */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TzAbbrevCache {
    pub abbrev: [c_char; TOKMAXLEN + 1], /* always NUL-terminated */
    pub ftype: c_char,                   /* TZ, DTZ, or DYNTZ */
    pub offset: c_int,                   /* GMT offset, if fixed-offset */
    pub tz: *mut pg_tz,                  /* relevant zone, if variable-offset */
}

static mut tzabbrevcache: [TzAbbrevCache; MAXDATEFIELDS] = [TzAbbrevCache {
    abbrev: [0; TOKMAXLEN + 1],
    ftype: 0,
    offset: 0,
    tz: std::ptr::null_mut(),
}; MAXDATEFIELDS];

/*
 * Calendar time to Julian date conversions.
 */
pub unsafe fn date2j(mut year: c_int, mut month: c_int, day: c_int) -> c_int {
    let mut julian: c_int;
    let century: c_int;

    if month > 2 {
        month += 1;
        year += 4800;
    } else {
        month += 13;
        year += 4799;
    }

    century = year / 100;
    julian = year * 365 - 32167;
    julian += year / 4 - century + century / 4;
    julian += 7834 * month / 256 + day;

    julian
} /* date2j() */

pub unsafe fn j2date(jd: c_int, year: *mut c_int, month: *mut c_int, day: *mut c_int) {
    let mut julian: u32;
    let mut quad: u32;
    let extra: u32;
    let mut y: c_int;

    julian = jd as u32;
    julian = julian.wrapping_add(32044);
    quad = julian / 146097;
    extra = (julian.wrapping_sub(quad.wrapping_mul(146097))).wrapping_mul(4).wrapping_add(3);
    julian = julian.wrapping_add(60 + quad.wrapping_mul(3) + extra / 146097);
    quad = julian / 1461;
    julian = julian.wrapping_sub(quad.wrapping_mul(1461));
    y = (julian.wrapping_mul(4) / 1461) as c_int;
    julian = (if y != 0 {
        (julian + 305) % 365
    } else {
        (julian + 306) % 366
    }) + 123;
    y += quad.wrapping_mul(4) as c_int;
    *year = y - 4800;
    quad = julian.wrapping_mul(2141) / 65536;
    *day = (julian.wrapping_sub(7834u32.wrapping_mul(quad) / 256)) as c_int;
    *month = ((quad.wrapping_add(10) % MONTHS_PER_YEAR as u32) + 1) as c_int;
} /* j2date() */

/*
 * j2day - convert Julian date to day-of-week (0..6 == Sun..Sat)
 */
pub unsafe fn j2day(mut date: c_int) -> c_int {
    date += 1;
    date %= 7;
    /* Cope if division truncates towards zero, as it probably does */
    if date < 0 {
        date += 7;
    }

    date
} /* j2day() */

/*
 * GetCurrentDateTime()
 */
pub unsafe fn GetCurrentDateTime(tm: *mut pg_tm) {
    let mut fsec: fsec_t = 0;

    GetCurrentTimeUsec(tm, &mut fsec, std::ptr::null_mut());
}

/*
 * GetCurrentTimeUsec()
 *
 * Internally, we cache the result, since this could be called many times
 * in a transaction, within which now() doesn't change.
 */
pub unsafe fn GetCurrentTimeUsec(tm: *mut pg_tm, fsec: *mut fsec_t, tzp: *mut c_int) {
    let cur_ts: TimestampTz = GetCurrentTransactionStartTimestamp();

    /*
     * The cache key must include both current time and current timezone.
     */
    static mut cache_ts: TimestampTz = 0;
    static mut cache_timezone: *mut pg_tz = std::ptr::null_mut();
    static mut cache_tm: pg_tm = pg_tm {
        tm_sec: 0,
        tm_min: 0,
        tm_hour: 0,
        tm_mday: 0,
        tm_mon: 0,
        tm_year: 0,
        tm_wday: 0,
        tm_yday: 0,
        tm_isdst: 0,
        tm_gmtoff: 0,
        tm_zone: std::ptr::null(),
    };
    static mut cache_fsec: fsec_t = 0;
    static mut cache_tz: c_int = 0;

    if cur_ts != cache_ts || session_timezone != cache_timezone {
        /*
         * Make sure cache is marked invalid in case of error after partial
         * update within timestamp2tm.
         */
        cache_timezone = std::ptr::null_mut();

        /*
         * Perform the computation, storing results into cache.
         */
        if timestamp2tm(
            cur_ts,
            &raw mut cache_tz,
            &raw mut cache_tm,
            &raw mut cache_fsec,
            std::ptr::null_mut(),
            session_timezone,
        ) != 0
        {
            ereport!(ERROR, errmsg!("timestamp out of range"));
        }

        /* OK, so mark the cache valid. */
        cache_ts = cur_ts;
        cache_timezone = session_timezone;
    }

    *tm = core::ptr::read(&raw const cache_tm);
    *fsec = cache_fsec;
    if !tzp.is_null() {
        *tzp = cache_tz;
    }
}

/*
 * Append seconds and fractional seconds (if any) at *cp.
 */
pub unsafe fn AppendSeconds(
    mut cp: *mut c_char,
    sec: c_int,
    fsec: fsec_t,
    mut precision: c_int,
    fillzeros: bool,
) -> *mut c_char {
    Assert!(precision >= 0);

    if fillzeros {
        cp = pg_ultostr_zeropad(cp, abs_i32(sec) as uint32, 2);
    } else {
        cp = pg_ultostr(cp, abs_i32(sec) as uint32);
    }

    /* fsec_t is just an int32 */
    if fsec != 0 {
        let mut value: int32 = abs_i32(fsec);
        let mut end: *mut c_char = &mut *cp.offset((precision + 1) as isize);
        let mut gotnonzero = false;

        *cp = b'.' as c_char;
        cp = cp.offset(1);

        /*
         * Append the fractional seconds part.
         */
        while precision != 0 {
            precision -= 1;
            let oldval: int32 = value;
            let remainder: int32;

            value /= 10;
            remainder = oldval - value * 10;

            /* check if we got a non-zero */
            if remainder != 0 {
                gotnonzero = true;
            }

            if gotnonzero {
                *cp.offset(precision as isize) = (b'0' as int32 + remainder) as c_char;
            } else {
                end = &mut *cp.offset(precision as isize);
            }
        }

        /*
         * If we still have a non-zero value then precision must have not been
         * enough to print the number.
         */
        if value != 0 {
            return pg_ultostr(cp, abs_i32(fsec) as uint32);
        }

        return end;
    } else {
        cp
    }
}

/*
 * Variant of above that's specialized to timestamp case.
 */
unsafe fn AppendTimestampSeconds(cp: *mut c_char, tm: *mut pg_tm, fsec: fsec_t) -> *mut c_char {
    AppendSeconds(cp, (*tm).tm_sec, fsec, MAX_TIMESTAMP_PRECISION, true)
}

/*
 * Add val * multiplier to *sum.
 */
unsafe fn int64_multiply_add(val: int64, multiplier: int64, sum: *mut int64) -> bool {
    let mut product: int64 = 0;

    if pg_mul_s64_overflow(val, multiplier, &mut product) || pg_add_s64_overflow(*sum, product, sum)
    {
        return false;
    }
    true
}

/*
 * Multiply frac by scale (to produce microseconds) and add to itm_in->tm_usec.
 */
unsafe fn AdjustFractMicroseconds(mut frac: f64, scale: int64, itm_in: *mut pg_itm_in) -> bool {
    let usec: int64;

    /* Fast path for common case */
    if frac == 0.0 {
        return true;
    }

    frac *= scale as f64;
    let mut usec_v = frac as int64;

    /* Round off any fractional microsecond */
    frac -= usec_v as f64;
    if frac > 0.5 {
        usec_v += 1;
    } else if frac < -0.5 {
        usec_v -= 1;
    }
    usec = usec_v;

    !pg_add_s64_overflow((*itm_in).tm_usec, usec, &raw mut (*itm_in).tm_usec)
}

/*
 * Multiply frac by scale (to produce days).
 */
unsafe fn AdjustFractDays(mut frac: f64, scale: c_int, itm_in: *mut pg_itm_in) -> bool {
    let extra_days: c_int;

    /* Fast path for common case */
    if frac == 0.0 {
        return true;
    }

    frac *= scale as f64;
    extra_days = frac as c_int;

    /* ... but this could overflow, if tm_mday is already nonzero */
    if pg_add_s32_overflow((*itm_in).tm_mday, extra_days, &raw mut (*itm_in).tm_mday) {
        return false;
    }

    /* Handle any fractional day */
    frac -= extra_days as f64;
    AdjustFractMicroseconds(frac, USECS_PER_DAY, itm_in)
}

/*
 * Multiply frac by scale (to produce years), then further scale up to months.
 */
unsafe fn AdjustFractYears(frac: f64, scale: c_int, itm_in: *mut pg_itm_in) -> bool {
    let extra_months: c_int = rint(frac * scale as f64 * MONTHS_PER_YEAR as f64) as c_int;

    !pg_add_s32_overflow((*itm_in).tm_mon, extra_months, &raw mut (*itm_in).tm_mon)
}

/*
 * Add (val + fval) * scale to itm_in->tm_usec.
 */
unsafe fn AdjustMicroseconds(
    val: int64,
    fval: f64,
    scale: int64,
    itm_in: *mut pg_itm_in,
) -> bool {
    /* Handle the integer part */
    if !int64_multiply_add(val, scale, &raw mut (*itm_in).tm_usec) {
        return false;
    }
    /* Handle the float part */
    AdjustFractMicroseconds(fval, scale, itm_in)
}

/*
 * Multiply val by scale (to produce days) and add to itm_in->tm_mday.
 */
unsafe fn AdjustDays(val: int64, scale: c_int, itm_in: *mut pg_itm_in) -> bool {
    let mut ndays: int32 = 0;

    if val < INT_MIN_C || val > INT_MAX_C {
        return false;
    }
    !pg_mul_s32_overflow(val as int32, scale, &mut ndays)
        && !pg_add_s32_overflow((*itm_in).tm_mday, ndays, &raw mut (*itm_in).tm_mday)
}

/*
 * Add val to itm_in->tm_mon.
 */
unsafe fn AdjustMonths(val: int64, itm_in: *mut pg_itm_in) -> bool {
    if val < INT_MIN_C || val > INT_MAX_C {
        return false;
    }
    !pg_add_s32_overflow((*itm_in).tm_mon, val as int32, &raw mut (*itm_in).tm_mon)
}

/*
 * Multiply val by scale (to produce years) and add to itm_in->tm_year.
 */
unsafe fn AdjustYears(val: int64, scale: c_int, itm_in: *mut pg_itm_in) -> bool {
    let mut years: int32 = 0;

    if val < INT_MIN_C || val > INT_MAX_C {
        return false;
    }
    !pg_mul_s32_overflow(val as int32, scale, &mut years)
        && !pg_add_s32_overflow((*itm_in).tm_year, years, &raw mut (*itm_in).tm_year)
}

/*
 * Parse the fractional part of a number.
 * Returns 0 if successful, DTERR code if bogus input detected.
 */
unsafe fn ParseFraction(cp: *mut c_char, frac: *mut f64) -> c_int {
    /* Caller should always pass the start of the fraction part */
    Assert!(*cp == b'.' as c_char);

    /*
     * We want to allow just "." with no digits, but some versions of strtod
     * will report EINVAL for that, so special-case it.
     */
    if *cp.offset(1) == 0 {
        *frac = 0.0;
    } else {
        /*
         * reject anything that's not digits after the "."
         */
        if strspn(cp.offset(1), b"0123456789\0".as_ptr() as *const c_char)
            != strlen(cp.offset(1))
        {
            return DTERR_BAD_FORMAT;
        }

        set_errno(0);
        let mut endp: *mut c_char = std::ptr::null_mut();
        *frac = strtod(cp, &mut endp);
        /* check for parse failure (probably redundant given prior check) */
        if *endp != 0 || get_errno() != 0 {
            return DTERR_BAD_FORMAT;
        }
    }
    0
}

/*
 * Fetch a fractional-second value with suitable error checking.
 */
unsafe fn ParseFractionalSecond(cp: *mut c_char, fsec: *mut fsec_t) -> c_int {
    let mut frac: f64 = 0.0;
    let dterr: c_int;

    dterr = ParseFraction(cp, &mut frac);
    if dterr != 0 {
        return dterr;
    }
    *fsec = rint(frac * 1000000.0) as fsec_t;
    0
}

/* ParseDateTime()
 *	Break string into tokens based on a date/time context.
 *	Returns 0 if successful, DTERR code if bogus input detected.
 */
pub unsafe fn ParseDateTime(
    timestr: *const c_char,
    workbuf: *mut c_char,
    buflen: usize,
    field: *mut *mut c_char,
    ftype: *mut c_int,
    maxfields: c_int,
    numfields: *mut c_int,
) -> c_int {
    let mut nf: c_int = 0;
    let mut cp: *const c_char = timestr;
    let mut bufp: *mut c_char = workbuf;
    let bufend: *const c_char = workbuf.offset(buflen as isize);

    /*
     * APPEND_CHAR(bufptr, end, newchar): append a char to the work buffer,
     * returning DTERR_BAD_FORMAT if there is no room.
     */
    macro_rules! APPEND_CHAR {
        ($bufptr:expr, $end:expr, $newchar:expr) => {{
            if ($bufptr).offset(1) >= ($end) as *mut c_char {
                return DTERR_BAD_FORMAT;
            }
            *($bufptr) = $newchar;
            $bufptr = ($bufptr).offset(1);
        }};
    }

    /* outer loop through fields */
    while *cp != 0 {
        /* Ignore spaces between fields */
        if isspace(*cp as u8 as c_int) != 0 {
            cp = cp.offset(1);
            continue;
        }

        /* Record start of current field */
        if nf >= maxfields {
            return DTERR_BAD_FORMAT;
        }
        *field.offset(nf as isize) = bufp;

        /* leading digit? then date or time */
        if isdigit(*cp as u8 as c_int) != 0 {
            APPEND_CHAR!(bufp, bufend, {
                let c = *cp;
                cp = cp.offset(1);
                c
            });
            while isdigit(*cp as u8 as c_int) != 0 {
                APPEND_CHAR!(bufp, bufend, {
                    let c = *cp;
                    cp = cp.offset(1);
                    c
                });
            }

            /* time field? */
            if *cp == b':' as c_char {
                *ftype.offset(nf as isize) = DTK_TIME;
                APPEND_CHAR!(bufp, bufend, {
                    let c = *cp;
                    cp = cp.offset(1);
                    c
                });
                while isdigit(*cp as u8 as c_int) != 0
                    || *cp == b':' as c_char
                    || *cp == b'.' as c_char
                {
                    APPEND_CHAR!(bufp, bufend, {
                        let c = *cp;
                        cp = cp.offset(1);
                        c
                    });
                }
            }
            /* date field? allow embedded text month */
            else if *cp == b'-' as c_char || *cp == b'/' as c_char || *cp == b'.' as c_char {
                /* save delimiting character to use later */
                let delim: c_char = *cp;

                APPEND_CHAR!(bufp, bufend, {
                    let c = *cp;
                    cp = cp.offset(1);
                    c
                });
                /* second field is all digits? then no embedded text month */
                if isdigit(*cp as u8 as c_int) != 0 {
                    *ftype.offset(nf as isize) =
                        if delim == b'.' as c_char { DTK_NUMBER } else { DTK_DATE };
                    while isdigit(*cp as u8 as c_int) != 0 {
                        APPEND_CHAR!(bufp, bufend, {
                            let c = *cp;
                            cp = cp.offset(1);
                            c
                        });
                    }

                    /*
                     * insist that the delimiters match to get a three-field
                     * date.
                     */
                    if *cp == delim {
                        *ftype.offset(nf as isize) = DTK_DATE;
                        APPEND_CHAR!(bufp, bufend, {
                            let c = *cp;
                            cp = cp.offset(1);
                            c
                        });
                        while isdigit(*cp as u8 as c_int) != 0 || *cp == delim {
                            APPEND_CHAR!(bufp, bufend, {
                                let c = *cp;
                                cp = cp.offset(1);
                                c
                            });
                        }
                    }
                } else {
                    *ftype.offset(nf as isize) = DTK_DATE;
                    while isalnum(*cp as u8 as c_int) != 0 || *cp == delim {
                        APPEND_CHAR!(bufp, bufend, {
                            let c = pg_tolower(*cp as u8) as c_char;
                            cp = cp.offset(1);
                            c
                        });
                    }
                }
            }
            /*
             * otherwise, number only and will determine year, month, day, or
             * concatenated fields later...
             */
            else {
                *ftype.offset(nf as isize) = DTK_NUMBER;
            }
        }
        /* Leading decimal point? Then fractional seconds... */
        else if *cp == b'.' as c_char {
            APPEND_CHAR!(bufp, bufend, {
                let c = *cp;
                cp = cp.offset(1);
                c
            });
            while isdigit(*cp as u8 as c_int) != 0 {
                APPEND_CHAR!(bufp, bufend, {
                    let c = *cp;
                    cp = cp.offset(1);
                    c
                });
            }

            *ftype.offset(nf as isize) = DTK_NUMBER;
        }
        /*
         * text? then date string, month, day of week, special, or timezone
         */
        else if isalpha(*cp as u8 as c_int) != 0 {
            let mut is_date: bool;

            *ftype.offset(nf as isize) = DTK_STRING;
            APPEND_CHAR!(bufp, bufend, {
                let c = pg_tolower(*cp as u8) as c_char;
                cp = cp.offset(1);
                c
            });
            while isalpha(*cp as u8 as c_int) != 0 {
                APPEND_CHAR!(bufp, bufend, {
                    let c = pg_tolower(*cp as u8) as c_char;
                    cp = cp.offset(1);
                    c
                });
            }

            /*
             * Dates can have embedded '-', '/', or '.' separators.
             */
            is_date = false;
            if *cp == b'-' as c_char || *cp == b'/' as c_char || *cp == b'.' as c_char {
                is_date = true;
            } else if *cp == b'+' as c_char || isdigit(*cp as u8 as c_int) != 0 {
                *bufp = 0; /* null-terminate current field value */
                /* we need search only the core token table, not TZ names */
                if datebsearch(
                    *field.offset(nf as isize),
                    datetktbl.as_ptr(),
                    szdatetktbl,
                )
                .is_null()
                {
                    is_date = true;
                }
            }
            if is_date {
                *ftype.offset(nf as isize) = DTK_DATE;
                loop {
                    APPEND_CHAR!(bufp, bufend, {
                        let c = pg_tolower(*cp as u8) as c_char;
                        cp = cp.offset(1);
                        c
                    });
                    if !(*cp == b'+' as c_char
                        || *cp == b'-' as c_char
                        || *cp == b'/' as c_char
                        || *cp == b'_' as c_char
                        || *cp == b'.' as c_char
                        || *cp == b':' as c_char
                        || isalnum(*cp as u8 as c_int) != 0)
                    {
                        break;
                    }
                }
            }
        }
        /* sign? then special or numeric timezone */
        else if *cp == b'+' as c_char || *cp == b'-' as c_char {
            APPEND_CHAR!(bufp, bufend, {
                let c = *cp;
                cp = cp.offset(1);
                c
            });
            /* soak up leading whitespace */
            while isspace(*cp as u8 as c_int) != 0 {
                cp = cp.offset(1);
            }
            /* numeric timezone? */
            if isdigit(*cp as u8 as c_int) != 0 {
                *ftype.offset(nf as isize) = DTK_TZ;
                APPEND_CHAR!(bufp, bufend, {
                    let c = *cp;
                    cp = cp.offset(1);
                    c
                });
                while isdigit(*cp as u8 as c_int) != 0
                    || *cp == b':' as c_char
                    || *cp == b'.' as c_char
                    || *cp == b'-' as c_char
                {
                    APPEND_CHAR!(bufp, bufend, {
                        let c = *cp;
                        cp = cp.offset(1);
                        c
                    });
                }
            }
            /* special? */
            else if isalpha(*cp as u8 as c_int) != 0 {
                *ftype.offset(nf as isize) = DTK_SPECIAL;
                APPEND_CHAR!(bufp, bufend, {
                    let c = pg_tolower(*cp as u8) as c_char;
                    cp = cp.offset(1);
                    c
                });
                while isalpha(*cp as u8 as c_int) != 0 {
                    APPEND_CHAR!(bufp, bufend, {
                        let c = pg_tolower(*cp as u8) as c_char;
                        cp = cp.offset(1);
                        c
                    });
                }
            }
            /* otherwise something wrong... */
            else {
                return DTERR_BAD_FORMAT;
            }
        }
        /* ignore other punctuation but use as delimiter */
        else if ispunct(*cp as u8 as c_int) != 0 {
            cp = cp.offset(1);
            continue;
        }
        /* otherwise, something is not right... */
        else {
            return DTERR_BAD_FORMAT;
        }

        /* force in a delimiter after each field */
        *bufp = 0;
        bufp = bufp.offset(1);
        nf += 1;
    }

    *numfields = nf;

    0
}

/* DecodeDateTime()
 * Interpret previously parsed fields for general date and time.
 * Return 0 if full date, 1 if only time, and negative DTERR code if problems.
 */
pub unsafe fn DecodeDateTime(
    field: *mut *mut c_char,
    ftype: *const c_int,
    nf: c_int,
    dtype: *mut c_int,
    tm: *mut pg_tm,
    fsec: *mut fsec_t,
    tzp: *mut c_int,
    extra: *mut DateTimeErrorExtra,
) -> c_int {
    let mut fmask: c_int = 0;
    let mut tmask: c_int = 0;
    let mut r#type: c_int;
    let mut ptype: c_int = 0; /* "prefix type" for ISO and Julian formats */
    let mut i: c_int;
    let mut val: c_int = 0;
    let mut dterr: c_int;
    let mut mer: c_int = HR24;
    let mut haveTextMonth = false;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut namedTz: *mut pg_tz = std::ptr::null_mut();
    let mut abbrevTz: *mut pg_tz = std::ptr::null_mut();
    let mut valtz: *mut pg_tz = std::ptr::null_mut();
    let mut abbrev: *mut c_char = std::ptr::null_mut();
    let mut cur_tm: pg_tm = std::mem::zeroed();

    /*
     * We'll insist on at least all of the date fields, but initialize the
     * remaining fields in case they are not set later...
     */
    *dtype = DTK_DATE;
    (*tm).tm_hour = 0;
    (*tm).tm_min = 0;
    (*tm).tm_sec = 0;
    *fsec = 0;
    /* don't know daylight savings time status apriori */
    (*tm).tm_isdst = -1;
    if !tzp.is_null() {
        *tzp = 0;
    }

    i = 0;
    while i < nf {
        match *ftype.offset(i as isize) {
            x if x == DTK_DATE => {
                /*
                 * Integral julian day with attached time zone?
                 */
                if ptype == DTK_JULIAN {
                    let mut endp: *mut c_char = std::ptr::null_mut();
                    let jday: c_int;

                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }

                    set_errno(0);
                    jday = strtoint(*field.offset(i as isize), &mut endp, 10);
                    if get_errno() == ERANGE || jday < 0 {
                        return DTERR_FIELD_OVERFLOW;
                    }

                    j2date(
                        jday,
                        &raw mut (*tm).tm_year,
                        &raw mut (*tm).tm_mon,
                        &raw mut (*tm).tm_mday,
                    );
                    isjulian = true;

                    /* Get the time zone from the end of the string */
                    dterr = DecodeTimezone(endp, tzp);
                    if dterr != 0 {
                        return dterr;
                    }

                    tmask = DTK_DATE_M() | DTK_TIME_M() | DTK_M(TZ);
                    ptype = 0;
                }
                /*
                 * Already have a date? Then this might be a time zone name with
                 * embedded punctuation or a run-together time with trailing
                 * time zone.
                 */
                else if ptype != 0
                    || (fmask & (DTK_M(MONTH) | DTK_M(DAY))) == (DTK_M(MONTH) | DTK_M(DAY))
                {
                    /* No time zone accepted? Then quit... */
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }

                    if isdigit(**field.offset(i as isize) as u8 as c_int) != 0 || ptype != 0 {
                        let cp: *mut c_char;

                        /*
                         * Allow a preceding "t" field, but no other units.
                         */
                        if ptype != 0 {
                            /* Sanity check; should not fail this test */
                            if ptype != DTK_TIME {
                                return DTERR_BAD_FORMAT;
                            }
                            ptype = 0;
                        }

                        /*
                         * Starts with a digit but we already have a time field?
                         */
                        if (fmask & DTK_TIME_M()) == DTK_TIME_M() {
                            return DTERR_BAD_FORMAT;
                        }

                        cp = strchr(*field.offset(i as isize), b'-' as c_int);
                        if cp.is_null() {
                            return DTERR_BAD_FORMAT;
                        }

                        /* Get the time zone from the end of the string */
                        dterr = DecodeTimezone(cp, tzp);
                        if dterr != 0 {
                            return dterr;
                        }
                        *cp = 0;

                        /*
                         * Then read the rest of the field as a concatenated time
                         */
                        dterr = DecodeNumberField(
                            strlen(*field.offset(i as isize)) as c_int,
                            *field.offset(i as isize),
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }

                        tmask |= DTK_M(TZ);
                    } else {
                        namedTz = pg_tzset(*field.offset(i as isize));
                        if namedTz.is_null() {
                            (*extra).dtee_timezone = *field.offset(i as isize);
                            return DTERR_BAD_TIMEZONE;
                        }
                        /* we'll apply the zone setting below */
                        tmask = DTK_M(TZ);
                    }
                } else {
                    dterr = DecodeDate(
                        *field.offset(i as isize),
                        fmask,
                        &mut tmask,
                        &mut is2digits,
                        tm,
                    );
                    if dterr != 0 {
                        return dterr;
                    }
                }
            }

            x if x == DTK_TIME => {
                /*
                 * This might be an ISO time following a "t" field.
                 */
                if ptype != 0 {
                    /* Sanity check; should not fail this test */
                    if ptype != DTK_TIME {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = 0;
                }
                dterr = DecodeTime(
                    *field.offset(i as isize),
                    fmask,
                    INTERVAL_FULL_RANGE,
                    &mut tmask,
                    tm,
                    fsec,
                );
                if dterr != 0 {
                    return dterr;
                }

                /* check for time overflow */
                if time_overflows((*tm).tm_hour, (*tm).tm_min, (*tm).tm_sec, *fsec) {
                    return DTERR_FIELD_OVERFLOW;
                }
            }

            x if x == DTK_TZ => {
                let mut tz: c_int = 0;

                if tzp.is_null() {
                    return DTERR_BAD_FORMAT;
                }

                dterr = DecodeTimezone(*field.offset(i as isize), &mut tz);
                if dterr != 0 {
                    return dterr;
                }
                *tzp = tz;
                tmask = DTK_M(TZ);
            }

            x if x == DTK_NUMBER => {
                /*
                 * Deal with cases where previous field labeled this one
                 */
                if ptype != 0 {
                    let mut cp: *mut c_char = std::ptr::null_mut();
                    let value: c_int;

                    set_errno(0);
                    value = strtoint(*field.offset(i as isize), &mut cp, 10);
                    if get_errno() == ERANGE {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if *cp != b'.' as c_char && *cp != 0 {
                        return DTERR_BAD_FORMAT;
                    }

                    if ptype == DTK_JULIAN {
                        /* previous field was a label for "julian date" */
                        if value < 0 {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_DATE_M();
                        j2date(
                            value,
                            &raw mut (*tm).tm_year,
                            &raw mut (*tm).tm_mon,
                            &raw mut (*tm).tm_mday,
                        );
                        isjulian = true;

                        /* fractional Julian Day? */
                        if *cp == b'.' as c_char {
                            let mut time: f64 = 0.0;

                            dterr = ParseFraction(cp, &mut time);
                            if dterr != 0 {
                                return dterr;
                            }
                            time *= USECS_PER_DAY as f64;
                            dt2time(
                                time as int64,
                                &raw mut (*tm).tm_hour,
                                &raw mut (*tm).tm_min,
                                &raw mut (*tm).tm_sec,
                                fsec,
                            );
                            tmask |= DTK_TIME_M();
                        }
                    } else if ptype == DTK_TIME {
                        /* previous field was "t" for ISO time */
                        dterr = DecodeNumberField(
                            strlen(*field.offset(i as isize)) as c_int,
                            *field.offset(i as isize),
                            fmask | DTK_DATE_M(),
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                        if tmask != DTK_TIME_M() {
                            return DTERR_BAD_FORMAT;
                        }
                    } else {
                        return DTERR_BAD_FORMAT;
                    }

                    ptype = 0;
                    *dtype = DTK_DATE;
                } else {
                    let cp: *mut c_char;
                    let flen: c_int;

                    flen = strlen(*field.offset(i as isize)) as c_int;
                    cp = strchr(*field.offset(i as isize), b'.' as c_int);

                    /* Embedded decimal and no date yet? */
                    if !cp.is_null() && (fmask & DTK_DATE_M()) == 0 {
                        dterr = DecodeDate(
                            *field.offset(i as isize),
                            fmask,
                            &mut tmask,
                            &mut is2digits,
                            tm,
                        );
                        if dterr != 0 {
                            return dterr;
                        }
                    }
                    /* embedded decimal and several digits before? */
                    else if !cp.is_null() && flen - strlen(cp) as c_int > 2 {
                        dterr = DecodeNumberField(
                            flen,
                            *field.offset(i as isize),
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                    }
                    /*
                     * Is this a YMD or HMS specification, or a year number?
                     */
                    else if flen >= 6
                        && ((fmask & DTK_DATE_M()) == 0 || (fmask & DTK_TIME_M()) == 0)
                    {
                        dterr = DecodeNumberField(
                            flen,
                            *field.offset(i as isize),
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                    }
                    /* otherwise it is a single date/time field... */
                    else {
                        dterr = DecodeNumber(
                            flen,
                            *field.offset(i as isize),
                            haveTextMonth,
                            fmask,
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr != 0 {
                            return dterr;
                        }
                    }
                }
            }

            x if x == DTK_STRING || x == DTK_SPECIAL => {
                /* timezone abbrevs take precedence over built-in tokens */
                let mut type_out: c_int = 0;
                dterr = DecodeTimezoneAbbrev(
                    i,
                    *field.offset(i as isize),
                    &mut type_out,
                    &mut val,
                    &mut valtz,
                    extra,
                );
                if dterr != 0 {
                    return dterr;
                }
                r#type = type_out;
                if r#type == UNKNOWN_FIELD {
                    r#type = DecodeSpecial(i, *field.offset(i as isize), &mut val);
                }
                if r#type == IGNORE_DTF {
                    i += 1;
                    continue;
                }

                tmask = DTK_M(r#type);
                if r#type == RESERV {
                    match val {
                        x if x == DTK_NOW => {
                            tmask = DTK_DATE_M() | DTK_TIME_M() | DTK_M(TZ);
                            *dtype = DTK_DATE;
                            GetCurrentTimeUsec(tm, fsec, tzp);
                        }
                        x if x == DTK_YESTERDAY => {
                            tmask = DTK_DATE_M();
                            *dtype = DTK_DATE;
                            GetCurrentDateTime(&mut cur_tm);
                            j2date(
                                date2j(cur_tm.tm_year, cur_tm.tm_mon, cur_tm.tm_mday) - 1,
                                &raw mut (*tm).tm_year,
                                &raw mut (*tm).tm_mon,
                                &raw mut (*tm).tm_mday,
                            );
                        }
                        x if x == DTK_TODAY => {
                            tmask = DTK_DATE_M();
                            *dtype = DTK_DATE;
                            GetCurrentDateTime(&mut cur_tm);
                            (*tm).tm_year = cur_tm.tm_year;
                            (*tm).tm_mon = cur_tm.tm_mon;
                            (*tm).tm_mday = cur_tm.tm_mday;
                        }
                        x if x == DTK_TOMORROW => {
                            tmask = DTK_DATE_M();
                            *dtype = DTK_DATE;
                            GetCurrentDateTime(&mut cur_tm);
                            j2date(
                                date2j(cur_tm.tm_year, cur_tm.tm_mon, cur_tm.tm_mday) + 1,
                                &raw mut (*tm).tm_year,
                                &raw mut (*tm).tm_mon,
                                &raw mut (*tm).tm_mday,
                            );
                        }
                        x if x == DTK_ZULU => {
                            tmask = DTK_TIME_M() | DTK_M(TZ);
                            *dtype = DTK_DATE;
                            (*tm).tm_hour = 0;
                            (*tm).tm_min = 0;
                            (*tm).tm_sec = 0;
                            if !tzp.is_null() {
                                *tzp = 0;
                            }
                        }
                        x if x == DTK_EPOCH || x == DTK_LATE || x == DTK_EARLY => {
                            tmask = DTK_DATE_M() | DTK_TIME_M() | DTK_M(TZ);
                            *dtype = val;
                            /* caller ignores tm for these dtype codes */
                        }
                        _ => {
                            elog!(ERROR, "unrecognized RESERV datetime token: {}", val);
                        }
                    }
                } else if r#type == MONTH {
                    /*
                     * already have a (numeric) month? then see if we can
                     * substitute...
                     */
                    if (fmask & DTK_M(MONTH)) != 0
                        && !haveTextMonth
                        && (fmask & DTK_M(DAY)) == 0
                        && (*tm).tm_mon >= 1
                        && (*tm).tm_mon <= 31
                    {
                        (*tm).tm_mday = (*tm).tm_mon;
                        tmask = DTK_M(DAY);
                    }
                    haveTextMonth = true;
                    (*tm).tm_mon = val;
                } else if r#type == DTZMOD {
                    /*
                     * daylight savings time modifier (solves "MET DST" syntax)
                     */
                    tmask |= DTK_M(DTZ);
                    (*tm).tm_isdst = 1;
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    *tzp -= val;
                } else if r#type == DTZ {
                    /*
                     * set mask for TZ here _or_ check for DTZ later when getting
                     * default timezone
                     */
                    tmask |= DTK_M(TZ);
                    (*tm).tm_isdst = 1;
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    *tzp = -val;
                } else if r#type == TZ {
                    (*tm).tm_isdst = 0;
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    *tzp = -val;
                } else if r#type == DYNTZ {
                    tmask |= DTK_M(TZ);
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    /* we'll determine the actual offset later */
                    abbrevTz = valtz;
                    abbrev = *field.offset(i as isize);
                } else if r#type == AMPM {
                    mer = val;
                } else if r#type == ADBC {
                    bc = val == BC;
                } else if r#type == DOW {
                    (*tm).tm_wday = val;
                } else if r#type == UNITS {
                    tmask = 0;
                    /* reject consecutive unhandled units */
                    if ptype != 0 {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = val;
                } else if r#type == ISOTIME {
                    /*
                     * This is a filler field "t" indicating that the next field
                     * is time.
                     */
                    tmask = 0;

                    /* No preceding date? Then quit... */
                    if (fmask & DTK_DATE_M()) != DTK_DATE_M() {
                        return DTERR_BAD_FORMAT;
                    }

                    /* reject consecutive unhandled units */
                    if ptype != 0 {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = val;
                } else if r#type == UNKNOWN_FIELD {
                    /*
                     * Before giving up and declaring error, check to see if it
                     * is an all-alpha timezone name.
                     */
                    namedTz = pg_tzset(*field.offset(i as isize));
                    if namedTz.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    /* we'll apply the zone setting below */
                    tmask = DTK_M(TZ);
                } else {
                    return DTERR_BAD_FORMAT;
                }
            }

            _ => {
                return DTERR_BAD_FORMAT;
            }
        }

        if (tmask & fmask) != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= tmask;

        i += 1;
    } /* end loop over fields */

    /* reject if prefix type appeared and was never handled */
    if ptype != 0 {
        return DTERR_BAD_FORMAT;
    }

    /* do additional checking for normal date specs (but not "infinity" etc) */
    if *dtype == DTK_DATE {
        /* do final checking/adjustment of Y/M/D fields */
        dterr = ValidateDate(fmask, isjulian, is2digits, bc, tm);
        if dterr != 0 {
            return dterr;
        }

        /* handle AM/PM */
        if mer != HR24 && (*tm).tm_hour > HOURS_PER_DAY / 2 {
            return DTERR_FIELD_OVERFLOW;
        }
        if mer == AM && (*tm).tm_hour == HOURS_PER_DAY / 2 {
            (*tm).tm_hour = 0;
        } else if mer == PM && (*tm).tm_hour != HOURS_PER_DAY / 2 {
            (*tm).tm_hour += HOURS_PER_DAY / 2;
        }

        /* check for incomplete input */
        if (fmask & DTK_DATE_M()) != DTK_DATE_M() {
            if (fmask & DTK_TIME_M()) == DTK_TIME_M() {
                return 1;
            }
            return DTERR_BAD_FORMAT;
        }

        /*
         * If we had a full timezone spec, compute the offset.
         */
        if !namedTz.is_null() {
            /* daylight savings time modifier disallowed with full TZ */
            if (fmask & DTK_M(DTZMOD)) != 0 {
                return DTERR_BAD_FORMAT;
            }

            *tzp = DetermineTimeZoneOffset(tm, namedTz);
        }

        /*
         * Likewise, if we had a dynamic timezone abbreviation, resolve it now.
         */
        if !abbrevTz.is_null() {
            /* daylight savings time modifier disallowed with dynamic TZ */
            if (fmask & DTK_M(DTZMOD)) != 0 {
                return DTERR_BAD_FORMAT;
            }

            *tzp = DetermineTimeZoneAbbrevOffset(tm, abbrev, abbrevTz);
        }

        /* timezone not specified? then use session timezone */
        if !tzp.is_null() && (fmask & DTK_M(TZ)) == 0 {
            /*
             * daylight savings time modifier but no standard timezone? then
             * error
             */
            if (fmask & DTK_M(DTZMOD)) != 0 {
                return DTERR_BAD_FORMAT;
            }

            *tzp = DetermineTimeZoneOffset(tm, session_timezone);
        }
    }

    0
}

/* DetermineTimeZoneOffset()
 *
 * Given a struct pg_tm and a zic-style time zone definition, determine the
 * applicable GMT offset and daylight-savings status at that time.
 */
pub unsafe fn DetermineTimeZoneOffset(tm: *mut pg_tm, tzp: *mut pg_tz) -> c_int {
    let mut t: pg_time_t = 0;

    DetermineTimeZoneOffsetInternal(tm, tzp, &mut t)
}

/* DetermineTimeZoneOffsetInternal()
 *
 * As above, but also return the actual UTC time imputed to the date/time
 * into *tp.
 */
unsafe fn DetermineTimeZoneOffsetInternal(
    tm: *mut pg_tm,
    tzp: *mut pg_tz,
    tp: *mut pg_time_t,
) -> c_int {
    let date: c_int;
    let sec: c_int;
    let day: pg_time_t;
    let mytime: pg_time_t;
    let prevtime: pg_time_t;
    let mut boundary: pg_time_t = 0;
    let beforetime: pg_time_t;
    let aftertime: pg_time_t;
    let mut before_gmtoff: c_long = 0;
    let mut after_gmtoff: c_long = 0;
    let mut before_isdst: c_int = 0;
    let mut after_isdst: c_int = 0;
    let res: c_int;

    /*
     * First, generate the pg_time_t value corresponding to the given
     * y/m/d/h/m/s taken as GMT time.
     */
    if !IS_VALID_JULIAN((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) {
        return determine_tz_overflow(tm, tp);
    }
    date = date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday) - UNIX_EPOCH_JDATE;

    day = (date as pg_time_t) * SECS_PER_DAY as pg_time_t;
    if day / SECS_PER_DAY as pg_time_t != date as pg_time_t {
        return determine_tz_overflow(tm, tp);
    }
    sec = (*tm).tm_sec
        + ((*tm).tm_min + (*tm).tm_hour * MINS_PER_HOUR) * SECS_PER_MINUTE;
    mytime = day + sec as pg_time_t;
    /* since sec >= 0, overflow could only be from +day to -mytime */
    if mytime < 0 && day > 0 {
        return determine_tz_overflow(tm, tp);
    }

    /*
     * Find the DST time boundary just before or following the target time.
     */
    prevtime = mytime - SECS_PER_DAY as pg_time_t;
    if mytime < 0 && prevtime > 0 {
        return determine_tz_overflow(tm, tp);
    }

    let mut prevtime_mut = prevtime;
    res = pg_next_dst_boundary(
        &mut prevtime_mut,
        &mut before_gmtoff,
        &mut before_isdst,
        &mut boundary,
        &mut after_gmtoff,
        &mut after_isdst,
        tzp,
    );
    if res < 0 {
        return determine_tz_overflow(tm, tp); /* failure? */
    }

    if res == 0 {
        /* Non-DST zone, life is simple */
        (*tm).tm_isdst = before_isdst;
        *tp = mytime - before_gmtoff as pg_time_t;
        return -(before_gmtoff as c_int);
    }

    /*
     * Form the candidate pg_time_t values with local-time adjustment
     */
    beforetime = mytime - before_gmtoff as pg_time_t;
    if (before_gmtoff > 0 && mytime < 0 && beforetime > 0)
        || (before_gmtoff <= 0 && mytime > 0 && beforetime < 0)
    {
        return determine_tz_overflow(tm, tp);
    }
    aftertime = mytime - after_gmtoff as pg_time_t;
    if (after_gmtoff > 0 && mytime < 0 && aftertime > 0)
        || (after_gmtoff <= 0 && mytime > 0 && aftertime < 0)
    {
        return determine_tz_overflow(tm, tp);
    }

    /*
     * If both before or both after the boundary time, we know what to do.
     */
    if beforetime < boundary && aftertime < boundary {
        (*tm).tm_isdst = before_isdst;
        *tp = beforetime;
        return -(before_gmtoff as c_int);
    }
    if beforetime > boundary && aftertime >= boundary {
        (*tm).tm_isdst = after_isdst;
        *tp = aftertime;
        return -(after_gmtoff as c_int);
    }

    /*
     * It's an invalid or ambiguous time due to timezone transition.
     */
    if beforetime > aftertime {
        (*tm).tm_isdst = before_isdst;
        *tp = beforetime;
        return -(before_gmtoff as c_int);
    }
    (*tm).tm_isdst = after_isdst;
    *tp = aftertime;
    -(after_gmtoff as c_int)
}

/* the C `overflow:` label folded into a helper */
unsafe fn determine_tz_overflow(tm: *mut pg_tm, tp: *mut pg_time_t) -> c_int {
    /* Given date is out of range, so assume UTC */
    (*tm).tm_isdst = 0;
    *tp = 0;
    0
}

/* DetermineTimeZoneAbbrevOffset()
 *
 * Determine the GMT offset and DST flag to be attributed to a dynamic
 * time zone abbreviation.
 */
pub unsafe fn DetermineTimeZoneAbbrevOffset(
    tm: *mut pg_tm,
    abbr: *const c_char,
    tzp: *mut pg_tz,
) -> c_int {
    let mut t: pg_time_t = 0;
    let zone_offset: c_int;
    let mut abbr_offset: c_int = 0;
    let mut abbr_isdst: c_int = 0;

    /*
     * Compute the UTC time we want to probe at.
     */
    zone_offset = DetermineTimeZoneOffsetInternal(tm, tzp, &mut t);

    /*
     * Try to match the abbreviation to something in the zone definition.
     */
    if DetermineTimeZoneAbbrevOffsetInternal(t, abbr, tzp, &mut abbr_offset, &mut abbr_isdst) {
        /* Success, so use the abbrev-specific answers. */
        (*tm).tm_isdst = abbr_isdst;
        return abbr_offset;
    }

    /*
     * No match, so use the answers we already got.
     */
    zone_offset
}

/* DetermineTimeZoneAbbrevOffsetTS()
 */
pub unsafe fn DetermineTimeZoneAbbrevOffsetTS(
    ts: TimestampTz,
    abbr: *const c_char,
    tzp: *mut pg_tz,
    isdst: *mut c_int,
) -> c_int {
    let t: pg_time_t = timestamptz_to_time_t(ts);
    let zone_offset: c_int;
    let mut abbr_offset: c_int = 0;
    let mut tz: c_int = 0;
    let mut tm: pg_tm = std::mem::zeroed();
    let mut fsec: fsec_t = 0;

    /*
     * If the abbrev matches anything in the zone data, this is pretty easy.
     */
    if DetermineTimeZoneAbbrevOffsetInternal(t, abbr, tzp, &mut abbr_offset, isdst) {
        return abbr_offset;
    }

    /*
     * Else, break down the timestamp so we can use DetermineTimeZoneOffset.
     */
    if timestamp2tm(ts, &mut tz, &mut tm, &mut fsec, std::ptr::null_mut(), tzp) != 0 {
        ereport!(ERROR, errmsg!("timestamp out of range"));
    }

    zone_offset = DetermineTimeZoneOffset(&mut tm, tzp);
    *isdst = tm.tm_isdst;
    zone_offset
}

/* DetermineTimeZoneAbbrevOffsetInternal()
 */
unsafe fn DetermineTimeZoneAbbrevOffsetInternal(
    mut t: pg_time_t,
    abbr: *const c_char,
    tzp: *mut pg_tz,
    offset: *mut c_int,
    isdst: *mut c_int,
) -> bool {
    let mut upabbr: [c_char; TZ_STRLEN_MAX + 1] = [0; TZ_STRLEN_MAX + 1];
    let mut p: *mut u8;
    let mut gmtoff: c_long = 0;

    /* We need to force the abbrev to upper case */
    strlcpy(upabbr.as_mut_ptr(), abbr, std::mem::size_of_val(&upabbr));
    p = upabbr.as_mut_ptr() as *mut u8;
    while *p != 0 {
        *p = pg_toupper(*p);
        p = p.offset(1);
    }

    /* Look up the abbrev's meaning at this time in this zone */
    if pg_interpret_timezone_abbrev(
        upabbr.as_ptr(),
        &mut t,
        &mut gmtoff,
        isdst,
        tzp,
    ) {
        /* Change sign to agree with DetermineTimeZoneOffset() */
        *offset = -(gmtoff as c_int);
        return true;
    }
    false
}

/* TimeZoneAbbrevIsKnown()
 */
unsafe fn TimeZoneAbbrevIsKnown(
    abbr: *const c_char,
    tzp: *mut pg_tz,
    isfixed: *mut bool,
    offset: *mut c_int,
    isdst: *mut c_int,
) -> bool {
    let mut upabbr: [c_char; TZ_STRLEN_MAX + 1] = [0; TZ_STRLEN_MAX + 1];
    let mut p: *mut u8;
    let mut gmtoff: c_long = 0;

    /* We need to force the abbrev to upper case */
    strlcpy(upabbr.as_mut_ptr(), abbr, std::mem::size_of_val(&upabbr));
    p = upabbr.as_mut_ptr() as *mut u8;
    while *p != 0 {
        *p = pg_toupper(*p);
        p = p.offset(1);
    }

    /* Look up the abbrev's meaning in this zone */
    if pg_timezone_abbrev_is_known(upabbr.as_ptr(), isfixed, &mut gmtoff, isdst, tzp) {
        /* Change sign to agree with DetermineTimeZoneOffset() */
        *offset = -(gmtoff as c_int);
        return true;
    }
    false
}

/* DecodeTimeOnly()
 * Interpret parsed string as time fields only.
 * Returns 0 if successful, DTERR code if bogus input detected.
 */
pub unsafe fn DecodeTimeOnly(
    field: *mut *mut c_char,
    ftype: *mut c_int,
    nf: c_int,
    dtype: *mut c_int,
    tm: *mut pg_tm,
    fsec: *mut fsec_t,
    tzp: *mut c_int,
    extra: *mut DateTimeErrorExtra,
) -> c_int {
    let mut fmask: c_int = 0;
    let mut tmask: c_int = 0;
    let mut r#type: c_int;
    let mut ptype: c_int = 0; /* "prefix type" for ISO and Julian formats */
    let mut i: c_int;
    let mut val: c_int = 0;
    let mut dterr: c_int;
    let mut isjulian = false;
    let mut is2digits = false;
    let mut bc = false;
    let mut mer: c_int = HR24;
    let mut namedTz: *mut pg_tz = std::ptr::null_mut();
    let mut abbrevTz: *mut pg_tz = std::ptr::null_mut();
    let mut abbrev: *mut c_char = std::ptr::null_mut();
    let mut valtz: *mut pg_tz = std::ptr::null_mut();

    *dtype = DTK_TIME;
    (*tm).tm_hour = 0;
    (*tm).tm_min = 0;
    (*tm).tm_sec = 0;
    *fsec = 0;
    /* don't know daylight savings time status apriori */
    (*tm).tm_isdst = -1;

    if !tzp.is_null() {
        *tzp = 0;
    }

    i = 0;
    while i < nf {
        match *ftype.offset(i as isize) {
            x if x == DTK_DATE => {
                /*
                 * Time zone not allowed? Then should not accept dates or time
                 * zones no matter what else!
                 */
                if tzp.is_null() {
                    return DTERR_BAD_FORMAT;
                }

                /* Under limited circumstances, we will accept a date... */
                if i == 0
                    && nf >= 2
                    && (*ftype.offset((nf - 1) as isize) == DTK_DATE
                        || *ftype.offset(1) == DTK_TIME)
                {
                    dterr = DecodeDate(
                        *field.offset(i as isize),
                        fmask,
                        &mut tmask,
                        &mut is2digits,
                        tm,
                    );
                    if dterr != 0 {
                        return dterr;
                    }
                }
                /* otherwise, this is a time and/or time zone */
                else {
                    if isdigit(**field.offset(i as isize) as u8 as c_int) != 0 {
                        let cp: *mut c_char;

                        /*
                         * Starts with a digit but we already have a time field?
                         */
                        if (fmask & DTK_TIME_M()) == DTK_TIME_M() {
                            return DTERR_BAD_FORMAT;
                        }

                        cp = strchr(*field.offset(i as isize), b'-' as c_int);
                        if cp.is_null() {
                            return DTERR_BAD_FORMAT;
                        }

                        /* Get the time zone from the end of the string */
                        dterr = DecodeTimezone(cp, tzp);
                        if dterr != 0 {
                            return dterr;
                        }
                        *cp = 0;

                        /*
                         * Then read the rest of the field as a concatenated time
                         */
                        dterr = DecodeNumberField(
                            strlen(*field.offset(i as isize)) as c_int,
                            *field.offset(i as isize),
                            fmask | DTK_DATE_M(),
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                        *ftype.offset(i as isize) = dterr;

                        tmask |= DTK_M(TZ);
                    } else {
                        namedTz = pg_tzset(*field.offset(i as isize));
                        if namedTz.is_null() {
                            (*extra).dtee_timezone = *field.offset(i as isize);
                            return DTERR_BAD_TIMEZONE;
                        }
                        /* we'll apply the zone setting below */
                        *ftype.offset(i as isize) = DTK_TZ;
                        tmask = DTK_M(TZ);
                    }
                }
            }

            x if x == DTK_TIME => {
                /*
                 * This might be an ISO time following a "t" field.
                 */
                if ptype != 0 {
                    if ptype != DTK_TIME {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = 0;
                }

                dterr = DecodeTime(
                    *field.offset(i as isize),
                    fmask | DTK_DATE_M(),
                    INTERVAL_FULL_RANGE,
                    &mut tmask,
                    tm,
                    fsec,
                );
                if dterr != 0 {
                    return dterr;
                }
            }

            x if x == DTK_TZ => {
                let mut tz: c_int = 0;

                if tzp.is_null() {
                    return DTERR_BAD_FORMAT;
                }

                dterr = DecodeTimezone(*field.offset(i as isize), &mut tz);
                if dterr != 0 {
                    return dterr;
                }
                *tzp = tz;
                tmask = DTK_M(TZ);
            }

            x if x == DTK_NUMBER => {
                /*
                 * Deal with cases where previous field labeled this one
                 */
                if ptype != 0 {
                    let mut cp: *mut c_char = std::ptr::null_mut();
                    let value: c_int;

                    set_errno(0);
                    value = strtoint(*field.offset(i as isize), &mut cp, 10);
                    if get_errno() == ERANGE {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if *cp != b'.' as c_char && *cp != 0 {
                        return DTERR_BAD_FORMAT;
                    }

                    if ptype == DTK_JULIAN {
                        /* previous field was a label for "julian date" */
                        if tzp.is_null() {
                            return DTERR_BAD_FORMAT;
                        }
                        if value < 0 {
                            return DTERR_FIELD_OVERFLOW;
                        }
                        tmask = DTK_DATE_M();
                        j2date(
                            value,
                            &raw mut (*tm).tm_year,
                            &raw mut (*tm).tm_mon,
                            &raw mut (*tm).tm_mday,
                        );
                        isjulian = true;

                        if *cp == b'.' as c_char {
                            let mut time: f64 = 0.0;

                            dterr = ParseFraction(cp, &mut time);
                            if dterr != 0 {
                                return dterr;
                            }
                            time *= USECS_PER_DAY as f64;
                            dt2time(
                                time as int64,
                                &raw mut (*tm).tm_hour,
                                &raw mut (*tm).tm_min,
                                &raw mut (*tm).tm_sec,
                                fsec,
                            );
                            tmask |= DTK_TIME_M();
                        }
                    } else if ptype == DTK_TIME {
                        /* previous field was "t" for ISO time */
                        dterr = DecodeNumberField(
                            strlen(*field.offset(i as isize)) as c_int,
                            *field.offset(i as isize),
                            fmask | DTK_DATE_M(),
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                        *ftype.offset(i as isize) = dterr;

                        if tmask != DTK_TIME_M() {
                            return DTERR_BAD_FORMAT;
                        }
                    } else {
                        return DTERR_BAD_FORMAT;
                    }

                    ptype = 0;
                    *dtype = DTK_DATE;
                } else {
                    let cp: *mut c_char;
                    let flen: c_int;

                    flen = strlen(*field.offset(i as isize)) as c_int;
                    cp = strchr(*field.offset(i as isize), b'.' as c_int);

                    /* Embedded decimal? */
                    if !cp.is_null() {
                        /* Under limited circumstances, we will accept a date... */
                        if i == 0 && nf >= 2 && *ftype.offset((nf - 1) as isize) == DTK_DATE {
                            dterr = DecodeDate(
                                *field.offset(i as isize),
                                fmask,
                                &mut tmask,
                                &mut is2digits,
                                tm,
                            );
                            if dterr != 0 {
                                return dterr;
                            }
                        }
                        /* embedded decimal and several digits before? */
                        else if flen - strlen(cp) as c_int > 2 {
                            dterr = DecodeNumberField(
                                flen,
                                *field.offset(i as isize),
                                fmask | DTK_DATE_M(),
                                &mut tmask,
                                tm,
                                fsec,
                                &mut is2digits,
                            );
                            if dterr < 0 {
                                return dterr;
                            }
                            *ftype.offset(i as isize) = dterr;
                        } else {
                            return DTERR_BAD_FORMAT;
                        }
                    } else if flen > 4 {
                        dterr = DecodeNumberField(
                            flen,
                            *field.offset(i as isize),
                            fmask | DTK_DATE_M(),
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr < 0 {
                            return dterr;
                        }
                        *ftype.offset(i as isize) = dterr;
                    }
                    /* otherwise it is a single date/time field... */
                    else {
                        dterr = DecodeNumber(
                            flen,
                            *field.offset(i as isize),
                            false,
                            fmask | DTK_DATE_M(),
                            &mut tmask,
                            tm,
                            fsec,
                            &mut is2digits,
                        );
                        if dterr != 0 {
                            return dterr;
                        }
                    }
                }
            }

            x if x == DTK_STRING || x == DTK_SPECIAL => {
                /* timezone abbrevs take precedence over built-in tokens */
                let mut type_out: c_int = 0;
                dterr = DecodeTimezoneAbbrev(
                    i,
                    *field.offset(i as isize),
                    &mut type_out,
                    &mut val,
                    &mut valtz,
                    extra,
                );
                if dterr != 0 {
                    return dterr;
                }
                r#type = type_out;
                if r#type == UNKNOWN_FIELD {
                    r#type = DecodeSpecial(i, *field.offset(i as isize), &mut val);
                }
                if r#type == IGNORE_DTF {
                    i += 1;
                    continue;
                }

                tmask = DTK_M(r#type);
                if r#type == RESERV {
                    if val == DTK_NOW {
                        tmask = DTK_TIME_M();
                        *dtype = DTK_TIME;
                        GetCurrentTimeUsec(tm, fsec, std::ptr::null_mut());
                    } else if val == DTK_ZULU {
                        tmask = DTK_TIME_M() | DTK_M(TZ);
                        *dtype = DTK_TIME;
                        (*tm).tm_hour = 0;
                        (*tm).tm_min = 0;
                        (*tm).tm_sec = 0;
                        (*tm).tm_isdst = 0;
                    } else {
                        return DTERR_BAD_FORMAT;
                    }
                } else if r#type == DTZMOD {
                    /*
                     * daylight savings time modifier (solves "MET DST" syntax)
                     */
                    tmask |= DTK_M(DTZ);
                    (*tm).tm_isdst = 1;
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    *tzp -= val;
                } else if r#type == DTZ {
                    /*
                     * set mask for TZ here _or_ check for DTZ later when getting
                     * default timezone
                     */
                    tmask |= DTK_M(TZ);
                    (*tm).tm_isdst = 1;
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    *tzp = -val;
                    *ftype.offset(i as isize) = DTK_TZ;
                } else if r#type == TZ {
                    (*tm).tm_isdst = 0;
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    *tzp = -val;
                    *ftype.offset(i as isize) = DTK_TZ;
                } else if r#type == DYNTZ {
                    tmask |= DTK_M(TZ);
                    if tzp.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    /* we'll determine the actual offset later */
                    abbrevTz = valtz;
                    abbrev = *field.offset(i as isize);
                    *ftype.offset(i as isize) = DTK_TZ;
                } else if r#type == AMPM {
                    mer = val;
                } else if r#type == ADBC {
                    bc = val == BC;
                } else if r#type == UNITS {
                    tmask = 0;
                    /* reject consecutive unhandled units */
                    if ptype != 0 {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = val;
                } else if r#type == ISOTIME {
                    tmask = 0;
                    /* reject consecutive unhandled units */
                    if ptype != 0 {
                        return DTERR_BAD_FORMAT;
                    }
                    ptype = val;
                } else if r#type == UNKNOWN_FIELD {
                    /*
                     * Before giving up and declaring error, check to see if it
                     * is an all-alpha timezone name.
                     */
                    namedTz = pg_tzset(*field.offset(i as isize));
                    if namedTz.is_null() {
                        return DTERR_BAD_FORMAT;
                    }
                    /* we'll apply the zone setting below */
                    tmask = DTK_M(TZ);
                } else {
                    return DTERR_BAD_FORMAT;
                }
            }

            _ => {
                return DTERR_BAD_FORMAT;
            }
        }

        if (tmask & fmask) != 0 {
            return DTERR_BAD_FORMAT;
        }
        fmask |= tmask;

        i += 1;
    } /* end loop over fields */

    /* reject if prefix type appeared and was never handled */
    if ptype != 0 {
        return DTERR_BAD_FORMAT;
    }

    /* do final checking/adjustment of Y/M/D fields */
    dterr = ValidateDate(fmask, isjulian, is2digits, bc, tm);
    if dterr != 0 {
        return dterr;
    }

    /* handle AM/PM */
    if mer != HR24 && (*tm).tm_hour > HOURS_PER_DAY / 2 {
        return DTERR_FIELD_OVERFLOW;
    }
    if mer == AM && (*tm).tm_hour == HOURS_PER_DAY / 2 {
        (*tm).tm_hour = 0;
    } else if mer == PM && (*tm).tm_hour != HOURS_PER_DAY / 2 {
        (*tm).tm_hour += HOURS_PER_DAY / 2;
    }

    /* check for time overflow */
    if time_overflows((*tm).tm_hour, (*tm).tm_min, (*tm).tm_sec, *fsec) {
        return DTERR_FIELD_OVERFLOW;
    }

    if (fmask & DTK_TIME_M()) != DTK_TIME_M() {
        return DTERR_BAD_FORMAT;
    }

    /*
     * If we had a full timezone spec, compute the offset.
     */
    if !namedTz.is_null() {
        let mut gmtoff: c_long = 0;

        /* daylight savings time modifier disallowed with full TZ */
        if (fmask & DTK_M(DTZMOD)) != 0 {
            return DTERR_BAD_FORMAT;
        }

        /* if non-DST zone, we do not need to know the date */
        if pg_get_timezone_offset(namedTz, &mut gmtoff) {
            *tzp = -(gmtoff as c_int);
        } else {
            /* a date has to be specified */
            if (fmask & DTK_DATE_M()) != DTK_DATE_M() {
                return DTERR_BAD_FORMAT;
            }
            *tzp = DetermineTimeZoneOffset(tm, namedTz);
        }
    }

    /*
     * Likewise, if we had a dynamic timezone abbreviation, resolve it now.
     */
    if !abbrevTz.is_null() {
        let mut tt: pg_tm = std::mem::zeroed();
        let tmp: *mut pg_tm = &mut tt;

        /*
         * daylight savings time modifier but no standard timezone? then error
         */
        if (fmask & DTK_M(DTZMOD)) != 0 {
            return DTERR_BAD_FORMAT;
        }

        if (fmask & DTK_DATE_M()) == 0 {
            GetCurrentDateTime(tmp);
        } else {
            /* a date has to be specified */
            if (fmask & DTK_DATE_M()) != DTK_DATE_M() {
                return DTERR_BAD_FORMAT;
            }
            (*tmp).tm_year = (*tm).tm_year;
            (*tmp).tm_mon = (*tm).tm_mon;
            (*tmp).tm_mday = (*tm).tm_mday;
        }
        (*tmp).tm_hour = (*tm).tm_hour;
        (*tmp).tm_min = (*tm).tm_min;
        (*tmp).tm_sec = (*tm).tm_sec;
        *tzp = DetermineTimeZoneAbbrevOffset(tmp, abbrev, abbrevTz);
        (*tm).tm_isdst = (*tmp).tm_isdst;
    }

    /* timezone not specified? then use session timezone */
    if !tzp.is_null() && (fmask & DTK_M(TZ)) == 0 {
        let mut tt: pg_tm = std::mem::zeroed();
        let tmp: *mut pg_tm = &mut tt;

        /*
         * daylight savings time modifier but no standard timezone? then error
         */
        if (fmask & DTK_M(DTZMOD)) != 0 {
            return DTERR_BAD_FORMAT;
        }

        if (fmask & DTK_DATE_M()) == 0 {
            GetCurrentDateTime(tmp);
        } else {
            /* a date has to be specified */
            if (fmask & DTK_DATE_M()) != DTK_DATE_M() {
                return DTERR_BAD_FORMAT;
            }
            (*tmp).tm_year = (*tm).tm_year;
            (*tmp).tm_mon = (*tm).tm_mon;
            (*tmp).tm_mday = (*tm).tm_mday;
        }
        (*tmp).tm_hour = (*tm).tm_hour;
        (*tmp).tm_min = (*tm).tm_min;
        (*tmp).tm_sec = (*tm).tm_sec;
        *tzp = DetermineTimeZoneOffset(tmp, session_timezone);
        (*tm).tm_isdst = (*tmp).tm_isdst;
    }

    0
}

/* DecodeDate()
 * Decode date string which includes delimiters.
 * Return 0 if okay, a DTERR code if not.
 */
unsafe fn DecodeDate(
    mut str: *mut c_char,
    mut fmask: c_int,
    tmask: *mut c_int,
    is2digits: *mut bool,
    tm: *mut pg_tm,
) -> c_int {
    let mut fsec: fsec_t = 0;
    let mut nf: c_int = 0;
    let mut i: c_int;
    let mut len: c_int;
    let mut dterr: c_int;
    let mut haveTextMonth = false;
    let mut r#type: c_int;
    let mut val: c_int = 0;
    let mut dmask: c_int = 0;
    let mut field: [*mut c_char; MAXDATEFIELDS] = [std::ptr::null_mut(); MAXDATEFIELDS];

    *tmask = 0;

    /* parse this string... */
    while *str != 0 && nf < MAXDATEFIELDS as c_int {
        /* skip field separators */
        while *str != 0 && isalnum(*str as u8 as c_int) == 0 {
            str = str.offset(1);
        }

        if *str == 0 {
            return DTERR_BAD_FORMAT; /* end of string after separator */
        }

        field[nf as usize] = str;
        if isdigit(*str as u8 as c_int) != 0 {
            while isdigit(*str as u8 as c_int) != 0 {
                str = str.offset(1);
            }
        } else if isalpha(*str as u8 as c_int) != 0 {
            while isalpha(*str as u8 as c_int) != 0 {
                str = str.offset(1);
            }
        }

        /* Just get rid of any non-digit, non-alpha characters... */
        if *str != 0 {
            *str = 0;
            str = str.offset(1);
        }
        nf += 1;
    }

    /* look first for text fields, since that will be unambiguous month */
    i = 0;
    while i < nf {
        if isalpha(*field[i as usize] as u8 as c_int) != 0 {
            r#type = DecodeSpecial(i, field[i as usize], &mut val);
            if r#type == IGNORE_DTF {
                i += 1;
                continue;
            }

            dmask = DTK_M(r#type);
            if r#type == MONTH {
                (*tm).tm_mon = val;
                haveTextMonth = true;
            } else {
                return DTERR_BAD_FORMAT;
            }
            if (fmask & dmask) != 0 {
                return DTERR_BAD_FORMAT;
            }

            fmask |= dmask;
            *tmask |= dmask;

            /* mark this field as being completed */
            field[i as usize] = std::ptr::null_mut();
        }
        i += 1;
    }

    /* now pick up remaining numeric fields */
    i = 0;
    while i < nf {
        if field[i as usize].is_null() {
            i += 1;
            continue;
        }

        len = strlen(field[i as usize]) as c_int;
        if len <= 0 {
            return DTERR_BAD_FORMAT;
        }

        dterr = DecodeNumber(
            len,
            field[i as usize],
            haveTextMonth,
            fmask,
            &mut dmask,
            tm,
            &mut fsec,
            is2digits,
        );
        if dterr != 0 {
            return dterr;
        }

        if (fmask & dmask) != 0 {
            return DTERR_BAD_FORMAT;
        }

        fmask |= dmask;
        *tmask |= dmask;
        i += 1;
    }

    if (fmask & !(DTK_M(DOY) | DTK_M(TZ))) != DTK_DATE_M() {
        return DTERR_BAD_FORMAT;
    }

    /* validation of the field values must wait until ValidateDate() */

    0
}

/* ValidateDate()
 * Check valid year/month/day values, handle BC and DOY cases
 * Return 0 if okay, a DTERR code if not.
 */
pub unsafe fn ValidateDate(
    fmask: c_int,
    isjulian: bool,
    is2digits: bool,
    bc: bool,
    tm: *mut pg_tm,
) -> c_int {
    if (fmask & DTK_M(YEAR)) != 0 {
        if isjulian {
            /* tm_year is correct and should not be touched */
        } else if bc {
            /* there is no year zero in AD/BC notation */
            if (*tm).tm_year <= 0 {
                return DTERR_FIELD_OVERFLOW;
            }
            /* internally, we represent 1 BC as year zero, 2 BC as -1, etc */
            (*tm).tm_year = -((*tm).tm_year - 1);
        } else if is2digits {
            /* process 1 or 2-digit input as 1970-2069 AD, allow '0' and '00' */
            if (*tm).tm_year < 0 {
                /* just paranoia */
                return DTERR_FIELD_OVERFLOW;
            }
            if (*tm).tm_year < 70 {
                (*tm).tm_year += 2000;
            } else if (*tm).tm_year < 100 {
                (*tm).tm_year += 1900;
            }
        } else {
            /* there is no year zero in AD/BC notation */
            if (*tm).tm_year <= 0 {
                return DTERR_FIELD_OVERFLOW;
            }
        }
    }

    /* now that we have correct year, decode DOY */
    if (fmask & DTK_M(DOY)) != 0 {
        j2date(
            date2j((*tm).tm_year, 1, 1) + (*tm).tm_yday - 1,
            &raw mut (*tm).tm_year,
            &raw mut (*tm).tm_mon,
            &raw mut (*tm).tm_mday,
        );
    }

    /* check for valid month */
    if (fmask & DTK_M(MONTH)) != 0 {
        if (*tm).tm_mon < 1 || (*tm).tm_mon > MONTHS_PER_YEAR {
            return DTERR_MD_FIELD_OVERFLOW;
        }
    }

    /* minimal check for valid day */
    if (fmask & DTK_M(DAY)) != 0 {
        if (*tm).tm_mday < 1 || (*tm).tm_mday > 31 {
            return DTERR_MD_FIELD_OVERFLOW;
        }
    }

    if (fmask & DTK_DATE_M()) == DTK_DATE_M() {
        /*
         * Check for valid day of month, now that we know for sure the month
         * and year.
         */
        if (*tm).tm_mday > day_tab[isleap((*tm).tm_year) as usize][((*tm).tm_mon - 1) as usize] {
            return DTERR_FIELD_OVERFLOW;
        }
    }

    0
}

/* DecodeTimeCommon()
 * Decode time string which includes delimiters.
 * Return 0 if okay, a DTERR code if not.
 */
unsafe fn DecodeTimeCommon(
    str: *mut c_char,
    _fmask: c_int,
    range: c_int,
    tmask: *mut c_int,
    itm: *mut pg_itm,
) -> c_int {
    let mut cp: *mut c_char = std::ptr::null_mut();
    let dterr: c_int;
    let mut fsec: fsec_t = 0;

    *tmask = DTK_TIME_M();

    set_errno(0);
    (*itm).tm_hour = strtoi64(str, &mut cp, 10);
    if get_errno() == ERANGE {
        return DTERR_FIELD_OVERFLOW;
    }
    if *cp != b':' as c_char {
        return DTERR_BAD_FORMAT;
    }
    set_errno(0);
    (*itm).tm_min = strtoint(cp.offset(1), &mut cp, 10);
    if get_errno() == ERANGE {
        return DTERR_FIELD_OVERFLOW;
    }
    if *cp == 0 {
        (*itm).tm_sec = 0;
        /* If it's a MINUTE TO SECOND interval, take 2 fields as being mm:ss */
        if range == INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND) {
            if (*itm).tm_hour > INT_MAX_C || (*itm).tm_hour < INT_MIN_C {
                return DTERR_FIELD_OVERFLOW;
            }
            (*itm).tm_sec = (*itm).tm_min;
            (*itm).tm_min = (*itm).tm_hour as c_int;
            (*itm).tm_hour = 0;
        }
    } else if *cp == b'.' as c_char {
        /* always assume mm:ss.sss is MINUTE TO SECOND */
        dterr = ParseFractionalSecond(cp, &mut fsec);
        if dterr != 0 {
            return dterr;
        }
        if (*itm).tm_hour > INT_MAX_C || (*itm).tm_hour < INT_MIN_C {
            return DTERR_FIELD_OVERFLOW;
        }
        (*itm).tm_sec = (*itm).tm_min;
        (*itm).tm_min = (*itm).tm_hour as c_int;
        (*itm).tm_hour = 0;
    } else if *cp == b':' as c_char {
        set_errno(0);
        (*itm).tm_sec = strtoint(cp.offset(1), &mut cp, 10);
        if get_errno() == ERANGE {
            return DTERR_FIELD_OVERFLOW;
        }
        if *cp == b'.' as c_char {
            dterr = ParseFractionalSecond(cp, &mut fsec);
            if dterr != 0 {
                return dterr;
            }
        } else if *cp != 0 {
            return DTERR_BAD_FORMAT;
        }
    } else {
        return DTERR_BAD_FORMAT;
    }

    /* do a sanity check; but caller must check the range of tm_hour */
    if (*itm).tm_hour < 0
        || (*itm).tm_min < 0
        || (*itm).tm_min > MINS_PER_HOUR - 1
        || (*itm).tm_sec < 0
        || (*itm).tm_sec > SECS_PER_MINUTE
        || fsec < 0
        || fsec as int64 > USECS_PER_SEC
    {
        return DTERR_FIELD_OVERFLOW;
    }

    (*itm).tm_usec = fsec;

    0
}

/* DecodeTime()
 * This version is used for timestamps.
 */
unsafe fn DecodeTime(
    str: *mut c_char,
    fmask: c_int,
    range: c_int,
    tmask: *mut c_int,
    tm: *mut pg_tm,
    fsec: *mut fsec_t,
) -> c_int {
    let mut itm: pg_itm = std::mem::zeroed();
    let dterr: c_int;

    dterr = DecodeTimeCommon(str, fmask, range, tmask, &mut itm);
    if dterr != 0 {
        return dterr;
    }

    if itm.tm_hour > INT_MAX_C {
        return DTERR_FIELD_OVERFLOW;
    }
    (*tm).tm_hour = itm.tm_hour as c_int;
    (*tm).tm_min = itm.tm_min;
    (*tm).tm_sec = itm.tm_sec;
    *fsec = itm.tm_usec;

    0
}

/* DecodeTimeForInterval()
 * This version is used for intervals.
 */
unsafe fn DecodeTimeForInterval(
    str: *mut c_char,
    fmask: c_int,
    range: c_int,
    tmask: *mut c_int,
    itm_in: *mut pg_itm_in,
) -> c_int {
    let mut itm: pg_itm = std::mem::zeroed();
    let dterr: c_int;

    dterr = DecodeTimeCommon(str, fmask, range, tmask, &mut itm);
    if dterr != 0 {
        return dterr;
    }

    (*itm_in).tm_usec = itm.tm_usec as int64;
    if !int64_multiply_add(itm.tm_hour, USECS_PER_HOUR, &raw mut (*itm_in).tm_usec)
        || !int64_multiply_add(itm.tm_min as int64, USECS_PER_MINUTE, &raw mut (*itm_in).tm_usec)
        || !int64_multiply_add(itm.tm_sec as int64, USECS_PER_SEC, &raw mut (*itm_in).tm_usec)
    {
        return DTERR_FIELD_OVERFLOW;
    }

    0
}

/* DecodeNumber()
 * Interpret plain numeric field as a date value in context.
 */
unsafe fn DecodeNumber(
    flen: c_int,
    str: *mut c_char,
    haveTextMonth: bool,
    fmask: c_int,
    tmask: *mut c_int,
    tm: *mut pg_tm,
    fsec: *mut fsec_t,
    is2digits: *mut bool,
) -> c_int {
    let val: c_int;
    let mut cp: *mut c_char = std::ptr::null_mut();
    let mut dterr: c_int;

    *tmask = 0;

    set_errno(0);
    val = strtoint(str, &mut cp, 10);
    if get_errno() == ERANGE {
        return DTERR_FIELD_OVERFLOW;
    }
    if cp == str {
        return DTERR_BAD_FORMAT;
    }

    if *cp == b'.' as c_char {
        /*
         * More than two digits before decimal point? Then could be a date or
         * a run-together time: 2001.360 20011225 040506.789
         */
        if cp.offset_from(str) > 2 {
            dterr = DecodeNumberField(
                flen,
                str,
                fmask | DTK_DATE_M(),
                tmask,
                tm,
                fsec,
                is2digits,
            );
            if dterr < 0 {
                return dterr;
            }
            return 0;
        }

        dterr = ParseFractionalSecond(cp, fsec);
        if dterr != 0 {
            return dterr;
        }
    } else if *cp != 0 {
        return DTERR_BAD_FORMAT;
    }

    /* Special case for day of year */
    if flen == 3 && (fmask & DTK_DATE_M()) == DTK_M(YEAR) && val >= 1 && val <= 366 {
        *tmask = DTK_M(DOY) | DTK_M(MONTH) | DTK_M(DAY);
        (*tm).tm_yday = val;
        /* tm_mon and tm_mday can't actually be set yet ... */
        return 0;
    }

    /* Switch based on what we have so far */
    let f = fmask & DTK_DATE_M();
    if f == 0 {
        /*
         * Nothing so far; make a decision about what we think the input is.
         */
        if flen >= 3 || DateOrder == DATEORDER_YMD {
            *tmask = DTK_M(YEAR);
            (*tm).tm_year = val;
        } else if DateOrder == DATEORDER_DMY {
            *tmask = DTK_M(DAY);
            (*tm).tm_mday = val;
        } else {
            *tmask = DTK_M(MONTH);
            (*tm).tm_mon = val;
        }
    } else if f == DTK_M(YEAR) {
        /* Must be at second field of YY-MM-DD */
        *tmask = DTK_M(MONTH);
        (*tm).tm_mon = val;
    } else if f == DTK_M(MONTH) {
        if haveTextMonth {
            /*
             * We are at the first numeric field of a date that included a
             * textual month name.
             */
            if flen >= 3 || DateOrder == DATEORDER_YMD {
                *tmask = DTK_M(YEAR);
                (*tm).tm_year = val;
            } else {
                *tmask = DTK_M(DAY);
                (*tm).tm_mday = val;
            }
        } else {
            /* Must be at second field of MM-DD-YY */
            *tmask = DTK_M(DAY);
            (*tm).tm_mday = val;
        }
    } else if f == DTK_M(YEAR) | DTK_M(MONTH) {
        if haveTextMonth {
            /* Need to accept DD-MON-YYYY even in YMD mode */
            if flen >= 3 && *is2digits {
                /* Guess that first numeric field is day was wrong */
                *tmask = DTK_M(DAY); /* YEAR is already set */
                (*tm).tm_mday = (*tm).tm_year;
                (*tm).tm_year = val;
                *is2digits = false;
            } else {
                *tmask = DTK_M(DAY);
                (*tm).tm_mday = val;
            }
        } else {
            /* Must be at third field of YY-MM-DD */
            *tmask = DTK_M(DAY);
            (*tm).tm_mday = val;
        }
    } else if f == DTK_M(DAY) {
        /* Must be at second field of DD-MM-YY */
        *tmask = DTK_M(MONTH);
        (*tm).tm_mon = val;
    } else if f == DTK_M(MONTH) | DTK_M(DAY) {
        /* Must be at third field of DD-MM-YY or MM-DD-YY */
        *tmask = DTK_M(YEAR);
        (*tm).tm_year = val;
    } else if f == DTK_M(YEAR) | DTK_M(MONTH) | DTK_M(DAY) {
        /* we have all the date, so it must be a time field */
        dterr = DecodeNumberField(flen, str, fmask, tmask, tm, fsec, is2digits);
        if dterr < 0 {
            return dterr;
        }
        return 0;
    } else {
        /* Anything else is bogus input */
        return DTERR_BAD_FORMAT;
    }

    /*
     * When processing a year field, mark it for adjustment if it's only one
     * or two digits.
     */
    if *tmask == DTK_M(YEAR) {
        *is2digits = flen <= 2;
    }

    0
}

/* DecodeNumberField()
 * Interpret numeric string as a concatenated date or time field.
 * Return a DTK token (>= 0) if successful, a DTERR code (< 0) if not.
 */
unsafe fn DecodeNumberField(
    mut len: c_int,
    str: *mut c_char,
    fmask: c_int,
    tmask: *mut c_int,
    tm: *mut pg_tm,
    fsec: *mut fsec_t,
    is2digits: *mut bool,
) -> c_int {
    let cp: *mut c_char;

    /*
     * Reject if it's not a valid DTK_NUMBER, that is digits and decimal
     * point(s).
     */
    if strspn(str, b"0123456789.\0".as_ptr() as *const c_char) != len as usize {
        return DTERR_BAD_FORMAT;
    }

    /*
     * Have a decimal point? Then this is a date or something with a seconds
     * field...
     */
    cp = strchr(str, b'.' as c_int);
    if !cp.is_null() {
        let dterr: c_int;

        /* Convert the fraction and store at *fsec */
        dterr = ParseFractionalSecond(cp, fsec);
        if dterr != 0 {
            return dterr;
        }
        /* Now truncate off the fraction for further processing */
        *cp = 0;
        len = strlen(str) as c_int;
    }
    /* No decimal point and no complete date yet? */
    else if (fmask & DTK_DATE_M()) != DTK_DATE_M() {
        if len >= 6 {
            *tmask = DTK_DATE_M();

            /*
             * Start from end and consider first 2 as Day, next 2 as Month,
             * and the rest as Year.
             */
            (*tm).tm_mday = atoi(str.offset((len - 2) as isize));
            *str.offset((len - 2) as isize) = 0;
            (*tm).tm_mon = atoi(str.offset((len - 4) as isize));
            *str.offset((len - 4) as isize) = 0;
            (*tm).tm_year = atoi(str);
            if (len - 4) == 2 {
                *is2digits = true;
            }

            return DTK_DATE;
        }
    }

    /* not all time fields are specified? */
    if (fmask & DTK_TIME_M()) != DTK_TIME_M() {
        /* hhmmss */
        if len == 6 {
            *tmask = DTK_TIME_M();
            (*tm).tm_sec = atoi(str.offset(4));
            *str.offset(4) = 0;
            (*tm).tm_min = atoi(str.offset(2));
            *str.offset(2) = 0;
            (*tm).tm_hour = atoi(str);

            return DTK_TIME;
        }
        /* hhmm? */
        else if len == 4 {
            *tmask = DTK_TIME_M();
            (*tm).tm_sec = 0;
            (*tm).tm_min = atoi(str.offset(2));
            *str.offset(2) = 0;
            (*tm).tm_hour = atoi(str);

            return DTK_TIME;
        }
    }

    DTERR_BAD_FORMAT
}

/* DecodeTimezone()
 * Interpret string as a numeric timezone.
 */
pub unsafe fn DecodeTimezone(str: *const c_char, tzp: *mut c_int) -> c_int {
    let tz: c_int;
    let mut hr: c_int;
    let min: c_int;
    let mut sec: c_int = 0;
    let mut cp: *mut c_char = std::ptr::null_mut();

    /* leading character must be "+" or "-" */
    if *str != b'+' as c_char && *str != b'-' as c_char {
        return DTERR_BAD_FORMAT;
    }

    set_errno(0);
    hr = strtoint(str.offset(1), &mut cp, 10);
    if get_errno() == ERANGE {
        return DTERR_TZDISP_OVERFLOW;
    }

    /* explicit delimiter? */
    if *cp == b':' as c_char {
        set_errno(0);
        min = strtoint(cp.offset(1), &mut cp, 10);
        if get_errno() == ERANGE {
            return DTERR_TZDISP_OVERFLOW;
        }
        if *cp == b':' as c_char {
            set_errno(0);
            sec = strtoint(cp.offset(1), &mut cp, 10);
            if get_errno() == ERANGE {
                return DTERR_TZDISP_OVERFLOW;
            }
        }
    }
    /* otherwise, might have run things together... */
    else if *cp == 0 && strlen(str) > 3 {
        min = hr % 100;
        hr /= 100;
        /* we could, but don't, support a run-together hhmmss format */
    } else {
        min = 0;
    }

    /* Range-check the values; see notes in datatype/timestamp.h */
    if hr < 0 || hr > crate::utils::adt::date::MAX_TZDISP_HOUR {
        return DTERR_TZDISP_OVERFLOW;
    }
    if min < 0 || min >= MINS_PER_HOUR {
        return DTERR_TZDISP_OVERFLOW;
    }
    if sec < 0 || sec >= SECS_PER_MINUTE {
        return DTERR_TZDISP_OVERFLOW;
    }

    tz = (hr * MINS_PER_HOUR + min) * SECS_PER_MINUTE + sec;
    let tz = if *str == b'-' as c_char { -tz } else { tz };

    *tzp = -tz;

    if *cp != 0 {
        return DTERR_BAD_FORMAT;
    }

    0
}

/* DecodeTimezoneAbbrev()
 * Interpret string as a timezone abbreviation, if possible.
 */
pub unsafe fn DecodeTimezoneAbbrev(
    field: c_int,
    lowtoken: *const c_char,
    ftype: *mut c_int,
    offset: *mut c_int,
    tz: *mut *mut pg_tz,
    extra: *mut DateTimeErrorExtra,
) -> c_int {
    let tzc: *mut TzAbbrevCache = &raw mut tzabbrevcache[field as usize];
    let mut isfixed = false;
    let mut isdst: c_int = 0;
    let tp: *const datetkn;

    /*
     * Do we have a cached result?
     */
    if strncmp(lowtoken, (*tzc).abbrev.as_ptr(), TOKMAXLEN) == 0 {
        *ftype = (*tzc).ftype as c_int;
        *offset = (*tzc).offset;
        *tz = (*tzc).tz;
        return 0;
    }

    /*
     * See if the current session_timezone recognizes it.
     */
    if !session_timezone.is_null()
        && TimeZoneAbbrevIsKnown(
            lowtoken,
            session_timezone,
            &mut isfixed,
            offset,
            &mut isdst,
        )
    {
        *ftype = if isfixed {
            if isdst != 0 {
                DTZ
            } else {
                TZ
            }
        } else {
            DYNTZ
        };
        *tz = if isfixed {
            std::ptr::null_mut()
        } else {
            session_timezone
        };
        /* flip sign to agree with the convention used in zoneabbrevtbl */
        *offset = -(*offset);
        /* cache result; use strlcpy to truncate name if necessary */
        strlcpy((*tzc).abbrev.as_mut_ptr(), lowtoken, TOKMAXLEN + 1);
        (*tzc).ftype = *ftype as c_char;
        (*tzc).offset = *offset;
        (*tzc).tz = *tz;
        return 0;
    }

    /* Nope, so look in zoneabbrevtbl */
    if !zoneabbrevtbl.is_null() {
        tp = datebsearch(
            lowtoken,
            (*zoneabbrevtbl).abbrevs.as_ptr(),
            (*zoneabbrevtbl).numabbrevs,
        );
    } else {
        tp = std::ptr::null();
    }
    if tp.is_null() {
        *ftype = UNKNOWN_FIELD;
        *offset = 0;
        *tz = std::ptr::null_mut();
        /* failure results are not cached */
    } else {
        *ftype = (*tp).r#type as c_int;
        if (*tp).r#type as c_int == DYNTZ {
            *offset = 0;
            *tz = FetchDynamicTimeZone(zoneabbrevtbl, tp, extra);
            if (*tz).is_null() {
                return DTERR_BAD_ZONE_ABBREV;
            }
        } else {
            *offset = (*tp).value;
            *tz = std::ptr::null_mut();
        }

        /* cache result; use strlcpy to truncate name if necessary */
        strlcpy((*tzc).abbrev.as_mut_ptr(), lowtoken, TOKMAXLEN + 1);
        (*tzc).ftype = *ftype as c_char;
        (*tzc).offset = *offset;
        (*tzc).tz = *tz;
    }

    0
}

/*
 * Reset tzabbrevcache after a change in session_timezone.
 */
pub unsafe fn ClearTimeZoneAbbrevCache() {
    memset(
        tzabbrevcache.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of_val(&tzabbrevcache),
    );
}

/* DecodeSpecial()
 * Decode text string using lookup table.
 */
pub unsafe fn DecodeSpecial(field: c_int, lowtoken: *const c_char, val: *mut c_int) -> c_int {
    let r#type: c_int;
    let mut tp: *const datetkn;

    tp = datecache[field as usize];
    /* use strncmp so that we match truncated tokens */
    if tp.is_null() || strncmp(lowtoken, (*tp).token.as_ptr(), TOKMAXLEN) != 0 {
        tp = datebsearch(lowtoken, datetktbl.as_ptr(), szdatetktbl);
    }
    if tp.is_null() {
        r#type = UNKNOWN_FIELD;
        *val = 0;
    } else {
        datecache[field as usize] = tp;
        r#type = (*tp).r#type as c_int;
        *val = (*tp).value;
    }

    r#type
}

/* DecodeTimezoneName()
 * Interpret string as a timezone abbreviation or name.
 */
pub unsafe fn DecodeTimezoneName(
    tzname: *const c_char,
    offset: *mut c_int,
    tz: *mut *mut pg_tz,
) -> c_int {
    let lowzone: *mut c_char;
    let dterr: c_int;
    let mut r#type: c_int = 0;
    let mut extra: DateTimeErrorExtra = std::mem::zeroed();

    /* DecodeTimezoneAbbrev requires lowercase input */
    lowzone = downcase_truncate_identifier(tzname, strlen(tzname) as c_int, false);

    dterr = DecodeTimezoneAbbrev(0, lowzone, &mut r#type, offset, tz, &mut extra);
    if dterr != 0 {
        DateTimeParseError(
            dterr,
            &mut extra,
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null_mut(),
        );
    }

    if r#type == TZ || r#type == DTZ {
        /* fixed-offset abbreviation, return the offset */
        TZNAME_FIXED_OFFSET
    } else if r#type == DYNTZ {
        /* dynamic-offset abbreviation, return its referenced timezone */
        TZNAME_DYNTZ
    } else {
        /* try it as a full zone name */
        *tz = pg_tzset(tzname);
        if (*tz).is_null() {
            ereport!(
                ERROR,
                errmsg!("time zone \"{}\" not recognized", cstr(tzname))
            );
        }
        TZNAME_ZONE
    }
}

/* DecodeTimezoneNameToTz()
 */
pub unsafe fn DecodeTimezoneNameToTz(tzname: *const c_char) -> *mut pg_tz {
    let mut result: *mut pg_tz = std::ptr::null_mut();
    let mut offset: c_int = 0;

    if DecodeTimezoneName(tzname, &mut offset, &mut result) == TZNAME_FIXED_OFFSET {
        /* fixed-offset abbreviation, get a pg_tz descriptor for that */
        result = pg_tzset_offset(-offset as c_long); /* flip to POSIX sign convention */
    }
    result
}

/* DecodeTimezoneAbbrevPrefix()
 * Interpret prefix of string as a timezone abbreviation, if possible.
 */
pub unsafe fn DecodeTimezoneAbbrevPrefix(
    mut str: *const c_char,
    offset: *mut c_int,
    tz: *mut *mut pg_tz,
) -> c_int {
    let mut lowtoken: [c_char; TOKMAXLEN + 1] = [0; TOKMAXLEN + 1];
    let mut len: c_int;

    *offset = 0; /* avoid uninitialized vars on failure */
    *tz = std::ptr::null_mut();

    /* Downcase as much of the string as we could need */
    len = 0;
    while (len as usize) < TOKMAXLEN {
        if *str == 0 || isalpha(*str as u8 as c_int) == 0 {
            break;
        }
        lowtoken[len as usize] = pg_tolower(*str as u8) as c_char;
        str = str.offset(1);
        len += 1;
    }
    lowtoken[len as usize] = 0;

    /*
     * Search with successively truncated strings.
     */
    while len > 0 {
        let mut isfixed = false;
        let mut isdst: c_int = 0;
        let tp: *const datetkn;

        /* See if the current session_timezone recognizes it. */
        if !session_timezone.is_null()
            && TimeZoneAbbrevIsKnown(
                lowtoken.as_ptr(),
                session_timezone,
                &mut isfixed,
                offset,
                &mut isdst,
            )
        {
            if isfixed {
                /* flip sign to agree with the convention in zoneabbrevtbl */
                *offset = -(*offset);
            } else {
                /* Caller must resolve the abbrev's current meaning */
                *tz = session_timezone;
            }
            return len;
        }

        /* Known in zoneabbrevtbl? */
        if !zoneabbrevtbl.is_null() {
            tp = datebsearch(
                lowtoken.as_ptr(),
                (*zoneabbrevtbl).abbrevs.as_ptr(),
                (*zoneabbrevtbl).numabbrevs,
            );
        } else {
            tp = std::ptr::null();
        }
        if !tp.is_null() {
            if (*tp).r#type as c_int == DYNTZ {
                let mut extra: DateTimeErrorExtra = std::mem::zeroed();
                let tzp: *mut pg_tz = FetchDynamicTimeZone(zoneabbrevtbl, tp, &mut extra);

                if !tzp.is_null() {
                    /* Caller must resolve the abbrev's current meaning */
                    *tz = tzp;
                    return len;
                }
            } else {
                /* Fixed-offset zone abbrev, so it's easy */
                *offset = (*tp).value;
                return len;
            }
        }

        /* Nope, try the next shorter string. */
        len -= 1;
        lowtoken[len as usize] = 0;
    }

    /* Did not find a match */
    -1
}

/* ClearPgItmIn
 *
 * Zero out a pg_itm_in
 */
#[inline]
pub unsafe fn ClearPgItmIn(itm_in: *mut pg_itm_in) {
    (*itm_in).tm_usec = 0;
    (*itm_in).tm_mday = 0;
    (*itm_in).tm_mon = 0;
    (*itm_in).tm_year = 0;
}

/* DecodeInterval()
 * Interpret previously parsed fields for general time interval.
 * Returns 0 if successful, DTERR code if bogus input detected.
 */
pub unsafe fn DecodeInterval(
    field: *mut *mut c_char,
    ftype: *const c_int,
    nf: c_int,
    range: c_int,
    dtype: *mut c_int,
    itm_in: *mut pg_itm_in,
) -> c_int {
    let mut force_negative = false;
    let mut is_before = false;
    let mut parsing_unit_val = false;
    let mut cp: *mut c_char = std::ptr::null_mut();
    let mut fmask: c_int = 0;
    let mut tmask: c_int = 0;
    let mut r#type: c_int;
    let mut uval: c_int = 0;
    let mut i: c_int;
    let mut dterr: c_int;
    let mut val: int64;
    let mut fval: f64 = 0.0;

    *dtype = DTK_DELTA;
    r#type = IGNORE_DTF;
    ClearPgItmIn(itm_in);

    /*
     * In SQL_STANDARD intervalstyle, we apply the leading sign to all fields
     * if there are no other explicit signs.
     */
    if IntervalStyle == INTSTYLE_SQL_STANDARD && nf > 0 && **field.offset(0) == b'-' as c_char {
        force_negative = true;
        /* Check for additional explicit signs */
        i = 1;
        while i < nf {
            if **field.offset(i as isize) == b'-' as c_char
                || **field.offset(i as isize) == b'+' as c_char
            {
                force_negative = false;
                break;
            }
            i += 1;
        }
    }

    /* read through list backwards to pick up units before values */
    i = nf - 1;
    while i >= 0 {
        match *ftype.offset(i as isize) {
            x if x == DTK_TIME => {
                dterr = DecodeTimeForInterval(
                    *field.offset(i as isize),
                    fmask,
                    range,
                    &mut tmask,
                    itm_in,
                );
                if dterr != 0 {
                    return dterr;
                }
                if force_negative && (*itm_in).tm_usec > 0 {
                    (*itm_in).tm_usec = -(*itm_in).tm_usec;
                }
                r#type = DTK_DAY;
                parsing_unit_val = false;

                if (tmask & fmask) != 0 {
                    return DTERR_BAD_FORMAT;
                }
                fmask |= tmask;
                i -= 1;
                continue;
            }

            x if x == DTK_TZ || x == DTK_DATE || x == DTK_NUMBER => {
                /*
                 * DTK_TZ: a token with a leading sign character and at least
                 * one digit; could be signed hh:mm or hh:mm:ss, otherwise it
                 * falls through to the DTK_NUMBER handling.
                 */
                if x == DTK_TZ {
                    Assert!(
                        **field.offset(i as isize) == b'-' as c_char
                            || **field.offset(i as isize) == b'+' as c_char
                    );

                    if !strchr(field.offset(i as isize).read().offset(1), b':' as c_int).is_null()
                        && DecodeTimeForInterval(
                            field.offset(i as isize).read().offset(1),
                            fmask,
                            range,
                            &mut tmask,
                            itm_in,
                        ) == 0
                    {
                        if **field.offset(i as isize) == b'-' as c_char {
                            /* flip the sign on time field */
                            if (*itm_in).tm_usec == i64::MIN {
                                return DTERR_FIELD_OVERFLOW;
                            }
                            (*itm_in).tm_usec = -(*itm_in).tm_usec;
                        }

                        if force_negative && (*itm_in).tm_usec > 0 {
                            (*itm_in).tm_usec = -(*itm_in).tm_usec;
                        }

                        r#type = DTK_DAY;
                        parsing_unit_val = false;

                        if (tmask & fmask) != 0 {
                            return DTERR_BAD_FORMAT;
                        }
                        fmask |= tmask;
                        i -= 1;
                        continue;
                    }
                    /* Otherwise, fall through to DTK_NUMBER case. */
                }

                if r#type == IGNORE_DTF {
                    /* use typmod to decide what rightmost field is */
                    if range == INTERVAL_MASK(YEAR) {
                        r#type = DTK_YEAR;
                    } else if range == INTERVAL_MASK(MONTH)
                        || range == INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH)
                    {
                        r#type = DTK_MONTH;
                    } else if range == INTERVAL_MASK(DAY) {
                        r#type = DTK_DAY;
                    } else if range == INTERVAL_MASK(HOUR)
                        || range == INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR)
                    {
                        r#type = DTK_HOUR;
                    } else if range == INTERVAL_MASK(MINUTE)
                        || range == INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE)
                        || range
                            == INTERVAL_MASK(DAY)
                                | INTERVAL_MASK(HOUR)
                                | INTERVAL_MASK(MINUTE)
                    {
                        r#type = DTK_MINUTE;
                    } else if range == INTERVAL_MASK(SECOND)
                        || range == INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND)
                        || range
                            == INTERVAL_MASK(HOUR)
                                | INTERVAL_MASK(MINUTE)
                                | INTERVAL_MASK(SECOND)
                        || range
                            == INTERVAL_MASK(DAY)
                                | INTERVAL_MASK(HOUR)
                                | INTERVAL_MASK(MINUTE)
                                | INTERVAL_MASK(SECOND)
                    {
                        r#type = DTK_SECOND;
                    } else {
                        r#type = DTK_SECOND;
                    }
                }

                set_errno(0);
                val = strtoi64(*field.offset(i as isize), &mut cp, 10);
                if get_errno() == ERANGE {
                    return DTERR_FIELD_OVERFLOW;
                }

                if *cp == b'-' as c_char {
                    /* SQL "years-months" syntax */
                    let mut val2: c_int;

                    val2 = strtoint(cp.offset(1), &mut cp, 10);
                    if get_errno() == ERANGE || val2 < 0 || val2 >= MONTHS_PER_YEAR {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if *cp != 0 {
                        return DTERR_BAD_FORMAT;
                    }
                    r#type = DTK_MONTH;
                    if **field.offset(i as isize) == b'-' as c_char {
                        val2 = -val2;
                    }
                    if pg_mul_s64_overflow(val, MONTHS_PER_YEAR as int64, &mut val) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    if pg_add_s64_overflow(val, val2 as int64, &mut val) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    fval = 0.0;
                } else if *cp == b'.' as c_char {
                    dterr = ParseFraction(cp, &mut fval);
                    if dterr != 0 {
                        return dterr;
                    }
                    if **field.offset(i as isize) == b'-' as c_char {
                        fval = -fval;
                    }
                } else if *cp == 0 {
                    fval = 0.0;
                } else {
                    return DTERR_BAD_FORMAT;
                }

                tmask = 0; /* DTK_M(type); */

                if force_negative {
                    /* val and fval should be of same sign, but test anyway */
                    if val > 0 {
                        val = -val;
                    }
                    if fval > 0.0 {
                        fval = -fval;
                    }
                }

                if r#type == DTK_MICROSEC {
                    if !AdjustMicroseconds(val, fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MICROSECOND);
                } else if r#type == DTK_MILLISEC {
                    if !AdjustMicroseconds(val, fval, 1000, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MILLISECOND);
                } else if r#type == DTK_SECOND {
                    if !AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }

                    /*
                     * If any subseconds were specified, consider this
                     * microsecond and millisecond input as well.
                     */
                    if fval == 0.0 {
                        tmask = DTK_M(SECOND);
                    } else {
                        tmask = DTK_ALL_SECS_M();
                    }
                } else if r#type == DTK_MINUTE {
                    if !AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MINUTE);
                } else if r#type == DTK_HOUR {
                    if !AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(HOUR);
                    r#type = DTK_DAY; /* set for next field */
                } else if r#type == DTK_DAY {
                    if !AdjustDays(val, 1, itm_in)
                        || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(DAY);
                } else if r#type == DTK_WEEK {
                    if !AdjustDays(val, 7, itm_in) || !AdjustFractDays(fval, 7, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(WEEK);
                } else if r#type == DTK_MONTH {
                    if !AdjustMonths(val, itm_in)
                        || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MONTH);
                } else if r#type == DTK_YEAR {
                    if !AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(YEAR);
                } else if r#type == DTK_DECADE {
                    if !AdjustYears(val, 10, itm_in) || !AdjustFractYears(fval, 10, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(DECADE);
                } else if r#type == DTK_CENTURY {
                    if !AdjustYears(val, 100, itm_in) || !AdjustFractYears(fval, 100, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(CENTURY);
                } else if r#type == DTK_MILLENNIUM {
                    if !AdjustYears(val, 1000, itm_in) || !AdjustFractYears(fval, 1000, itm_in) {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    tmask = DTK_M(MILLENNIUM);
                } else {
                    return DTERR_BAD_FORMAT;
                }
                parsing_unit_val = false;

                if (tmask & fmask) != 0 {
                    return DTERR_BAD_FORMAT;
                }
                fmask |= tmask;
                i -= 1;
                continue;
            }

            x if x == DTK_STRING || x == DTK_SPECIAL => {
                /* reject consecutive unhandled units */
                if parsing_unit_val {
                    return DTERR_BAD_FORMAT;
                }
                r#type = DecodeUnits(i, *field.offset(i as isize), &mut uval);
                if r#type == UNKNOWN_FIELD {
                    r#type = DecodeSpecial(i, *field.offset(i as isize), &mut uval);
                }
                if r#type == IGNORE_DTF {
                    i -= 1;
                    continue;
                }

                tmask = 0; /* DTK_M(type); */
                if r#type == UNITS {
                    r#type = uval;
                    parsing_unit_val = true;
                } else if r#type == AGO {
                    /*
                     * "ago" is only allowed to appear at the end of the
                     * interval.
                     */
                    if i != nf - 1 {
                        return DTERR_BAD_FORMAT;
                    }
                    is_before = true;
                    r#type = uval;
                } else if r#type == RESERV {
                    tmask = DTK_DATE_M() | DTK_TIME_M();

                    /*
                     * Only reserved words corresponding to infinite intervals
                     * are accepted.
                     */
                    if uval != DTK_LATE && uval != DTK_EARLY {
                        return DTERR_BAD_FORMAT;
                    }

                    /*
                     * Infinity cannot be followed by anything else.
                     */
                    if i != nf - 1 {
                        return DTERR_BAD_FORMAT;
                    }

                    *dtype = uval;
                } else {
                    return DTERR_BAD_FORMAT;
                }

                if (tmask & fmask) != 0 {
                    return DTERR_BAD_FORMAT;
                }
                fmask |= tmask;
                i -= 1;
                continue;
            }

            _ => {
                return DTERR_BAD_FORMAT;
            }
        }
    }

    /* ensure that at least one time field has been found */
    if fmask == 0 {
        return DTERR_BAD_FORMAT;
    }

    /* reject if unit appeared and was never handled */
    if parsing_unit_val {
        return DTERR_BAD_FORMAT;
    }

    /* finally, AGO negates everything */
    if is_before {
        if (*itm_in).tm_usec == i64::MIN
            || (*itm_in).tm_mday == i32::MIN
            || (*itm_in).tm_mon == i32::MIN
            || (*itm_in).tm_year == i32::MIN
        {
            return DTERR_FIELD_OVERFLOW;
        }

        (*itm_in).tm_usec = -(*itm_in).tm_usec;
        (*itm_in).tm_mday = -(*itm_in).tm_mday;
        (*itm_in).tm_mon = -(*itm_in).tm_mon;
        (*itm_in).tm_year = -(*itm_in).tm_year;
    }

    0
}

/*
 * Helper functions to avoid duplicated code in DecodeISO8601Interval.
 */
unsafe fn ParseISO8601Number(
    str: *mut c_char,
    endptr: *mut *mut c_char,
    ipart: *mut int64,
    fpart: *mut f64,
) -> c_int {
    let val: f64;

    if !(isdigit(*str as u8 as c_int) != 0 || *str == b'-' as c_char || *str == b'.' as c_char) {
        return DTERR_BAD_FORMAT;
    }
    set_errno(0);
    val = strtod(str, endptr);
    /* did we not see anything that looks like a double? */
    if *endptr == str || get_errno() != 0 {
        return DTERR_BAD_FORMAT;
    }
    /* watch out for overflow, including infinities; reject NaN too */
    if isnan(val) != 0 || val < -1.0e15 || val > 1.0e15 {
        return DTERR_FIELD_OVERFLOW;
    }
    /* be very sure we truncate towards zero (cf dtrunc()) */
    if val >= 0.0 {
        *ipart = floor(val) as int64;
    } else {
        *ipart = -(floor(-val) as int64);
    }
    *fpart = val - *ipart as f64;
    /* Callers expect this to hold */
    Assert!(*fpart > -1.0 && *fpart < 1.0);
    0
}

/*
 * Determine number of integral digits in a valid ISO 8601 number field
 */
unsafe fn ISO8601IntegerWidth(mut fieldstart: *mut c_char) -> c_int {
    /* We might have had a leading '-' */
    if *fieldstart == b'-' as c_char {
        fieldstart = fieldstart.offset(1);
    }
    strspn(fieldstart, b"0123456789\0".as_ptr() as *const c_char) as c_int
}

/* DecodeISO8601Interval()
 *
 * The C original uses switch-fallthrough between the basic and the two
 * "extended alternative format" cases.  We model that fallthrough by having
 * the date-part 'T'/'\0'/'-' cases and the time-part '\0'/':' cases share an
 * `extended` code path, entered after the basic-format early returns fail.
 * The per-iteration control flow is expressed with a small enum mirroring the
 * C `return`/`continue`/fall-through outcomes.
 */
pub unsafe fn DecodeISO8601Interval(
    mut str: *mut c_char,
    dtype: *mut c_int,
    itm_in: *mut pg_itm_in,
) -> c_int {
    let mut datepart = true;
    let mut havefield = false;

    *dtype = DTK_DELTA;
    ClearPgItmIn(itm_in);

    if strlen(str) < 2 || *str.offset(0) != b'P' as c_char {
        return DTERR_BAD_FORMAT;
    }

    str = str.offset(1);
    while *str != 0 {
        let fieldstart: *mut c_char;
        let mut val: int64 = 0;
        let mut fval: f64 = 0.0;
        let unit: c_char;
        let mut dterr: c_int;

        if *str == b'T' as c_char {
            /* T indicates the beginning of the time part */
            datepart = false;
            havefield = false;
            str = str.offset(1);
            continue;
        }

        fieldstart = str;
        dterr = ParseISO8601Number(str, &mut str, &mut val, &mut fval);
        if dterr != 0 {
            return dterr;
        }

        /*
         * Note: we could step off the end of the string here.  Code below
         * *must* exit the loop if unit == '\0'.
         */
        unit = *str;
        str = str.offset(1);

        if datepart {
            /* before T: Y M W D */
            let mut do_extended = false;
            if unit == b'Y' as c_char {
                if !AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
            } else if unit == b'M' as c_char {
                if !AdjustMonths(val, itm_in) || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
            } else if unit == b'W' as c_char {
                if !AdjustDays(val, 7, itm_in) || !AdjustFractDays(fval, 7, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
            } else if unit == b'D' as c_char {
                if !AdjustDays(val, 1, itm_in)
                    || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                {
                    return DTERR_FIELD_OVERFLOW;
                }
            } else if unit == b'T' as c_char || unit == 0 {
                /* ISO 8601 4.4.3.3 Alternative Format / Basic */
                if ISO8601IntegerWidth(fieldstart) == 8 && !havefield {
                    if !AdjustYears(val / 10000, 1, itm_in)
                        || !AdjustMonths((val / 100) % 100, itm_in)
                        || !AdjustDays(val % 100, 1, itm_in)
                        || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
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
                /* Else fall through to extended alternative format */
                do_extended = true;
            } else if unit == b'-' as c_char {
                /* ISO 8601 4.4.3.3 Alternative Format, Extended */
                do_extended = true;
            } else {
                /* not a valid date unit suffix */
                return DTERR_BAD_FORMAT;
            }

            if do_extended {
                if havefield {
                    return DTERR_BAD_FORMAT;
                }

                if !AdjustYears(val, 1, itm_in) || !AdjustFractYears(fval, 1, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
                if unit == 0 {
                    return 0;
                }
                if unit == b'T' as c_char {
                    datepart = false;
                    havefield = false;
                    continue;
                }

                dterr = ParseISO8601Number(str, &mut str, &mut val, &mut fval);
                if dterr != 0 {
                    return dterr;
                }
                if !AdjustMonths(val, itm_in) || !AdjustFractDays(fval, DAYS_PER_MONTH, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
                if *str == 0 {
                    return 0;
                }
                if *str == b'T' as c_char {
                    datepart = false;
                    havefield = false;
                    continue;
                }
                if *str != b'-' as c_char {
                    return DTERR_BAD_FORMAT;
                }
                str = str.offset(1);

                dterr = ParseISO8601Number(str, &mut str, &mut val, &mut fval);
                if dterr != 0 {
                    return dterr;
                }
                if !AdjustDays(val, 1, itm_in)
                    || !AdjustFractMicroseconds(fval, USECS_PER_DAY, itm_in)
                {
                    return DTERR_FIELD_OVERFLOW;
                }
                if *str == 0 {
                    return 0;
                }
                if *str == b'T' as c_char {
                    datepart = false;
                    havefield = false;
                    continue;
                }
                return DTERR_BAD_FORMAT;
            }
        } else {
            /* after T: H M S */
            let mut do_extended = false;
            if unit == b'H' as c_char {
                if !AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
            } else if unit == b'M' as c_char {
                if !AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
            } else if unit == b'S' as c_char {
                if !AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
            } else if unit == 0 {
                /* ISO 8601 4.4.3.3 Alternative Format */
                if ISO8601IntegerWidth(fieldstart) == 6 && !havefield {
                    if !AdjustMicroseconds(val / 10000, 0.0, USECS_PER_HOUR, itm_in)
                        || !AdjustMicroseconds((val / 100) % 100, 0.0, USECS_PER_MINUTE, itm_in)
                        || !AdjustMicroseconds(val % 100, 0.0, USECS_PER_SEC, itm_in)
                        || !AdjustFractMicroseconds(fval, 1, itm_in)
                    {
                        return DTERR_FIELD_OVERFLOW;
                    }
                    return 0;
                }
                /* Else fall through to extended alternative format */
                do_extended = true;
            } else if unit == b':' as c_char {
                /* ISO 8601 4.4.3.3 Alternative Format, Extended */
                do_extended = true;
            } else {
                /* not a valid time unit suffix */
                return DTERR_BAD_FORMAT;
            }

            if do_extended {
                if havefield {
                    return DTERR_BAD_FORMAT;
                }

                if !AdjustMicroseconds(val, fval, USECS_PER_HOUR, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
                if unit == 0 {
                    return 0;
                }

                dterr = ParseISO8601Number(str, &mut str, &mut val, &mut fval);
                if dterr != 0 {
                    return dterr;
                }
                if !AdjustMicroseconds(val, fval, USECS_PER_MINUTE, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
                if *str == 0 {
                    return 0;
                }
                if *str != b':' as c_char {
                    return DTERR_BAD_FORMAT;
                }
                str = str.offset(1);

                dterr = ParseISO8601Number(str, &mut str, &mut val, &mut fval);
                if dterr != 0 {
                    return dterr;
                }
                if !AdjustMicroseconds(val, fval, USECS_PER_SEC, itm_in) {
                    return DTERR_FIELD_OVERFLOW;
                }
                if *str == 0 {
                    return 0;
                }
                return DTERR_BAD_FORMAT;
            }
        }

        havefield = true;
    }

    0
}

/* DecodeUnits()
 * Decode text string using lookup table.
 */
pub unsafe fn DecodeUnits(field: c_int, lowtoken: *const c_char, val: *mut c_int) -> c_int {
    let r#type: c_int;
    let mut tp: *const datetkn;

    tp = deltacache[field as usize];
    /* use strncmp so that we match truncated tokens */
    if tp.is_null() || strncmp(lowtoken, (*tp).token.as_ptr(), TOKMAXLEN) != 0 {
        tp = datebsearch(lowtoken, deltatktbl.as_ptr(), szdeltatktbl);
    }
    if tp.is_null() {
        r#type = UNKNOWN_FIELD;
        *val = 0;
    } else {
        deltacache[field as usize] = tp;
        r#type = (*tp).r#type as c_int;
        *val = (*tp).value;
    }

    r#type
} /* DecodeUnits() */

/*
 * Report an error detected by one of the datetime input processing routines.
 */
pub unsafe fn DateTimeParseError(
    dterr: c_int,
    extra: *const DateTimeErrorExtra,
    str: *const c_char,
    datatype: *const c_char,
    _escontext: *mut Node,
) {
    /*
     * The C code uses errsave(escontext, ...); we always raise (escontext
     * NULL semantics) because ereport! folds to (level, errmsg).
     */
    if dterr == DTERR_FIELD_OVERFLOW {
        ereport!(
            ERROR,
            errmsg!("date/time field value out of range: \"{}\"", cstr(str))
        );
    } else if dterr == DTERR_MD_FIELD_OVERFLOW {
        /* same as above, but add hint about DateStyle */
        ereport!(
            ERROR,
            errmsg!("date/time field value out of range: \"{}\"", cstr(str))
        );
    } else if dterr == DTERR_INTERVAL_OVERFLOW {
        ereport!(
            ERROR,
            errmsg!("interval field value out of range: \"{}\"", cstr(str))
        );
    } else if dterr == DTERR_TZDISP_OVERFLOW {
        ereport!(
            ERROR,
            errmsg!("time zone displacement out of range: \"{}\"", cstr(str))
        );
    } else if dterr == DTERR_BAD_TIMEZONE {
        ereport!(
            ERROR,
            errmsg!(
                "time zone \"{}\" not recognized",
                cstr((*extra).dtee_timezone)
            )
        );
    } else if dterr == DTERR_BAD_ZONE_ABBREV {
        ereport!(
            ERROR,
            errmsg!(
                "time zone \"{}\" not recognized",
                cstr((*extra).dtee_timezone)
            )
        );
    } else {
        /* DTERR_BAD_FORMAT and default */
        ereport!(
            ERROR,
            errmsg!(
                "invalid input syntax for type {}: \"{}\"",
                cstr(datatype),
                cstr(str)
            )
        );
    }
}

/* datebsearch()
 * Binary search -- from Knuth (6.2.1) Algorithm B.
 */
unsafe fn datebsearch(key: *const c_char, mut base: *const datetkn, nel: c_int) -> *const datetkn {
    if nel > 0 {
        let mut last: *const datetkn = base.offset((nel - 1) as isize);
        let mut position: *const datetkn;
        let mut result: c_int;

        while last >= base {
            position = base.offset(last.offset_from(base) >> 1);
            /* precheck the first character for a bit of extra speed */
            result = *key.offset(0) as c_int - (*position).token[0] as c_int;
            if result == 0 {
                /* use strncmp so that we match truncated tokens */
                result = strncmp(key, (*position).token.as_ptr(), TOKMAXLEN);
                if result == 0 {
                    return position;
                }
            }
            if result < 0 {
                last = position.offset(-1);
            } else {
                base = position.offset(1);
            }
        }
    }
    std::ptr::null()
}

/*
 * Helper: write a Rust string (no NUL) at cp, returning cp advanced past it.
 * Used to stand in for the C sprintf(cp, ...); cp += strlen(cp) idiom.  The
 * formatted output is ASCII-only in all call sites.
 */
unsafe fn putstr(cp: *mut c_char, s: &str) -> *mut c_char {
    let bytes = s.as_bytes();
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, cp, bytes.len());
    cp.add(bytes.len())
}

/* EncodeTimezone()
 *		Copies representation of a numeric timezone offset to str.
 */
pub unsafe fn EncodeTimezone(mut str: *mut c_char, tz: c_int, style: c_int) -> *mut c_char {
    let hour: c_int;
    let mut min: c_int;
    let mut sec: c_int;

    sec = abs_i32(tz);
    min = sec / SECS_PER_MINUTE;
    sec -= min * SECS_PER_MINUTE;
    hour = min / MINS_PER_HOUR;
    min -= hour * MINS_PER_HOUR;

    /* TZ is negated compared to sign we wish to display ... */
    *str = if tz <= 0 { b'+' as c_char } else { b'-' as c_char };
    str = str.offset(1);

    if sec != 0 {
        str = pg_ultostr_zeropad(str, hour as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, min as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, sec as uint32, 2);
    } else if min != 0 || style == USE_XSD_DATES {
        str = pg_ultostr_zeropad(str, hour as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, min as uint32, 2);
    } else {
        str = pg_ultostr_zeropad(str, hour as uint32, 2);
    }
    str
}

#[inline]
unsafe fn year_for_output(tm_year: c_int) -> uint32 {
    (if tm_year > 0 { tm_year } else { -(tm_year - 1) }) as uint32
}

/* EncodeDateOnly()
 * Encode date as local time.
 */
pub unsafe fn EncodeDateOnly(tm: *mut pg_tm, style: c_int, mut str: *mut c_char) {
    Assert!((*tm).tm_mon >= 1 && (*tm).tm_mon <= MONTHS_PER_YEAR);

    if style == USE_ISO_DATES || style == USE_XSD_DATES {
        /* compatible with ISO date formats */
        str = pg_ultostr_zeropad(str, year_for_output((*tm).tm_year), 4);
        *str = b'-' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
        *str = b'-' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
    } else if style == USE_SQL_DATES {
        /* compatible with Oracle/Ingres date formats */
        if DateOrder == DATEORDER_DMY {
            str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
            *str = b'/' as c_char;
            str = str.offset(1);
            str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
        } else {
            str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
            *str = b'/' as c_char;
            str = str.offset(1);
            str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
        }
        *str = b'/' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, year_for_output((*tm).tm_year), 4);
    } else if style == USE_GERMAN_DATES {
        /* German-style date format */
        str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
        *str = b'.' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
        *str = b'.' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, year_for_output((*tm).tm_year), 4);
    } else {
        /* traditional date-only style for Postgres (USE_POSTGRES_DATES) */
        if DateOrder == DATEORDER_DMY {
            str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
            *str = b'-' as c_char;
            str = str.offset(1);
            str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
        } else {
            str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
            *str = b'-' as c_char;
            str = str.offset(1);
            str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
        }
        *str = b'-' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, year_for_output((*tm).tm_year), 4);
    }

    if (*tm).tm_year <= 0 {
        memcpy(
            str as *mut c_void,
            b" BC".as_ptr() as *const c_void,
            3,
        ); /* Don't copy NUL */
        str = str.offset(3);
    }
    *str = 0;
}

/* EncodeTimeOnly()
 * Encode time fields only.
 */
pub unsafe fn EncodeTimeOnly(
    tm: *mut pg_tm,
    fsec: fsec_t,
    print_tz: bool,
    tz: c_int,
    style: c_int,
    mut str: *mut c_char,
) {
    str = pg_ultostr_zeropad(str, (*tm).tm_hour as uint32, 2);
    *str = b':' as c_char;
    str = str.offset(1);
    str = pg_ultostr_zeropad(str, (*tm).tm_min as uint32, 2);
    *str = b':' as c_char;
    str = str.offset(1);
    str = AppendSeconds(str, (*tm).tm_sec, fsec, MAX_TIME_PRECISION, true);
    if print_tz {
        str = EncodeTimezone(str, tz, style);
    }
    *str = 0;
}

/* EncodeDateTime()
 * Encode date and time interpreted as local time.
 */
pub unsafe fn EncodeDateTime(
    tm: *mut pg_tm,
    fsec: fsec_t,
    mut print_tz: bool,
    tz: c_int,
    tzn: *const c_char,
    style: c_int,
    mut str: *mut c_char,
) {
    let day: c_int;

    Assert!((*tm).tm_mon >= 1 && (*tm).tm_mon <= MONTHS_PER_YEAR);

    /*
     * Negative tm_isdst means we have no valid time zone translation.
     */
    if (*tm).tm_isdst < 0 {
        print_tz = false;
    }

    if style == USE_ISO_DATES || style == USE_XSD_DATES {
        /* Compatible with ISO-8601 date formats */
        str = pg_ultostr_zeropad(str, year_for_output((*tm).tm_year), 4);
        *str = b'-' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
        *str = b'-' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
        *str = if style == USE_ISO_DATES { b' ' as c_char } else { b'T' as c_char };
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_hour as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_min as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = AppendTimestampSeconds(str, tm, fsec);
        if print_tz {
            str = EncodeTimezone(str, tz, style);
        }
    } else if style == USE_SQL_DATES {
        /* Compatible with Oracle/Ingres date formats */
        if DateOrder == DATEORDER_DMY {
            str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
            *str = b'/' as c_char;
            str = str.offset(1);
            str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
        } else {
            str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
            *str = b'/' as c_char;
            str = str.offset(1);
            str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
        }
        *str = b'/' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, year_for_output((*tm).tm_year), 4);
        *str = b' ' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_hour as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_min as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = AppendTimestampSeconds(str, tm, fsec);

        if print_tz {
            if !tzn.is_null() {
                str = putstr(str, &format!(" {}", clip_tzn(tzn)));
            } else {
                str = EncodeTimezone(str, tz, style);
            }
        }
    } else if style == USE_GERMAN_DATES {
        /* German variant on European style */
        str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
        *str = b'.' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_mon as uint32, 2);
        *str = b'.' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, year_for_output((*tm).tm_year), 4);
        *str = b' ' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_hour as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_min as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = AppendTimestampSeconds(str, tm, fsec);

        if print_tz {
            if !tzn.is_null() {
                str = putstr(str, &format!(" {}", clip_tzn(tzn)));
            } else {
                str = EncodeTimezone(str, tz, style);
            }
        }
    } else {
        /* Backward-compatible with traditional Postgres abstime dates */
        day = date2j((*tm).tm_year, (*tm).tm_mon, (*tm).tm_mday);
        (*tm).tm_wday = j2day(day);
        memcpy(
            str as *mut c_void,
            days[(*tm).tm_wday as usize] as *const c_void,
            3,
        );
        str = str.offset(3);
        *str = b' ' as c_char;
        str = str.offset(1);
        if DateOrder == DATEORDER_DMY {
            str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
            *str = b' ' as c_char;
            str = str.offset(1);
            memcpy(
                str as *mut c_void,
                months[((*tm).tm_mon - 1) as usize] as *const c_void,
                3,
            );
            str = str.offset(3);
        } else {
            memcpy(
                str as *mut c_void,
                months[((*tm).tm_mon - 1) as usize] as *const c_void,
                3,
            );
            str = str.offset(3);
            *str = b' ' as c_char;
            str = str.offset(1);
            str = pg_ultostr_zeropad(str, (*tm).tm_mday as uint32, 2);
        }
        *str = b' ' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_hour as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, (*tm).tm_min as uint32, 2);
        *str = b':' as c_char;
        str = str.offset(1);
        str = AppendTimestampSeconds(str, tm, fsec);
        *str = b' ' as c_char;
        str = str.offset(1);
        str = pg_ultostr_zeropad(str, year_for_output((*tm).tm_year), 4);

        if print_tz {
            if !tzn.is_null() {
                str = putstr(str, &format!(" {}", clip_tzn(tzn)));
            } else {
                /*
                 * We have a time zone, but no string version.  Use the numeric
                 * form, but include a leading space.
                 */
                *str = b' ' as c_char;
                str = str.offset(1);
                str = EncodeTimezone(str, tz, style);
            }
        }
    }

    if (*tm).tm_year <= 0 {
        memcpy(
            str as *mut c_void,
            b" BC".as_ptr() as *const c_void,
            3,
        ); /* Don't copy NUL */
        str = str.offset(3);
    }
    *str = 0;
}

/* clip a timezone name to MAXTZLEN chars, as C's "%.*s" with MAXTZLEN does */
unsafe fn clip_tzn(tzn: *const c_char) -> std::string::String {
    let full = cstr(tzn);
    let max = MAXTZLEN as usize;
    if full.len() > max {
        full[..max].to_string()
    } else {
        full
    }
}

/*
 * Helper functions to avoid duplicated code in EncodeInterval.
 */

/* Append an ISO-8601-style interval field, but only if value isn't zero */
unsafe fn AddISO8601IntPart(cp: *mut c_char, value: int64, units: c_char) -> *mut c_char {
    if value == 0 {
        return cp;
    }
    putstr(cp, &format!("{}{}", value, units as u8 as char))
}

/* Append a postgres-style interval field, but only if value isn't zero */
unsafe fn AddPostgresIntPart(
    cp: *mut c_char,
    value: int64,
    units: *const c_char,
    is_zero: *mut bool,
    is_before: *mut bool,
) -> *mut c_char {
    if value == 0 {
        return cp;
    }
    let cp = putstr(
        cp,
        &format!(
            "{}{}{} {}{}",
            if !*is_zero { " " } else { "" },
            if *is_before && value > 0 { "+" } else { "" },
            value,
            cstr(units),
            if value != 1 { "s" } else { "" },
        ),
    );

    /*
     * Each nonzero field sets is_before for (only) the next one.
     */
    *is_before = value < 0;
    *is_zero = false;
    cp
}

/* Append a verbose-style interval field, but only if value isn't zero */
unsafe fn AddVerboseIntPart(
    cp: *mut c_char,
    mut value: int64,
    units: *const c_char,
    is_zero: *mut bool,
    is_before: *mut bool,
) -> *mut c_char {
    if value == 0 {
        return cp;
    }
    /* first nonzero value sets is_before */
    if *is_zero {
        *is_before = value < 0;
        value = i64abs(value);
    } else if *is_before {
        value = -value;
    }
    let cp = putstr(
        cp,
        &format!(
            " {} {}{}",
            value,
            cstr(units),
            if value == 1 { "" } else { "s" }
        ),
    );
    *is_zero = false;
    cp
}

/* EncodeInterval()
 * Interpret time structure as a delta time and convert to string.
 */
pub unsafe fn EncodeInterval(itm: *mut pg_itm, style: c_int, str: *mut c_char) {
    let mut cp: *mut c_char = str;
    let mut year: c_int = (*itm).tm_year;
    let mut mon: c_int = (*itm).tm_mon;
    let mut mday: int64 = (*itm).tm_mday as int64; /* tm_mday could be INT_MIN */
    let mut hour: int64 = (*itm).tm_hour;
    let mut min: c_int = (*itm).tm_min;
    let mut sec: c_int = (*itm).tm_sec;
    let mut fsec: c_int = (*itm).tm_usec;
    let mut is_before = false;
    let mut is_zero = true;

    if style == INTSTYLE_SQL_STANDARD {
        /* SQL Standard interval format */
        let has_negative =
            year < 0 || mon < 0 || mday < 0 || hour < 0 || min < 0 || sec < 0 || fsec < 0;
        let has_positive =
            year > 0 || mon > 0 || mday > 0 || hour > 0 || min > 0 || sec > 0 || fsec > 0;
        let has_year_month = year != 0 || mon != 0;
        let has_day_time = mday != 0 || hour != 0 || min != 0 || sec != 0 || fsec != 0;
        let has_day = mday != 0;
        let sql_standard_value =
            !(has_negative && has_positive) && !(has_year_month && has_day_time);

        /*
         * SQL Standard wants only 1 "<sign>" preceding the whole interval ...
         * but can't do that if mixed signs.
         */
        if has_negative && sql_standard_value {
            *cp = b'-' as c_char;
            cp = cp.offset(1);
            year = -year;
            mon = -mon;
            mday = -mday;
            hour = -hour;
            min = -min;
            sec = -sec;
            fsec = -fsec;
        }

        if !has_negative && !has_positive {
            cp = putstr(cp, "0");
        } else if !sql_standard_value {
            /*
             * For non sql-standard interval values, force outputting the signs.
             */
            let year_sign = if year < 0 || mon < 0 { '-' } else { '+' };
            let day_sign = if mday < 0 { '-' } else { '+' };
            let sec_sign = if hour < 0 || min < 0 || sec < 0 || fsec < 0 {
                '-'
            } else {
                '+'
            };

            cp = putstr(
                cp,
                &format!(
                    "{}{}-{} {}{} {}{}:{:02}:",
                    year_sign,
                    abs_i32(year),
                    abs_i32(mon),
                    day_sign,
                    i64abs(mday),
                    sec_sign,
                    i64abs(hour),
                    abs_i32(min),
                ),
            );
            cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            *cp = 0;
        } else if has_year_month {
            cp = putstr(cp, &format!("{}-{}", year, mon));
        } else if has_day {
            cp = putstr(cp, &format!("{} {}:{:02}:", mday, hour, min));
            cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            *cp = 0;
        } else {
            cp = putstr(cp, &format!("{}:{:02}:", hour, min));
            cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            *cp = 0;
        }
    } else if style == INTSTYLE_ISO_8601 {
        /* ISO 8601 "time-intervals by duration only" */
        /* special-case zero to avoid printing nothing */
        if year == 0 && mon == 0 && mday == 0 && hour == 0 && min == 0 && sec == 0 && fsec == 0 {
            let end = putstr(cp, "PT0S");
            *end = 0;
            return;
        }
        *cp = b'P' as c_char;
        cp = cp.offset(1);
        cp = AddISO8601IntPart(cp, year as int64, b'Y' as c_char);
        cp = AddISO8601IntPart(cp, mon as int64, b'M' as c_char);
        cp = AddISO8601IntPart(cp, mday, b'D' as c_char);
        if hour != 0 || min != 0 || sec != 0 || fsec != 0 {
            *cp = b'T' as c_char;
            cp = cp.offset(1);
        }
        cp = AddISO8601IntPart(cp, hour, b'H' as c_char);
        cp = AddISO8601IntPart(cp, min as int64, b'M' as c_char);
        if sec != 0 || fsec != 0 {
            if sec < 0 || fsec < 0 {
                *cp = b'-' as c_char;
                cp = cp.offset(1);
            }
            cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
            *cp = b'S' as c_char;
            cp = cp.offset(1);
            *cp = 0;
            cp = cp.offset(1);
        }
    } else if style == INTSTYLE_POSTGRES {
        /* Compatible with postgresql < 8.4 when DateStyle = 'iso' */
        cp = AddPostgresIntPart(
            cp,
            year as int64,
            b"year\0".as_ptr() as *const c_char,
            &mut is_zero,
            &mut is_before,
        );

        /*
         * Ideally we should spell out "month", but for backward compatibility
         * we can't.
         */
        cp = AddPostgresIntPart(
            cp,
            mon as int64,
            b"mon\0".as_ptr() as *const c_char,
            &mut is_zero,
            &mut is_before,
        );
        cp = AddPostgresIntPart(
            cp,
            mday,
            b"day\0".as_ptr() as *const c_char,
            &mut is_zero,
            &mut is_before,
        );
        if is_zero || hour != 0 || min != 0 || sec != 0 || fsec != 0 {
            let minus = hour < 0 || min < 0 || sec < 0 || fsec < 0;

            cp = putstr(
                cp,
                &format!(
                    "{}{}{:02}:{:02}:",
                    if is_zero { "" } else { " " },
                    if minus {
                        "-"
                    } else if is_before {
                        "+"
                    } else {
                        ""
                    },
                    i64abs(hour),
                    abs_i32(min),
                ),
            );
            cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, true);
            *cp = 0;
        }
    } else {
        /* INTSTYLE_POSTGRES_VERBOSE and default */
        strcpy(cp, b"@\0".as_ptr() as *const c_char);
        cp = cp.offset(1);
        cp = AddVerboseIntPart(
            cp,
            year as int64,
            b"year\0".as_ptr() as *const c_char,
            &mut is_zero,
            &mut is_before,
        );
        cp = AddVerboseIntPart(
            cp,
            mon as int64,
            b"mon\0".as_ptr() as *const c_char,
            &mut is_zero,
            &mut is_before,
        );
        cp = AddVerboseIntPart(
            cp,
            mday,
            b"day\0".as_ptr() as *const c_char,
            &mut is_zero,
            &mut is_before,
        );
        cp = AddVerboseIntPart(
            cp,
            hour,
            b"hour\0".as_ptr() as *const c_char,
            &mut is_zero,
            &mut is_before,
        );
        cp = AddVerboseIntPart(
            cp,
            min as int64,
            b"min\0".as_ptr() as *const c_char,
            &mut is_zero,
            &mut is_before,
        );
        if sec != 0 || fsec != 0 {
            *cp = b' ' as c_char;
            cp = cp.offset(1);
            if sec < 0 || (sec == 0 && fsec < 0) {
                if is_zero {
                    is_before = true;
                } else if !is_before {
                    *cp = b'-' as c_char;
                    cp = cp.offset(1);
                }
            } else if is_before {
                *cp = b'-' as c_char;
                cp = cp.offset(1);
            }
            cp = AppendSeconds(cp, sec, fsec, MAX_INTERVAL_PRECISION, false);
            /* We output "ago", not negatives, so use abs(). */
            cp = putstr(
                cp,
                &format!(
                    " sec{}",
                    if abs_i32(sec) != 1 || fsec != 0 { "s" } else { "" }
                ),
            );
            is_zero = false;
        }
        /* identically zero? then put in a unitless zero... */
        if is_zero {
            strcat(cp, b" 0\0".as_ptr() as *const c_char);
        }
        if is_before {
            strcat(cp, b" ago\0".as_ptr() as *const c_char);
        }
    }
}

/*
 * We've been burnt by stupid errors in the ordering of the datetkn tables
 * once too often.  Arrange to check them during postmaster start.
 */
unsafe fn CheckDateTokenTable(tablename: *const c_char, base: *const datetkn, nel: c_int) -> bool {
    let mut ok = true;
    let mut i: c_int;

    i = 0;
    while i < nel {
        let bi = base.offset(i as isize);
        /* check for token strings that don't fit */
        if strlen((*bi).token.as_ptr()) > TOKMAXLEN {
            elog!(
                LOG,
                "token too long in {} table: \"{}\"",
                cstr(tablename),
                cstr((*bi).token.as_ptr())
            );
            ok = false;
            break; /* don't risk applying strcmp */
        }
        /* check for out of order */
        if i > 0 && strcmp((*base.offset((i - 1) as isize)).token.as_ptr(), (*bi).token.as_ptr()) >= 0
        {
            elog!(
                LOG,
                "ordering error in {} table: \"{}\" >= \"{}\"",
                cstr(tablename),
                cstr((*base.offset((i - 1) as isize)).token.as_ptr()),
                cstr((*bi).token.as_ptr())
            );
            ok = false;
        }
        i += 1;
    }
    ok
}

pub unsafe fn CheckDateTokenTables() -> bool {
    let mut ok = true;

    Assert!(UNIX_EPOCH_JDATE == date2j(1970, 1, 1));
    Assert!(crate::utils::adt::date::POSTGRES_EPOCH_JDATE == date2j(2000, 1, 1));

    ok &= CheckDateTokenTable(
        b"datetktbl\0".as_ptr() as *const c_char,
        datetktbl.as_ptr(),
        szdatetktbl,
    );
    ok &= CheckDateTokenTable(
        b"deltatktbl\0".as_ptr() as *const c_char,
        deltatktbl.as_ptr(),
        szdeltatktbl,
    );
    ok
}

/*
 * Common code for temporal prosupport functions: simplify, if possible,
 * a call to a temporal type's length-coercion function.
 *
 * The node-graph manipulation (castNode/IsA/exprTypmod/relabel_to_typmod/
 * list_length/lsecond/linitial) lives in the nodes subsystem, not yet ported,
 * so this is carried as a stub.
 */
pub unsafe fn TemporalSimplify(_max_precis: int32, _node: *mut Node) -> *mut Node {
    // TODO(pg-port): real symbol depends on crate::nodes::nodeFuncs
    // (castNode/IsA/exprTypmod/relabel_to_typmod) which are not yet ported.
    unimplemented!("TemporalSimplify: crate::nodes::nodeFuncs / primnodes")
}

/*
 * This function gets called during timezone config file load or reload
 * to create the final array of timezone tokens.
 */
pub unsafe fn ConvertTimeZoneAbbrevs(
    abbrevs: *mut tzEntry,
    n: c_int,
) -> *mut TimeZoneAbbrevTable {
    let tbl: *mut TimeZoneAbbrevTable;
    let mut tbl_size: Size;
    let mut i: c_int;

    /* Space for fixed fields and datetkn array */
    tbl_size = offsetof_abbrevs() + n as Size * std::mem::size_of::<datetkn>() as Size;
    tbl_size = MAXALIGN(tbl_size as usize) as Size;
    /* Count up space for dynamic abbreviations */
    i = 0;
    while i < n {
        let abbr = abbrevs.offset(i as isize);

        if !(*abbr).zone.is_null() {
            let dsize: Size = offsetof_dynzone()
                + strlen((*abbr).zone) as Size
                + 1;
            tbl_size += MAXALIGN(dsize as usize) as Size;
        }
        i += 1;
    }

    /* Alloc the result ... */
    tbl = guc_malloc(LOG, tbl_size) as *mut TimeZoneAbbrevTable;
    if tbl.is_null() {
        return std::ptr::null_mut();
    }

    /* ... and fill it in */
    (*tbl).tblsize = tbl_size;
    (*tbl).numabbrevs = n;
    /* in this loop, tbl_size reprises the space calculation above */
    tbl_size = offsetof_abbrevs() + n as Size * std::mem::size_of::<datetkn>() as Size;
    tbl_size = MAXALIGN(tbl_size as usize) as Size;
    let abbrevs_base = (*tbl).abbrevs.as_mut_ptr();
    i = 0;
    while i < n {
        let abbr = abbrevs.offset(i as isize);
        let dtoken = abbrevs_base.offset(i as isize);

        /* use strlcpy to truncate name if necessary */
        strlcpy((*dtoken).token.as_mut_ptr(), (*abbr).abbrev, TOKMAXLEN + 1);
        if !(*abbr).zone.is_null() {
            /* Allocate a DynamicZoneAbbrev for this abbreviation */
            let dtza: *mut DynamicZoneAbbrev =
                (tbl as *mut c_char).offset(tbl_size as isize) as *mut DynamicZoneAbbrev;

            (*dtza).tz = std::ptr::null_mut();
            strcpy((*dtza).zone.as_mut_ptr(), (*abbr).zone);

            (*dtoken).r#type = DYNTZ as c_char;
            /* value is offset from table start to DynamicZoneAbbrev */
            (*dtoken).value = tbl_size as int32;

            let dsize: Size = offsetof_dynzone() + strlen((*abbr).zone) as Size + 1;
            tbl_size += MAXALIGN(dsize as usize) as Size;
        } else {
            (*dtoken).r#type = if (*abbr).is_dst { DTZ } else { TZ } as c_char;
            (*dtoken).value = (*abbr).offset;
        }
        i += 1;
    }

    /* Assert the two loops above agreed on size calculations */
    Assert!((*tbl).tblsize == tbl_size);

    /* Check the ordering, if testing */
    Assert!(CheckDateTokenTable(
        b"timezone abbreviations\0".as_ptr() as *const c_char,
        (*tbl).abbrevs.as_ptr(),
        n
    ));

    tbl
}

/* offsetof(TimeZoneAbbrevTable, abbrevs) */
#[inline]
fn offsetof_abbrevs() -> Size {
    std::mem::offset_of!(TimeZoneAbbrevTable, abbrevs) as Size
}
/* offsetof(DynamicZoneAbbrev, zone) */
#[inline]
fn offsetof_dynzone() -> Size {
    std::mem::offset_of!(DynamicZoneAbbrev, zone) as Size
}

/*
 * Install a TimeZoneAbbrevTable as the active table.
 */
pub unsafe fn InstallTimeZoneAbbrevs(tbl: *mut TimeZoneAbbrevTable) {
    zoneabbrevtbl = tbl;
    /* reset tzabbrevcache, which may contain results from old table */
    memset(
        tzabbrevcache.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of_val(&tzabbrevcache),
    );
}

/*
 * Helper subroutine to locate pg_tz timezone for a dynamic abbreviation.
 */
unsafe fn FetchDynamicTimeZone(
    tbl: *mut TimeZoneAbbrevTable,
    tp: *const datetkn,
    extra: *mut DateTimeErrorExtra,
) -> *mut pg_tz {
    let dtza: *mut DynamicZoneAbbrev;

    /* Just some sanity checks to prevent indexing off into nowhere */
    Assert!((*tp).r#type as c_int == DYNTZ);
    Assert!((*tp).value > 0 && ((*tp).value as Size) < (*tbl).tblsize);

    dtza = (tbl as *mut c_char).offset((*tp).value as isize) as *mut DynamicZoneAbbrev;

    /* Look up the underlying zone if we haven't already */
    if (*dtza).tz.is_null() {
        (*dtza).tz = pg_tzset((*dtza).zone.as_ptr());
        if (*dtza).tz.is_null() {
            /* Ooops, bogus zone name in config file entry */
            (*extra).dtee_timezone = (*dtza).zone.as_ptr();
            (*extra).dtee_abbrev = (*tp).token.as_ptr();
        }
    }
    (*dtza).tz
}

/*
 * This set-returning function reads all the time zone abbreviations
 * defined by the IANA data for the current timezone setting,
 * and returns a set of (abbrev, utc_offset, is_dst).
 *
 * The set-returning machinery (FuncCallContext/SRF_*, get_call_result_type,
 * heap_form_tuple, HeapTupleGetDatum) and itmin2interval are not yet ported,
 * so the body is carried as a stub.
 */
pub unsafe fn pg_timezone_abbrevs_zone(_fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): real symbol depends on funcapi (SRF) + heap + crate::utils::adt::timestamp
    unimplemented!("pg_timezone_abbrevs_zone: funcapi / access::heaptuple / timestamp")
}

/*
 * This set-returning function reads all the time zone abbreviations
 * defined by the timezone_abbreviations setting,
 * and returns a set of (abbrev, utc_offset, is_dst).
 */
pub unsafe fn pg_timezone_abbrevs_abbrevs(_fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): real symbol depends on funcapi (SRF) + heap + crate::utils::adt::timestamp
    unimplemented!("pg_timezone_abbrevs_abbrevs: funcapi / access::heaptuple / timestamp")
}

/*
 * This set-returning function reads all the available full time zones
 * and returns a set of (name, abbrev, utc_offset, is_dst).
 */
pub unsafe fn pg_timezone_names(_fcinfo: FunctionCallInfo) -> Datum {
    // TODO(pg-port): real symbol depends on funcapi (Materialized SRF) +
    // tuplestore + crate::utils::adt::timestamp (timestamp2tm/itmin2interval)
    unimplemented!("pg_timezone_names: funcapi / tuplestore / timestamp")
}

