//! Translated from PostgreSQL src/include/pgtime.h
//! PostgreSQL internal timezone library.

/// 64-bit signed seconds-since-epoch (distinct from C library time_t).
pub type pg_time_t = i64;

/// Broken-down timestamp. NB: mon/year conventions differ between the IANA
/// timezone library and Postgres datetime code (see C header comment).
pub struct pg_tm {
    pub sec: i32,
    pub min: i32,
    pub hour: i32,
    pub mday: i32,
    pub mon: i32,
    pub year: i32,
    pub wday: i32,
    pub yday: i32,
    pub isdst: i32,
    pub gmtoff: i64,
    pub zone: Option<String>, // const char *
}

/// Opaque outside the timezone library.
pub struct pg_tz;
/// Opaque timezone enumerator.
pub struct pg_tzenum;

/// Max length of a timezone name (excluding trailing NUL).
pub const TZ_STRLEN_MAX: usize = 255;

// localtime.c
pub fn pg_localtime(_timep: pg_time_t, _tz: &pg_tz) -> pg_tm {
    unimplemented!()
}
pub fn pg_gmtime(_timep: pg_time_t) -> pg_tm {
    unimplemented!()
}

/// Multiple out-params -> named struct (function-mapping 5.3).
pub struct DstBoundary {
    pub before_gmtoff: i64,
    pub before_isdst: i32,
    pub boundary: pg_time_t,
    pub after_gmtoff: i64,
    pub after_isdst: i32,
}
pub fn pg_next_dst_boundary(_timep: pg_time_t, _tz: &pg_tz) -> Option<DstBoundary> {
    unimplemented!()
}

/// bool success + out-params -> Option of a result struct.
pub struct TimezoneAbbrev {
    pub gmtoff: i64,
    pub isdst: i32,
}
pub fn pg_interpret_timezone_abbrev(_abbrev: &str, _timep: pg_time_t, _tz: &pg_tz) -> Option<TimezoneAbbrev> {
    unimplemented!()
}

pub struct KnownAbbrev {
    pub isfixed: bool,
    pub gmtoff: i64,
    pub isdst: i32,
}
pub fn pg_timezone_abbrev_is_known(_abbrev: &str, _tz: &pg_tz) -> Option<KnownAbbrev> {
    unimplemented!()
}

/// Iterator-style: returns the next abbrev and advances `indx`.
pub fn pg_get_next_timezone_abbrev(_indx: &mut i32, _tz: &pg_tz) -> Option<String> {
    unimplemented!()
}

/// bool success + gmtoff out-param -> Option.
pub fn pg_get_timezone_offset(_tz: &pg_tz) -> Option<i64> {
    unimplemented!()
}
pub fn pg_get_timezone_name(_tz: &pg_tz) -> String {
    unimplemented!()
}
pub fn pg_tz_acceptable(_tz: &pg_tz) -> bool {
    unimplemented!()
}

// strftime.c
pub fn pg_strftime(_s: &mut [u8], _maxsize: usize, _format: &str, _t: &pg_tm) -> usize {
    unimplemented!()
}

// pgtz.c - process-global session/log timezones (later: Session-threaded state).
pub static mut session_timezone: Option<Box<pg_tz>> = None;
pub static mut log_timezone: Option<Box<pg_tz>> = None;

pub fn pg_timezone_initialize() {
    unimplemented!()
}
pub fn pg_tzset(_tzname: &str) -> Option<Box<pg_tz>> {
    unimplemented!()
}
pub fn pg_tzset_offset(_gmtoffset: i64) -> Box<pg_tz> {
    unimplemented!()
}
pub fn pg_tzenumerate_start() -> Box<pg_tzenum> {
    unimplemented!()
}
pub fn pg_tzenumerate_next(_dir: &mut pg_tzenum) -> Option<Box<pg_tz>> {
    unimplemented!()
}
pub fn pg_tzenumerate_end(_dir: Box<pg_tzenum>) {
    unimplemented!()
}
