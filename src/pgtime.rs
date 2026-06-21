//! pgtime.h - PostgreSQL internal timezone library.

use std::ffi::{c_char, c_int, c_long, c_void};

use crate::c::{int64, Size};

/*
 * The API of this library is generally similar to the corresponding
 * C library functions, except that we use pg_time_t which (we hope) is
 * 64 bits wide, and which is most definitely signed not unsigned.
 */

pub type pg_time_t = int64;

/*
 * Data structure representing a broken-down timestamp.
 *
 * CAUTION: the IANA timezone library (src/timezone/) follows the POSIX
 * convention that tm_mon counts from 0 and tm_year is relative to 1900.
 * However, Postgres' datetime functions generally treat tm_mon as counting
 * from 1 and tm_year as relative to 1 BC.  Be sure to make the appropriate
 * adjustments when moving from one code domain to the other.
 */
#[repr(C)]
pub struct pg_tm {
    pub tm_sec: c_int,
    pub tm_min: c_int,
    pub tm_hour: c_int,
    pub tm_mday: c_int,
    pub tm_mon: c_int,  /* see above */
    pub tm_year: c_int, /* see above */
    pub tm_wday: c_int,
    pub tm_yday: c_int,
    pub tm_isdst: c_int,
    pub tm_gmtoff: c_long,
    pub tm_zone: *const c_char,
}

/* These structs are opaque outside the timezone library */
// TODO: dedup - opaque structs defined in the timezone library
pub type pg_tz = c_void;
pub type pg_tzenum = c_void;

/* Maximum length of a timezone name (not including trailing null) */
pub const TZ_STRLEN_MAX: usize = 255;

/* these functions are in localtime.c */

pub unsafe fn pg_localtime(timep: *const pg_time_t, tz: *const pg_tz) -> *mut pg_tm {
    unimplemented!()
}

pub unsafe fn pg_gmtime(timep: *const pg_time_t) -> *mut pg_tm {
    unimplemented!()
}

pub unsafe fn pg_next_dst_boundary(
    timep: *const pg_time_t,
    before_gmtoff: *mut c_long,
    before_isdst: *mut c_int,
    boundary: *mut pg_time_t,
    after_gmtoff: *mut c_long,
    after_isdst: *mut c_int,
    tz: *const pg_tz,
) -> c_int {
    unimplemented!()
}

pub unsafe fn pg_interpret_timezone_abbrev(
    abbrev: *const c_char,
    timep: *const pg_time_t,
    gmtoff: *mut c_long,
    isdst: *mut c_int,
    tz: *const pg_tz,
) -> bool {
    unimplemented!()
}

pub unsafe fn pg_timezone_abbrev_is_known(
    abbrev: *const c_char,
    isfixed: *mut bool,
    gmtoff: *mut c_long,
    isdst: *mut c_int,
    tz: *const pg_tz,
) -> bool {
    unimplemented!()
}

pub unsafe fn pg_get_next_timezone_abbrev(indx: *mut c_int, tz: *const pg_tz) -> *const c_char {
    unimplemented!()
}

pub unsafe fn pg_get_timezone_offset(tz: *const pg_tz, gmtoff: *mut c_long) -> bool {
    unimplemented!()
}

pub unsafe fn pg_get_timezone_name(tz: *mut pg_tz) -> *const c_char {
    unimplemented!()
}

pub unsafe fn pg_tz_acceptable(tz: *mut pg_tz) -> bool {
    unimplemented!()
}

/* these functions are in strftime.c */

pub unsafe fn pg_strftime(
    s: *mut c_char,
    maxsize: Size,
    format: *const c_char,
    t: *const pg_tm,
) -> Size {
    unimplemented!()
}

/* these functions and variables are in pgtz.c */

// Real definitions (export the C symbols too) - the tz subsystem owns these in C (pgtz.c).
#[no_mangle]
pub static mut session_timezone: *mut pg_tz = core::ptr::null_mut();
#[no_mangle]
pub static mut log_timezone: *mut pg_tz = core::ptr::null_mut();

#[no_mangle]
pub unsafe extern "C" fn pg_timezone_initialize() {
    // TODO(pg-port): real tz-database load (src/timezone/pgtz.c not ported). Placeholder so
    // session_timezone/log_timezone are non-null and boot proceeds (real impl: pg_tzset("GMT")).
    static mut GMT_PLACEHOLDER: u8 = 0;
    session_timezone = core::ptr::addr_of_mut!(GMT_PLACEHOLDER) as *mut pg_tz;
    log_timezone = session_timezone;
}

pub unsafe fn pg_tzset(tzname: *const c_char) -> *mut pg_tz {
    unimplemented!()
}

pub unsafe fn pg_tzset_offset(gmtoffset: c_long) -> *mut pg_tz {
    unimplemented!()
}

pub unsafe fn pg_tzenumerate_start() -> *mut pg_tzenum {
    unimplemented!()
}

pub unsafe fn pg_tzenumerate_next(dir: *mut pg_tzenum) -> *mut pg_tz {
    unimplemented!()
}

pub unsafe fn pg_tzenumerate_end(dir: *mut pg_tzenum) {
    unimplemented!()
}
