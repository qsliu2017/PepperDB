//! common/logging.h - Logging framework for frontend programs.

use crate::c::uint64;
use std::ffi::{c_char, c_int};

// va_list for the printf-style variadic prototype. Stubbed locally as an
// opaque pointer; there is no portable Rust va_list in this crate.
// TODO: dedup
pub type va_list = *mut c_void;
use std::ffi::c_void;

/*
 * Log levels are informational only.  They do not affect program flow.
 *
 * enum pg_log_level
 */
pub type pg_log_level = c_int;

/// Not initialized yet (not to be used as an actual message log level).
pub const PG_LOG_NOTSET: pg_log_level = 0;
/// Low level messages that are normally off by default.
pub const PG_LOG_DEBUG: pg_log_level = 1;
/// Any program messages that go to stderr, shown by default.
pub const PG_LOG_INFO: pg_log_level = 2;
/// Warnings and "almost" errors, depends on the program.
pub const PG_LOG_WARNING: pg_log_level = 3;
/// Errors.
pub const PG_LOG_ERROR: pg_log_level = 4;
/// Turn all logging off (not to be used as an actual message log level).
pub const PG_LOG_OFF: pg_log_level = 5;

extern "C" {
    /// __pg_log_level is the minimum log level that will actually be shown.
    pub static mut __pg_log_level: pg_log_level;
}

/*
 * A log message can have several parts.  The primary message is required,
 * others are optional.  When emitting multiple parts, do so in the order of
 * this enum, for consistency.
 *
 * enum pg_log_part
 */
pub type pg_log_part = c_int;

/// The primary message.
pub const PG_LOG_PRIMARY: pg_log_part = 0;
/// Additional detail.
pub const PG_LOG_DETAIL: pg_log_part = 1;
/// Hint (not guaranteed correct) about how to fix the problem.
pub const PG_LOG_HINT: pg_log_part = 2;

/*
 * Kind of a hack to be able to produce the psql output exactly as required by
 * the regression tests.
 */
pub const PG_LOG_FLAG_TERSE: c_int = 1;

pub unsafe fn pg_logging_init(argv0: *const c_char) {
    unimplemented!()
}

pub unsafe fn pg_logging_config(new_flags: c_int) {
    unimplemented!()
}

pub unsafe fn pg_logging_set_level(new_level: pg_log_level) {
    unimplemented!()
}

pub unsafe fn pg_logging_increase_verbosity() {
    unimplemented!()
}

pub unsafe fn pg_logging_set_pre_callback(cb: Option<unsafe extern "C" fn()>) {
    unimplemented!()
}

pub unsafe fn pg_logging_set_locus_callback(
    cb: Option<unsafe extern "C" fn(filename: *mut *const c_char, lineno: *mut uint64)>,
) {
    unimplemented!()
}

pub unsafe fn pg_log_generic(
    level: pg_log_level,
    part: pg_log_part,
    fmt: *const c_char,
    // ...  (variadic, pg_attribute_printf(3, 4))
) {
    unimplemented!()
}

pub unsafe fn pg_log_generic_v(
    level: pg_log_level,
    part: pg_log_part,
    fmt: *const c_char,
    ap: va_list,
) {
    unimplemented!()
}

/*
 * Preferred style is to use these macros to perform logging.  The original
 * macros are variadic printf-style wrappers around pg_log_generic; since Rust
 * has no direct equivalent of C variadic forwarding, these are provided as
 * thin convenience wrappers taking the already-formatted message pointer.
 */

#[inline]
pub unsafe fn pg_log_error(fmt: *const c_char) {
    pg_log_generic(PG_LOG_ERROR, PG_LOG_PRIMARY, fmt)
}

#[inline]
pub unsafe fn pg_log_error_detail(fmt: *const c_char) {
    pg_log_generic(PG_LOG_ERROR, PG_LOG_DETAIL, fmt)
}

#[inline]
pub unsafe fn pg_log_error_hint(fmt: *const c_char) {
    pg_log_generic(PG_LOG_ERROR, PG_LOG_HINT, fmt)
}

#[inline]
pub unsafe fn pg_log_warning(fmt: *const c_char) {
    pg_log_generic(PG_LOG_WARNING, PG_LOG_PRIMARY, fmt)
}

#[inline]
pub unsafe fn pg_log_warning_detail(fmt: *const c_char) {
    pg_log_generic(PG_LOG_WARNING, PG_LOG_DETAIL, fmt)
}

#[inline]
pub unsafe fn pg_log_warning_hint(fmt: *const c_char) {
    pg_log_generic(PG_LOG_WARNING, PG_LOG_HINT, fmt)
}

#[inline]
pub unsafe fn pg_log_info(fmt: *const c_char) {
    pg_log_generic(PG_LOG_INFO, PG_LOG_PRIMARY, fmt)
}

#[inline]
pub unsafe fn pg_log_info_detail(fmt: *const c_char) {
    pg_log_generic(PG_LOG_INFO, PG_LOG_DETAIL, fmt)
}

#[inline]
pub unsafe fn pg_log_info_hint(fmt: *const c_char) {
    pg_log_generic(PG_LOG_INFO, PG_LOG_HINT, fmt)
}

#[inline]
pub unsafe fn pg_log_debug(fmt: *const c_char) {
    if __pg_log_level <= PG_LOG_DEBUG {
        pg_log_generic(PG_LOG_DEBUG, PG_LOG_PRIMARY, fmt);
    }
}

#[inline]
pub unsafe fn pg_log_debug_detail(fmt: *const c_char) {
    if __pg_log_level <= PG_LOG_DEBUG {
        pg_log_generic(PG_LOG_DEBUG, PG_LOG_DETAIL, fmt);
    }
}

#[inline]
pub unsafe fn pg_log_debug_hint(fmt: *const c_char) {
    if __pg_log_level <= PG_LOG_DEBUG {
        pg_log_generic(PG_LOG_DEBUG, PG_LOG_HINT, fmt);
    }
}

/*
 * A common shortcut: pg_log_error() and immediately exit(1).
 */
#[inline]
pub unsafe fn pg_fatal(fmt: *const c_char) -> ! {
    pg_log_generic(PG_LOG_ERROR, PG_LOG_PRIMARY, fmt);
    std::process::exit(1);
}

/*
 * Use these variants for "can't happen" cases, if it seems translating their
 * messages would be a waste of effort.
 */
#[inline]
pub unsafe fn pg_log_error_internal(fmt: *const c_char) {
    pg_log_error(fmt)
}

#[inline]
pub unsafe fn pg_fatal_internal(fmt: *const c_char) -> ! {
    pg_fatal(fmt)
}
