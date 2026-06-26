//! Translated from PostgreSQL src/include/common/logging.h
// Frontend logging framework. printf-style varargs collapse to a formatted &str.

/// Log levels (informational only; do not affect program flow).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PgLogLevel {
    /// Not initialized yet (not a usable message level).
    Notset = 0,
    /// Low-level messages, off by default.
    Debug,
    /// Messages to stderr, shown by default.
    Info,
    /// Warnings and "almost" errors.
    Warning,
    /// Errors.
    Error,
    /// Turn all logging off (not a usable message level).
    Off,
}

/// Parts of a log message; emit in this order for consistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgLogPart {
    /// The primary message.
    Primary,
    /// Additional detail.
    Detail,
    /// Hint about how to fix the problem.
    Hint,
}

/// Minimum log level that will actually be shown.
pub static mut PG_LOG_LEVEL: PgLogLevel = PgLogLevel::Notset;

/// Produce psql output exactly as the regression tests require.
pub const PG_LOG_FLAG_TERSE: i32 = 1;

pub fn pg_logging_init(argv0: &str) {
    let _ = argv0;
    unimplemented!()
}

pub fn pg_logging_config(new_flags: i32) {
    let _ = new_flags;
    unimplemented!()
}

pub fn pg_logging_set_level(new_level: PgLogLevel) {
    let _ = new_level;
    unimplemented!()
}

pub fn pg_logging_increase_verbosity() {
    unimplemented!()
}

pub fn pg_logging_set_pre_callback(cb: fn()) {
    let _ = cb;
    unimplemented!()
}

pub fn pg_logging_set_locus_callback(cb: fn() -> (Option<String>, u64)) {
    let _ = cb;
    unimplemented!()
}

/// Emit a fully-formatted message. (printf format+args collapse to `msg`.)
pub fn pg_log_generic(level: PgLogLevel, part: PgLogPart, msg: &str) {
    let _ = (level, part, msg);
    unimplemented!()
}

// The pg_log_error/warning/info/debug + _detail/_hint macros are thin wrappers
// over pg_log_generic; callers pass an already-formatted message.
pub fn pg_log_error(msg: &str) {
    pg_log_generic(PgLogLevel::Error, PgLogPart::Primary, msg);
}
pub fn pg_log_error_detail(msg: &str) {
    pg_log_generic(PgLogLevel::Error, PgLogPart::Detail, msg);
}
pub fn pg_log_error_hint(msg: &str) {
    pg_log_generic(PgLogLevel::Error, PgLogPart::Hint, msg);
}
pub fn pg_log_warning(msg: &str) {
    pg_log_generic(PgLogLevel::Warning, PgLogPart::Primary, msg);
}
pub fn pg_log_warning_detail(msg: &str) {
    pg_log_generic(PgLogLevel::Warning, PgLogPart::Detail, msg);
}
pub fn pg_log_warning_hint(msg: &str) {
    pg_log_generic(PgLogLevel::Warning, PgLogPart::Hint, msg);
}
pub fn pg_log_info(msg: &str) {
    pg_log_generic(PgLogLevel::Info, PgLogPart::Primary, msg);
}
pub fn pg_log_info_detail(msg: &str) {
    pg_log_generic(PgLogLevel::Info, PgLogPart::Detail, msg);
}
pub fn pg_log_info_hint(msg: &str) {
    pg_log_generic(PgLogLevel::Info, PgLogPart::Hint, msg);
}
pub fn pg_log_debug(msg: &str) {
    pg_log_generic(PgLogLevel::Debug, PgLogPart::Primary, msg);
}
pub fn pg_log_debug_detail(msg: &str) {
    pg_log_generic(PgLogLevel::Debug, PgLogPart::Detail, msg);
}
pub fn pg_log_debug_hint(msg: &str) {
    pg_log_generic(PgLogLevel::Debug, PgLogPart::Hint, msg);
}

/// pg_log_error() then exit(1).
pub fn pg_fatal(msg: &str) -> ! {
    pg_log_generic(PgLogLevel::Error, PgLogPart::Primary, msg);
    std::process::exit(1)
}
