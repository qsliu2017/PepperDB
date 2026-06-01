//! Translation of postgres/src/include/utils/elog.h (interface) - bootstrap subset.
//!
//! PostgreSQL's error reporting is built on `setjmp`/`longjmp` (PG_TRY/PG_CATCH):
//! an `ereport(ERROR, ...)` unwinds to the nearest error-handling context. Rust has
//! no `longjmp`; the faithful analogue is `panic!`/`catch_unwind`, which this shim
//! uses. A full translation of postgres/src/backend/utils/error/elog.c (error
//! stacks, error data, log destinations) is future work.
//!
//! TODO(pg-port): translate elog.c (ErrorData, EmitErrorReport, PG_exception_stack).

use core::ffi::c_int;

// ---- Error severity levels (elog.h) ----
pub const DEBUG5: c_int = 10;
pub const DEBUG4: c_int = 11;
pub const DEBUG3: c_int = 12;
pub const DEBUG2: c_int = 13;
pub const DEBUG1: c_int = 14;
pub const LOG: c_int = 15;
pub const LOG_SERVER_ONLY: c_int = 16;
pub const COMMERROR: c_int = LOG_SERVER_ONLY;
pub const INFO: c_int = 17;
pub const NOTICE: c_int = 18;
pub const WARNING: c_int = 19;
pub const PGWARNING: c_int = WARNING;
pub const WARNING_CLIENT_ONLY: c_int = 20;
pub const ERROR: c_int = 21;
pub const FATAL: c_int = 22;
pub const PANIC: c_int = 23;

/// Backing routine for the `elog!`/`ereport!` macros. Levels `>= ERROR` abort the
/// current operation by panicking (the unwinding analogue of `longjmp`); lower
/// levels write to stderr.
pub fn emit_log(level: c_int, message: &str, file: &str, line: u32) {
    if level >= ERROR {
        panic!("ERROR ({}:{}, level {}): {}", file, line, level, message);
    } else {
        eprintln!("{}:{}: level {}: {}", file, line, level, message);
    }
}

/// `errmsg`/`errdetail`/`errhint`: in C these append fields to the in-flight
/// ErrorData. In this shim they simply produce the formatted string, which the
/// `ereport!` macro forwards to [`emit_log`]. Use as `errmsg!("...", args)`.
#[macro_export]
macro_rules! errmsg {
    ($($arg:tt)*) => { format!($($arg)*) };
}

/// `errcode(code)`: classification code for an error. The shim ignores it.
#[inline]
pub fn errcode(_code: c_int) -> &'static str {
    ""
}

/// `elog!(level, fmt, args...)`: simple logging/error call. The C `elog` uses
/// printf-style `%` formatting; translated call sites use Rust `{}` formatting.
#[macro_export]
macro_rules! elog {
    ($level:expr, $($arg:tt)*) => {
        $crate::utils::elog::emit_log($level, &format!($($arg)*), file!(), line!())
    };
}

/// `ereport!(level, msg)`: rich error report. The shim accepts the level and a
/// single already-formatted message string (typically built with `errmsg!`).
#[macro_export]
macro_rules! ereport {
    ($level:expr, $msg:expr $(,)?) => {
        $crate::utils::elog::emit_log($level, &$msg, file!(), line!())
    };
}
