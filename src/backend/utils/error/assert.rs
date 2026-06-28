//! Assert support code. Translated from backend/utils/error/assert.c.
//!
//! Provides the handler invoked when a runtime `Assert()` fails. The single
//! entry point, `ExceptionalCondition`, reports the failed condition, source
//! location, and process id on stderr and then aborts the process. It
//! deliberately does not route through the error-reporting machinery
//! (`elog`/`ereport`): an assertion failure should require as little working
//! infrastructure as possible to surface, since the very subsystems that
//! reporting relies on may be the ones that are corrupt.
//!
//! PepperDB keeps the stderr-and-abort behavior but omits two optional
//! build-time features of the original: it does not dump a backtrace after the
//! message, and it does not offer the indefinite sleep that lets a developer
//! attach a debugger before the process dies.

/// C: `pg_noreturn extern void ExceptionalCondition(...)`. Handles a failed
/// Assert(): reports on stderr and aborts. Intentionally bypasses elog() to
/// minimize infrastructure needed to report an assertion failure.
pub fn ExceptionalCondition(condition_name: &str, file_name: &str, line_number: i32) -> ! {
    use std::io::Write;
    let pid = std::process::id();
    let mut stderr = std::io::stderr();
    if condition_name.is_empty() || file_name.is_empty() {
        let _ = writeln!(stderr, "TRAP: ExceptionalCondition: bad arguments in PID {pid}");
    } else {
        let _ = writeln!(
            stderr,
            "TRAP: failed Assert(\"{condition_name}\"), File: \"{file_name}\", Line: {line_number}, PID: {pid}"
        );
    }
    let _ = stderr.flush();
    std::process::abort();
}
