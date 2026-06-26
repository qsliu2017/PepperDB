//! Translated from PostgreSQL src/backend/utils/error/assert.c

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
