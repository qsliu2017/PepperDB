//! Assert support code.
//!
//! Source: postgres/src/backend/utils/error/assert.c
//! #include "postgres.h"  -> use crate::prelude::*;  (PointerIsValid from c.h)
//! #include <unistd.h>     -> libc getpid()
//! #ifdef HAVE_EXECINFO_H / <execinfo.h> -> libc backtrace/backtrace_symbols_fd
//!
//! The backend Assert() failure handler. We intentionally do NOT route through
//! elog() here, matching the C rationale of minimizing the infrastructure that
//! must be working to report an assertion failure.

use crate::prelude::*;
use std::io::Write;

// <unistd.h>: getpid(). Bound directly; std has no stable getpid().
extern "C" {
    fn getpid() -> c_int;
}

// <execinfo.h>: backtrace support. These are present on glibc and macOS libc.
// Guarded by `unix` so non-unix targets compile without the symbols (the C code
// guards on HAVE_EXECINFO_H / HAVE_BACKTRACE_SYMBOLS configure probes).
#[cfg(unix)]
extern "C" {
    fn backtrace(buffer: *mut *mut c_void, size: c_int) -> c_int;
    fn backtrace_symbols_fd(buffer: *const *mut c_void, size: c_int, fd: c_int);
}

/// ExceptionalCondition - Handles the failure of an Assert()
///
/// Mirrors the C signature
/// `void ExceptionalCondition(const char *conditionName, const char *fileName,
///                            int lineNumber)`,
/// but is `-> !` because it never returns (it always abort()s). Declared
/// `extern "C"` so the Assert!/AssertMacro machinery and any C callers can
/// invoke it through the same ABI.
#[no_mangle]
pub extern "C" fn ExceptionalCondition(
    conditionName: *const c_char,
    fileName: *const c_char,
    lineNumber: c_int,
) -> ! {
    // Report the failure on stderr (or local equivalent).
    //
    // C uses write_stderr(), a low-level fprintf-to-stderr that also mirrors to
    // the Windows event log. We have no port::write_stderr yet, so we write
    // straight to fd 2 via std::io::stderr(), which is the unix equivalent and
    // keeps this self-contained. TODO: route through port::write_stderr once the
    // win32 event-log path is ported.
    let pid = unsafe { getpid() } as c_int;
    let stderr = std::io::stderr();
    let mut h = stderr.lock();

    if !PointerIsValid(conditionName) || !PointerIsValid(fileName) {
        let _ = write!(h, "TRAP: ExceptionalCondition: bad arguments in PID {}\n", pid);
    } else {
        // C uses the raw char* directly; here we reconstruct &CStr borrows.
        // Both pointers are valid per the check above.
        let cond = unsafe { std::ffi::CStr::from_ptr(conditionName) };
        let file = unsafe { std::ffi::CStr::from_ptr(fileName) };
        let _ = write!(
            h,
            "TRAP: failed Assert(\"{}\"), File: \"{}\", Line: {}, PID: {}\n",
            cond.to_string_lossy(),
            file.to_string_lossy(),
            lineNumber,
            pid
        );
    }

    // Usually this shouldn't be needed, but make sure the msg went out.
    let _ = h.flush();
    drop(h);

    // If we have support for it, dump a simple backtrace.
    // C: #ifdef HAVE_BACKTRACE_SYMBOLS { void *buf[100]; ... }
    #[cfg(unix)]
    unsafe {
        const NBUF: usize = 100; // lengthof(buf)
        let mut buf: [*mut c_void; NBUF] = [null_mut(); NBUF];
        let nframes = backtrace(buf.as_mut_ptr(), NBUF as c_int);
        // fileno(stderr) == 2 on unix.
        backtrace_symbols_fd(buf.as_ptr(), nframes, 2);
    }

    // C also has an optional `#ifdef SLEEP_ON_ASSERT { sleep(1000000); }` block
    // to let a developer attach a debugger. That is a non-default build option;
    // omitted here. TODO: add behind a cfg if SLEEP_ON_ASSERT support is wanted.

    std::process::abort();
}

// TEST: ExceptionalCondition cannot be exercised directly (it abort()s the
// process). We only assert, at compile time, that the symbol has the exact
// expected diverging extern "C" signature.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_is_stable() {
        let _f: extern "C" fn(*const c_char, *const c_char, c_int) -> ! = ExceptionalCondition;
    }
}
