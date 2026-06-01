//! pgstrsignal.rs
//!   Identify a Unix signal number
//!
//! On platforms compliant with modern POSIX, this just wraps strsignal(3).
//! Elsewhere, we do the best we can.
//!
//! Translated 1:1 from postgres/src/port/pgstrsignal.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use std::ffi::{c_char, c_int};

extern "C" {
    fn strsignal(sig: c_int) -> *mut c_char;
}

/*
 * pg_strsignal
 *
 * Return a string identifying the given Unix signal number.
 *
 * The result is declared "const char *" because callers should not
 * modify the string.  Note, however, that POSIX does not promise that
 * the string will remain valid across later calls to strsignal().
 *
 * This version guarantees to return a non-NULL pointer, although
 * some platforms' versions of strsignal() reputedly do not.
 *
 * Note that the fallback cases just return constant strings such as
 * "unrecognized signal".  Project style is for callers to print the
 * numeric signal value along with the result of this function, so
 * there's no need to work harder than that.
 */
pub unsafe fn pg_strsignal(signum: c_int) -> *const c_char {
    let mut result: *const c_char;

    /*
     * If we have strsignal(3), use that --- but check its result for NULL.
     */
    #[cfg(unix)]
    {
        result = strsignal(signum) as *const c_char;
        if result.is_null() {
            result = c"unrecognized signal".as_ptr();
        }
    }

    /*
     * We used to have code here to try to use sys_siglist[] if available.
     * However, it seems that all platforms with sys_siglist[] have also had
     * strsignal() for many years now, so that was just a waste of code.
     */
    #[cfg(not(unix))]
    {
        let _ = signum;
        result = c"(signal names not available on this platform)".as_ptr();
    }

    result
}
