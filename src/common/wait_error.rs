//! Translation of postgres/src/common/wait_error.c
//!
//! Convert a wait/waitpid(2) result code to a human-readable string.
//!
//! The .c begins with:
//!     #ifndef FRONTEND
//!     #include "postgres.h"
//!     #else
//!     #include "postgres_fe.h"
//!     #endif
//!     #include <signal.h>
//!     #include <sys/wait.h>
//! We translate the BACKEND path (the `#ifndef FRONTEND` branch); `wait_result_to_str`
//! returns a `pstrdup`'d (palloc'd) string rather than the frontend's `strdup`'d one.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

// ----------------------------------------------------------------
//   extern "C" / TODO(pg-port) shims
// ----------------------------------------------------------------

// We bind two libc routines directly:
//
//  - strsignal(3): pg_strsignal() (src/port/pgstrsignal.c) is a thin wrapper
//    over libc strsignal() that substitutes "unrecognized signal" for a NULL
//    return.  That module is not part of this translation unit, so we inline the
//    wrapper here over the libc symbol.
//
//  - strerror(3): used to expand the "%m" conversion that the C snprintf()
//    performs against the current errno (PostgreSQL's snprintf supports the
//    glibc "%m" extension).
//
// Both return a `const char *` owned by libc.
extern "C" {
    fn strsignal(signum: c_int) -> *const c_char;
    fn strerror(errnum: c_int) -> *const c_char;

    // Per-thread errno location.  macOS/BSD expose it as __error(); glibc uses
    // __errno_location().  Both return `*mut c_int`.
    #[cfg_attr(
        any(target_os = "macos", target_os = "ios", target_vendor = "apple"),
        link_name = "__error"
    )]
    #[cfg_attr(
        not(any(target_os = "macos", target_os = "ios", target_vendor = "apple")),
        link_name = "__errno_location"
    )]
    fn pg_errno_location() -> *mut c_int;
}

/// Minimal `strlen` over a C string (mirrors libc strlen).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
#[inline]
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/// Read the current C `errno` value.
#[inline]
unsafe fn errno() -> c_int {
    *pg_errno_location()
}

/// Render a libc `const char *` (from strerror/strsignal) as an owned Rust
/// String, treating a NULL pointer as an empty string.
///
/// # Safety
/// `s` must be NULL or point to a valid NUL-terminated C string.
#[inline]
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    let len = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    String::from_utf8_lossy(bytes).into_owned()
}

/// Inline of `pg_strsignal` (src/port/pgstrsignal.c) over libc strsignal(3):
/// guarantees a non-NULL result by substituting "unrecognized signal".
///
/// # Safety
/// Calls libc; the returned String owns its bytes.
#[inline]
unsafe fn pg_strsignal(signum: c_int) -> String {
    // We used to have code here to try to use sys_siglist[] if available.
    // However, it seems that all platforms with sys_siglist[] have also had
    // strsignal() for many years now, so that was just a waste of code.
    let result = strsignal(signum);
    if result.is_null() {
        "unrecognized signal".to_string()
    } else {
        cstr_to_string(result)
    }
}

// ----------------------------------------------------------------
//   <sys/wait.h> status-decoding macros
//
//   Translated as inline fns using the standard glibc/BSD bit layout that
//   <bits/waitstatus.h> implements.  A wait status packs the low byte with the
//   termination cause and the next byte with the exit code:
//
//     bits 0..6  : terminating signal number (0 => normal exit)
//     bit  7     : WCOREDUMP flag
//     bits 8..15 : exit code (valid when the signal field is 0)
//
//   WIFEXITED  : (status & 0x7f) == 0
//   WEXITSTATUS: (status & 0xff00) >> 8
//   WIFSIGNALED: ((signed char)((status & 0x7f) + 1) >> 1) > 0
//   WTERMSIG   : status & 0x7f
//   WCOREDUMP  : status & 0x80
// ----------------------------------------------------------------

/// `WTERMSIG(status)` - terminating signal number.
#[inline]
fn WTERMSIG(status: c_int) -> c_int {
    status & 0x7f
}

/// `WEXITSTATUS(status)` - child's exit() code.
#[inline]
fn WEXITSTATUS(status: c_int) -> c_int {
    (status & 0xff00) >> 8
}

/// `WIFEXITED(status)` - true if the child terminated normally via exit().
#[inline]
fn WIFEXITED(status: c_int) -> bool {
    WTERMSIG(status) == 0
}

/// `WIFSIGNALED(status)` - true if the child was terminated by a signal.
///
/// Mirrors glibc's `((signed char) (((status & 0x7f) + 1) >> 1)) > 0`, which is
/// true for any nonzero, non-0x7f signal field (0x7f means WIFSTOPPED).
#[inline]
fn WIFSIGNALED(status: c_int) -> bool {
    // ((status & 0x7f) + 1) ranges over 1..=128; the +1 then arithmetic >>1 of a
    // signed char yields > 0 exactly when the signal field is in 1..=126.
    (((WTERMSIG(status) + 1) as i8) >> 1) as c_int > 0
}

// ----------------------------------------------------------------
//   functions in src/common/wait_error.c
// ----------------------------------------------------------------

/// Width of the C `char str[512]` scratch buffer.
const STR_BUFLEN: usize = 512;

/// Mirror `snprintf(str, sizeof(str), ...)` writing into a fixed `char[512]`:
/// truncate the rendered message to at most `STR_BUFLEN - 1` bytes (reserving
/// room for the NUL), then `pstrdup` it just as the C does.
///
/// # Safety
/// Allocates via palloc; the returned pointer must be pfree'd by the caller.
unsafe fn snprintf_pstrdup(msg: &str) -> *mut c_char {
    let bytes = msg.as_bytes();
    let n = core::cmp::min(bytes.len(), STR_BUFLEN - 1);
    // palloc n + 1 for the trailing NUL and copy the (possibly truncated) text.
    let p = palloc(n + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, p, n);
    *p.add(n) = 0;
    p
}

/*
 * Return a human-readable string explaining the reason a child process
 * terminated. The argument is a return code returned by wait(2) or
 * waitpid(2), which also applies to pclose(3) and system(3). The result is a
 * translated, palloc'd or malloc'd string.
 */
///
/// # Safety
/// Allocates via palloc and reads libc errno/strerror/strsignal; the returned
/// pointer is a palloc'd C string owned by the caller.
pub unsafe fn wait_result_to_str(exitstatus: c_int) -> *mut c_char {
    // C declares `char str[512];` and snprintf()s into it; we render with
    // format! (Rust's stand-in for snprintf) and truncate to the buffer width
    // inside snprintf_pstrdup().  The `_()` gettext wrappers are no-ops here.
    let str: String;

    /*
     * To simplify using this after pclose() and system(), handle status -1
     * first.  In that case, there is no wait result but some error indicated
     * by errno.
     */
    if exitstatus == -1 {
        // snprintf(str, sizeof(str), "%m"): expand the "%m" extension to the
        // current errno's strerror text.
        str = cstr_to_string(strerror(errno()));
    } else if WIFEXITED(exitstatus) {
        /*
         * Give more specific error message for some common exit codes that
         * have a special meaning in shells.
         */
        match WEXITSTATUS(exitstatus) {
            126 => {
                str = "command not executable".to_string();
            }

            127 => {
                str = "command not found".to_string();
            }

            _ => {
                str = format!(
                    "child process exited with exit code {}",
                    WEXITSTATUS(exitstatus)
                );
            }
        }
    } else if WIFSIGNALED(exitstatus) {
        // TODO(pg-port): WIN32 branch reports
        //   "child process was terminated by exception 0x%X" with WTERMSIG().
        // We translate the non-Windows path:
        str = format!(
            "child process was terminated by signal {}: {}",
            WTERMSIG(exitstatus),
            pg_strsignal(WTERMSIG(exitstatus))
        );
    } else {
        str = format!(
            "child process exited with unrecognized status {}",
            exitstatus
        );
    }

    snprintf_pstrdup(&str)
}

/*
 * Return true if a wait(2) result indicates that the child process
 * died due to the specified signal.
 *
 * The reason this is worth having a wrapper function for is that
 * there are two cases: the signal might have been received by our
 * immediate child process, or there might've been a shell process
 * between us and the child that died.  The shell will, per POSIX,
 * report the child death using exit code 128 + signal number.
 *
 * If there is no possibility of an intermediate shell, this function
 * need not (and probably should not) be used.
 */
pub fn wait_result_is_signal(exit_status: c_int, signum: c_int) -> bool {
    if WIFSIGNALED(exit_status) && WTERMSIG(exit_status) == signum {
        return true;
    }
    if WIFEXITED(exit_status) && WEXITSTATUS(exit_status) == 128 + signum {
        return true;
    }
    false
}

/*
 * Return true if a wait(2) result indicates that the child process
 * died due to any signal.  We consider either direct child death
 * or a shell report of child process death as matching the condition.
 *
 * If include_command_not_found is true, also return true for shell
 * exit codes indicating "command not found" and the like
 * (specifically, exit codes 126 and 127; see above).
 */
pub fn wait_result_is_any_signal(exit_status: c_int, include_command_not_found: bool) -> bool {
    if WIFSIGNALED(exit_status) {
        return true;
    }
    if WIFEXITED(exit_status)
        && WEXITSTATUS(exit_status) > (if include_command_not_found { 125 } else { 128 })
    {
        return true;
    }
    false
}

/*
 * Return the shell exit code (normally 0 to 255) that corresponds to the
 * given wait status.  The argument is a wait status as returned by wait(2)
 * or waitpid(2), which also applies to pclose(3) and system(3).  To support
 * the latter two cases, we pass through "-1" unchanged.
 */
pub fn wait_result_to_exit_code(exit_status: c_int) -> c_int {
    if exit_status == -1 {
        return -1; /* failure of pclose() or system() */
    }
    if WIFEXITED(exit_status) {
        return WEXITSTATUS(exit_status);
    }
    if WIFSIGNALED(exit_status) {
        return 128 + WTERMSIG(exit_status);
    }
    /* On many systems, this is unreachable */
    -1
}
