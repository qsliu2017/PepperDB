//! Translated from PostgreSQL `src/port/pgsleep.c`
//! (declaration in `src/include/port.h`:
//! `extern void pg_usleep(long microsec);`).
//!
//! Portable delay handling.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/port/pgsleep.c

use crate::prelude::*;

/*
 * In a Windows backend, we don't use this implementation, but rather
 * the signal-aware version in src/backend/port/win32/signal.c.
 */
// We translate the `defined(FRONTEND) || !defined(WIN32)` (non-Windows) path.

// struct timespec as defined by POSIX <time.h>. On the platforms we target
// tv_sec is time_t (a 64-bit signed integer) and tv_nsec is `long`.
#[repr(C)]
struct timespec {
    tv_sec: i64, // time_t
    tv_nsec: c_long,
}

// libc nanosleep(2). No std equivalent exposes the EINTR/remaining-time
// semantics we need here, so we bind it directly.
extern "C" {
    fn nanosleep(req: *const timespec, rem: *mut timespec) -> c_int;
}

/*
 * pg_usleep --- delay the specified number of microseconds.
 *
 * NOTE: Although the delay is specified in microseconds, older Unixen and
 * Windows use periodic kernel ticks to wake up, which might increase the delay
 * time significantly.  We've observed delay increases as large as 20
 * milliseconds on supported platforms.
 *
 * On machines where "long" is 32 bits, the maximum delay is ~2000 seconds.
 *
 * CAUTION: It's not a good idea to use long sleeps in the backend.  They will
 * silently return early if a signal is caught, but that doesn't include
 * latches being set on most OSes, and even signal handlers that set MyLatch
 * might happen to run before the sleep begins, allowing the full delay.
 * Better practice is to use WaitLatch() with a timeout, so that backends
 * respond to latches and signals promptly.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_usleep(microsec: c_long) {
    if microsec > 0 {
        // #ifndef WIN32
        let delay = timespec {
            tv_sec: (microsec / 1000000) as i64,
            tv_nsec: (microsec % 1000000) * 1000,
        };
        // (void) nanosleep(&delay, NULL);
        let _ = nanosleep(&delay, core::ptr::null_mut());
        // #else
        //   SleepEx((microsec < 500 ? 1 : (microsec + 500) / 1000), FALSE);
        //   TODO(pg-port): the WIN32 SleepEx branch is not translated.
        // #endif
    }
}
