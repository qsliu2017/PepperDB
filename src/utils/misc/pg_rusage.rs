//! Translation of postgres/src/backend/utils/misc/pg_rusage.c
//!
//! Resource usage measurement support routines (used by progress/VACUUM logging).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does:
//!   #include "postgres.h"
//!   #include <unistd.h>
//!   #include "utils/pg_rusage.h"   (MERGED here: PGRUsage struct + prototypes)
//!
//! `postgres.h` -> crate::prelude.  pg_rusage.h's PGRUsage is { struct timeval tv;
//! struct rusage ru; }, so we define `struct timeval` and `struct rusage` here
//! (#[repr(C)]) matching the platform's <sys/time.h>/<sys/resource.h> layouts, and
//! bind getrusage()/gettimeofday() via extern "C" (libc).  The platform structs are
//! cfg-gated the same way portability/instr_time.rs handles `struct timespec`:
//! on macOS `timeval.tv_usec` is `suseconds_t` = i32, on Linux it is `long` = i64.
//!
//! The C uses `snprintf` into a `static char result[100]`; we reproduce that with a
//! `static mut` byte buffer and write the NUL-terminated bytes ourselves (the format
//! "CPU: user: %d.%02d s, system: %d.%02d s, elapsed: %d.%02d s" is plain integers,
//! so no libc snprintf is needed).  Like the C, this is not thread-safe, which is
//! fine for the single-threaded backend.

use crate::prelude::*; // c_char/c_int/c_void, etc.
use core::ffi::{c_char, c_int, c_void};

// ----------------------------------------------------------------
//   Platform structs (<sys/time.h> / <sys/resource.h>)
// ----------------------------------------------------------------

/// `struct timeval` from <sys/time.h>.
///
/// `tv_sec` is `time_t` (i64 on 64-bit Unix).  `tv_usec` is `suseconds_t`, which is
/// `i32` on macOS/Darwin and `long` (i64) on Linux/glibc.  cfg-gate to match.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct timeval {
    pub tv_sec: i64,
    #[cfg(target_os = "macos")]
    pub tv_usec: i32,
    #[cfg(not(target_os = "macos"))]
    pub tv_usec: i64,
}

impl Default for timeval {
    fn default() -> Self {
        timeval { tv_sec: 0, tv_usec: 0 }
    }
}

/// `struct rusage` from <sys/resource.h>.
///
/// pg_rusage only ever touches `ru_utime` and `ru_stime` (both `struct timeval`).
/// The remaining members are a tail of `long` (i64) counters; the exact count
/// differs slightly per platform, but only the leading layout matters here and the
/// total just has to be large enough for the kernel to fill.  Both macOS and Linux
/// place ru_utime/ru_stime first followed by 14 `long` fields, which we model as a
/// fixed array so getrusage() has the right amount of storage to write into.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct rusage {
    pub ru_utime: timeval, /* user time used */
    pub ru_stime: timeval, /* system time used */
    /* the remaining members are platform-specific long counters; we keep an
     * opaque tail large enough to back the kernel's write. */
    ru_rest: [i64; 14],
}

impl Default for rusage {
    fn default() -> Self {
        rusage {
            ru_utime: timeval::default(),
            ru_stime: timeval::default(),
            ru_rest: [0; 14],
        }
    }
}

// RUSAGE_SELF from <sys/resource.h> (0 on both macOS and Linux).
const RUSAGE_SELF: c_int = 0;

extern "C" {
    fn getrusage(who: c_int, usage: *mut rusage) -> c_int;
    fn gettimeofday(tp: *mut timeval, tz: *mut c_void) -> c_int;
}

// ----------------------------------------------------------------
//   PGRUsage (utils/pg_rusage.h, merged)
// ----------------------------------------------------------------

/// State structure for pg_rusage_init/pg_rusage_show.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PGRUsage {
    pub tv: timeval,
    pub ru: rusage,
}

impl Default for PGRUsage {
    fn default() -> Self {
        PGRUsage {
            tv: timeval::default(),
            ru: rusage::default(),
        }
    }
}

/*
 * Initialize usage snapshot.
 *
 * # Safety
 * `ru0` must point to a valid, writable PGRUsage.
 */
pub unsafe fn pg_rusage_init(ru0: *mut PGRUsage) {
    getrusage(RUSAGE_SELF, &mut (*ru0).ru);
    gettimeofday(&mut (*ru0).tv, null_mut());
}

/* C uses `static char result[100]`.  Reproduce with a static mut byte buffer. */
static mut RESULT: [c_char; 100] = [0; 100];

/*
 * Compute elapsed time since ru0 usage snapshot, and format into a displayable
 * string.  Result is in a static string, which is tacky, but no one ever claimed
 * that the Postgres backend is threadable...
 *
 * # Safety
 * `ru0` must point to a valid PGRUsage.  The returned pointer aliases a static
 * buffer that the next call overwrites (matching the C contract).
 */
pub unsafe fn pg_rusage_show(ru0: *const PGRUsage) -> *const c_char {
    let mut ru1 = PGRUsage::default();

    pg_rusage_init(&mut ru1);

    if ru1.tv.tv_usec < (*ru0).tv.tv_usec {
        ru1.tv.tv_sec -= 1;
        ru1.tv.tv_usec += 1000000;
    }
    if ru1.ru.ru_stime.tv_usec < (*ru0).ru.ru_stime.tv_usec {
        ru1.ru.ru_stime.tv_sec -= 1;
        ru1.ru.ru_stime.tv_usec += 1000000;
    }
    if ru1.ru.ru_utime.tv_usec < (*ru0).ru.ru_utime.tv_usec {
        ru1.ru.ru_utime.tv_sec -= 1;
        ru1.ru.ru_utime.tv_usec += 1000000;
    }

    // Match the C casts to (int): the deltas are formatted as %d / %02d.
    let u_sec = (ru1.ru.ru_utime.tv_sec - (*ru0).ru.ru_utime.tv_sec) as c_int;
    let u_cs = ((ru1.ru.ru_utime.tv_usec - (*ru0).ru.ru_utime.tv_usec) as c_int) / 10000;
    let s_sec = (ru1.ru.ru_stime.tv_sec - (*ru0).ru.ru_stime.tv_sec) as c_int;
    let s_cs = ((ru1.ru.ru_stime.tv_usec - (*ru0).ru.ru_stime.tv_usec) as c_int) / 10000;
    let e_sec = (ru1.tv.tv_sec - (*ru0).tv.tv_sec) as c_int;
    let e_cs = ((ru1.tv.tv_usec - (*ru0).tv.tv_usec) as c_int) / 10000;

    // snprintf(result, sizeof(result),
    //   "CPU: user: %d.%02d s, system: %d.%02d s, elapsed: %d.%02d s", ...)
    let s = format!(
        "CPU: user: {}.{:02} s, system: {}.{:02} s, elapsed: {}.{:02} s",
        u_sec, u_cs, s_sec, s_cs, e_sec, e_cs
    );

    // Copy into the static buffer, truncating to fit (leaving room for the NUL),
    // exactly as snprintf into char[100] would.
    let bytes = s.as_bytes();
    let buf = &raw mut RESULT;
    let cap = (*buf).len();
    let n = core::cmp::min(bytes.len(), cap - 1);
    let dst = buf as *mut c_char;
    for i in 0..n {
        *dst.add(i) = bytes[i] as c_char;
    }
    *dst.add(n) = 0;

    dst as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_then_show_starts_with_cpu() {
        unsafe {
            let mut ru0 = PGRUsage::default();
            pg_rusage_init(&mut ru0);

            // Burn a little CPU + wall time so the snapshot is meaningful.
            let mut acc: u64 = 0;
            for i in 0..2_000_000u64 {
                acc = acc.wrapping_add(i);
            }
            assert!(acc > 0); // keep the loop from being optimized away.

            let p = pg_rusage_show(&ru0);
            assert!(!p.is_null());

            // Read back the C string.
            let mut len = 0usize;
            while *p.add(len) != 0 {
                len += 1;
            }
            assert!(len > 0, "result must be non-empty");
            let bytes = core::slice::from_raw_parts(p as *const u8, len);
            let s = core::str::from_utf8(bytes).unwrap();
            assert!(s.starts_with("CPU:"), "got: {:?}", s);
            // Sanity: format has the three labels.
            assert!(s.contains("user:"));
            assert!(s.contains("system:"));
            assert!(s.contains("elapsed:"));
        }
    }
}
