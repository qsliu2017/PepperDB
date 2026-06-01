//! Translation of postgres/src/include/portability/instr_time.h
//!
//! Macros for measuring time intervals with high resolution.  On Unix we use
//! clock_gettime(CLOCK_MONOTONIC) and store nanoseconds in `instr_time.ticks`
//! (this is the Linux branch of the C header; it is also valid on modern macOS,
//! which provides clock_gettime since 10.12).  The C macros become inline fns;
//! the ones that assign to their argument take it by `&mut`.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::int64;
use core::ffi::c_int;

/// `instr_time` - an opaque interval-timer reading (nanoseconds on Unix).
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct instr_time {
    pub ticks: int64, /* in platform-specific unit (ns here) */
}

pub const NS_PER_S: int64 = 1_000_000_000;
pub const NS_PER_MS: int64 = 1_000_000;
pub const NS_PER_US: int64 = 1_000;

// struct timespec (64-bit Unix: two longs).
#[repr(C)]
struct timespec {
    tv_sec: int64,
    tv_nsec: int64,
}

// CLOCK_MONOTONIC: 1 on Linux, 6 on macOS/Darwin.
#[cfg(target_os = "macos")]
const PG_INSTR_CLOCK: c_int = 6;
#[cfg(not(target_os = "macos"))]
const PG_INSTR_CLOCK: c_int = 1;

extern "C" {
    fn clock_gettime(clk_id: c_int, tp: *mut timespec) -> c_int;
}

/// INSTR_TIME_SET_CURRENT(t): read the monotonic clock into `t`.
#[inline]
pub fn INSTR_TIME_SET_CURRENT(t: &mut instr_time) {
    unsafe {
        let mut tmp = timespec { tv_sec: 0, tv_nsec: 0 };
        clock_gettime(PG_INSTR_CLOCK, &mut tmp);
        t.ticks = tmp.tv_nsec + tmp.tv_sec * NS_PER_S;
    }
}

/// INSTR_TIME_SET_CURRENT_LAZY(t): set only if currently zero; returns whether it set.
#[inline]
pub fn INSTR_TIME_SET_CURRENT_LAZY(t: &mut instr_time) -> bool {
    if INSTR_TIME_IS_ZERO(*t) {
        INSTR_TIME_SET_CURRENT(t);
        true
    } else {
        false
    }
}

#[inline]
pub fn INSTR_TIME_IS_ZERO(t: instr_time) -> bool {
    t.ticks == 0
}

#[inline]
pub fn INSTR_TIME_SET_ZERO(t: &mut instr_time) {
    t.ticks = 0;
}

/// INSTR_TIME_ADD(x, y): x += y.
#[inline]
pub fn INSTR_TIME_ADD(x: &mut instr_time, y: instr_time) {
    x.ticks += y.ticks;
}

/// INSTR_TIME_SUBTRACT(x, y): x -= y.
#[inline]
pub fn INSTR_TIME_SUBTRACT(x: &mut instr_time, y: instr_time) {
    x.ticks -= y.ticks;
}

/// INSTR_TIME_ACCUM_DIFF(x, y, z): x += (y - z).
#[inline]
pub fn INSTR_TIME_ACCUM_DIFF(x: &mut instr_time, y: instr_time, z: instr_time) {
    x.ticks += y.ticks - z.ticks;
}

#[inline]
pub fn INSTR_TIME_GET_NANOSEC(t: instr_time) -> int64 {
    t.ticks
}

#[inline]
pub fn INSTR_TIME_GET_DOUBLE(t: instr_time) -> f64 {
    INSTR_TIME_GET_NANOSEC(t) as f64 / NS_PER_S as f64
}

#[inline]
pub fn INSTR_TIME_GET_MILLISEC(t: instr_time) -> f64 {
    INSTR_TIME_GET_NANOSEC(t) as f64 / NS_PER_MS as f64
}

#[inline]
pub fn INSTR_TIME_GET_MICROSEC(t: instr_time) -> int64 {
    INSTR_TIME_GET_NANOSEC(t) / NS_PER_US
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_and_conversions() {
        let mut a = instr_time::default();
        assert!(INSTR_TIME_IS_ZERO(a));
        a.ticks = 1_500_000_000; // 1.5 s
        assert_eq!(INSTR_TIME_GET_NANOSEC(a), 1_500_000_000);
        assert_eq!(INSTR_TIME_GET_MICROSEC(a), 1_500_000);
        assert_eq!(INSTR_TIME_GET_MILLISEC(a), 1500.0);
        assert_eq!(INSTR_TIME_GET_DOUBLE(a), 1.5);

        let b = instr_time { ticks: 500_000_000 };
        INSTR_TIME_SUBTRACT(&mut a, b);
        assert_eq!(a.ticks, 1_000_000_000);
        let mut acc = instr_time::default();
        INSTR_TIME_ACCUM_DIFF(&mut acc, a, b); // 1e9 - 5e8
        assert_eq!(acc.ticks, 500_000_000);
    }

    #[test]
    fn clock_is_monotonic_nonzero() {
        let mut t = instr_time::default();
        INSTR_TIME_SET_CURRENT(&mut t);
        assert!(t.ticks > 0);
        let mut t2 = instr_time::default();
        INSTR_TIME_SET_CURRENT(&mut t2);
        assert!(t2.ticks >= t.ticks);
    }
}
