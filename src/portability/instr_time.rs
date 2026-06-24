//! Translated from PostgreSQL src/include/portability/instr_time.h

// Portable high-precision interval timing. PG stores ticks as int64 ns from a
// monotonic clock; we back the current-time source with std::time::Instant and
// keep the same int64-ns tick representation so the arithmetic API is exact.

use std::sync::OnceLock;
use std::time::Instant;

pub const NS_PER_S: i64 = 1_000_000_000;
pub const NS_PER_MS: i64 = 1_000_000;
pub const NS_PER_US: i64 = 1_000;

/// Opaque interval/absolute time, in nanosecond ticks.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct InstrTime {
    pub ticks: i64,
}

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

impl InstrTime {
    pub const fn zero() -> Self {
        Self { ticks: 0 }
    }

    /// INSTR_TIME_SET_CURRENT
    pub fn now() -> Self {
        Self { ticks: epoch().elapsed().as_nanos() as i64 }
    }

    /// INSTR_TIME_IS_ZERO
    pub const fn is_zero(self) -> bool {
        self.ticks == 0
    }

    /// INSTR_TIME_SET_ZERO
    pub fn set_zero(&mut self) {
        self.ticks = 0;
    }

    /// INSTR_TIME_SET_CURRENT_LAZY: set to now if zero; returns whether changed.
    pub fn set_current_lazy(&mut self) -> bool {
        if self.is_zero() {
            *self = Self::now();
            true
        } else {
            false
        }
    }

    /// INSTR_TIME_ADD: self += other.
    pub fn add(&mut self, other: Self) {
        self.ticks += other.ticks;
    }

    /// INSTR_TIME_SUBTRACT: self -= other.
    pub fn subtract(&mut self, other: Self) {
        self.ticks -= other.ticks;
    }

    /// INSTR_TIME_ACCUM_DIFF: self += (y - z).
    pub fn accum_diff(&mut self, y: Self, z: Self) {
        self.ticks += y.ticks - z.ticks;
    }

    /// INSTR_TIME_GET_NANOSEC
    pub const fn nanosec(self) -> i64 {
        self.ticks
    }

    /// INSTR_TIME_GET_DOUBLE (seconds)
    pub fn double(self) -> f64 {
        self.ticks as f64 / NS_PER_S as f64
    }

    /// INSTR_TIME_GET_MILLISEC
    pub fn millisec(self) -> f64 {
        self.ticks as f64 / NS_PER_MS as f64
    }

    /// INSTR_TIME_GET_MICROSEC
    pub const fn microsec(self) -> i64 {
        self.ticks / NS_PER_US
    }
}
