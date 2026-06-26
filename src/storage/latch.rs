//! Translated from PostgreSQL src/include/storage/latch.h
//!
//! Latch (process wakeup) -> tokio::sync::Notify with a sticky `is_set` flag.
//! PG's latch.h fields owner_pid / is_shared / maybe_sleeping exist only to route
//! a cross-process wakeup (self-pipe / SIGURG / Windows event) to the one owner
//! and to skip signaling when nobody sleeps. Under the single-process async model
//! a Latch is just shared heap state; we keep the observable contract -- "a set
//! before wait is not lost" -- with `is_set` + Notify's stored permit. The owner
//! bookkeeping collapses, so those fields are dropped.

use std::sync::atomic::AtomicBool;

use tokio::sync::Notify;

/// A latch: a one-bit wakeup primitive. `SetLatch` makes a current or future
/// `wait()` return; `ResetLatch` clears the bit.
pub struct Latch {
    /// Sticky: a `SetLatch` that lands before `wait()` is observed by `wait()`.
    pub(crate) is_set: AtomicBool,
    /// Stored-permit wakeup: a `notify_one` before `notified().await` is kept.
    pub(crate) notify: Notify,
}

// The latch behavior lives as idiomatic methods on `Latch` (in the backend
// module). The original C-named free functions are kept here as deprecated
// inline shims for cross-reference and mechanical-port compatibility.

#[deprecated(note = "use `latch.init()`")]
#[inline]
pub fn InitLatch(latch: &Latch) {
    latch.init();
}

/// Single-process: identical to a local latch; the shared/owner distinction is
/// gone (callers share it via `Arc`).
#[deprecated(note = "use `latch.init()`")]
#[inline]
pub fn InitSharedLatch(latch: &Latch) {
    latch.init();
}

#[deprecated(note = "use `latch.set()`")]
#[inline]
pub fn SetLatch(latch: &Latch) {
    latch.set();
}

#[deprecated(note = "use `latch.reset()`")]
#[inline]
pub fn ResetLatch(latch: &Latch) {
    latch.reset();
}

// OwnLatch/DisownLatch/ShutdownLatchSupport managed the self-pipe / owner_pid in
// PG's multiprocess design. Single-process has no transport to own.
#[deprecated(note = "single-process: no latch ownership to manage")]
#[inline]
pub fn OwnLatch(_latch: &Latch) {} // single-process: no-op
#[deprecated(note = "single-process: no latch ownership to manage")]
#[inline]
pub fn DisownLatch(_latch: &Latch) {} // single-process: no-op
#[deprecated(note = "single-process: no latch support to shut down")]
#[inline]
pub fn ShutdownLatchSupport() {} // single-process: no-op
