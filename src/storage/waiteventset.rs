//! Translated from PostgreSQL src/include/storage/waiteventset.h
//!
//! The OS event-multiplexing transport (epoll/kqueue/poll/win32) is deleted;
//! tokio drives readiness, timeouts, latch wakeups, and the postmaster-death
//! signal. This header keeps the public types/consts; the `WaitEventSet`
//! implementation and all functions live in the backend module and are
//! re-exported below.

use bitflags::bitflags;

use crate::postgres::Datum;

// storage/latch.h is no longer tombstoned: the real Latch lives in
// crate::storage::latch. Re-export it so existing waiteventset::Latch users keep
// working.
pub use crate::storage::latch::Latch;

// pgsocket is `int` (port.h, not yet translated); -1 is PGINVALID_SOCKET.
pub type pgsocket = i32;
pub const PGINVALID_SOCKET: pgsocket = -1;

bitflags! {
    /// Events that may wake WaitLatch()/WaitLatchOrSocket()/WaitEventSetWait().
    /// On Linux/macOS WL_SOCKET_CONNECTED == WL_SOCKET_WRITEABLE and
    /// WL_SOCKET_ACCEPT == WL_SOCKET_READABLE (no separate Windows bits).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct WaitEventFlags: u32 {
        const LATCH_SET        = 1 << 0;
        const SOCKET_READABLE  = 1 << 1;
        const SOCKET_WRITEABLE = 1 << 2;
        const TIMEOUT          = 1 << 3; // not for WaitEventSetWait()
        const POSTMASTER_DEATH = 1 << 4;
        const EXIT_ON_PM_DEATH = 1 << 5;
        const SOCKET_CONNECTED = Self::SOCKET_WRITEABLE.bits();
        const SOCKET_CLOSED    = 1 << 7;
        const SOCKET_ACCEPT    = Self::SOCKET_READABLE.bits();
        const SOCKET_MASK = Self::SOCKET_READABLE.bits()
            | Self::SOCKET_WRITEABLE.bits()
            | Self::SOCKET_CONNECTED.bits()
            | Self::SOCKET_ACCEPT.bits()
            | Self::SOCKET_CLOSED.bits();
    }
}

/// One occurred event returned from WaitEventSetWait.
#[derive(Debug, Clone)]
pub struct WaitEvent {
    pub pos: i32,         // position in the event data structure
    pub events: u32,      // triggered events (WaitEventFlags bits)
    pub fd: pgsocket,     // socket fd associated with event
    pub user_data: Datum, // pointer provided in AddWaitEventToSet TODO(ptr)
}

// WaitEventSet is opaque to callers (private to waiteventset.c in PG). The struct
// and PostmasterDeath are defined in the backend module and re-exported here; the
// behavior lives as idiomatic methods on WaitEventSet / Latch.
pub use crate::backend::storage::ipc::waiteventset::{PostmasterDeath, WaitEventSet};

// The original C-named free functions are kept here as deprecated inline shims
// for cross-reference and mechanical-port compatibility.

#[deprecated(note = "use `WaitEventSet::new(nevents)`")]
#[inline]
pub fn CreateWaitEventSet<'a>(nevents: i32) -> WaitEventSet<'a> {
    WaitEventSet::new(nevents)
}

#[deprecated(note = "use `set.add_event(...)`")]
#[inline]
pub fn AddWaitEventToSet<'a>(
    set: &mut WaitEventSet<'a>,
    events: WaitEventFlags,
    fd: pgsocket,
    latch: Option<&'a Latch>,
    pmdeath: Option<PostmasterDeath>,
    user_data: Datum,
) -> i32 {
    set.add_event(events, fd, latch, pmdeath, user_data)
}

#[deprecated(note = "use `set.modify_event(...)`")]
#[inline]
pub fn ModifyWaitEvent(
    set: &mut WaitEventSet,
    pos: i32,
    events: WaitEventFlags,
    latch: Option<&Latch>,
) {
    set.modify_event(pos, events, latch);
}

/// RAII handles teardown; dropping the set frees it.
#[deprecated(note = "drop the `WaitEventSet` (RAII)")]
#[inline]
pub fn FreeWaitEventSet(_set: WaitEventSet<'_>) {} // RAII: dropped on move-in

#[deprecated(note = "use `set.wait(timeout, max_events)`")]
#[inline]
pub async fn WaitEventSetWait(
    set: &WaitEventSet<'_>,
    timeout: i64,
    max_events: usize,
) -> Vec<WaitEvent> {
    set.wait(timeout, max_events).await
}

#[deprecated(note = "use `latch.wait_for(events, PGINVALID_SOCKET, timeout, pmdeath)`")]
#[inline]
pub async fn WaitLatch(
    latch: &Latch,
    wake_events: WaitEventFlags,
    timeout: i64,
    pmdeath: Option<PostmasterDeath>,
) -> WaitEventFlags {
    latch
        .wait_for(wake_events, PGINVALID_SOCKET, timeout, pmdeath)
        .await
}

#[deprecated(note = "use `latch.wait_for(events, sock, timeout, pmdeath)`")]
#[inline]
pub async fn WaitLatchOrSocket(
    latch: &Latch,
    wake_events: WaitEventFlags,
    sock: pgsocket,
    timeout: i64,
    pmdeath: Option<PostmasterDeath>,
) -> WaitEventFlags {
    latch.wait_for(wake_events, sock, timeout, pmdeath).await
}
