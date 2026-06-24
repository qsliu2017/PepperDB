//! Translated from PostgreSQL src/include/storage/waiteventset.h

use bitflags::bitflags;

use crate::postgres::Datum;
use crate::utils::resowner::ResourceOwner;

// storage/latch.h is tombstoned (Latch -> tokio::sync::Notify). The `struct Latch`
// forward decl is modeled as an opaque handle here; the real wakeup primitive
// (tokio::sync::Notify) is wired in when the I/O leaves get async impls.
pub struct Latch {
    _private: (),
}

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

/// Opaque to callers (implementation private to waiteventset.c).
pub struct WaitEventSet {
    _private: (),
}

pub fn InitializeWaitEventSupport() {
    unimplemented!()
}

pub fn CreateWaitEventSet(_resowner: ResourceOwner, _nevents: i32) -> Box<WaitEventSet> {
    unimplemented!()
}

pub fn FreeWaitEventSet(_set: &mut WaitEventSet) {
    unimplemented!()
}

pub fn FreeWaitEventSetAfterFork(_set: &mut WaitEventSet) {
    unimplemented!()
}

pub fn AddWaitEventToSet(
    _set: &mut WaitEventSet,
    _events: u32,
    _fd: pgsocket,
    _latch: Option<&Latch>,
    _user_data: Datum,
) -> i32 {
    unimplemented!()
}

pub fn ModifyWaitEvent(_set: &mut WaitEventSet, _pos: i32, _events: u32, _latch: Option<&Latch>) {
    unimplemented!()
}

pub fn WaitEventSetWait(
    _set: &mut WaitEventSet,
    _timeout: i64,
    _occurred_events: &mut [WaitEvent],
    _nevents: i32,
    _wait_event_info: u32,
) -> i32 {
    unimplemented!()
}

pub fn GetNumRegisteredWaitEvents(_set: &WaitEventSet) -> i32 {
    unimplemented!()
}

pub fn WaitEventSetCanReportClosed() -> bool {
    unimplemented!()
}

pub fn WakeupMyProc() {
    unimplemented!()
}

pub fn WakeupOtherProc(_pid: i32) {
    unimplemented!()
}
