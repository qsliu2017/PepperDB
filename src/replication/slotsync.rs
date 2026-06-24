//! Translated from PostgreSQL src/include/replication/slotsync.h
//! Exports for slot synchronization.
//!
//! The slot-sync worker API; bodies stubbed. `SlotSyncCtxStruct` is shared-memory
//! state (defined in slotsync.c, not the header) - modelled in-memory per
//! LEVEL2-NOTES (spinlock dropped, single-process).

use std::sync::atomic::AtomicBool;

use crate::replication::walreceiver::WalReceiverConn;

pub static mut sync_replication_slots: bool = false;

/// Interrupt flag set by `HandleSlotSyncMessageInterrupt()`. C: `volatile
/// sig_atomic_t` -> an atomic bool.
pub static SlotSyncShutdownPending: AtomicBool = AtomicBool::new(false);

// GUCs needed by the slot sync worker to connect to the primary server.
pub static mut PrimaryConnInfo: Option<String> = None;
pub static mut PrimarySlotName: Option<String> = None;

/// Shared-memory control state for slot synchronization (C `SlotSyncCtxStruct`,
/// defined in slotsync.c). Single-process: the `slock_t mutex` is dropped.
pub struct SlotSyncCtxStruct {
    /// PID of the slot sync worker, or 0 if not running.
    pub pid: i32,
    pub stopsignaled: bool,
    pub syncing: bool,
    pub last_start_time: crate::pgtime::pg_time_t,
    // slock_t mutex -> dropped (single-process).
}

/// C returns NULL when the dbname can't be derived -> `Option`.
pub fn CheckAndGetDbnameFromConninfo() -> Option<String> {
    unimplemented!()
}
pub fn ValidateSlotSyncParams(_elevel: i32) -> bool {
    unimplemented!()
}

/// C: `pg_noreturn`. Worker entry point.
pub fn ReplSlotSyncWorkerMain(_startup_data: &[u8]) -> ! {
    unimplemented!()
}

pub fn ShutDownSlotSync() {
    unimplemented!()
}
pub fn SlotSyncWorkerCanRestart() -> bool {
    unimplemented!()
}
pub fn IsSyncingReplicationSlots() -> bool {
    unimplemented!()
}
pub fn SlotSyncShmemSize() -> usize {
    unimplemented!()
}
pub fn SlotSyncShmemInit() {
    unimplemented!()
}
pub fn SyncReplicationSlots(_wrconn: &mut WalReceiverConn) {
    unimplemented!()
}
pub fn HandleSlotSyncMessageInterrupt() {
    unimplemented!()
}
pub fn ProcessSlotSyncMessage() {
    unimplemented!()
}
