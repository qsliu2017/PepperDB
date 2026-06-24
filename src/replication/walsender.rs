//! Translated from PostgreSQL src/include/replication/walsender.h

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};

/// What to do with a snapshot in CREATE_REPLICATION_SLOT. (C enum.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CRSSnapshotAction {
    ExportSnapshot,
    NoexportSnapshot,
    UseSnapshot,
}

// Global state. TODO(global)
pub static mut am_walsender: bool = false;
pub static mut am_cascading_walsender: bool = false;
pub static mut am_db_walsender: bool = false;
pub static mut wake_wal_senders: bool = false;

// User-settable parameters. TODO(global)
pub static mut max_wal_senders: i32 = 0;
pub static mut wal_sender_timeout: i32 = 0;
pub static mut log_replication_commands: bool = false;

pub fn InitWalSender() {
    unimplemented!()
}
pub fn exec_replication_command(cmd_string: &str) -> bool {
    unimplemented!()
}
pub fn WalSndErrorCleanup() {
    unimplemented!()
}
pub fn PhysicalWakeupLogicalWalSnd() {
    unimplemented!()
}
/// C: `XLogRecPtr GetStandbyFlushRecPtr(TimeLineID *tli)` - returns the LSN plus
/// the timeline out-param.
pub fn GetStandbyFlushRecPtr() -> (XLogRecPtr, TimeLineID) {
    unimplemented!()
}
pub fn WalSndSignals() {
    unimplemented!()
}
pub fn WalSndShmemSize() -> usize {
    unimplemented!()
}
pub fn WalSndShmemInit() {
    unimplemented!()
}
pub fn WalSndWakeup(physical: bool, logical: bool) {
    unimplemented!()
}
pub fn WalSndInitStopping() {
    unimplemented!()
}
pub fn WalSndWaitStopping() {
    unimplemented!()
}
pub fn HandleWalSndInitStopping() {
    unimplemented!()
}
pub fn WalSndRqstFileReload() {
    unimplemented!()
}

/// C: `WalSndWakeupRequest()`.
pub fn wal_snd_wakeup_request() {
    unsafe { wake_wal_senders = true }
}

/// C: `WalSndWakeupProcessRequests(physical, logical)`.
pub fn wal_snd_wakeup_process_requests(physical: bool, logical: bool) {
    unsafe {
        if wake_wal_senders {
            wake_wal_senders = false;
            if max_wal_senders > 0 {
                WalSndWakeup(physical, logical);
            }
        }
    }
}
