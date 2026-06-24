//! Translated from PostgreSQL src/include/replication/syncrep.h
//! Synchronous replication.

use crate::access::xlogdefs::XLogRecPtr;

// SyncRepWaitMode (ordinals, one is -1 sentinel) -> consts.
pub const SYNC_REP_NO_WAIT: i32 = -1;
pub const SYNC_REP_WAIT_WRITE: i32 = 0;
pub const SYNC_REP_WAIT_FLUSH: i32 = 1;
pub const SYNC_REP_WAIT_APPLY: i32 = 2;
pub const NUM_SYNC_REP_WAIT_MODE: usize = 3;

// syncRepState
pub const SYNC_REP_NOT_WAITING: i32 = 0;
pub const SYNC_REP_WAITING: i32 = 1;
pub const SYNC_REP_WAIT_COMPLETE: i32 = 2;

// syncrep_method of SyncRepConfigData
pub const SYNC_REP_PRIORITY: u8 = 0;
pub const SYNC_REP_QUORUM: u8 = 1;

/// One candidate synchronous walsender (copy of WalSnd shared fields). In-memory.
pub struct SyncRepStandbyData {
    pub pid: i32,
    pub write: XLogRecPtr,
    pub flush: XLogRecPtr,
    pub apply: XLogRecPtr,
    pub sync_standby_priority: i32,
    pub walsnd_index: i32,
    pub is_me: bool,
}

/// Configuration of synchronous replication. The C struct is a flat malloc'd
/// blob with a trailing run of NUL-terminated names; here `member_names` is a
/// `Vec<String>` (config_size/nmembers become derived/len).
pub struct SyncRepConfigData {
    pub num_sync: i32,        // number of sync standbys to wait for
    pub syncrep_method: u8,   // method to choose sync standbys
    pub member_names: Vec<String>,
}

// GUC / global state. TODO(global)
pub static mut SyncRepConfig: Option<Box<SyncRepConfigData>> = None;
pub static mut SyncRepStandbyNames: Option<String> = None;

pub fn SyncRepWaitForLSN(lsn: XLogRecPtr, commit: bool) {
    unimplemented!()
}
pub fn SyncRepCleanupAtProcExit() {
    unimplemented!()
}
pub fn SyncRepInitConfig() {
    unimplemented!()
}
pub fn SyncRepReleaseWaiters() {
    unimplemented!()
}

/// C: `int SyncRepGetCandidateStandbys(SyncRepStandbyData **standbys)` - returns
/// the array (count = len) directly.
pub fn SyncRepGetCandidateStandbys() -> Vec<SyncRepStandbyData> {
    unimplemented!()
}

pub fn SyncRepUpdateSyncStandbysDefined() {
    unimplemented!()
}

// The synchronous_standby_names grammar parser (syncrep_gram.y / scanner.l)
// stays opaque; a Rust parser will replace flex/bison later.
pub fn syncrep_scanner_init(s: &str) {
    unimplemented!()
}
pub fn syncrep_scanner_finish() {
    unimplemented!()
}
