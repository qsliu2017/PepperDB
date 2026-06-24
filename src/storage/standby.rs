//! Translated from PostgreSQL src/include/storage/standby.h
//! Definitions for hot standby mode.

use crate::access::transam::FullTransactionId;
use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
use crate::datatype::timestamp::TimestampTz;
use crate::postgres_ext::Oid;
use crate::storage::lock::{VirtualTransactionId, LOCKTAG};
use crate::storage::procsignal::ProcSignalReason;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::sinval::SharedInvalidationMessage;
use crate::storage::standbydefs::xl_running_xacts;

// User-settable GUC parameters.
pub static mut max_standby_archive_delay: i32 = 0;
pub static mut max_standby_streaming_delay: i32 = 0;
pub static mut log_recovery_conflict_waits: bool = false;

pub fn init_recovery_transaction_environment() {
    unimplemented!()
}

pub fn shutdown_recovery_transaction_environment() {
    unimplemented!()
}

pub fn resolve_recovery_conflict_with_snapshot(
    _snapshot_conflict_horizon: TransactionId,
    _is_catalog_rel: bool,
    _locator: RelFileLocator,
) {
    unimplemented!()
}

pub fn resolve_recovery_conflict_with_snapshot_full_xid(
    _snapshot_conflict_horizon: FullTransactionId,
    _is_catalog_rel: bool,
    _locator: RelFileLocator,
) {
    unimplemented!()
}

pub fn resolve_recovery_conflict_with_tablespace(_tsid: Oid) {
    unimplemented!()
}

pub fn resolve_recovery_conflict_with_database(_dbid: Oid) {
    unimplemented!()
}

pub fn resolve_recovery_conflict_with_lock(_locktag: LOCKTAG, _logging_conflict: bool) {
    unimplemented!()
}

pub fn resolve_recovery_conflict_with_buffer_pin() {
    unimplemented!()
}

pub fn check_recovery_conflict_deadlock() {
    unimplemented!()
}

pub fn standby_dead_lock_handler() {
    unimplemented!()
}

pub fn standby_timeout_handler() {
    unimplemented!()
}

pub fn standby_lock_timeout_handler() {
    unimplemented!()
}

pub fn log_recovery_conflict(
    _reason: ProcSignalReason,
    _wait_start: TimestampTz,
    _now: TimestampTz,
    _wait_list: &[VirtualTransactionId],
    _still_waiting: bool,
) {
    unimplemented!()
}

// Standby Rmgr (RM_STANDBY_ID).

pub fn standby_acquire_access_exclusive_lock(_xid: TransactionId, _db_oid: Oid, _rel_oid: Oid) {
    unimplemented!()
}

pub fn standby_release_lock_tree(_xid: TransactionId, _subxids: &[TransactionId]) {
    unimplemented!()
}

pub fn standby_release_all_locks() {
    unimplemented!()
}

pub fn standby_release_old_locks(_oldxid: TransactionId) {
    unimplemented!()
}

/// `MinSizeOfXactRunningXacts` - offset of the `xids` flexible array in
/// xl_running_xacts. The FAM is elided in the Rust struct, so this equals the
/// size of the fixed part.
pub const fn min_size_of_xact_running_xacts() -> usize {
    core::mem::size_of::<xl_running_xacts>()
}

/// `subxids_array_status` - how a running-xacts snapshot represents subxids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubxidsArrayStatus {
    /// xids array includes all running subxids.
    InArray,
    /// Snapshot overflowed, subxids are missing.
    Missing,
    /// Subxids not in 'xids', but pg_subtrans is fully up-to-date.
    InSubtrans,
}

/// `RunningTransactionsData` - running-xact data for building a standby's initial
/// snapshot. The C `xids` flexible array becomes a `Vec` (xcnt+subxcnt entries).
pub struct RunningTransactionsData {
    pub xcnt: i32,
    pub subxcnt: i32,
    pub subxid_status: SubxidsArrayStatus,
    /// xid from TransamVariables->nextXid.
    pub next_xid: TransactionId,
    /// *not* oldestXmin.
    pub oldest_running_xid: TransactionId,
    /// Same as above, but within the current database.
    pub oldest_database_running_xid: TransactionId,
    /// So we can set xmax.
    pub latest_completed_xid: TransactionId,
    /// Array of (sub)xids still running.
    pub xids: Vec<TransactionId>,
}

pub fn log_access_exclusive_lock(_db_oid: Oid, _rel_oid: Oid) {
    unimplemented!()
}

pub fn log_access_exclusive_lock_prepare() {
    unimplemented!()
}

pub fn log_standby_snapshot() -> XLogRecPtr {
    unimplemented!()
}

pub fn log_standby_invalidations(
    _msgs: &[SharedInvalidationMessage],
    _relcache_init_file_inval: bool,
) {
    unimplemented!()
}
