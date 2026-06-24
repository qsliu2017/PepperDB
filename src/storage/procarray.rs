//! Translated from PostgreSQL src/include/storage/procarray.h
//!
//! POSTGRES process array. Under the single-process port the shmem-resident
//! ProcArrayStruct collapses to ordinary Arc-shared state; these are the public
//! ProcArray entry points, stubbed. PGPROC pointers become references;
//! not-found / invalid-xid sentinels become Option; pointer out-params become
//! tuples or owned Vecs.

use crate::c::{Size, TransactionId};
use crate::postgres_ext::Oid;
use crate::storage::lock::VirtualTransactionId;
use crate::storage::proc::PGPROC;
use crate::storage::procnumber::ProcNumber;
use crate::storage::procsignal::ProcSignalReason;
use crate::storage::standby::RunningTransactionsData;
use crate::utils::snapshot::SnapshotData;

pub fn proc_array_shmem_size() -> Size {
    unimplemented!()
}
pub fn proc_array_shmem_init() {
    unimplemented!()
}
pub fn proc_array_add(_proc: &mut PGPROC) {
    unimplemented!()
}
pub fn proc_array_remove(_proc: &mut PGPROC, _latest_xid: TransactionId) {
    unimplemented!()
}

pub fn proc_array_end_transaction(_proc: &mut PGPROC, _latest_xid: TransactionId) {
    unimplemented!()
}
pub fn proc_array_clear_transaction(_proc: &mut PGPROC) {
    unimplemented!()
}

pub fn proc_array_init_recovery(_initialized_upto_xid: TransactionId) {
    unimplemented!()
}
pub fn proc_array_apply_recovery_info(_running: &RunningTransactionsData) {
    unimplemented!()
}
pub fn proc_array_apply_xid_assignment(_topxid: TransactionId, _subxids: &[TransactionId]) {
    unimplemented!()
}

pub fn record_known_assigned_transaction_ids(_xid: TransactionId) {
    unimplemented!()
}
pub fn expire_tree_known_assigned_transaction_ids(
    _xid: TransactionId,
    _subxids: &[TransactionId],
    _max_xid: TransactionId,
) {
    unimplemented!()
}
pub fn expire_all_known_assigned_transaction_ids() {
    unimplemented!()
}
pub fn expire_old_known_assigned_transaction_ids(_xid: TransactionId) {
    unimplemented!()
}
pub fn known_assigned_transaction_ids_idle_maintenance() {
    unimplemented!()
}

pub fn get_max_snapshot_xid_count() -> i32 {
    unimplemented!()
}
pub fn get_max_snapshot_subxid_count() -> i32 {
    unimplemented!()
}

/// GetSnapshotData: fill and return the caller's snapshot.
pub fn get_snapshot_data(_snapshot: &mut SnapshotData) -> &mut SnapshotData {
    unimplemented!()
}

/// Returns true if installed (C bool success).
pub fn proc_array_install_imported_xmin(
    _xmin: TransactionId,
    _sourcevxid: &VirtualTransactionId,
) -> bool {
    unimplemented!()
}
pub fn proc_array_install_restored_xmin(_xmin: TransactionId, _proc: &mut PGPROC) -> bool {
    unimplemented!()
}

pub fn get_running_transaction_data() -> RunningTransactionsData {
    unimplemented!()
}

pub fn transaction_id_is_in_progress(_xid: TransactionId) -> bool {
    unimplemented!()
}
pub fn transaction_id_is_active(_xid: TransactionId) -> bool {
    unimplemented!()
}
pub fn get_oldest_non_removable_transaction_id(_rel: &crate::utils::relcache::RelationData) -> TransactionId {
    unimplemented!()
}
pub fn get_oldest_transaction_id_considered_running() -> TransactionId {
    unimplemented!()
}
pub fn get_oldest_active_transaction_id() -> TransactionId {
    unimplemented!()
}
pub fn get_oldest_safe_decoding_transaction_id(_catalog_only: bool) -> TransactionId {
    unimplemented!()
}

/// Out-params `xmin`/`catalog_xmin` -> a tuple.
pub fn get_replication_horizons() -> (TransactionId, TransactionId) {
    unimplemented!()
}

/// `int *nvxids` count out-param collapses into the returned Vec's length.
pub fn get_virtual_xids_delaying_chkpt(_type_: i32) -> Vec<VirtualTransactionId> {
    unimplemented!()
}
pub fn have_virtual_xids_delaying_chkpt(_vxids: &[VirtualTransactionId], _type_: i32) -> bool {
    unimplemented!()
}

/// Invalid procNumber -> None.
pub fn proc_number_get_proc(_proc_number: ProcNumber) -> Option<&'static mut PGPROC> {
    unimplemented!()
}

/// C out-params (xid, xmin, nsubxid, overflowed) -> a struct.
pub struct ProcNumberXids {
    pub xid: TransactionId,
    pub xmin: TransactionId,
    pub nsubxid: i32,
    pub overflowed: bool,
}
pub fn proc_number_get_transaction_ids(_proc_number: ProcNumber) -> ProcNumberXids {
    unimplemented!()
}

pub fn backend_pid_get_proc(_pid: i32) -> Option<&'static mut PGPROC> {
    unimplemented!()
}
pub fn backend_pid_get_proc_with_lock(_pid: i32) -> Option<&'static mut PGPROC> {
    unimplemented!()
}
/// Returns 0 if not found (C sentinel).
pub fn backend_xid_get_pid(_xid: TransactionId) -> i32 {
    unimplemented!()
}
pub fn is_backend_pid(_pid: i32) -> bool {
    unimplemented!()
}

pub fn get_current_virtual_xids(
    _limit_xmin: TransactionId,
    _exclude_xmin0: bool,
    _all_dbs: bool,
    _exclude_vacuum: i32,
) -> Vec<VirtualTransactionId> {
    unimplemented!()
}
pub fn get_conflicting_virtual_xids(
    _limit_xmin: TransactionId,
    _db_oid: Oid,
) -> Vec<VirtualTransactionId> {
    unimplemented!()
}
/// C returns pid_t.
pub fn cancel_virtual_transaction(
    _vxid: VirtualTransactionId,
    _sigmode: ProcSignalReason,
) -> i32 {
    unimplemented!()
}
pub fn signal_virtual_transaction(
    _vxid: VirtualTransactionId,
    _sigmode: ProcSignalReason,
    _conflict_pending: bool,
) -> i32 {
    unimplemented!()
}

pub fn minimum_active_backends(_min: i32) -> bool {
    unimplemented!()
}
pub fn count_db_backends(_databaseid: Oid) -> i32 {
    unimplemented!()
}
pub fn count_db_connections(_databaseid: Oid) -> i32 {
    unimplemented!()
}
pub fn cancel_db_backends(_databaseid: Oid, _sigmode: ProcSignalReason, _conflict_pending: bool) {
    unimplemented!()
}
pub fn count_user_backends(_roleid: Oid) -> i32 {
    unimplemented!()
}
/// C: bool + `(nbackends, nprepared)` out-params -> Option of the counts (true
/// with the pair when there are other backends, None otherwise).
pub fn count_other_db_backends(_database_id: Oid) -> Option<(i32, i32)> {
    unimplemented!()
}
pub fn terminate_other_db_backends(_database_id: Oid) {
    unimplemented!()
}

pub fn xid_cache_remove_running_xids(
    _xid: TransactionId,
    _xids: &[TransactionId],
    _latest_xid: TransactionId,
) {
    unimplemented!()
}

pub fn proc_array_set_replication_slot_xmin(
    _xmin: TransactionId,
    _catalog_xmin: TransactionId,
    _already_locked: bool,
) {
    unimplemented!()
}

/// Out-params `xmin`/`catalog_xmin` -> a tuple.
pub fn proc_array_get_replication_slot_xmin() -> (TransactionId, TransactionId) {
    unimplemented!()
}
