//! Translated from PostgreSQL src/include/replication/snapbuild.h
//!
//! Exports from replication/logical/snapbuild.c.

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::TransactionId;
pub use crate::replication::snapbuild_internal::SnapBuild;
use crate::utils::snapshot::Snapshot;

/// SnapBuildState. Keep `get_snapbuild_state_desc()` (pg_logicalinspect) in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SnapBuildState {
    /// Initial state, we can't do much yet.
    START = -1,
    /// Collecting committed transactions, to build the initial catalog snapshot.
    BUILDING_SNAPSHOT = 0,
    /// Enough info to decode tuples in transactions started after this.
    FULL_SNAPSHOT = 1,
    /// All transactions running at FULL_SNAPSHOT have finished.
    CONSISTENT = 2,
}

// ReorderBuffer / xl_heap_new_cid / xl_running_xacts are referenced only via the
// canonical modules; imported at use sites in Phase 2.

pub fn check_point_snap_build() {
    unimplemented!()
}

pub fn allocate_snapshot_builder(
    _reorder: &mut crate::replication::reorderbuffer::ReorderBuffer,
    _xmin_horizon: TransactionId,
    _start_lsn: XLogRecPtr,
    _need_full_snapshot: bool,
    _in_slot_creation: bool,
    _two_phase_at: XLogRecPtr,
) -> Box<SnapBuild> {
    unimplemented!()
}

pub fn free_snapshot_builder(_builder: Box<SnapBuild>) {
    unimplemented!()
}

pub fn snap_build_snap_dec_refcount(_snap: Snapshot) {
    unimplemented!()
}

pub fn snap_build_initial_snapshot(_builder: &mut SnapBuild) -> Snapshot<'static> {
    unimplemented!()
}
pub fn snap_build_export_snapshot(_builder: &mut SnapBuild) -> String {
    unimplemented!()
}
pub fn snap_build_clear_exported_snapshot() {
    unimplemented!()
}
pub fn snap_build_reset_exported_snapshot_state() {
    unimplemented!()
}

pub fn snap_build_current_state(_builder: &mut SnapBuild) -> SnapBuildState {
    unimplemented!()
}
pub fn snap_build_get_or_build_snapshot(_builder: &mut SnapBuild) -> Snapshot<'static> {
    unimplemented!()
}

pub fn snap_build_xact_needs_skip(_builder: &mut SnapBuild, _ptr: XLogRecPtr) -> bool {
    unimplemented!()
}
pub fn snap_build_get_two_phase_at(_builder: &mut SnapBuild) -> XLogRecPtr {
    unimplemented!()
}
pub fn snap_build_set_two_phase_at(_builder: &mut SnapBuild, _ptr: XLogRecPtr) {
    unimplemented!()
}

pub fn snap_build_commit_txn(
    _builder: &mut SnapBuild,
    _lsn: XLogRecPtr,
    _xid: TransactionId,
    _subxacts: &[TransactionId],
    _xinfo: u32,
) {
    unimplemented!()
}
pub fn snap_build_process_change(_builder: &mut SnapBuild, _xid: TransactionId, _lsn: XLogRecPtr) -> bool {
    unimplemented!()
}
pub fn snap_build_process_new_cid(
    _builder: &mut SnapBuild,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _xlrec: &mut crate::access::heapam_xlog::xl_heap_new_cid,
) {
    unimplemented!()
}
pub fn snap_build_process_running_xacts(
    _builder: &mut SnapBuild,
    _lsn: XLogRecPtr,
    _running: &mut crate::storage::standbydefs::xl_running_xacts,
) {
    unimplemented!()
}
pub fn snap_build_serialization_point(_builder: &mut SnapBuild, _lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn snap_build_snapshot_exists(_lsn: XLogRecPtr) -> bool {
    unimplemented!()
}
