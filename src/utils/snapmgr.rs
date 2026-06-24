//! Translated from PostgreSQL src/include/utils/snapmgr.h
//! POSTGRES snapshot manager. In-memory API.
#![allow(deprecated)] // GlobalVisState is a Phase-2 forward-decl in utils::snapshot

use crate::access::transam::FullTransactionId;
use crate::c::{CommandId, TransactionId};
use crate::postgres_ext::Oid;
use crate::utils::relcache::Relation;
use crate::utils::resowner::ResourceOwner;
use crate::utils::snapshot::{GlobalVisState, Snapshot, SnapshotData, SnapshotType};
use std::collections::HashMap;

// Process globals (TODO(global): move to Session/task state).
pub static mut FirstSnapshotSet: bool = false;
pub static mut TransactionXmin: TransactionId = TransactionId(0);
pub static mut RecentXmin: TransactionId = TransactionId(0);

// Special snapshot semantics. C exposes static SnapshotData; kept as globals.
pub static mut SnapshotSelfData: Option<SnapshotData> = None;
pub static mut SnapshotAnyData: Option<SnapshotData> = None;
pub static mut SnapshotToastData: Option<SnapshotData> = None;

/// C: `InitDirtySnapshot(snapshotdata)`.
pub fn InitDirtySnapshot(snapshotdata: &mut SnapshotData) {
    snapshotdata.snapshot_type = SnapshotType::Dirty;
}

/// C: `InitNonVacuumableSnapshot(snapshotdata, vistestp)`.
#[allow(deprecated)]
pub fn InitNonVacuumableSnapshot(
    snapshotdata: &mut SnapshotData,
    vistest: Option<Box<GlobalVisState>>,
) {
    snapshotdata.snapshot_type = SnapshotType::NonVacuumable;
    snapshotdata.vistest = vistest;
}

/// C: `IsMVCCSnapshot(snapshot)`.
pub fn IsMVCCSnapshot(snapshot: &SnapshotData) -> bool {
    matches!(
        snapshot.snapshot_type,
        SnapshotType::Mvcc | SnapshotType::HistoricMvcc
    )
}

pub fn GetTransactionSnapshot() -> Snapshot<'static> {
    unimplemented!()
}
pub fn GetLatestSnapshot() -> Snapshot<'static> {
    unimplemented!()
}
pub fn SnapshotSetCommandId(_curcid: CommandId) {
    unimplemented!()
}

pub fn GetCatalogSnapshot(_relid: Oid) -> Snapshot<'static> {
    unimplemented!()
}
pub fn GetNonHistoricCatalogSnapshot(_relid: Oid) -> Snapshot<'static> {
    unimplemented!()
}
pub fn InvalidateCatalogSnapshot() {
    unimplemented!()
}
pub fn InvalidateCatalogSnapshotConditionally() {
    unimplemented!()
}

pub fn PushActiveSnapshot(_snapshot: Snapshot) {
    unimplemented!()
}
pub fn PushActiveSnapshotWithLevel(_snapshot: Snapshot, _snap_level: i32) {
    unimplemented!()
}
pub fn PushCopiedSnapshot(_snapshot: Snapshot) {
    unimplemented!()
}
pub fn UpdateActiveSnapshotCommandId() {
    unimplemented!()
}
pub fn PopActiveSnapshot() {
    unimplemented!()
}
pub fn GetActiveSnapshot() -> Snapshot<'static> {
    unimplemented!()
}
pub fn ActiveSnapshotSet() -> bool {
    unimplemented!()
}

pub fn RegisterSnapshot(_snapshot: Snapshot) -> Snapshot<'static> {
    unimplemented!()
}
pub fn UnregisterSnapshot(_snapshot: Snapshot) {
    unimplemented!()
}
pub fn RegisterSnapshotOnOwner(_snapshot: Snapshot, _owner: ResourceOwner) -> Snapshot<'static> {
    unimplemented!()
}
pub fn UnregisterSnapshotFromOwner(_snapshot: Snapshot, _owner: ResourceOwner) {
    unimplemented!()
}

pub fn AtSubCommit_Snapshot(_level: i32) {
    unimplemented!()
}
pub fn AtSubAbort_Snapshot(_level: i32) {
    unimplemented!()
}
pub fn AtEOXact_Snapshot(_is_commit: bool, _reset_xmin: bool) {
    unimplemented!()
}

pub fn ImportSnapshot(_idstr: &str) {
    unimplemented!()
}
pub fn XactHasExportedSnapshots() -> bool {
    unimplemented!()
}
pub fn DeleteAllExportedSnapshotFiles() {
    unimplemented!()
}
pub fn WaitForOlderSnapshots(_limit_xmin: TransactionId, _progress: bool) {
    unimplemented!()
}
pub fn ThereAreNoPriorRegisteredSnapshots() -> bool {
    unimplemented!()
}
pub fn HaveRegisteredOrActiveSnapshot() -> bool {
    unimplemented!()
}

pub fn ExportSnapshot(_snapshot: Snapshot) -> String {
    unimplemented!()
}

// These live in procarray.c but thematically belong here.
pub fn GlobalVisTestFor(_rel: Relation) -> *mut GlobalVisState {
    unimplemented!() // TODO(ptr)
}
pub fn GlobalVisTestIsRemovableXid(_state: &mut GlobalVisState, _xid: TransactionId) -> bool {
    unimplemented!()
}
pub fn GlobalVisTestIsRemovableFullXid(
    _state: &mut GlobalVisState,
    _fxid: FullTransactionId,
) -> bool {
    unimplemented!()
}
pub fn GlobalVisCheckRemovableXid(_rel: Relation, _xid: TransactionId) -> bool {
    unimplemented!()
}
pub fn GlobalVisCheckRemovableFullXid(_rel: Relation, _fxid: FullTransactionId) -> bool {
    unimplemented!()
}

pub fn XidInMVCCSnapshot(_xid: TransactionId, _snapshot: Snapshot) -> bool {
    unimplemented!()
}

// Catalog timetravel for logical decoding. C `struct HTAB *` -> HashMap.
// The tuplecids map keys/values are opaque here; modelled as a raw HashMap.
pub fn HistoricSnapshotGetTupleCids() -> Option<*mut HashMap<u64, u64>> {
    unimplemented!() // TODO(ptr): real CID map element types
}
pub fn SetupHistoricSnapshot(_historic_snapshot: Snapshot, _tuplecids: *mut HashMap<u64, u64>) {
    unimplemented!()
}
pub fn TeardownHistoricSnapshot(_is_error: bool) {
    unimplemented!()
}
pub fn HistoricSnapshotActive() -> bool {
    unimplemented!()
}

pub fn EstimateSnapshotSpace(_snapshot: Snapshot) -> usize {
    unimplemented!()
}
pub fn SerializeSnapshot(_snapshot: Snapshot, _start_address: &mut [u8]) {
    unimplemented!()
}
pub fn RestoreSnapshot(_start_address: &[u8]) -> Snapshot<'static> {
    unimplemented!()
}
/// `void *source_pgproc` -> opaque PGPROC pointer. TODO(ptr).
pub fn RestoreTransactionSnapshot(_snapshot: Snapshot, _source_pgproc: *mut core::ffi::c_void) {
    unimplemented!()
}
