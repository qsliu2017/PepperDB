//! Translated from PostgreSQL src/include/utils/snapmgr.h
//! POSTGRES snapshot manager. In-memory API.
//!
//! The header declares; the backend module
//! (`backend::utils::time::snapmgr`) defines (rules s2). Most entry points grew
//! a leading `shared: &Arc<SharedState>` (they need the ProcArray /
//! VariableCache to build snapshots, and the FdManager for exported-snapshot
//! files); the file-touching and subtrans-probing paths became `async`. The
//! re-exports below carry those NEW shapes, mirroring how `storage::procarray`
//! re-exports the procarray entry points.
//!
//! `FirstSnapshotSet` / `TransactionXmin` / `RecentXmin` are no longer process
//! globals: `FirstSnapshotSet` is the per-task `first_snapshot_set()` accessor
//! (re-exported here); `TransactionXmin`/`RecentXmin` live in
//! `storage::procarray` as per-task cells (`transaction_xmin()`/`recent_xmin()`).
#![allow(deprecated)] // GlobalVisState is a Phase-2 forward-decl in utils::snapshot

use crate::access::transam::FullTransactionId;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::{GlobalVisState, SnapshotData, SnapshotType};

// Snapshot-manager entry points (PascalCase preserved; new `shared`-taking and
// async shapes). Callers thread `shared` and `.await` the async ones.
pub use crate::backend::utils::time::snapmgr::{
    ActiveSnapshotSet, AtEOXact_Snapshot, AtSubAbort_Snapshot, AtSubCommit_Snapshot,
    DeleteAllExportedSnapshotFiles, EstimateSnapshotSpace, ExportSnapshot, GetActiveSnapshot,
    GetCatalogSnapshot, GetLatestSnapshot, GetNonHistoricCatalogSnapshot, GetTransactionSnapshot,
    GlobalVisTestFor, GlobalVisTestIsRemovableFullXid, GlobalVisTestIsRemovableXid,
    HaveRegisteredOrActiveSnapshot, HistoricSnapshotActive, HistoricSnapshotGetTupleCids,
    ImportSnapshot, InvalidateCatalogSnapshot, InvalidateCatalogSnapshotConditionally,
    PopActiveSnapshot, PushActiveSnapshot, PushActiveSnapshotWithLevel, PushCopiedSnapshot,
    RegisterSnapshot, RegisterSnapshotOnOwner, RestoreSnapshot, RestoreTransactionSnapshot,
    SerializeSnapshot, SetupHistoricSnapshot, SnapshotSetCommandId, TeardownHistoricSnapshot,
    ThereAreNoPriorRegisteredSnapshots, UnregisterSnapshot, UnregisterSnapshotFromOwner,
    UpdateActiveSnapshotCommandId, WaitForOlderSnapshots, XactHasExportedSnapshots,
    XidInMVCCSnapshot, first_snapshot_set,
};

// Special-snapshot semantics. C exposes `static SnapshotData` globals; under the
// async/per-task model they are constructed on demand. `get_self_snapshot()` /
// `get_any_snapshot()` build a fresh one (cheap, no XID arrays). Kept as
// constructors rather than mutable statics (rules s6.1).
/// C `SnapshotSelf` (`&SnapshotSelfData`).
pub fn get_self_snapshot() -> SnapshotData {
    special_snapshot(SnapshotType::Self_)
}
/// C `SnapshotAny` (`&SnapshotAnyData`).
pub fn get_any_snapshot() -> SnapshotData {
    special_snapshot(SnapshotType::Any)
}
/// C `SnapshotToastData`. (Use `get_toast_snapshot()` per the header note.)
pub fn get_toast_snapshot() -> SnapshotData {
    special_snapshot(SnapshotType::Toast)
}

fn special_snapshot(t: SnapshotType) -> SnapshotData {
    SnapshotData {
        snapshot_type: t,
        xmin: crate::access::transam::INVALID_TRANSACTION_ID,
        xmax: crate::access::transam::INVALID_TRANSACTION_ID,
        xip: Vec::new(),
        subxip: Vec::new(),
        suboverflowed: false,
        taken_during_recovery: false,
        copied: false,
        curcid: crate::c::CommandId(0),
        speculative_token: 0,
        vistest: None,
        active_count: 0,
        regd_count: 0,
        snap_xact_completion_count: 0,
    }
}

/// C: `InitDirtySnapshot(snapshotdata)`.
pub fn InitDirtySnapshot(snapshotdata: &mut SnapshotData) {
    snapshotdata.snapshot_type = SnapshotType::Dirty;
}

/// C: `InitNonVacuumableSnapshot(snapshotdata, vistestp)`.
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

/// procarray.c/snapmgr.c `GlobalVisCheckRemovableXid`. Staging stub: the
/// `BTPageIsRecyclable` caller (nbtree static-inline) has no `SharedState`
/// handle yet (step 15 threads it through the AM call path). The real
/// implementation is `backend::utils::time::snapmgr::GlobalVisCheckRemovableXid`.
pub fn GlobalVisCheckRemovableXid(_rel: Relation, _xid: crate::c::TransactionId) -> bool {
    unimplemented!() // TODO(step15): thread SharedState through the AM call path
}

/// procarray.c/snapmgr.c `GlobalVisCheckRemovableFullXid`. Staging stub (see
/// `GlobalVisCheckRemovableXid`).
pub fn GlobalVisCheckRemovableFullXid(_rel: Relation, _fxid: FullTransactionId) -> bool {
    unimplemented!() // TODO(step15): thread SharedState through the AM call path
}
