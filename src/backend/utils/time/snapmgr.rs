//! The snapshot manager. Translated from backend/utils/time/snapmgr.c.
//!
//! Hands out the MVCC snapshots used in tuple-visibility checks
//! (`GetTransactionSnapshot`, `GetLatestSnapshot`, `GetCatalogSnapshot`) and
//! owns the bookkeeping that keeps them alive for exactly as long as something
//! needs them. The get-snapshot entry points return a short-lived snapshot that
//! is liable to change on the next snapshot call; callers who need to hold one
//! push it onto the active-snapshot stack or register it. The active stack
//! mirrors the execution call stack so that the snapshot at the top is the one
//! current visibility checks use, while registered snapshots have an
//! independent lifetime. A snapshot is freed once it is no longer on the stack
//! or in the registered set, which is also what lets the backend's reported
//! xmin be reset or advanced as the oldest in-use snapshot changes.
//!
//! Beyond MVCC snapshots, the module manages the exported snapshots used by
//! `pg_export_snapshot` (synchronizing snapshots across sessions through files
//! in `pg_snapshots`), the historic snapshots used during logical decoding, and
//! the special non-MVCC snapshots (Self, Any, Dirty) that cannot be registered
//! or pushed.
//!
//! Where PostgreSQL keeps the current/secondary/catalog snapshots in
//! process-static storage and the active stack and registered set in
//! transaction-lifetime memory contexts, PepperDB gathers all of that
//! per-backend state into a single `RefCell<SnapMgrState>` held in a
//! `task_local!`, one instance per backend task. A snapshot is an
//! `Arc<SnapshotData>` rather than a raw pointer into long-lived storage: the
//! get-snapshot entry points return a cheap `Arc::clone`, so the same snapshot
//! can sit on the active stack, in the registered set, and in the caller's hand
//! at once with shared ownership and no aliasing. The transaction-xmin and
//! recent-xmin values are not redeclared here; they live with the process-array
//! subsystem and are read and set through its accessors.
//!
//! The hot snapshot-get path is synchronous, since taking a snapshot is an
//! in-memory scan of the process array. Only the paths that touch the
//! filesystem or sleep are async: exporting, importing, and clearing exported
//! snapshot files, and the visibility/wait helpers that probe sub-transaction
//! state or wait for older snapshots to drain. No `RefCell` borrow is held
//! across an await point. PostgreSQL's pairing-heap of registered snapshots
//! (keyed by xmin under modular comparison) is represented as a vector scanned
//! for the minimum xmin, because modular xid order is not a consistent total
//! order that an ordered map could rely on.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

#![allow(clippy::await_holding_refcell_ref)] // enforced by hand; see module note

use std::cell::RefCell;
use std::fmt::Write as _;
use std::sync::Arc;

use crate::access::transam::{FullTransactionId, INVALID_TRANSACTION_ID};
use crate::backend::storage::ipc::procarray;
use crate::c::{CommandId, TransactionId};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::storage::lock::VirtualTransactionId;
use crate::utils::snapshot::{GlobalVisState, Snapshot, SnapshotData, SnapshotType};

const SNAPSHOT_EXPORT_DIR: &str = "pg_snapshots";

// ---------------------------------------------------------------------------
// Per-task snapshot-manager state
// ---------------------------------------------------------------------------

/// snapmgr.c `ActiveSnapshotElt`: one active-stack entry, owning one
/// `active_count` on its snapshot.
struct ActiveSnapshotElt {
    as_snap: Arc<SnapshotData>,
    as_level: i32,
}

/// snapmgr.c `ExportedSnapshot`: the file name plus the registered copy.
struct ExportedSnapshot {
    snapfile: String,
    snapshot: Arc<SnapshotData>,
}

/// One `RegisteredSnapshots` entry: the shared snapshot plus its registration
/// count (kept beside the Arc so we don't COW the snapshot just to refcount).
struct RegisteredSnapshot {
    snap: Arc<SnapshotData>,
    regd_count: u32,
}

/// All of snapmgr.c's per-backend statics, gathered into one per-task struct.
#[derive(Default)]
struct SnapMgrState {
    /// Current/Secondary/Catalog snapshots (C `CurrentSnapshot` etc.). Each is
    /// the Arc most recently built by `GetSnapshotData`; getters clone it.
    current: Option<Arc<SnapshotData>>,
    secondary: Option<Arc<SnapshotData>>,
    catalog: Option<Arc<SnapshotData>>,

    /// C `FirstSnapshotSet`.
    first_snapshot_set: bool,
    /// C `FirstXactSnapshot`: the SAME Arc that is also in `registered` (shared
    /// identity, matching PG where they are the same pointer).
    first_xact_snapshot: Option<Arc<SnapshotData>>,

    /// C `ActiveSnapshot` stack (top = last element).
    active: Vec<ActiveSnapshotElt>,

    /// C `RegisteredSnapshots`: registered snapshots, owned here. PG uses a
    /// pairingheap keyed by xmin (modular cmp); needs lib/pairingheap.c --
    /// deferred. A plain BTreeMap can't substitute (modular xid order is not a
    /// consistent total order); we keep a Vec scanned for the min xmin.
    registered: Vec<RegisteredSnapshot>,

    /// C `exportedSnapshots`.
    exported: Vec<ExportedSnapshot>,

    /// C `HistoricSnapshot` (borrowed; lifetime managed by logical decoding).
    historic_active: bool,
}

tokio::task_local! {
    static SNAPMGR: RefCell<SnapMgrState>;
}

/// Run `f` with a fresh per-task snapshot-manager state in scope.
pub async fn snapmgr_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    SNAPMGR
        .scope(RefCell::new(SnapMgrState::default()), f)
        .await
}

fn in_scope() -> bool {
    SNAPMGR.try_with(|_| ()).is_ok()
}

// ---------------------------------------------------------------------------
// FirstSnapshotSet accessor (header re-exports the static-style global).
// ---------------------------------------------------------------------------

/// C `FirstSnapshotSet`.
pub fn first_snapshot_set() -> bool {
    SNAPMGR
        .try_with(|s| s.borrow().first_snapshot_set)
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// CopySnapshot / FreeSnapshot (Box clone)
// ---------------------------------------------------------------------------

/// snapmgr.c `CopySnapshot`: a fresh long-lived copy with zeroed refcounts.
fn copy_snapshot(snapshot: &SnapshotData) -> SnapshotData {
    let mut s = snapshot.clone();
    s.regd_count = 0;
    s.active_count = 0;
    s.copied = true;
    s.snap_xact_completion_count = 0;
    if snapshot.suboverflowed && !snapshot.taken_during_recovery {
        s.subxip.clear();
    }
    s
}

// ---------------------------------------------------------------------------
// GetTransactionSnapshot / GetLatestSnapshot / catalog snapshots
// ---------------------------------------------------------------------------

/// snapmgr.c `GetTransactionSnapshot`.
pub fn GetTransactionSnapshot(shared: &Arc<SharedState>) -> Snapshot {
    if HistoricSnapshotActive() {
        // Caller is responsible for using the historic snapshot correctly; PG
        // returns it directly. We don't track the historic pointer here
        // (logical decoding is out of foundation) -- return None as a sentinel.
        return None;
    }

    let first = SNAPMGR.with(|s| s.borrow().first_snapshot_set);
    if !first {
        InvalidateCatalogSnapshot();

        // TODO(panic): migrate to Result + ?
        assert!(!crate::access::xact::IsInParallelMode(), "cannot take query snapshot during a parallel operation");

        let data = build_snapshot(shared);
        if crate::access::xact::IsolationUsesXactSnapshot() {
            // Serializable goes through predicate.c (stub, step lock-manager).
            // In transaction-snapshot mode the first snapshot must outlive the
            // call: one Arc is shared as CurrentSnapshot, the registered entry,
            // and FirstXactSnapshot (shared identity, matching PG).
            let arc = Arc::new(copy_snapshot(&data));
            return SNAPMGR.with(|s| {
                let mut st = s.borrow_mut();
                st.registered.push(RegisteredSnapshot {
                    snap: arc.clone(),
                    regd_count: 1,
                });
                st.first_xact_snapshot = Some(arc.clone());
                st.current = Some(arc.clone());
                st.first_snapshot_set = true;
                Some(arc)
            });
        }

        return SNAPMGR.with(|s| {
            let mut st = s.borrow_mut();
            let arc = Arc::new(data);
            st.current = Some(arc.clone());
            st.first_snapshot_set = true;
            Some(arc)
        });
    }

    if crate::access::xact::IsolationUsesXactSnapshot() {
        return SNAPMGR.with(|s| s.borrow().current.clone());
    }

    InvalidateCatalogSnapshot();
    let data = build_snapshot(shared);
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        let arc = Arc::new(data);
        st.current = Some(arc.clone());
        Some(arc)
    })
}

/// snapmgr.c `GetLatestSnapshot`.
pub fn GetLatestSnapshot(shared: &Arc<SharedState>) -> Snapshot {
    // TODO(panic): migrate to Result + ?
    assert!(!crate::access::xact::IsInParallelMode(), "cannot update SecondarySnapshot during a parallel operation");
    debug_assert!(!HistoricSnapshotActive());

    if !SNAPMGR.with(|s| s.borrow().first_snapshot_set) {
        return GetTransactionSnapshot(shared);
    }

    let data = build_snapshot(shared);
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        let arc = Arc::new(data);
        st.secondary = Some(arc.clone());
        Some(arc)
    })
}

/// snapmgr.c `GetCatalogSnapshot`.
pub fn GetCatalogSnapshot(shared: &Arc<SharedState>, relid: Oid) -> Snapshot {
    if HistoricSnapshotActive() {
        return None;
    }
    GetNonHistoricCatalogSnapshot(shared, relid)
}

/// snapmgr.c `GetNonHistoricCatalogSnapshot`.
pub fn GetNonHistoricCatalogSnapshot(shared: &Arc<SharedState>, relid: Oid) -> Snapshot {
    let valid = SNAPMGR.with(|s| s.borrow().catalog.is_some());
    if valid
        && !crate::utils::syscache::RelationInvalidatesSnapshotsOnly(relid)
        && !crate::utils::syscache::RelationHasSysCache(relid)
    {
        InvalidateCatalogSnapshot();
    }

    if SNAPMGR.with(|s| s.borrow().catalog.is_none()) {
        let data = build_snapshot(shared);
        SNAPMGR.with(|s| s.borrow_mut().catalog = Some(Arc::new(data)));
        // The catalog snapshot participates in xmin tracking via being valid;
        // its xmin is consulted by snapshot_reset_xmin().
    }
    SNAPMGR.with(|s| s.borrow().catalog.clone())
}

/// snapmgr.c `InvalidateCatalogSnapshot`.
pub fn InvalidateCatalogSnapshot() {
    if !in_scope() {
        return;
    }
    let had = SNAPMGR.with(|s| s.borrow_mut().catalog.take().is_some());
    if had {
        snapshot_reset_xmin();
    }
}

/// snapmgr.c `InvalidateCatalogSnapshotConditionally`.
pub fn InvalidateCatalogSnapshotConditionally() {
    let drop = SNAPMGR.with(|s| {
        let st = s.borrow();
        st.catalog.is_some() && st.active.is_empty() && registered_len(&st) == 1
    });
    if drop {
        InvalidateCatalogSnapshot();
    }
}

/// snapmgr.c `SnapshotSetCommandId`. COW: `Arc::make_mut` clones the snapshot
/// only if it has other holders, then sets curcid in place.
pub fn SnapshotSetCommandId(curcid: CommandId) {
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        if !st.first_snapshot_set {
            return;
        }
        if let Some(arc) = st.current.as_mut() {
            Arc::make_mut(arc).curcid = curcid;
        }
        if let Some(arc) = st.secondary.as_mut() {
            Arc::make_mut(arc).curcid = curcid;
        }
    });
}

// ---------------------------------------------------------------------------
// Snapshot building
// ---------------------------------------------------------------------------

fn blank_mvcc() -> SnapshotData {
    SnapshotData {
        snapshot_type: SnapshotType::Mvcc,
        xmin: INVALID_TRANSACTION_ID,
        xmax: INVALID_TRANSACTION_ID,
        xip: Vec::new(),
        subxip: Vec::new(),
        suboverflowed: false,
        taken_during_recovery: false,
        copied: false,
        curcid: CommandId(0),
        speculative_token: 0,
        vistest: None,
        active_count: 0,
        regd_count: 0,
        snap_xact_completion_count: 0,
    }
}

/// Build a fresh `SnapshotData` via procarray's `get_snapshot_data` (sync).
/// Done outside any `RefCell` borrow so we never hold it while procarray locks.
fn build_snapshot(shared: &Arc<SharedState>) -> SnapshotData {
    let mut data = blank_mvcc();
    shared
        .proc_array()
        .get_snapshot_data(shared.variable_cache(), &mut data);
    data
}

fn registered_len(st: &SnapMgrState) -> usize {
    let mut n = st.registered.len();
    if st.catalog.is_some() {
        n += 1; // catalog snapshot is logically in RegisteredSnapshots
    }
    n
}

// ---------------------------------------------------------------------------
// Active snapshot stack
// ---------------------------------------------------------------------------

/// snapmgr.c `PushActiveSnapshot`.
pub fn PushActiveSnapshot(snapshot: Snapshot) {
    PushActiveSnapshotWithLevel(
        snapshot,
        crate::access::xact::GetCurrentTransactionNestLevel(),
    );
}

/// snapmgr.c `PushActiveSnapshotWithLevel`.
pub fn PushActiveSnapshotWithLevel(snapshot: Snapshot, snap_level: i32) {
    let snap = snapshot.expect("PushActiveSnapshot: InvalidSnapshot");
    // Static/non-copied snapshots must be copied to long-lived storage.
    let mut data = if snap.copied {
        (*snap).clone()
    } else {
        copy_snapshot(&snap)
    };
    data.active_count += 1;
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        debug_assert!(
            st.active
                .last()
                .is_none_or(|e| snap_level >= e.as_level),
            "active snapshot level must be non-decreasing"
        );
        st.active.push(ActiveSnapshotElt {
            as_snap: Arc::new(data),
            as_level: snap_level,
        });
    });
}

/// snapmgr.c `PushCopiedSnapshot`.
pub fn PushCopiedSnapshot(snapshot: Snapshot) {
    let snap = snapshot.expect("PushCopiedSnapshot: InvalidSnapshot");
    let mut data = copy_snapshot(&snap);
    data.active_count += 1;
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        let level = crate::access::xact::GetCurrentTransactionNestLevel();
        st.active.push(ActiveSnapshotElt {
            as_snap: Arc::new(data),
            as_level: level,
        });
    });
}

/// snapmgr.c `UpdateActiveSnapshotCommandId`. COW: `Arc::make_mut` sets curcid.
pub fn UpdateActiveSnapshotCommandId() {
    let curcid = crate::access::xact::GetCurrentCommandId(false);
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        let top = st.active.last_mut().expect("no active snapshot");
        debug_assert!(top.as_snap.active_count == 1 && top.as_snap.regd_count == 0);
        // TODO(panic): migrate to Result + ?
        assert!(!(crate::access::xact::IsInParallelMode() && top.as_snap.curcid != curcid), "cannot modify commandid in active snapshot during a parallel operation");
        Arc::make_mut(&mut top.as_snap).curcid = curcid;
    });
}

/// snapmgr.c `PopActiveSnapshot`.
pub fn PopActiveSnapshot() {
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        let elt = st.active.pop().expect("PopActiveSnapshot: empty stack");
        debug_assert!(elt.as_snap.active_count > 0);
        // Arc dropped here; FreeSnapshot when the last holder releases it.
    });
    snapshot_reset_xmin();
}

/// snapmgr.c `GetActiveSnapshot`.
pub fn GetActiveSnapshot() -> Snapshot {
    SNAPMGR.with(|s| {
        let st = s.borrow();
        let top = st.active.last().expect("no active snapshot");
        Some(top.as_snap.clone())
    })
}

/// snapmgr.c `ActiveSnapshotSet`.
pub fn ActiveSnapshotSet() -> bool {
    SNAPMGR.with(|s| !s.borrow().active.is_empty())
}

// ---------------------------------------------------------------------------
// Registered snapshots
// ---------------------------------------------------------------------------

/// snapmgr.c `RegisterSnapshot`. Registration is tracked here; the resource
/// owner integration (UnregisterSnapshotNoOwner on owner release) is left as
/// TODO(resowner) because the snapshot-tracking resowner API isn't wired yet --
/// the count is reset at end of (sub)xact regardless.
pub fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot {
    let snap = snapshot?;
    let data = if snap.copied {
        (*snap).clone()
    } else {
        copy_snapshot(&snap)
    };
    let arc = Arc::new(data);
    SNAPMGR.with(|s| {
        s.borrow_mut().registered.push(RegisteredSnapshot {
            snap: arc.clone(),
            regd_count: 1,
        });
    });
    Some(arc)
}

/// snapmgr.c `RegisterSnapshotOnOwner`.
pub fn RegisterSnapshotOnOwner(
    snapshot: Snapshot,
    _owner: crate::utils::resowner::ResourceOwner,
) -> Snapshot {
    // TODO(resowner): ResourceOwnerRememberSnapshot(owner, snap).
    RegisterSnapshot(snapshot)
}

/// snapmgr.c `UnregisterSnapshot`.
pub fn UnregisterSnapshot(snapshot: Snapshot) {
    let Some(snap) = snapshot else { return };
    unregister_snapshot_no_owner(&snap);
}

/// snapmgr.c `UnregisterSnapshotFromOwner`.
pub fn UnregisterSnapshotFromOwner(
    snapshot: Snapshot,
    _owner: crate::utils::resowner::ResourceOwner,
) {
    // TODO(resowner): ResourceOwnerForgetSnapshot(owner, snapshot).
    UnregisterSnapshot(snapshot);
}

/// snapmgr.c `UnregisterSnapshotNoOwner`: drop by Arc identity.
fn unregister_snapshot_no_owner(snap: &Arc<SnapshotData>) {
    let freed = SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        if let Some(pos) = st
            .registered
            .iter()
            .position(|b| Arc::ptr_eq(&b.snap, snap))
        {
            let entry = &mut st.registered[pos];
            debug_assert!(entry.regd_count > 0);
            entry.regd_count -= 1;
            if entry.regd_count == 0 {
                st.registered.remove(pos);
                return true;
            }
        }
        false
    });
    if freed {
        snapshot_reset_xmin();
    }
}

// ---------------------------------------------------------------------------
// SnapshotResetXmin
// ---------------------------------------------------------------------------

/// snapmgr.c `SnapshotResetXmin`: drop/advance our xmin when snapshots leave.
fn snapshot_reset_xmin() {
    if !in_scope() {
        return;
    }
    let min_xmin = SNAPMGR.with(|s| {
        let st = s.borrow();
        if !st.active.is_empty() {
            return None; // only recompute when the active stack is empty
        }
        // Min xmin over registered snapshots (+ catalog if valid).
        let mut min: Option<TransactionId> = None;
        for b in &st.registered {
            let xmin = b.snap.xmin;
            min = Some(match min {
                Some(m) if m.precedes(xmin) => m,
                _ => xmin,
            });
        }
        if let Some(c) = st.catalog.as_ref() {
            min = Some(match min {
                Some(m) if m.precedes(c.xmin) => m,
                _ => c.xmin,
            });
        }
        Some(min.unwrap_or(INVALID_TRANSACTION_ID))
    });

    match min_xmin {
        None => {}
        Some(x) if !x.is_valid() => {
            // No registered snapshots: reset TransactionXmin (and MyProc->xmin,
            // owned by procarray/step 15).
            procarray::set_transaction_xmin_public(INVALID_TRANSACTION_ID);
        }
        Some(min) => {
            let cur = procarray::transaction_xmin();
            if cur.precedes(min) {
                procarray::set_transaction_xmin_public(min);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sub/EO-xact cleanup
// ---------------------------------------------------------------------------

/// snapmgr.c `AtSubCommit_Snapshot`: relabel this subxact's active snapshots to
/// the parent level.
pub fn AtSubCommit_Snapshot(level: i32) {
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        for elt in st.active.iter_mut().rev() {
            if elt.as_level < level {
                break;
            }
            elt.as_level = level - 1;
        }
    });
}

/// snapmgr.c `AtSubAbort_Snapshot`: forget active snapshots set in this subxact.
pub fn AtSubAbort_Snapshot(level: i32) {
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        while let Some(top) = st.active.last() {
            if top.as_level < level {
                break;
            }
            let elt = st.active.pop().unwrap();
            debug_assert!(elt.as_snap.active_count >= 1);
            // Arc drops here if no longer referenced (FreeSnapshot).
        }
    });
    snapshot_reset_xmin();
}

/// snapmgr.c `AtEOXact_Snapshot`: end-of-transaction cleanup.
pub fn AtEOXact_Snapshot(is_commit: bool, reset_xmin: bool) {
    // Exported snapshot files are unlinked by AtEOXact synchronously in C via
    // unlink(); here the files were written through shared.fd() and their
    // removal is best-effort. We can't await in this sync function, so we only
    // clear the in-memory list; orphaned files are swept by
    // DeleteAllExportedSnapshotFiles at startup. TODO(resowner/async-eoxact).
    // C calls InvalidateCatalogSnapshot() first so the catalog snapshot doesn't
    // count as a leak below.
    InvalidateCatalogSnapshot();

    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        st.first_xact_snapshot = None;
        st.exported.clear();

        if is_commit {
            if registered_len(&st) != 0 {
                // TODO(warn): elog(WARNING, "registered snapshots seem to remain
                // after cleanup");
            }
            if !st.active.is_empty() {
                // TODO(warn): elog(WARNING, "snapshot %p still active").
            }
        }

        st.active.clear();
        st.registered.clear();
        st.current = None;
        st.secondary = None;
        st.first_snapshot_set = false;
    });

    if reset_xmin {
        snapshot_reset_xmin();
    }
}

// ---------------------------------------------------------------------------
// ThereAreNoPriorRegisteredSnapshots / HaveRegisteredOrActiveSnapshot
// ---------------------------------------------------------------------------

/// snapmgr.c `ThereAreNoPriorRegisteredSnapshots`.
pub fn ThereAreNoPriorRegisteredSnapshots() -> bool {
    SNAPMGR.with(|s| registered_len(&s.borrow()) <= 1)
}

/// snapmgr.c `HaveRegisteredOrActiveSnapshot`.
pub fn HaveRegisteredOrActiveSnapshot() -> bool {
    SNAPMGR.with(|s| {
        let st = s.borrow();
        if !st.active.is_empty() {
            return true;
        }
        // The catalog snapshot alone does not count.
        if st.catalog.is_some() && registered_len(&st) == 1 {
            return false;
        }
        registered_len(&st) != 0
    })
}

// ---------------------------------------------------------------------------
// Exported snapshots (file I/O -> async)
// ---------------------------------------------------------------------------

/// snapmgr.c `ExportSnapshot`. Async: writes the export file via `shared.fd()`.
pub async fn ExportSnapshot(shared: &Arc<SharedState>, snapshot: Snapshot) -> String {
    let snap = snapshot.expect("ExportSnapshot: InvalidSnapshot");

    let top_xid = crate::access::xact::GetTopTransactionIdIfAny();
    // TODO(panic): migrate to Result + ?
    assert!(!crate::access::xact::IsSubTransaction(), "cannot export a snapshot from a subtransaction");
    let children = crate::access::xact::xactGetCommittedChildren();

    // Snapshot copy registered for the rest of the xact (xmin honored).
    let data = copy_snapshot(&snap);
    let snap_for_text = data.clone();
    let reg = Arc::new(data);

    let idx = SNAPMGR.with(|s| s.borrow().exported.len()) + 1;
    let (procnum, lxid) = my_vxid(shared);
    let path = format!(
        "{SNAPSHOT_EXPORT_DIR}/{procnum:08X}-{lxid:08X}-{idx}"
    );

    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        st.exported.push(ExportedSnapshot {
            snapfile: path.clone(),
            snapshot: reg,
        });
    });

    let buf = build_export_text(shared, &snap_for_text, top_xid, &children);

    // Write to <path>.tmp then rename, via shared.fd().
    write_export_file(shared, &path, &buf).await;

    path[SNAPSHOT_EXPORT_DIR.len() + 1..].to_string()
}

fn my_vxid(shared: &Arc<SharedState>) -> (u32, u32) {
    // MyProc->vxid (step 15). Use the session's synthetic identity for now.
    let pid = crate::session::try_current()
        .map_or(0, |s| s.proc_pid());
    let _ = shared;
    (pid as u32, pid as u32)
}

fn build_export_text(
    shared: &Arc<SharedState>,
    snapshot: &SnapshotData,
    top_xid: Option<TransactionId>,
    children: &[TransactionId],
) -> Vec<u8> {
    let (procnum, lxid) = my_vxid(shared);
    let pid = crate::session::try_current()
        .map_or(0, |s| s.proc_pid());
    let dbid = crate::session::try_current()
        .map_or(Oid(0), |s| s.database_id());
    let iso = crate::access::xact::XactIsoLevel();
    let ro = i32::from(crate::access::xact::XactReadOnly());

    let mut s = String::new();
    writeln!(s, "vxid:{procnum}/{lxid}").unwrap();
    writeln!(s, "pid:{pid}").unwrap();
    writeln!(s, "dbid:{}", dbid.0).unwrap();
    writeln!(s, "iso:{iso}").unwrap();
    writeln!(s, "ro:{ro}").unwrap();
    writeln!(s, "xmin:{}", snapshot.xmin.0).unwrap();
    writeln!(s, "xmax:{}", snapshot.xmax.0).unwrap();

    let add_top = top_xid
        .is_some_and(|t| t.is_valid() && t.precedes(snapshot.xmax));
    writeln!(s, "xcnt:{}", snapshot.xip.len() + usize::from(add_top)).unwrap();
    for x in &snapshot.xip {
        writeln!(s, "xip:{}", x.0).unwrap();
    }
    if add_top {
        writeln!(s, "xip:{}", top_xid.unwrap().0).unwrap();
    }

    let max_sub = shared.proc_array().get_max_snapshot_subxid_count() as usize;
    if snapshot.suboverflowed || snapshot.subxip.len() + children.len() > max_sub {
        s.push_str("sof:1\n");
    } else {
        s.push_str("sof:0\n");
        writeln!(s, "sxcnt:{}", snapshot.subxip.len() + children.len()).unwrap();
        for x in &snapshot.subxip {
            writeln!(s, "sxp:{}", x.0).unwrap();
        }
        for c in children {
            writeln!(s, "sxp:{}", c.0).unwrap();
        }
    }
    writeln!(s, "rec:{}", u32::from(snapshot.taken_during_recovery)).unwrap();
    s.into_bytes()
}

fn data_dir_base(shared: &Arc<SharedState>) -> std::path::PathBuf {
    shared
        .config()
        .data_dir()
        .map_or_else(|| std::path::PathBuf::from("."), std::path::PathBuf::from)
}

async fn write_export_file(shared: &Arc<SharedState>, path: &str, buf: &[u8]) {
    use std::io::Write;
    let data_dir = data_dir_base(shared);
    let dir = data_dir.join(SNAPSHOT_EXPORT_DIR);
    let final_path = data_dir.join(path);
    let tmp_path = data_dir.join(format!("{path}.tmp"));
    let buf = buf.to_vec();
    tokio::task::spawn_blocking(move || {
        let _ = std::fs::create_dir_all(&dir);
        {
            let mut f = std::fs::File::create(&tmp_path).expect("could not create export file");
            f.write_all(&buf).expect("could not write export file");
        }
        std::fs::rename(&tmp_path, &final_path).expect("could not rename export file");
    })
    .await
    .expect("export-file task panicked");
}

/// snapmgr.c `XactHasExportedSnapshots`.
pub fn XactHasExportedSnapshots() -> bool {
    SNAPMGR.with(|s| !s.borrow().exported.is_empty())
}

/// snapmgr.c `DeleteAllExportedSnapshotFiles`. Async: scans + unlinks via fd.
pub async fn DeleteAllExportedSnapshotFiles(shared: &Arc<SharedState>) {
    let dir = data_dir_base(shared).join(SNAPSHOT_EXPORT_DIR);
    tokio::task::spawn_blocking(move || {
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for ent in rd.flatten() {
                let name = ent.file_name();
                if name == "." || name == ".." {
                    continue;
                }
                let _ = std::fs::remove_file(ent.path());
            }
        }
    })
    .await
    .expect("delete-exported task panicked");
}

/// snapmgr.c `ImportSnapshot`. Async: reads the export file via `shared.fd()`.
pub async fn ImportSnapshot(shared: &Arc<SharedState>, idstr: &str) {
    if first_snapshot_set()
        || crate::access::xact::GetTopTransactionIdIfAny().is_some()
        || crate::access::xact::IsSubTransaction()
    {
        // TODO(panic): migrate to Result + ?
        panic!("SET TRANSACTION SNAPSHOT must be called before any query");
    }
    assert!(crate::access::xact::IsolationUsesXactSnapshot(), 
        "a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ"
    );
    assert!(idstr.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'), "invalid snapshot identifier: \"{idstr}\"");

    let path = data_dir_base(shared).join(SNAPSHOT_EXPORT_DIR).join(idstr);
    let content = tokio::task::spawn_blocking(move || std::fs::read_to_string(&path))
        .await
        .expect("import-file task panicked")
        .unwrap_or_else(|_| panic!("snapshot \"{idstr}\" does not exist"));

    let max_xcnt = shared.proc_array().get_max_snapshot_xid_count();
    let max_subxcnt = shared.proc_array().get_max_snapshot_subxid_count();
    let (snapshot, src_vxid, src_dbid) = parse_import(&content, max_xcnt, max_subxcnt);

    // C: VirtualTransactionIdIsValid(src_vxid) && OidIsValid(src_dbid) checks.
    assert!(
        src_vxid.is_valid() && src_dbid != Oid(0),
        "invalid snapshot data"
    );

    // TODO(predicate): when IsolationIsSerializable() is wired, reject a
    // non-SERIALIZABLE source (src_isolevel) and a read-only source adopted by a
    // non-read-only importer (src_readonly && !XactReadOnly), per C.

    if src_dbid
        != crate::session::try_current()
            .map_or(Oid(0), |s| s.database_id())
    {
        panic!("cannot import a snapshot from a different database");
    }

    set_transaction_snapshot(shared, &snapshot, Some(&src_vxid), InvalidPid_value(), None);
}

fn InvalidPid_value() -> i32 {
    crate::miscadmin::InvalidPid
}

/// Parse the text export format into (snapshot, src_vxid, src_dbid).
fn parse_import(
    content: &str,
    max_xcnt: i32,
    max_subxcnt: i32,
) -> (SnapshotData, VirtualTransactionId, Oid) {
    let mut lines = content.lines();
    let mut next = |prefix: &str| -> String {
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("invalid snapshot data"));
        let v = line
            .strip_prefix(prefix)
            .unwrap_or_else(|| panic!("invalid snapshot data"));
        v.to_string()
    };

    let vxid_s = next("vxid:");
    let (pn, lx) = vxid_s
        .split_once('/')
        .unwrap_or_else(|| panic!("invalid snapshot data"));
    let src_vxid = VirtualTransactionId {
        proc_number: pn.parse().unwrap(),
        local_transaction_id: crate::c::LocalTransactionId(lx.parse().unwrap()),
    };
    let _pid: i32 = next("pid:").parse().unwrap();
    let src_dbid = Oid(next("dbid:").parse().unwrap());
    let _iso: i32 = next("iso:").parse().unwrap();
    let _ro: i32 = next("ro:").parse().unwrap();

    let mut snap = blank_mvcc();
    snap.xmin = TransactionId(next("xmin:").parse().unwrap());
    snap.xmax = TransactionId(next("xmax:").parse().unwrap());
    let xcnt: i64 = next("xcnt:").parse().unwrap();
    // Sanity-check the count against the procarray bound before allocating.
    assert!(xcnt >= 0 && xcnt <= i64::from(max_xcnt), "invalid snapshot data");
    for _ in 0..xcnt {
        snap.xip.push(TransactionId(next("xip:").parse().unwrap()));
    }
    let sof: i32 = next("sof:").parse().unwrap();
    if sof == 0 {
        let sxcnt: i64 = next("sxcnt:").parse().unwrap();
        assert!(sxcnt >= 0 && sxcnt <= i64::from(max_subxcnt), "invalid snapshot data");
        for _ in 0..sxcnt {
            snap.subxip
                .push(TransactionId(next("sxp:").parse().unwrap()));
        }
        snap.suboverflowed = false;
    } else {
        snap.suboverflowed = true;
    }
    snap.taken_during_recovery = next("rec:").parse::<u32>().unwrap() != 0;

    assert!(!(!snap.xmin.is_normal() || !snap.xmax.is_normal()), "invalid snapshot data");
    (snap, src_vxid, src_dbid)
}

// ---------------------------------------------------------------------------
// SetTransactionSnapshot / RestoreTransactionSnapshot
// ---------------------------------------------------------------------------

/// snapmgr.c `SetTransactionSnapshot` (static in C; pub(crate) here).
fn set_transaction_snapshot(
    shared: &Arc<SharedState>,
    sourcesnap: &SnapshotData,
    sourcevxid: Option<&VirtualTransactionId>,
    _sourcepid: i32,
    _sourceproc: Option<&crate::storage::proc::PGPROC>,
) {
    debug_assert!(!first_snapshot_set());
    InvalidateCatalogSnapshot();

    // Build a snapshot to allocate arrays + update GlobalVis* (we discard the
    // running set and overlay the imported fields).
    let mut buf = build_snapshot(shared);
    buf.xmin = sourcesnap.xmin;
    buf.xmax = sourcesnap.xmax;
    buf.xip.clone_from(&sourcesnap.xip);
    buf.subxip.clone_from(&sourcesnap.subxip);
    buf.suboverflowed = sourcesnap.suboverflowed;
    buf.taken_during_recovery = sourcesnap.taken_during_recovery;
    buf.snap_xact_completion_count = 0;

    let installed = match sourcevxid {
        Some(vxid) => shared
            .proc_array()
            .proc_array_install_imported_xmin(buf.xmin, vxid),
        None => false, // restored path needs PGPROC (step 15) -- TODO(restore)
    };
    // TODO(panic): migrate to Result + ?
    assert!(sourcevxid.is_none() || installed, "could not import the requested snapshot");

    let uses_xact = crate::access::xact::IsolationUsesXactSnapshot();
    let arc = Arc::new(buf);
    SNAPMGR.with(|s| {
        let mut st = s.borrow_mut();
        st.current = Some(arc.clone());
        if uses_xact {
            // Same Arc shared as the registered entry and FirstXactSnapshot.
            st.registered.push(RegisteredSnapshot {
                snap: arc.clone(),
                regd_count: 1,
            });
            st.first_xact_snapshot = Some(arc);
        }
        st.first_snapshot_set = true;
    });
}

/// snapmgr.c `RestoreTransactionSnapshot`.
pub fn RestoreTransactionSnapshot(
    shared: &Arc<SharedState>,
    snapshot: Snapshot,
    _source_pgproc: *mut core::ffi::c_void,
) {
    let snap = snapshot.expect("RestoreTransactionSnapshot: InvalidSnapshot");
    set_transaction_snapshot(shared, &snap, None, crate::miscadmin::InvalidPid, None);
}

// ---------------------------------------------------------------------------
// WaitForOlderSnapshots
// ---------------------------------------------------------------------------

/// snapmgr.c `WaitForOlderSnapshots`. Async: it waits on other backends'
/// virtual xids (VirtualXactLock). The lock-manager wait is step out-of-scope
/// here; over the empty procarray there is nothing to wait for, so this is a
/// faithful no-op until the lock manager + populated procarray land.
pub async fn WaitForOlderSnapshots(_limit_xmin: TransactionId, _progress: bool) {
    // TODO(lock-manager): VirtualXactLock on each conflicting vxid.
}

// ---------------------------------------------------------------------------
// XidInMVCCSnapshot
// ---------------------------------------------------------------------------

/// snapmgr.c `XidInMVCCSnapshot`. Async: the overflow path consults pg_subtrans.
pub async fn XidInMVCCSnapshot(
    shared: &Arc<SharedState>,
    mut xid: TransactionId,
    snapshot: &SnapshotData,
) -> bool {
    if xid.precedes(snapshot.xmin) {
        return false;
    }
    if xid.follows_or_equals(snapshot.xmax) {
        return true;
    }

    if snapshot.taken_during_recovery {
        if snapshot.suboverflowed {
            xid = shared
                .subtrans()
                .sub_trans_get_topmost_transaction(xid, procarray::transaction_xmin())
                .await;
            if xid.precedes(snapshot.xmin) {
                return false;
            }
        }
        if pg_lfind(&snapshot.subxip, xid) {
            return true;
        }
    } else {
        if snapshot.suboverflowed {
            xid = shared
                .subtrans()
                .sub_trans_get_topmost_transaction(xid, procarray::transaction_xmin())
                .await;
            if xid.precedes(snapshot.xmin) {
                return false;
            }
        } else if pg_lfind(&snapshot.subxip, xid) {
            return true;
        }
        if pg_lfind(&snapshot.xip, xid) {
            return true;
        }
    }
    false
}

fn pg_lfind(arr: &[TransactionId], xid: TransactionId) -> bool {
    crate::port::pg_lfind::pg_lfind32(xid.0, unsafe {
        std::slice::from_raw_parts(arr.as_ptr().cast::<u32>(), arr.len())
    })
}

// ---------------------------------------------------------------------------
// Historic snapshots (logical decoding)
// ---------------------------------------------------------------------------

/// snapmgr.c `SetupHistoricSnapshot`. Logical decoding is out of foundation; we
/// only track the active flag and the tuplecid map opaquely.
#[allow(clippy::implicit_hasher, reason = "raw-ptr stub mirrors C HTAB* arg; default hasher intended")]
pub fn SetupHistoricSnapshot(
    _historic_snapshot: Snapshot,
    _tuplecids: *mut std::collections::HashMap<u64, u64>,
) {
    SNAPMGR.with(|s| s.borrow_mut().historic_active = true);
}

/// snapmgr.c `TeardownHistoricSnapshot`.
pub fn TeardownHistoricSnapshot(_is_error: bool) {
    if in_scope() {
        SNAPMGR.with(|s| s.borrow_mut().historic_active = false);
    }
}

/// snapmgr.c `HistoricSnapshotActive`.
pub fn HistoricSnapshotActive() -> bool {
    SNAPMGR
        .try_with(|s| s.borrow().historic_active)
        .unwrap_or(false)
}

/// snapmgr.c `HistoricSnapshotGetTupleCids`.
pub fn HistoricSnapshotGetTupleCids() -> Option<*mut std::collections::HashMap<u64, u64>> {
    debug_assert!(HistoricSnapshotActive());
    None // TODO(logical-decoding): real (cmin,cmax) map.
}

// ---------------------------------------------------------------------------
// Snapshot serialization (parallel workers)
// ---------------------------------------------------------------------------

/// snapmgr.c `SerializedSnapshotData` (header part).
#[repr(C)]
struct SerializedSnapshotData {
    xmin: TransactionId,
    xmax: TransactionId,
    xcnt: u32,
    subxcnt: i32,
    suboverflowed: bool,
    taken_during_recovery: bool,
    curcid: CommandId,
}

const SERIALIZED_HEADER_SIZE: usize = std::mem::size_of::<SerializedSnapshotData>();

// repr(C) field order + size is load-bearing for the DSM hand-off (xmin, xmax,
// u32 xcnt, i32 subxcnt, 2x bool, u32 curcid with 2 bytes pad -> 24); guard it.
const _: () = assert!(SERIALIZED_HEADER_SIZE == 24);

/// C `subxcnt>0 && (!suboverflowed || takenDuringRecovery)`: whether the subxip
/// array is part of the serialized image. One predicate so EstimateSnapshotSpace
/// and SerializeSnapshot can never disagree on the wire size.
fn serialize_subxip(snap: &SnapshotData) -> bool {
    !snap.subxip.is_empty() && (!snap.suboverflowed || snap.taken_during_recovery)
}

/// snapmgr.c `EstimateSnapshotSpace`.
pub fn EstimateSnapshotSpace(snapshot: Snapshot) -> usize {
    let snap = snapshot.expect("EstimateSnapshotSpace: InvalidSnapshot");
    debug_assert_eq!(snap.snapshot_type, SnapshotType::Mvcc);
    let mut size = SERIALIZED_HEADER_SIZE + snap.xip.len() * std::mem::size_of::<TransactionId>();
    if serialize_subxip(&snap) {
        size += snap.subxip.len() * std::mem::size_of::<TransactionId>();
    }
    size
}

/// snapmgr.c `SerializeSnapshot`.
pub fn SerializeSnapshot(snapshot: Snapshot, start_address: &mut [u8]) {
    let snap = snapshot.expect("SerializeSnapshot: InvalidSnapshot");
    let subxcnt = if serialize_subxip(&snap) {
        snap.subxip.len() as i32
    } else {
        0
    };

    let hdr = SerializedSnapshotData {
        xmin: snap.xmin,
        xmax: snap.xmax,
        xcnt: snap.xip.len() as u32,
        subxcnt,
        suboverflowed: snap.suboverflowed,
        taken_during_recovery: snap.taken_during_recovery,
        curcid: snap.curcid,
    };
    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(
            (&raw const hdr).cast::<u8>(),
            SERIALIZED_HEADER_SIZE,
        )
    };
    start_address[..SERIALIZED_HEADER_SIZE].copy_from_slice(hdr_bytes);

    let mut off = SERIALIZED_HEADER_SIZE;
    for x in &snap.xip {
        start_address[off..off + 4].copy_from_slice(&x.0.to_ne_bytes());
        off += 4;
    }
    if subxcnt > 0 {
        for x in &snap.subxip {
            start_address[off..off + 4].copy_from_slice(&x.0.to_ne_bytes());
            off += 4;
        }
    }
}

/// snapmgr.c `RestoreSnapshot`.
pub fn RestoreSnapshot(start_address: &[u8]) -> Snapshot {
    // start_address is &[u8] (no alignment guarantee); read_unaligned avoids the
    // unaligned-deref UB. Fields are all Copy, so this is identical to the C overlay.
    let hdr = unsafe { start_address.as_ptr().cast::<SerializedSnapshotData>().read_unaligned() };
    let mut snap = blank_mvcc();
    snap.xmin = hdr.xmin;
    snap.xmax = hdr.xmax;
    snap.suboverflowed = hdr.suboverflowed;
    snap.taken_during_recovery = hdr.taken_during_recovery;
    snap.curcid = hdr.curcid;
    snap.copied = true;

    let mut off = SERIALIZED_HEADER_SIZE;
    for _ in 0..hdr.xcnt {
        snap.xip.push(TransactionId(u32::from_ne_bytes(
            start_address[off..off + 4].try_into().unwrap(),
        )));
        off += 4;
    }
    for _ in 0..hdr.subxcnt.max(0) {
        snap.subxip.push(TransactionId(u32::from_ne_bytes(
            start_address[off..off + 4].try_into().unwrap(),
        )));
        off += 4;
    }

    // Restored snapshot starts with regd_count=0 (the caller registers it),
    // matching C `RestoreSnapshot`.
    let arc = Arc::new(snap);
    SNAPMGR.with(|s| {
        s.borrow_mut().registered.push(RegisteredSnapshot {
            snap: arc.clone(),
            regd_count: 0,
        });
    });
    Some(arc)
}

// ---------------------------------------------------------------------------
// GlobalVisTest* family (delegates to procarray, 14b)
// ---------------------------------------------------------------------------

/// snapmgr.c/procarray.c `GlobalVisTestFor`.
pub fn GlobalVisTestFor(
    shared: &Arc<SharedState>,
    rel: Option<&crate::utils::relcache::RelationData>,
) -> GlobalVisState {
    shared.proc_array().global_vis_test_for(rel)
}

/// procarray.c `GlobalVisTestIsRemovableXid`.
pub fn GlobalVisTestIsRemovableXid(
    shared: &Arc<SharedState>,
    state: &GlobalVisState,
    xid: TransactionId,
) -> bool {
    shared
        .proc_array()
        .global_vis_test_is_removable_xid(shared.variable_cache(), state, xid)
}

/// procarray.c `GlobalVisTestIsRemovableFullXid`.
pub fn GlobalVisTestIsRemovableFullXid(
    shared: &Arc<SharedState>,
    state: &GlobalVisState,
    fxid: FullTransactionId,
) -> bool {
    shared
        .proc_array()
        .global_vis_test_is_removable_full_xid(shared.variable_cache(), state, fxid)
}

/// procarray.c `GlobalVisCheckRemovableXid`.
pub fn GlobalVisCheckRemovableXid(
    shared: &Arc<SharedState>,
    rel: Option<&crate::utils::relcache::RelationData>,
    xid: TransactionId,
) -> bool {
    let pa = shared.proc_array();
    let state = pa.global_vis_test_for(rel);
    pa.global_vis_test_is_removable_xid(shared.variable_cache(), &state, xid)
}

/// procarray.c `GlobalVisCheckRemovableFullXid`.
pub fn GlobalVisCheckRemovableFullXid(
    shared: &Arc<SharedState>,
    rel: Option<&crate::utils::relcache::RelationData>,
    fxid: FullTransactionId,
) -> bool {
    let pa = shared.proc_array();
    let state = pa.global_vis_test_for(rel);
    pa.global_vis_test_is_removable_full_xid(shared.variable_cache(), &state, fxid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    fn test_shared() -> Arc<SharedState> {
        let dir = std::env::temp_dir().join(format!("pepperdb-snapmgr-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn first_transaction_snapshot_sets_flag() {
        let shared = test_shared();
        snapmgr_scope(async {
            assert!(!first_snapshot_set());
            let snap = GetTransactionSnapshot(&shared);
            assert!(snap.is_some());
            assert!(first_snapshot_set());
        })
        .await;
    }

    #[tokio::test]
    async fn active_stack_push_pop() {
        let shared = test_shared();
        snapmgr_scope(async {
            assert!(!ActiveSnapshotSet());
            let s = GetTransactionSnapshot(&shared);
            PushActiveSnapshot(s);
            assert!(ActiveSnapshotSet());
            assert!(GetActiveSnapshot().is_some());
            PopActiveSnapshot();
            assert!(!ActiveSnapshotSet());
        })
        .await;
    }

    // B1 regression: two snapshots can be held live simultaneously. With the
    // old `&'static mut` lending this aliased one buffer (UB); with Arc each
    // holder owns a refcount, so this is sound.
    #[tokio::test]
    async fn two_snapshots_held_live_is_sound() {
        let shared = test_shared();
        snapmgr_scope(async {
            let a = GetTransactionSnapshot(&shared).unwrap();
            let b = GetLatestSnapshot(&shared).unwrap();
            // Both readable at once; distinct Arc handles, no aliasing &mut.
            assert!(!Arc::ptr_eq(&a, &b));
            let _ = (a.xmin, b.xmin);
            assert!(Arc::strong_count(&a) >= 1);
        })
        .await;
    }

    #[tokio::test]
    async fn register_unregister_accounting() {
        let shared = test_shared();
        snapmgr_scope(async {
            let s = GetTransactionSnapshot(&shared);
            let reg = RegisterSnapshot(s);
            assert!(reg.is_some());
            let count = SNAPMGR.with(|st| st.borrow().registered.len());
            assert_eq!(count, 1);
            UnregisterSnapshot(reg);
            let count = SNAPMGR.with(|st| st.borrow().registered.len());
            assert_eq!(count, 0);
        })
        .await;
    }

    #[tokio::test]
    async fn at_eoxact_resets() {
        let shared = test_shared();
        snapmgr_scope(async {
            let _ = GetTransactionSnapshot(&shared);
            assert!(first_snapshot_set());
            AtEOXact_Snapshot(true, true);
            assert!(!first_snapshot_set());
            assert!(!ActiveSnapshotSet());
        })
        .await;
    }
}
