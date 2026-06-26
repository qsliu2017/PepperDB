//! Translated from PostgreSQL src/backend/storage/ipc/procarray.c
//!
//! The process array: snapshot building (`GetSnapshotData`), xid horizons
//! (`ComputeXidHorizons` + the `GlobalVisState` family), running-xact tests
//! (`TransactionIdIsInProgress`/`IsActive`), proc add/remove/end-transaction,
//! and the hot-standby `KnownAssignedXids` machinery.
//!
//! Concurrency mapping (rules s9): PG's `ProcArrayLock` (an LWLock) becomes a
//! `RwLock<ProcArrayInner>` wrapping the data the lock protected -- the
//! `pgprocnos` offset array, the KnownAssignedXids state, and the
//! replication-slot xmins. Snapshots take the read (shared) side; add/remove/
//! end-transaction take the write (exclusive) side. The whole subsystem is
//! in-memory computation: NO `.await` is ever held across a procarray guard
//! (procarray is the one MVCC subsystem that is almost entirely synchronous).
//! The few functions that consult clog/subtrans (`TransactionIdIsInProgress`
//! step 4, recovery apply) drop the guard first, exactly as PG releases
//! ProcArrayLock before the pg_subtrans probe.
//!
//! Staging (design step14 s0): `proc.c` / `InitProcGlobal` / the real PGPROC
//! array land in step 15. We translate procarray.c IN FULL now, but its scans
//! run over the existing `ProcGlobal` stub (`src/storage/proc.rs`), which is an
//! empty/unpopulated `PROC_HDR`. So `GetSnapshotData` etc. compile and are
//! logically complete; they see an empty array until step 15 populates it.
//! `MyProc` (the current backend's PGPROC) is likewise a step-15 stub -- where
//! C reads `MyProc->pgxactoff` / `MyProc->xid` we treat "no MyProc yet" as a
//! backend with no advertised xid (the common read-only case), which is the
//! correct behavior for an unpopulated array.
//!
//! Per-task `TransactionXmin`/`RecentXmin` (PG process globals): set by
//! `GetSnapshotData` and read by visibility code. They become a per-task
//! `task_local` here (the GetSnapshotData writer owns them); snapmgr.c (step
//! 14c) reaches them through the accessors below. See rules s6.1 / design s7.

#![allow(clippy::needless_range_loop)]

use std::cell::Cell;
use std::sync::{Arc, RwLock};

use crate::access::transam::{
    full_transaction_id_advance, full_transaction_id_newer, transaction_id_advance,
    transaction_id_is_normal, transaction_id_is_valid, transaction_id_older,
    xid_from_full_transaction_id, FullTransactionId, INVALID_FULL_TRANSACTION_ID,
};
use crate::backend::access::transam::transam::{
    transaction_id_follows, transaction_id_follows_or_equals, transaction_id_latest,
    transaction_id_precedes, transaction_id_precedes_or_equals,
};
use crate::c::{Size, TransactionId};
use crate::postgres_ext::Oid;
use crate::storage::lock::VirtualTransactionId;
use crate::storage::proc::{PGPROC, PGPROC_MAX_CACHED_SUBXIDS};
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};
use crate::storage::standby::{RunningTransactionsData, SubxidsArrayStatus};
use crate::shared_state::SharedState;
use crate::utils::relcache::RelationData;
use crate::utils::snapshot::{GlobalVisState, SnapshotData};

const INVALID_XID: TransactionId = crate::access::transam::INVALID_TRANSACTION_ID;

// ---------------------------------------------------------------------------
// per-task TransactionXmin / RecentXmin (PG backend globals -> task_local)
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// Oldest xmin of any snapshot in use in the current transaction (PG
    /// `TransactionXmin`; == MyProc->xmin). Owner-only Cell: only the owning
    /// task reads/writes it, never across an `.await`.
    static TRANSACTION_XMIN: Cell<TransactionId>;
    /// xmin computed for the most recent snapshot (PG `RecentXmin`).
    static RECENT_XMIN: Cell<TransactionId>;
}

/// PG `TransactionXmin`. Returns InvalidTransactionId outside a backend scope.
pub fn transaction_xmin() -> TransactionId {
    TRANSACTION_XMIN.try_with(|c| c.get()).unwrap_or(INVALID_XID)
}

/// PG `RecentXmin`. Returns InvalidTransactionId outside a backend scope.
pub fn recent_xmin() -> TransactionId {
    RECENT_XMIN.try_with(|c| c.get()).unwrap_or(INVALID_XID)
}

fn set_transaction_xmin(xid: TransactionId) {
    let _ = TRANSACTION_XMIN.try_with(|c| c.set(xid));
}

/// Public setter for snapmgr's `SnapshotResetXmin` (PG sets
/// `MyProc->xmin = TransactionXmin = ...`). MyProc->xmin lands in step 15.
pub fn set_transaction_xmin_public(xid: TransactionId) {
    set_transaction_xmin(xid);
    // TODO(step15): also set MyProc->xmin once the proc array is populated.
}

fn set_recent_xmin(xid: TransactionId) {
    let _ = RECENT_XMIN.try_with(|c| c.set(xid));
}

/// Publish per-task `TransactionXmin`/`RecentXmin` for `f` (backend entry).
pub async fn snapshot_globals_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    TRANSACTION_XMIN
        .scope(
            Cell::new(INVALID_XID),
            RECENT_XMIN.scope(Cell::new(INVALID_XID), f),
        )
        .await
}

// ---------------------------------------------------------------------------
// GlobalVisState bodies (procarray.c owns the real shape)
// ---------------------------------------------------------------------------

/// procarray.c `ComputeXidHorizonsResult`.
#[derive(Clone, Copy)]
pub struct ComputeXidHorizonsResult {
    pub latest_completed: FullTransactionId,
    pub slot_xmin: TransactionId,
    pub slot_catalog_xmin: TransactionId,
    pub oldest_considered_running: TransactionId,
    pub shared_oldest_nonremovable: TransactionId,
    pub shared_oldest_nonremovable_raw: TransactionId,
    pub catalog_oldest_nonremovable: TransactionId,
    pub data_oldest_nonremovable: TransactionId,
    pub temp_oldest_nonremovable: TransactionId,
}

/// procarray.c `GlobalVisHorizonKind`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GlobalVisHorizonKind {
    Shared,
    Catalog,
    Data,
    Temp,
}

/// procarray.c `KAXCompressReason`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KaxCompressReason {
    NoSpace,
    Prune,
    TransactionEnd,
    StartupProcessIdle,
}

// ---------------------------------------------------------------------------
// ProcArrayInner: the data ProcArrayLock protected (rules s9)
// ---------------------------------------------------------------------------

/// procarray.c `ProcArrayStruct` plus the parallel `KnownAssignedXids[]` /
/// `KnownAssignedXidsValid[]` arrays. Wrapped by `ProcArray`'s `RwLock`.
struct ProcArrayInner {
    num_procs: usize,
    max_procs: usize,

    // Known-assigned-xids (hot standby): sorted ring with a parallel valid[].
    max_known_assigned_xids: usize,
    num_known_assigned_xids: usize,
    tail_known_assigned_xids: usize,
    head_known_assigned_xids: usize,
    known_assigned_xids: Vec<TransactionId>,
    known_assigned_xids_valid: Vec<bool>,

    last_overflowed_xid: TransactionId,
    replication_slot_xmin: TransactionId,
    replication_slot_catalog_xmin: TransactionId,

    /// procarray.c `pgprocnos[]`: indexes into ProcGlobal->allProcs, kept sorted.
    pgprocnos: Vec<ProcNumber>,
}

impl ProcArrayInner {
    fn new(max_procs: usize, total_max_cached_subxids: usize) -> Self {
        ProcArrayInner {
            num_procs: 0,
            max_procs,
            max_known_assigned_xids: total_max_cached_subxids,
            num_known_assigned_xids: 0,
            tail_known_assigned_xids: 0,
            head_known_assigned_xids: 0,
            known_assigned_xids: vec![INVALID_XID; total_max_cached_subxids],
            known_assigned_xids_valid: vec![false; total_max_cached_subxids],
            last_overflowed_xid: INVALID_XID,
            replication_slot_xmin: INVALID_XID,
            replication_slot_catalog_xmin: INVALID_XID,
            pgprocnos: Vec::with_capacity(max_procs),
        }
    }
}

/// procarray.c `GlobalVis{Shared,Catalog,Data,Temp}Rels`. Backend-lifetime
/// approximate horizons; in PG these are per-process statics. We share them on
/// `ProcArray` behind a `Mutex` (they are updated by GetSnapshotData /
/// ComputeXidHorizons and read by GlobalVisTest*). The xids are
/// FullTransactionIds to dodge wraparound (see struct GlobalVisState comment).
struct GlobalVisStates {
    shared: GlobalVisState,
    catalog: GlobalVisState,
    data: GlobalVisState,
    temp: GlobalVisState,
    /// procarray.c `ComputeXidHorizonsResultLastXmin`.
    last_xmin: TransactionId,
}

impl GlobalVisStates {
    fn new() -> Self {
        let z = GlobalVisState {
            definitely_needed: INVALID_FULL_TRANSACTION_ID,
            maybe_needed: INVALID_FULL_TRANSACTION_ID,
        };
        GlobalVisStates {
            shared: z,
            catalog: z,
            data: z,
            temp: z,
            last_xmin: INVALID_XID,
        }
    }
}

/// The process array (ex-shmem `ProcArrayStruct`). `Arc<ProcArray>` on
/// `SharedState`; `ProcArrayLock` is the `inner` `RwLock` (rules s9).
pub struct ProcArray {
    inner: RwLock<ProcArrayInner>,
    /// The approximate GlobalVis horizons (procarray.c statics).
    vis: std::sync::Mutex<GlobalVisStates>,
}

impl ProcArray {
    /// procarray.c `ProcArrayShmemInit`: build the empty array. `max_procs` is
    /// PG's `PROCARRAY_MAXPROCS` (MaxBackends + max_prepared_xacts).
    pub fn new(max_procs: usize) -> Self {
        let total = total_max_cached_subxids(max_procs);
        ProcArray {
            inner: RwLock::new(ProcArrayInner::new(max_procs, total)),
            vis: std::sync::Mutex::new(GlobalVisStates::new()),
        }
    }
}

/// procarray.c `TOTAL_MAX_CACHED_SUBXIDS`.
fn total_max_cached_subxids(max_procs: usize) -> usize {
    (PGPROC_MAX_CACHED_SUBXIDS + 1) * max_procs
}

// ---------------------------------------------------------------------------
// ProcArrayShmemSize / construction
// ---------------------------------------------------------------------------

/// procarray.c `ProcArrayShmemSize`: bytes the array would occupy. Under the
/// Arc model nothing is allocated from a segment; this is an estimate.
pub fn proc_array_shmem_size(max_procs: usize) -> Size {
    let total = total_max_cached_subxids(max_procs);
    std::mem::size_of::<ProcArrayInner>()
        + max_procs * std::mem::size_of::<ProcNumber>()
        + total * (std::mem::size_of::<TransactionId>() + std::mem::size_of::<bool>())
}

/// procarray.c `ProcArrayShmemInit`: build the shared `ProcArray`. Wired into
/// `SharedState::new` at the ProcArrayShmemInit marker.
pub fn proc_array_shmem_init(max_procs: usize) -> Arc<ProcArray> {
    Arc::new(ProcArray::new(max_procs))
}

// ===========================================================================
// ProcArrayAdd / Remove / EndTransaction
//
// Staging: these mutate the ProcGlobal mirror arrays (xids[]/subxidStates[]/
// statusFlags[]) and PGPROC.pgxactoff, all of which proc.c (step 15) owns. We
// translate the pgprocnos bookkeeping in full; the ProcGlobal mirror writes
// land on the step-15 stub (ProcGlobal is empty), which is correct staging.
// ===========================================================================

/// procarray.c `ProcArrayAdd`: insert a proc keeping `pgprocnos` sorted by
/// proc number. (PG holds ProcArrayLock + XidGenLock exclusively; here the
/// write guard suffices.)
pub fn proc_array_add(shared: &Arc<SharedState>, pgprocno: ProcNumber) {
    let mut a = shared.proc_array().inner.write().unwrap();
    if a.num_procs >= a.max_procs {
        // TODO(panic): ereport(FATAL, too many clients). Step 15 wires FATAL.
        panic!("sorry, too many clients already");
    }
    let index = a.pgprocnos.partition_point(|&p| p <= pgprocno);
    a.pgprocnos.insert(index, pgprocno);
    a.num_procs += 1;
    // ProcGlobal mirror writes (xids/subxidStates/statusFlags) + pgxactoff are
    // owned by step 15; the array stays sorted regardless.
}

/// procarray.c `ProcArrayRemove`: drop a proc; if `latest_xid` is valid this is
/// a live 2PC gxact going away, so advance latestCompletedXid.
pub fn proc_array_remove(shared: &Arc<SharedState>, pgprocno: ProcNumber, latest_xid: TransactionId) {
    let pa = shared.proc_array();
    let mut a = pa.inner.write().unwrap();
    if transaction_id_is_valid(latest_xid) {
        maintain_latest_completed_xid(shared, latest_xid);
        shared.variable_cache().with(|v| v.xact_completion_count += 1);
    }
    if let Some(pos) = a.pgprocnos.iter().position(|&p| p == pgprocno) {
        a.pgprocnos.remove(pos);
        a.num_procs -= 1;
    }
}

/// procarray.c `ProcArrayEndTransaction`: mark a transaction no longer running.
/// The commit/abort must already be in WAL + pg_xact. Group XID clearing
/// (`ProcArrayGroupClearXid`) is replaced by a direct clear under the write
/// guard (TODO(perf): re-add the group optimization once there is contention).
pub fn proc_array_end_transaction(
    shared: &Arc<SharedState>,
    proc: &mut PGPROC,
    latest_xid: TransactionId,
) {
    if transaction_id_is_valid(latest_xid) {
        let pa = shared.proc_array();
        let _a = pa.inner.write().unwrap();
        proc_array_end_transaction_internal(shared, proc, latest_xid);
    } else {
        // No XID: no need to lock to clear our own non-shared bookkeeping.
        proc.vxid.lxid = crate::c::LocalTransactionId(0);
        proc.xmin = INVALID_XID;
        proc.delay_chkpt_flags = crate::storage::proc::DelayChkptFlags::empty();
        proc.recovery_conflict_pending = false;
        if proc
            .status_flags
            .intersects(crate::storage::proc::ProcStatusFlags::PROC_VACUUM_STATE_MASK)
        {
            let pa = shared.proc_array();
            let _a = pa.inner.write().unwrap();
            proc.status_flags
                .remove(crate::storage::proc::ProcStatusFlags::PROC_VACUUM_STATE_MASK);
            // ProcGlobal->statusFlags[pgxactoff] mirror: step 15.
        }
    }
}

/// procarray.c `ProcArrayEndTransactionInternal`: clear a write transaction's
/// advertised xid/xmin/subxids and advance latestCompletedXid. Caller holds the
/// write guard.
fn proc_array_end_transaction_internal(
    shared: &Arc<SharedState>,
    proc: &mut PGPROC,
    latest_xid: TransactionId,
) {
    proc.xid = INVALID_XID;
    proc.vxid.lxid = crate::c::LocalTransactionId(0);
    proc.xmin = INVALID_XID;
    proc.delay_chkpt_flags = crate::storage::proc::DelayChkptFlags::empty();
    proc.recovery_conflict_pending = false;
    proc.status_flags
        .remove(crate::storage::proc::ProcStatusFlags::PROC_VACUUM_STATE_MASK);
    if proc.subxid_status.count > 0 || proc.subxid_status.overflowed {
        proc.subxid_status.count = 0;
        proc.subxid_status.overflowed = false;
    }
    // ProcGlobal mirror writes (xids/subxidStates/statusFlags): step 15.
    maintain_latest_completed_xid(shared, latest_xid);
    shared.variable_cache().with(|v| v.xact_completion_count += 1);
}

/// procarray.c `ProcArrayClearTransaction`: clear our own PGPROC after a
/// successful PREPARE (the gxact still represents us in the array).
pub fn proc_array_clear_transaction(shared: &Arc<SharedState>, proc: &mut PGPROC) {
    let pa = shared.proc_array();
    let _a = pa.inner.write().unwrap();
    proc.xid = INVALID_XID;
    proc.vxid.lxid = crate::c::LocalTransactionId(0);
    proc.xmin = INVALID_XID;
    proc.recovery_conflict_pending = false;
    shared.variable_cache().with(|v| v.xact_completion_count += 1);
    if proc.subxid_status.count > 0 || proc.subxid_status.overflowed {
        proc.subxid_status.count = 0;
        proc.subxid_status.overflowed = false;
    }
}

/// procarray.c `MaintainLatestCompletedXid`: bump latestCompletedXid to
/// `latest_xid` if older. Caller holds the write guard (so the read-modify-write
/// of latestCompletedXid is atomic wrt other procarray mutators).
fn maintain_latest_completed_xid(shared: &Arc<SharedState>, latest_xid: TransactionId) {
    shared.variable_cache().with(|v| {
        let cur = v.latest_completed_xid;
        if transaction_id_precedes(xid_from_full_transaction_id(cur), latest_xid) {
            v.latest_completed_xid = full_xid_relative_to(cur, latest_xid);
        }
    });
}

/// procarray.c `MaintainLatestCompletedXidRecovery`: same, for WAL replay
/// (latestCompletedXid may be uninitialized; relative to nextXid).
fn maintain_latest_completed_xid_recovery(shared: &Arc<SharedState>, latest_xid: TransactionId) {
    shared.variable_cache().with(|v| {
        let cur = v.latest_completed_xid;
        let rel = v.next_xid;
        if !crate::access::transam::full_transaction_id_is_valid(cur)
            || transaction_id_precedes(xid_from_full_transaction_id(cur), latest_xid)
        {
            v.latest_completed_xid = full_xid_relative_to(rel, latest_xid);
        }
    });
}

// ===========================================================================
// Recovery: ProcArrayInitRecovery / ApplyRecoveryInfo / ApplyXidAssignment
//
// Translated in full; exercised only under recovery (step out-of-foundation).
// standbyState / StandbyReleaseOldLocks / StandbyTransactionIdIsPrepared land
// on existing stubs. clog/subtrans probes are async, so these functions are
// async; the ProcArray write guard is dropped before each await.
// ===========================================================================

/// procarray.c `ProcArrayInitRecovery`.
pub fn proc_array_init_recovery(initialized_upto_xid: TransactionId) {
    let mut x = initialized_upto_xid;
    crate::access::transam::transaction_id_retreat(&mut x);
    set_latest_observed_xid(x);
}

/// procarray.c `ProcArrayApplyRecoveryInfo`. Apply running-xact info from the
/// primary to KnownAssignedXids. Async: walks clog/subtrans for completed xids.
pub async fn proc_array_apply_recovery_info(
    shared: &Arc<SharedState>,
    running: &RunningTransactionsData,
) {
    expire_old_known_assigned_transaction_ids(shared, running.oldest_running_xid);

    let mut advance_next = running.next_xid;
    crate::access::transam::transaction_id_retreat(&mut advance_next);
    crate::backend::access::transam::varsup::AdvanceNextFullTransactionIdPastXid(
        shared,
        advance_next,
    );

    // StandbyReleaseOldLocks(running.oldest_running_xid): standby stub.

    // The full STANDBY_SNAPSHOT_PENDING/READY state machine (standbyState) is
    // owned by the recovery driver (out of foundation). We implement the
    // KnownAssignedXids population, which is the procarray-owned part.
    let total = running.xcnt as usize + running.subxcnt as usize;
    let mut xids: Vec<TransactionId> = Vec::with_capacity(total);
    for i in 0..total {
        let xid = running.xids[i];
        // Skip xids already completed (DidCommit/DidAbort consult clog/subtrans).
        let txmin = transaction_xmin();
        let committed =
            crate::backend::access::transam::transam::transaction_id_did_commit(shared, xid, txmin)
                .await;
        let aborted =
            crate::backend::access::transam::transam::transaction_id_did_abort(shared, xid, txmin)
                .await;
        if committed || aborted {
            continue;
        }
        xids.push(xid);
    }

    if !xids.is_empty() {
        // Sort logically (all same-epoch normal xids from RUNNING_XACTS).
        xids.sort_by(|a, b| {
            if transaction_id_precedes(*a, *b) {
                std::cmp::Ordering::Less
            } else if transaction_id_precedes(*b, *a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        let pa = shared.proc_array();
        let mut a = pa.inner.write().unwrap();
        let mut prev: Option<TransactionId> = None;
        for &xid in &xids {
            if prev == Some(xid) {
                continue; // dup from prepared xacts
            }
            known_assigned_xids_add(&mut a, xid, xid, true);
            prev = Some(xid);
        }
    }

    // Initialize subtrans gaplessly up to nextXid - 1 (ExtendSUBTRANS is async).
    let mut latest = latest_observed_xid();
    if transaction_id_is_normal(latest) {
        transaction_id_advance(&mut latest);
        while transaction_id_precedes(latest, running.next_xid) {
            crate::backend::access::transam::subtrans::extend_subtrans(shared, latest).await;
            transaction_id_advance(&mut latest);
        }
        crate::access::transam::transaction_id_retreat(&mut latest);
        set_latest_observed_xid(latest);
    }

    // lastOverflowedXid + latestCompletedXid (subxid_status drives overflow).
    {
        let pa = shared.proc_array();
        let mut a = pa.inner.write().unwrap();
        match running.subxid_status {
            SubxidsArrayStatus::Missing => {
                a.last_overflowed_xid = latest_observed_xid();
            }
            SubxidsArrayStatus::InSubtrans => {
                a.last_overflowed_xid = latest_observed_xid();
            }
            SubxidsArrayStatus::InArray => {
                a.last_overflowed_xid = INVALID_XID;
            }
        }
    }
    maintain_latest_completed_xid_recovery(shared, running.latest_completed_xid);
}

/// procarray.c `ProcArrayApplyXidAssignment`: process XLOG_XACT_ASSIGNMENT.
pub async fn proc_array_apply_xid_assignment(
    shared: &Arc<SharedState>,
    topxid: TransactionId,
    subxids: &[TransactionId],
) {
    let max_xid = transaction_id_latest(topxid, subxids);
    record_known_assigned_transaction_ids(shared, max_xid).await;

    for &s in subxids {
        crate::backend::access::transam::subtrans::sub_trans_set_parent(shared, s, topxid).await;
    }

    {
        let pa = shared.proc_array();
        let mut a = pa.inner.write().unwrap();
        known_assigned_xids_remove_tree(&mut a, INVALID_XID, subxids);
        if transaction_id_precedes(a.last_overflowed_xid, max_xid) {
            a.last_overflowed_xid = max_xid;
        }
    }
}

// ===========================================================================
// TransactionIdIsInProgress / IsActive
// ===========================================================================

/// procarray.c `TransactionIdIsInProgress`. Async because step 4 may consult
/// clog (DidAbort) + subtrans (topmost). The ProcArrayLock-held steps 1-3 drop
/// the read guard before the (async) step 4, exactly as PG does.
pub async fn transaction_id_is_in_progress(
    shared: &Arc<SharedState>,
    xid: TransactionId,
) -> bool {
    // Older than RecentXmin: cannot be running (also rejects invalid/permanent).
    if transaction_id_precedes(xid, recent_xmin()) {
        return false;
    }

    // (The single-item cachedXidIsNotInProgress cache and the
    // TransactionIdIsCurrentTransactionId fast paths are xact.c concerns
    // (step 14d); omitted here -- they only ever return early, never wrongly.)

    let mut top_xids: Vec<TransactionId> = Vec::new();
    {
        let pa = shared.proc_array();
        let a = pa.inner.read().unwrap();

        let latest_completed = shared
            .variable_cache()
            .with(|v| xid_from_full_transaction_id(v.latest_completed_xid));
        if transaction_id_precedes(latest_completed, xid) {
            return true;
        }

        // Scan ProcGlobal->xids[] / subxids (steps 1-2; empty until step 15).
        let found = with_proc_globals(shared, |g| {
            let n = a.pgprocnos.len().min(g.xids.len());
            for off in 0..n {
                let pxid = g.xids[off];
                if !transaction_id_is_valid(pxid) {
                    continue;
                }
                // Step 1: the main Xid.
                if pxid.0 == xid.0 {
                    return true;
                }
                // Younger main Xids can't be xid's parent.
                if transaction_id_precedes(xid, pxid) {
                    continue;
                }
                // Step 2: the cached child Xids.
                let pgprocno = a.pgprocnos[off];
                if let Some(proc) = g.proc(pgprocno) {
                    let pxids = proc.subxid_status.count as usize;
                    for j in (0..pxids).rev() {
                        if proc.subxids.xids[j].0 == xid.0 {
                            return true;
                        }
                    }
                }
                // Remember main Xids with uncached (overflowed) children for step 4.
                if off < g.subxid_states.len() && g.subxid_states[off].overflowed {
                    top_xids.push(pxid);
                }
            }
            false
        });
        if found {
            return true;
        }

        // Step 3: recovery KnownAssignedXids check (empty in normal running).
        if crate::access::transam::TransactionStartedDuringRecovery() {
            if known_assigned_xid_exists(&a, xid) {
                return true;
            }
            if transaction_id_precedes_or_equals(xid, a.last_overflowed_xid) {
                top_xids = known_assigned_xids_get(&a, xid);
            }
        }
    } // drop read guard before clog/subtrans

    if top_xids.is_empty() {
        return false;
    }

    // Step 4: consult pg_subtrans / pg_xact (async).
    let txmin = transaction_xmin();
    if crate::backend::access::transam::transam::transaction_id_did_abort(shared, xid, txmin).await {
        return false;
    }
    let topxid = crate::backend::access::transam::subtrans::sub_trans_get_topmost_transaction(
        shared, xid, txmin,
    )
    .await;
    if topxid.0 != xid.0 && top_xids.iter().any(|t| t.0 == topxid.0) {
        return true;
    }
    false
}

/// procarray.c `TransactionIdIsActive`: is `xid` the top-level XID of an active
/// (non-prepared, non-standby) backend?
pub fn transaction_id_is_active(shared: &Arc<SharedState>, xid: TransactionId) -> bool {
    if transaction_id_precedes(xid, recent_xmin()) {
        return false;
    }
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        let n = a.pgprocnos.len().min(g.xids.len());
        for i in 0..n {
            let pxid = g.xids[i];
            if !transaction_id_is_valid(pxid) {
                continue;
            }
            let pgprocno = a.pgprocnos[i];
            if let Some(proc) = g.proc(pgprocno) {
                if proc.pid == 0 {
                    continue; // prepared xact
                }
                if pxid.0 == xid.0 {
                    return true;
                }
            }
        }
        false
    })
}

// ===========================================================================
// XID horizons: ComputeXidHorizons + GlobalVis*
// ===========================================================================

/// procarray.c `ComputeXidHorizons`. Sync: all in-memory. Takes the read guard,
/// scans the array, fetches slot horizons, drops the guard, finishes the
/// arithmetic, then updates the approximate GlobalVis horizons.
fn compute_xid_horizons(shared: &Arc<SharedState>) -> ComputeXidHorizonsResult {
    let in_recovery = crate::access::transam::TransactionStartedDuringRecovery();
    let my_database_id = crate::session::try_current()
        .map(|s| s.database_id())
        .unwrap_or(crate::postgres_ext::InvalidOid);

    let (latest_completed, oldest_xid) = shared
        .variable_cache()
        .with(|v| (v.latest_completed_xid, v.oldest_xid));

    let mut initial = xid_from_full_transaction_id(latest_completed);
    transaction_id_advance(&mut initial);

    let mut h = ComputeXidHorizonsResult {
        latest_completed,
        slot_xmin: INVALID_XID,
        slot_catalog_xmin: INVALID_XID,
        oldest_considered_running: initial,
        shared_oldest_nonremovable: initial,
        shared_oldest_nonremovable_raw: INVALID_XID,
        catalog_oldest_nonremovable: INVALID_XID,
        data_oldest_nonremovable: initial,
        temp_oldest_nonremovable: initial,
    };

    let kaxmin;
    {
        let pa = shared.proc_array();
        let a = pa.inner.read().unwrap();

        // temp horizon: only this backend's own xid matters.
        let myxid = my_proc_xid(shared);
        h.temp_oldest_nonremovable = if transaction_id_is_valid(myxid) {
            myxid
        } else {
            initial
        };

        h.slot_xmin = a.replication_slot_xmin;
        h.slot_catalog_xmin = a.replication_slot_catalog_xmin;

        with_proc_globals(shared, |g| {
            let n = a.pgprocnos.len().min(g.xids.len());
            for index in 0..n {
                let pgprocno = a.pgprocnos[index];
                let status_flags = g
                    .status_flags
                    .get(index)
                    .copied()
                    .map(crate::storage::proc::ProcStatusFlags::from_bits_truncate)
                    .unwrap_or_else(crate::storage::proc::ProcStatusFlags::empty);
                let xid = g.xids[index];
                let xmin = g.proc(pgprocno).map(|p| p.xmin).unwrap_or(INVALID_XID);

                let xmin = transaction_id_older(xmin, xid);
                if !transaction_id_is_valid(xmin) {
                    continue;
                }
                h.oldest_considered_running =
                    transaction_id_older(h.oldest_considered_running, xmin);

                if status_flags.intersects(
                    crate::storage::proc::ProcStatusFlags::PROC_IN_VACUUM
                        | crate::storage::proc::ProcStatusFlags::PROC_IN_LOGICAL_DECODING,
                ) {
                    continue;
                }
                h.shared_oldest_nonremovable =
                    transaction_id_older(h.shared_oldest_nonremovable, xmin);

                let proc_db = g.proc(pgprocno).map(|p| p.database_id).unwrap_or(Oid(0));
                if proc_db == my_database_id
                    || my_database_id == crate::postgres_ext::InvalidOid
                    || status_flags
                        .contains(crate::storage::proc::ProcStatusFlags::PROC_AFFECTS_ALL_HORIZONS)
                    || in_recovery
                {
                    h.data_oldest_nonremovable =
                        transaction_id_older(h.data_oldest_nonremovable, xmin);
                }
            }
        });

        kaxmin = if in_recovery {
            known_assigned_xids_get_oldest_xmin(&a)
        } else {
            INVALID_XID
        };
    } // drop read guard

    if in_recovery {
        h.oldest_considered_running = transaction_id_older(h.oldest_considered_running, kaxmin);
        h.shared_oldest_nonremovable = transaction_id_older(h.shared_oldest_nonremovable, kaxmin);
        h.data_oldest_nonremovable = transaction_id_older(h.data_oldest_nonremovable, kaxmin);
    }

    h.shared_oldest_nonremovable =
        transaction_id_older(h.shared_oldest_nonremovable, h.slot_xmin);
    h.data_oldest_nonremovable = transaction_id_older(h.data_oldest_nonremovable, h.slot_xmin);

    h.shared_oldest_nonremovable_raw = h.shared_oldest_nonremovable;
    h.shared_oldest_nonremovable =
        transaction_id_older(h.shared_oldest_nonremovable, h.slot_catalog_xmin);
    h.catalog_oldest_nonremovable = h.data_oldest_nonremovable;
    h.catalog_oldest_nonremovable =
        transaction_id_older(h.catalog_oldest_nonremovable, h.slot_catalog_xmin);

    h.oldest_considered_running =
        transaction_id_older(h.oldest_considered_running, h.shared_oldest_nonremovable);
    h.oldest_considered_running =
        transaction_id_older(h.oldest_considered_running, h.catalog_oldest_nonremovable);
    h.oldest_considered_running =
        transaction_id_older(h.oldest_considered_running, h.data_oldest_nonremovable);

    global_vis_update_apply(shared, &h);
    h
}

/// procarray.c `GlobalVisHorizonKindForRel`. Conservative staging: the rel-kind
/// inspection (relisshared / IsCatalogRelation / RELATION_IS_LOCAL) needs
/// relcache internals that are stubs, so we use the most conservative SHARED
/// horizon for any relation. TODO(relcache): refine per-rel once rd_rel lands.
fn global_vis_horizon_kind_for_rel(_rel: Option<&RelationData>) -> GlobalVisHorizonKind {
    GlobalVisHorizonKind::Shared
}

/// procarray.c `GetOldestNonRemovableTransactionId`.
pub fn get_oldest_non_removable_transaction_id(
    shared: &Arc<SharedState>,
    rel: Option<&RelationData>,
) -> TransactionId {
    let h = compute_xid_horizons(shared);
    match global_vis_horizon_kind_for_rel(rel) {
        GlobalVisHorizonKind::Shared => h.shared_oldest_nonremovable,
        GlobalVisHorizonKind::Catalog => h.catalog_oldest_nonremovable,
        GlobalVisHorizonKind::Data => h.data_oldest_nonremovable,
        GlobalVisHorizonKind::Temp => h.temp_oldest_nonremovable,
    }
}

/// procarray.c `GetOldestTransactionIdConsideredRunning`.
pub fn get_oldest_transaction_id_considered_running(shared: &Arc<SharedState>) -> TransactionId {
    compute_xid_horizons(shared).oldest_considered_running
}

/// procarray.c `GetReplicationHorizons` (out-params -> tuple).
pub fn get_replication_horizons(shared: &Arc<SharedState>) -> (TransactionId, TransactionId) {
    let h = compute_xid_horizons(shared);
    (h.shared_oldest_nonremovable_raw, h.slot_catalog_xmin)
}

/// procarray.c `GetMaxSnapshotXidCount`.
pub fn get_max_snapshot_xid_count(shared: &Arc<SharedState>) -> i32 {
    shared.proc_array().inner.read().unwrap().max_procs as i32
}

/// procarray.c `GetMaxSnapshotSubxidCount`.
pub fn get_max_snapshot_subxid_count(shared: &Arc<SharedState>) -> i32 {
    let max_procs = shared.proc_array().inner.read().unwrap().max_procs;
    total_max_cached_subxids(max_procs) as i32
}

// ===========================================================================
// GetSnapshotData
// ===========================================================================

/// procarray.c `GetSnapshotData`: fill `snapshot` with the set of running
/// transactions. Sync: takes the read guard, scans the array, computes
/// xmin/xmax/xip/subxip + suboverflowed, sets per-task TransactionXmin/
/// RecentXmin, drops the guard. NO `.await` while the guard is held.
///
/// The `GetSnapshotDataReuse` fast path (reuse the prior snapshot when
/// xactCompletionCount is unchanged) is omitted -- TODO(perf); always rebuild.
pub fn get_snapshot_data<'a>(
    shared: &Arc<SharedState>,
    snapshot: &'a mut SnapshotData,
) -> &'a mut SnapshotData {
    let mut count = 0usize;
    let mut subcount = 0usize;
    let mut suboverflowed = false;
    snapshot.xip.clear();
    snapshot.subxip.clear();

    let taken_during_recovery = crate::access::transam::TransactionStartedDuringRecovery();

    let (latest_completed, oldest_xid, cur_completion) = shared
        .variable_cache()
        .with(|v| (v.latest_completed_xid, v.oldest_xid, v.xact_completion_count));

    let myxid = my_proc_xid(shared);

    // xmax = latestCompletedXid + 1.
    let mut xmax = xid_from_full_transaction_id(latest_completed);
    transaction_id_advance(&mut xmax);

    let mut xmin = xmax;
    if transaction_id_is_normal(myxid)
        && crate::access::transam::normal_transaction_id_precedes(myxid, xmin)
    {
        xmin = myxid;
    }

    let (replication_slot_xmin, replication_slot_catalog_xmin);
    {
        let pa = shared.proc_array();
        let a = pa.inner.read().unwrap();

        if !taken_during_recovery {
            let myoff = my_proc_pgxactoff(shared);
            with_proc_globals(shared, |g| {
                let n = a.pgprocnos.len().min(g.xids.len());
                for pgxactoff in 0..n {
                    let xid = g.xids[pgxactoff];
                    if xid == INVALID_XID {
                        continue;
                    }
                    if Some(pgxactoff) == myoff {
                        continue; // own xid excluded
                    }
                    if !crate::access::transam::normal_transaction_id_precedes(xid, xmax) {
                        continue; // >= xmax => treated as running anyway
                    }
                    let status_flags = g
                        .status_flags
                        .get(pgxactoff)
                        .copied()
                        .map(crate::storage::proc::ProcStatusFlags::from_bits_truncate)
                        .unwrap_or_else(crate::storage::proc::ProcStatusFlags::empty);
                    if status_flags.intersects(
                        crate::storage::proc::ProcStatusFlags::PROC_IN_LOGICAL_DECODING
                            | crate::storage::proc::ProcStatusFlags::PROC_IN_VACUUM,
                    ) {
                        continue;
                    }
                    if crate::access::transam::normal_transaction_id_precedes(xid, xmin) {
                        xmin = xid;
                    }
                    snapshot.xip.push(xid);
                    count += 1;

                    if !suboverflowed {
                        if g.subxid_states
                            .get(pgxactoff)
                            .map(|s| s.overflowed)
                            .unwrap_or(false)
                        {
                            suboverflowed = true;
                        } else {
                            let pgprocno = a.pgprocnos[pgxactoff];
                            if let Some(proc) = g.proc(pgprocno) {
                                let nsub = proc.subxid_status.count as usize;
                                for j in 0..nsub {
                                    snapshot.subxip.push(proc.subxids.xids[j]);
                                }
                                subcount += nsub;
                            }
                        }
                    }
                }
            });
        } else {
            // Hot standby: pull from KnownAssignedXids into subxip.
            let mut kxmin = xmin;
            subcount = known_assigned_xids_get_and_set_xmin(&a, &mut snapshot.subxip, &mut kxmin, xmax);
            xmin = kxmin;
            if transaction_id_precedes_or_equals(xmin, a.last_overflowed_xid) {
                suboverflowed = true;
            }
        }

        replication_slot_xmin = a.replication_slot_xmin;
        replication_slot_catalog_xmin = a.replication_slot_catalog_xmin;

        // Set MyProc->xmin / TransactionXmin if not already set.
        if !transaction_id_is_valid(transaction_xmin()) {
            set_transaction_xmin(xmin);
            set_my_proc_xmin(shared, xmin);
        }
    } // drop read guard

    // maintain approximate GlobalVis* state (post-lock arithmetic).
    {
        let oldestfxid = full_xid_relative_to(latest_completed, oldest_xid);
        let def_vis_xid_data = transaction_id_older(xmin, replication_slot_xmin);
        let def_vis_xid = transaction_id_older(replication_slot_catalog_xmin, def_vis_xid_data);
        let def_vis_fxid = full_xid_relative_to(latest_completed, def_vis_xid);
        let def_vis_fxid_data = full_xid_relative_to(latest_completed, def_vis_xid_data);

        let mut vis = shared.proc_array().vis.lock().unwrap();
        vis.shared.definitely_needed =
            full_transaction_id_newer(def_vis_fxid, vis.shared.definitely_needed);
        vis.catalog.definitely_needed =
            full_transaction_id_newer(def_vis_fxid, vis.catalog.definitely_needed);
        vis.data.definitely_needed =
            full_transaction_id_newer(def_vis_fxid_data, vis.data.definitely_needed);
        if transaction_id_is_normal(myxid) {
            vis.temp.definitely_needed = full_xid_relative_to(latest_completed, myxid);
        } else {
            vis.temp.definitely_needed = latest_completed;
            full_transaction_id_advance(&mut vis.temp.definitely_needed);
        }
        vis.shared.maybe_needed =
            full_transaction_id_newer(vis.shared.maybe_needed, oldestfxid);
        vis.catalog.maybe_needed =
            full_transaction_id_newer(vis.catalog.maybe_needed, oldestfxid);
        vis.data.maybe_needed = full_transaction_id_newer(vis.data.maybe_needed, oldestfxid);
        vis.temp.maybe_needed = vis.temp.definitely_needed;
    }

    set_recent_xmin(xmin);

    snapshot.xmin = xmin;
    snapshot.xmax = xmax;
    snapshot.suboverflowed = suboverflowed;
    snapshot.taken_during_recovery = taken_during_recovery;
    snapshot.snap_xact_completion_count = cur_completion;
    // curcid is GetCurrentCommandId(false) (xact.c, step 14d): leave as set by
    // the caller (snapmgr) for now.
    snapshot.active_count = 0;
    snapshot.regd_count = 0;
    snapshot.copied = false;
    let _ = (count, subcount);
    snapshot
}

/// procarray.c `ProcArrayInstallImportedXmin`: install an imported snapshot's
/// xmin into MyProc->xmin, but only if the source xact is still running.
pub fn proc_array_install_imported_xmin(
    shared: &Arc<SharedState>,
    xmin: TransactionId,
    sourcevxid: &VirtualTransactionId,
) -> bool {
    if !transaction_id_is_normal(xmin) {
        return false;
    }
    let my_database_id = crate::session::try_current()
        .map(|s| s.database_id())
        .unwrap_or(crate::postgres_ext::InvalidOid);
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    let found = with_proc_globals(shared, |g| {
        for index in 0..a.pgprocnos.len() {
            let pgprocno = a.pgprocnos[index];
            let proc = match g.proc(pgprocno) {
                Some(p) => p,
                None => continue,
            };
            let status_flags = g
                .status_flags
                .get(index)
                .copied()
                .map(crate::storage::proc::ProcStatusFlags::from_bits_truncate)
                .unwrap_or_else(crate::storage::proc::ProcStatusFlags::empty);
            if status_flags.contains(crate::storage::proc::ProcStatusFlags::PROC_IN_VACUUM) {
                continue;
            }
            if proc.vxid.proc_number != sourcevxid.proc_number {
                continue;
            }
            if proc.vxid.lxid != sourcevxid.local_transaction_id {
                continue;
            }
            if proc.database_id != my_database_id {
                continue;
            }
            let xid = proc.xmin;
            if !transaction_id_is_normal(xid) || !transaction_id_precedes_or_equals(xid, xmin) {
                continue;
            }
            return true;
        }
        false
    });
    if found {
        set_my_proc_xmin(shared, xmin);
        set_transaction_xmin(xmin);
    }
    found
}

/// procarray.c `ProcArrayInstallRestoredXmin`.
pub fn proc_array_install_restored_xmin(
    shared: &Arc<SharedState>,
    xmin: TransactionId,
    proc: &PGPROC,
) -> bool {
    if !transaction_id_is_normal(xmin) {
        return false;
    }
    let my_database_id = crate::session::try_current()
        .map(|s| s.database_id())
        .unwrap_or(crate::postgres_ext::InvalidOid);
    let pa = shared.proc_array();
    let _a = pa.inner.write().unwrap();
    let xid = proc.xmin;
    if proc.database_id == my_database_id
        && transaction_id_is_normal(xid)
        && transaction_id_precedes_or_equals(xid, xmin)
    {
        set_my_proc_xmin(shared, xmin);
        set_transaction_xmin(xmin);
        // statusFlags propagation (PROC_XMIN_FLAGS) lands in step 15.
        true
    } else {
        false
    }
}

// ===========================================================================
// GetRunningTransactionData / oldest-active / safe-decoding
// ===========================================================================

/// procarray.c `GetRunningTransactionData`. PG holds ProcArrayLock + XidGenLock
/// for the caller to release after WAL-logging; here we collect and return owned
/// data (the locks are internal and dropped before return).
pub fn get_running_transaction_data(shared: &Arc<SharedState>) -> RunningTransactionsData {
    let my_database_id = crate::session::try_current()
        .map(|s| s.database_id())
        .unwrap_or(crate::postgres_ext::InvalidOid);
    let mut xids: Vec<TransactionId> = Vec::new();
    let mut count = 0usize;
    let mut subcount = 0usize;
    let mut suboverflowed = false;

    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    let (latest_completed, next_xid) = shared
        .variable_cache()
        .with(|v| (xid_from_full_transaction_id(v.latest_completed_xid), xid_from_full_transaction_id(v.next_xid)));
    let mut oldest_running_xid = next_xid;
    let mut oldest_database_running_xid = next_xid;

    with_proc_globals(shared, |g| {
        let n = a.pgprocnos.len().min(g.xids.len());
        for index in 0..n {
            let xid = g.xids[index];
            if !transaction_id_is_valid(xid) {
                continue;
            }
            if transaction_id_precedes(xid, oldest_running_xid) {
                oldest_running_xid = xid;
            }
            if transaction_id_precedes(xid, oldest_database_running_xid) {
                let pgprocno = a.pgprocnos[index];
                if g.proc(pgprocno).map(|p| p.database_id).unwrap_or(Oid(0)) == my_database_id {
                    oldest_database_running_xid = xid;
                }
            }
            if g.subxid_states.get(index).map(|s| s.overflowed).unwrap_or(false) {
                suboverflowed = true;
            }
            xids.push(xid);
            count += 1;
        }
        if !suboverflowed {
            for index in 0..n {
                let pgprocno = a.pgprocnos[index];
                if let Some(proc) = g.proc(pgprocno) {
                    let nsub = proc.subxid_status.count as usize;
                    for j in 0..nsub {
                        xids.push(proc.subxids.xids[j]);
                    }
                    count += nsub;
                    subcount += nsub;
                }
            }
        }
    });

    RunningTransactionsData {
        xcnt: (count - subcount) as i32,
        subxcnt: subcount as i32,
        subxid_status: if suboverflowed {
            SubxidsArrayStatus::InSubtrans
        } else {
            SubxidsArrayStatus::InArray
        },
        next_xid,
        oldest_running_xid,
        oldest_database_running_xid,
        latest_completed_xid: latest_completed,
        xids,
    }
}

/// procarray.c `GetOldestActiveTransactionId`.
pub fn get_oldest_active_transaction_id(shared: &Arc<SharedState>) -> TransactionId {
    let mut oldest_running_xid = shared
        .variable_cache()
        .with(|v| xid_from_full_transaction_id(v.next_xid));
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        let n = a.pgprocnos.len().min(g.xids.len());
        for index in 0..n {
            let xid = g.xids[index];
            if !transaction_id_is_normal(xid) {
                continue;
            }
            if transaction_id_precedes(xid, oldest_running_xid) {
                oldest_running_xid = xid;
            }
        }
    });
    oldest_running_xid
}

/// procarray.c `GetOldestSafeDecodingTransactionId`. (PG requires ProcArrayLock
/// held by the caller; here we take it internally.)
pub fn get_oldest_safe_decoding_transaction_id(
    shared: &Arc<SharedState>,
    catalog_only: bool,
) -> TransactionId {
    let recovery_in_progress = crate::access::transam::TransactionStartedDuringRecovery();
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();

    let mut oldest_safe_xid = shared
        .variable_cache()
        .with(|v| xid_from_full_transaction_id(v.next_xid));

    if transaction_id_is_valid(a.replication_slot_xmin)
        && transaction_id_precedes(a.replication_slot_xmin, oldest_safe_xid)
    {
        oldest_safe_xid = a.replication_slot_xmin;
    }
    if catalog_only
        && transaction_id_is_valid(a.replication_slot_catalog_xmin)
        && transaction_id_precedes(a.replication_slot_catalog_xmin, oldest_safe_xid)
    {
        oldest_safe_xid = a.replication_slot_catalog_xmin;
    }

    if !recovery_in_progress {
        with_proc_globals(shared, |g| {
            let n = a.pgprocnos.len().min(g.xids.len());
            for index in 0..n {
                let xid = g.xids[index];
                if !transaction_id_is_normal(xid) {
                    continue;
                }
                if transaction_id_precedes(xid, oldest_safe_xid) {
                    oldest_safe_xid = xid;
                }
            }
        });
    }
    oldest_safe_xid
}

// ===========================================================================
// VXID delaying checkpoint
// ===========================================================================

/// procarray.c `GetVirtualXIDsDelayingChkpt` (the count out-param folds into the
/// Vec length).
pub fn get_virtual_xids_delaying_chkpt(
    shared: &Arc<SharedState>,
    type_: i32,
) -> Vec<VirtualTransactionId> {
    let mut vxids = Vec::new();
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if let Some(proc) = g.proc(pgprocno) {
                if proc.delay_chkpt_flags.bits() & type_ != 0 {
                    let vxid = vxid_from_proc(proc);
                    if vxid.is_valid() {
                        vxids.push(vxid);
                    }
                }
            }
        }
    });
    vxids
}

/// procarray.c `HaveVirtualXIDsDelayingChkpt`.
pub fn have_virtual_xids_delaying_chkpt(
    shared: &Arc<SharedState>,
    vxids: &[VirtualTransactionId],
    type_: i32,
) -> bool {
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if let Some(proc) = g.proc(pgprocno) {
                let vxid = vxid_from_proc(proc);
                if proc.delay_chkpt_flags.bits() & type_ != 0
                    && vxid.is_valid()
                    && vxids.iter().any(|v| *v == vxid)
                {
                    return true;
                }
            }
        }
        false
    })
}

// ===========================================================================
// Proc lookups by number / pid / xid
// ===========================================================================

/// procarray.c `ProcNumberGetProc`. Staging: returns None until step 15
/// populates ProcGlobal (the `'static mut` ref form is a step-15 concern; we
/// keep the header signature and return None for the empty array).
pub fn proc_number_get_proc(_proc_number: ProcNumber) -> Option<&'static mut PGPROC> {
    // ProcGlobal->allProcs is empty until step 15; nothing to return.
    None
}

/// C out-params (xid, xmin, nsubxid, overflowed) -> a struct.
pub struct ProcNumberXids {
    pub xid: TransactionId,
    pub xmin: TransactionId,
    pub nsubxid: i32,
    pub overflowed: bool,
}

/// procarray.c `ProcNumberGetTransactionIds`.
pub fn proc_number_get_transaction_ids(
    shared: &Arc<SharedState>,
    proc_number: ProcNumber,
) -> ProcNumberXids {
    let mut out = ProcNumberXids {
        xid: INVALID_XID,
        xmin: INVALID_XID,
        nsubxid: 0,
        overflowed: false,
    };
    let pa = shared.proc_array();
    let _a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        if let Some(proc) = g.proc(proc_number) {
            if proc.pid != 0 {
                out.xid = proc.xid;
                out.xmin = proc.xmin;
                out.nsubxid = proc.subxid_status.count as i32;
                out.overflowed = proc.subxid_status.overflowed;
            }
        }
    });
    out
}

/// procarray.c `BackendPidGetProc`. Staging: None over the empty array.
pub fn backend_pid_get_proc(_pid: i32) -> Option<&'static mut PGPROC> {
    None
}

/// procarray.c `BackendPidGetProcWithLock`. Staging: None over the empty array.
pub fn backend_pid_get_proc_with_lock(_pid: i32) -> Option<&'static mut PGPROC> {
    None
}

/// procarray.c `BackendXidGetPid`: pid owning `xid`, or 0 if not found.
pub fn backend_xid_get_pid(shared: &Arc<SharedState>, xid: TransactionId) -> i32 {
    if xid == INVALID_XID {
        return 0;
    }
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        let n = a.pgprocnos.len().min(g.xids.len());
        for index in 0..n {
            if g.xids[index] == xid {
                let pgprocno = a.pgprocnos[index];
                if let Some(proc) = g.proc(pgprocno) {
                    return proc.pid;
                }
            }
        }
        0
    })
}

/// procarray.c `IsBackendPid`.
pub fn is_backend_pid(pid: i32) -> bool {
    backend_pid_get_proc(pid).is_some()
}

// ===========================================================================
// VXID scans, backend counting, signaling (recovery / DROP DATABASE helpers)
// ===========================================================================

/// procarray.c `GetCurrentVirtualXIDs`.
pub fn get_current_virtual_xids(
    shared: &Arc<SharedState>,
    limit_xmin: TransactionId,
    exclude_xmin0: bool,
    all_dbs: bool,
    exclude_vacuum: i32,
) -> Vec<VirtualTransactionId> {
    let my_database_id = crate::session::try_current()
        .map(|s| s.database_id())
        .unwrap_or(crate::postgres_ext::InvalidOid);
    let mut vxids = Vec::new();
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        let n = a.pgprocnos.len();
        for index in 0..n {
            let pgprocno = a.pgprocnos[index];
            let status_flags = g
                .status_flags
                .get(index)
                .copied()
                .unwrap_or(0);
            let proc = match g.proc(pgprocno) {
                Some(p) => p,
                None => continue,
            };
            if exclude_vacuum & status_flags as i32 != 0 {
                continue;
            }
            if all_dbs || proc.database_id == my_database_id {
                let pxmin = proc.xmin;
                if exclude_xmin0 && !transaction_id_is_valid(pxmin) {
                    continue;
                }
                if !transaction_id_is_valid(limit_xmin)
                    || transaction_id_precedes_or_equals(pxmin, limit_xmin)
                {
                    let vxid = vxid_from_proc(proc);
                    if vxid.is_valid() {
                        vxids.push(vxid);
                    }
                }
            }
        }
    });
    vxids
}

/// procarray.c `GetConflictingVirtualXIDs` (recovery-conflict helper).
pub fn get_conflicting_virtual_xids(
    shared: &Arc<SharedState>,
    limit_xmin: TransactionId,
    db_oid: Oid,
) -> Vec<VirtualTransactionId> {
    let mut vxids = Vec::new();
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            let proc = match g.proc(pgprocno) {
                Some(p) => p,
                None => continue,
            };
            if proc.pid == 0 {
                continue; // prepared xact
            }
            if db_oid == crate::postgres_ext::InvalidOid || proc.database_id == db_oid {
                let pxmin = proc.xmin;
                if !transaction_id_is_valid(limit_xmin)
                    || (transaction_id_is_valid(pxmin) && !transaction_id_follows(pxmin, limit_xmin))
                {
                    let vxid = vxid_from_proc(proc);
                    if vxid.is_valid() {
                        vxids.push(vxid);
                    }
                }
            }
        }
    });
    vxids
}

/// procarray.c `CancelVirtualTransaction`.
pub fn cancel_virtual_transaction(
    shared: &Arc<SharedState>,
    vxid: VirtualTransactionId,
    sigmode: crate::storage::procsignal::ProcSignalReason,
) -> i32 {
    signal_virtual_transaction(shared, vxid, sigmode, true)
}

/// procarray.c `SignalVirtualTransaction`. SendProcSignal lands on the procsignal
/// subsystem (step 04); the lookup is procarray's.
pub fn signal_virtual_transaction(
    shared: &Arc<SharedState>,
    vxid: VirtualTransactionId,
    _sigmode: crate::storage::procsignal::ProcSignalReason,
    conflict_pending: bool,
) -> i32 {
    let pa = shared.proc_array();
    let a = pa.inner.write().unwrap();
    with_proc_globals_mut(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if let Some(proc) = g.proc_mut(pgprocno) {
                let procvxid = vxid_from_proc(proc);
                if procvxid.proc_number == vxid.proc_number
                    && procvxid.local_transaction_id == vxid.local_transaction_id
                {
                    proc.recovery_conflict_pending = conflict_pending;
                    let pid = proc.pid;
                    // SendProcSignal(pid, sigmode, vxid.proc_number): procsignal.
                    return pid;
                }
            }
        }
        0
    })
}

/// procarray.c `MinimumActiveBackends` (lock-free heuristic in PG).
pub fn minimum_active_backends(shared: &Arc<SharedState>, min: i32) -> bool {
    if min == 0 {
        return true;
    }
    let mut count = 0;
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if pgprocno == -1 {
                continue;
            }
            if let Some(proc) = g.proc(pgprocno) {
                if proc.xid == INVALID_XID || proc.pid == 0 || proc.wait_lock.is_some() {
                    continue;
                }
                count += 1;
                if count >= min {
                    break;
                }
            }
        }
    });
    count >= min
}

/// procarray.c `CountDBBackends`.
pub fn count_db_backends(shared: &Arc<SharedState>, databaseid: Oid) -> i32 {
    let mut count = 0;
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if let Some(proc) = g.proc(pgprocno) {
                if proc.pid == 0 {
                    continue;
                }
                if databaseid == crate::postgres_ext::InvalidOid || proc.database_id == databaseid {
                    count += 1;
                }
            }
        }
    });
    count
}

/// procarray.c `CountDBConnections`.
pub fn count_db_connections(shared: &Arc<SharedState>, databaseid: Oid) -> i32 {
    let mut count = 0;
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if let Some(proc) = g.proc(pgprocno) {
                if proc.pid == 0 || !proc.is_regular_backend {
                    continue;
                }
                if databaseid == crate::postgres_ext::InvalidOid || proc.database_id == databaseid {
                    count += 1;
                }
            }
        }
    });
    count
}

/// procarray.c `CancelDBBackends`.
pub fn cancel_db_backends(
    shared: &Arc<SharedState>,
    databaseid: Oid,
    _sigmode: crate::storage::procsignal::ProcSignalReason,
    conflict_pending: bool,
) {
    let pa = shared.proc_array();
    let a = pa.inner.write().unwrap();
    with_proc_globals_mut(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if let Some(proc) = g.proc_mut(pgprocno) {
                if databaseid == crate::postgres_ext::InvalidOid || proc.database_id == databaseid {
                    proc.recovery_conflict_pending = conflict_pending;
                    // SendProcSignal: procsignal.
                }
            }
        }
    });
}

/// procarray.c `CountUserBackends`.
pub fn count_user_backends(shared: &Arc<SharedState>, roleid: Oid) -> i32 {
    let mut count = 0;
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if let Some(proc) = g.proc(pgprocno) {
                if proc.pid == 0 || !proc.is_regular_backend {
                    continue;
                }
                if proc.role_id == roleid {
                    count += 1;
                }
            }
        }
    });
    count
}

/// procarray.c `CountOtherDBBackends`: bool + (nbackends, nprepared) out-params
/// -> Option of the counts. The 5-second SIGTERM/sleep retry loop is the
/// supervisor/autovacuum concern (step 17); here we report the counts once.
pub fn count_other_db_backends(shared: &Arc<SharedState>, database_id: Oid) -> Option<(i32, i32)> {
    let mut nbackends = 0;
    let mut nprepared = 0;
    let mut found = false;
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    with_proc_globals(shared, |g| {
        for &pgprocno in &a.pgprocnos {
            if let Some(proc) = g.proc(pgprocno) {
                if proc.database_id != database_id {
                    continue;
                }
                // C also skips proc == MyProc; MyProc identity lands in step 15.
                found = true;
                if proc.pid == 0 {
                    nprepared += 1;
                } else {
                    nbackends += 1;
                }
            }
        }
    });
    if found {
        Some((nbackends, nprepared))
    } else {
        None
    }
}

/// procarray.c `TerminateOtherDBBackends`. Permission checks + kill() are the
/// supervisor/catalog concern; here we collect the targets (no-op kill staged).
pub fn terminate_other_db_backends(shared: &Arc<SharedState>, database_id: Oid) {
    let pa = shared.proc_array();
    let _a = pa.inner.read().unwrap();
    let _ = (shared, database_id);
    // pids/nprepared collection + SIGTERM: step 15/17 (proc + supervisor).
}

/// procarray.c `XidCacheRemoveRunningXids`: drop aborted subxids from MyProc's
/// cache. The MyProc subxid mutation is owned by step 15; we advance
/// latestCompletedXid + xactCompletionCount under the write guard.
pub fn xid_cache_remove_running_xids(
    shared: &Arc<SharedState>,
    _xid: TransactionId,
    _xids: &[TransactionId],
    latest_xid: TransactionId,
) {
    let pa = shared.proc_array();
    let _a = pa.inner.write().unwrap();
    // MyProc->subxids cache removal: step 15.
    maintain_latest_completed_xid(shared, latest_xid);
    shared.variable_cache().with(|v| v.xact_completion_count += 1);
}

// ===========================================================================
// Replication slot xmin
// ===========================================================================

/// procarray.c `ProcArraySetReplicationSlotXmin`.
pub fn proc_array_set_replication_slot_xmin(
    shared: &Arc<SharedState>,
    xmin: TransactionId,
    catalog_xmin: TransactionId,
    _already_locked: bool,
) {
    let pa = shared.proc_array();
    let mut a = pa.inner.write().unwrap();
    a.replication_slot_xmin = xmin;
    a.replication_slot_catalog_xmin = catalog_xmin;
}

/// procarray.c `ProcArrayGetReplicationSlotXmin` (out-params -> tuple).
pub fn proc_array_get_replication_slot_xmin(
    shared: &Arc<SharedState>,
) -> (TransactionId, TransactionId) {
    let pa = shared.proc_array();
    let a = pa.inner.read().unwrap();
    (a.replication_slot_xmin, a.replication_slot_catalog_xmin)
}

// ===========================================================================
// GlobalVisTest* family
// ===========================================================================

/// procarray.c `GlobalVisTestFor`: get the GlobalVisState for a relation. Under
/// the conservative staging this always returns the SHARED state (copy).
pub fn global_vis_test_for(shared: &Arc<SharedState>, _rel: Option<&RelationData>) -> GlobalVisState {
    shared.proc_array().vis.lock().unwrap().shared
}

/// procarray.c `GlobalVisTestShouldUpdate`.
fn global_vis_test_should_update(state: &GlobalVisState, last_xmin: TransactionId) -> bool {
    if !transaction_id_is_valid(last_xmin) {
        return true;
    }
    if crate::access::transam::full_transaction_id_follows_or_equals(
        state.maybe_needed,
        state.definitely_needed,
    ) {
        return false;
    }
    recent_xmin() != last_xmin
}

/// procarray.c `GlobalVisUpdateApply`: refresh the maybe_needed/definitely_needed
/// bounds from computed horizons.
fn global_vis_update_apply(shared: &Arc<SharedState>, h: &ComputeXidHorizonsResult) {
    let mut vis = shared.proc_array().vis.lock().unwrap();
    vis.shared.maybe_needed =
        full_xid_relative_to(h.latest_completed, h.shared_oldest_nonremovable);
    vis.catalog.maybe_needed =
        full_xid_relative_to(h.latest_completed, h.catalog_oldest_nonremovable);
    vis.data.maybe_needed = full_xid_relative_to(h.latest_completed, h.data_oldest_nonremovable);
    vis.temp.maybe_needed = full_xid_relative_to(h.latest_completed, h.temp_oldest_nonremovable);

    vis.shared.definitely_needed =
        full_transaction_id_newer(vis.shared.maybe_needed, vis.shared.definitely_needed);
    vis.catalog.definitely_needed =
        full_transaction_id_newer(vis.catalog.maybe_needed, vis.catalog.definitely_needed);
    vis.data.definitely_needed =
        full_transaction_id_newer(vis.data.maybe_needed, vis.data.definitely_needed);
    vis.temp.definitely_needed = vis.temp.maybe_needed;

    vis.last_xmin = recent_xmin();
}

/// procarray.c `GlobalVisTestIsRemovableFullXid`.
pub fn global_vis_test_is_removable_full_xid(
    shared: &Arc<SharedState>,
    state: &GlobalVisState,
    fxid: FullTransactionId,
) -> bool {
    if crate::access::transam::full_transaction_id_precedes(fxid, state.maybe_needed) {
        return true;
    }
    if crate::access::transam::full_transaction_id_follows_or_equals(fxid, state.definitely_needed) {
        return false;
    }
    let last_xmin = shared.proc_array().vis.lock().unwrap().last_xmin;
    if global_vis_test_should_update(state, last_xmin) {
        let _ = compute_xid_horizons(shared);
        let fresh = shared.proc_array().vis.lock().unwrap().shared;
        crate::access::transam::full_transaction_id_precedes(fxid, fresh.maybe_needed)
    } else {
        false
    }
}

/// procarray.c `GlobalVisTestIsRemovableXid` (32-bit wrapper).
pub fn global_vis_test_is_removable_xid(
    shared: &Arc<SharedState>,
    state: &GlobalVisState,
    xid: TransactionId,
) -> bool {
    let fxid = full_xid_relative_to(state.definitely_needed, xid);
    global_vis_test_is_removable_full_xid(shared, state, fxid)
}

// ===========================================================================
// KnownAssignedXids submodule (hot standby). Exercised only under recovery.
// All callers hold the ProcArray write guard (passed as &mut ProcArrayInner)
// except snapshot reads which pass &ProcArrayInner.
// ===========================================================================

tokio::task_local! {
    /// procarray.c `latestObservedXid` (startup-process global). Per-task is the
    /// closest fit; only the recovery/startup task touches it.
    static LATEST_OBSERVED_XID: Cell<TransactionId>;
}

fn latest_observed_xid() -> TransactionId {
    LATEST_OBSERVED_XID
        .try_with(|c| c.get())
        .unwrap_or(INVALID_XID)
}

fn set_latest_observed_xid(xid: TransactionId) {
    let _ = LATEST_OBSERVED_XID.try_with(|c| c.set(xid));
}

/// procarray.c `RecordKnownAssignedTransactionIds`. Async: ExtendSUBTRANS.
pub async fn record_known_assigned_transaction_ids(
    shared: &Arc<SharedState>,
    xid: TransactionId,
) {
    let latest = latest_observed_xid();
    if transaction_id_follows(xid, latest) {
        let mut next_expected = latest;
        while transaction_id_precedes(next_expected, xid) {
            transaction_id_advance(&mut next_expected);
            crate::backend::access::transam::subtrans::extend_subtrans(shared, next_expected).await;
        }
        // KnownAssignedXids add (latestObservedXid, xid] under the write guard.
        let mut next = latest;
        transaction_id_advance(&mut next);
        {
            let pa = shared.proc_array();
            let mut a = pa.inner.write().unwrap();
            known_assigned_xids_add(&mut a, next, xid, false);
        }
        set_latest_observed_xid(xid);
        crate::backend::access::transam::varsup::AdvanceNextFullTransactionIdPastXid(shared, xid);
    }
}

/// procarray.c `ExpireTreeKnownAssignedTransactionIds`.
pub fn expire_tree_known_assigned_transaction_ids(
    shared: &Arc<SharedState>,
    xid: TransactionId,
    subxids: &[TransactionId],
    max_xid: TransactionId,
) {
    let pa = shared.proc_array();
    let mut a = pa.inner.write().unwrap();
    known_assigned_xids_remove_tree(&mut a, xid, subxids);
    drop(a);
    maintain_latest_completed_xid_recovery(shared, max_xid);
    shared.variable_cache().with(|v| v.xact_completion_count += 1);
}

/// procarray.c `ExpireAllKnownAssignedTransactionIds`.
pub fn expire_all_known_assigned_transaction_ids(shared: &Arc<SharedState>) {
    let pa = shared.proc_array();
    let mut a = pa.inner.write().unwrap();
    known_assigned_xids_remove_preceding(&mut a, INVALID_XID);
    a.last_overflowed_xid = INVALID_XID;
    drop(a);
    shared.variable_cache().with(|v| {
        let mut latest = v.next_xid;
        crate::access::transam::full_transaction_id_retreat(&mut latest);
        v.latest_completed_xid = latest;
        v.xact_completion_count += 1;
    });
}

/// procarray.c `ExpireOldKnownAssignedTransactionIds`.
pub fn expire_old_known_assigned_transaction_ids(shared: &Arc<SharedState>, xid: TransactionId) {
    let mut latest = xid;
    crate::access::transam::transaction_id_retreat(&mut latest);
    maintain_latest_completed_xid_recovery(shared, latest);
    shared.variable_cache().with(|v| v.xact_completion_count += 1);
    let pa = shared.proc_array();
    let mut a = pa.inner.write().unwrap();
    if transaction_id_precedes(a.last_overflowed_xid, xid) {
        a.last_overflowed_xid = INVALID_XID;
    }
    known_assigned_xids_remove_preceding(&mut a, xid);
}

/// procarray.c `KnownAssignedTransactionIdsIdleMaintenance`.
pub fn known_assigned_transaction_ids_idle_maintenance(shared: &Arc<SharedState>) {
    let pa = shared.proc_array();
    let mut a = pa.inner.write().unwrap();
    known_assigned_xids_compress(&mut a, KaxCompressReason::StartupProcessIdle);
}

// --- KnownAssignedXids private primitives (operate on &mut/&ProcArrayInner) ---

/// procarray.c `KnownAssignedXidsCompress`: shift valid entries to the front.
/// Caller holds the write guard (we always pass &mut). The idle-time-interval
/// heuristic (lastCompressTs / GetCurrentTimestamp) is simplified to "always
/// compress when asked" -- TODO(perf): re-add the timestamp throttle.
fn known_assigned_xids_compress(a: &mut ProcArrayInner, reason: KaxCompressReason) {
    let head = a.head_known_assigned_xids;
    let tail = a.tail_known_assigned_xids;
    let nelements = head - tail;

    if nelements == a.num_known_assigned_xids {
        if reason != KaxCompressReason::NoSpace {
            return;
        }
    } else if reason == KaxCompressReason::TransactionEnd
        && nelements < 2 * a.num_known_assigned_xids
    {
        return;
    }

    let mut compress_index = 0;
    for i in tail..head {
        if a.known_assigned_xids_valid[i] {
            a.known_assigned_xids[compress_index] = a.known_assigned_xids[i];
            a.known_assigned_xids_valid[compress_index] = true;
            compress_index += 1;
        }
    }
    a.tail_known_assigned_xids = 0;
    a.head_known_assigned_xids = compress_index;
}

/// procarray.c `KnownAssignedXidsAdd`.
fn known_assigned_xids_add(
    a: &mut ProcArrayInner,
    from_xid: TransactionId,
    to_xid: TransactionId,
    exclusive_lock: bool,
) {
    let nxids = if to_xid.0 >= from_xid.0 {
        (to_xid.0 - from_xid.0 + 1) as usize
    } else {
        let mut n = 1usize;
        let mut next = from_xid;
        while transaction_id_precedes(next, to_xid) {
            n += 1;
            transaction_id_advance(&mut next);
        }
        n
    };

    let mut head = a.head_known_assigned_xids;
    let tail = a.tail_known_assigned_xids;

    if head > tail && transaction_id_follows_or_equals(a.known_assigned_xids[head - 1], from_xid) {
        // TODO(panic): elog(ERROR, out-of-order XID insertion).
        panic!("out-of-order XID insertion in KnownAssignedXids");
    }

    if head + nxids > a.max_known_assigned_xids {
        known_assigned_xids_compress(a, KaxCompressReason::NoSpace);
        head = a.head_known_assigned_xids;
        if head + nxids > a.max_known_assigned_xids {
            panic!("too many KnownAssignedXids");
        }
    }

    let mut next = from_xid;
    for _ in 0..nxids {
        a.known_assigned_xids[head] = next;
        a.known_assigned_xids_valid[head] = true;
        transaction_id_advance(&mut next);
        head += 1;
    }
    a.num_known_assigned_xids += nxids;
    let _ = exclusive_lock; // write guard always held; no barrier needed
    a.head_known_assigned_xids = head;
}

/// procarray.c `KnownAssignedXidsSearch` (binary search, optional remove).
fn known_assigned_xids_search(a: &mut ProcArrayInner, xid: TransactionId, remove: bool) -> bool {
    let tail = a.tail_known_assigned_xids;
    let head = a.head_known_assigned_xids;
    if head == 0 {
        return false;
    }
    let mut first = tail as isize;
    let mut last = head as isize - 1;
    let mut result_index: isize = -1;
    while first <= last {
        let mid = ((first + last) / 2) as usize;
        let mid_xid = a.known_assigned_xids[mid];
        if xid.0 == mid_xid.0 {
            result_index = mid as isize;
            break;
        } else if transaction_id_precedes(xid, mid_xid) {
            last = mid as isize - 1;
        } else {
            first = mid as isize + 1;
        }
    }
    if result_index < 0 {
        return false;
    }
    let ri = result_index as usize;
    if !a.known_assigned_xids_valid[ri] {
        return false;
    }
    if remove {
        a.known_assigned_xids_valid[ri] = false;
        a.num_known_assigned_xids -= 1;
        if ri == tail {
            let mut t = tail + 1;
            while t < head && !a.known_assigned_xids_valid[t] {
                t += 1;
            }
            if t >= head {
                a.head_known_assigned_xids = 0;
                a.tail_known_assigned_xids = 0;
            } else {
                a.tail_known_assigned_xids = t;
            }
        }
    }
    true
}

/// procarray.c `KnownAssignedXidExists` (read side; binary search w/o mutation).
fn known_assigned_xid_exists(a: &ProcArrayInner, xid: TransactionId) -> bool {
    let tail = a.tail_known_assigned_xids;
    let head = a.head_known_assigned_xids;
    if head == 0 {
        return false;
    }
    let mut first = tail as isize;
    let mut last = head as isize - 1;
    while first <= last {
        let mid = ((first + last) / 2) as usize;
        let mid_xid = a.known_assigned_xids[mid];
        if xid.0 == mid_xid.0 {
            return a.known_assigned_xids_valid[mid];
        } else if transaction_id_precedes(xid, mid_xid) {
            last = mid as isize - 1;
        } else {
            first = mid as isize + 1;
        }
    }
    false
}

/// procarray.c `KnownAssignedXidsRemove`.
fn known_assigned_xids_remove(a: &mut ProcArrayInner, xid: TransactionId) {
    let _ = known_assigned_xids_search(a, xid, true);
}

/// procarray.c `KnownAssignedXidsRemoveTree`.
fn known_assigned_xids_remove_tree(
    a: &mut ProcArrayInner,
    xid: TransactionId,
    subxids: &[TransactionId],
) {
    if transaction_id_is_valid(xid) {
        known_assigned_xids_remove(a, xid);
    }
    for &s in subxids {
        known_assigned_xids_remove(a, s);
    }
    known_assigned_xids_compress(a, KaxCompressReason::TransactionEnd);
}

/// procarray.c `KnownAssignedXidsRemovePreceding`. StandbyTransactionIdIsPrepared
/// lands on the standby stub (treated as not-prepared in the foundation).
fn known_assigned_xids_remove_preceding(a: &mut ProcArrayInner, remove_xid: TransactionId) {
    if !transaction_id_is_valid(remove_xid) {
        a.num_known_assigned_xids = 0;
        a.head_known_assigned_xids = 0;
        a.tail_known_assigned_xids = 0;
        return;
    }
    let tail = a.tail_known_assigned_xids;
    let head = a.head_known_assigned_xids;
    let mut count = 0;
    for i in tail..head {
        if a.known_assigned_xids_valid[i] {
            let known_xid = a.known_assigned_xids[i];
            if transaction_id_follows_or_equals(known_xid, remove_xid) {
                break;
            }
            // StandbyTransactionIdIsPrepared(known_xid) == false in foundation.
            a.known_assigned_xids_valid[i] = false;
            count += 1;
        }
    }
    a.num_known_assigned_xids -= count;

    let mut i = tail;
    while i < head {
        if a.known_assigned_xids_valid[i] {
            break;
        }
        i += 1;
    }
    if i >= head {
        a.head_known_assigned_xids = 0;
        a.tail_known_assigned_xids = 0;
    } else {
        a.tail_known_assigned_xids = i;
    }
    known_assigned_xids_compress(a, KaxCompressReason::Prune);
}

/// procarray.c `KnownAssignedXidsGet`.
fn known_assigned_xids_get(a: &ProcArrayInner, xmax: TransactionId) -> Vec<TransactionId> {
    let mut xmin = INVALID_XID;
    let mut out = Vec::new();
    known_assigned_xids_get_and_set_xmin(a, &mut out, &mut xmin, xmax);
    out
}

/// procarray.c `KnownAssignedXidsGetAndSetXmin`. Returns count; appends into
/// `out` and lowers `*xmin` to the first (lowest) seen xid.
fn known_assigned_xids_get_and_set_xmin(
    a: &ProcArrayInner,
    out: &mut Vec<TransactionId>,
    xmin: &mut TransactionId,
    xmax: TransactionId,
) -> usize {
    let tail = a.tail_known_assigned_xids;
    let head = a.head_known_assigned_xids;
    let mut count = 0;
    for i in tail..head {
        if a.known_assigned_xids_valid[i] {
            let known_xid = a.known_assigned_xids[i];
            if count == 0 && transaction_id_precedes(known_xid, *xmin) {
                *xmin = known_xid;
            }
            if transaction_id_is_valid(xmax) && transaction_id_follows_or_equals(known_xid, xmax) {
                break;
            }
            out.push(known_xid);
            count += 1;
        }
    }
    count
}

/// procarray.c `KnownAssignedXidsGetOldestXmin`.
fn known_assigned_xids_get_oldest_xmin(a: &ProcArrayInner) -> TransactionId {
    let tail = a.tail_known_assigned_xids;
    let head = a.head_known_assigned_xids;
    for i in tail..head {
        if a.known_assigned_xids_valid[i] {
            return a.known_assigned_xids[i];
        }
    }
    INVALID_XID
}

// ===========================================================================
// helpers: FullXidRelativeTo, ProcGlobal/MyProc access (staged via step 15)
// ===========================================================================

/// procarray.c `FullXidRelativeTo`: lift a 32-bit xid into the epoch of `rel`.
fn full_xid_relative_to(rel: FullTransactionId, xid: TransactionId) -> FullTransactionId {
    let rel_xid = xid_from_full_transaction_id(rel);
    let delta = xid.0.wrapping_sub(rel_xid.0) as i32;
    crate::access::transam::full_transaction_id_from_u64(
        crate::access::transam::u64_from_full_transaction_id(rel).wrapping_add(delta as i64 as u64),
    )
}

/// A read-only view over ProcGlobal's mirror arrays + allProcs. Until step 15
/// populates ProcGlobal these are empty. Helper unifies the unsafe access to the
/// `static mut ProcGlobal` in one place (proc.rs owns the real lifecycle).
struct ProcGlobalView<'a> {
    xids: &'a [TransactionId],
    subxid_states: &'a [crate::storage::proc::XidCacheStatus],
    status_flags: &'a [u8],
    all_procs: &'a [*mut PGPROC],
}

impl<'a> ProcGlobalView<'a> {
    fn proc(&self, pgprocno: ProcNumber) -> Option<&'a PGPROC> {
        let idx = pgprocno as usize;
        self.all_procs
            .get(idx)
            .and_then(|p| if p.is_null() { None } else { Some(unsafe { &**p }) })
    }
    fn proc_mut(&self, pgprocno: ProcNumber) -> Option<&'a mut PGPROC> {
        let idx = pgprocno as usize;
        self.all_procs
            .get(idx)
            .and_then(|p| if p.is_null() { None } else { Some(unsafe { &mut **p }) })
    }
}

/// Run `f` over a read view of ProcGlobal. Empty until step 15.
fn with_proc_globals<R>(_shared: &Arc<SharedState>, f: impl FnOnce(&ProcGlobalView) -> R) -> R {
    // SAFETY: ProcGlobal is a process-lifetime static populated once at startup
    // (step 15) before any backend scans it; reads here are under the ProcArray
    // guard. Until step 15 the Option is None and we pass empty slices.
    unsafe {
        match crate::storage::proc::ProcGlobal {
            Some(ptr) => {
                let g = &*ptr;
                let view = ProcGlobalView {
                    xids: &g.xids,
                    subxid_states: &g.subxid_states,
                    status_flags: &g.status_flags,
                    all_procs: &g.all_procs,
                };
                f(&view)
            }
            None => {
                let view = ProcGlobalView {
                    xids: &[],
                    subxid_states: &[],
                    status_flags: &[],
                    all_procs: &[],
                };
                f(&view)
            }
        }
    }
}

fn with_proc_globals_mut<R>(shared: &Arc<SharedState>, f: impl FnOnce(&ProcGlobalView) -> R) -> R {
    with_proc_globals(shared, f)
}

/// PG `MyProc->xid` (the current backend's advertised xid). None until step 15.
fn my_proc_xid(_shared: &Arc<SharedState>) -> TransactionId {
    unsafe {
        match crate::storage::proc::MyProc {
            Some(p) => (*p).xid,
            None => INVALID_XID,
        }
    }
}

/// PG `MyProc->pgxactoff`. None until step 15.
fn my_proc_pgxactoff(_shared: &Arc<SharedState>) -> Option<usize> {
    unsafe {
        match crate::storage::proc::MyProc {
            Some(p) => Some((*p).pgxactoff as usize),
            None => None,
        }
    }
}

/// PG `MyProc->xmin = x`. No-op until step 15 (no MyProc yet).
fn set_my_proc_xmin(_shared: &Arc<SharedState>, x: TransactionId) {
    unsafe {
        if let Some(p) = crate::storage::proc::MyProc {
            (*p).xmin = x;
        }
    }
}

/// procarray.c `GET_VXID_FROM_PGPROC`.
fn vxid_from_proc(proc: &PGPROC) -> VirtualTransactionId {
    VirtualTransactionId {
        proc_number: proc.vxid.proc_number,
        local_transaction_id: proc.vxid.lxid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    fn shared() -> Arc<SharedState> {
        SharedState::new(SharedStateConfig::default())
    }

    // Deep snapshot/horizon tests await step 15 (ProcGlobal population). These
    // exercise the logic over an empty ProcGlobal, which is the correct staged
    // behavior: a backend with no other running transactions.

    #[tokio::test]
    async fn snapshot_over_empty_procglobal_is_sane() {
        let s = shared();
        snapshot_globals_scope(async {
            let mut snap = empty_snapshot();
            // latestCompletedXid starts at FIRST_NORMAL_FULL_TRANSACTION_ID (=3),
            // so xmax = xmin = 4 and xip is empty.
            get_snapshot_data(&s, &mut snap);
            assert!(snap.xip.is_empty());
            assert_eq!(snap.xmin.0, snap.xmax.0, "no running xacts: xmin == xmax");
            assert_eq!(snap.xmax.0, 4, "xmax = latestCompletedXid(3) + 1");
            assert!(!snap.suboverflowed);
            assert_eq!(recent_xmin().0, snap.xmin.0);
        })
        .await;
    }

    #[test]
    fn horizons_over_empty_array() {
        let s = shared();
        let h = compute_xid_horizons(&s);
        // With no procs and slots, every horizon collapses to latestCompleted+1.
        assert_eq!(h.oldest_considered_running.0, 4);
        assert_eq!(h.shared_oldest_nonremovable.0, 4);
        assert_eq!(h.data_oldest_nonremovable.0, 4);
    }

    #[tokio::test]
    async fn is_in_progress_false_for_empty_and_old() {
        let s = shared();
        snapshot_globals_scope(async {
            // RecentXmin is 0 here; an old/committed xid (2 = frozen) is rejected.
            assert!(!transaction_id_is_in_progress(&s, TransactionId(2)).await);
            // A future xid (>= latestCompleted+1) over an empty array: the
            // latestCompleted shortcut reports it running.
            assert!(transaction_id_is_in_progress(&s, TransactionId(100)).await);
        })
        .await;
    }

    #[test]
    fn known_assigned_add_get_remove_roundtrip() {
        // Exercise the KnownAssignedXids ring directly (hot-standby machinery).
        let mut a = ProcArrayInner::new(8, total_max_cached_subxids(8));
        known_assigned_xids_add(&mut a, TransactionId(10), TransactionId(13), true);
        assert_eq!(a.num_known_assigned_xids, 4);
        assert!(known_assigned_xid_exists(&a, TransactionId(11)));
        assert!(!known_assigned_xid_exists(&a, TransactionId(20)));

        let got = known_assigned_xids_get(&a, TransactionId(12));
        assert_eq!(got, vec![TransactionId(10), TransactionId(11)]);

        known_assigned_xids_remove(&mut a, TransactionId(10));
        assert_eq!(a.num_known_assigned_xids, 3);
        assert!(!known_assigned_xid_exists(&a, TransactionId(10)));

        let oldest = known_assigned_xids_get_oldest_xmin(&a);
        assert_eq!(oldest, TransactionId(11));
    }

    #[test]
    fn known_assigned_remove_preceding_prunes() {
        let mut a = ProcArrayInner::new(8, total_max_cached_subxids(8));
        known_assigned_xids_add(&mut a, TransactionId(10), TransactionId(20), true);
        known_assigned_xids_remove_preceding(&mut a, TransactionId(15));
        // 10..14 (5 xids) pruned, 15..20 (6 xids) remain.
        assert_eq!(a.num_known_assigned_xids, 6);
        assert!(!known_assigned_xid_exists(&a, TransactionId(14)));
        assert!(known_assigned_xid_exists(&a, TransactionId(15)));
    }

    #[test]
    fn full_xid_relative_to_lifts_epoch() {
        // xid just below rel's xid stays in the same epoch.
        let rel = crate::access::transam::full_transaction_id_from_u64((5u64 << 32) | 100);
        let f = full_xid_relative_to(rel, TransactionId(90));
        assert_eq!(crate::access::transam::epoch_from_full_transaction_id(f), 5);
        assert_eq!(xid_from_full_transaction_id(f).0, 90);
    }

    fn empty_snapshot() -> SnapshotData {
        SnapshotData {
            snapshot_type: crate::utils::snapshot::SnapshotType::Mvcc,
            xmin: INVALID_XID,
            xmax: INVALID_XID,
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
}
