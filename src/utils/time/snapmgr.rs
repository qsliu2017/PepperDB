//! utils/time/snapmgr.c - PostgreSQL snapshot manager
//!
//! The following functions return an MVCC snapshot that can be used in tuple
//! visibility checks:
//!
//! - GetTransactionSnapshot
//! - GetLatestSnapshot
//! - GetCatalogSnapshot
//! - GetNonHistoricCatalogSnapshot
//!
//! Each of these functions returns a reference to a statically allocated
//! snapshot.  The statically allocated snapshot is subject to change on any
//! snapshot-related function call, and should not be used directly.  Instead,
//! call PushActiveSnapshot() or RegisterSnapshot() to create a longer-lived
//! copy and use that.
//!
//! We keep track of snapshots in two ways: those "registered" by resowner.c,
//! and the "active snapshot" stack.  All snapshots in either of them live in
//! persistent memory.  When a snapshot is no longer in any of these lists
//! (tracked by separate refcounts on each snapshot), its memory can be freed.
//!
//! See the C source header comment for the full discussion of the
//! ActiveSnapshot stack and registered snapshots.

use crate::prelude::*;

use core::ffi::CStr;

use crate::access::subtrans::SubTransGetTopmostTransaction;
use crate::access::transam::{
    FirstNormalTransactionId, InvalidTransactionId, TransactionIdIsNormal, TransactionIdIsValid,
};
use crate::access::transam::transam::{
    TransactionIdFollowsOrEquals, TransactionIdPrecedes, TransactionIdFollows,
};
use crate::access::transam::xact::{
    xactGetCommittedChildren, GetCurrentCommandId, GetCurrentTransactionNestLevel,
    GetTopTransactionIdIfAny, IsSubTransaction, XactIsoLevel, XactReadOnly,
};
use crate::lib::pairingheap::{
    pairingheap, pairingheap_add, pairingheap_first, pairingheap_is_empty,
    pairingheap_is_singular, pairingheap_node, pairingheap_remove, pairingheap_reset,
};
use crate::miscadmin::{InvalidPid, MyDatabaseId};
use crate::nodes::pg_list::{lappend, lfirst, list_length, List, NIL};
use crate::port::pg_lfind::pg_lfind32;
use crate::storage::file::fd::{
    dirent, stat_t, AllocateDir, AllocateFile, FreeDir, FreeFile, ReadDirExtended, DIR,
};
use crate::storage::ipc::procarray::{
    GetMaxSnapshotSubxidCount, GetMaxSnapshotXidCount, GetSnapshotData,
    ProcArrayInstallImportedXmin, ProcArrayInstallRestoredXmin,
};
use crate::storage::ipc::shmem::{add_size, mul_size};
use crate::storage::lmgr::lock::{VirtualTransactionId, VirtualTransactionIdIsValid};
use crate::storage::procnumber::ProcNumber;
use crate::storage::lmgr::predicate::{
    GetSerializableTransactionSnapshot, SetSerializableTransactionSnapshot,
};
use crate::storage::lmgr::proc::{MyProc, MyProcPid, PGPROC};
use crate::utils::builtins::cstring_to_text;
use crate::utils::cache::syscache::{RelationHasSysCache, RelationInvalidatesSnapshotsOnly};
use crate::utils::hash::dynahash::HTAB;
use crate::utils::mmgr::mcxt::TopTransactionContext;
use crate::utils::resowner::resowner::{
    CurrentResourceOwner, ResourceOwner, ResourceOwnerDesc, ResourceOwnerEnlarge,
    ResourceOwnerForget, ResourceOwnerRemember, RELEASE_PRIO_SNAPSHOT_REFS,
    RESOURCE_RELEASE_AFTER_LOCKS,
};
use crate::utils::snapshot::{
    InvalidSnapshot, Snapshot, SnapshotData, SNAPSHOT_ANY, SNAPSHOT_MVCC, SNAPSHOT_SELF,
    SNAPSHOT_TOAST,
};

use crate::lib::stringinfo::{appendStringInfoString, initStringInfo, StringInfoData};
use crate::{appendStringInfo, current_cell, elog, ereport, errmsg, foreach};

use crate::pairingheap_const_container;
use crate::pairingheap_container;

/* ---- locally-stubbed, not-yet-ported dependencies ---- */

/// access/transam/parallel.c IsInParallelMode (not yet ported)
unsafe fn IsInParallelMode() -> bool {
    // TODO(pg-port): parallel.c IsInParallelMode
    false
}

/// access/xact.h IsolationUsesXactSnapshot() macro:
/// (XactIsoLevel >= XACT_REPEATABLE_READ)
unsafe fn IsolationUsesXactSnapshot() -> bool {
    XactIsoLevel >= XACT_REPEATABLE_READ
}

/// access/xact.h IsolationIsSerializable() macro:
/// (XactIsoLevel == XACT_SERIALIZABLE)
unsafe fn IsolationIsSerializable() -> bool {
    XactIsoLevel == XACT_SERIALIZABLE
}

/// access/xact.h XACT_REPEATABLE_READ
const XACT_REPEATABLE_READ: i32 = 2;
/// access/xact.h XACT_SERIALIZABLE
const XACT_SERIALIZABLE: i32 = 3;

/// storage/predicate.h PG_BINARY read/write modes for AllocateFile.
const PG_BINARY_R: &CStr = c"rb";
const PG_BINARY_W: &CStr = c"wb";

/// elog.h errcode_for_file_access (folded into block-comment "C also:" notes)
unsafe fn errcode_for_file_access() -> c_int {
    // TODO(pg-port): elog.c errcode_for_file_access
    0
}

/// storage/fd.h MAXPGPATH
const MAXPGPATH: usize = crate::pg_config_manual::MAXPGPATH;

/*
 * CurrentSnapshot points to the only snapshot taken in transaction-snapshot
 * mode, and to the latest one taken in a read-committed transaction.
 * SecondarySnapshot is a snapshot that's always up-to-date as of the current
 * instant, even in transaction-snapshot mode.  It should only be used for
 * special-purpose code (say, RI checking.)  CatalogSnapshot points to an
 * MVCC snapshot intended to be used for catalog scans; we must invalidate it
 * whenever a system catalog change occurs.
 *
 * These SnapshotData structs are static to simplify memory allocation
 * (see the hack in GetSnapshotData to avoid repeated malloc/free).
 */
static mut CurrentSnapshotData: SnapshotData = new_snapshot_data(SNAPSHOT_MVCC);
static mut SecondarySnapshotData: SnapshotData = new_snapshot_data(SNAPSHOT_MVCC);
static mut CatalogSnapshotData: SnapshotData = new_snapshot_data(SNAPSHOT_MVCC);
pub static mut SnapshotSelfData: SnapshotData = new_snapshot_data(SNAPSHOT_SELF);
pub static mut SnapshotAnyData: SnapshotData = new_snapshot_data(SNAPSHOT_ANY);
pub static mut SnapshotToastData: SnapshotData = new_snapshot_data(SNAPSHOT_TOAST);

/// Helper to build a zero-initialized SnapshotData with just the snapshot_type
/// set, matching the C aggregate initializer `{SNAPSHOT_xxx}`.
const fn new_snapshot_data(snapshot_type: crate::utils::snapshot::SnapshotType) -> SnapshotData {
    SnapshotData {
        snapshot_type,
        xmin: 0,
        xmax: 0,
        xip: null_mut(),
        xcnt: 0,
        subxip: null_mut(),
        subxcnt: 0,
        suboverflowed: false,
        takenDuringRecovery: false,
        copied: false,
        curcid: 0,
        speculativeToken: 0,
        vistest: null_mut(),
        active_count: 0,
        regd_count: 0,
        ph_node: pairingheap_node {
            first_child: null_mut(),
            next_sibling: null_mut(),
            prev_or_parent: null_mut(),
        },
        snapXactCompletionCount: 0,
    }
}

/* Pointers to valid snapshots */
static mut CurrentSnapshot: Snapshot = null_mut();
static mut SecondarySnapshot: Snapshot = null_mut();
static mut CatalogSnapshot: Snapshot = null_mut();
static mut HistoricSnapshot: Snapshot = null_mut();

/*
 * These are updated by GetSnapshotData.  We initialize them this way
 * for the convenience of TransactionIdIsInProgress: even in bootstrap
 * mode, we don't want it to say that BootstrapTransactionId is in progress.
 */
pub static mut TransactionXmin: TransactionId = FirstNormalTransactionId;
pub static mut RecentXmin: TransactionId = FirstNormalTransactionId;

/* (table, ctid) => (cmin, cmax) mapping during timetravel */
static mut tuplecid_data: *mut HTAB = null_mut();

/*
 * Elements of the active snapshot stack.
 *
 * Each element here accounts for exactly one active_count on SnapshotData.
 *
 * NB: the code assumes that elements in this list are in non-increasing
 * order of as_level; also, the list must be NULL-terminated.
 */
#[repr(C)]
pub struct ActiveSnapshotElt {
    pub as_snap: Snapshot,
    pub as_level: c_int,
    pub as_next: *mut ActiveSnapshotElt,
}

/* Top of the stack of active snapshots */
static mut ActiveSnapshot: *mut ActiveSnapshotElt = null_mut();

/*
 * Currently registered Snapshots.  Ordered in a heap by xmin, so that we can
 * quickly find the one with lowest xmin, to advance our MyProc->xmin.
 */
static mut RegisteredSnapshots: pairingheap = pairingheap {
    ph_compare: xmin_cmp,
    ph_arg: null_mut(),
    ph_root: null_mut(),
};

/* first GetTransactionSnapshot call in a transaction? */
pub static mut FirstSnapshotSet: bool = false;

/*
 * Remember the serializable transaction snapshot, if any.  We cannot trust
 * FirstSnapshotSet in combination with IsolationUsesXactSnapshot(), because
 * GUC may be reset before us, changing the value of IsolationUsesXactSnapshot.
 */
static mut FirstXactSnapshot: Snapshot = null_mut();

/* Define pathname of exported-snapshot files */
const SNAPSHOT_EXPORT_DIR: &str = "pg_snapshots";

/* Structure holding info about exported snapshot. */
#[repr(C)]
pub struct ExportedSnapshot {
    pub snapfile: *mut c_char,
    pub snapshot: Snapshot,
}

/* Current xact's exported snapshots (a list of ExportedSnapshot structs) */
static mut exportedSnapshots: *mut List = NIL;

/* ResourceOwner callbacks to track snapshot references */

static snapshot_resowner_desc: ResourceOwnerDesc = ResourceOwnerDesc {
    name: c"snapshot reference".as_ptr(),
    release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
    release_priority: RELEASE_PRIO_SNAPSHOT_REFS,
    ReleaseResource: ResOwnerReleaseSnapshot,
    DebugPrint: None, /* the default message is fine */
};

/* Convenience wrappers over ResourceOwnerRemember/Forget */
#[inline]
unsafe fn ResourceOwnerRememberSnapshot(owner: ResourceOwner, snap: Snapshot) {
    ResourceOwnerRemember(
        owner,
        PointerGetDatum(snap as *const c_void),
        &snapshot_resowner_desc,
    );
}
#[inline]
unsafe fn ResourceOwnerForgetSnapshot(owner: ResourceOwner, snap: Snapshot) {
    ResourceOwnerForget(
        owner,
        PointerGetDatum(snap as *const c_void),
        &snapshot_resowner_desc,
    );
}

/*
 * Snapshot fields to be serialized.
 *
 * Only these fields need to be sent to the cooperating backend; the
 * remaining ones can (and must) be set by the receiver upon restore.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SerializedSnapshotData {
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub xcnt: uint32,
    pub subxcnt: int32,
    pub suboverflowed: bool,
    pub takenDuringRecovery: bool,
    pub curcid: CommandId,
}

/*
 * GetTransactionSnapshot
 *		Get the appropriate snapshot for a new query in a transaction.
 *
 * Note that the return value points at static storage that will be modified
 * by future calls and by CommandCounterIncrement().  Callers must call
 * RegisterSnapshot or PushActiveSnapshot on the returned snap before doing
 * any other non-trivial work that could invalidate it.
 */
pub unsafe fn GetTransactionSnapshot() -> Snapshot {
    /*
     * Return historic snapshot if doing logical decoding.
     *
     * Historic snapshots are only usable for catalog access, not for
     * general-purpose queries.  The caller is responsible for ensuring that
     * the snapshot is used correctly! (PostgreSQL code never calls this
     * during logical decoding, but extensions can do it.)
     */
    if HistoricSnapshotActive() {
        /*
         * We'll never need a non-historic transaction snapshot in this
         * (sub-)transaction, so there's no need to be careful to set one up
         * for later calls to GetTransactionSnapshot().
         */
        Assert!(!FirstSnapshotSet);
        return HistoricSnapshot;
    }

    /* First call in transaction? */
    if !FirstSnapshotSet {
        /*
         * Don't allow catalog snapshot to be older than xact snapshot.  Must
         * do this first to allow the empty-heap Assert to succeed.
         */
        InvalidateCatalogSnapshot();

        Assert!(pairingheap_is_empty(&raw mut RegisteredSnapshots));
        Assert!(FirstXactSnapshot.is_null());

        if IsInParallelMode() {
            elog!(
                ERROR,
                "cannot take query snapshot during a parallel operation"
            );
        }

        /*
         * In transaction-snapshot mode, the first snapshot must live until
         * end of xact regardless of what the caller does with it, so we must
         * make a copy of it rather than returning CurrentSnapshotData
         * directly.  Furthermore, if we're running in serializable mode,
         * predicate.c needs to wrap the snapshot fetch in its own processing.
         */
        if IsolationUsesXactSnapshot() {
            /* First, create the snapshot in CurrentSnapshotData */
            if IsolationIsSerializable() {
                CurrentSnapshot =
                    GetSerializableTransactionSnapshot(&raw mut CurrentSnapshotData);
            } else {
                CurrentSnapshot = GetSnapshotData(&raw mut CurrentSnapshotData);
            }
            /* Make a saved copy */
            CurrentSnapshot = CopySnapshot(CurrentSnapshot);
            FirstXactSnapshot = CurrentSnapshot;
            /* Mark it as "registered" in FirstXactSnapshot */
            (*FirstXactSnapshot).regd_count += 1;
            pairingheap_add(&raw mut RegisteredSnapshots, &raw mut (*FirstXactSnapshot).ph_node);
        } else {
            CurrentSnapshot = GetSnapshotData(&raw mut CurrentSnapshotData);
        }

        FirstSnapshotSet = true;
        return CurrentSnapshot;
    }

    if IsolationUsesXactSnapshot() {
        return CurrentSnapshot;
    }

    /* Don't allow catalog snapshot to be older than xact snapshot. */
    InvalidateCatalogSnapshot();

    CurrentSnapshot = GetSnapshotData(&raw mut CurrentSnapshotData);

    CurrentSnapshot
}

/*
 * GetLatestSnapshot
 *		Get a snapshot that is up-to-date as of the current instant,
 *		even if we are executing in transaction-snapshot mode.
 */
pub unsafe fn GetLatestSnapshot() -> Snapshot {
    /*
     * We might be able to relax this, but nothing that could otherwise work
     * needs it.
     */
    if IsInParallelMode() {
        elog!(
            ERROR,
            "cannot update SecondarySnapshot during a parallel operation"
        );
    }

    /*
     * So far there are no cases requiring support for GetLatestSnapshot()
     * during logical decoding, but it wouldn't be hard to add if required.
     */
    Assert!(!HistoricSnapshotActive());

    /* If first call in transaction, go ahead and set the xact snapshot */
    if !FirstSnapshotSet {
        return GetTransactionSnapshot();
    }

    SecondarySnapshot = GetSnapshotData(&raw mut SecondarySnapshotData);

    SecondarySnapshot
}

/*
 * GetCatalogSnapshot
 *		Get a snapshot that is sufficiently up-to-date for scan of the
 *		system catalog with the specified OID.
 */
pub unsafe fn GetCatalogSnapshot(relid: Oid) -> Snapshot {
    /*
     * Return historic snapshot while we're doing logical decoding, so we can
     * see the appropriate state of the catalog.
     *
     * This is the primary reason for needing to reset the system caches after
     * finishing decoding.
     */
    if HistoricSnapshotActive() {
        return HistoricSnapshot;
    }

    GetNonHistoricCatalogSnapshot(relid)
}

/*
 * GetNonHistoricCatalogSnapshot
 *		Get a snapshot that is sufficiently up-to-date for scan of the system
 *		catalog with the specified OID, even while historic snapshots are set
 *		up.
 */
pub unsafe fn GetNonHistoricCatalogSnapshot(relid: Oid) -> Snapshot {
    /*
     * If the caller is trying to scan a relation that has no syscache, no
     * catcache invalidations will be sent when it is updated.  For a few key
     * relations, snapshot invalidations are sent instead.  If we're trying to
     * scan a relation for which neither catcache nor snapshot invalidations
     * are sent, we must refresh the snapshot every time.
     */
    if !CatalogSnapshot.is_null()
        && !RelationInvalidatesSnapshotsOnly(relid)
        && !RelationHasSysCache(relid)
    {
        InvalidateCatalogSnapshot();
    }

    if CatalogSnapshot.is_null() {
        /* Get new snapshot. */
        CatalogSnapshot = GetSnapshotData(&raw mut CatalogSnapshotData);

        /*
         * Make sure the catalog snapshot will be accounted for in decisions
         * about advancing PGPROC->xmin.  We could apply RegisterSnapshot, but
         * that would result in making a physical copy, which is overkill; and
         * it would also create a dependency on some resource owner, which we
         * do not want for reasons explained at the head of this file. Instead
         * just shove the CatalogSnapshot into the pairing heap manually. This
         * has to be reversed in InvalidateCatalogSnapshot, of course.
         *
         * NB: it had better be impossible for this to throw error, since the
         * CatalogSnapshot pointer is already valid.
         */
        pairingheap_add(&raw mut RegisteredSnapshots, &raw mut (*CatalogSnapshot).ph_node);
    }

    CatalogSnapshot
}

/*
 * InvalidateCatalogSnapshot
 *		Mark the current catalog snapshot, if any, as invalid
 *
 * We could change this API to allow the caller to provide more fine-grained
 * invalidation details, so that a change to relation A wouldn't prevent us
 * from using our cached snapshot to scan relation B, but so far there's no
 * evidence that the CPU cycles we spent tracking such fine details would be
 * well-spent.
 */
pub unsafe fn InvalidateCatalogSnapshot() {
    if !CatalogSnapshot.is_null() {
        pairingheap_remove(&raw mut RegisteredSnapshots, &raw mut (*CatalogSnapshot).ph_node);
        CatalogSnapshot = null_mut();
        SnapshotResetXmin();
    }
}

/*
 * InvalidateCatalogSnapshotConditionally
 *		Drop catalog snapshot if it's the only one we have
 *
 * This is called when we are about to wait for client input, so we don't
 * want to continue holding the catalog snapshot if it might mean that the
 * global xmin horizon can't advance.  However, if there are other snapshots
 * still active or registered, the catalog snapshot isn't likely to be the
 * oldest one, so we might as well keep it.
 */
pub unsafe fn InvalidateCatalogSnapshotConditionally() {
    if !CatalogSnapshot.is_null()
        && ActiveSnapshot.is_null()
        && pairingheap_is_singular(&raw mut RegisteredSnapshots)
    {
        InvalidateCatalogSnapshot();
    }
}

/*
 * SnapshotSetCommandId
 *		Propagate CommandCounterIncrement into the static snapshots, if set
 */
pub unsafe fn SnapshotSetCommandId(curcid: CommandId) {
    if !FirstSnapshotSet {
        return;
    }

    if !CurrentSnapshot.is_null() {
        (*CurrentSnapshot).curcid = curcid;
    }
    if !SecondarySnapshot.is_null() {
        (*SecondarySnapshot).curcid = curcid;
    }
    /* Should we do the same with CatalogSnapshot? */
}

/*
 * SetTransactionSnapshot
 *		Set the transaction's snapshot from an imported MVCC snapshot.
 *
 * Note that this is very closely tied to GetTransactionSnapshot --- it
 * must take care of all the same considerations as the first-snapshot case
 * in GetTransactionSnapshot.
 */
unsafe fn SetTransactionSnapshot(
    sourcesnap: Snapshot,
    sourcevxid: *mut VirtualTransactionId,
    sourcepid: c_int,
    sourceproc: *mut PGPROC,
) {
    /* Caller should have checked this already */
    Assert!(!FirstSnapshotSet);

    /* Better do this to ensure following Assert succeeds. */
    InvalidateCatalogSnapshot();

    Assert!(pairingheap_is_empty(&raw mut RegisteredSnapshots));
    Assert!(FirstXactSnapshot.is_null());
    Assert!(!HistoricSnapshotActive());

    /*
     * Even though we are not going to use the snapshot it computes, we must
     * call GetSnapshotData, for two reasons: (1) to be sure that
     * CurrentSnapshotData's XID arrays have been allocated, and (2) to update
     * the state for GlobalVis*.
     */
    CurrentSnapshot = GetSnapshotData(&raw mut CurrentSnapshotData);

    /*
     * Now copy appropriate fields from the source snapshot.
     */
    (*CurrentSnapshot).xmin = (*sourcesnap).xmin;
    (*CurrentSnapshot).xmax = (*sourcesnap).xmax;
    (*CurrentSnapshot).xcnt = (*sourcesnap).xcnt;
    Assert!((*sourcesnap).xcnt <= GetMaxSnapshotXidCount() as uint32);
    if (*sourcesnap).xcnt > 0 {
        core::ptr::copy_nonoverlapping(
            (*sourcesnap).xip,
            (*CurrentSnapshot).xip,
            (*sourcesnap).xcnt as usize,
        );
    }
    (*CurrentSnapshot).subxcnt = (*sourcesnap).subxcnt;
    Assert!((*sourcesnap).subxcnt <= GetMaxSnapshotSubxidCount());
    if (*sourcesnap).subxcnt > 0 {
        core::ptr::copy_nonoverlapping(
            (*sourcesnap).subxip,
            (*CurrentSnapshot).subxip,
            (*sourcesnap).subxcnt as usize,
        );
    }
    (*CurrentSnapshot).suboverflowed = (*sourcesnap).suboverflowed;
    (*CurrentSnapshot).takenDuringRecovery = (*sourcesnap).takenDuringRecovery;
    /* NB: curcid should NOT be copied, it's a local matter */

    (*CurrentSnapshot).snapXactCompletionCount = 0;

    /*
     * Now we have to fix what GetSnapshotData did with MyProc->xmin and
     * TransactionXmin.  There is a race condition: to make sure we are not
     * causing the global xmin to go backwards, we have to test that the
     * source transaction is still running, and that has to be done
     * atomically. So let procarray.c do it.
     *
     * Note: in serializable mode, predicate.c will do this a second time. It
     * doesn't seem worth contorting the logic here to avoid two calls,
     * especially since it's not clear that predicate.c *must* do this.
     */
    if !sourceproc.is_null() {
        if !ProcArrayInstallRestoredXmin((*CurrentSnapshot).xmin, sourceproc) {
            ereport!(
                ERROR,
                errmsg!("could not import the requested snapshot")
            );
            // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            //         errdetail("The source transaction is not running anymore.")
        }
    } else if !ProcArrayInstallImportedXmin((*CurrentSnapshot).xmin, sourcevxid) {
        ereport!(
            ERROR,
            errmsg!(
                "could not import the requested snapshot"
            )
        );
        // C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        //         errdetail("The source process with PID %d is not running anymore.", sourcepid)
        let _ = sourcepid;
    }

    /*
     * In transaction-snapshot mode, the first snapshot must live until end of
     * xact, so we must make a copy of it.  Furthermore, if we're running in
     * serializable mode, predicate.c needs to do its own processing.
     */
    if IsolationUsesXactSnapshot() {
        if IsolationIsSerializable() {
            SetSerializableTransactionSnapshot(CurrentSnapshot, sourcevxid, sourcepid);
        }
        /* Make a saved copy */
        CurrentSnapshot = CopySnapshot(CurrentSnapshot);
        FirstXactSnapshot = CurrentSnapshot;
        /* Mark it as "registered" in FirstXactSnapshot */
        (*FirstXactSnapshot).regd_count += 1;
        pairingheap_add(&raw mut RegisteredSnapshots, &raw mut (*FirstXactSnapshot).ph_node);
    }

    FirstSnapshotSet = true;
}

/*
 * CopySnapshot
 *		Copy the given snapshot.
 *
 * The copy is palloc'd in TopTransactionContext and has initial refcounts set
 * to 0.  The returned snapshot has the copied flag set.
 */
unsafe fn CopySnapshot(snapshot: Snapshot) -> Snapshot {
    let newsnap: Snapshot;
    let subxipoff: Size;
    let mut size: Size;

    Assert!(snapshot != InvalidSnapshot);

    /* We allocate any XID arrays needed in the same palloc block. */
    size = core::mem::size_of::<SnapshotData>()
        + (*snapshot).xcnt as usize * core::mem::size_of::<TransactionId>();
    subxipoff = size;
    if (*snapshot).subxcnt > 0 {
        size += (*snapshot).subxcnt as usize * core::mem::size_of::<TransactionId>();
    }

    newsnap = MemoryContextAlloc(TopTransactionContext, size) as Snapshot;
    core::ptr::copy_nonoverlapping(
        snapshot as *const u8,
        newsnap as *mut u8,
        core::mem::size_of::<SnapshotData>(),
    );

    (*newsnap).regd_count = 0;
    (*newsnap).active_count = 0;
    (*newsnap).copied = true;
    (*newsnap).snapXactCompletionCount = 0;

    /* setup XID array */
    if (*snapshot).xcnt > 0 {
        (*newsnap).xip = newsnap.add(1) as *mut TransactionId;
        core::ptr::copy_nonoverlapping(
            (*snapshot).xip,
            (*newsnap).xip,
            (*snapshot).xcnt as usize,
        );
    } else {
        (*newsnap).xip = null_mut();
    }

    /*
     * Setup subXID array. Don't bother to copy it if it had overflowed,
     * though, because it's not used anywhere in that case. Except if it's a
     * snapshot taken during recovery; all the top-level XIDs are in subxip as
     * well in that case, so we mustn't lose them.
     */
    if (*snapshot).subxcnt > 0 && (!(*snapshot).suboverflowed || (*snapshot).takenDuringRecovery) {
        (*newsnap).subxip = (newsnap as *mut c_char).add(subxipoff) as *mut TransactionId;
        core::ptr::copy_nonoverlapping(
            (*snapshot).subxip,
            (*newsnap).subxip,
            (*snapshot).subxcnt as usize,
        );
    } else {
        (*newsnap).subxip = null_mut();
    }

    newsnap
}

/*
 * FreeSnapshot
 *		Free the memory associated with a snapshot.
 */
unsafe fn FreeSnapshot(snapshot: Snapshot) {
    Assert!((*snapshot).regd_count == 0);
    Assert!((*snapshot).active_count == 0);
    Assert!((*snapshot).copied);

    pfree(snapshot as *mut c_void);
}

/*
 * PushActiveSnapshot
 *		Set the given snapshot as the current active snapshot
 *
 * If the passed snapshot is a statically-allocated one, or it is possibly
 * subject to a future command counter update, create a new long-lived copy
 * with active refcount=1.  Otherwise, only increment the refcount.
 */
pub unsafe fn PushActiveSnapshot(snapshot: Snapshot) {
    PushActiveSnapshotWithLevel(snapshot, GetCurrentTransactionNestLevel());
}

/*
 * PushActiveSnapshotWithLevel
 *		Set the given snapshot as the current active snapshot
 *
 * Same as PushActiveSnapshot except that caller can specify the
 * transaction nesting level that "owns" the snapshot.  This level
 * must not be deeper than the current top of the snapshot stack.
 */
pub unsafe fn PushActiveSnapshotWithLevel(snapshot: Snapshot, snap_level: c_int) {
    let newactive: *mut ActiveSnapshotElt;

    Assert!(snapshot != InvalidSnapshot);
    Assert!(ActiveSnapshot.is_null() || snap_level >= (*ActiveSnapshot).as_level);

    newactive = MemoryContextAlloc(
        TopTransactionContext,
        core::mem::size_of::<ActiveSnapshotElt>(),
    ) as *mut ActiveSnapshotElt;

    /*
     * Checking SecondarySnapshot is probably useless here, but it seems
     * better to be sure.
     */
    if snapshot == CurrentSnapshot || snapshot == SecondarySnapshot || !(*snapshot).copied {
        (*newactive).as_snap = CopySnapshot(snapshot);
    } else {
        (*newactive).as_snap = snapshot;
    }

    (*newactive).as_next = ActiveSnapshot;
    (*newactive).as_level = snap_level;

    (*(*newactive).as_snap).active_count += 1;

    ActiveSnapshot = newactive;
}

/*
 * PushCopiedSnapshot
 *		As above, except forcibly copy the presented snapshot.
 *
 * This should be used when the ActiveSnapshot has to be modifiable, for
 * example if the caller intends to call UpdateActiveSnapshotCommandId.
 * The new snapshot will be released when popped from the stack.
 */
pub unsafe fn PushCopiedSnapshot(snapshot: Snapshot) {
    PushActiveSnapshot(CopySnapshot(snapshot));
}

/*
 * UpdateActiveSnapshotCommandId
 *
 * Update the current CID of the active snapshot.  This can only be applied
 * to a snapshot that is not referenced elsewhere.
 */
pub unsafe fn UpdateActiveSnapshotCommandId() {
    let save_curcid: CommandId;
    let curcid: CommandId;

    Assert!(!ActiveSnapshot.is_null());
    Assert!((*(*ActiveSnapshot).as_snap).active_count == 1);
    Assert!((*(*ActiveSnapshot).as_snap).regd_count == 0);

    /*
     * Don't allow modification of the active snapshot during parallel
     * operation.  We share the snapshot to worker backends at the beginning
     * of parallel operation, so any change to the snapshot can lead to
     * inconsistencies.  We have other defenses against
     * CommandCounterIncrement, but there are a few places that call this
     * directly, so we put an additional guard here.
     */
    save_curcid = (*(*ActiveSnapshot).as_snap).curcid;
    curcid = GetCurrentCommandId(false);
    if IsInParallelMode() && save_curcid != curcid {
        elog!(
            ERROR,
            "cannot modify commandid in active snapshot during a parallel operation"
        );
    }
    (*(*ActiveSnapshot).as_snap).curcid = curcid;
}

/*
 * PopActiveSnapshot
 *
 * Remove the topmost snapshot from the active snapshot stack, decrementing the
 * reference count, and free it if this was the last reference.
 */
pub unsafe fn PopActiveSnapshot() {
    let newstack: *mut ActiveSnapshotElt;

    newstack = (*ActiveSnapshot).as_next;

    Assert!((*(*ActiveSnapshot).as_snap).active_count > 0);

    (*(*ActiveSnapshot).as_snap).active_count -= 1;

    if (*(*ActiveSnapshot).as_snap).active_count == 0
        && (*(*ActiveSnapshot).as_snap).regd_count == 0
    {
        FreeSnapshot((*ActiveSnapshot).as_snap);
    }

    pfree(ActiveSnapshot as *mut c_void);
    ActiveSnapshot = newstack;

    SnapshotResetXmin();
}

/*
 * GetActiveSnapshot
 *		Return the topmost snapshot in the Active stack.
 */
pub unsafe fn GetActiveSnapshot() -> Snapshot {
    Assert!(!ActiveSnapshot.is_null());

    (*ActiveSnapshot).as_snap
}

/*
 * ActiveSnapshotSet
 *		Return whether there is at least one snapshot in the Active stack
 */
pub unsafe fn ActiveSnapshotSet() -> bool {
    !ActiveSnapshot.is_null()
}

/*
 * RegisterSnapshot
 *		Register a snapshot as being in use by the current resource owner
 *
 * If InvalidSnapshot is passed, it is not registered.
 */
pub unsafe fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot {
    if snapshot == InvalidSnapshot {
        return InvalidSnapshot;
    }

    RegisterSnapshotOnOwner(snapshot, CurrentResourceOwner)
}

/*
 * RegisterSnapshotOnOwner
 *		As above, but use the specified resource owner
 */
pub unsafe fn RegisterSnapshotOnOwner(snapshot: Snapshot, owner: ResourceOwner) -> Snapshot {
    let snap: Snapshot;

    if snapshot == InvalidSnapshot {
        return InvalidSnapshot;
    }

    /* Static snapshot?  Create a persistent copy */
    snap = if (*snapshot).copied {
        snapshot
    } else {
        CopySnapshot(snapshot)
    };

    /* and tell resowner.c about it */
    ResourceOwnerEnlarge(owner);
    (*snap).regd_count += 1;
    ResourceOwnerRememberSnapshot(owner, snap);

    if (*snap).regd_count == 1 {
        pairingheap_add(&raw mut RegisteredSnapshots, &raw mut (*snap).ph_node);
    }

    snap
}

/*
 * UnregisterSnapshot
 *
 * Decrement the reference count of a snapshot, remove the corresponding
 * reference from CurrentResourceOwner, and free the snapshot if no more
 * references remain.
 */
pub unsafe fn UnregisterSnapshot(snapshot: Snapshot) {
    if snapshot.is_null() {
        return;
    }

    UnregisterSnapshotFromOwner(snapshot, CurrentResourceOwner);
}

/*
 * UnregisterSnapshotFromOwner
 *		As above, but use the specified resource owner
 */
pub unsafe fn UnregisterSnapshotFromOwner(snapshot: Snapshot, owner: ResourceOwner) {
    if snapshot.is_null() {
        return;
    }

    ResourceOwnerForgetSnapshot(owner, snapshot);
    UnregisterSnapshotNoOwner(snapshot);
}

unsafe fn UnregisterSnapshotNoOwner(snapshot: Snapshot) {
    Assert!((*snapshot).regd_count > 0);
    Assert!(!pairingheap_is_empty(&raw mut RegisteredSnapshots));

    (*snapshot).regd_count -= 1;
    if (*snapshot).regd_count == 0 {
        pairingheap_remove(&raw mut RegisteredSnapshots, &raw mut (*snapshot).ph_node);
    }

    if (*snapshot).regd_count == 0 && (*snapshot).active_count == 0 {
        FreeSnapshot(snapshot);
        SnapshotResetXmin();
    }
}

/*
 * Comparison function for RegisteredSnapshots heap.  Snapshots are ordered
 * by xmin, so that the snapshot with smallest xmin is at the top.
 */
unsafe fn xmin_cmp(a: *const pairingheap_node, b: *const pairingheap_node, _arg: *mut c_void) -> c_int {
    let asnap: *const SnapshotData = pairingheap_const_container!(SnapshotData, ph_node, a);
    let bsnap: *const SnapshotData = pairingheap_const_container!(SnapshotData, ph_node, b);

    if TransactionIdPrecedes((*asnap).xmin, (*bsnap).xmin) {
        1
    } else if TransactionIdFollows((*asnap).xmin, (*bsnap).xmin) {
        -1
    } else {
        0
    }
}

/*
 * SnapshotResetXmin
 *
 * If there are no more snapshots, we can reset our PGPROC->xmin to
 * InvalidTransactionId. Note we can do this without locking because we assume
 * that storing an Xid is atomic.
 *
 * Even if there are some remaining snapshots, we may be able to advance our
 * PGPROC->xmin to some degree.  This typically happens when a portal is
 * dropped.  For efficiency, we only consider recomputing PGPROC->xmin when
 * the active snapshot stack is empty; this allows us not to need to track
 * which active snapshot is oldest.
 */
unsafe fn SnapshotResetXmin() {
    let minSnapshot: Snapshot;

    if !ActiveSnapshot.is_null() {
        return;
    }

    if pairingheap_is_empty(&raw mut RegisteredSnapshots) {
        (*MyProc).xmin = InvalidTransactionId;
        TransactionXmin = InvalidTransactionId;
        return;
    }

    minSnapshot = pairingheap_container!(
        SnapshotData,
        ph_node,
        pairingheap_first(&raw mut RegisteredSnapshots)
    );

    if TransactionIdPrecedes((*MyProc).xmin, (*minSnapshot).xmin) {
        (*MyProc).xmin = (*minSnapshot).xmin;
        TransactionXmin = (*minSnapshot).xmin;
    }
}

/*
 * AtSubCommit_Snapshot
 */
pub unsafe fn AtSubCommit_Snapshot(level: c_int) {
    let mut active: *mut ActiveSnapshotElt;

    /*
     * Relabel the active snapshots set in this subtransaction as though they
     * are owned by the parent subxact.
     */
    active = ActiveSnapshot;
    while !active.is_null() {
        if (*active).as_level < level {
            break;
        }
        (*active).as_level = level - 1;
        active = (*active).as_next;
    }
}

/*
 * AtSubAbort_Snapshot
 *		Clean up snapshots after a subtransaction abort
 */
pub unsafe fn AtSubAbort_Snapshot(level: c_int) {
    /* Forget the active snapshots set by this subtransaction */
    while !ActiveSnapshot.is_null() && (*ActiveSnapshot).as_level >= level {
        let next: *mut ActiveSnapshotElt;

        next = (*ActiveSnapshot).as_next;

        /*
         * Decrement the snapshot's active count.  If it's still registered or
         * marked as active by an outer subtransaction, we can't free it yet.
         */
        Assert!((*(*ActiveSnapshot).as_snap).active_count >= 1);
        (*(*ActiveSnapshot).as_snap).active_count -= 1;

        if (*(*ActiveSnapshot).as_snap).active_count == 0
            && (*(*ActiveSnapshot).as_snap).regd_count == 0
        {
            FreeSnapshot((*ActiveSnapshot).as_snap);
        }

        /* and free the stack element */
        pfree(ActiveSnapshot as *mut c_void);

        ActiveSnapshot = next;
    }

    SnapshotResetXmin();
}

/*
 * AtEOXact_Snapshot
 *		Snapshot manager's cleanup function for end of transaction
 */
pub unsafe fn AtEOXact_Snapshot(isCommit: bool, resetXmin: bool) {
    /*
     * In transaction-snapshot mode we must release our privately-managed
     * reference to the transaction snapshot.  We must remove it from
     * RegisteredSnapshots to keep the check below happy.  But we don't bother
     * to do FreeSnapshot, for two reasons: the memory will go away with
     * TopTransactionContext anyway, and if someone has left the snapshot
     * stacked as active, we don't want the code below to be chasing through a
     * dangling pointer.
     */
    if !FirstXactSnapshot.is_null() {
        Assert!((*FirstXactSnapshot).regd_count > 0);
        Assert!(!pairingheap_is_empty(&raw mut RegisteredSnapshots));
        pairingheap_remove(&raw mut RegisteredSnapshots, &raw mut (*FirstXactSnapshot).ph_node);
    }
    FirstXactSnapshot = null_mut();

    /*
     * If we exported any snapshots, clean them up.
     */
    if exportedSnapshots != NIL {
        /*
         * Get rid of the files.  Unlink failure is only a WARNING because (1)
         * it's too late to abort the transaction, and (2) leaving a leaked
         * file around has little real consequence anyway.
         *
         * We also need to remove the snapshots from RegisteredSnapshots to
         * prevent a warning below.
         *
         * As with the FirstXactSnapshot, we don't need to free resources of
         * the snapshot itself as it will go away with the memory context.
         */
        foreach!(lc, exportedSnapshots, {
            let esnap: *mut ExportedSnapshot = lfirst(current_cell!(lc)) as *mut ExportedSnapshot;

            if unlink((*esnap).snapfile) != 0 {
                elog!(
                    WARNING,
                    "could not unlink file \"{}\": {}",
                    CStr::from_ptr((*esnap).snapfile).to_string_lossy(),
                    pg_strerror_errno()
                );
            }

            pairingheap_remove(
                &raw mut RegisteredSnapshots,
                &raw mut (*(*esnap).snapshot).ph_node,
            );
        });

        exportedSnapshots = NIL;
    }

    /* Drop catalog snapshot if any */
    InvalidateCatalogSnapshot();

    /* On commit, complain about leftover snapshots */
    if isCommit {
        let mut active: *mut ActiveSnapshotElt;

        if !pairingheap_is_empty(&raw mut RegisteredSnapshots) {
            elog!(WARNING, "registered snapshots seem to remain after cleanup");
        }

        /* complain about unpopped active snapshots */
        active = ActiveSnapshot;
        while !active.is_null() {
            elog!(WARNING, "snapshot {:p} still active", active);
            active = (*active).as_next;
        }
    }

    /*
     * And reset our state.  We don't need to free the memory explicitly --
     * it'll go away with TopTransactionContext.
     */
    ActiveSnapshot = null_mut();
    pairingheap_reset(&raw mut RegisteredSnapshots);

    CurrentSnapshot = null_mut();
    SecondarySnapshot = null_mut();

    FirstSnapshotSet = false;

    /*
     * During normal commit processing, we call ProcArrayEndTransaction() to
     * reset the MyProc->xmin. That call happens prior to the call to
     * AtEOXact_Snapshot(), so we need not touch xmin here at all.
     */
    if resetXmin {
        SnapshotResetXmin();
    }

    Assert!(resetXmin || (*MyProc).xmin == 0);
}

/*
 * ExportSnapshot
 *		Export the snapshot to a file so that other backends can import it.
 *		Returns the token (the file name) that can be used to import this
 *		snapshot.
 */
pub unsafe fn ExportSnapshot(mut snapshot: Snapshot) -> *mut c_char {
    let topXid: TransactionId;
    let mut children: *mut TransactionId = null_mut();
    let esnap: *mut ExportedSnapshot;
    let nchildren: c_int;
    let addTopXid: c_int;
    let mut buf: StringInfoData = core::mem::zeroed();
    let f: *mut c_void; /* FILE* */
    let mut i: c_int;
    let oldcxt: MemoryContext;
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut pathtmp: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    /*
     * It's tempting to call RequireTransactionBlock here, since it's not very
     * useful to export a snapshot that will disappear immediately afterwards.
     * However, we haven't got enough information to do that, since we don't
     * know if we're at top level or not.  For example, we could be inside a
     * plpgsql function that is going to fire off other transactions via
     * dblink.  Rather than disallow perfectly legitimate usages, don't make a
     * check.
     *
     * Also note that we don't make any restriction on the transaction's
     * isolation level; however, importers must check the level if they are
     * serializable.
     */

    /*
     * Get our transaction ID if there is one, to include in the snapshot.
     */
    topXid = GetTopTransactionIdIfAny();

    /*
     * We cannot export a snapshot from a subtransaction because there's no
     * easy way for importers to verify that the same subtransaction is still
     * running.
     */
    if IsSubTransaction() {
        ereport!(
            ERROR,
            errmsg!("cannot export a snapshot from a subtransaction")
        );
        // C also: errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
    }

    /*
     * We do however allow previous committed subtransactions to exist.
     * Importers of the snapshot must see them as still running, so get their
     * XIDs to add them to the snapshot.
     */
    nchildren = xactGetCommittedChildren(&raw mut children);

    /*
     * Generate file path for the snapshot.  We start numbering of snapshots
     * inside the transaction from 1.
     */
    snprintf_path(
        &mut path,
        &format!(
            "{}/{:08X}-{:08X}-{}",
            SNAPSHOT_EXPORT_DIR,
            (*MyProc).vxid.procNumber,
            (*MyProc).vxid.lxid,
            list_length(exportedSnapshots) + 1
        ),
    );

    /*
     * Copy the snapshot into TopTransactionContext, add it to the
     * exportedSnapshots list, and mark it pseudo-registered.  We do this to
     * ensure that the snapshot's xmin is honored for the rest of the
     * transaction.
     */
    snapshot = CopySnapshot(snapshot);

    oldcxt = MemoryContextSwitchTo(TopTransactionContext);
    esnap = palloc(core::mem::size_of::<ExportedSnapshot>()) as *mut ExportedSnapshot;
    (*esnap).snapfile = pstrdup(path.as_ptr());
    (*esnap).snapshot = snapshot;
    exportedSnapshots = lappend(exportedSnapshots, esnap as *mut c_void);
    MemoryContextSwitchTo(oldcxt);

    (*snapshot).regd_count += 1;
    pairingheap_add(&raw mut RegisteredSnapshots, &raw mut (*snapshot).ph_node);

    /*
     * Fill buf with a text serialization of the snapshot, plus identification
     * data about this transaction.  The format expected by ImportSnapshot is
     * pretty rigid: each line must be fieldname:value.
     */
    initStringInfo(&raw mut buf);

    appendStringInfo!(
        &raw mut buf,
        "vxid:{}/{}\n",
        (*MyProc).vxid.procNumber,
        (*MyProc).vxid.lxid
    );
    appendStringInfo!(&raw mut buf, "pid:{}\n", MyProcPid);
    appendStringInfo!(&raw mut buf, "dbid:{}\n", MyDatabaseId);
    appendStringInfo!(&raw mut buf, "iso:{}\n", XactIsoLevel);
    appendStringInfo!(&raw mut buf, "ro:{}\n", XactReadOnly as c_int);

    appendStringInfo!(&raw mut buf, "xmin:{}\n", (*snapshot).xmin);
    appendStringInfo!(&raw mut buf, "xmax:{}\n", (*snapshot).xmax);

    /*
     * We must include our own top transaction ID in the top-xid data, since
     * by definition we will still be running when the importing transaction
     * adopts the snapshot, but GetSnapshotData never includes our own XID in
     * the snapshot.  (There must, therefore, be enough room to add it.)
     *
     * However, it could be that our topXid is after the xmax, in which case
     * we shouldn't include it because xip[] members are expected to be before
     * xmax.  (We need not make the same check for subxip[] members, see
     * snapshot.h.)
     */
    addTopXid = if TransactionIdIsValid(topXid) && TransactionIdPrecedes(topXid, (*snapshot).xmax) {
        1
    } else {
        0
    };
    appendStringInfo!(
        &raw mut buf,
        "xcnt:{}\n",
        (*snapshot).xcnt as c_int + addTopXid
    );
    i = 0;
    while i < (*snapshot).xcnt as c_int {
        appendStringInfo!(&raw mut buf, "xip:{}\n", *(*snapshot).xip.add(i as usize));
        i += 1;
    }
    if addTopXid != 0 {
        appendStringInfo!(&raw mut buf, "xip:{}\n", topXid);
    }

    /*
     * Similarly, we add our subcommitted child XIDs to the subxid data. Here,
     * we have to cope with possible overflow.
     */
    if (*snapshot).suboverflowed
        || (*snapshot).subxcnt + nchildren > GetMaxSnapshotSubxidCount()
    {
        appendStringInfoString(&raw mut buf, c"sof:1\n".as_ptr());
    } else {
        appendStringInfoString(&raw mut buf, c"sof:0\n".as_ptr());
        appendStringInfo!(
            &raw mut buf,
            "sxcnt:{}\n",
            (*snapshot).subxcnt + nchildren
        );
        i = 0;
        while i < (*snapshot).subxcnt {
            appendStringInfo!(&raw mut buf, "sxp:{}\n", *(*snapshot).subxip.add(i as usize));
            i += 1;
        }
        i = 0;
        while i < nchildren {
            appendStringInfo!(&raw mut buf, "sxp:{}\n", *children.add(i as usize));
            i += 1;
        }
    }
    appendStringInfo!(
        &raw mut buf,
        "rec:{}\n",
        (*snapshot).takenDuringRecovery as c_uint
    );

    /*
     * Now write the text representation into a file.  We first write to a
     * ".tmp" filename, and rename to final filename if no error.  This
     * ensures that no other backend can read an incomplete file
     * (ImportSnapshot won't allow it because of its valid-characters check).
     */
    snprintf_path(
        &mut pathtmp,
        &format!("{}.tmp", cstr_to_string(path.as_ptr())),
    );
    f = AllocateFile(pathtmp.as_ptr(), PG_BINARY_W.as_ptr());
    if f.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "could not create file \"{}\": {}",
                cstr_to_string(pathtmp.as_ptr()),
                pg_strerror_errno()
            )
        );
        // C also: errcode_for_file_access()
        let _ = errcode_for_file_access();
    }

    if libc_fwrite(buf.data as *const c_void, buf.len as usize, 1, f) != 1 {
        ereport!(
            ERROR,
            errmsg!(
                "could not write to file \"{}\": {}",
                cstr_to_string(pathtmp.as_ptr()),
                pg_strerror_errno()
            )
        );
        // C also: errcode_for_file_access()
    }

    /* no fsync() since file need not survive a system crash */

    if FreeFile(f) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not write to file \"{}\": {}",
                cstr_to_string(pathtmp.as_ptr()),
                pg_strerror_errno()
            )
        );
        // C also: errcode_for_file_access()
    }

    /*
     * Now that we have written everything into a .tmp file, rename the file
     * to remove the .tmp suffix.
     */
    if libc_rename(pathtmp.as_ptr(), path.as_ptr()) < 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not rename file \"{}\" to \"{}\": {}",
                cstr_to_string(pathtmp.as_ptr()),
                cstr_to_string(path.as_ptr()),
                pg_strerror_errno()
            )
        );
        // C also: errcode_for_file_access()
    }

    /*
     * The basename of the file is what we return from pg_export_snapshot().
     * It's already in path in a textual format and we know that the path
     * starts with SNAPSHOT_EXPORT_DIR.  Skip over the prefix and the slash
     * and pstrdup it so as not to return the address of a local variable.
     */
    pstrdup(path.as_ptr().add(SNAPSHOT_EXPORT_DIR.len() + 1))
}

/*
 * pg_export_snapshot
 *		SQL-callable wrapper for ExportSnapshot.
 */
pub unsafe fn pg_export_snapshot(_fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    let snapshotName: *mut c_char;

    snapshotName = ExportSnapshot(GetActiveSnapshot());
    crate::PG_RETURN_TEXT_P!(cstring_to_text(snapshotName))
}

/*
 * Parsing subroutines for ImportSnapshot: parse a line with the given
 * prefix followed by a value, and advance *s to the next line.  The
 * filename is provided for use in error messages.
 */
unsafe fn parseIntFromText(
    prefix: *const c_char,
    s: *mut *mut c_char,
    filename: *const c_char,
) -> c_int {
    let mut ptr: *mut c_char = *s;
    let prefixlen: c_int = libc_strlen(prefix) as c_int;
    let mut val: c_int = 0;

    if libc_strncmp(ptr, prefix, prefixlen as usize) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    ptr = ptr.add(prefixlen as usize);
    if libc_sscanf_int(ptr, c"%d".as_ptr(), &raw mut val) != 1 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    ptr = libc_strchr(ptr, '\n' as c_int);
    if ptr.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    *s = ptr.add(1);
    val
}

unsafe fn parseXidFromText(
    prefix: *const c_char,
    s: *mut *mut c_char,
    filename: *const c_char,
) -> TransactionId {
    let mut ptr: *mut c_char = *s;
    let prefixlen: c_int = libc_strlen(prefix) as c_int;
    let mut val: TransactionId = 0;

    if libc_strncmp(ptr, prefix, prefixlen as usize) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    ptr = ptr.add(prefixlen as usize);
    if libc_sscanf_uint(ptr, c"%u".as_ptr(), &raw mut val) != 1 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    ptr = libc_strchr(ptr, '\n' as c_int);
    if ptr.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    *s = ptr.add(1);
    val
}

unsafe fn parseVxidFromText(
    prefix: *const c_char,
    s: *mut *mut c_char,
    filename: *const c_char,
    vxid: *mut VirtualTransactionId,
) {
    let mut ptr: *mut c_char = *s;
    let prefixlen: c_int = libc_strlen(prefix) as c_int;

    if libc_strncmp(ptr, prefix, prefixlen as usize) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    ptr = ptr.add(prefixlen as usize);
    if libc_sscanf_vxid(
        ptr,
        c"%d/%u".as_ptr(),
        &raw mut (*vxid).procNumber,
        &raw mut (*vxid).localTransactionId,
    ) != 2
    {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    ptr = libc_strchr(ptr, '\n' as c_int);
    if ptr.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(filename)
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }
    *s = ptr.add(1);
}

/*
 * ImportSnapshot
 *		Import a previously exported snapshot.  The argument should be a
 *		filename in SNAPSHOT_EXPORT_DIR.  Load the snapshot from that file.
 *		This is called by "SET TRANSACTION SNAPSHOT 'foo'".
 */
pub unsafe fn ImportSnapshot(idstr: *const c_char) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let f: *mut c_void; /* FILE* */
    let mut stat_buf: stat_t = core::mem::zeroed();
    let filebuf: *mut c_char;
    let mut xcnt: c_int;
    let mut i: c_int;
    let mut src_vxid: VirtualTransactionId = core::mem::zeroed();
    let src_pid: c_int;
    let src_dbid: Oid;
    let src_isolevel: c_int;
    let src_readonly: bool;
    let mut snapshot: SnapshotData = core::mem::zeroed();

    /*
     * Must be at top level of a fresh transaction.  Note in particular that
     * we check we haven't acquired an XID --- if we have, it's conceivable
     * that the snapshot would show it as not running, making for very screwy
     * behavior.
     */
    if FirstSnapshotSet
        || GetTopTransactionIdIfAny() != InvalidTransactionId
        || IsSubTransaction()
    {
        ereport!(
            ERROR,
            errmsg!("SET TRANSACTION SNAPSHOT must be called before any query")
        );
        // C also: errcode(ERRCODE_ACTIVE_SQL_TRANSACTION)
    }

    /*
     * If we are in read committed mode then the next query would execute with
     * a new snapshot thus making this function call quite useless.
     */
    if !IsolationUsesXactSnapshot() {
        ereport!(
            ERROR,
            errmsg!("a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ")
        );
        // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
    }

    /*
     * Verify the identifier: only 0-9, A-F and hyphens are allowed.  We do
     * this mainly to prevent reading arbitrary files.
     */
    if libc_strspn(idstr, c"0123456789ABCDEF-".as_ptr()) != libc_strlen(idstr) {
        ereport!(
            ERROR,
            errmsg!("invalid snapshot identifier: \"{}\"", cstr_to_string(idstr))
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
    }

    /* OK, read the file */
    snprintf_path(
        &mut path,
        &format!("{}/{}", SNAPSHOT_EXPORT_DIR, cstr_to_string(idstr)),
    );

    f = AllocateFile(path.as_ptr(), PG_BINARY_R.as_ptr());
    if f.is_null() {
        /*
         * If file is missing while identifier has a correct format, avoid
         * system errors.
         */
        if errno() == ENOENT {
            ereport!(
                ERROR,
                errmsg!("snapshot \"{}\" does not exist", cstr_to_string(idstr))
            );
            // C also: errcode(ERRCODE_UNDEFINED_OBJECT)
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "could not open file \"{}\" for reading: {}",
                    cstr_to_string(path.as_ptr()),
                    pg_strerror_errno()
                )
            );
            // C also: errcode_for_file_access()
        }
    }

    /* get the size of the file so that we know how much memory we need */
    if libc_fstat(libc_fileno(f), &raw mut stat_buf) != 0 {
        elog!(
            ERROR,
            "could not stat file \"{}\": {}",
            cstr_to_string(path.as_ptr()),
            pg_strerror_errno()
        );
    }

    /* and read the file into a palloc'd string */
    filebuf = palloc(stat_buf.st_size as usize + 1) as *mut c_char;
    if libc_fread(filebuf as *mut c_void, stat_buf.st_size as usize, 1, f) != 1 {
        elog!(
            ERROR,
            "could not read file \"{}\": {}",
            cstr_to_string(path.as_ptr()),
            pg_strerror_errno()
        );
    }

    *filebuf.add(stat_buf.st_size as usize) = 0;

    FreeFile(f);

    /*
     * Construct a snapshot struct by parsing the file content.
     */
    let mut filebufp: *mut c_char = filebuf;

    parseVxidFromText(c"vxid:".as_ptr(), &raw mut filebufp, path.as_ptr(), &raw mut src_vxid);
    src_pid = parseIntFromText(c"pid:".as_ptr(), &raw mut filebufp, path.as_ptr());
    /* we abuse parseXidFromText a bit here ... */
    src_dbid = parseXidFromText(c"dbid:".as_ptr(), &raw mut filebufp, path.as_ptr());
    src_isolevel = parseIntFromText(c"iso:".as_ptr(), &raw mut filebufp, path.as_ptr());
    src_readonly = parseIntFromText(c"ro:".as_ptr(), &raw mut filebufp, path.as_ptr()) != 0;

    snapshot.snapshot_type = SNAPSHOT_MVCC;

    snapshot.xmin = parseXidFromText(c"xmin:".as_ptr(), &raw mut filebufp, path.as_ptr());
    snapshot.xmax = parseXidFromText(c"xmax:".as_ptr(), &raw mut filebufp, path.as_ptr());

    xcnt = parseIntFromText(c"xcnt:".as_ptr(), &raw mut filebufp, path.as_ptr());
    snapshot.xcnt = xcnt as uint32;

    /* sanity-check the xid count before palloc */
    if xcnt < 0 || xcnt > GetMaxSnapshotXidCount() {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(path.as_ptr())
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }

    snapshot.xip =
        palloc(xcnt as usize * core::mem::size_of::<TransactionId>()) as *mut TransactionId;
    i = 0;
    while i < xcnt {
        *snapshot.xip.add(i as usize) =
            parseXidFromText(c"xip:".as_ptr(), &raw mut filebufp, path.as_ptr());
        i += 1;
    }

    snapshot.suboverflowed =
        parseIntFromText(c"sof:".as_ptr(), &raw mut filebufp, path.as_ptr()) != 0;

    if !snapshot.suboverflowed {
        xcnt = parseIntFromText(c"sxcnt:".as_ptr(), &raw mut filebufp, path.as_ptr());
        snapshot.subxcnt = xcnt;

        /* sanity-check the xid count before palloc */
        if xcnt < 0 || xcnt > GetMaxSnapshotSubxidCount() {
            ereport!(
                ERROR,
                errmsg!(
                    "invalid snapshot data in file \"{}\"",
                    cstr_to_string(path.as_ptr())
                )
            );
            // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
        }

        snapshot.subxip =
            palloc(xcnt as usize * core::mem::size_of::<TransactionId>()) as *mut TransactionId;
        i = 0;
        while i < xcnt {
            *snapshot.subxip.add(i as usize) =
                parseXidFromText(c"sxp:".as_ptr(), &raw mut filebufp, path.as_ptr());
            i += 1;
        }
    } else {
        snapshot.subxcnt = 0;
        snapshot.subxip = null_mut();
    }

    snapshot.takenDuringRecovery =
        parseIntFromText(c"rec:".as_ptr(), &raw mut filebufp, path.as_ptr()) != 0;

    /*
     * Do some additional sanity checking, just to protect ourselves.  We
     * don't trouble to check the array elements, just the most critical
     * fields.
     */
    if !VirtualTransactionIdIsValid(src_vxid)
        || !OidIsValid(src_dbid)
        || !TransactionIdIsNormal(snapshot.xmin)
        || !TransactionIdIsNormal(snapshot.xmax)
    {
        ereport!(
            ERROR,
            errmsg!(
                "invalid snapshot data in file \"{}\"",
                cstr_to_string(path.as_ptr())
            )
        );
        // C also: errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
    }

    /*
     * If we're serializable, the source transaction must be too, otherwise
     * predicate.c has problems (SxactGlobalXmin could go backwards).  Also, a
     * non-read-only transaction can't adopt a snapshot from a read-only
     * transaction, as predicate.c handles the cases very differently.
     */
    if IsolationIsSerializable() {
        if src_isolevel != XACT_SERIALIZABLE {
            ereport!(
                ERROR,
                errmsg!("a serializable transaction cannot import a snapshot from a non-serializable transaction")
            );
            // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        }
        if src_readonly && !XactReadOnly {
            ereport!(
                ERROR,
                errmsg!("a non-read-only serializable transaction cannot import a snapshot from a read-only transaction")
            );
            // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        }
    }

    /*
     * We cannot import a snapshot that was taken in a different database,
     * because vacuum calculates OldestXmin on a per-database basis; so the
     * source transaction's xmin doesn't protect us from data loss.  This
     * restriction could be removed if the source transaction were to mark its
     * xmin as being globally applicable.  But that would require some
     * additional syntax, since that has to be known when the snapshot is
     * initially taken.  (See pgsql-hackers discussion of 2011-10-21.)
     */
    if src_dbid != MyDatabaseId {
        ereport!(
            ERROR,
            errmsg!("cannot import a snapshot from a different database")
        );
        // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
    }

    /* OK, install the snapshot */
    SetTransactionSnapshot(&raw mut snapshot, &raw mut src_vxid, src_pid, null_mut());
}

/*
 * XactHasExportedSnapshots
 *		Test whether current transaction has exported any snapshots.
 */
pub unsafe fn XactHasExportedSnapshots() -> bool {
    exportedSnapshots != NIL
}

/*
 * DeleteAllExportedSnapshotFiles
 *		Clean up any files that have been left behind by a crashed backend
 *		that had exported snapshots before it died.
 *
 * This should be called during database startup or crash recovery.
 */
pub unsafe fn DeleteAllExportedSnapshotFiles() {
    let mut buf: [c_char; MAXPGPATH + SNAPSHOT_EXPORT_DIR_SIZE] =
        [0; MAXPGPATH + SNAPSHOT_EXPORT_DIR_SIZE];
    let s_dir: *mut DIR;
    let mut s_de: *mut dirent;

    /*
     * Problems in reading the directory, or unlinking files, are reported at
     * LOG level.  Since we're running in the startup process, ERROR level
     * would prevent database start, and it's not important enough for that.
     */
    s_dir = AllocateDir(c"pg_snapshots".as_ptr());

    loop {
        s_de = ReadDirExtended(s_dir, c"pg_snapshots".as_ptr(), LOG);
        if s_de.is_null() {
            break;
        }

        if libc_strcmp((*s_de).d_name.as_ptr(), c".".as_ptr()) == 0
            || libc_strcmp((*s_de).d_name.as_ptr(), c"..".as_ptr()) == 0
        {
            continue;
        }

        snprintf_path_big(
            &mut buf,
            &format!(
                "{}/{}",
                SNAPSHOT_EXPORT_DIR,
                cstr_to_string((*s_de).d_name.as_ptr())
            ),
        );

        if unlink(buf.as_ptr()) != 0 {
            ereport!(
                LOG,
                errmsg!(
                    "could not remove file \"{}\": {}",
                    cstr_to_string(buf.as_ptr()),
                    pg_strerror_errno()
                )
            );
            // C also: errcode_for_file_access()
        }
    }

    FreeDir(s_dir);
}

/*
 * ThereAreNoPriorRegisteredSnapshots
 *		Is the registered snapshot count less than or equal to one?
 *
 * Don't use this to settle important decisions.  While zero registrations and
 * no ActiveSnapshot would confirm a certain idleness, the system makes no
 * guarantees about the significance of one registered snapshot.
 */
pub unsafe fn ThereAreNoPriorRegisteredSnapshots() -> bool {
    if pairingheap_is_empty(&raw mut RegisteredSnapshots)
        || pairingheap_is_singular(&raw mut RegisteredSnapshots)
    {
        return true;
    }

    false
}

/*
 * HaveRegisteredOrActiveSnapshot
 *		Is there any registered or active snapshot?
 *
 * NB: Unless pushed or active, the cached catalog snapshot will not cause
 * this function to return true. That allows this function to be used in
 * checks enforcing a longer-lived snapshot.
 */
pub unsafe fn HaveRegisteredOrActiveSnapshot() -> bool {
    if !ActiveSnapshot.is_null() {
        return true;
    }

    /*
     * The catalog snapshot is in RegisteredSnapshots when valid, but can be
     * removed at any time due to invalidation processing. If explicitly
     * registered more than one snapshot has to be in RegisteredSnapshots.
     */
    if !CatalogSnapshot.is_null() && pairingheap_is_singular(&raw mut RegisteredSnapshots) {
        return false;
    }

    !pairingheap_is_empty(&raw mut RegisteredSnapshots)
}

/*
 * Setup a snapshot that replaces normal catalog snapshots that allows catalog
 * access to behave just like it did at a certain point in the past.
 *
 * Needed for logical decoding.
 */
pub unsafe fn SetupHistoricSnapshot(historic_snapshot: Snapshot, tuplecids: *mut HTAB) {
    Assert!(!historic_snapshot.is_null());

    /* setup the timetravel snapshot */
    HistoricSnapshot = historic_snapshot;

    /* setup (cmin, cmax) lookup hash */
    tuplecid_data = tuplecids;
}

/*
 * Make catalog snapshots behave normally again.
 */
pub unsafe fn TeardownHistoricSnapshot(_is_error: bool) {
    HistoricSnapshot = null_mut();
    tuplecid_data = null_mut();
}

pub unsafe fn HistoricSnapshotActive() -> bool {
    !HistoricSnapshot.is_null()
}

pub unsafe fn HistoricSnapshotGetTupleCids() -> *mut HTAB {
    Assert!(HistoricSnapshotActive());
    tuplecid_data
}

/*
 * EstimateSnapshotSpace
 *		Returns the size needed to store the given snapshot.
 *
 * We are exporting only required fields from the Snapshot, stored in
 * SerializedSnapshotData.
 */
pub unsafe fn EstimateSnapshotSpace(snapshot: Snapshot) -> Size {
    let mut size: Size;

    Assert!(snapshot != InvalidSnapshot);
    Assert!((*snapshot).snapshot_type == SNAPSHOT_MVCC);

    /* We allocate any XID arrays needed in the same palloc block. */
    size = add_size(
        core::mem::size_of::<SerializedSnapshotData>(),
        mul_size(
            (*snapshot).xcnt as Size,
            core::mem::size_of::<TransactionId>(),
        ),
    );
    if (*snapshot).subxcnt > 0 && (!(*snapshot).suboverflowed || (*snapshot).takenDuringRecovery) {
        size = add_size(
            size,
            mul_size(
                (*snapshot).subxcnt as Size,
                core::mem::size_of::<TransactionId>(),
            ),
        );
    }

    size
}

/*
 * SerializeSnapshot
 *		Dumps the serialized snapshot (extracted from given snapshot) onto the
 *		memory location at start_address.
 */
pub unsafe fn SerializeSnapshot(snapshot: Snapshot, start_address: *mut c_char) {
    let mut serialized_snapshot: SerializedSnapshotData = core::mem::zeroed();

    Assert!((*snapshot).subxcnt >= 0);

    /* Copy all required fields */
    serialized_snapshot.xmin = (*snapshot).xmin;
    serialized_snapshot.xmax = (*snapshot).xmax;
    serialized_snapshot.xcnt = (*snapshot).xcnt;
    serialized_snapshot.subxcnt = (*snapshot).subxcnt;
    serialized_snapshot.suboverflowed = (*snapshot).suboverflowed;
    serialized_snapshot.takenDuringRecovery = (*snapshot).takenDuringRecovery;
    serialized_snapshot.curcid = (*snapshot).curcid;

    /*
     * Ignore the SubXID array if it has overflowed, unless the snapshot was
     * taken during recovery - in that case, top-level XIDs are in subxip as
     * well, and we mustn't lose them.
     */
    if serialized_snapshot.suboverflowed && !(*snapshot).takenDuringRecovery {
        serialized_snapshot.subxcnt = 0;
    }

    /* Copy struct to possibly-unaligned buffer */
    core::ptr::copy_nonoverlapping(
        &raw const serialized_snapshot as *const c_char,
        start_address,
        core::mem::size_of::<SerializedSnapshotData>(),
    );

    /* Copy XID array */
    if (*snapshot).xcnt > 0 {
        core::ptr::copy_nonoverlapping(
            (*snapshot).xip,
            start_address.add(core::mem::size_of::<SerializedSnapshotData>())
                as *mut TransactionId,
            (*snapshot).xcnt as usize,
        );
    }

    /*
     * Copy SubXID array. Don't bother to copy it if it had overflowed,
     * though, because it's not used anywhere in that case. Except if it's a
     * snapshot taken during recovery; all the top-level XIDs are in subxip as
     * well in that case, so we mustn't lose them.
     */
    if serialized_snapshot.subxcnt > 0 {
        let subxipoff: Size = core::mem::size_of::<SerializedSnapshotData>()
            + (*snapshot).xcnt as usize * core::mem::size_of::<TransactionId>();

        core::ptr::copy_nonoverlapping(
            (*snapshot).subxip,
            start_address.add(subxipoff) as *mut TransactionId,
            (*snapshot).subxcnt as usize,
        );
    }
}

/*
 * RestoreSnapshot
 *		Restore a serialized snapshot from the specified address.
 *
 * The copy is palloc'd in TopTransactionContext and has initial refcounts set
 * to 0.  The returned snapshot has the copied flag set.
 */
pub unsafe fn RestoreSnapshot(start_address: *mut c_char) -> Snapshot {
    let mut serialized_snapshot: SerializedSnapshotData = core::mem::zeroed();
    let size: Size;
    let snapshot: Snapshot;
    let serialized_xids: *mut TransactionId;

    core::ptr::copy_nonoverlapping(
        start_address as *const c_char,
        &raw mut serialized_snapshot as *mut c_char,
        core::mem::size_of::<SerializedSnapshotData>(),
    );
    serialized_xids =
        start_address.add(core::mem::size_of::<SerializedSnapshotData>()) as *mut TransactionId;

    /* We allocate any XID arrays needed in the same palloc block. */
    size = core::mem::size_of::<SnapshotData>()
        + serialized_snapshot.xcnt as usize * core::mem::size_of::<TransactionId>()
        + serialized_snapshot.subxcnt as usize * core::mem::size_of::<TransactionId>();

    /* Copy all required fields */
    snapshot = MemoryContextAlloc(TopTransactionContext, size) as Snapshot;
    (*snapshot).snapshot_type = SNAPSHOT_MVCC;
    (*snapshot).xmin = serialized_snapshot.xmin;
    (*snapshot).xmax = serialized_snapshot.xmax;
    (*snapshot).xip = null_mut();
    (*snapshot).xcnt = serialized_snapshot.xcnt;
    (*snapshot).subxip = null_mut();
    (*snapshot).subxcnt = serialized_snapshot.subxcnt;
    (*snapshot).suboverflowed = serialized_snapshot.suboverflowed;
    (*snapshot).takenDuringRecovery = serialized_snapshot.takenDuringRecovery;
    (*snapshot).curcid = serialized_snapshot.curcid;
    (*snapshot).snapXactCompletionCount = 0;

    /* Copy XIDs, if present. */
    if serialized_snapshot.xcnt > 0 {
        (*snapshot).xip = snapshot.add(1) as *mut TransactionId;
        core::ptr::copy_nonoverlapping(
            serialized_xids,
            (*snapshot).xip,
            serialized_snapshot.xcnt as usize,
        );
    }

    /* Copy SubXIDs, if present. */
    if serialized_snapshot.subxcnt > 0 {
        (*snapshot).subxip =
            (snapshot.add(1) as *mut TransactionId).add(serialized_snapshot.xcnt as usize);
        core::ptr::copy_nonoverlapping(
            serialized_xids.add(serialized_snapshot.xcnt as usize),
            (*snapshot).subxip,
            serialized_snapshot.subxcnt as usize,
        );
    }

    /* Set the copied flag so that the caller will set refcounts correctly. */
    (*snapshot).regd_count = 0;
    (*snapshot).active_count = 0;
    (*snapshot).copied = true;

    snapshot
}

/*
 * Install a restored snapshot as the transaction snapshot.
 *
 * The second argument is of type void * so that snapmgr.h need not include
 * the declaration for PGPROC.
 */
pub unsafe fn RestoreTransactionSnapshot(snapshot: Snapshot, source_pgproc: *mut c_void) {
    SetTransactionSnapshot(snapshot, null_mut(), InvalidPid, source_pgproc as *mut PGPROC);
}

/*
 * XidInMVCCSnapshot
 *		Is the given XID still-in-progress according to the snapshot?
 *
 * Note: GetSnapshotData never stores either top xid or subxids of our own
 * backend into a snapshot, so these xids will not be reported as "running"
 * by this function.  This is OK for current uses, because we always check
 * TransactionIdIsCurrentTransactionId first, except when it's known the
 * XID could not be ours anyway.
 */
pub unsafe fn XidInMVCCSnapshot(mut xid: TransactionId, snapshot: Snapshot) -> bool {
    /*
     * Make a quick range check to eliminate most XIDs without looking at the
     * xip arrays.  Note that this is OK even if we convert a subxact XID to
     * its parent below, because a subxact with XID < xmin has surely also got
     * a parent with XID < xmin, while one with XID >= xmax must belong to a
     * parent that was not yet committed at the time of this snapshot.
     */

    /* Any xid < xmin is not in-progress */
    if TransactionIdPrecedes(xid, (*snapshot).xmin) {
        return false;
    }
    /* Any xid >= xmax is in-progress */
    if TransactionIdFollowsOrEquals(xid, (*snapshot).xmax) {
        return true;
    }

    /*
     * Snapshot information is stored slightly differently in snapshots taken
     * during recovery.
     */
    if !(*snapshot).takenDuringRecovery {
        /*
         * If the snapshot contains full subxact data, the fastest way to
         * check things is just to compare the given XID against both subxact
         * XIDs and top-level XIDs.  If the snapshot overflowed, we have to
         * use pg_subtrans to convert a subxact XID to its parent XID, but
         * then we need only look at top-level XIDs not subxacts.
         */
        if !(*snapshot).suboverflowed {
            /* we have full data, so search subxip */
            if pg_lfind32(xid, (*snapshot).subxip, (*snapshot).subxcnt as uint32) {
                return true;
            }

            /* not there, fall through to search xip[] */
        } else {
            /*
             * Snapshot overflowed, so convert xid to top-level.  This is safe
             * because we eliminated too-old XIDs above.
             */
            xid = SubTransGetTopmostTransaction(xid);

            /*
             * If xid was indeed a subxact, we might now have an xid < xmin,
             * so recheck to avoid an array scan.  No point in rechecking
             * xmax.
             */
            if TransactionIdPrecedes(xid, (*snapshot).xmin) {
                return false;
            }
        }

        if pg_lfind32(xid, (*snapshot).xip, (*snapshot).xcnt) {
            return true;
        }
    } else {
        /*
         * In recovery we store all xids in the subxip array because it is by
         * far the bigger array, and we mostly don't know which xids are
         * top-level and which are subxacts. The xip array is empty.
         *
         * We start by searching subtrans, if we overflowed.
         */
        if (*snapshot).suboverflowed {
            /*
             * Snapshot overflowed, so convert xid to top-level.  This is safe
             * because we eliminated too-old XIDs above.
             */
            xid = SubTransGetTopmostTransaction(xid);

            /*
             * If xid was indeed a subxact, we might now have an xid < xmin,
             * so recheck to avoid an array scan.  No point in rechecking
             * xmax.
             */
            if TransactionIdPrecedes(xid, (*snapshot).xmin) {
                return false;
            }
        }

        /*
         * We now have either a top-level xid higher than xmin or an
         * indeterminate xid. We don't know whether it's top level or subxact
         * but it doesn't matter. If it's present, the xid is visible.
         */
        if pg_lfind32(xid, (*snapshot).subxip, (*snapshot).subxcnt as uint32) {
            return true;
        }
    }

    false
}

/* ResourceOwner callbacks */

unsafe fn ResOwnerReleaseSnapshot(res: Datum) {
    UnregisterSnapshotNoOwner(DatumGetPointer(res) as Snapshot);
}

/* ---- local libc / formatting helpers (mirror the C stdio/string calls) ---- */

const SNAPSHOT_EXPORT_DIR_SIZE: usize = 13; /* sizeof("pg_snapshots") */

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strerror(errnum: c_int) -> *mut c_char;
    fn unlink(path: *const c_char) -> c_int;
    fn rename(oldpath: *const c_char, newpath: *const c_char) -> c_int;
    fn fwrite(ptr: *const c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize;
    fn fread(ptr: *mut c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize;
    fn fileno(stream: *mut c_void) -> c_int;
    #[link_name = "fstat$INODE64"]
    fn fstat(fd: c_int, buf: *mut stat_t) -> c_int;
    fn sscanf(s: *const c_char, fmt: *const c_char, ...) -> c_int;
    /// errno access (thread-local). macOS/Darwin uses __error().
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}

#[inline]
unsafe fn libc_strlen(s: *const c_char) -> usize {
    strlen(s)
}
#[inline]
unsafe fn libc_strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int {
    strncmp(s1, s2, n)
}
#[inline]
unsafe fn libc_strcmp(s1: *const c_char, s2: *const c_char) -> c_int {
    strcmp(s1, s2)
}
#[inline]
unsafe fn libc_strspn(s: *const c_char, accept: *const c_char) -> usize {
    strspn(s, accept)
}
#[inline]
unsafe fn libc_strchr(s: *const c_char, c: c_int) -> *mut c_char {
    strchr(s, c)
}
#[inline]
unsafe fn libc_rename(oldpath: *const c_char, newpath: *const c_char) -> c_int {
    rename(oldpath, newpath)
}
#[inline]
unsafe fn libc_fwrite(ptr: *const c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize {
    fwrite(ptr, size, nmemb, stream)
}
#[inline]
unsafe fn libc_fread(ptr: *mut c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize {
    fread(ptr, size, nmemb, stream)
}
#[inline]
unsafe fn libc_fileno(stream: *mut c_void) -> c_int {
    fileno(stream)
}
#[inline]
unsafe fn libc_fstat(fd: c_int, buf: *mut stat_t) -> c_int {
    fstat(fd, buf)
}
#[inline]
unsafe fn libc_sscanf_int(s: *const c_char, fmt: *const c_char, out: *mut c_int) -> c_int {
    sscanf(s, fmt, out)
}
#[inline]
unsafe fn libc_sscanf_uint(s: *const c_char, fmt: *const c_char, out: *mut uint32) -> c_int {
    sscanf(s, fmt, out)
}
#[inline]
unsafe fn libc_sscanf_vxid(
    s: *const c_char,
    fmt: *const c_char,
    out1: *mut ProcNumber,
    out2: *mut LocalTransactionId,
) -> c_int {
    sscanf(s, fmt, out1, out2)
}

/// errno() rendered as a string, mirroring the C "%m" conversion.
unsafe fn pg_strerror_errno() -> String {
    CStr::from_ptr(strerror(errno())).to_string_lossy().into_owned()
}

/// Render a NUL-terminated C string pointer for use with Rust formatting.
unsafe fn cstr_to_string(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    CStr::from_ptr(p).to_string_lossy().into_owned()
}

/// snprintf into a fixed C buffer using a Rust-formatted string. Truncates to
/// fit and always NUL-terminates, mirroring the bounded snprintf calls.
unsafe fn snprintf_into(buf: *mut c_char, cap: usize, s: &str) {
    if cap == 0 {
        return;
    }
    let bytes = s.as_bytes();
    let n = core::cmp::min(bytes.len(), cap - 1);
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, n);
    *buf.add(n) = 0;
}

#[inline]
unsafe fn snprintf_path(buf: &mut [c_char; MAXPGPATH], s: &str) {
    snprintf_into(buf.as_mut_ptr(), MAXPGPATH, s);
}

#[inline]
unsafe fn snprintf_path_big(buf: &mut [c_char; MAXPGPATH + SNAPSHOT_EXPORT_DIR_SIZE], s: &str) {
    snprintf_into(buf.as_mut_ptr(), MAXPGPATH + SNAPSHOT_EXPORT_DIR_SIZE, s);
}
