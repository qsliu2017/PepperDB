//! src/backend/access/index/indexam.c
//!
//! general index access method routines
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! INTERFACE ROUTINES
//!		index_open		- open an index relation by relation OID
//!		index_close		- close an index relation
//!		index_beginscan - start a scan of an index with amgettuple
//!		index_beginscan_bitmap - start a scan of an index with amgetbitmap
//!		index_rescan	- restart a scan of an index
//!		index_endscan	- end a scan
//!		index_insert	- insert an index tuple into a relation
//!		index_markpos	- mark a scan position
//!		index_restrpos	- restore a scan position
//!		index_parallelscan_estimate - estimate shared memory for parallel scan
//!		index_parallelscan_initialize - initialize parallel scan
//!		index_parallelrescan  - (re)start a parallel scan of an index
//!		index_beginscan_parallel - join parallel index scan
//!		index_getnext_tid	- get the next TID from a scan
//!		index_fetch_heap		- get the scan's next heap tuple
//!		index_getnext_slot	- get the next tuple from a scan
//!		index_getbitmap - get all tuples from a scan
//!		index_bulk_delete	- bulk deletion of index tuples
//!		index_vacuum_cleanup	- post-deletion cleanup of an index
//!		index_can_return	- does index support index-only scans?
//!		index_getprocid - get a support procedure OID
//!		index_getprocinfo - get a support procedure's lookup info
//!
//! NOTES
//!		This file contains the index_ routines which used
//!		to be a scattered collection of stuff in access/genam.

use crate::prelude::*;
use crate::pg_config::USE_FLOAT8_BYVAL;

use std::ffi::{c_int, c_void};
use std::ptr;

use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;
use crate::c::{int64, uint16, Size};
use crate::storage::block::BlockNumber;

// ----------------------------------------------------------------
// Type aliases / opaque stub types pulled in from headers
// ----------------------------------------------------------------

pub type LOCKMODE = c_int;
// RegProcedure comes from the prelude (crate::c::RegProcedure).

// access/relscan.h, nodes/execnodes.h, etc. -- treated as opaque pointers here
use crate::utils::rel::{Relation, RelationData};
use crate::access::relscan::{IndexScanDesc, IndexScanDescData, ParallelIndexScanDescData};
use crate::access::relscan::{IndexScanInstrumentation, SnapshotData};
use crate::access::htup_details::HeapTupleData;
pub type ParallelIndexScanDesc = *mut ParallelIndexScanDescData;
pub type Snapshot = *mut SnapshotData;
pub type ScanKey = *mut ScanKeyData;
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
pub type TupleTableSlot = crate::nodes::execnodes::TupleTableSlot;
pub type FmgrInfo = crate::utils::fmgr::FmgrInfo;
pub type IndexInfo = crate::nodes::execnodes::IndexInfo;
pub type TIDBitmap = TIDBitmapStub;
pub type IndexBulkDeleteResult = IndexBulkDeleteResultStub;
pub use crate::access::index::genam::IndexVacuumInfo;
pub type ScanDirection = c_int;

#[repr(C)]
pub struct ScanKeyData {
    _private: [u8; 0],
}
#[repr(C)]
pub struct TIDBitmapStub {
    _private: [u8; 0],
}
#[repr(C)]
pub struct IndexBulkDeleteResultStub {
    _private: [u8; 0],
}
#[repr(C)]
pub struct IndexVacuumInfoStub {
    _private: [u8; 0],
}

pub type IndexUniqueCheck = c_int;

/// IndexScanInstrumentation (nodes/execnodes.h)

/// SharedIndexScanInstrumentation (nodes/execnodes.h)
#[repr(C)]
pub struct SharedIndexScanInstrumentation {
    pub num_workers: c_int,
    pub winstrument: [IndexScanInstrumentation; 0], // FLEXIBLE_ARRAY_MEMBER
}

/// IndexOrderByDistance (access/relscan.h)
#[repr(C)]
pub struct IndexOrderByDistance {
    pub value: f64,
    pub isnull: bool,
}

/// IndexBulkDeleteCallback (genam.h)
pub type IndexBulkDeleteCallback =
    Option<unsafe extern "C" fn(itemptr: ItemPointer, state: *mut c_void) -> bool>;

// ----------------------------------------------------------------
//					macros used in index_ routines
//
// Note: the ReindexIsProcessingIndex() check in RELATION_CHECKS is there
// to check that we don't try to scan or do retail insertions into an index
// that is currently being rebuilt or pending rebuild.  This helps to catch
// things that don't work when reindexing system catalogs, as well as prevent
// user errors like index expressions that access their own tables.  The check
// doesn't prevent the actual rebuild because we don't use RELATION_CHECKS
// when calling the index AM's ambuild routine, and there is no reason for
// ambuild to call its subsidiary routines through this file.
// ----------------------------------------------------------------

// #define RELATION_CHECKS
macro_rules! RELATION_CHECKS {
    ($indexRelation:expr) => {{
        Assert!(RelationIsValid($indexRelation));
        Assert!(PointerIsValid((*$indexRelation).rd_indam));
        if unlikely(ReindexIsProcessingIndex(RelationGetRelid($indexRelation))) {
            ereport!(
                ERROR,
                "cannot access index while it is being reindexed"
            );
        }
    }};
}

// #define SCAN_CHECKS
macro_rules! SCAN_CHECKS {
    ($scan:expr) => {{
        AssertMacro!(IndexScanIsValid($scan));
        AssertMacro!(RelationIsValid((*$scan).indexRelation));
        AssertMacro!(PointerIsValid((*(*$scan).indexRelation).rd_indam));
    }};
}

// #define CHECK_REL_PROCEDURE(pname)
macro_rules! CHECK_REL_PROCEDURE {
    ($indexRelation:expr, $pname:ident) => {{
        if (*(*$indexRelation).rd_indam).$pname.is_none() {
            elog!(
                ERROR,
                "function \"{}\" is not defined for index",
                stringify!($pname)
            );
        }
    }};
}

// #define CHECK_SCAN_PROCEDURE(pname)
macro_rules! CHECK_SCAN_PROCEDURE {
    ($scan:expr, $pname:ident) => {{
        if (*(*(*$scan).indexRelation).rd_indam).$pname.is_none() {
            elog!(
                ERROR,
                "function \"{}\" is not defined for index",
                stringify!($pname)
            );
        }
    }};
}

// ----------------------------------------------------------------
//				   index_ interface functions
// ----------------------------------------------------------------

// ----------------
//		index_open - open an index relation by relation OID
//
//		If lockmode is not "NoLock", the specified kind of lock is
//		obtained on the index.  (Generally, NoLock should only be
//		used if the caller knows it has some appropriate lock on the
//		index already.)
//
//		An error is raised if the index does not exist.
//
//		This is a convenience routine adapted for indexscan use.
//		Some callers may prefer to use relation_open directly.
// ----------------
pub unsafe fn index_open(relationId: Oid, lockmode: LOCKMODE) -> Relation {
    let r: Relation = relation_open(relationId, lockmode);

    validate_relation_kind(r);

    r
}

// ----------------
//		try_index_open - open an index relation by relation OID
//
//		Same as index_open, except return NULL instead of failing
//		if the relation does not exist.
// ----------------
pub unsafe fn try_index_open(relationId: Oid, lockmode: LOCKMODE) -> Relation {
    let r: Relation = try_relation_open(relationId, lockmode);

    /* leave if index does not exist */
    if r.is_null() {
        return ptr::null_mut();
    }

    validate_relation_kind(r);

    r
}

// ----------------
//		index_close - close an index relation
//
//		If lockmode is not "NoLock", we then release the specified lock.
//
//		Note that it is often sensible to hold a lock beyond index_close;
//		in that case, the lock is released automatically at xact end.
// ----------------
pub unsafe fn index_close(relation: Relation, lockmode: LOCKMODE) {
    let mut relid: LockRelId = (*relation).rd_lockInfo.lockRelId;

    Assert!(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

    /* The relcache does the real work... */
    RelationClose(relation);

    if lockmode != NoLock {
        UnlockRelationId(&mut relid, lockmode);
    }
}

// ----------------
//		validate_relation_kind - check the relation's kind
//
//		Make sure relkind is an index or a partitioned index.
// ----------------
unsafe fn validate_relation_kind(r: Relation) {
    if (*(*r).rd_rel).relkind != RELKIND_INDEX
        && (*(*r).rd_rel).relkind != RELKIND_PARTITIONED_INDEX
    {
        ereport!(ERROR, "is not an index");
    }
}

// ----------------
//		index_insert - insert an index tuple into a relation
// ----------------
pub unsafe fn index_insert(
    indexRelation: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    heap_t_ctid: ItemPointer,
    heapRelation: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool {
    RELATION_CHECKS!(indexRelation);
    CHECK_REL_PROCEDURE!(indexRelation, aminsert);

    if !((*(*indexRelation).rd_indam).ampredlocks) {
        CheckForSerializableConflictIn(
            indexRelation,
            ptr::null_mut::<ItemPointerData>(),
            InvalidBlockNumber,
        );
    }

    ((*(*indexRelation).rd_indam).aminsert.unwrap())(
        indexRelation,
        values,
        isnull,
        heap_t_ctid,
        heapRelation,
        checkUnique,
        indexUnchanged,
        indexInfo as *mut core::ffi::c_void,
    )
}

// -------------------------
//		index_insert_cleanup - clean up after all index inserts are done
// -------------------------
pub unsafe fn index_insert_cleanup(indexRelation: Relation, indexInfo: *mut IndexInfo) {
    RELATION_CHECKS!(indexRelation);

    if let Some(aminsertcleanup) = (*(*indexRelation).rd_indam).aminsertcleanup {
        aminsertcleanup(indexRelation, indexInfo as *mut core::ffi::c_void);
    }
}

// index_beginscan - start a scan of an index with amgettuple
//
// Caller must be holding suitable locks on the heap and the index.
pub unsafe fn index_beginscan(
    heapRelation: Relation,
    indexRelation: Relation,
    snapshot: Snapshot,
    instrument: *mut IndexScanInstrumentation,
    nkeys: c_int,
    norderbys: c_int,
) -> IndexScanDesc {
    Assert!(snapshot != InvalidSnapshot);

    let scan: IndexScanDesc = index_beginscan_internal(
        indexRelation,
        nkeys,
        norderbys,
        snapshot,
        ptr::null_mut(),
        false,
    );

    /*
     * Save additional parameters into the scandesc.  Everything else was set
     * up by RelationGetIndexScan.
     */
    (*scan).heapRelation = heapRelation;
    (*scan).xs_snapshot = snapshot;
    (*scan).instrument = instrument;

    /* prepare to fetch index matches from table */
    (*scan).xs_heapfetch = table_index_fetch_begin(heapRelation);

    scan
}

// index_beginscan_bitmap - start a scan of an index with amgetbitmap
//
// As above, caller had better be holding some lock on the parent heap
// relation, even though it's not explicitly mentioned here.
pub unsafe fn index_beginscan_bitmap(
    indexRelation: Relation,
    snapshot: Snapshot,
    instrument: *mut IndexScanInstrumentation,
    nkeys: c_int,
) -> IndexScanDesc {
    Assert!(snapshot != InvalidSnapshot);

    let scan: IndexScanDesc =
        index_beginscan_internal(indexRelation, nkeys, 0, snapshot, ptr::null_mut(), false);

    /*
     * Save additional parameters into the scandesc.  Everything else was set
     * up by RelationGetIndexScan.
     */
    (*scan).xs_snapshot = snapshot;
    (*scan).instrument = instrument;

    scan
}

// index_beginscan_internal --- common code for index_beginscan variants
unsafe fn index_beginscan_internal(
    indexRelation: Relation,
    nkeys: c_int,
    norderbys: c_int,
    snapshot: Snapshot,
    pscan: ParallelIndexScanDesc,
    temp_snap: bool,
) -> IndexScanDesc {
    RELATION_CHECKS!(indexRelation);
    CHECK_REL_PROCEDURE!(indexRelation, ambeginscan);

    if !((*(*indexRelation).rd_indam).ampredlocks) {
        PredicateLockRelation(indexRelation, snapshot);
    }

    /*
     * We hold a reference count to the relcache entry throughout the scan.
     */
    RelationIncrementReferenceCount(indexRelation);

    /*
     * Tell the AM to open a scan.
     */
    let scan: IndexScanDesc =
        ((*(*indexRelation).rd_indam).ambeginscan.unwrap())(indexRelation, nkeys, norderbys) as IndexScanDesc;
    /* Initialize information for parallel scan. */
    (*scan).parallel_scan = pscan;
    (*scan).xs_temp_snap = temp_snap;

    scan
}

// ----------------
//		index_rescan  - (re)start a scan of an index
//
// During a restart, the caller may specify a new set of scankeys and/or
// orderbykeys; but the number of keys cannot differ from what index_beginscan
// was told.  (Later we might relax that to "must not exceed", but currently
// the index AMs tend to assume that scan->numberOfKeys is what to believe.)
// To restart the scan without changing keys, pass NULL for the key arrays.
// (Of course, keys *must* be passed on the first call, unless
// scan->numberOfKeys is zero.)
// ----------------
pub unsafe fn index_rescan(
    scan: IndexScanDesc,
    keys: ScanKey,
    nkeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) {
    SCAN_CHECKS!(scan);
    CHECK_SCAN_PROCEDURE!(scan, amrescan);

    Assert!(nkeys == (*scan).numberOfKeys);
    Assert!(norderbys == (*scan).numberOfOrderBys);

    /* Release resources (like buffer pins) from table accesses */
    if !(*scan).xs_heapfetch.is_null() {
        table_index_fetch_reset((*scan).xs_heapfetch);
    }

    (*scan).kill_prior_tuple = false; /* for safety */
    (*scan).xs_heap_continue = false;

    ((*(*(*scan).indexRelation).rd_indam).amrescan.unwrap())(scan as *mut core::ffi::c_void, keys as *mut core::ffi::c_void, nkeys, orderbys as *mut core::ffi::c_void, norderbys);
}

// ----------------
//		index_endscan - end a scan
// ----------------
pub unsafe fn index_endscan(scan: IndexScanDesc) {
    SCAN_CHECKS!(scan);
    CHECK_SCAN_PROCEDURE!(scan, amendscan);

    /* Release resources (like buffer pins) from table accesses */
    if !(*scan).xs_heapfetch.is_null() {
        table_index_fetch_end((*scan).xs_heapfetch);
        (*scan).xs_heapfetch = ptr::null_mut();
    }

    /* End the AM's scan */
    ((*(*(*scan).indexRelation).rd_indam).amendscan.unwrap())(scan as *mut core::ffi::c_void);

    /* Release index refcount acquired by index_beginscan */
    RelationDecrementReferenceCount((*scan).indexRelation);

    if (*scan).xs_temp_snap {
        UnregisterSnapshot((*scan).xs_snapshot);
    }

    /* Release the scan data structure itself */
    IndexScanEnd(scan);
}

// ----------------
//		index_markpos  - mark a scan position
// ----------------
pub unsafe fn index_markpos(scan: IndexScanDesc) {
    SCAN_CHECKS!(scan);
    CHECK_SCAN_PROCEDURE!(scan, ammarkpos);

    ((*(*(*scan).indexRelation).rd_indam).ammarkpos.unwrap())(scan as *mut core::ffi::c_void);
}

// ----------------
//		index_restrpos	- restore a scan position
//
// NOTE: this only restores the internal scan state of the index AM.  See
// comments for ExecRestrPos().
//
// NOTE: For heap, in the presence of HOT chains, mark/restore only works
// correctly if the scan's snapshot is MVCC-safe; that ensures that there's at
// most one returnable tuple in each HOT chain, and so restoring the prior
// state at the granularity of the index AM is sufficient.  Since the only
// current user of mark/restore functionality is nodeMergejoin.c, this
// effectively means that merge-join plans only work for MVCC snapshots.  This
// could be fixed if necessary, but for now it seems unimportant.
// ----------------
pub unsafe fn index_restrpos(scan: IndexScanDesc) {
    Assert!(IsMVCCSnapshot((*scan).xs_snapshot));

    SCAN_CHECKS!(scan);
    CHECK_SCAN_PROCEDURE!(scan, amrestrpos);

    /* release resources (like buffer pins) from table accesses */
    if !(*scan).xs_heapfetch.is_null() {
        table_index_fetch_reset((*scan).xs_heapfetch);
    }

    (*scan).kill_prior_tuple = false; /* for safety */
    (*scan).xs_heap_continue = false;

    ((*(*(*scan).indexRelation).rd_indam).amrestrpos.unwrap())(scan as *mut core::ffi::c_void);
}

// index_parallelscan_estimate - estimate shared memory for parallel scan
//
// When instrument=true, estimate includes SharedIndexScanInstrumentation
// space.  When parallel_aware=true, estimate includes whatever space the
// index AM's amestimateparallelscan routine requested when called.
pub unsafe fn index_parallelscan_estimate(
    indexRelation: Relation,
    nkeys: c_int,
    norderbys: c_int,
    snapshot: Snapshot,
    instrument: bool,
    parallel_aware: bool,
    nworkers: c_int,
) -> Size {
    Assert!(instrument || parallel_aware);

    RELATION_CHECKS!(indexRelation);

    let mut nbytes: Size =
        core::mem::offset_of!(ParallelIndexScanDescDataFull, ps_snapshot_data) as Size;
    nbytes = add_size(nbytes, EstimateSnapshotSpace(snapshot));
    nbytes = MAXALIGN(nbytes);

    if instrument {
        let sharedinfosz: Size = (core::mem::offset_of!(
            SharedIndexScanInstrumentation,
            winstrument
        ) as Size)
            + (nworkers as Size) * (core::mem::size_of::<IndexScanInstrumentation>() as Size);
        nbytes = add_size(nbytes, sharedinfosz);
        nbytes = MAXALIGN(nbytes);
    }

    /*
     * If parallel scan index AM interface can't be used (or index AM provides
     * no such interface), assume there is no AM-specific data needed
     */
    if parallel_aware && (*(*indexRelation).rd_indam).amestimateparallelscan.is_some() {
        nbytes = add_size(
            nbytes,
            ((*(*indexRelation).rd_indam).amestimateparallelscan.unwrap())(
                indexRelation,
                nkeys,
                norderbys,
            ),
        );
    }

    nbytes
}

// index_parallelscan_initialize - initialize parallel scan
//
// We initialize both the ParallelIndexScanDesc proper and the AM-specific
// information which follows it.
//
// This function calls access method specific initialization routine to
// initialize am specific information.  Call this just once in the leader
// process; then, individual workers attach via index_beginscan_parallel.
pub unsafe fn index_parallelscan_initialize(
    heapRelation: Relation,
    indexRelation: Relation,
    snapshot: Snapshot,
    instrument: bool,
    parallel_aware: bool,
    nworkers: c_int,
    sharedinfo: *mut *mut SharedIndexScanInstrumentation,
    target: ParallelIndexScanDesc,
) {
    Assert!(instrument || parallel_aware);

    RELATION_CHECKS!(indexRelation);

    let mut offset: Size = add_size(
        core::mem::offset_of!(ParallelIndexScanDescDataFull, ps_snapshot_data) as Size,
        EstimateSnapshotSpace(snapshot),
    );
    offset = MAXALIGN(offset);

    (*target).ps_locator = core::mem::transmute((*heapRelation).rd_locator);
    (*target).ps_indexlocator = core::mem::transmute((*indexRelation).rd_locator);
    (*target).ps_offset_ins = 0;
    (*target).ps_offset_am = 0;
    SerializeSnapshot(snapshot, (*target).ps_snapshot_data.as_mut_ptr());

    if instrument {
        (*target).ps_offset_ins = offset;
        let sharedinfosz: Size = (core::mem::offset_of!(
            SharedIndexScanInstrumentation,
            winstrument
        ) as Size)
            + (nworkers as Size) * (core::mem::size_of::<IndexScanInstrumentation>() as Size);
        offset = add_size(offset, sharedinfosz);
        offset = MAXALIGN(offset);

        /* Set leader's *sharedinfo pointer, and initialize stats */
        *sharedinfo =
            OffsetToPointer(target as *mut c_void, (*target).ps_offset_ins)
                as *mut SharedIndexScanInstrumentation;
        memset(*sharedinfo as *mut c_void, 0, sharedinfosz);
        (**sharedinfo).num_workers = nworkers;
    }

    /* aminitparallelscan is optional; assume no-op if not provided by AM */
    if parallel_aware && (*(*indexRelation).rd_indam).aminitparallelscan.is_some() {
        (*target).ps_offset_am = offset;
        let amtarget: *mut c_void = OffsetToPointer(target as *mut c_void, (*target).ps_offset_am);
        ((*(*indexRelation).rd_indam).aminitparallelscan.unwrap())(amtarget);
    }
}

// ----------------
//		index_parallelrescan  - (re)start a parallel scan of an index
// ----------------
pub unsafe fn index_parallelrescan(scan: IndexScanDesc) {
    SCAN_CHECKS!(scan);

    if !(*scan).xs_heapfetch.is_null() {
        table_index_fetch_reset((*scan).xs_heapfetch);
    }

    /* amparallelrescan is optional; assume no-op if not provided by AM */
    if let Some(amparallelrescan) = (*(*(*scan).indexRelation).rd_indam).amparallelrescan {
        amparallelrescan(scan as *mut core::ffi::c_void);
    }
}

// index_beginscan_parallel - join parallel index scan
//
// Caller must be holding suitable locks on the heap and the index.
pub unsafe fn index_beginscan_parallel(
    heaprel: Relation,
    indexrel: Relation,
    instrument: *mut IndexScanInstrumentation,
    nkeys: c_int,
    norderbys: c_int,
    pscan: ParallelIndexScanDesc,
) -> IndexScanDesc {
    Assert!(RelFileLocatorEquals(
        core::mem::transmute::<_, crate::storage::relfilelocator::RelFileLocator>((*heaprel).rd_locator),
        (*pscan).ps_locator
    ));
    Assert!(RelFileLocatorEquals(
        core::mem::transmute::<_, crate::storage::relfilelocator::RelFileLocator>((*indexrel).rd_locator),
        (*pscan).ps_indexlocator
    ));

    let snapshot: Snapshot = RestoreSnapshot((*pscan).ps_snapshot_data.as_mut_ptr());
    RegisterSnapshot(snapshot);
    let scan: IndexScanDesc =
        index_beginscan_internal(indexrel, nkeys, norderbys, snapshot, pscan, true);

    /*
     * Save additional parameters into the scandesc.  Everything else was set
     * up by index_beginscan_internal.
     */
    (*scan).heapRelation = heaprel;
    (*scan).xs_snapshot = snapshot;
    (*scan).instrument = instrument;

    /* prepare to fetch index matches from table */
    (*scan).xs_heapfetch = table_index_fetch_begin(heaprel);

    scan
}

// ----------------
// index_getnext_tid - get the next TID from a scan
//
// The result is the next TID satisfying the scan keys,
// or NULL if no more matching tuples exist.
// ----------------
pub unsafe fn index_getnext_tid(scan: IndexScanDesc, direction: ScanDirection) -> ItemPointer {
    SCAN_CHECKS!(scan);
    CHECK_SCAN_PROCEDURE!(scan, amgettuple);

    /* XXX: we should assert that a snapshot is pushed or registered */
    Assert!(TransactionIdIsValid(RecentXmin));

    /*
     * The AM's amgettuple proc finds the next index entry matching the scan
     * keys, and puts the TID into scan->xs_heaptid.  It should also set
     * scan->xs_recheck and possibly scan->xs_itup/scan->xs_hitup, though we
     * pay no attention to those fields here.
     */
    let found: bool = ((*(*(*scan).indexRelation).rd_indam).amgettuple.unwrap())(scan as *mut core::ffi::c_void, direction);

    /* Reset kill flag immediately for safety */
    (*scan).kill_prior_tuple = false;
    (*scan).xs_heap_continue = false;

    /* If we're out of index entries, we're done */
    if !found {
        /* release resources (like buffer pins) from table accesses */
        if !(*scan).xs_heapfetch.is_null() {
            table_index_fetch_reset((*scan).xs_heapfetch);
        }

        return ptr::null_mut();
    }
    Assert!(ItemPointerIsValid(&mut (*scan).xs_heaptid));

    pgstat_count_index_tuples((*scan).indexRelation, 1);

    /* Return the TID of the tuple we found. */
    &mut (*scan).xs_heaptid
}

// ----------------
//		index_fetch_heap - get the scan's next heap tuple
//
// The result is a visible heap tuple associated with the index TID most
// recently fetched by index_getnext_tid, or NULL if no more matching tuples
// exist.  (There can be more than one matching tuple because of HOT chains,
// although when using an MVCC snapshot it should be impossible for more than
// one such tuple to exist.)
//
// On success, the buffer containing the heap tup is pinned (the pin will be
// dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
// call).
//
// Note: caller must check scan->xs_recheck, and perform rechecking of the
// scan keys if required.  We do not do that here because we don't have
// enough information to do it efficiently in the general case.
// ----------------
pub unsafe fn index_fetch_heap(scan: IndexScanDesc, slot: *mut TupleTableSlot) -> bool {
    let mut all_dead: bool = false;

    let found: bool = table_index_fetch_tuple(
        (*scan).xs_heapfetch,
        &mut (*scan).xs_heaptid,
        (*scan).xs_snapshot,
        slot,
        &mut (*scan).xs_heap_continue,
        &mut all_dead,
    );

    if found {
        pgstat_count_heap_fetch((*scan).indexRelation);
    }

    /*
     * If we scanned a whole HOT chain and found only dead tuples, tell index
     * AM to kill its entry for that TID (this will take effect in the next
     * amgettuple call, in index_getnext_tid).  We do not do this when in
     * recovery because it may violate MVCC to do so.  See comments in
     * RelationGetIndexScan().
     */
    if !(*scan).xactStartedInRecovery {
        (*scan).kill_prior_tuple = all_dead;
    }

    found
}

// ----------------
//		index_getnext_slot - get the next tuple from a scan
//
// The result is true if a tuple satisfying the scan keys and the snapshot was
// found, false otherwise.  The tuple is stored in the specified slot.
//
// On success, resources (like buffer pins) are likely to be held, and will be
// dropped by a future index_getnext_tid, index_fetch_heap or index_endscan
// call).
//
// Note: caller must check scan->xs_recheck, and perform rechecking of the
// scan keys if required.  We do not do that here because we don't have
// enough information to do it efficiently in the general case.
// ----------------
pub unsafe fn index_getnext_slot(
    scan: IndexScanDesc,
    direction: ScanDirection,
    slot: *mut TupleTableSlot,
) -> bool {
    loop {
        if !(*scan).xs_heap_continue {
            /* Time to fetch the next TID from the index */
            let tid: ItemPointer = index_getnext_tid(scan, direction);

            /* If we're out of index entries, we're done */
            if tid.is_null() {
                break;
            }

            Assert!(ItemPointerEquals(tid, &mut (*scan).xs_heaptid));
        }

        /*
         * Fetch the next (or only) visible heap tuple for this index entry.
         * If we don't find anything, loop around and grab the next TID from
         * the index.
         */
        Assert!(ItemPointerIsValid(&mut (*scan).xs_heaptid));
        if index_fetch_heap(scan, slot) {
            return true;
        }
    }

    false
}

// ----------------
//		index_getbitmap - get all tuples at once from an index scan
//
// Adds the TIDs of all heap tuples satisfying the scan keys to a bitmap.
// Since there's no interlock between the index scan and the eventual heap
// access, this is only safe to use with MVCC-based snapshots: the heap
// item slot could have been replaced by a newer tuple by the time we get
// to it.
//
// Returns the number of matching tuples found.  (Note: this might be only
// approximate, so it should only be used for statistical purposes.)
// ----------------
pub unsafe fn index_getbitmap(scan: IndexScanDesc, bitmap: *mut TIDBitmap) -> int64 {
    SCAN_CHECKS!(scan);
    CHECK_SCAN_PROCEDURE!(scan, amgetbitmap);

    /* just make sure this is false... */
    (*scan).kill_prior_tuple = false;

    /*
     * have the am's getbitmap proc do all the work.
     */
    let ntids: int64 = ((*(*(*scan).indexRelation).rd_indam).amgetbitmap.unwrap())(scan as *mut core::ffi::c_void, bitmap as *mut core::ffi::c_void);

    pgstat_count_index_tuples((*scan).indexRelation, ntids);

    ntids
}

// ----------------
//		index_bulk_delete - do mass deletion of index entries
//
//		callback routine tells whether a given main-heap tuple is
//		to be deleted
//
//		return value is an optional palloc'd struct of statistics
// ----------------
pub unsafe fn index_bulk_delete(
    info: *mut IndexVacuumInfo,
    istat: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    let indexRelation: Relation = (*info).index;

    RELATION_CHECKS!(indexRelation);
    CHECK_REL_PROCEDURE!(indexRelation, ambulkdelete);

    ((*(*indexRelation).rd_indam).ambulkdelete.unwrap())(info as *mut core::ffi::c_void, istat as *mut core::ffi::c_void, callback, callback_state) as *mut IndexBulkDeleteResult
}

// ----------------
//		index_vacuum_cleanup - do post-deletion cleanup of an index
//
//		return value is an optional palloc'd struct of statistics
// ----------------
pub unsafe fn index_vacuum_cleanup(
    info: *mut IndexVacuumInfo,
    istat: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    let indexRelation: Relation = (*info).index;

    RELATION_CHECKS!(indexRelation);
    CHECK_REL_PROCEDURE!(indexRelation, amvacuumcleanup);

    ((*(*indexRelation).rd_indam).amvacuumcleanup.unwrap())(info as *mut core::ffi::c_void, istat as *mut core::ffi::c_void) as *mut IndexBulkDeleteResult
}

// ----------------
//		index_can_return
//
//		Does the index access method support index-only scans for the given
//		column?
// ----------------
pub unsafe fn index_can_return(indexRelation: Relation, attno: c_int) -> bool {
    RELATION_CHECKS!(indexRelation);

    /* amcanreturn is optional; assume false if not provided by AM */
    match (*(*indexRelation).rd_indam).amcanreturn {
        None => false,
        Some(amcanreturn) => amcanreturn(indexRelation, attno),
    }
}

// ----------------
//		index_getprocid
//
//		Index access methods typically require support routines that are
//		not directly the implementation of any WHERE-clause query operator
//		and so cannot be kept in pg_amop.  Instead, such routines are kept
//		in pg_amproc.  These registered procedure OIDs are assigned numbers
//		according to a convention established by the access method.
//		The general index code doesn't know anything about the routines
//		involved; it just builds an ordered list of them for
//		each attribute on which an index is defined.
//
//		As of Postgres 8.3, support routines within an operator family
//		are further subdivided by the "left type" and "right type" of the
//		query operator(s) that they support.  The "default" functions for a
//		particular indexed attribute are those with both types equal to
//		the index opclass' opcintype (note that this is subtly different
//		from the indexed attribute's own type: it may be a binary-compatible
//		type instead).  Only the default functions are stored in relcache
//		entries --- access methods can use the syscache to look up non-default
//		functions.
//
//		This routine returns the requested default procedure OID for a
//		particular indexed attribute.
// ----------------
pub unsafe fn index_getprocid(irel: Relation, attnum: AttrNumber, procnum: uint16) -> RegProcedure {
    let nproc: c_int = (*(*irel).rd_indam).amsupport as c_int;

    Assert!(procnum > 0 && procnum <= nproc as uint16);

    let procindex: c_int = (nproc * (attnum as c_int - 1)) + (procnum as c_int - 1);

    let loc: *mut RegProcedure = (*irel).rd_support;

    Assert!(!loc.is_null());

    *loc.add(procindex as usize)
}

// ----------------
//		index_getprocinfo
//
//		This routine allows index AMs to keep fmgr lookup info for
//		support procs in the relcache.  As above, only the "default"
//		functions for any particular indexed attribute are cached.
//
// Note: the return value points into cached data that will be lost during
// any relcache rebuild!  Therefore, either use the callinfo right away,
// or save it only after having acquired some type of lock on the index rel.
// ----------------
#[no_mangle]
pub unsafe fn index_getprocinfo(
    irel: Relation,
    attnum: AttrNumber,
    procnum: uint16,
) -> *mut FmgrInfo {
    let nproc: c_int = (*(*irel).rd_indam).amsupport as c_int;
    let optsproc: c_int = (*(*irel).rd_indam).amoptsprocnum as c_int;

    Assert!(procnum > 0 && procnum <= nproc as uint16);

    let procindex: c_int = (nproc * (attnum as c_int - 1)) + (procnum as c_int - 1);

    let mut locinfo: *mut FmgrInfo = (*irel).rd_supportinfo;

    Assert!(!locinfo.is_null());

    locinfo = locinfo.add(procindex as usize);

    /* Initialize the lookup info if first time through */
    if (*locinfo).fn_oid == InvalidOid {
        let loc: *mut RegProcedure = (*irel).rd_support;

        Assert!(!loc.is_null());

        let procId: RegProcedure = *loc.add(procindex as usize);

        /*
         * Complain if function was not found during IndexSupportInitialize.
         * This should not happen unless the system tables contain bogus
         * entries for the index opclass.  (If an AM wants to allow a support
         * function to be optional, it can use index_getprocid.)
         */
        if !RegProcedureIsValid(procId) {
            elog!(
                ERROR,
                "missing support function {} for attribute {} of index",
                procnum,
                attnum
            );
        }

        fmgr_info_cxt(procId, locinfo, (*irel).rd_indexcxt as crate::utils::palloc::MemoryContext);

        if procnum as c_int != optsproc {
            /* Initialize locinfo->fn_expr with opclass options Const */
            let attoptions: *mut *mut bytea = RelationGetIndexAttOptions(irel, false);
            let oldcxt: MemoryContext = MemoryContextSwitchTo((*irel).rd_indexcxt as crate::utils::palloc::MemoryContext);

            set_fn_opclass_options(locinfo, *attoptions.add((attnum as usize) - 1));

            MemoryContextSwitchTo(oldcxt);
        }
    }

    locinfo
}

// ----------------
//		index_store_float8_orderby_distances
//
//		Convert AM distance function's results (that can be inexact)
//		to ORDER BY types and save them into xs_orderbyvals/xs_orderbynulls
//		for a possible recheck.
// ----------------
pub unsafe fn index_store_float8_orderby_distances(
    scan: IndexScanDesc,
    orderByTypes: *mut Oid,
    distances: *mut IndexOrderByDistance,
    recheckOrderBy: bool,
) {
    Assert!(!distances.is_null() || !recheckOrderBy);

    (*scan).xs_recheckorderby = recheckOrderBy;

    let mut i: c_int = 0;
    while i < (*scan).numberOfOrderBys {
        let ot: Oid = *orderByTypes.add(i as usize);
        if ot == FLOAT8OID {
            // #ifndef USE_FLOAT8_BYVAL
            if !USE_FLOAT8_BYVAL {
                /* must free any old value to avoid memory leakage */
                if !*(*scan).xs_orderbynulls.add(i as usize) {
                    pfree(DatumGetPointer(*(*scan).xs_orderbyvals.add(i as usize)) as *mut core::ffi::c_void);
                }
            }
            if !distances.is_null() && !(*distances.add(i as usize)).isnull {
                *(*scan).xs_orderbyvals.add(i as usize) =
                    Float8GetDatum((*distances.add(i as usize)).value);
                *(*scan).xs_orderbynulls.add(i as usize) = false;
            } else {
                *(*scan).xs_orderbyvals.add(i as usize) = 0 as Datum;
                *(*scan).xs_orderbynulls.add(i as usize) = true;
            }
        } else if ot == FLOAT4OID {
            /* convert distance function's result to ORDER BY type */
            if !distances.is_null() && !(*distances.add(i as usize)).isnull {
                *(*scan).xs_orderbyvals.add(i as usize) =
                    Float4GetDatum((*distances.add(i as usize)).value as f32);
                *(*scan).xs_orderbynulls.add(i as usize) = false;
            } else {
                *(*scan).xs_orderbyvals.add(i as usize) = 0 as Datum;
                *(*scan).xs_orderbynulls.add(i as usize) = true;
            }
        } else {
            /*
             * If the ordering operator's return value is anything else, we
             * don't know how to convert the float8 bound calculated by the
             * distance function to that.  The executor won't actually need
             * the order by values we return here, if there are no lossy
             * results, so only insist on converting if the *recheck flag is
             * set.
             */
            if (*scan).xs_recheckorderby {
                elog!(
                    ERROR,
                    "ORDER BY operator must return float8 or float4 if the distance function is lossy"
                );
            }
            *(*scan).xs_orderbynulls.add(i as usize) = true;
        }
        i += 1;
    }
}

// ----------------
//      index_opclass_options
//
//      Parse opclass-specific options for index column.
// ----------------
pub unsafe fn index_opclass_options(
    indrel: Relation,
    attnum: AttrNumber,
    attoptions: Datum,
    validate: bool,
) -> *mut bytea {
    let amoptsprocnum: c_int = (*(*indrel).rd_indam).amoptsprocnum as c_int;
    let mut procid: Oid = InvalidOid;
    let procinfo: *mut FmgrInfo;
    let mut relopts: local_relopts = std::mem::zeroed();

    /* fetch options support procedure if specified */
    if amoptsprocnum != 0 {
        procid = index_getprocid(indrel, attnum, amoptsprocnum as uint16);
    }

    if !OidIsValid(procid) {
        if DatumGetPointer(attoptions).is_null() {
            return ptr::null_mut(); /* ok, no options, no procedure */
        }

        /*
         * Report an error if the opclass's options-parsing procedure does not
         * exist but the opclass options are specified.
         */
        let indclassDatum: Datum = SysCacheGetAttrNotNull(
            INDEXRELID,
            (*indrel).rd_indextuple as *mut HeapTupleData,
            Anum_pg_index_indclass,
        );
        let indclass: *mut oidvector = DatumGetPointer(indclassDatum) as *mut oidvector;
        let _opclass: Oid = *(*indclass).values.as_ptr().add((attnum as usize) - 1);

        ereport!(ERROR, "operator class has no options");
    }

    init_local_reloptions(&mut relopts, 0);

    procinfo = index_getprocinfo(indrel, attnum, amoptsprocnum as uint16);

    FunctionCall1(procinfo, PointerGetDatum(&mut relopts as *mut _ as *mut c_void));

    build_local_reloptions(&mut relopts, attoptions, validate)
}

// ----------------------------------------------------------------
//   Local stub types for fields accessed via opaque pointers
//   (these mirror the real C structs; the main agent will reconcile
//   them with the canonical definitions when those modules land)
// ----------------------------------------------------------------

/// Layout helper used only for offsetof(ParallelIndexScanDescData, ps_snapshot_data).
#[repr(C)]
pub struct ParallelIndexScanDescDataFull {
    pub ps_locator: RelFileLocator,
    pub ps_indexlocator: RelFileLocator,
    pub ps_offset_ins: Size,
    pub ps_offset_am: Size,
    pub ps_snapshot_data: [u8; 0], // FLEXIBLE_ARRAY_MEMBER (char[])
}

// Opaque foreign types used in signatures.
// bytea and MemoryContext come from the prelude (crate::c::bytea,
// crate::utils::palloc::MemoryContext) - do not redefine them here.
pub use crate::utils::rel::LockRelId;
pub use crate::storage::relfilelocator::RelFileLocator;
pub type local_relopts = LocalReloptsStub;
pub type oidvector = OidVectorStub;

#[repr(C)]
pub struct LockRelIdStub {
    _private: [u8; 0],
}
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RelFileLocatorStub {
    _private: [u8; 0],
}
#[repr(C)]
pub struct LocalReloptsStub {
    _private: [u8; 0],
}
#[repr(C)]
pub struct OidVectorStub {
    pub values: [Oid; 0], // FLEXIBLE_ARRAY_MEMBER
}

// ----------------------------------------------------------------
//   Local stubs for unported helper functions
// ----------------------------------------------------------------

unsafe fn relation_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    crate::access::common::relation::relation_open(_relationId as _, _lockmode as _) as _
}
unsafe fn try_relation_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    crate::access::common::relation::try_relation_open(_relationId as _, _lockmode as _) as _
}
unsafe fn RelationClose(_relation: Relation) {
    crate::utils::cache::relcache::RelationClose(_relation as _);
}
unsafe fn UnlockRelationId(_relid: *mut LockRelId, _lockmode: LOCKMODE) {
    crate::storage::lmgr::lmgr::UnlockRelationId(_relid as _, _lockmode as _);
}
unsafe fn CheckForSerializableConflictIn(
    _relation: Relation,
    _tid: ItemPointer,
    _blkno: BlockNumber,
) { unimplemented!() }
unsafe fn PredicateLockRelation(_relation: Relation, _snapshot: Snapshot) {
    crate::storage::lmgr::predicate::PredicateLockRelation(_relation as _, _snapshot as _);
}
unsafe fn RelationIncrementReferenceCount(_rel: Relation) {
    crate::utils::cache::relcache::RelationIncrementReferenceCount(_rel as _);
}
unsafe fn RelationDecrementReferenceCount(_rel: Relation) {
    crate::utils::cache::relcache::RelationDecrementReferenceCount(_rel as _);
}
unsafe fn table_index_fetch_begin(_rel: Relation) -> *mut IndexFetchTableData {
    crate::access::table::tableam::table_index_fetch_begin(_rel as _) as _
}
unsafe fn table_index_fetch_reset(_scan: *mut IndexFetchTableData) {
    crate::access::table::tableam::table_index_fetch_reset(_scan as _)
}
unsafe fn table_index_fetch_end(_scan: *mut IndexFetchTableData) {
    crate::access::table::tableam::table_index_fetch_end(_scan as _);
}
unsafe fn table_index_fetch_tuple(
    _scan: *mut IndexFetchTableData,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
    _call_again: *mut bool,
    _all_dead: *mut bool,
) -> bool {
    crate::access::table::tableam::table_index_fetch_tuple(_scan as _, _tid as _, _snapshot as _, _slot as _, _call_again as _, _all_dead as _) as _
}
unsafe fn UnregisterSnapshot(_snapshot: Snapshot) {
    crate::utils::time::snapmgr::UnregisterSnapshot(_snapshot as _)
}
unsafe fn RegisterSnapshot(_snapshot: Snapshot) -> Snapshot {
    crate::utils::time::snapmgr::RegisterSnapshot(_snapshot as _) as _
}
unsafe fn RestoreSnapshot(_start_address: *mut std::ffi::c_char) -> Snapshot { crate::utils::time::snapmgr::RestoreSnapshot(_start_address as _) }
unsafe fn SerializeSnapshot(_snapshot: Snapshot, _start_address: *mut std::ffi::c_char) { crate::utils::time::snapmgr::SerializeSnapshot(_snapshot, _start_address as _) }
unsafe fn EstimateSnapshotSpace(_snapshot: Snapshot) -> Size { crate::utils::time::snapmgr::EstimateSnapshotSpace(_snapshot) }
unsafe fn IndexScanEnd(_scan: IndexScanDesc) {
    crate::access::index::genam::IndexScanEnd(_scan as _)
}
unsafe fn pgstat_count_index_tuples(_rel: Relation, _n: int64) {
    { let _=(_rel,_n); }
}
unsafe fn pgstat_count_heap_fetch(_rel: Relation) {
    { let _=_rel; }
}
unsafe fn fmgr_info_cxt(_functionId: Oid, _finfo: *mut FmgrInfo, _mcxt: MemoryContext) {
    crate::utils::fmgr::fmgr_info_cxt(_functionId as _, _finfo as _, _mcxt as _);
}
unsafe fn set_fn_opclass_options(_finfo: *mut FmgrInfo, _options: *mut bytea) {
    // no-op: opclass options unused for catalog index support functions
}
unsafe fn RelationGetIndexAttOptions(_relation: Relation, _copy: bool) -> *mut *mut bytea {
    core::ptr::null_mut() // catalog indexes carry no per-attribute opclass options
}
unsafe fn FunctionCall1(_flinfo: *mut FmgrInfo, _arg1: Datum) -> Datum {
    unimplemented!() // TODO: utils/fmgr/fmgr.c
}
unsafe fn init_local_reloptions(_relopts: *mut local_relopts, _relopt_struct_size: Size) { unimplemented!() }
unsafe fn build_local_reloptions(
    _relopts: *mut local_relopts,
    _options: Datum,
    _validate: bool,
) -> *mut bytea { unimplemented!() }
unsafe fn SysCacheGetAttrNotNull(
    _cacheId: c_int,
    _tup: *mut HeapTupleData,
    _attributeNumber: AttrNumber,
) -> Datum {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn generate_opclass_name(_opclass: Oid) -> *mut std::ffi::c_char { unimplemented!() }
pub use crate::catalog::index::ReindexIsProcessingIndex;
unsafe fn RelationGetRelid(_relation: Relation) -> Oid {
    crate::utils::rel::RelationGetRelid(_relation as _) as _
}

// Inline helpers / macros from headers, stubbed locally
#[inline]
unsafe fn RelationIsValid(relation: Relation) -> bool {
    !relation.is_null()
}
#[inline]
unsafe fn PointerIsValid<T>(p: *const T) -> bool {
    !p.is_null()
}
#[inline]
unsafe fn IndexScanIsValid(scan: IndexScanDesc) -> bool {
    !scan.is_null()
}
// unlikely() comes from the prelude (crate::c::unlikely).
#[inline]
unsafe fn ItemPointerIsValid(_pointer: ItemPointer) -> bool {
    crate::storage::itemptr::ItemPointerIsValid(_pointer as _)
}
#[inline]
unsafe fn ItemPointerEquals(_p1: ItemPointer, _p2: ItemPointer) -> bool {
    crate::storage::itemptr::ItemPointerEquals(_p1 as _, _p2 as _)
}
#[inline]
unsafe fn TransactionIdIsValid(_xid: crate::c::TransactionId) -> bool {
    crate::access::transam::TransactionIdIsValid(_xid as _)
}
#[inline]
unsafe fn IsMVCCSnapshot(_snapshot: Snapshot) -> bool {
    (*_snapshot).snapshot_type == 0 // SNAPSHOT_MVCC
}
#[inline]
unsafe fn RelFileLocatorEquals(_a: RelFileLocator, _b: RelFileLocator) -> bool { unimplemented!() }
#[inline]
unsafe fn RegProcedureIsValid(p: RegProcedure) -> bool {
    p != InvalidOid
}
#[inline]
unsafe fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}
#[inline]
fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2 // TODO: utils/memutils.c add_size (overflow check)
}
#[inline]
unsafe fn OffsetToPointer(base: *mut c_void, offset: Size) -> *mut c_void {
    (base as *mut u8).add(offset) as *mut c_void
}
#[inline]
unsafe fn Float8GetDatum(_value: f64) -> Datum { crate::postgres::Float8GetDatum(_value as _) }
#[inline]
unsafe fn Float4GetDatum(_value: f32) -> Datum { crate::postgres::Float4GetDatum(_value as _) }
#[inline]
unsafe fn PointerGetDatum(_p: *mut c_void) -> Datum {
    unimplemented!() // TODO: postgres.h
}

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// Foreign types referenced only as pointers in stubs
pub use crate::access::relscan::IndexFetchTableData;

// Constants from headers (stubbed)
pub const NoLock: LOCKMODE = 0;
pub const MAX_LOCKMODES: LOCKMODE = 10;
pub const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;
pub const InvalidSnapshot: Snapshot = ptr::null_mut();
pub const RELKIND_INDEX: std::ffi::c_char = b'i' as std::ffi::c_char;
pub const RELKIND_PARTITIONED_INDEX: std::ffi::c_char = b'I' as std::ffi::c_char;
pub const INDEXRELID: c_int = 34; // TODO: utils/syscache.h
pub const Anum_pg_index_indclass: AttrNumber = 0; // TODO: catalog/pg_index.h
pub const FLOAT8OID: Oid = 701;
pub const FLOAT4OID: Oid = 700;

// RecentXmin global (utils/time/snapmgr.c)
pub static mut RecentXmin: crate::c::TransactionId = 0;
