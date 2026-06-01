//! src/backend/access/index/genam.c
//!
//! general index access method routines
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES
//!   many of the old access method routines have been turned into
//!   macros and moved to genam.h -cim 4/30/91
//!
//! This file also incorporates the contents of src/include/access/genam.h.

use crate::prelude::*;

use std::ffi::{c_int, c_void};

use crate::c::{uint16, uint64, Size, FLEXIBLE_ARRAY_MEMBER, TransactionId};
use crate::access::attnum::AttrNumber;
use crate::storage::block::BlockNumber;

/* ===================== from genam.h ===================== */

/* We don't want this file to depend on execnodes.h. */
// struct IndexInfo;  -- opaque forward decl; represented via raw pointers below

/*
 * Struct for statistics maintained by amgettuple and amgetbitmap
 *
 * Note: IndexScanInstrumentation can't contain any pointers, since it is
 * copied into a SharedIndexScanInstrumentation during parallel scans
 */
#[repr(C)]
pub struct IndexScanInstrumentation {
    /* Index search count (incremented with pgstat_count_index_scan call) */
    pub nsearches: uint64,
}

/*
 * Struct for every worker's IndexScanInstrumentation, stored in shared memory
 */
#[repr(C)]
pub struct SharedIndexScanInstrumentation {
    pub num_workers: c_int,
    pub winstrument: [IndexScanInstrumentation; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * Struct for statistics returned by ambuild
 */
#[repr(C)]
pub struct IndexBuildResult {
    pub heap_tuples: f64,  /* # of tuples seen in parent table */
    pub index_tuples: f64, /* # of tuples inserted into index */
}

/*
 * Struct for input arguments passed to ambulkdelete and amvacuumcleanup
 *
 * num_heap_tuples is accurate only when estimated_count is false;
 * otherwise it's just an estimate (currently, the estimate is the
 * prior value of the relation's pg_class.reltuples field, so it could
 * even be -1).  It will always just be an estimate during ambulkdelete.
 */
#[repr(C)]
pub struct IndexVacuumInfo {
    pub index: Relation,          /* the index being vacuumed */
    pub heaprel: Relation,        /* the heap relation the index belongs to */
    pub analyze_only: bool,       /* ANALYZE (without any actual vacuum) */
    pub report_progress: bool,    /* emit progress.h status reports */
    pub estimated_count: bool,    /* num_heap_tuples is an estimate */
    pub message_level: c_int,     /* ereport level for progress messages */
    pub num_heap_tuples: f64,     /* tuples remaining in heap */
    pub strategy: BufferAccessStrategy, /* access strategy for reads */
}

/*
 * Struct for statistics returned by ambulkdelete and amvacuumcleanup
 */
#[repr(C)]
pub struct IndexBulkDeleteResult {
    pub num_pages: BlockNumber,          /* pages remaining in index */
    pub estimated_count: bool,           /* num_index_tuples is an estimate */
    pub num_index_tuples: f64,           /* tuples remaining */
    pub tuples_removed: f64,             /* # removed during vacuum operation */
    pub pages_newly_deleted: BlockNumber, /* # pages marked deleted by us  */
    pub pages_deleted: BlockNumber,      /* # pages marked deleted (could be by us) */
    pub pages_free: BlockNumber,         /* # pages available for reuse */
}

/* Typedef for callback function to determine if a tuple is bulk-deletable */
pub type IndexBulkDeleteCallback =
    Option<unsafe extern "C" fn(itemptr: ItemPointer, state: *mut c_void) -> bool>;

/* struct definitions appear in relscan.h */
pub type IndexScanDesc = *mut IndexScanDescData;
pub type SysScanDesc = *mut SysScanDescData;

pub type ParallelIndexScanDesc = *mut ParallelIndexScanDescData;

/*
 * Enumeration specifying the type of uniqueness check to perform in
 * index_insert().
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum IndexUniqueCheck {
    UNIQUE_CHECK_NO,       /* Don't do any uniqueness checking */
    UNIQUE_CHECK_YES,      /* Enforce uniqueness at insertion time */
    UNIQUE_CHECK_PARTIAL,  /* Test uniqueness, but no error */
    UNIQUE_CHECK_EXISTING, /* Check if existing tuple is unique */
}

/* Nullable "ORDER BY col op const" distance */
#[repr(C)]
pub struct IndexOrderByDistance {
    pub value: f64,
    pub isnull: bool,
}

/*
 * IndexScanIsValid
 *      True iff the index scan is valid.
 */
#[allow(unused_macros)]
macro_rules! IndexScanIsValid {
    ($scan:expr) => {
        PointerIsValid($scan)
    };
}

/* ===================== local stub types ===================== */

#[repr(C)]
pub struct IndexScanDescData {
    pub heapRelation: Relation,
    pub indexRelation: Relation,
    pub xs_snapshot: Snapshot,
    pub numberOfKeys: c_int,
    pub numberOfOrderBys: c_int,
    pub keyData: ScanKey,
    pub orderByData: ScanKey,
    pub xs_want_itup: bool,
    pub kill_prior_tuple: bool,
    pub xactStartedInRecovery: bool,
    pub ignore_killed_tuples: bool,
    pub opaque: *mut c_void,
    pub instrument: *mut IndexScanInstrumentation,
    pub xs_itup: IndexTuple,
    pub xs_itupdesc: TupleDesc,
    pub xs_hitup: HeapTuple,
    pub xs_hitupdesc: TupleDesc,
    pub xs_heapfetch: *mut c_void,
    pub xs_recheck: bool,
}

#[repr(C)]
pub struct SysScanDescData {
    pub heap_rel: Relation,
    pub irel: Relation,
    pub slot: *mut TupleTableSlot,
    pub scan: TableScanDesc,
    pub iscan: IndexScanDesc,
    pub snapshot: Snapshot,
}

#[repr(C)]
pub struct ParallelIndexScanDescData {
    _private: [u8; 0],
}

/* ===================== genam.c ===================== */

/* ----------------------------------------------------------------
 *      general access method routines
 *
 *      All indexed access methods use an identical scan structure.
 *      We don't know how the various AMs do locking, however, so we don't
 *      do anything about that here.
 * ----------------------------------------------------------------
 */

/* ----------------
 *  RelationGetIndexScan -- Create and fill an IndexScanDesc.
 *
 *      This routine creates an index scan structure and sets up initial
 *      contents for it.
 *
 *      Parameters:
 *              indexRelation -- index relation for scan.
 *              nkeys -- count of scan keys (index qual conditions).
 *              norderbys -- count of index order-by operators.
 *
 *      Returns:
 *              An initialized IndexScanDesc.
 * ----------------
 */
pub unsafe fn RelationGetIndexScan(
    indexRelation: Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> IndexScanDesc {
    let scan: IndexScanDesc;

    scan = palloc(std::mem::size_of::<IndexScanDescData>()) as IndexScanDesc;

    (*scan).heapRelation = std::ptr::null_mut(); /* may be set later */
    (*scan).xs_heapfetch = std::ptr::null_mut();
    (*scan).indexRelation = indexRelation;
    (*scan).xs_snapshot = InvalidSnapshot; /* caller must initialize this */
    (*scan).numberOfKeys = nkeys;
    (*scan).numberOfOrderBys = norderbys;

    /*
     * We allocate key workspace here, but it won't get filled until amrescan.
     */
    if nkeys > 0 {
        (*scan).keyData =
            palloc(std::mem::size_of::<ScanKeyData>() * nkeys as usize) as ScanKey;
    } else {
        (*scan).keyData = std::ptr::null_mut();
    }
    if norderbys > 0 {
        (*scan).orderByData =
            palloc(std::mem::size_of::<ScanKeyData>() * norderbys as usize) as ScanKey;
    } else {
        (*scan).orderByData = std::ptr::null_mut();
    }

    (*scan).xs_want_itup = false; /* may be set later */

    /*
     * During recovery we ignore killed tuples and don't bother to kill them
     * either. We do this because the xmin on the primary node could easily be
     * later than the xmin on the standby node, so that what the primary
     * thinks is killed is supposed to be visible on standby. So for correct
     * MVCC for queries during recovery we must ignore these hints and check
     * all tuples. Do *not* set ignore_killed_tuples to true when running in a
     * transaction that was started during recovery. xactStartedInRecovery
     * should not be altered by index AMs.
     */
    (*scan).kill_prior_tuple = false;
    (*scan).xactStartedInRecovery = TransactionStartedDuringRecovery();
    (*scan).ignore_killed_tuples = !(*scan).xactStartedInRecovery;

    (*scan).opaque = std::ptr::null_mut();
    (*scan).instrument = std::ptr::null_mut();

    (*scan).xs_itup = std::ptr::null_mut();
    (*scan).xs_itupdesc = std::ptr::null_mut() as TupleDesc;
    (*scan).xs_hitup = std::ptr::null_mut();
    (*scan).xs_hitupdesc = std::ptr::null_mut() as TupleDesc;

    scan
}

/* ----------------
 *  IndexScanEnd -- End an index scan.
 *
 *      This routine just releases the storage acquired by
 *      RelationGetIndexScan().  Any AM-level resources are
 *      assumed to already have been released by the AM's
 *      endscan routine.
 *
 *  Returns:
 *      None.
 * ----------------
 */
pub unsafe fn IndexScanEnd(scan: IndexScanDesc) {
    if !(*scan).keyData.is_null() {
        pfree((*scan).keyData as *mut c_void);
    }
    if !(*scan).orderByData.is_null() {
        pfree((*scan).orderByData as *mut c_void);
    }

    pfree(scan as *mut c_void);
}

/*
 * BuildIndexValueDescription
 *
 * Construct a string describing the contents of an index entry, in the
 * form "(key_name, ...)=(key_value, ...)".  This is currently used
 * for building unique-constraint, exclusion-constraint error messages, and
 * logical replication conflict error messages so only key columns of the index
 * are checked and printed.
 *
 * Note that if the user does not have permissions to view all of the
 * columns involved then a NULL is returned.  Returning a partial key seems
 * unlikely to be useful and we have no way to know which of the columns the
 * user provided (unlike in ExecBuildSlotValueDescription).
 *
 * The passed-in values/nulls arrays are the "raw" input to the index AM,
 * e.g. results of FormIndexDatum --- this is not necessarily what is stored
 * in the index, but it's what the user perceives to be stored.
 *
 * Note: if you change anything here, check whether
 * ExecBuildSlotPartitionKeyDescription() in execMain.c needs a similar
 * change.
 */
pub unsafe fn BuildIndexValueDescription(
    indexRelation: Relation,
    values: *const Datum,
    isnull: *const bool,
) -> *mut c_char {
    let mut buf: StringInfoData = std::mem::zeroed();
    let idxrec: Form_pg_index;
    let indnkeyatts: c_int;
    let mut i: c_int;
    let mut keyno: c_int;
    let indexrelid: Oid = RelationGetRelid(indexRelation);
    let indrelid: Oid;
    let aclresult: AclResult;

    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(indexRelation);

    /*
     * Check permissions- if the user does not have access to view all of the
     * key columns then return NULL to avoid leaking data.
     *
     * First check if RLS is enabled for the relation.  If so, return NULL to
     * avoid leaking data.
     *
     * Next we need to check table-level SELECT access and then, if there is
     * no access there, check column-level permissions.
     */
    idxrec = (*indexRelation).rd_index as *mut _;
    indrelid = (*idxrec).indrelid;
    Assert!(indexrelid == (*idxrec).indexrelid);

    /* RLS check- if RLS is enabled then we don't return anything. */
    if check_enable_rls(indrelid, InvalidOid, true) == RLS_ENABLED {
        return std::ptr::null_mut();
    }

    /* Table-level SELECT is enough, if the user has it */
    aclresult = pg_class_aclcheck(indrelid, GetUserId(), ACL_SELECT);
    if aclresult != ACLCHECK_OK {
        /*
         * No table-level access, so step through the columns in the index and
         * make sure the user has SELECT rights on all of them.
         */
        keyno = 0;
        while keyno < indnkeyatts {
            let attnum: AttrNumber = *(*idxrec).indkey.values.as_ptr().add(keyno as usize);

            /*
             * Note that if attnum == InvalidAttrNumber, then this is an index
             * based on an expression and we return no detail rather than try
             * to figure out what column(s) the expression includes and if the
             * user has SELECT rights on them.
             */
            if attnum == InvalidAttrNumber
                || pg_attribute_aclcheck(indrelid, attnum, GetUserId(), ACL_SELECT) != ACLCHECK_OK
            {
                /* No access, so clean up and return */
                return std::ptr::null_mut();
            }
            keyno += 1;
        }
    }

    initStringInfo(&mut buf);
    appendStringInfo(
        &mut buf,
        c"(%s)=(".as_ptr(),
        pg_get_indexdef_columns(indexrelid, true),
    );

    i = 0;
    while i < indnkeyatts {
        let val: *mut c_char;

        if *isnull.add(i as usize) {
            val = c"null".as_ptr() as *mut c_char;
        } else {
            let mut foutoid: Oid = InvalidOid;
            let mut typisvarlena: bool = false;

            /*
             * The provided data is not necessarily of the type stored in the
             * index; rather it is of the index opclass's input type. So look
             * at rd_opcintype not the index tupdesc.
             *
             * Note: this is a bit shaky for opclasses that have pseudotype
             * input types such as ANYARRAY or RECORD.  Currently, the
             * typoutput functions associated with the pseudotypes will work
             * okay, but we might have to try harder in future.
             */
            getTypeOutputInfo(
                *(*indexRelation).rd_opcintype.add(i as usize),
                &mut foutoid,
                &mut typisvarlena,
            );
            val = OidOutputFunctionCall(foutoid, *values.add(i as usize));
        }

        if i > 0 {
            appendStringInfoString(&mut buf, c", ".as_ptr());
        }
        appendStringInfoString(&mut buf, val);
        i += 1;
    }

    appendStringInfoChar(&mut buf, b')' as c_char);

    buf.data
}

/*
 * Get the snapshotConflictHorizon from the table entries pointed to by the
 * index tuples being deleted using an AM-generic approach.
 *
 * This is a table_index_delete_tuples() shim used by index AMs that only need
 * to consult the tableam to get a snapshotConflictHorizon value, and only
 * expect to delete index tuples that are already known deletable (typically
 * due to having LP_DEAD bits set).  When a snapshotConflictHorizon value
 * isn't needed in index AM's deletion WAL record, it is safe for it to skip
 * calling here entirely.
 *
 * We assume that caller index AM uses the standard IndexTuple representation,
 * with table TIDs stored in the t_tid field.  We also expect (and assert)
 * that the line pointers on page for 'itemnos' offsets are already marked
 * LP_DEAD.
 */
pub unsafe fn index_compute_xid_horizon_for_tuples(
    irel: Relation,
    hrel: Relation,
    ibuf: Buffer,
    itemnos: *mut OffsetNumber,
    nitems: c_int,
) -> TransactionId {
    let mut delstate: TM_IndexDeleteOp = std::mem::zeroed();
    let snapshotConflictHorizon: TransactionId;
    let ipage: Page = BufferGetPage(ibuf);
    let mut itup: IndexTuple;

    Assert!(nitems > 0);

    delstate.irel = irel;
    delstate.iblknum = BufferGetBlockNumber(ibuf);
    delstate.bottomup = false;
    delstate.bottomupfreespace = 0;
    delstate.ndeltids = 0;
    delstate.deltids =
        palloc(nitems as usize * std::mem::size_of::<TM_IndexDelete>()) as *mut TM_IndexDelete;
    delstate.status =
        palloc(nitems as usize * std::mem::size_of::<TM_IndexStatus>()) as *mut TM_IndexStatus;

    /* identify what the index tuples about to be deleted point to */
    let mut i: c_int = 0;
    while i < nitems {
        let offnum: OffsetNumber = *itemnos.add(i as usize);
        let iitemid: ItemId;

        iitemid = PageGetItemId(ipage, offnum);
        itup = PageGetItem(ipage, iitemid) as IndexTuple;

        Assert!(ItemIdIsDead(iitemid));

        ItemPointerCopy(&mut (*itup).t_tid, &mut (*delstate.deltids.add(i as usize)).tid);
        (*delstate.deltids.add(i as usize)).id = delstate.ndeltids;
        (*delstate.status.add(i as usize)).idxoffnum = offnum;
        (*delstate.status.add(i as usize)).knowndeletable = true; /* LP_DEAD-marked */
        (*delstate.status.add(i as usize)).promising = false; /* unused */
        (*delstate.status.add(i as usize)).freespace = 0; /* unused */

        delstate.ndeltids += 1;
        i += 1;
    }

    /* determine the actual xid horizon */
    snapshotConflictHorizon = table_index_delete_tuples(hrel, &mut delstate);

    /* assert tableam agrees that all items are deletable */
    Assert!(delstate.ndeltids == nitems);

    pfree(delstate.deltids as *mut c_void);
    pfree(delstate.status as *mut c_void);

    snapshotConflictHorizon
}

/* ----------------------------------------------------------------
 *      heap-or-index-scan access to system catalogs
 *
 *      These functions support system catalog accesses that normally use
 *      an index but need to be capable of being switched to heap scans
 *      if the system indexes are unavailable.
 * ----------------------------------------------------------------
 */

/*
 * systable_beginscan --- set up for heap-or-index scan
 *
 *  rel: catalog to scan, already opened and suitably locked
 *  indexId: OID of index to conditionally use
 *  indexOK: if false, forces a heap scan (see notes below)
 *  snapshot: time qual to use (NULL for a recent catalog snapshot)
 *  nkeys, key: scan keys
 *
 * The attribute numbers in the scan key should be set for the heap case.
 * If we choose to index, we convert them to 1..n to reference the index
 * columns.  Note this means there must be one scankey qualification per
 * index column!  This is checked by the Asserts in the normal, index-using
 * case, but won't be checked if the heapscan path is taken.
 *
 * The routine checks the normal cases for whether an indexscan is safe,
 * but caller can make additional checks and pass indexOK=false if needed.
 * In standard case indexOK can simply be constant TRUE.
 */
pub unsafe fn systable_beginscan(
    heapRelation: Relation,
    indexId: Oid,
    indexOK: bool,
    mut snapshot: Snapshot,
    nkeys: c_int,
    key: ScanKey,
) -> SysScanDesc {
    let sysscan: SysScanDesc;
    let irel: Relation;

    if indexOK && !IgnoreSystemIndexes && !ReindexIsProcessingIndex(indexId) {
        irel = index_open(indexId, AccessShareLock);
    } else {
        irel = std::ptr::null_mut();
    }

    sysscan = palloc(std::mem::size_of::<SysScanDescData>()) as SysScanDesc;

    (*sysscan).heap_rel = heapRelation;
    (*sysscan).irel = irel;
    (*sysscan).slot = table_slot_create(heapRelation, std::ptr::null_mut());

    if snapshot.is_null() {
        let relid: Oid = RelationGetRelid(heapRelation);

        snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));
        (*sysscan).snapshot = snapshot;
    } else {
        /* Caller is responsible for any snapshot. */
        (*sysscan).snapshot = std::ptr::null_mut();
    }

    if !irel.is_null() {
        let mut i: c_int;
        let idxkey: ScanKey;

        idxkey = palloc(std::mem::size_of::<ScanKeyData>() * nkeys as usize) as ScanKey;

        /* Convert attribute numbers to be index column numbers. */
        i = 0;
        while i < nkeys {

            std::ptr::copy_nonoverlapping(
                key.add(i as usize),
                idxkey.add(i as usize),
                1,
            );

            // TODO(pg-port): pg_index.indkey (int2vector, CATALOG_VARLEN) is omitted
            // from the ported FormData_pg_index, so the heap-attno -> index-column
            // remap below is unavailable. Leave the copied (heap) sk_attno in place;
            // wire the real remap once int2vector catalog access is ported.
            i += 1;
        }

        (*sysscan).iscan = index_beginscan(
            heapRelation,
            irel,
            snapshot,
            std::ptr::null_mut(),
            nkeys,
            0,
        );
        index_rescan((*sysscan).iscan, idxkey, nkeys, std::ptr::null_mut(), 0);
        (*sysscan).scan = std::ptr::null_mut();

        pfree(idxkey as *mut c_void);
    } else {
        /*
         * We disallow synchronized scans when forced to use a heapscan on a
         * catalog.  In most cases the desired rows are near the front, so
         * that the unpredictable start point of a syncscan is a serious
         * disadvantage; and there are no compensating advantages, because
         * it's unlikely that such scans will occur in parallel.
         */
        (*sysscan).scan =
            table_beginscan_strat(heapRelation, snapshot, nkeys, key, true, false);
        (*sysscan).iscan = std::ptr::null_mut();
    }

    /*
     * If CheckXidAlive is set then set a flag to indicate that system table
     * scan is in-progress.  See detailed comments in xact.c where these
     * variables are declared.
     */
    if TransactionIdIsValid(CheckXidAlive) {
        bsysscan = true;
    }

    sysscan
}

/*
 * HandleConcurrentAbort - Handle concurrent abort of the CheckXidAlive.
 *
 * Error out, if CheckXidAlive is aborted. We can't directly use
 * TransactionIdDidAbort as after crash such transaction might not have been
 * marked as aborted.  See detailed comments in xact.c where the variable
 * is declared.
 */
#[inline]
unsafe fn HandleConcurrentAbort() {
    if TransactionIdIsValid(CheckXidAlive)
        && !TransactionIdIsInProgress(CheckXidAlive)
        && !TransactionIdDidCommit(CheckXidAlive)
    {
        ereport!(ERROR, "transaction aborted during system catalog scan");
    }
}

/*
 * systable_getnext --- get next tuple in a heap-or-index scan
 *
 * Returns NULL if no more tuples available.
 *
 * Note that returned tuple is a reference to data in a disk buffer;
 * it must not be modified, and should be presumed inaccessible after
 * next getnext() or endscan() call.
 *
 * XXX: It'd probably make sense to offer a slot based interface, at least
 * optionally.
 */
pub unsafe fn systable_getnext(sysscan: SysScanDesc) -> HeapTuple {
    let mut htup: HeapTuple = std::ptr::null_mut();

    if !(*sysscan).irel.is_null() {
        if index_getnext_slot((*sysscan).iscan, ForwardScanDirection, (*sysscan).slot) {
            let mut shouldFree: bool = false;

            htup = ExecFetchSlotHeapTuple((*sysscan).slot, false, &mut shouldFree);
            Assert!(!shouldFree);

            /*
             * We currently don't need to support lossy index operators for
             * any system catalog scan.  It could be done here, using the scan
             * keys to drive the operator calls, if we arranged to save the
             * heap attnums during systable_beginscan(); this is practical
             * because we still wouldn't need to support indexes on
             * expressions.
             */
            if (*(*sysscan).iscan).xs_recheck {
                elog!(
                    ERROR,
                    "system catalog scans with lossy index conditions are not implemented"
                );
            }
        }
    } else {
        if table_scan_getnextslot((*sysscan).scan, ForwardScanDirection, (*sysscan).slot) {
            let mut shouldFree: bool = false;

            htup = ExecFetchSlotHeapTuple((*sysscan).slot, false, &mut shouldFree);
            Assert!(!shouldFree);
        }
    }

    /*
     * Handle the concurrent abort while fetching the catalog tuple during
     * logical streaming of a transaction.
     */
    HandleConcurrentAbort();

    htup
}

/*
 * systable_recheck_tuple --- recheck visibility of most-recently-fetched tuple
 *
 * In particular, determine if this tuple would be visible to a catalog scan
 * that started now.  We don't handle the case of a non-MVCC scan snapshot,
 * because no caller needs that yet.
 *
 * This is useful to test whether an object was deleted while we waited to
 * acquire lock on it.
 *
 * Note: we don't actually *need* the tuple to be passed in, but it's a
 * good crosscheck that the caller is interested in the right tuple.
 */
pub unsafe fn systable_recheck_tuple(sysscan: SysScanDesc, tup: HeapTuple) -> bool {
    let mut freshsnap: Snapshot;
    let result: bool;

    Assert!(tup == ExecFetchSlotHeapTuple((*sysscan).slot, false, std::ptr::null_mut()));

    freshsnap = GetCatalogSnapshot(RelationGetRelid((*sysscan).heap_rel));
    freshsnap = RegisterSnapshot(freshsnap);

    result = table_tuple_satisfies_snapshot((*sysscan).heap_rel, (*sysscan).slot, freshsnap);
    UnregisterSnapshot(freshsnap);

    /*
     * Handle the concurrent abort while fetching the catalog tuple during
     * logical streaming of a transaction.
     */
    HandleConcurrentAbort();

    result
}

/*
 * systable_endscan --- close scan, release resources
 *
 * Note that it's still up to the caller to close the heap relation.
 */
pub unsafe fn systable_endscan(sysscan: SysScanDesc) {
    if !(*sysscan).slot.is_null() {
        ExecDropSingleTupleTableSlot((*sysscan).slot);
        (*sysscan).slot = std::ptr::null_mut();
    }

    if !(*sysscan).irel.is_null() {
        index_endscan((*sysscan).iscan);
        index_close((*sysscan).irel, AccessShareLock);
    } else {
        table_endscan((*sysscan).scan);
    }

    if !(*sysscan).snapshot.is_null() {
        UnregisterSnapshot((*sysscan).snapshot);
    }

    /*
     * Reset the bsysscan flag at the end of the systable scan.  See detailed
     * comments in xact.c where these variables are declared.
     */
    if TransactionIdIsValid(CheckXidAlive) {
        bsysscan = false;
    }

    pfree(sysscan as *mut c_void);
}

/*
 * systable_beginscan_ordered --- set up for ordered catalog scan
 *
 * These routines have essentially the same API as systable_beginscan etc,
 * except that they guarantee to return multiple matching tuples in
 * index order.  Also, for largely historical reasons, the index to use
 * is opened and locked by the caller, not here.
 *
 * Currently we do not support non-index-based scans here.  (In principle
 * we could do a heapscan and sort, but the uses are in places that
 * probably don't need to still work with corrupted catalog indexes.)
 * For the moment, therefore, these functions are merely the thinest of
 * wrappers around index_beginscan/index_getnext_slot.  The main reason for
 * their existence is to centralize possible future support of lossy operators
 * in catalog scans.
 */
pub unsafe fn systable_beginscan_ordered(
    heapRelation: Relation,
    indexRelation: Relation,
    mut snapshot: Snapshot,
    nkeys: c_int,
    key: ScanKey,
) -> SysScanDesc {
    let sysscan: SysScanDesc;
    let mut i: c_int;
    let idxkey: ScanKey;

    /* REINDEX can probably be a hard error here ... */
    if ReindexIsProcessingIndex(RelationGetRelid(indexRelation)) {
        elog!(
            ERROR,
            "cannot access index \"{}\" while it is being reindexed",
            std::ffi::CStr::from_ptr(RelationGetRelationName(indexRelation))
                .to_string_lossy()
        );
    }
    /* ... but we only throw a warning about violating IgnoreSystemIndexes */
    if IgnoreSystemIndexes {
        elog!(
            WARNING,
            "using index \"{}\" despite IgnoreSystemIndexes",
            std::ffi::CStr::from_ptr(RelationGetRelationName(indexRelation))
                .to_string_lossy()
        );
    }

    sysscan = palloc(std::mem::size_of::<SysScanDescData>()) as SysScanDesc;

    (*sysscan).heap_rel = heapRelation;
    (*sysscan).irel = indexRelation;
    (*sysscan).slot = table_slot_create(heapRelation, std::ptr::null_mut());

    if snapshot.is_null() {
        let relid: Oid = RelationGetRelid(heapRelation);

        snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));
        (*sysscan).snapshot = snapshot;
    } else {
        /* Caller is responsible for any snapshot. */
        (*sysscan).snapshot = std::ptr::null_mut();
    }

    idxkey = palloc(std::mem::size_of::<ScanKeyData>() * nkeys as usize) as ScanKey;

    /* Convert attribute numbers to be index column numbers. */
    i = 0;
    while i < nkeys {

        std::ptr::copy_nonoverlapping(key.add(i as usize), idxkey.add(i as usize), 1);

        // TODO(pg-port): pg_index.indkey (int2vector, CATALOG_VARLEN) is omitted
        // from the ported FormData_pg_index, so the heap-attno -> index-column
        // remap below is unavailable. Leave the copied (heap) sk_attno in place;
        // wire the real remap once int2vector catalog access is ported.
        i += 1;
    }

    (*sysscan).iscan = index_beginscan(
        heapRelation,
        indexRelation,
        snapshot,
        std::ptr::null_mut(),
        nkeys,
        0,
    );
    index_rescan((*sysscan).iscan, idxkey, nkeys, std::ptr::null_mut(), 0);
    (*sysscan).scan = std::ptr::null_mut();

    pfree(idxkey as *mut c_void);

    /*
     * If CheckXidAlive is set then set a flag to indicate that system table
     * scan is in-progress.  See detailed comments in xact.c where these
     * variables are declared.
     */
    if TransactionIdIsValid(CheckXidAlive) {
        bsysscan = true;
    }

    sysscan
}

/*
 * systable_getnext_ordered --- get next tuple in an ordered catalog scan
 */
pub unsafe fn systable_getnext_ordered(
    sysscan: SysScanDesc,
    direction: ScanDirection,
) -> HeapTuple {
    let mut htup: HeapTuple = std::ptr::null_mut();

    Assert!(!(*sysscan).irel.is_null());
    if index_getnext_slot((*sysscan).iscan, direction, (*sysscan).slot) {
        htup = ExecFetchSlotHeapTuple((*sysscan).slot, false, std::ptr::null_mut());
    }

    /* See notes in systable_getnext */
    if !htup.is_null() && (*(*sysscan).iscan).xs_recheck {
        elog!(
            ERROR,
            "system catalog scans with lossy index conditions are not implemented"
        );
    }

    /*
     * Handle the concurrent abort while fetching the catalog tuple during
     * logical streaming of a transaction.
     */
    HandleConcurrentAbort();

    htup
}

/*
 * systable_endscan_ordered --- close scan, release resources
 */
pub unsafe fn systable_endscan_ordered(sysscan: SysScanDesc) {
    if !(*sysscan).slot.is_null() {
        ExecDropSingleTupleTableSlot((*sysscan).slot);
        (*sysscan).slot = std::ptr::null_mut();
    }

    Assert!(!(*sysscan).irel.is_null());
    index_endscan((*sysscan).iscan);
    if !(*sysscan).snapshot.is_null() {
        UnregisterSnapshot((*sysscan).snapshot);
    }

    /*
     * Reset the bsysscan flag at the end of the systable scan.  See detailed
     * comments in xact.c where these variables are declared.
     */
    if TransactionIdIsValid(CheckXidAlive) {
        bsysscan = false;
    }

    pfree(sysscan as *mut c_void);
}

/*
 * systable_inplace_update_begin --- update a row "in place" (overwrite it)
 *
 * Overwriting violates both MVCC and transactional safety, so the uses of
 * this function in Postgres are extremely limited.  Nonetheless we find some
 * places to use it.  See README.tuplock section "Locking to write
 * inplace-updated tables" and later sections for expectations of readers and
 * writers of a table that gets inplace updates.  Standard flow:
 *
 * ... [any slow preparation not requiring oldtup] ...
 * systable_inplace_update_begin([...], &tup, &inplace_state);
 * if (!HeapTupleIsValid(tup))
 *  elog(ERROR, [...]);
 * ... [buffer is exclusive-locked; mutate "tup"] ...
 * if (dirty)
 *  systable_inplace_update_finish(inplace_state, tup);
 * else
 *  systable_inplace_update_cancel(inplace_state);
 *
 * The first several params duplicate the systable_beginscan() param list.
 * "oldtupcopy" is an output parameter, assigned NULL if the key ceases to
 * find a live tuple.  (In PROC_IN_VACUUM, that is a low-probability transient
 * condition.)  If "oldtupcopy" gets non-NULL, you must pass output parameter
 * "state" to systable_inplace_update_finish() or
 * systable_inplace_update_cancel().
 */
pub unsafe fn systable_inplace_update_begin(
    relation: Relation,
    indexId: Oid,
    indexOK: bool,
    snapshot: Snapshot,
    nkeys: c_int,
    key: *const ScanKeyData,
    oldtupcopy: *mut HeapTuple,
    state: *mut *mut c_void,
) {
    let mut retries: c_int = 0;
    let mut scan: SysScanDesc;
    let mut oldtup: HeapTuple;
    let mut bslot: *mut BufferHeapTupleTableSlot;

    /*
     * For now, we don't allow parallel updates.  Unlike a regular update,
     * this should never create a combo CID, so it might be possible to relax
     * this restriction, but not without more thought and testing.  It's not
     * clear that it would be useful, anyway.
     */
    if IsInParallelMode() {
        ereport!(ERROR, "cannot update tuples during a parallel operation");
    }

    /*
     * Accept a snapshot argument, for symmetry, but this function advances
     * its snapshot as needed to reach the tail of the updated tuple chain.
     */
    Assert!(snapshot.is_null());

    Assert!(IsInplaceUpdateRelation(relation) || !IsSystemRelation(relation));

    /* Loop for an exclusive-locked buffer of a non-updated tuple. */
    loop {
        let slot: *mut TupleTableSlot;

        CHECK_FOR_INTERRUPTS!();

        /*
         * Processes issuing heap_update (e.g. GRANT) at maximum speed could
         * drive us to this error.  A hostile table owner has stronger ways to
         * damage their own table, so that's minor.
         */
        retries += 1;
        if retries > 10000 {
            elog!(ERROR, "giving up after too many tries to overwrite row");
        }

        INJECTION_POINT!("inplace-before-pin", std::ptr::null_mut::<core::ffi::c_void>());
        scan = systable_beginscan(
            relation,
            indexId,
            indexOK,
            snapshot,
            nkeys,
            key as *mut ScanKeyData,
        );
        oldtup = systable_getnext(scan);
        if !HeapTupleIsValid(oldtup) {
            systable_endscan(scan);
            *oldtupcopy = std::ptr::null_mut();
            return;
        }

        slot = (*scan).slot;
        Assert!(TTS_IS_BUFFERTUPLE(slot));
        bslot = slot as *mut BufferHeapTupleTableSlot;

        if heap_inplace_lock(
            (*scan).heap_rel,
            (*bslot).base.tuple,
            (*bslot).buffer,
            systable_endscan_callback,
            scan as *mut c_void,
        ) {
            break;
        }
    }

    *oldtupcopy = heap_copytuple(oldtup);
    *state = scan as *mut c_void;
}

/* Trampoline so the address-of systable_endscan can be passed as a C callback */
unsafe extern "C" fn systable_endscan_callback(arg: *mut c_void) {
    systable_endscan(arg as SysScanDesc);
}

/*
 * systable_inplace_update_finish --- second phase of inplace update
 *
 * The tuple cannot change size, and therefore its header fields and null
 * bitmap (if any) don't change either.
 */
pub unsafe fn systable_inplace_update_finish(state: *mut c_void, tuple: HeapTuple) {
    let scan: SysScanDesc = state as SysScanDesc;
    let relation: Relation = (*scan).heap_rel;
    let slot: *mut TupleTableSlot = (*scan).slot;
    let bslot: *mut BufferHeapTupleTableSlot = slot as *mut BufferHeapTupleTableSlot;
    let oldtup: HeapTuple = (*bslot).base.tuple;
    let buffer: Buffer = (*bslot).buffer;

    heap_inplace_update_and_unlock(relation, oldtup, tuple, buffer);
    systable_endscan(scan);
}

/*
 * systable_inplace_update_cancel --- abandon inplace update
 *
 * This is an alternative to making a no-op update.
 */
pub unsafe fn systable_inplace_update_cancel(state: *mut c_void) {
    let scan: SysScanDesc = state as SysScanDesc;
    let relation: Relation = (*scan).heap_rel;
    let slot: *mut TupleTableSlot = (*scan).slot;
    let bslot: *mut BufferHeapTupleTableSlot = slot as *mut BufferHeapTupleTableSlot;
    let oldtup: HeapTuple = (*bslot).base.tuple;
    let buffer: Buffer = (*bslot).buffer;

    heap_inplace_unlock(relation, oldtup, buffer);
    systable_endscan(scan);
}

/* ===================== local stubs for unported dependencies ===================== */

// Types referenced via raw pointers / values from unported modules.
use crate::utils::rel::Relation;
pub type Snapshot = *mut c_void;
pub type ScanKey = *mut ScanKeyData;
use crate::access::common::scankey::ScanKeyData;
pub type Form_pg_index = *mut FormData_pg_index;
pub type AclResult = c_int;
pub type StringInfoData = StringInfoDataStub;
pub type IndexTuple = *mut IndexTupleData_stub;
pub type HeapTuple = *mut c_void;
pub type TupleDesc = *mut c_void;
pub type TupleTableSlot = c_void;
pub type TableScanDesc = *mut c_void;
pub type BufferAccessStrategy = *mut c_void;
pub type Buffer = c_int;
pub type Page = *mut c_void;
pub type ItemId = *mut c_void;
pub type OffsetNumber = u16;
pub type ItemPointer = *mut ItemPointerData_stub;
pub type ItemPointerData = ItemPointerData_stub;
pub type ScanDirection = c_int;
// RegProcedure is provided by crate::c (re-exported via prelude glob); the local
// alias is unused here, so it is omitted to avoid shadowing the crate definition.

#[repr(C)]
pub struct StringInfoDataStub {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursorpos: c_int,
}

#[repr(C)]
pub struct FormData_pg_index {
    pub indexrelid: Oid,
    pub indrelid: Oid,
    pub indkey: int2vector_stub,
}

#[repr(C)]
pub struct int2vector_stub {
    pub values: [AttrNumber; FLEXIBLE_ARRAY_MEMBER],
}

#[repr(C)]
pub struct TM_IndexDeleteOp {
    pub irel: Relation,
    pub iblknum: BlockNumber,
    pub bottomup: bool,
    pub bottomupfreespace: c_int,
    pub ndeltids: c_int,
    pub deltids: *mut TM_IndexDelete,
    pub status: *mut TM_IndexStatus,
}

#[repr(C)]
pub struct TM_IndexDelete {
    pub tid: ItemPointerData_stub,
    pub id: c_int,
}

#[repr(C)]
pub struct TM_IndexStatus {
    pub idxoffnum: OffsetNumber,
    pub knowndeletable: bool,
    pub promising: bool,
    pub freespace: i16,
}

#[repr(C)]
pub struct ItemPointerData_stub {
    _private: [u8; 6],
}

#[repr(C)]
pub struct IndexTupleData_stub {
    pub t_tid: ItemPointerData_stub,
}

#[repr(C)]
pub struct BufferHeapTupleTableSlot {
    pub base: HeapTupleTableSlot_stub,
    pub buffer: Buffer,
}

#[repr(C)]
pub struct HeapTupleTableSlot_stub {
    pub tuple: HeapTuple,
}

/* Constants from unported modules */
const InvalidSnapshot: Snapshot = std::ptr::null_mut();
const InvalidOid: Oid = 0;
const InvalidAttrNumber: AttrNumber = 0;
const AccessShareLock: c_int = 1;
const ACL_SELECT: u64 = 1 << 1;
const ACLCHECK_OK: AclResult = 0;
const RLS_ENABLED: c_int = 2;
const ForwardScanDirection: ScanDirection = 1;
const ERROR: c_int = 21;
const WARNING: c_int = 19;

/* extern globals (defined elsewhere) */
extern "C" {
    static mut bsysscan: bool;
    static mut CheckXidAlive: TransactionId;
    static mut IgnoreSystemIndexes: bool;
}

/* helper-fn stubs */
unsafe fn TransactionStartedDuringRecovery() -> bool {
    unimplemented!() // TODO: access/transam/xact.c
}
unsafe fn RelationGetRelid(_relation: Relation) -> Oid {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn RelationGetRelationName(_relation: Relation) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn IndexRelationGetNumberOfKeyAttributes(_relation: Relation) -> c_int {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn IndexRelationGetNumberOfAttributes(_relation: Relation) -> c_int {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn check_enable_rls(_relid: Oid, _checkAsUser: Oid, _noError: bool) -> c_int {
    unimplemented!() // TODO: utils/rls.c
}
unsafe fn pg_class_aclcheck(_table_oid: Oid, _roleid: Oid, _mode: u64) -> AclResult {
    unimplemented!() // TODO: utils/adt/acl.c
}
unsafe fn pg_attribute_aclcheck(
    _table_oid: Oid,
    _attnum: AttrNumber,
    _roleid: Oid,
    _mode: u64,
) -> AclResult {
    unimplemented!() // TODO: utils/adt/acl.c
}
unsafe fn GetUserId() -> Oid {
    unimplemented!() // TODO: utils/init/miscinit.c
}
unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn appendStringInfo(_str: *mut StringInfoData, _fmt: *const c_char, _arg: *mut c_char) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn appendStringInfoString(_str: *mut StringInfoData, _s: *const c_char) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn appendStringInfoChar(_str: *mut StringInfoData, _ch: c_char) {
    unimplemented!() // TODO: lib/stringinfo.c
}
unsafe fn pg_get_indexdef_columns(_indexrelid: Oid, _pretty: bool) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/ruleutils.c
}
unsafe fn getTypeOutputInfo(_type: Oid, _typOutput: *mut Oid, _typIsVarlena: *mut bool) {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn OidOutputFunctionCall(_functionId: Oid, _val: Datum) -> *mut c_char {
    unimplemented!() // TODO: utils/fmgr/fmgr.c
}
unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}
unsafe fn PageGetItemId(_page: Page, _offsetNumber: OffsetNumber) -> ItemId {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn PageGetItem(_page: Page, _itemId: ItemId) -> *mut c_void {
    unimplemented!() // TODO: storage/bufpage.h
}
unsafe fn ItemIdIsDead(_itemId: ItemId) -> bool {
    unimplemented!() // TODO: storage/itemid.h
}
unsafe fn ItemPointerCopy(_src: ItemPointer, _dst: ItemPointer) {
    unimplemented!() // TODO: storage/itemptr.h
}
unsafe fn table_index_delete_tuples(_rel: Relation, _delstate: *mut TM_IndexDeleteOp) -> TransactionId {
    unimplemented!() // TODO: access/table/tableamapi.c
}
pub use crate::catalog::index::ReindexIsProcessingIndex;
unsafe fn index_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/index/indexam.c
}
unsafe fn index_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/index/indexam.c
}
unsafe fn index_beginscan(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: Snapshot,
    _instrument: *mut IndexScanInstrumentation,
    _nkeys: c_int,
    _norderbys: c_int,
) -> IndexScanDesc {
    unimplemented!() // TODO: access/index/indexam.c
}
unsafe fn index_rescan(
    _scan: IndexScanDesc,
    _keys: ScanKey,
    _nkeys: c_int,
    _orderbys: ScanKey,
    _norderbys: c_int,
) {
    unimplemented!() // TODO: access/index/indexam.c
}
unsafe fn index_endscan(_scan: IndexScanDesc) {
    unimplemented!() // TODO: access/index/indexam.c
}
unsafe fn index_getnext_slot(
    _scan: IndexScanDesc,
    _direction: ScanDirection,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: access/index/indexam.c
}
unsafe fn table_slot_create(_relation: Relation, _reglist: *mut c_void) -> *mut TupleTableSlot {
    unimplemented!() // TODO: access/table/tableam.c
}
unsafe fn table_beginscan_strat(
    _relation: Relation,
    _snapshot: Snapshot,
    _nkeys: c_int,
    _key: ScanKey,
    _allow_strat: bool,
    _allow_sync: bool,
) -> TableScanDesc {
    unimplemented!() // TODO: access/table/tableam.h
}
unsafe fn table_endscan(_scan: TableScanDesc) {
    unimplemented!() // TODO: access/table/tableam.h
}
unsafe fn table_scan_getnextslot(
    _sscan: TableScanDesc,
    _direction: ScanDirection,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: access/table/tableam.h
}
unsafe fn table_tuple_satisfies_snapshot(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _snapshot: Snapshot,
) -> bool {
    unimplemented!() // TODO: access/table/tableam.h
}
unsafe fn RegisterSnapshot(_snapshot: Snapshot) -> Snapshot {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn UnregisterSnapshot(_snapshot: Snapshot) {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn GetCatalogSnapshot(_relid: Oid) -> Snapshot {
    unimplemented!() // TODO: utils/time/snapmgr.c
}
unsafe fn ExecFetchSlotHeapTuple(
    _slot: *mut TupleTableSlot,
    _materialize: bool,
    _shouldFree: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn TransactionIdIsValid(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: access/transam.h
}
unsafe fn TransactionIdIsInProgress(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: storage/ipc/procarray.c
}
unsafe fn TransactionIdDidCommit(_xid: TransactionId) -> bool {
    unimplemented!() // TODO: access/transam/transam.c
}
unsafe fn IsInParallelMode() -> bool {
    unimplemented!() // TODO: access/transam/xact.c
}
unsafe fn IsInplaceUpdateRelation(_relation: Relation) -> bool {
    unimplemented!() // TODO: catalog/catalog.c
}
unsafe fn IsSystemRelation(_relation: Relation) -> bool {
    unimplemented!() // TODO: catalog/catalog.c
}
unsafe fn HeapTupleIsValid(_tuple: HeapTuple) -> bool {
    unimplemented!() // TODO: access/htup.h
}
unsafe fn TTS_IS_BUFFERTUPLE(_slot: *mut TupleTableSlot) -> bool {
    unimplemented!() // TODO: executor/tuptable.h
}
unsafe fn heap_inplace_lock(
    _relation: Relation,
    _oldtup_ptr: HeapTuple,
    _buffer: Buffer,
    _release_callback: unsafe extern "C" fn(*mut c_void),
    _arg: *mut c_void,
) -> bool {
    unimplemented!() // TODO: access/heap/heapam.c
}
unsafe fn heap_inplace_update_and_unlock(
    _relation: Relation,
    _oldtup: HeapTuple,
    _tuple: HeapTuple,
    _buffer: Buffer,
) {
    unimplemented!() // TODO: access/heap/heapam.c
}
unsafe fn heap_inplace_unlock(_relation: Relation, _oldtup: HeapTuple, _buffer: Buffer) {
    unimplemented!() // TODO: access/heap/heapam.c
}
unsafe fn heap_copytuple(_tuple: HeapTuple) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn PointerIsValid<T>(_ptr: *const T) -> bool {
    unimplemented!() // TODO: c.h
}

/* Macro stubs */
#[allow(unused_macros)]
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{
        // TODO: miscadmin.h
    }};
}
use CHECK_FOR_INTERRUPTS;

#[allow(unused_macros)]
macro_rules! INJECTION_POINT {
    ($name:expr, $arg:expr) => {{
        // TODO: utils/injection_point.h
        let _ = ($name, $arg);
    }};
}
use INJECTION_POINT;

// Assert is provided by crate::prelude (crate-root #[macro_export] macro);
// do not redefine it here.
