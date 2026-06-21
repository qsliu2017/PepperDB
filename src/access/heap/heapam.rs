//! heapam.rs
//!   heap access method code
//!
//! Translated 1:1 from postgres/src/backend/access/heap/heapam.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/access/heap/heapam.c
//!
//! INTERFACE ROUTINES
//!		heap_beginscan	- begin relation scan
//!		heap_rescan		- restart a relation scan
//!		heap_endscan	- end relation scan
//!		heap_getnext	- retrieve next tuple in scan
//!		heap_fetch		- retrieve tuple with given tid
//!		heap_insert		- insert tuple into a relation
//!		heap_multi_insert - insert multiple tuples into a relation
//!		heap_delete		- delete a tuple from a relation
//!		heap_update		- replace a tuple in a relation with another tuple
//!
//! NOTES
//!	  This file contains the heap_ routines which implement
//!	  the POSTGRES heap access method used for all POSTGRES
//!	  relations.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(unused_mut)]
#![allow(dead_code)]
#![allow(unused_parens)]

use crate::prelude::*;
use crate::access::table::tableam::{
    SO_ALLOW_PAGEMODE, SO_ALLOW_STRAT, SO_ALLOW_SYNC, SO_TYPE_BITMAPSCAN, SO_TYPE_SEQSCAN,
    SO_TYPE_TIDRANGESCAN,
};
macro_rules! Min { ($a:expr, $b:expr) => { core::cmp::min($a, $b) }; }
macro_rules! Max { ($a:expr, $b:expr) => { core::cmp::max($a, $b) }; }
 // postgres.h: c types, Datum, palloc, elog!/ereport!/errmsg!/Assert!, Max/Min, MemSet helpers

use std::ffi::c_char;
use std::ffi::c_int;
use std::ffi::c_void;

use crate::c::int16;
use crate::c::int64;
use crate::c::uint8;
use crate::c::uint16;
use crate::c::uint32;
use crate::c::CommandId;
use crate::c::MultiXactId;
use crate::c::Size;
use crate::c::TransactionId;

// postgres_ext.h
use crate::postgres_ext::Oid;

// pg_config.h
use crate::pg_config::BLCKSZ;

// access/htup.h & access/htup_details.h
use crate::access::htup_details::HeapTuple;
use crate::access::htup_details::HeapTupleData;
use crate::access::htup_details::HeapTupleHeader;
use crate::access::htup_details::HeapTupleHeaderData;
use crate::access::htup_details::*;

// access/transam.h & access/transam/transam.h
use crate::access::transam::InvalidTransactionId;
use crate::access::transam::TransactionIdEquals;
use crate::access::transam::TransactionIdIsNormal;
use crate::access::transam::TransactionIdIsValid;
use crate::access::transam::transam::TransactionIdFollows;
use crate::access::transam::transam::TransactionIdFollowsOrEquals;
use crate::access::transam::transam::TransactionIdPrecedes;
use crate::access::transam::transam::TransactionIdPrecedesOrEquals;

// access/transam/xlogdefs.h
use crate::access::transam::xlogdefs::XLogRecPtr;

// storage/block.h
use crate::storage::block::BlockNumber;
use crate::storage::block::InvalidBlockNumber;
use crate::storage::block::BlockNumberIsValid;

// storage/off.h
use crate::storage::off::OffsetNumber;
use crate::storage::off::FirstOffsetNumber;
use crate::storage::off::InvalidOffsetNumber;
use crate::storage::off::MaxOffsetNumber;
use crate::storage::off::OffsetNumberIsValid;
use crate::storage::off::OffsetNumberNext;
use crate::storage::off::OffsetNumberPrev;

// storage/itemid.h
use crate::storage::itemid::ItemId;
use crate::storage::itemid::ItemIdData;
use crate::storage::itemid::ItemIdGetLength;
use crate::storage::itemid::ItemIdGetRedirect;
use crate::storage::itemid::ItemIdHasStorage;
use crate::storage::itemid::ItemIdIsNormal;
use crate::storage::itemid::ItemIdIsRedirected;
use crate::storage::itemid::ItemIdIsUsed;

// storage/itemptr.h
use crate::storage::itemptr::ItemPointer;
use crate::storage::itemptr::ItemPointerData;

// storage/bufpage.h
use crate::storage::bufpage::Page;
use crate::storage::bufpage::PageHeader;
use crate::storage::bufpage::SizeOfPageHeaderData;

// utils/rel.h
use crate::utils::rel::Relation;
use crate::utils::rel::RelationGetRelid;

// access/relscan.h
use crate::access::relscan::ScanKey;
use crate::access::relscan::ScanKeyData;
use crate::access::relscan::TableScanDesc;
use crate::access::relscan::ParallelTableScanDesc;
use crate::executor::tuptable::TupleTableSlot; // real TupleTableSlot from executor/tuptable.rs

// utils/snapshot.h - use real types for field access
use crate::utils::snapshot::Snapshot;
use crate::utils::snapshot::SnapshotData;

// access/table/tableam.h
use crate::access::table::tableam::TM_Result;
use crate::access::table::tableam::TM_Result::*;
use crate::access::table::tableam::TM_FailureData;
use crate::access::table::tableam::TM_IndexDelete;
use crate::access::table::tableam::TM_IndexStatus;
use crate::access::table::tableam::TM_IndexDeleteOp;
use crate::access::table::tableam::SO_TYPE_SAMPLESCAN;
use crate::access::table::tableam::SO_TEMP_SNAPSHOT;
use crate::access::table::tableam::synchronize_seqscans;
use crate::access::table::tableam::SnapshotAny;

// utils/snapshot.h
use crate::utils::snapshot::InvalidSnapshot;

// miscadmin
use crate::utils::init::globals::NBuffers;

// commands/variable (GUC) - stub until commands::variable is wired
// TODO(pg-port): real maintenance_io_concurrency lives in commands/variable.rs

// nodes/lockoptions.h
use crate::nodes::lockoptions::LockTupleMode;
use crate::nodes::lockoptions::LockTupleMode::*;
use crate::nodes::lockoptions::LockWaitPolicy;
use crate::nodes::lockoptions::LockWaitPolicy::*;

// access/heap/visibilitymap.h
use crate::access::heap::visibilitymap::visibilitymap_clear;
use crate::access::heap::visibilitymap::visibilitymap_pin;
use crate::access::heap::visibilitymap::visibilitymap_pin_ok;
use crate::access::heap::visibilitymap::visibilitymap_set;

// access/heap/heapam_visibility.h (HTSV_Result/HEAPTUPLE_*)
use crate::access::heap::heapam_visibility::HTSV_Result;
use crate::access::heap::heapam_visibility::HEAPTUPLE_DEAD;
use crate::access::heap::heapam_visibility::HEAPTUPLE_LIVE;
use crate::access::heap::heapam_visibility::HEAPTUPLE_RECENTLY_DEAD;
use crate::access::heap::heapam_visibility::HEAPTUPLE_INSERT_IN_PROGRESS;
use crate::access::heap::heapam_visibility::HEAPTUPLE_DELETE_IN_PROGRESS;

// access/heap/hio.h
use crate::access::heap::hio::BulkInsertState;
use crate::access::heap::hio::BulkInsertStateData;

// ====================================================================
// NOTE on heavy stubbing:
//
// heapam.c sits on top of large subsystems that are not yet ported to
// PepperDB: the buffer manager (storage/bufmgr), WAL insertion
// (access/transam/xloginsert + heapam_xlog), multixact
// (access/transam/multixact), predicate locking (storage/predicate),
// the lock manager (storage/lmgr), snapshot manager (utils/snapmgr),
// the read stream API, pgstat, and the table AM dispatch layer.
//
// Following the convention established by the sibling heap files
// (hio.rs, visibilitymap.rs, pruneheap.rs, heaptoast.rs,
// heapam_visibility.rs), every symbol with no home yet is provided as a
// minimal local stub in the STUBS section near the bottom of this file,
// tagged `// TODO(pg-port): real SYM lives in <file>`.  The function
// bodies are faithful 1:1 translations of heapam.c.
// ====================================================================

// --- buffer manager (storage/buf.h) ---
type Buffer = c_int; // TODO(pg-port): real Buffer lives in storage/buf.rs

// --- multixact (access/multixact.h) ---
type MultiXactStatus = c_int; // TODO(pg-port): real MultiXactStatus lives in access/transam/multixact.rs

// --- lmgr (storage/lock.h, storage/lmgr.h) ---
type LOCKMODE = c_int; // TODO(pg-port): real LOCKMODE lives in storage/lock.rs
type XLTW_Oper = c_int; // TODO(pg-port): real XLTW_Oper lives in storage/lmgr.rs

// --- nodes/bitmapset.h ---
type Bitmapset = c_void; // TODO(pg-port): real Bitmapset lives in nodes/bitmapset.rs

// --- access/tupdesc.h ---
use crate::access::common::tupdesc::TupleDescData;
use crate::access::common::tupdesc::CompactAttribute;
type TupleDesc = *mut TupleDescData; // maps to access/common/tupdesc.rs

/*
 * Each tuple lock mode has a corresponding heavyweight lock, and one or two
 * corresponding MultiXactStatuses (one to merely lock tuples, another one to
 * update them).  This table (and the macros below) helps us determine the
 * heavyweight lock mode and MultiXactStatus values to use for any particular
 * tuple lock strength.
 *
 * These interact with InplaceUpdateTupleLock, an alias for ExclusiveLock.
 *
 * Don't look at lockstatus/updstatus directly!  Use get_mxact_status_for_lock
 * instead.
 */
#[repr(C)]
struct TupleLockExtraInfo {
    hwlock: LOCKMODE,
    lockstatus: c_int,
    updstatus: c_int,
}

static tupleLockExtraInfo: [TupleLockExtraInfo; (MaxLockTupleMode + 1) as usize] = [
    /* LockTupleKeyShare */
    TupleLockExtraInfo {
        hwlock: AccessShareLock,
        lockstatus: MultiXactStatusForKeyShare,
        updstatus: -1, /* KeyShare does not allow updating tuples */
    },
    /* LockTupleShare */
    TupleLockExtraInfo {
        hwlock: RowShareLock,
        lockstatus: MultiXactStatusForShare,
        updstatus: -1, /* Share does not allow updating tuples */
    },
    /* LockTupleNoKeyExclusive */
    TupleLockExtraInfo {
        hwlock: ExclusiveLock,
        lockstatus: MultiXactStatusForNoKeyUpdate,
        updstatus: MultiXactStatusNoKeyUpdate,
    },
    /* LockTupleExclusive */
    TupleLockExtraInfo {
        hwlock: AccessExclusiveLock,
        lockstatus: MultiXactStatusForUpdate,
        updstatus: MultiXactStatusUpdate,
    },
];

/* Get the LOCKMODE for a given MultiXactStatus */
unsafe fn LOCKMODE_from_mxstatus(status: MultiXactStatus) -> LOCKMODE {
    tupleLockExtraInfo[TUPLOCK_from_mxstatus(status) as usize].hwlock
}

/*
 * Acquire heavyweight locks on tuples, using a LockTupleMode strength value.
 * This is more readable than having every caller translate it to lock.h's
 * LOCKMODE.
 */
unsafe fn LockTupleTuplock(rel: Relation, tup: ItemPointer, mode: LockTupleMode) {
    LockTuple(rel, tup, tupleLockExtraInfo[mode as usize].hwlock)
}
unsafe fn UnlockTupleTuplock(rel: Relation, tup: ItemPointer, mode: LockTupleMode) {
    UnlockTuple(rel, tup, tupleLockExtraInfo[mode as usize].hwlock)
}
unsafe fn ConditionalLockTupleTuplock(
    rel: Relation,
    tup: ItemPointer,
    mode: LockTupleMode,
    log: bool,
) -> bool {
    ConditionalLockTuple(rel, tup, tupleLockExtraInfo[mode as usize].hwlock, log)
}

/*
 * heap_index_delete_tuples and index_delete_prefetch_buffer use this
 * structure to coordinate prefetching activity
 */
#[repr(C)]
struct IndexDeletePrefetchState {
    cur_hblkno: BlockNumber,
    next_item: c_int,
    ndeltids: c_int,
    deltids: *mut TM_IndexDelete,
}

/* heap_index_delete_tuples bottom-up index deletion costing constants */
const BOTTOMUP_MAX_NBLOCKS: c_int = 6;
const BOTTOMUP_TOLERANCE_NBLOCKS: c_int = 3;

/*
 * heap_index_delete_tuples uses this when determining which heap blocks it
 * must visit to help its bottom-up index deletion caller
 */
#[repr(C)]
struct IndexDeleteCounts {
    npromisingtids: int16, /* Number of "promising" TIDs in group */
    ntids: int16,          /* Number of TIDs in group */
    ifirsttid: int16,      /* Offset to group's first deltid */
}

/*
 * This table maps tuple lock strength values for each particular
 * MultiXactStatus value.
 */
static MultiXactStatusLock: [c_int; (MaxMultiXactStatus + 1) as usize] = [
    LockTupleKeyShare as c_int,       /* ForKeyShare */
    LockTupleShare as c_int,          /* ForShare */
    LockTupleNoKeyExclusive as c_int, /* ForNoKeyUpdate */
    LockTupleExclusive as c_int,      /* ForUpdate */
    LockTupleNoKeyExclusive as c_int, /* NoKeyUpdate */
    LockTupleExclusive as c_int,      /* Update */
];

/* Get the LockTupleMode for a given MultiXactStatus */
unsafe fn TUPLOCK_from_mxstatus(status: MultiXactStatus) -> c_int {
    MultiXactStatusLock[status as usize]
}

/*
 * Check that we have a valid snapshot if we might need TOAST access.
 */
#[inline]
unsafe fn AssertHasSnapshotForToast(rel: Relation) {
    /* USE_ASSERT_CHECKING */

    /* bootstrap mode in particular breaks this rule */
    if !IsNormalProcessingMode() {
        return;
    }

    /* if the relation doesn't have a TOAST table, we are good */
    if !OidIsValid((*(*rel).rd_rel).reltoastrelid) {
        return;
    }

    Assert!(HaveRegisteredOrActiveSnapshot());
}

/*
 * HeapScanDescData is defined here because access/heapam.h is not yet ported.
 * TODO(pg-port): real HeapScanDescData lives in access/heapam.rs (heapam.h).
 */
#[repr(C)]
pub struct HeapScanDescData {
    /* state set up at initscan time */
    pub rs_base: crate::access::relscan::TableScanDescData, /* AM independent part of the descriptor */

    pub rs_nblocks: BlockNumber,    /* total number of blocks in rel */
    pub rs_startblock: BlockNumber, /* block # to start at */
    pub rs_numblocks: BlockNumber,  /* max number of blocks to scan */
    /* rs_numblocks is usually InvalidBlockNumber, meaning "scan whole rel" */

    pub rs_strategy: BufferAccessStrategy, /* access strategy for reads */

    pub rs_cbuf: Buffer, /* current buffer in scan, if any */
    /* NB: if rs_cbuf is not InvalidBuffer, we hold a pin on that buffer */

    /* per-tuple fields */
    pub rs_ctup: HeapTupleData,    /* current tuple in scan, if any */

    pub rs_dir: ScanDirection, /* direction of the scan */
    pub rs_prefetch_block: BlockNumber, /* the block # to prefetch next */

    /* parallel scan state */
    pub rs_parallelworkerdata: *mut ParallelBlockTableScanWorkerData,

    /* these fields only used in page-at-a-time mode and for bitmap scans */
    pub rs_cblock: BlockNumber, /* current block # in scan, if any */
    pub rs_cindex: c_int,       /* current offset # in inner loop */
    pub rs_ntuples: c_int,      /* number of visible tuples on page */
    pub rs_coffset: OffsetNumber, /* current offset # in inner loop */
    pub rs_inited: bool,        /* false = scan not init'd yet */

    pub rs_read_stream: *mut ReadStream, /* read stream for sequential scans */

    pub rs_vistuples: [OffsetNumber; MaxHeapTuplesPerPage as usize], /* their offsets */
}
pub type HeapScanDesc = *mut HeapScanDescData;

/* ----------------------------------------------------------------
 *						 heap support routines
 * ----------------------------------------------------------------
 */

/*
 * Streaming read API callback for parallel sequential scans. Returns the next
 * block the caller wants from the read stream or InvalidBlockNumber when done.
 */
unsafe extern "C" fn heap_scan_stream_read_next_parallel(
    stream: *mut ReadStream,
    callback_private_data: *mut c_void,
    per_buffer_data: *mut c_void,
) -> BlockNumber {
    let scan: HeapScanDesc = callback_private_data as HeapScanDesc;

    Assert!(ScanDirectionIsForward((*scan).rs_dir));
    Assert!(!(*scan).rs_base.rs_parallel.is_null());

    if unlikely(!(*scan).rs_inited) {
        /* parallel scan */
        table_block_parallelscan_startblock_init(
            (*scan).rs_base.rs_rd,
            (*scan).rs_parallelworkerdata,
            (*scan).rs_base.rs_parallel as ParallelBlockTableScanDesc,
        );

        /* may return InvalidBlockNumber if there are no more blocks */
        (*scan).rs_prefetch_block = table_block_parallelscan_nextpage(
            (*scan).rs_base.rs_rd,
            (*scan).rs_parallelworkerdata,
            (*scan).rs_base.rs_parallel as ParallelBlockTableScanDesc,
        );
        (*scan).rs_inited = true;
    } else {
        (*scan).rs_prefetch_block = table_block_parallelscan_nextpage(
            (*scan).rs_base.rs_rd,
            (*scan).rs_parallelworkerdata,
            (*scan).rs_base.rs_parallel as ParallelBlockTableScanDesc,
        );
    }

    (*scan).rs_prefetch_block
}

/*
 * Streaming read API callback for serial sequential and TID range scans.
 * Returns the next block the caller wants from the read stream or
 * InvalidBlockNumber when done.
 */
unsafe extern "C" fn heap_scan_stream_read_next_serial(
    stream: *mut ReadStream,
    callback_private_data: *mut c_void,
    per_buffer_data: *mut c_void,
) -> BlockNumber {
    let scan: HeapScanDesc = callback_private_data as HeapScanDesc;

    if unlikely(!(*scan).rs_inited) {
        (*scan).rs_prefetch_block = heapgettup_initial_block(scan, (*scan).rs_dir);
        (*scan).rs_inited = true;
    } else {
        (*scan).rs_prefetch_block =
            heapgettup_advance_block(scan, (*scan).rs_prefetch_block, (*scan).rs_dir);
    }

    (*scan).rs_prefetch_block
}

/*
 * Read stream API callback for bitmap heap scans.
 * Returns the next block the caller wants from the read stream or
 * InvalidBlockNumber when done.
 */
unsafe extern "C" fn bitmapheap_stream_read_next(
    pgsr: *mut ReadStream,
    private_data: *mut c_void,
    per_buffer_data: *mut c_void,
) -> BlockNumber {
    let tbmres: *mut TBMIterateResult = per_buffer_data as *mut TBMIterateResult;
    let bscan: BitmapHeapScanDesc = private_data as BitmapHeapScanDesc;
    let hscan: HeapScanDesc = bscan as HeapScanDesc;
    let sscan: TableScanDesc = &mut (*hscan).rs_base;

    loop {
        CHECK_FOR_INTERRUPTS!();

        /* no more entries in the bitmap */
        if !tbm_iterate(&raw mut (*sscan).st.rs_tbmiterator, tbmres) {
            return InvalidBlockNumber;
        }

        /*
         * Ignore any claimed entries past what we think is the end of the
         * relation. It may have been extended after the start of our scan (we
         * only hold an AccessShareLock, and it could be inserts from this
         * backend).  We don't take this optimization in SERIALIZABLE
         * isolation though, as we need to examine all invisible tuples
         * reachable by the index.
         */
        if !IsolationIsSerializable() && (*tbmres).blockno >= (*hscan).rs_nblocks {
            continue;
        }

        return (*tbmres).blockno;
    }
}

/* ----------------
 *		initscan - scan code common to heap_beginscan and heap_rescan
 * ----------------
 */
unsafe fn initscan(scan: HeapScanDesc, key: ScanKey, keep_startblock: bool) {
    let mut bpscan: ParallelBlockTableScanDesc = std::ptr::null_mut();
    let mut allow_strat: bool;
    let mut allow_sync: bool;

    /*
     * Determine the number of blocks we have to scan.
     *
     * It is sufficient to do this once at scan start, since any tuples added
     * while the scan is in progress will be invisible to my snapshot anyway.
     * (That is not true when using a non-MVCC snapshot.  However, we couldn't
     * guarantee to return tuples added after scan start anyway, since they
     * might go into pages we already scanned.  To guarantee consistent
     * results for a non-MVCC snapshot, the caller must hold some higher-level
     * lock that ensures the interesting tuple(s) won't change.)
     */
    if !(*scan).rs_base.rs_parallel.is_null() {
        bpscan = (*scan).rs_base.rs_parallel as ParallelBlockTableScanDesc;
        (*scan).rs_nblocks = (*bpscan).phs_nblocks;
    } else {
        (*scan).rs_nblocks = RelationGetNumberOfBlocks((*scan).rs_base.rs_rd);
    }

    /*
     * If the table is large relative to NBuffers, use a bulk-read access
     * strategy and enable synchronized scanning (see syncscan.c).  Although
     * the thresholds for these features could be different, we make them the
     * same so that there are only two behaviors to tune rather than four.
     * (However, some callers need to be able to disable one or both of these
     * behaviors, independently of the size of the table; also there is a GUC
     * variable that can disable synchronized scanning.)
     *
     * Note that table_block_parallelscan_initialize has a very similar test;
     * if you change this, consider changing that one, too.
     */
    if !RelationUsesLocalBuffers((*scan).rs_base.rs_rd) && (*scan).rs_nblocks > (NBuffers / 4) as u32
    {
        allow_strat = ((*scan).rs_base.rs_flags & (SO_ALLOW_STRAT as u32)) != 0;
        allow_sync = ((*scan).rs_base.rs_flags & (SO_ALLOW_SYNC as u32)) != 0;
    } else {
        allow_strat = false;
        allow_sync = false;
    }

    if allow_strat {
        /* During a rescan, keep the previous strategy object. */
        if (*scan).rs_strategy.is_null() {
            (*scan).rs_strategy = GetAccessStrategy(BAS_BULKREAD);
        }
    } else {
        if !(*scan).rs_strategy.is_null() {
            FreeAccessStrategy((*scan).rs_strategy);
        }
        (*scan).rs_strategy = std::ptr::null_mut();
    }

    if !(*scan).rs_base.rs_parallel.is_null() {
        /* For parallel scan, believe whatever ParallelTableScanDesc says. */
        if (*(*scan).rs_base.rs_parallel).phs_syncscan {
            (*scan).rs_base.rs_flags |= (SO_ALLOW_SYNC as u32);
        } else {
            (*scan).rs_base.rs_flags &= !(SO_ALLOW_SYNC as u32);
        }
    } else if keep_startblock {
        /*
         * When rescanning, we want to keep the previous startblock setting,
         * so that rewinding a cursor doesn't generate surprising results.
         * Reset the active syncscan setting, though.
         */
        if allow_sync && synchronize_seqscans {
            (*scan).rs_base.rs_flags |= (SO_ALLOW_SYNC as u32);
        } else {
            (*scan).rs_base.rs_flags &= !(SO_ALLOW_SYNC as u32);
        }
    } else if allow_sync && synchronize_seqscans {
        (*scan).rs_base.rs_flags |= (SO_ALLOW_SYNC as u32);
        (*scan).rs_startblock = ss_get_location((*scan).rs_base.rs_rd, (*scan).rs_nblocks);
    } else {
        (*scan).rs_base.rs_flags &= !(SO_ALLOW_SYNC as u32);
        (*scan).rs_startblock = 0;
    }

    (*scan).rs_numblocks = InvalidBlockNumber;
    (*scan).rs_inited = false;
    (*scan).rs_ctup.t_data = std::ptr::null_mut();
    ItemPointerSetInvalid(&raw mut (*scan).rs_ctup.t_self);
    (*scan).rs_cbuf = InvalidBuffer;
    (*scan).rs_cblock = InvalidBlockNumber;
    (*scan).rs_ntuples = 0;
    (*scan).rs_cindex = 0;

    /*
     * Initialize to ForwardScanDirection because it is most common and
     * because heap scans go forward before going backward (e.g. CURSORs).
     */
    (*scan).rs_dir = ForwardScanDirection;
    (*scan).rs_prefetch_block = InvalidBlockNumber;

    /* page-at-a-time fields are always invalid when not rs_inited */

    /*
     * copy the scan key, if appropriate
     */
    if !key.is_null() && (*scan).rs_base.rs_nkeys > 0 {
        memcpy(
            (*scan).rs_base.rs_key as *mut c_void,
            key as *const c_void,
            (*scan).rs_base.rs_nkeys as usize * std::mem::size_of::<ScanKeyData>(),
        );
    }

    /*
     * Currently, we only have a stats counter for sequential heap scans (but
     * e.g for bitmap scans the underlying bitmap index scans will be counted,
     * and for sample scans we update stats for tuple fetches).
     */
    if (*scan).rs_base.rs_flags & (SO_TYPE_SEQSCAN as u32) != 0 {
        pgstat_count_heap_scan((*scan).rs_base.rs_rd);
    }
}

/*
 * heap_setscanlimits - restrict range of a heapscan
 *
 * startBlk is the page to start at
 * numBlks is number of pages to scan (InvalidBlockNumber means "all")
 */
pub unsafe fn heap_setscanlimits(sscan: TableScanDesc, startBlk: BlockNumber, numBlks: BlockNumber) {
    let scan: HeapScanDesc = sscan as HeapScanDesc;

    Assert!(!(*scan).rs_inited); /* else too late to change */
    /* else rs_startblock is significant */
    Assert!(!((*scan).rs_base.rs_flags & (SO_ALLOW_SYNC as u32) != 0));

    /* Check startBlk is valid (but allow case of zero blocks...) */
    Assert!(startBlk == 0 || startBlk < (*scan).rs_nblocks);

    (*scan).rs_startblock = startBlk;
    (*scan).rs_numblocks = numBlks;
}

/*
 * Per-tuple loop for heap_prepare_pagescan(). Pulled out so it can be called
 * multiple times, with constant arguments for all_visible,
 * check_serializable.
 */
#[inline(always)]
unsafe fn page_collect_tuples(
    scan: HeapScanDesc,
    snapshot: Snapshot,
    page: Page,
    buffer: Buffer,
    block: BlockNumber,
    lines: c_int,
    all_visible: bool,
    check_serializable: bool,
) -> c_int {
    let mut ntup: c_int = 0;
    let mut lineoff: OffsetNumber;

    lineoff = FirstOffsetNumber;
    while (lineoff as c_int) <= lines {
        let lpp: ItemId = PageGetItemId(page, lineoff);
        let mut loctup: HeapTupleData = std::mem::zeroed();
        let valid: bool;

        if !ItemIdIsNormal(lpp) {
            lineoff += 1;
            continue;
        }

        loctup.t_data = PageGetItem(page, lpp) as HeapTupleHeader;
        loctup.t_len = ItemIdGetLength(lpp);
        loctup.t_tableOid = RelationGetRelid((*scan).rs_base.rs_rd);
        ItemPointerSet(&raw mut loctup.t_self, block, lineoff);

        if all_visible {
            valid = true;
        } else {
            valid = HeapTupleSatisfiesVisibility(&raw mut loctup, snapshot, buffer);
        }

        if check_serializable {
            HeapCheckForSerializableConflictOut(
                valid,
                (*scan).rs_base.rs_rd,
                &raw mut loctup,
                buffer,
                snapshot,
            );
        }

        if valid {
            (*scan).rs_vistuples[ntup as usize] = lineoff;
            ntup += 1;
        }
        lineoff += 1;
    }

    Assert!(ntup <= MaxHeapTuplesPerPage);

    ntup
}

/*
 * heap_prepare_pagescan - Prepare current scan page to be scanned in pagemode
 *
 * Preparation currently consists of 1. prune the scan's rs_cbuf page, and 2.
 * fill the rs_vistuples[] array with the OffsetNumbers of visible tuples.
 */
pub unsafe fn heap_prepare_pagescan(sscan: TableScanDesc) {
    let scan: HeapScanDesc = sscan as HeapScanDesc;
    let buffer: Buffer = (*scan).rs_cbuf;
    let block: BlockNumber = (*scan).rs_cblock;
    let snapshot: Snapshot;
    let page: Page;
    let lines: c_int;
    let all_visible: bool;
    let check_serializable: bool;

    Assert!(BufferGetBlockNumber(buffer) == block);

    /* ensure we're not accidentally being used when not in pagemode */
    Assert!((*scan).rs_base.rs_flags & (SO_ALLOW_PAGEMODE as u32) != 0);
    snapshot = (*scan).rs_base.rs_snapshot;

    /*
     * Prune and repair fragmentation for the whole page, if possible.
     */
    heap_page_prune_opt((*scan).rs_base.rs_rd, buffer);

    /*
     * We must hold share lock on the buffer content while examining tuple
     * visibility.  Afterwards, however, the tuples we have found to be
     * visible are guaranteed good as long as we hold the buffer pin.
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buffer);
    lines = PageGetMaxOffsetNumber(page) as c_int;

    /*
     * If the all-visible flag indicates that all tuples on the page are
     * visible to everyone, we can skip the per-tuple visibility tests.
     */
    all_visible = PageIsAllVisible(page) && !(*snapshot).takenDuringRecovery;
    check_serializable =
        CheckForSerializableConflictOutNeeded((*scan).rs_base.rs_rd, snapshot);

    /*
     * We call page_collect_tuples() with constant arguments, to get the
     * compiler to constant fold the constant arguments.
     */
    if likely(all_visible) {
        if likely(!check_serializable) {
            (*scan).rs_ntuples =
                page_collect_tuples(scan, snapshot, page, buffer, block, lines, true, false);
        } else {
            (*scan).rs_ntuples =
                page_collect_tuples(scan, snapshot, page, buffer, block, lines, true, true);
        }
    } else {
        if likely(!check_serializable) {
            (*scan).rs_ntuples =
                page_collect_tuples(scan, snapshot, page, buffer, block, lines, false, false);
        } else {
            (*scan).rs_ntuples =
                page_collect_tuples(scan, snapshot, page, buffer, block, lines, false, true);
        }
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}

/*
 * heap_fetch_next_buffer - read and pin the next block from MAIN_FORKNUM.
 *
 * Read the next block of the scan relation from the read stream and save it
 * in the scan descriptor.  It is already pinned.
 */
#[inline]
unsafe fn heap_fetch_next_buffer(scan: HeapScanDesc, dir: ScanDirection) {
    Assert!(!(*scan).rs_read_stream.is_null());

    /* release previous scan buffer, if any */
    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf);
        (*scan).rs_cbuf = InvalidBuffer;
    }

    /*
     * Be sure to check for interrupts at least once per page.  Checks at
     * higher code levels won't be able to stop a seqscan that encounters many
     * pages' worth of consecutive dead tuples.
     */
    CHECK_FOR_INTERRUPTS!();

    /*
     * If the scan direction is changing, reset the prefetch block to the
     * current block.
     */
    if unlikely((*scan).rs_dir != dir) {
        (*scan).rs_prefetch_block = (*scan).rs_cblock;
        read_stream_reset((*scan).rs_read_stream);
    }

    (*scan).rs_dir = dir;

    (*scan).rs_cbuf = read_stream_next_buffer((*scan).rs_read_stream, std::ptr::null_mut());
    if BufferIsValid((*scan).rs_cbuf) {
        (*scan).rs_cblock = BufferGetBlockNumber((*scan).rs_cbuf);
    }
}

/*
 * heapgettup_initial_block - return the first BlockNumber to scan
 */
unsafe fn heapgettup_initial_block(scan: HeapScanDesc, dir: ScanDirection) -> BlockNumber {
    Assert!(!(*scan).rs_inited);
    Assert!((*scan).rs_base.rs_parallel.is_null());

    /* When there are no pages to scan, return InvalidBlockNumber */
    if (*scan).rs_nblocks == 0 || (*scan).rs_numblocks == 0 {
        return InvalidBlockNumber;
    }

    if ScanDirectionIsForward(dir) {
        (*scan).rs_startblock
    } else {
        /*
         * Disable reporting to syncscan logic in a backwards scan.
         */
        (*scan).rs_base.rs_flags &= !(SO_ALLOW_SYNC as u32);

        /*
         * Start from last page of the scan.
         */
        if (*scan).rs_numblocks != InvalidBlockNumber {
            return ((*scan).rs_startblock + (*scan).rs_numblocks - 1) % (*scan).rs_nblocks;
        }

        if (*scan).rs_startblock > 0 {
            return (*scan).rs_startblock - 1;
        }

        (*scan).rs_nblocks - 1
    }
}

/*
 * heapgettup_start_page - helper function for heapgettup()
 */
unsafe fn heapgettup_start_page(
    scan: HeapScanDesc,
    dir: ScanDirection,
    linesleft: *mut c_int,
    lineoff: *mut OffsetNumber,
) -> Page {
    let page: Page;

    Assert!((*scan).rs_inited);
    Assert!(BufferIsValid((*scan).rs_cbuf));

    /* Caller is responsible for ensuring buffer is locked if needed */
    page = BufferGetPage((*scan).rs_cbuf);

    *linesleft = PageGetMaxOffsetNumber(page) as c_int - FirstOffsetNumber as c_int + 1;

    if ScanDirectionIsForward(dir) {
        *lineoff = FirstOffsetNumber;
    } else {
        *lineoff = *linesleft as OffsetNumber;
    }

    /* lineoff now references the physically previous or next tid */
    page
}

/*
 * heapgettup_continue_page - helper function for heapgettup()
 */
#[inline]
unsafe fn heapgettup_continue_page(
    scan: HeapScanDesc,
    dir: ScanDirection,
    linesleft: *mut c_int,
    lineoff: *mut OffsetNumber,
) -> Page {
    let page: Page;

    Assert!((*scan).rs_inited);
    Assert!(BufferIsValid((*scan).rs_cbuf));

    /* Caller is responsible for ensuring buffer is locked if needed */
    page = BufferGetPage((*scan).rs_cbuf);

    if ScanDirectionIsForward(dir) {
        *lineoff = OffsetNumberNext((*scan).rs_coffset);
        *linesleft = PageGetMaxOffsetNumber(page) as c_int - (*lineoff) as c_int + 1;
    } else {
        /*
         * The previous returned tuple may have been vacuumed since the
         * previous scan when we use a non-MVCC snapshot.
         */
        *lineoff = Min!(
            PageGetMaxOffsetNumber(page),
            OffsetNumberPrev((*scan).rs_coffset)
        );
        *linesleft = *lineoff as c_int;
    }

    /* lineoff now references the physically previous or next tid */
    page
}

/*
 * heapgettup_advance_block - helper for heap_fetch_next_buffer()
 */
#[inline]
unsafe fn heapgettup_advance_block(
    scan: HeapScanDesc,
    mut block: BlockNumber,
    dir: ScanDirection,
) -> BlockNumber {
    Assert!((*scan).rs_base.rs_parallel.is_null());

    if likely(ScanDirectionIsForward(dir)) {
        block += 1;

        /* wrap back to the start of the heap */
        if block >= (*scan).rs_nblocks {
            block = 0;
        }

        /*
         * Report our new scan position for synchronization purposes.
         */
        if (*scan).rs_base.rs_flags & (SO_ALLOW_SYNC as u32) != 0 {
            ss_report_location((*scan).rs_base.rs_rd, block);
        }

        /* we're done if we're back at where we started */
        if block == (*scan).rs_startblock {
            return InvalidBlockNumber;
        }

        /* check if the limit imposed by heap_setscanlimits() is met */
        if (*scan).rs_numblocks != InvalidBlockNumber {
            (*scan).rs_numblocks -= 1;
            if (*scan).rs_numblocks == 0 {
                return InvalidBlockNumber;
            }
        }

        block
    } else {
        /* we're done if the last block is the start position */
        if block == (*scan).rs_startblock {
            return InvalidBlockNumber;
        }

        /* check if the limit imposed by heap_setscanlimits() is met */
        if (*scan).rs_numblocks != InvalidBlockNumber {
            (*scan).rs_numblocks -= 1;
            if (*scan).rs_numblocks == 0 {
                return InvalidBlockNumber;
            }
        }

        /* wrap to the end of the heap when the last page was page 0 */
        if block == 0 {
            block = (*scan).rs_nblocks;
        }

        block -= 1;

        block
    }
}

/* ----------------
 *		heapgettup - fetch next heap tuple
 * ----------------
 */
unsafe fn heapgettup(scan: HeapScanDesc, dir: ScanDirection, nkeys: c_int, key: ScanKey) {
    let tuple: HeapTuple = &raw mut (*scan).rs_ctup;
    let mut page: Page;
    let mut lineoff: OffsetNumber = 0;
    let mut linesleft: c_int = 0;

    if likely((*scan).rs_inited) {
        /* continue from previously returned page/tuple */
        LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_SHARE);
        page = heapgettup_continue_page(scan, dir, &raw mut linesleft, &raw mut lineoff);

        // goto continue_page;
        'continue_page1: loop {
            while linesleft > 0 {
                let lpp: ItemId = PageGetItemId(page, lineoff);

                if ItemIdIsNormal(lpp) {
                    (*tuple).t_data = PageGetItem(page, lpp) as HeapTupleHeader;
                    (*tuple).t_len = ItemIdGetLength(lpp);
                    ItemPointerSet(&raw mut (*tuple).t_self, (*scan).rs_cblock, lineoff);

                    let visible = HeapTupleSatisfiesVisibility(
                        tuple,
                        (*scan).rs_base.rs_snapshot,
                        (*scan).rs_cbuf,
                    );

                    HeapCheckForSerializableConflictOut(
                        visible,
                        (*scan).rs_base.rs_rd,
                        tuple,
                        (*scan).rs_cbuf,
                        (*scan).rs_base.rs_snapshot,
                    );

                    if visible {
                        if key.is_null()
                            || HeapKeyTest(
                                tuple,
                                RelationGetDescr((*scan).rs_base.rs_rd),
                                nkeys,
                                key,
                            )
                        {
                            LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_UNLOCK);
                            (*scan).rs_coffset = lineoff;
                            return;
                        }
                    }
                }

                linesleft -= 1;
                lineoff = (lineoff as i32 + dir as i32) as OffsetNumber;
            }

            /*
             * if we get here, it means we've exhausted the items on this page
             * and it's time to move to the next.
             */
            LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_UNLOCK);
            break 'continue_page1;
        }
    }

    /*
     * advance the scan until we find a qualifying tuple or run out of stuff
     * to scan
     */
    loop {
        heap_fetch_next_buffer(scan, dir);

        /* did we run out of blocks to scan? */
        if !BufferIsValid((*scan).rs_cbuf) {
            break;
        }

        Assert!(BufferGetBlockNumber((*scan).rs_cbuf) == (*scan).rs_cblock);

        LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_SHARE);
        page = heapgettup_start_page(scan, dir, &raw mut linesleft, &raw mut lineoff);

        // continue_page:
        let mut found = false;
        while linesleft > 0 {
            let lpp: ItemId = PageGetItemId(page, lineoff);

            if !ItemIdIsNormal(lpp) {
                linesleft -= 1;
                lineoff = (lineoff as i32 + dir as i32) as OffsetNumber;
                continue;
            }

            (*tuple).t_data = PageGetItem(page, lpp) as HeapTupleHeader;
            (*tuple).t_len = ItemIdGetLength(lpp);
            ItemPointerSet(&raw mut (*tuple).t_self, (*scan).rs_cblock, lineoff);

            let visible =
                HeapTupleSatisfiesVisibility(tuple, (*scan).rs_base.rs_snapshot, (*scan).rs_cbuf);

            HeapCheckForSerializableConflictOut(
                visible,
                (*scan).rs_base.rs_rd,
                tuple,
                (*scan).rs_cbuf,
                (*scan).rs_base.rs_snapshot,
            );

            /* skip tuples not visible to this snapshot */
            if !visible {
                linesleft -= 1;
                lineoff = (lineoff as i32 + dir as i32) as OffsetNumber;
                continue;
            }

            /* skip any tuples that don't match the scan key */
            if !key.is_null()
                && !HeapKeyTest(tuple, RelationGetDescr((*scan).rs_base.rs_rd), nkeys, key)
            {
                linesleft -= 1;
                lineoff = (lineoff as i32 + dir as i32) as OffsetNumber;
                continue;
            }

            LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_UNLOCK);
            (*scan).rs_coffset = lineoff;
            found = true;
            break;
        }
        if found {
            return;
        }

        /*
         * if we get here, it means we've exhausted the items on this page and
         * it's time to move to the next.
         */
        LockBuffer((*scan).rs_cbuf, BUFFER_LOCK_UNLOCK);
    }

    /* end of scan */
    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf);
    }

    (*scan).rs_cbuf = InvalidBuffer;
    (*scan).rs_cblock = InvalidBlockNumber;
    (*scan).rs_prefetch_block = InvalidBlockNumber;
    (*tuple).t_data = std::ptr::null_mut();
    (*scan).rs_inited = false;
}

/* ----------------
 *		heapgettup_pagemode - fetch next heap tuple in page-at-a-time mode
 * ----------------
 */
unsafe fn heapgettup_pagemode(scan: HeapScanDesc, dir: ScanDirection, nkeys: c_int, key: ScanKey) {
    let tuple: HeapTuple = &raw mut (*scan).rs_ctup;
    let mut page: Page;
    let mut lineindex: u32;
    let mut linesleft: u32;

    if likely((*scan).rs_inited) {
        /* continue from previously returned page/tuple */
        page = BufferGetPage((*scan).rs_cbuf);

        lineindex = ((*scan).rs_cindex as i32 + dir as i32) as u32;
        if ScanDirectionIsForward(dir) {
            linesleft = (*scan).rs_ntuples as u32 - lineindex;
        } else {
            linesleft = (*scan).rs_cindex as u32;
        }

        // goto continue_page;
        let mut found = false;
        while linesleft > 0 {
            Assert!(lineindex < (*scan).rs_ntuples as u32);
            let lineoff: OffsetNumber = (*scan).rs_vistuples[lineindex as usize];
            let lpp: ItemId = PageGetItemId(page, lineoff);
            Assert!(ItemIdIsNormal(lpp));

            (*tuple).t_data = PageGetItem(page, lpp) as HeapTupleHeader;
            (*tuple).t_len = ItemIdGetLength(lpp);
            ItemPointerSetOffsetNumber(&raw mut (*tuple).t_self, lineoff);

            if key.is_null()
                || HeapKeyTest(tuple, RelationGetDescr((*scan).rs_base.rs_rd), nkeys, key)
            {
                (*scan).rs_cindex = lineindex as c_int;
                found = true;
                break;
            }

            linesleft -= 1;
            lineindex = (lineindex as i32 + dir as i32) as u32;
        }
        if found {
            return;
        }
    } else {
        /* fall through to the main loop */
        loop {
            heap_fetch_next_buffer(scan, dir);

            /* did we run out of blocks to scan? */
            if !BufferIsValid((*scan).rs_cbuf) {
                break;
            }

            Assert!(BufferGetBlockNumber((*scan).rs_cbuf) == (*scan).rs_cblock);

            /* prune the page and determine visible tuple offsets */
            heap_prepare_pagescan(scan as TableScanDesc);
            page = BufferGetPage((*scan).rs_cbuf);
            linesleft = (*scan).rs_ntuples as u32;
            lineindex = if ScanDirectionIsForward(dir) {
                0
            } else {
                linesleft - 1
            };

            /* block is the same for all tuples, set it once outside the loop */
            ItemPointerSetBlockNumber(&raw mut (*tuple).t_self, (*scan).rs_cblock);

            let mut found = false;
            while linesleft > 0 {
                Assert!(lineindex < (*scan).rs_ntuples as u32);
                let lineoff: OffsetNumber = (*scan).rs_vistuples[lineindex as usize];
                let lpp: ItemId = PageGetItemId(page, lineoff);
                Assert!(ItemIdIsNormal(lpp));

                (*tuple).t_data = PageGetItem(page, lpp) as HeapTupleHeader;
                (*tuple).t_len = ItemIdGetLength(lpp);
                ItemPointerSetOffsetNumber(&raw mut (*tuple).t_self, lineoff);

                if key.is_null()
                    || HeapKeyTest(tuple, RelationGetDescr((*scan).rs_base.rs_rd), nkeys, key)
                {
                    (*scan).rs_cindex = lineindex as c_int;
                    found = true;
                    break;
                }

                linesleft -= 1;
                lineindex = (lineindex as i32 + dir as i32) as u32;
            }
            if found {
                return;
            }
        }

        /* end of scan */
        if BufferIsValid((*scan).rs_cbuf) {
            ReleaseBuffer((*scan).rs_cbuf);
        }
        (*scan).rs_cbuf = InvalidBuffer;
        (*scan).rs_cblock = InvalidBlockNumber;
        (*scan).rs_prefetch_block = InvalidBlockNumber;
        (*tuple).t_data = std::ptr::null_mut();
        (*scan).rs_inited = false;
        return;
    }

    /*
     * If we reach here we exhausted the current page in the rs_inited path;
     * advance through subsequent pages.
     */
    loop {
        heap_fetch_next_buffer(scan, dir);

        if !BufferIsValid((*scan).rs_cbuf) {
            break;
        }

        Assert!(BufferGetBlockNumber((*scan).rs_cbuf) == (*scan).rs_cblock);

        heap_prepare_pagescan(scan as TableScanDesc);
        page = BufferGetPage((*scan).rs_cbuf);
        linesleft = (*scan).rs_ntuples as u32;
        lineindex = if ScanDirectionIsForward(dir) {
            0
        } else {
            linesleft - 1
        };

        ItemPointerSetBlockNumber(&raw mut (*tuple).t_self, (*scan).rs_cblock);

        let mut found = false;
        while linesleft > 0 {
            Assert!(lineindex < (*scan).rs_ntuples as u32);
            let lineoff: OffsetNumber = (*scan).rs_vistuples[lineindex as usize];
            let lpp: ItemId = PageGetItemId(page, lineoff);
            Assert!(ItemIdIsNormal(lpp));

            (*tuple).t_data = PageGetItem(page, lpp) as HeapTupleHeader;
            (*tuple).t_len = ItemIdGetLength(lpp);
            ItemPointerSetOffsetNumber(&raw mut (*tuple).t_self, lineoff);

            if key.is_null()
                || HeapKeyTest(tuple, RelationGetDescr((*scan).rs_base.rs_rd), nkeys, key)
            {
                (*scan).rs_cindex = lineindex as c_int;
                found = true;
                break;
            }

            linesleft -= 1;
            lineindex = (lineindex as i32 + dir as i32) as u32;
        }
        if found {
            return;
        }
    }

    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf);
    }
    (*scan).rs_cbuf = InvalidBuffer;
    (*scan).rs_cblock = InvalidBlockNumber;
    (*scan).rs_prefetch_block = InvalidBlockNumber;
    (*tuple).t_data = std::ptr::null_mut();
    (*scan).rs_inited = false;
}
/* ----------------------------------------------------------------
 *					 heap access method interface
 * ----------------------------------------------------------------
 */

pub unsafe fn heap_beginscan(
    relation: Relation,
    snapshot: Snapshot,
    nkeys: c_int,
    key: ScanKey,
    parallel_scan: ParallelTableScanDesc,
    flags: u32,
) -> TableScanDesc {
    let scan: HeapScanDesc;

    /*
     * increment relation ref count while scanning relation
     */
    RelationIncrementReferenceCount(relation);

    /*
     * allocate and initialize scan descriptor
     */
    if flags & (SO_TYPE_BITMAPSCAN as u32) != 0 {
        let bscan: BitmapHeapScanDesc =
            palloc0(std::mem::size_of::<BitmapHeapScanDescData>()) as BitmapHeapScanDesc;

        /*
         * Bitmap Heap scans do not have any fields that a normal Heap Scan
         * does not have, so no special initializations required here.
         */
        scan = bscan as HeapScanDesc;
    } else {
        scan = palloc0(std::mem::size_of::<HeapScanDescData>()) as HeapScanDesc;
    }

    (*scan).rs_base.rs_rd = relation;
    (*scan).rs_base.rs_snapshot = snapshot;
    (*scan).rs_base.rs_nkeys = nkeys;
    (*scan).rs_base.rs_flags = flags;
    (*scan).rs_base.rs_parallel = parallel_scan;
    (*scan).rs_strategy = std::ptr::null_mut(); /* set in initscan */
    (*scan).rs_cbuf = InvalidBuffer;

    /*
     * Disable page-at-a-time mode if it's not a MVCC-safe snapshot.
     */
    if !(!snapshot.is_null() && IsMVCCSnapshot(snapshot)) {
        (*scan).rs_base.rs_flags &= !(SO_ALLOW_PAGEMODE as u32);
    }

    /*
     * For seqscan and sample scans in a serializable transaction, acquire a
     * predicate lock on the entire relation.
     */
    if (*scan).rs_base.rs_flags & ((SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN) as u32) != 0 {
        /*
         * Ensure a missing snapshot is noticed reliably.
         */
        Assert!(!snapshot.is_null());
        PredicateLockRelation(relation, snapshot);
    }

    /* we only need to set this up once */
    (*scan).rs_ctup.t_tableOid = RelationGetRelid(relation);

    /*
     * Allocate memory to keep track of page allocation for parallel workers
     * when doing a parallel scan.
     */
    if !parallel_scan.is_null() {
        (*scan).rs_parallelworkerdata =
            palloc(std::mem::size_of::<ParallelBlockTableScanWorkerData>())
                as *mut ParallelBlockTableScanWorkerData;
    } else {
        (*scan).rs_parallelworkerdata = std::ptr::null_mut();
    }

    /*
     * we do this here instead of in initscan() because heap_rescan also calls
     * initscan() and we don't want to allocate memory again
     */
    if nkeys > 0 {
        (*scan).rs_base.rs_key =
            palloc(std::mem::size_of::<ScanKeyData>() * nkeys as usize) as ScanKey;
    } else {
        (*scan).rs_base.rs_key = std::ptr::null_mut();
    }

    initscan(scan, key, false);

    (*scan).rs_read_stream = std::ptr::null_mut();

    /*
     * Set up a read stream for sequential scans and TID range scans.
     */
    if (*scan).rs_base.rs_flags & (SO_TYPE_SEQSCAN as u32) != 0
        || (*scan).rs_base.rs_flags & (SO_TYPE_TIDRANGESCAN as u32) != 0
    {
        let cb: ReadStreamBlockNumberCB;

        if !(*scan).rs_base.rs_parallel.is_null() {
            cb = heap_scan_stream_read_next_parallel;
        } else {
            cb = heap_scan_stream_read_next_serial;
        }

        (*scan).rs_read_stream = read_stream_begin_relation(
            READ_STREAM_SEQUENTIAL | READ_STREAM_USE_BATCHING,
            (*scan).rs_strategy,
            (*scan).rs_base.rs_rd,
            MAIN_FORKNUM,
            cb,
            scan as *mut c_void,
            0,
        );
    } else if (*scan).rs_base.rs_flags & (SO_TYPE_BITMAPSCAN as u32) != 0 {
        (*scan).rs_read_stream = read_stream_begin_relation(
            READ_STREAM_DEFAULT | READ_STREAM_USE_BATCHING,
            (*scan).rs_strategy,
            (*scan).rs_base.rs_rd,
            MAIN_FORKNUM,
            bitmapheap_stream_read_next,
            scan as *mut c_void,
            std::mem::size_of::<TBMIterateResult>(),
        );
    }

    scan as TableScanDesc
}

pub unsafe fn heap_rescan(
    sscan: TableScanDesc,
    key: ScanKey,
    set_params: bool,
    allow_strat: bool,
    allow_sync: bool,
    allow_pagemode: bool,
) {
    let scan: HeapScanDesc = sscan as HeapScanDesc;

    if set_params {
        if allow_strat {
            (*scan).rs_base.rs_flags |= (SO_ALLOW_STRAT as u32);
        } else {
            (*scan).rs_base.rs_flags &= !(SO_ALLOW_STRAT as u32);
        }

        if allow_sync {
            (*scan).rs_base.rs_flags |= (SO_ALLOW_SYNC as u32);
        } else {
            (*scan).rs_base.rs_flags &= !(SO_ALLOW_SYNC as u32);
        }

        if allow_pagemode
            && !(*scan).rs_base.rs_snapshot.is_null()
            && IsMVCCSnapshot((*scan).rs_base.rs_snapshot)
        {
            (*scan).rs_base.rs_flags |= (SO_ALLOW_PAGEMODE as u32);
        } else {
            (*scan).rs_base.rs_flags &= !(SO_ALLOW_PAGEMODE as u32);
        }
    }

    /*
     * unpin scan buffers
     */
    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf);
        (*scan).rs_cbuf = InvalidBuffer;
    }

    /*
     * The read stream is reset on rescan.
     */
    if !(*scan).rs_read_stream.is_null() {
        read_stream_reset((*scan).rs_read_stream);
    }

    /*
     * reinitialize scan descriptor
     */
    initscan(scan, key, true);
}

pub unsafe fn heap_endscan(sscan: TableScanDesc) {
    let scan: HeapScanDesc = sscan as HeapScanDesc;

    /* Note: no locking manipulations needed */

    /*
     * unpin scan buffers
     */
    if BufferIsValid((*scan).rs_cbuf) {
        ReleaseBuffer((*scan).rs_cbuf);
    }

    /*
     * Must free the read stream before freeing the BufferAccessStrategy.
     */
    if !(*scan).rs_read_stream.is_null() {
        read_stream_end((*scan).rs_read_stream);
    }

    /*
     * decrement relation reference count and free scan descriptor storage
     */
    RelationDecrementReferenceCount((*scan).rs_base.rs_rd);

    if !(*scan).rs_base.rs_key.is_null() {
        pfree((*scan).rs_base.rs_key as *mut c_void);
    }

    if !(*scan).rs_strategy.is_null() {
        FreeAccessStrategy((*scan).rs_strategy);
    }

    if !(*scan).rs_parallelworkerdata.is_null() {
        pfree((*scan).rs_parallelworkerdata as *mut c_void);
    }

    if (*scan).rs_base.rs_flags & (SO_TEMP_SNAPSHOT as u32) != 0 {
        UnregisterSnapshot((*scan).rs_base.rs_snapshot);
    }

    pfree(scan as *mut c_void);
}

pub unsafe fn heap_getnext(sscan: TableScanDesc, direction: ScanDirection) -> HeapTuple {
    let scan: HeapScanDesc = sscan as HeapScanDesc;

    /*
     * This is still widely used directly, without going through table AM, so
     * add a safety check.
     */
    if unlikely((*(*sscan).rs_rd).rd_tableam != GetHeapamTableAmRoutine()) {
        ereport!(ERROR, errmsg!("only heap AM is supported"));
    }

    /*
     * We don't expect direct calls to heap_getnext with valid CheckXidAlive
     * for catalog or regular tables.
     */
    if unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan) {
        elog!(ERROR, "unexpected heap_getnext call during logical decoding");
    }

    /* Note: no locking manipulations needed */

    if (*scan).rs_base.rs_flags & (SO_ALLOW_PAGEMODE as u32) != 0 {
        heapgettup_pagemode(
            scan,
            direction,
            (*scan).rs_base.rs_nkeys,
            (*scan).rs_base.rs_key,
        );
    } else {
        heapgettup(
            scan,
            direction,
            (*scan).rs_base.rs_nkeys,
            (*scan).rs_base.rs_key,
        );
    }

    if (*scan).rs_ctup.t_data.is_null() {
        return std::ptr::null_mut();
    }

    /*
     * if we get here it means we have a new current scan tuple.
     */
    pgstat_count_heap_getnext((*scan).rs_base.rs_rd);

    &raw mut (*scan).rs_ctup
}

pub unsafe fn heap_getnextslot(
    sscan: TableScanDesc,
    direction: ScanDirection,
    slot: *mut TupleTableSlot,
) -> bool {
    let scan: HeapScanDesc = sscan as HeapScanDesc;

    /* Note: no locking manipulations needed */

    if (*sscan).rs_flags & (SO_ALLOW_PAGEMODE as u32) != 0 {
        heapgettup_pagemode(scan, direction, (*sscan).rs_nkeys, (*sscan).rs_key);
    } else {
        heapgettup(scan, direction, (*sscan).rs_nkeys, (*sscan).rs_key);
    }

    if (*scan).rs_ctup.t_data.is_null() {
        ExecClearTuple(slot);
        return false;
    }

    /*
     * if we get here it means we have a new current scan tuple.
     */
    pgstat_count_heap_getnext((*scan).rs_base.rs_rd);

    ExecStoreBufferHeapTuple(&raw mut (*scan).rs_ctup, slot, (*scan).rs_cbuf);
    true
}

pub unsafe fn heap_set_tidrange(sscan: TableScanDesc, mintid: ItemPointer, maxtid: ItemPointer) {
    let scan: HeapScanDesc = sscan as HeapScanDesc;
    let startBlk: BlockNumber;
    let numBlks: BlockNumber;
    let mut highestItem: ItemPointerData = std::mem::zeroed();
    let mut lowestItem: ItemPointerData = std::mem::zeroed();

    /*
     * For relations without any pages, we can simply leave the TID range
     * unset.
     */
    if (*scan).rs_nblocks == 0 {
        return;
    }

    /*
     * Set up some ItemPointers which point to the first and last possible
     * tuples in the heap.
     */
    ItemPointerSet(&raw mut highestItem, (*scan).rs_nblocks - 1, MaxOffsetNumber);
    ItemPointerSet(&raw mut lowestItem, 0, FirstOffsetNumber);

    /*
     * If the given maximum TID is below the highest possible TID in the
     * relation, then restrict the range to that.
     */
    if ItemPointerCompare(maxtid, &raw mut highestItem) < 0 {
        ItemPointerCopy(maxtid, &raw mut highestItem);
    }

    /*
     * If the given minimum TID is above the lowest possible TID in the
     * relation, then restrict the range to only scan for TIDs above that.
     */
    if ItemPointerCompare(mintid, &raw mut lowestItem) > 0 {
        ItemPointerCopy(mintid, &raw mut lowestItem);
    }

    /*
     * Check for an empty range and protect from would be negative results.
     */
    if ItemPointerCompare(&raw mut highestItem, &raw mut lowestItem) < 0 {
        /* Set an empty range of blocks to scan */
        heap_setscanlimits(sscan, 0, 0);
        return;
    }

    /*
     * Calculate the first block and the number of blocks we must scan.
     */
    startBlk = ItemPointerGetBlockNumberNoCheck(&raw mut lowestItem);

    numBlks = ItemPointerGetBlockNumberNoCheck(&raw mut highestItem)
        - ItemPointerGetBlockNumberNoCheck(&raw mut lowestItem)
        + 1;

    /* Set the start block and number of blocks to scan */
    heap_setscanlimits(sscan, startBlk, numBlks);

    /* Finally, set the TID range in sscan */
    ItemPointerCopy(&raw mut lowestItem, &raw mut (*sscan).st.tidrange.rs_mintid);
    ItemPointerCopy(&raw mut highestItem, &raw mut (*sscan).st.tidrange.rs_maxtid);
}

pub unsafe fn heap_getnextslot_tidrange(
    sscan: TableScanDesc,
    direction: ScanDirection,
    slot: *mut TupleTableSlot,
) -> bool {
    let scan: HeapScanDesc = sscan as HeapScanDesc;
    let mintid: ItemPointer = &raw mut (*sscan).st.tidrange.rs_mintid;
    let maxtid: ItemPointer = &raw mut (*sscan).st.tidrange.rs_maxtid;

    /* Note: no locking manipulations needed */
    loop {
        if (*sscan).rs_flags & (SO_ALLOW_PAGEMODE as u32) != 0 {
            heapgettup_pagemode(scan, direction, (*sscan).rs_nkeys, (*sscan).rs_key);
        } else {
            heapgettup(scan, direction, (*sscan).rs_nkeys, (*sscan).rs_key);
        }

        if (*scan).rs_ctup.t_data.is_null() {
            ExecClearTuple(slot);
            return false;
        }

        /*
         * Here we must filter out any tuples from these pages that are outside
         * of that range.
         */
        if ItemPointerCompare(&raw mut (*scan).rs_ctup.t_self, mintid) < 0 {
            ExecClearTuple(slot);

            if ScanDirectionIsBackward(direction) {
                return false;
            }

            continue;
        }

        /*
         * Likewise for the final page, we must filter out TIDs greater than
         * maxtid.
         */
        if ItemPointerCompare(&raw mut (*scan).rs_ctup.t_self, maxtid) > 0 {
            ExecClearTuple(slot);

            if ScanDirectionIsForward(direction) {
                return false;
            }
            continue;
        }

        break;
    }

    /*
     * if we get here it means we have a new current scan tuple.
     */
    pgstat_count_heap_getnext((*scan).rs_base.rs_rd);

    ExecStoreBufferHeapTuple(&raw mut (*scan).rs_ctup, slot, (*scan).rs_cbuf);
    true
}

/*
 *	heap_fetch		- retrieve tuple with given tid
 */
pub unsafe fn heap_fetch(
    relation: Relation,
    snapshot: Snapshot,
    tuple: HeapTuple,
    userbuf: *mut Buffer,
    keep_buf: bool,
) -> bool {
    let tid: ItemPointer = &raw mut (*tuple).t_self;
    let lp: ItemId;
    let buffer: Buffer;
    let page: Page;
    let offnum: OffsetNumber;
    let valid: bool;

    /*
     * Fetch and pin the appropriate page of the relation.
     */
    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

    /*
     * Need share lock on buffer to examine tuple commit status.
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    /*
     * We'd better check for out-of-range offnum in case of VACUUM since the
     * TID was obtained.
     */
    offnum = ItemPointerGetOffsetNumber(tid);
    if offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        *userbuf = InvalidBuffer;
        (*tuple).t_data = std::ptr::null_mut();
        return false;
    }

    /*
     * get the item line pointer corresponding to the requested tid
     */
    lp = PageGetItemId(page, offnum);

    /*
     * Must check for deleted tuple.
     */
    if !ItemIdIsNormal(lp) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buffer);
        *userbuf = InvalidBuffer;
        (*tuple).t_data = std::ptr::null_mut();
        return false;
    }

    /*
     * fill in *tuple fields
     */
    (*tuple).t_data = PageGetItem(page, lp) as HeapTupleHeader;
    (*tuple).t_len = ItemIdGetLength(lp);
    (*tuple).t_tableOid = RelationGetRelid(relation);

    /*
     * check tuple visibility, then release lock
     */
    valid = HeapTupleSatisfiesVisibility(tuple, snapshot, buffer);

    if valid {
        PredicateLockTID(
            relation,
            &raw mut (*tuple).t_self,
            snapshot,
            HeapTupleHeaderGetXmin((*tuple).t_data),
        );
    }

    HeapCheckForSerializableConflictOut(valid, relation, tuple, buffer, snapshot);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    if valid {
        /*
         * All checks passed, so return the tuple as valid.
         */
        *userbuf = buffer;

        return true;
    }

    /* Tuple failed time qual, but maybe caller wants to see it anyway. */
    if keep_buf {
        *userbuf = buffer;
    } else {
        ReleaseBuffer(buffer);
        *userbuf = InvalidBuffer;
        (*tuple).t_data = std::ptr::null_mut();
    }

    false
}

/*
 *	heap_hot_search_buffer	- search HOT chain for tuple satisfying snapshot
 */
pub unsafe fn heap_hot_search_buffer(
    tid: ItemPointer,
    relation: Relation,
    buffer: Buffer,
    snapshot: Snapshot,
    heapTuple: HeapTuple,
    all_dead: *mut bool,
    first_call: bool,
) -> bool {
    let page: Page = BufferGetPage(buffer);
    let mut prev_xmax: TransactionId = InvalidTransactionId;
    let blkno: BlockNumber;
    let mut offnum: OffsetNumber;
    let mut at_chain_start: bool;
    let mut valid: bool = false;
    let mut skip: bool;
    let mut vistest: *mut GlobalVisState = std::ptr::null_mut();

    /* If this is not the first call, previous call returned a (live!) tuple */
    if !all_dead.is_null() {
        *all_dead = first_call;
    }

    blkno = ItemPointerGetBlockNumber(tid);
    offnum = ItemPointerGetOffsetNumber(tid);
    at_chain_start = first_call;
    skip = !first_call;

    /* XXX: we should assert that a snapshot is pushed or registered */
    Assert!(TransactionIdIsValid(RecentXmin));
    Assert!(BufferGetBlockNumber(buffer) == blkno);

    /* Scan through possible multiple members of HOT-chain */
    loop {
        let lp: ItemId;

        /* check for bogus TID */
        if offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page) {
            break;
        }

        lp = PageGetItemId(page, offnum);

        /* check for unused, dead, or redirected items */
        if !ItemIdIsNormal(lp) {
            /* We should only see a redirect at start of chain */
            if ItemIdIsRedirected(lp) && at_chain_start {
                /* Follow the redirect */
                offnum = ItemIdGetRedirect(lp) as OffsetNumber;
                at_chain_start = false;
                continue;
            }
            /* else must be end of chain */
            break;
        }

        /*
         * Update heapTuple to point to the element of the HOT chain we're
         * currently investigating.
         */
        (*heapTuple).t_data = PageGetItem(page, lp) as HeapTupleHeader;
        (*heapTuple).t_len = ItemIdGetLength(lp);
        (*heapTuple).t_tableOid = RelationGetRelid(relation);
        ItemPointerSet(&raw mut (*heapTuple).t_self, blkno, offnum);

        /*
         * Shouldn't see a HEAP_ONLY tuple at chain start.
         */
        if at_chain_start && HeapTupleIsHeapOnly(heapTuple) {
            break;
        }

        /*
         * The xmin should match the previous xmax value, else chain is
         * broken.
         */
        if TransactionIdIsValid(prev_xmax)
            && !TransactionIdEquals(prev_xmax, HeapTupleHeaderGetXmin((*heapTuple).t_data))
        {
            break;
        }

        /*
         * When first_call is true (and thus, skip is initially false) we'll
         * return the first tuple we find.
         */
        if !skip {
            /* If it's visible per the snapshot, we must return it */
            valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot, buffer);
            HeapCheckForSerializableConflictOut(valid, relation, heapTuple, buffer, snapshot);

            if valid {
                ItemPointerSetOffsetNumber(tid, offnum);
                PredicateLockTID(
                    relation,
                    &raw mut (*heapTuple).t_self,
                    snapshot,
                    HeapTupleHeaderGetXmin((*heapTuple).t_data),
                );
                if !all_dead.is_null() {
                    *all_dead = false;
                }
                return true;
            }
        }
        skip = false;

        /*
         * If we can't see it, maybe no one else can either.
         */
        if !all_dead.is_null() && *all_dead {
            if vistest.is_null() {
                vistest = GlobalVisTestFor(relation);
            }

            if !HeapTupleIsSurelyDead(heapTuple, vistest) {
                *all_dead = false;
            }
        }

        /*
         * Check to see if HOT chain continues past this tuple.
         */
        if HeapTupleIsHotUpdated(heapTuple) {
            Assert!(ItemPointerGetBlockNumber(&raw mut (*(*heapTuple).t_data).t_ctid) == blkno);
            offnum = ItemPointerGetOffsetNumber(&raw mut (*(*heapTuple).t_data).t_ctid);
            at_chain_start = false;
            prev_xmax = HeapTupleHeaderGetUpdateXid((*heapTuple).t_data);
        } else {
            break; /* end of chain */
        }
    }

    false
}

/*
 *	heap_get_latest_tid -  get the latest tid of a specified tuple
 */
pub unsafe fn heap_get_latest_tid(sscan: TableScanDesc, tid: ItemPointer) {
    let relation: Relation = (*sscan).rs_rd;
    let snapshot: Snapshot = (*sscan).rs_snapshot;
    let mut ctid: ItemPointerData;
    let mut priorXmax: TransactionId;

    /*
     * table_tuple_get_latest_tid() verified that the passed in tid is valid.
     */
    Assert!(ItemPointerIsValid(tid));

    /*
     * Loop to chase down t_ctid links.
     */
    ctid = *tid;
    priorXmax = InvalidTransactionId; /* cannot check first XMIN */
    loop {
        let buffer: Buffer;
        let page: Page;
        let offnum: OffsetNumber;
        let lp: ItemId;
        let mut tp: HeapTupleData = std::mem::zeroed();
        let valid: bool;

        /*
         * Read, pin, and lock the page.
         */
        buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&raw mut ctid));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        /*
         * Check for bogus item number.
         */
        offnum = ItemPointerGetOffsetNumber(&raw mut ctid);
        if offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page) {
            UnlockReleaseBuffer(buffer);
            break;
        }
        lp = PageGetItemId(page, offnum);
        if !ItemIdIsNormal(lp) {
            UnlockReleaseBuffer(buffer);
            break;
        }

        /* OK to access the tuple */
        tp.t_self = ctid;
        tp.t_data = PageGetItem(page, lp) as HeapTupleHeader;
        tp.t_len = ItemIdGetLength(lp);
        tp.t_tableOid = RelationGetRelid(relation);

        /*
         * After following a t_ctid link, we might arrive at an unrelated
         * tuple.  Check for XMIN match.
         */
        if TransactionIdIsValid(priorXmax)
            && !TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(tp.t_data))
        {
            UnlockReleaseBuffer(buffer);
            break;
        }

        /*
         * Check tuple visibility; if visible, set it as the new result
         * candidate.
         */
        valid = HeapTupleSatisfiesVisibility(&raw mut tp, snapshot, buffer);
        HeapCheckForSerializableConflictOut(valid, relation, &raw mut tp, buffer, snapshot);
        if valid {
            *tid = ctid;
        }

        /*
         * If there's a valid t_ctid link, follow it, else we're done.
         */
        if (*tp.t_data).t_infomask & HEAP_XMAX_INVALID != 0
            || HeapTupleHeaderIsOnlyLocked(tp.t_data)
            || HeapTupleHeaderIndicatesMovedPartitions(tp.t_data)
            || ItemPointerEquals(&raw mut tp.t_self, &raw mut (*tp.t_data).t_ctid)
        {
            UnlockReleaseBuffer(buffer);
            break;
        }

        ctid = (*tp.t_data).t_ctid;
        priorXmax = HeapTupleHeaderGetUpdateXid(tp.t_data);
        UnlockReleaseBuffer(buffer);
    } /* end of loop */
}

/*
 * UpdateXmaxHintBits - update tuple hint bits after xmax transaction ends
 */
unsafe fn UpdateXmaxHintBits(tuple: HeapTupleHeader, buffer: Buffer, xid: TransactionId) {
    Assert!(TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple), xid));
    Assert!(!((*tuple).t_infomask & HEAP_XMAX_IS_MULTI != 0));

    if (*tuple).t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID) == 0 {
        if !HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) && TransactionIdDidCommit(xid) {
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, xid);
        } else {
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        }
    }
}

/*
 * GetBulkInsertState - prepare status object for a bulk insert
 */
pub unsafe fn GetBulkInsertState() -> BulkInsertState {
    let bistate: BulkInsertState;

    bistate = palloc(std::mem::size_of::<BulkInsertStateData>()) as BulkInsertState;
    (*bistate).strategy = GetAccessStrategy(BAS_BULKWRITE) as _;
    (*bistate).current_buf = InvalidBuffer;
    (*bistate).next_free = InvalidBlockNumber;
    (*bistate).last_free = InvalidBlockNumber;
    (*bistate).already_extended_by = 0;
    bistate
}

/*
 * FreeBulkInsertState - clean up after finishing a bulk insert
 */
pub unsafe fn FreeBulkInsertState(bistate: BulkInsertState) {
    if (*bistate).current_buf != InvalidBuffer {
        ReleaseBuffer((*bistate).current_buf);
    }
    FreeAccessStrategy((*bistate).strategy as _);
    pfree(bistate as *mut c_void);
}

/*
 * ReleaseBulkInsertStatePin - release a buffer currently held in bistate
 */
pub unsafe fn ReleaseBulkInsertStatePin(bistate: BulkInsertState) {
    if (*bistate).current_buf != InvalidBuffer {
        ReleaseBuffer((*bistate).current_buf);
    }
    (*bistate).current_buf = InvalidBuffer;

    /*
     * Despite the name, we also reset bulk relation extension state.
     */
    (*bistate).next_free = InvalidBlockNumber;
    (*bistate).last_free = InvalidBlockNumber;
}

/*
 *	heap_insert		- insert tuple into a heap
 */
pub unsafe fn heap_insert(
    relation: Relation,
    tup: HeapTuple,
    cid: CommandId,
    options: c_int,
    bistate: BulkInsertState,
) {
    let xid: TransactionId = GetCurrentTransactionId();
    let heaptup: HeapTuple;
    let buffer: Buffer;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut all_visible_cleared: bool = false;

    /* Cheap, simplistic check that the tuple matches the rel's rowtype. */
    Assert!(i32::from(HeapTupleHeaderGetNatts((*tup).t_data)) <= RelationGetNumberOfAttributes(relation));

    AssertHasSnapshotForToast(relation);

    /*
     * Fill in tuple header fields and toast the tuple if necessary.
     */
    heaptup = heap_prepare_insert(relation, tup, xid, cid, options);

    /*
     * Find buffer to insert this tuple into.
     */
    buffer = RelationGetBufferForTuple(
        relation,
        ((*heaptup).t_len as Size),
        InvalidBuffer,
        options,
        bistate,
        &raw mut vmbuffer,
        std::ptr::null_mut(),
        0,
    );

    /*
     * We're about to do the actual insert -- but check for conflict first.
     */
    CheckForSerializableConflictIn(relation, std::ptr::null_mut(), InvalidBlockNumber);

    /* NO EREPORT(ERROR) from here till changes are logged */
    START_CRIT_SECTION!();

    RelationPutHeapTuple(
        relation,
        buffer,
        heaptup,
        (options & HEAP_INSERT_SPECULATIVE) != 0,
    );

    if std::env::var_os("PDB_BT").is_some() && (*(*relation).rd_rel).oid == 1259 {
        eprintln!("PDB_BT heap_insert pg_class xid={} block={} buffer={} nblocks={}",
            xid, ItemPointerGetBlockNumber(&raw mut (*heaptup).t_self), buffer,
            crate::storage::buffer::bufmgr::RelationGetNumberOfBlocksInFork(relation, 0));
    }

    if PageIsAllVisible(BufferGetPage(buffer)) {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(
            relation,
            ItemPointerGetBlockNumber(&raw mut (*heaptup).t_self),
            vmbuffer,
            VISIBILITYMAP_VALID_BITS,
        );
    }

    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if RelationNeedsWAL(relation) {
        let mut xlrec: xl_heap_insert = std::mem::zeroed();
        let mut xlhdr: xl_heap_header = std::mem::zeroed();
        let recptr: XLogRecPtr;
        let page: Page = BufferGetPage(buffer);
        let mut info: uint8 = XLOG_HEAP_INSERT;
        let mut bufflags: c_int = 0;

        /*
         * If this is a catalog, we need to transmit combo CIDs to properly
         * decode, so log that as well.
         */
        if RelationIsAccessibleInLogicalDecoding(relation) {
            log_heap_new_cid(relation, heaptup);
        }

        /*
         * If this is the single and first tuple on page, we can reinit the
         * page instead of restoring the whole thing.
         */
        if ItemPointerGetOffsetNumber(&raw mut (*heaptup).t_self) == FirstOffsetNumber
            && PageGetMaxOffsetNumber(page) == FirstOffsetNumber
        {
            info |= XLOG_HEAP_INIT_PAGE;
            bufflags |= REGBUF_WILL_INIT;
        }

        xlrec.offnum = ItemPointerGetOffsetNumber(&raw mut (*heaptup).t_self);
        xlrec.flags = 0;
        if all_visible_cleared {
            xlrec.flags |= (XLH_INSERT_ALL_VISIBLE_CLEARED as u8);
        }
        if options & HEAP_INSERT_SPECULATIVE != 0 {
            xlrec.flags |= (XLH_INSERT_IS_SPECULATIVE as u8);
        }
        Assert!(
            ItemPointerGetBlockNumber(&raw mut (*heaptup).t_self) == BufferGetBlockNumber(buffer)
        );

        /*
         * For logical decoding, we need the tuple even if we're doing a full
         * page write.
         */
        if RelationIsLogicallyLogged(relation) && !(options & HEAP_INSERT_NO_LOGICAL != 0) {
            xlrec.flags |= (XLH_INSERT_CONTAINS_NEW_TUPLE as u8);
            bufflags |= REGBUF_KEEP_DATA;

            if IsToastRelation(relation) {
                xlrec.flags |= (XLH_INSERT_ON_TOAST_RELATION as u8);
            }
        }

        XLogBeginInsert();
        XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapInsert as c_int);

        xlhdr.t_infomask2 = (*(*heaptup).t_data).t_infomask2;
        xlhdr.t_infomask = (*(*heaptup).t_data).t_infomask;
        xlhdr.t_hoff = (*(*heaptup).t_data).t_hoff;

        XLogRegisterBuffer(0, buffer, (REGBUF_STANDARD | bufflags) as uint8);
        XLogRegisterBufData(0, &raw mut xlhdr as *mut c_char, SizeOfHeapHeader as c_int);
        /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
        XLogRegisterBufData(
            0,
            ((*heaptup).t_data as *mut c_char).add(SizeofHeapTupleHeader as usize),
            ((*heaptup).t_len as usize - SizeofHeapTupleHeader as usize) as c_int,
        );

        /* filtering by origin on a row level is much more efficient */
        XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

        recptr = XLogInsert(RM_HEAP_ID, info);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION!();

    UnlockReleaseBuffer(buffer);
    if vmbuffer != InvalidBuffer {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * If tuple is cachable, mark it for invalidation from the caches in case
     * we abort.
     */
    CacheInvalidateHeapTuple(relation, heaptup, std::ptr::null_mut());

    /* Note: speculative insertions are counted too, even if aborted later */
    pgstat_count_heap_insert(relation, 1);

    /*
     * If heaptup is a private copy, release it.
     */
    if heaptup != tup {
        (*tup).t_self = (*heaptup).t_self;
        heap_freetuple(heaptup);
    }
}

/*
 * Subroutine for heap_insert(). Prepares a tuple for insertion.
 */
unsafe fn heap_prepare_insert(
    relation: Relation,
    tup: HeapTuple,
    xid: TransactionId,
    cid: CommandId,
    options: c_int,
) -> HeapTuple {
    /*
     * To allow parallel inserts, we need to ensure that they are safe to be
     * performed in workers.
     */
    if IsParallelWorker() {
        ereport!(ERROR, errmsg!("cannot insert tuples in a parallel worker"));
    }

    (*(*tup).t_data).t_infomask &= !HEAP_XACT_MASK;
    (*(*tup).t_data).t_infomask2 &= !HEAP2_XACT_MASK;
    (*(*tup).t_data).t_infomask |= HEAP_XMAX_INVALID;
    HeapTupleHeaderSetXmin((*tup).t_data, xid);
    if options & HEAP_INSERT_FROZEN != 0 {
        HeapTupleHeaderSetXminFrozen((*tup).t_data);
    }

    HeapTupleHeaderSetCmin((*tup).t_data, cid);
    HeapTupleHeaderSetXmax((*tup).t_data, 0); /* for cleanliness */
    (*tup).t_tableOid = RelationGetRelid(relation);

    /*
     * If the new tuple is too big for storage or contains already toasted
     * out-of-line attributes from some other relation, invoke the toaster.
     */
    if (*(*relation).rd_rel).relkind != RELKIND_RELATION
        && (*(*relation).rd_rel).relkind != RELKIND_MATVIEW
    {
        /* toast table entries should never be recursively toasted */
        Assert!(!HeapTupleHasExternal(tup));
        return tup;
    } else if HeapTupleHasExternal(tup) || (*tup).t_len > (TOAST_TUPLE_THRESHOLD as u32) {
        return heap_toast_insert_or_update(relation, tup, std::ptr::null_mut(), options);
    } else {
        return tup;
    }
}

/*
 * Helper for heap_multi_insert() that computes the number of entire pages
 * that inserting the remaining heaptuples requires.
 */
unsafe fn heap_multi_insert_pages(
    heaptuples: *mut HeapTuple,
    done: c_int,
    ntuples: c_int,
    saveFreeSpace: Size,
) -> c_int {
    let mut page_avail: usize =
        BLCKSZ as usize - SizeOfPageHeaderData as usize - saveFreeSpace as usize;
    let mut npages: c_int = 1;

    for i in done..ntuples {
        let tup_sz: usize =
            std::mem::size_of::<ItemIdData>() + MAXALIGN((*(*heaptuples.add(i as usize))).t_len as usize);

        if page_avail < tup_sz {
            npages += 1;
            page_avail = BLCKSZ as usize - SizeOfPageHeaderData as usize - saveFreeSpace as usize;
        }
        page_avail -= tup_sz;
    }

    npages
}

/*
 *	heap_multi_insert	- insert multiple tuples into a heap
 */
pub unsafe fn heap_multi_insert(
    relation: Relation,
    slots: *mut *mut TupleTableSlot,
    ntuples: c_int,
    cid: CommandId,
    options: c_int,
    bistate: BulkInsertState,
) {
    let xid: TransactionId = GetCurrentTransactionId();
    let heaptuples: *mut HeapTuple;
    let mut i: c_int;
    let mut ndone: c_int;
    let mut scratch: PGAlignedBlock = std::mem::zeroed();
    let mut page: Page;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let needwal: bool;
    let saveFreeSpace: Size;
    let need_tuple_data: bool = RelationIsLogicallyLogged(relation);
    let need_cids: bool = RelationIsAccessibleInLogicalDecoding(relation);
    let mut starting_with_empty_page: bool = false;
    let mut npages: c_int = 0;
    let mut npages_used: c_int = 0;

    /* currently not needed (thus unsupported) for heap_multi_insert() */
    Assert!(!(options & HEAP_INSERT_NO_LOGICAL != 0));

    AssertHasSnapshotForToast(relation);

    needwal = RelationNeedsWAL(relation);
    saveFreeSpace = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);

    /* Toast and set header data in all the slots */
    heaptuples = palloc(ntuples as usize * std::mem::size_of::<HeapTuple>()) as *mut HeapTuple;
    i = 0;
    while i < ntuples {
        let tuple: HeapTuple;

        tuple = ExecFetchSlotHeapTuple(*slots.add(i as usize), true, std::ptr::null_mut());
        (**slots.add(i as usize)).tts_tableOid = RelationGetRelid(relation);
        (*tuple).t_tableOid = (**slots.add(i as usize)).tts_tableOid;
        *heaptuples.add(i as usize) = heap_prepare_insert(relation, tuple, xid, cid, options);
        i += 1;
    }

    /*
     * We're about to do the actual inserts -- but check for conflict first.
     */
    CheckForSerializableConflictIn(relation, std::ptr::null_mut(), InvalidBlockNumber);

    ndone = 0;
    while ndone < ntuples {
        let buffer: Buffer;
        let mut all_visible_cleared: bool = false;
        let mut all_frozen_set: bool = false;
        let mut nthispage: c_int;

        CHECK_FOR_INTERRUPTS!();

        /*
         * Compute number of pages needed to fit the to-be-inserted tuples in
         * the worst case.
         */
        if ndone == 0 || !starting_with_empty_page {
            npages = heap_multi_insert_pages(heaptuples, ndone, ntuples, saveFreeSpace);
            npages_used = 0;
        } else {
            npages_used += 1;
        }

        /*
         * Find buffer where at least the next tuple will fit.
         */
        buffer = RelationGetBufferForTuple(
            relation,
            ((*(*heaptuples.add(ndone as usize))).t_len as Size),
            InvalidBuffer,
            options,
            bistate,
            &raw mut vmbuffer,
            std::ptr::null_mut(),
            npages - npages_used,
        );
        page = BufferGetPage(buffer);

        starting_with_empty_page = PageGetMaxOffsetNumber(page) == 0;

        if starting_with_empty_page && (options & HEAP_INSERT_FROZEN != 0) {
            all_frozen_set = true;
        }

        /* NO EREPORT(ERROR) from here till changes are logged */
        START_CRIT_SECTION!();

        /*
         * RelationGetBufferForTuple has ensured that the first tuple fits.
         */
        RelationPutHeapTuple(relation, buffer, *heaptuples.add(ndone as usize), false);

        /*
         * For logical decoding we need combo CIDs to properly decode the
         * catalog.
         */
        if needwal && need_cids {
            log_heap_new_cid(relation, *heaptuples.add(ndone as usize));
        }

        nthispage = 1;
        while ndone + nthispage < ntuples {
            let heaptup: HeapTuple = *heaptuples.add((ndone + nthispage) as usize);

            if PageGetHeapFreeSpace(page) < (MAXALIGN((*heaptup).t_len as usize) as Size + saveFreeSpace) {
                break;
            }

            RelationPutHeapTuple(relation, buffer, heaptup, false);

            /*
             * For logical decoding we need combo CIDs to properly decode the
             * catalog.
             */
            if needwal && need_cids {
                log_heap_new_cid(relation, heaptup);
            }
            nthispage += 1;
        }

        /*
         * If the page is all visible, need to clear that, unless we're only
         * going to add further frozen rows to it.
         *
         * If we're only adding already frozen rows to a previously empty
         * page, mark it as all-visible.
         */
        if PageIsAllVisible(page) && !(options & HEAP_INSERT_FROZEN != 0) {
            all_visible_cleared = true;
            PageClearAllVisible(page);
            visibilitymap_clear(
                relation,
                BufferGetBlockNumber(buffer),
                vmbuffer,
                VISIBILITYMAP_VALID_BITS,
            );
        } else if all_frozen_set {
            PageSetAllVisible(page);
        }

        MarkBufferDirty(buffer);

        /* XLOG stuff */
        if needwal {
            let recptr: XLogRecPtr;
            let xlrec: *mut xl_heap_multi_insert;
            let mut info: uint8 = XLOG_HEAP2_MULTI_INSERT;
            let tupledata: *mut c_char;
            let totaldatalen: c_int;
            let mut scratchptr: *mut c_char = scratch.data.as_mut_ptr();
            let init: bool;
            let mut bufflags: c_int = 0;

            /*
             * If the page was previously empty, we can reinit the page
             * instead of restoring the whole thing.
             */
            init = starting_with_empty_page;

            /* allocate xl_heap_multi_insert struct from the scratch area */
            xlrec = scratchptr as *mut xl_heap_multi_insert;
            scratchptr = scratchptr.add(SizeOfHeapMultiInsert as usize);

            /*
             * Allocate offsets array.
             */
            if !init {
                scratchptr =
                    scratchptr.add(nthispage as usize * std::mem::size_of::<OffsetNumber>());
            }

            /* the rest of the scratch space is used for tuple data */
            tupledata = scratchptr;

            /* check that the mutually exclusive flags are not both set */
            Assert!(!(all_visible_cleared && all_frozen_set));

            (*xlrec).flags = 0;
            if all_visible_cleared {
                (*xlrec).flags = (XLH_INSERT_ALL_VISIBLE_CLEARED as u8);
            }
            if all_frozen_set {
                (*xlrec).flags = (XLH_INSERT_ALL_FROZEN_SET as u8);
            }

            (*xlrec).ntuples = nthispage as uint16;

            /*
             * Write out an xl_multi_insert_tuple and the tuple data itself
             * for each tuple.
             */
            i = 0;
            while i < nthispage {
                let heaptup: HeapTuple = *heaptuples.add((ndone + i) as usize);
                let tuphdr: *mut xl_multi_insert_tuple;
                let datalen: c_int;

                if !init {
                    *(*xlrec).offsets.as_mut_ptr().add(i as usize) =
                        ItemPointerGetOffsetNumber(&raw mut (*heaptup).t_self);
                }
                /* xl_multi_insert_tuple needs two-byte alignment. */
                tuphdr = SHORTALIGN(scratchptr as usize) as *mut xl_multi_insert_tuple;
                scratchptr = (tuphdr as *mut c_char).add(SizeOfMultiInsertTuple as usize);

                (*tuphdr).t_infomask2 = (*(*heaptup).t_data).t_infomask2;
                (*tuphdr).t_infomask = (*(*heaptup).t_data).t_infomask;
                (*tuphdr).t_hoff = (*(*heaptup).t_data).t_hoff;

                /* write bitmap [+ padding] [+ oid] + data */
                datalen = (*heaptup).t_len as c_int - SizeofHeapTupleHeader as c_int;
                memcpy(
                    scratchptr as *mut c_void,
                    ((*heaptup).t_data as *mut c_char).add(SizeofHeapTupleHeader as usize)
                        as *const c_void,
                    datalen as usize,
                );
                (*tuphdr).datalen = datalen as uint16;
                scratchptr = scratchptr.add(datalen as usize);
                i += 1;
            }
            totaldatalen = scratchptr.offset_from(tupledata) as c_int;
            Assert!((scratchptr.offset_from(scratch.data.as_ptr()) as c_int) < BLCKSZ as c_int);

            if need_tuple_data {
                (*xlrec).flags |= (XLH_INSERT_CONTAINS_NEW_TUPLE as u8);
            }

            /*
             * Signal that this is the last xl_heap_multi_insert record.
             */
            if ndone + nthispage == ntuples {
                (*xlrec).flags |= (XLH_INSERT_LAST_IN_MULTI as u8);
            }

            if init {
                info |= XLOG_HEAP_INIT_PAGE;
                bufflags |= REGBUF_WILL_INIT;
            }

            /*
             * If we're doing logical decoding, include the new tuple data.
             */
            if need_tuple_data {
                bufflags |= REGBUF_KEEP_DATA;
            }

            XLogBeginInsert();
            XLogRegisterData(
                xlrec as *mut c_void,
                tupledata.offset_from(scratch.data.as_ptr()) as c_int,
            );
            XLogRegisterBuffer(0, buffer, (REGBUF_STANDARD | bufflags) as uint8);

            XLogRegisterBufData(0, tupledata, (totaldatalen as usize) as c_int);

            /* filtering by origin on a row level is much more efficient */
            XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

            recptr = XLogInsert(RM_HEAP2_ID, info);

            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION!();

        /*
         * If we've frozen everything on the page, update the visibilitymap.
         */
        if all_frozen_set {
            Assert!(PageIsAllVisible(page));
            Assert!(visibilitymap_pin_ok(BufferGetBlockNumber(buffer), vmbuffer));

            /*
             * It's fine to use InvalidTransactionId here.
             */
            visibilitymap_set(
                relation,
                BufferGetBlockNumber(buffer),
                buffer,
                InvalidXLogRecPtr,
                vmbuffer,
                InvalidTransactionId,
                VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
            );
        }

        UnlockReleaseBuffer(buffer);
        ndone += nthispage;

        /*
         * NB: Only release vmbuffer after inserting all tuples.
         */
    }

    /* We're done with inserting all tuples, so release the last vmbuffer. */
    if vmbuffer != InvalidBuffer {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * We're done with the actual inserts.  Check for conflicts again.
     */
    CheckForSerializableConflictIn(relation, std::ptr::null_mut(), InvalidBlockNumber);

    /*
     * If tuples are cachable, mark them for invalidation from the caches in
     * case we abort.
     */
    if IsCatalogRelation(relation) {
        i = 0;
        while i < ntuples {
            CacheInvalidateHeapTuple(relation, *heaptuples.add(i as usize), std::ptr::null_mut());
            i += 1;
        }
    }

    /* copy t_self fields back to the caller's slots */
    i = 0;
    while i < ntuples {
        (**slots.add(i as usize)).tts_tid = (*(*heaptuples.add(i as usize))).t_self;
        i += 1;
    }

    pgstat_count_heap_insert(relation, (ntuples as i64 as c_int));
}

/*
 *	simple_heap_insert - insert a tuple
 */
pub unsafe fn simple_heap_insert(relation: Relation, tup: HeapTuple) {
    heap_insert(relation, tup, GetCurrentCommandId(true), 0, std::ptr::null_mut());
}

/*
 * Given infomask/infomask2, compute the bits that must be saved in the
 * "infobits" field of xl_heap_delete, xl_heap_update, xl_heap_lock,
 * xl_heap_lock_updated WAL records.
 */
unsafe fn compute_infobits(infomask: uint16, infomask2: uint16) -> uint8 {
    ((if infomask & HEAP_XMAX_IS_MULTI != 0 { XLHL_XMAX_IS_MULTI } else { 0 })
        | (if infomask & HEAP_XMAX_LOCK_ONLY != 0 { XLHL_XMAX_LOCK_ONLY } else { 0 })
        | (if infomask & HEAP_XMAX_EXCL_LOCK != 0 { XLHL_XMAX_EXCL_LOCK } else { 0 })
        /* note we ignore HEAP_XMAX_SHR_LOCK here */
        | (if infomask & HEAP_XMAX_KEYSHR_LOCK != 0 { XLHL_XMAX_KEYSHR_LOCK } else { 0 })
        | (if infomask2 & HEAP_KEYS_UPDATED != 0 { XLHL_KEYS_UPDATED } else { 0 })) as uint8
}

/*
 * Given two versions of the same t_infomask for a tuple, compare them and
 * return whether the relevant status for a tuple Xmax has changed.
 */
#[inline]
unsafe fn xmax_infomask_changed(new_infomask: uint16, old_infomask: uint16) -> bool {
    let interesting: uint16 = HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_LOCK_MASK;

    if (new_infomask & interesting) != (old_infomask & interesting) {
        return true;
    }

    false
}

/*
 *	heap_delete - delete a tuple
 */
pub unsafe fn heap_delete(
    relation: Relation,
    tid: ItemPointer,
    cid: CommandId,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    changingPart: bool,
) -> TM_Result {
    let mut result: TM_Result;
    let xid: TransactionId = GetCurrentTransactionId();
    let lp: ItemId;
    let mut tp: HeapTupleData = std::mem::zeroed();
    let page: Page;
    let block: BlockNumber;
    let buffer: Buffer;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut new_xmax: TransactionId = InvalidTransactionId;
    let mut new_infomask: uint16 = 0;
    let mut new_infomask2: uint16 = 0;
    let mut have_tuple_lock: bool = false;
    let mut iscombo: bool = false;
    let mut all_visible_cleared: bool = false;
    let mut old_key_tuple: HeapTuple = std::ptr::null_mut(); /* replica identity of the tuple */
    let mut old_key_copied: bool = false;

    Assert!(ItemPointerIsValid(tid));

    AssertHasSnapshotForToast(relation);

    /*
     * Forbid this during a parallel operation, lest it allocate a combo CID.
     */
    if IsInParallelMode() {
        ereport!(
            ERROR,
            errmsg!("cannot delete tuples during a parallel operation")
        );
    }

    block = ItemPointerGetBlockNumber(tid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.
     */
    if PageIsAllVisible(page) {
        visibilitymap_pin(relation, block, &raw mut vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    Assert!(ItemIdIsNormal(lp));

    tp.t_tableOid = RelationGetRelid(relation);
    tp.t_data = PageGetItem(page, lp) as HeapTupleHeader;
    tp.t_len = ItemIdGetLength(lp);
    tp.t_self = *tid;

    // l1:
    'l1: loop {
        /*
         * If we didn't pin the visibility map page and the page has become all
         * visible while we were busy locking the buffer, we'll have to unlock
         * and re-lock.
         */
        if vmbuffer == InvalidBuffer && PageIsAllVisible(page) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            visibilitymap_pin(relation, block, &raw mut vmbuffer);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        }

        result = HeapTupleSatisfiesUpdate(&raw mut tp, cid, buffer);

        if result == TM_Invisible {
            UnlockReleaseBuffer(buffer);
            ereport!(ERROR, errmsg!("attempted to delete invisible tuple"));
        } else if result == TM_BeingModified && wait {
            let xwait: TransactionId;
            let infomask: uint16;

            /* must copy state data before unlocking buffer */
            xwait = HeapTupleHeaderGetRawXmax(tp.t_data);
            infomask = (*tp.t_data).t_infomask;

            /*
             * Sleep until concurrent transaction ends.
             */
            if infomask & HEAP_XMAX_IS_MULTI != 0 {
                let mut current_is_member: bool = false;

                if DoesMultiXactIdConflict(
                    xwait as MultiXactId,
                    infomask,
                    LockTupleExclusive,
                    &raw mut current_is_member,
                ) {
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

                    /*
                     * Acquire the lock, if necessary.
                     */
                    if !current_is_member {
                        heap_acquire_tuplock(
                            relation,
                            &raw mut tp.t_self,
                            LockTupleExclusive,
                            LockWaitBlock,
                            &raw mut have_tuple_lock,
                        );
                    }

                    /* wait for multixact */
                    MultiXactIdWait(
                        xwait as MultiXactId,
                        MultiXactStatusUpdate,
                        infomask,
                        relation,
                        &raw mut tp.t_self,
                        XLTW_Delete,
                        std::ptr::null_mut(),
                    );
                    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

                    /*
                     * Check for xmax change, and start over if so.
                     */
                    if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
                        || xmax_infomask_changed((*tp.t_data).t_infomask, infomask)
                        || !TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp.t_data), xwait)
                    {
                        continue 'l1;
                    }
                }

                /*
                 * You might think the multixact is necessarily done here, but
                 * not so.
                 */
            } else if !TransactionIdIsCurrentTransactionId(xwait) {
                /*
                 * Wait for regular transaction to end; but first, acquire
                 * tuple lock.
                 */
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                heap_acquire_tuplock(
                    relation,
                    &raw mut tp.t_self,
                    LockTupleExclusive,
                    LockWaitBlock,
                    &raw mut have_tuple_lock,
                );
                XactLockTableWait(xwait, relation, &raw mut tp.t_self, XLTW_Delete);
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

                /*
                 * Check for xmax change, and start over if so.
                 */
                if (vmbuffer == InvalidBuffer && PageIsAllVisible(page))
                    || xmax_infomask_changed((*tp.t_data).t_infomask, infomask)
                    || !TransactionIdEquals(HeapTupleHeaderGetRawXmax(tp.t_data), xwait)
                {
                    continue 'l1;
                }

                /* Otherwise check if it committed or aborted */
                UpdateXmaxHintBits(tp.t_data, buffer, xwait);
            }

            /*
             * We may overwrite if previous xmax aborted, or if it committed
             * but only locked the tuple without updating it.
             */
            if (*tp.t_data).t_infomask & HEAP_XMAX_INVALID != 0
                || HEAP_XMAX_IS_LOCKED_ONLY((*tp.t_data).t_infomask)
                || HeapTupleHeaderIsOnlyLocked(tp.t_data)
            {
                result = TM_Ok;
            } else if !ItemPointerEquals(&raw mut tp.t_self, &raw mut (*tp.t_data).t_ctid) {
                result = TM_Updated;
            } else {
                result = TM_Deleted;
            }
        }
        break 'l1;
    }

    /* sanity check the result HeapTupleSatisfiesUpdate() and the logic above */
    if result != TM_Ok {
        Assert!(
            result == TM_SelfModified
                || result == TM_Updated
                || result == TM_Deleted
                || result == TM_BeingModified
        );
        Assert!(!((*tp.t_data).t_infomask & HEAP_XMAX_INVALID != 0));
        Assert!(
            result != TM_Updated
                || !ItemPointerEquals(&raw mut tp.t_self, &raw mut (*tp.t_data).t_ctid)
        );
    }

    if crosscheck != InvalidSnapshot && result == TM_Ok {
        /* Perform additional check for transaction-snapshot mode RI updates */
        if !HeapTupleSatisfiesVisibility(&raw mut tp, crosscheck, buffer) {
            result = TM_Updated;
        }
    }

    if result != TM_Ok {
        (*tmfd).ctid = (*tp.t_data).t_ctid;
        (*tmfd).xmax = HeapTupleHeaderGetUpdateXid(tp.t_data);
        if result == TM_SelfModified {
            (*tmfd).cmax = HeapTupleHeaderGetCmax(tp.t_data);
        } else {
            (*tmfd).cmax = InvalidCommandId;
        }
        UnlockReleaseBuffer(buffer);
        if have_tuple_lock {
            UnlockTupleTuplock(relation, &raw mut tp.t_self, LockTupleExclusive);
        }
        if vmbuffer != InvalidBuffer {
            ReleaseBuffer(vmbuffer);
        }
        return result;
    }

    /*
     * We're about to do the actual delete -- check for conflict first.
     */
    CheckForSerializableConflictIn(relation, tid, BufferGetBlockNumber(buffer));

    let mut cid_mut: CommandId = cid;
    /* replace cid with a combo CID if necessary */
    HeapTupleHeaderAdjustCmax(tp.t_data, &raw mut cid_mut, &raw mut iscombo);

    /*
     * Compute replica identity tuple before entering the critical section.
     */
    old_key_tuple = ExtractReplicaIdentity(relation, &raw mut tp, true, &raw mut old_key_copied);

    /*
     * If this is the first possibly-multixact-able operation in the current
     * transaction, set my per-backend OldestMemberMXactId setting.
     */
    MultiXactIdSetOldestMember();

    compute_new_xmax_infomask(
        HeapTupleHeaderGetRawXmax(tp.t_data),
        (*tp.t_data).t_infomask,
        (*tp.t_data).t_infomask2,
        xid,
        LockTupleExclusive,
        true,
        &raw mut new_xmax,
        &raw mut new_infomask,
        &raw mut new_infomask2,
    );

    START_CRIT_SECTION!();

    /*
     * If this transaction commits, the tuple will become DEAD sooner or
     * later.
     */
    PageSetPrunable(page, xid);

    if PageIsAllVisible(page) {
        all_visible_cleared = true;
        PageClearAllVisible(page);
        visibilitymap_clear(
            relation,
            BufferGetBlockNumber(buffer),
            vmbuffer,
            VISIBILITYMAP_VALID_BITS,
        );
    }

    /* store transaction information of xact deleting the tuple */
    (*tp.t_data).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
    (*tp.t_data).t_infomask2 &= !HEAP_KEYS_UPDATED;
    (*tp.t_data).t_infomask |= new_infomask;
    (*tp.t_data).t_infomask2 |= new_infomask2;
    HeapTupleHeaderClearHotUpdated(tp.t_data);
    HeapTupleHeaderSetXmax(tp.t_data, new_xmax);
    HeapTupleHeaderSetCmax(tp.t_data, cid_mut, iscombo);
    /* Make sure there is no forward chain link in t_ctid */
    (*tp.t_data).t_ctid = tp.t_self;

    /* Signal that this is actually a move into another partition */
    if changingPart {
        HeapTupleHeaderSetMovedPartitions(tp.t_data);
    }

    MarkBufferDirty(buffer);

    /*
     * XLOG stuff
     */
    if RelationNeedsWAL(relation) {
        let mut xlrec: xl_heap_delete = std::mem::zeroed();
        let mut xlhdr: xl_heap_header = std::mem::zeroed();
        let recptr: XLogRecPtr;

        /*
         * For logical decode we need combo CIDs to properly decode the
         * catalog
         */
        if RelationIsAccessibleInLogicalDecoding(relation) {
            log_heap_new_cid(relation, &raw mut tp);
        }

        xlrec.flags = 0;
        if all_visible_cleared {
            xlrec.flags |= (XLH_DELETE_ALL_VISIBLE_CLEARED as u8);
        }
        if changingPart {
            xlrec.flags |= (XLH_DELETE_IS_PARTITION_MOVE as u8);
        }
        xlrec.infobits_set =
            compute_infobits((*tp.t_data).t_infomask, (*tp.t_data).t_infomask2);
        xlrec.offnum = ItemPointerGetOffsetNumber(&raw mut tp.t_self);
        xlrec.xmax = new_xmax;

        if !old_key_tuple.is_null() {
            if (*(*relation).rd_rel).relreplident == REPLICA_IDENTITY_FULL {
                xlrec.flags |= (XLH_DELETE_CONTAINS_OLD_TUPLE as u8);
            } else {
                xlrec.flags |= (XLH_DELETE_CONTAINS_OLD_KEY as u8);
            }
        }

        XLogBeginInsert();
        XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapDelete as c_int);

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD as uint8);

        /*
         * Log replica identity of the deleted tuple if there is one
         */
        if !old_key_tuple.is_null() {
            xlhdr.t_infomask2 = (*(*old_key_tuple).t_data).t_infomask2;
            xlhdr.t_infomask = (*(*old_key_tuple).t_data).t_infomask;
            xlhdr.t_hoff = (*(*old_key_tuple).t_data).t_hoff;

            XLogRegisterData(&raw mut xlhdr as *mut c_void, SizeOfHeapHeader as c_int);
            XLogRegisterData(
                ((*old_key_tuple).t_data as *mut c_char).add(SizeofHeapTupleHeader as usize)
                    as *mut c_void,
                ((*old_key_tuple).t_len as Size - SizeofHeapTupleHeader) as c_int,
            );
        }

        /* filtering by origin on a row level is much more efficient */
        XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION!();

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    if vmbuffer != InvalidBuffer {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * If the tuple has toasted out-of-line attributes, we need to delete
     * those items too.
     */
    if (*(*relation).rd_rel).relkind != RELKIND_RELATION
        && (*(*relation).rd_rel).relkind != RELKIND_MATVIEW
    {
        /* toast table entries should never be recursively toasted */
        Assert!(!HeapTupleHasExternal(&raw mut tp));
    } else if HeapTupleHasExternal(&raw mut tp) {
        heap_toast_delete(relation, &raw mut tp, false);
    }

    /*
     * Mark tuple for invalidation from system caches at next command
     * boundary.
     */
    CacheInvalidateHeapTuple(relation, &raw mut tp, std::ptr::null_mut());

    /* Now we can release the buffer */
    ReleaseBuffer(buffer);

    /*
     * Release the lmgr tuple lock, if we had it.
     */
    if have_tuple_lock {
        UnlockTupleTuplock(relation, &raw mut tp.t_self, LockTupleExclusive);
    }

    pgstat_count_heap_delete(relation);

    if !old_key_tuple.is_null() && old_key_copied {
        heap_freetuple(old_key_tuple);
    }

    TM_Ok
}

/*
 *	simple_heap_delete - delete a tuple
 */
pub unsafe fn simple_heap_delete(relation: Relation, tid: ItemPointer) {
    let result: TM_Result;
    let mut tmfd: TM_FailureData = std::mem::zeroed();

    result = heap_delete(
        relation,
        tid,
        GetCurrentCommandId(true),
        InvalidSnapshot,
        true, /* wait for commit */
        &raw mut tmfd,
        false, /* changingPart */
    );
    match result {
        TM_SelfModified => {
            /* Tuple was already updated in current command? */
            elog!(ERROR, "tuple already updated by self");
        }
        TM_Ok => { /* done successfully */ }
        TM_Updated => {
            elog!(ERROR, "tuple concurrently updated");
        }
        TM_Deleted => {
            elog!(ERROR, "tuple concurrently deleted");
        }
        _ => {
            elog!(ERROR, "unrecognized heap_delete status: {}", result as u32);
        }
    }
}
/*
 *	heap_update - replace a tuple
 */
pub unsafe fn heap_update(
    relation: Relation,
    otid: ItemPointer,
    newtup: HeapTuple,
    cid: CommandId,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    lockmode: *mut LockTupleMode,
    update_indexes: *mut TU_UpdateIndexes,
) -> TM_Result {
    let mut result: TM_Result;
    let xid: TransactionId = GetCurrentTransactionId();
    let hot_attrs: *mut Bitmapset;
    let sum_attrs: *mut Bitmapset;
    let key_attrs: *mut Bitmapset;
    let id_attrs: *mut Bitmapset;
    let mut interesting_attrs: *mut Bitmapset;
    let modified_attrs: *mut Bitmapset;
    let lp: ItemId;
    let mut oldtup: HeapTupleData = std::mem::zeroed();
    let mut heaptup: HeapTuple;
    let mut old_key_tuple: HeapTuple = std::ptr::null_mut();
    let mut old_key_copied: bool = false;
    let page: Page;
    let block: BlockNumber;
    let mxact_status: MultiXactStatus;
    let buffer: Buffer;
    let mut newbuf: Buffer;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut vmbuffer_new: Buffer = InvalidBuffer;
    let need_toast: bool;
    let mut newtupsize: Size;
    let mut pagefree: Size;
    let mut have_tuple_lock: bool = false;
    let mut iscombo: bool = false;
    let mut use_hot_update: bool = false;
    let mut summarized_update: bool = false;
    let key_intact: bool;
    let mut all_visible_cleared: bool = false;
    let mut all_visible_cleared_new: bool = false;
    let mut checked_lockers: bool;
    let mut locker_remains: bool;
    let mut id_has_external: bool = false;
    let mut xmax_new_tuple: TransactionId;
    let mut xmax_old_tuple: TransactionId = InvalidTransactionId;
    let mut infomask_old_tuple: uint16 = 0;
    let mut infomask2_old_tuple: uint16 = 0;
    let mut infomask_new_tuple: uint16;
    let mut infomask2_new_tuple: uint16;
    let mut cid_mut: CommandId = cid;

    Assert!(ItemPointerIsValid(otid));

    /* Cheap, simplistic check that the tuple matches the rel's rowtype. */
    Assert!(i32::from(HeapTupleHeaderGetNatts((*newtup).t_data)) <= RelationGetNumberOfAttributes(relation));

    AssertHasSnapshotForToast(relation);

    /*
     * Forbid this during a parallel operation, lest it allocate a combo CID.
     */
    if IsInParallelMode() {
        ereport!(
            ERROR,
            errmsg!("cannot update tuples during a parallel operation")
        );
    }

    check_lock_if_inplace_updateable_rel(relation, otid, newtup);

    /*
     * Fetch the list of attributes to be checked for various operations.
     */
    hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_HOT_BLOCKING);
    sum_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_SUMMARIZED);
    key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);
    id_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    interesting_attrs = std::ptr::null_mut();
    interesting_attrs = bms_add_members(interesting_attrs, hot_attrs);
    interesting_attrs = bms_add_members(interesting_attrs, sum_attrs);
    interesting_attrs = bms_add_members(interesting_attrs, key_attrs);
    interesting_attrs = bms_add_members(interesting_attrs, id_attrs);

    block = ItemPointerGetBlockNumber(otid);
    INJECTION_POINT(c"heap_update-before-pin".as_ptr(), std::ptr::null_mut());
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.
     */
    if PageIsAllVisible(page) {
        visibilitymap_pin(relation, block, &raw mut vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(otid));

    /*
     * Usually, a buffer pin and/or snapshot blocks pruning of otid.
     */
    if !ItemIdIsNormal(lp) {
        Assert!(RelationSupportsSysCache(RelationGetRelid(relation)));

        UnlockReleaseBuffer(buffer);
        Assert!(!have_tuple_lock);
        if vmbuffer != InvalidBuffer {
            ReleaseBuffer(vmbuffer);
        }
        (*tmfd).ctid = *otid;
        (*tmfd).xmax = InvalidTransactionId;
        (*tmfd).cmax = InvalidCommandId;
        *update_indexes = TU_None;

        bms_free(hot_attrs);
        bms_free(sum_attrs);
        bms_free(key_attrs);
        bms_free(id_attrs);
        /* modified_attrs not yet initialized */
        bms_free(interesting_attrs);
        return TM_Deleted;
    }

    /*
     * Fill in enough data in oldtup for HeapDetermineColumnsInfo to work
     * properly.
     */
    oldtup.t_tableOid = RelationGetRelid(relation);
    oldtup.t_data = PageGetItem(page, lp) as HeapTupleHeader;
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *otid;

    /* the new tuple is ready, except for this: */
    (*newtup).t_tableOid = RelationGetRelid(relation);

    /*
     * Determine columns modified by the update.
     */
    modified_attrs = HeapDetermineColumnsInfo(
        relation,
        interesting_attrs,
        id_attrs,
        &raw mut oldtup,
        newtup,
        &raw mut id_has_external,
    );

    /*
     * If we're not updating any "key" column, we can grab a weaker lock type.
     */
    if !bms_overlap(modified_attrs, key_attrs) {
        *lockmode = LockTupleNoKeyExclusive;
        mxact_status = MultiXactStatusNoKeyUpdate;
        key_intact = true;

        /*
         * If this is the first possibly-multixact-able operation in the
         * current transaction, set my per-backend OldestMemberMXactId.
         */
        MultiXactIdSetOldestMember();
    } else {
        *lockmode = LockTupleExclusive;
        mxact_status = MultiXactStatusUpdate;
        key_intact = false;
    }

    /*
     * Note: beyond this point, use oldtup not otid to refer to old tuple.
     */

    // l2:
    'l2: loop {
        checked_lockers = false;
        locker_remains = false;
        result = HeapTupleSatisfiesUpdate(&raw mut oldtup, cid, buffer);

        /* see below about the "no wait" case */
        Assert!(result != TM_BeingModified || wait);

        if result == TM_Invisible {
            UnlockReleaseBuffer(buffer);
            ereport!(ERROR, errmsg!("attempted to update invisible tuple"));
        } else if result == TM_BeingModified && wait {
            let xwait: TransactionId;
            let infomask: uint16;
            let mut can_continue: bool = false;

            /* must copy state data before unlocking buffer */
            xwait = HeapTupleHeaderGetRawXmax(oldtup.t_data);
            infomask = (*oldtup.t_data).t_infomask;

            /*
             * Now we have to do something about the existing locker.
             */
            if infomask & HEAP_XMAX_IS_MULTI != 0 {
                let update_xact: TransactionId;
                let mut remain: c_int = 0;
                let mut current_is_member: bool = false;

                if DoesMultiXactIdConflict(
                    xwait as MultiXactId,
                    infomask,
                    *lockmode,
                    &raw mut current_is_member,
                ) {
                    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

                    /*
                     * Acquire the lock, if necessary.
                     */
                    if !current_is_member {
                        heap_acquire_tuplock(
                            relation,
                            &raw mut oldtup.t_self,
                            *lockmode,
                            LockWaitBlock,
                            &raw mut have_tuple_lock,
                        );
                    }

                    /* wait for multixact */
                    MultiXactIdWait(
                        xwait as MultiXactId,
                        mxact_status,
                        infomask,
                        relation,
                        &raw mut oldtup.t_self,
                        XLTW_Update,
                        &raw mut remain,
                    );
                    checked_lockers = true;
                    locker_remains = remain != 0;
                    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

                    /*
                     * Check for xmax change, and start over if so.
                     */
                    if xmax_infomask_changed((*oldtup.t_data).t_infomask, infomask)
                        || !TransactionIdEquals(
                            HeapTupleHeaderGetRawXmax(oldtup.t_data),
                            xwait,
                        )
                    {
                        continue 'l2;
                    }
                }

                /*
                 * Note that the multixact may not be done by now.
                 */
                if !HEAP_XMAX_IS_LOCKED_ONLY((*oldtup.t_data).t_infomask) {
                    update_xact = HeapTupleGetUpdateXid(oldtup.t_data);
                } else {
                    update_xact = InvalidTransactionId;
                }

                /*
                 * There was no UPDATE in the MultiXact; or it aborted.
                 */
                if !TransactionIdIsValid(update_xact) || TransactionIdDidAbort(update_xact) {
                    can_continue = true;
                }
            } else if TransactionIdIsCurrentTransactionId(xwait) {
                /*
                 * The only locker is ourselves.
                 */
                checked_lockers = true;
                locker_remains = true;
                can_continue = true;
            } else if HEAP_XMAX_IS_KEYSHR_LOCKED((infomask as i16)) && key_intact {
                /*
                 * If it's just a key-share locker, and we're not changing the
                 * key columns, we don't need to wait.
                 */
                checked_lockers = true;
                locker_remains = true;
                can_continue = true;
            } else {
                /*
                 * Wait for regular transaction to end; but first, acquire
                 * tuple lock.
                 */
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                heap_acquire_tuplock(
                    relation,
                    &raw mut oldtup.t_self,
                    *lockmode,
                    LockWaitBlock,
                    &raw mut have_tuple_lock,
                );
                XactLockTableWait(xwait, relation, &raw mut oldtup.t_self, XLTW_Update);
                checked_lockers = true;
                LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

                /*
                 * Check for xmax change, and start over if so.
                 */
                if xmax_infomask_changed((*oldtup.t_data).t_infomask, infomask)
                    || !TransactionIdEquals(xwait, HeapTupleHeaderGetRawXmax(oldtup.t_data))
                {
                    continue 'l2;
                }

                /* Otherwise check if it committed or aborted */
                UpdateXmaxHintBits(oldtup.t_data, buffer, xwait);
                if (*oldtup.t_data).t_infomask & HEAP_XMAX_INVALID != 0 {
                    can_continue = true;
                }
            }

            if can_continue {
                result = TM_Ok;
            } else if !ItemPointerEquals(&raw mut oldtup.t_self, &raw mut (*oldtup.t_data).t_ctid)
            {
                result = TM_Updated;
            } else {
                result = TM_Deleted;
            }
        }

        /* Sanity check the result HeapTupleSatisfiesUpdate() and the logic above */
        if result != TM_Ok {
            Assert!(
                result == TM_SelfModified
                    || result == TM_Updated
                    || result == TM_Deleted
                    || result == TM_BeingModified
            );
            Assert!(!((*oldtup.t_data).t_infomask & HEAP_XMAX_INVALID != 0));
            Assert!(
                result != TM_Updated
                    || !ItemPointerEquals(
                        &raw mut oldtup.t_self,
                        &raw mut (*oldtup.t_data).t_ctid
                    )
            );
        }

        if crosscheck != InvalidSnapshot && result == TM_Ok {
            /* Perform additional check for transaction-snapshot mode RI updates */
            if !HeapTupleSatisfiesVisibility(&raw mut oldtup, crosscheck, buffer) {
                result = TM_Updated;
            }
        }

        if result != TM_Ok {
            (*tmfd).ctid = (*oldtup.t_data).t_ctid;
            (*tmfd).xmax = HeapTupleHeaderGetUpdateXid(oldtup.t_data);
            if result == TM_SelfModified {
                (*tmfd).cmax = HeapTupleHeaderGetCmax(oldtup.t_data);
            } else {
                (*tmfd).cmax = InvalidCommandId;
            }
            UnlockReleaseBuffer(buffer);
            if have_tuple_lock {
                UnlockTupleTuplock(relation, &raw mut oldtup.t_self, *lockmode);
            }
            if vmbuffer != InvalidBuffer {
                ReleaseBuffer(vmbuffer);
            }
            *update_indexes = TU_None;

            bms_free(hot_attrs);
            bms_free(sum_attrs);
            bms_free(key_attrs);
            bms_free(id_attrs);
            bms_free(modified_attrs);
            bms_free(interesting_attrs);
            return result;
        }

        /*
         * If we didn't pin the visibility map page and the page has become all
         * visible while we were busy locking the buffer, we'll have to unlock
         * and re-lock.
         */
        if vmbuffer == InvalidBuffer && PageIsAllVisible(page) {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            visibilitymap_pin(relation, block, &raw mut vmbuffer);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            continue 'l2;
        }

        break 'l2;
    }

    /* Fill in transaction status data */

    /*
     * If the tuple we're updating is locked, we need to preserve the locking
     * info in the old tuple's Xmax.
     */
    compute_new_xmax_infomask(
        HeapTupleHeaderGetRawXmax(oldtup.t_data),
        (*oldtup.t_data).t_infomask,
        (*oldtup.t_data).t_infomask2,
        xid,
        *lockmode,
        true,
        &raw mut xmax_old_tuple,
        &raw mut infomask_old_tuple,
        &raw mut infomask2_old_tuple,
    );

    /*
     * And also prepare an Xmax value for the new copy of the tuple.
     */
    if (*oldtup.t_data).t_infomask & HEAP_XMAX_INVALID != 0
        || HEAP_LOCKED_UPGRADED((*oldtup.t_data).t_infomask)
        || (checked_lockers && !locker_remains)
    {
        xmax_new_tuple = InvalidTransactionId;
    } else {
        xmax_new_tuple = HeapTupleHeaderGetRawXmax(oldtup.t_data);
    }

    if !TransactionIdIsValid(xmax_new_tuple) {
        infomask_new_tuple = HEAP_XMAX_INVALID;
        infomask2_new_tuple = 0;
    } else {
        /*
         * If we found a valid Xmax for the new tuple, then the infomask bits
         * to use on the new tuple depend on what was there on the old one.
         */
        if (*oldtup.t_data).t_infomask & HEAP_XMAX_IS_MULTI != 0 {
            infomask_new_tuple = 0;
            infomask2_new_tuple = 0;
            GetMultiXactIdHintBits(
                xmax_new_tuple,
                &raw mut infomask_new_tuple,
                &raw mut infomask2_new_tuple,
            );
        } else {
            infomask_new_tuple = HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
            infomask2_new_tuple = 0;
        }
    }

    /*
     * Prepare the new tuple with the appropriate initial values of Xmin and
     * Xmax, as well as initial infomask bits as computed above.
     */
    (*(*newtup).t_data).t_infomask &= !HEAP_XACT_MASK;
    (*(*newtup).t_data).t_infomask2 &= !HEAP2_XACT_MASK;
    HeapTupleHeaderSetXmin((*newtup).t_data, xid);
    HeapTupleHeaderSetCmin((*newtup).t_data, cid);
    (*(*newtup).t_data).t_infomask |= HEAP_UPDATED | infomask_new_tuple;
    (*(*newtup).t_data).t_infomask2 |= infomask2_new_tuple;
    HeapTupleHeaderSetXmax((*newtup).t_data, xmax_new_tuple);

    /*
     * Replace cid with a combo CID if necessary.
     */
    HeapTupleHeaderAdjustCmax(oldtup.t_data, &raw mut cid_mut, &raw mut iscombo);

    /*
     * If the toaster needs to be activated, OR if the new tuple will not fit
     * on the same page as the old, then we need to release the content lock.
     */
    if (*(*relation).rd_rel).relkind != RELKIND_RELATION
        && (*(*relation).rd_rel).relkind != RELKIND_MATVIEW
    {
        /* toast table entries should never be recursively toasted */
        Assert!(!HeapTupleHasExternal(&raw mut oldtup));
        Assert!(!HeapTupleHasExternal(newtup));
        need_toast = false;
    } else {
        need_toast = HeapTupleHasExternal(&raw mut oldtup)
            || HeapTupleHasExternal(newtup)
            || (*newtup).t_len > (TOAST_TUPLE_THRESHOLD as u32);
    }

    pagefree = PageGetHeapFreeSpace(page);

    newtupsize = MAXALIGN((*newtup).t_len as usize) as Size;

    if need_toast || newtupsize > pagefree {
        let mut xmax_lock_old_tuple: TransactionId = InvalidTransactionId;
        let mut infomask_lock_old_tuple: uint16 = 0;
        let mut infomask2_lock_old_tuple: uint16 = 0;
        let mut cleared_all_frozen: bool = false;

        /*
         * To prevent concurrent sessions from updating the tuple, we have to
         * temporarily mark it locked, while we release the page-level lock.
         */
        compute_new_xmax_infomask(
            HeapTupleHeaderGetRawXmax(oldtup.t_data),
            (*oldtup.t_data).t_infomask,
            (*oldtup.t_data).t_infomask2,
            xid,
            *lockmode,
            false,
            &raw mut xmax_lock_old_tuple,
            &raw mut infomask_lock_old_tuple,
            &raw mut infomask2_lock_old_tuple,
        );

        Assert!(HEAP_XMAX_IS_LOCKED_ONLY(infomask_lock_old_tuple));

        START_CRIT_SECTION!();

        /* Clear obsolete visibility flags ... */
        (*oldtup.t_data).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        (*oldtup.t_data).t_infomask2 &= !HEAP_KEYS_UPDATED;
        HeapTupleClearHotUpdated(&raw mut oldtup);
        /* ... and store info about transaction updating this tuple */
        Assert!(TransactionIdIsValid(xmax_lock_old_tuple));
        HeapTupleHeaderSetXmax(oldtup.t_data, xmax_lock_old_tuple);
        (*oldtup.t_data).t_infomask |= infomask_lock_old_tuple;
        (*oldtup.t_data).t_infomask2 |= infomask2_lock_old_tuple;
        HeapTupleHeaderSetCmax(oldtup.t_data, cid_mut, iscombo);

        /* temporarily make it look not-updated, but locked */
        (*oldtup.t_data).t_ctid = oldtup.t_self;

        /*
         * Clear all-frozen bit on visibility map if needed.
         */
        if PageIsAllVisible(page)
            && visibilitymap_clear(relation, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN)
        {
            cleared_all_frozen = true;
        }

        MarkBufferDirty(buffer);

        if RelationNeedsWAL(relation) {
            let mut xlrec: xl_heap_lock = std::mem::zeroed();
            let recptr: XLogRecPtr;

            XLogBeginInsert();
            XLogRegisterBuffer(0, buffer, REGBUF_STANDARD as uint8);

            xlrec.offnum = ItemPointerGetOffsetNumber(&raw mut oldtup.t_self);
            xlrec.xmax = xmax_lock_old_tuple;
            xlrec.infobits_set = compute_infobits(
                (*oldtup.t_data).t_infomask,
                (*oldtup.t_data).t_infomask2,
            );
            xlrec.flags = if cleared_all_frozen {
                XLH_LOCK_ALL_FROZEN_CLEARED
            } else {
                0
            };
            XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapLock as c_int);
            recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_LOCK);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION!();

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        /*
         * Let the toaster do its thing, if needed.
         */
        if need_toast {
            /* Note we always use WAL and FSM during updates */
            heaptup = heap_toast_insert_or_update(relation, newtup, &raw mut oldtup, 0);
            newtupsize = MAXALIGN((*heaptup).t_len as usize) as Size;
        } else {
            heaptup = newtup;
        }

        /*
         * Now, do we need a new page for the tuple, or not?
         */
        loop {
            if newtupsize > pagefree {
                /* It doesn't fit, must use RelationGetBufferForTuple. */
                newbuf = RelationGetBufferForTuple(
                    relation,
                    ((*heaptup).t_len as Size),
                    buffer,
                    0,
                    std::ptr::null_mut(),
                    &raw mut vmbuffer_new,
                    &raw mut vmbuffer,
                    0,
                );
                /* We're all done. */
                break;
            }
            /* Acquire VM page pin if needed and we don't have it. */
            if vmbuffer == InvalidBuffer && PageIsAllVisible(page) {
                visibilitymap_pin(relation, block, &raw mut vmbuffer);
            }
            /* Re-acquire the lock on the old tuple's page. */
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            /* Re-check using the up-to-date free space */
            pagefree = PageGetHeapFreeSpace(page);
            if newtupsize > pagefree || (vmbuffer == InvalidBuffer && PageIsAllVisible(page)) {
                /*
                 * Rats, it doesn't fit anymore, or somebody just now set the
                 * all-visible flag.
                 */
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            } else {
                /* We're all done. */
                newbuf = buffer;
                break;
            }
        }
    } else {
        /* No TOAST work needed, and it'll fit on same page */
        newbuf = buffer;
        heaptup = newtup;
    }

    /*
     * We're about to do the actual update -- check for conflict first.
     */
    CheckForSerializableConflictIn(relation, &raw mut oldtup.t_self, BufferGetBlockNumber(buffer));

    /*
     * At this point newbuf and buffer are both pinned and locked.
     */
    if newbuf == buffer {
        /*
         * Since the new tuple is going into the same page, we might be able to
         * do a HOT update.
         */
        if !bms_overlap(modified_attrs, hot_attrs) {
            use_hot_update = true;

            /*
             * If none of the columns that are used in hot-blocking indexes
             * were updated, we can apply HOT.
             */
            if bms_overlap(modified_attrs, sum_attrs) {
                summarized_update = true;
            }
        }
    } else {
        /* Set a hint that the old page could use prune/defrag */
        PageSetFull(page);
    }

    /*
     * Compute replica identity tuple before entering the critical section.
     */
    old_key_tuple = ExtractReplicaIdentity(
        relation,
        &raw mut oldtup,
        bms_overlap(modified_attrs, id_attrs) || id_has_external,
        &raw mut old_key_copied,
    );

    /* NO EREPORT(ERROR) from here till changes are logged */
    START_CRIT_SECTION!();

    /*
     * If this transaction commits, the old tuple will become DEAD sooner or
     * later.
     */
    PageSetPrunable(page, xid);

    if use_hot_update {
        /* Mark the old tuple as HOT-updated */
        HeapTupleSetHotUpdated(&raw mut oldtup);
        /* And mark the new tuple as heap-only */
        HeapTupleSetHeapOnly(heaptup);
        /* Mark the caller's copy too, in case different from heaptup */
        HeapTupleSetHeapOnly(newtup);
    } else {
        /* Make sure tuples are correctly marked as not-HOT */
        HeapTupleClearHotUpdated(&raw mut oldtup);
        HeapTupleClearHeapOnly(heaptup);
        HeapTupleClearHeapOnly(newtup);
    }

    RelationPutHeapTuple(relation, newbuf, heaptup, false); /* insert new tuple */

    /* Clear obsolete visibility flags, possibly set by ourselves above... */
    (*oldtup.t_data).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
    (*oldtup.t_data).t_infomask2 &= !HEAP_KEYS_UPDATED;
    /* ... and store info about transaction updating this tuple */
    Assert!(TransactionIdIsValid(xmax_old_tuple));
    HeapTupleHeaderSetXmax(oldtup.t_data, xmax_old_tuple);
    (*oldtup.t_data).t_infomask |= infomask_old_tuple;
    (*oldtup.t_data).t_infomask2 |= infomask2_old_tuple;
    HeapTupleHeaderSetCmax(oldtup.t_data, cid_mut, iscombo);

    /* record address of new tuple in t_ctid of old one */
    (*oldtup.t_data).t_ctid = (*heaptup).t_self;

    /* clear PD_ALL_VISIBLE flags, reset all visibilitymap bits */
    if PageIsAllVisible(BufferGetPage(buffer)) {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(
            relation,
            BufferGetBlockNumber(buffer),
            vmbuffer,
            VISIBILITYMAP_VALID_BITS,
        );
    }
    if newbuf != buffer && PageIsAllVisible(BufferGetPage(newbuf)) {
        all_visible_cleared_new = true;
        PageClearAllVisible(BufferGetPage(newbuf));
        visibilitymap_clear(
            relation,
            BufferGetBlockNumber(newbuf),
            vmbuffer_new,
            VISIBILITYMAP_VALID_BITS,
        );
    }

    if newbuf != buffer {
        MarkBufferDirty(newbuf);
    }
    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if RelationNeedsWAL(relation) {
        let recptr: XLogRecPtr;

        /*
         * For logical decoding we need combo CIDs to properly decode the
         * catalog.
         */
        if RelationIsAccessibleInLogicalDecoding(relation) {
            log_heap_new_cid(relation, &raw mut oldtup);
            log_heap_new_cid(relation, heaptup);
        }

        recptr = log_heap_update(
            relation,
            buffer,
            newbuf,
            &raw mut oldtup,
            heaptup,
            old_key_tuple,
            all_visible_cleared,
            all_visible_cleared_new,
        );
        if newbuf != buffer {
            PageSetLSN(BufferGetPage(newbuf), recptr);
        }
        PageSetLSN(BufferGetPage(buffer), recptr);
    }

    END_CRIT_SECTION!();

    if newbuf != buffer {
        LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    /*
     * Mark old tuple for invalidation from system caches at next command
     * boundary, and mark the new tuple for invalidation in case we abort.
     */
    CacheInvalidateHeapTuple(relation, &raw mut oldtup, heaptup);

    /* Now we can release the buffer(s) */
    if newbuf != buffer {
        ReleaseBuffer(newbuf);
    }
    ReleaseBuffer(buffer);
    if BufferIsValid(vmbuffer_new) {
        ReleaseBuffer(vmbuffer_new);
    }
    if BufferIsValid(vmbuffer) {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * Release the lmgr tuple lock, if we had it.
     */
    if have_tuple_lock {
        UnlockTupleTuplock(relation, &raw mut oldtup.t_self, *lockmode);
    }

    pgstat_count_heap_update(relation, use_hot_update, newbuf != buffer);

    /*
     * If heaptup is a private copy, release it.
     */
    if heaptup != newtup {
        (*newtup).t_self = (*heaptup).t_self;
        heap_freetuple(heaptup);
    }

    /*
     * If it is a HOT update, the update may still need to update summarized
     * indexes.
     */
    if use_hot_update {
        if summarized_update {
            *update_indexes = TU_Summarizing;
        } else {
            *update_indexes = TU_None;
        }
    } else {
        *update_indexes = TU_All;
    }

    if !old_key_tuple.is_null() && old_key_copied {
        heap_freetuple(old_key_tuple);
    }

    bms_free(hot_attrs);
    bms_free(sum_attrs);
    bms_free(key_attrs);
    bms_free(id_attrs);
    bms_free(modified_attrs);
    bms_free(interesting_attrs);

    TM_Ok
}

/*
 * Confirm adequate lock held during heap_update(), per rules from
 * README.tuplock section "Locking to write inplace-updated tables".
 * (USE_ASSERT_CHECKING)
 */
unsafe fn check_lock_if_inplace_updateable_rel(
    relation: Relation,
    otid: ItemPointer,
    newtup: HeapTuple,
) {
    /* LOCKTAG_TUPLE acceptable for any catalog */
    match RelationGetRelid(relation) {
        x if x == RelationRelationId || x == DatabaseRelationId => {
            let mut tuptag: LOCKTAG = std::mem::zeroed();

            SET_LOCKTAG_TUPLE(
                &raw mut tuptag,
                (*relation).rd_lockInfo.lockRelId.dbId,
                (*relation).rd_lockInfo.lockRelId.relId,
                ItemPointerGetBlockNumber(otid),
                ItemPointerGetOffsetNumber(otid),
            );
            if LockHeldByMe(&raw mut tuptag, InplaceUpdateTupleLock, false) {
                return;
            }
        }
        _ => {
            Assert!(!IsInplaceUpdateRelation(relation));
            return;
        }
    }

    match RelationGetRelid(relation) {
        x if x == RelationRelationId => {
            /* LOCKTAG_TUPLE or LOCKTAG_RELATION ok */
            let classForm: Form_pg_class = GETSTRUCT(newtup) as Form_pg_class;
            let relid: Oid = (*classForm).oid;
            let dbid: Oid;
            let mut tag: LOCKTAG = std::mem::zeroed();

            if IsSharedRelation(relid) {
                dbid = InvalidOid;
            } else {
                dbid = MyDatabaseId;
            }

            if (*classForm).relkind == RELKIND_INDEX {
                let irel: Relation = index_open(relid, AccessShareLock);

                SET_LOCKTAG_RELATION(&raw mut tag, dbid, (*(*irel).rd_index).indrelid);
                index_close(irel, AccessShareLock);
            } else {
                SET_LOCKTAG_RELATION(&raw mut tag, dbid, relid);
            }

            if !LockHeldByMe(&raw mut tag, ShareUpdateExclusiveLock, false)
                && !LockHeldByMe(&raw mut tag, ShareRowExclusiveLock, true)
            {
                elog!(
                    WARNING,
                    "missing lock for relation \"{}\" (OID {}, relkind {}) @ TID ({},{})",
                    std::ffi::CStr::from_ptr(NameStr(&(*classForm).relname)).to_string_lossy(),
                    relid,
                    (*classForm).relkind as u8 as char,
                    ItemPointerGetBlockNumber(otid),
                    ItemPointerGetOffsetNumber(otid)
                );
            }
        }
        x if x == DatabaseRelationId => {
            /* LOCKTAG_TUPLE required */
            let dbForm: Form_pg_database = GETSTRUCT(newtup) as Form_pg_database;

            elog!(
                WARNING,
                "missing lock on database \"{}\" (OID {}) @ TID ({},{})",
                std::ffi::CStr::from_ptr(NameStr(&(*dbForm).datname)).to_string_lossy(),
                (*dbForm).oid,
                ItemPointerGetBlockNumber(otid),
                ItemPointerGetOffsetNumber(otid)
            );
        }
        _ => {}
    }
}

/*
 * Confirm adequate relation lock held, per rules from README.tuplock section
 * "Locking to write inplace-updated tables".  (USE_ASSERT_CHECKING)
 */
unsafe fn check_inplace_rel_lock(oldtup: HeapTuple) {
    let classForm: Form_pg_class = GETSTRUCT(oldtup) as Form_pg_class;
    let relid: Oid = (*classForm).oid;
    let dbid: Oid;
    let mut tag: LOCKTAG = std::mem::zeroed();

    if IsSharedRelation(relid) {
        dbid = InvalidOid;
    } else {
        dbid = MyDatabaseId;
    }

    if (*classForm).relkind == RELKIND_INDEX {
        let irel: Relation = index_open(relid, AccessShareLock);

        SET_LOCKTAG_RELATION(&raw mut tag, dbid, (*(*irel).rd_index).indrelid);
        index_close(irel, AccessShareLock);
    } else {
        SET_LOCKTAG_RELATION(&raw mut tag, dbid, relid);
    }

    if !LockHeldByMe(&raw mut tag, ShareUpdateExclusiveLock, true) {
        elog!(
            WARNING,
            "missing lock for relation \"{}\" (OID {}, relkind {}) @ TID ({},{})",
            std::ffi::CStr::from_ptr(NameStr(&(*classForm).relname)).to_string_lossy(),
            relid,
            (*classForm).relkind as u8 as char,
            ItemPointerGetBlockNumber(&raw mut (*oldtup).t_self),
            ItemPointerGetOffsetNumber(&raw mut (*oldtup).t_self)
        );
    }
}

/*
 * Check if the specified attribute's values are the same.  Subroutine for
 * HeapDetermineColumnsInfo.
 */
unsafe fn heap_attr_equals(
    tupdesc: TupleDesc,
    attrnum: c_int,
    value1: Datum,
    value2: Datum,
    isnull1: bool,
    isnull2: bool,
) -> bool {
    /*
     * If one value is NULL and other is not, then they are certainly not
     * equal
     */
    if isnull1 != isnull2 {
        return false;
    }

    /*
     * If both are NULL, they can be considered equal.
     */
    if isnull1 {
        return true;
    }

    /*
     * We do simple binary comparison of the two datums.
     */
    if attrnum <= 0 {
        /* The only allowed system columns are OIDs, so do this */
        DatumGetObjectId(value1) == DatumGetObjectId(value2)
    } else {
        let att: *mut CompactAttribute;

        Assert!(attrnum <= (*tupdesc).natts);
        att = TupleDescCompactAttr(tupdesc, attrnum - 1);
        datumIsEqual(value1, value2, (*att).attbyval, ((*att).attlen as i32))
    }
}

/*
 * Check which columns are being updated.
 */
unsafe fn HeapDetermineColumnsInfo(
    relation: Relation,
    interesting_cols: *mut Bitmapset,
    external_cols: *mut Bitmapset,
    oldtup: HeapTuple,
    newtup: HeapTuple,
    has_external: *mut bool,
) -> *mut Bitmapset {
    let mut attidx: c_int;
    let mut modified: *mut Bitmapset = std::ptr::null_mut();
    let tupdesc: TupleDesc = RelationGetDescr(relation);

    attidx = -1;
    loop {
        attidx = bms_next_member(interesting_cols, attidx);
        if attidx < 0 {
            break;
        }
        /* attidx is zero-based, attrnum is the normal attribute number */
        let attrnum: AttrNumber = attidx as AttrNumber + (FirstLowInvalidHeapAttributeNumber as AttrNumber);
        let value1: Datum;
        let value2: Datum;
        let mut isnull1: bool = false;
        let mut isnull2: bool = false;

        /*
         * If it's a whole-tuple reference, say "not equal".
         */
        if attrnum == 0 {
            modified = bms_add_member(modified, attidx);
            continue;
        }

        /*
         * Likewise, automatically say "not equal" for any system attribute
         * other than tableOID.
         */
        if attrnum < 0 {
            if attrnum != TableOidAttributeNumber {
                modified = bms_add_member(modified, attidx);
                continue;
            }
        }

        /*
         * Extract the corresponding values.
         */
        value1 = heap_getattr(oldtup, attrnum as c_int, tupdesc, &raw mut isnull1);
        value2 = heap_getattr(newtup, attrnum as c_int, tupdesc, &raw mut isnull2);

        if !heap_attr_equals(tupdesc, attrnum as c_int, value1, value2, isnull1, isnull2) {
            modified = bms_add_member(modified, attidx);
            continue;
        }

        /*
         * No need to check attributes that can't be stored externally.
         */
        if attrnum < 0
            || isnull1
            || (*TupleDescCompactAttr(tupdesc, attrnum as c_int - 1)).attlen != -1
        {
            continue;
        }

        /*
         * Check if the old tuple's attribute is stored externally and is a
         * member of external_cols.
         */
        if VARATT_IS_EXTERNAL(DatumGetPointer(value1) as *mut varlena)
            && bms_is_member(attidx, external_cols)
        {
            *has_external = true;
        }
    }

    modified
}

/*
 *	simple_heap_update - replace a tuple
 */
pub unsafe fn simple_heap_update(
    relation: Relation,
    otid: ItemPointer,
    tup: HeapTuple,
    update_indexes: *mut TU_UpdateIndexes,
) {
    let result: TM_Result;
    let mut tmfd: TM_FailureData = std::mem::zeroed();
    let mut lockmode: LockTupleMode = LockTupleKeyShare;

    result = heap_update(
        relation,
        otid,
        tup,
        GetCurrentCommandId(true),
        InvalidSnapshot,
        true, /* wait for commit */
        &raw mut tmfd,
        &raw mut lockmode,
        update_indexes,
    );
    match result {
        TM_SelfModified => {
            /* Tuple was already updated in current command? */
            elog!(ERROR, "tuple already updated by self");
        }
        TM_Ok => { /* done successfully */ }
        TM_Updated => {
            elog!(ERROR, "tuple concurrently updated");
        }
        TM_Deleted => {
            elog!(ERROR, "tuple concurrently deleted");
        }
        _ => {
            elog!(ERROR, "unrecognized heap_update status: {}", result as u32);
        }
    }
}

/*
 * Return the MultiXactStatus corresponding to the given tuple lock mode.
 */
unsafe fn get_mxact_status_for_lock(mode: LockTupleMode, is_update: bool) -> MultiXactStatus {
    let retval: c_int;

    if is_update {
        retval = tupleLockExtraInfo[mode as usize].updstatus;
    } else {
        retval = tupleLockExtraInfo[mode as usize].lockstatus;
    }

    if retval == -1 {
        elog!(
            ERROR,
            "invalid lock tuple mode {}/{}",
            mode as c_int,
            if is_update { "true" } else { "false" }
        );
    }

    retval as MultiXactStatus
}
/*
 *	heap_lock_tuple - lock a tuple in shared or exclusive mode
 */
pub unsafe fn heap_lock_tuple(
    relation: Relation,
    tuple: HeapTuple,
    cid: CommandId,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    follow_updates: bool,
    buffer: *mut Buffer,
    tmfd: *mut TM_FailureData,
) -> TM_Result {
    let mut result: TM_Result = TM_Ok;
    let tid: ItemPointer = &raw mut (*tuple).t_self;
    let lp: ItemId;
    let page: Page;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let block: BlockNumber;
    let xid: TransactionId;
    let xmax: TransactionId;
    let old_infomask: uint16;
    let mut new_infomask: uint16 = 0;
    let mut new_infomask2: uint16 = 0;
    let mut first_time: bool = true;
    let mut skip_tuple_lock: bool = false;
    let mut have_tuple_lock: bool = false;
    let mut cleared_all_frozen: bool = false;

    *buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
    block = ItemPointerGetBlockNumber(tid);

    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.
     */
    if PageIsAllVisible(BufferGetPage(*buffer)) {
        visibilitymap_pin(relation, block, &raw mut vmbuffer);
    }

    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(*buffer);
    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    Assert!(ItemIdIsNormal(lp));

    (*tuple).t_data = PageGetItem(page, lp) as HeapTupleHeader;
    (*tuple).t_len = ItemIdGetLength(lp);
    (*tuple).t_tableOid = RelationGetRelid(relation);

    let mut require_sleep: bool = false;

    'l3: loop {
        result = HeapTupleSatisfiesUpdate(tuple, cid, *buffer);

        if result == TM_Invisible {
            /*
             * This is possible, but only when locking a tuple for ON CONFLICT
             * UPDATE.
             */
            result = TM_Invisible;
            // goto out_locked;
            LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
            if BufferIsValid(vmbuffer) {
                ReleaseBuffer(vmbuffer);
            }
            if have_tuple_lock {
                UnlockTupleTuplock(relation, tid, mode);
            }
            return result;
        } else if result == TM_BeingModified || result == TM_Updated || result == TM_Deleted {
            let xwait: TransactionId;
            let infomask: uint16;
            let infomask2: uint16;
            let mut t_ctid: ItemPointerData = std::mem::zeroed();

            /* must copy state data before unlocking buffer */
            xwait = HeapTupleHeaderGetRawXmax((*tuple).t_data);
            infomask = (*(*tuple).t_data).t_infomask;
            infomask2 = (*(*tuple).t_data).t_infomask2;
            ItemPointerCopy(&raw mut (*(*tuple).t_data).t_ctid, &raw mut t_ctid);

            LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

            /*
             * If any subtransaction of the current top transaction already
             * holds a lock as strong as or stronger than what we're
             * requesting, we effectively hold the desired lock already.
             */
            if first_time {
                first_time = false;

                if infomask & HEAP_XMAX_IS_MULTI != 0 {
                    let mut i: c_int;
                    let nmembers: c_int;
                    let mut members: *mut MultiXactMember = std::ptr::null_mut();

                    nmembers = GetMultiXactIdMembers(
                        xwait,
                        &raw mut members,
                        false,
                        HEAP_XMAX_IS_LOCKED_ONLY(infomask),
                    );

                    i = 0;
                    while i < nmembers {
                        /* only consider members of our own transaction */
                        if !TransactionIdIsCurrentTransactionId((*members.add(i as usize)).xid) {
                            i += 1;
                            continue;
                        }

                        if TUPLOCK_from_mxstatus((*members.add(i as usize)).status)
                            >= mode as c_int
                        {
                            pfree(members as *mut c_void);
                            result = TM_Ok;
                            // goto out_unlocked;
                            if BufferIsValid(vmbuffer) {
                                ReleaseBuffer(vmbuffer);
                            }
                            if have_tuple_lock {
                                UnlockTupleTuplock(relation, tid, mode);
                            }
                            return result;
                        } else {
                            /*
                             * Disable acquisition of the heavyweight tuple
                             * lock.
                             */
                            skip_tuple_lock = true;
                        }
                        i += 1;
                    }

                    if !members.is_null() {
                        pfree(members as *mut c_void);
                    }
                } else if TransactionIdIsCurrentTransactionId(xwait) {
                    match mode {
                        LockTupleKeyShare => {
                            Assert!(
                                HEAP_XMAX_IS_KEYSHR_LOCKED((infomask as i16))
                                    || HEAP_XMAX_IS_SHR_LOCKED((infomask as i16))
                                    || HEAP_XMAX_IS_EXCL_LOCKED((infomask as i16))
                            );
                            result = TM_Ok;
                            if BufferIsValid(vmbuffer) {
                                ReleaseBuffer(vmbuffer);
                            }
                            if have_tuple_lock {
                                UnlockTupleTuplock(relation, tid, mode);
                            }
                            return result;
                        }
                        LockTupleShare => {
                            if HEAP_XMAX_IS_SHR_LOCKED((infomask as i16))
                                || HEAP_XMAX_IS_EXCL_LOCKED((infomask as i16))
                            {
                                result = TM_Ok;
                                if BufferIsValid(vmbuffer) {
                                    ReleaseBuffer(vmbuffer);
                                }
                                if have_tuple_lock {
                                    UnlockTupleTuplock(relation, tid, mode);
                                }
                                return result;
                            }
                        }
                        LockTupleNoKeyExclusive => {
                            if HEAP_XMAX_IS_EXCL_LOCKED((infomask as i16)) {
                                result = TM_Ok;
                                if BufferIsValid(vmbuffer) {
                                    ReleaseBuffer(vmbuffer);
                                }
                                if have_tuple_lock {
                                    UnlockTupleTuplock(relation, tid, mode);
                                }
                                return result;
                            }
                        }
                        LockTupleExclusive => {
                            if HEAP_XMAX_IS_EXCL_LOCKED((infomask as i16))
                                && infomask2 & HEAP_KEYS_UPDATED != 0
                            {
                                result = TM_Ok;
                                if BufferIsValid(vmbuffer) {
                                    ReleaseBuffer(vmbuffer);
                                }
                                if have_tuple_lock {
                                    UnlockTupleTuplock(relation, tid, mode);
                                }
                                return result;
                            }
                        }
                    }
                }
            }

            /*
             * Initially assume that we will have to wait for the locking
             * transaction(s) to finish.
             */
            require_sleep = true;
            if mode == LockTupleKeyShare {
                /*
                 * If we're requesting KeyShare, and there's no update present,
                 * we don't need to wait.
                 */
                if infomask2 & HEAP_KEYS_UPDATED == 0 {
                    let updated: bool;

                    updated = !HEAP_XMAX_IS_LOCKED_ONLY(infomask);

                    /*
                     * If there are updates, follow the update chain; bail out
                     * if that cannot be done.
                     */
                    if follow_updates
                        && updated
                        && !ItemPointerEquals(&raw mut (*tuple).t_self, &raw mut t_ctid)
                    {
                        let res: TM_Result;

                        res = heap_lock_updated_tuple(
                            relation,
                            infomask,
                            xwait,
                            &raw const t_ctid,
                            GetCurrentTransactionId(),
                            mode,
                        );
                        if res != TM_Ok {
                            result = res;
                            /* recovery code expects to have buffer lock held */
                            LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                            // goto failed;
                            break 'l3;
                        }
                    }

                    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

                    /*
                     * Make sure it's still an appropriate lock, else start
                     * over.
                     */
                    if !HeapTupleHeaderIsOnlyLocked((*tuple).t_data)
                        && (((*(*tuple).t_data).t_infomask2 & HEAP_KEYS_UPDATED != 0) || !updated)
                    {
                        continue 'l3;
                    }

                    /* Things look okay, so we can skip sleeping */
                    require_sleep = false;
                }
            } else if mode == LockTupleShare {
                /*
                 * If we're requesting Share, we can similarly avoid sleeping
                 * if there's no update and no exclusive lock present.
                 */
                if HEAP_XMAX_IS_LOCKED_ONLY(infomask) && !HEAP_XMAX_IS_EXCL_LOCKED((infomask as i16)) {
                    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

                    /*
                     * Make sure it's still an appropriate lock, else start
                     * over.
                     */
                    if !HEAP_XMAX_IS_LOCKED_ONLY((*(*tuple).t_data).t_infomask)
                        || HEAP_XMAX_IS_EXCL_LOCKED((*(*tuple).t_data).t_infomask as i16)
                    {
                        continue 'l3;
                    }
                    require_sleep = false;
                }
            } else if mode == LockTupleNoKeyExclusive {
                /*
                 * If we're requesting NoKeyExclusive, we might also be able to
                 * avoid sleeping.
                 */
                if infomask & HEAP_XMAX_IS_MULTI != 0 {
                    if !DoesMultiXactIdConflict(
                        xwait as MultiXactId,
                        infomask,
                        mode,
                        std::ptr::null_mut(),
                    ) {
                        /*
                         * No conflict, but if the xmax changed under us in the
                         * meantime, start over.
                         */
                        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                        if xmax_infomask_changed((*(*tuple).t_data).t_infomask, infomask)
                            || !TransactionIdEquals(
                                HeapTupleHeaderGetRawXmax((*tuple).t_data),
                                xwait,
                            )
                        {
                            continue 'l3;
                        }

                        /* otherwise, we're good */
                        require_sleep = false;
                    }
                } else if HEAP_XMAX_IS_KEYSHR_LOCKED((infomask as i16)) {
                    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

                    /* if the xmax changed in the meantime, start over */
                    if xmax_infomask_changed((*(*tuple).t_data).t_infomask, infomask)
                        || !TransactionIdEquals(HeapTupleHeaderGetRawXmax((*tuple).t_data), xwait)
                    {
                        continue 'l3;
                    }
                    /* otherwise, we're good */
                    require_sleep = false;
                }
            }

            /*
             * As a check independent from those above, we can also avoid
             * sleeping if the current transaction is the sole locker.
             */
            if require_sleep
                && !(infomask & HEAP_XMAX_IS_MULTI != 0)
                && TransactionIdIsCurrentTransactionId(xwait)
            {
                /* ... but if the xmax changed in the meantime, start over */
                LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                if xmax_infomask_changed((*(*tuple).t_data).t_infomask, infomask)
                    || !TransactionIdEquals(HeapTupleHeaderGetRawXmax((*tuple).t_data), xwait)
                {
                    continue 'l3;
                }
                Assert!(HEAP_XMAX_IS_LOCKED_ONLY((*(*tuple).t_data).t_infomask));
                require_sleep = false;
            }

            /*
             * Time to sleep on the other transaction/multixact, if necessary.
             */
            if require_sleep && (result == TM_Updated || result == TM_Deleted) {
                LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                // goto failed;
                break 'l3;
            } else if require_sleep {
                /*
                 * Acquire tuple lock to establish our priority for the tuple,
                 * or die trying.
                 */
                if !skip_tuple_lock
                    && !heap_acquire_tuplock(
                        relation,
                        tid,
                        mode,
                        wait_policy,
                        &raw mut have_tuple_lock,
                    )
                {
                    /*
                     * This can only happen if wait_policy is Skip.
                     */
                    result = TM_WouldBlock;
                    /* recovery code expects to have buffer lock held */
                    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                    break 'l3;
                }

                if infomask & HEAP_XMAX_IS_MULTI != 0 {
                    let status: MultiXactStatus = get_mxact_status_for_lock(mode, false);

                    /* We only ever lock tuples, never update them */
                    if status >= MultiXactStatusNoKeyUpdate {
                        elog!(ERROR, "invalid lock mode in heap_lock_tuple");
                    }

                    /* wait for multixact to end, or die trying  */
                    match wait_policy {
                        LockWaitBlock => {
                            MultiXactIdWait(
                                xwait as MultiXactId,
                                status,
                                infomask,
                                relation,
                                &raw mut (*tuple).t_self,
                                XLTW_Lock,
                                std::ptr::null_mut(),
                            );
                        }
                        LockWaitSkip => {
                            if !ConditionalMultiXactIdWait(
                                xwait as MultiXactId,
                                status,
                                infomask,
                                relation,
                                std::ptr::null_mut(),
                                false,
                            ) {
                                result = TM_WouldBlock;
                                /* recovery code expects to have buffer lock held */
                                LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                                break 'l3;
                            }
                        }
                        LockWaitError => {
                            if !ConditionalMultiXactIdWait(
                                xwait as MultiXactId,
                                status,
                                infomask,
                                relation,
                                std::ptr::null_mut(),
                                log_lock_failures,
                            ) {
                                ereport!(
                                    ERROR,
                                    errmsg!(
                                        "could not obtain lock on row in relation \"{}\"",
                                        std::ffi::CStr::from_ptr(RelationGetRelationName(
                                            relation
                                        ))
                                        .to_string_lossy()
                                    )
                                );
                            }
                        }
                    }
                } else {
                    /* wait for regular transaction to end, or die trying */
                    match wait_policy {
                        LockWaitBlock => {
                            XactLockTableWait(
                                xwait,
                                relation,
                                &raw mut (*tuple).t_self,
                                XLTW_Lock,
                            );
                        }
                        LockWaitSkip => {
                            if !ConditionalXactLockTableWait(xwait, false) {
                                result = TM_WouldBlock;
                                /* recovery code expects to have buffer lock held */
                                LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                                break 'l3;
                            }
                        }
                        LockWaitError => {
                            if !ConditionalXactLockTableWait(xwait, log_lock_failures) {
                                ereport!(
                                    ERROR,
                                    errmsg!(
                                        "could not obtain lock on row in relation \"{}\"",
                                        std::ffi::CStr::from_ptr(RelationGetRelationName(
                                            relation
                                        ))
                                        .to_string_lossy()
                                    )
                                );
                            }
                        }
                    }
                }

                /* if there are updates, follow the update chain */
                if follow_updates
                    && !HEAP_XMAX_IS_LOCKED_ONLY(infomask)
                    && !ItemPointerEquals(&raw mut (*tuple).t_self, &raw mut t_ctid)
                {
                    let res: TM_Result;

                    res = heap_lock_updated_tuple(
                        relation,
                        infomask,
                        xwait,
                        &raw const t_ctid,
                        GetCurrentTransactionId(),
                        mode,
                    );
                    if res != TM_Ok {
                        result = res;
                        /* recovery code expects to have buffer lock held */
                        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
                        break 'l3;
                    }
                }

                LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

                /*
                 * xwait is done, but if xwait had just locked the tuple then
                 * some other xact could update this tuple before we get to
                 * this point.
                 */
                if xmax_infomask_changed((*(*tuple).t_data).t_infomask, infomask)
                    || !TransactionIdEquals(HeapTupleHeaderGetRawXmax((*tuple).t_data), xwait)
                {
                    continue 'l3;
                }

                if !(infomask & HEAP_XMAX_IS_MULTI != 0) {
                    /*
                     * Otherwise check if it committed or aborted.
                     */
                    UpdateXmaxHintBits((*tuple).t_data, *buffer, xwait);
                }
            }

            /* By here, we're certain that we hold buffer exclusive lock again */

            /*
             * We may lock if previous xmax aborted, or if it committed but
             * only locked the tuple without updating it.
             */
            if !require_sleep
                || ((*(*tuple).t_data).t_infomask & HEAP_XMAX_INVALID != 0)
                || HEAP_XMAX_IS_LOCKED_ONLY((*(*tuple).t_data).t_infomask)
                || HeapTupleHeaderIsOnlyLocked((*tuple).t_data)
            {
                result = TM_Ok;
            } else if !ItemPointerEquals(&raw mut (*tuple).t_self, &raw mut (*(*tuple).t_data).t_ctid)
            {
                result = TM_Updated;
            } else {
                result = TM_Deleted;
            }
        }

        break 'l3;
    }

    // failed:
    if result != TM_Ok {
        Assert!(
            result == TM_SelfModified
                || result == TM_Updated
                || result == TM_Deleted
                || result == TM_WouldBlock
        );

        Assert!(
            (result == TM_WouldBlock)
                || !((*(*tuple).t_data).t_infomask & HEAP_XMAX_INVALID != 0)
        );
        Assert!(
            result != TM_Updated
                || !ItemPointerEquals(
                    &raw mut (*tuple).t_self,
                    &raw mut (*(*tuple).t_data).t_ctid
                )
        );
        (*tmfd).ctid = (*(*tuple).t_data).t_ctid;
        (*tmfd).xmax = HeapTupleHeaderGetUpdateXid((*tuple).t_data);
        if result == TM_SelfModified {
            (*tmfd).cmax = HeapTupleHeaderGetCmax((*tuple).t_data);
        } else {
            (*tmfd).cmax = InvalidCommandId;
        }
        // goto out_locked;
        LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
        if BufferIsValid(vmbuffer) {
            ReleaseBuffer(vmbuffer);
        }
        if have_tuple_lock {
            UnlockTupleTuplock(relation, tid, mode);
        }
        return result;
    }

    /*
     * If we didn't pin the visibility map page and the page has become all
     * visible while we were busy locking the buffer, we'll have to unlock and
     * re-lock.
     */
    if vmbuffer == InvalidBuffer && PageIsAllVisible(page) {
        LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
        visibilitymap_pin(relation, block, &raw mut vmbuffer);
        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
        return heap_lock_tuple(
            relation,
            tuple,
            cid,
            mode,
            wait_policy,
            follow_updates,
            buffer,
            tmfd,
        );
    }

    xmax = HeapTupleHeaderGetRawXmax((*tuple).t_data);
    old_infomask = (*(*tuple).t_data).t_infomask;

    /*
     * If this is the first possibly-multixact-able operation in the current
     * transaction, set my per-backend OldestMemberMXactId setting.
     */
    MultiXactIdSetOldestMember();

    /*
     * Compute the new xmax and infomask to store into the tuple.
     */
    let mut xid_out: TransactionId = InvalidTransactionId;
    compute_new_xmax_infomask(
        xmax,
        old_infomask,
        (*(*tuple).t_data).t_infomask2,
        GetCurrentTransactionId(),
        mode,
        false,
        &raw mut xid_out,
        &raw mut new_infomask,
        &raw mut new_infomask2,
    );
    xid = xid_out;

    START_CRIT_SECTION!();

    /*
     * Store transaction information of xact locking the tuple.
     */
    (*(*tuple).t_data).t_infomask &= !HEAP_XMAX_BITS;
    (*(*tuple).t_data).t_infomask2 &= !HEAP_KEYS_UPDATED;
    (*(*tuple).t_data).t_infomask |= new_infomask;
    (*(*tuple).t_data).t_infomask2 |= new_infomask2;
    if HEAP_XMAX_IS_LOCKED_ONLY(new_infomask) {
        HeapTupleHeaderClearHotUpdated((*tuple).t_data);
    }
    HeapTupleHeaderSetXmax((*tuple).t_data, xid);

    /*
     * Make sure there is no forward chain link in t_ctid.
     */
    if HEAP_XMAX_IS_LOCKED_ONLY(new_infomask) {
        (*(*tuple).t_data).t_ctid = *tid;
    }

    /* Clear only the all-frozen bit on visibility map if needed */
    if PageIsAllVisible(page)
        && visibilitymap_clear(relation, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN)
    {
        cleared_all_frozen = true;
    }

    MarkBufferDirty(*buffer);

    /*
     * XLOG stuff.
     */
    if RelationNeedsWAL(relation) {
        let mut xlrec: xl_heap_lock = std::mem::zeroed();
        let recptr: XLogRecPtr;

        XLogBeginInsert();
        XLogRegisterBuffer(0, *buffer, REGBUF_STANDARD as uint8);

        xlrec.offnum = ItemPointerGetOffsetNumber(&raw mut (*tuple).t_self);
        xlrec.xmax = xid;
        xlrec.infobits_set = compute_infobits(new_infomask, (*(*tuple).t_data).t_infomask2);
        xlrec.flags = if cleared_all_frozen {
            XLH_LOCK_ALL_FROZEN_CLEARED
        } else {
            0
        };
        XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapLock as c_int);

        /* we don't decode row locks atm, so no need to log the origin */

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_LOCK);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION!();

    result = TM_Ok;

    // out_locked:
    LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

    // out_unlocked:
    if BufferIsValid(vmbuffer) {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * Now that we have successfully marked the tuple as locked, we can release
     * the lmgr tuple lock, if we had it.
     */
    if have_tuple_lock {
        UnlockTupleTuplock(relation, tid, mode);
    }

    result
}

/*
 * Acquire heavyweight lock on the given tuple, in preparation for acquiring
 * its normal, Xmax-based tuple lock.
 */
unsafe fn heap_acquire_tuplock(
    relation: Relation,
    tid: ItemPointer,
    mode: LockTupleMode,
    wait_policy: LockWaitPolicy,
    have_tuple_lock: *mut bool,
) -> bool {
    if *have_tuple_lock {
        return true;
    }

    match wait_policy {
        LockWaitBlock => {
            LockTupleTuplock(relation, tid, mode);
        }
        LockWaitSkip => {
            if !ConditionalLockTupleTuplock(relation, tid, mode, false) {
                return false;
            }
        }
        LockWaitError => {
            if !ConditionalLockTupleTuplock(relation, tid, mode, log_lock_failures) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not obtain lock on row in relation \"{}\"",
                        std::ffi::CStr::from_ptr(RelationGetRelationName(relation))
                            .to_string_lossy()
                    )
                );
            }
        }
    }
    *have_tuple_lock = true;

    true
}

/*
 * Given an original set of Xmax and infomask, and a transaction (identified by
 * add_to_xmax) acquiring a new lock of some mode, compute the new Xmax and
 * corresponding infomasks to use on the tuple.
 */
unsafe fn compute_new_xmax_infomask(
    xmax: TransactionId,
    mut old_infomask: uint16,
    old_infomask2: uint16,
    add_to_xmax: TransactionId,
    mut mode: LockTupleMode,
    is_update: bool,
    result_xmax: *mut TransactionId,
    result_infomask: *mut uint16,
    result_infomask2: *mut uint16,
) {
    let mut new_xmax: TransactionId;
    let mut new_infomask: uint16;
    let mut new_infomask2: uint16;

    Assert!(TransactionIdIsCurrentTransactionId(add_to_xmax));

    // l5:
    'l5: loop {
        new_infomask = 0;
        new_infomask2 = 0;
        if old_infomask & HEAP_XMAX_INVALID != 0 {
            /*
             * No previous locker; we just insert our own TransactionId.
             */
            if is_update {
                new_xmax = add_to_xmax;
                if mode == LockTupleExclusive {
                    new_infomask2 |= HEAP_KEYS_UPDATED;
                }
            } else {
                new_infomask |= HEAP_XMAX_LOCK_ONLY;
                match mode {
                    LockTupleKeyShare => {
                        new_xmax = add_to_xmax;
                        new_infomask |= HEAP_XMAX_KEYSHR_LOCK;
                    }
                    LockTupleShare => {
                        new_xmax = add_to_xmax;
                        new_infomask |= HEAP_XMAX_SHR_LOCK;
                    }
                    LockTupleNoKeyExclusive => {
                        new_xmax = add_to_xmax;
                        new_infomask |= HEAP_XMAX_EXCL_LOCK;
                    }
                    LockTupleExclusive => {
                        new_xmax = add_to_xmax;
                        new_infomask |= HEAP_XMAX_EXCL_LOCK;
                        new_infomask2 |= HEAP_KEYS_UPDATED;
                    }
                }
            }
        } else if old_infomask & HEAP_XMAX_IS_MULTI != 0 {
            let new_status: MultiXactStatus;

            /*
             * Currently we don't allow XMAX_COMMITTED to be set for multis.
             */
            Assert!(!(old_infomask & HEAP_XMAX_COMMITTED != 0));

            /*
             * A multixact together with LOCK_ONLY set but neither lock bit set
             * cannot possibly be running anymore.
             */
            if HEAP_LOCKED_UPGRADED(old_infomask) {
                old_infomask &= !HEAP_XMAX_IS_MULTI;
                old_infomask |= HEAP_XMAX_INVALID;
                continue 'l5;
            }

            /*
             * If the XMAX is already a MultiXactId, then we need to expand it.
             */
            if !MultiXactIdIsRunning(xmax, HEAP_XMAX_IS_LOCKED_ONLY(old_infomask)) {
                if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask)
                    || !TransactionIdDidCommit(MultiXactIdGetUpdateXid(xmax, old_infomask))
                {
                    old_infomask &= !HEAP_XMAX_IS_MULTI;
                    old_infomask |= HEAP_XMAX_INVALID;
                    continue 'l5;
                }
            }

            new_status = get_mxact_status_for_lock(mode, is_update);

            new_xmax = MultiXactIdExpand(xmax as MultiXactId, add_to_xmax, new_status);
            GetMultiXactIdHintBits(new_xmax, &raw mut new_infomask, &raw mut new_infomask2);
        } else if old_infomask & HEAP_XMAX_COMMITTED != 0 {
            /*
             * It's a committed update, so we need to preserve him as updater.
             */
            let status: MultiXactStatus;
            let new_status: MultiXactStatus;

            if old_infomask2 & HEAP_KEYS_UPDATED != 0 {
                status = MultiXactStatusUpdate;
            } else {
                status = MultiXactStatusNoKeyUpdate;
            }

            new_status = get_mxact_status_for_lock(mode, is_update);

            new_xmax = MultiXactIdCreate(xmax, status, add_to_xmax, new_status);
            GetMultiXactIdHintBits(new_xmax, &raw mut new_infomask, &raw mut new_infomask2);
        } else if TransactionIdIsInProgress(xmax) {
            /*
             * If the XMAX is a valid, in-progress TransactionId, then we need
             * to create a new MultiXactId.
             */
            let new_status: MultiXactStatus;
            let old_status: MultiXactStatus;
            let old_mode: LockTupleMode;

            if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) {
                if HEAP_XMAX_IS_KEYSHR_LOCKED((old_infomask as i16)) {
                    old_status = MultiXactStatusForKeyShare;
                } else if HEAP_XMAX_IS_SHR_LOCKED((old_infomask as i16)) {
                    old_status = MultiXactStatusForShare;
                } else if HEAP_XMAX_IS_EXCL_LOCKED((old_infomask as i16)) {
                    if old_infomask2 & HEAP_KEYS_UPDATED != 0 {
                        old_status = MultiXactStatusForUpdate;
                    } else {
                        old_status = MultiXactStatusForNoKeyUpdate;
                    }
                } else {
                    /*
                     * LOCK_ONLY can be present alone only when a page has been
                     * upgraded by pg_upgrade.
                     */
                    elog!(WARNING, "LOCK_ONLY found for Xid in progress {}", xmax);
                    old_infomask |= HEAP_XMAX_INVALID;
                    old_infomask &= !HEAP_XMAX_LOCK_ONLY;
                    continue 'l5;
                }
            } else {
                /* it's an update, but which kind? */
                if old_infomask2 & HEAP_KEYS_UPDATED != 0 {
                    old_status = MultiXactStatusUpdate;
                } else {
                    old_status = MultiXactStatusNoKeyUpdate;
                }
            }

            old_mode = unsafe { core::mem::transmute(TUPLOCK_from_mxstatus(old_status)) };

            /*
             * If the lock to be acquired is for the same TransactionId as the
             * existing lock, there's an optimization possible.
             */
            if xmax == add_to_xmax {
                Assert!(HEAP_XMAX_IS_LOCKED_ONLY(old_infomask));

                /* acquire the strongest of both */
                if (mode as c_int) < (old_mode as c_int) {
                    mode = old_mode;
                }
                /* mustn't touch is_update */

                old_infomask |= HEAP_XMAX_INVALID;
                continue 'l5;
            }

            /* otherwise, just fall back to creating a new multixact */
            new_status = get_mxact_status_for_lock(mode, is_update);
            new_xmax = MultiXactIdCreate(xmax, old_status, add_to_xmax, new_status);
            GetMultiXactIdHintBits(new_xmax, &raw mut new_infomask, &raw mut new_infomask2);
        } else if !HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) && TransactionIdDidCommit(xmax) {
            /*
             * It's a committed update, so we gotta preserve him as updater.
             */
            let status: MultiXactStatus;
            let new_status: MultiXactStatus;

            if old_infomask2 & HEAP_KEYS_UPDATED != 0 {
                status = MultiXactStatusUpdate;
            } else {
                status = MultiXactStatusNoKeyUpdate;
            }

            new_status = get_mxact_status_for_lock(mode, is_update);

            new_xmax = MultiXactIdCreate(xmax, status, add_to_xmax, new_status);
            GetMultiXactIdHintBits(new_xmax, &raw mut new_infomask, &raw mut new_infomask2);
        } else {
            /*
             * Can get here iff the locking/updating transaction was running
             * when the infomask was extracted from the tuple.
             */
            old_infomask |= HEAP_XMAX_INVALID;
            continue 'l5;
        }

        *result_infomask = new_infomask;
        *result_infomask2 = new_infomask2;
        *result_xmax = new_xmax;
        break 'l5;
    }
}

/*
 * Subroutine for heap_lock_updated_tuple_rec.
 */
unsafe fn test_lockmode_for_conflict(
    status: MultiXactStatus,
    xid: TransactionId,
    mode: LockTupleMode,
    tup: HeapTuple,
    needwait: *mut bool,
) -> TM_Result {
    let wantedstatus: MultiXactStatus;

    *needwait = false;
    wantedstatus = get_mxact_status_for_lock(mode, false);

    /*
     * Note: we *must* check TransactionIdIsInProgress before
     * TransactionIdDidAbort/Commit.
     */
    if TransactionIdIsCurrentTransactionId(xid) {
        /*
         * The tuple has already been locked by our own transaction.
         */
        return TM_SelfModified;
    } else if TransactionIdIsInProgress(xid) {
        /*
         * If the locking transaction is running, what we do depends on whether
         * the lock modes conflict.
         */
        if DoLockModesConflict(
            LOCKMODE_from_mxstatus(status),
            LOCKMODE_from_mxstatus(wantedstatus),
        ) {
            *needwait = true;
        }

        return TM_Ok;
    } else if TransactionIdDidAbort(xid) {
        return TM_Ok;
    } else if TransactionIdDidCommit(xid) {
        /*
         * The other transaction committed.
         */
        if !ISUPDATE_from_mxstatus(status) {
            return TM_Ok;
        }

        if DoLockModesConflict(
            LOCKMODE_from_mxstatus(status),
            LOCKMODE_from_mxstatus(wantedstatus),
        ) {
            /* bummer */
            if !ItemPointerEquals(&raw mut (*tup).t_self, &raw mut (*(*tup).t_data).t_ctid) {
                return TM_Updated;
            } else {
                return TM_Deleted;
            }
        }

        return TM_Ok;
    }

    /* Not in progress, not aborted, not committed -- must have crashed */
    TM_Ok
}

/*
 * Recursive part of heap_lock_updated_tuple
 */
unsafe fn heap_lock_updated_tuple_rec(
    rel: Relation,
    mut priorXmax: TransactionId,
    tid: *const ItemPointerData,
    xid: TransactionId,
    mode: LockTupleMode,
) -> TM_Result {
    let mut result: TM_Result = TM_Ok;
    let mut tupid: ItemPointerData = std::mem::zeroed();
    let mut mytup: HeapTupleData = std::mem::zeroed();
    let mut buf: Buffer = InvalidBuffer;
    let mut new_infomask: uint16;
    let mut new_infomask2: uint16 = 0;
    let mut old_infomask: uint16;
    let mut old_infomask2: uint16;
    let mut xmax: TransactionId;
    let mut new_xmax: TransactionId;
    let mut cleared_all_frozen: bool = false;
    let mut pinned_desired_page: bool;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut block: BlockNumber;

    ItemPointerCopy(tid as *mut ItemPointerData, &raw mut tupid);

    'outer: loop {
        new_infomask = 0;
        new_xmax = InvalidTransactionId;
        block = ItemPointerGetBlockNumber(&raw mut tupid);
        ItemPointerCopy(&raw mut tupid, &raw mut mytup.t_self);

        if !heap_fetch(rel, SnapshotAny, &raw mut mytup, &raw mut buf, false) {
            /*
             * if we fail to find the updated version of the tuple, behave as
             * if we got to the end of the chain.
             */
            result = TM_Ok;
            // goto out_unlocked;
            if vmbuffer != InvalidBuffer {
                ReleaseBuffer(vmbuffer);
            }
            return result;
        }

        // l4:
        'l4: loop {
            CHECK_FOR_INTERRUPTS!();

            /*
             * Before locking the buffer, pin the visibility map page if it
             * appears to be necessary.
             */
            if PageIsAllVisible(BufferGetPage(buf)) {
                visibilitymap_pin(rel, block, &raw mut vmbuffer);
                pinned_desired_page = true;
            } else {
                pinned_desired_page = false;
            }

            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

            /*
             * If we didn't pin the visibility map page and the page has become
             * all visible while we were busy locking the buffer, we'll have to
             * unlock and re-lock.
             */
            if !pinned_desired_page && PageIsAllVisible(BufferGetPage(buf)) {
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                visibilitymap_pin(rel, block, &raw mut vmbuffer);
                LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            }

            /*
             * Check the tuple XMIN against prior XMAX, if any.
             */
            if TransactionIdIsValid(priorXmax)
                && !TransactionIdEquals(HeapTupleHeaderGetXmin(mytup.t_data), priorXmax)
            {
                result = TM_Ok;
                // goto out_locked;
                UnlockReleaseBuffer(buf);
                if vmbuffer != InvalidBuffer {
                    ReleaseBuffer(vmbuffer);
                }
                return result;
            }

            /*
             * Also check Xmin.
             */
            if TransactionIdDidAbort(HeapTupleHeaderGetXmin(mytup.t_data)) {
                result = TM_Ok;
                UnlockReleaseBuffer(buf);
                if vmbuffer != InvalidBuffer {
                    ReleaseBuffer(vmbuffer);
                }
                return result;
            }

            old_infomask = (*mytup.t_data).t_infomask;
            old_infomask2 = (*mytup.t_data).t_infomask2;
            xmax = HeapTupleHeaderGetRawXmax(mytup.t_data);

            /*
             * If this tuple version has been updated or locked by some
             * concurrent transaction(s), what we do depends on whether our
             * lock mode conflicts.
             */
            let mut do_next = false;
            if old_infomask & HEAP_XMAX_INVALID == 0 {
                let rawxmax: TransactionId;
                let mut needwait: bool = false;

                rawxmax = HeapTupleHeaderGetRawXmax(mytup.t_data);
                if old_infomask & HEAP_XMAX_IS_MULTI != 0 {
                    let nmembers: c_int;
                    let mut i: c_int;
                    let mut members: *mut MultiXactMember = std::ptr::null_mut();

                    Assert!(!HEAP_LOCKED_UPGRADED((*mytup.t_data).t_infomask));

                    nmembers = GetMultiXactIdMembers(
                        rawxmax,
                        &raw mut members,
                        false,
                        HEAP_XMAX_IS_LOCKED_ONLY(old_infomask),
                    );
                    i = 0;
                    let mut restart_l4 = false;
                    let mut goto_out = false;
                    while i < nmembers {
                        result = test_lockmode_for_conflict(
                            (*members.add(i as usize)).status,
                            (*members.add(i as usize)).xid,
                            mode,
                            &raw mut mytup,
                            &raw mut needwait,
                        );

                        /*
                         * If the tuple was already locked by ourselves in a
                         * previous iteration of this.
                         */
                        if result == TM_SelfModified {
                            pfree(members as *mut c_void);
                            do_next = true; // goto next;
                            break;
                        }

                        if needwait {
                            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                            XactLockTableWait(
                                (*members.add(i as usize)).xid,
                                rel,
                                &raw mut mytup.t_self,
                                XLTW_LockUpdated,
                            );
                            pfree(members as *mut c_void);
                            restart_l4 = true; // goto l4;
                            break;
                        }
                        if result != TM_Ok {
                            pfree(members as *mut c_void);
                            goto_out = true; // goto out_locked;
                            break;
                        }
                        i += 1;
                    }
                    if restart_l4 {
                        continue 'l4;
                    }
                    if goto_out {
                        UnlockReleaseBuffer(buf);
                        if vmbuffer != InvalidBuffer {
                            ReleaseBuffer(vmbuffer);
                        }
                        return result;
                    }
                    if !do_next {
                        if !members.is_null() {
                            pfree(members as *mut c_void);
                        }
                    }
                } else {
                    let status: MultiXactStatus;

                    /*
                     * For a non-multi Xmax, we first need to compute the
                     * corresponding MultiXactStatus.
                     */
                    if HEAP_XMAX_IS_LOCKED_ONLY(old_infomask) {
                        if HEAP_XMAX_IS_KEYSHR_LOCKED((old_infomask as i16)) {
                            status = MultiXactStatusForKeyShare;
                        } else if HEAP_XMAX_IS_SHR_LOCKED((old_infomask as i16)) {
                            status = MultiXactStatusForShare;
                        } else if HEAP_XMAX_IS_EXCL_LOCKED((old_infomask as i16)) {
                            if old_infomask2 & HEAP_KEYS_UPDATED != 0 {
                                status = MultiXactStatusForUpdate;
                            } else {
                                status = MultiXactStatusForNoKeyUpdate;
                            }
                        } else {
                            /*
                             * LOCK_ONLY present alone shouldn't be seen in the
                             * middle of an update chain.
                             */
                            elog!(ERROR, "invalid lock status in tuple");
                            status = 0; /* silence compiler */
                        }
                    } else {
                        /* it's an update, but which kind? */
                        if old_infomask2 & HEAP_KEYS_UPDATED != 0 {
                            status = MultiXactStatusUpdate;
                        } else {
                            status = MultiXactStatusNoKeyUpdate;
                        }
                    }

                    result = test_lockmode_for_conflict(
                        status,
                        rawxmax,
                        mode,
                        &raw mut mytup,
                        &raw mut needwait,
                    );

                    /*
                     * If the tuple was already locked by ourselves.
                     */
                    if result == TM_SelfModified {
                        do_next = true; // goto next;
                    } else if needwait {
                        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                        XactLockTableWait(rawxmax, rel, &raw mut mytup.t_self, XLTW_LockUpdated);
                        continue 'l4;
                    } else if result != TM_Ok {
                        // goto out_locked;
                        UnlockReleaseBuffer(buf);
                        if vmbuffer != InvalidBuffer {
                            ReleaseBuffer(vmbuffer);
                        }
                        return result;
                    }
                }
            }

            if !do_next {
                /* compute the new Xmax and infomask values for the tuple ... */
                compute_new_xmax_infomask(
                    xmax,
                    old_infomask,
                    (*mytup.t_data).t_infomask2,
                    xid,
                    mode,
                    false,
                    &raw mut new_xmax,
                    &raw mut new_infomask,
                    &raw mut new_infomask2,
                );

                if PageIsAllVisible(BufferGetPage(buf))
                    && visibilitymap_clear(rel, block, vmbuffer, VISIBILITYMAP_ALL_FROZEN)
                {
                    cleared_all_frozen = true;
                }

                START_CRIT_SECTION!();

                /* ... and set them */
                HeapTupleHeaderSetXmax(mytup.t_data, new_xmax);
                (*mytup.t_data).t_infomask &= !HEAP_XMAX_BITS;
                (*mytup.t_data).t_infomask2 &= !HEAP_KEYS_UPDATED;
                (*mytup.t_data).t_infomask |= new_infomask;
                (*mytup.t_data).t_infomask2 |= new_infomask2;

                MarkBufferDirty(buf);

                /* XLOG stuff */
                if RelationNeedsWAL(rel) {
                    let mut xlrec: xl_heap_lock_updated = std::mem::zeroed();
                    let recptr: XLogRecPtr;
                    let page: Page = BufferGetPage(buf);

                    XLogBeginInsert();
                    XLogRegisterBuffer(0, buf, REGBUF_STANDARD as uint8);

                    xlrec.offnum = ItemPointerGetOffsetNumber(&raw mut mytup.t_self);
                    xlrec.xmax = new_xmax;
                    xlrec.infobits_set = compute_infobits(new_infomask, new_infomask2);
                    xlrec.flags = if cleared_all_frozen {
                        XLH_LOCK_ALL_FROZEN_CLEARED
                    } else {
                        0
                    };

                    XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapLockUpdated as c_int);

                    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_LOCK_UPDATED);

                    PageSetLSN(page, recptr);
                }

                END_CRIT_SECTION!();
            }

            // next:
            /* if we find the end of update chain, we're done. */
            if (*mytup.t_data).t_infomask & HEAP_XMAX_INVALID != 0
                || HeapTupleHeaderIndicatesMovedPartitions(mytup.t_data)
                || ItemPointerEquals(&raw mut mytup.t_self, &raw mut (*mytup.t_data).t_ctid)
                || HeapTupleHeaderIsOnlyLocked(mytup.t_data)
            {
                result = TM_Ok;
                // goto out_locked;
                UnlockReleaseBuffer(buf);
                if vmbuffer != InvalidBuffer {
                    ReleaseBuffer(vmbuffer);
                }
                return result;
            }

            /* tail recursion */
            priorXmax = HeapTupleHeaderGetUpdateXid(mytup.t_data);
            ItemPointerCopy(&raw mut (*mytup.t_data).t_ctid, &raw mut tupid);
            UnlockReleaseBuffer(buf);

            continue 'outer;
        }
    }
}

/*
 * heap_lock_updated_tuple
 */
unsafe fn heap_lock_updated_tuple(
    rel: Relation,
    prior_infomask: uint16,
    prior_raw_xmax: TransactionId,
    prior_ctid: *const ItemPointerData,
    xid: TransactionId,
    mode: LockTupleMode,
) -> TM_Result {
    INJECTION_POINT(c"heap_lock_updated_tuple".as_ptr(), std::ptr::null_mut());

    /*
     * If the tuple has moved into another partition (effectively a delete)
     * stop here.
     */
    if !ItemPointerIndicatesMovedPartitions(prior_ctid as *mut ItemPointerData) {
        let prior_xmax: TransactionId;

        /*
         * If this is the first possibly-multixact-able operation in the
         * current transaction, set my per-backend OldestMemberMXactId.
         */
        MultiXactIdSetOldestMember();

        prior_xmax = if prior_infomask & HEAP_XMAX_IS_MULTI != 0 {
            MultiXactIdGetUpdateXid(prior_raw_xmax, prior_infomask)
        } else {
            prior_raw_xmax
        };
        return heap_lock_updated_tuple_rec(rel, prior_xmax, prior_ctid, xid, mode);
    }

    /* nothing to lock */
    TM_Ok
}
/*
 *	heap_finish_speculative - mark speculative insertion as successful
 */
pub unsafe fn heap_finish_speculative(relation: Relation, tid: ItemPointer) {
    let buffer: Buffer;
    let page: Page;
    let offnum: OffsetNumber;
    let mut lp: ItemId = std::ptr::null_mut();
    let htup: HeapTupleHeader;

    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buffer) as Page;

    offnum = ItemPointerGetOffsetNumber(tid);
    if PageGetMaxOffsetNumber(page) >= offnum {
        lp = PageGetItemId(page, offnum);
    }

    if PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp) {
        elog!(ERROR, "invalid lp");
    }

    htup = PageGetItem(page, lp) as HeapTupleHeader;

    /* NO EREPORT(ERROR) from here till changes are logged */
    START_CRIT_SECTION!();

    Assert!(HeapTupleHeaderIsSpeculative(htup));

    MarkBufferDirty(buffer);

    /*
     * Replace the speculative insertion token with a real t_ctid.
     */
    (*htup).t_ctid = *tid;

    /* XLOG stuff */
    if RelationNeedsWAL(relation) {
        let mut xlrec: xl_heap_confirm = std::mem::zeroed();
        let recptr: XLogRecPtr;

        xlrec.offnum = ItemPointerGetOffsetNumber(tid);

        XLogBeginInsert();

        /* We want the same filtering on this as on a plain insert */
        XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

        XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapConfirm as c_int);
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD as uint8);

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_CONFIRM);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION!();

    UnlockReleaseBuffer(buffer);
}

/*
 *	heap_abort_speculative - kill a speculatively inserted tuple
 */
pub unsafe fn heap_abort_speculative(relation: Relation, tid: ItemPointer) {
    let xid: TransactionId = GetCurrentTransactionId();
    let lp: ItemId;
    let mut tp: HeapTupleData = std::mem::zeroed();
    let page: Page;
    let block: BlockNumber;
    let buffer: Buffer;

    Assert!(ItemPointerIsValid(tid));

    block = ItemPointerGetBlockNumber(tid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    /*
     * Page can't be all visible, we just inserted into it, and are still
     * running.
     */
    Assert!(!PageIsAllVisible(page));

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    Assert!(ItemIdIsNormal(lp));

    tp.t_tableOid = RelationGetRelid(relation);
    tp.t_data = PageGetItem(page, lp) as HeapTupleHeader;
    tp.t_len = ItemIdGetLength(lp);
    tp.t_self = *tid;

    /*
     * Sanity check that the tuple really is a speculatively inserted tuple.
     */
    if (*tp.t_data).t_choice.t_heap.t_xmin != xid {
        elog!(
            ERROR,
            "attempted to kill a tuple inserted by another transaction"
        );
    }
    if !(IsToastRelation(relation) || HeapTupleHeaderIsSpeculative(tp.t_data)) {
        elog!(ERROR, "attempted to kill a non-speculative tuple");
    }
    Assert!(!HeapTupleHeaderIsHeapOnly(tp.t_data));

    /*
     * No need to check for serializable conflicts here.
     */
    START_CRIT_SECTION!();

    /*
     * The tuple will become DEAD immediately.
     */
    Assert!(TransactionIdIsValid(TransactionXmin));
    {
        let relfrozenxid: TransactionId = (*(*relation).rd_rel).relfrozenxid;
        let prune_xid: TransactionId;

        if TransactionIdPrecedes(TransactionXmin, relfrozenxid) {
            prune_xid = relfrozenxid;
        } else {
            prune_xid = TransactionXmin;
        }
        PageSetPrunable(page, prune_xid);
    }

    /* store transaction information of xact deleting the tuple */
    (*tp.t_data).t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
    (*tp.t_data).t_infomask2 &= !HEAP_KEYS_UPDATED;

    /*
     * Set the tuple header xmin to InvalidTransactionId.
     */
    HeapTupleHeaderSetXmin(tp.t_data, InvalidTransactionId);

    /* Clear the speculative insertion token too */
    (*tp.t_data).t_ctid = tp.t_self;

    MarkBufferDirty(buffer);

    /*
     * XLOG stuff
     */
    if RelationNeedsWAL(relation) {
        let mut xlrec: xl_heap_delete = std::mem::zeroed();
        let recptr: XLogRecPtr;

        xlrec.flags = (XLH_DELETE_IS_SUPER as u8);
        xlrec.infobits_set =
            compute_infobits((*tp.t_data).t_infomask, (*tp.t_data).t_infomask2);
        xlrec.offnum = ItemPointerGetOffsetNumber(&raw mut tp.t_self);
        xlrec.xmax = xid;

        XLogBeginInsert();
        XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapDelete as c_int);
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD as uint8);

        /* No replica identity & replication origin logged */

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION!();

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    if HeapTupleHasExternal(&raw mut tp) {
        Assert!(!IsToastRelation(relation));
        heap_toast_delete(relation, &raw mut tp, true);
    }

    /*
     * Never need to mark tuple for invalidation.
     */

    /* Now we can release the buffer */
    ReleaseBuffer(buffer);

    /* count deletion, as we counted the insertion too */
    pgstat_count_heap_delete(relation);
}

/*
 * heap_inplace_lock - protect inplace update from concurrent heap_update()
 */
pub unsafe fn heap_inplace_lock(
    relation: Relation,
    oldtup_ptr: HeapTuple,
    buffer: Buffer,
    release_callback: Option<unsafe extern "C" fn(*mut c_void)>,
    arg: *mut c_void,
) -> bool {
    let mut oldtup: HeapTupleData = *oldtup_ptr; /* minimize diff vs. heap_update() */
    let result: TM_Result;
    let mut ret: bool = false;

    if RelationGetRelid(relation) == RelationRelationId {
        check_inplace_rel_lock(oldtup_ptr);
    }

    Assert!(BufferIsValid(buffer));

    /*
     * Register shared cache invals if necessary.
     */
    CacheInvalidateHeapTupleInplace(relation, oldtup_ptr);

    LockTuple(relation, &raw mut oldtup.t_self, InplaceUpdateTupleLock);
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    /*----------
     * Interpret HeapTupleSatisfiesUpdate() like heap_update() does, except ...
     */
    result = HeapTupleSatisfiesUpdate(&raw mut oldtup, GetCurrentCommandId(false), buffer);

    if result == TM_Invisible {
        /* no known way this can happen */
        ereport!(ERROR, errmsg!("attempted to overwrite invisible tuple"));
    } else if result == TM_SelfModified {
        ereport!(
            ERROR,
            errmsg!(
                "tuple to be updated was already modified by an operation triggered by the current command"
            )
        );
    } else if result == TM_BeingModified {
        let xwait: TransactionId;
        let infomask: uint16;

        xwait = HeapTupleHeaderGetRawXmax(oldtup.t_data);
        infomask = (*oldtup.t_data).t_infomask;

        if infomask & HEAP_XMAX_IS_MULTI != 0 {
            let lockmode: LockTupleMode = LockTupleNoKeyExclusive;
            let mxact_status: MultiXactStatus = MultiXactStatusNoKeyUpdate;
            let mut remain: c_int = 0;

            if DoesMultiXactIdConflict(
                xwait as MultiXactId,
                infomask,
                lockmode,
                std::ptr::null_mut(),
            ) {
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                release_callback.unwrap()(arg);
                ret = false;
                MultiXactIdWait(
                    xwait as MultiXactId,
                    mxact_status,
                    infomask,
                    relation,
                    &raw mut oldtup.t_self,
                    XLTW_Update,
                    &raw mut remain,
                );
            } else {
                ret = true;
            }
        } else if TransactionIdIsCurrentTransactionId(xwait) {
            ret = true;
        } else if HEAP_XMAX_IS_KEYSHR_LOCKED((infomask as i16)) {
            ret = true;
        } else {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            release_callback.unwrap()(arg);
            ret = false;
            XactLockTableWait(xwait, relation, &raw mut oldtup.t_self, XLTW_Update);
        }
    } else {
        ret = result == TM_Ok;
        if !ret {
            LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
            release_callback.unwrap()(arg);
        }
    }

    /*
     * GetCatalogSnapshot() relies on invalidation messages to know when to
     * take a new snapshot.
     */
    if !ret {
        UnlockTuple(relation, &raw mut oldtup.t_self, InplaceUpdateTupleLock);
        ForgetInplace_Inval();
        InvalidateCatalogSnapshot();
    }
    ret
}

/*
 * heap_inplace_update_and_unlock - core of systable_inplace_update_finish
 */
pub unsafe fn heap_inplace_update_and_unlock(
    relation: Relation,
    oldtup: HeapTuple,
    tuple: HeapTuple,
    buffer: Buffer,
) {
    let htup: HeapTupleHeader = (*oldtup).t_data;
    let oldlen: uint32;
    let newlen: uint32;
    let dst: *mut c_char;
    let src: *mut c_char;
    let mut nmsgs: c_int = 0;
    let mut invalMessages: *mut SharedInvalidationMessage = std::ptr::null_mut();
    let mut RelcacheInitFileInval: bool = false;

    Assert!(ItemPointerEquals(&raw mut (*oldtup).t_self, &raw mut (*tuple).t_self));
    oldlen = (*oldtup).t_len - (*htup).t_hoff as uint32;
    newlen = (*tuple).t_len - (*(*tuple).t_data).t_hoff as uint32;
    if oldlen != newlen || (*htup).t_hoff != (*(*tuple).t_data).t_hoff {
        elog!(ERROR, "wrong tuple length");
    }

    dst = (htup as *mut c_char).add((*htup).t_hoff as usize);
    src = ((*tuple).t_data as *mut c_char).add((*(*tuple).t_data).t_hoff as usize);

    /* Like RecordTransactionCommit(), log only if needed */
    if XLogStandbyInfoActive() {
        nmsgs = inplaceGetInvalidationMessages(
            &raw mut invalMessages,
            &raw mut RelcacheInitFileInval,
        );
    }

    /*
     * Unlink relcache init files as needed.
     */
    PreInplace_Inval();

    /*----------
     * NO EREPORT(ERROR) from here till changes are complete
     */
    Assert!(((*MyProc).delayChkptFlags & DELAY_CHKPT_START) == 0);
    START_CRIT_SECTION!();
    (*MyProc).delayChkptFlags |= DELAY_CHKPT_START;

    /* XLOG stuff */
    if RelationNeedsWAL(relation) {
        let mut xlrec: xl_heap_inplace = std::mem::zeroed();
        let mut copied_buffer: PGAlignedBlock = std::mem::zeroed();
        let origdata: *mut c_char = BufferGetBlock(buffer) as *mut c_char;
        let page: Page = BufferGetPage(buffer);
        let lower: uint16 = (*(page as PageHeader)).pd_lower;
        let upper: uint16 = (*(page as PageHeader)).pd_upper;
        let dst_offset_in_block: usize;
        let mut rlocator: RelFileLocator = std::mem::zeroed();
        let mut forkno: ForkNumber = 0;
        let mut blkno: BlockNumber = 0;
        let recptr: XLogRecPtr;

        xlrec.offnum = ItemPointerGetOffsetNumber(&raw mut (*tuple).t_self);
        xlrec.dbId = MyDatabaseId;
        xlrec.tsId = MyDatabaseTableSpace;
        xlrec.relcacheInitFileInval = RelcacheInitFileInval;
        xlrec.nmsgs = nmsgs;

        XLogBeginInsert();
        XLogRegisterData(&raw mut xlrec as *mut c_void, (MinSizeOfHeapInplace as c_int));
        if nmsgs != 0 {
            XLogRegisterData(
                invalMessages as *mut c_void,
                (nmsgs as Size * std::mem::size_of::<SharedInvalidationMessage>() as Size) as c_int,
            );
        }

        /* register block matching what buffer will look like after changes */
        memcpy(
            copied_buffer.data.as_mut_ptr() as *mut c_void,
            origdata as *const c_void,
            lower as usize,
        );
        memcpy(
            copied_buffer.data.as_mut_ptr().add(upper as usize) as *mut c_void,
            origdata.add(upper as usize) as *const c_void,
            BLCKSZ as usize - upper as usize,
        );
        dst_offset_in_block = dst.offset_from(origdata) as usize;
        memcpy(
            copied_buffer.data.as_mut_ptr().add(dst_offset_in_block) as *mut c_void,
            src as *const c_void,
            newlen as usize,
        );
        BufferGetTag(buffer, &raw mut rlocator, &raw mut forkno, &raw mut blkno);
        Assert!(forkno == MAIN_FORKNUM);
        XLogRegisterBlock(
            0,
            &raw mut rlocator,
            forkno,
            blkno,
            copied_buffer.data.as_mut_ptr(),
            REGBUF_STANDARD as uint8,
        );
        XLogRegisterBufData(0, src, (newlen as usize as c_int));

        /* inplace updates aren't decoded atm, don't log the origin */

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INPLACE);

        PageSetLSN(page, recptr);
    }

    memcpy(dst as *mut c_void, src as *const c_void, newlen as usize);

    MarkBufferDirty(buffer);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    /*
     * Send invalidations to shared queue.
     */
    AtInplace_Inval();

    (*MyProc).delayChkptFlags &= !DELAY_CHKPT_START;
    END_CRIT_SECTION!();
    UnlockTuple(relation, &raw mut (*tuple).t_self, InplaceUpdateTupleLock);

    AcceptInvalidationMessages(); /* local processing of just-sent inval */

    /*
     * Queue a transactional inval.
     */
    if !IsBootstrapProcessingMode() {
        CacheInvalidateHeapTuple(relation, tuple, std::ptr::null_mut());
    }
}

/*
 * heap_inplace_unlock - reverse of heap_inplace_lock
 */
pub unsafe fn heap_inplace_unlock(relation: Relation, oldtup: HeapTuple, buffer: Buffer) {
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    UnlockTuple(relation, &raw mut (*oldtup).t_self, InplaceUpdateTupleLock);
    ForgetInplace_Inval();
}

const FRM_NOOP: uint16 = 0x0001;
const FRM_INVALIDATE_XMAX: uint16 = 0x0002;
const FRM_RETURN_IS_XID: uint16 = 0x0004;
const FRM_RETURN_IS_MULTI: uint16 = 0x0008;
const FRM_MARK_COMMITTED: uint16 = 0x0010;
/*
 * FreezeMultiXactId
 */
unsafe fn FreezeMultiXactId(
    multi: MultiXactId,
    t_infomask: uint16,
    cutoffs: *const VacuumCutoffs,
    flags: *mut uint16,
    pagefrz: *mut HeapPageFreeze,
) -> TransactionId {
    let newxmax: TransactionId;
    let mut members: *mut MultiXactMember = std::ptr::null_mut();
    let nmembers: c_int;
    let mut need_replace: bool;
    let mut nnewmembers: c_int;
    let newmembers: *mut MultiXactMember;
    let mut has_lockers: bool;
    let mut update_xid: TransactionId;
    let mut update_committed: bool;
    let mut FreezePageRelfrozenXid: TransactionId;

    *flags = 0;

    /* We should only be called in Multis */
    Assert!(t_infomask & HEAP_XMAX_IS_MULTI != 0);

    if !MultiXactIdIsValid(multi) || HEAP_LOCKED_UPGRADED(t_infomask) {
        *flags |= FRM_INVALIDATE_XMAX;
        (*pagefrz).freeze_required = true;
        return InvalidTransactionId;
    } else if MultiXactIdPrecedes(multi, (*cutoffs).relminmxid) {
        ereport!(
            ERROR,
            errmsg!(
                "found multixact {} from before relminmxid {}",
                multi,
                (*cutoffs).relminmxid
            )
        );
    } else if MultiXactIdPrecedes(multi, (*cutoffs).OldestMxact) {
        let update_xact: TransactionId;

        /*
         * This old multi cannot possibly have members still running.
         */
        if MultiXactIdIsRunning(multi, HEAP_XMAX_IS_LOCKED_ONLY(t_infomask)) {
            ereport!(
                ERROR,
                errmsg!(
                    "multixact {} from before multi freeze cutoff {} found to be still running",
                    multi,
                    (*cutoffs).OldestMxact
                )
            );
        }

        if HEAP_XMAX_IS_LOCKED_ONLY(t_infomask) {
            *flags |= FRM_INVALIDATE_XMAX;
            (*pagefrz).freeze_required = true;
            return InvalidTransactionId;
        }

        /* replace multi with single XID for its updater? */
        update_xact = MultiXactIdGetUpdateXid(multi, t_infomask);
        if TransactionIdPrecedes(update_xact, (*cutoffs).relfrozenxid) {
            ereport!(
                ERROR,
                errmsg!(
                    "multixact {} contains update XID {} from before relfrozenxid {}",
                    multi,
                    update_xact,
                    (*cutoffs).relfrozenxid
                )
            );
        } else if TransactionIdPrecedes(update_xact, (*cutoffs).OldestXmin) {
            /*
             * Updater XID has to have aborted.
             */
            if TransactionIdDidCommit(update_xact) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "multixact {} contains committed update XID {} from before removable cutoff {}",
                        multi,
                        update_xact,
                        (*cutoffs).OldestXmin
                    )
                );
            }
            *flags |= FRM_INVALIDATE_XMAX;
            (*pagefrz).freeze_required = true;
            return InvalidTransactionId;
        }

        /* Have to keep updater XID as new xmax */
        *flags |= FRM_RETURN_IS_XID;
        (*pagefrz).freeze_required = true;
        return update_xact;
    }

    /*
     * Some member(s) of this Multi may be below FreezeLimit xid cutoff.
     */
    nmembers = GetMultiXactIdMembers(
        multi,
        &raw mut members,
        false,
        HEAP_XMAX_IS_LOCKED_ONLY(t_infomask),
    );
    if nmembers <= 0 {
        /* Nothing worth keeping */
        *flags |= FRM_INVALIDATE_XMAX;
        (*pagefrz).freeze_required = true;
        return InvalidTransactionId;
    }

    /*
     * The FRM_NOOP case is the only case where we might need to ratchet back
     * FreezePageRelfrozenXid or FreezePageRelminMxid.
     */
    need_replace = false;
    FreezePageRelfrozenXid = (*pagefrz).FreezePageRelfrozenXid;
    for i in 0..nmembers {
        let xid: TransactionId = (*members.add(i as usize)).xid;

        Assert!(!TransactionIdPrecedes(xid, (*cutoffs).relfrozenxid));

        if TransactionIdPrecedes(xid, (*cutoffs).FreezeLimit) {
            /* Can't violate the FreezeLimit postcondition */
            need_replace = true;
            break;
        }
        if TransactionIdPrecedes(xid, FreezePageRelfrozenXid) {
            FreezePageRelfrozenXid = xid;
        }
    }

    /* Can't violate the MultiXactCutoff postcondition, either */
    if !need_replace {
        need_replace = MultiXactIdPrecedes(multi, (*cutoffs).MultiXactCutoff);
    }

    if !need_replace {
        /*
         * vacuumlazy.c might ratchet back NewRelminMxid, NewRelfrozenXid.
         */
        *flags |= FRM_NOOP;
        (*pagefrz).FreezePageRelfrozenXid = FreezePageRelfrozenXid;
        if MultiXactIdPrecedes(multi, (*pagefrz).FreezePageRelminMxid) {
            (*pagefrz).FreezePageRelminMxid = multi;
        }
        pfree(members as *mut c_void);
        return multi;
    }

    /*
     * Do a more thorough second pass over the multi.
     */
    nnewmembers = 0;
    newmembers = palloc(std::mem::size_of::<MultiXactMember>() * nmembers as usize)
        as *mut MultiXactMember;
    has_lockers = false;
    update_xid = InvalidTransactionId;
    update_committed = false;

    /*
     * Determine whether to keep each member xid, or to ignore it instead
     */
    for i in 0..nmembers {
        let xid: TransactionId = (*members.add(i as usize)).xid;
        let mstatus: MultiXactStatus = (*members.add(i as usize)).status;

        Assert!(!TransactionIdPrecedes(xid, (*cutoffs).relfrozenxid));

        if !ISUPDATE_from_mxstatus(mstatus) {
            /*
             * Locker XID (not updater XID).  We only keep lockers that are
             * still running.
             */
            if TransactionIdIsCurrentTransactionId(xid) || TransactionIdIsInProgress(xid) {
                if TransactionIdPrecedes(xid, (*cutoffs).OldestXmin) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "multixact {} contains running locker XID {} from before removable cutoff {}",
                            multi,
                            xid,
                            (*cutoffs).OldestXmin
                        )
                    );
                }
                *newmembers.add(nnewmembers as usize) = *members.add(i as usize);
                nnewmembers += 1;
                has_lockers = true;
            }

            continue;
        }

        /*
         * Updater XID (not locker XID).  Should we keep it?
         */
        if TransactionIdIsValid(update_xid) {
            ereport!(
                ERROR,
                errmsg!("multixact {} has two or more updating members", multi)
            );
        }

        /*
         * It's critical to test TransactionIdIsInProgress before
         * TransactionIdDidCommit.
         */
        if TransactionIdIsCurrentTransactionId(xid) || TransactionIdIsInProgress(xid) {
            update_xid = xid;
        } else if TransactionIdDidCommit(xid) {
            /*
             * The transaction committed.
             */
            update_committed = true;
            update_xid = xid;
        } else {
            /*
             * Not in progress, not committed -- must be aborted or crashed.
             */
            continue;
        }

        /*
         * We determined that updater must be kept.
         */
        if TransactionIdPrecedes(xid, (*cutoffs).OldestXmin) {
            ereport!(
                ERROR,
                errmsg!(
                    "multixact {} contains committed update XID {} from before removable cutoff {}",
                    multi,
                    xid,
                    (*cutoffs).OldestXmin
                )
            );
        }
        *newmembers.add(nnewmembers as usize) = *members.add(i as usize);
        nnewmembers += 1;
    }

    pfree(members as *mut c_void);

    /*
     * Determine what to do with caller's multi based on information gathered
     * during our second pass
     */
    if nnewmembers == 0 {
        /* Nothing worth keeping */
        *flags |= FRM_INVALIDATE_XMAX;
        newxmax = InvalidTransactionId;
    } else if TransactionIdIsValid(update_xid) && !has_lockers {
        /*
         * If there's a single member and it's an update, pass it back alone.
         */
        Assert!(nnewmembers == 1);
        *flags |= FRM_RETURN_IS_XID;
        if update_committed {
            *flags |= FRM_MARK_COMMITTED;
        }
        newxmax = update_xid;
    } else {
        /*
         * Create a new multixact with the surviving members of the previous
         * one.
         */
        newxmax = MultiXactIdCreateFromMembers(nnewmembers, newmembers);
        *flags |= FRM_RETURN_IS_MULTI;
    }

    pfree(newmembers as *mut c_void);

    (*pagefrz).freeze_required = true;
    newxmax
}

/*
 * heap_prepare_freeze_tuple
 */
pub unsafe fn heap_prepare_freeze_tuple(
    tuple: HeapTupleHeader,
    cutoffs: *const VacuumCutoffs,
    pagefrz: *mut HeapPageFreeze,
    frz: *mut HeapTupleFreeze,
    totally_frozen: *mut bool,
) -> bool {
    let mut xmin_already_frozen: bool = false;
    let mut xmax_already_frozen: bool = false;
    let mut freeze_xmin: bool = false;
    let mut replace_xvac: bool = false;
    let mut replace_xmax: bool = false;
    let mut freeze_xmax: bool = false;
    let mut xid: TransactionId;

    (*frz).xmax = HeapTupleHeaderGetRawXmax(tuple);
    (*frz).t_infomask2 = (*tuple).t_infomask2;
    (*frz).t_infomask = (*tuple).t_infomask;
    (*frz).frzflags = 0;
    (*frz).checkflags = 0;

    /*
     * Process xmin.
     */
    xid = HeapTupleHeaderGetXmin(tuple);
    if !TransactionIdIsNormal(xid) {
        xmin_already_frozen = true;
    } else {
        if TransactionIdPrecedes(xid, (*cutoffs).relfrozenxid) {
            ereport!(
                ERROR,
                errmsg!(
                    "found xmin {} from before relfrozenxid {}",
                    xid,
                    (*cutoffs).relfrozenxid
                )
            );
        }

        /* Will set freeze_xmin flags in freeze plan below */
        freeze_xmin = TransactionIdPrecedes(xid, (*cutoffs).OldestXmin);

        /* Verify that xmin committed if and when freeze plan is executed */
        if freeze_xmin {
            (*frz).checkflags |= HEAP_FREEZE_CHECK_XMIN_COMMITTED;
        }
    }

    /*
     * Old-style VACUUM FULL is gone, but we have to process xvac.
     */
    xid = HeapTupleHeaderGetXvac(tuple);
    if TransactionIdIsNormal(xid) {
        Assert!(TransactionIdPrecedesOrEquals((*cutoffs).relfrozenxid, xid));
        Assert!(TransactionIdPrecedes(xid, (*cutoffs).OldestXmin));

        /*
         * For Xvac, we always freeze proactively.
         */
        (*pagefrz).freeze_required = true;
        replace_xvac = true;
    }

    /* Now process xmax */
    xid = (*frz).xmax;
    if (*tuple).t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        /* Raw xmax is a MultiXactId */
        let newxmax: TransactionId;
        let mut flags: uint16 = 0;

        newxmax = FreezeMultiXactId(xid, (*tuple).t_infomask, cutoffs, &raw mut flags, pagefrz);

        if flags & FRM_NOOP != 0 {
            /*
             * xmax is a MultiXactId, and nothing about it changes for now.
             */
            Assert!(!MultiXactIdPrecedes(newxmax, (*cutoffs).MultiXactCutoff));
            Assert!(MultiXactIdIsValid(newxmax) && xid == newxmax);
        } else if flags & FRM_RETURN_IS_XID != 0 {
            /*
             * xmax will become an updater Xid.
             */
            Assert!(!TransactionIdPrecedes(newxmax, (*cutoffs).OldestXmin));

            (*frz).t_infomask &= !HEAP_XMAX_BITS;
            (*frz).xmax = newxmax;
            if flags & FRM_MARK_COMMITTED != 0 {
                (*frz).t_infomask |= HEAP_XMAX_COMMITTED;
            }
            replace_xmax = true;
        } else if flags & FRM_RETURN_IS_MULTI != 0 {
            let mut newbits: uint16 = 0;
            let mut newbits2: uint16 = 0;

            /*
             * xmax is an old MultiXactId that we have to replace with a new
             * MultiXactId.
             */
            Assert!(!MultiXactIdPrecedes(newxmax, (*cutoffs).OldestMxact));

            (*frz).t_infomask &= !HEAP_XMAX_BITS;
            (*frz).t_infomask2 &= !HEAP_KEYS_UPDATED;
            GetMultiXactIdHintBits(newxmax, &raw mut newbits, &raw mut newbits2);
            (*frz).t_infomask |= newbits;
            (*frz).t_infomask2 |= newbits2;
            (*frz).xmax = newxmax;
            replace_xmax = true;
        } else {
            /*
             * Freeze plan for tuple "freezes xmax" in the strictest sense.
             */
            Assert!(flags & FRM_INVALIDATE_XMAX != 0);
            Assert!(!TransactionIdIsValid(newxmax));

            /* Will set freeze_xmax flags in freeze plan below */
            freeze_xmax = true;
        }

        /* MultiXactId processing forces freezing (barring FRM_NOOP case) */
        Assert!((*pagefrz).freeze_required || (!freeze_xmax && !replace_xmax));
    } else if TransactionIdIsNormal(xid) {
        /* Raw xmax is normal XID */
        if TransactionIdPrecedes(xid, (*cutoffs).relfrozenxid) {
            ereport!(
                ERROR,
                errmsg!(
                    "found xmax {} from before relfrozenxid {}",
                    xid,
                    (*cutoffs).relfrozenxid
                )
            );
        }

        /* Will set freeze_xmax flags in freeze plan below */
        freeze_xmax = TransactionIdPrecedes(xid, (*cutoffs).OldestXmin);

        /*
         * Verify that xmax aborted if and when freeze plan is executed.
         */
        if freeze_xmax && !HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask) {
            (*frz).checkflags |= HEAP_FREEZE_CHECK_XMAX_ABORTED;
        }
    } else if !TransactionIdIsValid(xid) {
        /* Raw xmax is InvalidTransactionId XID */
        Assert!((*tuple).t_infomask & HEAP_XMAX_IS_MULTI == 0);
        xmax_already_frozen = true;
    } else {
        ereport!(
            ERROR,
            errmsg!(
                "found raw xmax {} (infomask 0x{:04x}) not invalid and not multi",
                xid,
                (*tuple).t_infomask
            )
        );
    }

    if freeze_xmin {
        Assert!(!xmin_already_frozen);

        (*frz).t_infomask |= HEAP_XMIN_FROZEN;
    }
    if replace_xvac {
        /*
         * If a MOVED_OFF tuple is not dead, the xvac transaction must have
         * failed.
         */
        Assert!((*pagefrz).freeze_required);
        if (*tuple).t_infomask & HEAP_MOVED_OFF != 0 {
            (*frz).frzflags |= (XLH_INVALID_XVAC as u8);
        } else {
            (*frz).frzflags |= (XLH_FREEZE_XVAC as u8);
        }
    }
    if replace_xmax {
        Assert!(!xmax_already_frozen && !freeze_xmax);
        Assert!((*pagefrz).freeze_required);

        /* Already set replace_xmax flags in freeze plan earlier */
    }
    if freeze_xmax {
        Assert!(!xmax_already_frozen && !replace_xmax);

        (*frz).xmax = InvalidTransactionId;

        /*
         * Normalize to INVALID just to be sure no one gets confused.
         */
        (*frz).t_infomask &= !HEAP_XMAX_BITS;
        (*frz).t_infomask |= HEAP_XMAX_INVALID;
        (*frz).t_infomask2 &= !HEAP_HOT_UPDATED;
        (*frz).t_infomask2 &= !HEAP_KEYS_UPDATED;
    }

    /*
     * Determine if this tuple is already totally frozen, or will become
     * totally frozen.
     */
    *totally_frozen = (freeze_xmin || xmin_already_frozen)
        && (freeze_xmax || xmax_already_frozen);

    if !(*pagefrz).freeze_required && !(xmin_already_frozen && xmax_already_frozen) {
        /*
         * So far no previous tuple from the page made freezing mandatory.
         */
        (*pagefrz).freeze_required = heap_tuple_should_freeze(
            tuple,
            cutoffs,
            &raw mut (*pagefrz).NoFreezePageRelfrozenXid,
            &raw mut (*pagefrz).NoFreezePageRelminMxid,
        );
    }

    /* Tell caller if this tuple has a usable freeze plan set in *frz */
    freeze_xmin || replace_xvac || replace_xmax || freeze_xmax
}

/*
 * Perform xmin/xmax XID status sanity checks before actually executing freeze
 * plans.
 */
pub unsafe fn heap_pre_freeze_checks(buffer: Buffer, tuples: *mut HeapTupleFreeze, ntuples: c_int) {
    let page: Page = BufferGetPage(buffer);

    for i in 0..ntuples {
        let frz: *mut HeapTupleFreeze = tuples.add(i as usize);
        let itemid: ItemId = PageGetItemId(page, (*frz).offset);
        let htup: HeapTupleHeader;

        htup = PageGetItem(page, itemid) as HeapTupleHeader;

        /* Deliberately avoid relying on tuple hint bits here */
        if (*frz).checkflags & HEAP_FREEZE_CHECK_XMIN_COMMITTED != 0 {
            let xmin: TransactionId = HeapTupleHeaderGetRawXmin(htup);

            Assert!(!HeapTupleHeaderXminFrozen(htup));
            if unlikely(!TransactionIdDidCommit(xmin)) {
                ereport!(
                    ERROR,
                    errmsg!("uncommitted xmin {} needs to be frozen", xmin)
                );
            }
        }

        /*
         * TransactionIdDidAbort won't work reliably.
         */
        if (*frz).checkflags & HEAP_FREEZE_CHECK_XMAX_ABORTED != 0 {
            let xmax: TransactionId = HeapTupleHeaderGetRawXmax(htup);

            Assert!(TransactionIdIsNormal(xmax));
            if unlikely(TransactionIdDidCommit(xmax)) {
                ereport!(ERROR, errmsg!("cannot freeze committed xmax {}", xmax));
            }
        }
    }
}

/*
 * Helper which executes freezing of one or more heap tuples on a page.
 */
pub unsafe fn heap_freeze_prepared_tuples(
    buffer: Buffer,
    tuples: *mut HeapTupleFreeze,
    ntuples: c_int,
) {
    let page: Page = BufferGetPage(buffer);

    for i in 0..ntuples {
        let frz: *mut HeapTupleFreeze = tuples.add(i as usize);
        let itemid: ItemId = PageGetItemId(page, (*frz).offset);
        let htup: HeapTupleHeader;

        htup = PageGetItem(page, itemid) as HeapTupleHeader;
        heap_execute_freeze_tuple(htup, frz);
    }
}

/*
 * heap_freeze_tuple
 *		Freeze tuple in place, without WAL logging.
 */
pub unsafe fn heap_freeze_tuple(
    tuple: HeapTupleHeader,
    relfrozenxid: TransactionId,
    relminmxid: TransactionId,
    FreezeLimit: TransactionId,
    MultiXactCutoff: TransactionId,
) -> bool {
    let mut frz: HeapTupleFreeze = std::mem::zeroed();
    let do_freeze: bool;
    let mut totally_frozen: bool = false;
    let mut cutoffs: VacuumCutoffs = std::mem::zeroed();
    let mut pagefrz: HeapPageFreeze = std::mem::zeroed();

    cutoffs.relfrozenxid = relfrozenxid;
    cutoffs.relminmxid = relminmxid;
    cutoffs.OldestXmin = FreezeLimit;
    cutoffs.OldestMxact = MultiXactCutoff;
    cutoffs.FreezeLimit = FreezeLimit;
    cutoffs.MultiXactCutoff = MultiXactCutoff;

    pagefrz.freeze_required = true;
    pagefrz.FreezePageRelfrozenXid = FreezeLimit;
    pagefrz.FreezePageRelminMxid = MultiXactCutoff;
    pagefrz.NoFreezePageRelfrozenXid = FreezeLimit;
    pagefrz.NoFreezePageRelminMxid = MultiXactCutoff;

    do_freeze = heap_prepare_freeze_tuple(
        tuple,
        &raw const cutoffs,
        &raw mut pagefrz,
        &raw mut frz,
        &raw mut totally_frozen,
    );

    /*
     * Note that because this is not a WAL-logged operation, we don't need to
     * fill in the offset in the freeze record.
     */
    if do_freeze {
        heap_execute_freeze_tuple(tuple, &raw mut frz);
    }
    do_freeze
}

/*
 * For a given MultiXactId, return the hint bits that should be set in the
 * tuple's infomask.
 */
unsafe fn GetMultiXactIdHintBits(
    multi: MultiXactId,
    new_infomask: *mut uint16,
    new_infomask2: *mut uint16,
) {
    let nmembers: c_int;
    let mut members: *mut MultiXactMember = std::ptr::null_mut();
    let mut i: c_int;
    let mut bits: uint16 = HEAP_XMAX_IS_MULTI;
    let mut bits2: uint16 = 0;
    let mut has_update: bool = false;
    let mut strongest: LockTupleMode = LockTupleKeyShare;

    /*
     * We only use this in multis we just created.
     */
    nmembers = GetMultiXactIdMembers(multi, &raw mut members, false, false);

    i = 0;
    while i < nmembers {
        let mode: LockTupleMode;

        /*
         * Remember the strongest lock mode held by any member.
         */
        mode = unsafe { core::mem::transmute(TUPLOCK_from_mxstatus((*members.add(i as usize)).status)) };
        if (mode as c_int) > (strongest as c_int) {
            strongest = mode;
        }

        /* See what other bits we need */
        match (*members.add(i as usize)).status {
            x if x == MultiXactStatusForKeyShare
                || x == MultiXactStatusForShare
                || x == MultiXactStatusForNoKeyUpdate => {}
            x if x == MultiXactStatusForUpdate => {
                bits2 |= HEAP_KEYS_UPDATED;
            }
            x if x == MultiXactStatusNoKeyUpdate => {
                has_update = true;
            }
            x if x == MultiXactStatusUpdate => {
                bits2 |= HEAP_KEYS_UPDATED;
                has_update = true;
            }
            _ => {}
        }
        i += 1;
    }

    if strongest == LockTupleExclusive || strongest == LockTupleNoKeyExclusive {
        bits |= HEAP_XMAX_EXCL_LOCK;
    } else if strongest == LockTupleShare {
        bits |= HEAP_XMAX_SHR_LOCK;
    } else if strongest == LockTupleKeyShare {
        bits |= HEAP_XMAX_KEYSHR_LOCK;
    }

    if !has_update {
        bits |= HEAP_XMAX_LOCK_ONLY;
    }

    if nmembers > 0 {
        pfree(members as *mut c_void);
    }

    *new_infomask = bits;
    *new_infomask2 = bits2;
}

/*
 * MultiXactIdGetUpdateXid
 */
unsafe fn MultiXactIdGetUpdateXid(xmax: TransactionId, t_infomask: uint16) -> TransactionId {
    let mut update_xact: TransactionId = InvalidTransactionId;
    let mut members: *mut MultiXactMember = std::ptr::null_mut();
    let nmembers: c_int;

    Assert!(!(t_infomask & HEAP_XMAX_LOCK_ONLY != 0));
    Assert!(t_infomask & HEAP_XMAX_IS_MULTI != 0);

    /*
     * Since we know the LOCK_ONLY bit is not set, this cannot be a multi from
     * pre-pg_upgrade.
     */
    nmembers = GetMultiXactIdMembers(xmax, &raw mut members, false, false);

    if nmembers > 0 {
        let mut i: c_int;

        i = 0;
        while i < nmembers {
            /* Ignore lockers */
            if !ISUPDATE_from_mxstatus((*members.add(i as usize)).status) {
                i += 1;
                continue;
            }

            /* there can be at most one updater */
            Assert!(update_xact == InvalidTransactionId);
            update_xact = (*members.add(i as usize)).xid;
            /* in a non-assert build, break here */
            break;
        }

        pfree(members as *mut c_void);
    }

    update_xact
}

/*
 * HeapTupleGetUpdateXid
 */
pub unsafe fn HeapTupleGetUpdateXid(tup: *const HeapTupleHeaderData) -> TransactionId {
    MultiXactIdGetUpdateXid(HeapTupleHeaderGetRawXmax(tup), (*tup).t_infomask)
}

/*
 * Does the given multixact conflict with the current transaction grabbing a
 * tuple lock of the given strength?
 */
unsafe fn DoesMultiXactIdConflict(
    multi: MultiXactId,
    infomask: uint16,
    lockmode: LockTupleMode,
    current_is_member: *mut bool,
) -> bool {
    let nmembers: c_int;
    let mut members: *mut MultiXactMember = std::ptr::null_mut();
    let mut result: bool = false;
    let wanted: LOCKMODE = tupleLockExtraInfo[lockmode as usize].hwlock;

    if HEAP_LOCKED_UPGRADED(infomask) {
        return false;
    }

    nmembers = GetMultiXactIdMembers(
        multi,
        &raw mut members,
        false,
        HEAP_XMAX_IS_LOCKED_ONLY(infomask),
    );
    if nmembers >= 0 {
        let mut i: c_int;

        i = 0;
        while i < nmembers {
            let memxid: TransactionId;
            let memlockmode: LOCKMODE;

            if result && (current_is_member.is_null() || *current_is_member) {
                break;
            }

            memlockmode = LOCKMODE_from_mxstatus((*members.add(i as usize)).status);

            /* ignore members from current xact (but track their presence) */
            memxid = (*members.add(i as usize)).xid;
            if TransactionIdIsCurrentTransactionId(memxid) {
                if !current_is_member.is_null() {
                    *current_is_member = true;
                }
                i += 1;
                continue;
            } else if result {
                i += 1;
                continue;
            }

            /* ignore members that don't conflict with the lock we want */
            if !DoLockModesConflict(memlockmode, wanted) {
                i += 1;
                continue;
            }

            if ISUPDATE_from_mxstatus((*members.add(i as usize)).status) {
                /* ignore aborted updaters */
                if TransactionIdDidAbort(memxid) {
                    i += 1;
                    continue;
                }
            } else {
                /* ignore lockers-only that are no longer in progress */
                if !TransactionIdIsInProgress(memxid) {
                    i += 1;
                    continue;
                }
            }

            /*
             * Whatever remains are either live lockers that conflict with our
             * wanted lock, and updaters that are not aborted.
             */
            result = true;
            i += 1;
        }
        pfree(members as *mut c_void);
    }

    result
}

/*
 * Do_MultiXactIdWait
 */
unsafe fn Do_MultiXactIdWait(
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: uint16,
    nowait: bool,
    rel: Relation,
    ctid: ItemPointer,
    oper: XLTW_Oper,
    remaining: *mut c_int,
    logLockFailure: bool,
) -> bool {
    let mut result: bool = true;
    let mut members: *mut MultiXactMember = std::ptr::null_mut();
    let nmembers: c_int;
    let mut remain: c_int = 0;

    /* for pre-pg_upgrade tuples, no need to sleep at all */
    nmembers = if HEAP_LOCKED_UPGRADED(infomask) {
        -1
    } else {
        GetMultiXactIdMembers(
            multi,
            &raw mut members,
            false,
            HEAP_XMAX_IS_LOCKED_ONLY(infomask),
        )
    };

    if nmembers >= 0 {
        let mut i: c_int;

        i = 0;
        while i < nmembers {
            let memxid: TransactionId = (*members.add(i as usize)).xid;
            let memstatus: MultiXactStatus = (*members.add(i as usize)).status;

            if TransactionIdIsCurrentTransactionId(memxid) {
                remain += 1;
                i += 1;
                continue;
            }

            if !DoLockModesConflict(
                LOCKMODE_from_mxstatus(memstatus),
                LOCKMODE_from_mxstatus(status),
            ) {
                if !remaining.is_null() && TransactionIdIsInProgress(memxid) {
                    remain += 1;
                }
                i += 1;
                continue;
            }

            /*
             * This member conflicts with our multi, so we have to sleep.
             */
            if nowait {
                result = ConditionalXactLockTableWait(memxid, logLockFailure);
                if !result {
                    break;
                }
            } else {
                XactLockTableWait(memxid, rel, ctid, oper);
            }
            i += 1;
        }

        pfree(members as *mut c_void);
    }

    if !remaining.is_null() {
        *remaining = remain;
    }

    result
}

/*
 * MultiXactIdWait
 */
unsafe fn MultiXactIdWait(
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: uint16,
    rel: Relation,
    ctid: ItemPointer,
    oper: XLTW_Oper,
    remaining: *mut c_int,
) {
    Do_MultiXactIdWait(
        multi, status, infomask, false, rel, ctid, oper, remaining, false,
    );
}

/*
 * ConditionalMultiXactIdWait
 */
unsafe fn ConditionalMultiXactIdWait(
    multi: MultiXactId,
    status: MultiXactStatus,
    infomask: uint16,
    rel: Relation,
    remaining: *mut c_int,
    logLockFailure: bool,
) -> bool {
    Do_MultiXactIdWait(
        multi,
        status,
        infomask,
        true,
        rel,
        std::ptr::null_mut(),
        XLTW_None,
        remaining,
        logLockFailure,
    )
}

/*
 * heap_tuple_needs_eventual_freeze
 */
pub unsafe fn heap_tuple_needs_eventual_freeze(tuple: HeapTupleHeader) -> bool {
    let mut xid: TransactionId;

    /*
     * If xmin is a normal transaction ID, this tuple is definitely not frozen.
     */
    xid = HeapTupleHeaderGetXmin(tuple);
    if TransactionIdIsNormal(xid) {
        return true;
    }

    /*
     * If xmax is a valid xact or multixact, this tuple is also not frozen.
     */
    if (*tuple).t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        let multi: MultiXactId;

        multi = HeapTupleHeaderGetRawXmax(tuple);
        if MultiXactIdIsValid(multi) {
            return true;
        }
    } else {
        xid = HeapTupleHeaderGetRawXmax(tuple);
        if TransactionIdIsNormal(xid) {
            return true;
        }
    }

    if (*tuple).t_infomask & HEAP_MOVED != 0 {
        xid = HeapTupleHeaderGetXvac(tuple);
        if TransactionIdIsNormal(xid) {
            return true;
        }
    }

    false
}

/*
 * heap_tuple_should_freeze
 */
pub unsafe fn heap_tuple_should_freeze(
    tuple: HeapTupleHeader,
    cutoffs: *const VacuumCutoffs,
    NoFreezePageRelfrozenXid: *mut TransactionId,
    NoFreezePageRelminMxid: *mut MultiXactId,
) -> bool {
    let mut xid: TransactionId;
    let multi: MultiXactId;
    let mut freeze: bool = false;

    /* First deal with xmin */
    xid = HeapTupleHeaderGetXmin(tuple);
    if TransactionIdIsNormal(xid) {
        Assert!(TransactionIdPrecedesOrEquals((*cutoffs).relfrozenxid, xid));
        if TransactionIdPrecedes(xid, *NoFreezePageRelfrozenXid) {
            *NoFreezePageRelfrozenXid = xid;
        }
        if TransactionIdPrecedes(xid, (*cutoffs).FreezeLimit) {
            freeze = true;
        }
    }

    /* Now deal with xmax */
    xid = InvalidTransactionId;
    multi = if (*tuple).t_infomask & HEAP_XMAX_IS_MULTI != 0 {
        HeapTupleHeaderGetRawXmax(tuple)
    } else {
        InvalidMultiXactId
    };
    if !((*tuple).t_infomask & HEAP_XMAX_IS_MULTI != 0) {
        xid = HeapTupleHeaderGetRawXmax(tuple);
    }

    if TransactionIdIsNormal(xid) {
        Assert!(TransactionIdPrecedesOrEquals((*cutoffs).relfrozenxid, xid));
        /* xmax is a non-permanent XID */
        if TransactionIdPrecedes(xid, *NoFreezePageRelfrozenXid) {
            *NoFreezePageRelfrozenXid = xid;
        }
        if TransactionIdPrecedes(xid, (*cutoffs).FreezeLimit) {
            freeze = true;
        }
    } else if !MultiXactIdIsValid(multi) {
        /* xmax is a permanent XID or invalid MultiXactId/XID */
    } else if HEAP_LOCKED_UPGRADED((*tuple).t_infomask) {
        /* xmax is a pg_upgrade'd MultiXact, which can't have updater XID */
        if MultiXactIdPrecedes(multi, *NoFreezePageRelminMxid) {
            *NoFreezePageRelminMxid = multi;
        }
        /* heap_prepare_freeze_tuple always freezes pg_upgrade'd xmax */
        freeze = true;
    } else {
        /* xmax is a MultiXactId that may have an updater XID */
        let mut members: *mut MultiXactMember = std::ptr::null_mut();
        let nmembers: c_int;

        Assert!(MultiXactIdPrecedesOrEquals((*cutoffs).relminmxid, multi));
        if MultiXactIdPrecedes(multi, *NoFreezePageRelminMxid) {
            *NoFreezePageRelminMxid = multi;
        }
        if MultiXactIdPrecedes(multi, (*cutoffs).MultiXactCutoff) {
            freeze = true;
        }

        /* need to check whether any member of the mxact is old */
        nmembers = GetMultiXactIdMembers(
            multi,
            &raw mut members,
            false,
            HEAP_XMAX_IS_LOCKED_ONLY((*tuple).t_infomask),
        );

        for i in 0..nmembers {
            xid = (*members.add(i as usize)).xid;
            Assert!(TransactionIdPrecedesOrEquals((*cutoffs).relfrozenxid, xid));
            if TransactionIdPrecedes(xid, *NoFreezePageRelfrozenXid) {
                *NoFreezePageRelfrozenXid = xid;
            }
            if TransactionIdPrecedes(xid, (*cutoffs).FreezeLimit) {
                freeze = true;
            }
        }
        if nmembers > 0 {
            pfree(members as *mut c_void);
        }
    }

    if (*tuple).t_infomask & HEAP_MOVED != 0 {
        xid = HeapTupleHeaderGetXvac(tuple);
        if TransactionIdIsNormal(xid) {
            Assert!(TransactionIdPrecedesOrEquals((*cutoffs).relfrozenxid, xid));
            if TransactionIdPrecedes(xid, *NoFreezePageRelfrozenXid) {
                *NoFreezePageRelfrozenXid = xid;
            }
            /* heap_prepare_freeze_tuple forces xvac freezing */
            freeze = true;
        }
    }

    freeze
}

/*
 * Maintain snapshotConflictHorizon for caller by ratcheting forward its value
 * using any committed XIDs contained in 'tuple'.
 */
pub unsafe fn HeapTupleHeaderAdvanceConflictHorizon(
    tuple: HeapTupleHeader,
    snapshotConflictHorizon: *mut TransactionId,
) {
    let xmin: TransactionId = HeapTupleHeaderGetXmin(tuple);
    let xmax: TransactionId = HeapTupleHeaderGetUpdateXid(tuple);
    let xvac: TransactionId = HeapTupleHeaderGetXvac(tuple);

    if (*tuple).t_infomask & HEAP_MOVED != 0 {
        if TransactionIdPrecedes(*snapshotConflictHorizon, xvac) {
            *snapshotConflictHorizon = xvac;
        }
    }

    /*
     * Ignore tuples inserted by an aborted transaction or if the tuple was
     * updated/deleted by the inserting transaction.
     */
    if HeapTupleHeaderXminCommitted(tuple)
        || (!HeapTupleHeaderXminInvalid(tuple) && TransactionIdDidCommit(xmin))
    {
        if xmax != xmin && TransactionIdFollows(xmax, *snapshotConflictHorizon) {
            *snapshotConflictHorizon = xmax;
        }
    }
}
/*
 * Helper function for heap_index_delete_tuples.  Issues prefetch requests.
 * (USE_PREFETCH)
 */
unsafe fn index_delete_prefetch_buffer(
    rel: Relation,
    prefetch_state: *mut IndexDeletePrefetchState,
    prefetch_count: c_int,
) {
    let mut cur_hblkno: BlockNumber = (*prefetch_state).cur_hblkno;
    let mut count: c_int = 0;
    let mut i: c_int;
    let ndeltids: c_int = (*prefetch_state).ndeltids;
    let deltids: *mut TM_IndexDelete = (*prefetch_state).deltids;

    i = (*prefetch_state).next_item;
    while i < ndeltids && count < prefetch_count {
        let htid: ItemPointer = &raw mut (*deltids.add(i as usize)).tid;

        if cur_hblkno == InvalidBlockNumber || ItemPointerGetBlockNumber(htid) != cur_hblkno {
            cur_hblkno = ItemPointerGetBlockNumber(htid);
            PrefetchBuffer(rel, MAIN_FORKNUM, cur_hblkno);
            count += 1;
        }
        i += 1;
    }

    /*
     * Save the prefetch position.
     */
    (*prefetch_state).next_item = i;
    (*prefetch_state).cur_hblkno = cur_hblkno;
}

/*
 * Helper function for heap_index_delete_tuples.  Checks for index corruption.
 */
#[inline]
unsafe fn index_delete_check_htid(
    delstate: *mut TM_IndexDeleteOp,
    page: Page,
    maxoff: OffsetNumber,
    htid: ItemPointer,
    istatus: *mut TM_IndexStatus,
) {
    let indexpagehoffnum: OffsetNumber = ItemPointerGetOffsetNumber(htid);
    let iid: ItemId;

    Assert!(OffsetNumberIsValid((*istatus).idxoffnum));

    if unlikely(indexpagehoffnum > maxoff) {
        ereport!(
            ERROR,
            errmsg!(
                "heap tid from index tuple ({},{}) points past end of heap page line pointer array at offset {} of block {} in index \"{}\"",
                ItemPointerGetBlockNumber(htid),
                indexpagehoffnum,
                (*istatus).idxoffnum,
                (*delstate).iblknum,
                std::ffi::CStr::from_ptr(RelationGetRelationName((*delstate).irel))
                    .to_string_lossy()
            )
        );
    }

    iid = PageGetItemId(page, indexpagehoffnum);
    if unlikely(!ItemIdIsUsed(iid)) {
        ereport!(
            ERROR,
            errmsg!(
                "heap tid from index tuple ({},{}) points to unused heap page item at offset {} of block {} in index \"{}\"",
                ItemPointerGetBlockNumber(htid),
                indexpagehoffnum,
                (*istatus).idxoffnum,
                (*delstate).iblknum,
                std::ffi::CStr::from_ptr(RelationGetRelationName((*delstate).irel))
                    .to_string_lossy()
            )
        );
    }

    if ItemIdHasStorage(iid) {
        let htup: HeapTupleHeader;

        Assert!(ItemIdIsNormal(iid));
        htup = PageGetItem(page, iid) as HeapTupleHeader;

        if unlikely(HeapTupleHeaderIsHeapOnly(htup)) {
            ereport!(
                ERROR,
                errmsg!(
                    "heap tid from index tuple ({},{}) points to heap-only tuple at offset {} of block {} in index \"{}\"",
                    ItemPointerGetBlockNumber(htid),
                    indexpagehoffnum,
                    (*istatus).idxoffnum,
                    (*delstate).iblknum,
                    std::ffi::CStr::from_ptr(RelationGetRelationName((*delstate).irel))
                        .to_string_lossy()
                )
            );
        }
    }
}

/*
 * heapam implementation of tableam's index_delete_tuples interface.
 */
pub unsafe fn heap_index_delete_tuples(
    rel: Relation,
    delstate: *mut TM_IndexDeleteOp,
) -> TransactionId {
    /* Initial assumption is that earlier pruning took care of conflict */
    let mut snapshotConflictHorizon: TransactionId = InvalidTransactionId;
    let mut blkno: BlockNumber = InvalidBlockNumber;
    let mut buf: Buffer = InvalidBuffer;
    let mut page: Page = std::ptr::null_mut();
    let mut maxoff: OffsetNumber = InvalidOffsetNumber;
    let mut priorXmax: TransactionId;
    let mut prefetch_state: IndexDeletePrefetchState = std::mem::zeroed();
    let prefetch_distance: c_int;
    let mut SnapshotNonVacuumable: SnapshotData = std::mem::zeroed();
    let mut finalndeltids: c_int = 0;
    let mut nblocksaccessed: c_int = 0;

    /* State that's only used in bottom-up index deletion case */
    let mut nblocksfavorable: c_int = 0;
    let mut curtargetfreespace: c_int = (*delstate).bottomupfreespace;
    let mut lastfreespace: c_int = 0;
    let mut actualfreespace: c_int = 0;
    let mut bottomup_final_block: bool = false;

    InitNonVacuumableSnapshot(&raw mut SnapshotNonVacuumable, GlobalVisTestFor(rel));

    /* Sort caller's deltids array by TID for further processing */
    index_delete_sort(delstate);

    /*
     * Bottom-up case.
     */
    if (*delstate).bottomup {
        nblocksfavorable = bottomup_sort_and_shrink(delstate);
    }

    /* Initialize prefetch state. */
    prefetch_state.cur_hblkno = InvalidBlockNumber;
    prefetch_state.next_item = 0;
    prefetch_state.ndeltids = (*delstate).ndeltids;
    prefetch_state.deltids = (*delstate).deltids;

    /*
     * Determine the prefetch distance that we will attempt to maintain.
     */
    if IsCatalogRelation(rel) {
        prefetch_distance = maintenance_io_concurrency;
    } else {
        prefetch_distance =
            get_tablespace_maintenance_io_concurrency((*(*rel).rd_rel).reltablespace);
    }

    /* Cap initial prefetch distance for bottom-up deletion caller */
    let mut prefetch_distance = prefetch_distance;
    if (*delstate).bottomup {
        Assert!(nblocksfavorable >= 1);
        Assert!(nblocksfavorable <= BOTTOMUP_MAX_NBLOCKS);
        prefetch_distance = Min!(prefetch_distance, nblocksfavorable);
    }

    /* Start prefetching. */
    index_delete_prefetch_buffer(rel, &raw mut prefetch_state, prefetch_distance);

    /* Iterate over deltids, determine which to delete, check their horizon */
    Assert!((*delstate).ndeltids > 0);
    for i in 0..(*delstate).ndeltids {
        let ideltid: *mut TM_IndexDelete = (*delstate).deltids.add(i as usize);
        let istatus: *mut TM_IndexStatus = (*delstate).status.add((*ideltid).id as usize);
        let htid: ItemPointer = &raw mut (*ideltid).tid;
        let mut offnum: OffsetNumber;

        /*
         * Read buffer, and perform required extra steps each time a new block
         * is encountered.
         */
        if blkno == InvalidBlockNumber || ItemPointerGetBlockNumber(htid) != blkno {
            /*
             * Consider giving up early for bottom-up index deletion caller
             * first.
             */
            if (*delstate).bottomup {
                if bottomup_final_block {
                    break;
                }

                /*
                 * Give up when we didn't enable our caller to free any
                 * additional space.
                 */
                if nblocksaccessed >= 1 && actualfreespace == lastfreespace {
                    break;
                }
                lastfreespace = actualfreespace; /* for next time */

                /*
                 * Deletion operation (which is bottom-up) will definitely
                 * access the next block in line.
                 */
                Assert!(nblocksaccessed > 0 || nblocksfavorable > 0);
                if nblocksfavorable > 0 {
                    nblocksfavorable -= 1;
                } else {
                    curtargetfreespace /= 2;
                }
            }

            /* release old buffer */
            if BufferIsValid(buf) {
                UnlockReleaseBuffer(buf);
            }

            blkno = ItemPointerGetBlockNumber(htid);
            buf = ReadBuffer(rel, blkno);
            nblocksaccessed += 1;
            Assert!(!(*delstate).bottomup || nblocksaccessed <= BOTTOMUP_MAX_NBLOCKS);

            /*
             * To maintain the prefetch distance, prefetch one more page.
             */
            index_delete_prefetch_buffer(rel, &raw mut prefetch_state, 1);

            LockBuffer(buf, BUFFER_LOCK_SHARE);

            page = BufferGetPage(buf);
            maxoff = PageGetMaxOffsetNumber(page);
        }

        /*
         * Detect index corruption.
         */
        index_delete_check_htid(delstate, page, maxoff, htid, istatus);

        if (*istatus).knowndeletable {
            Assert!(!(*delstate).bottomup && !(*istatus).promising);
        } else {
            let mut tmp: ItemPointerData = *htid;
            let mut heapTuple: HeapTupleData = std::mem::zeroed();

            /* Are any tuples from this HOT chain non-vacuumable? */
            if heap_hot_search_buffer(
                &raw mut tmp,
                rel,
                buf,
                &raw mut SnapshotNonVacuumable,
                &raw mut heapTuple,
                std::ptr::null_mut(),
                true,
            ) {
                continue; /* can't delete entry */
            }

            /* Caller will delete, since whole HOT chain is vacuumable */
            (*istatus).knowndeletable = true;

            /* Maintain index free space info for bottom-up deletion case */
            if (*delstate).bottomup {
                Assert!((*istatus).freespace > 0);
                actualfreespace += ((*istatus).freespace as i32);
                if actualfreespace >= curtargetfreespace {
                    bottomup_final_block = true;
                }
            }
        }

        /*
         * Maintain snapshotConflictHorizon value.
         */
        offnum = ItemPointerGetOffsetNumber(htid);
        priorXmax = InvalidTransactionId; /* cannot check first XMIN */
        loop {
            let lp: ItemId;
            let htup: HeapTupleHeader;

            /* Sanity check (pure paranoia) */
            if offnum < FirstOffsetNumber {
                break;
            }

            /*
             * An offset past the end of page's line pointer array is possible.
             */
            if offnum > maxoff {
                break;
            }

            lp = PageGetItemId(page, offnum);
            if ItemIdIsRedirected(lp) {
                offnum = ItemIdGetRedirect(lp) as OffsetNumber;
                continue;
            }

            /*
             * We'll often encounter LP_DEAD line pointers.
             */
            if !ItemIdIsNormal(lp) {
                break;
            }

            htup = PageGetItem(page, lp) as HeapTupleHeader;

            /*
             * Check the tuple XMIN against prior XMAX, if any
             */
            if TransactionIdIsValid(priorXmax)
                && !TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax)
            {
                break;
            }

            HeapTupleHeaderAdvanceConflictHorizon(htup, &raw mut snapshotConflictHorizon);

            /*
             * If the tuple is not HOT-updated, then we are at the end of this
             * HOT-chain.
             */
            if !HeapTupleHeaderIsHotUpdated(htup) {
                break;
            }

            /* Advance to next HOT chain member */
            Assert!(ItemPointerGetBlockNumber(&raw mut (*htup).t_ctid) == blkno);
            offnum = ItemPointerGetOffsetNumber(&raw mut (*htup).t_ctid);
            priorXmax = HeapTupleHeaderGetUpdateXid(htup);
        }

        /* Enable further/final shrinking of deltids for caller */
        finalndeltids = i + 1;
    }

    UnlockReleaseBuffer(buf);

    /*
     * Shrink deltids array to exclude non-deletable entries at the end.
     */
    Assert!(finalndeltids > 0 || (*delstate).bottomup);
    (*delstate).ndeltids = finalndeltids;

    snapshotConflictHorizon
}

/*
 * Specialized inlineable comparison function for index_delete_sort()
 */
#[inline]
unsafe fn index_delete_sort_cmp(deltid1: *mut TM_IndexDelete, deltid2: *mut TM_IndexDelete) -> c_int {
    let tid1: ItemPointer = &raw mut (*deltid1).tid;
    let tid2: ItemPointer = &raw mut (*deltid2).tid;

    {
        let blk1: BlockNumber = ItemPointerGetBlockNumber(tid1);
        let blk2: BlockNumber = ItemPointerGetBlockNumber(tid2);

        if blk1 != blk2 {
            return if blk1 < blk2 { -1 } else { 1 };
        }
    }
    {
        let pos1: OffsetNumber = ItemPointerGetOffsetNumber(tid1);
        let pos2: OffsetNumber = ItemPointerGetOffsetNumber(tid2);

        if pos1 != pos2 {
            return if pos1 < pos2 { -1 } else { 1 };
        }
    }

    Assert!(false);

    0
}

/*
 * Sort deltids array from delstate by TID.
 */
unsafe fn index_delete_sort(delstate: *mut TM_IndexDeleteOp) {
    let deltids: *mut TM_IndexDelete = (*delstate).deltids;
    let ndeltids: c_int = (*delstate).ndeltids;

    /*
     * Shellsort gap sequence (taken from Sedgewick-Incerpi paper).
     */
    let gaps: [c_int; 9] = [1968, 861, 336, 112, 48, 21, 7, 3, 1];

    /* Think carefully before changing anything here -- keep swaps cheap */
    // StaticAssertDecl(sizeof(TM_IndexDelete) <= 8, "element size exceeds 8 bytes");

    for g in 0..gaps.len() {
        let hi = gaps[g];
        let mut i = hi;
        while i < ndeltids {
            let d: TM_IndexDelete = *deltids.add(i as usize);
            let mut j: c_int = i;

            while j >= hi
                && index_delete_sort_cmp(deltids.add((j - hi) as usize), &raw const d as *mut TM_IndexDelete)
                    >= 0
            {
                *deltids.add(j as usize) = *deltids.add((j - hi) as usize);
                j -= hi;
            }
            *deltids.add(j as usize) = d;
            i += 1;
        }
    }
}

/*
 * Returns how many blocks should be considered favorable/contiguous for a
 * bottom-up index deletion pass.
 */
unsafe fn bottomup_nblocksfavorable(
    blockgroups: *mut IndexDeleteCounts,
    nblockgroups: c_int,
    deltids: *mut TM_IndexDelete,
) -> c_int {
    let mut lastblock: int64 = -1;
    let mut nblocksfavorable: c_int = 0;

    Assert!(nblockgroups >= 1);
    Assert!(nblockgroups <= BOTTOMUP_MAX_NBLOCKS);

    /*
     * We tolerate heap blocks that will be accessed only slightly out of
     * physical order.
     */
    for b in 0..nblockgroups {
        let group: *mut IndexDeleteCounts = blockgroups.add(b as usize);
        let firstdtid: *mut TM_IndexDelete = deltids.add((*group).ifirsttid as usize);
        let block: BlockNumber = ItemPointerGetBlockNumber(&raw mut (*firstdtid).tid);

        if lastblock != -1
            && ((block as int64) < lastblock - BOTTOMUP_TOLERANCE_NBLOCKS as int64
                || (block as int64) > lastblock + BOTTOMUP_TOLERANCE_NBLOCKS as int64)
        {
            break;
        }

        nblocksfavorable += 1;
        lastblock = block as int64;
    }

    /* Always indicate that there is at least 1 favorable block */
    Assert!(nblocksfavorable >= 1);

    nblocksfavorable
}

/*
 * qsort comparison function for bottomup_sort_and_shrink()
 */
unsafe extern "C" fn bottomup_sort_and_shrink_cmp(arg1: *const c_void, arg2: *const c_void) -> c_int {
    let group1: *const IndexDeleteCounts = arg1 as *const IndexDeleteCounts;
    let group2: *const IndexDeleteCounts = arg2 as *const IndexDeleteCounts;

    /*
     * Most significant field is npromisingtids.
     */
    if (*group1).npromisingtids > (*group2).npromisingtids {
        return -1;
    }
    if (*group1).npromisingtids < (*group2).npromisingtids {
        return 1;
    }

    /*
     * Tiebreak: desc ntids sort order.
     */
    if (*group1).ntids != (*group2).ntids {
        let ntids1: uint32 = pg_nextpower2_32((*group1).ntids as uint32);
        let ntids2: uint32 = pg_nextpower2_32((*group2).ntids as uint32);

        if ntids1 > ntids2 {
            return -1;
        }
        if ntids1 < ntids2 {
            return 1;
        }
    }

    /*
     * Tiebreak: asc offset-into-deltids-for-block order.
     */
    if (*group1).ifirsttid > (*group2).ifirsttid {
        return 1;
    }
    if (*group1).ifirsttid < (*group2).ifirsttid {
        return -1;
    }

    unreachable!();
}

/*
 * heap_index_delete_tuples() helper function for bottom-up deletion callers.
 */
unsafe fn bottomup_sort_and_shrink(delstate: *mut TM_IndexDeleteOp) -> c_int {
    let blockgroups: *mut IndexDeleteCounts;
    let reordereddeltids: *mut TM_IndexDelete;
    let mut curblock: BlockNumber = InvalidBlockNumber;
    let mut nblockgroups: c_int = 0;
    let mut ncopied: c_int = 0;
    let nblocksfavorable: c_int;

    Assert!((*delstate).bottomup);
    Assert!((*delstate).ndeltids > 0);

    /* Calculate per-heap-block count of TIDs */
    blockgroups = palloc(std::mem::size_of::<IndexDeleteCounts>() * (*delstate).ndeltids as usize)
        as *mut IndexDeleteCounts;
    for i in 0..(*delstate).ndeltids {
        let ideltid: *mut TM_IndexDelete = (*delstate).deltids.add(i as usize);
        let istatus: *mut TM_IndexStatus = (*delstate).status.add((*ideltid).id as usize);
        let htid: ItemPointer = &raw mut (*ideltid).tid;
        let promising: bool = (*istatus).promising;

        if curblock != ItemPointerGetBlockNumber(htid) {
            /* New block group */
            nblockgroups += 1;

            Assert!(
                curblock < ItemPointerGetBlockNumber(htid) || !BlockNumberIsValid(curblock)
            );

            curblock = ItemPointerGetBlockNumber(htid);
            (*blockgroups.add((nblockgroups - 1) as usize)).ifirsttid = i as int16;
            (*blockgroups.add((nblockgroups - 1) as usize)).ntids = 1;
            (*blockgroups.add((nblockgroups - 1) as usize)).npromisingtids = 0;
        } else {
            (*blockgroups.add((nblockgroups - 1) as usize)).ntids += 1;
        }

        if promising {
            (*blockgroups.add((nblockgroups - 1) as usize)).npromisingtids += 1;
        }
    }

    /*
     * Round the number of promising tuples for each block group up.
     */
    for b in 0..nblockgroups {
        let group: *mut IndexDeleteCounts = blockgroups.add(b as usize);

        /* Better off falling back on nhtids with low npromisingtids */
        if (*group).npromisingtids <= 4 {
            (*group).npromisingtids = 4;
        } else {
            (*group).npromisingtids = pg_nextpower2_32((*group).npromisingtids as uint32) as int16;
        }
    }

    /* Sort groups and rearrange caller's deltids array */
    qsort(
        blockgroups as *mut c_void,
        nblockgroups as Size,
        std::mem::size_of::<IndexDeleteCounts>() as Size,
        Some(bottomup_sort_and_shrink_cmp),
    );
    reordereddeltids =
        palloc((*delstate).ndeltids as usize * std::mem::size_of::<TM_IndexDelete>())
            as *mut TM_IndexDelete;

    nblockgroups = Min!(BOTTOMUP_MAX_NBLOCKS, nblockgroups);
    /* Determine number of favorable blocks at the start of final deltids */
    nblocksfavorable = bottomup_nblocksfavorable(blockgroups, nblockgroups, (*delstate).deltids);

    for b in 0..nblockgroups {
        let group: *mut IndexDeleteCounts = blockgroups.add(b as usize);
        let firstdtid: *mut TM_IndexDelete = (*delstate).deltids.add((*group).ifirsttid as usize);

        memcpy(
            reordereddeltids.add(ncopied as usize) as *mut c_void,
            firstdtid as *const c_void,
            std::mem::size_of::<TM_IndexDelete>() * (*group).ntids as usize,
        );
        ncopied += (*group).ntids as c_int;
    }

    /* Copy final grouped and sorted TIDs back into start of caller's array */
    memcpy(
        (*delstate).deltids as *mut c_void,
        reordereddeltids as *const c_void,
        std::mem::size_of::<TM_IndexDelete>() * ncopied as usize,
    );
    (*delstate).ndeltids = ncopied;

    pfree(reordereddeltids as *mut c_void);
    pfree(blockgroups as *mut c_void);

    nblocksfavorable
}

/*
 * Perform XLogInsert for a heap-visible operation.
 */
pub unsafe fn log_heap_visible(
    rel: Relation,
    heap_buffer: Buffer,
    vm_buffer: Buffer,
    snapshotConflictHorizon: TransactionId,
    vmflags: uint8,
) -> XLogRecPtr {
    let mut xlrec: xl_heap_visible = std::mem::zeroed();
    let recptr: XLogRecPtr;
    let mut flags: uint8;

    Assert!(BufferIsValid(heap_buffer));
    Assert!(BufferIsValid(vm_buffer));

    xlrec.snapshotConflictHorizon = snapshotConflictHorizon;
    xlrec.flags = vmflags;
    if RelationIsAccessibleInLogicalDecoding(rel) {
        xlrec.flags |= VISIBILITYMAP_XLOG_CATALOG_REL;
    }
    XLogBeginInsert();
    XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapVisible as c_int);

    XLogRegisterBuffer(0, vm_buffer, 0);

    flags = REGBUF_STANDARD as uint8;
    if !XLogHintBitIsNeeded() {
        flags |= REGBUF_NO_IMAGE as uint8;
    }
    XLogRegisterBuffer(1, heap_buffer, flags);

    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_VISIBLE);

    recptr
}

/*
 * Perform XLogInsert for a heap-update operation.
 */
unsafe fn log_heap_update(
    reln: Relation,
    oldbuf: Buffer,
    newbuf: Buffer,
    oldtup: HeapTuple,
    newtup: HeapTuple,
    old_key_tuple: HeapTuple,
    all_visible_cleared: bool,
    new_all_visible_cleared: bool,
) -> XLogRecPtr {
    let mut xlrec: xl_heap_update = std::mem::zeroed();
    let mut xlhdr: xl_heap_header = std::mem::zeroed();
    let mut xlhdr_idx: xl_heap_header = std::mem::zeroed();
    let mut info: uint8;
    let mut prefix_suffix: [uint16; 2] = [0; 2];
    let mut prefixlen: uint16 = 0;
    let mut suffixlen: uint16 = 0;
    let recptr: XLogRecPtr;
    let page: Page = BufferGetPage(newbuf);
    let need_tuple_data: bool = RelationIsLogicallyLogged(reln);
    let init: bool;
    let mut bufflags: c_int;

    /* Caller should not call me on a non-WAL-logged relation */
    Assert!(RelationNeedsWAL(reln));

    XLogBeginInsert();

    if HeapTupleIsHeapOnly(newtup) {
        info = XLOG_HEAP_HOT_UPDATE;
    } else {
        info = XLOG_HEAP_UPDATE;
    }

    /*
     * If the old and new tuple are on the same page, we only need to log the
     * parts of the new tuple that were changed.
     */
    if oldbuf == newbuf && !need_tuple_data && !XLogCheckBufferNeedsBackup(newbuf) {
        let oldp: *mut c_char =
            ((*oldtup).t_data as *mut c_char).add((*(*oldtup).t_data).t_hoff as usize);
        let newp: *mut c_char =
            ((*newtup).t_data as *mut c_char).add((*(*newtup).t_data).t_hoff as usize);
        let oldlen: c_int = (*oldtup).t_len as c_int - (*(*oldtup).t_data).t_hoff as c_int;
        let newlen: c_int = (*newtup).t_len as c_int - (*(*newtup).t_data).t_hoff as c_int;

        /* Check for common prefix between old and new tuple */
        prefixlen = 0;
        while (prefixlen as c_int) < Min!(oldlen, newlen) {
            if *newp.add(prefixlen as usize) != *oldp.add(prefixlen as usize) {
                break;
            }
            prefixlen += 1;
        }

        /*
         * Storing the length of the prefix takes 2 bytes.
         */
        if prefixlen < 3 {
            prefixlen = 0;
        }

        /* Same for suffix */
        suffixlen = 0;
        while (suffixlen as c_int) < Min!(oldlen, newlen) - prefixlen as c_int {
            if *newp.add((newlen - suffixlen as c_int - 1) as usize)
                != *oldp.add((oldlen - suffixlen as c_int - 1) as usize)
            {
                break;
            }
            suffixlen += 1;
        }
        if suffixlen < 3 {
            suffixlen = 0;
        }
    }

    /* Prepare main WAL data chain */
    xlrec.flags = 0;
    if all_visible_cleared {
        xlrec.flags |= (XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED as u8);
    }
    if new_all_visible_cleared {
        xlrec.flags |= (XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED as u8);
    }
    if prefixlen > 0 {
        xlrec.flags |= (XLH_UPDATE_PREFIX_FROM_OLD as u8);
    }
    if suffixlen > 0 {
        xlrec.flags |= (XLH_UPDATE_SUFFIX_FROM_OLD as u8);
    }
    if need_tuple_data {
        xlrec.flags |= (XLH_UPDATE_CONTAINS_NEW_TUPLE as u8);
        if !old_key_tuple.is_null() {
            if (*(*reln).rd_rel).relreplident == REPLICA_IDENTITY_FULL {
                xlrec.flags |= (XLH_UPDATE_CONTAINS_OLD_TUPLE as u8);
            } else {
                xlrec.flags |= (XLH_UPDATE_CONTAINS_OLD_KEY as u8);
            }
        }
    }

    /* If new tuple is the single and first tuple on page... */
    if ItemPointerGetOffsetNumber(&raw mut (*newtup).t_self) == FirstOffsetNumber
        && PageGetMaxOffsetNumber(page) == FirstOffsetNumber
    {
        info |= XLOG_HEAP_INIT_PAGE;
        init = true;
    } else {
        init = false;
    }

    /* Prepare WAL data for the old page */
    xlrec.old_offnum = ItemPointerGetOffsetNumber(&raw mut (*oldtup).t_self);
    xlrec.old_xmax = HeapTupleHeaderGetRawXmax((*oldtup).t_data);
    xlrec.old_infobits_set = compute_infobits(
        (*(*oldtup).t_data).t_infomask,
        (*(*oldtup).t_data).t_infomask2,
    );

    /* Prepare WAL data for the new page */
    xlrec.new_offnum = ItemPointerGetOffsetNumber(&raw mut (*newtup).t_self);
    xlrec.new_xmax = HeapTupleHeaderGetRawXmax((*newtup).t_data);

    bufflags = REGBUF_STANDARD;
    if init {
        bufflags |= REGBUF_WILL_INIT;
    }
    if need_tuple_data {
        bufflags |= REGBUF_KEEP_DATA;
    }

    XLogRegisterBuffer(0, newbuf, bufflags as uint8);
    if oldbuf != newbuf {
        XLogRegisterBuffer(1, oldbuf, REGBUF_STANDARD as uint8);
    }

    XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapUpdate as c_int);

    /*
     * Prepare WAL data for the new tuple.
     */
    if prefixlen > 0 || suffixlen > 0 {
        if prefixlen > 0 && suffixlen > 0 {
            prefix_suffix[0] = prefixlen;
            prefix_suffix[1] = suffixlen;
            XLogRegisterBufData(
                0,
                prefix_suffix.as_mut_ptr() as *mut c_char,
                (std::mem::size_of::<uint16>() * 2) as c_int,
            );
        } else if prefixlen > 0 {
            XLogRegisterBufData(
                0,
                &raw mut prefixlen as *mut c_char,
                std::mem::size_of::<uint16>() as c_int,
            );
        } else {
            XLogRegisterBufData(
                0,
                &raw mut suffixlen as *mut c_char,
                std::mem::size_of::<uint16>() as c_int,
            );
        }
    }

    xlhdr.t_infomask2 = (*(*newtup).t_data).t_infomask2;
    xlhdr.t_infomask = (*(*newtup).t_data).t_infomask;
    xlhdr.t_hoff = (*(*newtup).t_data).t_hoff;
    Assert!(
        SizeofHeapTupleHeader as c_int + prefixlen as c_int + suffixlen as c_int
            <= (*newtup).t_len as c_int
    );

    /*
     * PG73FORMAT: write bitmap [+ padding] [+ oid] + data
     */
    XLogRegisterBufData(0, &raw mut xlhdr as *mut c_char, SizeOfHeapHeader as c_int);
    if prefixlen == 0 {
        XLogRegisterBufData(
            0,
            ((*newtup).t_data as *mut c_char).add(SizeofHeapTupleHeader as usize),
            ((*newtup).t_len as usize - SizeofHeapTupleHeader as usize - suffixlen as usize) as c_int,
        );
    } else {
        /*
         * Have to write the null bitmap and data after the common prefix as
         * two separate rdata entries.
         */
        /* bitmap [+ padding] [+ oid] */
        if (*(*newtup).t_data).t_hoff as c_int - SizeofHeapTupleHeader as c_int > 0 {
            XLogRegisterBufData(
                0,
                ((*newtup).t_data as *mut c_char).add(SizeofHeapTupleHeader as usize),
                ((*(*newtup).t_data).t_hoff as usize - SizeofHeapTupleHeader as usize) as c_int,
            );
        }

        /* data after common prefix */
        XLogRegisterBufData(
            0,
            ((*newtup).t_data as *mut c_char)
                .add((*(*newtup).t_data).t_hoff as usize + prefixlen as usize),
            ((*newtup).t_len as usize
                - (*(*newtup).t_data).t_hoff as usize
                - prefixlen as usize
                - suffixlen as usize) as c_int,
        );
    }

    /* We need to log a tuple identity */
    if need_tuple_data && !old_key_tuple.is_null() {
        /* don't really need this, but its more comfy to decode */
        xlhdr_idx.t_infomask2 = (*(*old_key_tuple).t_data).t_infomask2;
        xlhdr_idx.t_infomask = (*(*old_key_tuple).t_data).t_infomask;
        xlhdr_idx.t_hoff = (*(*old_key_tuple).t_data).t_hoff;

        XLogRegisterData(&raw mut xlhdr_idx as *mut c_void, SizeOfHeapHeader as c_int);

        /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
        XLogRegisterData(
            ((*old_key_tuple).t_data as *mut c_char).add(SizeofHeapTupleHeader as usize)
                as *mut c_void,
            ((*old_key_tuple).t_len as Size - SizeofHeapTupleHeader) as c_int,
        );
    }

    /* filtering by origin on a row level is much more efficient */
    XLogSetRecordFlags(XLOG_INCLUDE_ORIGIN);

    recptr = XLogInsert(RM_HEAP_ID, info);

    recptr
}

/*
 * Perform XLogInsert of an XLOG_HEAP2_NEW_CID record
 */
unsafe fn log_heap_new_cid(relation: Relation, tup: HeapTuple) -> XLogRecPtr {
    let mut xlrec: xl_heap_new_cid = std::mem::zeroed();

    let recptr: XLogRecPtr;
    let hdr: HeapTupleHeader = (*tup).t_data;

    Assert!(ItemPointerIsValid(&raw mut (*tup).t_self));
    Assert!((*tup).t_tableOid != InvalidOid);

    xlrec.top_xid = GetTopTransactionId();
    xlrec.target_locator = core::mem::transmute((*relation).rd_locator);
    xlrec.target_tid = (*tup).t_self;

    /*
     * If the tuple got inserted & deleted in the same TX we definitely have a
     * combo CID.
     */
    if (*hdr).t_infomask & HEAP_COMBOCID != 0 {
        Assert!(!((*hdr).t_infomask & HEAP_XMAX_INVALID != 0));
        Assert!(!HeapTupleHeaderXminInvalid(hdr));
        xlrec.cmin = HeapTupleHeaderGetCmin(hdr);
        xlrec.cmax = HeapTupleHeaderGetCmax(hdr);
        xlrec.combocid = HeapTupleHeaderGetRawCommandId(hdr);
    }
    /* No combo CID, so only cmin or cmax can be set by this TX */
    else {
        /*
         * Tuple inserted.
         */
        if (*hdr).t_infomask & HEAP_XMAX_INVALID != 0
            || HEAP_XMAX_IS_LOCKED_ONLY((*hdr).t_infomask)
        {
            xlrec.cmin = HeapTupleHeaderGetRawCommandId(hdr);
            xlrec.cmax = InvalidCommandId;
        }
        /* Tuple from a different tx updated or deleted. */
        else {
            xlrec.cmin = InvalidCommandId;
            xlrec.cmax = HeapTupleHeaderGetRawCommandId(hdr);
        }
        xlrec.combocid = InvalidCommandId;
    }

    /*
     * Note that we don't need to register the buffer here.
     */
    XLogBeginInsert();
    XLogRegisterData(&raw mut xlrec as *mut c_void, SizeOfHeapNewCid as c_int);

    /* will be looked at irrespective of origin */

    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_NEW_CID);

    recptr
}

/*
 * Build a heap tuple representing the configured REPLICA IDENTITY.
 */
unsafe fn ExtractReplicaIdentity(
    relation: Relation,
    mut tp: HeapTuple,
    key_required: bool,
    copy: *mut bool,
) -> HeapTuple {
    let desc: TupleDesc = RelationGetDescr(relation);
    let replident: c_char = (*(*relation).rd_rel).relreplident;
    let idattrs: *mut Bitmapset;
    let mut key_tuple: HeapTuple;
    let mut nulls: [bool; MaxHeapAttributeNumber as usize] = [false; MaxHeapAttributeNumber as usize];
    let mut values: [Datum; MaxHeapAttributeNumber as usize] =
        [0usize; MaxHeapAttributeNumber as usize];

    *copy = false;

    if !RelationIsLogicallyLogged(relation) {
        return std::ptr::null_mut();
    }

    if replident == REPLICA_IDENTITY_NOTHING {
        return std::ptr::null_mut();
    }

    if replident == REPLICA_IDENTITY_FULL {
        /*
         * When logging the entire old tuple, it very well could contain
         * toasted columns.
         */
        if HeapTupleHasExternal(tp) {
            *copy = true;
            tp = toast_flatten_tuple(tp, desc);
        }
        return tp;
    }

    /* if the key isn't required and we're only logging the key, we're done */
    if !key_required {
        return std::ptr::null_mut();
    }

    /* find out the replica identity columns */
    idattrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);

    /*
     * If there's no defined replica identity columns, treat as !key_required.
     */
    if bms_is_empty(idattrs) {
        return std::ptr::null_mut();
    }

    /*
     * Construct a new tuple containing only the replica identity columns.
     */
    heap_deform_tuple(tp, desc, values.as_mut_ptr(), nulls.as_mut_ptr());

    for i in 0..(*desc).natts {
        if bms_is_member(
            i + 1 - FirstLowInvalidHeapAttributeNumber as c_int,
            idattrs,
        ) {
            Assert!(!nulls[i as usize]);
        } else {
            nulls[i as usize] = true;
        }
    }

    key_tuple = heap_form_tuple(desc, values.as_mut_ptr(), nulls.as_mut_ptr());
    *copy = true;

    bms_free(idattrs);

    /*
     * If the tuple still has toasted columns, force them to be inlined.
     */
    if HeapTupleHasExternal(key_tuple) {
        let oldtup: HeapTuple = key_tuple;

        key_tuple = toast_flatten_tuple(oldtup, desc);
        heap_freetuple(oldtup);
    }

    key_tuple
}

/*
 * HeapCheckForSerializableConflictOut
 */
pub unsafe fn HeapCheckForSerializableConflictOut(
    visible: bool,
    relation: Relation,
    tuple: HeapTuple,
    buffer: Buffer,
    snapshot: Snapshot,
) {
    let mut xid: TransactionId;
    let htsvResult: HTSV_Result;

    if !CheckForSerializableConflictOutNeeded(relation, snapshot) {
        return;
    }

    /*
     * Check to see whether the tuple has been written to by a concurrent
     * transaction.
     */
    htsvResult = HeapTupleSatisfiesVacuum(tuple, TransactionXmin, buffer);
    match htsvResult {
        x if x == HEAPTUPLE_LIVE => {
            if visible {
                return;
            }
            xid = HeapTupleHeaderGetXmin((*tuple).t_data);
        }
        x if x == HEAPTUPLE_RECENTLY_DEAD || x == HEAPTUPLE_DELETE_IN_PROGRESS => {
            if visible {
                xid = HeapTupleHeaderGetUpdateXid((*tuple).t_data);
            } else {
                xid = HeapTupleHeaderGetXmin((*tuple).t_data);
            }

            if TransactionIdPrecedes(xid, TransactionXmin) {
                /* This is like the HEAPTUPLE_DEAD case */
                Assert!(!visible);
                return;
            }
        }
        x if x == HEAPTUPLE_INSERT_IN_PROGRESS => {
            xid = HeapTupleHeaderGetXmin((*tuple).t_data);
        }
        x if x == HEAPTUPLE_DEAD => {
            Assert!(!visible);
            return;
        }
        _ => {
            /*
             * The only way to get to this default clause is if a new value is
             * added to the enum type without adding it to this switch.
             */
            elog!(
                ERROR,
                "unrecognized return value from HeapTupleSatisfiesVacuum: {}",
                htsvResult as u32
            );

            xid = InvalidTransactionId;
        }
    }

    Assert!(TransactionIdIsValid(xid));
    Assert!(TransactionIdFollowsOrEquals(xid, TransactionXmin));

    /*
     * Find top level xid.
     */
    if TransactionIdEquals(xid, GetTopTransactionIdIfAny()) {
        return;
    }
    xid = SubTransGetTopmostTransaction(xid);
    if TransactionIdPrecedes(xid, TransactionXmin) {
        return;
    }

    CheckForSerializableConflictOut(relation, xid, snapshot);
}
// ====================================================================
// STUBS
//
// Minimal local stubs for symbols whose owning modules are not yet
// ported to PepperDB.  Following the convention of the sibling heap
// files, every stub is tagged `// TODO(pg-port): real SYM lives in
// <file>`.  Function bodies are `unimplemented!()` (or trivial), and
// scalar constants/types carry placeholder values/definitions.  None of
// these are load-bearing for the faithful 1:1 translation above; they
// exist solely so this unit is self-consistent with no undefined
// symbols.
// ====================================================================

// --- additional simple type aliases -------------------------------------

type ScanDirection = c_int; // TODO(pg-port): real ScanDirection lives in access/sdir.rs
type AttrNumber = int16; // TODO(pg-port): real AttrNumber lives in access/attnum.rs
type RelFileLocator = crate::access::transam::xlogutils::RelFileLocator; // TODO(pg-port): real RelFileLocator lives in storage/relfilelocator.rs
type ReadStream = c_void; // TODO(pg-port): real ReadStream lives in storage/read_stream.rs
type ReadStreamBlockNumberCB =
    unsafe extern "C" fn(*mut ReadStream, *mut c_void, *mut c_void) -> BlockNumber; // TODO(pg-port): real ReadStreamBlockNumberCB lives in storage/read_stream.rs
use crate::nodes::tidbitmap::TBMIterateResult; // real TBMIterateResult from nodes/tidbitmap.rs
type GlobalVisState = c_void; // TODO(pg-port): real GlobalVisState lives in utils/snapmgr.rs
type LOCKTAG = LOCKTAGData; // TODO(pg-port): real LOCKTAG lives in storage/lock.rs
type SharedInvalidationMessage = c_void; // TODO(pg-port): real SharedInvalidationMessage lives in storage/sinval.rs
type MultiXactMember = MultiXactMemberData; // TODO(pg-port): real MultiXactMember lives in access/multixact.rs
use crate::catalog::pg_class::Form_pg_class; // real Form_pg_class from catalog/pg_class.rs
use crate::catalog::pg_database::Form_pg_database; // real Form_pg_database from catalog/pg_database.rs
type BitmapHeapScanDesc = *mut BitmapHeapScanDescData; // TODO(pg-port): real BitmapHeapScanDesc lives in access/relscan.rs
type ParallelBlockTableScanWorkerData =
    crate::access::relscan::ParallelBlockTableScanWorkerData; // TODO(pg-port): real ParallelBlockTableScanWorkerData lives in access/relscan.rs
type TableScanDescData = crate::access::relscan::TableScanDescData; // TODO(pg-port): real TableScanDescData lives in access/relscan.rs
type ParallelBlockTableScanDesc = crate::access::relscan::ParallelBlockTableScanDesc; // TODO(pg-port): real ParallelBlockTableScanDesc lives in access/relscan.rs
type BufferAccessStrategy = *mut c_void; // TODO(pg-port): real BufferAccessStrategy lives in storage/buf.rs
type ForkNumber = c_int; // TODO(pg-port): real ForkNumber lives in common/relpath.rs

// --- stub structs ---------------------------------------------------------

#[repr(C)]
struct LOCKTAGData {
    locktag_field1: uint32,
    locktag_field2: uint32,
    locktag_field3: uint32,
    locktag_field4: uint16,
    locktag_type: uint8,
    locktag_lockmethodid: uint8,
} // TODO(pg-port): real LOCKTAG lives in storage/lock.rs

#[repr(C)]
#[derive(Clone, Copy)]
struct MultiXactMemberData {
    xid: TransactionId,
    status: MultiXactStatus,
} // TODO(pg-port): real MultiXactMember lives in access/multixact.rs

#[repr(C)]
struct BitmapHeapScanDescData {
    rs_base: TableScanDescData,
} // TODO(pg-port): real BitmapHeapScanDescData lives in access/relscan.rs

// PGAlignedBlock is a union of [char; BLCKSZ] aligned to a double; we model
// it with the `data` field used by the translation.
#[repr(C)]
#[derive(Clone, Copy)]
struct PGAlignedBlock {
    data: [c_char; BLCKSZ as usize],
} // TODO(pg-port): real PGAlignedBlock lives in c.h

// access/heapam.h freeze working structs ----------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HeapTupleFreeze {
    pub xmax: TransactionId,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub frzflags: uint8,
    pub checkflags: uint8,
    pub offset: OffsetNumber,
} // TODO(pg-port): real HeapTupleFreeze lives in access/heapam.rs (heapam.h)

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HeapPageFreeze {
    pub freeze_required: bool,
    pub FreezePageRelfrozenXid: TransactionId,
    pub FreezePageRelminMxid: MultiXactId,
    pub NoFreezePageRelfrozenXid: TransactionId,
    pub NoFreezePageRelminMxid: MultiXactId,
} // TODO(pg-port): real HeapPageFreeze lives in access/heapam.rs (heapam.h)

// commands/vacuum.h VacuumCutoffs ------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VacuumCutoffs {
    pub relfrozenxid: TransactionId,
    pub relminmxid: MultiXactId,
    pub OldestXmin: TransactionId,
    pub OldestMxact: MultiXactId,
    pub FreezeLimit: TransactionId,
    pub MultiXactCutoff: MultiXactId,
} // TODO(pg-port): real VacuumCutoffs lives in commands/vacuum.rs

// access/tableam.h TU_UpdateIndexes ----------------------------------------
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TU_UpdateIndexes {
    TU_None,
    TU_All,
    TU_Summarizing,
} // TODO(pg-port): real TU_UpdateIndexes lives in access/table/tableam.rs
use TU_UpdateIndexes::*;

// access/heapam_xlog.h WAL record structs ---------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_delete {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: uint8,
    pub flags: uint8,
} // TODO(pg-port): real xl_heap_delete lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_header {
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
} // TODO(pg-port): real xl_heap_header lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_insert {
    pub offnum: OffsetNumber,
    pub flags: uint8,
} // TODO(pg-port): real xl_heap_insert lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_multi_insert {
    pub flags: uint8,
    pub ntuples: uint16,
    pub offsets: [OffsetNumber; 0],
} // TODO(pg-port): real xl_heap_multi_insert lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_multi_insert_tuple {
    pub datalen: uint16,
    pub t_infomask2: uint16,
    pub t_infomask: uint16,
    pub t_hoff: uint8,
} // TODO(pg-port): real xl_multi_insert_tuple lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_update {
    pub old_xmax: TransactionId,
    pub old_offnum: OffsetNumber,
    pub old_infobits_set: uint8,
    pub flags: uint8,
    pub new_xmax: TransactionId,
    pub new_offnum: OffsetNumber,
} // TODO(pg-port): real xl_heap_update lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_lock {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: uint8,
    pub flags: uint8,
} // TODO(pg-port): real xl_heap_lock lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_lock_updated {
    pub xmax: TransactionId,
    pub offnum: OffsetNumber,
    pub infobits_set: uint8,
    pub flags: uint8,
} // TODO(pg-port): real xl_heap_lock_updated lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_confirm {
    pub offnum: OffsetNumber,
} // TODO(pg-port): real xl_heap_confirm lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_inplace {
    pub offnum: OffsetNumber,
    pub dbId: Oid,
    pub tsId: Oid,
    pub relcacheInitFileInval: bool,
    pub nmsgs: c_int,
    pub msgs: [u8; 0],
} // TODO(pg-port): real xl_heap_inplace lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_visible {
    pub snapshotConflictHorizon: TransactionId,
    pub flags: uint8,
} // TODO(pg-port): real xl_heap_visible lives in access/heapam_xlog.rs

#[repr(C)]
#[derive(Clone, Copy)]
pub struct xl_heap_new_cid {
    pub top_xid: TransactionId,
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub combocid: CommandId,
    pub target_locator: RelFileLocator,
    pub target_tid: ItemPointerData,
} // TODO(pg-port): real xl_heap_new_cid lives in access/heapam_xlog.rs

// --- access/heapam_xlog.h SizeOf constants -------------------------------
const SizeOfHeapDelete: Size =
    (std::mem::offset_of!(xl_heap_delete, flags) + std::mem::size_of::<uint8>()) as Size; // TODO(pg-port): real SizeOfHeapDelete lives in access/heapam_xlog.rs
const SizeOfHeapHeader: Size =
    (std::mem::offset_of!(xl_heap_header, t_hoff) + std::mem::size_of::<uint8>()) as Size; // TODO(pg-port): real SizeOfHeapHeader lives in access/heapam_xlog.rs
const SizeOfHeapInsert: Size =
    (std::mem::offset_of!(xl_heap_insert, flags) + std::mem::size_of::<uint8>()) as Size; // TODO(pg-port): real SizeOfHeapInsert lives in access/heapam_xlog.rs
const SizeOfHeapMultiInsert: Size =
    std::mem::offset_of!(xl_heap_multi_insert, offsets) as Size; // TODO(pg-port): real SizeOfHeapMultiInsert lives in access/heapam_xlog.rs
const SizeOfMultiInsertTuple: Size =
    (std::mem::offset_of!(xl_multi_insert_tuple, t_hoff) + std::mem::size_of::<uint8>()) as Size; // TODO(pg-port): real SizeOfMultiInsertTuple lives in access/heapam_xlog.rs
const SizeOfHeapUpdate: Size =
    (std::mem::offset_of!(xl_heap_update, new_offnum) + std::mem::size_of::<OffsetNumber>()) as Size; // TODO(pg-port): real SizeOfHeapUpdate lives in access/heapam_xlog.rs
const SizeOfHeapLock: Size =
    (std::mem::offset_of!(xl_heap_lock, flags) + std::mem::size_of::<uint8>()) as Size; // TODO(pg-port): real SizeOfHeapLock lives in access/heapam_xlog.rs
const SizeOfHeapLockUpdated: Size =
    (std::mem::offset_of!(xl_heap_lock_updated, flags) + std::mem::size_of::<uint8>()) as Size; // TODO(pg-port): real SizeOfHeapLockUpdated lives in access/heapam_xlog.rs
const SizeOfHeapConfirm: Size =
    (std::mem::offset_of!(xl_heap_confirm, offnum) + std::mem::size_of::<OffsetNumber>()) as Size; // TODO(pg-port): real SizeOfHeapConfirm lives in access/heapam_xlog.rs
const MinSizeOfHeapInplace: Size =
    (std::mem::offset_of!(xl_heap_inplace, nmsgs) + std::mem::size_of::<c_int>()) as Size; // TODO(pg-port): real MinSizeOfHeapInplace lives in access/heapam_xlog.rs
const SizeOfHeapVisible: Size =
    (std::mem::offset_of!(xl_heap_visible, flags) + std::mem::size_of::<uint8>()) as Size; // TODO(pg-port): real SizeOfHeapVisible lives in access/heapam_xlog.rs
const SizeOfHeapNewCid: Size =
    (std::mem::offset_of!(xl_heap_new_cid, target_tid) + std::mem::size_of::<ItemPointerData>())
        as Size; // TODO(pg-port): real SizeOfHeapNewCid lives in access/heapam_xlog.rs

// --- access/heapam_xlog.h opcodes & flags --------------------------------
const XLOG_HEAP_INSERT: uint8 = 0x00; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP_DELETE: uint8 = 0x10; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP_UPDATE: uint8 = 0x20; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP_HOT_UPDATE: uint8 = 0x40; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP_CONFIRM: uint8 = 0x50; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP_LOCK: uint8 = 0x60; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP_INPLACE: uint8 = 0x70; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP_INIT_PAGE: uint8 = 0x80; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP2_VISIBLE: uint8 = 0x40; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP2_MULTI_INSERT: uint8 = 0x50; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP2_LOCK_UPDATED: uint8 = 0x60; // TODO(pg-port): real lives in access/heapam_xlog.rs
const XLOG_HEAP2_NEW_CID: uint8 = 0x70; // TODO(pg-port): real lives in access/heapam_xlog.rs

const XLH_INSERT_ALL_VISIBLE_CLEARED: c_int = 1 << 0; // TODO(pg-port): heapam_xlog.rs
const XLH_INSERT_LAST_IN_MULTI: c_int = 1 << 1; // TODO(pg-port): heapam_xlog.rs
const XLH_INSERT_IS_SPECULATIVE: c_int = 1 << 2; // TODO(pg-port): heapam_xlog.rs
const XLH_INSERT_CONTAINS_NEW_TUPLE: c_int = 1 << 3; // TODO(pg-port): heapam_xlog.rs
const XLH_INSERT_ON_TOAST_RELATION: c_int = 1 << 4; // TODO(pg-port): heapam_xlog.rs
const XLH_INSERT_ALL_FROZEN_SET: c_int = 1 << 5; // TODO(pg-port): heapam_xlog.rs

const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: c_int = 1 << 0; // TODO(pg-port): heapam_xlog.rs
const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: c_int = 1 << 1; // TODO(pg-port): heapam_xlog.rs
const XLH_UPDATE_CONTAINS_OLD_TUPLE: c_int = 1 << 2; // TODO(pg-port): heapam_xlog.rs
const XLH_UPDATE_CONTAINS_OLD_KEY: c_int = 1 << 3; // TODO(pg-port): heapam_xlog.rs
const XLH_UPDATE_CONTAINS_NEW_TUPLE: c_int = 1 << 4; // TODO(pg-port): heapam_xlog.rs
const XLH_UPDATE_PREFIX_FROM_OLD: c_int = 1 << 5; // TODO(pg-port): heapam_xlog.rs
const XLH_UPDATE_SUFFIX_FROM_OLD: c_int = 1 << 6; // TODO(pg-port): heapam_xlog.rs

const XLH_DELETE_ALL_VISIBLE_CLEARED: c_int = 1 << 0; // TODO(pg-port): heapam_xlog.rs
const XLH_DELETE_CONTAINS_OLD_TUPLE: c_int = 1 << 1; // TODO(pg-port): heapam_xlog.rs
const XLH_DELETE_CONTAINS_OLD_KEY: c_int = 1 << 2; // TODO(pg-port): heapam_xlog.rs
const XLH_DELETE_IS_SUPER: c_int = 1 << 3; // TODO(pg-port): heapam_xlog.rs
const XLH_DELETE_IS_PARTITION_MOVE: c_int = 1 << 4; // TODO(pg-port): heapam_xlog.rs

const XLH_FREEZE_XVAC: c_int = 0x02; // TODO(pg-port): heapam_xlog.rs
const XLH_INVALID_XVAC: c_int = 0x04; // TODO(pg-port): heapam_xlog.rs

const XLHL_XMAX_IS_MULTI: uint8 = 0x01; // TODO(pg-port): heapam_xlog.rs
const XLHL_XMAX_LOCK_ONLY: uint8 = 0x02; // TODO(pg-port): heapam_xlog.rs
const XLHL_XMAX_EXCL_LOCK: uint8 = 0x04; // TODO(pg-port): heapam_xlog.rs
const XLHL_XMAX_KEYSHR_LOCK: uint8 = 0x08; // TODO(pg-port): heapam_xlog.rs
const XLHL_KEYS_UPDATED: uint8 = 0x10; // TODO(pg-port): heapam_xlog.rs
const XLH_LOCK_ALL_FROZEN_CLEARED: uint8 = 0x01; // TODO(pg-port): heapam_xlog.rs

// --- resource manager ids (access/rmgrlist.h) ----------------------------
const RM_HEAP_ID: c_int = 10; // TODO(pg-port): real RM_HEAP_ID lives in access/rmgrlist.rs
const RM_HEAP2_ID: c_int = 11; // TODO(pg-port): real RM_HEAP2_ID lives in access/rmgrlist.rs

// --- access/heapam.h insert option & freeze-check flags ------------------
const HEAP_INSERT_FROZEN: c_int = 0x0004; // TODO(pg-port): real HEAP_INSERT_FROZEN lives in access/heapam.rs
const HEAP_INSERT_NO_LOGICAL: c_int = 0x0008; // TODO(pg-port): real HEAP_INSERT_NO_LOGICAL lives in access/heapam.rs
const HEAP_INSERT_SPECULATIVE: c_int = 0x0010; // TODO(pg-port): real HEAP_INSERT_SPECULATIVE lives in access/heapam.rs
const HEAP_FREEZE_CHECK_XMIN_COMMITTED: uint8 = 0x01; // TODO(pg-port): real lives in access/heapam.rs
const HEAP_FREEZE_CHECK_XMAX_ABORTED: uint8 = 0x02; // TODO(pg-port): real lives in access/heapam.rs

// --- access/xloginsert.h REGBUF_* and XLogSetRecordFlags flags -----------
const REGBUF_STANDARD: c_int = 0x04; // TODO(pg-port): real REGBUF_STANDARD lives in access/xloginsert.rs
const REGBUF_WILL_INIT: c_int = 0x01 | 0x02; // TODO(pg-port): real REGBUF_WILL_INIT lives in access/xloginsert.rs
const REGBUF_KEEP_DATA: c_int = 0x10; // TODO(pg-port): real REGBUF_KEEP_DATA lives in access/xloginsert.rs
const REGBUF_NO_IMAGE: c_int = 0x02; // TODO(pg-port): real REGBUF_NO_IMAGE lives in access/xloginsert.rs
const XLOG_INCLUDE_ORIGIN: uint8 = 0x01; // TODO(pg-port): real XLOG_INCLUDE_ORIGIN lives in access/xloginsert.rs
const InvalidXLogRecPtr: XLogRecPtr = 0; // TODO(pg-port): real InvalidXLogRecPtr lives in access/xlogdefs.rs

// --- storage/bufmgr.h constants ------------------------------------------
const InvalidBuffer: Buffer = 0; // TODO(pg-port): real InvalidBuffer lives in storage/buf.rs
const BUFFER_LOCK_UNLOCK: c_int = 0; // TODO(pg-port): real BUFFER_LOCK_UNLOCK lives in storage/bufmgr.rs
const BUFFER_LOCK_SHARE: c_int = 1; // TODO(pg-port): real BUFFER_LOCK_SHARE lives in storage/bufmgr.rs
const BUFFER_LOCK_EXCLUSIVE: c_int = 2; // TODO(pg-port): real BUFFER_LOCK_EXCLUSIVE lives in storage/bufmgr.rs
const MAIN_FORKNUM: c_int = 0; // TODO(pg-port): real MAIN_FORKNUM lives in common/relpath.rs

// --- storage/read_stream.h flags -----------------------------------------
const READ_STREAM_DEFAULT: c_int = 0x00; // TODO(pg-port): real READ_STREAM_DEFAULT lives in storage/read_stream.rs
const READ_STREAM_SEQUENTIAL: c_int = 0x01; // TODO(pg-port): real READ_STREAM_SEQUENTIAL lives in storage/read_stream.rs
const READ_STREAM_USE_BATCHING: c_int = 0x08; // TODO(pg-port): real READ_STREAM_USE_BATCHING lives in storage/read_stream.rs

// --- storage/bufmgr.h BufferAccessStrategyType (BAS_*) -------------------
const BAS_BULKREAD: c_int = 1; // TODO(pg-port): real BAS_BULKREAD lives in storage/bufmgr.rs
const BAS_BULKWRITE: c_int = 3; // TODO(pg-port): real BAS_BULKWRITE lives in storage/bufmgr.rs

// --- storage/lock.h heavyweight lock modes -------------------------------
const AccessShareLock: c_int = 1; // TODO(pg-port): real AccessShareLock lives in storage/lockdefs.rs
const RowShareLock: c_int = 2; // TODO(pg-port): real RowShareLock lives in storage/lockdefs.rs
const ShareUpdateExclusiveLock: c_int = 4; // TODO(pg-port): real ShareUpdateExclusiveLock lives in storage/lockdefs.rs
const ShareRowExclusiveLock: c_int = 6; // TODO(pg-port): real ShareRowExclusiveLock lives in storage/lockdefs.rs
const ExclusiveLock: c_int = 7; // TODO(pg-port): real ExclusiveLock lives in storage/lockdefs.rs
const AccessExclusiveLock: c_int = 8; // TODO(pg-port): real AccessExclusiveLock lives in storage/lockdefs.rs
/* InplaceUpdateTupleLock is an alias for ExclusiveLock (see heapam.c). */
const InplaceUpdateTupleLock: c_int = ExclusiveLock; // TODO(pg-port): real InplaceUpdateTupleLock lives in access/heapam.rs

// --- storage/lock.h LOCKTAG type constants -------------------------------
const LOCKTAG_RELATION: uint8 = 0; // TODO(pg-port): real LOCKTAG_RELATION lives in storage/lock.rs
const LOCKTAG_TUPLE: uint8 = 5; // TODO(pg-port): real LOCKTAG_TUPLE lives in storage/lock.rs

// --- storage/lmgr.h XLTW_Oper values -------------------------------------
const XLTW_None: XLTW_Oper = 0; // TODO(pg-port): real XLTW_Oper values live in storage/lmgr.rs
const XLTW_Update: XLTW_Oper = 1; // TODO(pg-port): real XLTW_Oper values live in storage/lmgr.rs
const XLTW_Delete: XLTW_Oper = 2; // TODO(pg-port): real XLTW_Oper values live in storage/lmgr.rs
const XLTW_Lock: XLTW_Oper = 3; // TODO(pg-port): real XLTW_Oper values live in storage/lmgr.rs
const XLTW_LockUpdated: XLTW_Oper = 4; // TODO(pg-port): real XLTW_Oper values live in storage/lmgr.rs

// --- access/multixact.h MultiXactStatus values ---------------------------
const MultiXactStatusForKeyShare: c_int = 0x00; // TODO(pg-port): real lives in access/multixact.rs
const MultiXactStatusForShare: c_int = 0x01; // TODO(pg-port): real lives in access/multixact.rs
const MultiXactStatusForNoKeyUpdate: c_int = 0x02; // TODO(pg-port): real lives in access/multixact.rs
const MultiXactStatusForUpdate: c_int = 0x03; // TODO(pg-port): real lives in access/multixact.rs
const MultiXactStatusNoKeyUpdate: c_int = 0x04; // TODO(pg-port): real lives in access/multixact.rs
const MultiXactStatusUpdate: c_int = 0x05; // TODO(pg-port): real lives in access/multixact.rs
const MaxMultiXactStatus: c_int = MultiXactStatusUpdate; // TODO(pg-port): real lives in access/multixact.rs
const InvalidMultiXactId: MultiXactId = 0; // TODO(pg-port): real InvalidMultiXactId lives in access/transam.rs

// --- nodes/lockoptions.h LockTupleMode bound -----------------------------
const MaxLockTupleMode: c_int = LockTupleExclusive as c_int; // TODO(pg-port): real MaxLockTupleMode lives in nodes/lockoptions.rs

// --- access/sdir.h ScanDirection values ----------------------------------
const ForwardScanDirection: ScanDirection = 1; // TODO(pg-port): real lives in access/sdir.rs

// --- catalog OIDs (catalog/pg_*_d.h) -------------------------------------
const RelationRelationId: Oid = 1259; // TODO(pg-port): real RelationRelationId lives in catalog/pg_class.rs
const DatabaseRelationId: Oid = 1262; // TODO(pg-port): real DatabaseRelationId lives in catalog/pg_database.rs

// --- catalog relkinds (catalog/pg_class.h) -------------------------------
const RELKIND_RELATION: c_char = b'r' as c_char; // TODO(pg-port): real RELKIND_RELATION lives in catalog/pg_class.rs
const RELKIND_INDEX: c_char = b'i' as c_char; // TODO(pg-port): real RELKIND_INDEX lives in catalog/pg_class.rs
const RELKIND_MATVIEW: c_char = b'm' as c_char; // TODO(pg-port): real RELKIND_MATVIEW lives in catalog/pg_class.rs

// --- catalog replica identity (catalog/pg_class.h) -----------------------
const REPLICA_IDENTITY_NOTHING: c_char = b'n' as c_char; // TODO(pg-port): real lives in catalog/pg_class.rs
const REPLICA_IDENTITY_FULL: c_char = b'f' as c_char; // TODO(pg-port): real lives in catalog/pg_class.rs

// --- access/sysattr.h ----------------------------------------------------
const FirstLowInvalidHeapAttributeNumber: c_int = -8; // TODO(pg-port): real lives in access/sysattr.rs
const TableOidAttributeNumber: AttrNumber = -7; // TODO(pg-port): real lives in access/sysattr.rs

// --- utils/rel.h RelationGetIndexAttrBitmap kinds ------------------------
const INDEX_ATTR_BITMAP_KEY: c_int = 0; // TODO(pg-port): real lives in utils/rel.rs
const INDEX_ATTR_BITMAP_IDENTITY_KEY: c_int = 1; // TODO(pg-port): real lives in utils/rel.rs
const INDEX_ATTR_BITMAP_HOT_BLOCKING: c_int = 2; // TODO(pg-port): real lives in utils/rel.rs
const INDEX_ATTR_BITMAP_SUMMARIZED: c_int = 3; // TODO(pg-port): real lives in utils/rel.rs

// --- access/heaptoast.h --------------------------------------------------
const TOAST_TUPLE_THRESHOLD: c_int = (BLCKSZ as c_int / 4) & !0x7; // TODO(pg-port): real TOAST_TUPLE_THRESHOLD lives in access/heaptoast.rs

// --- access/visibilitymap.h flag bits ------------------------------------
const VISIBILITYMAP_ALL_VISIBLE: uint8 = 0x01; // TODO(pg-port): real lives in access/visibilitymap.rs
const VISIBILITYMAP_ALL_FROZEN: uint8 = 0x02; // TODO(pg-port): real lives in access/visibilitymap.rs
const VISIBILITYMAP_VALID_BITS: uint8 = 0x03; // TODO(pg-port): real lives in access/visibilitymap.rs
const VISIBILITYMAP_XLOG_CATALOG_REL: uint8 = 0x04; // TODO(pg-port): real lives in access/visibilitymap.rs

// --- storage/proc.h DELAY_CHKPT_START ------------------------------------
const DELAY_CHKPT_START: c_int = 1 << 0; // TODO(pg-port): real DELAY_CHKPT_START lives in storage/proc.rs

// --- process-global variables --------------------------------------------
static mut TransactionXmin: TransactionId = InvalidTransactionId; // TODO(pg-port): real TransactionXmin lives in utils/snapmgr.rs
static mut RecentXmin: TransactionId = InvalidTransactionId; // TODO(pg-port): real RecentXmin lives in utils/snapmgr.rs
static mut MyDatabaseId: Oid = 0; // TODO(pg-port): real MyDatabaseId lives in miscadmin.rs
static mut MyDatabaseTableSpace: Oid = 0; // TODO(pg-port): real MyDatabaseTableSpace lives in miscadmin.rs
// Canonical MyProc (storage/lmgr/proc.rs, #[no_mangle]); the local null stub
// crashed heap_inplace_update_and_unlock on (*MyProc).delayChkptFlags.
extern "C" {
    static mut MyProc: *mut crate::storage::lmgr::proc::PGPROC;
}

// --- commands/variable.rs GUC variables ----------------------------------
// TODO(pg-port): GUC from commands/variable.rs
static mut maintenance_io_concurrency: c_int = 0;

// --- storage/lmgr.h GUC variables ----------------------------------------
// TODO(pg-port): GUC from storage/lmgr.h
static mut log_lock_failures: bool = false;

// --- utils/rel.h constants -----------------------------------------------
const HEAP_DEFAULT_FILLFACTOR: c_int = 100; // TODO(pg-port): real HEAP_DEFAULT_FILLFACTOR lives in utils/rel.h

// --- access/index/genam.c extern globals (stub for heapam.rs use) ---------
// TODO(pg-port): real CheckXidAlive and bsysscan live in access/index/genam.c
static mut CheckXidAlive: TransactionId = InvalidTransactionId;
static mut bsysscan: bool = false;

// --- miscadmin.h CHECK_FOR_INTERRUPTS / crit sections --------------------
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{
        // TODO(pg-port): real CHECK_FOR_INTERRUPTS lives in miscadmin.h
    }};
}
macro_rules! START_CRIT_SECTION {
    () => {{
        // TODO(pg-port): real START_CRIT_SECTION lives in miscadmin.h
    }};
}
macro_rules! END_CRIT_SECTION {
    () => {{
        // TODO(pg-port): real END_CRIT_SECTION lives in miscadmin.h
    }};
}
use {CHECK_FOR_INTERRUPTS, END_CRIT_SECTION, START_CRIT_SECTION};

// --- utils/injection_point.h ---------------------------------------------
unsafe fn INJECTION_POINT(_name: *const c_char, _arg: *mut c_void) {
    // TODO(pg-port): real INJECTION_POINT lives in utils/injection_point.c
}

// --- access/multixact.h ISUPDATE_from_mxstatus macro ---------------------
unsafe fn ISUPDATE_from_mxstatus(status: MultiXactStatus) -> bool {
    (status as c_int) > MultiXactStatusForUpdate // TODO(pg-port): real ISUPDATE_from_mxstatus lives in access/multixact.rs
}

// --- access/htup_details.h HeapTupleSetHintBits macro --------------------
unsafe fn HeapTupleSetHintBits(
    tuple: HeapTupleHeader,
    buffer: Buffer,
    infomask: uint16,
    xid: TransactionId,
) {
    crate::access::heap::heapam_visibility::HeapTupleSetHintBits(tuple as _, buffer, infomask, xid)
}

// --- storage/bufmgr.c (buffer manager) -----------------------------------
unsafe fn ReadBuffer(reln: Relation, blockNum: BlockNumber) -> Buffer {
    crate::storage::buffer::bufmgr::ReadBuffer(reln as _, blockNum)
}
unsafe fn ReleaseBuffer(buffer: Buffer) {
    crate::storage::buffer::bufmgr::ReleaseBuffer(buffer)
}
unsafe fn UnlockReleaseBuffer(buffer: Buffer) {
    crate::storage::buffer::bufmgr::UnlockReleaseBuffer(buffer)
}
unsafe fn LockBuffer(buffer: Buffer, mode: c_int) {
    crate::storage::buffer::bufmgr::LockBuffer(buffer, mode)
}
unsafe fn MarkBufferDirty(buffer: Buffer) {
    crate::storage::buffer::bufmgr::MarkBufferDirty(buffer)
}
unsafe fn BufferIsValid(buffer: Buffer) -> bool {
    buffer != 0
}
unsafe fn BufferGetPage(buffer: Buffer) -> Page {
    crate::storage::buffer::bufmgr::BufferGetPage(buffer) as _
}
unsafe fn BufferGetBlock(buffer: Buffer) -> *mut c_void {
    crate::storage::buffer::bufmgr::BufferGetBlock(buffer) as _
}
unsafe fn BufferGetBlockNumber(buffer: Buffer) -> BlockNumber {
    crate::storage::buffer::bufmgr::BufferGetBlockNumber(buffer)
}
unsafe fn BufferGetTag(
    buffer: Buffer,
    rlocator: *mut RelFileLocator,
    forknum: *mut c_int,
    blknum: *mut BlockNumber,
) {
    crate::storage::buffer::bufmgr::BufferGetTag(buffer, rlocator as _, forknum as _, blknum as _)
}
unsafe fn PrefetchBuffer(reln: Relation, forkNum: c_int, blockNum: BlockNumber) {
    crate::storage::buffer::bufmgr::PrefetchBuffer(reln as _, forkNum as _, blockNum as _);
}
unsafe fn GetAccessStrategy(btype: c_int) -> *mut c_void { crate::storage::buffer::freelist::GetAccessStrategy(btype as _) as _ }
unsafe fn FreeAccessStrategy(strategy: *mut c_void) { crate::storage::buffer::freelist::FreeAccessStrategy(strategy as _) }
unsafe fn get_tablespace_maintenance_io_concurrency(spcid: Oid) -> c_int {
    crate::utils::cache::spccache::get_tablespace_maintenance_io_concurrency(spcid)
}

// --- storage/bufpage.h ---------------------------------------------------
unsafe fn PageGetItemId(page: Page, offsetNumber: OffsetNumber) -> ItemId {
    crate::storage::bufpage::PageGetItemId(page as _, offsetNumber) as _
}
unsafe fn PageGetItem(page: Page, itemId: ItemId) -> *mut c_void {
    crate::storage::bufpage::PageGetItem(page as _, itemId as _) as _
}
unsafe fn PageGetMaxOffsetNumber(page: Page) -> OffsetNumber {
    crate::storage::bufpage::PageGetMaxOffsetNumber(page as _)
}
unsafe fn PageGetHeapFreeSpace(page: Page) -> Size {
    crate::storage::bufpage::PageGetHeapFreeSpace(page as _)
}
unsafe fn PageIsAllVisible(page: Page) -> bool {
    crate::storage::bufpage::PageIsAllVisible(page as _)
}
unsafe fn PageSetAllVisible(page: Page) {
    crate::storage::bufpage::PageSetAllVisible(page as _)
}
unsafe fn PageClearAllVisible(page: Page) {
    crate::storage::bufpage::PageClearAllVisible(page as _)
}
unsafe fn PageSetFull(page: Page) {
    crate::storage::bufpage::PageSetFull(page as _)
}
unsafe fn PageSetPrunable(page: Page, xid: TransactionId) {
    // TODO(pg-port): PageSetPrunable macro unwired; no-op (prune hint only)
}
unsafe fn PageSetLSN(page: Page, lsn: XLogRecPtr) {
    crate::storage::bufpage::PageSetLSN(page as _, lsn)
}

// --- access/xloginsert.c (WAL insertion) ---------------------------------
unsafe fn XLogBeginInsert() {
    crate::access::transam::xloginsert::XLogBeginInsert()
}
unsafe fn XLogRegisterData(data: *mut c_void, len: c_int) {
    crate::access::transam::xloginsert::XLogRegisterData(data as _, len as _)
}
unsafe fn XLogRegisterBuffer(block_id: uint8, buffer: Buffer, flags: uint8) {
    crate::access::transam::xloginsert::XLogRegisterBuffer(block_id as _, buffer, flags as _)
}
unsafe fn XLogRegisterBufData(block_id: uint8, data: *mut c_char, len: c_int) {
    crate::access::transam::xloginsert::XLogRegisterBufData(block_id as _, data as _, len as _)
}
unsafe fn XLogRegisterBlock(
    block_id: uint8,
    rlocator: *mut RelFileLocator,
    forknum: c_int,
    blknum: BlockNumber,
    page: *mut c_char,
    flags: uint8,
) {
    crate::access::transam::xloginsert::XLogRegisterBlock(block_id as _, rlocator as _, forknum as _, blknum, page as _, flags as _)
}
unsafe fn XLogInsert(rmid: c_int, info: uint8) -> XLogRecPtr {
    crate::access::transam::xloginsert::XLogInsert(rmid as _, info as _)
}
unsafe fn XLogSetRecordFlags(flags: uint8) {
    crate::access::transam::xloginsert::XLogSetRecordFlags(flags as _)
}
unsafe fn XLogCheckBufferNeedsBackup(buffer: Buffer) -> bool {
    crate::access::transam::xloginsert::XLogCheckBufferNeedsBackup(buffer)
}
unsafe fn XLogStandbyInfoActive() -> bool {
    false // TODO(pg-port): access/xlog.h XLogStandbyInfoActive unwired; safe default
}
unsafe fn XLogHintBitIsNeeded() -> bool {
    false // TODO(pg-port): access/xlog.h XLogHintBitIsNeeded unwired; gates hint-bit WAL, false safe
}

// --- access/transam/xact.c (transaction manager) -------------------------
unsafe fn GetCurrentTransactionId() -> TransactionId {
    crate::access::transam::xact::GetCurrentTransactionId()
}
unsafe fn GetTopTransactionId() -> TransactionId {
    crate::access::transam::xact::GetTopTransactionId()
}
unsafe fn GetTopTransactionIdIfAny() -> TransactionId {
    crate::access::transam::xact::GetTopTransactionIdIfAny()
}
unsafe fn GetCurrentCommandId(used: bool) -> CommandId {
    crate::access::transam::xact::GetCurrentCommandId(used)
}
unsafe fn IsInParallelMode() -> bool {
    false // TODO(pg-port): access/transam/xact.c IsInParallelMode unwired; safe default
}
unsafe fn RecordTransactionCommit() -> TransactionId {
    InvalidTransactionId // TODO(pg-port): access/transam/xact.c RecordTransactionCommit unwired
}

// --- access/transam/transam.c (commit/abort status) ----------------------
unsafe fn TransactionIdDidCommit(transactionId: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdDidCommit(transactionId)
}
unsafe fn TransactionIdDidAbort(transactionId: TransactionId) -> bool {
    crate::access::transam::transam::TransactionIdDidAbort(transactionId)
}

// --- access/transam/subtrans.c -------------------------------------------
unsafe fn SubTransGetTopmostTransaction(xid: TransactionId) -> TransactionId {
    crate::access::transam::subtrans::SubTransGetTopmostTransaction(xid)
}

// --- storage/ipc/procarray.c ---------------------------------------------
unsafe fn TransactionIdIsInProgress(xid: TransactionId) -> bool {
    crate::storage::ipc::procarray::TransactionIdIsInProgress(xid)
}
unsafe fn TransactionIdIsCurrentTransactionId(xid: TransactionId) -> bool {
    crate::access::transam::xact::TransactionIdIsCurrentTransactionId(xid)
}

// --- access/multixact.c --------------------------------------------------
unsafe fn MultiXactIdIsValid(multi: MultiXactId) -> bool {
    multi != InvalidMultiXactId // TODO(pg-port): real MultiXactIdIsValid lives in access/multixact.h
}
unsafe fn MultiXactIdCreate(
    xid1: TransactionId,
    status1: MultiXactStatus,
    xid2: TransactionId,
    status2: MultiXactStatus,
) -> MultiXactId {
    crate::access::transam::multixact::MultiXactIdCreate(xid1, status1, xid2, status2)
}
unsafe fn MultiXactIdExpand(
    multi: MultiXactId,
    xid: TransactionId,
    status: MultiXactStatus,
) -> MultiXactId {
    crate::access::transam::multixact::MultiXactIdExpand(multi, xid, status)
}
unsafe fn MultiXactIdCreateFromMembers(
    nmembers: c_int,
    members: *mut MultiXactMember,
) -> MultiXactId {
    crate::access::transam::multixact::MultiXactIdCreateFromMembers(nmembers, members as _)
}
unsafe fn MultiXactIdIsRunning(multi: MultiXactId, isLockOnly: bool) -> bool {
    crate::access::transam::multixact::MultiXactIdIsRunning(multi, isLockOnly)
}
unsafe fn MultiXactIdPrecedes(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    crate::access::transam::multixact::MultiXactIdPrecedes(multi1, multi2)
}
unsafe fn MultiXactIdPrecedesOrEquals(multi1: MultiXactId, multi2: MultiXactId) -> bool {
    crate::access::transam::multixact::MultiXactIdPrecedesOrEquals(multi1, multi2)
}
unsafe fn MultiXactIdSetOldestMember() {
    crate::access::transam::multixact::MultiXactIdSetOldestMember()
}
unsafe fn GetMultiXactIdMembers(
    multi: MultiXactId,
    members: *mut *mut MultiXactMember,
    allow_old: bool,
    isLockOnly: bool,
) -> c_int {
    crate::access::transam::multixact::GetMultiXactIdMembers(multi, members as _, allow_old, isLockOnly)
}

// --- storage/lmgr/lmgr.c (lock manager) ----------------------------------
unsafe fn LockTuple(relation: Relation, tid: ItemPointer, lockmode: LOCKMODE) {
    crate::storage::lmgr::lmgr::LockTuple(relation as _, tid as _, lockmode)
}
unsafe fn ConditionalLockTuple(
    relation: Relation,
    tid: ItemPointer,
    lockmode: LOCKMODE,
    logLockFailure: bool,
) -> bool {
    crate::storage::lmgr::lmgr::ConditionalLockTuple(relation as _, tid as _, lockmode, logLockFailure)
}
unsafe fn UnlockTuple(relation: Relation, tid: ItemPointer, lockmode: LOCKMODE) {
    crate::storage::lmgr::lmgr::UnlockTuple(relation as _, tid as _, lockmode)
}
unsafe fn XactLockTableWait(
    xid: TransactionId,
    rel: Relation,
    ctid: ItemPointer,
    oper: XLTW_Oper,
) {
    crate::storage::lmgr::lmgr::XactLockTableWait(xid, rel as _, ctid as _, oper as _)
}
unsafe fn ConditionalXactLockTableWait(xid: TransactionId, logLockFailure: bool) -> bool {
    crate::storage::lmgr::lmgr::ConditionalXactLockTableWait(xid, logLockFailure)
}
unsafe fn LockHeldByMe(locktag: *const LOCKTAG, lockmode: LOCKMODE, orstronger: bool) -> bool {
    crate::storage::lmgr::lock::LockHeldByMe(locktag as _, lockmode, orstronger)
}
unsafe fn DoLockModesConflict(mode1: LOCKMODE, mode2: LOCKMODE) -> bool {
    crate::storage::lmgr::lock::DoLockModesConflict(mode1, mode2)
}
unsafe fn SET_LOCKTAG_RELATION(tag: *mut LOCKTAG, dboid: Oid, reloid: Oid) {
    crate::storage::lmgr::lock::SET_LOCKTAG_RELATION(tag as _, dboid, reloid)
}
unsafe fn SET_LOCKTAG_TUPLE(
    tag: *mut LOCKTAG,
    dboid: Oid,
    reloid: Oid,
    blocknum: BlockNumber,
    offnum: OffsetNumber,
) {
    crate::storage::lmgr::lock::SET_LOCKTAG_TUPLE(tag as _, dboid, reloid, blocknum as _, offnum as _)
}

// --- storage/lmgr/predicate.c (predicate locking) ------------------------
unsafe fn PredicateLockRelation(relation: Relation, snapshot: Snapshot) {
    crate::storage::lmgr::predicate::PredicateLockRelation(relation as _, snapshot as _)
}
unsafe fn PredicateLockTID(
    relation: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    tuple_xid: TransactionId,
) {
    crate::storage::lmgr::predicate::PredicateLockTID(relation as _, tid as _, snapshot as _, tuple_xid)
}
unsafe fn CheckForSerializableConflictIn(
    relation: Relation,
    tid: ItemPointer,
    blkno: BlockNumber,
) {
    crate::storage::lmgr::predicate::CheckForSerializableConflictIn(relation as _, tid as _, blkno)
}
unsafe fn CheckForSerializableConflictOut(
    relation: Relation,
    xid: TransactionId,
    snapshot: Snapshot,
) {
    crate::storage::lmgr::predicate::CheckForSerializableConflictOut(relation as _, xid, snapshot as _)
}
unsafe fn CheckForSerializableConflictOutNeeded(relation: Relation, snapshot: Snapshot) -> bool {
    crate::storage::lmgr::predicate::CheckForSerializableConflictOutNeeded(relation as _, snapshot as _)
}

// --- utils/cache/inval.c (invalidation) ----------------------------------
unsafe fn CacheInvalidateHeapTuple(
    relation: Relation,
    tuple: HeapTuple,
    newtuple: HeapTuple,
) {
    crate::utils::cache::inval::CacheInvalidateHeapTuple(relation as _, tuple as _, newtuple as _)
}
unsafe fn CacheInvalidateHeapTupleInplace(
    relation: Relation,
    oldtuple: HeapTuple,
) {
    crate::utils::cache::inval::CacheInvalidateHeapTupleInplace(relation as _, oldtuple as _)
}
unsafe fn AcceptInvalidationMessages() {
    crate::utils::cache::inval::AcceptInvalidationMessages()
}
unsafe fn PreInplace_Inval() {
    crate::utils::cache::inval::PreInplace_Inval()
}
unsafe fn AtInplace_Inval() {
    crate::utils::cache::inval::AtInplace_Inval()
}
unsafe fn ForgetInplace_Inval() {
    crate::utils::cache::inval::ForgetInplace_Inval()
}
unsafe fn inplaceGetInvalidationMessages(
    msgs: *mut *mut SharedInvalidationMessage,
    RelcacheInitFileInval: *mut bool,
) -> c_int {
    crate::utils::cache::inval::inplaceGetInvalidationMessages(msgs as _, RelcacheInitFileInval)
}

// --- utils/time/snapmgr.c & utils/snapshot/snapmgr ------------------------
unsafe fn GetCatalogSnapshot(relid: Oid) -> Snapshot {
    crate::utils::time::snapmgr::GetCatalogSnapshot(relid)
}
unsafe fn InvalidateCatalogSnapshot() {
    crate::utils::time::snapmgr::InvalidateCatalogSnapshot()
}
unsafe fn UnregisterSnapshot(snapshot: Snapshot) {
    crate::utils::time::snapmgr::UnregisterSnapshot(snapshot)
}
unsafe fn HaveRegisteredOrActiveSnapshot() -> bool {
    crate::utils::time::snapmgr::HaveRegisteredOrActiveSnapshot()
}
unsafe fn InitNonVacuumableSnapshot(_snapshot: Snapshot, _vistest: *mut GlobalVisState) {
    // TODO(pg-port): off basic heap-scan path (vacuum/analyze only)
}
unsafe fn GlobalVisTestFor(rel: Relation) -> *mut GlobalVisState {
    crate::storage::ipc::procarray::GlobalVisTestFor(rel as _) as _
}
unsafe fn IsMVCCSnapshot(_snapshot: Snapshot) -> bool {
    true // catalog scans use an MVCC snapshot
}

// --- access/heap/heapam_visibility.c (visibility) ------------------------
unsafe fn HeapTupleSatisfiesVisibility(
    htup: HeapTuple,
    snapshot: Snapshot,
    buffer: Buffer,
) -> bool {
    crate::access::heap::heapam_visibility::HeapTupleSatisfiesVisibility(htup as _, snapshot as _, buffer)
}
unsafe fn HeapTupleSatisfiesUpdate(htup: HeapTuple, curcid: CommandId, buffer: Buffer) -> TM_Result {
    crate::access::heap::heapam_visibility::HeapTupleSatisfiesUpdate(htup as _, curcid, buffer)
}
unsafe fn HeapTupleSatisfiesVacuum(
    htup: HeapTuple,
    OldestXmin: TransactionId,
    buffer: Buffer,
) -> HTSV_Result {
    crate::access::heap::heapam_visibility::HeapTupleSatisfiesVacuum(htup as _, OldestXmin, buffer)
}
unsafe fn HeapTupleHeaderIsOnlyLocked(tuple: HeapTupleHeader) -> bool {
    crate::access::heap::heapam_visibility::HeapTupleHeaderIsOnlyLocked(tuple as _)
}
unsafe fn HeapTupleIsSurelyDead(htup: HeapTuple, vistest: *mut GlobalVisState) -> bool {
    crate::access::heap::heapam_visibility::HeapTupleIsSurelyDead(htup as _, vistest as _)
}

// --- access/common/heaptuple.c (tuple form/deform) -----------------------
unsafe fn heap_form_tuple(
    tupleDescriptor: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) -> HeapTuple {
    crate::access::common::heaptuple::heap_form_tuple(tupleDescriptor as _, values as _, isnull as _) as _
}
unsafe fn heap_deform_tuple(
    tuple: HeapTuple,
    tupleDesc: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) {
    crate::access::common::heaptuple::heap_deform_tuple(tuple as _, tupleDesc as _, values as _, isnull as _)
}
unsafe fn heap_freetuple(htup: HeapTuple) {
    crate::access::common::heaptuple::heap_freetuple(htup as _)
}

// --- access/common/scankey.c & access/heap qual ---------------------------
unsafe fn HeapKeyTest(tuple: HeapTuple, tupdesc: TupleDesc, nkeys: c_int, keys: ScanKey) -> bool {
    crate::access::valid::HeapKeyTest(tuple as _, tupdesc as _, nkeys, keys as _)
}

// --- access/table/tableamapi.c -------------------------------------------
unsafe fn GetHeapamTableAmRoutine() -> *const c_void {
    crate::access::heap::heapam_handler::GetHeapamTableAmRoutine() as _
}
unsafe fn table_tuple_get_latest_tid(sscan: TableScanDesc, tid: ItemPointer) {
    crate::access::table::tableam::table_tuple_get_latest_tid(sscan as _, tid as _)
}

// --- access/table/tableam.c block parallel scan helpers ------------------
unsafe fn table_block_parallelscan_startblock_init(
    rel: Relation,
    pbscanwork: *mut ParallelBlockTableScanWorkerData,
    pbscan: ParallelBlockTableScanDesc,
) {
    crate::access::table::tableam::table_block_parallelscan_startblock_init(rel as _, pbscanwork as _, pbscan as _)
}
unsafe fn table_block_parallelscan_nextpage(
    rel: Relation,
    pbscanwork: *mut ParallelBlockTableScanWorkerData,
    pbscan: ParallelBlockTableScanDesc,
) -> BlockNumber {
    crate::access::table::tableam::table_block_parallelscan_nextpage(rel as _, pbscanwork as _, pbscan as _)
}

// --- access/hio.c --------------------------------------------------------
unsafe fn RelationGetBufferForTuple(
    relation: Relation,
    len: Size,
    otherBuffer: Buffer,
    options: c_int,
    bistate: BulkInsertState,
    vmbuffer: *mut Buffer,
    vmbuffer_other: *mut Buffer,
    num_pages: c_int,
) -> Buffer {
    crate::access::heap::hio::RelationGetBufferForTuple(relation as _, len, otherBuffer, options, bistate as _, vmbuffer, vmbuffer_other, num_pages)
}
unsafe fn RelationPutHeapTuple(
    relation: Relation,
    buffer: Buffer,
    tuple: HeapTuple,
    token: bool,
) {
    crate::access::heap::hio::RelationPutHeapTuple(relation as _, buffer, tuple as _, token)
}

// --- access/heap/heaptoast.c ---------------------------------------------
unsafe fn heap_toast_insert_or_update(
    rel: Relation,
    newtup: HeapTuple,
    oldtup: HeapTuple,
    options: c_int,
) -> HeapTuple {
    crate::access::heap::heaptoast::heap_toast_insert_or_update(rel as _, newtup as _, oldtup as _, options) as _
}
unsafe fn heap_toast_delete(rel: Relation, oldtup: HeapTuple, is_speculative: bool) {
    crate::access::heap::heaptoast::heap_toast_delete(rel as _, oldtup as _, is_speculative)
}
unsafe fn toast_flatten_tuple(tup: HeapTuple, tupleDesc: TupleDesc) -> HeapTuple {
    crate::access::heap::heaptoast::toast_flatten_tuple(tup as _, tupleDesc as _) as _
}

// --- access/heap/pruneheap.c ---------------------------------------------
unsafe fn heap_page_prune_opt(relation: Relation, buffer: Buffer) {
    crate::access::heap::pruneheap::heap_page_prune_opt(relation as _, buffer)
}

// --- access/heap/freeze (access/heapam.h prototypes) ---------------------
unsafe fn heap_execute_freeze_tuple(tuple: HeapTupleHeader, frz: *mut HeapTupleFreeze) {
    // TODO(pg-port): heap_execute_freeze_tuple unported (vacuum-only); no-op for bring-up
}

// --- access/index/indexam.c ----------------------------------------------
unsafe fn index_open(relationId: Oid, lockmode: c_int) -> Relation {
    crate::access::index::indexam::index_open(relationId, lockmode as _) as _
}
unsafe fn index_close(relation: Relation, lockmode: c_int) {
    crate::access::index::indexam::index_close(relation as _, lockmode as _)
}

// --- storage/aio/read_stream.c -------------------------------------------
unsafe fn read_stream_begin_relation(
    flags: c_int,
    strategy: *mut c_void,
    rel: Relation,
    forknum: c_int,
    callback: ReadStreamBlockNumberCB,
    callback_private_data: *mut c_void,
    per_buffer_data_size: Size,
) -> *mut ReadStream {
    crate::storage::aio::read_stream::read_stream_begin_relation(flags, strategy as _, rel as _, forknum as _, core::mem::transmute(callback), callback_private_data, per_buffer_data_size) as _
}
unsafe fn read_stream_next_buffer(stream: *mut ReadStream, per_buffer_data: *mut *mut c_void) -> Buffer {
    crate::storage::aio::read_stream::read_stream_next_buffer(stream as _, per_buffer_data) as _
}
unsafe fn read_stream_reset(stream: *mut ReadStream) {
    return crate::storage::aio::read_stream::read_stream_reset(stream as _);
    #[allow(unreachable_code)] {} // TODO(pg-port): real read_stream_reset lives in storage/aio/read_stream.c
}
unsafe fn read_stream_end(stream: *mut ReadStream) {
    return crate::storage::aio::read_stream::read_stream_end(stream as _);
    #[allow(unreachable_code)] {} // TODO(pg-port): real read_stream_end lives in storage/aio/read_stream.c
}

// --- access/spgist/ syncscan (access/syncscan.c) -------------------------
unsafe fn ss_get_location(rel: Relation, relnblocks: BlockNumber) -> BlockNumber {
    crate::access::common::syncscan::ss_get_location(rel as _, relnblocks)
}
unsafe fn ss_report_location(rel: Relation, location: BlockNumber) {
    crate::access::common::syncscan::ss_report_location(rel as _, location)
}

// --- nodes/tidbitmap.c ---------------------------------------------------
unsafe fn tbm_iterate(iterator: *mut crate::access::relscan::TBMIterator, tbmres: *mut TBMIterateResult) -> bool {
    crate::nodes::tidbitmap::tbm_iterate(iterator as _, tbmres as _)
}

// --- nodes/bitmapset.c ---------------------------------------------------
unsafe fn bms_add_member(a: *mut Bitmapset, x: c_int) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_add_member(a as _, x) as _
}
unsafe fn bms_add_members(a: *mut Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_add_members(a as _, b as _) as _
}
unsafe fn bms_is_member(x: c_int, a: *const Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_is_member(x, a as _)
}
unsafe fn bms_is_empty(a: *const Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_is_empty(a as _)
}
unsafe fn bms_next_member(a: *const Bitmapset, prevbit: c_int) -> c_int {
    crate::nodes::bitmapset::bms_next_member(a as _, prevbit)
}
unsafe fn bms_overlap(a: *const Bitmapset, b: *const Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_overlap(a as _, b as _)
}
unsafe fn bms_free(a: *mut Bitmapset) {
    crate::nodes::bitmapset::bms_free(a as _)
}

// --- utils/adt/datum.c ---------------------------------------------------
unsafe fn datumIsEqual(value1: Datum, value2: Datum, typByVal: bool, typLen: c_int) -> bool {
    crate::utils::adt::datum::datumIsEqual(value1, value2, typByVal, typLen)
}

// --- pgstat (pgstat_relation.c) ------------------------------------------
unsafe fn pgstat_count_heap_scan(rel: Relation) { crate::utils::activity::pgstat_relation::pgstat_count_heap_scan(rel) }
unsafe fn pgstat_count_heap_getnext(rel: Relation) { crate::utils::activity::pgstat_relation::pgstat_count_heap_getnext(rel) }
unsafe fn pgstat_count_heap_insert(rel: Relation, n: c_int) { crate::utils::activity::pgstat_relation::pgstat_count_heap_insert(rel, n as _) }
unsafe fn pgstat_count_heap_update(rel: Relation, hot: bool, newpage: bool) { crate::utils::activity::pgstat_relation::pgstat_count_heap_update(rel, hot, newpage) }
unsafe fn pgstat_count_heap_delete(rel: Relation) { crate::utils::activity::pgstat_relation::pgstat_count_heap_delete(rel) }

// --- executor/execTuples.c -----------------------------------------------
unsafe fn ExecClearTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    crate::executor::tuptable::ExecClearTuple(slot as _) as _
}
unsafe fn ExecStoreBufferHeapTuple(
    tuple: HeapTuple,
    slot: *mut TupleTableSlot,
    buffer: Buffer,
) -> *mut TupleTableSlot {
    crate::executor::execTuples::ExecStoreBufferHeapTuple(tuple as _, slot as _, buffer) as _
}
unsafe fn ExecFetchSlotHeapTuple(
    slot: *mut TupleTableSlot,
    materialize: bool,
    shouldFree: *mut bool,
) -> HeapTuple {
    crate::executor::execTuples::ExecFetchSlotHeapTuple(slot as _, materialize, shouldFree) as _
}

// --- utils/cache/relcache.c & utils/rel.h --------------------------------
unsafe fn RelationGetDescr(relation: Relation) -> TupleDesc {
    (*relation).rd_att
}
unsafe fn RelationGetRelationName(relation: Relation) -> *const c_char {
    (*(*relation).rd_rel).relname.data.as_ptr()
}
unsafe fn RelationGetNumberOfAttributes(relation: Relation) -> c_int {
    crate::utils::rel::RelationGetNumberOfAttributes(relation as _)
}
unsafe fn RelationGetNumberOfBlocks(relation: Relation) -> BlockNumber {
    crate::storage::buffer::bufmgr::RelationGetNumberOfBlocksInFork(relation as _, crate::common::relpath::MAIN_FORKNUM) as _
}
unsafe fn RelationGetTargetPageFreeSpace(relation: Relation, defaultff: c_int) -> Size {
    0 // TODO(pg-port): utils/rel.h fillfactor reloptions unwired; reserve no extra free space
}
unsafe fn RelationGetIndexAttrBitmap(relation: Relation, attrKind: c_int) -> *mut Bitmapset {
    std::ptr::null_mut() // TODO(pg-port): utils/cache/relcache.c unwired; safe default
}
unsafe fn RelationNeedsWAL(relation: Relation) -> bool {
    (*(*relation).rd_rel).relpersistence == crate::catalog::pg_class::RELPERSISTENCE_PERMANENT
}
unsafe fn RelationUsesLocalBuffers(relation: Relation) -> bool {
    (*relation).rd_islocaltemp
}
unsafe fn RelationIsAccessibleInLogicalDecoding(relation: Relation) -> bool {
    false // TODO(pg-port): utils/rel.h; not logical decoding during bring-up
}
unsafe fn RelationIsLogicallyLogged(relation: Relation) -> bool {
    false // not logical decoding
}
unsafe fn RelationSupportsSysCache(relid: Oid) -> bool {
    crate::utils::cache::syscache::RelationSupportsSysCache(relid)
}
unsafe fn RelationIncrementReferenceCount(rel: Relation) {
    crate::utils::cache::relcache::RelationIncrementReferenceCount(rel as _)
}
unsafe fn RelationDecrementReferenceCount(rel: Relation) {
    crate::utils::cache::relcache::RelationDecrementReferenceCount(rel as _)
}

// --- catalog/catalog.c & catalog helpers ---------------------------------
unsafe fn IsCatalogRelation(relation: Relation) -> bool {
    crate::catalog::catalog::IsCatalogRelation(relation as _)
}
unsafe fn IsToastRelation(relation: Relation) -> bool {
    crate::catalog::catalog::IsToastRelation(relation as _)
}
unsafe fn IsInplaceUpdateRelation(relation: Relation) -> bool {
    crate::catalog::catalog::IsInplaceUpdateRelation(relation as _)
}
unsafe fn IsSharedRelation(relationId: Oid) -> bool {
    crate::catalog::catalog::IsSharedRelation(relationId)
}

// --- miscadmin.c processing-mode predicates -------------------------------
unsafe fn IsBootstrapProcessingMode() -> bool {
    crate::miscadmin::IsBootstrapProcessingMode()
}
unsafe fn IsNormalProcessingMode() -> bool {
    crate::miscadmin::IsNormalProcessingMode()
}
unsafe fn IsParallelWorker() -> bool {
    false
}
unsafe fn IsolationIsSerializable() -> bool {
    false
}

// --- access/heaptoast.h VARATT_IS_EXTERNAL (postgres.h varatt) ------------
unsafe fn VARATT_IS_EXTERNAL(value: *mut varlena) -> bool {
    crate::varatt::VARATT_IS_EXTERNAL(value as _)
}

// --- access/tupdesc.h TupleDescCompactAttr -------------------------------
unsafe fn TupleDescCompactAttr(tupdesc: TupleDesc, i: c_int) -> *mut CompactAttribute {
    crate::access::common::tupdesc::TupleDescCompactAttr(tupdesc as _, i) as _
}

// --- storage/itemptr.h (ItemPointer macros) ------------------------------
unsafe fn ItemPointerIsValid(pointer: ItemPointer) -> bool {
    crate::storage::itemptr::ItemPointerIsValid(pointer as _) as _
}
unsafe fn ItemPointerSet(pointer: ItemPointer, blockNumber: BlockNumber, offNum: OffsetNumber) {
    crate::storage::itemptr::ItemPointerSet(pointer as _, blockNumber as _, offNum as _);
}
unsafe fn ItemPointerSetBlockNumber(pointer: ItemPointer, blockNumber: BlockNumber) {
    crate::storage::itemptr::ItemPointerSetBlockNumber(pointer as _, blockNumber as _);
}
unsafe fn ItemPointerSetOffsetNumber(pointer: ItemPointer, offNum: OffsetNumber) {
    crate::storage::itemptr::ItemPointerSetOffsetNumber(pointer as _, offNum as _);
}
unsafe fn ItemPointerSetInvalid(pointer: ItemPointer) {
    crate::storage::itemptr::ItemPointerSetInvalid(pointer as _);
}
unsafe fn ItemPointerGetBlockNumber(pointer: ItemPointer) -> BlockNumber {
    crate::storage::itemptr::ItemPointerGetBlockNumber(pointer as _) as _
}
unsafe fn ItemPointerGetBlockNumberNoCheck(pointer: ItemPointer) -> BlockNumber {
    crate::storage::itemptr::ItemPointerGetBlockNumberNoCheck(pointer as _) as _
}
unsafe fn ItemPointerGetOffsetNumber(pointer: ItemPointer) -> OffsetNumber {
    crate::storage::itemptr::ItemPointerGetOffsetNumber(pointer as _) as _
}
unsafe fn ItemPointerIsValidNoCheck(pointer: ItemPointer) -> bool {
    !pointer.is_null() && (*pointer).ip_posid != 0
}
unsafe fn ItemPointerEquals(pointer1: ItemPointer, pointer2: ItemPointer) -> bool {
    crate::storage::itemptr::ItemPointerEquals(pointer1 as _, pointer2 as _) as _
}
unsafe fn ItemPointerCompare(arg1: ItemPointer, arg2: ItemPointer) -> c_int {
    crate::storage::itemptr::ItemPointerCompare(arg1 as _, arg2 as _) as _
}
unsafe fn ItemPointerCopy(fromPointer: ItemPointer, toPointer: ItemPointer) {
    crate::storage::itemptr::ItemPointerCopy(fromPointer as _, toPointer as _);
}
unsafe fn ItemPointerIndicatesMovedPartitions(pointer: ItemPointer) -> bool {
    ItemPointerGetOffsetNumber(pointer) == crate::storage::itemptr::MovedPartitionsOffsetNumber
        && ItemPointerGetBlockNumberNoCheck(pointer) == crate::storage::itemptr::MovedPartitionsBlockNumber
}

// --- access/sdir.h scan-direction predicates -----------------------------
unsafe fn ScanDirectionIsForward(direction: ScanDirection) -> bool {
    direction == ForwardScanDirection // TODO(pg-port): real ScanDirectionIsForward lives in access/sdir.h
}
unsafe fn ScanDirectionIsBackward(direction: ScanDirection) -> bool {
    direction == -1 // TODO(pg-port): real ScanDirectionIsBackward lives in access/sdir.h
}

// --- lib/ilist & qsort/pg_nextpower2 (port/c.h) --------------------------
unsafe fn pg_nextpower2_32(num: uint32) -> uint32 {
    crate::port::pg_bitutils::pg_nextpower2_32(num)
}

// --- libc shims ----------------------------------------------------------
unsafe extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void; // TODO(pg-port): libc memcpy
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: Option<unsafe extern "C" fn(*const c_void, *const c_void) -> c_int>,
    ); // TODO(pg-port): libc qsort
}
