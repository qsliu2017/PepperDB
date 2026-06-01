//! src/backend/access/table/tableam.c
//! src/include/access/tableam.h
//!
//! tableam.c
//!     Table access method routines too big to be inline functions.
//!
//! POSTGRES table access method definitions (merged from tableam.h).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES
//!     Note that most function in here are documented in tableam.h, rather than
//!     here. That's because there's a lot of inline functions in tableam.h and
//!     it'd be harder to understand if one constantly had to switch between files.

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32, uint8, uint32, uint64, Size, TransactionId, CommandId, MultiXactId};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::{BlockNumber, InvalidBlockNumber, MaxBlockNumber};
use crate::storage::off::OffsetNumber;
use crate::storage::itemptr::{ItemPointer, ItemPointerData};
use crate::storage::relfilelocator::{RelFileLocator, RelFileLocatorEquals};
use crate::nodes::nodes::NodeTag;
use crate::nodes::pg_list::{List, lappend};
use crate::utils::rel::Relation;
use crate::utils::snapshot::{Snapshot, InvalidSnapshot};
use crate::common::relpath::{ForkNumber, InvalidForkNumber, MAX_FORKNUM};
use crate::access::relscan::{
    TableScanDesc, ParallelTableScanDesc, IndexFetchTableData,
    ParallelBlockTableScanDesc, ParallelBlockTableScanDescData, ParallelBlockTableScanWorker,
};
use crate::access::common::scankey::ScanKeyData;
use crate::access::sdir::ScanDirection;
use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};
use crate::executor::execTuples::{TTSOpsVirtual, TTSOpsHeapTuple};
use crate::nodes::lockoptions::{LockTupleMode, LockWaitPolicy};
use crate::nodes::execnodes::{IndexInfo, SampleScanState};
use crate::catalog::pg_class::{RELKIND_FOREIGN_TABLE, RELKIND_VIEW, RELKIND_PARTITIONED_TABLE};
use crate::access::transam::TransactionIdIsValid;

/* StaticAssertStmt - compile-time assert; no-op stub (c.h) */
macro_rules! StaticAssertStmt {
    ($cond:expr, $msg:expr) => {};
}

use crate::pg_config::BLCKSZ;
use crate::port::pg_bitutils::pg_nextpower2_32;
use crate::storage::buf::BufferAccessStrategy;
use crate::utils::init::globals::NBuffers;
use crate::port::atomics::pg_atomic_uint64;
use crate::storage::lmgr::s_lock::slock_t;

/* ----------------------------------------------------------------------------
 * Header (tableam.h) type definitions
 * ----------------------------------------------------------------------------
 */

pub const DEFAULT_TABLE_ACCESS_METHOD: &[u8] = b"heap\0";

/*
 * Bitmask values for the flags argument to the scan_begin callback.
 */
pub type ScanOptions = c_int;
/* one of SO_TYPE_* may be specified */
pub const SO_TYPE_SEQSCAN: ScanOptions = 1 << 0;
pub const SO_TYPE_BITMAPSCAN: ScanOptions = 1 << 1;
pub const SO_TYPE_SAMPLESCAN: ScanOptions = 1 << 2;
pub const SO_TYPE_TIDSCAN: ScanOptions = 1 << 3;
pub const SO_TYPE_TIDRANGESCAN: ScanOptions = 1 << 4;
pub const SO_TYPE_ANALYZE: ScanOptions = 1 << 5;

/* several of SO_ALLOW_* may be specified */
/* allow or disallow use of access strategy */
pub const SO_ALLOW_STRAT: ScanOptions = 1 << 6;
/* report location to syncscan logic? */
pub const SO_ALLOW_SYNC: ScanOptions = 1 << 7;
/* verify visibility page-at-a-time? */
pub const SO_ALLOW_PAGEMODE: ScanOptions = 1 << 8;

/* unregister snapshot at scan end? */
pub const SO_TEMP_SNAPSHOT: ScanOptions = 1 << 9;

/*
 * Result codes for table_{update,delete,lock_tuple}, and for visibility
 * routines inside table AMs.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TM_Result {
    /*
     * Signals that the action succeeded (i.e. update/delete performed, lock
     * was acquired)
     */
    TM_Ok,

    /* The affected tuple wasn't visible to the relevant snapshot */
    TM_Invisible,

    /* The affected tuple was already modified by the calling backend */
    TM_SelfModified,

    /*
     * The affected tuple was updated by another transaction. This includes
     * the case where tuple was moved to another partition.
     */
    TM_Updated,

    /* The affected tuple was deleted by another transaction */
    TM_Deleted,

    /*
     * The affected tuple is currently being modified by another session. This
     * will only be returned if table_(update/delete/lock_tuple) are
     * instructed not to wait.
     */
    TM_BeingModified,

    /* lock couldn't be acquired, action skipped. Only used by lock_tuple */
    TM_WouldBlock,
}
pub use TM_Result::*;

/*
 * Result codes for table_update(..., update_indexes*..).
 * Used to determine which indexes to update.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TU_UpdateIndexes {
    /* No indexed columns were updated (incl. TID addressing of tuple) */
    TU_None,

    /* A non-summarizing indexed column was updated, or the TID has changed */
    TU_All,

    /* Only summarized columns were updated, TID is unchanged */
    TU_Summarizing,
}
pub use TU_UpdateIndexes::*;

/*
 * When table_tuple_update, table_tuple_delete, or table_tuple_lock fail
 * because the target tuple is already outdated, they fill in this struct to
 * provide information to the caller about what happened.
 */
#[repr(C)]
pub struct TM_FailureData {
    pub ctid: ItemPointerData,
    pub xmax: TransactionId,
    pub cmax: CommandId,
    pub traversed: bool,
}

/*
 * State used when calling table_index_delete_tuples().
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TM_IndexDelete {
    pub tid: ItemPointerData,   /* table TID from index tuple */
    pub id: int16,              /* Offset into TM_IndexStatus array */
}

#[repr(C)]
pub struct TM_IndexStatus {
    pub idxoffnum: OffsetNumber,    /* Index am page offset number */
    pub knowndeletable: bool,       /* Currently known to be deletable? */

    /* Bottom-up index deletion specific fields follow */
    pub promising: bool,            /* Promising (duplicate) index tuple? */
    pub freespace: int16,           /* Space freed in index if deleted */
}

#[repr(C)]
pub struct TM_IndexDeleteOp {
    pub irel: Relation,             /* Target index relation */
    pub iblknum: BlockNumber,       /* Index block number (for error reports) */
    pub bottomup: bool,             /* Bottom-up (not simple) deletion? */
    pub bottomupfreespace: c_int,   /* Bottom-up space target */

    /* Mutable per-TID information follows (index AM initializes entries) */
    pub ndeltids: c_int,            /* Current # of deltids/status elements */
    pub deltids: *mut TM_IndexDelete,
    pub status: *mut TM_IndexStatus,
}

/* "options" flag bits for table_tuple_insert */
/* TABLE_INSERT_SKIP_WAL was 0x0001; RelationNeedsWAL() now governs */
pub const TABLE_INSERT_SKIP_FSM: c_int = 0x0002;
pub const TABLE_INSERT_FROZEN: c_int = 0x0004;
pub const TABLE_INSERT_NO_LOGICAL: c_int = 0x0008;

/* flag bits for table_tuple_lock */
/* Follow tuples whose update is in progress if lock modes don't conflict  */
pub const TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS: c_int = 1 << 0;
/* Follow update chain and lock latest version of tuple */
pub const TUPLE_LOCK_FLAG_FIND_LAST_VERSION: c_int = 1 << 1;

/* Typedef for callback function for table_index_build_scan */
pub type IndexBuildCallback = Option<
    unsafe extern "C" fn(
        index: Relation,
        tid: ItemPointer,
        values: *mut Datum,
        isnull: *mut bool,
        tupleIsAlive: bool,
        state: *mut c_void,
    ),
>;

/*
 * API struct for a table AM.
 */
#[repr(C)]
pub struct TableAmRoutine {
    /* this must be set to T_TableAmRoutine */
    pub r#type: NodeTag,

    /* Slot related callbacks. */
    pub slot_callbacks: Option<unsafe fn(rel: Relation) -> *const TupleTableSlotOps>,

    /* Table scan callbacks. */
    pub scan_begin: Option<
        unsafe fn(
            rel: Relation,
            snapshot: Snapshot,
            nkeys: c_int,
            key: *mut ScanKeyData,
            pscan: ParallelTableScanDesc,
            flags: uint32,
        ) -> TableScanDesc,
    >,

    pub scan_end: Option<unsafe fn(scan: TableScanDesc)>,

    pub scan_rescan: Option<
        unsafe fn(
            scan: TableScanDesc,
            key: *mut ScanKeyData,
            set_params: bool,
            allow_strat: bool,
            allow_sync: bool,
            allow_pagemode: bool,
        ),
    >,

    pub scan_getnextslot:
        Option<unsafe fn(scan: TableScanDesc, direction: ScanDirection, slot: *mut TupleTableSlot) -> bool>,

    /* Optional functions to provide scanning for ranges of ItemPointers. */
    pub scan_set_tidrange:
        Option<unsafe fn(scan: TableScanDesc, mintid: ItemPointer, maxtid: ItemPointer)>,

    pub scan_getnextslot_tidrange:
        Option<unsafe fn(scan: TableScanDesc, direction: ScanDirection, slot: *mut TupleTableSlot) -> bool>,

    /* Parallel table scan related functions. */
    pub parallelscan_estimate: Option<unsafe fn(rel: Relation) -> Size>,

    pub parallelscan_initialize:
        Option<unsafe fn(rel: Relation, pscan: ParallelTableScanDesc) -> Size>,

    pub parallelscan_reinitialize:
        Option<unsafe fn(rel: Relation, pscan: ParallelTableScanDesc)>,

    /* Index Scan Callbacks */
    pub index_fetch_begin: Option<unsafe fn(rel: Relation) -> *mut IndexFetchTableData>,

    pub index_fetch_reset: Option<unsafe fn(data: *mut IndexFetchTableData)>,

    pub index_fetch_end: Option<unsafe fn(data: *mut IndexFetchTableData)>,

    pub index_fetch_tuple: Option<
        unsafe fn(
            scan: *mut IndexFetchTableData,
            tid: ItemPointer,
            snapshot: Snapshot,
            slot: *mut TupleTableSlot,
            call_again: *mut bool,
            all_dead: *mut bool,
        ) -> bool,
    >,

    /* Callbacks for non-modifying operations on individual tuples */
    pub tuple_fetch_row_version: Option<
        unsafe fn(rel: Relation, tid: ItemPointer, snapshot: Snapshot, slot: *mut TupleTableSlot) -> bool,
    >,

    pub tuple_tid_valid: Option<unsafe fn(scan: TableScanDesc, tid: ItemPointer) -> bool>,

    pub tuple_get_latest_tid: Option<unsafe fn(scan: TableScanDesc, tid: ItemPointer)>,

    pub tuple_satisfies_snapshot:
        Option<unsafe fn(rel: Relation, slot: *mut TupleTableSlot, snapshot: Snapshot) -> bool>,

    /* see table_index_delete_tuples() */
    pub index_delete_tuples:
        Option<unsafe fn(rel: Relation, delstate: *mut TM_IndexDeleteOp) -> TransactionId>,

    /* Manipulations of physical tuples. */
    pub tuple_insert: Option<
        unsafe fn(
            rel: Relation,
            slot: *mut TupleTableSlot,
            cid: CommandId,
            options: c_int,
            bistate: *mut BulkInsertStateData,
        ),
    >,

    pub tuple_insert_speculative: Option<
        unsafe fn(
            rel: Relation,
            slot: *mut TupleTableSlot,
            cid: CommandId,
            options: c_int,
            bistate: *mut BulkInsertStateData,
            specToken: uint32,
        ),
    >,

    pub tuple_complete_speculative: Option<
        unsafe fn(rel: Relation, slot: *mut TupleTableSlot, specToken: uint32, succeeded: bool),
    >,

    pub multi_insert: Option<
        unsafe fn(
            rel: Relation,
            slots: *mut *mut TupleTableSlot,
            nslots: c_int,
            cid: CommandId,
            options: c_int,
            bistate: *mut BulkInsertStateData,
        ),
    >,

    pub tuple_delete: Option<
        unsafe fn(
            rel: Relation,
            tid: ItemPointer,
            cid: CommandId,
            snapshot: Snapshot,
            crosscheck: Snapshot,
            wait: bool,
            tmfd: *mut TM_FailureData,
            changingPart: bool,
        ) -> TM_Result,
    >,

    pub tuple_update: Option<
        unsafe fn(
            rel: Relation,
            otid: ItemPointer,
            slot: *mut TupleTableSlot,
            cid: CommandId,
            snapshot: Snapshot,
            crosscheck: Snapshot,
            wait: bool,
            tmfd: *mut TM_FailureData,
            lockmode: *mut LockTupleMode,
            update_indexes: *mut TU_UpdateIndexes,
        ) -> TM_Result,
    >,

    pub tuple_lock: Option<
        unsafe fn(
            rel: Relation,
            tid: ItemPointer,
            snapshot: Snapshot,
            slot: *mut TupleTableSlot,
            cid: CommandId,
            mode: LockTupleMode,
            wait_policy: LockWaitPolicy,
            flags: uint8,
            tmfd: *mut TM_FailureData,
        ) -> TM_Result,
    >,

    pub finish_bulk_insert: Option<unsafe fn(rel: Relation, options: c_int)>,

    /* DDL related functionality. */
    pub relation_set_new_filelocator: Option<
        unsafe fn(
            rel: Relation,
            newrlocator: *const RelFileLocator,
            persistence: c_char,
            freezeXid: *mut TransactionId,
            minmulti: *mut MultiXactId,
        ),
    >,

    pub relation_nontransactional_truncate: Option<unsafe fn(rel: Relation)>,

    pub relation_copy_data: Option<unsafe fn(rel: Relation, newrlocator: *const RelFileLocator)>,

    pub relation_copy_for_cluster: Option<
        unsafe fn(
            OldTable: Relation,
            NewTable: Relation,
            OldIndex: Relation,
            use_sort: bool,
            OldestXmin: TransactionId,
            xid_cutoff: *mut TransactionId,
            multi_cutoff: *mut MultiXactId,
            num_tuples: *mut f64,
            tups_vacuumed: *mut f64,
            tups_recently_dead: *mut f64,
        ),
    >,

    pub relation_vacuum:
        Option<unsafe fn(rel: Relation, params: *mut VacuumParams, bstrategy: BufferAccessStrategy)>,

    pub scan_analyze_next_block:
        Option<unsafe fn(scan: TableScanDesc, stream: *mut ReadStream) -> bool>,

    pub scan_analyze_next_tuple: Option<
        unsafe fn(
            scan: TableScanDesc,
            OldestXmin: TransactionId,
            liverows: *mut f64,
            deadrows: *mut f64,
            slot: *mut TupleTableSlot,
        ) -> bool,
    >,

    pub index_build_range_scan: Option<
        unsafe fn(
            table_rel: Relation,
            index_rel: Relation,
            index_info: *mut IndexInfo,
            allow_sync: bool,
            anyvisible: bool,
            progress: bool,
            start_blockno: BlockNumber,
            numblocks: BlockNumber,
            callback: IndexBuildCallback,
            callback_state: *mut c_void,
            scan: TableScanDesc,
        ) -> f64,
    >,

    pub index_validate_scan: Option<
        unsafe fn(
            table_rel: Relation,
            index_rel: Relation,
            index_info: *mut IndexInfo,
            snapshot: Snapshot,
            state: *mut ValidateIndexState,
        ),
    >,

    /* Miscellaneous functions. */
    pub relation_size: Option<unsafe fn(rel: Relation, forkNumber: ForkNumber) -> uint64>,

    pub relation_needs_toast_table: Option<unsafe fn(rel: Relation) -> bool>,

    pub relation_toast_am: Option<unsafe fn(rel: Relation) -> Oid>,

    pub relation_fetch_toast_slice: Option<
        unsafe fn(
            toastrel: Relation,
            valueid: Oid,
            attrsize: int32,
            sliceoffset: int32,
            slicelength: int32,
            result: *mut varlena,
        ),
    >,

    /* Planner related functions. */
    pub relation_estimate_size: Option<
        unsafe fn(
            rel: Relation,
            attr_widths: *mut int32,
            pages: *mut BlockNumber,
            tuples: *mut f64,
            allvisfrac: *mut f64,
        ),
    >,

    /* Executor related functions. */
    pub scan_bitmap_next_tuple: Option<
        unsafe fn(
            scan: TableScanDesc,
            slot: *mut TupleTableSlot,
            recheck: *mut bool,
            lossy_pages: *mut uint64,
            exact_pages: *mut uint64,
        ) -> bool,
    >,

    pub scan_sample_next_block:
        Option<unsafe fn(scan: TableScanDesc, scanstate: *mut SampleScanState) -> bool>,

    pub scan_sample_next_tuple: Option<
        unsafe fn(scan: TableScanDesc, scanstate: *mut SampleScanState, slot: *mut TupleTableSlot) -> bool,
    >,
}

/* ----------------------------------------------------------------------------
 * tableam.c implementation
 * ----------------------------------------------------------------------------
 */

/*
 * Constants to control the behavior of block allocation to parallel workers
 * during a parallel seqscan.  Technically these values do not need to be
 * powers of 2, but having them as powers of 2 makes the math more optimal
 * and makes the ramp-down stepping more even.
 */

/* The number of I/O chunks we try to break a parallel seqscan down into */
const PARALLEL_SEQSCAN_NCHUNKS: BlockNumber = 2048;
/* Ramp down size of allocations when we've only this number of chunks left */
const PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS: BlockNumber = 64;
/* Cap the size of parallel I/O chunks to this number of blocks */
const PARALLEL_SEQSCAN_MAX_CHUNK_SIZE: BlockNumber = 8192;

/* GUC variables */
pub static mut default_table_access_method: *mut c_char =
    DEFAULT_TABLE_ACCESS_METHOD.as_ptr() as *mut c_char;
pub static mut synchronize_seqscans: bool = true;


/* ----------------------------------------------------------------------------
 * Slot functions.
 * ----------------------------------------------------------------------------
 */

pub unsafe fn table_slot_callbacks(relation: Relation) -> *const TupleTableSlotOps {
    let tts_cb: *const TupleTableSlotOps;

    if !(*relation).rd_tableam.is_null() {
        tts_cb = ((*((*relation).rd_tableam as *const TableAmRoutine)).slot_callbacks.unwrap())(relation);
    } else if (*(*relation).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
        /*
         * Historically FDWs expect to store heap tuples in slots. Continue
         * handing them one, to make it less painful to adapt FDWs to new
         * versions. The cost of a heap slot over a virtual slot is pretty
         * small.
         */
        tts_cb = &raw const TTSOpsHeapTuple;
    } else {
        /*
         * These need to be supported, as some parts of the code (like COPY)
         * need to create slots for such relations too. It seems better to
         * centralize the knowledge that a heap slot is the right thing in
         * that case here.
         */
        Assert!(
            (*(*relation).rd_rel).relkind == RELKIND_VIEW
                || (*(*relation).rd_rel).relkind == RELKIND_PARTITIONED_TABLE
        );
        tts_cb = &raw const TTSOpsVirtual;
    }

    tts_cb
}

pub unsafe fn table_slot_create(
    relation: Relation,
    reglist: *mut *mut List,
) -> *mut TupleTableSlot {
    let tts_cb: *const TupleTableSlotOps;
    let slot: *mut TupleTableSlot;

    tts_cb = table_slot_callbacks(relation);
    slot = MakeSingleTupleTableSlot(RelationGetDescr(relation), tts_cb);

    if !reglist.is_null() {
        *reglist = lappend(*reglist, slot as *mut c_void);
    }

    slot
}


/* ----------------------------------------------------------------------------
 * Table scan functions.
 * ----------------------------------------------------------------------------
 */

pub unsafe fn table_beginscan_catalog(
    relation: Relation,
    nkeys: c_int,
    key: *mut ScanKeyData,
) -> TableScanDesc {
    let flags: uint32 = (SO_TYPE_SEQSCAN
        | SO_ALLOW_STRAT
        | SO_ALLOW_SYNC
        | SO_ALLOW_PAGEMODE
        | SO_TEMP_SNAPSHOT) as uint32;
    let relid: Oid = RelationGetRelid(relation);
    let snapshot: Snapshot = RegisterSnapshot(GetCatalogSnapshot(relid));

    ((*((*relation).rd_tableam as *const TableAmRoutine)).scan_begin.unwrap())(
        relation,
        snapshot,
        nkeys,
        key,
        std::ptr::null_mut(),
        flags,
    )
}


/* ----------------------------------------------------------------------------
 * Parallel table scan related functions.
 * ----------------------------------------------------------------------------
 */

pub unsafe fn table_parallelscan_estimate(rel: Relation, snapshot: Snapshot) -> Size {
    let mut sz: Size = 0;

    if IsMVCCSnapshot(snapshot) {
        sz = add_size(sz, EstimateSnapshotSpace(snapshot));
    } else {
        Assert!(snapshot == SnapshotAny);
    }

    sz = add_size(sz, ((*((*rel).rd_tableam as *const TableAmRoutine)).parallelscan_estimate.unwrap())(rel));

    sz
}

pub unsafe fn table_parallelscan_initialize(
    rel: Relation,
    pscan: ParallelTableScanDesc,
    snapshot: Snapshot,
) {
    let snapshot_off: Size =
        ((*((*rel).rd_tableam as *const TableAmRoutine)).parallelscan_initialize.unwrap())(rel, pscan);

    (*pscan).phs_snapshot_off = snapshot_off;

    if IsMVCCSnapshot(snapshot) {
        SerializeSnapshot(
            snapshot,
            (pscan as *mut c_char).add((*pscan).phs_snapshot_off),
        );
        (*pscan).phs_snapshot_any = false;
    } else {
        Assert!(snapshot == SnapshotAny);
        (*pscan).phs_snapshot_any = true;
    }
}

pub unsafe fn table_beginscan_parallel(
    relation: Relation,
    pscan: ParallelTableScanDesc,
) -> TableScanDesc {
    let snapshot: Snapshot;
    let mut flags: uint32 =
        (SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE) as uint32;

    Assert!(RelFileLocatorEquals(core::mem::transmute(&(*relation).rd_locator), core::mem::transmute(&(*pscan).phs_locator)));

    if !(*pscan).phs_snapshot_any {
        /* Snapshot was serialized -- restore it */
        snapshot = RestoreSnapshot((pscan as *mut c_char).add((*pscan).phs_snapshot_off));
        RegisterSnapshot(snapshot);
        flags |= SO_TEMP_SNAPSHOT as uint32;
    } else {
        /* SnapshotAny passed by caller (not serialized) */
        snapshot = SnapshotAny;
    }

    ((*((*relation).rd_tableam as *const TableAmRoutine)).scan_begin.unwrap())(
        relation,
        snapshot,
        0,
        std::ptr::null_mut(),
        pscan,
        flags,
    )
}


/* ----------------------------------------------------------------------------
 * Index scan related functions.
 * ----------------------------------------------------------------------------
 */

/*
 * To perform that check simply start an index scan, create the necessary
 * slot, do the heap lookup, and shut everything down again.
 *
 * Note that *tid may be modified when we return true if the AM supports
 * storing multiple row versions reachable via a single index entry (like
 * heap's HOT).
 */
pub unsafe fn table_index_fetch_tuple_check(
    rel: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    all_dead: *mut bool,
) -> bool {
    let scan: *mut IndexFetchTableData;
    let slot: *mut TupleTableSlot;
    let mut call_again: bool = false;
    let found: bool;

    slot = table_slot_create(rel, std::ptr::null_mut());
    scan = table_index_fetch_begin(rel);
    found = table_index_fetch_tuple(scan, tid, snapshot, slot, &mut call_again, all_dead);
    table_index_fetch_end(scan);
    ExecDropSingleTupleTableSlot(slot);

    found
}


/* ------------------------------------------------------------------------
 * Functions for non-modifying operations on individual tuples
 * ------------------------------------------------------------------------
 */

pub unsafe fn table_tuple_get_latest_tid(scan: TableScanDesc, tid: ItemPointer) {
    let rel: Relation = (*scan).rs_rd;
    let tableam: *const TableAmRoutine = (*rel).rd_tableam as *const TableAmRoutine;

    /*
     * We don't expect direct calls to table_tuple_get_latest_tid with valid
     * CheckXidAlive for catalog or regular tables.  See detailed comments in
     * xact.c where these variables are declared.
     */
    if unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan) {
        elog!(ERROR, "unexpected table_tuple_get_latest_tid call during logical decoding");
    }

    /*
     * Since this can be called with user-supplied TID, don't trust the input
     * too much.
     */
    if !((*tableam).tuple_tid_valid.unwrap())(scan, tid) {
        ereport!(
            ERROR,
            // errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            // errmsg("tid (%u, %u) is not valid for relation \"%s\"", ...)
            format!(
                "tid ({}, {}) is not valid for relation \"{}\"",
                ItemPointerGetBlockNumberNoCheck(tid),
                ItemPointerGetOffsetNumberNoCheck(tid),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
            )
        );
    }

    ((*tableam).tuple_get_latest_tid.unwrap())(scan, tid);
}


/* ----------------------------------------------------------------------------
 * Functions to make modifications a bit simpler.
 * ----------------------------------------------------------------------------
 */

/*
 * simple_table_tuple_insert - insert a tuple
 *
 * Currently, this routine differs from table_tuple_insert only in supplying a
 * default command ID and not allowing access to the speedup options.
 */
pub unsafe fn simple_table_tuple_insert(rel: Relation, slot: *mut TupleTableSlot) {
    table_tuple_insert(rel, slot, GetCurrentCommandId(true), 0, std::ptr::null_mut());
}

/*
 * simple_table_tuple_delete - delete a tuple
 *
 * This routine may be used to delete a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).  Any failure is reported
 * via ereport().
 */
pub unsafe fn simple_table_tuple_delete(rel: Relation, tid: ItemPointer, snapshot: Snapshot) {
    let result: TM_Result;
    let mut tmfd: TM_FailureData = std::mem::zeroed();

    result = table_tuple_delete(
        rel,
        tid,
        GetCurrentCommandId(true),
        snapshot,
        InvalidSnapshot,
        true, /* wait for commit */
        &mut tmfd,
        false, /* changingPart */
    );

    match result {
        TM_SelfModified => {
            /* Tuple was already updated in current command? */
            elog!(ERROR, "tuple already updated by self");
        }

        TM_Ok => {
            /* done successfully */
        }

        TM_Updated => {
            elog!(ERROR, "tuple concurrently updated");
        }

        TM_Deleted => {
            elog!(ERROR, "tuple concurrently deleted");
        }

        _ => {
            elog!(ERROR, "unrecognized table_tuple_delete status: {}", result as uint32);
        }
    }
}

/*
 * simple_table_tuple_update - replace a tuple
 *
 * This routine may be used to update a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).  Any failure is reported
 * via ereport().
 */
pub unsafe fn simple_table_tuple_update(
    rel: Relation,
    otid: ItemPointer,
    slot: *mut TupleTableSlot,
    snapshot: Snapshot,
    update_indexes: *mut TU_UpdateIndexes,
) {
    let result: TM_Result;
    let mut tmfd: TM_FailureData = std::mem::zeroed();
    let mut lockmode: LockTupleMode = std::mem::zeroed();

    result = table_tuple_update(
        rel,
        otid,
        slot,
        GetCurrentCommandId(true),
        snapshot,
        InvalidSnapshot,
        true, /* wait for commit */
        &mut tmfd,
        &mut lockmode,
        update_indexes,
    );

    match result {
        TM_SelfModified => {
            /* Tuple was already updated in current command? */
            elog!(ERROR, "tuple already updated by self");
        }

        TM_Ok => {
            /* done successfully */
        }

        TM_Updated => {
            elog!(ERROR, "tuple concurrently updated");
        }

        TM_Deleted => {
            elog!(ERROR, "tuple concurrently deleted");
        }

        _ => {
            elog!(ERROR, "unrecognized table_tuple_update status: {}", result as uint32);
        }
    }
}


/* ----------------------------------------------------------------------------
 * Helper functions to implement parallel scans for block oriented AMs.
 * ----------------------------------------------------------------------------
 */

pub unsafe fn table_block_parallelscan_estimate(_rel: Relation) -> Size {
    std::mem::size_of::<ParallelBlockTableScanDescData>()
}

pub unsafe fn table_block_parallelscan_initialize(
    rel: Relation,
    pscan: ParallelTableScanDesc,
) -> Size {
    let bpscan: ParallelBlockTableScanDesc = pscan as ParallelBlockTableScanDesc;

    (*bpscan).base.phs_locator = core::mem::transmute((*rel).rd_locator);
    (*bpscan).phs_nblocks = RelationGetNumberOfBlocks(rel);
    /* compare phs_syncscan initialization to similar logic in initscan */
    (*bpscan).base.phs_syncscan = synchronize_seqscans
        && !RelationUsesLocalBuffers(rel)
        && (*bpscan).phs_nblocks > (NBuffers / 4) as BlockNumber;
    SpinLockInit(&mut (*bpscan).phs_mutex);
    (*bpscan).phs_startblock = InvalidBlockNumber;
    pg_atomic_init_u64(&mut (*bpscan).phs_nallocated, 0);

    std::mem::size_of::<ParallelBlockTableScanDescData>()
}

pub unsafe fn table_block_parallelscan_reinitialize(
    _rel: Relation,
    pscan: ParallelTableScanDesc,
) {
    let bpscan: ParallelBlockTableScanDesc = pscan as ParallelBlockTableScanDesc;

    pg_atomic_write_u64(&mut (*bpscan).phs_nallocated, 0);
}

/*
 * find and set the scan's startblock
 *
 * Determine where the parallel seq scan should start.  This function may be
 * called many times, once by each parallel worker.  We must be careful only
 * to set the startblock once.
 */
pub unsafe fn table_block_parallelscan_startblock_init(
    rel: Relation,
    pbscanwork: ParallelBlockTableScanWorker,
    pbscan: ParallelBlockTableScanDesc,
) {
    let mut sync_startpage: BlockNumber = InvalidBlockNumber;

    /* Reset the state we use for controlling allocation size. */
    std::ptr::write_bytes(pbscanwork, 0, 1);

    StaticAssertStmt!(
        MaxBlockNumber <= 0xFFFFFFFE,
        "pg_nextpower2_32 may be too small for non-standard BlockNumber width"
    );

    /*
     * We determine the chunk size based on the size of the relation. First we
     * split the relation into PARALLEL_SEQSCAN_NCHUNKS chunks but we then
     * take the next highest power of 2 number of the chunk size.  This means
     * we split the relation into somewhere between PARALLEL_SEQSCAN_NCHUNKS
     * and PARALLEL_SEQSCAN_NCHUNKS / 2 chunks.
     */
    (*pbscanwork).phsw_chunk_size = pg_nextpower2_32(Max(
        (*pbscan).phs_nblocks / PARALLEL_SEQSCAN_NCHUNKS,
        1,
    ));

    /*
     * Ensure we don't go over the maximum chunk size with larger tables. This
     * means we may get much more than PARALLEL_SEQSCAN_NCHUNKS for larger
     * tables.  Too large a chunk size has been shown to be detrimental to
     * synchronous scan performance.
     */
    (*pbscanwork).phsw_chunk_size =
        Min((*pbscanwork).phsw_chunk_size, PARALLEL_SEQSCAN_MAX_CHUNK_SIZE);

    'retry: loop {
        /* Grab the spinlock. */
        SpinLockAcquire(&mut (*pbscan).phs_mutex);

        /*
         * If the scan's startblock has not yet been initialized, we must do so
         * now.  If this is not a synchronized scan, we just start at block 0, but
         * if it is a synchronized scan, we must get the starting position from
         * the synchronized scan machinery.  We can't hold the spinlock while
         * doing that, though, so release the spinlock, get the information we
         * need, and retry.  If nobody else has initialized the scan in the
         * meantime, we'll fill in the value we fetched on the second time
         * through.
         */
        if (*pbscan).phs_startblock == InvalidBlockNumber {
            if !(*pbscan).base.phs_syncscan {
                (*pbscan).phs_startblock = 0;
            } else if sync_startpage != InvalidBlockNumber {
                (*pbscan).phs_startblock = sync_startpage;
            } else {
                SpinLockRelease(&mut (*pbscan).phs_mutex);
                sync_startpage = ss_get_location(rel, (*pbscan).phs_nblocks);
                continue 'retry;
            }
        }
        SpinLockRelease(&mut (*pbscan).phs_mutex);
        break;
    }
}

/*
 * get the next page to scan
 *
 * Get the next page to scan.  Even if there are no pages left to scan,
 * another backend could have grabbed a page to scan and not yet finished
 * looking at it, so it doesn't follow that the scan is done when the first
 * backend gets an InvalidBlockNumber return.
 */
pub unsafe fn table_block_parallelscan_nextpage(
    rel: Relation,
    pbscanwork: ParallelBlockTableScanWorker,
    pbscan: ParallelBlockTableScanDesc,
) -> BlockNumber {
    let page: BlockNumber;
    let nallocated: uint64;

    /*
     * The logic below allocates block numbers out to parallel workers in a
     * way that each worker will receive a set of consecutive block numbers to
     * scan.  See the full comment in the C source for the rationale.
     *
     * First check if we have any remaining blocks in a previous chunk for
     * this worker.  We must consume all of the blocks from that before we
     * allocate a new chunk to the worker.
     */
    if (*pbscanwork).phsw_chunk_remaining > 0 {
        /*
         * Give them the next block in the range and update the remaining
         * number of blocks.
         */
        (*pbscanwork).phsw_nallocated += 1;
        nallocated = (*pbscanwork).phsw_nallocated;
        (*pbscanwork).phsw_chunk_remaining -= 1;
    } else {
        /*
         * When we've only got PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS chunks
         * remaining in the scan, we half the chunk size.  Since we reduce the
         * chunk size here, we'll hit this again after doing
         * PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS at the new size.  After a few
         * iterations of this, we'll end up doing the last few blocks with the
         * chunk size set to 1.
         */
        if (*pbscanwork).phsw_chunk_size > 1
            && (*pbscanwork).phsw_nallocated as BlockNumber
                > (*pbscan).phs_nblocks
                    - ((*pbscanwork).phsw_chunk_size * PARALLEL_SEQSCAN_RAMPDOWN_CHUNKS)
        {
            (*pbscanwork).phsw_chunk_size >>= 1;
        }

        (*pbscanwork).phsw_nallocated = pg_atomic_fetch_add_u64(
            &mut (*pbscan).phs_nallocated,
            (*pbscanwork).phsw_chunk_size as uint64,
        );
        nallocated = (*pbscanwork).phsw_nallocated;

        /*
         * Set the remaining number of blocks in this chunk so that subsequent
         * calls from this worker continue on with this chunk until it's done.
         */
        (*pbscanwork).phsw_chunk_remaining = (*pbscanwork).phsw_chunk_size - 1;
    }

    if nallocated >= (*pbscan).phs_nblocks as uint64 {
        page = InvalidBlockNumber; /* all blocks have been allocated */
    } else {
        page = ((nallocated + (*pbscan).phs_startblock as uint64)
            % (*pbscan).phs_nblocks as uint64) as BlockNumber;
    }

    /*
     * Report scan location.  Normally, we report the current page number.
     * When we reach the end of the scan, though, we report the starting page,
     * not the ending page, just so the starting positions for later scans
     * doesn't slew backwards.  We only report the position at the end of the
     * scan once, though: subsequent callers will report nothing.
     */
    if (*pbscan).base.phs_syncscan {
        if page != InvalidBlockNumber {
            ss_report_location(rel, page);
        } else if nallocated == (*pbscan).phs_nblocks as uint64 {
            ss_report_location(rel, (*pbscan).phs_startblock);
        }
    }

    page
}

/* ----------------------------------------------------------------------------
 * Helper functions to implement relation sizing for block oriented AMs.
 * ----------------------------------------------------------------------------
 */

/*
 * table_block_relation_size
 *
 * If a table AM uses the various relation forks as the sole place where data
 * is stored, and if it uses them in the expected manner (e.g. the actual data
 * is in the main fork rather than some other), it can use this implementation
 * of the relation_size callback rather than implementing its own.
 */
pub unsafe fn table_block_relation_size(rel: Relation, forkNumber: ForkNumber) -> uint64 {
    let mut nblocks: uint64 = 0;

    /* InvalidForkNumber indicates returning the size for all forks */
    if forkNumber == InvalidForkNumber {
        for i in 0..MAX_FORKNUM {
            nblocks += smgrnblocks(RelationGetSmgr(rel), i) as uint64;
        }
    } else {
        nblocks = smgrnblocks(RelationGetSmgr(rel), forkNumber) as uint64;
    }

    nblocks * BLCKSZ as uint64
}

/*
 * table_block_relation_estimate_size
 *
 * This function can't be directly used as the implementation of the
 * relation_estimate_size callback, because it has a few additional parameters.
 * Instead, it is intended to be used as a helper function; the caller can
 * pass through the arguments to its relation_estimate_size function plus the
 * additional values required here.
 *
 * overhead_bytes_per_tuple should contain the approximate number of bytes
 * of storage required to store a tuple above and beyond what is required for
 * the tuple data proper.
 *
 * usable_bytes_per_page should contain the approximate number of bytes per
 * page usable for tuple data, excluding the page header and any anticipated
 * special space.
 */
pub unsafe fn table_block_relation_estimate_size(
    rel: Relation,
    attr_widths: *mut int32,
    pages: *mut BlockNumber,
    tuples: *mut f64,
    allvisfrac: *mut f64,
    overhead_bytes_per_tuple: Size,
    usable_bytes_per_page: Size,
) {
    let mut curpages: BlockNumber;
    let relpages: BlockNumber;
    let reltuples: f64;
    let relallvisible: BlockNumber;
    let mut density: f64;

    /* it should have storage, so we can call the smgr */
    curpages = RelationGetNumberOfBlocks(rel);

    /* coerce values in pg_class to more desirable types */
    relpages = (*(*rel).rd_rel).relpages as BlockNumber;
    reltuples = (*(*rel).rd_rel).reltuples as f64;
    relallvisible = (*(*rel).rd_rel).relallvisible as BlockNumber;

    /*
     * HACK: if the relation has never yet been vacuumed, use a minimum size
     * estimate of 10 pages.  The idea here is to avoid assuming a
     * newly-created table is really small, even if it currently is, because
     * that may not be true once some data gets loaded into it.
     *
     * We test "never vacuumed" by seeing whether reltuples < 0.
     *
     * If the table has inheritance children, we don't apply this heuristic.
     */
    if curpages < 10 && reltuples < 0.0 && !(*(*rel).rd_rel).relhassubclass {
        curpages = 10;
    }

    /* report estimated # pages */
    *pages = curpages;
    /* quick exit if rel is clearly empty */
    if curpages == 0 {
        *tuples = 0.0;
        *allvisfrac = 0.0;
        return;
    }

    /* estimate number of tuples from previous tuple density */
    if reltuples >= 0.0 && relpages > 0 {
        density = reltuples / relpages as f64;
    } else {
        /*
         * When we have no data because the relation was never yet vacuumed,
         * estimate tuple width from attribute datatypes.  We assume here that
         * the pages are completely full, which is OK for tables but is
         * probably an overestimate for indexes.
         *
         * Note: this code intentionally disregards alignment considerations.
         */
        let mut tuple_width: int32;
        let fillfactor: c_int;

        /*
         * Without reltuples/relpages, we also need to consider fillfactor.
         * The other branch considers it implicitly by calculating density
         * from actual relpages/reltuples statistics.
         */
        fillfactor = RelationGetFillFactor(rel, HEAP_DEFAULT_FILLFACTOR);

        tuple_width = get_rel_data_width(rel, attr_widths);
        tuple_width += overhead_bytes_per_tuple as int32;
        /* note: integer division is intentional here */
        density = ((usable_bytes_per_page as c_int * fillfactor / 100) / tuple_width) as f64;
        /* There's at least one row on the page, even with low fillfactor. */
        density = clamp_row_est(density);
    }
    *tuples = rint(density * curpages as f64);

    /*
     * We use relallvisible as-is, rather than scaling it up like we do for
     * the pages and tuples counts, on the theory that any pages added since
     * the last VACUUM are most likely not marked all-visible.  But costsize.c
     * wants it converted to a fraction.
     */
    if relallvisible == 0 || curpages == 0 {
        *allvisfrac = 0.0;
    } else if relallvisible as f64 >= curpages as f64 {
        *allvisfrac = 1.0;
    } else {
        *allvisfrac = relallvisible as f64 / curpages as f64;
    }
}


/* ----------------------------------------------------------------------------
 * Selected static inline wrappers from tableam.h, needed by tableam.c.
 * ----------------------------------------------------------------------------
 */

/*
 * Prepare to fetch tuples from the relation, as needed when fetching tuples
 * for an index scan.
 */
pub unsafe fn table_index_fetch_begin(rel: Relation) -> *mut IndexFetchTableData {
    ((*((*rel).rd_tableam as *const TableAmRoutine)).index_fetch_begin.unwrap())(rel)
}

/*
 * Release resources and deallocate index fetch.
 */
pub unsafe fn table_index_fetch_end(scan: *mut IndexFetchTableData) {
    ((*((*(*scan).rel).rd_tableam as *const TableAmRoutine)).index_fetch_end.unwrap())(scan);
}

/*
 * Fetches, as part of an index scan, tuple at `tid` into `slot`.
 */
pub unsafe fn table_index_fetch_tuple(
    scan: *mut IndexFetchTableData,
    tid: ItemPointer,
    snapshot: Snapshot,
    slot: *mut TupleTableSlot,
    call_again: *mut bool,
    all_dead: *mut bool,
) -> bool {
    /*
     * We don't expect direct calls to table_index_fetch_tuple with valid
     * CheckXidAlive for catalog or regular tables.  See detailed comments in
     * xact.c where these variables are declared.
     */
    if unlikely(TransactionIdIsValid(CheckXidAlive) && !bsysscan) {
        elog!(ERROR, "unexpected table_index_fetch_tuple call during logical decoding");
    }

    ((*((*(*scan).rel).rd_tableam as *const TableAmRoutine)).index_fetch_tuple.unwrap())(
        scan, tid, snapshot, slot, call_again, all_dead,
    )
}

/*
 * Insert a tuple from a slot into table AM routine.
 */
pub unsafe fn table_tuple_insert(
    rel: Relation,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    options: c_int,
    bistate: *mut BulkInsertStateData,
) {
    ((*((*rel).rd_tableam as *const TableAmRoutine)).tuple_insert.unwrap())(rel, slot, cid, options, bistate);
}

/*
 * Delete a tuple.
 */
pub unsafe fn table_tuple_delete(
    rel: Relation,
    tid: ItemPointer,
    cid: CommandId,
    snapshot: Snapshot,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    changingPart: bool,
) -> TM_Result {
    ((*((*rel).rd_tableam as *const TableAmRoutine)).tuple_delete.unwrap())(
        rel, tid, cid, snapshot, crosscheck, wait, tmfd, changingPart,
    )
}

/*
 * Update a tuple.
 */
pub unsafe fn table_tuple_update(
    rel: Relation,
    otid: ItemPointer,
    slot: *mut TupleTableSlot,
    cid: CommandId,
    snapshot: Snapshot,
    crosscheck: Snapshot,
    wait: bool,
    tmfd: *mut TM_FailureData,
    lockmode: *mut LockTupleMode,
    update_indexes: *mut TU_UpdateIndexes,
) -> TM_Result {
    ((*((*rel).rd_tableam as *const TableAmRoutine)).tuple_update.unwrap())(
        rel, otid, slot, cid, snapshot, crosscheck, wait, tmfd, lockmode, update_indexes,
    )
}


/* ----------------------------------------------------------------------------
 * Local stubs for unported dependencies.
 * ----------------------------------------------------------------------------
 */

unsafe fn rint(_x: f64) -> f64 {
    unimplemented!() // TODO: math.h
}

/* add_size - Size addition with overflow check (shmem.h) */
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2
}

/* Opaque/unported types referenced by TableAmRoutine fields */
pub type BulkInsertStateData = c_void;
pub type VacuumParams = c_void;
pub type ReadStream = c_void;
pub type ValidateIndexState = c_void;

/* TODO: globals from xact.c */
static mut CheckXidAlive: TransactionId = 0;
static mut bsysscan: bool = false;

/* TODO: storage/itemptr.h accessors */
unsafe fn ItemPointerGetBlockNumberNoCheck(_pointer: ItemPointer) -> BlockNumber {
    unimplemented!()
}
unsafe fn ItemPointerGetOffsetNumberNoCheck(_pointer: ItemPointer) -> OffsetNumber {
    unimplemented!()
}

/* TODO: executor/tuptable.h, access/htup */
unsafe fn MakeSingleTupleTableSlot(
    _tupdesc: *mut c_void,
    _tts_ops: *const TupleTableSlotOps,
) -> *mut TupleTableSlot {
    unimplemented!()
}
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {
    unimplemented!()
}

/* TODO: utils/rel.h accessors */
unsafe fn RelationGetRelid(_relation: Relation) -> Oid {
    unimplemented!()
}
unsafe fn RelationGetDescr(_relation: Relation) -> *mut c_void {
    unimplemented!()
}
unsafe fn RelationGetRelationName(_relation: Relation) -> *const c_char {
    unimplemented!()
}
unsafe fn RelationGetNumberOfBlocks(_relation: Relation) -> BlockNumber {
    unimplemented!()
}
unsafe fn RelationGetSmgr(_relation: Relation) -> *mut c_void {
    unimplemented!()
}
unsafe fn RelationUsesLocalBuffers(_relation: Relation) -> bool {
    unimplemented!()
}
unsafe fn RelationGetFillFactor(_relation: Relation, _defaultff: c_int) -> c_int {
    unimplemented!()
}

/* TODO: utils/snapshot.h, utils/snapmgr.h */
pub const SnapshotAny: Snapshot = std::ptr::null_mut();
unsafe fn RegisterSnapshot(snapshot: Snapshot) -> Snapshot {
    snapshot
}
unsafe fn GetCatalogSnapshot(_relid: Oid) -> Snapshot {
    unimplemented!()
}
unsafe fn IsMVCCSnapshot(_snapshot: Snapshot) -> bool {
    unimplemented!()
}
unsafe fn EstimateSnapshotSpace(_snapshot: Snapshot) -> Size {
    unimplemented!()
}
unsafe fn SerializeSnapshot(_snapshot: Snapshot, _start_address: *mut c_char) {
    unimplemented!()
}
unsafe fn RestoreSnapshot(_start_address: *mut c_char) -> Snapshot {
    unimplemented!()
}

/* TODO: access/xact.h */
unsafe fn GetCurrentCommandId(_used: bool) -> CommandId {
    unimplemented!()
}

/* TODO: storage/smgr.h */
unsafe fn smgrnblocks(_reln: *mut c_void, _forknum: ForkNumber) -> BlockNumber {
    unimplemented!()
}

/* TODO: optimizer/plancat.c, optimizer/cost.h */
unsafe fn clamp_row_est(_nrows: f64) -> f64 {
    unimplemented!()
}
unsafe fn get_rel_data_width(_rel: Relation, _attr_widths: *mut int32) -> int32 {
    unimplemented!()
}

/* TODO: access/syncscan.h */
unsafe fn ss_get_location(_rel: Relation, _relnblocks: BlockNumber) -> BlockNumber {
    unimplemented!()
}
unsafe fn ss_report_location(_rel: Relation, _location: BlockNumber) {
    unimplemented!()
}

/* TODO: storage/spin.h */
unsafe fn SpinLockInit(_lock: *mut slock_t) {
    unimplemented!()
}
unsafe fn SpinLockAcquire(_lock: *mut slock_t) {
    unimplemented!()
}
unsafe fn SpinLockRelease(_lock: *mut slock_t) {
    unimplemented!()
}

/* TODO: port/atomics.h */
unsafe fn pg_atomic_init_u64(_ptr: *mut pg_atomic_uint64, _val: uint64) {
    unimplemented!()
}
unsafe fn pg_atomic_write_u64(_ptr: *mut pg_atomic_uint64, _val: uint64) {
    unimplemented!()
}
unsafe fn pg_atomic_fetch_add_u64(_ptr: *mut pg_atomic_uint64, _add_: uint64) -> uint64 {
    unimplemented!()
}

/* HEAP_DEFAULT_FILLFACTOR - access/heaptoast.h / rel.h default */
const HEAP_DEFAULT_FILLFACTOR: c_int = 100;
