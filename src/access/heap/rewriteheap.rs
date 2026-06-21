//! rewriteheap.rs
//!   Support functions to rewrite tables.
//!
//! Translated 1:1 from postgres/src/backend/access/heap/rewriteheap.c
//!
//! These functions provide a facility to completely rewrite a heap, while
//! preserving visibility information and update chains.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994-5, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/access/heap/rewriteheap.c
//!
//! INTERFACE
//!
//! The caller is responsible for creating the new heap, all catalog
//! changes, supplying the tuples to be written to the new heap, and
//! rebuilding indexes.  The caller must hold AccessExclusiveLock on the
//! target table, because we assume no one else is writing into it.
//!
//! `#include`s mapped:
//!   - access/heapam.h          -> crate::access::heap::heapam
//!   - access/heapam_xlog.h     -> xl_heap_rewrite_mapping / XLOG_HEAP2_REWRITE (stubbed below; no home yet)
//!   - access/heaptoast.h       -> crate::access::heap::heaptoast (TOAST_TUPLE_THRESHOLD stubbed below)
//!   - access/rewriteheap.h     -> this file (LogicalRewriteMappingData stubbed below)
//!   - access/transam.h         -> crate::access::transam
//!   - access/xact.h            -> GetCurrentTransactionId (stubbed below)
//!   - access/xloginsert.h      -> XLogBeginInsert/XLogRegisterData/XLogInsert (stubbed below)
//!   - common/file_utils.h      -> get_dirent_type / fsync_fname (stubbed below)
//!   - lib/ilist.h              -> crate::lib::ilist
//!   - miscadmin.h              -> MyDatabaseId (stubbed below)
//!   - pgstat.h                 -> pgstat_report_wait_* (stubbed below)
//!   - replication/slot.h       -> ReplicationSlotsComputeLogicalRestartLSN (stubbed below)
//!   - storage/bufmgr.h         -> crate::storage::bufmgr (none directly used)
//!   - storage/bulk_write.h     -> BulkWriteState/BulkWriteBuffer/smgr_bulk_* (stubbed below; no home yet)
//!   - storage/fd.h             -> FileWrite/FileSync/FileClose/OpenTransientFile/... (stubbed below)
//!   - storage/procarray.h      -> ProcArrayGetReplicationSlotXmin (stubbed below)
//!   - utils/memutils.h         -> crate::utils::memutils
//!   - utils/rel.h              -> crate::utils::rel

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

// access/htup.h + access/htup_details.h (HeapTuple*/HeapTupleFields all live in htup_details)
use crate::access::htup_details::{
    HeapTuple, HeapTupleData, HeapTupleHeader, HeapTupleFields,
    HeapTupleHasExternal, HeapTupleHeaderGetUpdateXid, HeapTupleHeaderGetXmin,
    HeapTupleHeaderIndicatesMovedPartitions, HeapTupleHeaderIsOnlyLocked, HEAP2_XACT_MASK,
    HEAP_UPDATED, HEAP_XACT_MASK, HEAP_XMAX_INVALID, HEAP_XMAX_IS_LOCKED_ONLY,
};
use crate::access::common::heaptuple::{heap_copytuple, heap_freetuple};

// access/heapam.h
use crate::access::heap::heapam::heap_freeze_tuple;
// access/heaptoast.h
use crate::access::heap::heaptoast::heap_toast_insert_or_update;

// access/transam.h (helpers in transam mod + multixact)
use crate::access::transam::{
    TransactionIdEquals, TransactionIdIsNormal, InvalidTransactionId,
};
use crate::access::transam::transam::TransactionIdPrecedes;
// MultiXactId comes from crate::c via the prelude.
use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, XLogRecPtr};

// catalog/pg_class.h
use crate::catalog::pg_class::RELKIND_TOASTVALUE;

// common/relpath.h
use crate::common::relpath::MAIN_FORKNUM;

// lib/ilist.h
use crate::lib::ilist::{dclist_head, dclist_init, dlist_node};

// storage
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{
    Item, Page, PageAddItem, PageGetHeapFreeSpace, PageGetItem, PageGetItemId, PageInit,
};
use crate::storage::itemid::ItemId;
use crate::storage::itemptr::{
    ItemPointerData, ItemPointerEquals, ItemPointerIsValid, ItemPointerSet, ItemPointerSetInvalid,
};
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber};
use crate::storage::relfilelocator::RelFileLocator;

// utils
use crate::utils::rel::{
    Relation, RelationGetRelid,
};

use crate::c::{int16, int32, uint16, uint32, uint64, Size};
use crate::pg_config::BLCKSZ;

// hashtable
use crate::utils::hash::dynahash::{
    hash_create, hash_search, hash_seq_init, hash_seq_search, HASHACTION, HASHCTL,
    HASH_BLOBS, HASH_CONTEXT, HASH_ELEM, HASH_SEQ_STATUS, HTAB,
};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

// ----------------------------------------------------------------------------
// Local stubs for symbols that don't have a home yet in the port.
// ----------------------------------------------------------------------------

// TODO(pg-port): real off_t comes from libc; model as i64 here.
type off_t = i64;

// TODO(pg-port): real MAXPGPATH lives in pg_config_manual.h
const MAXPGPATH: usize = 1024;

// TODO(pg-port): real HASH_FIND/HASH_ENTER/HASH_REMOVE variants live in utils/hash/dynahash.rs (HASHACTION)
use crate::utils::hash::dynahash::HASHACTION::{HASH_ENTER, HASH_FIND, HASH_REMOVE};

// TODO(pg-port): real heaptoast TOAST_TUPLE_THRESHOLD lives in access/heaptoast.h
const TOAST_TUPLE_THRESHOLD: uint32 = (BLCKSZ as uint32) / 4;

// TODO(pg-port): real HEAP_INSERT_* flags live in access/heapam.rs
const HEAP_INSERT_SKIP_FSM: c_int = 0x0002;
const HEAP_INSERT_NO_LOGICAL: c_int = 0x0008;

// TODO(pg-port): real MaxHeapTupleSize lives in access/htup_details.rs (re-export via use; kept local stub-free)
use crate::access::htup_details::MaxHeapTupleSize;

// TODO(pg-port): real HEAP_DEFAULT_FILLFACTOR lives in utils/rel.h
const HEAP_DEFAULT_FILLFACTOR: c_int = 100;

// TODO(pg-port): real RelationGetTargetPageFreeSpace lives in utils/rel.h
unsafe fn RelationGetTargetPageFreeSpace(_relation: Relation, _defaultff: c_int) -> Size {
    unimplemented!()
}

// TODO(pg-port): real RelationGetNumberOfBlocks lives in utils/rel.h / storage/bufmgr
unsafe fn RelationGetNumberOfBlocks(_relation: Relation) -> BlockNumber { crate::access::nbtree::nbtpage::RelationGetNumberOfBlocks(_relation) }

// TODO(pg-port): real RelationIsAccessibleInLogicalDecoding lives in utils/rel.h
unsafe fn RelationIsAccessibleInLogicalDecoding(_relation: Relation) -> bool {
    unimplemented!()
}

// TODO(pg-port): real MyDatabaseId lives in miscadmin.h (global)
static mut MyDatabaseId: Oid = InvalidOid;

// TODO(pg-port): real GetCurrentTransactionId lives in access/xact.rs
unsafe fn GetCurrentTransactionId() -> TransactionId { crate::access::transam::xact::GetCurrentTransactionId() }

// TODO(pg-port): real GetXLogInsertRecPtr lives in access/transam/xlog.rs
unsafe fn GetXLogInsertRecPtr() -> XLogRecPtr { crate::access::transam::xlog::GetXLogInsertRecPtr() }
// TODO(pg-port): real GetRedoRecPtr lives in access/transam/xlog.rs
unsafe fn GetRedoRecPtr() -> XLogRecPtr {
    unimplemented!()
}

// TODO(pg-port): real ProcArrayGetReplicationSlotXmin lives in storage/ipc/procarray.rs
unsafe fn ProcArrayGetReplicationSlotXmin(
    _xmin: *mut TransactionId,
    catalog_xmin: *mut TransactionId,
) { crate::storage::ipc::procarray::ProcArrayGetReplicationSlotXmin(_xmin, catalog_xmin) }

// TODO(pg-port): real ReplicationSlotsComputeLogicalRestartLSN lives in replication/slot.rs
unsafe fn ReplicationSlotsComputeLogicalRestartLSN() -> XLogRecPtr { crate::replication::slot::ReplicationSlotsComputeLogicalRestartLSN() }

// TODO(pg-port): real bulk_write API (BulkWriteState/BulkWriteBuffer/smgr_bulk_*) lives in storage/bulk_write.rs
type BulkWriteState = c_void;
type BulkWriteBuffer = *mut c_void;
unsafe fn smgr_bulk_start_rel(_rel: Relation, _forknum: c_int) -> *mut BulkWriteState { crate::storage::smgr::bulk_write::smgr_bulk_start_rel(_rel, _forknum as _) }
unsafe fn smgr_bulk_get_buf(_bulkstate: *mut BulkWriteState) -> BulkWriteBuffer { crate::storage::smgr::bulk_write::smgr_bulk_get_buf(_bulkstate) }
unsafe fn smgr_bulk_write(
    _bulkstate: *mut BulkWriteState,
    _blocknum: BlockNumber,
    _buf: BulkWriteBuffer,
    _page_std: bool,
) { crate::storage::smgr::bulk_write::smgr_bulk_write(_bulkstate, _blocknum, _buf, _page_std) }
unsafe fn smgr_bulk_finish(_bulkstate: *mut BulkWriteState) { crate::storage::smgr::bulk_write::smgr_bulk_finish(_bulkstate) }

// TODO(pg-port): real File / fd API (File/FileWrite/FileSync/FileClose/PathNameOpenFile/
// OpenTransientFile/CloseTransientFile) lives in storage/file/fd.rs
type File = c_int;
unsafe fn FileWrite(
    _file: File,
    _buffer: *const c_void,
    _amount: c_int,
    _offset: off_t,
    _wait_event_info: uint32,
) -> c_int {
    unimplemented!()
}
unsafe fn FileSync(_file: File, _wait_event_info: uint32) -> c_int { crate::storage::file::fd::FileSync(_file, _wait_event_info as _) }
unsafe fn FileClose(_file: File) { crate::storage::file::fd::FileClose(_file) }
unsafe fn PathNameOpenFile(_path: *const c_char, _flags: c_int) -> File {
    unimplemented!()
}
unsafe fn OpenTransientFile(_path: *const c_char, _flags: c_int) -> c_int {
    unimplemented!()
}
unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    unimplemented!()
}

// TODO(pg-port): real O_* / PG_BINARY flags come from libc / port.h
const O_CREAT: c_int = 0o100;
const O_EXCL: c_int = 0o200;
const O_WRONLY: c_int = 0o1;
const O_RDWR: c_int = 0o2;
const PG_BINARY: c_int = 0;

// TODO(pg-port): real ftruncate/pg_pwrite/pg_fsync/unlink come from libc / port
unsafe fn ftruncate(_fd: c_int, _length: off_t) -> c_int {
    unimplemented!()
}
unsafe fn pg_pwrite(_fd: c_int, _buf: *const c_void, _count: usize, _offset: off_t) -> isize { crate::port::port_api::pg_pwrite(_fd, _buf, _count as _, _offset) }
unsafe fn pg_fsync(_fd: c_int) -> c_int {
    unimplemented!()
}
unsafe fn unlink(_path: *const c_char) -> c_int {
    unimplemented!()
}

// TODO(pg-port): real data_sync_elevel lives in storage/file/fd.rs
unsafe fn data_sync_elevel(elevel: c_int) -> c_int {
    elevel
}

// TODO(pg-port): real WAIT_EVENT_LOGICAL_REWRITE_* enum lives in utils/wait_event.rs / pgstat
const WAIT_EVENT_LOGICAL_REWRITE_WRITE: uint32 = 0;
const WAIT_EVENT_LOGICAL_REWRITE_SYNC: uint32 = 0;
const WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE: uint32 = 0;
const WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE: uint32 = 0;
const WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC: uint32 = 0;
const WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC: uint32 = 0;

// TODO(pg-port): real pgstat_report_wait_start/end live in utils/activity/pgstat.rs
unsafe fn pgstat_report_wait_start(_wait_event_info: uint32) {}
unsafe fn pgstat_report_wait_end() {}

// TODO(pg-port): real XLog insert API (XLogBeginInsert/XLogRegisterData/XLogInsert) lives in
// access/transam/xloginsert.rs
unsafe fn XLogBeginInsert() {
    unimplemented!()
}
unsafe fn XLogRegisterData(_data: *const c_void, _len: c_int) {
    unimplemented!()
}
unsafe fn XLogInsert(_rmid: uint8, _info: uint8) -> XLogRecPtr {
    unimplemented!()
}
// TODO(pg-port): real RM_HEAP2_ID / XLOG_HEAP2_REWRITE live in access/rmgrlist.rs / access/heapam_xlog.h
const RM_HEAP2_ID: uint8 = 11;
const XLOG_HEAP2_REWRITE: uint8 = 0x00;

// TODO(pg-port): real xl_heap_rewrite_mapping lives in access/heapam_xlog.h
#[repr(C)]
pub struct xl_heap_rewrite_mapping {
    pub mapped_xid: TransactionId, /* xid that might need to see the row */
    pub mapped_db: Oid,            /* DbOid or InvalidOid for shared rels */
    pub mapped_rel: Oid,           /* Oid of the mapped relation */
    pub offset: off_t,             /* How far have we written so far */
    pub num_mappings: uint32,      /* Number of in-memory mappings */
    pub start_lsn: XLogRecPtr,     /* Insert LSN at begin of rewrite */
}

// TODO(pg-port): real LogicalRewriteMappingData lives in access/rewriteheap.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LogicalRewriteMappingData {
    pub old_locator: RelFileLocator,
    pub new_locator: RelFileLocator,
    pub old_tid: ItemPointerData,
    pub new_tid: ItemPointerData,
}

// TODO(pg-port): real XLogReaderState lives in access/transam/xlogreader.rs
type XLogReaderState = c_void;
unsafe fn XLogRecGetData(_r: *mut XLogReaderState) -> *mut c_char { crate::access::transam::xlogreader::XLogRecGetData(_r) }
unsafe fn XLogRecGetXid(_r: *mut XLogReaderState) -> TransactionId { crate::access::transam::xlogreader::XLogRecGetXid(_r) }

// TODO(pg-port): real dir scanning (DIR/dirent/AllocateDir/ReadDir/FreeDir/get_dirent_type/
// fsync_fname) lives in storage/file/fd.rs + common/file_utils.rs
type DIR = c_void;
#[repr(C)]
struct dirent {
    d_name: [c_char; 256],
}
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!()
}
unsafe fn ReadDir(_dir: *mut DIR, _dirname: *const c_char) -> *mut dirent {
    unimplemented!()
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int { crate::storage::file::fd::FreeDir(_dir) }
unsafe fn fsync_fname(_fname: *const c_char, _isdir: bool) {
    unimplemented!()
}
type PGFileType = c_int;
const PGFILETYPE_ERROR: PGFileType = 0;
const PGFILETYPE_REG: PGFileType = 2;
unsafe fn get_dirent_type(
    _path: *const c_char,
    _de: *const dirent,
    _look_through_symlinks: bool,
    _elevel: c_int,
) -> PGFileType {
    unimplemented!()
}

// TODO(pg-port): real PG_LOGICAL_MAPPINGS_DIR / LOGICAL_REWRITE_FORMAT live in replication/logical/...
const PG_LOGICAL_MAPPINGS_DIR: &[u8] = b"pg_logical/mappings\0";

// ----------------------------------------------------------------------------
// Public opaque RewriteState pointer (declared in access/rewriteheap.h).
// ----------------------------------------------------------------------------

pub type RewriteState = *mut RewriteStateData;

/*
 * State associated with a rewrite operation. This is opaque to the user
 * of the rewrite facility.
 */
#[repr(C)]
pub struct RewriteStateData {
    pub rs_old_rel: Relation,                 /* source heap */
    pub rs_new_rel: Relation,                 /* destination heap */
    pub rs_bulkstate: *mut BulkWriteState,    /* writer for the destination */
    pub rs_buffer: BulkWriteBuffer,           /* page currently being built */
    pub rs_blockno: BlockNumber,              /* block where page will go */
    pub rs_logical_rewrite: bool,             /* do we need to do logical rewriting */
    pub rs_oldest_xmin: TransactionId,        /* oldest xmin used by caller to determine
                                               * tuple visibility */
    pub rs_freeze_xid: TransactionId,         /* Xid that will be used as freeze cutoff
                                               * point */
    pub rs_logical_xmin: TransactionId,       /* Xid that will be used as cutoff point
                                               * for logical rewrites */
    pub rs_cutoff_multi: MultiXactId,         /* MultiXactId that will be used as cutoff
                                               * point for multixacts */
    pub rs_cxt: MemoryContext,                /* for hash tables and entries and tuples in
                                               * them */
    pub rs_begin_lsn: XLogRecPtr,             /* XLogInsertLsn when starting the rewrite */
    pub rs_unresolved_tups: *mut HTAB,        /* unmatched A tuples */
    pub rs_old_new_tid_map: *mut HTAB,        /* unmatched B tuples */
    pub rs_logical_mappings: *mut HTAB,       /* logical remapping files */
    pub rs_num_rewrite_mappings: uint32,      /* # in memory mappings */
}

/*
 * The lookup keys for the hash tables are tuple TID and xmin (we must check
 * both to avoid false matches from dead tuples).  Beware that there is
 * probably some padding space in this struct; it must be zeroed out for
 * correct hashtable operation.
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct TidHashKey {
    xmin: TransactionId,    /* tuple xmin */
    tid: ItemPointerData,   /* tuple location in old heap */
}

/*
 * Entry structures for the hash tables
 */
#[repr(C)]
struct UnresolvedTupData {
    key: TidHashKey,          /* expected xmin/old location of B tuple */
    old_tid: ItemPointerData, /* A's location in the old heap */
    tuple: HeapTuple,         /* A's tuple contents */
}

type UnresolvedTup = *mut UnresolvedTupData;

#[repr(C)]
struct OldToNewMappingData {
    key: TidHashKey,          /* actual xmin/old location of B tuple */
    new_tid: ItemPointerData, /* where we put it in the new heap */
}

type OldToNewMapping = *mut OldToNewMappingData;

/*
 * In-Memory data for an xid that might need logical remapping entries
 * to be logged.
 */
#[repr(C)]
struct RewriteMappingFile {
    xid: TransactionId,         /* xid that might need to see the row */
    vfd: c_int,                 /* fd of mappings file */
    off: off_t,                 /* how far have we written yet */
    mappings: dclist_head,      /* list of in-memory mappings */
    path: [c_char; MAXPGPATH],  /* path, for error messages */
}

/*
 * A single In-Memory logical rewrite mapping, hanging off
 * RewriteMappingFile->mappings.
 */
#[repr(C)]
struct RewriteMappingDataEntry {
    map: LogicalRewriteMappingData, /* map between old and new location of the
                                     * tuple */
    node: dlist_node,
}

/*
 * Begin a rewrite of a table
 *
 * old_heap		old, locked heap relation tuples will be read from
 * new_heap		new, locked heap relation to insert tuples to
 * oldest_xmin	xid used by the caller to determine which tuples are dead
 * freeze_xid	xid before which tuples will be frozen
 * cutoff_multi	multixact before which multis will be removed
 *
 * Returns an opaque RewriteState, allocated in current memory context,
 * to be used in subsequent calls to the other functions.
 */
pub unsafe fn begin_heap_rewrite(
    old_heap: Relation,
    new_heap: Relation,
    oldest_xmin: TransactionId,
    freeze_xid: TransactionId,
    cutoff_multi: MultiXactId,
) -> RewriteState {
    let state: RewriteState;
    let rw_cxt: MemoryContext;
    let old_cxt: MemoryContext;
    let mut hash_ctl: HASHCTL = std::mem::zeroed();

    /*
     * To ease cleanup, make a separate context that will contain the
     * RewriteState struct itself plus all subsidiary data.
     */
    rw_cxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"Table rewrite".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    old_cxt = MemoryContextSwitchTo(rw_cxt);

    /* Create and fill in the state struct */
    state = palloc0(std::mem::size_of::<RewriteStateData>()) as RewriteState;

    (*state).rs_old_rel = old_heap;
    (*state).rs_new_rel = new_heap;
    (*state).rs_buffer = std::ptr::null_mut();
    /* new_heap needn't be empty, just locked */
    (*state).rs_blockno = RelationGetNumberOfBlocks(new_heap);
    (*state).rs_oldest_xmin = oldest_xmin;
    (*state).rs_freeze_xid = freeze_xid;
    (*state).rs_cutoff_multi = cutoff_multi;
    (*state).rs_cxt = rw_cxt;
    (*state).rs_bulkstate = smgr_bulk_start_rel(new_heap, MAIN_FORKNUM);

    /* Initialize hash tables used to track update chains */
    hash_ctl.keysize = std::mem::size_of::<TidHashKey>();
    hash_ctl.entrysize = std::mem::size_of::<UnresolvedTupData>();
    hash_ctl.hcxt = (*state).rs_cxt;

    (*state).rs_unresolved_tups = hash_create(
        c"Rewrite / Unresolved ctids".as_ptr(),
        128, /* arbitrary initial size */
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    hash_ctl.entrysize = std::mem::size_of::<OldToNewMappingData>();

    (*state).rs_old_new_tid_map = hash_create(
        c"Rewrite / Old to new tid map".as_ptr(),
        128, /* arbitrary initial size */
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    MemoryContextSwitchTo(old_cxt);

    logical_begin_heap_rewrite(state);

    state
}

/*
 * End a rewrite.
 *
 * state and any other resources are freed.
 */
pub unsafe fn end_heap_rewrite(state: RewriteState) {
    let mut seq_status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut unresolved: UnresolvedTup;

    /*
     * Write any remaining tuples in the UnresolvedTups table. If we have any
     * left, they should in fact be dead, but let's err on the safe side.
     */
    hash_seq_init(&mut seq_status, (*state).rs_unresolved_tups);

    loop {
        unresolved = hash_seq_search(&mut seq_status) as UnresolvedTup;
        if unresolved.is_null() {
            break;
        }
        ItemPointerSetInvalid(&raw mut (*(*(*unresolved).tuple).t_data).t_ctid);
        raw_heap_insert(state, (*unresolved).tuple);
    }

    /* Write the last page, if any */
    if !(*state).rs_buffer.is_null() {
        smgr_bulk_write(
            (*state).rs_bulkstate,
            (*state).rs_blockno,
            (*state).rs_buffer,
            true,
        );
        (*state).rs_buffer = std::ptr::null_mut();
    }

    smgr_bulk_finish((*state).rs_bulkstate);

    logical_end_heap_rewrite(state);

    /* Deleting the context frees everything */
    MemoryContextDelete((*state).rs_cxt);
}

/*
 * Add a tuple to the new heap.
 *
 * Visibility information is copied from the original tuple, except that
 * we "freeze" very-old tuples.  Note that since we scribble on new_tuple,
 * it had better be temp storage not a pointer to the original tuple.
 *
 * state		opaque state as returned by begin_heap_rewrite
 * old_tuple	original tuple in the old heap
 * new_tuple	new, rewritten tuple to be inserted to new heap
 */
pub unsafe fn rewrite_heap_tuple(
    state: RewriteState,
    old_tuple: HeapTuple,
    mut new_tuple: HeapTuple,
) {
    let old_cxt: MemoryContext;
    let mut old_tid: ItemPointerData;
    let mut hashkey: TidHashKey = std::mem::zeroed();
    let mut found: bool = false;
    let mut free_new: bool;

    old_cxt = MemoryContextSwitchTo((*state).rs_cxt);

    /*
     * Copy the original tuple's visibility information into new_tuple.
     *
     * XXX we might later need to copy some t_infomask2 bits, too? Right now,
     * we intentionally clear the HOT status bits.
     */
    memcpy(
        &raw mut (*(*new_tuple).t_data).t_choice.t_heap as *mut c_void,
        &raw const (*(*old_tuple).t_data).t_choice.t_heap as *const c_void,
        std::mem::size_of::<HeapTupleFields>(),
    );

    (*(*new_tuple).t_data).t_infomask &= !HEAP_XACT_MASK;
    (*(*new_tuple).t_data).t_infomask2 &= !HEAP2_XACT_MASK;
    (*(*new_tuple).t_data).t_infomask |= (*(*old_tuple).t_data).t_infomask & HEAP_XACT_MASK;

    /*
     * While we have our hands on the tuple, we may as well freeze any
     * eligible xmin or xmax, so that future VACUUM effort can be saved.
     */
    heap_freeze_tuple(
        (*new_tuple).t_data,
        (*(*(*state).rs_old_rel).rd_rel).relfrozenxid,
        (*(*(*state).rs_old_rel).rd_rel).relminmxid,
        (*state).rs_freeze_xid,
        (*state).rs_cutoff_multi,
    );

    /*
     * Invalid ctid means that ctid should point to the tuple itself. We'll
     * override it later if the tuple is part of an update chain.
     */
    ItemPointerSetInvalid(&raw mut (*(*new_tuple).t_data).t_ctid);

    /*
     * If the tuple has been updated, check the old-to-new mapping hash table.
     */
    if !(((*(*old_tuple).t_data).t_infomask & HEAP_XMAX_INVALID) != 0
        || HeapTupleHeaderIsOnlyLocked((*old_tuple).t_data))
        && !HeapTupleHeaderIndicatesMovedPartitions((*old_tuple).t_data)
        && !(ItemPointerEquals(
            &raw mut (*old_tuple).t_self,
            &raw mut (*(*old_tuple).t_data).t_ctid,
        ))
    {
        let mapping: OldToNewMapping;

        memset(
            &raw mut hashkey as *mut c_void,
            0,
            std::mem::size_of::<TidHashKey>(),
        );
        hashkey.xmin = HeapTupleHeaderGetUpdateXid((*old_tuple).t_data);
        hashkey.tid = (*(*old_tuple).t_data).t_ctid;

        mapping = hash_search(
            (*state).rs_old_new_tid_map,
            &raw const hashkey as *const c_void,
            HASH_FIND,
            std::ptr::null_mut(),
        ) as OldToNewMapping;

        if !mapping.is_null() {
            /*
             * We've already copied the tuple that t_ctid points to, so we can
             * set the ctid of this tuple to point to the new location, and
             * insert it right away.
             */
            (*(*new_tuple).t_data).t_ctid = (*mapping).new_tid;

            /* We don't need the mapping entry anymore */
            hash_search(
                (*state).rs_old_new_tid_map,
                &raw const hashkey as *const c_void,
                HASH_REMOVE,
                &raw mut found,
            );
            Assert!(found);
        } else {
            /*
             * We haven't seen the tuple t_ctid points to yet. Stash this
             * tuple into unresolved_tups to be written later.
             */
            let unresolved: UnresolvedTup;

            unresolved = hash_search(
                (*state).rs_unresolved_tups,
                &raw const hashkey as *const c_void,
                HASH_ENTER,
                &raw mut found,
            ) as UnresolvedTup;
            Assert!(!found);

            (*unresolved).old_tid = (*old_tuple).t_self;
            (*unresolved).tuple = heap_copytuple(new_tuple);

            /*
             * We can't do anything more now, since we don't know where the
             * tuple will be written.
             */
            MemoryContextSwitchTo(old_cxt);
            return;
        }
    }

    /*
     * Now we will write the tuple, and then check to see if it is the B tuple
     * in any new or known pair.  When we resolve a known pair, we will be
     * able to write that pair's A tuple, and then we have to check if it
     * resolves some other pair.  Hence, we need a loop here.
     */
    old_tid = (*old_tuple).t_self;
    free_new = false;

    loop {
        let new_tid: ItemPointerData;

        /* Insert the tuple and find out where it's put in new_heap */
        raw_heap_insert(state, new_tuple);
        new_tid = (*new_tuple).t_self;

        logical_rewrite_heap_tuple(state, old_tid, new_tuple);

        /*
         * If the tuple is the updated version of a row, and the prior version
         * wouldn't be DEAD yet, then we need to either resolve the prior
         * version (if it's waiting in rs_unresolved_tups), or make an entry
         * in rs_old_new_tid_map (so we can resolve it when we do see it). The
         * previous tuple's xmax would equal this one's xmin, so it's
         * RECENTLY_DEAD if and only if the xmin is not before OldestXmin.
         */
        if ((*(*new_tuple).t_data).t_infomask & HEAP_UPDATED) != 0
            && !TransactionIdPrecedes(
                HeapTupleHeaderGetXmin((*new_tuple).t_data),
                (*state).rs_oldest_xmin,
            )
        {
            /*
             * Okay, this is B in an update pair.  See if we've seen A.
             */
            let unresolved: UnresolvedTup;

            memset(
                &raw mut hashkey as *mut c_void,
                0,
                std::mem::size_of::<TidHashKey>(),
            );
            hashkey.xmin = HeapTupleHeaderGetXmin((*new_tuple).t_data);
            hashkey.tid = old_tid;

            unresolved = hash_search(
                (*state).rs_unresolved_tups,
                &raw const hashkey as *const c_void,
                HASH_FIND,
                std::ptr::null_mut(),
            ) as UnresolvedTup;

            if !unresolved.is_null() {
                /*
                 * We have seen and memorized the previous tuple already. Now
                 * that we know where we inserted the tuple its t_ctid points
                 * to, fix its t_ctid and insert it to the new heap.
                 */
                if free_new {
                    heap_freetuple(new_tuple);
                }
                new_tuple = (*unresolved).tuple;
                free_new = true;
                old_tid = (*unresolved).old_tid;
                (*(*new_tuple).t_data).t_ctid = new_tid;

                /*
                 * We don't need the hash entry anymore, but don't free its
                 * tuple just yet.
                 */
                hash_search(
                    (*state).rs_unresolved_tups,
                    &raw const hashkey as *const c_void,
                    HASH_REMOVE,
                    &raw mut found,
                );
                Assert!(found);

                /* loop back to insert the previous tuple in the chain */
                continue;
            } else {
                /*
                 * Remember the new tid of this tuple. We'll use it to set the
                 * ctid when we find the previous tuple in the chain.
                 */
                let mapping: OldToNewMapping;

                mapping = hash_search(
                    (*state).rs_old_new_tid_map,
                    &raw const hashkey as *const c_void,
                    HASH_ENTER,
                    &raw mut found,
                ) as OldToNewMapping;
                Assert!(!found);

                (*mapping).new_tid = new_tid;
            }
        }

        /* Done with this (chain of) tuples, for now */
        if free_new {
            heap_freetuple(new_tuple);
        }
        break;
    }

    MemoryContextSwitchTo(old_cxt);
}

/*
 * Register a dead tuple with an ongoing rewrite. Dead tuples are not
 * copied to the new table, but we still make note of them so that we
 * can release some resources earlier.
 *
 * Returns true if a tuple was removed from the unresolved_tups table.
 * This indicates that that tuple, previously thought to be "recently dead",
 * is now known really dead and won't be written to the output.
 */
pub unsafe fn rewrite_heap_dead_tuple(state: RewriteState, old_tuple: HeapTuple) -> bool {
    /*
     * If we have already seen an earlier tuple in the update chain that
     * points to this tuple, let's forget about that earlier tuple. It's in
     * fact dead as well, our simple xmax < OldestXmin test in
     * HeapTupleSatisfiesVacuum just wasn't enough to detect it. It happens
     * when xmin of a tuple is greater than xmax, which sounds
     * counter-intuitive but is perfectly valid.
     *
     * We don't bother to try to detect the situation the other way round,
     * when we encounter the dead tuple first and then the recently dead one
     * that points to it. If that happens, we'll have some unmatched entries
     * in the UnresolvedTups hash table at the end. That can happen anyway,
     * because a vacuum might have removed the dead tuple in the chain before
     * us.
     */
    let unresolved: UnresolvedTup;
    let mut hashkey: TidHashKey = std::mem::zeroed();
    let mut found: bool = false;

    memset(
        &raw mut hashkey as *mut c_void,
        0,
        std::mem::size_of::<TidHashKey>(),
    );
    hashkey.xmin = HeapTupleHeaderGetXmin((*old_tuple).t_data);
    hashkey.tid = (*old_tuple).t_self;

    unresolved = hash_search(
        (*state).rs_unresolved_tups,
        &raw const hashkey as *const c_void,
        HASH_FIND,
        std::ptr::null_mut(),
    ) as UnresolvedTup;

    if !unresolved.is_null() {
        /* Need to free the contained tuple as well as the hashtable entry */
        heap_freetuple((*unresolved).tuple);
        hash_search(
            (*state).rs_unresolved_tups,
            &raw const hashkey as *const c_void,
            HASH_REMOVE,
            &raw mut found,
        );
        Assert!(found);
        return true;
    }

    false
}

/*
 * Insert a tuple to the new relation.  This has to track heap_insert
 * and its subsidiary functions!
 *
 * t_self of the tuple is set to the new TID of the tuple. If t_ctid of the
 * tuple is invalid on entry, it's replaced with the new TID as well (in
 * the inserted data only, not in the caller's copy).
 */
unsafe fn raw_heap_insert(state: RewriteState, tup: HeapTuple) {
    let mut page: Page;
    let pageFreeSpace: Size;
    let saveFreeSpace: Size;
    let len: Size;
    let newoff: OffsetNumber;
    let heaptup: HeapTuple;

    /*
     * If the new tuple is too big for storage or contains already toasted
     * out-of-line attributes from some other relation, invoke the toaster.
     *
     * Note: below this point, heaptup is the data we actually intend to store
     * into the relation; tup is the caller's original untoasted data.
     */
    if (*(*(*state).rs_new_rel).rd_rel).relkind == RELKIND_TOASTVALUE {
        /* toast table entries should never be recursively toasted */
        Assert!(!HeapTupleHasExternal(tup));
        heaptup = tup;
    } else if HeapTupleHasExternal(tup) || (*tup).t_len > TOAST_TUPLE_THRESHOLD {
        let mut options: c_int = HEAP_INSERT_SKIP_FSM;

        /*
         * While rewriting the heap for VACUUM FULL / CLUSTER, make sure data
         * for the TOAST table are not logically decoded.  The main heap is
         * WAL-logged as XLOG FPI records, which are not logically decoded.
         */
        options |= HEAP_INSERT_NO_LOGICAL;

        heaptup = heap_toast_insert_or_update(
            (*state).rs_new_rel,
            tup,
            std::ptr::null_mut(),
            options,
        );
    } else {
        heaptup = tup;
    }

    len = MAXALIGN((*heaptup).t_len as Size); /* be conservative */

    /*
     * If we're gonna fail for oversize tuple, do it right away
     */
    if len > MaxHeapTupleSize {
        ereport!(
            ERROR,
            errmsg!(
                "row is too big: size {}, maximum size {}",
                len,
                MaxHeapTupleSize
            )
        );
    }

    /* Compute desired extra freespace due to fillfactor option */
    saveFreeSpace =
        RelationGetTargetPageFreeSpace((*state).rs_new_rel, HEAP_DEFAULT_FILLFACTOR);

    /* Now we can check to see if there's enough free space already. */
    page = (*state).rs_buffer as Page;
    if !page.is_null() {
        pageFreeSpace = PageGetHeapFreeSpace(page);

        if len + saveFreeSpace > pageFreeSpace {
            /*
             * Doesn't fit, so write out the existing page.  It always
             * contains a tuple.  Hence, unlike RelationGetBufferForTuple(),
             * enforce saveFreeSpace unconditionally.
             */
            smgr_bulk_write(
                (*state).rs_bulkstate,
                (*state).rs_blockno,
                (*state).rs_buffer,
                true,
            );
            (*state).rs_buffer = std::ptr::null_mut();
            page = std::ptr::null_mut();
            (*state).rs_blockno += 1;
        }
    }

    if page.is_null() {
        /* Initialize a new empty page */
        (*state).rs_buffer = smgr_bulk_get_buf((*state).rs_bulkstate);
        page = (*state).rs_buffer as Page;
        PageInit(page, BLCKSZ, 0);
    }

    /* And now we can insert the tuple into the page */
    newoff = PageAddItem(
        page,
        (*heaptup).t_data as Item,
        (*heaptup).t_len as Size,
        InvalidOffsetNumber,
        false,
        true,
    );
    if newoff == InvalidOffsetNumber {
        elog!(ERROR, "failed to add tuple");
    }

    /* Update caller's t_self to the actual position where it was stored */
    ItemPointerSet(&raw mut (*tup).t_self, (*state).rs_blockno, newoff);

    /*
     * Insert the correct position into CTID of the stored tuple, too, if the
     * caller didn't supply a valid CTID.
     */
    if !ItemPointerIsValid(&raw const (*(*tup).t_data).t_ctid) {
        let newitemid: ItemId;
        let onpage_tup: HeapTupleHeader;

        newitemid = PageGetItemId(page, newoff);
        onpage_tup = PageGetItem(page, newitemid) as HeapTupleHeader;

        (*onpage_tup).t_ctid = (*tup).t_self;
    }

    /* If heaptup is a private copy, release it. */
    if heaptup != tup {
        heap_freetuple(heaptup);
    }
}

/* ------------------------------------------------------------------------
 * Logical rewrite support
 *
 * When doing logical decoding - which relies on using cmin/cmax of catalog
 * tuples, via xl_heap_new_cid records - heap rewrites have to log enough
 * information to allow the decoding backend to update its internal mapping
 * of (relfilelocator,ctid) => (cmin, cmax) to be correct for the rewritten heap.
 *
 * For that, every time we find a tuple that's been modified in a catalog
 * relation within the xmin horizon of any decoding slot, we log a mapping
 * from the old to the new location.
 *
 * To deal with rewrites that abort the filename of a mapping file contains
 * the xid of the transaction performing the rewrite, which then can be
 * checked before being read in.
 *
 * For efficiency we don't immediately spill every single map mapping for a
 * row to disk but only do so in batches when we've collected several of them
 * in memory or when end_heap_rewrite() has been called.
 *
 * Crash-Safety: This module diverts from the usual patterns of doing WAL
 * since it cannot rely on checkpoint flushing out all buffers and thus
 * waiting for exclusive locks on buffers. Usually the XLogInsert() covering
 * buffer modifications is performed while the buffer(s) that are being
 * modified are exclusively locked guaranteeing that both the WAL record and
 * the modified heap are on either side of the checkpoint. But since the
 * mapping files we log aren't in shared_buffers that interlock doesn't work.
 *
 * Instead we simply write the mapping files out to disk, *before* the
 * XLogInsert() is performed. That guarantees that either the XLogInsert() is
 * inserted after the checkpoint's redo pointer or that the checkpoint (via
 * CheckPointLogicalRewriteHeap()) has flushed the (partial) mapping file to
 * disk. That leaves the tail end that has not yet been flushed open to
 * corruption, which is solved by including the current offset in the
 * xl_heap_rewrite_mapping records and truncating the mapping file to it
 * during replay. Every time a rewrite is finished all generated mapping files
 * are synced to disk.
 *
 * Note that if we were only concerned about crash safety we wouldn't have to
 * deal with WAL logging at all - an fsync() at the end of a rewrite would be
 * sufficient for crash safety. Any mapping that hasn't been safely flushed to
 * disk has to be by an aborted (explicitly or via a crash) transaction and is
 * ignored by virtue of the xid in its name being subject to a
 * TransactionDidCommit() check. But we want to support having standbys via
 * physical replication, both for availability and to do logical decoding
 * there.
 * ------------------------------------------------------------------------
 */

/*
 * Do preparations for logging logical mappings during a rewrite if
 * necessary. If we detect that we don't need to log anything we'll prevent
 * any further action by the various logical rewrite functions.
 */
unsafe fn logical_begin_heap_rewrite(state: RewriteState) {
    let mut hash_ctl: HASHCTL = std::mem::zeroed();
    let mut logical_xmin: TransactionId = InvalidTransactionId;

    /*
     * We only need to persist these mappings if the rewritten table can be
     * accessed during logical decoding, if not, we can skip doing any
     * additional work.
     */
    (*state).rs_logical_rewrite = RelationIsAccessibleInLogicalDecoding((*state).rs_old_rel);

    if !(*state).rs_logical_rewrite {
        return;
    }

    ProcArrayGetReplicationSlotXmin(std::ptr::null_mut(), &raw mut logical_xmin);

    /*
     * If there are no logical slots in progress we don't need to do anything,
     * there cannot be any remappings for relevant rows yet. The relation's
     * lock protects us against races.
     */
    if logical_xmin == InvalidTransactionId {
        (*state).rs_logical_rewrite = false;
        return;
    }

    (*state).rs_logical_xmin = logical_xmin;
    (*state).rs_begin_lsn = GetXLogInsertRecPtr();
    (*state).rs_num_rewrite_mappings = 0;

    hash_ctl.keysize = std::mem::size_of::<TransactionId>();
    hash_ctl.entrysize = std::mem::size_of::<RewriteMappingFile>();
    hash_ctl.hcxt = (*state).rs_cxt;

    (*state).rs_logical_mappings = hash_create(
        c"Logical rewrite mapping".as_ptr(),
        128, /* arbitrary initial size */
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );
}

/*
 * Flush all logical in-memory mappings to disk, but don't fsync them yet.
 */
unsafe fn logical_heap_rewrite_flush_mappings(state: RewriteState) {
    let mut seq_status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut src: *mut RewriteMappingFile;

    Assert!((*state).rs_logical_rewrite);

    /* no logical rewrite in progress, no need to iterate over mappings */
    if (*state).rs_num_rewrite_mappings == 0 {
        return;
    }

    elog!(
        DEBUG1,
        "flushing {} logical rewrite mapping entries",
        (*state).rs_num_rewrite_mappings
    );

    hash_seq_init(&mut seq_status, (*state).rs_logical_mappings);
    loop {
        src = hash_seq_search(&mut seq_status) as *mut RewriteMappingFile;
        if src.is_null() {
            break;
        }

        let mut waldata: *mut c_char;
        let waldata_start: *mut c_char;
        let mut xlrec: xl_heap_rewrite_mapping = std::mem::zeroed();
        let dboid: Oid;
        let len: uint32;
        let written: c_int;
        let num_mappings: uint32 = dclist_count(&(*src).mappings);

        /* this file hasn't got any new mappings */
        if num_mappings == 0 {
            continue;
        }

        if (*(*(*state).rs_old_rel).rd_rel).relisshared {
            dboid = InvalidOid;
        } else {
            dboid = MyDatabaseId;
        }

        xlrec.num_mappings = num_mappings;
        xlrec.mapped_rel = RelationGetRelid((*state).rs_old_rel);
        xlrec.mapped_xid = (*src).xid;
        xlrec.mapped_db = dboid;
        xlrec.offset = (*src).off;
        xlrec.start_lsn = (*state).rs_begin_lsn;

        /* write all mappings consecutively */
        len = num_mappings * std::mem::size_of::<LogicalRewriteMappingData>() as uint32;
        waldata_start = palloc(len as Size) as *mut c_char;
        waldata = waldata_start;

        /*
         * collect data we need to write out, but don't modify ondisk data yet
         */
        dclist_foreach_modify!(iter, &raw mut (*src).mappings, {
            let pmap: *mut RewriteMappingDataEntry =
                dclist_container!(RewriteMappingDataEntry, node, iter.cur);

            memcpy(
                waldata as *mut c_void,
                &raw const (*pmap).map as *const c_void,
                std::mem::size_of::<LogicalRewriteMappingData>(),
            );
            waldata = waldata.add(std::mem::size_of::<LogicalRewriteMappingData>());

            /* remove from the list and free */
            dclist_delete_from(&raw mut (*src).mappings, &raw mut (*pmap).node);
            pfree(pmap as *mut c_void);

            /* update bookkeeping */
            (*state).rs_num_rewrite_mappings -= 1;
        });

        Assert!(dclist_count(&(*src).mappings) == 0);
        Assert!(waldata == waldata_start.add(len as usize));

        /*
         * Note that we deviate from the usual WAL coding practices here,
         * check the above "Logical rewrite support" comment for reasoning.
         */
        written = FileWrite(
            (*src).vfd,
            waldata_start as *const c_void,
            len as c_int,
            (*src).off,
            WAIT_EVENT_LOGICAL_REWRITE_WRITE,
        );
        if written != len as c_int {
            ereport!(
                ERROR,
                errmsg!(
                    "could not write to file, wrote {} of {}",
                    written,
                    len
                )
            );
        }
        (*src).off += len as off_t;

        XLogBeginInsert();
        XLogRegisterData(
            &raw const xlrec as *const c_void,
            std::mem::size_of::<xl_heap_rewrite_mapping>() as c_int,
        );
        XLogRegisterData(waldata_start as *const c_void, len as c_int);

        /* write xlog record */
        XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_REWRITE);

        pfree(waldata_start as *mut c_void);
    }
    Assert!((*state).rs_num_rewrite_mappings == 0);
}

/*
 * Logical remapping part of end_heap_rewrite().
 */
unsafe fn logical_end_heap_rewrite(state: RewriteState) {
    let mut seq_status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut src: *mut RewriteMappingFile;

    /* done, no logical rewrite in progress */
    if !(*state).rs_logical_rewrite {
        return;
    }

    /* writeout remaining in-memory entries */
    if (*state).rs_num_rewrite_mappings > 0 {
        logical_heap_rewrite_flush_mappings(state);
    }

    /* Iterate over all mappings we have written and fsync the files. */
    hash_seq_init(&mut seq_status, (*state).rs_logical_mappings);
    loop {
        src = hash_seq_search(&mut seq_status) as *mut RewriteMappingFile;
        if src.is_null() {
            break;
        }
        if FileSync((*src).vfd, WAIT_EVENT_LOGICAL_REWRITE_SYNC) != 0 {
            ereport!(
                data_sync_elevel(ERROR),
                errmsg!("could not fsync file")
            );
        }
        FileClose((*src).vfd);
    }
    /* memory context cleanup will deal with the rest */
}

/*
 * Log a single (old->new) mapping for 'xid'.
 */
unsafe fn logical_rewrite_log_mapping(
    state: RewriteState,
    xid: TransactionId,
    map: *mut LogicalRewriteMappingData,
) {
    let src: *mut RewriteMappingFile;
    let pmap: *mut RewriteMappingDataEntry;
    let relid: Oid;
    let mut found: bool = false;

    relid = RelationGetRelid((*state).rs_old_rel);

    /* look for existing mappings for this 'mapped' xid */
    src = hash_search(
        (*state).rs_logical_mappings,
        &raw const xid as *const c_void,
        HASH_ENTER,
        &raw mut found,
    ) as *mut RewriteMappingFile;

    /*
     * We haven't yet had the need to map anything for this xid, create
     * per-xid data structures.
     */
    if !found {
        let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
        let dboid: Oid;

        if (*(*(*state).rs_old_rel).rd_rel).relisshared {
            dboid = InvalidOid;
        } else {
            dboid = MyDatabaseId;
        }

        snprintf(
            path.as_mut_ptr(),
            MAXPGPATH,
            c"%s/map-%x-%x-%X_%X-%x-%x".as_ptr(),
            PG_LOGICAL_MAPPINGS_DIR.as_ptr(),
            dboid,
            relid,
            ((*state).rs_begin_lsn >> 32) as uint32,
            (*state).rs_begin_lsn as uint32,
            xid,
            GetCurrentTransactionId(),
        );

        dclist_init(&raw mut (*src).mappings);
        (*src).off = 0;
        memcpy(
            (*src).path.as_mut_ptr() as *mut c_void,
            path.as_ptr() as *const c_void,
            std::mem::size_of::<[c_char; MAXPGPATH]>(),
        );
        (*src).vfd = PathNameOpenFile(
            path.as_ptr(),
            O_CREAT | O_EXCL | O_WRONLY | PG_BINARY,
        );
        if (*src).vfd < 0 {
            ereport!(
                ERROR,
                errmsg!("could not create file")
            );
        }
    }

    pmap = MemoryContextAlloc(
        (*state).rs_cxt,
        std::mem::size_of::<RewriteMappingDataEntry>(),
    ) as *mut RewriteMappingDataEntry;
    memcpy(
        &raw mut (*pmap).map as *mut c_void,
        map as *const c_void,
        std::mem::size_of::<LogicalRewriteMappingData>(),
    );
    dclist_push_tail(&raw mut (*src).mappings, &raw mut (*pmap).node);
    (*state).rs_num_rewrite_mappings += 1;

    /*
     * Write out buffer every time we've too many in-memory entries across all
     * mapping files.
     */
    if (*state).rs_num_rewrite_mappings >= 1000
    /* arbitrary number */
    {
        logical_heap_rewrite_flush_mappings(state);
    }
}

/*
 * Perform logical remapping for a tuple that's mapped from old_tid to
 * new_tuple->t_self by rewrite_heap_tuple() if necessary for the tuple.
 */
unsafe fn logical_rewrite_heap_tuple(
    state: RewriteState,
    old_tid: ItemPointerData,
    new_tuple: HeapTuple,
) {
    let new_tid: ItemPointerData = (*new_tuple).t_self;
    let cutoff: TransactionId = (*state).rs_logical_xmin;
    let xmin: TransactionId;
    let xmax: TransactionId;
    let mut do_log_xmin: bool = false;
    let mut do_log_xmax: bool = false;
    let mut map: LogicalRewriteMappingData = std::mem::zeroed();

    /* no logical rewrite in progress, we don't need to log anything */
    if !(*state).rs_logical_rewrite {
        return;
    }

    xmin = HeapTupleHeaderGetXmin((*new_tuple).t_data);
    /* use *GetUpdateXid to correctly deal with multixacts */
    xmax = HeapTupleHeaderGetUpdateXid((*new_tuple).t_data);

    /*
     * Log the mapping iff the tuple has been created recently.
     */
    if TransactionIdIsNormal(xmin) && !TransactionIdPrecedes(xmin, cutoff) {
        do_log_xmin = true;
    }

    if !TransactionIdIsNormal(xmax) {
        /*
         * no xmax is set, can't have any permanent ones, so this check is
         * sufficient
         */
    } else if HEAP_XMAX_IS_LOCKED_ONLY((*(*new_tuple).t_data).t_infomask) {
        /* only locked, we don't care */
    } else if !TransactionIdPrecedes(xmax, cutoff) {
        /* tuple has been deleted recently, log */
        do_log_xmax = true;
    }

    /* if neither needs to be logged, we're done */
    if !do_log_xmin && !do_log_xmax {
        return;
    }

    /* fill out mapping information */
    map.old_locator = (*(*state).rs_old_rel).rd_locator;
    map.old_tid = old_tid;
    map.new_locator = (*(*state).rs_new_rel).rd_locator;
    map.new_tid = new_tid;

    /* ---
     * Now persist the mapping for the individual xids that are affected. We
     * need to log for both xmin and xmax if they aren't the same transaction
     * since the mapping files are per "affected" xid.
     * We don't muster all that much effort detecting whether xmin and xmax
     * are actually the same transaction, we just check whether the xid is the
     * same disregarding subtransactions. Logging too much is relatively
     * harmless and we could never do the check fully since subtransaction
     * data is thrown away during restarts.
     * ---
     */
    if do_log_xmin {
        logical_rewrite_log_mapping(state, xmin, &raw mut map);
    }
    /* separately log mapping for xmax unless it'd be redundant */
    if do_log_xmax && !TransactionIdEquals(xmin, xmax) {
        logical_rewrite_log_mapping(state, xmax, &raw mut map);
    }
}

/*
 * Replay XLOG_HEAP2_REWRITE records
 */
pub unsafe fn heap_xlog_logical_rewrite(r: *mut XLogReaderState) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let fd: c_int;
    let xlrec: *mut xl_heap_rewrite_mapping;
    let len: uint32;
    let data: *mut c_char;

    xlrec = XLogRecGetData(r) as *mut xl_heap_rewrite_mapping;

    snprintf(
        path.as_mut_ptr(),
        MAXPGPATH,
        c"%s/map-%x-%x-%X_%X-%x-%x".as_ptr(),
        PG_LOGICAL_MAPPINGS_DIR.as_ptr(),
        (*xlrec).mapped_db,
        (*xlrec).mapped_rel,
        ((*xlrec).start_lsn >> 32) as uint32,
        (*xlrec).start_lsn as uint32,
        (*xlrec).mapped_xid,
        XLogRecGetXid(r),
    );

    fd = OpenTransientFile(path.as_ptr(), O_CREAT | O_WRONLY | PG_BINARY);
    if fd < 0 {
        ereport!(
            ERROR,
            errmsg!("could not create file")
        );
    }

    /*
     * Truncate all data that's not guaranteed to have been safely fsynced (by
     * previous record or by the last checkpoint).
     */
    pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_TRUNCATE);
    if ftruncate(fd, (*xlrec).offset) != 0 {
        ereport!(
            ERROR,
            errmsg!(
                "could not truncate file to {}",
                (*xlrec).offset as uint32
            )
        );
    }
    pgstat_report_wait_end();

    let data = XLogRecGetData(r).add(std::mem::size_of::<xl_heap_rewrite_mapping>());

    len = (*xlrec).num_mappings * std::mem::size_of::<LogicalRewriteMappingData>() as uint32;

    /* write out tail end of mapping file (again) */
    set_errno(0);
    pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_MAPPING_WRITE);
    if pg_pwrite(fd, data as *const c_void, len as usize, (*xlrec).offset) != len as isize {
        /* if write didn't set errno, assume problem is no disk space */
        if get_errno() == 0 {
            set_errno(ENOSPC);
        }
        ereport!(
            ERROR,
            errmsg!("could not write to file")
        );
    }
    pgstat_report_wait_end();

    /*
     * Now fsync all previously written data. We could improve things and only
     * do this for the last write to a file, but the required bookkeeping
     * doesn't seem worth the trouble.
     */
    pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_MAPPING_SYNC);
    if pg_fsync(fd) != 0 {
        ereport!(
            data_sync_elevel(ERROR),
            errmsg!("could not fsync file")
        );
    }
    pgstat_report_wait_end();

    if CloseTransientFile(fd) != 0 {
        ereport!(
            ERROR,
            errmsg!("could not close file")
        );
    }
}

// TODO(pg-port): real errno access lives in libc; stub get/set + ENOSPC here.
const ENOSPC: c_int = 28;
unsafe fn get_errno() -> c_int {
    *__errno_location()
}
unsafe fn set_errno(e: c_int) {
    *__errno_location() = e;
}
extern "C" {
    #[cfg_attr(
        any(target_os = "macos", target_os = "ios"),
        link_name = "__error"
    )]
    #[cfg_attr(target_os = "linux", link_name = "__errno_location")]
    fn __errno_location() -> *mut c_int;
}

/* ---
 * Perform a checkpoint for logical rewrite mappings
 *
 * This serves two tasks:
 * 1) Remove all mappings not needed anymore based on the logical restart LSN
 * 2) Flush all remaining mappings to disk, so that replay after a checkpoint
 *	  only has to deal with the parts of a mapping that have been written out
 *	  after the checkpoint started.
 * ---
 */
pub unsafe fn CheckPointLogicalRewriteHeap() {
    let mut cutoff: XLogRecPtr;
    let redo: XLogRecPtr;
    let mappings_dir: *mut DIR;
    let mut mapping_de: *mut dirent;
    let mut path: [c_char; MAXPGPATH + 19] = [0; MAXPGPATH + 19];

    /*
     * We start of with a minimum of the last redo pointer. No new decoding
     * slot will start before that, so that's a safe upper bound for removal.
     */
    redo = GetRedoRecPtr();

    /* now check for the restart ptrs from existing slots */
    cutoff = ReplicationSlotsComputeLogicalRestartLSN();

    /* don't start earlier than the restart lsn */
    if cutoff != InvalidXLogRecPtr && redo < cutoff {
        cutoff = redo;
    }

    mappings_dir = AllocateDir(PG_LOGICAL_MAPPINGS_DIR.as_ptr() as *const c_char);
    loop {
        mapping_de = ReadDir(mappings_dir, PG_LOGICAL_MAPPINGS_DIR.as_ptr() as *const c_char);
        if mapping_de.is_null() {
            break;
        }

        let mut dboid: Oid = 0;
        let mut relid: Oid = 0;
        let lsn: XLogRecPtr;
        let mut rewrite_xid: TransactionId = 0;
        let mut create_xid: TransactionId = 0;
        let mut hi: uint32 = 0;
        let mut lo: uint32 = 0;
        let de_type: PGFileType;

        if libc_strcmp((*mapping_de).d_name.as_ptr(), c".".as_ptr()) == 0
            || libc_strcmp((*mapping_de).d_name.as_ptr(), c"..".as_ptr()) == 0
        {
            continue;
        }

        snprintf(
            path.as_mut_ptr(),
            std::mem::size_of::<[c_char; MAXPGPATH + 19]>(),
            c"%s/%s".as_ptr(),
            PG_LOGICAL_MAPPINGS_DIR.as_ptr(),
            (*mapping_de).d_name.as_ptr(),
        );
        de_type = get_dirent_type(path.as_ptr(), mapping_de, false, DEBUG1);

        if de_type != PGFILETYPE_ERROR && de_type != PGFILETYPE_REG {
            continue;
        }

        /* Skip over files that cannot be ours. */
        if libc_strncmp((*mapping_de).d_name.as_ptr(), c"map-".as_ptr(), 4) != 0 {
            continue;
        }

        if libc_sscanf(
            (*mapping_de).d_name.as_ptr(),
            c"map-%x-%x-%X_%X-%x-%x".as_ptr(),
            &raw mut dboid,
            &raw mut relid,
            &raw mut hi,
            &raw mut lo,
            &raw mut rewrite_xid,
            &raw mut create_xid,
        ) != 6
        {
            elog!(ERROR, "could not parse filename");
        }

        lsn = ((hi as uint64) << 32) | lo as uint64;

        if lsn < cutoff || cutoff == InvalidXLogRecPtr {
            elog!(DEBUG1, "removing logical rewrite file");
            if unlink(path.as_ptr()) < 0 {
                ereport!(
                    ERROR,
                    errmsg!("could not remove file")
                );
            }
        } else {
            /* on some operating systems fsyncing a file requires O_RDWR */
            let fd: c_int = OpenTransientFile(path.as_ptr(), O_RDWR | PG_BINARY);

            /*
             * The file cannot vanish due to concurrency since this function
             * is the only one removing logical mappings and only one
             * checkpoint can be in progress at a time.
             */
            if fd < 0 {
                ereport!(
                    ERROR,
                    errmsg!("could not open file")
                );
            }

            /*
             * We could try to avoid fsyncing files that either haven't
             * changed or have only been created since the checkpoint's start,
             * but it's currently not deemed worth the effort.
             */
            pgstat_report_wait_start(WAIT_EVENT_LOGICAL_REWRITE_CHECKPOINT_SYNC);
            if pg_fsync(fd) != 0 {
                ereport!(
                    data_sync_elevel(ERROR),
                    errmsg!("could not fsync file")
                );
            }
            pgstat_report_wait_end();

            if CloseTransientFile(fd) != 0 {
                ereport!(
                    ERROR,
                    errmsg!("could not close file")
                );
            }
        }
    }
    FreeDir(mappings_dir);

    /* persist directory entries to disk */
    fsync_fname(PG_LOGICAL_MAPPINGS_DIR.as_ptr() as *const c_char, true);
}

// dclist runtime helpers used above (declared in crate::lib::ilist).
use crate::lib::ilist::{dclist_count, dclist_delete_from, dclist_push_tail};
use crate::{dclist_container, dclist_foreach_modify};

// TODO(pg-port): real strcmp/strncmp/sscanf come from libc.
extern "C" {
    #[link_name = "strcmp"]
    fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int;
    #[link_name = "strncmp"]
    fn libc_strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    #[link_name = "sscanf"]
    fn libc_sscanf(s: *const c_char, fmt: *const c_char, ...) -> c_int;
}
