/*-------------------------------------------------------------------------
 *
 * predicate.rs
 *   POSTGRES predicate locking
 *   to support full serializable transaction isolation
 *
 * The approach taken is to implement Serializable Snapshot Isolation (SSI)
 * as initially described in this paper:
 *
 *   Michael J. Cahill, Uwe Rohm, and Alan D. Fekete. 2008.
 *   Serializable isolation for snapshot databases.
 *   In SIGMOD '08: Proceedings of the 2008 ACM SIGMOD
 *   international conference on Management of data,
 *   pages 729-738, New York, NY, USA. ACM.
 *   http://doi.acm.org/10.1145/1376616.1376690
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/backend/storage/lmgr/predicate.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use crate::prelude::*;
use core::mem::size_of;
use std::ffi::c_int;
use std::ptr;

// lib/ilist.h
type dlist_head = crate::lib::ilist::dlist_head;
type dlist_node = crate::lib::ilist::dlist_node;
type dlist_iter = crate::lib::ilist::dlist_iter;
type dlist_mutable_iter = crate::lib::ilist::dlist_mutable_iter;
use crate::lib::ilist::{
    dlist_delete, dlist_init, dlist_is_empty, dlist_node_init, dlist_pop_head_node,
    dlist_push_tail,
};
use crate::{dlist_container, dlist_foreach, dlist_foreach_modify, dlist_head_element};

// storage/predicate_internals.h
use crate::storage::predicate_internals::{
    FirstNormalSerCommitSeqNo, InvalidSerCommitSeqNo, LOCALPREDICATELOCK, PREDICATELOCK,
    PREDICATELOCKTAG, PREDICATELOCKTARGET, PREDICATELOCKTARGETTAG, PREDLOCKTAG_PAGE,
    PREDLOCKTAG_RELATION, PREDLOCKTAG_TUPLE, PredXactListData, PredXactListDataSize,
    RWConflictData, RWConflictPoolHeaderData, RWConflictPoolHeaderDataSize, RecoverySerCommitSeqNo,
    SERIALIZABLEXACT, SERIALIZABLEXID, SERIALIZABLEXIDTAG, SerCommitSeqNo,
    SXACT_FLAG_COMMITTED, SXACT_FLAG_CONFLICT_OUT, SXACT_FLAG_DEFERRABLE_WAITING,
    SXACT_FLAG_DOOMED, SXACT_FLAG_PARTIALLY_RELEASED, SXACT_FLAG_PREPARED,
    SXACT_FLAG_READ_ONLY, SXACT_FLAG_RO_SAFE, SXACT_FLAG_RO_UNSAFE, SXACT_FLAG_ROLLED_BACK,
    SXACT_FLAG_SUMMARY_CONFLICT_IN, SXACT_FLAG_SUMMARY_CONFLICT_OUT,
    TwoPhasePredicateLockRecord, TwoPhasePredicateRecord, TwoPhasePredicateXactRecord,
    TWOPHASEPREDICATERECORD_LOCK, TWOPHASEPREDICATERECORD_XACT, PredicateLockData,
    GET_PREDICATELOCKTARGETTAG_DB, GET_PREDICATELOCKTARGETTAG_OFFSET,
    GET_PREDICATELOCKTARGETTAG_PAGE, GET_PREDICATELOCKTARGETTAG_RELATION,
    GET_PREDICATELOCKTARGETTAG_TYPE, SET_PREDICATELOCKTARGETTAG_PAGE,
    SET_PREDICATELOCKTARGETTAG_RELATION, SET_PREDICATELOCKTARGETTAG_TUPLE,
    InvalidSerializableXact,
};

// storage/block.h
use crate::storage::block::{BlockNumber, InvalidBlockNumber};

// storage/off.h
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber};

// Oid from postgres_ext.h (via prelude)
use crate::postgres_ext::Oid;
use crate::c::{Size, TransactionId};

// --- Stub types for unported dependencies ---

// TODO(pg-port): access/slru.h not yet ported
#[repr(C)]
pub struct SlruCtlData {
    pub shared: *mut SlruSharedData,
    pub PagePrecedes: Option<unsafe extern "C" fn(i64, i64) -> bool>,
}
#[repr(C)]
pub struct SlruSharedData {
    pub page_buffer: *mut *mut u8,
    pub page_dirty: *mut bool,
}
pub type SlruCtl = *mut SlruCtlData;

// TODO(pg-port): storage/lwlock.h
pub type LWLock = c_void;
pub type LWLockMode = c_int;
pub const LW_SHARED: LWLockMode = 0;
pub const LW_EXCLUSIVE: LWLockMode = 1;

// TODO(pg-port): storage/lock.h - MainLWLockArray
#[repr(C)]
pub struct LWLockHandle {
    pub lock: LWLock,
}
extern "C" {
    pub static mut MainLWLockArray: *mut LWLockHandle;
}

// TODO(pg-port): LWLock constants (lwlock.h)
pub const PREDICATELOCK_MANAGER_LWLOCK_OFFSET: c_int = 0; // placeholder
pub const NUM_PREDICATELOCK_PARTITIONS: c_int = 16;       // placeholder
pub const LOG2_NUM_PREDICATELOCK_PARTITIONS: c_int = 4;   // placeholder

// TODO(pg-port): lwlock.h tranche IDs
pub type LWLockTranche = c_int;
pub const LWTRANCHE_SERIAL_BUFFER: LWLockTranche = 0;
pub const LWTRANCHE_SERIAL_SLRU: LWLockTranche = 0;
pub const LWTRANCHE_PER_XACT_PREDICATE_LIST: LWLockTranche = 0;

// TODO(pg-port): access/slru.h sync handler
pub type SyncRequestHandler = c_int;
pub const SYNC_HANDLER_NONE: SyncRequestHandler = 0;

// TODO(pg-port): storage/shmem.h
pub use crate::utils::hash::dynahash::{HTAB, HASHCTL, HASH_ELEM, HASH_BLOBS, HASH_FUNCTION, HASH_PARTITION, HASH_FIXED_SIZE};
pub type HASH_SEQ_STATUS = c_void;
pub const HASH_ENTER: c_int = 1;
pub const HASH_ENTER_NULL: c_int = 2;
pub const HASH_FIND: c_int = 3;
pub const HASH_REMOVE: c_int = 4;

// TODO(pg-port): utils/rel.h - Relation
pub type RelationData = c_void;
pub type Relation = *mut RelationData;

// TODO(pg-port): utils/snapshot.h
pub type SnapshotData = c_void;
pub type Snapshot = *mut SnapshotData;

// TODO(pg-port): storage/itemptr.h
pub type ItemPointerData = c_void;
pub type ItemPointer = *mut ItemPointerData;

// TODO(pg-port): access/transam.h VirtualTransactionId + related
#[repr(C)]
#[derive(Copy, Clone)]
pub struct VirtualTransactionId {
    pub procNumber: c_int,
    pub localTransactionId: uint32,
}
pub type LocalTransactionId = uint32;

// TODO(pg-port): access/twophase_rmgr.h
pub const TWOPHASE_RM_PREDICATELOCK_ID: uint8 = 0;

// TODO(pg-port): utils/guc.h GucSource
pub type GucSource = c_int;

pub type SerializableXactHandle = *mut SERIALIZABLEXACT;

// Relation field accessors - TODO(pg-port): utils/rel.h
#[inline]
unsafe fn relation_rd_id(rel: Relation) -> Oid {
    /* TODO(pg-port): access rel->rd_id */ 0
}
#[inline]
unsafe fn relation_rd_locator_dboid(rel: Relation) -> Oid {
    /* TODO(pg-port): access rel->rd_locator.dbOid */ 0
}
#[inline]
unsafe fn relation_rd_index(rel: Relation) -> *mut c_void {
    /* TODO(pg-port): access rel->rd_index */ ptr::null_mut()
}
#[inline]
unsafe fn relation_rd_index_indrelid(rel: Relation) -> Oid {
    /* TODO(pg-port): access rel->rd_index->indrelid */ 0
}

// Snapshot field accessors - TODO(pg-port): utils/snapshot.h
#[inline]
unsafe fn snapshot_xmin(snap: Snapshot) -> TransactionId {
    /* TODO(pg-port): access snap->xmin */ 0
}
#[inline]
unsafe fn snapshot_xmax(snap: Snapshot) -> TransactionId {
    /* TODO(pg-port): access snap->xmax */ 0
}
#[inline]
unsafe fn snapshot_xip(snap: Snapshot) -> *const TransactionId {
    /* TODO(pg-port): access snap->xip */ ptr::null()
}
#[inline]
unsafe fn snapshot_xcnt(snap: Snapshot) -> uint32 {
    /* TODO(pg-port): access snap->xcnt */ 0
}

// ItemPointer field accessors - TODO(pg-port)
#[inline]
unsafe fn ItemPointerGetBlockNumber(tid: ItemPointer) -> BlockNumber {
    /* TODO(pg-port) */ 0
}
#[inline]
unsafe fn ItemPointerGetOffsetNumber(tid: ItemPointer) -> OffsetNumber {
    /* TODO(pg-port) */ 0
}

// External function stubs - TODO(pg-port) unported modules

#[inline]
unsafe fn LWLockAcquire(lock: *mut LWLock, mode: LWLockMode) {
    /* TODO(pg-port): storage/lwlock.h LWLockAcquire */
}
#[inline]
unsafe fn LWLockRelease(lock: *mut LWLock) {
    /* TODO(pg-port): storage/lwlock.h LWLockRelease */
}
#[inline]
unsafe fn LWLockHeldByMe(lock: *const LWLock) -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn LWLockHeldByMeInMode(lock: *const LWLock, mode: LWLockMode) -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn LWLockInitialize(lock: *mut LWLock, tranche_id: LWLockTranche) {
    /* TODO(pg-port) */
}
#[inline]
unsafe fn SimpleLruInit(
    ctl: SlruCtl,
    name: &str,
    nbufs: c_int,
    xid_wrap_limit: c_int,
    subdir: &str,
    buffer_tranche_id: LWLockTranche,
    bank_tranche_id: LWLockTranche,
    sync_handler: SyncRequestHandler,
    long_segment_names: bool,
) {
    /* TODO(pg-port): access/slru.h */
}
#[inline]
unsafe fn SimpleLruZeroPage(ctl: SlruCtl, pageno: i64) -> c_int {
    /* TODO(pg-port) */ 0
}
#[inline]
unsafe fn SimpleLruReadPage(ctl: SlruCtl, pageno: i64, write_ok: bool, xid: TransactionId) -> c_int {
    /* TODO(pg-port) */ 0
}
#[inline]
unsafe fn SimpleLruReadPage_ReadOnly(ctl: SlruCtl, pageno: i64, xid: TransactionId) -> c_int {
    /* TODO(pg-port) */ 0
}
#[inline]
unsafe fn SimpleLruGetBankLock(ctl: SlruCtl, pageno: i64) -> *mut LWLock {
    /* TODO(pg-port) */ ptr::null_mut()
}
#[inline]
unsafe fn SimpleLruTruncate(ctl: SlruCtl, cutoffpage: i64) {
    /* TODO(pg-port) */
}
#[inline]
unsafe fn SimpleLruWriteAll(ctl: SlruCtl, checkpoint: bool) {
    /* TODO(pg-port) */
}
#[inline]
unsafe fn SimpleLruShmemSize(nbufs: c_int, nlsns: c_int) -> Size {
    /* TODO(pg-port) */ 0
}
#[inline]
unsafe fn SlruPagePrecedesUnitTests(ctl: SlruCtl, entries_per_page: c_int) {
    /* TODO(pg-port) */
}
#[inline]
unsafe fn check_slru_buffers(name: &str, newval: *mut c_int) -> bool {
    /* TODO(pg-port) */ true
}
#[inline]
unsafe fn ShmemInitStruct(name: &str, size: Size, found: *mut bool) -> *mut c_void {
    let cname = std::ffi::CString::new(name).unwrap();
    crate::storage::ipc::shmem::ShmemInitStruct(cname.as_ptr(), size, found)
}
#[inline]
unsafe fn ShmemInitHash(
    name: &str,
    init_size: i64,
    max_size: i64,
    infoptr: *const HASHCTL,
    hash_flags: c_int,
) -> *mut HTAB {
    let cname = std::ffi::CString::new(name).unwrap();
    crate::storage::ipc::shmem::ShmemInitHash(cname.as_ptr(), init_size as c_long, max_size as c_long, infoptr as _, hash_flags) as *mut HTAB
}
#[inline]
unsafe fn ShmemAddrIsValid(addr: *const c_void) -> bool {
    /* TODO(pg-port) */ !addr.is_null()
}
#[inline]
unsafe fn hash_create(
    name: &str,
    nelem: c_int,
    info: *const HASHCTL,
    flags: c_int,
) -> *mut HTAB {
    /* TODO(pg-port) */ ptr::null_mut()
}
#[inline]
unsafe fn hash_destroy(hashp: *mut HTAB) {
    /* TODO(pg-port) */
}
#[inline]
unsafe fn hash_search(
    hashp: *mut HTAB,
    keyptr: *const c_void,
    action: c_int,
    foundPtr: *mut bool,
) -> *mut c_void {
    /* TODO(pg-port) */ ptr::null_mut()
}
#[inline]
unsafe fn hash_search_with_hash_value(
    hashp: *mut HTAB,
    keyptr: *const c_void,
    hashvalue: uint32,
    action: c_int,
    foundPtr: *mut bool,
) -> *mut c_void {
    /* TODO(pg-port) */ ptr::null_mut()
}
#[inline]
unsafe fn hash_seq_init(status: *mut HASH_SEQ_STATUS, hashp: *mut HTAB) {
    /* TODO(pg-port) */
}
#[inline]
unsafe fn hash_seq_search(status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    /* TODO(pg-port) */ ptr::null_mut()
}
#[inline]
unsafe fn hash_get_num_entries(hashp: *mut HTAB) -> c_int {
    /* TODO(pg-port) */ 0
}
#[inline]
unsafe fn hash_estimate_size(num_entries: i64, entrysize: Size) -> Size {
    /* TODO(pg-port) */ 0
}
#[inline]
unsafe fn get_hash_value(hashp: *mut HTAB, key: *const c_void) -> uint32 {
    /* TODO(pg-port) */ 0
}
#[inline]
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    /* TODO(pg-port) */ s1.saturating_mul(s2)
}
#[inline]
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    /* TODO(pg-port) */ s1.saturating_add(s2)
}

// access/transam.h stubs
#[inline]
unsafe fn TransactionIdIsValid(xid: TransactionId) -> bool {
    crate::access::transam::TransactionIdIsValid(xid)
}
#[inline]
unsafe fn TransactionIdEquals(xid1: TransactionId, xid2: TransactionId) -> bool {
    xid1 == xid2
}
#[inline]
unsafe fn TransactionIdPrecedes(xid1: TransactionId, xid2: TransactionId) -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn TransactionIdFollows(xid1: TransactionId, xid2: TransactionId) -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn TransactionIdPrecedesOrEquals(xid1: TransactionId, xid2: TransactionId) -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn TransactionIdFollowsOrEquals(xid1: TransactionId, xid2: TransactionId) -> bool {
    /* TODO(pg-port) */ false
}
pub const InvalidTransactionId: TransactionId = 0;
pub const FirstNormalTransactionId: TransactionId = 3;

// miscadmin.h stubs
#[inline]
unsafe fn RecoveryInProgress() -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn IsInParallelMode() -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn IsParallelWorker() -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn ParallelContextActive() -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn IsSubTransaction() -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn IsUnderPostmaster() -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn IsolationIsSerializable() -> bool {
    /* TODO(pg-port) */ false
}
static mut XactReadOnly: bool = false;
static mut XactDeferrable: bool = false;
static mut MyProcPid: c_int = 0;
extern "C" { pub static mut MyProcNumber: c_int; }
// miscadmin.h - FirstUnpinnedObjectId
pub const FirstUnpinnedObjectId: Oid = 12000; // placeholder

// storage/procnumber.h
pub const INVALID_PROC_NUMBER: c_int = -1;

// storage/proc.h
#[repr(C)]
pub struct PGPROC {
    pub pid: c_int,
}
extern "C" { pub static mut MyProc: *mut PGPROC; }
macro_rules! GET_VXID_FROM_PGPROC {
    ($vxid:ident, $proc:expr) => {
        /* TODO(pg-port): storage/proc.h GET_VXID_FROM_PGPROC */
    };
}

// utils/snapmgr.h stubs
#[inline]
unsafe fn GetSnapshotData(snapshot: Snapshot) -> Snapshot {
    /* TODO(pg-port) */ snapshot
}
#[inline]
unsafe fn GetTransactionSnapshot() -> Snapshot {
    /* TODO(pg-port) */ ptr::null_mut()
}
#[inline]
unsafe fn IsMVCCSnapshot(snapshot: Snapshot) -> bool {
    /* TODO(pg-port) */ true
}
#[inline]
unsafe fn GetTopTransactionIdIfAny() -> TransactionId {
    /* TODO(pg-port) */ InvalidTransactionId
}

// access/transam.h - TransamVariables
pub use crate::access::transam::varsup::TransamVariablesData as VariableCacheData;
pub use crate::access::transam::varsup::TransamVariables;
#[inline]
unsafe fn XidFromFullTransactionId(fxid: u64) -> TransactionId {
    fxid as TransactionId
}

// procarray.h stubs
#[inline]
unsafe fn ProcArrayInstallImportedXmin(
    xmin: TransactionId,
    sourcevxid: *const VirtualTransactionId,
) -> bool {
    /* TODO(pg-port) */ false
}

// proc.h stubs
#[inline]
unsafe fn ProcWaitForSignal(wait_event: uint32) {
    /* TODO(pg-port) */
}
#[inline]
unsafe fn ProcSendSignal(pgprocno: c_int) {
    /* TODO(pg-port) */
}
// wait event - TODO(pg-port): pgstat.h / wait_event_names.h
pub const WAIT_EVENT_SAFE_SNAPSHOT: uint32 = 0;

// access/xact.h stubs
#[inline]
unsafe fn TransactionIdIsCurrentTransactionId(xid: TransactionId) -> bool {
    /* TODO(pg-port) */ false
}
#[inline]
unsafe fn SetInvalidVirtualTransactionId(vxid: VirtualTransactionId) -> VirtualTransactionId {
    /* TODO(pg-port) */ VirtualTransactionId { procNumber: INVALID_PROC_NUMBER, localTransactionId: 0 }
}

// access/twophase.h stubs
#[inline]
unsafe fn RegisterTwoPhaseRecord(rmid: uint8, info: uint16, data: *const c_void, len: uint32) {
    /* TODO(pg-port) */
}

// utils/rel.h - RelationUsesLocalBuffers stub
#[inline]
unsafe fn RelationUsesLocalBuffers(rel: Relation) -> bool {
    /* TODO(pg-port) */ false
}

// port/pg_lfind.h
#[inline]
unsafe fn pg_lfind32(value: uint32, buf: *const uint32, len: uint32) -> bool {
    /* TODO(pg-port) */ false
}

// utils/guc_hooks.h
pub unsafe fn check_serial_buffers(newval: *mut c_int, extra: *mut *mut c_void, source: GucSource) -> bool {
    check_slru_buffers("serializable_buffers", newval)
}

// GUC variables
pub static mut max_predicate_locks_per_xact: c_int = 64;
pub static mut max_predicate_locks_per_relation: c_int = -2;
pub static mut max_predicate_locks_per_page: c_int = 2;
static mut serializable_buffers: c_int = 32;
static mut max_prepared_xacts: c_int = 0;

// InvalidPid
pub const InvalidPid: c_int = -1;

// elog level re-exports
use crate::utils::elog::{DEBUG2, ERROR};

// dlist_delete_thoroughly - same as dlist_delete for this port
#[inline]
unsafe fn dlist_delete_thoroughly(node: *mut dlist_node) {
    dlist_delete(node);
}

/*------------------------------------------------------------------------*/

/*
 * The SLRU buffer area through which we access the old xids.
 */
static mut SerialSlruCtlData: SlruCtlData = SlruCtlData {
    shared: ptr::null_mut(),
    PagePrecedes: None,
};
/* SerialSlruCtl = &SerialSlruCtlData */

const SERIAL_PAGESIZE: usize = 8192; // BLCKSZ placeholder
const SERIAL_ENTRYSIZE: usize = size_of::<SerCommitSeqNo>();
const SERIAL_ENTRIESPERPAGE: usize = SERIAL_PAGESIZE / SERIAL_ENTRYSIZE;

/* SERIAL_MAX_PAGE = MaxTransactionId / SERIAL_ENTRIESPERPAGE */
const SERIAL_MAX_PAGE: i64 = (u32::MAX as i64) / SERIAL_ENTRIESPERPAGE as i64;

#[inline]
fn SerialNextPage(page: i64) -> i64 {
    if page >= SERIAL_MAX_PAGE { 0 } else { page + 1 }
}

/// Read the SerCommitSeqNo slot for xid in the given SLRU slot.
#[inline]
unsafe fn SerialValue(slotno: c_int, xid: TransactionId) -> *mut SerCommitSeqNo {
    let shared = (*(*SerialSlruCtlData.shared).page_buffer.add(slotno as usize));
    let off = ((xid as usize) % SERIAL_ENTRIESPERPAGE) * SERIAL_ENTRYSIZE;
    shared.add(off) as *mut SerCommitSeqNo
}

#[inline]
fn SerialPage(xid: TransactionId) -> i64 {
    (xid as i64) / SERIAL_ENTRIESPERPAGE as i64
}

#[repr(C)]
pub struct SerialControlData {
    pub headPage: i64,      /* newest initialized page */
    pub headXid: TransactionId, /* newest valid Xid in the SLRU */
    pub tailXid: TransactionId, /* oldest xmin we might be interested in */
}
pub type SerialControl = *mut SerialControlData;

static mut serialControl: SerialControl = ptr::null_mut();

/*
 * When the oldest committed transaction on the "finished" list is moved to
 * SLRU, its predicate locks will be moved to this "dummy" transaction,
 * collapsing duplicate targets.
 */
static mut OldCommittedSxact: *mut SERIALIZABLEXACT = ptr::null_mut();

/*
 * These provide shared-memory structures for predicate locking.
 */
static mut PredXact: crate::storage::predicate_internals::PredXactList = ptr::null_mut();
static mut RWConflictPool: crate::storage::predicate_internals::RWConflictPoolHeader =
    ptr::null_mut();

static mut SerializableXidHash: *mut HTAB = ptr::null_mut();
static mut PredicateLockTargetHash: *mut HTAB = ptr::null_mut();
static mut PredicateLockHash: *mut HTAB = ptr::null_mut();
static mut FinishedSerializableTransactions: *mut dlist_head = ptr::null_mut();

/*
 * Tag for a dummy entry in PredicateLockTargetHash.
 */
static ScratchTargetTag: PREDICATELOCKTARGETTAG = PREDICATELOCKTARGETTAG {
    locktag_field1: 0,
    locktag_field2: 0,
    locktag_field3: 0,
    locktag_field4: 0,
};
static mut ScratchTargetTagHash: uint32 = 0;
static mut ScratchPartitionLock: *mut LWLock = ptr::null_mut();

/*
 * The local hash table used to determine when to combine multiple fine-
 * grained locks into a single coarser-grained lock.
 */
static mut LocalPredicateLockHash: *mut HTAB = ptr::null_mut();

/*
 * Keep a pointer to the currently-running serializable transaction (if any)
 * for quick reference.
 */
static mut MySerializableXact: *mut SERIALIZABLEXACT = ptr::null_mut();
static mut MyXactDidWrite: bool = false;

/*
 * The SXACT_FLAG_RO_UNSAFE optimization might lead us to release
 * MySerializableXact early.
 */
static mut SavedSerializableXact: *mut SERIALIZABLEXACT = ptr::null_mut();

/*------------------------------------------------------------------------*/

/* Predicate lock locks (LWLock stubs) - TODO(pg-port): lwlock.h */
#[inline]
unsafe fn SerializableFinishedListLock() -> *mut LWLock {
    crate::backend_link_shims::SerializableFinishedListLock as *mut LWLock
}
#[inline]
unsafe fn SerializablePredicateListLock() -> *mut LWLock {
    crate::backend_link_shims::SerializablePredicateListLock as *mut LWLock
}
#[inline]
unsafe fn SerializableXactHashLock() -> *mut LWLock {
    crate::backend_link_shims::SerializableXactHashLock as *mut LWLock
}
#[inline]
unsafe fn SerialControlLock() -> *mut LWLock {
    crate::backend_link_shims::SerialControlLock as *mut LWLock
}

/*
 * Partition lock helpers.
 */
#[inline]
unsafe fn PredicateLockHashPartition(hashcode: uint32) -> c_int {
    (hashcode % NUM_PREDICATELOCK_PARTITIONS as uint32) as c_int
}
#[inline]
unsafe fn PredicateLockHashPartitionLock(hashcode: uint32) -> *mut LWLock {
    /* &MainLWLockArray[PREDICATELOCK_MANAGER_LWLOCK_OFFSET + partition].lock */
    /* TODO(pg-port) */ ptr::null_mut()
}
#[inline]
unsafe fn PredicateLockHashPartitionLockByIndex(i: c_int) -> *mut LWLock {
    /* TODO(pg-port) */ ptr::null_mut()
}

#[inline]
fn NPREDICATELOCKTARGETENTS() -> i64 {
    unsafe {
        mul_size(
            max_predicate_locks_per_xact as Size,
            add_size(MaxBackends() as Size, max_prepared_xacts as Size),
        ) as i64
    }
}

/* SxactIsOnFinishedList */
#[inline]
unsafe fn SxactIsOnFinishedList(sxact: *const SERIALIZABLEXACT) -> bool {
    !crate::lib::ilist::dlist_node_is_detached(
        &(*sxact).finishedLink as *const dlist_node,
    )
}

/* flag accessor macros */
#[inline]
fn SxactIsCommitted(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_COMMITTED) != 0 }
}
#[inline]
fn SxactIsPrepared(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_PREPARED) != 0 }
}
#[inline]
fn SxactIsRolledBack(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_ROLLED_BACK) != 0 }
}
#[inline]
fn SxactIsDoomed(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_DOOMED) != 0 }
}
#[inline]
fn SxactIsReadOnly(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_READ_ONLY) != 0 }
}
#[inline]
fn SxactHasSummaryConflictIn(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_SUMMARY_CONFLICT_IN) != 0 }
}
#[inline]
fn SxactHasSummaryConflictOut(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_SUMMARY_CONFLICT_OUT) != 0 }
}
/*
 * The following macro actually means that the specified transaction has a
 * conflict out *to a transaction which committed ahead of it*.
 */
#[inline]
fn SxactHasConflictOut(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_CONFLICT_OUT) != 0 }
}
#[inline]
fn SxactIsDeferrableWaiting(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_DEFERRABLE_WAITING) != 0 }
}
#[inline]
fn SxactIsROSafe(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_RO_SAFE) != 0 }
}
#[inline]
fn SxactIsROUnsafe(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_RO_UNSAFE) != 0 }
}
#[inline]
fn SxactIsPartiallyReleased(sxact: *const SERIALIZABLEXACT) -> bool {
    unsafe { ((*sxact).flags & SXACT_FLAG_PARTIALLY_RELEASED) != 0 }
}

/*
 * Compute the hash code associated with a PREDICATELOCKTARGETTAG.
 */
#[inline]
unsafe fn PredicateLockTargetTagHashCode(tag: *const PREDICATELOCKTARGETTAG) -> uint32 {
    get_hash_value(PredicateLockTargetHash, tag as *const c_void)
}

/*
 * Given a predicate lock tag, and the hash for its target,
 * compute the lock hash.
 */
#[inline]
unsafe fn PredicateLockHashCodeFromTargetHashCode(
    predicatelocktag: *const PREDICATELOCKTAG,
    targethash: uint32,
) -> uint32 {
    let addr = (*predicatelocktag).myXact as usize as uint32;
    targethash ^ (addr << LOG2_NUM_PREDICATELOCK_PARTITIONS)
}

/*
 * Test the most selective fields first, for performance.
 * a is covered by b if all of the following hold:
 */
#[inline]
unsafe fn TargetTagIsCoveredBy(
    covered_target: PREDICATELOCKTARGETTAG,
    covering_target: PREDICATELOCKTARGETTAG,
) -> bool {
    /* (2) */ GET_PREDICATELOCKTARGETTAG_RELATION(&covered_target)
        == GET_PREDICATELOCKTARGETTAG_RELATION(&covering_target)
        /* (3) */ && GET_PREDICATELOCKTARGETTAG_OFFSET(&covering_target) == InvalidOffsetNumber
        && (/* (4a) */ (GET_PREDICATELOCKTARGETTAG_OFFSET(&covered_target) != InvalidOffsetNumber
            && GET_PREDICATELOCKTARGETTAG_PAGE(&covering_target)
                == GET_PREDICATELOCKTARGETTAG_PAGE(&covered_target))
            || /* (4b) */ (GET_PREDICATELOCKTARGETTAG_PAGE(&covering_target)
                == InvalidBlockNumber
                && GET_PREDICATELOCKTARGETTAG_PAGE(&covered_target) != InvalidBlockNumber))
        /* (1) */ && GET_PREDICATELOCKTARGETTAG_DB(&covered_target)
            == GET_PREDICATELOCKTARGETTAG_DB(&covering_target)
}

// MaxBackends stub - TODO(pg-port): utils/init/globals.c
#[inline]
unsafe fn MaxBackends() -> c_int {
    crate::utils::init::globals::MaxBackends
}

/*------------------------------------------------------------------------*/

/*
 * Does this relation participate in predicate locking? Temporary and system
 * relations are exempt.
 */
#[inline]
unsafe fn PredicateLockingNeededForRelation(relation: Relation) -> bool {
    !(relation_rd_id(relation) < FirstUnpinnedObjectId
        || RelationUsesLocalBuffers(relation))
}

/*
 * When a public interface method is called for a read, this is the test to
 * see if we should do a quick return.
 *
 * Note: this function has side-effects! If this transaction has been flagged
 * as RO-safe since the last call, we release all predicate locks and reset
 * MySerializableXact.
 */
#[inline]
unsafe fn SerializationNeededForRead(relation: Relation, snapshot: Snapshot) -> bool {
    /* Nothing to do if this is not a serializable transaction */
    if MySerializableXact == InvalidSerializableXact() {
        return false;
    }

    /*
     * Don't acquire locks or conflict when scanning with a special snapshot.
     */
    if !IsMVCCSnapshot(snapshot) {
        return false;
    }

    /*
     * Check if we have just become "RO-safe".
     */
    if SxactIsROSafe(MySerializableXact) {
        ReleasePredicateLocks(false, true);
        return false;
    }

    /* Check if the relation doesn't participate in predicate locking */
    if !PredicateLockingNeededForRelation(relation) {
        return false;
    }

    true /* no excuse to skip predicate locking */
}

/*
 * Like SerializationNeededForRead(), but called on writes.
 */
#[inline]
unsafe fn SerializationNeededForWrite(relation: Relation) -> bool {
    /* Nothing to do if this is not a serializable transaction */
    if MySerializableXact == InvalidSerializableXact() {
        return false;
    }

    /* Check if the relation doesn't participate in predicate locking */
    if !PredicateLockingNeededForRelation(relation) {
        return false;
    }

    true /* no excuse to skip predicate locking */
}

/*------------------------------------------------------------------------*/

/*
 * These functions are a simple implementation of a list for this specific
 * type of struct.
 */
unsafe fn CreatePredXact() -> *mut SERIALIZABLEXACT {
    if dlist_is_empty(&(*PredXact).availableList) {
        return ptr::null_mut();
    }

    let sxact = dlist_container!(
        SERIALIZABLEXACT,
        xactLink,
        dlist_pop_head_node(&mut (*PredXact).availableList)
    );
    dlist_push_tail(&mut (*PredXact).activeList, &mut (*sxact).xactLink);
    sxact
}

unsafe fn ReleasePredXact(sxact: *mut SERIALIZABLEXACT) {
    Assert!(ShmemAddrIsValid(sxact as *const c_void));

    dlist_delete(&mut (*sxact).xactLink);
    dlist_push_tail(&mut (*PredXact).availableList, &mut (*sxact).xactLink);
}

/*------------------------------------------------------------------------*/

/*
 * These functions manage primitive access to the RWConflict pool and lists.
 */
unsafe fn RWConflictExists(
    reader: *const SERIALIZABLEXACT,
    writer: *const SERIALIZABLEXACT,
) -> bool {
    Assert!(reader != writer);

    /* Check the ends of the purported conflict first. */
    if SxactIsDoomed(reader)
        || SxactIsDoomed(writer)
        || dlist_is_empty(&(*reader).outConflicts)
        || dlist_is_empty(&(*writer).inConflicts)
    {
        return false;
    }

    /*
     * A conflict is possible; walk the list to find out.
     */
    let mut iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(iter, &mut (*(reader as *mut SERIALIZABLEXACT)).outConflicts, {
        let conflict = dlist_container!(
            RWConflictData,
            outLink,
            iter.cur
        );

        if (*conflict).sxactIn == writer as *mut SERIALIZABLEXACT {
            return true;
        }
    });

    /* No conflict found. */
    false
}

unsafe fn SetRWConflict(reader: *mut SERIALIZABLEXACT, writer: *mut SERIALIZABLEXACT) {
    Assert!(reader != writer);
    Assert!(!RWConflictExists(reader, writer));

    if dlist_is_empty(&(*RWConflictPool).availableList) {
        ereport!(
            ERROR,
            errmsg!(
                "not enough elements in RWConflictPool to record a read/write conflict"
            )
            /* C also: errcode(ERRCODE_OUT_OF_MEMORY),
               errhint("You might need to run fewer transactions at a time or increase \"max_connections\".") */
        );
    }

    let conflict = dlist_head_element!(
        RWConflictData,
        outLink,
        &mut (*RWConflictPool).availableList
    );
    dlist_delete(&mut (*conflict).outLink);

    (*conflict).sxactOut = reader;
    (*conflict).sxactIn = writer;
    dlist_push_tail(&mut (*reader).outConflicts, &mut (*conflict).outLink);
    dlist_push_tail(&mut (*writer).inConflicts, &mut (*conflict).inLink);
}

unsafe fn SetPossibleUnsafeConflict(
    roXact: *mut SERIALIZABLEXACT,
    activeXact: *mut SERIALIZABLEXACT,
) {
    Assert!(roXact != activeXact);
    Assert!(SxactIsReadOnly(roXact));
    Assert!(!SxactIsReadOnly(activeXact));

    if dlist_is_empty(&(*RWConflictPool).availableList) {
        ereport!(
            ERROR,
            errmsg!(
                "not enough elements in RWConflictPool to record a potential read/write conflict"
            )
            /* C also: errcode(ERRCODE_OUT_OF_MEMORY),
               errhint("You might need to run fewer transactions at a time or increase \"max_connections\".") */
        );
    }

    let conflict = dlist_head_element!(
        RWConflictData,
        outLink,
        &mut (*RWConflictPool).availableList
    );
    dlist_delete(&mut (*conflict).outLink);

    (*conflict).sxactOut = activeXact;
    (*conflict).sxactIn = roXact;
    dlist_push_tail(
        &mut (*activeXact).possibleUnsafeConflicts,
        &mut (*conflict).outLink,
    );
    dlist_push_tail(
        &mut (*roXact).possibleUnsafeConflicts,
        &mut (*conflict).inLink,
    );
}

unsafe fn ReleaseRWConflict(conflict: *mut RWConflictData) {
    dlist_delete(&mut (*conflict).inLink);
    dlist_delete(&mut (*conflict).outLink);
    dlist_push_tail(
        &mut (*RWConflictPool).availableList,
        &mut (*conflict).outLink,
    );
}

unsafe fn FlagSxactUnsafe(sxact: *mut SERIALIZABLEXACT) {
    Assert!(SxactIsReadOnly(sxact));
    Assert!(!SxactIsROSafe(sxact));

    (*sxact).flags |= SXACT_FLAG_RO_UNSAFE;

    /*
     * We know this isn't a safe snapshot, so we can stop looking for other
     * potential conflicts.
     */
    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut (*sxact).possibleUnsafeConflicts, {
        let conflict = dlist_container!(
            RWConflictData,
            inLink,
            iter.cur
        );

        Assert!(!SxactIsReadOnly((*conflict).sxactOut));
        Assert!(sxact == (*conflict).sxactIn);

        ReleaseRWConflict(conflict);
    });
}

/*------------------------------------------------------------------------*/

/*
 * Decide whether a Serial page number is "older" for truncation purposes.
 * Analogous to CLOGPagePrecedes().
 */
unsafe fn SerialPagePrecedesLogically(page1: i64, page2: i64) -> bool {
    let xid1: TransactionId = (page1 as TransactionId)
        .wrapping_mul(SERIAL_ENTRIESPERPAGE as TransactionId)
        .wrapping_add(FirstNormalTransactionId + 1);
    let xid2: TransactionId = (page2 as TransactionId)
        .wrapping_mul(SERIAL_ENTRIESPERPAGE as TransactionId)
        .wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(
            xid1,
            xid2.wrapping_add(SERIAL_ENTRIESPERPAGE as TransactionId - 1),
        )
}

#[cfg(debug_assertions)]
unsafe fn SerialPagePrecedesLogicallyUnitTests() {
    let per_page = SERIAL_ENTRIESPERPAGE as i64;
    let offset = per_page / 2;

    /* GetNewTransactionId() has assigned the last XID it can safely use. */
    let newestPage: i64 = 2 * 32 - 1; /* SLRU_PAGES_PER_SEGMENT placeholder */
    let newestXact: TransactionId = (newestPage * per_page + offset) as TransactionId;
    Assert!(newestXact as i64 / per_page == newestPage);
    let oldestXact: TransactionId = newestXact.wrapping_add(1).wrapping_sub(1u32 << 31);
    let oldestPage: i64 = (oldestXact as i64) / per_page;

    /*
     * In this scenario... Function must return false so SerialAdd() doesn't
     * zero tailPage and half the SLRU.
     */
    let headPage: i64 = newestPage;
    let targetPage: i64 = oldestPage;
    Assert!(!SerialPagePrecedesLogically(headPage, targetPage));

    /*
     * In this scenario... Function must return true to make SerialAdd()
     * create targetPage.
     */
    let headPage: i64 = oldestPage;
    let targetPage: i64 = newestPage;
    Assert!(SerialPagePrecedesLogically(headPage, targetPage - 1));
    /* #if 0: Assert!(SerialPagePrecedesLogically(headPage, targetPage)); */
}

/*
 * Initialize for the tracking of old serializable committed xids.
 */
unsafe fn SerialInit() {
    let mut found: bool = false;

    /*
     * Set up SLRU management of the pg_serial data.
     */
    SerialSlruCtlData.PagePrecedes = Some(serial_page_precedes_shim);
    SimpleLruInit(
        &mut SerialSlruCtlData as SlruCtl,
        "serializable",
        serializable_buffers,
        0,
        "pg_serial",
        LWTRANCHE_SERIAL_BUFFER,
        LWTRANCHE_SERIAL_SLRU,
        SYNC_HANDLER_NONE,
        false,
    );
    #[cfg(debug_assertions)]
    SerialPagePrecedesLogicallyUnitTests();
    SlruPagePrecedesUnitTests(
        &mut SerialSlruCtlData as SlruCtl,
        SERIAL_ENTRIESPERPAGE as c_int,
    );

    /*
     * Create or attach to the SerialControl structure.
     */
    serialControl = ShmemInitStruct(
        "SerialControlData",
        size_of::<SerialControlData>() as Size,
        &mut found,
    ) as SerialControl;

    Assert!(found == IsUnderPostmaster());
    if !found {
        /*
         * Set control information to reflect empty SLRU.
         */
        LWLockAcquire(SerialControlLock(), LW_EXCLUSIVE);
        (*serialControl).headPage = -1;
        (*serialControl).headXid = InvalidTransactionId;
        (*serialControl).tailXid = InvalidTransactionId;
        LWLockRelease(SerialControlLock());
    }
}

/* C-callable shim for the PagePrecedes function pointer */
unsafe extern "C" fn serial_page_precedes_shim(page1: i64, page2: i64) -> bool {
    SerialPagePrecedesLogically(page1, page2)
}

/*
 * Record a committed read write serializable xid and the minimum
 * commitSeqNo of any transactions to which this xid had a rw-conflict out.
 */
unsafe fn SerialAdd(xid: TransactionId, minConflictCommitSeqNo: SerCommitSeqNo) {
    let tailXid: TransactionId;
    let targetPage: i64;
    let mut slotno: c_int;
    let mut firstZeroPage: i64;
    let isNewPage: bool;
    let lock: *mut LWLock;

    Assert!(TransactionIdIsValid(xid));

    targetPage = SerialPage(xid);
    lock = SimpleLruGetBankLock(&mut SerialSlruCtlData as SlruCtl, targetPage);

    /*
     * In this routine, we must hold both SerialControlLock and the SLRU bank
     * lock simultaneously while making the SLRU data catch up with the new
     * state that we determine.
     */
    LWLockAcquire(SerialControlLock(), LW_EXCLUSIVE);

    /*
     * If 'xid' is older than the global xmin (== tailXid), there's no need to
     * store it.
     */
    tailXid = (*serialControl).tailXid;
    if !TransactionIdIsValid(tailXid) || TransactionIdPrecedes(xid, tailXid) {
        LWLockRelease(SerialControlLock());
        return;
    }

    /*
     * If the SLRU is currently unused, zero out the whole active region.
     */
    if (*serialControl).headPage < 0 {
        firstZeroPage = SerialPage(tailXid);
        isNewPage = true;
    } else {
        firstZeroPage = SerialNextPage((*serialControl).headPage);
        isNewPage = SerialPagePrecedesLogically((*serialControl).headPage, targetPage);
    }

    if !TransactionIdIsValid((*serialControl).headXid)
        || TransactionIdFollows(xid, (*serialControl).headXid)
    {
        (*serialControl).headXid = xid;
    }
    if isNewPage {
        (*serialControl).headPage = targetPage;
    }

    if isNewPage {
        /* Initialize intervening pages; might involve trading locks */
        loop {
            let page_lock = SimpleLruGetBankLock(
                &mut SerialSlruCtlData as SlruCtl,
                firstZeroPage,
            );
            LWLockAcquire(page_lock, LW_EXCLUSIVE);
            slotno = SimpleLruZeroPage(&mut SerialSlruCtlData as SlruCtl, firstZeroPage);
            if firstZeroPage == targetPage {
                break;
            }
            firstZeroPage = SerialNextPage(firstZeroPage);
            LWLockRelease(page_lock);
        }
    } else {
        LWLockAcquire(lock, LW_EXCLUSIVE);
        slotno = SimpleLruReadPage(&mut SerialSlruCtlData as SlruCtl, targetPage, true, xid);
    }

    *SerialValue(slotno, xid) = minConflictCommitSeqNo;
    (*(*SerialSlruCtlData.shared).page_dirty.add(slotno as usize)) = true;

    LWLockRelease(lock);
    LWLockRelease(SerialControlLock());
}

/*
 * Get the minimum commitSeqNo for any conflict out for the given xid.
 */
unsafe fn SerialGetMinConflictCommitSeqNo(xid: TransactionId) -> SerCommitSeqNo {
    let headXid: TransactionId;
    let tailXid: TransactionId;
    let val: SerCommitSeqNo;
    let slotno: c_int;

    Assert!(TransactionIdIsValid(xid));

    LWLockAcquire(SerialControlLock(), LW_SHARED);
    headXid = (*serialControl).headXid;
    tailXid = (*serialControl).tailXid;
    LWLockRelease(SerialControlLock());

    if !TransactionIdIsValid(headXid) {
        return 0;
    }

    Assert!(TransactionIdIsValid(tailXid));

    if TransactionIdPrecedes(xid, tailXid) || TransactionIdFollows(xid, headXid) {
        return 0;
    }

    /*
     * The following function must be called without holding SLRU bank lock,
     * but will return with that lock held, which must then be released.
     */
    let slotno = SimpleLruReadPage_ReadOnly(
        &mut SerialSlruCtlData as SlruCtl,
        SerialPage(xid),
        xid,
    );
    let val = *SerialValue(slotno, xid);
    LWLockRelease(SimpleLruGetBankLock(
        &mut SerialSlruCtlData as SlruCtl,
        SerialPage(xid),
    ));
    val
}

/*
 * Call this whenever there is a new xmin for active serializable
 * transactions.
 */
unsafe fn SerialSetActiveSerXmin(xid: TransactionId) {
    LWLockAcquire(SerialControlLock(), LW_EXCLUSIVE);

    /*
     * When no sxacts are active, nothing overlaps, set the xid values to
     * invalid to show that there are no valid entries.
     */
    if !TransactionIdIsValid(xid) {
        (*serialControl).tailXid = InvalidTransactionId;
        (*serialControl).headXid = InvalidTransactionId;
        LWLockRelease(SerialControlLock());
        return;
    }

    /*
     * When we're recovering prepared transactions, the global xmin might move
     * backwards.
     */
    if RecoveryInProgress() {
        Assert!((*serialControl).headPage < 0);
        if !TransactionIdIsValid((*serialControl).tailXid)
            || TransactionIdPrecedes(xid, (*serialControl).tailXid)
        {
            (*serialControl).tailXid = xid;
        }
        LWLockRelease(SerialControlLock());
        return;
    }

    Assert!(
        !TransactionIdIsValid((*serialControl).tailXid)
            || TransactionIdFollows(xid, (*serialControl).tailXid)
    );

    (*serialControl).tailXid = xid;

    LWLockRelease(SerialControlLock());
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
pub unsafe fn CheckPointPredicate() {
    let truncateCutoffPage: i64;

    LWLockAcquire(SerialControlLock(), LW_EXCLUSIVE);

    /* Exit quickly if the SLRU is currently not in use. */
    if (*serialControl).headPage < 0 {
        LWLockRelease(SerialControlLock());
        return;
    }

    let truncateCutoffPage = if TransactionIdIsValid((*serialControl).tailXid) {
        let tailPage = SerialPage((*serialControl).tailXid);

        /*
         * It is possible for the tailXid to be ahead of the headXid.
         */
        if SerialPagePrecedesLogically(tailPage, (*serialControl).headPage) {
            /* We can truncate the SLRU up to the page containing tailXid */
            tailPage
        } else {
            (*serialControl).headPage
        }
    } else {
        /*----------
         * The SLRU is no longer needed. Truncate to head before we set head
         * invalid.
         */
        let tmp = (*serialControl).headPage;
        (*serialControl).headPage = -1;
        tmp
    };

    LWLockRelease(SerialControlLock());

    /*
     * Truncate away pages that are no longer required.
     */
    SimpleLruTruncate(&mut SerialSlruCtlData as SlruCtl, truncateCutoffPage);

    /*
     * Write dirty SLRU pages to disk
     */
    SimpleLruWriteAll(&mut SerialSlruCtlData as SlruCtl, true);
}

/*------------------------------------------------------------------------*/

/*
 * PredicateLockShmemInit -- Initialize the predicate locking data structures.
 */
pub unsafe fn PredicateLockShmemInit() {
    let mut info: HASHCTL = core::mem::zeroed();
    let mut max_table_size: i64;
    let mut requestSize: Size;
    let mut found: bool = false;

    /* #ifndef EXEC_BACKEND */
    Assert!(!IsUnderPostmaster());

    /*
     * Compute size of predicate lock target hashtable. Note these
     * calculations must agree with PredicateLockShmemSize!
     */
    max_table_size = NPREDICATELOCKTARGETENTS();

    /*
     * Allocate hash table for PREDICATELOCKTARGET structs.
     */
    info.keysize = size_of::<PREDICATELOCKTARGETTAG>() as Size;
    info.entrysize = size_of::<PREDICATELOCKTARGET>() as Size;
    info.num_partitions = NUM_PREDICATELOCK_PARTITIONS as c_long;

    PredicateLockTargetHash = ShmemInitHash(
        "PREDICATELOCKTARGET hash",
        max_table_size,
        max_table_size,
        &info,
        HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FIXED_SIZE,
    );

    /*
     * Reserve a dummy entry in the hash table.
     */
    if !IsUnderPostmaster() {
        let _ = hash_search(
            PredicateLockTargetHash,
            &ScratchTargetTag as *const PREDICATELOCKTARGETTAG as *const c_void,
            HASH_ENTER,
            &mut found,
        );
        Assert!(!found);
    }

    /* Pre-calculate the hash and partition lock of the scratch entry */
    ScratchTargetTagHash = PredicateLockTargetTagHashCode(&ScratchTargetTag);
    ScratchPartitionLock = PredicateLockHashPartitionLock(ScratchTargetTagHash);

    /*
     * Allocate hash table for PREDICATELOCK structs.
     */
    info.keysize = size_of::<PREDICATELOCKTAG>() as Size;
    info.entrysize = size_of::<PREDICATELOCK>() as Size;
    info.hash = Some(predicatelock_hash);
    info.num_partitions = NUM_PREDICATELOCK_PARTITIONS as c_long;

    /* Assume an average of 2 xacts per target */
    max_table_size *= 2;

    PredicateLockHash = ShmemInitHash(
        "PREDICATELOCK hash",
        max_table_size,
        max_table_size,
        &info,
        HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_FIXED_SIZE,
    );

    /*
     * Compute size for serializable transaction hashtable.
     */
    max_table_size = (MaxBackends() + max_prepared_xacts) as i64;

    /*
     * Allocate a list to hold information on transactions participating in
     * predicate locking.
     *
     * Assume an average of 10 predicate locking transactions per backend.
     */
    max_table_size *= 10;

    requestSize = add_size(
        PredXactListDataSize() as Size,
        mul_size(max_table_size as Size, size_of::<SERIALIZABLEXACT>() as Size),
    );

    PredXact = ShmemInitStruct("PredXactList", requestSize, &mut found)
        as crate::storage::predicate_internals::PredXactList;
    Assert!(found == IsUnderPostmaster());
    if !found {
        /* clean everything, both the header and the element */
        ptr::write_bytes(PredXact as *mut u8, 0, requestSize);

        dlist_init(&mut (*PredXact).availableList);
        dlist_init(&mut (*PredXact).activeList);
        (*PredXact).SxactGlobalXmin = InvalidTransactionId;
        (*PredXact).SxactGlobalXminCount = 0;
        (*PredXact).WritableSxactCount = 0;
        (*PredXact).LastSxactCommitSeqNo = FirstNormalSerCommitSeqNo - 1;
        (*PredXact).CanPartialClearThrough = 0;
        (*PredXact).HavePartialClearedThrough = 0;
        (*PredXact).element = (PredXact as *mut u8)
            .add(PredXactListDataSize() as usize)
            as *mut SERIALIZABLEXACT;
        /* Add all elements to available list, clean. */
        for i in 0..max_table_size as usize {
            LWLockInitialize(
                &mut (*(*PredXact).element.add(i)).perXactPredicateListLock,
                LWTRANCHE_PER_XACT_PREDICATE_LIST,
            );
            dlist_push_tail(
                &mut (*PredXact).availableList,
                &mut (*(*PredXact).element.add(i)).xactLink,
            );
        }
        (*PredXact).OldCommittedSxact = CreatePredXact();
        let old = (*PredXact).OldCommittedSxact;
        let invalid_vxid = SetInvalidVirtualTransactionId(VirtualTransactionId {
            procNumber: INVALID_PROC_NUMBER,
            localTransactionId: 0,
        });
        /* SetInvalidVirtualTransactionId sets the vxid; copy bytes (vxid is an
         * opaque c_void stub for predicate_internals::VirtualTransactionId). */
        ptr::copy_nonoverlapping(
            &invalid_vxid as *const VirtualTransactionId as *const u8,
            &mut (*old).vxid as *mut _ as *mut u8,
            size_of::<VirtualTransactionId>(),
        );
        (*old).prepareSeqNo = 0;
        (*old).commitSeqNo = 0;
        (*old).SeqNo.lastCommitBeforeSnapshot = 0;
        dlist_init(&mut (*old).outConflicts);
        dlist_init(&mut (*old).inConflicts);
        dlist_init(&mut (*old).predicateLocks);
        dlist_node_init(&mut (*old).finishedLink);
        dlist_init(&mut (*old).possibleUnsafeConflicts);
        (*old).topXid = InvalidTransactionId;
        (*old).finishedBefore = InvalidTransactionId;
        (*old).xmin = InvalidTransactionId;
        (*old).flags = SXACT_FLAG_COMMITTED;
        (*old).pid = 0;
        (*old).pgprocno = INVALID_PROC_NUMBER;
    }
    /* This never changes, so let's keep a local copy. */
    OldCommittedSxact = (*PredXact).OldCommittedSxact;

    /*
     * Allocate hash table for SERIALIZABLEXID structs.
     */
    info.keysize = size_of::<SERIALIZABLEXIDTAG>() as Size;
    info.entrysize = size_of::<SERIALIZABLEXID>() as Size;
    info.hash = None;
    info.num_partitions = 0;

    SerializableXidHash = ShmemInitHash(
        "SERIALIZABLEXID hash",
        max_table_size,
        max_table_size,
        &info,
        HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE,
    );

    /*
     * Allocate space for tracking rw-conflicts in lists attached to the
     * transactions.
     *
     * Assume an average of 5 conflicts per transaction.
     */
    max_table_size *= 5;

    requestSize = add_size(
        RWConflictPoolHeaderDataSize() as Size,
        mul_size(max_table_size as Size, crate::storage::predicate_internals::RWConflictDataSize() as Size),
    );

    RWConflictPool = ShmemInitStruct("RWConflictPool", requestSize, &mut found)
        as crate::storage::predicate_internals::RWConflictPoolHeader;
    Assert!(found == IsUnderPostmaster());
    if !found {
        /* clean everything, including the elements */
        ptr::write_bytes(RWConflictPool as *mut u8, 0, requestSize);

        dlist_init(&mut (*RWConflictPool).availableList);
        (*RWConflictPool).element = (RWConflictPool as *mut u8)
            .add(RWConflictPoolHeaderDataSize() as usize)
            as crate::storage::predicate_internals::RWConflict;
        /* Add all elements to available list, clean. */
        for i in 0..max_table_size as usize {
            dlist_push_tail(
                &mut (*RWConflictPool).availableList,
                &mut (*(*RWConflictPool).element.add(i)).outLink,
            );
        }
    }

    /*
     * Create or attach to the header for the list of finished serializable
     * transactions.
     */
    FinishedSerializableTransactions = ShmemInitStruct(
        "FinishedSerializableTransactions",
        size_of::<dlist_head>() as Size,
        &mut found,
    ) as *mut dlist_head;
    Assert!(found == IsUnderPostmaster());
    if !found {
        dlist_init(&mut *FinishedSerializableTransactions);
    }

    /*
     * Initialize the SLRU storage for old committed serializable
     * transactions.
     */
    SerialInit();
}

/*
 * Estimate shared-memory space used for predicate lock table
 */
pub unsafe fn PredicateLockShmemSize() -> Size {
    let mut size: Size = 0;
    let mut max_table_size: i64;

    /* predicate lock target hash table */
    max_table_size = NPREDICATELOCKTARGETENTS();
    size = add_size(
        size,
        hash_estimate_size(max_table_size, size_of::<PREDICATELOCKTARGET>() as Size),
    );

    /* predicate lock hash table */
    max_table_size *= 2;
    size = add_size(
        size,
        hash_estimate_size(max_table_size, size_of::<PREDICATELOCK>() as Size),
    );

    /*
     * Since NPREDICATELOCKTARGETENTS is only an estimate, add 10% safety
     * margin.
     */
    size = add_size(size, size / 10);

    /* transaction list */
    max_table_size = (MaxBackends() + max_prepared_xacts) as i64;
    max_table_size *= 10;
    size = add_size(size, PredXactListDataSize() as Size);
    size = add_size(
        size,
        mul_size(max_table_size as Size, size_of::<SERIALIZABLEXACT>() as Size),
    );

    /* transaction xid table */
    size = add_size(
        size,
        hash_estimate_size(max_table_size, size_of::<SERIALIZABLEXID>() as Size),
    );

    /* rw-conflict pool */
    max_table_size *= 5;
    size = add_size(size, RWConflictPoolHeaderDataSize() as Size);
    size = add_size(
        size,
        mul_size(
            max_table_size as Size,
            crate::storage::predicate_internals::RWConflictDataSize() as Size,
        ),
    );

    /* Head for list of finished serializable transactions. */
    size = add_size(size, size_of::<dlist_head>() as Size);

    /* Shared memory structures for SLRU tracking of old committed xids. */
    size = add_size(size, size_of::<SerialControlData>() as Size);
    size = add_size(
        size,
        SimpleLruShmemSize(serializable_buffers, 0),
    );

    size
}

/*
 * Compute the hash code associated with a PREDICATELOCKTAG.
 */
unsafe extern "C" fn predicatelock_hash(key: *const c_void, keysize: Size) -> uint32 {
    let predicatelocktag = key as *const PREDICATELOCKTAG;
    let targethash: uint32;

    Assert!(keysize == size_of::<PREDICATELOCKTAG>() as Size);

    /* Look into the associated target object, and compute its hash code */
    targethash = PredicateLockTargetTagHashCode(&(*(*predicatelocktag).myTarget).tag);

    PredicateLockHashCodeFromTargetHashCode(predicatelocktag, targethash)
}

/*
 * GetPredicateLockStatusData
 *   Return a table containing the internal state of the predicate
 *   lock manager for use in pg_lock_status.
 */
pub unsafe fn GetPredicateLockStatusData() -> *mut PredicateLockData {
    let data: *mut PredicateLockData;
    let mut i: c_int;
    let els: c_int;
    let mut el: c_int;
    let mut seqstat: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut predlock: *mut PREDICATELOCK;

    data = palloc(size_of::<PredicateLockData>() as Size) as *mut PredicateLockData;

    /*
     * To ensure consistency, take simultaneous locks on all partition locks
     * in ascending order, then SerializableXactHashLock.
     */
    i = 0;
    while i < NUM_PREDICATELOCK_PARTITIONS {
        LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_SHARED);
        i += 1;
    }
    LWLockAcquire(SerializableXactHashLock(), LW_SHARED);

    /* Get number of locks and allocate appropriately-sized arrays. */
    els = hash_get_num_entries(PredicateLockHash);
    (*data).nelements = els;
    (*data).locktags = palloc(
        (size_of::<PREDICATELOCKTARGETTAG>() * els as usize) as Size,
    ) as *mut PREDICATELOCKTARGETTAG;
    (*data).xacts = palloc(
        (size_of::<SERIALIZABLEXACT>() * els as usize) as Size,
    ) as *mut SERIALIZABLEXACT;

    /* Scan through PredicateLockHash and copy contents */
    hash_seq_init(&mut seqstat, PredicateLockHash);

    el = 0;

    loop {
        predlock = hash_seq_search(&mut seqstat) as *mut PREDICATELOCK;
        if predlock.is_null() {
            break;
        }
        *(*data).locktags.offset(el as isize) = (*(*predlock).tag.myTarget).tag;
        ptr::copy_nonoverlapping(
            (*predlock).tag.myXact as *const SERIALIZABLEXACT,
            (*data).xacts.offset(el as isize),
            1,
        );
        el += 1;
    }

    Assert!(el == els);

    /* Release locks in reverse order */
    LWLockRelease(SerializableXactHashLock());
    i = NUM_PREDICATELOCK_PARTITIONS - 1;
    while i >= 0 {
        LWLockRelease(PredicateLockHashPartitionLockByIndex(i));
        i -= 1;
    }

    data
}

/*
 * Free up shared memory structures by pushing the oldest sxact into summary form.
 */
unsafe fn SummarizeOldestCommittedSxact() {
    let sxact: *mut SERIALIZABLEXACT;

    LWLockAcquire(SerializableFinishedListLock(), LW_EXCLUSIVE);

    /*
     * This function is only called if there are no sxact slots available.
     */
    if dlist_is_empty(&*FinishedSerializableTransactions) {
        LWLockRelease(SerializableFinishedListLock());
        return;
    }

    /*
     * Grab the first sxact off the finished list -- this will be the earliest
     * commit.  Remove it from the list.
     */
    let sxact = dlist_head_element!(
        SERIALIZABLEXACT,
        finishedLink,
        &mut *FinishedSerializableTransactions
    );
    dlist_delete_thoroughly(&mut (*sxact).finishedLink);

    /* Add to SLRU summary information. */
    if TransactionIdIsValid((*sxact).topXid) && !SxactIsReadOnly(sxact) {
        SerialAdd(
            (*sxact).topXid,
            if SxactHasConflictOut(sxact) {
                (*sxact).SeqNo.earliestOutConflictCommit
            } else {
                InvalidSerCommitSeqNo
            },
        );
    }

    /* Summarize and release the detail. */
    ReleaseOneSerializableXact(sxact, false, true);

    LWLockRelease(SerializableFinishedListLock());
}

/*
 * GetSafeSnapshot
 *   Obtain and register a snapshot for a READ ONLY DEFERRABLE
 *   transaction.
 */
unsafe fn GetSafeSnapshot(origSnapshot: Snapshot) -> Snapshot {
    let mut snapshot: Snapshot;

    Assert!(XactReadOnly && XactDeferrable);

    loop {
        /*
         * GetSerializableTransactionSnapshotInt is going to call
         * GetSnapshotData.
         */
        snapshot = GetSerializableTransactionSnapshotInt(origSnapshot, ptr::null_mut(), InvalidPid);

        if MySerializableXact == InvalidSerializableXact() {
            return snapshot; /* no concurrent r/w xacts; it's safe */
        }

        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

        /*
         * Wait for concurrent transactions to finish.
         */
        (*MySerializableXact).flags |= SXACT_FLAG_DEFERRABLE_WAITING;
        while !(dlist_is_empty(&(*MySerializableXact).possibleUnsafeConflicts)
            || SxactIsROUnsafe(MySerializableXact))
        {
            LWLockRelease(SerializableXactHashLock());
            ProcWaitForSignal(WAIT_EVENT_SAFE_SNAPSHOT);
            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);
        }
        (*MySerializableXact).flags &= !SXACT_FLAG_DEFERRABLE_WAITING;

        if !SxactIsROUnsafe(MySerializableXact) {
            LWLockRelease(SerializableXactHashLock());
            break; /* success */
        }

        LWLockRelease(SerializableXactHashLock());

        /* else, need to retry... */
        ereport!(
            DEBUG2,
            errmsg!("deferrable snapshot was unsafe; trying a new one")
            /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE) */
        );
        ReleasePredicateLocks(false, false);
    }

    /*
     * Now we have a safe snapshot, so we don't need to do any further checks.
     */
    Assert!(SxactIsROSafe(MySerializableXact));
    ReleasePredicateLocks(false, true);

    snapshot
}

/*
 * GetSafeSnapshotBlockingPids
 *   If the specified process is currently blocked in GetSafeSnapshot,
 *   write the process IDs of all processes that it is blocked by.
 */
pub unsafe fn GetSafeSnapshotBlockingPids(
    blocked_pid: c_int,
    output: *mut c_int,
    output_size: c_int,
) -> c_int {
    let mut num_written: c_int = 0;
    let mut blocking_sxact: *mut SERIALIZABLEXACT = ptr::null_mut();

    LWLockAcquire(SerializableXactHashLock(), LW_SHARED);

    /* Find blocked_pid's SERIALIZABLEXACT by linear search. */
    let mut iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(iter, &mut (*PredXact).activeList, {
        let sxact = dlist_container!(
            SERIALIZABLEXACT,
            xactLink,
            iter.cur
        );

        if (*sxact).pid == blocked_pid {
            blocking_sxact = sxact;
            break;
        }
    });

    /* Did we find it, and is it currently waiting in GetSafeSnapshot? */
    if !blocking_sxact.is_null() && SxactIsDeferrableWaiting(blocking_sxact) {
        /* Traverse the list of possible unsafe conflicts collecting PIDs. */
        dlist_foreach!(iter, &mut (*blocking_sxact).possibleUnsafeConflicts, {
            let possibleUnsafeConflict = dlist_container!(
                RWConflictData,
                inLink,
                iter.cur
            );

            *output.offset(num_written as isize) = (*(*possibleUnsafeConflict).sxactOut).pid;
            num_written += 1;

            if num_written >= output_size {
                break;
            }
        });
    }

    LWLockRelease(SerializableXactHashLock());

    num_written
}

/*
 * Acquire a snapshot that can be used for the current transaction.
 */
pub unsafe fn GetSerializableTransactionSnapshot(snapshot: Snapshot) -> Snapshot {
    Assert!(IsolationIsSerializable());

    /*
     * Can't use serializable mode while recovery is still active.
     */
    if RecoveryInProgress() {
        ereport!(
            ERROR,
            errmsg!("cannot use serializable mode in a hot standby")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errdetail("\"default_transaction_isolation\" is set to \"serializable\"."),
               errhint("You can use \"SET default_transaction_isolation = 'repeatable read'\" to change the default.") */
        );
    }

    /*
     * A special optimization is available for SERIALIZABLE READ ONLY
     * DEFERRABLE transactions.
     */
    if XactReadOnly && XactDeferrable {
        return GetSafeSnapshot(snapshot);
    }

    GetSerializableTransactionSnapshotInt(snapshot, ptr::null_mut(), InvalidPid)
}

/*
 * Import a snapshot to be used for the current transaction.
 */
pub unsafe fn SetSerializableTransactionSnapshot(
    snapshot: Snapshot,
    sourcevxid: *mut VirtualTransactionId,
    sourcepid: c_int,
) {
    Assert!(IsolationIsSerializable());

    /*
     * If this is called by parallel.c in a parallel worker, we don't want to
     * create a SERIALIZABLEXACT just yet.
     */
    if IsParallelWorker() {
        return;
    }

    /*
     * We do not allow SERIALIZABLE READ ONLY DEFERRABLE transactions to
     * import snapshots.
     */
    if XactReadOnly && XactDeferrable {
        ereport!(
            ERROR,
            errmsg!("a snapshot-importing transaction must not be READ ONLY DEFERRABLE")
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
        );
    }

    let _ = GetSerializableTransactionSnapshotInt(snapshot, sourcevxid, sourcepid);
}

/*
 * Guts of GetSerializableTransactionSnapshot
 */
unsafe fn GetSerializableTransactionSnapshotInt(
    snapshot: Snapshot,
    sourcevxid: *mut VirtualTransactionId,
    sourcepid: c_int,
) -> Snapshot {
    let mut snapshot = snapshot;
    let proc_: *mut PGPROC;
    let vxid: VirtualTransactionId = VirtualTransactionId {
        procNumber: 0,
        localTransactionId: 0,
    };
    let mut sxact: *mut SERIALIZABLEXACT;
    let mut othersxact: *mut SERIALIZABLEXACT;

    /* We only do this for serializable transactions.  Once. */
    Assert!(MySerializableXact == InvalidSerializableXact());

    Assert!(!RecoveryInProgress());

    /*
     * Since all parts of a serializable transaction must use the same
     * snapshot, it is too late to establish one after a parallel operation
     * has begun.
     */
    if IsInParallelMode() {
        elog!(ERROR, "cannot establish serializable snapshot during a parallel operation");
    }

    proc_ = MyProc;
    Assert!(!proc_.is_null());
    GET_VXID_FROM_PGPROC!(vxid, *proc_);

    /* #ifdef TEST_SUMMARIZE_SERIAL -- disabled by default */
    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);
    loop {
        sxact = CreatePredXact();
        /* If null, push out committed sxact to SLRU summary & retry. */
        if sxact.is_null() {
            LWLockRelease(SerializableXactHashLock());
            SummarizeOldestCommittedSxact();
            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);
        } else {
            break;
        }
    }

    /* Get the snapshot, or check that it's safe to use */
    if sourcevxid.is_null() {
        snapshot = GetSnapshotData(snapshot);
    } else if !ProcArrayInstallImportedXmin(snapshot_xmin(snapshot), sourcevxid) {
        ReleasePredXact(sxact);
        LWLockRelease(SerializableXactHashLock());
        ereport!(
            ERROR,
            errmsg!(
                "could not import the requested snapshot"
            )
            /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
               errdetail("The source process with PID {} is not running anymore.", sourcepid) */
        );
    }

    /*
     * If there are no serializable transactions which are not read-only,
     * we can "opt out" of predicate locking.
     */
    if XactReadOnly && (*PredXact).WritableSxactCount == 0 {
        ReleasePredXact(sxact);
        LWLockRelease(SerializableXactHashLock());
        return snapshot;
    }

    /* Initialize the structure. */
    ptr::copy_nonoverlapping(
        &vxid as *const VirtualTransactionId as *const u8,
        &mut (*sxact).vxid as *mut _ as *mut u8,
        size_of::<VirtualTransactionId>(),
    );
    (*sxact).SeqNo.lastCommitBeforeSnapshot = (*PredXact).LastSxactCommitSeqNo;
    (*sxact).prepareSeqNo = InvalidSerCommitSeqNo;
    (*sxact).commitSeqNo = InvalidSerCommitSeqNo;
    dlist_init(&mut (*sxact).outConflicts);
    dlist_init(&mut (*sxact).inConflicts);
    dlist_init(&mut (*sxact).possibleUnsafeConflicts);
    (*sxact).topXid = GetTopTransactionIdIfAny();
    (*sxact).finishedBefore = InvalidTransactionId;
    (*sxact).xmin = snapshot_xmin(snapshot);
    (*sxact).pid = MyProcPid;
    (*sxact).pgprocno = MyProcNumber;
    dlist_init(&mut (*sxact).predicateLocks);
    dlist_node_init(&mut (*sxact).finishedLink);
    (*sxact).flags = 0;
    if XactReadOnly {
        (*sxact).flags |= SXACT_FLAG_READ_ONLY;

        /*
         * Register all concurrent r/w transactions as possible conflicts.
         */
        let mut iter: dlist_iter = core::mem::zeroed();
        dlist_foreach!(iter, &mut (*PredXact).activeList, {
            othersxact = dlist_container!(
                SERIALIZABLEXACT,
                xactLink,
                iter.cur
            );

            if !SxactIsCommitted(othersxact)
                && !SxactIsDoomed(othersxact)
                && !SxactIsReadOnly(othersxact)
            {
                SetPossibleUnsafeConflict(sxact, othersxact);
            }
        });

        /*
         * If we didn't find any possibly unsafe conflicts, we can opt out.
         */
        if dlist_is_empty(&(*sxact).possibleUnsafeConflicts) {
            ReleasePredXact(sxact);
            LWLockRelease(SerializableXactHashLock());
            return snapshot;
        }
    } else {
        (*PredXact).WritableSxactCount += 1;
        Assert!(
            (*PredXact).WritableSxactCount
                <= (MaxBackends() + max_prepared_xacts)
        );
    }

    /* Maintain serializable global xmin info. */
    if !TransactionIdIsValid((*PredXact).SxactGlobalXmin) {
        Assert!((*PredXact).SxactGlobalXminCount == 0);
        (*PredXact).SxactGlobalXmin = snapshot_xmin(snapshot);
        (*PredXact).SxactGlobalXminCount = 1;
        SerialSetActiveSerXmin(snapshot_xmin(snapshot));
    } else if TransactionIdEquals(snapshot_xmin(snapshot), (*PredXact).SxactGlobalXmin) {
        Assert!((*PredXact).SxactGlobalXminCount > 0);
        (*PredXact).SxactGlobalXminCount += 1;
    } else {
        Assert!(TransactionIdFollows(snapshot_xmin(snapshot), (*PredXact).SxactGlobalXmin));
    }

    MySerializableXact = sxact;
    MyXactDidWrite = false; /* haven't written anything yet */

    LWLockRelease(SerializableXactHashLock());

    CreateLocalPredicateLockHash();

    snapshot
}

unsafe fn CreateLocalPredicateLockHash() {
    let mut hash_ctl: HASHCTL = core::mem::zeroed();

    /* Initialize the backend-local hash table of parent locks */
    Assert!(LocalPredicateLockHash.is_null());
    hash_ctl.keysize = size_of::<PREDICATELOCKTARGETTAG>() as Size;
    hash_ctl.entrysize = size_of::<LOCALPREDICATELOCK>() as Size;
    LocalPredicateLockHash = hash_create(
        "Local predicate lock",
        max_predicate_locks_per_xact,
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS,
    );
}

/*
 * Register the top level XID in SerializableXidHash.
 */
pub unsafe fn RegisterPredicateLockingXid(xid: TransactionId) {
    let mut sxidtag: SERIALIZABLEXIDTAG = SERIALIZABLEXIDTAG { xid: 0 };
    let sxid: *mut SERIALIZABLEXID;
    let mut found: bool = false;

    /*
     * If we're not tracking predicate lock data for this transaction, ignore.
     */
    if MySerializableXact == InvalidSerializableXact() {
        return;
    }

    /* We should have a valid XID and be at the top level. */
    Assert!(TransactionIdIsValid(xid));

    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

    /* This should only be done once per transaction. */
    Assert!((*MySerializableXact).topXid == InvalidTransactionId);

    (*MySerializableXact).topXid = xid;

    sxidtag.xid = xid;
    sxid = hash_search(
        SerializableXidHash,
        &sxidtag as *const SERIALIZABLEXIDTAG as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut SERIALIZABLEXID;
    Assert!(!found);

    /* Initialize the structure. */
    (*sxid).myXact = MySerializableXact;
    LWLockRelease(SerializableXactHashLock());
}

/*
 * Check whether there are any predicate locks held by any transaction
 * for the page at the given block number.
 */
pub unsafe fn PageIsPredicateLocked(relation: Relation, blkno: BlockNumber) -> bool {
    let mut targettag: PREDICATELOCKTARGETTAG = core::mem::zeroed();
    let targettaghash: uint32;
    let partitionLock: *mut LWLock;
    let target: *mut PREDICATELOCKTARGET;

    SET_PREDICATELOCKTARGETTAG_PAGE(
        &mut targettag,
        relation_rd_locator_dboid(relation),
        relation_rd_id(relation),
        blkno,
    );

    targettaghash = PredicateLockTargetTagHashCode(&targettag);
    partitionLock = PredicateLockHashPartitionLock(targettaghash);
    LWLockAcquire(partitionLock, LW_SHARED);
    target = hash_search_with_hash_value(
        PredicateLockTargetHash,
        &targettag as *const PREDICATELOCKTARGETTAG as *const c_void,
        targettaghash,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut PREDICATELOCKTARGET;
    LWLockRelease(partitionLock);

    !target.is_null()
}

/*
 * Check whether a particular lock is held by this transaction.
 */
unsafe fn PredicateLockExists(targettag: *const PREDICATELOCKTARGETTAG) -> bool {
    let lock: *mut LOCALPREDICATELOCK;

    /* check local hash table */
    lock = hash_search(
        LocalPredicateLockHash,
        targettag as *const c_void,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut LOCALPREDICATELOCK;

    if lock.is_null() {
        return false;
    }

    /*
     * Found entry in the table, but still need to check whether it's actually
     * held -- it could just be a parent of some held lock.
     */
    (*lock).held
}

/*
 * Return the parent lock tag in the lock hierarchy.
 */
unsafe fn GetParentPredicateLockTag(
    tag: *const PREDICATELOCKTARGETTAG,
    parent: *mut PREDICATELOCKTARGETTAG,
) -> bool {
    match GET_PREDICATELOCKTARGETTAG_TYPE(&*tag) {
        t if t == PREDLOCKTAG_RELATION => {
            /* relation locks have no parent lock */
            false
        }
        t if t == PREDLOCKTAG_PAGE => {
            /* parent lock is relation lock */
            SET_PREDICATELOCKTARGETTAG_RELATION(
                &mut *parent,
                GET_PREDICATELOCKTARGETTAG_DB(&*tag),
                GET_PREDICATELOCKTARGETTAG_RELATION(&*tag),
            );
            true
        }
        t if t == PREDLOCKTAG_TUPLE => {
            /* parent lock is page lock */
            SET_PREDICATELOCKTARGETTAG_PAGE(
                &mut *parent,
                GET_PREDICATELOCKTARGETTAG_DB(&*tag),
                GET_PREDICATELOCKTARGETTAG_RELATION(&*tag),
                GET_PREDICATELOCKTARGETTAG_PAGE(&*tag),
            );
            true
        }
        _ => {
            /* not reachable */
            Assert!(false);
            false
        }
    }
}

/*
 * Check whether the lock we are considering is already covered by a
 * coarser lock for our transaction.
 */
unsafe fn CoarserLockCovers(newtargettag: *const PREDICATELOCKTARGETTAG) -> bool {
    let mut targettag: PREDICATELOCKTARGETTAG = *newtargettag;
    let mut parenttag: PREDICATELOCKTARGETTAG = core::mem::zeroed();

    /* check parents iteratively until no more */
    while GetParentPredicateLockTag(&targettag, &mut parenttag) {
        targettag = parenttag;
        if PredicateLockExists(&targettag) {
            return true;
        }
    }

    /* no more parents to check; lock is not covered */
    false
}

/*
 * Remove the dummy entry from the predicate lock target hash.
 */
unsafe fn RemoveScratchTarget(lockheld: bool) {
    let mut found: bool = false;

    Assert!(LWLockHeldByMe(SerializablePredicateListLock()));

    if !lockheld {
        LWLockAcquire(ScratchPartitionLock, LW_EXCLUSIVE);
    }
    hash_search_with_hash_value(
        PredicateLockTargetHash,
        &ScratchTargetTag as *const PREDICATELOCKTARGETTAG as *const c_void,
        ScratchTargetTagHash,
        HASH_REMOVE,
        &mut found,
    );
    Assert!(found);
    if !lockheld {
        LWLockRelease(ScratchPartitionLock);
    }
}

/*
 * Re-insert the dummy entry in predicate lock target hash.
 */
unsafe fn RestoreScratchTarget(lockheld: bool) {
    let mut found: bool = false;

    Assert!(LWLockHeldByMe(SerializablePredicateListLock()));

    if !lockheld {
        LWLockAcquire(ScratchPartitionLock, LW_EXCLUSIVE);
    }
    hash_search_with_hash_value(
        PredicateLockTargetHash,
        &ScratchTargetTag as *const PREDICATELOCKTARGETTAG as *const c_void,
        ScratchTargetTagHash,
        HASH_ENTER,
        &mut found,
    );
    Assert!(!found);
    if !lockheld {
        LWLockRelease(ScratchPartitionLock);
    }
}

/*
 * Check whether the list of related predicate locks is empty for a
 * predicate lock target, and remove the target if it is.
 */
unsafe fn RemoveTargetIfNoLongerUsed(target: *mut PREDICATELOCKTARGET, targettaghash: uint32) {
    Assert!(LWLockHeldByMe(SerializablePredicateListLock()));

    /* Can't remove it until no locks at this target. */
    if !dlist_is_empty(&(*target).predicateLocks) {
        return;
    }

    /* Actually remove the target. */
    let rmtarget = hash_search_with_hash_value(
        PredicateLockTargetHash,
        &(*target).tag as *const PREDICATELOCKTARGETTAG as *const c_void,
        targettaghash,
        HASH_REMOVE,
        ptr::null_mut(),
    ) as *mut PREDICATELOCKTARGET;
    Assert!(rmtarget == target);
}

/*
 * Delete child target locks owned by this process.
 */
unsafe fn DeleteChildTargetLocks(newtargettag: *const PREDICATELOCKTARGETTAG) {
    let sxact: *mut SERIALIZABLEXACT;

    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED);
    sxact = MySerializableXact;
    if IsInParallelMode() {
        LWLockAcquire(&mut (*sxact).perXactPredicateListLock, LW_EXCLUSIVE);
    }

    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut (*sxact).predicateLocks, {
        let predlock = dlist_container!(
            PREDICATELOCK,
            xactLink,
            iter.cur
        );
        let oldlocktag: PREDICATELOCKTAG = core::ptr::read(&(*predlock).tag);
        Assert!(oldlocktag.myXact == sxact);
        let oldtarget: *mut PREDICATELOCKTARGET = oldlocktag.myTarget;
        let oldtargettag: PREDICATELOCKTARGETTAG = (*oldtarget).tag;

        if TargetTagIsCoveredBy(oldtargettag, *newtargettag) {
            let oldtargettaghash: uint32 = PredicateLockTargetTagHashCode(&oldtargettag);
            let partitionLock: *mut LWLock = PredicateLockHashPartitionLock(oldtargettaghash);

            LWLockAcquire(partitionLock, LW_EXCLUSIVE);

            dlist_delete(&mut (*predlock).xactLink);
            dlist_delete(&mut (*predlock).targetLink);
            let rmpredlock = hash_search_with_hash_value(
                PredicateLockHash,
                &oldlocktag as *const PREDICATELOCKTAG as *const c_void,
                PredicateLockHashCodeFromTargetHashCode(&oldlocktag, oldtargettaghash),
                HASH_REMOVE,
                ptr::null_mut(),
            ) as *mut PREDICATELOCK;
            Assert!(rmpredlock == predlock);

            RemoveTargetIfNoLongerUsed(oldtarget, oldtargettaghash);

            LWLockRelease(partitionLock);

            DecrementParentLocks(&oldtargettag);
        }
    });

    if IsInParallelMode() {
        LWLockRelease(&mut (*sxact).perXactPredicateListLock);
    }
    LWLockRelease(SerializablePredicateListLock());
}

/*
 * Returns the promotion limit for a given predicate lock target.
 */
unsafe fn MaxPredicateChildLocks(tag: *const PREDICATELOCKTARGETTAG) -> c_int {
    match GET_PREDICATELOCKTARGETTAG_TYPE(&*tag) {
        t if t == PREDLOCKTAG_RELATION => {
            if max_predicate_locks_per_relation < 0 {
                (max_predicate_locks_per_xact / (-max_predicate_locks_per_relation)) - 1
            } else {
                max_predicate_locks_per_relation
            }
        }
        t if t == PREDLOCKTAG_PAGE => max_predicate_locks_per_page,
        t if t == PREDLOCKTAG_TUPLE => {
            /*
             * not reachable: nothing is finer-granularity than a tuple, so we
             * should never try to promote to it.
             */
            Assert!(false);
            0
        }
        _ => {
            /* not reachable */
            Assert!(false);
            0
        }
    }
}

/*
 * For all ancestors of a newly-acquired predicate lock, increment
 * their child count in the parent hash table.
 */
unsafe fn CheckAndPromotePredicateLockRequest(reqtag: *const PREDICATELOCKTARGETTAG) -> bool {
    let mut targettag: PREDICATELOCKTARGETTAG = *reqtag;
    let mut nexttag: PREDICATELOCKTARGETTAG = core::mem::zeroed();
    let mut promotiontag: PREDICATELOCKTARGETTAG = core::mem::zeroed();
    let mut parentlock: *mut LOCALPREDICATELOCK;
    let mut found: bool = false;
    let mut promote: bool = false;

    /* check parents iteratively */
    while GetParentPredicateLockTag(&targettag, &mut nexttag) {
        targettag = nexttag;
        parentlock = hash_search(
            LocalPredicateLockHash,
            &targettag as *const PREDICATELOCKTARGETTAG as *const c_void,
            HASH_ENTER,
            &mut found,
        ) as *mut LOCALPREDICATELOCK;
        if !found {
            (*parentlock).held = false;
            (*parentlock).childLocks = 1;
        } else {
            (*parentlock).childLocks += 1;
        }

        if (*parentlock).childLocks > MaxPredicateChildLocks(&targettag) {
            /*
             * We should promote to this parent lock. Continue to check its
             * ancestors, however.
             */
            promotiontag = targettag;
            promote = true;
        }
    }

    if promote {
        /* acquire coarsest ancestor eligible for promotion */
        PredicateLockAcquire(&promotiontag);
        true
    } else {
        false
    }
}

/*
 * When releasing a lock, decrement the child count on all ancestor locks.
 */
unsafe fn DecrementParentLocks(targettag: *const PREDICATELOCKTARGETTAG) {
    let mut parenttag: PREDICATELOCKTARGETTAG = *targettag;
    let mut nexttag: PREDICATELOCKTARGETTAG = core::mem::zeroed();

    while GetParentPredicateLockTag(&parenttag, &mut nexttag) {
        let targettaghash: uint32;
        let parentlock: *mut LOCALPREDICATELOCK;

        parenttag = nexttag;
        targettaghash = PredicateLockTargetTagHashCode(&parenttag);
        parentlock = hash_search_with_hash_value(
            LocalPredicateLockHash,
            &parenttag as *const PREDICATELOCKTARGETTAG as *const c_void,
            targettaghash,
            HASH_FIND,
            ptr::null_mut(),
        ) as *mut LOCALPREDICATELOCK;

        /*
         * There's a small chance the parent lock doesn't exist in the lock
         * table.
         */
        if parentlock.is_null() {
            continue;
        }

        (*parentlock).childLocks -= 1;

        /*
         * Under similar circumstances the parent lock's refcount might be
         * zero.
         */
        if (*parentlock).childLocks < 0 {
            Assert!((*parentlock).held);
            (*parentlock).childLocks = 0;
        }

        if (*parentlock).childLocks == 0 && !(*parentlock).held {
            let rmlock = hash_search_with_hash_value(
                LocalPredicateLockHash,
                &parenttag as *const PREDICATELOCKTARGETTAG as *const c_void,
                targettaghash,
                HASH_REMOVE,
                ptr::null_mut(),
            ) as *mut LOCALPREDICATELOCK;
            Assert!(rmlock == parentlock);
        }
    }
}

/*
 * Indicate that a predicate lock on the given target is held by the
 * specified transaction.
 */
unsafe fn CreatePredicateLock(
    targettag: *const PREDICATELOCKTARGETTAG,
    targettaghash: uint32,
    sxact: *mut SERIALIZABLEXACT,
) {
    let target: *mut PREDICATELOCKTARGET;
    let locktag: PREDICATELOCKTAG;
    let lock: *mut PREDICATELOCK;
    let partitionLock: *mut LWLock;
    let mut found: bool = false;

    partitionLock = PredicateLockHashPartitionLock(targettaghash);

    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED);
    if IsInParallelMode() {
        LWLockAcquire(&mut (*sxact).perXactPredicateListLock, LW_EXCLUSIVE);
    }
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /* Make sure that the target is represented. */
    let target = hash_search_with_hash_value(
        PredicateLockTargetHash,
        targettag as *const c_void,
        targettaghash,
        HASH_ENTER_NULL,
        &mut found,
    ) as *mut PREDICATELOCKTARGET;
    if target.is_null() {
        ereport!(
            ERROR,
            errmsg!("out of shared memory")
            /* C also: errcode(ERRCODE_OUT_OF_MEMORY),
               errhint("You might need to increase \"max_pred_locks_per_transaction\".") */
        );
    }
    if !found {
        dlist_init(&mut (*target).predicateLocks);
    }

    /* We've got the sxact and target, make sure they're joined. */
    locktag = PREDICATELOCKTAG {
        myTarget: target,
        myXact: sxact,
    };
    let lock = hash_search_with_hash_value(
        PredicateLockHash,
        &locktag as *const PREDICATELOCKTAG as *const c_void,
        PredicateLockHashCodeFromTargetHashCode(&locktag, targettaghash),
        HASH_ENTER_NULL,
        &mut found,
    ) as *mut PREDICATELOCK;
    if lock.is_null() {
        ereport!(
            ERROR,
            errmsg!("out of shared memory")
            /* C also: errcode(ERRCODE_OUT_OF_MEMORY),
               errhint("You might need to increase \"max_pred_locks_per_transaction\".") */
        );
    }

    if !found {
        dlist_push_tail(&mut (*target).predicateLocks, &mut (*lock).targetLink);
        dlist_push_tail(&mut (*sxact).predicateLocks, &mut (*lock).xactLink);
        (*lock).commitSeqNo = InvalidSerCommitSeqNo;
    }

    LWLockRelease(partitionLock);
    if IsInParallelMode() {
        LWLockRelease(&mut (*sxact).perXactPredicateListLock);
    }
    LWLockRelease(SerializablePredicateListLock());
}

/*
 * Acquire a predicate lock on the specified target for the current
 * connection if not already held.
 */
unsafe fn PredicateLockAcquire(targettag: *const PREDICATELOCKTARGETTAG) {
    let targettaghash: uint32;
    let mut found: bool = false;
    let locallock: *mut LOCALPREDICATELOCK;

    /* Do we have the lock already, or a covering lock? */
    if PredicateLockExists(targettag) {
        return;
    }

    if CoarserLockCovers(targettag) {
        return;
    }

    /* the same hash and LW lock apply to the lock target and the local lock. */
    targettaghash = PredicateLockTargetTagHashCode(targettag);

    /* Acquire lock in local table */
    let locallock = hash_search_with_hash_value(
        LocalPredicateLockHash,
        targettag as *const c_void,
        targettaghash,
        HASH_ENTER,
        &mut found,
    ) as *mut LOCALPREDICATELOCK;
    (*locallock).held = true;
    if !found {
        (*locallock).childLocks = 0;
    }

    /* Actually create the lock */
    CreatePredicateLock(targettag, targettaghash, MySerializableXact);

    /*
     * Lock has been acquired. Check whether it should be promoted.
     */
    if CheckAndPromotePredicateLockRequest(targettag) {
        /*
         * Lock request was promoted to a coarser-granularity lock.
         */
    } else {
        /* Clean up any finer-granularity locks */
        if GET_PREDICATELOCKTARGETTAG_TYPE(&*targettag) != PREDLOCKTAG_TUPLE {
            DeleteChildTargetLocks(targettag);
        }
    }
}

/*
 * PredicateLockRelation -- Gets a predicate lock at the relation level.
 */
pub unsafe fn PredicateLockRelation(relation: Relation, snapshot: Snapshot) {
    let mut tag: PREDICATELOCKTARGETTAG = core::mem::zeroed();

    if !SerializationNeededForRead(relation, snapshot) {
        return;
    }

    SET_PREDICATELOCKTARGETTAG_RELATION(
        &mut tag,
        relation_rd_locator_dboid(relation),
        relation_rd_id(relation),
    );
    PredicateLockAcquire(&tag);
}

/*
 * PredicateLockPage -- Gets a predicate lock at the page level.
 */
pub unsafe fn PredicateLockPage(relation: Relation, blkno: BlockNumber, snapshot: Snapshot) {
    let mut tag: PREDICATELOCKTARGETTAG = core::mem::zeroed();

    if !SerializationNeededForRead(relation, snapshot) {
        return;
    }

    SET_PREDICATELOCKTARGETTAG_PAGE(
        &mut tag,
        relation_rd_locator_dboid(relation),
        relation_rd_id(relation),
        blkno,
    );
    PredicateLockAcquire(&tag);
}

/*
 * PredicateLockTID -- Gets a predicate lock at the tuple level.
 */
pub unsafe fn PredicateLockTID(
    relation: Relation,
    tid: ItemPointer,
    snapshot: Snapshot,
    tuple_xid: TransactionId,
) {
    let mut tag: PREDICATELOCKTARGETTAG = core::mem::zeroed();

    if !SerializationNeededForRead(relation, snapshot) {
        return;
    }

    /*
     * Return if this xact wrote it.
     */
    if relation_rd_index(relation).is_null() {
        /* If we wrote it; we already have a write lock. */
        if TransactionIdIsCurrentTransactionId(tuple_xid) {
            return;
        }
    }

    /*
     * Do quick-but-not-definitive test for a relation lock first.
     */
    SET_PREDICATELOCKTARGETTAG_RELATION(
        &mut tag,
        relation_rd_locator_dboid(relation),
        relation_rd_id(relation),
    );
    if PredicateLockExists(&tag) {
        return;
    }

    SET_PREDICATELOCKTARGETTAG_TUPLE(
        &mut tag,
        relation_rd_locator_dboid(relation),
        relation_rd_id(relation),
        ItemPointerGetBlockNumber(tid),
        ItemPointerGetOffsetNumber(tid),
    );
    PredicateLockAcquire(&tag);
}

/*
 * DeleteLockTarget -- Remove a predicate lock target along with any locks held for it.
 */
unsafe fn DeleteLockTarget(target: *mut PREDICATELOCKTARGET, targettaghash: uint32) {
    Assert!(LWLockHeldByMeInMode(SerializablePredicateListLock(), LW_EXCLUSIVE));
    Assert!(LWLockHeldByMe(PredicateLockHashPartitionLock(targettaghash)));

    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut (*target).predicateLocks, {
        let predlock = dlist_container!(
            PREDICATELOCK,
            targetLink,
            iter.cur
        );
        let mut found: bool = false;

        dlist_delete(&mut (*predlock).xactLink);
        dlist_delete(&mut (*predlock).targetLink);

        hash_search_with_hash_value(
            PredicateLockHash,
            &(*predlock).tag as *const PREDICATELOCKTAG as *const c_void,
            PredicateLockHashCodeFromTargetHashCode(&(*predlock).tag, targettaghash),
            HASH_REMOVE,
            &mut found,
        );
        Assert!(found);
    });
    LWLockRelease(SerializableXactHashLock());

    /* Remove the target itself, if possible. */
    RemoveTargetIfNoLongerUsed(target, targettaghash);
}

/*
 * TransferPredicateLocksToNewTarget
 *   Move or copy all the predicate locks for a lock target.
 */
unsafe fn TransferPredicateLocksToNewTarget(
    oldtargettag: PREDICATELOCKTARGETTAG,
    newtargettag: PREDICATELOCKTARGETTAG,
    removeOld: bool,
) -> bool {
    let oldtargettaghash: uint32;
    let oldpartitionLock: *mut LWLock;
    let newtargettaghash: uint32;
    let newpartitionLock: *mut LWLock;
    let mut found: bool = false;
    let mut outOfShmem: bool = false;

    Assert!(LWLockHeldByMeInMode(SerializablePredicateListLock(), LW_EXCLUSIVE));

    oldtargettaghash = PredicateLockTargetTagHashCode(&oldtargettag);
    newtargettaghash = PredicateLockTargetTagHashCode(&newtargettag);
    oldpartitionLock = PredicateLockHashPartitionLock(oldtargettaghash);
    newpartitionLock = PredicateLockHashPartitionLock(newtargettaghash);

    if removeOld {
        /*
         * Remove the dummy entry to give us scratch space.
         */
        RemoveScratchTarget(false);
    }

    /*
     * We must get the partition locks in ascending sequence to avoid deadlocks.
     */
    if (oldpartitionLock as usize) < (newpartitionLock as usize) {
        LWLockAcquire(
            oldpartitionLock,
            if removeOld { LW_EXCLUSIVE } else { LW_SHARED },
        );
        LWLockAcquire(newpartitionLock, LW_EXCLUSIVE);
    } else if (oldpartitionLock as usize) > (newpartitionLock as usize) {
        LWLockAcquire(newpartitionLock, LW_EXCLUSIVE);
        LWLockAcquire(
            oldpartitionLock,
            if removeOld { LW_EXCLUSIVE } else { LW_SHARED },
        );
    } else {
        LWLockAcquire(newpartitionLock, LW_EXCLUSIVE);
    }

    /*
     * Look for the old target.
     */
    let oldtarget = hash_search_with_hash_value(
        PredicateLockTargetHash,
        &oldtargettag as *const PREDICATELOCKTARGETTAG as *const c_void,
        oldtargettaghash,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut PREDICATELOCKTARGET;

    if !oldtarget.is_null() {
        let newtarget: *mut PREDICATELOCKTARGET;
        let mut newpredlocktag: PREDICATELOCKTAG;

        let newtarget = hash_search_with_hash_value(
            PredicateLockTargetHash,
            &newtargettag as *const PREDICATELOCKTARGETTAG as *const c_void,
            newtargettaghash,
            HASH_ENTER_NULL,
            &mut found,
        ) as *mut PREDICATELOCKTARGET;

        if newtarget.is_null() {
            /* Failed to allocate due to insufficient shmem */
            outOfShmem = true;
        } else {
            /* If we created a new entry, initialize it */
            if !found {
                dlist_init(&mut (*newtarget).predicateLocks);
            }

            newpredlocktag = PREDICATELOCKTAG {
                myTarget: newtarget,
                myXact: ptr::null_mut(),
            };

            /*
             * Loop through all the locks on the old target.
             */
            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

            let mut iter: dlist_mutable_iter = core::mem::zeroed();
            dlist_foreach_modify!(iter, &mut (*oldtarget).predicateLocks, {
                let oldpredlock = dlist_container!(
                    PREDICATELOCK,
                    targetLink,
                    iter.cur
                );
                let oldCommitSeqNo: SerCommitSeqNo = (*oldpredlock).commitSeqNo;

                newpredlocktag.myXact = (*oldpredlock).tag.myXact;

                if removeOld {
                    dlist_delete(&mut (*oldpredlock).xactLink);
                    dlist_delete(&mut (*oldpredlock).targetLink);

                    hash_search_with_hash_value(
                        PredicateLockHash,
                        &(*oldpredlock).tag as *const PREDICATELOCKTAG as *const c_void,
                        PredicateLockHashCodeFromTargetHashCode(
                            &(*oldpredlock).tag,
                            oldtargettaghash,
                        ),
                        HASH_REMOVE,
                        &mut found,
                    );
                    Assert!(found);
                }

                let newpredlock = hash_search_with_hash_value(
                    PredicateLockHash,
                    &newpredlocktag as *const PREDICATELOCKTAG as *const c_void,
                    PredicateLockHashCodeFromTargetHashCode(&newpredlocktag, newtargettaghash),
                    HASH_ENTER_NULL,
                    &mut found,
                ) as *mut PREDICATELOCK;
                if newpredlock.is_null() {
                    /* Out of shared memory. Undo what we've done so far. */
                    LWLockRelease(SerializableXactHashLock());
                    DeleteLockTarget(newtarget, newtargettaghash);
                    outOfShmem = true;
                    // break out of foreach via labeled block
                } else {
                    if !found {
                        dlist_push_tail(
                            &mut (*newtarget).predicateLocks,
                            &mut (*newpredlock).targetLink,
                        );
                        dlist_push_tail(
                            &mut (*newpredlocktag.myXact).predicateLocks,
                            &mut (*newpredlock).xactLink,
                        );
                        (*newpredlock).commitSeqNo = oldCommitSeqNo;
                    } else {
                        if (*newpredlock).commitSeqNo < oldCommitSeqNo {
                            (*newpredlock).commitSeqNo = oldCommitSeqNo;
                        }
                    }

                    Assert!((*newpredlock).commitSeqNo != 0);
                    Assert!(
                        (*newpredlock).commitSeqNo == InvalidSerCommitSeqNo
                            || (*newpredlock).tag.myXact == OldCommittedSxact
                    );
                }

                /* if outOfShmem was set, we need to stop the loop; handled below */
            });

            if !outOfShmem {
                LWLockRelease(SerializableXactHashLock());

                if removeOld {
                    Assert!(dlist_is_empty(&(*oldtarget).predicateLocks));
                    RemoveTargetIfNoLongerUsed(oldtarget, oldtargettaghash);
                }
            }
        }
    }

    /* Release partition locks in reverse order of acquisition. */
    if (oldpartitionLock as usize) < (newpartitionLock as usize) {
        LWLockRelease(newpartitionLock);
        LWLockRelease(oldpartitionLock);
    } else if (oldpartitionLock as usize) > (newpartitionLock as usize) {
        LWLockRelease(oldpartitionLock);
        LWLockRelease(newpartitionLock);
    } else {
        LWLockRelease(newpartitionLock);
    }

    if removeOld {
        /* We shouldn't run out of memory if we're moving locks */
        Assert!(!outOfShmem);

        /* Put the scratch entry back */
        RestoreScratchTarget(false);
    }

    !outOfShmem
}

/*
 * Drop all predicate locks of any granularity from the specified relation,
 * which can be a heap relation or an index relation.  If 'transfer' is true,
 * acquire a relation lock on the heap for any transactions with any lock(s)
 * on the specified relation.
 *
 * This requires grabbing a lot of LW locks and scanning the entire lock
 * target table for matches.
 */
unsafe fn DropAllPredicateLocksFromTable(relation: Relation, transfer: bool) {
    let mut seqstat: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut oldtarget: *mut PREDICATELOCKTARGET;
    let mut heaptarget: *mut PREDICATELOCKTARGET = ptr::null_mut();
    let dbId: Oid = relation_rd_locator_dboid(relation);
    let relId: Oid = relation_rd_id(relation);
    let heapId: Oid;
    let mut i: c_int;
    let isIndex: bool;
    let mut found: bool = false;
    let mut heaptargettaghash: uint32 = 0;

    /*
     * Bail out quickly if there are no serializable transactions running.
     * It's safe to check this without taking locks because the caller is
     * holding an ACCESS EXCLUSIVE lock on the relation.
     */
    if !TransactionIdIsValid((*PredXact).SxactGlobalXmin) {
        return;
    }

    if !PredicateLockingNeededForRelation(relation) {
        return;
    }

    if relation_rd_index(relation).is_null() {
        isIndex = false;
        heapId = relId;
    } else {
        isIndex = true;
        heapId = relation_rd_index_indrelid(relation);
    }
    Assert!(heapId != 0); /* != InvalidOid */
    Assert!(!transfer || !isIndex || true); /* index OID only makes sense with transfer */

    /* Retrieve first time needed, then keep. */
    heaptargettaghash = 0;
    heaptarget = ptr::null_mut();

    /* Acquire locks on all lock partitions */
    LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE);
    i = 0;
    while i < NUM_PREDICATELOCK_PARTITIONS {
        LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_EXCLUSIVE);
        i += 1;
    }
    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

    /*
     * Remove the dummy entry to give us scratch space, so we know we'll be
     * able to create the new lock target.
     */
    if transfer {
        RemoveScratchTarget(true);
    }

    /* Scan through target map */
    hash_seq_init(&mut seqstat, PredicateLockTargetHash);

    loop {
        oldtarget = hash_seq_search(&mut seqstat) as *mut PREDICATELOCKTARGET;
        if oldtarget.is_null() {
            break;
        }

        /*
         * Check whether this is a target which needs attention.
         */
        if GET_PREDICATELOCKTARGETTAG_RELATION(&(*oldtarget).tag) != relId {
            continue; /* wrong relation id */
        }
        if GET_PREDICATELOCKTARGETTAG_DB(&(*oldtarget).tag) != dbId {
            continue; /* wrong database id */
        }
        if transfer && !isIndex
            && GET_PREDICATELOCKTARGETTAG_TYPE(&(*oldtarget).tag) == PREDLOCKTAG_RELATION
        {
            continue; /* already the right lock */
        }

        /*
         * If we made it here, we have work to do.  We make sure the heap
         * relation lock exists, then we walk the list of predicate locks for
         * the old target we found, moving all locks to the heap relation lock
         * -- unless they already hold that.
         */

        /*
         * First make sure we have the heap relation target.  We only need to
         * do this once.
         */
        if transfer && heaptarget.is_null() {
            let mut heaptargettag: PREDICATELOCKTARGETTAG = core::mem::zeroed();
            SET_PREDICATELOCKTARGETTAG_RELATION(&mut heaptargettag, dbId, heapId);
            heaptargettaghash = PredicateLockTargetTagHashCode(&heaptargettag);
            heaptarget = hash_search_with_hash_value(
                PredicateLockTargetHash,
                &heaptargettag as *const PREDICATELOCKTARGETTAG as *const c_void,
                heaptargettaghash,
                HASH_ENTER,
                &mut found,
            ) as *mut PREDICATELOCKTARGET;
            if !found {
                dlist_init(&mut (*heaptarget).predicateLocks);
            }
        }

        /*
         * Loop through all the locks on the old target, replacing them with
         * locks on the new target.
         */
        let mut iter: dlist_mutable_iter = core::mem::zeroed();
        dlist_foreach_modify!(iter, &mut (*oldtarget).predicateLocks, {
            let oldpredlock = dlist_container!(
                PREDICATELOCK,
                targetLink,
                iter.cur
            );
            let oldCommitSeqNo: SerCommitSeqNo = (*oldpredlock).commitSeqNo;
            let oldXact: *mut SERIALIZABLEXACT = (*oldpredlock).tag.myXact;

            /*
             * Remove the old lock first. This avoids the chance of running
             * out of lock structure entries for the hash table.
             */
            dlist_delete(&mut (*oldpredlock).xactLink);

            /*
             * No need for retail delete from oldtarget list, we're removing
             * the whole target anyway.
             */
            hash_search(
                PredicateLockHash,
                &(*oldpredlock).tag as *const PREDICATELOCKTAG as *const c_void,
                HASH_REMOVE,
                &mut found,
            );
            Assert!(found);

            if transfer {
                let newpredlocktag: PREDICATELOCKTAG = PREDICATELOCKTAG {
                    myTarget: heaptarget,
                    myXact: oldXact,
                };
                let newpredlock = hash_search_with_hash_value(
                    PredicateLockHash,
                    &newpredlocktag as *const PREDICATELOCKTAG as *const c_void,
                    PredicateLockHashCodeFromTargetHashCode(&newpredlocktag, heaptargettaghash),
                    HASH_ENTER,
                    &mut found,
                ) as *mut PREDICATELOCK;
                if !found {
                    dlist_push_tail(
                        &mut (*heaptarget).predicateLocks,
                        &mut (*newpredlock).targetLink,
                    );
                    dlist_push_tail(
                        &mut (*newpredlocktag.myXact).predicateLocks,
                        &mut (*newpredlock).xactLink,
                    );
                    (*newpredlock).commitSeqNo = oldCommitSeqNo;
                } else {
                    if (*newpredlock).commitSeqNo < oldCommitSeqNo {
                        (*newpredlock).commitSeqNo = oldCommitSeqNo;
                    }
                }

                Assert!((*newpredlock).commitSeqNo != 0);
                Assert!(
                    (*newpredlock).commitSeqNo == InvalidSerCommitSeqNo
                        || (*newpredlock).tag.myXact == OldCommittedSxact
                );
            }
        });

        hash_search(
            PredicateLockTargetHash,
            &(*oldtarget).tag as *const PREDICATELOCKTARGETTAG as *const c_void,
            HASH_REMOVE,
            &mut found,
        );
        Assert!(found);
    }

    /* Put the scratch entry back */
    if transfer {
        RestoreScratchTarget(true);
    }

    /* Release locks in reverse order */
    LWLockRelease(SerializableXactHashLock());
    i = NUM_PREDICATELOCK_PARTITIONS - 1;
    while i >= 0 {
        LWLockRelease(PredicateLockHashPartitionLockByIndex(i));
        i -= 1;
    }
    LWLockRelease(SerializablePredicateListLock());
}

/*
 * TransferPredicateLocksToHeapRelation
 *   For all transactions, transfer all predicate locks for the given
 *   relation to a single relation lock on the heap.
 */
pub unsafe fn TransferPredicateLocksToHeapRelation(relation: Relation) {
    DropAllPredicateLocksFromTable(relation, true);
}

/*
 * PredicateLockPageSplit
 *
 * Copies any predicate locks for the old page to the new page.
 * Skip if this is a temporary table or toast table.
 *
 * NOTE: A page split (or overflow) affects all serializable transactions,
 * even if it occurs in the context of another transaction isolation level.
 */
pub unsafe fn PredicateLockPageSplit(
    relation: Relation,
    oldblkno: BlockNumber,
    newblkno: BlockNumber,
) {
    let mut oldtargettag: PREDICATELOCKTARGETTAG = core::mem::zeroed();
    let mut newtargettag: PREDICATELOCKTARGETTAG = core::mem::zeroed();
    let mut success: bool;

    /*
     * Bail out quickly if there are no serializable transactions running.
     *
     * It's safe to do this check without taking any additional locks. Even if
     * a serializable transaction starts concurrently, we know it can't take
     * any SIREAD locks on the page being split because the caller is holding
     * the associated buffer page lock.
     */
    if !TransactionIdIsValid((*PredXact).SxactGlobalXmin) {
        return;
    }

    if !PredicateLockingNeededForRelation(relation) {
        return;
    }

    Assert!(oldblkno != newblkno);
    /* BlockNumberIsValid checks */
    Assert!(oldblkno != InvalidBlockNumber);
    Assert!(newblkno != InvalidBlockNumber);

    SET_PREDICATELOCKTARGETTAG_PAGE(
        &mut oldtargettag,
        relation_rd_locator_dboid(relation),
        relation_rd_id(relation),
        oldblkno,
    );
    SET_PREDICATELOCKTARGETTAG_PAGE(
        &mut newtargettag,
        relation_rd_locator_dboid(relation),
        relation_rd_id(relation),
        newblkno,
    );

    LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE);

    /*
     * Try copying the locks over to the new page's tag, creating it if
     * necessary.
     */
    success = TransferPredicateLocksToNewTarget(oldtargettag, newtargettag, false);

    if !success {
        /*
         * No more predicate lock entries are available. Failure isn't an
         * option here, so promote the page lock to a relation lock.
         */

        /* Get the parent relation lock's lock tag */
        success = GetParentPredicateLockTag(&oldtargettag, &mut newtargettag);
        Assert!(success);

        /*
         * Move the locks to the parent. This shouldn't fail.
         *
         * Note that here we are removing locks held by other backends,
         * leading to a possible inconsistency in their local lock hash table.
         * This is OK because we're replacing it with a lock that covers the
         * old one.
         */
        success = TransferPredicateLocksToNewTarget(oldtargettag, newtargettag, true);
        Assert!(success);
    }

    LWLockRelease(SerializablePredicateListLock());
}

/*
 * PredicateLockPageCombine
 *
 * Combines predicate locks for two existing pages.
 * Skip if this is a temporary table or toast table.
 *
 * NOTE: A page combine affects all serializable transactions, even if it
 * occurs in the context of another transaction isolation level.
 */
pub unsafe fn PredicateLockPageCombine(
    relation: Relation,
    oldblkno: BlockNumber,
    newblkno: BlockNumber,
) {
    /*
     * Page combines differ from page splits in that we ought to be able to
     * remove the locks on the old page after transferring them to the new
     * page, instead of duplicating them.  However, because we can't edit
     * other backends' local lock tables, removing the old lock would leave
     * them with an entry in their LocalPredicateLockHash for a lock they're
     * not holding, which isn't acceptable.  So we wind up having to do the
     * same work as a page split, acquiring a lock on the new page and keeping
     * the old page locked too.
     */
    PredicateLockPageSplit(relation, oldblkno, newblkno);
}

/*
 * Walk the list of in-progress serializable transactions and find the new
 * xmin.
 */
unsafe fn SetNewSxactGlobalXmin() {
    Assert!(LWLockHeldByMe(SerializableXactHashLock()));

    (*PredXact).SxactGlobalXmin = InvalidTransactionId;
    (*PredXact).SxactGlobalXminCount = 0;

    let mut iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(iter, &mut (*PredXact).activeList, {
        let sxact = dlist_container!(
            SERIALIZABLEXACT,
            xactLink,
            iter.cur
        );

        if !SxactIsRolledBack(sxact)
            && !SxactIsCommitted(sxact)
            && sxact != OldCommittedSxact
        {
            Assert!(sxact != ptr::null_mut() && (*sxact).xmin != InvalidTransactionId);
            if !TransactionIdIsValid((*PredXact).SxactGlobalXmin)
                || TransactionIdPrecedes((*sxact).xmin, (*PredXact).SxactGlobalXmin)
            {
                (*PredXact).SxactGlobalXmin = (*sxact).xmin;
                (*PredXact).SxactGlobalXminCount = 1;
            } else if TransactionIdEquals((*sxact).xmin, (*PredXact).SxactGlobalXmin) {
                (*PredXact).SxactGlobalXminCount += 1;
            }
        }
    });

    SerialSetActiveSerXmin((*PredXact).SxactGlobalXmin);
}

/*
 * ReleasePredicateLocks
 *
 * Releases predicate locks based on completion of the current transaction,
 * whether committed or rolled back.  It can also be called for a read only
 * transaction when it becomes impossible for the transaction to become
 * part of a dangerous structure.
 */
pub unsafe fn ReleasePredicateLocks(mut isCommit: bool, isReadOnlySafe: bool) {
    let mut partiallyReleasing: bool = false;
    let mut needToClear: bool;
    let mut roXact: *mut SERIALIZABLEXACT;

    /*
     * We can't trust XactReadOnly here, because a transaction which started
     * as READ WRITE can show as READ ONLY later, e.g., within
     * subtransactions.
     */
    let topLevelIsDeclaredReadOnly: bool;

    /* We can't be both committing and releasing early due to RO_SAFE. */
    Assert!(!(isCommit && isReadOnlySafe));

    /* Are we at the end of a transaction, that is, a commit or abort? */
    if !isReadOnlySafe {
        /*
         * Parallel workers mustn't release predicate locks at the end of
         * their transaction. The leader will do that at the end of its
         * transaction.
         */
        if IsParallelWorker() {
            ReleasePredicateLocksLocal();
            return;
        }

        /*
         * By the time the leader in a parallel query reaches end of
         * transaction, it has waited for all workers to exit.
         */
        Assert!(!ParallelContextActive());

        /*
         * If the leader in a parallel query earlier stashed a partially
         * released SERIALIZABLEXACT for final clean-up at end of transaction
         * (because workers might still have been accessing it), then it's
         * time to restore it.
         */
        if SavedSerializableXact != InvalidSerializableXact() {
            Assert!(MySerializableXact == InvalidSerializableXact());
            MySerializableXact = SavedSerializableXact;
            SavedSerializableXact = InvalidSerializableXact();
            Assert!(SxactIsPartiallyReleased(MySerializableXact));
        }
    }

    if MySerializableXact == InvalidSerializableXact() {
        Assert!(LocalPredicateLockHash.is_null());
        return;
    }

    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

    /*
     * If the transaction is committing, but it has been partially released
     * already, then treat this as a roll back.  It was marked as rolled back.
     */
    if isCommit && SxactIsPartiallyReleased(MySerializableXact) {
        isCommit = false;
    }

    /*
     * If we're called in the middle of a transaction because we discovered
     * that the SXACT_FLAG_RO_SAFE flag was set, then we'll partially release
     * it (that is, release the predicate locks and conflicts, but not the
     * SERIALIZABLEXACT itself) if we're the first backend to have noticed.
     */
    if isReadOnlySafe && IsInParallelMode() {
        /*
         * The leader needs to stash a pointer to it, so that it can
         * completely release it at end-of-transaction.
         */
        if !IsParallelWorker() {
            SavedSerializableXact = MySerializableXact;
        }

        /*
         * The first backend to reach this condition will partially release
         * the SERIALIZABLEXACT.  All others will just clear their
         * backend-local state so that they stop doing SSI checks.
         */
        if SxactIsPartiallyReleased(MySerializableXact) {
            LWLockRelease(SerializableXactHashLock());
            ReleasePredicateLocksLocal();
            return;
        } else {
            (*MySerializableXact).flags |= SXACT_FLAG_PARTIALLY_RELEASED;
            partiallyReleasing = true;
            /* ... and proceed to perform the partial release below. */
        }
    }
    Assert!(!isCommit || SxactIsPrepared(MySerializableXact));
    Assert!(!isCommit || !SxactIsDoomed(MySerializableXact));
    Assert!(!SxactIsCommitted(MySerializableXact));
    Assert!(
        SxactIsPartiallyReleased(MySerializableXact)
            || !SxactIsRolledBack(MySerializableXact)
    );

    /* may not be serializable during COMMIT/ROLLBACK PREPARED */
    Assert!((*MySerializableXact).pid == 0 || IsolationIsSerializable());

    /* We'd better not already be on the cleanup list. */
    Assert!(!SxactIsOnFinishedList(MySerializableXact));

    topLevelIsDeclaredReadOnly = SxactIsReadOnly(MySerializableXact);

    /*
     * We don't hold XidGenLock lock here, assuming that TransactionId is
     * atomic!
     */
    (*MySerializableXact).finishedBefore =
        XidFromFullTransactionId((*TransamVariables).nextXid.value);

    /*
     * If it's not a commit it's either a rollback or a read-only transaction
     * flagged SXACT_FLAG_RO_SAFE, and we can clear our locks immediately.
     */
    if isCommit {
        (*MySerializableXact).flags |= SXACT_FLAG_COMMITTED;
        (*MySerializableXact).commitSeqNo = {
            (*PredXact).LastSxactCommitSeqNo += 1;
            (*PredXact).LastSxactCommitSeqNo
        };
        /* Recognize implicit read-only transaction (commit without write). */
        if !MyXactDidWrite {
            (*MySerializableXact).flags |= SXACT_FLAG_READ_ONLY;
        }
    } else {
        /*
         * The DOOMED flag indicates that we intend to roll back this
         * transaction and so it should not cause serialization failures for
         * other transactions that conflict with it.
         *
         * The ROLLED_BACK flag further indicates that ReleasePredicateLocks
         * has been called, and so the SerializableXact is eligible for
         * cleanup.
         */
        (*MySerializableXact).flags |= SXACT_FLAG_DOOMED;
        (*MySerializableXact).flags |= SXACT_FLAG_ROLLED_BACK;

        /*
         * If the transaction was previously prepared, but is now failing due
         * to a ROLLBACK PREPARED or (hopefully very rare) error after the
         * prepare, clear the prepared flag.
         */
        (*MySerializableXact).flags &= !SXACT_FLAG_PREPARED;
    }

    if !topLevelIsDeclaredReadOnly {
        Assert!((*PredXact).WritableSxactCount > 0);
        (*PredXact).WritableSxactCount -= 1;
        if (*PredXact).WritableSxactCount == 0 {
            /*
             * Release predicate locks and rw-conflicts in for all committed
             * transactions.
             */
            (*PredXact).CanPartialClearThrough = (*PredXact).LastSxactCommitSeqNo;
        }
    } else {
        /*
         * Read-only transactions: clear the list of transactions that might
         * make us unsafe. Note that we use 'inLink' for the iteration as
         * opposed to 'outLink' for the r/w xacts.
         */
        let mut iter: dlist_mutable_iter = core::mem::zeroed();
        dlist_foreach_modify!(iter, &mut (*MySerializableXact).possibleUnsafeConflicts, {
            let possibleUnsafeConflict = dlist_container!(
                RWConflictData,
                inLink,
                iter.cur
            );

            Assert!(!SxactIsReadOnly((*possibleUnsafeConflict).sxactOut));
            Assert!(MySerializableXact == (*possibleUnsafeConflict).sxactIn);

            ReleaseRWConflict(possibleUnsafeConflict);
        });
    }

    /* Check for conflict out to old committed transactions. */
    if isCommit
        && !SxactIsReadOnly(MySerializableXact)
        && SxactHasSummaryConflictOut(MySerializableXact)
    {
        /*
         * we don't know which old committed transaction we conflicted with,
         * so be conservative and use FirstNormalSerCommitSeqNo here
         */
        (*MySerializableXact).SeqNo.earliestOutConflictCommit =
            FirstNormalSerCommitSeqNo;
        (*MySerializableXact).flags |= SXACT_FLAG_CONFLICT_OUT;
    }

    /*
     * Release all outConflicts to committed transactions.  If we're rolling
     * back clear them all.  Set SXACT_FLAG_CONFLICT_OUT if any point to
     * previously committed transactions.
     */
    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut (*MySerializableXact).outConflicts, {
        let conflict = dlist_container!(
            RWConflictData,
            outLink,
            iter.cur
        );

        if isCommit
            && !SxactIsReadOnly(MySerializableXact)
            && SxactIsCommitted((*conflict).sxactIn)
        {
            if ((*MySerializableXact).flags & SXACT_FLAG_CONFLICT_OUT) == 0
                || (*(*conflict).sxactIn).prepareSeqNo
                    < (*MySerializableXact).SeqNo.earliestOutConflictCommit
            {
                (*MySerializableXact).SeqNo.earliestOutConflictCommit =
                    (*(*conflict).sxactIn).prepareSeqNo;
            }
            (*MySerializableXact).flags |= SXACT_FLAG_CONFLICT_OUT;
        }

        if !isCommit
            || SxactIsCommitted((*conflict).sxactIn)
            || ((*(*conflict).sxactIn).SeqNo.lastCommitBeforeSnapshot
                >= (*PredXact).LastSxactCommitSeqNo)
        {
            ReleaseRWConflict(conflict);
        }
    });

    /*
     * Release all inConflicts from committed and read-only transactions. If
     * we're rolling back, clear them all.
     */
    dlist_foreach_modify!(iter, &mut (*MySerializableXact).inConflicts, {
        let conflict = dlist_container!(
            RWConflictData,
            inLink,
            iter.cur
        );

        if !isCommit
            || SxactIsCommitted((*conflict).sxactOut)
            || SxactIsReadOnly((*conflict).sxactOut)
        {
            ReleaseRWConflict(conflict);
        }
    });

    if !topLevelIsDeclaredReadOnly {
        /*
         * Remove ourselves from the list of possible conflicts for concurrent
         * READ ONLY transactions, flagging them as unsafe if we have a
         * conflict out.
         */
        let mut iter: dlist_mutable_iter = core::mem::zeroed();
        dlist_foreach_modify!(iter, &mut (*MySerializableXact).possibleUnsafeConflicts, {
            let possibleUnsafeConflict = dlist_container!(
                RWConflictData,
                outLink,
                iter.cur
            );

            roXact = (*possibleUnsafeConflict).sxactIn;
            Assert!(MySerializableXact == (*possibleUnsafeConflict).sxactOut);
            Assert!(SxactIsReadOnly(roXact));

            /* Mark conflicted if necessary. */
            if isCommit
                && MyXactDidWrite
                && SxactHasConflictOut(MySerializableXact)
                && ((*MySerializableXact).SeqNo.earliestOutConflictCommit
                    <= (*roXact).SeqNo.lastCommitBeforeSnapshot)
            {
                /*
                 * This releases possibleUnsafeConflict (as well as all other
                 * possible conflicts for roXact)
                 */
                FlagSxactUnsafe(roXact);
            } else {
                ReleaseRWConflict(possibleUnsafeConflict);

                /*
                 * If we were the last possible conflict, flag it safe.
                 */
                if dlist_is_empty(&(*roXact).possibleUnsafeConflicts) {
                    (*roXact).flags |= SXACT_FLAG_RO_SAFE;
                }
            }

            /*
             * Wake up the process for a waiting DEFERRABLE transaction if we
             * now know it's either safe or conflicted.
             */
            if SxactIsDeferrableWaiting(roXact)
                && (SxactIsROUnsafe(roXact) || SxactIsROSafe(roXact))
            {
                ProcSendSignal((*roXact).pgprocno);
            }
        });
    }

    /*
     * Check whether it's time to clean up old transactions.
     */
    needToClear = false;
    if (partiallyReleasing || !SxactIsPartiallyReleased(MySerializableXact))
        && TransactionIdEquals((*MySerializableXact).xmin, (*PredXact).SxactGlobalXmin)
    {
        Assert!((*PredXact).SxactGlobalXminCount > 0);
        (*PredXact).SxactGlobalXminCount -= 1;
        if (*PredXact).SxactGlobalXminCount == 0 {
            SetNewSxactGlobalXmin();
            needToClear = true;
        }
    }

    LWLockRelease(SerializableXactHashLock());

    LWLockAcquire(SerializableFinishedListLock(), LW_EXCLUSIVE);

    /* Add this to the list of transactions to check for later cleanup. */
    if isCommit {
        dlist_push_tail(
            &mut *FinishedSerializableTransactions,
            &mut (*MySerializableXact).finishedLink,
        );
    }

    /*
     * If we're releasing a RO_SAFE transaction in parallel mode, we'll only
     * partially release it.
     */
    if !isCommit {
        ReleaseOneSerializableXact(
            MySerializableXact,
            isReadOnlySafe && IsInParallelMode(),
            false,
        );
    }

    LWLockRelease(SerializableFinishedListLock());

    if needToClear {
        ClearOldPredicateLocks();
    }

    ReleasePredicateLocksLocal();
}

unsafe fn ReleasePredicateLocksLocal() {
    MySerializableXact = InvalidSerializableXact();
    MyXactDidWrite = false;

    /* Delete per-transaction lock table */
    if !LocalPredicateLockHash.is_null() {
        hash_destroy(LocalPredicateLockHash);
        LocalPredicateLockHash = ptr::null_mut();
    }
}

/*
 * Clear old predicate locks, belonging to committed transactions that are no
 * longer interesting to any in-progress transaction.
 */
unsafe fn ClearOldPredicateLocks() {
    /*
     * Loop through finished transactions. They are in commit order, so we can
     * stop as soon as we find one that's still interesting.
     */
    LWLockAcquire(SerializableFinishedListLock(), LW_EXCLUSIVE);
    LWLockAcquire(SerializableXactHashLock(), LW_SHARED);

    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut *FinishedSerializableTransactions, {
        let finishedSxact = dlist_container!(
            SERIALIZABLEXACT,
            finishedLink,
            iter.cur
        );

        if !TransactionIdIsValid((*PredXact).SxactGlobalXmin)
            || TransactionIdPrecedesOrEquals(
                (*finishedSxact).finishedBefore,
                (*PredXact).SxactGlobalXmin,
            )
        {
            /*
             * This transaction committed before any in-progress transaction
             * took its snapshot. It's no longer interesting.
             */
            LWLockRelease(SerializableXactHashLock());
            dlist_delete_thoroughly(&mut (*finishedSxact).finishedLink);
            ReleaseOneSerializableXact(finishedSxact, false, false);
            LWLockAcquire(SerializableXactHashLock(), LW_SHARED);
        } else if (*finishedSxact).commitSeqNo > (*PredXact).HavePartialClearedThrough
            && (*finishedSxact).commitSeqNo <= (*PredXact).CanPartialClearThrough
        {
            /*
             * Any active transactions that took their snapshot before this
             * transaction committed are read-only, so we can clear part of
             * its state.
             */
            LWLockRelease(SerializableXactHashLock());

            if SxactIsReadOnly(finishedSxact) {
                /* A read-only transaction can be removed entirely */
                dlist_delete_thoroughly(&mut (*finishedSxact).finishedLink);
                ReleaseOneSerializableXact(finishedSxact, false, false);
            } else {
                /*
                 * A read-write transaction can only be partially cleared. We
                 * need to keep the SERIALIZABLEXACT but can release the
                 * SIREAD locks and conflicts in.
                 */
                ReleaseOneSerializableXact(finishedSxact, true, false);
            }

            (*PredXact).HavePartialClearedThrough = (*finishedSxact).commitSeqNo;
            LWLockAcquire(SerializableXactHashLock(), LW_SHARED);
        } else {
            /* Still interesting. */
            break;
        }
    });

    LWLockRelease(SerializableXactHashLock());

    /*
     * Loop through predicate locks on dummy transaction for summarized data.
     */
    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED);

    dlist_foreach_modify!(iter, &mut (*OldCommittedSxact).predicateLocks, {
        let predlock = dlist_container!(
            PREDICATELOCK,
            xactLink,
            iter.cur
        );
        let canDoPartialCleanup: bool;

        LWLockAcquire(SerializableXactHashLock(), LW_SHARED);
        Assert!((*predlock).commitSeqNo != 0);
        Assert!((*predlock).commitSeqNo != InvalidSerCommitSeqNo);
        canDoPartialCleanup = (*predlock).commitSeqNo <= (*PredXact).CanPartialClearThrough;
        LWLockRelease(SerializableXactHashLock());

        /*
         * If this lock originally belonged to an old enough transaction, we
         * can release it.
         */
        if canDoPartialCleanup {
            let tag: PREDICATELOCKTAG = core::ptr::read(&(*predlock).tag);
            let target: *mut PREDICATELOCKTARGET = tag.myTarget;
            let targettag: PREDICATELOCKTARGETTAG = (*target).tag;
            let targettaghash: uint32 = PredicateLockTargetTagHashCode(&targettag);
            let partitionLock: *mut LWLock = PredicateLockHashPartitionLock(targettaghash);

            LWLockAcquire(partitionLock, LW_EXCLUSIVE);

            dlist_delete(&mut (*predlock).targetLink);
            dlist_delete(&mut (*predlock).xactLink);

            hash_search_with_hash_value(
                PredicateLockHash,
                &tag as *const PREDICATELOCKTAG as *const c_void,
                PredicateLockHashCodeFromTargetHashCode(&tag, targettaghash),
                HASH_REMOVE,
                ptr::null_mut(),
            );
            RemoveTargetIfNoLongerUsed(target, targettaghash);

            LWLockRelease(partitionLock);
        }
    });

    LWLockRelease(SerializablePredicateListLock());
    LWLockRelease(SerializableFinishedListLock());
}

/*
 * This is the normal way to delete anything from any of the predicate
 * locking hash tables.
 *
 * When the partial flag is set, we can release all predicate locks and
 * in-conflict information but keep the transaction entry itself and any
 * outConflicts.
 *
 * When the summarize flag is set, we've run short of room for sxact data
 * and must summarize to the SLRU.
 */
unsafe fn ReleaseOneSerializableXact(
    sxact: *mut SERIALIZABLEXACT,
    partial: bool,
    summarize: bool,
) {
    let mut sxidtag: SERIALIZABLEXIDTAG = SERIALIZABLEXIDTAG { xid: 0 };

    Assert!(!sxact.is_null());
    Assert!(SxactIsRolledBack(sxact) || SxactIsCommitted(sxact));
    Assert!(partial || !SxactIsOnFinishedList(sxact));
    Assert!(LWLockHeldByMe(SerializableFinishedListLock()));

    /*
     * First release all the predicate locks held by this xact (or transfer
     * them to OldCommittedSxact if summarize is true)
     */
    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED);
    if IsInParallelMode() {
        LWLockAcquire(&mut (*sxact).perXactPredicateListLock, LW_EXCLUSIVE);
    }

    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut (*sxact).predicateLocks, {
        let predlock = dlist_container!(
            PREDICATELOCK,
            xactLink,
            iter.cur
        );
        let tag: PREDICATELOCKTAG = core::ptr::read(&(*predlock).tag);
        let target: *mut PREDICATELOCKTARGET = tag.myTarget;
        let targettag: PREDICATELOCKTARGETTAG = (*target).tag;
        let targettaghash: uint32 = PredicateLockTargetTagHashCode(&targettag);
        let partitionLock: *mut LWLock = PredicateLockHashPartitionLock(targettaghash);

        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        dlist_delete(&mut (*predlock).targetLink);

        hash_search_with_hash_value(
            PredicateLockHash,
            &tag as *const PREDICATELOCKTAG as *const c_void,
            PredicateLockHashCodeFromTargetHashCode(&tag, targettaghash),
            HASH_REMOVE,
            ptr::null_mut(),
        );

        if summarize {
            let mut found: bool = false;

            /* Fold into dummy transaction list. */
            let mut newtag: PREDICATELOCKTAG = tag;
            newtag.myXact = OldCommittedSxact;
            let newpredlock = hash_search_with_hash_value(
                PredicateLockHash,
                &newtag as *const PREDICATELOCKTAG as *const c_void,
                PredicateLockHashCodeFromTargetHashCode(&newtag, targettaghash),
                HASH_ENTER_NULL,
                &mut found,
            ) as *mut PREDICATELOCK;

            if newpredlock.is_null() {
                ereport!(
                    ERROR,
                    errmsg!("out of shared memory")
                    /* C also: errcode(ERRCODE_OUT_OF_MEMORY),
                       errhint("You might need to increase \"max_pred_locks_per_transaction\".") */
                );
            }

            if found {
                Assert!((*newpredlock).commitSeqNo != 0);
                Assert!((*newpredlock).commitSeqNo != InvalidSerCommitSeqNo);
                if (*newpredlock).commitSeqNo < (*sxact).commitSeqNo {
                    (*newpredlock).commitSeqNo = (*sxact).commitSeqNo;
                }
            } else {
                dlist_push_tail(
                    &mut (*target).predicateLocks,
                    &mut (*newpredlock).targetLink,
                );
                dlist_push_tail(
                    &mut (*OldCommittedSxact).predicateLocks,
                    &mut (*newpredlock).xactLink,
                );
                (*newpredlock).commitSeqNo = (*sxact).commitSeqNo;
            }
        } else {
            RemoveTargetIfNoLongerUsed(target, targettaghash);
        }

        LWLockRelease(partitionLock);
    });

    /*
     * Rather than retail removal, just re-init the head after we've run
     * through the list.
     */
    dlist_init(&mut (*sxact).predicateLocks);

    if IsInParallelMode() {
        LWLockRelease(&mut (*sxact).perXactPredicateListLock);
    }
    LWLockRelease(SerializablePredicateListLock());

    sxidtag.xid = (*sxact).topXid;
    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

    /* Release all outConflicts (unless 'partial' is true) */
    if !partial {
        let mut iter: dlist_mutable_iter = core::mem::zeroed();
        dlist_foreach_modify!(iter, &mut (*sxact).outConflicts, {
            let conflict = dlist_container!(
                RWConflictData,
                outLink,
                iter.cur
            );

            if summarize {
                (*(*conflict).sxactIn).flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
            }
            ReleaseRWConflict(conflict);
        });
    }

    /* Release all inConflicts. */
    dlist_foreach_modify!(iter, &mut (*sxact).inConflicts, {
        let conflict = dlist_container!(
            RWConflictData,
            inLink,
            iter.cur
        );

        if summarize {
            (*(*conflict).sxactOut).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
        }
        ReleaseRWConflict(conflict);
    });

    /* Finally, get rid of the xid and the record of the transaction itself. */
    if !partial {
        if sxidtag.xid != InvalidTransactionId {
            hash_search(
                SerializableXidHash,
                &sxidtag as *const SERIALIZABLEXIDTAG as *const c_void,
                HASH_REMOVE,
                ptr::null_mut(),
            );
        }
        ReleasePredXact(sxact);
    }

    LWLockRelease(SerializableXactHashLock());
}

/*
 * Tests whether the given top level transaction is concurrent with
 * (overlaps) our current transaction.
 */
unsafe fn XidIsConcurrent(xid: TransactionId) -> bool {
    let snap: Snapshot;

    Assert!(TransactionIdIsValid(xid));
    Assert!(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));

    snap = GetTransactionSnapshot();

    if TransactionIdPrecedes(xid, snapshot_xmin(snap)) {
        return false;
    }

    if TransactionIdFollowsOrEquals(xid, snapshot_xmax(snap)) {
        return true;
    }

    pg_lfind32(xid, snapshot_xip(snap), snapshot_xcnt(snap))
}

pub unsafe fn CheckForSerializableConflictOutNeeded(
    relation: Relation,
    snapshot: Snapshot,
) -> bool {
    if !SerializationNeededForRead(relation, snapshot) {
        return false;
    }

    /* Check if someone else has already decided that we need to die */
    if SxactIsDoomed(MySerializableXact) {
        ereport!(
            ERROR,
            errmsg!(
                "could not serialize access due to read/write dependencies among transactions"
            )
            /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
               errdetail_internal("Reason code: Canceled on identification as a pivot, during conflict out checking."),
               errhint("The transaction might succeed if retried.") */
        );
    }

    true
}

/*
 * CheckForSerializableConflictOut
 *   A table AM is reading a tuple that has been modified.
 */
pub unsafe fn CheckForSerializableConflictOut(
    relation: Relation,
    xid: TransactionId,
    snapshot: Snapshot,
) {
    let mut sxidtag: SERIALIZABLEXIDTAG = SERIALIZABLEXIDTAG { xid: 0 };
    let sxid: *mut SERIALIZABLEXID;
    let sxact: *mut SERIALIZABLEXACT;

    if !SerializationNeededForRead(relation, snapshot) {
        return;
    }

    /* Check if someone else has already decided that we need to die */
    if SxactIsDoomed(MySerializableXact) {
        ereport!(
            ERROR,
            errmsg!(
                "could not serialize access due to read/write dependencies among transactions"
            )
            /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
               errdetail_internal("Reason code: Canceled on identification as a pivot, during conflict out checking."),
               errhint("The transaction might succeed if retried.") */
        );
    }
    Assert!(TransactionIdIsValid(xid));

    if TransactionIdEquals(xid, GetTopTransactionIdIfAny()) {
        return;
    }

    /*
     * Find sxact or summarized info for the top level xid.
     */
    sxidtag.xid = xid;
    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);
    sxid = hash_search(
        SerializableXidHash,
        &sxidtag as *const SERIALIZABLEXIDTAG as *const c_void,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut SERIALIZABLEXID;

    if sxid.is_null() {
        /*
         * Transaction not found in "normal" SSI structures.  Check whether it
         * got pushed out to SLRU storage for "old committed" transactions.
         */
        let conflictCommitSeqNo: SerCommitSeqNo = SerialGetMinConflictCommitSeqNo(xid);
        if conflictCommitSeqNo != 0 {
            if conflictCommitSeqNo != InvalidSerCommitSeqNo
                && (!SxactIsReadOnly(MySerializableXact)
                    || conflictCommitSeqNo
                        <= (*MySerializableXact).SeqNo.lastCommitBeforeSnapshot)
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not serialize access due to read/write dependencies among transactions"
                    )
                    /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                       errdetail_internal("Reason code: Canceled on conflict out to old pivot {}.", xid),
                       errhint("The transaction might succeed if retried.") */
                );
            }

            if SxactHasSummaryConflictIn(MySerializableXact)
                || !dlist_is_empty(&(*MySerializableXact).inConflicts)
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not serialize access due to read/write dependencies among transactions"
                    )
                    /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                       errdetail_internal("Reason code: Canceled on identification as a pivot, with conflict out to old committed transaction {}.", xid),
                       errhint("The transaction might succeed if retried.") */
                );
            }

            (*MySerializableXact).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
        }

        /* It's not serializable or otherwise not important. */
        LWLockRelease(SerializableXactHashLock());
        return;
    }

    sxact = (*sxid).myXact;
    Assert!(TransactionIdEquals((*sxact).topXid, xid));
    if sxact == MySerializableXact || SxactIsDoomed(sxact) {
        /* Can't conflict with ourself or a transaction that will roll back. */
        LWLockRelease(SerializableXactHashLock());
        return;
    }

    /*
     * We have a conflict out to a transaction which has a conflict out to a
     * summarized transaction.
     */
    if SxactHasSummaryConflictOut(sxact) {
        if !SxactIsPrepared(sxact) {
            (*sxact).flags |= SXACT_FLAG_DOOMED;
            LWLockRelease(SerializableXactHashLock());
            return;
        } else {
            LWLockRelease(SerializableXactHashLock());
            ereport!(
                ERROR,
                errmsg!(
                    "could not serialize access due to read/write dependencies among transactions"
                )
                /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                   errdetail_internal("Reason code: Canceled on conflict out to old pivot."),
                   errhint("The transaction might succeed if retried.") */
            );
        }
    }

    /*
     * If this is a read-only transaction and the writing transaction has
     * committed, and it doesn't have a rw-conflict to a transaction which
     * committed before it, no conflict.
     */
    if SxactIsReadOnly(MySerializableXact)
        && SxactIsCommitted(sxact)
        && !SxactHasSummaryConflictOut(sxact)
        && (!SxactHasConflictOut(sxact)
            || (*MySerializableXact).SeqNo.lastCommitBeforeSnapshot
                < (*sxact).SeqNo.earliestOutConflictCommit)
    {
        /* Read-only transaction will appear to run first.  No conflict. */
        LWLockRelease(SerializableXactHashLock());
        return;
    }

    if !XidIsConcurrent(xid) {
        /* This write was already in our snapshot; no conflict. */
        LWLockRelease(SerializableXactHashLock());
        return;
    }

    if RWConflictExists(MySerializableXact, sxact) {
        /* We don't want duplicate conflict records in the list. */
        LWLockRelease(SerializableXactHashLock());
        return;
    }

    /*
     * Flag the conflict.  But first, if this conflict creates a dangerous
     * structure, ereport an error.
     */
    FlagRWConflict(MySerializableXact, sxact);
    LWLockRelease(SerializableXactHashLock());
}

/*
 * Check a particular target for rw-dependency conflict in. A subroutine of
 * CheckForSerializableConflictIn().
 */
unsafe fn CheckTargetForConflictsIn(targettag: *mut PREDICATELOCKTARGETTAG) {
    let targettaghash: uint32;
    let partitionLock: *mut LWLock;
    let target: *mut PREDICATELOCKTARGET;
    let mut mypredlock: *mut PREDICATELOCK = ptr::null_mut();
    let mut mypredlocktag: PREDICATELOCKTAG = core::mem::zeroed();

    Assert!(MySerializableXact != InvalidSerializableXact());

    /*
     * The same hash and LW lock apply to the lock target and the lock itself.
     */
    targettaghash = PredicateLockTargetTagHashCode(targettag);
    partitionLock = PredicateLockHashPartitionLock(targettaghash);
    LWLockAcquire(partitionLock, LW_SHARED);
    target = hash_search_with_hash_value(
        PredicateLockTargetHash,
        targettag as *const c_void,
        targettaghash,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut PREDICATELOCKTARGET;

    if target.is_null() {
        /* Nothing has this target locked; we're done here. */
        LWLockRelease(partitionLock);
        return;
    }

    /*
     * Each lock for an overlapping transaction represents a conflict: a
     * rw-dependency in to this transaction.
     */
    LWLockAcquire(SerializableXactHashLock(), LW_SHARED);

    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut (*target).predicateLocks, {
        let predlock = dlist_container!(
            PREDICATELOCK,
            targetLink,
            iter.cur
        );
        let sxact: *mut SERIALIZABLEXACT = (*predlock).tag.myXact;

        if sxact == MySerializableXact {
            /*
             * If we're getting a write lock on a tuple, we don't need a
             * predicate (SIREAD) lock on the same tuple. We can safely remove
             * our SIREAD lock, but we'll defer doing so until after the loop.
             *
             * We can't use this optimization within a subtransaction.
             */
            if !IsSubTransaction()
                && GET_PREDICATELOCKTARGETTAG_OFFSET(&*targettag) != 0
            {
                mypredlock = predlock;
                mypredlocktag = core::ptr::read(&(*predlock).tag);
            }
        } else if !SxactIsDoomed(sxact)
            && (!SxactIsCommitted(sxact)
                || TransactionIdPrecedes(
                    snapshot_xmin(GetTransactionSnapshot()),
                    (*sxact).finishedBefore,
                ))
            && !RWConflictExists(sxact, MySerializableXact)
        {
            LWLockRelease(SerializableXactHashLock());
            LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

            /*
             * Re-check after getting exclusive lock because the other
             * transaction may have flagged a conflict.
             */
            if !SxactIsDoomed(sxact)
                && (!SxactIsCommitted(sxact)
                    || TransactionIdPrecedes(
                        snapshot_xmin(GetTransactionSnapshot()),
                        (*sxact).finishedBefore,
                    ))
                && !RWConflictExists(sxact, MySerializableXact)
            {
                FlagRWConflict(sxact, MySerializableXact);
            }

            LWLockRelease(SerializableXactHashLock());
            LWLockAcquire(SerializableXactHashLock(), LW_SHARED);
        }
    });

    LWLockRelease(SerializableXactHashLock());
    LWLockRelease(partitionLock);

    /*
     * If we found one of our own SIREAD locks to remove, remove it now.
     */
    if !mypredlock.is_null() {
        let predlockhashcode: uint32;
        let rmpredlock: *mut PREDICATELOCK;

        LWLockAcquire(SerializablePredicateListLock(), LW_SHARED);
        if IsInParallelMode() {
            LWLockAcquire(
                &mut (*MySerializableXact).perXactPredicateListLock,
                LW_EXCLUSIVE,
            );
        }
        LWLockAcquire(partitionLock, LW_EXCLUSIVE);
        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

        /*
         * Remove the predicate lock from shared memory, if it wasn't removed
         * while the locks were released.
         */
        predlockhashcode =
            PredicateLockHashCodeFromTargetHashCode(&mypredlocktag, targettaghash);
        let found_rmpredlock = hash_search_with_hash_value(
            PredicateLockHash,
            &mypredlocktag as *const PREDICATELOCKTAG as *const c_void,
            predlockhashcode,
            HASH_FIND,
            ptr::null_mut(),
        ) as *mut PREDICATELOCK;
        rmpredlock = found_rmpredlock;

        if !rmpredlock.is_null() {
            Assert!(rmpredlock == mypredlock);

            dlist_delete(&mut (*mypredlock).targetLink);
            dlist_delete(&mut (*mypredlock).xactLink);

            hash_search_with_hash_value(
                PredicateLockHash,
                &mypredlocktag as *const PREDICATELOCKTAG as *const c_void,
                predlockhashcode,
                HASH_REMOVE,
                ptr::null_mut(),
            );

            RemoveTargetIfNoLongerUsed(target, targettaghash);
        }

        LWLockRelease(SerializableXactHashLock());
        LWLockRelease(partitionLock);
        if IsInParallelMode() {
            LWLockRelease(&mut (*MySerializableXact).perXactPredicateListLock);
        }
        LWLockRelease(SerializablePredicateListLock());

        if !rmpredlock.is_null() {
            /*
             * Remove entry in local lock table if it exists.
             */
            hash_search_with_hash_value(
                LocalPredicateLockHash,
                targettag as *const c_void,
                targettaghash,
                HASH_REMOVE,
                ptr::null_mut(),
            );

            DecrementParentLocks(targettag);
        }
    }
}

/*
 * CheckForSerializableConflictIn
 *   We are writing the given tuple.  If that indicates a rw-conflict
 *   in from another serializable transaction, take appropriate action.
 */
pub unsafe fn CheckForSerializableConflictIn(
    relation: Relation,
    tid: ItemPointer,
    blkno: BlockNumber,
) {
    let mut targettag: PREDICATELOCKTARGETTAG = core::mem::zeroed();

    if !SerializationNeededForWrite(relation) {
        return;
    }

    /* Check if someone else has already decided that we need to die */
    if SxactIsDoomed(MySerializableXact) {
        ereport!(
            ERROR,
            errmsg!(
                "could not serialize access due to read/write dependencies among transactions"
            )
            /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
               errdetail_internal("Reason code: Canceled on identification as a pivot, during conflict in checking."),
               errhint("The transaction might succeed if retried.") */
        );
    }

    /*
     * We're doing a write which might cause rw-conflicts now or later.
     * Memorize that fact.
     */
    MyXactDidWrite = true;

    /*
     * It is important that we check for locks from the finest granularity to
     * the coarsest granularity, so that granularity promotion doesn't cause
     * us to miss a lock.
     */
    if !tid.is_null() {
        SET_PREDICATELOCKTARGETTAG_TUPLE(
            &mut targettag,
            relation_rd_locator_dboid(relation),
            relation_rd_id(relation),
            ItemPointerGetBlockNumber(tid),
            ItemPointerGetOffsetNumber(tid),
        );
        CheckTargetForConflictsIn(&mut targettag);
    }

    if blkno != InvalidBlockNumber {
        SET_PREDICATELOCKTARGETTAG_PAGE(
            &mut targettag,
            relation_rd_locator_dboid(relation),
            relation_rd_id(relation),
            blkno,
        );
        CheckTargetForConflictsIn(&mut targettag);
    }

    SET_PREDICATELOCKTARGETTAG_RELATION(
        &mut targettag,
        relation_rd_locator_dboid(relation),
        relation_rd_id(relation),
    );
    CheckTargetForConflictsIn(&mut targettag);
}

/*
 * CheckTableForSerializableConflictIn
 *   The entire table is going through a DDL-style logical mass delete
 *   like TRUNCATE or DROP TABLE.
 */
pub unsafe fn CheckTableForSerializableConflictIn(relation: Relation) {
    let mut seqstat: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut target: *mut PREDICATELOCKTARGET;
    let dbId: Oid = relation_rd_locator_dboid(relation);
    let heapId: Oid = relation_rd_id(relation);
    let mut i: c_int;

    /*
     * Bail out quickly if there are no serializable transactions running.
     */
    if !TransactionIdIsValid((*PredXact).SxactGlobalXmin) {
        return;
    }

    if !SerializationNeededForWrite(relation) {
        return;
    }

    /*
     * We're doing a write which might cause rw-conflicts now or later.
     * Memorize that fact.
     */
    MyXactDidWrite = true;

    Assert!(relation_rd_index(relation).is_null()); /* not an index relation */

    LWLockAcquire(SerializablePredicateListLock(), LW_EXCLUSIVE);
    i = 0;
    while i < NUM_PREDICATELOCK_PARTITIONS {
        LWLockAcquire(PredicateLockHashPartitionLockByIndex(i), LW_SHARED);
        i += 1;
    }
    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

    /* Scan through target list */
    hash_seq_init(&mut seqstat, PredicateLockTargetHash);

    loop {
        target = hash_seq_search(&mut seqstat) as *mut PREDICATELOCKTARGET;
        if target.is_null() {
            break;
        }

        /*
         * Check whether this is a target which needs attention.
         */
        if GET_PREDICATELOCKTARGETTAG_RELATION(&(*target).tag) != heapId {
            continue; /* wrong relation id */
        }
        if GET_PREDICATELOCKTARGETTAG_DB(&(*target).tag) != dbId {
            continue; /* wrong database id */
        }

        /*
         * Loop through locks for this target and flag conflicts.
         */
        let mut iter: dlist_mutable_iter = core::mem::zeroed();
        dlist_foreach_modify!(iter, &mut (*target).predicateLocks, {
            let predlock = dlist_container!(
                PREDICATELOCK,
                targetLink,
                iter.cur
            );

            if (*predlock).tag.myXact != MySerializableXact
                && !RWConflictExists((*predlock).tag.myXact, MySerializableXact)
            {
                FlagRWConflict((*predlock).tag.myXact, MySerializableXact);
            }
        });
    }

    /* Release locks in reverse order */
    LWLockRelease(SerializableXactHashLock());
    i = NUM_PREDICATELOCK_PARTITIONS - 1;
    while i >= 0 {
        LWLockRelease(PredicateLockHashPartitionLockByIndex(i));
        i -= 1;
    }
    LWLockRelease(SerializablePredicateListLock());
}

/*
 * Flag a rw-dependency between two serializable transactions.
 *
 * The caller is responsible for ensuring that we have a LW lock on
 * the transaction hash table.
 */
unsafe fn FlagRWConflict(reader: *mut SERIALIZABLEXACT, writer: *mut SERIALIZABLEXACT) {
    Assert!(reader != writer);

    /* First, see if this conflict causes failure. */
    OnConflict_CheckForSerializationFailure(reader, writer);

    /* Actually do the conflict flagging. */
    if reader == OldCommittedSxact {
        (*writer).flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
    } else if writer == OldCommittedSxact {
        (*reader).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
    } else {
        SetRWConflict(reader, writer);
    }
}

/*
 * We are about to add a RW-edge to the dependency graph - check that we don't
 * introduce a dangerous structure by doing so, and abort one of the
 * transactions if so.
 */
unsafe fn OnConflict_CheckForSerializationFailure(
    reader: *const SERIALIZABLEXACT,
    writer: *mut SERIALIZABLEXACT,
) {
    let mut failure: bool;

    Assert!(LWLockHeldByMe(SerializableXactHashLock()));

    failure = false;

    /*
     * Check for already-committed writer with rw-conflict out flagged
     * (conflict-flag on W means that T2 committed before W):
     *
     *   R ------> W ------> T2
     *       rw        rw
     */
    if SxactIsCommitted(writer)
        && (SxactHasConflictOut(writer) || SxactHasSummaryConflictOut(writer))
    {
        failure = true;
    }

    /*
     * Check whether the writer has become a pivot with an out-conflict
     * committed transaction (T2), and T2 committed first:
     *
     *   R ------> W ------> T2
     *       rw        rw
     */
    if !failure && SxactHasSummaryConflictOut(writer) {
        failure = true;
    } else if !failure {
        let mut iter: dlist_iter = core::mem::zeroed();
        dlist_foreach!(iter, &mut (*(writer as *mut SERIALIZABLEXACT)).outConflicts, {
            let conflict = dlist_container!(
                RWConflictData,
                outLink,
                iter.cur
            );
            let t2: *const SERIALIZABLEXACT = (*conflict).sxactIn;

            if SxactIsPrepared(t2)
                && (!SxactIsCommitted(reader)
                    || (*t2).prepareSeqNo <= (*reader).commitSeqNo)
                && (!SxactIsCommitted(writer)
                    || (*t2).prepareSeqNo <= (*writer).commitSeqNo)
                && (!SxactIsReadOnly(reader)
                    || (*t2).prepareSeqNo <= (*reader).SeqNo.lastCommitBeforeSnapshot)
            {
                failure = true;
                break;
            }
        });
    }

    /*
     * Check whether the reader has become a pivot with a writer
     * that's committed (or prepared):
     *
     *   T0 ------> R ------> W
     *        rw        rw
     */
    if !failure && SxactIsPrepared(writer) && !SxactIsReadOnly(reader) {
        if SxactHasSummaryConflictIn(reader) {
            failure = true;
        } else {
            let mut iter: dlist_iter = core::mem::zeroed();
            dlist_foreach!(iter, &mut (*(reader as *mut SERIALIZABLEXACT)).inConflicts, {
                let conflict = dlist_container!(
                    RWConflictData,
                    inLink,
                    iter.cur
                );
                let t0: *const SERIALIZABLEXACT = (*conflict).sxactOut;

                if !SxactIsDoomed(t0)
                    && (!SxactIsCommitted(t0)
                        || (*t0).commitSeqNo >= (*writer).prepareSeqNo)
                    && (!SxactIsReadOnly(t0)
                        || (*t0).SeqNo.lastCommitBeforeSnapshot >= (*writer).prepareSeqNo)
                {
                    failure = true;
                    break;
                }
            });
        }
    }

    if failure {
        /*
         * We have to kill a transaction to avoid a possible anomaly from
         * occurring.
         */
        if MySerializableXact == writer {
            LWLockRelease(SerializableXactHashLock());
            ereport!(
                ERROR,
                errmsg!(
                    "could not serialize access due to read/write dependencies among transactions"
                )
                /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                   errdetail_internal("Reason code: Canceled on identification as a pivot, during write."),
                   errhint("The transaction might succeed if retried.") */
            );
        } else if SxactIsPrepared(writer) {
            LWLockRelease(SerializableXactHashLock());

            /* if we're not the writer, we have to be the reader */
            Assert!(MySerializableXact == reader as *mut SERIALIZABLEXACT);
            ereport!(
                ERROR,
                errmsg!(
                    "could not serialize access due to read/write dependencies among transactions"
                )
                /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                   errdetail_internal("Reason code: Canceled on conflict out to pivot {}, during read.", (*writer).topXid),
                   errhint("The transaction might succeed if retried.") */
            );
        }
        (*writer).flags |= SXACT_FLAG_DOOMED;
    }
}

/*
 * PreCommit_CheckForSerializationFailure
 *   Check for dangerous structures in a serializable transaction at commit.
 */
pub unsafe fn PreCommit_CheckForSerializationFailure() {
    if MySerializableXact == InvalidSerializableXact() {
        return;
    }

    Assert!(IsolationIsSerializable());

    LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);

    /*
     * Check if someone else has already decided that we need to die.  Since
     * we set our own DOOMED flag when partially releasing, ignore in that
     * case.
     */
    if SxactIsDoomed(MySerializableXact) && !SxactIsPartiallyReleased(MySerializableXact) {
        LWLockRelease(SerializableXactHashLock());
        ereport!(
            ERROR,
            errmsg!(
                "could not serialize access due to read/write dependencies among transactions"
            )
            /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
               errdetail_internal("Reason code: Canceled on identification as a pivot, during commit attempt."),
               errhint("The transaction might succeed if retried.") */
        );
    }

    let mut near_iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(near_iter, &mut (*MySerializableXact).inConflicts, {
        let nearConflict = dlist_container!(
            RWConflictData,
            inLink,
            near_iter.cur
        );

        if !SxactIsCommitted((*nearConflict).sxactOut)
            && !SxactIsDoomed((*nearConflict).sxactOut)
        {
            let mut far_iter: dlist_iter = core::mem::zeroed();
            dlist_foreach!(far_iter, &mut (*(*nearConflict).sxactOut).inConflicts, {
                let farConflict = dlist_container!(
                    RWConflictData,
                    inLink,
                    far_iter.cur
                );

                if (*farConflict).sxactOut == MySerializableXact
                    || (!SxactIsCommitted((*farConflict).sxactOut)
                        && !SxactIsReadOnly((*farConflict).sxactOut)
                        && !SxactIsDoomed((*farConflict).sxactOut))
                {
                    /*
                     * Normally, we kill the pivot transaction to make sure we
                     * make progress if the failing transaction is retried.
                     * However, we can't kill it if it's already prepared, so
                     * in that case we commit suicide instead.
                     */
                    if SxactIsPrepared((*nearConflict).sxactOut) {
                        LWLockRelease(SerializableXactHashLock());
                        ereport!(
                            ERROR,
                            errmsg!(
                                "could not serialize access due to read/write dependencies among transactions"
                            )
                            /* C also: errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                               errdetail_internal("Reason code: Canceled on commit attempt with conflict in from prepared pivot."),
                               errhint("The transaction might succeed if retried.") */
                        );
                    }
                    (*(*nearConflict).sxactOut).flags |= SXACT_FLAG_DOOMED;
                    break;
                }
            });
        }
    });

    (*MySerializableXact).prepareSeqNo = {
        (*PredXact).LastSxactCommitSeqNo += 1;
        (*PredXact).LastSxactCommitSeqNo
    };
    (*MySerializableXact).flags |= SXACT_FLAG_PREPARED;

    LWLockRelease(SerializableXactHashLock());
}

/*------------------------------------------------------------------------*/

/*
 * Two-phase commit support
 */

/*
 * AtPrepare_Locks
 *   Do the preparatory work for a PREPARE: make 2PC state file
 *   records for all predicate locks currently held.
 */
pub unsafe fn AtPrepare_PredicateLocks() {
    let sxact: *mut SERIALIZABLEXACT;
    let mut record: TwoPhasePredicateRecord = core::mem::zeroed();

    sxact = MySerializableXact;

    if MySerializableXact == InvalidSerializableXact() {
        return;
    }

    /* Generate an xact record for our SERIALIZABLEXACT */
    record.r#type = TWOPHASEPREDICATERECORD_XACT;
    record.data.xactRecord.xmin = (*MySerializableXact).xmin;
    record.data.xactRecord.flags = (*MySerializableXact).flags;

    /*
     * Note that we don't include the list of conflicts in our out in the
     * statefile, because new conflicts can be added even after the
     * transaction prepares.
     */

    RegisterTwoPhaseRecord(
        TWOPHASE_RM_PREDICATELOCK_ID,
        0,
        &record as *const TwoPhasePredicateRecord as *const c_void,
        core::mem::size_of::<TwoPhasePredicateRecord>() as uint32,
    );

    /*
     * Generate a lock record for each lock.
     */
    LWLockAcquire(SerializablePredicateListLock(), LW_SHARED);

    /*
     * No need to take sxact->perXactPredicateListLock in parallel mode
     * because there cannot be any parallel workers running while we are
     * preparing a transaction.
     */
    Assert!(!IsParallelWorker() && !ParallelContextActive());

    let mut iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(iter, &mut (*sxact).predicateLocks, {
        let predlock = dlist_container!(
            PREDICATELOCK,
            xactLink,
            iter.cur
        );

        record.r#type = TWOPHASEPREDICATERECORD_LOCK;
        record.data.lockRecord.target = (*(*predlock).tag.myTarget).tag;

        RegisterTwoPhaseRecord(
            TWOPHASE_RM_PREDICATELOCK_ID,
            0,
            &record as *const TwoPhasePredicateRecord as *const c_void,
            core::mem::size_of::<TwoPhasePredicateRecord>() as uint32,
        );
    });

    LWLockRelease(SerializablePredicateListLock());
}

/*
 * PostPrepare_Locks
 *   Clean up after successful PREPARE.
 */
pub unsafe fn PostPrepare_PredicateLocks(xid: TransactionId) {
    if MySerializableXact == InvalidSerializableXact() {
        return;
    }

    Assert!(SxactIsPrepared(MySerializableXact));

    (*MySerializableXact).pid = 0;
    (*MySerializableXact).pgprocno = INVALID_PROC_NUMBER;

    hash_destroy(LocalPredicateLockHash);
    LocalPredicateLockHash = ptr::null_mut();

    MySerializableXact = InvalidSerializableXact();
    MyXactDidWrite = false;
}

/*
 * PredicateLockTwoPhaseFinish
 *   Release a prepared transaction's predicate locks once it
 *   commits or aborts.
 */
pub unsafe fn PredicateLockTwoPhaseFinish(xid: TransactionId, isCommit: bool) {
    let sxid: *mut SERIALIZABLEXID;
    let mut sxidtag: SERIALIZABLEXIDTAG = SERIALIZABLEXIDTAG { xid: 0 };

    sxidtag.xid = xid;

    LWLockAcquire(SerializableXactHashLock(), LW_SHARED);
    sxid = hash_search(
        SerializableXidHash,
        &sxidtag as *const SERIALIZABLEXIDTAG as *const c_void,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut SERIALIZABLEXID;
    LWLockRelease(SerializableXactHashLock());

    /* xid will not be found if it wasn't a serializable transaction */
    if sxid.is_null() {
        return;
    }

    /* Release its locks */
    MySerializableXact = (*sxid).myXact;
    MyXactDidWrite = true; /* conservatively assume that we wrote something */
    ReleasePredicateLocks(isCommit, false);
}

/*
 * Re-acquire a predicate lock belonging to a transaction that was prepared.
 */
pub unsafe fn predicatelock_twophase_recover(
    xid: TransactionId,
    info: uint16,
    recdata: *mut c_void,
    len: uint32,
) {
    let record: *mut TwoPhasePredicateRecord;

    Assert!(len == core::mem::size_of::<TwoPhasePredicateRecord>() as uint32);

    record = recdata as *mut TwoPhasePredicateRecord;

    Assert!(
        (*record).r#type == TWOPHASEPREDICATERECORD_XACT
            || (*record).r#type == TWOPHASEPREDICATERECORD_LOCK
    );

    if (*record).r#type == TWOPHASEPREDICATERECORD_XACT {
        /* Per-transaction record. Set up a SERIALIZABLEXACT. */
        let xactRecord: *mut crate::storage::predicate_internals::TwoPhasePredicateXactRecord =
            &raw mut (*record).data.xactRecord;
        let sxact: *mut SERIALIZABLEXACT;
        let sxid: *mut SERIALIZABLEXID;
        let mut sxidtag: SERIALIZABLEXIDTAG = SERIALIZABLEXIDTAG { xid: 0 };
        let mut found: bool = false;

        LWLockAcquire(SerializableXactHashLock(), LW_EXCLUSIVE);
        sxact = CreatePredXact();
        if sxact.is_null() {
            ereport!(
                ERROR,
                errmsg!("out of shared memory")
                /* C also: errcode(ERRCODE_OUT_OF_MEMORY) */
            );
        }

        /* vxid for a prepared xact is INVALID_PROC_NUMBER/xid; no pid */
        let prepared_vxid = VirtualTransactionId {
            procNumber: INVALID_PROC_NUMBER,
            localTransactionId: xid as LocalTransactionId,
        };
        ptr::copy_nonoverlapping(
            &prepared_vxid as *const VirtualTransactionId as *const u8,
            &mut (*sxact).vxid as *mut _ as *mut u8,
            size_of::<VirtualTransactionId>(),
        );
        (*sxact).pid = 0;
        (*sxact).pgprocno = INVALID_PROC_NUMBER;

        /* a prepared xact hasn't committed yet */
        (*sxact).prepareSeqNo = RecoverySerCommitSeqNo;
        (*sxact).commitSeqNo = InvalidSerCommitSeqNo;
        (*sxact).finishedBefore = InvalidTransactionId;

        (*sxact).SeqNo.lastCommitBeforeSnapshot = RecoverySerCommitSeqNo;

        /*
         * Don't need to track this; no transactions running at the time the
         * recovered xact started are still active, except possibly other
         * prepared xacts and we don't care whether those are RO_SAFE or not.
         */
        dlist_init(&mut (*sxact).possibleUnsafeConflicts);

        dlist_init(&mut (*sxact).predicateLocks);
        dlist_node_init(&mut (*sxact).finishedLink);

        (*sxact).topXid = xid;
        (*sxact).xmin = (*xactRecord).xmin;
        (*sxact).flags = (*xactRecord).flags;
        Assert!(SxactIsPrepared(sxact));
        if !SxactIsReadOnly(sxact) {
            (*PredXact).WritableSxactCount += 1;
            Assert!(
                (*PredXact).WritableSxactCount
                    <= (MaxBackends() + max_prepared_xacts)
            );
        }

        /*
         * We don't know whether the transaction had any conflicts or not, so
         * we'll conservatively assume that it had both a conflict in and a
         * conflict out, and represent that with the summary conflict flags.
         */
        dlist_init(&mut (*sxact).outConflicts);
        dlist_init(&mut (*sxact).inConflicts);
        (*sxact).flags |= SXACT_FLAG_SUMMARY_CONFLICT_IN;
        (*sxact).flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;

        /* Register the transaction's xid */
        sxidtag.xid = xid;
        sxid = hash_search(
            SerializableXidHash,
            &sxidtag as *const SERIALIZABLEXIDTAG as *const c_void,
            HASH_ENTER,
            &mut found,
        ) as *mut SERIALIZABLEXID;
        Assert!(!sxid.is_null());
        Assert!(!found);
        (*sxid).myXact = sxact;

        /*
         * Update global xmin. Note that this is a special case compared to
         * registering a normal transaction, because the global xmin might go
         * backwards.
         */
        if !TransactionIdIsValid((*PredXact).SxactGlobalXmin)
            || TransactionIdFollows((*PredXact).SxactGlobalXmin, (*sxact).xmin)
        {
            (*PredXact).SxactGlobalXmin = (*sxact).xmin;
            (*PredXact).SxactGlobalXminCount = 1;
            SerialSetActiveSerXmin((*sxact).xmin);
        } else if TransactionIdEquals((*sxact).xmin, (*PredXact).SxactGlobalXmin) {
            Assert!((*PredXact).SxactGlobalXminCount > 0);
            (*PredXact).SxactGlobalXminCount += 1;
        }

        LWLockRelease(SerializableXactHashLock());
    } else if (*record).r#type == TWOPHASEPREDICATERECORD_LOCK {
        /* Lock record. Recreate the PREDICATELOCK */
        let lockRecord: *mut crate::storage::predicate_internals::TwoPhasePredicateLockRecord =
            &raw mut (*record).data.lockRecord;
        let sxid: *mut SERIALIZABLEXID;
        let sxact: *mut SERIALIZABLEXACT;
        let mut sxidtag: SERIALIZABLEXIDTAG = SERIALIZABLEXIDTAG { xid: 0 };
        let targettaghash: uint32;

        targettaghash = PredicateLockTargetTagHashCode(&(*lockRecord).target);

        LWLockAcquire(SerializableXactHashLock(), LW_SHARED);
        sxidtag.xid = xid;
        sxid = hash_search(
            SerializableXidHash,
            &sxidtag as *const SERIALIZABLEXIDTAG as *const c_void,
            HASH_FIND,
            ptr::null_mut(),
        ) as *mut SERIALIZABLEXID;
        LWLockRelease(SerializableXactHashLock());

        Assert!(!sxid.is_null());
        sxact = (*sxid).myXact;
        Assert!(sxact != InvalidSerializableXact());

        CreatePredicateLock(&(*lockRecord).target, targettaghash, sxact);
    }
}

/*
 * Prepare to share the current SERIALIZABLEXACT with parallel workers.
 * Return a handle object that can be used by AttachSerializableXact() in a
 * parallel worker.
 */
pub unsafe fn ShareSerializableXact() -> SerializableXactHandle {
    MySerializableXact
}

/*
 * Allow parallel workers to import the leader's SERIALIZABLEXACT.
 */
pub unsafe fn AttachSerializableXact(handle: SerializableXactHandle) {
    Assert!(MySerializableXact == InvalidSerializableXact());

    MySerializableXact = handle;
    if MySerializableXact != InvalidSerializableXact() {
        CreateLocalPredicateLockHash();
    }
}
