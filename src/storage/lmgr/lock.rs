/*-------------------------------------------------------------------------
 *
 * lock.rs
 *   POSTGRES primary lock mechanism
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/backend/storage/lmgr/lock.c
 *
 * NOTES
 *   A lock table is a shared memory hash table.  When
 *   a process tries to acquire a lock of a type that conflicts
 *   with existing locks, it is put to sleep using the routines
 *   in storage/lmgr/proc.c.
 *
 *   For the most part, this code should be invoked via lmgr.c
 *   or another lock-management module, not directly.
 *
 *   Interface:
 *
 *   LockManagerShmemInit(), GetLocksMethodTable(), GetLockTagsMethodTable(),
 *   LockAcquire(), LockRelease(), LockReleaseAll(),
 *   LockCheckConflicts(), GrantLock()
 *
 *-------------------------------------------------------------------------
 */

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use crate::prelude::*;
use crate::storage::lmgr::lmgr::DescribeLockTag;
use crate::lib::ilist::{dlist_delete, dlist_init};
use core::mem::size_of;
use std::ptr;

// lib/ilist.h -- dlist / dclist support
type dlist_head = crate::lib::ilist::dlist_head;
type dlist_node = crate::lib::ilist::dlist_node;
type dlist_iter = crate::lib::ilist::dlist_iter;
type dlist_mutable_iter = crate::lib::ilist::dlist_mutable_iter;
type dclist_head = crate::lib::ilist::dclist_head;
use crate::{dlist_container, dlist_foreach, dlist_foreach_modify};
use crate::lib::ilist::{dlist_is_empty, dlist_push_tail};

// lib/stringinfo.h
type StringInfoData = crate::lib::stringinfo::StringInfoData;
type StringInfo = *mut StringInfoData;
use crate::appendStringInfo;

// storage/lockdefs.h -- LOCKMODE, lock-mode constants
use crate::storage::lockdefs::{
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, MaxLockMode, NoLock, RowExclusiveLock,
    RowShareLock, ShareLock, ShareRowExclusiveLock, ShareUpdateExclusiveLock, LOCKMASK, LOCKMODE,
};

// utils/init/globals.c
use crate::utils::init::globals::MaxBackends;

// access/transam.h
use crate::access::transam::{TransactionIdIsValid};

// miscadmin.h -- CHECK_FOR_INTERRUPTS
macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{ /* TODO(pg-port): miscadmin.h CHECK_FOR_INTERRUPTS() */ }};
}
use CHECK_FOR_INTERRUPTS;

// ============================================================
//  Types from storage/lock.h (owned here; lmgr.rs had stubs)
// ============================================================

pub type LOCKMETHODID = uint8;

/// Four-field lock tag (storage/lock.h).
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct LOCKTAG {
    pub locktag_field1: uint32,
    pub locktag_field2: uint32,
    pub locktag_field3: uint32,
    pub locktag_field4: uint16,
    pub locktag_type: uint8,
    pub locktag_lockmethodid: uint8,
}

/// LockAcquireResult values (storage/lock.h).
pub type LockAcquireResult = c_int;
pub const LOCKACQUIRE_NOT_AVAIL: LockAcquireResult = 0;
pub const LOCKACQUIRE_OK: LockAcquireResult = 1;
pub const LOCKACQUIRE_ALREADY_HELD: LockAcquireResult = 2;
pub const LOCKACQUIRE_ALREADY_CLEAR: LockAcquireResult = 3;

/// LockTagType values (storage/lock.h).
pub type LockTagType = c_int;
pub const LOCKTAG_RELATION: LockTagType = 0;
pub const LOCKTAG_RELATION_EXTEND: LockTagType = 1;
pub const LOCKTAG_DATABASE_FROZEN_IDS: LockTagType = 2;
pub const LOCKTAG_PAGE: LockTagType = 3;
pub const LOCKTAG_TUPLE: LockTagType = 4;
pub const LOCKTAG_TRANSACTION: LockTagType = 5;
pub const LOCKTAG_VIRTUALTRANSACTION: LockTagType = 6;
pub const LOCKTAG_SPECULATIVE_TOKEN: LockTagType = 7;
pub const LOCKTAG_OBJECT: LockTagType = 8;
pub const LOCKTAG_USERLOCK: LockTagType = 9;
pub const LOCKTAG_ADVISORY: LockTagType = 10;
pub const LOCKTAG_APPLY_TRANSACTION: LockTagType = 11;
pub const LOCKTAG_LAST_TYPE: uint16 = LOCKTAG_APPLY_TRANSACTION as uint16;

/// Default and user lock method IDs (storage/lock.h).
pub const DEFAULT_LOCKMETHOD: c_int = 1;
pub const USER_LOCKMETHOD: c_int = 2;

/// Maximum number of lock modes (storage/lock.h).
pub const MAX_LOCKMODES: usize = 10;

/// LOCKBIT_ON/OFF macros (storage/lock.h).
#[inline]
pub fn LOCKBIT_ON(lockmode: LOCKMODE) -> LOCKMASK {
    1 << lockmode
}
#[inline]
pub fn LOCKBIT_OFF(lockmode: LOCKMODE) -> LOCKMASK {
    !(1 << lockmode)
}

/// Number of lock table partitions (storage/lock.h).
pub const NUM_LOCK_PARTITIONS: usize = 16;
pub const LOG2_NUM_LOCK_PARTITIONS: usize = 4;

/// Fast-path locking slots/groups (storage/proc.h / lock.h).
pub const FP_LOCK_SLOTS_PER_GROUP: u32 = 16;
pub const FP_LOCK_GROUPS_PER_BACKEND_MAX: usize = 8;

// ============================================================
//  LockMethodData / LockMethod
// ============================================================

/// Per-lock-method data structure (storage/lock.h).
#[repr(C)]
pub struct LockMethodData {
    pub numLockModes: c_int,
    pub conflictTab: *const LOCKMASK,
    pub lockModeNames: *const *const c_char,
    pub trace_flag: *mut bool,
}
pub type LockMethod = *const LockMethodData;
unsafe impl Sync for LockMethodData {}

// ============================================================
//  LOCK / PROCLOCK / LOCALLOCK structures (storage/lock.h)
// ============================================================

/// Per-lockable-object shared-memory structure (storage/lock.h).
#[repr(C)]
pub struct LOCK {
    pub tag: LOCKTAG,           /* unique identifier of lockable object */
    pub grantMask: LOCKMASK,    /* bitmask for lock types currently granted */
    pub waitMask: LOCKMASK,     /* bitmask for lock types currently awaited */
    pub procLocks: dlist_head,  /* list of PROCLOCK objects assoc. with lock */
    pub waitProcs: dclist_head, /* list of PGPROC objects waiting on lock */
    pub nRequested: c_int,      /* total number of lock requests */
    pub nGranted: c_int,        /* total number of lock grants */
    pub requested: [c_int; MAX_LOCKMODES], /* counts for each lock mode */
    pub granted: [c_int; MAX_LOCKMODES],   /* counts for each granted mode */
}

/// LOCK_LOCKMETHOD extracts the lock method from a LOCK.
#[inline]
pub unsafe fn LOCK_LOCKMETHOD(lock: &LOCK) -> LOCKMETHODID {
    lock.tag.locktag_lockmethodid
}
/// LOCK_LOCKTAG returns the locktag_type of a LOCK.
#[inline]
pub unsafe fn LOCK_LOCKTAG(lock: &LOCK) -> LockTagType {
    lock.tag.locktag_type as LockTagType
}

/// Per-holder-per-lock shared-memory tag (storage/lock.h).
#[repr(C)]
pub struct PROCLOCKTAG {
    pub myLock: *mut LOCK,
    pub myProc: *mut PGPROC,
}

/// Per-holder-per-lock shared-memory structure (storage/lock.h).
#[repr(C)]
pub struct PROCLOCK {
    pub tag: PROCLOCKTAG,       /* unique identifier of proclock object */
    pub groupLeader: *mut PGPROC, /* group leader, or proc itself if no group */
    pub holdMask: LOCKMASK,     /* bitmask for lock types currently held */
    pub releaseMask: LOCKMASK,  /* bitmask for lock types to release */
    pub lockLink: dlist_node,   /* list link in LOCK's list of proclocks */
    pub procLink: dlist_node,   /* list link in PGPROC's list of proclocks */
}

/// PROCLOCK_LOCKMETHOD extracts the method from a PROCLOCK.
#[inline]
pub unsafe fn PROCLOCK_LOCKMETHOD(proclock: &PROCLOCK) -> LOCKMETHODID {
    LOCK_LOCKMETHOD(&*proclock.tag.myLock)
}

/// Per-lock owner record inside LOCALLOCK.
#[repr(C)]
pub struct LOCALLOCKOWNER {
    pub owner: ResourceOwner,
    pub nLocks: int64,
}

/// Local-process lock tag (storage/lock.h).
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct LOCALLOCKTAG {
    pub lock: LOCKTAG,
    pub mode: LOCKMODE,
}

/// Per-lock-per-process backend-private structure (storage/lock.h).
#[repr(C)]
pub struct LOCALLOCK {
    pub tag: LOCALLOCKTAG,          /* unique identifier of locallock object */
    pub hashcode: uint32,           /* copy of the hashcode for tag */
    pub lock: *mut LOCK,            /* associated LOCK object, if any */
    pub proclock: *mut PROCLOCK,    /* associated PROCLOCK object, if any */
    pub nLocks: int64,              /* total number of times lock is held */
    pub numLockOwners: c_int,       /* # of relevant ResOwner entries */
    pub maxLockOwners: c_int,       /* allocated size of array */
    pub holdsStrongLockCount: bool, /* bumped FastPathStrongRelationLocks */
    pub lockCleared: bool,          /* we read sinval msgs for this lock */
    pub lockOwners: *mut LOCALLOCKOWNER, /* dynamically-allocated array */
}

/// LOCALLOCK_LOCKMETHOD extracts the method from a LOCALLOCK.
#[inline]
pub unsafe fn LOCALLOCK_LOCKMETHOD(locallock: &LOCALLOCK) -> LOCKMETHODID {
    locallock.tag.lock.locktag_lockmethodid
}
/// LOCALLOCK_LOCKTAG extracts the locktag_type from a LOCALLOCK.
#[inline]
pub unsafe fn LOCALLOCK_LOCKTAG(locallock: &LOCALLOCK) -> LockTagType {
    locallock.tag.lock.locktag_type as LockTagType
}

// ============================================================
//  VirtualTransactionId (storage/lock.h)
// ============================================================

pub type LocalTransactionId = uint32;
pub const InvalidLocalTransactionId: LocalTransactionId = 0;

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: LocalTransactionId,
}

#[inline]
pub fn VirtualTransactionIdIsValid(vxid: VirtualTransactionId) -> bool {
    vxid.localTransactionId != InvalidLocalTransactionId
}
#[inline]
pub fn VirtualTransactionIdEquals(a: VirtualTransactionId, b: VirtualTransactionId) -> bool {
    a.procNumber == b.procNumber && a.localTransactionId == b.localTransactionId
}
#[inline]
pub fn VirtualTransactionIdIsRecoveredPreparedXact(vxid: VirtualTransactionId) -> bool {
    vxid.procNumber == INVALID_PROC_NUMBER
}

// ============================================================
//  LockInstanceData / LockData / BlockedProcsData (lock.h / locks.h)
// ============================================================

pub type TimestampTz = int64; // TODO(pg-port): utils/timestamp.h

#[repr(C)]
pub struct LockInstanceData {
    pub locktag: LOCKTAG,
    pub holdMask: LOCKMASK,
    pub waitLockMode: LOCKMODE,
    pub vxid: VirtualTransactionId,
    pub pid: c_int,
    pub leaderPid: c_int,
    pub fastpath: bool,
    pub waitStart: TimestampTz,
}

#[repr(C)]
pub struct LockData {
    pub nelements: c_int,
    pub locks: *mut LockInstanceData,
}

#[repr(C)]
pub struct BlockedProcData {
    pub pid: c_int,
    pub first_lock: c_int,
    pub num_locks: c_int,
    pub first_waiter: c_int,
    pub num_waiters: c_int,
}

#[repr(C)]
pub struct BlockedProcsData {
    pub procs: *mut BlockedProcData,
    pub locks: *mut LockInstanceData,
    pub waiter_pids: *mut c_int,
    pub nprocs: c_int,
    pub maxprocs: c_int,
    pub nlocks: c_int,
    pub maxlocks: c_int,
    pub npids: c_int,
    pub maxpids: c_int,
}

// ============================================================
//  PGPROC -- stub (real home: proc.c / proc.h)
//  TODO(pg-port): replace when storage::proc lands.
// ============================================================

pub type ProcNumber = c_int;
pub const INVALID_PROC_NUMBER: ProcNumber = -1;

#[repr(C)]
pub struct PGPROC_vxid {
    pub procNumber: ProcNumber,
    pub lxid: LocalTransactionId,
}

// PGPROC: use the canonical full definition from lmgr/proc.rs (the local stub
// had a different layout -- e.g. fpLockBits inline vs pointer -- which put
// myProcLocks at the wrong offset and crashed SetupLockInTable).
pub use crate::storage::lmgr::proc::PGPROC;

pub type ProcWaitStatus = c_int;
pub const PROC_WAIT_STATUS_OK: ProcWaitStatus = 0;
pub const PROC_WAIT_STATUS_WAITING: ProcWaitStatus = 1;
pub const PROC_WAIT_STATUS_ERROR: ProcWaitStatus = 2;

// ============================================================
//  ProcGlobal stub -- TODO(pg-port): real home is proc.c
// ============================================================

#[repr(C)]
pub struct PROC_HDR {
    pub allProcs: *mut PGPROC,
    pub allProcCount: uint32,
}

// ============================================================
//  LWLock / spinlock stubs
//  (real types in storage/lwlock.h, storage/spin.h)
// ============================================================

pub type LWLock = c_void; // TODO(pg-port): storage/lwlock.h
pub type slock_t = c_int; // TODO(pg-port): storage/s_lock.h

pub const LW_SHARED: c_int = 1;
pub const LW_EXCLUSIVE: c_int = 2;

// ============================================================
//  Hash table stubs (utils/dynahash.h conventions)
//  TODO(pg-port): replace with real dynahash bindings.
// ============================================================

pub type HTAB = c_void;
pub type Size = usize;

pub use crate::utils::hash::dynahash::{HASHCTL, HASH_ELEM, HASH_BLOBS, HASH_FUNCTION, HASH_PARTITION, HASH_CONTEXT};

pub use crate::utils::hash::dynahash::HASH_SEQ_STATUS;

pub const HASH_FIND: c_int = 0;
pub const HASH_ENTER: c_int = 1;
pub const HASH_REMOVE: c_int = 2;
pub const HASH_ENTER_NULL: c_int = 3;

// ============================================================
//  ResourceOwner / MemoryContext stubs
//  TODO(pg-port): replace with real resowner / mmgr types.
// ============================================================

pub type ResourceOwner = *mut c_void; // TODO(pg-port): utils/resowner.h
pub type MemoryContext = *mut c_void; // TODO(pg-port): utils/palloc.h

// ============================================================
//  xl_standby_lock (storage/standby.h / standby.c)
// ============================================================

#[repr(C)]
pub struct xl_standby_lock {
    pub xid: TransactionId,
    pub dbOid: Oid,
    pub relOid: Oid,
}

// ============================================================
//  TwoPhaseLockRecord (2PC state file record)
// ============================================================

#[repr(C)]
struct TwoPhaseLockRecord {
    locktag: LOCKTAG,
    lockmode: LOCKMODE,
}

// ============================================================
//  FastPathStrongRelationLockData
// ============================================================

const FAST_PATH_STRONG_LOCK_HASH_BITS: u32 = 10;
const FAST_PATH_STRONG_LOCK_HASH_PARTITIONS: usize =
    1 << FAST_PATH_STRONG_LOCK_HASH_BITS;

#[repr(C)]
struct FastPathStrongRelationLockData {
    mutex: slock_t,
    count: [uint32; FAST_PATH_STRONG_LOCK_HASH_PARTITIONS],
}

#[inline]
fn FastPathStrongLockHashPartition(hashcode: uint32) -> usize {
    (hashcode % FAST_PATH_STRONG_LOCK_HASH_PARTITIONS as uint32) as usize
}

// ============================================================
//  Fast-path helper macros (translated to inline fns)
// ============================================================

const FAST_PATH_BITS_PER_SLOT: u32 = 3;
const FAST_PATH_LOCKNUMBER_OFFSET: u32 = 1;
const FAST_PATH_MASK: u64 = (1 << FAST_PATH_BITS_PER_SLOT) - 1;

/// Calculate fast-path group from relation OID.
#[inline]
unsafe fn FAST_PATH_REL_GROUP(rel: Oid) -> u32 {
    ((rel as u64).wrapping_mul(49157) & (FastPathLockGroupsPerBackend as u64 - 1)) as u32
}

/// Calculate flat slot index from group+index.
#[inline]
unsafe fn FAST_PATH_SLOT(group: u32, index: u32) -> u32 {
    group * FP_LOCK_SLOTS_PER_GROUP + index
}
#[inline]
unsafe fn FAST_PATH_GROUP(index: u32) -> u32 {
    index / FP_LOCK_SLOTS_PER_GROUP
}
#[inline]
unsafe fn FAST_PATH_INDEX(index: u32) -> u32 {
    index % FP_LOCK_SLOTS_PER_GROUP
}

#[inline]
unsafe fn FAST_PATH_GET_BITS(proc_: *const PGPROC, n: u32) -> u64 {
    let grp = FAST_PATH_GROUP(n) as usize;
    let idx = FAST_PATH_INDEX(n);
    ((*(*proc_).fpLockBits.add((grp) as usize)) >> (FAST_PATH_BITS_PER_SLOT * idx)) & FAST_PATH_MASK
}
#[inline]
unsafe fn FAST_PATH_BIT_POSITION(n: u32, l: u32) -> u32 {
    (l - FAST_PATH_LOCKNUMBER_OFFSET) + FAST_PATH_BITS_PER_SLOT * FAST_PATH_INDEX(n)
}
#[inline]
unsafe fn FAST_PATH_SET_LOCKMODE(proc_: *mut PGPROC, n: u32, l: u32) {
    let grp = FAST_PATH_GROUP(n) as usize;
    (*(*proc_).fpLockBits.add((grp) as usize)) |= 1u64 << FAST_PATH_BIT_POSITION(n, l);
}
#[inline]
unsafe fn FAST_PATH_CLEAR_LOCKMODE(proc_: *mut PGPROC, n: u32, l: u32) {
    let grp = FAST_PATH_GROUP(n) as usize;
    (*(*proc_).fpLockBits.add((grp) as usize)) &= !(1u64 << FAST_PATH_BIT_POSITION(n, l));
}
#[inline]
unsafe fn FAST_PATH_CHECK_LOCKMODE(proc_: *const PGPROC, n: u32, l: u32) -> bool {
    let grp = FAST_PATH_GROUP(n) as usize;
    ((*(*proc_).fpLockBits.add((grp) as usize)) & (1u64 << FAST_PATH_BIT_POSITION(n, l))) != 0
}

/// EligibleForRelationFastPath -- can this lock use the fast path?
#[inline]
unsafe fn EligibleForRelationFastPath(locktag: *const LOCKTAG, mode: LOCKMODE) -> bool {
    (*locktag).locktag_lockmethodid == DEFAULT_LOCKMETHOD as uint8
        && (*locktag).locktag_type == LOCKTAG_RELATION as uint8
        && (*locktag).locktag_field1 == MyDatabaseId
        && MyDatabaseId != InvalidOid
        && mode < ShareUpdateExclusiveLock
}

/// ConflictsWithRelationFastPath -- can this lock conflict with fast-path locks?
#[inline]
unsafe fn ConflictsWithRelationFastPath(locktag: *const LOCKTAG, mode: LOCKMODE) -> bool {
    (*locktag).locktag_lockmethodid == DEFAULT_LOCKMETHOD as uint8
        && (*locktag).locktag_type == LOCKTAG_RELATION as uint8
        && (*locktag).locktag_field1 != InvalidOid
        && mode > ShareUpdateExclusiveLock
}

// ============================================================
//  SET_LOCKTAG_* functions (storage/lock.h macros -> fns)
// ============================================================

pub unsafe fn SET_LOCKTAG_RELATION(tag: *mut LOCKTAG, dboid: Oid, reloid: Oid) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = reloid;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_RELATION as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_RELATION_EXTEND(tag: *mut LOCKTAG, dboid: Oid, reloid: Oid) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = reloid;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_RELATION_EXTEND as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_DATABASE_FROZEN_IDS(tag: *mut LOCKTAG, dboid: Oid) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = 0;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_DATABASE_FROZEN_IDS as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_PAGE(
    tag: *mut LOCKTAG,
    dboid: Oid,
    reloid: Oid,
    blocknum: uint32,
) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = reloid;
    (*tag).locktag_field3 = blocknum;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_PAGE as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_TUPLE(
    tag: *mut LOCKTAG,
    dboid: Oid,
    reloid: Oid,
    blocknum: uint32,
    offnum: uint16,
) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = reloid;
    (*tag).locktag_field3 = blocknum;
    (*tag).locktag_field4 = offnum;
    (*tag).locktag_type = LOCKTAG_TUPLE as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_TRANSACTION(tag: *mut LOCKTAG, xid: TransactionId) {
    (*tag).locktag_field1 = xid;
    (*tag).locktag_field2 = 0;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_TRANSACTION as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_VIRTUALTRANSACTION(tag: *mut LOCKTAG, vxid: VirtualTransactionId) {
    (*tag).locktag_field1 = vxid.procNumber as uint32;
    (*tag).locktag_field2 = vxid.localTransactionId;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_VIRTUALTRANSACTION as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_SPECULATIVE_INSERTION(
    tag: *mut LOCKTAG,
    xid: TransactionId,
    token: uint32,
) {
    (*tag).locktag_field1 = xid;
    (*tag).locktag_field2 = token;
    (*tag).locktag_field3 = 0;
    (*tag).locktag_field4 = 0;
    (*tag).locktag_type = LOCKTAG_SPECULATIVE_TOKEN as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_OBJECT(
    tag: *mut LOCKTAG,
    dboid: Oid,
    classoid: Oid,
    objoid: Oid,
    objsubid: uint16,
) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = classoid;
    (*tag).locktag_field3 = objoid;
    (*tag).locktag_field4 = objsubid;
    (*tag).locktag_type = LOCKTAG_OBJECT as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}
pub unsafe fn SET_LOCKTAG_APPLY_TRANSACTION(
    tag: *mut LOCKTAG,
    dboid: Oid,
    suboid: Oid,
    xid: uint32,
    objid: uint16,
) {
    (*tag).locktag_field1 = dboid;
    (*tag).locktag_field2 = suboid;
    (*tag).locktag_field3 = xid;
    (*tag).locktag_field4 = objid;
    (*tag).locktag_type = LOCKTAG_APPLY_TRANSACTION as uint8;
    (*tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD as uint8;
}

// GET_VXID_FROM_PGPROC (storage/proc.h macro -> fn)
#[inline]
pub unsafe fn GET_VXID_FROM_PGPROC(vxid: *mut VirtualTransactionId, proc_: &PGPROC) {
    (*vxid).procNumber = proc_.vxid.procNumber;
    (*vxid).localTransactionId = proc_.vxid.lxid;
}

// ============================================================
//  GUC variables
// ============================================================

/// GUC variable: max_locks_per_xact
pub static mut max_locks_per_xact: c_int = 64;
/// GUC variable: log_lock_failures
pub static mut log_lock_failures: bool = false;

// NLOCKENTS() -- estimated total lock entries
#[inline]
unsafe fn NLOCKENTS() -> c_long {
    mul_size(
        max_locks_per_xact as Size,
        add_size(MaxBackends as Size, max_prepared_xacts as Size),
    ) as c_long
}

// ============================================================
//  LockHashPartition helpers
//  (real impls: include/storage/lock.h / storage/lwlock.c)
//  TODO(pg-port): wire to real partition-lock array.
// ============================================================

#[inline]
pub fn LockHashPartition(hashcode: uint32) -> usize {
    (hashcode as usize) & (NUM_LOCK_PARTITIONS - 1)
}
#[inline]
pub unsafe fn LockHashPartitionLock(hashcode: uint32) -> *mut LWLock {
    LockHashPartitionLockByIndex(LockHashPartition(hashcode))
}
pub unsafe fn LockHashPartitionLockByIndex(i: usize) -> *mut LWLock {
    let padded = crate::storage::lmgr::lwlock::MainLWLockArray
        .add(crate::storage::lmgr::lwlock::LOCK_MANAGER_LWLOCK_OFFSET as usize + i);
    &raw mut (*padded).lock as *mut LWLock
}

// ============================================================
//  Forward-declared static helpers (bodies below)
// ============================================================

static mut StrongLockInProgress: *mut LOCALLOCK = ptr::null_mut();
static mut awaitedLock: *mut LOCALLOCK = ptr::null_mut();
static mut awaitedOwner: ResourceOwner = ptr::null_mut();

/*
 * Count of the number of fast path lock slots we believe to be used.
 */
static mut FastPathLocalUseCounts: [c_int; FP_LOCK_GROUPS_PER_BACKEND_MAX] =
    [0; FP_LOCK_GROUPS_PER_BACKEND_MAX];

/*
 * Flag to indicate if the relation extension lock is held by this backend.
 */
static mut IsRelationExtensionLockHeld: bool = false;

/*
 * Number of fast-path locks per backend.
 */
pub static mut FastPathLockGroupsPerBackend: c_int = 0;
#[inline]
pub unsafe fn FastPathLockSlotsPerBackend() -> u32 {
    FastPathLockGroupsPerBackend as u32 * FP_LOCK_SLOTS_PER_GROUP
}

// ============================================================
//  Conflict table and lock method tables
// ============================================================

/* Names of lock modes, for debug printouts */
const lock_mode_names_arr: [*const c_char; 9] = [
    c"INVALID".as_ptr(),
    c"AccessShareLock".as_ptr(),
    c"RowShareLock".as_ptr(),
    c"RowExclusiveLock".as_ptr(),
    c"ShareUpdateExclusiveLock".as_ptr(),
    c"ShareLock".as_ptr(),
    c"ShareRowExclusiveLock".as_ptr(),
    c"ExclusiveLock".as_ptr(),
    c"AccessExclusiveLock".as_ptr(),
];

static LockConflicts: [LOCKMASK; 9] = [
    0,
    /* AccessShareLock */
    LOCKBIT_ON_CONST(AccessExclusiveLock),
    /* RowShareLock */
    LOCKBIT_ON_CONST(ExclusiveLock) | LOCKBIT_ON_CONST(AccessExclusiveLock),
    /* RowExclusiveLock */
    LOCKBIT_ON_CONST(ShareLock)
        | LOCKBIT_ON_CONST(ShareRowExclusiveLock)
        | LOCKBIT_ON_CONST(ExclusiveLock)
        | LOCKBIT_ON_CONST(AccessExclusiveLock),
    /* ShareUpdateExclusiveLock */
    LOCKBIT_ON_CONST(ShareUpdateExclusiveLock)
        | LOCKBIT_ON_CONST(ShareLock)
        | LOCKBIT_ON_CONST(ShareRowExclusiveLock)
        | LOCKBIT_ON_CONST(ExclusiveLock)
        | LOCKBIT_ON_CONST(AccessExclusiveLock),
    /* ShareLock */
    LOCKBIT_ON_CONST(RowExclusiveLock)
        | LOCKBIT_ON_CONST(ShareUpdateExclusiveLock)
        | LOCKBIT_ON_CONST(ShareRowExclusiveLock)
        | LOCKBIT_ON_CONST(ExclusiveLock)
        | LOCKBIT_ON_CONST(AccessExclusiveLock),
    /* ShareRowExclusiveLock */
    LOCKBIT_ON_CONST(RowExclusiveLock)
        | LOCKBIT_ON_CONST(ShareUpdateExclusiveLock)
        | LOCKBIT_ON_CONST(ShareLock)
        | LOCKBIT_ON_CONST(ShareRowExclusiveLock)
        | LOCKBIT_ON_CONST(ExclusiveLock)
        | LOCKBIT_ON_CONST(AccessExclusiveLock),
    /* ExclusiveLock */
    LOCKBIT_ON_CONST(RowShareLock)
        | LOCKBIT_ON_CONST(RowExclusiveLock)
        | LOCKBIT_ON_CONST(ShareUpdateExclusiveLock)
        | LOCKBIT_ON_CONST(ShareLock)
        | LOCKBIT_ON_CONST(ShareRowExclusiveLock)
        | LOCKBIT_ON_CONST(ExclusiveLock)
        | LOCKBIT_ON_CONST(AccessExclusiveLock),
    /* AccessExclusiveLock */
    LOCKBIT_ON_CONST(AccessShareLock)
        | LOCKBIT_ON_CONST(RowShareLock)
        | LOCKBIT_ON_CONST(RowExclusiveLock)
        | LOCKBIT_ON_CONST(ShareUpdateExclusiveLock)
        | LOCKBIT_ON_CONST(ShareLock)
        | LOCKBIT_ON_CONST(ShareRowExclusiveLock)
        | LOCKBIT_ON_CONST(ExclusiveLock)
        | LOCKBIT_ON_CONST(AccessExclusiveLock),
];

// const version of LOCKBIT_ON for use in static initializers
const fn LOCKBIT_ON_CONST(m: LOCKMODE) -> LOCKMASK {
    1 << m
}

static mut Dummy_trace: bool = false;

static default_lockmethod: LockMethodData = LockMethodData {
    numLockModes: MaxLockMode,
    conflictTab: LockConflicts.as_ptr(),
    lockModeNames: lock_mode_names_arr.as_ptr(),
    trace_flag: &raw mut Dummy_trace as *mut bool,
};

static user_lockmethod: LockMethodData = LockMethodData {
    numLockModes: MaxLockMode,
    conflictTab: LockConflicts.as_ptr(),
    lockModeNames: lock_mode_names_arr.as_ptr(),
    trace_flag: &raw mut Dummy_trace as *mut bool,
};

/*
 * map from lock method id to the lock table data structures
 */
const LockMethods: [LockMethod; 3] = [
    ptr::null(),
    &raw const default_lockmethod,
    &raw const user_lockmethod,
];

// ============================================================
//  Shared-memory hash tables
// ============================================================

static mut LockMethodLockHash: *mut HTAB = ptr::null_mut();
static mut LockMethodProcLockHash: *mut HTAB = ptr::null_mut();
static mut LockMethodLocalHash: *mut HTAB = ptr::null_mut();
static mut FastPathStrongRelationLocks: *mut FastPathStrongRelationLockData = ptr::null_mut();

// ============================================================
//  Unported-dependency stubs
// ============================================================

// miscadmin.h
static mut MyDatabaseId: Oid = InvalidOid;
extern "C" { pub static mut MyProcNumber: ProcNumber; }
// storage/proc.h
extern "C" { pub static mut MyProc: *mut PGPROC; }
// storage/ipc/procarray.c
extern "C" { pub static mut ProcGlobal: *mut PROC_HDR; }
// utils/init/globals.c
static mut max_prepared_xacts: c_int = 0;

// utils/resource_owner.h
extern "C" { pub static mut CurrentResourceOwner: ResourceOwner; }

// utils/palloc.h / memory context
pub type TopMemoryContext = *mut c_void;
pub static mut TopMemoryContext: *mut c_void = ptr::null_mut();
pub static mut CurrentMemoryContext: *mut c_void = ptr::null_mut();

// access/xlog.h
unsafe fn RecoveryInProgress() -> bool {
    false // TODO(pg-port): access/transam/xlog.c
}
unsafe fn InRecovery() -> bool {
    false // TODO(pg-port): access/transam/xlog.c
}
unsafe fn XLogStandbyInfoActive() -> bool {
    false // TODO(pg-port): access/transam/xlog.c
}

// utils/init/globals.h -- InHotStandby
static mut InHotStandby: bool = false;

// port/pg_bitutils.h
unsafe fn mul_size(a: Size, b: Size) -> Size {
    a.saturating_mul(b) // TODO(pg-port): real overflow check
}
unsafe fn add_size(a: Size, b: Size) -> Size {
    a.saturating_add(b) // TODO(pg-port): real overflow check
}

// dynahash.c stubs
unsafe fn hash_create(
    tabname: *const c_char,
    nelem: c_long,
    info: *const HASHCTL,
    flags: c_int,
) -> *mut HTAB {
    crate::utils::hash::dynahash::hash_create(tabname, nelem, info as _, flags) as _
}
unsafe fn ShmemInitHash(
    name: *const c_char,
    init_size: c_long,
    max_size: c_long,
    infoP: *const HASHCTL,
    hash_flags: c_int,
) -> *mut HTAB {
    crate::storage::ipc::shmem::ShmemInitHash(name, init_size, max_size, infoP as _, hash_flags) as _
}
unsafe fn ShmemInitStruct(
    name: *const c_char,
    size: Size,
    foundPtr: *mut bool,
) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(name, size, foundPtr)
}
unsafe fn hash_search(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    action: c_int,
    foundPtr: *mut bool,
) -> *mut c_void {
    crate::utils::hash::dynahash::hash_search(
        hashp as _,
        keyPtr,
        core::mem::transmute::<c_int, crate::utils::hash::dynahash::HASHACTION>(action),
        foundPtr,
    )
}
unsafe fn hash_search_with_hash_value(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    hashvalue: uint32,
    action: c_int,
    foundPtr: *mut bool,
) -> *mut c_void {
    crate::utils::hash::dynahash::hash_search_with_hash_value(
        hashp as _,
        keyPtr,
        hashvalue,
        core::mem::transmute::<c_int, crate::utils::hash::dynahash::HASHACTION>(action),
        foundPtr,
    )
}
unsafe fn hash_update_hash_key(
    hashp: *mut HTAB,
    existingEntry: *mut c_void,
    newKey: *const c_void,
) -> bool {
    crate::utils::hash::dynahash::hash_update_hash_key(hashp as _, existingEntry, newKey)
}
unsafe fn hash_destroy(hashp: *mut HTAB) {
    crate::utils::hash::dynahash::hash_destroy(hashp as _)
}
unsafe fn hash_seq_init(status: *mut HASH_SEQ_STATUS, hashp: *mut HTAB) {
    crate::utils::hash::dynahash::hash_seq_init(status as _, hashp as _)
}
unsafe fn hash_seq_search(status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    crate::utils::hash::dynahash::hash_seq_search(status as _)
}
unsafe fn hash_get_num_entries(hashp: *mut HTAB) -> c_long {
    crate::utils::hash::dynahash::hash_get_num_entries(hashp as _)
}
unsafe fn hash_estimate_size(num_entries: c_long, entrysize: Size) -> Size {
    crate::utils::hash::dynahash::hash_estimate_size(num_entries, entrysize)
}
unsafe fn get_hash_value(hashp: *mut HTAB, keyPtr: *const c_void) -> uint32 {
    crate::utils::hash::dynahash::get_hash_value(hashp as _, keyPtr)
}

// LWLock stubs
unsafe fn LWLockAcquire(lock: *mut LWLock, mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockAcquire(
        lock as _,
        core::mem::transmute::<c_int, crate::storage::lmgr::lwlock::LWLockMode>(mode),
    )
}
unsafe fn LWLockRelease(lock: *mut LWLock) {
    crate::storage::lmgr::lwlock::LWLockRelease(lock as _)
}

// SpinLock stubs
unsafe fn SpinLockInit(lock: *mut slock_t) {
    crate::storage::spin::SpinLockInit(lock as _)
}
unsafe fn SpinLockAcquire(lock: *mut slock_t) {
    crate::storage::spin::SpinLockAcquire(lock as _)
}
unsafe fn SpinLockRelease(lock: *mut slock_t) {
    crate::storage::spin::SpinLockRelease(lock as _)
}

// palloc / memory context stubs
unsafe fn MemoryContextAlloc(cxt: *mut c_void, size: Size) -> *mut c_void {
    crate::utils::palloc::MemoryContextAlloc(cxt as _, size)
}
unsafe fn repalloc(ptr: *mut c_void, size: Size) -> *mut c_void {
    crate::utils::palloc::repalloc(ptr, size)
}
unsafe fn palloc(size: Size) -> *mut c_void {
    crate::utils::palloc::palloc(size)
}
unsafe fn palloc0(size: Size) -> *mut c_void {
    crate::utils::palloc::palloc0(size)
}
unsafe fn pfree(ptr: *mut c_void) {
    crate::utils::palloc::pfree(ptr)
}

// resowner.h stubs
unsafe fn ResourceOwnerRememberLock(owner: ResourceOwner, locallock: *mut LOCALLOCK) {
    crate::utils::resowner::resowner::ResourceOwnerRememberLock(owner as _, locallock as _)
}
unsafe fn ResourceOwnerForgetLock(owner: ResourceOwner, locallock: *mut LOCALLOCK) {
    crate::utils::resowner::resowner::ResourceOwnerForgetLock(owner as _, locallock as _)
}
unsafe fn ResourceOwnerGetParent(owner: ResourceOwner) -> ResourceOwner {
    crate::utils::resowner::resowner::ResourceOwnerGetParent(owner as _) as _
}

// proc.c stubs
unsafe fn ProcSleep(locallock: *mut LOCALLOCK) -> ProcWaitStatus {
    crate::storage::lmgr::proc::ProcSleep(locallock as _) as _
}
unsafe fn ProcLockWakeup(lockMethodTable: LockMethod, lock: *mut LOCK) {
    crate::storage::lmgr::proc::ProcLockWakeup(lockMethodTable as _, lock as _)
}
unsafe fn JoinWaitQueue(
    locallock: *mut LOCALLOCK,
    lockMethodTable: LockMethod,
    dontWait: bool,
) -> ProcWaitStatus {
    crate::storage::lmgr::proc::JoinWaitQueue(locallock as _, lockMethodTable as _, dontWait) as _
}
unsafe fn DeadLockReport() -> ! {
    panic!("DeadLockReport") // TODO(pg-port): storage/lmgr/deadlock.c
}
unsafe fn ProcNumberGetProc(procNumber: ProcNumber) -> *mut PGPROC {
    crate::storage::ipc::procarray::ProcNumberGetProc(procNumber as _) as _
}
unsafe fn BackendPidGetProcWithLock(pid: c_int) -> *mut PGPROC {
    crate::storage::ipc::procarray::BackendPidGetProcWithLock(pid) as _
}

// access/xlog.h
unsafe fn LogAccessExclusiveLockPrepare() {
    unimplemented!() // TODO(pg-port): access/transam/xlog.c
}
unsafe fn LogAccessExclusiveLock(dbOid: Oid, relOid: Oid) {
    unimplemented!() // TODO(pg-port): access/transam/xlog.c
}

// storage/standby.h
unsafe fn StandbyAcquireAccessExclusiveLock(xid: TransactionId, dbOid: Oid, relOid: Oid) {
    unimplemented!() // TODO(pg-port): storage/ipc/standby.c
}

// access/twophase.h / access/twophase_rmgr.h
pub const TWOPHASE_RM_LOCK_ID: uint16 = 1; // TODO(pg-port): access/twophase_rmgr.h
unsafe fn RegisterTwoPhaseRecord(
    rmid: uint16,
    info: uint16,
    data: *const c_void,
    len: Size,
) {
    unimplemented!() // TODO(pg-port): access/transam/twophase.c
}
unsafe fn TwoPhaseGetDummyProc(xid: TransactionId, lock_being_dropped: bool) -> *mut PGPROC {
    unimplemented!() // TODO(pg-port): access/transam/twophase.c
}
unsafe fn TwoPhaseGetXidByVirtualXID(
    vxid: VirtualTransactionId,
    more: *mut bool,
) -> TransactionId {
    unimplemented!() // TODO(pg-port): access/transam/twophase.c
}

// utils/ps_status.h stubs -- process-title decoration is cosmetic; no-op for now.
unsafe fn set_ps_display_suffix(_suffix: *const c_char) {}
unsafe fn set_ps_display_remove_suffix() {}

// procarray.h
unsafe fn ProcArrayLock() -> *mut LWLock {
    crate::backend_link_shims::ProcArrayLock as *mut LWLock
}

// pg_atomic.h
unsafe fn pg_atomic_read_u64(ptr: *mut u64) -> u64 {
    unimplemented!() // TODO(pg-port): port/atomics.h
}

// START/END_CRIT_SECTION
unsafe fn START_CRIT_SECTION() { /* TODO(pg-port): miscadmin.h */ }
unsafe fn END_CRIT_SECTION() { /* TODO(pg-port): miscadmin.h */ }

// GetLockHoldersAndWaiters -- TODO(pg-port): lock.c internal helper
unsafe fn GetLockHoldersAndWaiters(
    locallock: *mut LOCALLOCK,
    holders_sbuf: *mut StringInfoData,
    waiters_sbuf: *mut StringInfoData,
    lockHoldersNum: *mut c_int,
) {
    unimplemented!() // TODO(pg-port): internal to lock.c
}

// initStringInfo / pfree convenience (lib/stringinfo.h)
unsafe fn initStringInfo(buf: *mut StringInfoData) {
    unimplemented!() // TODO(pg-port): lib/stringinfo.c
}

// dclist_count (lib/ilist.h)
unsafe fn dclist_count(dcl: *const dclist_head) -> c_int {
    crate::lib::ilist::dclist_count(dcl) as _
}

// MemSet (c.h)
unsafe fn MemSet(dest: *mut c_void, val: c_int, len: Size) {
    ptr::write_bytes(dest as *mut u8, val as u8, len);
}

// Max macro (c.h)
#[inline]
fn Max(a: c_int, b: c_int) -> c_int {
    if a > b { a } else { b }
}

// ============================================================
//  LockManagerShmemInit
// ============================================================

/*
 * Initialize the lock manager's shmem data structures.
 *
 * This is called from CreateSharedMemoryAndSemaphores(), which see for more
 * comments.  In the normal postmaster case, the shared hash tables are
 * created here, and backends inherit pointers to them via fork().  In the
 * EXEC_BACKEND case, each backend re-executes this code to obtain pointers
 * to the already existing shared hash tables.  In either case, each backend
 * must also call InitLockManagerAccess() to create the locallock hash table.
 */
pub unsafe fn LockManagerShmemInit() {
    let mut info: HASHCTL = core::mem::zeroed();
    let max_table_size: c_long;
    let init_table_size: c_long;
    let mut found: bool = false;

    /*
     * Compute init/max size to request for lock hashtables.  Note these
     * calculations must agree with LockManagerShmemSize!
     */
    max_table_size = NLOCKENTS();
    init_table_size = max_table_size / 2;

    /*
     * Allocate hash table for LOCK structs.  This stores per-locked-object
     * information.
     */
    info.keysize = size_of::<LOCKTAG>();
    info.entrysize = size_of::<LOCK>();
    info.num_partitions = NUM_LOCK_PARTITIONS as c_long;

    LockMethodLockHash = ShmemInitHash(
        c"LOCK hash".as_ptr(),
        init_table_size,
        max_table_size,
        &raw const info,
        HASH_ELEM | HASH_BLOBS | HASH_PARTITION,
    );

    /* Assume an average of 2 holders per lock */
    let max_table_size = max_table_size * 2;
    let init_table_size = init_table_size * 2;

    /*
     * Allocate hash table for PROCLOCK structs.  This stores
     * per-lock-per-holder information.
     */
    info.keysize = size_of::<PROCLOCKTAG>();
    info.entrysize = size_of::<PROCLOCK>();
    info.hash = Some(proclock_hash);
    info.num_partitions = NUM_LOCK_PARTITIONS as c_long;

    LockMethodProcLockHash = ShmemInitHash(
        c"PROCLOCK hash".as_ptr(),
        init_table_size,
        max_table_size,
        &raw const info,
        HASH_ELEM | HASH_FUNCTION | HASH_PARTITION,
    );

    /*
     * Allocate fast-path structures.
     */
    FastPathStrongRelationLocks = ShmemInitStruct(
        c"Fast Path Strong Relation Lock Data".as_ptr(),
        size_of::<FastPathStrongRelationLockData>(),
        &raw mut found,
    ) as *mut FastPathStrongRelationLockData;
    if !found {
        SpinLockInit(&raw mut (*FastPathStrongRelationLocks).mutex);
    }
}

/*
 * Initialize the lock manager's backend-private data structures.
 */
pub unsafe fn InitLockManagerAccess() {
    /*
     * Allocate non-shared hash table for LOCALLOCK structs.  This stores lock
     * counts and resource owner information.
     */
    let mut info: HASHCTL = core::mem::zeroed();

    info.keysize = size_of::<LOCALLOCKTAG>();
    info.entrysize = size_of::<LOCALLOCK>();

    LockMethodLocalHash = hash_create(
        c"LOCALLOCK hash".as_ptr(),
        16,
        &raw const info,
        HASH_ELEM | HASH_BLOBS,
    );
}

/*
 * Fetch the lock method table associated with a given lock
 */
pub unsafe fn GetLocksMethodTable(lock: *const LOCK) -> LockMethod {
    let lockmethodid = LOCK_LOCKMETHOD(&*lock);
    Assert!(0 < lockmethodid && (lockmethodid as usize) < LockMethods.len());
    LockMethods[lockmethodid as usize]
}

/*
 * Fetch the lock method table associated with a given locktag
 */
pub unsafe fn GetLockTagsMethodTable(locktag: *const LOCKTAG) -> LockMethod {
    let lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;
    Assert!(0 < lockmethodid && (lockmethodid as usize) < LockMethods.len());
    LockMethods[lockmethodid as usize]
}

/*
 * Compute the hash code associated with a LOCKTAG.
 *
 * To avoid unnecessary recomputations of the hash code, we try to do this
 * just once per function, and then pass it around as needed.
 */
pub unsafe fn LockTagHashCode(locktag: *const LOCKTAG) -> uint32 {
    get_hash_value(LockMethodLockHash, locktag as *const c_void)
}

/*
 * Compute the hash code associated with a PROCLOCKTAG.
 *
 * Because we want to use just one set of partition locks for both the
 * LOCK and PROCLOCK hash tables, we have to make sure that PROCLOCKs
 * fall into the same partition number as their associated LOCKs.
 * dynahash.c expects the partition number to be the low-order bits of
 * the hash code, and therefore a PROCLOCKTAG's hash code must have the
 * same low-order bits as the associated LOCKTAG's hash code.  We achieve
 * this with this specialized hash function.
 */
unsafe extern "C" fn proclock_hash(key: *const c_void, keysize: Size) -> uint32 {
    let proclocktag = key as *const PROCLOCKTAG;
    let mut lockhash: uint32;
    let procptr: usize;

    Assert!(keysize == size_of::<PROCLOCKTAG>());

    /* Look into the associated LOCK object, and compute its hash code */
    lockhash = LockTagHashCode(&(*(*proclocktag).myLock).tag);

    /*
     * To make the hash code also depend on the PGPROC, we xor the proc
     * struct's address into the hash code, left-shifted so that the
     * partition-number bits don't change.  Since this is only a hash, we
     * don't care if we lose high-order bits of the address.
     */
    procptr = (*proclocktag).myProc as usize;
    lockhash ^= (procptr as uint32) << LOG2_NUM_LOCK_PARTITIONS;

    lockhash
}

/*
 * Compute the hash code associated with a PROCLOCKTAG, given the hashcode
 * for its underlying LOCK.
 *
 * We use this just to avoid redundant calls of LockTagHashCode().
 */
#[inline]
unsafe fn ProcLockHashCode(proclocktag: *const PROCLOCKTAG, hashcode: uint32) -> uint32 {
    let mut lockhash = hashcode;
    let procptr: usize = (*proclocktag).myProc as usize;
    lockhash ^= (procptr as uint32) << LOG2_NUM_LOCK_PARTITIONS;
    lockhash
}

/*
 * Given two lock modes, return whether they would conflict.
 */
pub unsafe fn DoLockModesConflict(mode1: LOCKMODE, mode2: LOCKMODE) -> bool {
    let lockMethodTable = LockMethods[DEFAULT_LOCKMETHOD as usize];
    ((*lockMethodTable).conflictTab.add(mode1 as usize).read() & LOCKBIT_ON(mode2)) != 0
}

/*
 * LockHeldByMe -- test whether lock 'locktag' is held by the current
 *   transaction
 *
 * Returns true if current transaction holds a lock on 'tag' of mode
 * 'lockmode'.  If 'orstronger' is true, a stronger lockmode is also OK.
 */
pub unsafe fn LockHeldByMe(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    orstronger: bool,
) -> bool {
    let mut localtag: LOCALLOCKTAG = core::mem::zeroed();
    let locallock: *mut LOCALLOCK;

    /*
     * See if there is a LOCALLOCK entry for this lock and lockmode
     */
    /* must clear padding */
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    locallock = hash_search(
        LockMethodLocalHash,
        &raw const localtag as *const c_void,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut LOCALLOCK;

    if !locallock.is_null() && (*locallock).nLocks > 0 {
        return true;
    }

    if orstronger {
        let mut slockmode: LOCKMODE = lockmode + 1;
        while slockmode <= MaxLockMode {
            if LockHeldByMe(locktag, slockmode, false) {
                return true;
            }
            slockmode += 1;
        }
    }

    false
}

#[cfg(any())] /* USE_ASSERT_CHECKING build only */
/*
 * GetLockMethodLocalHash -- return the hash of local locks, for modules that
 *   evaluate assertions based on all locks held.
 */
pub unsafe fn GetLockMethodLocalHash() -> *mut HTAB {
    LockMethodLocalHash
}

/*
 * LockHasWaiters -- look up 'locktag' and check if releasing this
 *   lock would wake up other processes waiting for it.
 */
pub unsafe fn LockHasWaiters(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    sessionLock: bool,
) -> bool {
    let lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;
    let lockMethodTable: LockMethod;
    let mut localtag: LOCALLOCKTAG = core::mem::zeroed();
    let locallock: *mut LOCALLOCK;
    let lock: *mut LOCK;
    let proclock: *mut PROCLOCK;
    let partitionLock: *mut LWLock;
    let mut hasWaiters: bool = false;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }
    lockMethodTable = LockMethods[lockmethodid as usize];
    if lockmode <= 0 || lockmode > (*lockMethodTable).numLockModes {
        elog!(ERROR, "unrecognized lock mode: {}", lockmode);
    }

    /*
     * Find the LOCALLOCK entry for this lock and lockmode
     */
    /* must clear padding */
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    locallock = hash_search(
        LockMethodLocalHash,
        &raw const localtag as *const c_void,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut LOCALLOCK;

    /*
     * let the caller print its own error message, too. Do not ereport(ERROR).
     */
    if locallock.is_null() || (*locallock).nLocks <= 0 {
        elog!(
            WARNING,
            "you don't own a lock of type {}",
            std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy()
        );
        return false;
    }

    /*
     * Check the shared lock table.
     */
    partitionLock = LockHashPartitionLock((*locallock).hashcode);

    LWLockAcquire(partitionLock, LW_SHARED);

    /*
     * We don't need to re-find the lock or proclock, since we kept their
     * addresses in the locallock table, and they couldn't have been removed
     * while we were holding a lock on them.
     */
    lock = (*locallock).lock;
    proclock = (*locallock).proclock;

    /*
     * Double-check that we are actually holding a lock of the type we want to
     * release.
     */
    if ((*proclock).holdMask & LOCKBIT_ON(lockmode)) == 0 {
        LWLockRelease(partitionLock);
        elog!(
            WARNING,
            "you don't own a lock of type {}",
            std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy()
        );
        RemoveLocalLock(locallock);
        return false;
    }

    /*
     * Do the checking.
     */
    if ((*lockMethodTable).conflictTab.add(lockmode as usize).read()
        & (*lock).waitMask) != 0
    {
        hasWaiters = true;
    }

    LWLockRelease(partitionLock);

    hasWaiters
}

// ============================================================
//  LockAcquire / LockAcquireExtended
// ============================================================

/*
 * LockAcquire -- Check for lock conflicts, sleep if conflict found,
 *   set lock if/when no conflicts.
 */
pub unsafe fn LockAcquire(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    sessionLock: bool,
    dontWait: bool,
) -> LockAcquireResult {
    LockAcquireExtended(
        locktag,
        lockmode,
        sessionLock,
        dontWait,
        true,
        ptr::null_mut(),
        false,
    )
}

/*
 * LockAcquireExtended - allows us to specify additional options
 *
 * reportMemoryError specifies whether a lock request that fills the lock
 * table should generate an ERROR or not.
 *
 * If locallockp isn't NULL, *locallockp receives a pointer to the LOCALLOCK
 * table entry if a lock is successfully acquired, or NULL if not.
 *
 * logLockFailure indicates whether to log details when a lock acquisition
 * fails with dontWait = true.
 */
pub unsafe fn LockAcquireExtended(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    sessionLock: bool,
    dontWait: bool,
    reportMemoryError: bool,
    locallockp: *mut *mut LOCALLOCK,
    logLockFailure: bool,
) -> LockAcquireResult {
    let lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;
    let lockMethodTable: LockMethod;
    let mut localtag: LOCALLOCKTAG = core::mem::zeroed();
    let locallock: *mut LOCALLOCK;
    let lock: *mut LOCK;
    let proclock: *mut PROCLOCK;
    let mut found: bool = false;
    let owner: ResourceOwner;
    let hashcode: uint32;
    let partitionLock: *mut LWLock;
    let found_conflict: bool;
    let mut waitResult: ProcWaitStatus;
    let mut log_lock: bool = false;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }
    lockMethodTable = LockMethods[lockmethodid as usize];
    if lockmode <= 0 || lockmode > (*lockMethodTable).numLockModes {
        elog!(ERROR, "unrecognized lock mode: {}", lockmode);
    }

    if RecoveryInProgress()
        && !InRecovery()
        && ((*locktag).locktag_type == LOCKTAG_OBJECT as uint8
            || (*locktag).locktag_type == LOCKTAG_RELATION as uint8)
        && lockmode > RowExclusiveLock
    {
        ereport!(ERROR, errmsg!(
                "cannot acquire lock mode {} on database objects while recovery is in progress",
                std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy()
            ));
    }

    /* Identify owner for lock */
    if sessionLock {
        owner = ptr::null_mut();
    } else {
        owner = CurrentResourceOwner;
    }

    /*
     * Find or create a LOCALLOCK entry for this lock and lockmode
     */
    /* must clear padding */
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    let locallock = hash_search(
        LockMethodLocalHash,
        &raw const localtag as *const c_void,
        HASH_ENTER,
        &raw mut found,
    ) as *mut LOCALLOCK;

    /*
     * if it's a new locallock object, initialize it
     */
    if !found {
        (*locallock).lock = ptr::null_mut();
        (*locallock).proclock = ptr::null_mut();
        (*locallock).hashcode = LockTagHashCode(&raw const localtag.lock);
        (*locallock).nLocks = 0;
        (*locallock).holdsStrongLockCount = false;
        (*locallock).lockCleared = false;
        (*locallock).numLockOwners = 0;
        (*locallock).maxLockOwners = 8;
        (*locallock).lockOwners = ptr::null_mut(); /* in case next line fails */
        (*locallock).lockOwners = MemoryContextAlloc(
            TopMemoryContext,
            (*locallock).maxLockOwners as Size * size_of::<LOCALLOCKOWNER>(),
        ) as *mut LOCALLOCKOWNER;
    } else {
        /* Make sure there will be room to remember the lock */
        if (*locallock).numLockOwners >= (*locallock).maxLockOwners {
            let newsize = (*locallock).maxLockOwners * 2;
            (*locallock).lockOwners = repalloc(
                (*locallock).lockOwners as *mut c_void,
                newsize as Size * size_of::<LOCALLOCKOWNER>(),
            ) as *mut LOCALLOCKOWNER;
            (*locallock).maxLockOwners = newsize;
        }
    }
    let hashcode = (*locallock).hashcode;

    if !locallockp.is_null() {
        *locallockp = locallock;
    }

    /*
     * If we already hold the lock, we can just increase the count locally.
     *
     * If lockCleared is already set, caller need not worry about absorbing
     * sinval messages related to the lock's object.
     */
    if (*locallock).nLocks > 0 {
        GrantLockLocal(locallock, owner);
        if (*locallock).lockCleared {
            return LOCKACQUIRE_ALREADY_CLEAR;
        } else {
            return LOCKACQUIRE_ALREADY_HELD;
        }
    }

    /*
     * We don't acquire any other heavyweight lock while holding the relation
     * extension lock.
     */
    Assert!(!IsRelationExtensionLockHeld);

    /*
     * Prepare to emit a WAL record if acquisition of this lock needs to be
     * replayed in a standby server.
     */
    if lockmode >= AccessExclusiveLock
        && (*locktag).locktag_type == LOCKTAG_RELATION as uint8
        && !RecoveryInProgress()
        && XLogStandbyInfoActive()
    {
        LogAccessExclusiveLockPrepare();
        log_lock = true;
    }

    /*
     * Attempt to take lock via fast path, if eligible.
     */
    if EligibleForRelationFastPath(locktag, lockmode)
        && FastPathLocalUseCounts[FAST_PATH_REL_GROUP((*locktag).locktag_field2) as usize]
            < FP_LOCK_SLOTS_PER_GROUP as c_int
    {
        let fasthashcode = FastPathStrongLockHashPartition(hashcode);
        let acquired: bool;

        /*
         * LWLockAcquire acts as a memory sequencing point.
         */
        LWLockAcquire(&raw mut (*MyProc).fpInfoLock as *mut LWLock, LW_EXCLUSIVE);
        if (*FastPathStrongRelationLocks).count[fasthashcode] != 0 {
            acquired = false;
        } else {
            acquired = FastPathGrantRelationLock((*locktag).locktag_field2, lockmode);
        }
        LWLockRelease(&raw mut (*MyProc).fpInfoLock as *mut LWLock);
        if acquired {
            /*
             * The locallock might contain stale pointers to some old shared
             * objects; we MUST reset these to null before considering the
             * lock to be acquired via fast-path.
             */
            (*locallock).lock = ptr::null_mut();
            (*locallock).proclock = ptr::null_mut();
            GrantLockLocal(locallock, owner);
            return LOCKACQUIRE_OK;
        }
    }

    /*
     * If this lock could potentially have been taken via the fast-path by
     * some other backend, we must temporarily disable further use of the
     * fast-path for this lock tag, and migrate any locks already taken via
     * this method to the main lock table.
     */
    if ConflictsWithRelationFastPath(locktag, lockmode) {
        let fasthashcode = FastPathStrongLockHashPartition(hashcode);

        BeginStrongLockAcquire(locallock, fasthashcode as uint32);
        if !FastPathTransferRelationLocks(lockMethodTable, locktag, hashcode) {
            AbortStrongLockAcquire();
            if (*locallock).nLocks == 0 {
                RemoveLocalLock(locallock);
            }
            if !locallockp.is_null() {
                *locallockp = ptr::null_mut();
            }
            if reportMemoryError {
                ereport!(ERROR, errmsg!("out of shared memory")) /* C also: errhint */;
            } else {
                return LOCKACQUIRE_NOT_AVAIL;
            }
        }
    }

    /*
     * We didn't find the lock in our LOCALLOCK table, and we didn't manage
     * to take it via the fast-path, either, so we've got to mess with the
     * shared lock table.
     */
    let partitionLock = LockHashPartitionLock(hashcode);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * Find or create lock and proclock entries with this tag
     */
    let proclock = SetupLockInTable(lockMethodTable, MyProc, locktag, hashcode, lockmode);
    if proclock.is_null() {
        AbortStrongLockAcquire();
        LWLockRelease(partitionLock);
        if (*locallock).nLocks == 0 {
            RemoveLocalLock(locallock);
        }
        if !locallockp.is_null() {
            *locallockp = ptr::null_mut();
        }
        if reportMemoryError {
            ereport!(ERROR, errmsg!("out of shared memory")) /* C also: errhint */;
        } else {
            return LOCKACQUIRE_NOT_AVAIL;
        }
    }
    (*locallock).proclock = proclock;
    let lock = (*proclock).tag.myLock;
    (*locallock).lock = lock;

    /*
     * If lock requested conflicts with locks requested by waiters, must join
     * wait queue.  Otherwise, check for conflict with already-held locks.
     */
    if (*lockMethodTable).conflictTab.add(lockmode as usize).read() & (*lock).waitMask != 0 {
        let found_conflict = true;
        waitResult = JoinWaitQueue(locallock, lockMethodTable, dontWait);
    } else {
        let found_conflict = LockCheckConflicts(lockMethodTable, lockmode, lock, proclock);
        if !found_conflict {
            /* No conflict with held or previously requested locks */
            GrantLock(lock, proclock, lockmode);
            waitResult = PROC_WAIT_STATUS_OK;
        } else {
            /*
             * Join the lock's wait queue.
             */
            waitResult = JoinWaitQueue(locallock, lockMethodTable, dontWait);
        }
    }

    if waitResult == PROC_WAIT_STATUS_ERROR {
        /*
         * We're not getting the lock because a deadlock was detected already
         * while trying to join the wait queue, or because we would have to
         * wait but the caller requested no blocking.
         */
        AbortStrongLockAcquire();

        if (*proclock).holdMask == 0 {
            let proclock_hashcode = ProcLockHashCode(&raw const (*proclock).tag, hashcode);
            dlist_delete(&raw mut (*proclock).lockLink);
            dlist_delete(&raw mut (*proclock).procLink);
            if hash_search_with_hash_value(
                LockMethodProcLockHash,
                &raw const (*proclock).tag as *const c_void,
                proclock_hashcode,
                HASH_REMOVE,
                ptr::null_mut(),
            )
            .is_null()
            {
                elog!(PANIC, "proclock table corrupted");
            }
        }
        (*lock).nRequested -= 1;
        (*lock).requested[lockmode as usize] -= 1;
        Assert!(((*lock).nRequested > 0) && ((*lock).requested[lockmode as usize] >= 0));
        Assert!((*lock).nGranted <= (*lock).nRequested);
        LWLockRelease(partitionLock);
        if (*locallock).nLocks == 0 {
            RemoveLocalLock(locallock);
        }

        if dontWait {
            /*
             * Log lock holders and waiters as a detail log message if
             * logLockFailure = true and lock acquisition fails with dontWait = true
             */
            if logLockFailure {
                let mut buf: StringInfoData = core::mem::zeroed();
                let mut lock_waiters_sbuf: StringInfoData = core::mem::zeroed();
                let mut lock_holders_sbuf: StringInfoData = core::mem::zeroed();
                let mut lockHoldersNum: c_int = 0;

                initStringInfo(&raw mut buf);
                initStringInfo(&raw mut lock_waiters_sbuf);
                initStringInfo(&raw mut lock_holders_sbuf);

                DescribeLockTag(&raw mut buf, &raw const (*locallock).tag.lock as *const _);
                let modename =
                    GetLockmodeName((*locallock).tag.lock.locktag_lockmethodid as LOCKMETHODID, lockmode);

                /* Gather a list of all lock holders and waiters */
                LWLockAcquire(partitionLock, LW_SHARED);
                GetLockHoldersAndWaiters(
                    locallock,
                    &raw mut lock_holders_sbuf,
                    &raw mut lock_waiters_sbuf,
                    &raw mut lockHoldersNum,
                );
                LWLockRelease(partitionLock);

                elog!(
                    LOG,
                    "process {} could not obtain {} on {:?}",
                    (*MyProc).pid,
                    core::ffi::CStr::from_ptr(modename).to_string_lossy(),
                    buf.data
                );

                pfree(buf.data as *mut c_void);
                pfree(lock_holders_sbuf.data as *mut c_void);
                pfree(lock_waiters_sbuf.data as *mut c_void);
            }
            if !locallockp.is_null() {
                *locallockp = ptr::null_mut();
            }
            return LOCKACQUIRE_NOT_AVAIL;
        } else {
            DeadLockReport();
            /* DeadLockReport() will not return */
        }
    }

    /*
     * We are now in the lock queue, or the lock was already granted.
     * If queued, go to sleep.
     */
    if waitResult == PROC_WAIT_STATUS_WAITING {
        Assert!(!dontWait);
        LWLockRelease(partitionLock);

        waitResult = WaitOnLock(locallock, owner);

        if waitResult == PROC_WAIT_STATUS_ERROR {
            /*
             * We failed as a result of a deadlock, see CheckDeadLock().
             */
            Assert!(!dontWait);
            DeadLockReport();
            /* DeadLockReport() will not return */
        }
    } else {
        LWLockRelease(partitionLock);
    }
    Assert!(waitResult == PROC_WAIT_STATUS_OK);

    /* The lock was granted to us.  Update the local lock entry accordingly */
    Assert!(((*proclock).holdMask & LOCKBIT_ON(lockmode)) != 0);
    GrantLockLocal(locallock, owner);

    /*
     * Lock state is fully up-to-date now.
     */
    FinishStrongLockAcquire();

    /*
     * Emit a WAL record if acquisition of this lock needs to be replayed in a
     * standby server.
     */
    if log_lock {
        LogAccessExclusiveLock((*locktag).locktag_field1, (*locktag).locktag_field2);
    }

    LOCKACQUIRE_OK
}

// ============================================================
//  SetupLockInTable / helper fns
// ============================================================

/*
 * Find or create LOCK and PROCLOCK objects as needed for a new lock request.
 *
 * Returns the PROCLOCK object, or NULL if we failed to create the objects
 * for lack of shared memory.
 *
 * The appropriate partition lock must be held at entry, and will be held at exit.
 */
unsafe fn SetupLockInTable(
    lockMethodTable: LockMethod,
    proc_: *mut PGPROC,
    locktag: *const LOCKTAG,
    hashcode: uint32,
    lockmode: LOCKMODE,
) -> *mut PROCLOCK {
    let lock: *mut LOCK;
    let proclock: *mut PROCLOCK;
    let mut proclocktag: PROCLOCKTAG = core::mem::zeroed();
    let proclock_hashcode: uint32;
    let mut found: bool = false;

    /*
     * Find or create a lock with this tag.
     */
    lock = hash_search_with_hash_value(
        LockMethodLockHash,
        locktag as *const c_void,
        hashcode,
        HASH_ENTER_NULL,
        &raw mut found,
    ) as *mut LOCK;
    if lock.is_null() {
        return ptr::null_mut();
    }

    /*
     * if it's a new lock object, initialize it
     */
    if !found {
        (*lock).grantMask = 0;
        (*lock).waitMask = 0;
        dlist_init(&raw mut (*lock).procLocks);
        { dlist_init(&raw mut (*lock).waitProcs.dlist); (*lock).waitProcs.count = 0; }
        (*lock).nRequested = 0;
        (*lock).nGranted = 0;
        MemSet(
            (*lock).requested.as_mut_ptr() as *mut c_void,
            0,
            size_of::<c_int>() * MAX_LOCKMODES,
        );
        MemSet(
            (*lock).granted.as_mut_ptr() as *mut c_void,
            0,
            size_of::<c_int>() * MAX_LOCKMODES,
        );
    } else {
        Assert!(((*lock).nRequested >= 0) && ((*lock).requested[lockmode as usize] >= 0));
        Assert!(((*lock).nGranted >= 0) && ((*lock).granted[lockmode as usize] >= 0));
        Assert!((*lock).nGranted <= (*lock).nRequested);
    }

    /*
     * Create the hash key for the proclock table.
     */
    proclocktag.myLock = lock;
    proclocktag.myProc = proc_;

    proclock_hashcode = ProcLockHashCode(&raw const proclocktag, hashcode);

    /*
     * Find or create a proclock entry with this tag
     */
    proclock = hash_search_with_hash_value(
        LockMethodProcLockHash,
        &raw const proclocktag as *const c_void,
        proclock_hashcode,
        HASH_ENTER_NULL,
        &raw mut found,
    ) as *mut PROCLOCK;
    if proclock.is_null() {
        /* Oops, not enough shmem for the proclock */
        if (*lock).nRequested == 0 {
            /*
             * There are no other requestors of this lock, so garbage-collect
             * the lock object.
             */
            Assert!(dlist_is_empty(&raw const (*lock).procLocks));
            if hash_search_with_hash_value(
                LockMethodLockHash,
                &raw const (*lock).tag as *const c_void,
                hashcode,
                HASH_REMOVE,
                ptr::null_mut(),
            )
            .is_null()
            {
                elog!(PANIC, "lock table corrupted");
            }
        }
        return ptr::null_mut();
    }

    /*
     * If new, initialize the new entry
     */
    if !found {
        let partition = LockHashPartition(hashcode);

        /*
         * It might seem unsafe to access proclock->groupLeader without a
         * lock, but it's not really.  Either we are initializing a proclock
         * on our own behalf, in which case our group leader isn't changing
         * because the group leader for a process can only ever be changed by
         * the process itself; or else we are transferring a fast-path lock.
         */
        (*proclock).groupLeader = if !(*proc_).lockGroupLeader.is_null() {
            (*proc_).lockGroupLeader
        } else {
            proc_
        };
        (*proclock).holdMask = 0;
        (*proclock).releaseMask = 0;
        /* Add proclock to appropriate lists */
        dlist_push_tail(&raw mut (*lock).procLocks, &raw mut (*proclock).lockLink);
        dlist_push_tail(
            &raw mut (*proc_).myProcLocks[partition],
            &raw mut (*proclock).procLink
        );
    } else {
        Assert!(((*proclock).holdMask & !(*lock).grantMask) == 0);
    }

    /*
     * lock->nRequested and lock->requested[] count the total number of
     * requests, whether granted or waiting, so increment those immediately.
     */
    (*lock).nRequested += 1;
    (*lock).requested[lockmode as usize] += 1;
    Assert!(((*lock).nRequested > 0) && ((*lock).requested[lockmode as usize] > 0));

    /*
     * We shouldn't already hold the desired lock; else locallock table is broken.
     */
    if ((*proclock).holdMask & LOCKBIT_ON(lockmode)) != 0 {
        elog!(
            ERROR,
            "lock {} on object {}/{}/{} is already held",
            std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy(),
            (*lock).tag.locktag_field1,
            (*lock).tag.locktag_field2,
            (*lock).tag.locktag_field3
        );
    }

    proclock
}

/*
 * Check and set/reset the flag that we hold the relation extension lock.
 */
#[inline]
unsafe fn CheckAndSetLockHeld(locallock: *mut LOCALLOCK, acquired: bool) {
    #[cfg(any())] /* USE_ASSERT_CHECKING build only */
    {
        if LOCALLOCK_LOCKTAG(&*locallock) == LOCKTAG_RELATION_EXTEND {
            IsRelationExtensionLockHeld = acquired;
        }
    }
}

/*
 * Subroutine to free a locallock entry
 */
unsafe fn RemoveLocalLock(locallock: *mut LOCALLOCK) {
    let mut i: c_int;

    i = (*locallock).numLockOwners - 1;
    while i >= 0 {
        if !(*(*locallock).lockOwners.add(i as usize)).owner.is_null() {
            ResourceOwnerForgetLock(
                (*(*locallock).lockOwners.add(i as usize)).owner,
                locallock,
            );
        }
        i -= 1;
    }
    (*locallock).numLockOwners = 0;
    if !(*locallock).lockOwners.is_null() {
        pfree((*locallock).lockOwners as *mut c_void);
    }
    (*locallock).lockOwners = ptr::null_mut();

    if (*locallock).holdsStrongLockCount {
        let fasthashcode = FastPathStrongLockHashPartition((*locallock).hashcode);

        SpinLockAcquire(&raw mut (*FastPathStrongRelationLocks).mutex);
        Assert!((*FastPathStrongRelationLocks).count[fasthashcode] > 0);
        (*FastPathStrongRelationLocks).count[fasthashcode] -= 1;
        (*locallock).holdsStrongLockCount = false;
        SpinLockRelease(&raw mut (*FastPathStrongRelationLocks).mutex);
    }

    if hash_search(
        LockMethodLocalHash,
        &raw const (*locallock).tag as *const c_void,
        HASH_REMOVE,
        ptr::null_mut(),
    )
    .is_null()
    {
        elog!(WARNING, "locallock table corrupted");
    }

    /*
     * Indicate that the lock is released for certain types of locks
     */
    CheckAndSetLockHeld(locallock, false);
}

// ============================================================
//  LockCheckConflicts / GrantLock / UnGrantLock / CleanUpLock
// ============================================================

/*
 * LockCheckConflicts -- test whether requested lock conflicts with those
 *   already granted.
 *
 * Returns true if conflict, false if no conflict.
 */
pub unsafe fn LockCheckConflicts(
    lockMethodTable: LockMethod,
    lockmode: LOCKMODE,
    lock: *mut LOCK,
    proclock: *mut PROCLOCK,
) -> bool {
    let numLockModes = (*lockMethodTable).numLockModes;
    let myLocks: LOCKMASK;
    let conflictMask = (*lockMethodTable).conflictTab.add(lockmode as usize).read();
    let mut conflictsRemaining: [c_int; MAX_LOCKMODES] = [0; MAX_LOCKMODES];
    let mut totalConflictsRemaining: c_int = 0;
    let mut i: c_int;

    /*
     * First check for global conflicts.
     */
    if (conflictMask & (*lock).grantMask) == 0 {
        return false;
    }

    /*
     * Something conflicts.  But it could still be my own lock, or a
     * lock held by another member of my locking group.
     */
    myLocks = (*proclock).holdMask;
    i = 1;
    while i <= numLockModes {
        if (conflictMask & LOCKBIT_ON(i)) == 0 {
            conflictsRemaining[i as usize] = 0;
            i += 1;
            continue;
        }
        conflictsRemaining[i as usize] = (*lock).granted[i as usize];
        if (myLocks & LOCKBIT_ON(i)) != 0 {
            conflictsRemaining[i as usize] -= 1;
        }
        totalConflictsRemaining += conflictsRemaining[i as usize];
        i += 1;
    }

    /* If no conflicts remain, we get the lock. */
    if totalConflictsRemaining == 0 {
        return false;
    }

    /* If no group locking, it's definitely a conflict. */
    if (*proclock).groupLeader == MyProc && (*MyProc).lockGroupLeader.is_null() {
        Assert!((*proclock).tag.myProc == MyProc);
        return true;
    }

    /*
     * The relation extension lock conflicts even between the group members.
     */
    if LOCK_LOCKTAG(&*lock) == LOCKTAG_RELATION_EXTEND {
        return true;
    }

    /*
     * Locks held in conflicting modes by members of our own lock group are
     * not real conflicts.
     */
    let mut proclock_iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(proclock_iter, &mut (*lock).procLocks, {
        let otherproclock = dlist_container!(PROCLOCK, lockLink, proclock_iter.cur);

        if proclock != otherproclock
            && (*proclock).groupLeader == (*otherproclock).groupLeader
            && ((*otherproclock).holdMask & conflictMask) != 0
        {
            let intersectMask = (*otherproclock).holdMask & conflictMask;
            i = 1;
            while i <= numLockModes {
                if (intersectMask & LOCKBIT_ON(i)) != 0 {
                    if conflictsRemaining[i as usize] <= 0 {
                        elog!(PANIC, "proclocks held do not match lock");
                    }
                    conflictsRemaining[i as usize] -= 1;
                    totalConflictsRemaining -= 1;
                }
                i += 1;
            }

            if totalConflictsRemaining == 0 {
                return false;
            }
        }
    });

    /* Nope, it's a real conflict. */
    true
}

/*
 * GrantLock -- update the lock and proclock data structures to show
 *   the lock request has been granted.
 */
pub unsafe fn GrantLock(lock: *mut LOCK, proclock: *mut PROCLOCK, lockmode: LOCKMODE) {
    (*lock).nGranted += 1;
    (*lock).granted[lockmode as usize] += 1;
    (*lock).grantMask |= LOCKBIT_ON(lockmode);
    if (*lock).granted[lockmode as usize] == (*lock).requested[lockmode as usize] {
        (*lock).waitMask &= LOCKBIT_OFF(lockmode);
    }
    (*proclock).holdMask |= LOCKBIT_ON(lockmode);
    Assert!(((*lock).nGranted > 0) && ((*lock).granted[lockmode as usize] > 0));
    Assert!((*lock).nGranted <= (*lock).nRequested);
}

/*
 * UnGrantLock -- opposite of GrantLock.
 *
 * Returns true if there were any waiters waiting on the lock that should
 * now be woken up with ProcLockWakeup.
 */
unsafe fn UnGrantLock(
    lock: *mut LOCK,
    lockmode: LOCKMODE,
    proclock: *mut PROCLOCK,
    lockMethodTable: LockMethod,
) -> bool {
    let mut wakeupNeeded: bool = false;

    Assert!(((*lock).nRequested > 0) && ((*lock).requested[lockmode as usize] > 0));
    Assert!(((*lock).nGranted > 0) && ((*lock).granted[lockmode as usize] > 0));
    Assert!((*lock).nGranted <= (*lock).nRequested);

    /*
     * fix the general lock stats
     */
    (*lock).nRequested -= 1;
    (*lock).requested[lockmode as usize] -= 1;
    (*lock).nGranted -= 1;
    (*lock).granted[lockmode as usize] -= 1;

    if (*lock).granted[lockmode as usize] == 0 {
        /* change the conflict mask.  No more of this lock type. */
        (*lock).grantMask &= LOCKBIT_OFF(lockmode);
    }

    /*
     * We need only run ProcLockWakeup if the released lock conflicts with
     * at least one of the lock types requested by waiter(s).
     */
    if (*lockMethodTable).conflictTab.add(lockmode as usize).read() & (*lock).waitMask != 0 {
        wakeupNeeded = true;
    }

    /*
     * Now fix the per-proclock state.
     */
    (*proclock).holdMask &= LOCKBIT_OFF(lockmode);

    wakeupNeeded
}

/*
 * CleanUpLock -- clean up after releasing a lock.  We garbage-collect the
 * proclock and lock objects if possible, and call ProcLockWakeup if there
 * are remaining requests and the caller says it's OK.
 */
unsafe fn CleanUpLock(
    lock: *mut LOCK,
    proclock: *mut PROCLOCK,
    lockMethodTable: LockMethod,
    hashcode: uint32,
    wakeupNeeded: bool,
) {
    /*
     * If this was my last hold on this lock, delete my entry in the proclock
     * table.
     */
    if (*proclock).holdMask == 0 {
        let proclock_hashcode: uint32;

        dlist_delete(&raw mut (*proclock).lockLink);
        dlist_delete(&raw mut (*proclock).procLink);
        proclock_hashcode = ProcLockHashCode(&raw const (*proclock).tag, hashcode);
        if hash_search_with_hash_value(
            LockMethodProcLockHash,
            &raw const (*proclock).tag as *const c_void,
            proclock_hashcode,
            HASH_REMOVE,
            ptr::null_mut(),
        )
        .is_null()
        {
            elog!(PANIC, "proclock table corrupted");
        }
    }

    if (*lock).nRequested == 0 {
        /*
         * The caller just released the last lock, so garbage-collect the
         * lock object.
         */
        Assert!(dlist_is_empty(&raw const (*lock).procLocks));
        if hash_search_with_hash_value(
            LockMethodLockHash,
            &raw const (*lock).tag as *const c_void,
            hashcode,
            HASH_REMOVE,
            ptr::null_mut(),
        )
        .is_null()
        {
            elog!(PANIC, "lock table corrupted");
        }
    } else if wakeupNeeded {
        /* There are waiters on this lock, so wake them up. */
        ProcLockWakeup(lockMethodTable, lock);
    }
}

/*
 * GrantLockLocal -- update the locallock data structures to show
 *   the lock request has been granted.
 */
unsafe fn GrantLockLocal(locallock: *mut LOCALLOCK, owner: ResourceOwner) {
    let lockOwners: *mut LOCALLOCKOWNER = (*locallock).lockOwners;
    let mut i: c_int;

    Assert!((*locallock).numLockOwners < (*locallock).maxLockOwners);
    /* Count the total */
    (*locallock).nLocks += 1;
    /* Count the per-owner lock */
    i = 0;
    while i < (*locallock).numLockOwners {
        if (*lockOwners.add(i as usize)).owner == owner {
            (*lockOwners.add(i as usize)).nLocks += 1;
            return;
        }
        i += 1;
    }
    (*lockOwners.add(i as usize)).owner = owner;
    (*lockOwners.add(i as usize)).nLocks = 1;
    (*locallock).numLockOwners += 1;
    if !owner.is_null() {
        ResourceOwnerRememberLock(owner, locallock);
    }

    /* Indicate that the lock is acquired for certain types of locks. */
    CheckAndSetLockHeld(locallock, true);
}

/*
 * BeginStrongLockAcquire - inhibit use of fastpath for a given LOCALLOCK,
 * and arrange for error cleanup if it fails
 */
unsafe fn BeginStrongLockAcquire(locallock: *mut LOCALLOCK, fasthashcode: uint32) {
    Assert!(StrongLockInProgress.is_null());
    Assert!((*locallock).holdsStrongLockCount == false);

    SpinLockAcquire(&raw mut (*FastPathStrongRelationLocks).mutex);
    (*FastPathStrongRelationLocks).count[fasthashcode as usize] += 1;
    (*locallock).holdsStrongLockCount = true;
    StrongLockInProgress = locallock;
    SpinLockRelease(&raw mut (*FastPathStrongRelationLocks).mutex);
}

/*
 * FinishStrongLockAcquire - cancel pending cleanup for a strong lock
 * acquisition once it's no longer needed
 */
unsafe fn FinishStrongLockAcquire() {
    StrongLockInProgress = ptr::null_mut();
}

/*
 * AbortStrongLockAcquire - undo strong lock state changes performed by
 * BeginStrongLockAcquire.
 */
pub unsafe fn AbortStrongLockAcquire() {
    let fasthashcode: usize;
    let locallock: *mut LOCALLOCK = StrongLockInProgress;

    if locallock.is_null() {
        return;
    }

    fasthashcode = FastPathStrongLockHashPartition((*locallock).hashcode);
    Assert!((*locallock).holdsStrongLockCount == true);
    SpinLockAcquire(&raw mut (*FastPathStrongRelationLocks).mutex);
    Assert!((*FastPathStrongRelationLocks).count[fasthashcode] > 0);
    (*FastPathStrongRelationLocks).count[fasthashcode] -= 1;
    (*locallock).holdsStrongLockCount = false;
    StrongLockInProgress = ptr::null_mut();
    SpinLockRelease(&raw mut (*FastPathStrongRelationLocks).mutex);
}

/*
 * GrantAwaitedLock -- call GrantLockLocal for the lock we are doing
 *   WaitOnLock on.
 */
pub unsafe fn GrantAwaitedLock() {
    GrantLockLocal(awaitedLock, awaitedOwner);
}

/*
 * GetAwaitedLock -- Return the lock we're currently doing WaitOnLock on.
 */
pub unsafe fn GetAwaitedLock() -> *mut LOCALLOCK {
    awaitedLock
}

/*
 * ResetAwaitedLock -- Forget that we are waiting on a lock.
 */
pub unsafe fn ResetAwaitedLock() {
    awaitedLock = ptr::null_mut();
}

/*
 * MarkLockClear -- mark an acquired lock as "clear"
 */
pub unsafe fn MarkLockClear(locallock: *mut LOCALLOCK) {
    Assert!((*locallock).nLocks > 0);
    (*locallock).lockCleared = true;
}

/*
 * WaitOnLock -- wait to acquire a lock
 *
 * This is a wrapper around ProcSleep, with extra tracing and bookkeeping.
 */
unsafe fn WaitOnLock(locallock: *mut LOCALLOCK, owner: ResourceOwner) -> ProcWaitStatus {
    let result: ProcWaitStatus;

    /* adjust the process title to indicate that it's waiting */
    set_ps_display_suffix(c"waiting".as_ptr());

    /*
     * Record the fact that we are waiting for a lock, so that
     * LockErrorCleanup will clean up if cancel/die happens.
     */
    awaitedLock = locallock;
    awaitedOwner = owner;

    /*
     * NOTE: Think not to put any shared-state cleanup after the call to
     * ProcSleep, in either the normal or failure path.  The lock state must
     * be fully set by the lock grantor, or by CheckDeadLock if we give up
     * waiting for the lock.
     */
    // PG_TRY / PG_CATCH translated as a simple call with no unwind here.
    // TODO(pg-port): add proper error propagation when ereport/PG_TRY land.
    result = ProcSleep(locallock);

    /*
     * We no longer want LockErrorCleanup to do anything.
     */
    awaitedLock = ptr::null_mut();

    /* reset ps display to remove the suffix */
    set_ps_display_remove_suffix();

    result
}

// ============================================================
//  RemoveFromWaitQueue
// ============================================================

/*
 * Remove a proc from the wait-queue it is on (caller must know it is on one).
 * This is only used when the proc has failed to get the lock, so we set its
 * waitStatus to PROC_WAIT_STATUS_ERROR.
 */
pub unsafe fn RemoveFromWaitQueue(proc_: *mut PGPROC, hashcode: uint32) {
    let waitLock: *mut LOCK = (*proc_).waitLock;
    let proclock: *mut PROCLOCK = (*proc_).waitProcLock;
    let lockmode: LOCKMODE = (*proc_).waitLockMode;
    let lockmethodid = LOCK_LOCKMETHOD(&*waitLock);

    /* Make sure proc is waiting */
    Assert!((*proc_).waitStatus == PROC_WAIT_STATUS_WAITING);
    Assert!((*proc_).links.next != ptr::null_mut());
    Assert!(!waitLock.is_null());
    Assert!(0 < lockmethodid && (lockmethodid as usize) < LockMethods.len());

    /* Remove proc from lock's wait queue */
    // dclist_delete_from_thoroughly(&mut (*waitLock).waitProcs, &mut (*proc_).links);
    // TODO(pg-port): dclist_delete_from_thoroughly -- storage/lmgr/proc.c helper

    /* Undo increments of request counts by waiting process */
    Assert!((*waitLock).nRequested > 0);
    Assert!((*waitLock).nRequested > (*(*proc_).waitLock).nGranted);
    (*waitLock).nRequested -= 1;
    Assert!((*waitLock).requested[lockmode as usize] > 0);
    (*waitLock).requested[lockmode as usize] -= 1;
    /* don't forget to clear waitMask bit if appropriate */
    if (*waitLock).granted[lockmode as usize] == (*waitLock).requested[lockmode as usize] {
        (*waitLock).waitMask &= LOCKBIT_OFF(lockmode);
    }

    /* Clean up the proc's own state, and pass it the ok/fail signal */
    (*proc_).waitLock = ptr::null_mut();
    (*proc_).waitProcLock = ptr::null_mut();
    (*proc_).waitStatus = PROC_WAIT_STATUS_ERROR;

    /*
     * Delete the proclock immediately if it represents no already-held locks.
     */
    CleanUpLock(
        waitLock,
        proclock,
        LockMethods[lockmethodid as usize],
        hashcode,
        true,
    );
}

// ============================================================
//  LockRelease / LockReleaseAll / LockReleaseSession
//  LockReleaseCurrentOwner / ReleaseLockIfHeld
//  LockReassignCurrentOwner
// ============================================================

/*
 * LockRelease -- look up 'locktag' and release one 'lockmode' lock on it.
 *   Release a session lock if 'sessionLock' is true.
 */
pub unsafe fn LockRelease(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    sessionLock: bool,
) -> bool {
    let lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;
    let lockMethodTable: LockMethod;
    let mut localtag: LOCALLOCKTAG = core::mem::zeroed();
    let locallock: *mut LOCALLOCK;
    let lock: *mut LOCK;
    let proclock: *mut PROCLOCK;
    let partitionLock: *mut LWLock;
    let wakeupNeeded: bool;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }
    lockMethodTable = LockMethods[lockmethodid as usize];
    if lockmode <= 0 || lockmode > (*lockMethodTable).numLockModes {
        elog!(ERROR, "unrecognized lock mode: {}", lockmode);
    }

    /*
     * Find the LOCALLOCK entry for this lock and lockmode
     */
    /* must clear padding */
    localtag.lock = *locktag;
    localtag.mode = lockmode;

    let locallock = hash_search(
        LockMethodLocalHash,
        &raw const localtag as *const c_void,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut LOCALLOCK;

    /*
     * let the caller print its own error message, too.
     */
    if locallock.is_null() || (*locallock).nLocks <= 0 {
        elog!(
            WARNING,
            "you don't own a lock of type {}",
            std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy()
        );
        return false;
    }

    /*
     * Decrease the count for the resource owner.
     */
    {
        let lockOwners: *mut LOCALLOCKOWNER = (*locallock).lockOwners;
        let owner: ResourceOwner;
        let mut i: c_int;

        /* Identify owner for lock */
        if sessionLock {
            owner = ptr::null_mut();
        } else {
            owner = CurrentResourceOwner;
        }

        i = (*locallock).numLockOwners - 1;
        loop {
            if i < 0 {
                break;
            }
            if (*lockOwners.add(i as usize)).owner == owner {
                Assert!((*lockOwners.add(i as usize)).nLocks > 0);
                (*lockOwners.add(i as usize)).nLocks -= 1;
                if (*lockOwners.add(i as usize)).nLocks == 0 {
                    if !owner.is_null() {
                        ResourceOwnerForgetLock(owner, locallock);
                    }
                    /* compact out unused slot */
                    (*locallock).numLockOwners -= 1;
                    if i < (*locallock).numLockOwners {
                        core::ptr::copy(lockOwners.add((*locallock).numLockOwners as usize), lockOwners.add(i as usize), 1);
                    }
                }
                break;
            }
            i -= 1;
        }
        if i < 0 {
            /* don't release a lock belonging to another owner */
            elog!(
                WARNING,
                "you don't own a lock of type {}",
                std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy()
            );
            return false;
        }
    }

    /*
     * Decrease the total local count.  If we're still holding the lock,
     * we're done.
     */
    (*locallock).nLocks -= 1;

    if (*locallock).nLocks > 0 {
        return true;
    }

    /*
     * At this point we can no longer suppose we are clear of invalidation
     * messages related to this lock.
     */
    (*locallock).lockCleared = false;

    /* Attempt fast release of any lock eligible for the fast path. */
    if EligibleForRelationFastPath(locktag, lockmode)
        && FastPathLocalUseCounts
            [FAST_PATH_REL_GROUP((*locktag).locktag_field2) as usize]
            > 0
    {
        let released: bool;

        /*
         * We might not find the lock here, even if we originally entered
         * it here.  Another backend may have moved it to the main table.
         */
        LWLockAcquire(&raw mut (*MyProc).fpInfoLock as *mut LWLock, LW_EXCLUSIVE);
        released =
            FastPathUnGrantRelationLock((*locktag).locktag_field2, lockmode);
        LWLockRelease(&raw mut (*MyProc).fpInfoLock as *mut LWLock);
        if released {
            RemoveLocalLock(locallock);
            return true;
        }
    }

    /*
     * Otherwise we've got to mess with the shared lock table.
     */
    let partitionLock = LockHashPartitionLock((*locallock).hashcode);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * Normally, we don't need to re-find the lock or proclock.  But it's
     * possible that the lock was taken fast-path and has since been moved
     * to the main hash table by another backend, in which case we will
     * need to look up the objects here.
     */
    let mut lock = (*locallock).lock;
    let proclock: *mut PROCLOCK;
    if lock.is_null() {
        let mut proclocktag: PROCLOCKTAG = core::mem::zeroed();

        Assert!(EligibleForRelationFastPath(locktag, lockmode));
        let found_lock = hash_search_with_hash_value(
            LockMethodLockHash,
            locktag as *const c_void,
            (*locallock).hashcode,
            HASH_FIND,
            ptr::null_mut(),
        ) as *mut LOCK;
        if found_lock.is_null() {
            elog!(ERROR, "failed to re-find shared lock object");
        }
        (*locallock).lock = found_lock;

        proclocktag.myLock = found_lock;
        proclocktag.myProc = MyProc;
        let found_proclock = hash_search(
            LockMethodProcLockHash,
            &raw const proclocktag as *const c_void,
            HASH_FIND,
            ptr::null_mut(),
        ) as *mut PROCLOCK;
        if found_proclock.is_null() {
            elog!(ERROR, "failed to re-find shared proclock object");
        }
        (*locallock).proclock = found_proclock;
        lock = found_lock;
        proclock = found_proclock;
    } else {
        proclock = (*locallock).proclock;
    }

    /*
     * Double-check that we are actually holding a lock of the type we want
     * to release.
     */
    if ((*proclock).holdMask & LOCKBIT_ON(lockmode)) == 0 {
        LWLockRelease(partitionLock);
        elog!(
            WARNING,
            "you don't own a lock of type {}",
            std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy()
        );
        RemoveLocalLock(locallock);
        return false;
    }

    /*
     * Do the releasing.  CleanUpLock will waken any now-wakable waiters.
     */
    let wakeupNeeded = UnGrantLock(lock, lockmode, proclock, lockMethodTable);

    CleanUpLock(lock, proclock, lockMethodTable, (*locallock).hashcode, wakeupNeeded);

    LWLockRelease(partitionLock);

    RemoveLocalLock(locallock);
    true
}

/*
 * LockReleaseAll -- Release all locks of the specified lock method that
 *   are held by the current process.
 */
pub unsafe fn LockReleaseAll(lockmethodid: LOCKMETHODID, allLocks: bool) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let lockMethodTable: LockMethod;
    let numLockModes: c_int;
    let mut locallock: *mut LOCALLOCK;
    let mut lock: *mut LOCK;
    let mut partition: usize;
    let mut have_fast_path_lwlock: bool = false;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }
    lockMethodTable = LockMethods[lockmethodid as usize];

    /*
     * Get rid of our fast-path VXID lock, if appropriate.
     */
    if lockmethodid == DEFAULT_LOCKMETHOD as LOCKMETHODID {
        VirtualXactLockTableCleanup();
    }

    numLockModes = (*lockMethodTable).numLockModes;

    /*
     * First we run through the locallock table and get rid of unwanted
     * entries, then we scan the process's proclocks and get rid of those.
     */
    hash_seq_init(&raw mut status, LockMethodLocalHash);

    loop {
        locallock = hash_seq_search(&raw mut status) as *mut LOCALLOCK;
        if locallock.is_null() {
            break;
        }

        /*
         * If the LOCALLOCK entry is unused, something must've gone wrong.
         */
        if (*locallock).nLocks == 0 {
            RemoveLocalLock(locallock);
            continue;
        }

        /* Ignore items that are not of the lockmethod to be removed */
        if LOCALLOCK_LOCKMETHOD(&*locallock) != lockmethodid {
            continue;
        }

        /*
         * If we are asked to release all locks, we can just zap the entry.
         * Otherwise, must scan to see if there are session locks.
         */
        if !allLocks {
            let lockOwners: *mut LOCALLOCKOWNER = (*locallock).lockOwners;
            let mut i: c_int;

            /* If session lock is above array position 0, move it down to 0 */
            i = 0;
            while i < (*locallock).numLockOwners {
                if (*lockOwners.add(i as usize)).owner.is_null() {
                    core::ptr::copy(lockOwners.add(i as usize), lockOwners.add(0), 1);
                } else {
                    ResourceOwnerForgetLock((*lockOwners.add(i as usize)).owner, locallock);
                }
                i += 1;
            }

            if (*locallock).numLockOwners > 0
                && (*lockOwners.add(0)).owner.is_null()
                && (*lockOwners.add(0)).nLocks > 0
            {
                /* Fix the locallock to show just the session locks */
                (*locallock).nLocks = (*lockOwners.add(0)).nLocks;
                (*locallock).numLockOwners = 1;
                /* We aren't deleting this locallock, so done */
                continue;
            } else {
                (*locallock).numLockOwners = 0;
            }
        }

        #[cfg(any())] /* USE_ASSERT_CHECKING build only */
        {
            if LOCALLOCK_LOCKTAG(&*locallock) == LOCKTAG_TUPLE && !allLocks {
                elog!(WARNING, "tuple lock held at commit");
            }
        }

        /*
         * If the lock or proclock pointers are NULL, this lock was taken via
         * the relation fast-path.
         */
        if (*locallock).proclock.is_null() || (*locallock).lock.is_null() {
            let lockmode = (*locallock).tag.mode;
            let relid: Oid;

            /* Verify that a fast-path lock is what we've got. */
            if !EligibleForRelationFastPath(&raw const (*locallock).tag.lock, lockmode) {
                elog!(PANIC, "locallock table corrupted");
            }

            /*
             * If we don't currently hold the LWLock that protects our
             * fast-path data structures, we must acquire it.
             */
            if !have_fast_path_lwlock {
                LWLockAcquire(&raw mut (*MyProc).fpInfoLock as *mut LWLock, LW_EXCLUSIVE);
                have_fast_path_lwlock = true;
            }

            /* Attempt fast-path release. */
            relid = (*locallock).tag.lock.locktag_field2;
            if FastPathUnGrantRelationLock(relid, lockmode) {
                RemoveLocalLock(locallock);
                continue;
            }

            /*
             * Our lock has been transferred to the main lock table.
             */
            LWLockRelease(&raw mut (*MyProc).fpInfoLock as *mut LWLock);
            have_fast_path_lwlock = false;

            LockRefindAndRelease(
                lockMethodTable,
                MyProc,
                &raw mut (*locallock).tag.lock,
                lockmode,
                false,
            );
            RemoveLocalLock(locallock);
            continue;
        }

        /* Mark the proclock to show we need to release this lockmode */
        if (*locallock).nLocks > 0 {
            (*(*locallock).proclock).releaseMask |= LOCKBIT_ON((*locallock).tag.mode);
        }

        /* And remove the locallock hashtable entry */
        RemoveLocalLock(locallock);
    }

    /* Done with the fast-path data structures */
    if have_fast_path_lwlock {
        LWLockRelease(&raw mut (*MyProc).fpInfoLock as *mut LWLock);
    }

    /*
     * Now, scan each lock partition separately.
     */
    partition = 0;
    while partition < NUM_LOCK_PARTITIONS {
        let partitionLock: *mut LWLock;
        let procLocks: *mut dlist_head = &raw mut (*MyProc).myProcLocks[partition];
        let mut proclock_iter: dlist_mutable_iter = core::mem::zeroed();

        partitionLock = LockHashPartitionLockByIndex(partition);

        /*
         * If the proclock list for this partition is empty, skip.
         */
        if dlist_is_empty(procLocks) {
            partition += 1;
            continue;
        }

        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        dlist_foreach_modify!(proclock_iter, procLocks, {
            let proclock =
                dlist_container!(PROCLOCK, procLink, proclock_iter.cur) as *mut PROCLOCK;
            let mut wakeupNeeded: bool = false;
            let mut i: c_int;

            Assert!((*proclock).tag.myProc == MyProc);

            lock = (*proclock).tag.myLock;

            /* Ignore items that are not of the lockmethod to be removed */
            if LOCK_LOCKMETHOD(&*lock) != lockmethodid {
                continue;
            }

            /*
             * In allLocks mode, force release of all locks.
             */
            if allLocks {
                (*proclock).releaseMask = (*proclock).holdMask;
            } else {
                Assert!(((*proclock).releaseMask & !(*proclock).holdMask) == 0);
            }

            /*
             * Ignore items that have nothing to be released.
             */
            if (*proclock).releaseMask == 0 && (*proclock).holdMask != 0 {
                continue;
            }

            Assert!((*lock).nRequested >= 0);
            Assert!((*lock).nGranted >= 0);
            Assert!((*lock).nGranted <= (*lock).nRequested);
            Assert!(((*proclock).holdMask & !(*lock).grantMask) == 0);

            /*
             * Release the previously-marked lock modes
             */
            i = 1;
            while i <= numLockModes {
                if ((*proclock).releaseMask & LOCKBIT_ON(i)) != 0 {
                    wakeupNeeded |= UnGrantLock(lock, i, proclock, lockMethodTable);
                }
                i += 1;
            }
            Assert!(((*lock).nRequested >= 0) && ((*lock).nGranted >= 0));
            Assert!((*lock).nGranted <= (*lock).nRequested);

            (*proclock).releaseMask = 0;

            /* CleanUpLock will wake up waiters if needed. */
            CleanUpLock(
                lock,
                proclock,
                lockMethodTable,
                LockTagHashCode(&raw const (*lock).tag),
                wakeupNeeded,
            );
        }); /* loop over PROCLOCKs within this partition */

        LWLockRelease(partitionLock);
        partition += 1;
    } /* loop over partitions */
}

/*
 * LockReleaseSession -- Release all session locks of the specified lock method
 *   that are held by the current process.
 */
pub unsafe fn LockReleaseSession(lockmethodid: LOCKMETHODID) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut locallock: *mut LOCALLOCK;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }

    hash_seq_init(&raw mut status, LockMethodLocalHash);

    loop {
        locallock = hash_seq_search(&raw mut status) as *mut LOCALLOCK;
        if locallock.is_null() {
            break;
        }
        /* Ignore items that are not of the specified lock method */
        if LOCALLOCK_LOCKMETHOD(&*locallock) != lockmethodid {
            continue;
        }
        ReleaseLockIfHeld(locallock, true);
    }
}

/*
 * LockReleaseCurrentOwner -- Release all locks belonging to CurrentResourceOwner.
 */
pub unsafe fn LockReleaseCurrentOwner(locallocks: *mut *mut LOCALLOCK, nlocks: c_int) {
    if locallocks.is_null() {
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
        let mut locallock: *mut LOCALLOCK;

        hash_seq_init(&raw mut status, LockMethodLocalHash);

        loop {
            locallock = hash_seq_search(&raw mut status) as *mut LOCALLOCK;
            if locallock.is_null() {
                break;
            }
            ReleaseLockIfHeld(locallock, false);
        }
    } else {
        let mut i: c_int = nlocks - 1;
        while i >= 0 {
            ReleaseLockIfHeld(*locallocks.add(i as usize), false);
            i -= 1;
        }
    }
}

/*
 * ReleaseLockIfHeld -- release any session-level or current-owner locks
 *   on this lockable object.
 */
unsafe fn ReleaseLockIfHeld(locallock: *mut LOCALLOCK, sessionLock: bool) {
    let owner: ResourceOwner;
    let lockOwners: *mut LOCALLOCKOWNER;
    let mut i: c_int;

    /* Identify owner for lock (must match LockRelease!) */
    if sessionLock {
        owner = ptr::null_mut();
    } else {
        owner = CurrentResourceOwner;
    }

    /* Scan to see if there are any locks belonging to the target owner */
    lockOwners = (*locallock).lockOwners;
    i = (*locallock).numLockOwners - 1;
    loop {
        if i < 0 {
            break;
        }
        if (*lockOwners.add(i as usize)).owner == owner {
            Assert!((*lockOwners.add(i as usize)).nLocks > 0);
            if (*lockOwners.add(i as usize)).nLocks < (*locallock).nLocks {
                /*
                 * We will still hold this lock after forgetting this
                 * ResourceOwner.
                 */
                (*locallock).nLocks -= (*lockOwners.add(i as usize)).nLocks;
                /* compact out unused slot */
                (*locallock).numLockOwners -= 1;
                if !owner.is_null() {
                    ResourceOwnerForgetLock(owner, locallock);
                }
                if i < (*locallock).numLockOwners {
                    core::ptr::copy(lockOwners.add((*locallock).numLockOwners as usize), lockOwners.add(i as usize), 1);
                }
            } else {
                Assert!((*lockOwners.add(i as usize)).nLocks == (*locallock).nLocks);
                /* We want to call LockRelease just once */
                (*lockOwners.add(i as usize)).nLocks = 1;
                (*locallock).nLocks = 1;
                if !LockRelease(&raw const (*locallock).tag.lock, (*locallock).tag.mode, sessionLock) {
                    elog!(WARNING, "ReleaseLockIfHeld: failed??");
                }
            }
            break;
        }
        i -= 1;
    }
}

/*
 * LockReassignCurrentOwner -- Reassign all locks belonging to
 *   CurrentResourceOwner to belong to its parent resource owner.
 */
pub unsafe fn LockReassignCurrentOwner(locallocks: *mut *mut LOCALLOCK, nlocks: c_int) {
    let parent = ResourceOwnerGetParent(CurrentResourceOwner);

    Assert!(!parent.is_null());

    if locallocks.is_null() {
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
        let mut locallock: *mut LOCALLOCK;

        hash_seq_init(&raw mut status, LockMethodLocalHash);

        loop {
            locallock = hash_seq_search(&raw mut status) as *mut LOCALLOCK;
            if locallock.is_null() {
                break;
            }
            LockReassignOwner(locallock, parent);
        }
    } else {
        let mut i: c_int = nlocks - 1;
        while i >= 0 {
            LockReassignOwner(*locallocks.add(i as usize), parent);
            i -= 1;
        }
    }
}

/*
 * Subroutine of LockReassignCurrentOwner. Reassigns a given lock belonging
 * to CurrentResourceOwner to its parent.
 */
unsafe fn LockReassignOwner(locallock: *mut LOCALLOCK, parent: ResourceOwner) {
    let lockOwners: *mut LOCALLOCKOWNER;
    let mut i: c_int;
    let mut ic: c_int = -1;
    let mut ip: c_int = -1;

    /*
     * Scan to see if there are any locks belonging to current owner or
     * its parent
     */
    lockOwners = (*locallock).lockOwners;
    i = (*locallock).numLockOwners - 1;
    while i >= 0 {
        if (*lockOwners.add(i as usize)).owner == CurrentResourceOwner {
            ic = i;
        } else if (*lockOwners.add(i as usize)).owner == parent {
            ip = i;
        }
        i -= 1;
    }

    if ic < 0 {
        return; /* no current locks */
    }

    if ip < 0 {
        /* Parent has no slot, so just give it the child's slot */
        (*lockOwners.add(ic as usize)).owner = parent;
        ResourceOwnerRememberLock(parent, locallock);
    } else {
        /* Merge child's count with parent's */
        (*lockOwners.add(ip as usize)).nLocks += (*lockOwners.add(ic as usize)).nLocks;
        /* compact out unused slot */
        (*locallock).numLockOwners -= 1;
        if ic < (*locallock).numLockOwners {
            core::ptr::copy(lockOwners.add((*locallock).numLockOwners as usize), lockOwners.add(ic as usize), 1);
        }
    }
    ResourceOwnerForgetLock(CurrentResourceOwner, locallock);
}

// ============================================================
//  Fast-path helpers
// ============================================================

/*
 * FastPathGrantRelationLock
 *   Grant lock using per-backend fast-path array, if there is space.
 */
unsafe fn FastPathGrantRelationLock(relid: Oid, lockmode: LOCKMODE) -> bool {
    let mut i: u32;
    let mut unused_slot: u32 = FastPathLockSlotsPerBackend();

    /* fast-path group the lock belongs to */
    let group: u32 = FAST_PATH_REL_GROUP(relid);

    /* Scan for existing entry for this relid, remembering empty slot. */
    i = 0;
    while i < FP_LOCK_SLOTS_PER_GROUP {
        /* index into the whole per-backend array */
        let f = FAST_PATH_SLOT(group, i);

        if FAST_PATH_GET_BITS(MyProc, f) == 0 {
            unused_slot = f;
        } else if (*MyProc).fpRelId.add(f as usize).read() == relid {
            Assert!(!FAST_PATH_CHECK_LOCKMODE(MyProc, f, lockmode as u32));
            FAST_PATH_SET_LOCKMODE(MyProc, f, lockmode as u32);
            return true;
        }
        i += 1;
    }

    /* If no existing entry, use any empty slot. */
    if unused_slot < FastPathLockSlotsPerBackend() {
        (*MyProc).fpRelId.add(unused_slot as usize).write(relid);
        FAST_PATH_SET_LOCKMODE(MyProc, unused_slot, lockmode as u32);
        FastPathLocalUseCounts[group as usize] += 1;
        return true;
    }

    /* No existing entry, and no empty slot. */
    false
}

/*
 * FastPathUnGrantRelationLock
 *   Release fast-path lock, if present.
 */
unsafe fn FastPathUnGrantRelationLock(relid: Oid, lockmode: LOCKMODE) -> bool {
    let mut i: u32;
    let mut result: bool = false;

    /* fast-path group the lock belongs to */
    let group: u32 = FAST_PATH_REL_GROUP(relid);

    FastPathLocalUseCounts[group as usize] = 0;
    i = 0;
    while i < FP_LOCK_SLOTS_PER_GROUP {
        /* index into the whole per-backend array */
        let f = FAST_PATH_SLOT(group, i);

        if (*MyProc).fpRelId.add(f as usize).read() == relid
            && FAST_PATH_CHECK_LOCKMODE(MyProc, f, lockmode as u32)
        {
            Assert!(!result);
            FAST_PATH_CLEAR_LOCKMODE(MyProc, f, lockmode as u32);
            result = true;
            /* we continue iterating so as to update FastPathLocalUseCount */
        }
        if FAST_PATH_GET_BITS(MyProc, f) != 0 {
            FastPathLocalUseCounts[group as usize] += 1;
        }
        i += 1;
    }
    result
}

/*
 * FastPathTransferRelationLocks
 *   Transfer locks matching the given lock tag from per-backend fast-path
 *   arrays to the shared hash table.
 *
 * Returns true if successful, false if ran out of shared memory.
 */
unsafe fn FastPathTransferRelationLocks(
    lockMethodTable: LockMethod,
    locktag: *const LOCKTAG,
    hashcode: uint32,
) -> bool {
    let partitionLock: *mut LWLock = LockHashPartitionLock(hashcode);
    let relid: Oid = (*locktag).locktag_field2;
    let mut i: uint32;

    /* fast-path group the lock belongs to */
    let group: u32 = FAST_PATH_REL_GROUP(relid);

    /*
     * Every PGPROC that can potentially hold a fast-path lock is present in
     * ProcGlobal->allProcs.
     */
    i = 0;
    while i < (*ProcGlobal).allProcCount {
        let proc_: *mut PGPROC = (*ProcGlobal).allProcs.add(i as usize);
        let mut j: uint32;

        LWLockAcquire(&raw mut (*proc_).fpInfoLock as *mut LWLock, LW_EXCLUSIVE);

        /*
         * If the target backend isn't referencing the same database as the
         * lock, then we needn't examine the individual relation IDs at all;
         * none of them can be relevant.
         *
         * Also skip groups without any registered fast-path locks.
         */
        if (*proc_).databaseId != (*locktag).locktag_field1 || (*(*proc_).fpLockBits.add((group as usize) as usize)) == 0 {
            LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
            i += 1;
            continue;
        }

        j = 0;
        while j < FP_LOCK_SLOTS_PER_GROUP {
            let mut lockmode: u32;

            /* index into the whole per-backend array */
            let f = FAST_PATH_SLOT(group, j);

            /* Look for an allocated slot matching the given relid. */
            if relid != (*proc_).fpRelId.add(f as usize).read()
                || FAST_PATH_GET_BITS(proc_, f) == 0
            {
                j += 1;
                continue;
            }

            /* Find or create lock object. */
            LWLockAcquire(partitionLock, LW_EXCLUSIVE);
            lockmode = FAST_PATH_LOCKNUMBER_OFFSET;
            while lockmode < FAST_PATH_LOCKNUMBER_OFFSET + FAST_PATH_BITS_PER_SLOT {
                let proclock: *mut PROCLOCK;

                if !FAST_PATH_CHECK_LOCKMODE(proc_, f, lockmode) {
                    lockmode += 1;
                    continue;
                }
                proclock = SetupLockInTable(
                    lockMethodTable,
                    proc_,
                    locktag,
                    hashcode,
                    lockmode as LOCKMODE,
                );
                if proclock.is_null() {
                    LWLockRelease(partitionLock);
                    LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
                    return false;
                }
                GrantLock((*proclock).tag.myLock, proclock, lockmode as LOCKMODE);
                FAST_PATH_CLEAR_LOCKMODE(proc_, f, lockmode);
                lockmode += 1;
            }
            LWLockRelease(partitionLock);

            /* No need to examine remaining slots. */
            break;
        } /* loop over slots */
        LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
        i += 1;
    }
    true
}

/*
 * FastPathGetRelationLockEntry
 *   Return the PROCLOCK for a lock originally taken via the fast-path,
 *   transferring it to the primary lock table if necessary.
 */
unsafe fn FastPathGetRelationLockEntry(locallock: *mut LOCALLOCK) -> *mut PROCLOCK {
    let lockMethodTable: LockMethod = LockMethods[DEFAULT_LOCKMETHOD as usize];
    let locktag: *mut LOCKTAG = &raw mut (*locallock).tag.lock;
    let mut proclock: *mut PROCLOCK = ptr::null_mut();
    let partitionLock: *mut LWLock = LockHashPartitionLock((*locallock).hashcode);
    let relid: Oid = (*locktag).locktag_field2;
    let mut i: u32;

    /* fast-path group the lock belongs to */
    let group: u32 = FAST_PATH_REL_GROUP(relid);

    LWLockAcquire(&raw mut (*MyProc).fpInfoLock as *mut LWLock, LW_EXCLUSIVE);

    i = 0;
    while i < FP_LOCK_SLOTS_PER_GROUP {
        let lockmode: LOCKMODE;

        /* index into the whole per-backend array */
        let f = FAST_PATH_SLOT(group, i);

        /* Look for an allocated slot matching the given relid. */
        if relid != (*MyProc).fpRelId.add(f as usize).read()
            || FAST_PATH_GET_BITS(MyProc, f) == 0
        {
            i += 1;
            continue;
        }

        /* If we don't have a lock of the given mode, forget it! */
        lockmode = (*locallock).tag.mode;
        if !FAST_PATH_CHECK_LOCKMODE(MyProc, f, lockmode as u32) {
            break;
        }

        /* Find or create lock object. */
        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        proclock = SetupLockInTable(
            lockMethodTable,
            MyProc,
            locktag,
            (*locallock).hashcode,
            lockmode,
        );
        if proclock.is_null() {
            LWLockRelease(partitionLock);
            LWLockRelease(&raw mut (*MyProc).fpInfoLock as *mut LWLock);
            ereport!(ERROR, errmsg!("out of shared memory")) /* C also: errhint */;
        }
        GrantLock((*proclock).tag.myLock, proclock, lockmode);
        FAST_PATH_CLEAR_LOCKMODE(MyProc, f, lockmode as u32);

        LWLockRelease(partitionLock);

        /* No need to examine remaining slots. */
        break;
    }

    LWLockRelease(&raw mut (*MyProc).fpInfoLock as *mut LWLock);

    /* Lock may have already been transferred by some other backend. */
    if proclock.is_null() {
        let lock: *mut LOCK;
        let mut proclocktag: PROCLOCKTAG = core::mem::zeroed();
        let proclock_hashcode: uint32;

        LWLockAcquire(partitionLock, LW_SHARED);

        lock = hash_search_with_hash_value(
            LockMethodLockHash,
            locktag as *const c_void,
            (*locallock).hashcode,
            HASH_FIND,
            ptr::null_mut(),
        ) as *mut LOCK;
        if lock.is_null() {
            elog!(ERROR, "failed to re-find shared lock object");
        }

        proclocktag.myLock = lock;
        proclocktag.myProc = MyProc;

        proclock_hashcode = ProcLockHashCode(&raw const proclocktag, (*locallock).hashcode);
        proclock = hash_search_with_hash_value(
            LockMethodProcLockHash,
            &raw const proclocktag as *const c_void,
            proclock_hashcode,
            HASH_FIND,
            ptr::null_mut(),
        ) as *mut PROCLOCK;
        if proclock.is_null() {
            elog!(ERROR, "failed to re-find shared proclock object");
        }
        LWLockRelease(partitionLock);
    }

    proclock
}

// ============================================================
//  GetLockConflicts / LockRefindAndRelease
// ============================================================

/*
 * GetLockConflicts
 *   Get an array of VirtualTransactionIds of xacts currently holding locks
 *   that would conflict with the specified lock/lockmode.
 */
pub unsafe fn GetLockConflicts(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    countp: *mut c_int,
) -> *mut VirtualTransactionId {
    static mut vxids: *mut VirtualTransactionId = ptr::null_mut();
    let lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;
    let lockMethodTable: LockMethod;
    let lock: *mut LOCK;
    let conflictMask: LOCKMASK;
    let mut proclock_iter: dlist_iter = core::mem::zeroed();
    let proclock: *mut PROCLOCK;
    let hashcode: uint32;
    let partitionLock: *mut LWLock;
    let mut count: c_int = 0;
    let mut fast_count: c_int = 0;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }
    lockMethodTable = LockMethods[lockmethodid as usize];
    if lockmode <= 0 || lockmode > (*lockMethodTable).numLockModes {
        elog!(ERROR, "unrecognized lock mode: {}", lockmode);
    }

    /*
     * Allocate memory to store results, and fill with InvalidVXID.
     * In HotStandby, allocate once in TopMemoryContext.
     */
    if InHotStandby {
        if vxids.is_null() {
            vxids = MemoryContextAlloc(
                TopMemoryContext,
                size_of::<VirtualTransactionId>()
                    * (MaxBackends as usize + max_prepared_xacts as usize + 1),
            ) as *mut VirtualTransactionId;
        }
    } else {
        vxids = palloc0(
            size_of::<VirtualTransactionId>()
                * (MaxBackends as usize + max_prepared_xacts as usize + 1),
        ) as *mut VirtualTransactionId;
    }

    /* Compute hash code and partition lock, and look up conflicting modes. */
    hashcode = LockTagHashCode(locktag);
    partitionLock = LockHashPartitionLock(hashcode);
    conflictMask = (*lockMethodTable).conflictTab.add(lockmode as usize).read();

    /*
     * Fast path locks might not have been entered in the primary lock table.
     */
    if ConflictsWithRelationFastPath(locktag, lockmode) {
        let mut i: uint32;
        let relid: Oid = (*locktag).locktag_field2;
        let mut vxid: VirtualTransactionId = core::mem::zeroed();

        /* fast-path group the lock belongs to */
        let group: u32 = FAST_PATH_REL_GROUP(relid);

        /*
         * Iterate over relevant PGPROCs.
         */
        i = 0;
        while i < (*ProcGlobal).allProcCount {
            let proc_: *mut PGPROC = (*ProcGlobal).allProcs.add(i as usize);
            let mut j: u32;

            /* A backend never blocks itself */
            if proc_ == MyProc {
                i += 1;
                continue;
            }

            LWLockAcquire(&raw mut (*proc_).fpInfoLock as *mut LWLock, LW_SHARED);

            /*
             * If the target backend isn't referencing the same database or has
             * no fast-path locks in this group, skip it.
             */
            if (*proc_).databaseId != (*locktag).locktag_field1
                || (*(*proc_).fpLockBits.add((group as usize) as usize)) == 0
            {
                LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
                i += 1;
                continue;
            }

            j = 0;
            while j < FP_LOCK_SLOTS_PER_GROUP {
                let lockmask: u64;

                /* index into the whole per-backend array */
                let f = FAST_PATH_SLOT(group, j);

                /* Look for an allocated slot matching the given relid. */
                if relid != (*proc_).fpRelId.add(f as usize).read() {
                    j += 1;
                    continue;
                }
                lockmask = FAST_PATH_GET_BITS(proc_, f);
                if lockmask == 0 {
                    j += 1;
                    continue;
                }
                let lockmask = lockmask << FAST_PATH_LOCKNUMBER_OFFSET;

                /*
                 * There can only be one entry per relation, so if we found it
                 * and it doesn't conflict, we can skip the rest of the slots.
                 */
                if (lockmask as LOCKMASK & conflictMask) == 0 {
                    break;
                }

                /* Conflict! */
                GET_VXID_FROM_PGPROC(&raw mut vxid, &*proc_);

                if VirtualTransactionIdIsValid(vxid) {
                    vxids.add(count as usize).write(vxid);
                    count += 1;
                }
                /* else, xact already committed or aborted */

                /* No need to examine remaining slots. */
                break;
            }

            LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
            i += 1;
        }
    }

    /* Remember how many fast-path conflicts we found. */
    fast_count = count;

    /*
     * Look up the lock object matching the tag.
     */
    LWLockAcquire(partitionLock, LW_SHARED);

    let lock = hash_search_with_hash_value(
        LockMethodLockHash,
        locktag as *const c_void,
        hashcode,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut LOCK;
    if lock.is_null() {
        /*
         * If the lock object doesn't exist, there is nothing holding a lock
         * on this lockable object.
         */
        LWLockRelease(partitionLock);
        (*vxids.add(count as usize)).procNumber = INVALID_PROC_NUMBER;
        (*vxids.add(count as usize)).localTransactionId = InvalidLocalTransactionId;
        if !countp.is_null() {
            *countp = count;
        }
        return vxids;
    }

    /*
     * Examine each existing holder (or awaiter) of the lock.
     */
    dlist_foreach!(proclock_iter, &mut (*lock).procLocks, {
        let proclock = dlist_container!(PROCLOCK, lockLink, proclock_iter.cur) as *mut PROCLOCK;

        if (conflictMask & (*proclock).holdMask) != 0 {
            let proc_: *mut PGPROC = (*proclock).tag.myProc;

            /* A backend never blocks itself */
            if proc_ != MyProc {
                let mut vxid: VirtualTransactionId = core::mem::zeroed();

                GET_VXID_FROM_PGPROC(&raw mut vxid, &*proc_);

                if VirtualTransactionIdIsValid(vxid) {
                    let mut dup = false;
                    let mut fi: c_int = 0;
                    /* Avoid duplicate entries. */
                    while fi < fast_count {
                        if VirtualTransactionIdEquals(*vxids.add(fi as usize), vxid) {
                            dup = true;
                            break;
                        }
                        fi += 1;
                    }
                    if !dup {
                        vxids.add(count as usize).write(vxid);
                        count += 1;
                    }
                }
                /* else, xact already committed or aborted */
            }
        }
    });

    LWLockRelease(partitionLock);

    if count > MaxBackends + max_prepared_xacts {
        /* should never happen */
        elog!(PANIC, "too many conflicting locks found");
    }

    (*vxids.add(count as usize)).procNumber = INVALID_PROC_NUMBER;
    (*vxids.add(count as usize)).localTransactionId = InvalidLocalTransactionId;
    if !countp.is_null() {
        *countp = count;
    }
    vxids
}

/*
 * LockRefindAndRelease -- Find and release the lock indicated.
 */
unsafe fn LockRefindAndRelease(
    lockMethodTable: LockMethod,
    proc_: *mut PGPROC,
    locktag: *mut LOCKTAG,
    lockmode: LOCKMODE,
    decrement_strong_lock_count: bool,
) {
    let lock: *mut LOCK;
    let proclock: *mut PROCLOCK;
    let mut proclocktag: PROCLOCKTAG = core::mem::zeroed();
    let hashcode: uint32;
    let proclock_hashcode: uint32;
    let partitionLock: *mut LWLock;
    let wakeupNeeded: bool;

    hashcode = LockTagHashCode(locktag);
    partitionLock = LockHashPartitionLock(hashcode);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * Re-find the lock object (it had better be there).
     */
    let lock = hash_search_with_hash_value(
        LockMethodLockHash,
        locktag as *const c_void,
        hashcode,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut LOCK;
    if lock.is_null() {
        elog!(PANIC, "failed to re-find shared lock object");
    }

    /*
     * Re-find the proclock object (ditto).
     */
    proclocktag.myLock = lock;
    proclocktag.myProc = proc_;

    proclock_hashcode = ProcLockHashCode(&raw const proclocktag, hashcode);

    let proclock = hash_search_with_hash_value(
        LockMethodProcLockHash,
        &raw const proclocktag as *const c_void,
        proclock_hashcode,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut PROCLOCK;
    if proclock.is_null() {
        elog!(PANIC, "failed to re-find shared proclock object");
    }

    /*
     * Double-check that we are actually holding a lock of the type we want
     * to release.
     */
    if ((*proclock).holdMask & LOCKBIT_ON(lockmode)) == 0 {
        LWLockRelease(partitionLock);
        elog!(
            WARNING,
            "you don't own a lock of type {}",
            std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy()
        );
        return;
    }

    /*
     * Do the releasing.  CleanUpLock will waken any now-wakable waiters.
     */
    let wakeupNeeded = UnGrantLock(lock, lockmode, proclock, lockMethodTable);

    CleanUpLock(lock, proclock, lockMethodTable, hashcode, wakeupNeeded);

    LWLockRelease(partitionLock);

    /*
     * Decrement strong lock count.  This logic is needed only for 2PC.
     */
    if decrement_strong_lock_count && ConflictsWithRelationFastPath(locktag, lockmode) {
        let fasthashcode = FastPathStrongLockHashPartition(hashcode);

        SpinLockAcquire(&raw mut (*FastPathStrongRelationLocks).mutex);
        Assert!((*FastPathStrongRelationLocks).count[fasthashcode] > 0);
        (*FastPathStrongRelationLocks).count[fasthashcode] -= 1;
        SpinLockRelease(&raw mut (*FastPathStrongRelationLocks).mutex);
    }
}

// ============================================================
//  CheckForSessionAndXactLocks / AtPrepare_Locks / PostPrepare_Locks
//  LockManagerShmemSize / GetLockStatusData / GetBlockerStatusData
//  GetRunningTransactionLocks / GetLockmodeName / DumpLocks
//  2PC routines / VirtualXactLock
// ============================================================

/*
 * CheckForSessionAndXactLocks
 *   Check to see if transaction holds both session-level and xact-level
 *   locks on the same object; if so, throw an error.
 */
unsafe fn CheckForSessionAndXactLocks() {
    #[repr(C)]
    struct PerLockTagEntry {
        lock: LOCKTAG,
        sessLock: bool,
        xactLock: bool,
    }

    let mut hash_ctl: HASHCTL = core::mem::zeroed();
    let lockhtab: *mut HTAB;
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut locallock: *mut LOCALLOCK;

    /* Create a local hash table keyed by LOCKTAG only */
    hash_ctl.keysize = size_of::<LOCKTAG>();
    hash_ctl.entrysize = size_of::<PerLockTagEntry>();
    hash_ctl.hcxt = CurrentMemoryContext as _;

    let lockhtab = hash_create(
        c"CheckForSessionAndXactLocks table".as_ptr(),
        256,
        &raw const hash_ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    /* Scan local lock table to find entries for each LOCKTAG */
    hash_seq_init(&raw mut status, LockMethodLocalHash);

    loop {
        locallock = hash_seq_search(&raw mut status) as *mut LOCALLOCK;
        if locallock.is_null() {
            break;
        }
        let lockOwners: *mut LOCALLOCKOWNER = (*locallock).lockOwners;
        let hentry: *mut PerLockTagEntry;
        let mut found: bool = false;
        let mut i: c_int;

        /*
         * Ignore VXID locks.
         */
        if (*locallock).tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION as uint8 {
            continue;
        }

        /* Ignore it if we don't actually hold the lock */
        if (*locallock).nLocks <= 0 {
            continue;
        }

        /* Otherwise, find or make an entry in lockhtab */
        hentry = hash_search(
            lockhtab,
            &raw const (*locallock).tag.lock as *const c_void,
            HASH_ENTER,
            &raw mut found,
        ) as *mut PerLockTagEntry;
        if !found {
            /* initialize, if newly created */
            (*hentry).sessLock = false;
            (*hentry).xactLock = false;
        }

        /* Scan to see if we hold lock at session or xact level or both */
        i = (*locallock).numLockOwners - 1;
        while i >= 0 {
            if (*lockOwners.add(i as usize)).owner.is_null() {
                (*hentry).sessLock = true;
            } else {
                (*hentry).xactLock = true;
            }
            i -= 1;
        }

        /*
         * We can throw error immediately when we see both types of locks.
         */
        if (*hentry).sessLock && (*hentry).xactLock {
            ereport!(ERROR, errmsg!(
                    "cannot PREPARE while holding both session-level and \
                     transaction-level locks on the same object"
                ));
        }
    }

    /* Success, so clean up */
    hash_destroy(lockhtab);
}

/*
 * AtPrepare_Locks
 *   Do the preparatory work for a PREPARE: make 2PC state file records
 *   for all locks currently held.
 */
pub unsafe fn AtPrepare_Locks() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut locallock: *mut LOCALLOCK;

    /* First, verify there aren't locks of both xact and session level */
    CheckForSessionAndXactLocks();

    /* Now do the per-locallock cleanup work */
    hash_seq_init(&raw mut status, LockMethodLocalHash);

    loop {
        locallock = hash_seq_search(&raw mut status) as *mut LOCALLOCK;
        if locallock.is_null() {
            break;
        }
        let mut record: TwoPhaseLockRecord = core::mem::zeroed();
        let lockOwners: *mut LOCALLOCKOWNER = (*locallock).lockOwners;
        let mut haveSessionLock: bool;
        let mut haveXactLock: bool;
        let mut i: c_int;

        /*
         * Ignore VXID locks.
         */
        if (*locallock).tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION as uint8 {
            continue;
        }

        /* Ignore it if we don't actually hold the lock */
        if (*locallock).nLocks <= 0 {
            continue;
        }

        /* Scan to see whether we hold it at session or transaction level */
        haveSessionLock = false;
        haveXactLock = false;
        i = (*locallock).numLockOwners - 1;
        while i >= 0 {
            if (*lockOwners.add(i as usize)).owner.is_null() {
                haveSessionLock = true;
            } else {
                haveXactLock = true;
            }
            i -= 1;
        }

        /* Ignore it if we have only session lock */
        if !haveXactLock {
            continue;
        }

        /* This can't happen, because we already checked it */
        if haveSessionLock {
            ereport!(ERROR, errmsg!(
                    "cannot PREPARE while holding both session-level and \
                     transaction-level locks on the same object"
                ));
        }

        /*
         * If the local lock was taken via the fast-path, we need to move it
         * to the primary lock table.
         */
        if (*locallock).proclock.is_null() {
            (*locallock).proclock = FastPathGetRelationLockEntry(locallock);
            (*locallock).lock = (*(*locallock).proclock).tag.myLock;
        }

        /*
         * Arrange to not release any strong lock count held by this lock
         * entry.
         */
        (*locallock).holdsStrongLockCount = false;

        /*
         * Create a 2PC record.
         */
        ptr::copy_nonoverlapping(
            &raw const (*locallock).tag.lock,
            &raw mut record.locktag,
            1,
        );
        record.lockmode = (*locallock).tag.mode;

        RegisterTwoPhaseRecord(
            TWOPHASE_RM_LOCK_ID,
            0,
            &raw const record as *const c_void,
            size_of::<TwoPhaseLockRecord>(),
        );
    }
}

/*
 * PostPrepare_Locks
 *   Clean up after successful PREPARE.
 */
pub unsafe fn PostPrepare_Locks(xid: TransactionId) {
    let newproc: *mut PGPROC = TwoPhaseGetDummyProc(xid, false);
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut locallock: *mut LOCALLOCK;
    let lock: *mut LOCK;
    let proclock: *mut PROCLOCK;
    let mut proclocktag: PROCLOCKTAG = core::mem::zeroed();
    let mut partition: usize;

    /* Can't prepare a lock group follower. */
    Assert!(
        (*MyProc).lockGroupLeader.is_null() || (*MyProc).lockGroupLeader == MyProc
    );

    /* This is a critical section: any error means big trouble */
    START_CRIT_SECTION();

    hash_seq_init(&raw mut status, LockMethodLocalHash);

    loop {
        locallock = hash_seq_search(&raw mut status) as *mut LOCALLOCK;
        if locallock.is_null() {
            break;
        }
        let lockOwners: *mut LOCALLOCKOWNER = (*locallock).lockOwners;
        let mut haveSessionLock: bool;
        let mut haveXactLock: bool;
        let mut i: c_int;

        if (*locallock).proclock.is_null() || (*locallock).lock.is_null() {
            /*
             * We must've run out of shared memory while trying to set up
             * this lock.  Just forget the local entry.
             */
            Assert!((*locallock).nLocks == 0);
            RemoveLocalLock(locallock);
            continue;
        }

        /* Ignore VXID locks */
        if (*locallock).tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION as uint8 {
            continue;
        }

        /* Scan to see whether we hold it at session or transaction level */
        haveSessionLock = false;
        haveXactLock = false;
        i = (*locallock).numLockOwners - 1;
        while i >= 0 {
            if (*lockOwners.add(i as usize)).owner.is_null() {
                haveSessionLock = true;
            } else {
                haveXactLock = true;
            }
            i -= 1;
        }

        /* Ignore it if we have only session lock */
        if !haveXactLock {
            continue;
        }

        /* This can't happen, because we already checked it */
        if haveSessionLock {
            ereport!(PANIC, errmsg!(
                    "cannot PREPARE while holding both session-level and \
                     transaction-level locks on the same object"
                ));
        }

        /* Mark the proclock to show we need to release this lockmode */
        if (*locallock).nLocks > 0 {
            (*(*locallock).proclock).releaseMask |= LOCKBIT_ON((*locallock).tag.mode);
        }

        /* And remove the locallock hashtable entry */
        RemoveLocalLock(locallock);
    }

    /*
     * Now, scan each lock partition separately.
     */
    partition = 0;
    while partition < NUM_LOCK_PARTITIONS {
        let partitionLock: *mut LWLock;
        let procLocks: *mut dlist_head = &raw mut (*MyProc).myProcLocks[partition];
        let mut proclock_iter: dlist_mutable_iter = core::mem::zeroed();

        partitionLock = LockHashPartitionLockByIndex(partition);

        if dlist_is_empty(procLocks) {
            partition += 1;
            continue;
        }

        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        dlist_foreach_modify!(proclock_iter, procLocks, {
            let proclock =
                dlist_container!(PROCLOCK, procLink, proclock_iter.cur) as *mut PROCLOCK;

            Assert!((*proclock).tag.myProc == MyProc);

            let lock = (*proclock).tag.myLock;

            /* Ignore VXID locks */
            if (*lock).tag.locktag_type == LOCKTAG_VIRTUALTRANSACTION as uint8 {
                continue;
            }

            Assert!((*lock).nRequested >= 0);
            Assert!((*lock).nGranted >= 0);
            Assert!((*lock).nGranted <= (*lock).nRequested);
            Assert!(((*proclock).holdMask & !(*lock).grantMask) == 0);

            /* Ignore it if nothing to release (must be a session lock) */
            if (*proclock).releaseMask == 0 {
                continue;
            }

            /* Else we should be releasing all locks */
            if (*proclock).releaseMask != (*proclock).holdMask {
                elog!(PANIC, "we seem to have dropped a bit somewhere");
            }

            /*
             * We cannot simply modify proclock->tag.myProc to reassign
             * ownership of the lock, because that's part of the hash key.
             * Instead use hash_update_hash_key.
             */
            dlist_delete(&raw mut (*proclock).procLink);

            /*
             * Create the new hash key for the proclock.
             */
            proclocktag.myLock = lock;
            proclocktag.myProc = newproc;

            /*
             * Update groupLeader pointer to point to the new proc.
             */
            Assert!((*proclock).groupLeader == (*proclock).tag.myProc);
            (*proclock).groupLeader = newproc;

            /*
             * Update the proclock.
             */
            if !hash_update_hash_key(
                LockMethodProcLockHash,
                proclock as *mut c_void,
                &raw const proclocktag as *const c_void,
            ) {
                elog!(
                    PANIC,
                    "duplicate entry found while reassigning a prepared transaction's locks"
                );
            }

            /* Re-link into the new proc's proclock list */
            dlist_push_tail(
                &raw mut (*newproc).myProcLocks[partition],
                &raw mut (*proclock).procLink
            );
        }); /* loop over PROCLOCKs within this partition */

        LWLockRelease(partitionLock);
        partition += 1;
    } /* loop over partitions */

    END_CRIT_SECTION();
}

/*
 * Estimate shared-memory space used for lock tables
 */
pub unsafe fn LockManagerShmemSize() -> Size {
    let mut size: Size = 0;
    let mut max_table_size: c_long;

    /* lock hash table */
    max_table_size = NLOCKENTS();
    size = add_size(size, hash_estimate_size(max_table_size, size_of::<LOCK>()));

    /* proclock hash table */
    max_table_size *= 2;
    size = add_size(size, hash_estimate_size(max_table_size, size_of::<PROCLOCK>()));

    /*
     * Since NLOCKENTS is only an estimate, add 10% safety margin.
     */
    size = add_size(size, size / 10);

    size
}

/*
 * GetLockStatusData - Return a summary of the lock manager's internal status.
 */
pub unsafe fn GetLockStatusData() -> *mut LockData {
    let data: *mut LockData;
    let mut proclock: *mut PROCLOCK;
    let mut seqstat: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut els: c_int;
    let mut el: c_int;
    let mut i: c_int;

    data = palloc(size_of::<LockData>()) as *mut LockData;

    /* Guess how much space we'll need. */
    els = MaxBackends;
    el = 0;
    (*data).locks = palloc(size_of::<LockInstanceData>() * els as usize) as *mut LockInstanceData;

    /*
     * First, we iterate through the per-backend fast-path arrays.
     */
    i = 0;
    while i < (*ProcGlobal).allProcCount as c_int {
        let proc_: *mut PGPROC = (*ProcGlobal).allProcs.add(i as usize);

        /* Skip backends with pid=0 */
        if (*proc_).pid == 0 {
            i += 1;
            continue;
        }

        LWLockAcquire(&raw mut (*proc_).fpInfoLock as *mut LWLock, LW_SHARED);

        let mut g: u32 = 0;
        while g < FastPathLockGroupsPerBackend as u32 {
            /* Skip groups without registered fast-path locks */
            if (*(*proc_).fpLockBits.add((g as usize) as usize)) == 0 {
                g += 1;
                continue;
            }

            let mut j: c_int = 0;
            while j < FP_LOCK_SLOTS_PER_GROUP as c_int {
                let instance: *mut LockInstanceData;
                let f = FAST_PATH_SLOT(g, j as u32);
                let lockbits = FAST_PATH_GET_BITS(proc_, f);

                /* Skip unallocated slots */
                if lockbits == 0 {
                    j += 1;
                    continue;
                }

                if el >= els {
                    els += MaxBackends;
                    (*data).locks = repalloc(
                        (*data).locks as *mut c_void,
                        size_of::<LockInstanceData>() * els as usize,
                    ) as *mut LockInstanceData;
                }

                instance = (*data).locks.add(el as usize);
                SET_LOCKTAG_RELATION(&raw mut (*instance).locktag, (*proc_).databaseId, (*proc_).fpRelId.add(f as usize).read());
                (*instance).holdMask = (lockbits << FAST_PATH_LOCKNUMBER_OFFSET) as LOCKMASK;
                (*instance).waitLockMode = NoLock;
                (*instance).vxid.procNumber = (*proc_).vxid.procNumber;
                (*instance).vxid.localTransactionId = (*proc_).vxid.lxid;
                (*instance).pid = (*proc_).pid;
                (*instance).leaderPid = (*proc_).pid;
                (*instance).fastpath = true;
                (*instance).waitStart = 0;

                el += 1;
                j += 1;
            }
            g += 1;
        }

        if (*proc_).fpVXIDLock {
            let mut vxid: VirtualTransactionId = core::mem::zeroed();
            let instance: *mut LockInstanceData;

            if el >= els {
                els += MaxBackends;
                (*data).locks = repalloc(
                    (*data).locks as *mut c_void,
                    size_of::<LockInstanceData>() * els as usize,
                ) as *mut LockInstanceData;
            }

            vxid.procNumber = (*proc_).vxid.procNumber;
            vxid.localTransactionId = (*proc_).fpLocalTransactionId;

            instance = (*data).locks.add(el as usize);
            SET_LOCKTAG_VIRTUALTRANSACTION(&raw mut (*instance).locktag, vxid);
            (*instance).holdMask = LOCKBIT_ON(ExclusiveLock);
            (*instance).waitLockMode = NoLock;
            (*instance).vxid.procNumber = (*proc_).vxid.procNumber;
            (*instance).vxid.localTransactionId = (*proc_).vxid.lxid;
            (*instance).pid = (*proc_).pid;
            (*instance).leaderPid = (*proc_).pid;
            (*instance).fastpath = true;
            (*instance).waitStart = 0;

            el += 1;
        }

        LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
        i += 1;
    }

    /*
     * Next, acquire lock on the entire shared lock data structure.
     */
    i = 0;
    while i < NUM_LOCK_PARTITIONS as c_int {
        LWLockAcquire(LockHashPartitionLockByIndex(i as usize), LW_SHARED);
        i += 1;
    }

    /* Now we can safely count the number of proclocks */
    (*data).nelements = el + hash_get_num_entries(LockMethodProcLockHash) as c_int;
    if (*data).nelements > els {
        els = (*data).nelements;
        (*data).locks = repalloc(
            (*data).locks as *mut c_void,
            size_of::<LockInstanceData>() * els as usize,
        ) as *mut LockInstanceData;
    }

    /* Now scan the tables to copy the data */
    hash_seq_init(&raw mut seqstat, LockMethodProcLockHash);

    loop {
        let proclock = hash_seq_search(&raw mut seqstat) as *mut PROCLOCK;
        if proclock.is_null() {
            break;
        }
        let proc_: *mut PGPROC = (*proclock).tag.myProc;
        let lock: *mut LOCK = (*proclock).tag.myLock;
        let instance: *mut LockInstanceData = (*data).locks.add(el as usize);

        ptr::copy_nonoverlapping(&raw const (*lock).tag, &raw mut (*instance).locktag, 1);
        (*instance).holdMask = (*proclock).holdMask;
        if (*proc_).waitLock == (*proclock).tag.myLock {
            (*instance).waitLockMode = (*proc_).waitLockMode;
        } else {
            (*instance).waitLockMode = NoLock;
        }
        (*instance).vxid.procNumber = (*proc_).vxid.procNumber;
        (*instance).vxid.localTransactionId = (*proc_).vxid.lxid;
        (*instance).pid = (*proc_).pid;
        (*instance).leaderPid = (*(*proclock).groupLeader).pid;
        (*instance).fastpath = false;
        (*instance).waitStart = pg_atomic_read_u64(&raw mut (*proc_).waitStart as *mut u64) as TimestampTz;

        el += 1;
    }

    /*
     * And release locks, in reverse order.
     */
    i = NUM_LOCK_PARTITIONS as c_int;
    i -= 1;
    while i >= 0 {
        LWLockRelease(LockHashPartitionLockByIndex(i as usize));
        i -= 1;
    }

    Assert!(el == (*data).nelements);

    data
}

/*
 * GetBlockerStatusData - Return a summary of lock manager state concerning
 * locks blocking the specified PID.
 */
pub unsafe fn GetBlockerStatusData(blocked_pid: c_int) -> *mut BlockedProcsData {
    let data: *mut BlockedProcsData;
    let proc_: *mut PGPROC;
    let mut i: c_int;

    data = palloc(size_of::<BlockedProcsData>()) as *mut BlockedProcsData;

    (*data).nprocs = 0;
    (*data).nlocks = 0;
    (*data).npids = 0;
    (*data).maxprocs = MaxBackends;
    (*data).maxlocks = MaxBackends;
    (*data).maxpids = MaxBackends;
    (*data).procs = palloc(size_of::<BlockedProcData>() * (*data).maxprocs as usize)
        as *mut BlockedProcData;
    (*data).locks = palloc(size_of::<LockInstanceData>() * (*data).maxlocks as usize)
        as *mut LockInstanceData;
    (*data).waiter_pids =
        palloc(size_of::<c_int>() * (*data).maxpids as usize) as *mut c_int;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    proc_ = BackendPidGetProcWithLock(blocked_pid);

    /* Nothing to do if it's gone */
    if !proc_.is_null() {
        /*
         * Acquire lock on the entire shared lock data structure.
         */
        i = 0;
        while i < NUM_LOCK_PARTITIONS as c_int {
            LWLockAcquire(LockHashPartitionLockByIndex(i as usize), LW_SHARED);
            i += 1;
        }

        if (*proc_).lockGroupLeader.is_null() {
            /* Easy case, proc is not a lock group member */
            GetSingleProcBlockerStatusData(proc_, data);
        } else {
            /* Examine all procs in proc's lock group */
            let mut iter: dlist_iter = core::mem::zeroed();

            dlist_foreach!(iter, &mut (*(*proc_).lockGroupLeader).lockGroupMembers, {
                let memberProc = dlist_container!(PGPROC, lockGroupLink, iter.cur) as *mut PGPROC;
                GetSingleProcBlockerStatusData(memberProc, data);
            });
        }

        /*
         * And release locks, in reverse order.
         */
        i = NUM_LOCK_PARTITIONS as c_int - 1;
        while i >= 0 {
            LWLockRelease(LockHashPartitionLockByIndex(i as usize));
            i -= 1;
        }

        Assert!((*data).nprocs <= (*data).maxprocs);
    }

    LWLockRelease(ProcArrayLock());

    data
}

/* Accumulate data about one possibly-blocked proc for GetBlockerStatusData */
unsafe fn GetSingleProcBlockerStatusData(
    blocked_proc: *mut PGPROC,
    data: *mut BlockedProcsData,
) {
    let theLock: *mut LOCK = (*blocked_proc).waitLock;
    let bproc: *mut BlockedProcData;
    let mut proclock_iter: dlist_iter = core::mem::zeroed();
    let mut proc_iter: dlist_iter = core::mem::zeroed();
    let waitQueue: *const dclist_head;
    let queue_size: c_int;

    /* Nothing to do if this proc is not blocked */
    if theLock.is_null() {
        return;
    }

    /* Set up a procs[] element */
    bproc = (*data).procs.add((*data).nprocs as usize);
    (*data).nprocs += 1;
    (*bproc).pid = (*blocked_proc).pid;
    (*bproc).first_lock = (*data).nlocks;
    (*bproc).first_waiter = (*data).npids;

    /* Collect all PROCLOCKs associated with theLock */
    dlist_foreach!(proclock_iter, &mut (*theLock).procLocks, {
        let proclock =
            dlist_container!(PROCLOCK, lockLink, proclock_iter.cur) as *mut PROCLOCK;
        let proc_: *mut PGPROC = (*proclock).tag.myProc;
        let lock: *mut LOCK = (*proclock).tag.myLock;
        let instance: *mut LockInstanceData;

        if (*data).nlocks >= (*data).maxlocks {
            (*data).maxlocks += MaxBackends;
            (*data).locks = repalloc(
                (*data).locks as *mut c_void,
                size_of::<LockInstanceData>() * (*data).maxlocks as usize,
            ) as *mut LockInstanceData;
        }

        instance = (*data).locks.add((*data).nlocks as usize);
        ptr::copy_nonoverlapping(&raw const (*lock).tag, &raw mut (*instance).locktag, 1);
        (*instance).holdMask = (*proclock).holdMask;
        if (*proc_).waitLock == lock {
            (*instance).waitLockMode = (*proc_).waitLockMode;
        } else {
            (*instance).waitLockMode = NoLock;
        }
        (*instance).vxid.procNumber = (*proc_).vxid.procNumber;
        (*instance).vxid.localTransactionId = (*proc_).vxid.lxid;
        (*instance).pid = (*proc_).pid;
        (*instance).leaderPid = (*(*proclock).groupLeader).pid;
        (*instance).fastpath = false;
        (*data).nlocks += 1;
    });

    /* Enlarge waiter_pids[] if it's too small */
    waitQueue = &raw const (*theLock).waitProcs;
    queue_size = dclist_count(waitQueue);

    if queue_size > (*data).maxpids - (*data).npids {
        (*data).maxpids = Max((*data).maxpids + MaxBackends, (*data).npids + queue_size);
        (*data).waiter_pids = repalloc(
            (*data).waiter_pids as *mut c_void,
            size_of::<c_int>() * (*data).maxpids as usize,
        ) as *mut c_int;
    }

    /* Collect PIDs from the lock's wait queue, stopping at blocked_proc */
    // dclist_foreach over waitQueue
    // TODO(pg-port): dclist_foreach macro needed; using dlist_foreach on
    // the embedded dlist_head for now (same memory layout for head).
    dlist_foreach!(proc_iter, &mut (*theLock).waitProcs.dlist, {
        let queued_proc = dlist_container!(PGPROC, links, proc_iter.cur) as *mut PGPROC;

        if queued_proc == blocked_proc {
            break;
        }
        (*data).waiter_pids.add((*data).npids as usize).write((*queued_proc).pid);
        (*data).npids += 1;
    });

    (*bproc).num_locks = (*data).nlocks - (*bproc).first_lock;
    (*bproc).num_waiters = (*data).npids - (*bproc).first_waiter;
}

/*
 * GetRunningTransactionLocks
 *   Returns a list of currently held AccessExclusiveLocks.
 */
pub unsafe fn GetRunningTransactionLocks(nlocks: *mut c_int) -> *mut xl_standby_lock {
    let accessExclusiveLocks: *mut xl_standby_lock;
    let mut proclock: *mut PROCLOCK;
    let mut seqstat: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut i: c_int;
    let mut index: c_int;
    let els: c_int;

    /*
     * Acquire lock on the entire shared lock data structure.
     */
    i = 0;
    while i < NUM_LOCK_PARTITIONS as c_int {
        LWLockAcquire(LockHashPartitionLockByIndex(i as usize), LW_SHARED);
        i += 1;
    }

    /* Now we can safely count the number of proclocks */
    els = hash_get_num_entries(LockMethodProcLockHash) as c_int;

    accessExclusiveLocks =
        palloc(els as usize * size_of::<xl_standby_lock>()) as *mut xl_standby_lock;

    /* Now scan the tables to copy the data */
    hash_seq_init(&raw mut seqstat, LockMethodProcLockHash);

    index = 0;
    loop {
        let proclock = hash_seq_search(&raw mut seqstat) as *mut PROCLOCK;
        if proclock.is_null() {
            break;
        }
        /* make sure this definition matches the one used in LockAcquire */
        if ((*proclock).holdMask & LOCKBIT_ON(AccessExclusiveLock)) != 0
            && (*(*proclock).tag.myLock).tag.locktag_type == LOCKTAG_RELATION as uint8
        {
            let proc_: *mut PGPROC = (*proclock).tag.myProc;
            let lock: *mut LOCK = (*proclock).tag.myLock;
            let xid: TransactionId = (*proc_).xid;

            /*
             * Don't record locks for transactions that have already issued
             * their WAL record for commit.
             */
            if !TransactionIdIsValid(xid) {
                continue;
            }

            (*accessExclusiveLocks.add(index as usize)).xid = xid;
            (*accessExclusiveLocks.add(index as usize)).dbOid = (*lock).tag.locktag_field1;
            (*accessExclusiveLocks.add(index as usize)).relOid = (*lock).tag.locktag_field2;

            index += 1;
        }
    }

    Assert!(index <= els);

    /*
     * And release locks, in reverse order.
     */
    i = NUM_LOCK_PARTITIONS as c_int - 1;
    while i >= 0 {
        LWLockRelease(LockHashPartitionLockByIndex(i as usize));
        i -= 1;
    }

    *nlocks = index;
    accessExclusiveLocks
}

/*
 * GetLockmodeName -- Provide the textual name of any lock mode.
 */
pub unsafe fn GetLockmodeName(lockmethodid: LOCKMETHODID, mode: LOCKMODE) -> *const c_char {
    Assert!(lockmethodid > 0 && (lockmethodid as usize) < LockMethods.len());
    Assert!(mode > 0 && mode <= (*LockMethods[lockmethodid as usize]).numLockModes);
    (*LockMethods[lockmethodid as usize])
        .lockModeNames
        .add(mode as usize)
        .read()
}

// ============================================================
//  2PC resource manager routines
// ============================================================

/*
 * lock_twophase_recover -- Re-acquire a lock belonging to a transaction
 *   that was prepared.
 */
pub unsafe fn lock_twophase_recover(
    xid: TransactionId,
    info: uint16,
    recdata: *mut c_void,
    len: uint32,
) {
    let rec: *mut TwoPhaseLockRecord = recdata as *mut TwoPhaseLockRecord;
    let proc_: *mut PGPROC = TwoPhaseGetDummyProc(xid, false);
    let locktag: *mut LOCKTAG;
    let lockmode: LOCKMODE;
    let lockmethodid: LOCKMETHODID;
    let lock: *mut LOCK;
    let proclock: *mut PROCLOCK;
    let mut proclocktag: PROCLOCKTAG = core::mem::zeroed();
    let mut found: bool = false;
    let hashcode: uint32;
    let proclock_hashcode: uint32;
    let partition: usize;
    let partitionLock: *mut LWLock;
    let lockMethodTable: LockMethod;

    Assert!(len as usize == size_of::<TwoPhaseLockRecord>());
    locktag = &raw mut (*rec).locktag;
    lockmode = (*rec).lockmode;
    lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }
    lockMethodTable = LockMethods[lockmethodid as usize];

    hashcode = LockTagHashCode(locktag);
    partition = LockHashPartition(hashcode);
    partitionLock = LockHashPartitionLock(hashcode);

    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    /*
     * Find or create a lock with this tag.
     */
    let lock = hash_search_with_hash_value(
        LockMethodLockHash,
        locktag as *const c_void,
        hashcode,
        HASH_ENTER_NULL,
        &raw mut found,
    ) as *mut LOCK;
    if lock.is_null() {
        LWLockRelease(partitionLock);
        ereport!(ERROR, errmsg!("out of shared memory")) /* C also: errhint */;
    }

    /*
     * if it's a new lock object, initialize it
     */
    if !found {
        (*lock).grantMask = 0;
        (*lock).waitMask = 0;
        dlist_init(&raw mut (*lock).procLocks);
        { dlist_init(&raw mut (*lock).waitProcs.dlist); (*lock).waitProcs.count = 0; }
        (*lock).nRequested = 0;
        (*lock).nGranted = 0;
        MemSet(
            (*lock).requested.as_mut_ptr() as *mut c_void,
            0,
            size_of::<c_int>() * MAX_LOCKMODES,
        );
        MemSet(
            (*lock).granted.as_mut_ptr() as *mut c_void,
            0,
            size_of::<c_int>() * MAX_LOCKMODES,
        );
    } else {
        Assert!(((*lock).nRequested >= 0) && ((*lock).requested[lockmode as usize] >= 0));
        Assert!(((*lock).nGranted >= 0) && ((*lock).granted[lockmode as usize] >= 0));
        Assert!((*lock).nGranted <= (*lock).nRequested);
    }

    /*
     * Create the hash key for the proclock table.
     */
    proclocktag.myLock = lock;
    proclocktag.myProc = proc_;

    proclock_hashcode = ProcLockHashCode(&raw const proclocktag, hashcode);

    /*
     * Find or create a proclock entry with this tag
     */
    let proclock = hash_search_with_hash_value(
        LockMethodProcLockHash,
        &raw const proclocktag as *const c_void,
        proclock_hashcode,
        HASH_ENTER_NULL,
        &raw mut found,
    ) as *mut PROCLOCK;
    if proclock.is_null() {
        /* Oops, not enough shmem for the proclock */
        if (*lock).nRequested == 0 {
            Assert!(dlist_is_empty(&raw const (*lock).procLocks));
            if hash_search_with_hash_value(
                LockMethodLockHash,
                &raw const (*lock).tag as *const c_void,
                hashcode,
                HASH_REMOVE,
                ptr::null_mut(),
            )
            .is_null()
            {
                elog!(PANIC, "lock table corrupted");
            }
        }
        LWLockRelease(partitionLock);
        ereport!(ERROR, errmsg!("out of shared memory")) /* C also: errhint */;
    }

    /*
     * If new, initialize the new entry
     */
    if !found {
        Assert!((*proc_).lockGroupLeader.is_null());
        (*proclock).groupLeader = proc_;
        (*proclock).holdMask = 0;
        (*proclock).releaseMask = 0;
        /* Add proclock to appropriate lists */
        dlist_push_tail(&raw mut (*lock).procLocks, &raw mut (*proclock).lockLink);
        dlist_push_tail(
            &raw mut (*proc_).myProcLocks[partition],
            &raw mut (*proclock).procLink
        );
    } else {
        Assert!(((*proclock).holdMask & !(*lock).grantMask) == 0);
    }

    /*
     * lock->nRequested and lock->requested[] count the total number of
     * requests, whether granted or waiting.
     */
    (*lock).nRequested += 1;
    (*lock).requested[lockmode as usize] += 1;
    Assert!(((*lock).nRequested > 0) && ((*lock).requested[lockmode as usize] > 0));

    /*
     * We shouldn't already hold the desired lock.
     */
    if ((*proclock).holdMask & LOCKBIT_ON(lockmode)) != 0 {
        elog!(
            ERROR,
            "lock {} on object {}/{}/{} is already held",
            std::ffi::CStr::from_ptr(*(*lockMethodTable).lockModeNames.add(lockmode as usize)).to_string_lossy(),
            (*lock).tag.locktag_field1,
            (*lock).tag.locktag_field2,
            (*lock).tag.locktag_field3
        );
    }

    /*
     * We ignore any possible conflicts and just grant ourselves the lock.
     */
    GrantLock(lock, proclock, lockmode);

    /*
     * Bump strong lock count.
     */
    if ConflictsWithRelationFastPath(&raw const (*lock).tag, lockmode) {
        let fasthashcode = FastPathStrongLockHashPartition(hashcode);

        SpinLockAcquire(&raw mut (*FastPathStrongRelationLocks).mutex);
        (*FastPathStrongRelationLocks).count[fasthashcode] += 1;
        SpinLockRelease(&raw mut (*FastPathStrongRelationLocks).mutex);
    }

    LWLockRelease(partitionLock);
}

/*
 * lock_twophase_standby_recover -- Re-acquire a lock when starting up
 *   into hot standby mode.
 */
pub unsafe fn lock_twophase_standby_recover(
    xid: TransactionId,
    info: uint16,
    recdata: *mut c_void,
    len: uint32,
) {
    let rec: *mut TwoPhaseLockRecord = recdata as *mut TwoPhaseLockRecord;
    let locktag: *mut LOCKTAG;
    let lockmode: LOCKMODE;
    let lockmethodid: LOCKMETHODID;

    Assert!(len as usize == size_of::<TwoPhaseLockRecord>());
    locktag = &raw mut (*rec).locktag;
    lockmode = (*rec).lockmode;
    lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }

    if lockmode == AccessExclusiveLock
        && (*locktag).locktag_type == LOCKTAG_RELATION as uint8
    {
        StandbyAcquireAccessExclusiveLock(
            xid,
            (*locktag).locktag_field1, /* dboid */
            (*locktag).locktag_field2, /* reloid */
        );
    }
}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Find and release the lock indicated by the 2PC record.
 */
pub unsafe fn lock_twophase_postcommit(
    xid: TransactionId,
    info: uint16,
    recdata: *mut c_void,
    len: uint32,
) {
    let rec: *mut TwoPhaseLockRecord = recdata as *mut TwoPhaseLockRecord;
    let proc_: *mut PGPROC = TwoPhaseGetDummyProc(xid, true);
    let locktag: *mut LOCKTAG;
    let lockmethodid: LOCKMETHODID;
    let lockMethodTable: LockMethod;

    Assert!(len as usize == size_of::<TwoPhaseLockRecord>());
    locktag = &raw mut (*rec).locktag;
    lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }
    lockMethodTable = LockMethods[lockmethodid as usize];

    LockRefindAndRelease(lockMethodTable, proc_, locktag, (*rec).lockmode, true);
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * This is actually just the same as the COMMIT case.
 */
pub unsafe fn lock_twophase_postabort(
    xid: TransactionId,
    info: uint16,
    recdata: *mut c_void,
    len: uint32,
) {
    lock_twophase_postcommit(xid, info, recdata, len);
}

// ============================================================
//  VirtualXact lock functions
// ============================================================

/*
 * VirtualXactLockTableInsert
 *
 * Take vxid lock via the fast-path.
 */
pub unsafe fn VirtualXactLockTableInsert(vxid: VirtualTransactionId) {
    Assert!(VirtualTransactionIdIsValid(vxid));

    LWLockAcquire(&raw mut (*MyProc).fpInfoLock as *mut LWLock, LW_EXCLUSIVE);

    Assert!((*MyProc).vxid.procNumber == vxid.procNumber);
    Assert!((*MyProc).fpLocalTransactionId == InvalidLocalTransactionId);
    Assert!((*MyProc).fpVXIDLock == false);

    (*MyProc).fpVXIDLock = true;
    (*MyProc).fpLocalTransactionId = vxid.localTransactionId;

    LWLockRelease(&raw mut (*MyProc).fpInfoLock as *mut LWLock);
}

/*
 * VirtualXactLockTableCleanup
 *
 * Check whether a VXID lock has been materialized; if so, release it,
 * unblocking waiters.
 */
pub unsafe fn VirtualXactLockTableCleanup() {
    let fastpath: bool;
    let lxid: LocalTransactionId;

    Assert!((*MyProc).vxid.procNumber != INVALID_PROC_NUMBER);

    /*
     * Clean up shared memory state.
     */
    LWLockAcquire(&raw mut (*MyProc).fpInfoLock as *mut LWLock, LW_EXCLUSIVE);

    fastpath = (*MyProc).fpVXIDLock;
    lxid = (*MyProc).fpLocalTransactionId;
    (*MyProc).fpVXIDLock = false;
    (*MyProc).fpLocalTransactionId = InvalidLocalTransactionId;

    LWLockRelease(&raw mut (*MyProc).fpInfoLock as *mut LWLock);

    /*
     * If fpVXIDLock has been cleared without touching fpLocalTransactionId,
     * that means someone transferred the lock to the main lock table.
     */
    if !fastpath && lxid != InvalidLocalTransactionId {
        let mut vxid: VirtualTransactionId = core::mem::zeroed();
        let mut locktag: LOCKTAG = core::mem::zeroed();

        vxid.procNumber = MyProcNumber;
        vxid.localTransactionId = lxid;
        SET_LOCKTAG_VIRTUALTRANSACTION(&raw mut locktag, vxid);

        LockRefindAndRelease(
            LockMethods[DEFAULT_LOCKMETHOD as usize],
            MyProc,
            &raw mut locktag,
            ExclusiveLock,
            false,
        );
    }
}

/*
 * XactLockForVirtualXact
 *
 * If TransactionIdIsValid(xid), this is essentially XactLockTableWait(xid).
 */
unsafe fn XactLockForVirtualXact(
    vxid: VirtualTransactionId,
    mut xid: TransactionId,
    wait: bool,
) -> bool {
    let mut more: bool = false;

    /* There is no point to wait for 2PCs if you have no 2PCs. */
    if max_prepared_xacts == 0 {
        return true;
    }

    loop {
        let lar: LockAcquireResult;
        let mut tag: LOCKTAG = core::mem::zeroed();

        /* Clear state from previous iterations. */
        if more {
            xid = 0; // InvalidTransactionId
            more = false;
        }

        /* If we have no xid, try to find one. */
        if !TransactionIdIsValid(xid) {
            xid = TwoPhaseGetXidByVirtualXID(vxid, &raw mut more);
        }
        if !TransactionIdIsValid(xid) {
            Assert!(!more);
            return true;
        }

        /* Check or wait for XID completion. */
        SET_LOCKTAG_TRANSACTION(&raw mut tag, xid);
        lar = LockAcquire(&raw const tag, ShareLock, false, !wait);
        if lar == LOCKACQUIRE_NOT_AVAIL {
            return false;
        }
        LockRelease(&raw const tag, ShareLock, false);

        if !more {
            break;
        }
    }

    true
}

/*
 * VirtualXactLock
 *
 * If wait = true, wait as long as the given VXID or any XID acquired by the
 * same transaction is still running.  Then, return true.
 *
 * If wait = false, just check whether that VXID or one of those XIDs is
 * still running, and return true or false.
 */
pub unsafe fn VirtualXactLock(vxid: VirtualTransactionId, wait: bool) -> bool {
    let mut tag: LOCKTAG = core::mem::zeroed();
    let proc_: *mut PGPROC;
    let mut xid: TransactionId = 0; // InvalidTransactionId

    Assert!(VirtualTransactionIdIsValid(vxid));

    if VirtualTransactionIdIsRecoveredPreparedXact(vxid) {
        /* no vxid lock; localTransactionId is a normal, locked XID */
        return XactLockForVirtualXact(vxid, vxid.localTransactionId, wait);
    }

    SET_LOCKTAG_VIRTUALTRANSACTION(&raw mut tag, vxid);

    /*
     * If a lock table entry must be made, this is the PGPROC on whose
     * behalf it must be done.
     */
    proc_ = ProcNumberGetProc(vxid.procNumber);
    if proc_.is_null() {
        return XactLockForVirtualXact(vxid, 0, wait);
    }

    /*
     * We must acquire this lock before checking the procNumber and lxid.
     */
    LWLockAcquire(&raw mut (*proc_).fpInfoLock as *mut LWLock, LW_EXCLUSIVE);

    if (*proc_).vxid.procNumber != vxid.procNumber
        || (*proc_).fpLocalTransactionId != vxid.localTransactionId
    {
        /* VXID ended */
        LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
        return XactLockForVirtualXact(vxid, 0, wait);
    }

    /*
     * If we aren't asked to wait, there's no need to set up a lock table
     * entry.  The transaction is still in progress, so just return false.
     */
    if !wait {
        LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
        return false;
    }

    /*
     * OK, we're going to need to sleep on the VXID.  But first, we must set
     * up the primary lock table entry, if needed.
     */
    if (*proc_).fpVXIDLock {
        let proclock: *mut PROCLOCK;
        let hashcode: uint32;
        let partitionLock: *mut LWLock;

        hashcode = LockTagHashCode(&raw const tag);
        partitionLock = LockHashPartitionLock(hashcode);
        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        let proclock = SetupLockInTable(
            LockMethods[DEFAULT_LOCKMETHOD as usize],
            proc_,
            &raw const tag,
            hashcode,
            ExclusiveLock,
        );
        if proclock.is_null() {
            LWLockRelease(partitionLock);
            LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);
            ereport!(ERROR, errmsg!("out of shared memory")) /* C also: errhint */;
        }
        GrantLock((*proclock).tag.myLock, proclock, ExclusiveLock);

        LWLockRelease(partitionLock);

        (*proc_).fpVXIDLock = false;
    }

    /*
     * If the proc has an XID now, we'll avoid a TwoPhaseGetXidByVirtualXID()
     * search.
     */
    xid = (*proc_).xid;

    /* Done with proc->fpLockBits */
    LWLockRelease(&raw mut (*proc_).fpInfoLock as *mut LWLock);

    /* Time to wait. */
    LockAcquire(&raw const tag, ShareLock, false, false);
    LockRelease(&raw const tag, ShareLock, false);

    XactLockForVirtualXact(vxid, xid, wait)
}

/*
 * LockWaiterCount
 *
 * Find the number of lock requesters on this locktag.
 */
pub unsafe fn LockWaiterCount(locktag: *const LOCKTAG) -> c_int {
    let lockmethodid = (*locktag).locktag_lockmethodid as LOCKMETHODID;
    let mut found: bool = false;
    let hashcode: uint32;
    let partitionLock: *mut LWLock;
    let mut waiters: c_int = 0;

    if lockmethodid <= 0 || (lockmethodid as usize) >= LockMethods.len() {
        elog!(ERROR, "unrecognized lock method: {}", lockmethodid);
    }

    hashcode = LockTagHashCode(locktag);
    partitionLock = LockHashPartitionLock(hashcode);
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    let lock = hash_search_with_hash_value(
        LockMethodLockHash,
        locktag as *const c_void,
        hashcode,
        HASH_FIND,
        &raw mut found,
    ) as *mut LOCK;
    if found {
        Assert!(!lock.is_null());
        waiters = (*lock).nRequested;
    }
    LWLockRelease(partitionLock);

    waiters
}

// dclist_head needs a dlist field alias for the foreach macro above
// (dclist_head in ilist.h contains a dlist_head named "dlist").
// This impl note is for the integrator.
// TODO(pg-port): add dclist_foreach! macro to lib/ilist.rs.

/* TODO(pg-port): LOCK_DEBUG-only print helpers (lock.c LOCK_PRINT / PROCLOCK_PRINT macros) */
#[cfg(any())] /* LOCK_DEBUG build only */
unsafe fn LOCK_PRINT(_where: *const c_char, _lock: *const LOCK, _type: LOCKMODE) {}
#[cfg(any())] /* LOCK_DEBUG build only */
unsafe fn PROCLOCK_PRINT(_where: *const c_char, _proclockP: *const PROCLOCK) {}

/*
 * Dump all locks in the given proc's myProcLocks lists.
 *
 * Caller is responsible for having acquired appropriate LWLocks.
 */
#[cfg(any())] /* LOCK_DEBUG build only */
pub unsafe fn DumpLocks(proc_: *mut PGPROC) {
    let i: c_int;

    if proc_.is_null() {
        return;
    }

    if !(*proc_).waitLock.is_null() {
        LOCK_PRINT(c"DumpLocks: waiting on".as_ptr(), (*proc_).waitLock, 0);
    }

    for i in 0..(NUM_LOCK_PARTITIONS as c_int) {
        let procLocks: *mut dlist_head = &mut (*proc_).myProcLocks[i as usize];
        let mut iter: dlist_iter = core::mem::zeroed();

        dlist_foreach!(iter, procLocks, {
            let proclock: *mut PROCLOCK = dlist_container!(PROCLOCK, procLink, iter.cur);
            let lock: *mut LOCK = (*proclock).tag.myLock;

            Assert!((*proclock).tag.myProc == proc_);
            PROCLOCK_PRINT(c"DumpLocks".as_ptr(), proclock);
            LOCK_PRINT(c"DumpLocks".as_ptr(), lock, 0);
        });
    }
}

/*
 * Dump all lmgr locks.
 *
 * Caller is responsible for having acquired appropriate LWLocks.
 */
#[cfg(any())] /* LOCK_DEBUG build only */
pub unsafe fn DumpAllLocks() {
    let proc_: *mut PGPROC;
    let mut proclock: *mut PROCLOCK;
    let lock: *mut LOCK;
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();

    proc_ = MyProc;

    if !proc_.is_null() && !(*proc_).waitLock.is_null() {
        LOCK_PRINT(c"DumpAllLocks: waiting on".as_ptr(), (*proc_).waitLock, 0);
    }

    hash_seq_init(&raw mut status, LockMethodProcLockHash);

    loop {
        proclock = hash_seq_search(&raw mut status) as *mut PROCLOCK;
        if proclock.is_null() {
            break;
        }
        PROCLOCK_PRINT(c"DumpAllLocks".as_ptr(), proclock);

        lock = (*proclock).tag.myLock;
        if !lock.is_null() {
            LOCK_PRINT(c"DumpAllLocks".as_ptr(), lock, 0);
        } else {
            elog!(LOG, "DumpAllLocks: proclock->tag.myLock = NULL");
        }
    }
}
