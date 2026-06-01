//! storage/lmgr/proc.c -- routines to manage per-process shared memory data structure
//!
//! Merged declarations from storage/proc.h (PGPROC, PROC_HDR, constants).
//!
//! Interface (a):
//!     JoinWaitQueue(), ProcSleep(), ProcWakeup()
//!
//! Waiting for a lock causes the backend to be put to sleep.  Whoever releases
//! the lock wakes the process up again (and gives it an error code so it knows
//! whether it was awoken on an error condition).
//!
//! Interface (b):
//!
//! ProcReleaseLocks -- frees the locks associated with current transaction
//!
//! ProcKill -- destroys the shared memory state (and locks)
//!     associated with the process.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/lmgr/proc.c

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::too_many_arguments)]

use crate::prelude::*;
use crate::access::transam::InvalidTransactionId;
// TODO(pg-port): GUCs from guc_tables.c
static mut autovacuum_worker_slots: c_int = 16;
static mut max_prepared_xacts: c_int = 0;

use core::ffi::c_int;
use std::ptr;
use std::sync::atomic::Ordering;

// ilist types
use crate::lib::ilist::{
    dclist_delete_from_thoroughly, dclist_head, dclist_insert_before, dclist_is_empty,
    dclist_push_tail, dlist_delete, dlist_head, dlist_init, dlist_is_empty, dlist_node,
    dlist_node_init, dlist_node_is_detached, dlist_pop_head_node, dlist_push_head,
    dlist_push_tail,
};

// Atomic types/ops
use crate::port::atomics_backend::pg_atomic_uint64;
// TODO(pg-port): full atomics API lives in port/atomics.h; minimal local shims.
#[repr(C)]
pub struct pg_atomic_uint32 { pub value: u32 }
unsafe fn pg_atomic_init_u32_impl(p: *mut pg_atomic_uint32, v: u32) { (*p).value = v; }
unsafe fn pg_atomic_read_u32_impl(p: *mut pg_atomic_uint32) -> u32 { (*p).value }
unsafe fn pg_atomic_init_u64_impl_native(p: *mut pg_atomic_uint64, v: u64) { crate::port::atomics_backend::pg_atomic_init_u64_impl(p, v) }
unsafe fn pg_atomic_write_u64_impl(p: *mut pg_atomic_uint64, v: u64) { (*p).value = v; }

// Semaphore API
use crate::storage::pg_sema::{PGSemaphore, PGSemaphoreCreate, PGSemaphoreReset};

// Spinlock API
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::storage::lmgr::s_lock::{slock_t, DEFAULT_SPINS_PER_DELAY};

// Latch API
use crate::storage::ipc::latch::{
    DisownLatch, InitSharedLatch, Latch, OwnLatch, ResetLatch, SetLatch, WaitLatch,
    WL_EXIT_ON_PM_DEATH, WL_LATCH_SET,
};

// Shmem
use crate::storage::ipc::shmem::{add_size, mul_size, ShmemInitStruct};

// ipc callbacks
use crate::storage::ipc::ipc::{on_shmem_exit, pg_on_exit_callback};

// lockdefs
use crate::storage::lockdefs::{LOCKMODE, LOCKMASK};

// proclist types (proclist_node)
use crate::storage::proclist_types::proclist_node;

// ProcNumber
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

// XLogRecPtr
use crate::access::transam::xlogdefs::XLogRecPtr;

// MemSet
use crate::c::MemSet;

// Datum / Int32GetDatum / DatumGetInt32
use crate::postgres::{Datum, DatumGetInt32, Int32GetDatum};

// Size (usize)
use crate::c::Size;

// sig_atomic_t
use crate::miscadmin::sig_atomic_t;

// =============================================================================
// Constants from storage/proc.h
// =============================================================================

/// Each backend advertises up to PGPROC_MAX_CACHED_SUBXIDS subtransactions.
pub const PGPROC_MAX_CACHED_SUBXIDS: usize = 64;

/// Flags for PGPROC->statusFlags and PROC_HDR->statusFlags[].
pub const PROC_IS_AUTOVACUUM: u8 = 0x01;
pub const PROC_IN_VACUUM: u8 = 0x02;
pub const PROC_IN_SAFE_IC: u8 = 0x04;
pub const PROC_VACUUM_FOR_WRAPAROUND: u8 = 0x08;
pub const PROC_IN_LOGICAL_DECODING: u8 = 0x10;
pub const PROC_AFFECTS_ALL_HORIZONS: u8 = 0x20;

/// Flags reset at EOXact.
pub const PROC_VACUUM_STATE_MASK: u8 =
    PROC_IN_VACUUM | PROC_IN_SAFE_IC | PROC_VACUUM_FOR_WRAPAROUND;

/// Xmin-related flags.
pub const PROC_XMIN_FLAGS: u8 = PROC_IN_VACUUM | PROC_IN_SAFE_IC;

/// Fast-path lock group limits.
pub const FP_LOCK_GROUPS_PER_BACKEND_MAX: usize = 1024;
pub const FP_LOCK_SLOTS_PER_GROUP: usize = 16; /* don't change */

/// delayChkptFlags bits.
pub const DELAY_CHKPT_START: c_int = 1 << 0;
pub const DELAY_CHKPT_COMPLETE: c_int = 1 << 1;

/// Auxiliary process count: bgwriter + checkpointer + walwriter + walsummarizer
/// + archiver + startup + walreceiver (6 fixed) + MAX_IO_WORKERS (32).
pub const MAX_IO_WORKERS: usize = 32;
pub const NUM_AUXILIARY_PROCS: usize = 6 + MAX_IO_WORKERS;

/// NUM_SPECIAL_WORKER_PROCS: autovac launcher + slotsync worker.
pub const NUM_SPECIAL_WORKER_PROCS: usize = 2;

// =============================================================================
// Stubs for NUM_LOCK_PARTITIONS, LWLock, and related lock-manager internals.
// The real defs live in storage/lock.h / storage/lwlock.h (being ported in
// parallel).
// =============================================================================

/// Number of lock-manager hash partitions.  TODO(pg-port): pull from real lwlock.h.
pub const NUM_LOCK_PARTITIONS: usize = 16;

/// LW lock mode constants.  TODO(pg-port): real values in storage/lwlock.h.
pub const LW_EXCLUSIVE: c_int = 0;
pub const LW_SHARED: c_int = 1;

/// LWLock wait-state constants (storage/lwlock.h).
pub const LW_WS_NOT_WAITING: u8 = 0;

/// LWLock tranche for fast-path per-backend locks.  TODO(pg-port): lwlocklist.h.
pub const LWTRANCHE_LOCK_FASTPATH: c_int = 0;

/// Lock methods (storage/lock.h).
pub const DEFAULT_LOCKMETHOD: c_int = 1;
pub const USER_LOCKMETHOD: c_int = 2;

/// AccessExclusiveLock (storage/lockdefs.h).
pub const AccessExclusiveLock: LOCKMODE = 8;

/// LOCKBIT_ON(m): bitmask for lock mode m.
#[inline]
pub fn LOCKBIT_ON(mode: LOCKMODE) -> LOCKMASK {
    1 << mode
}

// Opaque LWLock struct (canonical definition in storage/lwlock.h, ported later).
#[repr(C)]
pub struct LWLock {
    pub tranche: c_int,
    // opaque padding -- real struct has more fields
    _pad: [u8; 16],
}

// LOCK, PROCLOCK, LOCALLOCK, LOCKTAG, LockMethod (lock.h) -- local minimal stubs.
// TODO(pg-port): replace when storage/lock.rs lands.

#[repr(C)]
pub struct LOCKTAG {
    pub locktag_field1: uint32,
    pub locktag_field2: uint32,
    pub locktag_field3: uint32,
    pub locktag_field4: uint16,
    pub locktag_type: uint8,
    pub locktag_lockmethodid: uint8,
}

#[repr(C)]
pub struct LOCK {
    pub tag: LOCKTAG,
    pub grantMask: LOCKMASK,
    pub waitMask: LOCKMASK,
    pub procLocks: dlist_head,
    pub waitProcs: dclist_head,
    pub requested: [c_int; 9],
    pub nRequested: c_int,
    pub granted: [c_int; 9],
    pub nGranted: c_int,
}

#[repr(C)]
pub struct PROCLOCKTAG {
    pub myLock: *mut LOCK,
    pub myProc: *mut PGPROC,
}

#[repr(C)]
pub struct PROCLOCK {
    pub tag: PROCLOCKTAG,
    pub groupLeader: *mut PGPROC,
    pub holdMask: LOCKMASK,
    pub releaseMask: LOCKMASK,
    pub lockLink: dlist_node,
    pub procLink: dlist_node,
}

/// LOCALLOCK -- per-backend local lock table entry (storage/lock.h).
#[repr(C)]
pub struct LOCALLOCK {
    pub tag: LOCALLOCKTAG,
    pub hashcode: uint32,
    pub lock: *mut LOCK,
    pub proclock: *mut PROCLOCK,
    pub nLocks: i64,
    // (other fields omitted; opaque to proc.c)
}

#[repr(C)]
pub struct LOCALLOCKTAG {
    pub lock: LOCKTAG,
    pub mode: LOCKMODE,
}

#[repr(C)]
pub struct LockMethodData {
    pub numLockModes: c_int,
    pub conflictTab: *const LOCKMASK, // array [numLockModes+1]
}
pub type LockMethod = *const LockMethodData;

// VirtualTransactionId (storage/lock.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: LocalTransactionId,
}

// =============================================================================
// XidCacheStatus / XidCache (from proc.h)
// =============================================================================

/// Number of cached subxids plus overflow flag.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct XidCacheStatus {
    /// number of cached subxids, never more than PGPROC_MAX_CACHED_SUBXIDS
    pub count: u8,
    /// has PGPROC->subxids overflowed
    pub overflowed: bool,
}

/// Cache for subtransaction XIDs.
#[repr(C)]
pub struct XidCache {
    pub xids: [TransactionId; PGPROC_MAX_CACHED_SUBXIDS],
}

// =============================================================================
// ProcWaitStatus (proc.h)
// =============================================================================

pub type ProcWaitStatus = c_int;
pub const PROC_WAIT_STATUS_OK: ProcWaitStatus = 0;
pub const PROC_WAIT_STATUS_WAITING: ProcWaitStatus = 1;
pub const PROC_WAIT_STATUS_ERROR: ProcWaitStatus = 2;

// =============================================================================
// PGPROC -- the canonical shared struct (proc.h).
// All parallel-port stubs (procarray.rs, shm_mq.rs, syncrep.rs, deadlock.rs,
// etc.) carry LOCAL PGPROC stubs; THIS is the canonical one.  Integrator
// repoints those stubs here once the crate-graph permits it.
// =============================================================================

/// Per-process shared memory data structure.
///
/// See storage/proc.h for detailed field commentary.
#[repr(C)]
pub struct PGPROC {
    /// list link if process is in a list
    pub links: dlist_node,
    /// procglobal list that owns this PGPROC
    pub procgloballist: *mut dlist_head,

    /// ONE semaphore to sleep on
    pub sem: PGSemaphore,
    pub waitStatus: ProcWaitStatus,

    /// generic latch for process
    pub procLatch: Latch,

    /// id of top-level transaction currently being executed; mirrored in
    /// ProcGlobal->xids[pgxactoff]
    pub xid: TransactionId,

    /// minimal running XID (exclusive of LAZY VACUUM)
    pub xmin: TransactionId,

    /// Backend's process ID; 0 if prepared xact
    pub pid: c_int,

    /// offset into various ProcGlobal->arrays with data mirrored from this
    /// PGPROC
    pub pgxactoff: c_int,

    /// virtual transaction id (pair of ProcNumber + lxid)
    pub vxid: PgProcVxid,

    /// OID of database this backend is using
    pub databaseId: Oid,
    /// OID of role using this backend
    pub roleId: Oid,
    /// OID of temp schema this backend is using
    pub tempNamespaceId: Oid,

    /// true if it is a regular backend
    pub isRegularBackend: bool,

    /// conflict signal sent for current transaction (hot standby)
    pub recoveryConflictPending: bool,

    /// LWLock wait state (see LWLockWaitState)
    pub lwWaiting: u8,
    /// lwlock mode being waited for
    pub lwWaitMode: u8,
    /// position in LW lock wait list
    pub lwWaitLink: proclist_node,

    /// position in CV wait list
    pub cvWaitLink: proclist_node,

    /// Lock object we're sleeping on (NULL if not waiting)
    pub waitLock: *mut LOCK,
    /// Per-holder info for awaited lock
    pub waitProcLock: *mut PROCLOCK,
    /// type of lock we're waiting for
    pub waitLockMode: LOCKMODE,
    /// bitmask for lock types already held on this lock object
    pub heldLocks: LOCKMASK,
    /// time at which wait for lock acquisition started
    pub waitStart: pg_atomic_uint64,

    /// DELAY_CHKPT_* flags
    pub delayChkptFlags: c_int,

    /// this backend's status flags; mirrored in ProcGlobal->statusFlags[pgxactoff]
    pub statusFlags: u8,

    /// waiting for this LSN or higher (InvalidXLogRecPtr if not waiting)
    pub waitLSN: XLogRecPtr,
    /// wait state for sync rep
    pub syncRepState: c_int,
    /// list link if process is in syncrep queue
    pub syncRepLinks: dlist_node,

    /// All PROCLOCK objects for locks held or awaited by this backend, partitioned.
    pub myProcLocks: [dlist_head; NUM_LOCK_PARTITIONS],

    /// cached subxid status; mirrored with ProcGlobal->subxidStates[i]
    pub subxidStatus: XidCacheStatus,
    /// cache for subtransaction XIDs
    pub subxids: XidCache,

    /// true if member of ProcArray group waiting for XID clear
    pub procArrayGroupMember: bool,
    /// next ProcArray group member waiting for XID clear
    pub procArrayGroupNext: pg_atomic_uint32,
    /// latest transaction id among main XID and subtransactions
    pub procArrayGroupMemberXid: TransactionId,

    /// proc's wait information
    pub wait_event_info: uint32,

    /// true if member of clog group
    pub clogGroupMember: bool,
    /// next clog group member
    pub clogGroupNext: pg_atomic_uint32,
    /// transaction id of clog group member
    pub clogGroupMemberXid: TransactionId,
    /// transaction status of clog group member
    pub clogGroupMemberXidStatus: XidStatus,
    /// clog page corresponding to transaction id of clog group member
    pub clogGroupMemberPage: i64,
    /// WAL location of commit record for clog group member
    pub clogGroupMemberLsn: XLogRecPtr,

    /// protects per-backend fast-path state
    pub fpInfoLock: LWLock,
    /// lock modes held for each fast-path slot (variable-length, allocated in shmem)
    pub fpLockBits: *mut uint64,
    /// slots for rel oids (variable-length, allocated in shmem)
    pub fpRelId: *mut Oid,
    /// are we holding a fast-path VXID lock?
    pub fpVXIDLock: bool,
    /// lxid for fast-path VXID lock
    pub fpLocalTransactionId: LocalTransactionId,

    /// lock group leader, if I'm a member
    pub lockGroupLeader: *mut PGPROC,
    /// list of members, if I'm a leader
    pub lockGroupMembers: dlist_head,
    /// my member link, if I'm a member
    pub lockGroupLink: dlist_node,
}

/// Inline struct for the virtual transaction id inside PGPROC.
#[repr(C)]
pub struct PgProcVxid {
    /// For regular backends, equal to GetNumberFromPGProc(proc).
    /// For prepared xacts, ID of the original backend.
    /// For unused entries, INVALID_PROC_NUMBER.
    pub procNumber: ProcNumber,
    /// local id of top-level transaction currently being executed, else
    /// InvalidLocalTransactionId
    pub lxid: LocalTransactionId,
}

// =============================================================================
// PROC_HDR -- global process table header (proc.h).
// =============================================================================

/// Global process table (one per cluster).
#[repr(C)]
pub struct PROC_HDR {
    /// Array of PGPROC structures (not including dummies for prepared txns)
    pub allProcs: *mut PGPROC,

    /// Array mirroring PGPROC.xid for each PGPROC currently in the procarray
    pub xids: *mut TransactionId,

    /// Array mirroring PGPROC.subxidStatus for each PGPROC currently in the
    /// procarray
    pub subxidStates: *mut XidCacheStatus,

    /// Array mirroring PGPROC.statusFlags for each PGPROC currently in the
    /// procarray
    pub statusFlags: *mut u8,

    /// Length of allProcs array
    pub allProcCount: uint32,

    /// Head of list of free PGPROC structures
    pub freeProcs: dlist_head,
    /// Head of list of autovacuum & special worker free PGPROC structures
    pub autovacFreeProcs: dlist_head,
    /// Head of list of bgworker free PGPROC structures
    pub bgworkerFreeProcs: dlist_head,
    /// Head of list of walsender free PGPROC structures
    pub walsenderFreeProcs: dlist_head,

    /// First pgproc waiting for group XID clear
    pub procArrayGroupFirst: pg_atomic_uint32,
    /// First pgproc waiting for group transaction status update
    pub clogGroupFirst: pg_atomic_uint32,

    /// Current slot numbers of some auxiliary processes
    pub walwriterProc: ProcNumber,
    pub checkpointerProc: ProcNumber,

    /// Current shared estimate of appropriate spins_per_delay value
    pub spins_per_delay: c_int,
    /// Buffer id of the buffer that Startup process waits for pin on, or -1
    pub startupBufferPinWaitBufId: c_int,
}

// =============================================================================
// Accessor macros translated to inline fns (proc.h).
// =============================================================================

/// GetPGProcByNumber(n) -- return pointer to the PGPROC with the given ProcNumber.
#[inline]
pub unsafe fn GetPGProcByNumber(n: ProcNumber) -> *mut PGPROC {
    (*ProcGlobal).allProcs.add(n as usize)
}

/// GetNumberFromPGProc(proc) -- return the ProcNumber for a given PGPROC pointer.
#[inline]
pub unsafe fn GetNumberFromPGProc(proc_: *const PGPROC) -> ProcNumber {
    proc_.offset_from((*ProcGlobal).allProcs) as ProcNumber
}

/// FIRST_PREPARED_XACT_PROC_NUMBER -- first PGPROC slot used for prepared xacts.
#[inline]
pub unsafe fn FIRST_PREPARED_XACT_PROC_NUMBER() -> usize {
    MaxBackends as usize + NUM_AUXILIARY_PROCS
}

// =============================================================================
// Stub types / helpers for unported dependencies.
// TODO(pg-port): replace with real implementations when those modules land.
// =============================================================================

// XidStatus (access/clog.h)
pub type XidStatus = c_int;
pub const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0x00;

// LocalTransactionId (access/xlogdefs.h)
pub type LocalTransactionId = uint32;
pub const InvalidLocalTransactionId: LocalTransactionId = 0;

// XLogRecPtr utilities.
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

// SYNC_REP_NOT_WAITING (replication/syncrep.h)
pub const SYNC_REP_NOT_WAITING: c_int = 0; // TODO(pg-port): replication/syncrep.h

// DeadLockState (storage/lock.h) -- same values as in deadlock.rs.
pub type DeadLockState = c_int;
pub const DS_NOT_YET_CHECKED: DeadLockState = -1;
pub const DS_NO_DEADLOCK: DeadLockState = 0;
pub const DS_SOFT_DEADLOCK: DeadLockState = 1;
pub const DS_HARD_DEADLOCK: DeadLockState = 2;
pub const DS_BLOCKED_BY_AUTOVACUUM: DeadLockState = 3;

// FastPathLockGroupsPerBackend global (runtime value).
// TODO(pg-port): real extern lives in storage/proc.c.
pub static mut FastPathLockGroupsPerBackend: c_int = 1;

#[inline]
pub unsafe fn FastPathLockSlotsPerBackend() -> usize {
    FP_LOCK_SLOTS_PER_GROUP * FastPathLockGroupsPerBackend as usize
}

// Globals from miscadmin / utils (forward-declared; real values come from their
// respective modules).
extern "C" {
    pub static mut MyProcPid: c_int;
    pub static mut MyProcNumber: ProcNumber;
    pub static mut IsUnderPostmaster: bool;
    pub static mut MyLatch: *mut ::core::ffi::c_void;
    pub static mut InRecovery: bool;
    pub static mut InHotStandby: bool;
    pub static mut AutovacuumLauncherPid: c_int;
    pub static mut log_recovery_conflict_waits: bool;
    pub static mut message_level_is_interesting_threshold: c_int;
}

use crate::utils::init::globals::{MaxBackends, MaxConnections, max_worker_processes};
// TODO(pg-port): max_wal_senders GUC lives in guc_tables.c / walsender.c
static mut max_wal_senders: c_int = 10;

// =============================================================================
// Global variables (proc.c statics / externs).
// =============================================================================

/* GUC variables */
pub static mut DeadlockTimeout: c_int = 1000;
pub static mut StatementTimeout: c_int = 0;
pub static mut LockTimeout: c_int = 0;
pub static mut IdleInTransactionSessionTimeout: c_int = 0;
pub static mut TransactionTimeout: c_int = 0;
pub static mut IdleSessionTimeout: c_int = 0;
pub static mut log_lock_waits: bool = false;

/// Pointer to this process's PGPROC struct, if any.
pub static mut MyProc: *mut PGPROC = ptr::null_mut();

/// This spinlock protects the freelist of recycled PGPROC structures.
/// We cannot use an LWLock because the LWLock manager depends on already
/// having a PGPROC and a wait semaphore!  But these structures are touched
/// relatively infrequently (only at backend startup or shutdown) and not for
/// very long, so a spinlock is okay.
pub static mut ProcStructLock: *mut slock_t = ptr::null_mut();

/// Pointers to shared-memory structures.
pub static mut ProcGlobal: *mut PROC_HDR = ptr::null_mut();
pub static mut AuxiliaryProcs: *mut PGPROC = ptr::null_mut();
pub static mut PreparedXactProcs: *mut PGPROC = ptr::null_mut();

static mut deadlock_state: DeadLockState = DS_NOT_YET_CHECKED;

/// Is a deadlock check pending?
static mut got_deadlock_timeout: sig_atomic_t = 0;

// =============================================================================
// CHECK_FOR_INTERRUPTS (miscadmin.h stub)
// =============================================================================

macro_rules! CHECK_FOR_INTERRUPTS {
    () => {{
        // TODO(pg-port): miscadmin.h CHECK_FOR_INTERRUPTS()
    }};
}

// =============================================================================
// Stub functions for unported lock-manager internals (lock.c / procarray.c /
// etc.).  These will be replaced once those translation units land.
// =============================================================================

/// TODO(pg-port): storage/lock.c -- LWLockAcquire
pub unsafe fn LWLockAcquire(lock: *mut LWLock, mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): storage/lwlock.c
}

/// TODO(pg-port): storage/lock.c -- LWLockRelease
pub unsafe fn LWLockRelease(lock: *mut LWLock) {
    unimplemented!() // TODO(pg-port): storage/lwlock.c
}

/// TODO(pg-port): storage/lwlock.c -- LWLockInitialize
unsafe fn LWLockInitialize(lock: *mut LWLock, tranche_id: c_int) {
    unimplemented!() // TODO(pg-port): storage/lwlock.c
}

/// TODO(pg-port): storage/lwlock.c -- LWLockReleaseAll
unsafe fn LWLockReleaseAll() {
    unimplemented!() // TODO(pg-port): storage/lwlock.c
}

/// TODO(pg-port): storage/lwlock.c -- LWLockHeldByMeInMode
unsafe fn LWLockHeldByMeInMode(lock: *mut LWLock, mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): storage/lwlock.c
}

/// TODO(pg-port): storage/lwlock.c -- LWLockHeldByMe
unsafe fn LWLockHeldByMe(lock: *mut LWLock) -> bool {
    unimplemented!() // TODO(pg-port): storage/lwlock.c
}

/// TODO(pg-port): storage/lock.c -- LockHashPartitionLock
unsafe fn LockHashPartitionLock(hashcode: uint32) -> *mut LWLock {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- LockHashPartitionLockByIndex
unsafe fn LockHashPartitionLockByIndex(i: usize) -> *mut LWLock {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- LockHashPartitionLockByProc
unsafe fn LockHashPartitionLockByProc(proc_: *const PGPROC) -> *mut LWLock {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- LockTagHashCode
unsafe fn LockTagHashCode(tag: *const LOCKTAG) -> uint32 {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- LockCheckConflicts
unsafe fn LockCheckConflicts(
    lockMethodTable: LockMethod,
    lockmode: LOCKMODE,
    lock: *mut LOCK,
    proclock: *mut PROCLOCK,
) -> bool {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- GrantLock
unsafe fn GrantLock(lock: *mut LOCK, proclock: *mut PROCLOCK, lockmode: LOCKMODE) {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- RemoveFromWaitQueue
unsafe fn RemoveFromWaitQueue(proc_: *mut PGPROC, hashcode: uint32) {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- LockReleaseAll
unsafe fn LockReleaseAll(lockmethodid: c_int, allLocks: bool) {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- AbortStrongLockAcquire
unsafe fn AbortStrongLockAcquire() {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- GetAwaitedLock
unsafe fn GetAwaitedLock() -> *mut LOCALLOCK {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- ResetAwaitedLock
unsafe fn ResetAwaitedLock() {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/lock.c -- GrantAwaitedLock
unsafe fn GrantAwaitedLock() {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/procarray.c -- ProcArrayAdd
unsafe fn ProcArrayAdd(proc_: *mut PGPROC) {
    unimplemented!() // TODO(pg-port): storage/procarray.c
}

/// TODO(pg-port): storage/procarray.c -- ProcArrayRemove
unsafe fn ProcArrayRemove(proc_: *mut PGPROC, latestXid: TransactionId) {
    unimplemented!() // TODO(pg-port): storage/procarray.c
}

/// TODO(pg-port): storage/procarray.c -- GetBlockingAutoVacuumPgproc
unsafe fn GetBlockingAutoVacuumPgproc() -> *mut PGPROC {
    unimplemented!() // TODO(pg-port): storage/procarray.c
}

/// TODO(pg-port): replication/syncrep.c -- SyncRepCleanupAtProcExit
unsafe fn SyncRepCleanupAtProcExit() {
    unimplemented!() // TODO(pg-port): replication/syncrep.c
}

/// TODO(pg-port): storage/condition_variable.c -- ConditionVariableCancelSleep
unsafe fn ConditionVariableCancelSleep() {
    unimplemented!() // TODO(pg-port): storage/condition_variable.c
}

/// TODO(pg-port): storage/lmgr/deadlock.c -- DeadLockCheck
unsafe fn DeadLockCheck(proc_: *mut PGPROC) -> DeadLockState {
    unimplemented!() // TODO(pg-port): storage/lmgr/deadlock.c
}

/// TODO(pg-port): storage/lmgr/deadlock.c -- RememberSimpleDeadLock
unsafe fn RememberSimpleDeadLock(
    myProc: *mut PGPROC,
    lockmode: LOCKMODE,
    lock: *mut LOCK,
    conflictProc: *mut PGPROC,
) {
    unimplemented!() // TODO(pg-port): storage/lmgr/deadlock.c
}

/// TODO(pg-port): storage/lmgr/deadlock.c -- InitDeadLockChecking
unsafe fn InitDeadLockChecking() {
    unimplemented!() // TODO(pg-port): storage/lmgr/deadlock.c
}

/// TODO(pg-port): storage/lmgr/lwlock.c -- InitLWLockAccess
unsafe fn InitLWLockAccess() {
    unimplemented!() // TODO(pg-port): storage/lmgr/lwlock.c
}

/// TODO(pg-port): utils/timeout.c -- timeout types and APIs.
pub type TimeoutId = c_int;
pub const DEADLOCK_TIMEOUT: TimeoutId = 0; // TODO(pg-port): utils/timeout.h
pub const LOCK_TIMEOUT: TimeoutId = 1; // TODO(pg-port): utils/timeout.h

#[repr(C)]
pub struct EnableTimeoutParams {
    pub id: TimeoutId,
    pub r#type: c_int,
    pub delay_ms: c_int,
}
pub const TMPARAM_AFTER: c_int = 0; // TODO(pg-port): utils/timeout.h

#[repr(C)]
pub struct DisableTimeoutParams {
    pub id: TimeoutId,
    pub keep_indicator: bool,
}

unsafe fn enable_timeout_after(id: TimeoutId, delay_ms: c_int) {
    unimplemented!() // TODO(pg-port): utils/timeout.c
}
unsafe fn enable_timeouts(timeouts: *const EnableTimeoutParams, count: c_int) {
    unimplemented!() // TODO(pg-port): utils/timeout.c
}
unsafe fn disable_timeout(id: TimeoutId, keep_indicator: bool) {
    unimplemented!() // TODO(pg-port): utils/timeout.c
}
unsafe fn disable_timeouts(timeouts: *const DisableTimeoutParams, count: c_int) {
    unimplemented!() // TODO(pg-port): utils/timeout.c
}
unsafe fn get_timeout_start_time(id: TimeoutId) -> TimestampTz {
    unimplemented!() // TODO(pg-port): utils/timeout.c
}

/// TODO(pg-port): utils/timestamp.h
pub type TimestampTz = i64;
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    unimplemented!() // TODO(pg-port): utils/timestamp.c
}
unsafe fn TimestampDifference(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    secs: *mut i64,
    microsecs: *mut c_int,
) {
    unimplemented!() // TODO(pg-port): utils/timestamp.c
}
unsafe fn TimestampDifferenceExceeds(
    start_time: TimestampTz,
    stop_time: TimestampTz,
    msec: c_int,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/timestamp.c
}

/// TODO(pg-port): postmaster/autovacuum.h
unsafe fn AmAutoVacuumWorkerProcess() -> bool {
    unimplemented!() // TODO(pg-port): postmaster/autovacuum.c
}

/// TODO(pg-port): postmaster -- process identity predicates.
unsafe fn AmSpecialWorkerProcess() -> bool {
    unimplemented!() // TODO(pg-port): postmaster
}
unsafe fn AmBackgroundWorkerProcess() -> bool {
    unimplemented!() // TODO(pg-port): postmaster
}
unsafe fn AmWalSenderProcess() -> bool {
    unimplemented!() // TODO(pg-port): postmaster
}
unsafe fn AmRegularBackendProcess() -> bool {
    unimplemented!() // TODO(pg-port): postmaster
}

/// TODO(pg-port): storage/standby.c
unsafe fn CheckRecoveryConflictDeadlock() {
    unimplemented!() // TODO(pg-port): storage/standby.c
}
unsafe fn ResolveRecoveryConflictWithLock(locktag: LOCKTAG, log_conflict: bool) {
    unimplemented!() // TODO(pg-port): storage/standby.c
}

/// TODO(pg-port): storage/lock.c
unsafe fn GetLockConflicts(
    locktag: *const LOCKTAG,
    lockmode: LOCKMODE,
    cnt: *mut c_int,
) -> *mut VirtualTransactionId {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): storage/standby.c
pub type ProcSignalReason = c_int;
pub const PROCSIG_RECOVERY_CONFLICT_LOCK: ProcSignalReason = 0; // TODO(pg-port)
unsafe fn LogRecoveryConflict(
    reason: ProcSignalReason,
    wait_start: TimestampTz,
    now: TimestampTz,
    wait_list: *const VirtualTransactionId,
    still_waiting: bool,
) {
    unimplemented!() // TODO(pg-port): storage/standby.c
}

/// TODO(pg-port): storage/lmgr/lmgr.c -- DescribeLockTag
unsafe fn DescribeLockTag(buf: *mut StringInfoData, tag: *const LOCKTAG) {
    unimplemented!() // TODO(pg-port): storage/lmgr/lmgr.c
}

/// TODO(pg-port): storage/lock.c -- GetLockmodeName
unsafe fn GetLockmodeName(lockmethodid: c_int, mode: LOCKMODE) -> *const c_char {
    unimplemented!() // TODO(pg-port): storage/lock.c
}

/// TODO(pg-port): pgstat -- wait event reporting.
unsafe fn pgstat_set_wait_event_storage(ptr: *mut uint32) {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}
unsafe fn pgstat_reset_wait_event_storage() {
    unimplemented!() // TODO(pg-port): utils/activity/pgstat.c
}

/// TODO(pg-port): postmaster -- register active child.
unsafe fn RegisterPostmasterChildActive() {
    unimplemented!() // TODO(pg-port): postmaster/postmaster.c
}

/// TODO(pg-port): backend/storage/ipc -- EXEC_BACKEND only.
unsafe fn AttachSharedMemoryStructs() {
    unimplemented!() // TODO(pg-port): EXEC_BACKEND path
}

/// TODO(pg-port): utils/init/globals.c -- set_spins_per_delay
unsafe fn set_spins_per_delay(shared: c_int) {
    // mirrors: static inline void set_spins_per_delay(int shared_spins)
    // TODO(pg-port): port s_lock.h set_spins_per_delay
}

/// TODO(pg-port): utils/init/globals.c -- update_spins_per_delay
unsafe fn update_spins_per_delay(shared: c_int) -> c_int {
    shared // TODO(pg-port): real tuning logic in s_lock.h
}

/// HOLD_INTERRUPTS / RESUME_INTERRUPTS (miscadmin.h)
macro_rules! HOLD_INTERRUPTS {
    () => {{
        // TODO(pg-port): miscadmin.h HOLD_INTERRUPTS()
    }};
}
macro_rules! RESUME_INTERRUPTS {
    () => {{
        // TODO(pg-port): miscadmin.h RESUME_INTERRUPTS()
    }};
}

// StringInfoData / initStringInfo / appendStringInfo (lib/stringinfo.h)
use crate::lib::stringinfo::StringInfoData;
unsafe fn initStringInfo(str_: *mut StringInfoData) {
    unimplemented!() // TODO(pg-port): lib/stringinfo.c
}
unsafe fn appendStringInfo(str_: *mut StringInfoData, fmt: *const c_char, pid: c_int) {
    unimplemented!() // TODO(pg-port): lib/stringinfo.c
}
unsafe fn pfree(ptr: *mut ::core::ffi::c_void) {
    unimplemented!() // TODO(pg-port): utils/mmgr/mcxt.c
}

/// message_level_is_interesting (elog.h)
unsafe fn message_level_is_interesting(level: c_int) -> bool {
    false // TODO(pg-port): utils/error/elog.c
}

pub const DEBUG1: c_int = -1; // TODO(pg-port): utils/elog.h
pub const ProcArrayLock: *mut LWLock = ptr::null_mut(); // TODO(pg-port): storage/procarray.c stub

// PG_WAIT_LOCK / locktag_type from lock.h -- just a u32 shifted value.
// TODO(pg-port): real macros in storage/lock.h
unsafe fn PG_WAIT_LOCK_TYPE(locktag_type: u8) -> uint32 {
    0x0300_0000 | locktag_type as uint32
}

// =============================================================================
// PGProcShmemSize (internal helper)
// =============================================================================

/// Report shared-memory space needed by PGPROC.
unsafe fn PGProcShmemSize() -> Size {
    let mut size: Size = 0;
    let TotalProcs: Size = add_size(
        MaxBackends as Size,
        add_size(NUM_AUXILIARY_PROCS, max_prepared_xacts as Size),
    );

    size = add_size(size, mul_size(TotalProcs, core::mem::size_of::<PGPROC>()));
    size = add_size(
        size,
        mul_size(TotalProcs, core::mem::size_of::<TransactionId>()),
    );
    size = add_size(
        size,
        mul_size(TotalProcs, core::mem::size_of::<XidCacheStatus>()),
    );
    size = add_size(size, mul_size(TotalProcs, core::mem::size_of::<u8>()));

    size
}

// =============================================================================
// FastPathLockShmemSize (internal helper)
// =============================================================================

/// Report shared-memory space needed for Fast-Path locks.
unsafe fn FastPathLockShmemSize() -> Size {
    let mut size: Size = 0;
    let TotalProcs: Size = add_size(
        MaxBackends as Size,
        add_size(NUM_AUXILIARY_PROCS, max_prepared_xacts as Size),
    );

    let fpLockBitsSize: Size = maxalign(FastPathLockGroupsPerBackend as Size * core::mem::size_of::<uint64>());
    let fpRelIdSize: Size = maxalign(FastPathLockSlotsPerBackend() * core::mem::size_of::<Oid>());

    size = add_size(size, mul_size(TotalProcs, fpLockBitsSize + fpRelIdSize));
    size
}

/// MAXALIGN stub -- align to pointer size.  TODO(pg-port): real MAXALIGN from c.h.
#[inline]
unsafe fn maxalign(size: Size) -> Size {
    let align = core::mem::align_of::<*mut ()>();
    (size + align - 1) & !(align - 1)
}

// =============================================================================
// ProcGlobalShmemSize
// =============================================================================

/// Report shared-memory space needed by InitProcGlobal.
pub unsafe fn ProcGlobalShmemSize() -> Size {
    let mut size: Size = 0;

    /* ProcGlobal */
    size = add_size(size, core::mem::size_of::<PROC_HDR>());
    size = add_size(size, core::mem::size_of::<slock_t>());

    size = add_size(size, PGProcShmemSize());
    size = add_size(size, FastPathLockShmemSize());

    size
}

// =============================================================================
// ProcGlobalSemas
// =============================================================================

/// Report number of semaphores needed by InitProcGlobal.
pub unsafe fn ProcGlobalSemas() -> c_int {
    /*
     * We need a sema per backend (including autovacuum), plus one for each
     * auxiliary process.
     */
    MaxBackends + NUM_AUXILIARY_PROCS as c_int
}

// =============================================================================
// InitProcGlobal
// =============================================================================

/// InitProcGlobal -
///   Initialize the global process table during postmaster or standalone
///   backend startup.
///
///   We also create all the per-process semaphores we will need to support
///   the requested number of backends.  We used to allocate semaphores
///   only when backends were actually started up, but that is bad because
///   it lets Postgres fail under load --- a lot of Unix systems are
///   (mis)configured with small limits on the number of semaphores, and
///   running out when trying to start another backend is a common failure.
///   So, now we grab enough semaphores to support the desired max number
///   of backends immediately at initialization --- if the sysadmin has set
///   MaxConnections, max_worker_processes, max_wal_senders, or
///   autovacuum_worker_slots higher than his kernel will support, he'll
///   find out sooner rather than later.
///
///   Another reason for creating semaphores here is that the semaphore
///   implementation typically requires us to create semaphores in the
///   postmaster, not in backends.
///
/// Note: this is NOT called by individual backends under a postmaster,
/// not even in the EXEC_BACKEND case.  The ProcGlobal and AuxiliaryProcs
/// pointers must be propagated specially for EXEC_BACKEND operation.
pub unsafe fn InitProcGlobal() {
    let procs: *mut PGPROC;
    let mut i: usize;
    let mut j: usize;
    let mut found: bool = false;
    let TotalProcs: usize =
        MaxBackends as usize + NUM_AUXILIARY_PROCS + max_prepared_xacts as usize;

    /* Used for setup of per-backend fast-path slots. */
    let mut fpPtr: *mut u8;
    let fpLockBitsSize: usize;
    let fpRelIdSize: usize;
    let requestSize: Size;
    let mut ptr: *mut u8;

    /* Create the ProcGlobal shared structure */
    ProcGlobal = ShmemInitStruct(
        b"Proc Header\0".as_ptr() as *const c_char,
        core::mem::size_of::<PROC_HDR>(),
        &mut found,
    ) as *mut PROC_HDR;
    Assert!(!found);

    /*
     * Initialize the data structures.
     */
    (*ProcGlobal).spins_per_delay = DEFAULT_SPINS_PER_DELAY;
    dlist_init(&mut (*ProcGlobal).freeProcs);
    dlist_init(&mut (*ProcGlobal).autovacFreeProcs);
    dlist_init(&mut (*ProcGlobal).bgworkerFreeProcs);
    dlist_init(&mut (*ProcGlobal).walsenderFreeProcs);
    (*ProcGlobal).startupBufferPinWaitBufId = -1;
    (*ProcGlobal).walwriterProc = INVALID_PROC_NUMBER;
    (*ProcGlobal).checkpointerProc = INVALID_PROC_NUMBER;
    pg_atomic_init_u32_impl(&raw mut (*ProcGlobal).procArrayGroupFirst, INVALID_PROC_NUMBER as uint32);
    pg_atomic_init_u32_impl(&raw mut (*ProcGlobal).clogGroupFirst, INVALID_PROC_NUMBER as uint32);

    /*
     * Create and initialize all the PGPROC structures we'll need.  There are
     * six separate consumers: (1) normal backends, (2) autovacuum workers and
     * special workers, (3) background workers, (4) walsenders, (5) auxiliary
     * processes, and (6) prepared transactions.  (For largely-historical
     * reasons, we combine autovacuum and special workers into one category
     * with a single freelist.)  Each PGPROC structure is dedicated to exactly
     * one of these purposes, and they do not move between groups.
     */
    requestSize = PGProcShmemSize();

    ptr = ShmemInitStruct(
        b"PGPROC structures\0".as_ptr() as *const c_char,
        requestSize,
        &mut found,
    ) as *mut u8;

    MemSet(ptr as *mut ::core::ffi::c_void, 0, requestSize);

    procs = ptr as *mut PGPROC;
    ptr = ptr.add(TotalProcs * core::mem::size_of::<PGPROC>());

    (*ProcGlobal).allProcs = procs;
    /* XXX allProcCount isn't really all of them; it excludes prepared xacts */
    (*ProcGlobal).allProcCount = (MaxBackends as usize + NUM_AUXILIARY_PROCS) as uint32;

    /*
     * Allocate arrays mirroring PGPROC fields in a dense manner. See
     * PROC_HDR.
     *
     * XXX: It might make sense to increase padding for these arrays, given
     * how hotly they are accessed.
     */
    (*ProcGlobal).xids = ptr as *mut TransactionId;
    ptr = ptr.add(TotalProcs * core::mem::size_of::<TransactionId>());

    (*ProcGlobal).subxidStates = ptr as *mut XidCacheStatus;
    ptr = ptr.add(TotalProcs * core::mem::size_of::<XidCacheStatus>());

    (*ProcGlobal).statusFlags = ptr as *mut u8;
    ptr = ptr.add(TotalProcs * core::mem::size_of::<u8>());

    /* make sure we didn't overflow */
    Assert!(
        (ptr as usize > procs as usize)
            && (ptr as usize <= procs as usize + requestSize)
    );

    /*
     * Allocate arrays for fast-path locks. Those are variable-length, so
     * can't be included in PGPROC directly. We allocate a separate piece of
     * shared memory and then divide that between backends.
     */
    fpLockBitsSize = maxalign(FastPathLockGroupsPerBackend as usize * core::mem::size_of::<uint64>());
    fpRelIdSize = maxalign(FastPathLockSlotsPerBackend() * core::mem::size_of::<Oid>());

    let fp_request_size = FastPathLockShmemSize();

    fpPtr = ShmemInitStruct(
        b"Fast-Path Lock Array\0".as_ptr() as *const c_char,
        fp_request_size,
        &mut found,
    ) as *mut u8;

    MemSet(fpPtr as *mut ::core::ffi::c_void, 0, fp_request_size);

    /* For asserts checking we did not overflow. */
    let fpEndPtr = fpPtr.add(fp_request_size);

    i = 0;
    while i < TotalProcs {
        let proc_: *mut PGPROC = &mut *procs.add(i);

        /* Common initialization for all PGPROCs, regardless of type. */

        /*
         * Set the fast-path lock arrays, and move the pointer. We interleave
         * the two arrays, to (hopefully) get some locality for each backend.
         */
        (*proc_).fpLockBits = fpPtr as *mut uint64;
        fpPtr = fpPtr.add(fpLockBitsSize);

        (*proc_).fpRelId = fpPtr as *mut Oid;
        fpPtr = fpPtr.add(fpRelIdSize);

        Assert!(fpPtr as usize <= fpEndPtr as usize);

        /*
         * Set up per-PGPROC semaphore, latch, and fpInfoLock.  Prepared xact
         * dummy PGPROCs don't need these though - they're never associated
         * with a real process
         */
        if i < FIRST_PREPARED_XACT_PROC_NUMBER() {
            (*proc_).sem = PGSemaphoreCreate();
            InitSharedLatch(&mut (*proc_).procLatch);
            LWLockInitialize(&mut (*proc_).fpInfoLock, LWTRANCHE_LOCK_FASTPATH);
        }

        /*
         * Newly created PGPROCs for normal backends, autovacuum workers,
         * special workers, bgworkers, and walsenders must be queued up on the
         * appropriate free list.  Because there can only ever be a small,
         * fixed number of auxiliary processes, no free list is used in that
         * case; InitAuxiliaryProcess() instead uses a linear search.  PGPROCs
         * for prepared transactions are added to a free list by
         * TwoPhaseShmemInit().
         */
        if i < MaxConnections as usize {
            /* PGPROC for normal backend, add to freeProcs list */
            dlist_push_tail(&mut (*ProcGlobal).freeProcs, &mut (*proc_).links);
            (*proc_).procgloballist = &mut (*ProcGlobal).freeProcs;
        } else if i
            < MaxConnections as usize
                + autovacuum_worker_slots as usize
                + NUM_SPECIAL_WORKER_PROCS
        {
            /* PGPROC for AV or special worker, add to autovacFreeProcs list */
            dlist_push_tail(&mut (*ProcGlobal).autovacFreeProcs, &mut (*proc_).links);
            (*proc_).procgloballist = &mut (*ProcGlobal).autovacFreeProcs;
        } else if i
            < MaxConnections as usize
                + autovacuum_worker_slots as usize
                + NUM_SPECIAL_WORKER_PROCS
                + max_worker_processes as usize
        {
            /* PGPROC for bgworker, add to bgworkerFreeProcs list */
            dlist_push_tail(&mut (*ProcGlobal).bgworkerFreeProcs, &mut (*proc_).links);
            (*proc_).procgloballist = &mut (*ProcGlobal).bgworkerFreeProcs;
        } else if i < MaxBackends as usize {
            /* PGPROC for walsender, add to walsenderFreeProcs list */
            dlist_push_tail(&mut (*ProcGlobal).walsenderFreeProcs, &mut (*proc_).links);
            (*proc_).procgloballist = &mut (*ProcGlobal).walsenderFreeProcs;
        }

        /* Initialize myProcLocks[] shared memory queues. */
        j = 0;
        while j < NUM_LOCK_PARTITIONS {
            dlist_init(&mut (*proc_).myProcLocks[j]);
            j += 1;
        }

        /* Initialize lockGroupMembers list. */
        dlist_init(&mut (*proc_).lockGroupMembers);

        /*
         * Initialize the atomic variables, otherwise, it won't be safe to
         * access them for backends that aren't currently in use.
         */
        pg_atomic_init_u32_impl(&raw mut (*proc_).procArrayGroupNext, INVALID_PROC_NUMBER as uint32);
        pg_atomic_init_u32_impl(&raw mut (*proc_).clogGroupNext, INVALID_PROC_NUMBER as uint32);
        pg_atomic_init_u64_impl_native(&raw mut (*proc_).waitStart, 0);

        i += 1;
    }

    /* Should have consumed exactly the expected amount of fast-path memory. */
    Assert!(fpPtr as usize == fpEndPtr as usize);

    /*
     * Save pointers to the blocks of PGPROC structures reserved for auxiliary
     * processes and prepared transactions.
     */
    AuxiliaryProcs = procs.add(MaxBackends as usize);
    PreparedXactProcs = procs.add(FIRST_PREPARED_XACT_PROC_NUMBER());

    /* Create ProcStructLock spinlock, too */
    ProcStructLock = ShmemInitStruct(
        b"ProcStructLock spinlock\0".as_ptr() as *const c_char,
        core::mem::size_of::<slock_t>(),
        &mut found,
    ) as *mut slock_t;
    SpinLockInit(ProcStructLock);
}

// =============================================================================
// InitProcess
// =============================================================================

/// InitProcess -- initialize a per-process PGPROC entry for this backend.
pub unsafe fn InitProcess() {
    let procgloballist: *mut dlist_head;

    /*
     * ProcGlobal should be set up already (if we are a backend, we inherit
     * this by fork() or EXEC_BACKEND mechanism from the postmaster).
     */
    if ProcGlobal.is_null() {
        elog!(PANIC, "proc header uninitialized");
    }

    if !MyProc.is_null() {
        elog!(ERROR, "you already exist");
    }

    /*
     * Before we start accessing the shared memory in a serious way, mark
     * ourselves as an active postmaster child; this is so that the postmaster
     * can detect it if we exit without cleaning up.
     */
    if IsUnderPostmaster {
        RegisterPostmasterChildActive();
    }

    /*
     * Decide which list should supply our PGPROC.  This logic must match the
     * way the freelists were constructed in InitProcGlobal().
     */
    if AmAutoVacuumWorkerProcess() || AmSpecialWorkerProcess() {
        procgloballist = &mut (*ProcGlobal).autovacFreeProcs;
    } else if AmBackgroundWorkerProcess() {
        procgloballist = &mut (*ProcGlobal).bgworkerFreeProcs;
    } else if AmWalSenderProcess() {
        procgloballist = &mut (*ProcGlobal).walsenderFreeProcs;
    } else {
        procgloballist = &mut (*ProcGlobal).freeProcs;
    }

    /*
     * Try to get a proc struct from the appropriate free list.  If this
     * fails, we must be out of PGPROC structures (not to mention semaphores).
     *
     * While we are holding the ProcStructLock, also copy the current shared
     * estimate of spins_per_delay to local storage.
     */
    SpinLockAcquire(ProcStructLock);

    set_spins_per_delay((*ProcGlobal).spins_per_delay);

    if !dlist_is_empty(procgloballist) {
        // dlist_container(PGPROC, links, dlist_pop_head_node(procgloballist))
        let node = dlist_pop_head_node(procgloballist);
        MyProc = (node as *mut u8)
            .sub(core::mem::offset_of!(PGPROC, links)) as *mut PGPROC;
        SpinLockRelease(ProcStructLock);
    } else {
        /*
         * If we reach here, all the PGPROCs are in use.  This is one of the
         * possible places to detect "too many backends", so give the standard
         * error message.  XXX do we need to give a different failure message
         * in the autovacuum case?
         */
        SpinLockRelease(ProcStructLock);
        if AmWalSenderProcess() {
            ereport!(
                FATAL,
                errmsg!(
                    "number of requested standby connections exceeds \"max_wal_senders\" (currently {})",
                    max_wal_senders
                )
            );
        }
        ereport!(FATAL, errmsg!("sorry, too many clients already"));
    }
    MyProcNumber = GetNumberFromPGProc(MyProc);

    /*
     * Cross-check that the PGPROC is of the type we expect; if this were not
     * the case, it would get returned to the wrong list.
     */
    Assert!((*MyProc).procgloballist == procgloballist);

    /*
     * Initialize all fields of MyProc, except for those previously
     * initialized by InitProcGlobal.
     */
    dlist_node_init(&mut (*MyProc).links);
    (*MyProc).waitStatus = PROC_WAIT_STATUS_OK;
    (*MyProc).fpVXIDLock = false;
    (*MyProc).fpLocalTransactionId = InvalidLocalTransactionId;
    (*MyProc).xid = InvalidTransactionId;
    (*MyProc).xmin = InvalidTransactionId;
    (*MyProc).pid = MyProcPid;
    (*MyProc).vxid.procNumber = MyProcNumber;
    (*MyProc).vxid.lxid = InvalidLocalTransactionId;
    /* databaseId and roleId will be filled in later */
    (*MyProc).databaseId = InvalidOid;
    (*MyProc).roleId = InvalidOid;
    (*MyProc).tempNamespaceId = InvalidOid;
    (*MyProc).isRegularBackend = AmRegularBackendProcess();
    (*MyProc).delayChkptFlags = 0;
    (*MyProc).statusFlags = 0;
    /* NB -- autovac launcher intentionally does not set IS_AUTOVACUUM */
    if AmAutoVacuumWorkerProcess() {
        (*MyProc).statusFlags |= PROC_IS_AUTOVACUUM;
    }
    (*MyProc).lwWaiting = LW_WS_NOT_WAITING;
    (*MyProc).lwWaitMode = 0;
    (*MyProc).waitLock = ptr::null_mut();
    (*MyProc).waitProcLock = ptr::null_mut();
    pg_atomic_write_u64_impl(&raw mut (*MyProc).waitStart, 0);

    // USE_ASSERT_CHECKING block:
    {
        let mut k = 0usize;
        /* Last process should have released all locks. */
        while k < NUM_LOCK_PARTITIONS {
            Assert!(dlist_is_empty(&(*MyProc).myProcLocks[k]));
            k += 1;
        }
    }

    (*MyProc).recoveryConflictPending = false;

    /* Initialize fields for sync rep */
    (*MyProc).waitLSN = 0;
    (*MyProc).syncRepState = SYNC_REP_NOT_WAITING;
    dlist_node_init(&mut (*MyProc).syncRepLinks);

    /* Initialize fields for group XID clearing. */
    (*MyProc).procArrayGroupMember = false;
    (*MyProc).procArrayGroupMemberXid = InvalidTransactionId;
    Assert!(pg_atomic_read_u32_impl(&raw mut (*MyProc).procArrayGroupNext) == INVALID_PROC_NUMBER as uint32);

    /* Check that group locking fields are in a proper initial state. */
    Assert!((*MyProc).lockGroupLeader.is_null());
    Assert!(dlist_is_empty(&(*MyProc).lockGroupMembers));

    /* Initialize wait event information. */
    (*MyProc).wait_event_info = 0;

    /* Initialize fields for group transaction status update. */
    (*MyProc).clogGroupMember = false;
    (*MyProc).clogGroupMemberXid = InvalidTransactionId;
    (*MyProc).clogGroupMemberXidStatus = TRANSACTION_STATUS_IN_PROGRESS;
    (*MyProc).clogGroupMemberPage = -1;
    (*MyProc).clogGroupMemberLsn = InvalidXLogRecPtr;
    Assert!(pg_atomic_read_u32_impl(&raw mut (*MyProc).clogGroupNext) == INVALID_PROC_NUMBER as uint32);

    /*
     * Acquire ownership of the PGPROC's latch, so that we can use WaitLatch
     * on it.  That allows us to repoint the process latch, which so far
     * points to process local one, to the shared one.
     */
    OwnLatch(&mut (*MyProc).procLatch);
    crate::miscadmin::SwitchToSharedLatch();

    /* now that we have a proc, report wait events to shared memory */
    pgstat_set_wait_event_storage(&mut (*MyProc).wait_event_info);

    /*
     * We might be reusing a semaphore that belonged to a failed process. So
     * be careful and reinitialize its value here.  (This is not strictly
     * necessary anymore, but seems like a good idea for cleanliness.)
     */
    PGSemaphoreReset((*MyProc).sem);

    /*
     * Arrange to clean up at backend exit.
     */
    on_shmem_exit(ProcKill, Int32GetDatum(0));

    /*
     * Now that we have a PGPROC, we could try to acquire locks, so initialize
     * local state needed for LWLocks, and the deadlock checker.
     */
    InitLWLockAccess();
    InitDeadLockChecking();

    // EXEC_BACKEND:
    // if IsUnderPostmaster { AttachSharedMemoryStructs(); }
}

// =============================================================================
// InitProcessPhase2
// =============================================================================

/// InitProcessPhase2 -- make MyProc visible in the shared ProcArray.
///
/// This is separate from InitProcess because we can't acquire LWLocks until
/// we've created a PGPROC, but in the EXEC_BACKEND case ProcArrayAdd won't
/// work until after we've done AttachSharedMemoryStructs.
pub unsafe fn InitProcessPhase2() {
    Assert!(!MyProc.is_null());

    /*
     * Add our PGPROC to the PGPROC array in shared memory.
     */
    ProcArrayAdd(MyProc);

    /*
     * Arrange to clean that up at backend exit.
     */
    on_shmem_exit(RemoveProcFromArray, Int32GetDatum(0));
}

// =============================================================================
// InitAuxiliaryProcess
// =============================================================================

/// InitAuxiliaryProcess -- create a PGPROC entry for an auxiliary process.
///
/// This is called by bgwriter and similar processes so that they will have a
/// MyProc value that's real enough to let them wait for LWLocks.  The PGPROC
/// and sema that are assigned are one of the extra ones created during
/// InitProcGlobal.
///
/// Auxiliary processes are presently not expected to wait for real (lockmgr)
/// locks, so we need not set up the deadlock checker.  They are never added
/// to the ProcArray or the sinval messaging mechanism, either.  They also
/// don't get a VXID assigned, since this is only useful when we actually
/// hold lockmgr locks.
///
/// Startup process however uses locks but never waits for them in the
/// normal backend sense. Startup process also takes part in sinval messaging
/// as a sendOnly process, so never reads messages from sinval queue. So
/// Startup process does have a VXID and does show up in pg_locks.
pub unsafe fn InitAuxiliaryProcess() {
    let mut auxproc: *mut PGPROC;
    let proctype: c_int;

    /*
     * ProcGlobal should be set up already (if we are a backend, we inherit
     * this by fork() or EXEC_BACKEND mechanism from the postmaster).
     */
    if ProcGlobal.is_null() || AuxiliaryProcs.is_null() {
        elog!(PANIC, "proc header uninitialized");
    }

    if !MyProc.is_null() {
        elog!(ERROR, "you already exist");
    }

    if IsUnderPostmaster {
        RegisterPostmasterChildActive();
    }

    /*
     * We use the ProcStructLock to protect assignment and releasing of
     * AuxiliaryProcs entries.
     *
     * While we are holding the ProcStructLock, also copy the current shared
     * estimate of spins_per_delay to local storage.
     */
    SpinLockAcquire(ProcStructLock);

    set_spins_per_delay((*ProcGlobal).spins_per_delay);

    /*
     * Find a free auxproc ... *big* trouble if there isn't one ...
     */
    let mut found_type: c_int = -1;
    {
        let mut t = 0i32;
        while t < NUM_AUXILIARY_PROCS as i32 {
            auxproc = AuxiliaryProcs.add(t as usize);
            if (*auxproc).pid == 0 {
                found_type = t;
                break;
            }
            t += 1;
        }
    }
    if found_type >= NUM_AUXILIARY_PROCS as i32 || found_type < 0 {
        SpinLockRelease(ProcStructLock);
        elog!(FATAL, "all AuxiliaryProcs are in use");
    }
    proctype = found_type;
    auxproc = AuxiliaryProcs.add(proctype as usize);

    /* Mark auxiliary proc as in use by me */
    /* use volatile pointer to prevent code rearrangement */
    (*(auxproc as *mut PGPROC)).pid = MyProcPid;

    SpinLockRelease(ProcStructLock);

    MyProc = auxproc;
    MyProcNumber = GetNumberFromPGProc(MyProc);

    /*
     * Initialize all fields of MyProc, except for those previously
     * initialized by InitProcGlobal.
     */
    dlist_node_init(&mut (*MyProc).links);
    (*MyProc).waitStatus = PROC_WAIT_STATUS_OK;
    (*MyProc).fpVXIDLock = false;
    (*MyProc).fpLocalTransactionId = InvalidLocalTransactionId;
    (*MyProc).xid = InvalidTransactionId;
    (*MyProc).xmin = InvalidTransactionId;
    (*MyProc).vxid.procNumber = INVALID_PROC_NUMBER;
    (*MyProc).vxid.lxid = InvalidLocalTransactionId;
    (*MyProc).databaseId = InvalidOid;
    (*MyProc).roleId = InvalidOid;
    (*MyProc).tempNamespaceId = InvalidOid;
    (*MyProc).isRegularBackend = false;
    (*MyProc).delayChkptFlags = 0;
    (*MyProc).statusFlags = 0;
    (*MyProc).lwWaiting = LW_WS_NOT_WAITING;
    (*MyProc).lwWaitMode = 0;
    (*MyProc).waitLock = ptr::null_mut();
    (*MyProc).waitProcLock = ptr::null_mut();
    pg_atomic_write_u64_impl(&raw mut (*MyProc).waitStart, 0);

    // USE_ASSERT_CHECKING block:
    {
        let mut k = 0usize;
        /* Last process should have released all locks. */
        while k < NUM_LOCK_PARTITIONS {
            Assert!(dlist_is_empty(&(*MyProc).myProcLocks[k]));
            k += 1;
        }
    }

    /*
     * Acquire ownership of the PGPROC's latch, so that we can use WaitLatch
     * on it.  That allows us to repoint the process latch, which so far
     * points to process local one, to the shared one.
     */
    OwnLatch(&mut (*MyProc).procLatch);
    crate::miscadmin::SwitchToSharedLatch();

    /* now that we have a proc, report wait events to shared memory */
    pgstat_set_wait_event_storage(&mut (*MyProc).wait_event_info);

    /* Check that group locking fields are in a proper initial state. */
    Assert!((*MyProc).lockGroupLeader.is_null());
    Assert!(dlist_is_empty(&(*MyProc).lockGroupMembers));

    /*
     * We might be reusing a semaphore that belonged to a failed process. So
     * be careful and reinitialize its value here.  (This is not strictly
     * necessary anymore, but seems like a good idea for cleanliness.)
     */
    PGSemaphoreReset((*MyProc).sem);

    /*
     * Arrange to clean up at process exit.
     */
    on_shmem_exit(AuxiliaryProcKill, Int32GetDatum(proctype));

    /*
     * Now that we have a PGPROC, we could try to acquire lightweight locks.
     * Initialize local state needed for them.  (Heavyweight locks cannot be
     * acquired in aux processes.)
     */
    InitLWLockAccess();

    // EXEC_BACKEND:
    // if IsUnderPostmaster { AttachSharedMemoryStructs(); }
}

// =============================================================================
// SetStartupBufferPinWaitBufId / GetStartupBufferPinWaitBufId
// =============================================================================

/// Used from bufmgr to share the value of the buffer that Startup waits on,
/// or to reset the value to "not waiting" (-1). This allows processing
/// of recovery conflicts for buffer pins. Set is made before backends look
/// at this value, so locking not required, especially since the set is
/// an atomic integer set operation.
pub unsafe fn SetStartupBufferPinWaitBufId(bufid: c_int) {
    /* use volatile pointer to prevent code rearrangement */
    let procglobal: *mut PROC_HDR = ProcGlobal;
    (*procglobal).startupBufferPinWaitBufId = bufid;
}

/// Used by backends when they receive a request to check for buffer pin waits.
pub unsafe fn GetStartupBufferPinWaitBufId() -> c_int {
    /* use volatile pointer to prevent code rearrangement */
    let procglobal: *mut PROC_HDR = ProcGlobal;
    (*procglobal).startupBufferPinWaitBufId
}

// =============================================================================
// HaveNFreeProcs
// =============================================================================

/// Check whether there are at least N free PGPROC objects.  If false is
/// returned, *nfree will be set to the number of free PGPROC objects.
/// Otherwise, *nfree will be set to n.
///
/// Note: this is designed on the assumption that N will generally be small.
pub unsafe fn HaveNFreeProcs(n: c_int, nfree: *mut c_int) -> bool {
    Assert!(n > 0);
    Assert!(!nfree.is_null());

    SpinLockAcquire(ProcStructLock);

    *nfree = 0;
    // dlist_foreach over freeProcs
    {
        let head: *const dlist_head = &(*ProcGlobal).freeProcs;
        let mut cur = (*head).head.next;
        loop {
            // sentinel: cur points back to head
            if cur as usize == head as usize {
                break;
            }
            *nfree += 1;
            if *nfree == n {
                break;
            }
            cur = (*cur).next;
        }
    }

    SpinLockRelease(ProcStructLock);

    *nfree == n
}

// =============================================================================
// LockErrorCleanup
// =============================================================================

/// Cancel any pending wait for lock, when aborting a transaction, and revert
/// any strong lock count acquisition for a lock being acquired.
///
/// (Normally, this would only happen if we accept a cancel/die
/// interrupt while waiting; but an ereport(ERROR) before or during the lock
/// wait is within the realm of possibility, too.)
pub unsafe fn LockErrorCleanup() {
    let lockAwaited: *mut LOCALLOCK;
    let partitionLock: *mut LWLock;
    let timeouts: [DisableTimeoutParams; 2] = [
        DisableTimeoutParams { id: DEADLOCK_TIMEOUT, keep_indicator: false },
        DisableTimeoutParams { id: LOCK_TIMEOUT,     keep_indicator: true  },
    ];

    HOLD_INTERRUPTS!();

    AbortStrongLockAcquire();

    /* Nothing to do if we weren't waiting for a lock */
    lockAwaited = GetAwaitedLock();
    if lockAwaited.is_null() {
        RESUME_INTERRUPTS!();
        return;
    }

    /*
     * Turn off the deadlock and lock timeout timers, if they are still
     * running (see ProcSleep).  Note we must preserve the LOCK_TIMEOUT
     * indicator flag, since this function is executed before
     * ProcessInterrupts when responding to SIGINT; else we'd lose the
     * knowledge that the SIGINT came from a lock timeout and not an external
     * source.
     */
    disable_timeouts(timeouts.as_ptr(), 2);

    /* Unlink myself from the wait queue, if on it (might not be anymore!) */
    partitionLock = LockHashPartitionLock((*lockAwaited).hashcode);
    LWLockAcquire(partitionLock, LW_EXCLUSIVE);

    if !dlist_node_is_detached(&(*MyProc).links) {
        /* We could not have been granted the lock yet */
        RemoveFromWaitQueue(MyProc, (*lockAwaited).hashcode);
    } else {
        /*
         * Somebody kicked us off the lock queue already.  Perhaps they
         * granted us the lock, or perhaps they detected a deadlock. If they
         * did grant us the lock, we'd better remember it in our local lock
         * table.
         */
        if (*MyProc).waitStatus == PROC_WAIT_STATUS_OK {
            GrantAwaitedLock();
        }
    }

    ResetAwaitedLock();

    LWLockRelease(partitionLock);

    RESUME_INTERRUPTS!();
}

// =============================================================================
// ProcReleaseLocks
// =============================================================================

/// ProcReleaseLocks() -- release locks associated with current transaction
///         at main transaction commit or abort.
///
/// At main transaction commit, we release standard locks except session locks.
/// At main transaction abort, we release all locks including session locks.
///
/// Advisory locks are released only if they are transaction-level;
/// session-level holds remain, whether this is a commit or not.
///
/// At subtransaction commit, we don't release any locks (so this func is not
/// needed at all); we will defer the releasing to the parent transaction.
/// At subtransaction abort, we release all locks held by the subtransaction;
/// this is implemented by retail releasing of the locks under control of
/// the ResourceOwner mechanism.
pub unsafe fn ProcReleaseLocks(isCommit: bool) {
    if MyProc.is_null() {
        return;
    }
    /* If waiting, get off wait queue (should only be needed after error) */
    LockErrorCleanup();
    /* Release standard locks, including session-level if aborting */
    LockReleaseAll(DEFAULT_LOCKMETHOD, !isCommit);
    /* Release transaction-level advisory locks */
    LockReleaseAll(USER_LOCKMETHOD, false);
}

// =============================================================================
// RemoveProcFromArray (on_shmem_exit callback)
// =============================================================================

/// RemoveProcFromArray() -- Remove this process from the shared ProcArray.
unsafe extern "C" fn RemoveProcFromArray(code: c_int, arg: Datum) {
    Assert!(!MyProc.is_null());
    ProcArrayRemove(MyProc, InvalidTransactionId);
}

// =============================================================================
// ProcKill (on_shmem_exit callback)
// =============================================================================

/// ProcKill() -- Destroy the per-proc data structure for this process.
///     Release any of its held LW locks.
unsafe extern "C" fn ProcKill(code: c_int, arg: Datum) {
    let proc_: *mut PGPROC;
    let procgloballist: *mut dlist_head;

    Assert!(!MyProc.is_null());

    /* not safe if forked by system(), etc. */
    if (*MyProc).pid != libc_getpid() {
        elog!(PANIC, "ProcKill() called in child process");
    }

    /* Make sure we're out of the sync rep lists */
    SyncRepCleanupAtProcExit();

    // USE_ASSERT_CHECKING block:
    {
        let mut k = 0usize;
        /* Last process should have released all locks. */
        while k < NUM_LOCK_PARTITIONS {
            Assert!(dlist_is_empty(&(*MyProc).myProcLocks[k]));
            k += 1;
        }
    }

    /*
     * Release any LW locks I am holding.  There really shouldn't be any, but
     * it's cheap to check again before we cut the knees off the LWLock
     * facility by releasing our PGPROC ...
     */
    LWLockReleaseAll();

    /* Cancel any pending condition variable sleep, too */
    ConditionVariableCancelSleep();

    /*
     * Detach from any lock group of which we are a member.  If the leader
     * exits before all other group members, its PGPROC will remain allocated
     * until the last group process exits; that process must return the
     * leader's PGPROC to the appropriate list.
     */
    if !(*MyProc).lockGroupLeader.is_null() {
        let leader: *mut PGPROC = (*MyProc).lockGroupLeader;
        let leader_lwlock: *mut LWLock = LockHashPartitionLockByProc(leader);

        LWLockAcquire(leader_lwlock, LW_EXCLUSIVE);
        Assert!(!dlist_is_empty(&(*leader).lockGroupMembers));
        dlist_delete(&mut (*MyProc).lockGroupLink);
        if dlist_is_empty(&(*leader).lockGroupMembers) {
            (*leader).lockGroupLeader = ptr::null_mut();
            if leader != MyProc {
                procgloballist = (*leader).procgloballist;

                /* Leader exited first; return its PGPROC. */
                SpinLockAcquire(ProcStructLock);
                dlist_push_head(procgloballist, &mut (*leader).links);
                SpinLockRelease(ProcStructLock);
            }
        } else if leader != MyProc {
            (*MyProc).lockGroupLeader = ptr::null_mut();
        }
        LWLockRelease(leader_lwlock);
    }

    /*
     * Reset MyLatch to the process local one.  This is so that signal
     * handlers et al can continue using the latch after the shared latch
     * isn't ours anymore.
     *
     * Similarly, stop reporting wait events to MyProc->wait_event_info.
     *
     * After that clear MyProc and disown the shared latch.
     */
    crate::miscadmin::SwitchBackToLocalLatch();
    pgstat_reset_wait_event_storage();

    proc_ = MyProc;
    MyProc = ptr::null_mut();
    MyProcNumber = INVALID_PROC_NUMBER;
    DisownLatch(&mut (*proc_).procLatch);

    /* Mark the proc no longer in use */
    (*proc_).pid = 0;
    (*proc_).vxid.procNumber = INVALID_PROC_NUMBER;
    (*proc_).vxid.lxid = InvalidTransactionId as LocalTransactionId;

    let saved_procgloballist = (*proc_).procgloballist;
    SpinLockAcquire(ProcStructLock);

    /*
     * If we're still a member of a locking group, that means we're a leader
     * which has somehow exited before its children.  The last remaining child
     * will release our PGPROC.  Otherwise, release it now.
     */
    if (*proc_).lockGroupLeader.is_null() {
        /* Since lockGroupLeader is NULL, lockGroupMembers should be empty. */
        Assert!(dlist_is_empty(&(*proc_).lockGroupMembers));

        /* Return PGPROC structure (and semaphore) to appropriate freelist */
        dlist_push_tail(saved_procgloballist, &mut (*proc_).links);
    }

    /* Update shared estimate of spins_per_delay */
    (*ProcGlobal).spins_per_delay =
        update_spins_per_delay((*ProcGlobal).spins_per_delay);

    SpinLockRelease(ProcStructLock);

    /* wake autovac launcher if needed -- see comments in FreeWorkerInfo */
    if AutovacuumLauncherPid != 0 {
        libc_kill(AutovacuumLauncherPid, libc_SIGUSR2());
    }
}

// =============================================================================
// AuxiliaryProcKill (on_shmem_exit callback)
// =============================================================================

/// AuxiliaryProcKill() -- Cut-down version of ProcKill for auxiliary
///     processes (bgwriter, etc).  The PGPROC and sema are not released, only
///     marked as not-in-use.
unsafe extern "C" fn AuxiliaryProcKill(code: c_int, arg: Datum) {
    let proctype: c_int = DatumGetInt32(arg);
    let proc_: *mut PGPROC;

    Assert!(proctype >= 0 && proctype < NUM_AUXILIARY_PROCS as i32);

    /* not safe if forked by system(), etc. */
    if (*MyProc).pid != libc_getpid() {
        elog!(PANIC, "AuxiliaryProcKill() called in child process");
    }

    let auxproc: *mut PGPROC = AuxiliaryProcs.add(proctype as usize);
    Assert!(MyProc == auxproc);

    /* Release any LW locks I am holding (see notes above) */
    LWLockReleaseAll();

    /* Cancel any pending condition variable sleep, too */
    ConditionVariableCancelSleep();

    /* look at the equivalent ProcKill() code for comments */
    crate::miscadmin::SwitchBackToLocalLatch();
    pgstat_reset_wait_event_storage();

    proc_ = MyProc;
    MyProc = ptr::null_mut();
    MyProcNumber = INVALID_PROC_NUMBER;
    DisownLatch(&mut (*proc_).procLatch);

    SpinLockAcquire(ProcStructLock);

    /* Mark auxiliary proc no longer in use */
    (*proc_).pid = 0;
    (*proc_).vxid.procNumber = INVALID_PROC_NUMBER;
    (*proc_).vxid.lxid = InvalidTransactionId as LocalTransactionId;

    /* Update shared estimate of spins_per_delay */
    (*ProcGlobal).spins_per_delay =
        update_spins_per_delay((*ProcGlobal).spins_per_delay);

    SpinLockRelease(ProcStructLock);
}

// =============================================================================
// AuxiliaryPidGetProc
// =============================================================================

/// AuxiliaryPidGetProc -- get PGPROC for an auxiliary process given its PID.
///
/// Returns NULL if not found.
pub unsafe fn AuxiliaryPidGetProc(pid: c_int) -> *mut PGPROC {
    let mut result: *mut PGPROC = ptr::null_mut();

    if pid == 0 {
        /* never match dummy PGPROCs */
        return ptr::null_mut();
    }

    let mut index = 0usize;
    while index < NUM_AUXILIARY_PROCS {
        let proc_: *mut PGPROC = AuxiliaryProcs.add(index);
        if (*proc_).pid == pid {
            result = proc_;
            break;
        }
        index += 1;
    }
    result
}

// =============================================================================
// Thin libc wrappers for system calls (getpid, kill, SIGUSR2)
// =============================================================================

/// getpid() -- return the calling process's PID.
unsafe fn libc_getpid() -> c_int {
    extern "C" {
        fn getpid() -> i32;
    }
    getpid()
}

/// kill(pid, sig) -- send signal to process.
unsafe fn libc_kill(pid: c_int, sig: c_int) -> c_int {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    kill(pid, sig)
}

/// SIGUSR2 signal number (platform-defined; use libc constant).
fn libc_SIGUSR2() -> c_int {
    12 // SIGUSR2 on Linux/macOS
}

// =============================================================================
// JoinWaitQueue
// =============================================================================

/// JoinWaitQueue -- join the wait queue on the specified lock.
///
/// It's not actually guaranteed that we need to wait when this function is
/// called, because it could be that when we try to find a position at which
/// to insert ourself into the wait queue, we discover that we must be inserted
/// ahead of everyone who wants a lock that conflicts with ours. In that case,
/// we get the lock immediately. Because of this, it's sensible for this function
/// to have a dontWait argument, despite the name.
///
/// On entry, the caller has already set up LOCK and PROCLOCK entries to
/// reflect that we have "requested" the lock.  The caller is responsible for
/// cleaning that up, if we end up not joining the queue after all.
///
/// The lock table's partition lock must be held at entry, and is still held
/// at exit.  The caller must release it before calling ProcSleep().
///
/// Result is one of the following:
///
///  PROC_WAIT_STATUS_OK       - lock was immediately granted
///  PROC_WAIT_STATUS_WAITING  - joined the wait queue; call ProcSleep()
///  PROC_WAIT_STATUS_ERROR    - immediate deadlock was detected, or would
///                              need to wait and dontWait == true
///
/// NOTES: The process queue is now a priority queue for locking.
pub unsafe fn JoinWaitQueue(
    locallock: *mut LOCALLOCK,
    lockMethodTable: LockMethod,
    dontWait: bool,
) -> ProcWaitStatus {
    let lockmode: LOCKMODE = (*locallock).tag.mode;
    let lock: *mut LOCK = (*locallock).lock;
    let proclock: *mut PROCLOCK = (*locallock).proclock;
    let hashcode: uint32 = (*locallock).hashcode;
    let waitQueue: *mut dclist_head = &mut (*lock).waitProcs;
    let mut insert_before: *mut PGPROC = ptr::null_mut();
    let myProcHeldLocks: LOCKMASK;
    let mut myHeldLocks: LOCKMASK;
    let mut early_deadlock: bool = false;
    let leader: *mut PGPROC = (*MyProc).lockGroupLeader;

    /*
     * Set bitmask of locks this process already holds on this object.
     */
    myHeldLocks = (*proclock).holdMask;
    (*MyProc).heldLocks = myHeldLocks;

    /*
     * Determine which locks we're already holding.
     *
     * If group locking is in use, locks held by members of my locking group
     * need to be included in myHeldLocks.  This is not required for relation
     * extension lock which conflict among group members. However, including
     * them in myHeldLocks will give group members the priority to get those
     * locks as compared to other backends which are also trying to acquire
     * those locks.  OTOH, we can avoid giving priority to group members for
     * that kind of locks, but there doesn't appear to be a clear advantage of
     * the same.
     */
    myProcHeldLocks = (*proclock).holdMask;
    myHeldLocks = myProcHeldLocks;
    if !leader.is_null() {
        // dlist_foreach over lock->procLocks
        let head: *const dlist_head = &(*lock).procLocks;
        let mut cur = (*head).head.next;
        loop {
            if cur as usize == head as usize {
                break;
            }
            let otherproclock: *mut PROCLOCK = (cur as *mut u8)
                .sub(core::mem::offset_of!(PROCLOCK, lockLink)) as *mut PROCLOCK;
            if (*otherproclock).groupLeader == leader {
                myHeldLocks |= (*otherproclock).holdMask;
            }
            cur = (*cur).next;
        }
    }

    /*
     * Determine where to add myself in the wait queue.
     *
     * Normally I should go at the end of the queue.  However, if I already
     * hold locks that conflict with the request of any previous waiter, put
     * myself in the queue just in front of the first such waiter. This is not
     * a necessary step, since deadlock detection would move me to before that
     * waiter anyway; but it's relatively cheap to detect such a conflict
     * immediately, and avoid delaying till deadlock timeout.
     *
     * Special case: if I find I should go in front of some waiter, check to
     * see if I conflict with already-held locks or the requests before that
     * waiter.  If not, then just grant myself the requested lock immediately.
     * This is the same as the test for immediate grant in LockAcquire, except
     * we are only considering the part of the wait queue before my insertion
     * point.
     */
    if myHeldLocks != 0 && !dclist_is_empty(waitQueue) {
        let mut aheadRequests: LOCKMASK = 0;
        // dclist_foreach over waitQueue
        let head: *const dclist_head = waitQueue;
        let dlist_head_ptr: *const dlist_head = head as *const _ as *const dlist_head;
        let mut cur = (*(dlist_head_ptr as *const dlist_node)).next;
        loop {
            // dclist sentinel: cur points back to sentinel node inside dclist_head
            if cur as *const _ as usize
                == &(*head).dlist as *const _ as usize
            {
                break;
            }
            let proc_: *mut PGPROC = (cur as *mut u8)
                .sub(core::mem::offset_of!(PGPROC, links)) as *mut PGPROC;

            /*
             * If we're part of the same locking group as this waiter, its
             * locks neither conflict with ours nor contribute to
             * aheadRequests.
             */
            if !leader.is_null() && leader == (*proc_).lockGroupLeader {
                cur = (*cur).next;
                continue;
            }

            /* Must he wait for me? */
            if (*(*lockMethodTable).conflictTab.add((*proc_).waitLockMode as usize)) & myHeldLocks != 0
            {
                /* Must I wait for him ? */
                if (*(*lockMethodTable).conflictTab.add(lockmode as usize)) & (*proc_).heldLocks != 0
                {
                    /*
                     * Yes, so we have a deadlock.  Easiest way to clean up
                     * correctly is to call RemoveFromWaitQueue(), but we
                     * can't do that until we are *on* the wait queue. So, set
                     * a flag to check below, and break out of loop.  Also,
                     * record deadlock info for later message.
                     */
                    RememberSimpleDeadLock(MyProc, lockmode, lock, proc_);
                    early_deadlock = true;
                    break;
                }
                /* I must go before this waiter.  Check special case. */
                if ((*(*lockMethodTable).conflictTab.add(lockmode as usize)) & aheadRequests) == 0
                    && !LockCheckConflicts(lockMethodTable, lockmode, lock, proclock)
                {
                    /* Skip the wait and just grant myself the lock. */
                    GrantLock(lock, proclock, lockmode);
                    return PROC_WAIT_STATUS_OK;
                }

                /* Put myself into wait queue before conflicting process */
                insert_before = proc_;
                break;
            }
            /* Nope, so advance to next waiter */
            aheadRequests |= LOCKBIT_ON((*proc_).waitLockMode);
            cur = (*cur).next;
        }
    }

    /*
     * If we detected deadlock, give up without waiting.  This must agree with
     * CheckDeadLock's recovery code.
     */
    if early_deadlock {
        return PROC_WAIT_STATUS_ERROR;
    }

    /*
     * At this point we know that we'd really need to sleep. If we've been
     * commanded not to do that, bail out.
     */
    if dontWait {
        return PROC_WAIT_STATUS_ERROR;
    }

    /*
     * Insert self into queue, at the position determined above.
     */
    if !insert_before.is_null() {
        dclist_insert_before(waitQueue, &mut (*insert_before).links, &mut (*MyProc).links);
    } else {
        dclist_push_tail(waitQueue, &mut (*MyProc).links);
    }

    (*lock).waitMask |= LOCKBIT_ON(lockmode);

    /* Set up wait information in PGPROC object, too */
    (*MyProc).heldLocks = myProcHeldLocks;
    (*MyProc).waitLock = lock;
    (*MyProc).waitProcLock = proclock;
    (*MyProc).waitLockMode = lockmode;

    (*MyProc).waitStatus = PROC_WAIT_STATUS_WAITING;

    PROC_WAIT_STATUS_WAITING
}

// =============================================================================
// ProcSleep
// =============================================================================

/// ProcSleep -- put process to sleep waiting on lock.
///
/// This must be called when JoinWaitQueue() returns PROC_WAIT_STATUS_WAITING.
/// Returns after the lock has been granted, or if a deadlock is detected.  Can
/// also bail out with ereport(ERROR), if some other error condition, or a
/// timeout or cancellation is triggered.
///
/// Result is one of the following:
///
///  PROC_WAIT_STATUS_OK      - lock was granted
///  PROC_WAIT_STATUS_ERROR   - a deadlock was detected
pub unsafe fn ProcSleep(locallock: *mut LOCALLOCK) -> ProcWaitStatus {
    let lockmode: LOCKMODE = (*locallock).tag.mode;
    let lock: *mut LOCK = (*locallock).lock;
    let hashcode: uint32 = (*locallock).hashcode;
    let partitionLock: *mut LWLock = LockHashPartitionLock(hashcode);
    let mut standbyWaitStart: TimestampTz = 0;
    let mut allow_autovacuum_cancel: bool = true;
    let mut logged_recovery_conflict: bool = false;
    let myWaitStatus: ProcWaitStatus;

    /* The caller must've armed the on-error cleanup mechanism */
    Assert!(GetAwaitedLock() == locallock);
    Assert!(!LWLockHeldByMe(partitionLock));

    /*
     * Now that we will successfully clean up after an ereport, it's safe to
     * check to see if there's a buffer pin deadlock against the Startup
     * process.  Of course, that's only necessary if we're doing Hot Standby
     * and are not the Startup process ourselves.
     */
    if RecoveryInProgress() && !InRecovery {
        CheckRecoveryConflictDeadlock();
    }

    /* Reset deadlock_state before enabling the timeout handler */
    deadlock_state = DS_NOT_YET_CHECKED;
    got_deadlock_timeout = 0;

    /*
     * Set timer so we can wake up after awhile and check for a deadlock. If a
     * deadlock is detected, the handler sets MyProc->waitStatus =
     * PROC_WAIT_STATUS_ERROR, allowing us to know that we must report failure
     * rather than success.
     *
     * By delaying the check until we've waited for a bit, we can avoid
     * running the rather expensive deadlock-check code in most cases.
     *
     * If LockTimeout is set, also enable the timeout for that.  We can save a
     * few cycles by enabling both timeout sources in one call.
     *
     * If InHotStandby we set lock waits slightly later for clarity with other
     * code.
     */
    if !InHotStandby {
        if LockTimeout > 0 {
            let timeouts: [EnableTimeoutParams; 2] = [
                EnableTimeoutParams {
                    id: DEADLOCK_TIMEOUT,
                    r#type: TMPARAM_AFTER,
                    delay_ms: DeadlockTimeout,
                },
                EnableTimeoutParams {
                    id: LOCK_TIMEOUT,
                    r#type: TMPARAM_AFTER,
                    delay_ms: LockTimeout,
                },
            ];
            enable_timeouts(timeouts.as_ptr(), 2);
        } else {
            enable_timeout_after(DEADLOCK_TIMEOUT, DeadlockTimeout);
        }

        /*
         * Use the current time obtained for the deadlock timeout timer as
         * waitStart (i.e., the time when this process started waiting for the
         * lock). Since getting the current time newly can cause overhead, we
         * reuse the already-obtained time to avoid that overhead.
         *
         * Note that waitStart is updated without holding the lock table's
         * partition lock, to avoid the overhead by additional lock
         * acquisition. This can cause "waitstart" in pg_locks to become NULL
         * for a very short period of time after the wait started even though
         * "granted" is false. This is OK in practice because we can assume
         * that users are likely to look at "waitstart" when waiting for the
         * lock for a long time.
         */
        pg_atomic_write_u64_impl(
            &raw mut (*MyProc).waitStart,
            get_timeout_start_time(DEADLOCK_TIMEOUT) as u64,
        );
    } else if log_recovery_conflict_waits {
        /*
         * Set the wait start timestamp if logging is enabled and in hot
         * standby.
         */
        standbyWaitStart = GetCurrentTimestamp();
    }

    /*
     * If somebody wakes us between LWLockRelease and WaitLatch, the latch
     * will not wait. But a set latch does not necessarily mean that the lock
     * is free now, as there are many other sources for latch sets than
     * somebody releasing the lock.
     *
     * We process interrupts whenever the latch has been set, so cancel/die
     * interrupts are processed quickly. This means we must not mind losing
     * control to a cancel/die interrupt here.  We don't, because we have no
     * shared-state-change work to do after being granted the lock (the
     * grantor did it all).  We do have to worry about canceling the deadlock
     * timeout and updating the locallock table, but if we lose control to an
     * error, LockErrorCleanup will fix that up.
     */
    let mut loop_wait_status: ProcWaitStatus;
    loop {
        if InHotStandby {
            let maybe_log_conflict: bool = standbyWaitStart != 0 && !logged_recovery_conflict;

            /* Set a timer and wait for that or for the lock to be granted */
            ResolveRecoveryConflictWithLock(core::ptr::read(&raw const (*locallock).tag.lock), maybe_log_conflict);

            /*
             * Emit the log message if the startup process is waiting longer
             * than deadlock_timeout for recovery conflict on lock.
             */
            if maybe_log_conflict {
                let now: TimestampTz = GetCurrentTimestamp();

                if TimestampDifferenceExceeds(standbyWaitStart, now, DeadlockTimeout) {
                    let mut cnt: c_int = 0;
                    let vxids: *mut VirtualTransactionId = GetLockConflicts(
                        &(*locallock).tag.lock,
                        AccessExclusiveLock,
                        &mut cnt,
                    );

                    /*
                     * Log the recovery conflict and the list of PIDs of
                     * backends holding the conflicting lock. Note that we do
                     * logging even if there are no such backends right now
                     * because the startup process here has already waited
                     * longer than deadlock_timeout.
                     */
                    LogRecoveryConflict(
                        PROCSIG_RECOVERY_CONFLICT_LOCK,
                        standbyWaitStart,
                        now,
                        if cnt > 0 { vxids } else { ptr::null() },
                        true,
                    );
                    logged_recovery_conflict = true;
                }
            }
        } else {
            WaitLatch(
                MyLatch as *mut Latch,
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                0,
                PG_WAIT_LOCK_TYPE((*locallock).tag.lock.locktag_type),
            );
            ResetLatch(MyLatch as *mut Latch);
            /* check for deadlocks first, as that's probably log-worthy */
            if got_deadlock_timeout != 0 {
                CheckDeadLock();
                got_deadlock_timeout = 0;
            }
            CHECK_FOR_INTERRUPTS!();
        }

        /*
         * waitStatus could change from PROC_WAIT_STATUS_WAITING to something
         * else asynchronously.  Read it just once per loop to prevent
         * surprising behavior (such as missing log messages).
         */
        loop_wait_status = core::ptr::read_volatile(&(*MyProc).waitStatus);

        /*
         * If we are not deadlocked, but are waiting on an autovacuum-induced
         * task, send a signal to interrupt it.
         */
        if deadlock_state == DS_BLOCKED_BY_AUTOVACUUM && allow_autovacuum_cancel {
            let autovac: *mut PGPROC = GetBlockingAutoVacuumPgproc();
            let statusFlags: u8;
            let lockmethod_copy: u8;
            let locktag_copy: LOCKTAG;

            /*
             * Grab info we need, then release lock immediately.  Note this
             * coding means that there is a tiny chance that the process
             * terminates its current transaction and starts a different one
             * before we have a change to send the signal; the worst possible
             * consequence is that a for-wraparound vacuum is canceled.  But
             * that could happen in any case unless we were to do kill() with
             * the lock held, which is much more undesirable.
             */
            LWLockAcquire(ProcArrayLock as *mut LWLock, LW_EXCLUSIVE);
            statusFlags = (*ProcGlobal).statusFlags.add((*autovac).pgxactoff as usize).read();
            lockmethod_copy = (*lock).tag.locktag_lockmethodid;
            // locktag_copy: copy by reading fields
            locktag_copy = core::ptr::read(&(*lock).tag);
            LWLockRelease(ProcArrayLock as *mut LWLock);

            /*
             * Only do it if the worker is not working to protect against Xid
             * wraparound.
             */
            if (statusFlags & PROC_IS_AUTOVACUUM) != 0
                && (statusFlags & PROC_VACUUM_FOR_WRAPAROUND) == 0
            {
                let pid: c_int = (*autovac).pid;

                /* report the case, if configured to do so */
                if message_level_is_interesting(DEBUG1) {
                    // In C: StringInfoData locktagbuf, logbuf; initStringInfo; DescribeLockTag; appendStringInfo; ereport; pfree.
                    // Stub: just log at DEBUG1 level without full formatting.
                    ereport!(
                        DEBUG1,
                        errmsg!("sending cancel to blocking autovacuum PID {}", pid)
                    );
                }

                /* send the autovacuum worker Back to Old Kent Road */
                if libc_kill(pid, 2 /* SIGINT */) < 0 {
                    /*
                     * There's a race condition here: once we release the
                     * ProcArrayLock, it's possible for the autovac worker to
                     * close up shop and exit before we can do the kill().
                     * Therefore, we do not whinge about no-such-process.
                     * Other errors such as EPERM could conceivably happen if
                     * the kernel recycles the PID fast enough, but such cases
                     * seem improbable enough that it's probably best to issue
                     * a warning if we see some other errno.
                     */
                    // errno != ESRCH check elided; TODO(pg-port): use real errno
                    ereport!(WARNING, errmsg!("could not send signal to process {}", pid));
                }
            }

            /* prevent signal from being sent again more than once */
            allow_autovacuum_cancel = false;
        }

        /*
         * If awoken after the deadlock check interrupt has run, and
         * log_lock_waits is on, then report about the wait.
         */
        if log_lock_waits && deadlock_state != DS_NOT_YET_CHECKED {
            let modename: *const c_char =
                GetLockmodeName((*locallock).tag.lock.locktag_lockmethodid as c_int, lockmode);
            let mut secs: i64 = 0;
            let mut usecs: c_int = 0;
            TimestampDifference(
                get_timeout_start_time(DEADLOCK_TIMEOUT),
                GetCurrentTimestamp(),
                &mut secs,
                &mut usecs,
            );
            let msecs: i64 = secs * 1000 + usecs as i64 / 1000;
            let usecs_rem: c_int = usecs % 1000;

            // Gather holders/waiters: stub -- TODO(pg-port): call GetLockHoldersAndWaiters.

            if deadlock_state == DS_SOFT_DEADLOCK {
                ereport!(
                    LOG,
                    errmsg!(
                        "process {} avoided deadlock for lock after {}.{:03} ms",
                        MyProcPid, msecs, usecs_rem
                    )
                );
            } else if deadlock_state == DS_HARD_DEADLOCK {
                ereport!(
                    LOG,
                    errmsg!(
                        "process {} detected deadlock while waiting for lock after {}.{:03} ms",
                        MyProcPid, msecs, usecs_rem
                    )
                );
            }

            if loop_wait_status == PROC_WAIT_STATUS_WAITING {
                ereport!(
                    LOG,
                    errmsg!(
                        "process {} still waiting for lock after {}.{:03} ms",
                        MyProcPid, msecs, usecs_rem
                    )
                );
            } else if loop_wait_status == PROC_WAIT_STATUS_OK {
                ereport!(
                    LOG,
                    errmsg!(
                        "process {} acquired lock after {}.{:03} ms",
                        MyProcPid, msecs, usecs_rem
                    )
                );
            } else {
                Assert!(loop_wait_status == PROC_WAIT_STATUS_ERROR);
                if deadlock_state != DS_HARD_DEADLOCK {
                    ereport!(
                        LOG,
                        errmsg!(
                            "process {} failed to acquire lock after {}.{:03} ms",
                            MyProcPid, msecs, usecs_rem
                        )
                    );
                }
            }

            /*
             * At this point we might still need to wait for the lock. Reset
             * state so we don't print the above messages again.
             */
            deadlock_state = DS_NO_DEADLOCK;
        }

        if loop_wait_status != PROC_WAIT_STATUS_WAITING {
            break;
        }
    }
    myWaitStatus = loop_wait_status;

    /*
     * Disable the timers, if they are still running.  As in LockErrorCleanup,
     * we must preserve the LOCK_TIMEOUT indicator flag: if a lock timeout has
     * already caused QueryCancelPending to become set, we want the cancel to
     * be reported as a lock timeout, not a user cancel.
     */
    if !InHotStandby {
        if LockTimeout > 0 {
            let timeouts: [DisableTimeoutParams; 2] = [
                DisableTimeoutParams { id: DEADLOCK_TIMEOUT, keep_indicator: false },
                DisableTimeoutParams { id: LOCK_TIMEOUT,     keep_indicator: true  },
            ];
            disable_timeouts(timeouts.as_ptr(), 2);
        } else {
            disable_timeout(DEADLOCK_TIMEOUT, false);
        }
    }

    /*
     * Emit the log message if recovery conflict on lock was resolved but the
     * startup process waited longer than deadlock_timeout for it.
     */
    if InHotStandby && logged_recovery_conflict {
        LogRecoveryConflict(
            PROCSIG_RECOVERY_CONFLICT_LOCK,
            standbyWaitStart,
            GetCurrentTimestamp(),
            ptr::null(),
            false,
        );
    }

    /*
     * We don't have to do anything else, because the awaker did all the
     * necessary updates of the lock table and MyProc. (The caller is
     * responsible for updating the local lock table.)
     */
    myWaitStatus
}

/// TODO(pg-port): access/transam/xlog.c -- RecoveryInProgress
unsafe fn RecoveryInProgress() -> bool {
    false // TODO(pg-port): access/transam/xlog.c
}

// =============================================================================
// ProcWakeup
// =============================================================================

/// ProcWakeup -- wake up a process by setting its latch.
///
///  Also remove the process from the wait queue and set its links invalid.
///
/// The appropriate lock partition lock must be held by caller.
///
/// XXX: presently, this code is only used for the "success" case, and only
/// works correctly for that case.  To clean up in failure case, would need
/// to twiddle the lock's request counts too --- see RemoveFromWaitQueue.
/// Hence, in practice the waitStatus parameter must be PROC_WAIT_STATUS_OK.
pub unsafe fn ProcWakeup(proc_: *mut PGPROC, waitStatus: ProcWaitStatus) {
    if dlist_node_is_detached(&(*proc_).links) {
        return;
    }

    Assert!((*proc_).waitStatus == PROC_WAIT_STATUS_WAITING);

    /* Remove process from wait queue */
    dclist_delete_from_thoroughly(&mut (*(*proc_).waitLock).waitProcs, &mut (*proc_).links);

    /* Clean up process' state and pass it the ok/fail signal */
    (*proc_).waitLock = ptr::null_mut();
    (*proc_).waitProcLock = ptr::null_mut();
    (*proc_).waitStatus = waitStatus;
    pg_atomic_write_u64_impl(&raw mut (*proc_).waitStart, 0);

    /* And awaken it */
    SetLatch(&mut (*proc_).procLatch);
}

// =============================================================================
// ProcLockWakeup
// =============================================================================

/// ProcLockWakeup -- routine for waking up processes when a lock is
///     released (or a prior waiter is aborted).  Scan all waiters
///     for lock, waken any that are no longer blocked.
///
/// The appropriate lock partition lock must be held by caller.
pub unsafe fn ProcLockWakeup(lockMethodTable: LockMethod, lock: *mut LOCK) {
    let waitQueue: *mut dclist_head = &mut (*lock).waitProcs;
    let mut aheadRequests: LOCKMASK = 0;

    if dclist_is_empty(waitQueue) {
        return;
    }

    // dclist_foreach_modify: iterate with deletion allowed
    let head_ptr: *mut dlist_head = waitQueue as *mut _ as *mut dlist_head;
    let mut cur = (*head_ptr).head.next;
    loop {
        // sentinel check: cur points back to &head_ptr->head when exhausted
        if cur as *const _ as usize == &(*head_ptr).head as *const _ as usize {
            break;
        }
        let next = (*cur).next; // save before possible deletion
        let proc_: *mut PGPROC = (cur as *mut u8)
            .sub(core::mem::offset_of!(PGPROC, links)) as *mut PGPROC;
        let lockmode: LOCKMODE = (*proc_).waitLockMode;

        /*
         * Waken if (a) doesn't conflict with requests of earlier waiters, and
         * (b) doesn't conflict with already-held locks.
         */
        if ((*(*lockMethodTable).conflictTab.add(lockmode as usize)) & aheadRequests) == 0
            && !LockCheckConflicts(lockMethodTable, lockmode, lock, (*proc_).waitProcLock)
        {
            /* OK to waken */
            GrantLock(lock, (*proc_).waitProcLock, lockmode);
            /* removes proc from the lock's waiting process queue */
            ProcWakeup(proc_, PROC_WAIT_STATUS_OK);
        } else {
            /*
             * Lock conflicts: Don't wake, but remember requested mode for
             * later checks.
             */
            aheadRequests |= LOCKBIT_ON(lockmode);
        }
        cur = next;
    }
}

// =============================================================================
// CheckDeadLock (internal)
// =============================================================================

/// CheckDeadLock
///
/// We only get to this routine, if DEADLOCK_TIMEOUT fired while waiting for a
/// lock to be released by some other process.  Check if there's a deadlock; if
/// not, just return.  (But signal ProcSleep to log a message, if
/// log_lock_waits is true.)  If we have a real deadlock, remove ourselves from
/// the lock's wait queue and signal an error to ProcSleep.
unsafe fn CheckDeadLock() {
    let mut i: usize;

    /*
     * Acquire exclusive lock on the entire shared lock data structures. Must
     * grab LWLocks in partition-number order to avoid LWLock deadlock.
     *
     * Note that the deadlock check interrupt had better not be enabled
     * anywhere that this process itself holds lock partition locks, else this
     * will wait forever.  Also note that LWLockAcquire creates a critical
     * section, so that this routine cannot be interrupted by cancel/die
     * interrupts.
     */
    i = 0;
    while i < NUM_LOCK_PARTITIONS {
        LWLockAcquire(LockHashPartitionLockByIndex(i), LW_EXCLUSIVE);
        i += 1;
    }

    /*
     * Check to see if we've been awoken by anyone in the interim.
     *
     * If we have, we can return and resume our transaction -- happy day.
     * Before we are awoken the process releasing the lock grants it to us so
     * we know that we don't have to wait anymore.
     *
     * We check by looking to see if we've been unlinked from the wait queue.
     * This is safe because we hold the lock partition lock.
     */
    if (*MyProc).links.prev.is_null() || (*MyProc).links.next.is_null() {
        // goto check_done
        i = NUM_LOCK_PARTITIONS;
        while i > 0 {
            i -= 1;
            LWLockRelease(LockHashPartitionLockByIndex(i));
        }
        return;
    }

    /* Run the deadlock check, and set deadlock_state for use by ProcSleep */
    deadlock_state = DeadLockCheck(MyProc);

    if deadlock_state == DS_HARD_DEADLOCK {
        /*
         * Oops.  We have a deadlock.
         *
         * Get this process out of wait state. (Note: we could do this more
         * efficiently by relying on lockAwaited, but use this coding to
         * preserve the flexibility to kill some other transaction than the
         * one detecting the deadlock.)
         *
         * RemoveFromWaitQueue sets MyProc->waitStatus to
         * PROC_WAIT_STATUS_ERROR, so ProcSleep will report an error after we
         * return from the signal handler.
         */
        Assert!(!(*MyProc).waitLock.is_null());
        RemoveFromWaitQueue(
            MyProc,
            LockTagHashCode(&(*(*MyProc).waitLock).tag),
        );

        /*
         * We're done here.  Transaction abort caused by the error that
         * ProcSleep will raise will cause any other locks we hold to be
         * released, thus allowing other processes to wake up; we don't need
         * to do that here.  NOTE: an exception is that releasing locks we
         * hold doesn't consider the possibility of waiters that were blocked
         * behind us on the lock we just failed to get, and might now be
         * wakable because we're not in front of them anymore.  However,
         * RemoveFromWaitQueue took care of waking up any such processes.
         */
    }

    /*
     * And release locks.  We do this in reverse order for two reasons: (1)
     * Anyone else who needs more than one of the locks will be trying to lock
     * them in increasing order; we don't want to release the other process
     * until it can get all the locks it needs. (2) This avoids O(N^2)
     * behavior inside LWLockRelease.
     */
    // check_done:
    i = NUM_LOCK_PARTITIONS;
    while i > 0 {
        i -= 1;
        LWLockRelease(LockHashPartitionLockByIndex(i));
    }
}

// =============================================================================
// CheckDeadLockAlert
// =============================================================================

/// CheckDeadLockAlert - Handle the expiry of deadlock_timeout.
///
/// NB: Runs inside a signal handler, be careful.
pub unsafe fn CheckDeadLockAlert() {
    // save_errno / restore_errno elided (Rust signal handlers are generally
    // unsafe; errno management TODO(pg-port)).
    got_deadlock_timeout = 1;

    /*
     * Have to set the latch again, even if handle_sig_alarm already did. Back
     * then got_deadlock_timeout wasn't yet set... It's unlikely that this
     * ever would be a problem, but setting a set latch again is cheap.
     *
     * Note that, when this function runs inside procsignal_sigusr1_handler(),
     * the handler function sets the latch again after the latch is set here.
     */
    SetLatch(MyLatch as *mut Latch);
}

// =============================================================================
// GetLockHoldersAndWaiters
// =============================================================================

/// GetLockHoldersAndWaiters - get lock holders and waiters for a lock.
///
/// Fill lock_holders_sbuf and lock_waiters_sbuf with the PIDs of processes
/// holding and waiting for the lock, and set lockHoldersNum to the number of
/// lock holders.
///
/// The lock table's partition lock must be held on entry and remains held on
/// exit.
pub unsafe fn GetLockHoldersAndWaiters(
    locallock: *mut LOCALLOCK,
    lock_holders_sbuf: *mut StringInfoData,
    lock_waiters_sbuf: *mut StringInfoData,
    lockHoldersNum: *mut c_int,
) {
    let lock: *mut LOCK = (*locallock).lock;
    let mut first_holder: bool = true;
    let mut first_waiter: bool = true;

    *lockHoldersNum = 0;

    /*
     * Loop over the lock's procLocks to gather a list of all holders and
     * waiters. Thus we will be able to provide more detailed information for
     * lock debugging purposes.
     *
     * lock->procLocks contains all processes which hold or wait for this
     * lock.
     */
    // dlist_foreach over lock->procLocks
    let head: *const dlist_head = &(*lock).procLocks;
    let mut cur = (*head).head.next;
    loop {
        if cur as usize == head as usize {
            break;
        }
        let curproclock: *mut PROCLOCK = (cur as *mut u8)
            .sub(core::mem::offset_of!(PROCLOCK, lockLink)) as *mut PROCLOCK;

        /*
         * We are a waiter if myProc->waitProcLock == curproclock; we are a
         * holder if it is NULL or something different.
         */
        if (*(*curproclock).tag.myProc).waitProcLock == curproclock {
            if first_waiter {
                appendStringInfo(
                    lock_waiters_sbuf,
                    b"%d\0".as_ptr() as *const c_char,
                    (*(*curproclock).tag.myProc).pid,
                );
                first_waiter = false;
            } else {
                appendStringInfo(
                    lock_waiters_sbuf,
                    b", %d\0".as_ptr() as *const c_char,
                    (*(*curproclock).tag.myProc).pid,
                );
            }
        } else {
            if first_holder {
                appendStringInfo(
                    lock_holders_sbuf,
                    b"%d\0".as_ptr() as *const c_char,
                    (*(*curproclock).tag.myProc).pid,
                );
                first_holder = false;
            } else {
                appendStringInfo(
                    lock_holders_sbuf,
                    b", %d\0".as_ptr() as *const c_char,
                    (*(*curproclock).tag.myProc).pid,
                );
            }
            *lockHoldersNum += 1;
        }
        cur = (*cur).next;
    }
}

// =============================================================================
// ProcWaitForSignal
// =============================================================================

/// ProcWaitForSignal - wait for a signal from another backend.
///
/// As this uses the generic process latch the caller has to be robust against
/// unrelated wakeups: Always check that the desired state has occurred, and
/// wait again if not.
pub unsafe fn ProcWaitForSignal(wait_event_info: uint32) {
    WaitLatch(
        MyLatch as *mut Latch,
        WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
        0,
        wait_event_info,
    );
    ResetLatch(MyLatch as *mut Latch);
    CHECK_FOR_INTERRUPTS!();
}

// =============================================================================
// ProcSendSignal
// =============================================================================

/// ProcSendSignal - set the latch of a backend identified by ProcNumber.
pub unsafe fn ProcSendSignal(procNumber: ProcNumber) {
    if procNumber < 0 || procNumber as uint32 >= (*ProcGlobal).allProcCount {
        elog!(ERROR, "procNumber out of range");
    }

    SetLatch(&mut (*(*ProcGlobal).allProcs.add(procNumber as usize)).procLatch);
}

// =============================================================================
// BecomeLockGroupLeader
// =============================================================================

/// BecomeLockGroupLeader - designate process as lock group leader.
///
/// Once this function has returned, other processes can join the lock group
/// by calling BecomeLockGroupMember.
pub unsafe fn BecomeLockGroupLeader() {
    let leader_lwlock: *mut LWLock;

    /* If we already did it, we don't need to do it again. */
    if (*MyProc).lockGroupLeader == MyProc {
        return;
    }

    /* We had better not be a follower. */
    Assert!((*MyProc).lockGroupLeader.is_null());

    /* Create single-member group, containing only ourselves. */
    leader_lwlock = LockHashPartitionLockByProc(MyProc);
    LWLockAcquire(leader_lwlock, LW_EXCLUSIVE);
    (*MyProc).lockGroupLeader = MyProc;
    dlist_push_head(&mut (*MyProc).lockGroupMembers, &mut (*MyProc).lockGroupLink);
    LWLockRelease(leader_lwlock);
}

// =============================================================================
// BecomeLockGroupMember
// =============================================================================

/// BecomeLockGroupMember - designate process as lock group member.
///
/// This is pretty straightforward except for the possibility that the leader
/// whose group we're trying to join might exit before we manage to do so;
/// and the PGPROC might get recycled for an unrelated process.  To avoid
/// that, we require the caller to pass the PID of the intended PGPROC as
/// an interlock.  Returns true if we successfully join the intended lock
/// group, and false if not.
pub unsafe fn BecomeLockGroupMember(leader: *mut PGPROC, pid: c_int) -> bool {
    let leader_lwlock: *mut LWLock;
    let mut ok: bool = false;

    /* Group leader can't become member of group */
    Assert!(MyProc != leader);

    /* Can't already be a member of a group */
    Assert!((*MyProc).lockGroupLeader.is_null());

    /* PID must be valid. */
    Assert!(pid != 0);

    /*
     * Get lock protecting the group fields.  Note LockHashPartitionLockByProc
     * calculates the proc number based on the PGPROC slot without looking at
     * its contents, so we will acquire the correct lock even if the leader
     * PGPROC is in process of being recycled.
     */
    leader_lwlock = LockHashPartitionLockByProc(leader);
    LWLockAcquire(leader_lwlock, LW_EXCLUSIVE);

    /* Is this the leader we're looking for? */
    if (*leader).pid == pid && (*leader).lockGroupLeader == leader {
        /* OK, join the group */
        ok = true;
        (*MyProc).lockGroupLeader = leader;
        dlist_push_tail(&mut (*leader).lockGroupMembers, &mut (*MyProc).lockGroupLink);
    }
    LWLockRelease(leader_lwlock);

    ok
}

// =============================================================================
// dclist_head helper -- access inner dlist_head sentinel (for dclist iteration).
// dclist_head in ilist.rs has a `dlist_head` field named `dlist_head`.
// We access it via a transparent cast; proc.c uses dclist_foreach / dclist_foreach_modify.
// =============================================================================

// NOTE: The PGPROC.links field is a plain dlist_node, NOT a dclist_node, so
// dclist functions on lock->waitProcs take &proc.links directly.
// The dclist_head contains an embedded dlist_head; we reach the sentinel via
// offset_of! on dclist_head.  This is already handled above by casting
// waitQueue to *mut dlist_head and iterating the sentinel.  The offset is 0
// because dclist_head's first field is the dlist_head.  TODO(pg-port): verify
// layout matches ilist.rs dclist_head definition.
