/*-------------------------------------------------------------------------
 *
 * procarray.rs
 *   POSTGRES process array code.
 *
 * This module maintains arrays of PGPROC substructures, as well as associated
 * arrays in ProcGlobal, for all active backends.  Although there are several
 * uses for this, the principal one is as a means of determining the set of
 * currently running transactions.
 *
 * Because of various subtle race conditions it is critical that a backend
 * hold the correct locks while setting or clearing its xid (in
 * ProcGlobal->xids[]/MyProc->xid).  See notes in
 * src/backend/access/transam/README.
 *
 * The process arrays now also include structures representing prepared
 * transactions.  The xid and subxids fields of these are valid, as are the
 * myProcLocks lists.  They can be distinguished from regular backend PGPROCs
 * at need by checking for pid == 0.
 *
 * During hot standby, we also keep a list of XIDs representing transactions
 * that are known to be running on the primary (or more precisely, were running
 * as of the current point in the WAL stream).  This list is kept in the
 * KnownAssignedXids array, and is updated by watching the sequence of
 * arriving XIDs.  This is necessary because if we leave those XIDs out of
 * snapshots taken for standby queries, then they will appear to be already
 * complete, leading to MVCC failures.  Note that in hot standby, the PGPROC
 * array represents standby processes, which by definition are not running
 * transactions that have XIDs.
 *
 * It is perhaps possible for a backend on the primary to terminate without
 * writing an abort record for its transaction.  While that shouldn't really
 * happen, it would tie up KnownAssignedXids indefinitely, so we protect
 * ourselves by pruning the array when a valid list of running XIDs arrives.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/storage/ipc/procarray.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use core::ffi::c_void;

// ---------------------------------------------------------------------------
// Imports from real ported modules
// ---------------------------------------------------------------------------
use crate::c::{
    uint8, uint32, uint64, int8, int32, int64,
    TransactionId, LocalTransactionId, SubTransactionId,
    Size,
};
use std::ffi::c_int;
use crate::postgres_ext::{Oid, InvalidOid};
use crate::access::transam::{InvalidTransactionId};
use crate::access::transam::transam::{TransactionIdPrecedes, TransactionIdPrecedesOrEquals, TransactionIdFollows, TransactionIdFollowsOrEquals};
// TODO(pg-port): InvalidSubTransactionId lives in c.h
const InvalidSubTransactionId: SubTransactionId = 0;
unsafe fn OidIsValid(o: Oid) -> bool { o != InvalidOid }
use crate::access::transam::{
    FullTransactionId, FullTransactionIdFromU64, FullTransactionIdFromEpochAndXid,
    U64FromFullTransactionId, XidFromFullTransactionId,
    InvalidFullTransactionId, FirstNormalFullTransactionId,
    FullTransactionIdIsValid, FullTransactionIdPrecedes,
    FullTransactionIdPrecedesOrEquals, FullTransactionIdFollows,
    FullTransactionIdFollowsOrEquals, FullTransactionIdAdvance, FullTransactionIdRetreat,
    TransactionIdIsValid, TransactionIdIsNormal, TransactionIdEquals,

    TransactionIdAdvance, TransactionIdRetreat,
    FirstNormalTransactionId,
};

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported symbols
// ---------------------------------------------------------------------------

// TODO(pg-port): real LWLock lives in storage/lmgr/lwlock.c
pub struct LWLock;
pub const LW_SHARED: c_int = 0;
pub const LW_EXCLUSIVE: c_int = 1;
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool { unimplemented!() }
unsafe fn LWLockRelease(_lock: *mut LWLock) { unimplemented!() }
unsafe fn LWLockConditionalAcquire(_lock: *mut LWLock, _mode: c_int) -> bool { unimplemented!() }
unsafe fn LWLockHeldByMe(_lock: *mut LWLock) -> bool { unimplemented!() }
unsafe fn LWLockHeldByMeInMode(_lock: *mut LWLock, _mode: c_int) -> bool { unimplemented!() }

// TODO(pg-port): real lock handles live in storage/lmgr/lwlocklist.h
unsafe fn ProcArrayLock() -> *mut LWLock { unimplemented!() }
unsafe fn XidGenLock() -> *mut LWLock { unimplemented!() }

// TODO(pg-port): real PGPROC/PROC_HDR live in storage/proc.h
pub const PGPROC_MAX_CACHED_SUBXIDS: c_int = 64;

#[repr(C)]
pub struct VXidCache {
    pub lxid: LocalTransactionId,
    pub procNumber: c_int,
}

#[repr(C)]
pub struct XidCacheStatus {
    pub count: uint8,
    pub overflowed: bool,
}

#[repr(C)]
pub struct SubXidCache {
    pub xids: [TransactionId; PGPROC_MAX_CACHED_SUBXIDS as usize],
}

pub type PGSemaphore = *mut c_void;

// pg_atomic_uint32 stub -- TODO(pg-port): real type lives in port/atomics.h
#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: uint32,
}

unsafe fn pg_atomic_read_u32(p: *const pg_atomic_uint32) -> uint32 {
    core::ptr::read_volatile(&(*p).value)
}
unsafe fn pg_atomic_write_u32(p: *mut pg_atomic_uint32, v: uint32) {
    core::ptr::write_volatile(&mut (*p).value, v)
}
unsafe fn pg_atomic_exchange_u32(p: *mut pg_atomic_uint32, v: uint32) -> uint32 {
    let old = core::ptr::read_volatile(&(*p).value);
    core::ptr::write_volatile(&mut (*p).value, v);
    old
}
unsafe fn pg_atomic_compare_exchange_u32(p: *mut pg_atomic_uint32, expected: *mut uint32, desired: uint32) -> bool {
    let cur = core::ptr::read_volatile(&(*p).value);
    if cur == *expected {
        core::ptr::write_volatile(&mut (*p).value, desired);
        true
    } else {
        *expected = cur;
        false
    }
}
unsafe fn pg_read_barrier() { core::sync::atomic::fence(core::sync::atomic::Ordering::Acquire); }
unsafe fn pg_write_barrier() { core::sync::atomic::fence(core::sync::atomic::Ordering::Release); }

/// PGPROC -- per-backend process descriptor.
/// TODO(pg-port): real PGPROC lives in storage/proc.h; this is a faithful-enough stub.
#[repr(C)]
pub struct PGPROC {
    /// transaction id, or InvalidTransactionId if none
    pub xid: TransactionId,
    /// subtransaction-XID cache status
    pub subxidStatus: XidCacheStatus,
    /// cached subtransaction XIDs
    pub subxids: SubXidCache,
    /// OS process id of backend, or 0 if prepared xact
    pub pid: c_int,
    /// virtual transaction id
    pub vxid: VXidCache,
    /// lowest Xid of snapshot (transaction's xmin)
    pub xmin: TransactionId,
    /// OID of database this backend is using
    pub databaseId: Oid,
    /// OID of role used to log in
    pub roleId: Oid,
    /// index into ProcGlobal arrays
    pub pgxactoff: c_int,
    /// flags for various states (PROC_* below)
    pub statusFlags: uint8,
    /// checkpoint delay flags
    pub delayChkptFlags: c_int,
    /// recovery conflict pending?
    pub recoveryConflictPending: bool,
    /// true if regular backend (not worker/aux)
    pub isRegularBackend: bool,
    /// semaphore to sleep on
    pub sem: PGSemaphore,
    /// group-clear linked list
    pub procArrayGroupMember: bool,
    pub procArrayGroupMemberXid: TransactionId,
    pub procArrayGroupNext: pg_atomic_uint32,
    /// lock this process is waiting for, or NULL
    pub waitLock: *mut c_void,
    // ... TODO(pg-port): storage/proc.h has many more fields
}

/// PROC_HDR -- global process table descriptor.
/// TODO(pg-port): real PROC_HDR lives in storage/proc.h.
#[repr(C)]
pub struct PROC_HDR {
    pub allProcs: *mut PGPROC,
    pub allProcCount: uint32,
    pub xids: *mut TransactionId,
    pub subxidStates: *mut XidCacheStatus,
    pub statusFlags: *mut uint8,
    pub procArrayGroupFirst: pg_atomic_uint32,
    // ... TODO(pg-port): storage/proc.h
}

pub static mut ProcGlobal: *mut PROC_HDR = core::ptr::null_mut(); // TODO(pg-port): real ProcGlobal lives in storage/lmgr/proc.c
pub static mut MyProc: *mut PGPROC = core::ptr::null_mut();       // TODO(pg-port): real MyProc lives in storage/lmgr/proc.c

// PROC status flags -- TODO(pg-port): real values in storage/proc.h
pub const PROC_IN_VACUUM: uint8               = 0x01;
pub const PROC_IN_LOGICAL_DECODING: uint8     = 0x02;
pub const PROC_VACUUM_STATE_MASK: uint8       = 0x03;
pub const PROC_IS_AUTOVACUUM: uint8           = 0x04;
pub const PROC_AFFECTS_ALL_HORIZONS: uint8    = 0x08;
pub const PROC_XMIN_FLAGS: uint8              = 0x10;

unsafe fn GetPGProcByNumber(n: c_int) -> *mut PGPROC {
    (*ProcGlobal).allProcs.add(n as usize)
}
unsafe fn GetNumberFromPGProc(proc_: *const PGPROC) -> c_int {
    proc_.offset_from((*ProcGlobal).allProcs) as c_int
}
pub const NUM_AUXILIARY_PROCS: c_int = 5; // TODO(pg-port): real value in storage/proc.h

// ProcNumber types -- TODO(pg-port): real defs in storage/procnumber.h
pub type ProcNumber = c_int;
pub static mut MyProcNumber: c_int = 0;
pub use crate::storage::procnumber::INVALID_PROC_NUMBER;

// PGSemaphore -- TODO(pg-port): real type in storage/pg_sema.h
unsafe fn PGSemaphoreLock(_sema: PGSemaphore) { unimplemented!() }
unsafe fn PGSemaphoreUnlock(_sema: PGSemaphore) { unimplemented!() }

// TransamVariablesData -- TODO(pg-port): real type in access/transam/varsup.c
#[repr(C)]
pub struct TransamVariablesData {
    pub nextXid: FullTransactionId,
    pub oldestXid: TransactionId,
    pub latestCompletedXid: FullTransactionId,
    pub xactCompletionCount: uint64,
    // ... TODO(pg-port): access/transam.h
}
pub static mut TransamVariables: *mut TransamVariablesData = core::ptr::null_mut(); // TODO(pg-port): real TransamVariables lives in access/transam/varsup.c

// Relation type stub -- TODO(pg-port): real type in utils/rel.h
pub use crate::utils::rel::{Relation, RelationData};
// relkind constants -- TODO(pg-port): real values in catalog/pg_class.h
pub const RELKIND_RELATION: u8 = b'r';
pub const RELKIND_MATVIEW: u8  = b'm';
pub const RELKIND_TOASTVALUE: u8 = b't';
#[repr(C)]
pub struct FormData_pg_class {
    pub relkind: u8,
    pub relisshared: bool,
}
unsafe fn IsCatalogRelation(_rel: Relation) -> bool { unimplemented!() } // TODO(pg-port): real fn in catalog/catalog.c
unsafe fn RelationIsAccessibleInLogicalDecoding(_rel: Relation) -> bool { unimplemented!() } // TODO(pg-port): real fn in utils/rel.h
unsafe fn RELATION_IS_LOCAL(_rel: Relation) -> bool { unimplemented!() } // TODO(pg-port): real macro in utils/rel.h

// Snapshot types -- real type in utils/snapshot.rs
pub use crate::utils::snapshot::{SnapshotData, Snapshot};

// VirtualTransactionId -- TODO(pg-port): real type in storage/lock.h
#[repr(C)]
#[derive(Copy, Clone)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: LocalTransactionId,
}
pub const InvalidLocalTransactionId: LocalTransactionId = 0;
unsafe fn VirtualTransactionIdIsValid(vxid: VirtualTransactionId) -> bool {
    vxid.localTransactionId != InvalidLocalTransactionId
}
unsafe fn VirtualTransactionIdEquals(a: VirtualTransactionId, b: VirtualTransactionId) -> bool {
    a.procNumber == b.procNumber && a.localTransactionId == b.localTransactionId
}
unsafe fn GET_VXID_FROM_PGPROC(vxid: &mut VirtualTransactionId, proc_: &PGPROC) {
    vxid.procNumber = proc_.vxid.procNumber;
    vxid.localTransactionId = proc_.vxid.lxid;
}

// RunningTransactions -- TODO(pg-port): real type in access/xact.h / storage/standbydefs.h
#[repr(C)]
pub enum SubxidStatus {
    SUBXIDS_IN_ARRAY    = 0,
    SUBXIDS_IN_SUBTRANS = 1,
    SUBXIDS_MISSING     = 2,
}
pub use SubxidStatus::*;

#[repr(C)]
pub struct RunningTransactionsData {
    pub xcnt: c_int,
    pub subxcnt: c_int,
    pub subxid_status: SubxidStatus,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub oldestDatabaseRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
    pub xids: *mut TransactionId,
}
pub type RunningTransactions = *mut RunningTransactionsData;

// Standby state -- real type in access/transam/xlogutils.rs
pub type HotStandbyState = c_int;
pub const STANDBY_DISABLED: HotStandbyState      = 0;
pub const STANDBY_INITIALIZED: HotStandbyState   = 1;
pub const STANDBY_SNAPSHOT_PENDING: HotStandbyState = 2;
pub const STANDBY_SNAPSHOT_READY: HotStandbyState = 3;
pub static mut standbyState: HotStandbyState = STANDBY_DISABLED; // TODO(pg-port): real standbyState in access/transam/xlogutils.c

// pg_lfind32 -- TODO(pg-port): real fn in port/pg_lfind.h
unsafe fn pg_lfind32(needle: uint32, haystack: *const uint32, n: c_int) -> bool {
    for i in 0..n as usize {
        if *haystack.add(i) == needle {
            return true;
        }
    }
    false
}

// Misc stubs
unsafe fn RecoveryInProgress() -> bool { unimplemented!() } // TODO(pg-port): real fn in access/transam/xlogutils.c
unsafe fn AmStartupProcess() -> bool { unimplemented!() }   // TODO(pg-port): real fn in miscadmin.c
pub static mut IsUnderPostmaster: bool = false;             // TODO(pg-port): real global in utils/misc/injection_point.c
unsafe fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool { unimplemented!() } // TODO(pg-port): real fn in access/transam/xact.c
unsafe fn IsBootstrapProcessingMode() -> bool { unimplemented!() }         // TODO(pg-port): real fn in utils/init/postinit.c
unsafe fn FullTransactionIdIsNormal(fxid: FullTransactionId) -> bool {
    FullTransactionIdFollowsOrEquals(fxid, FirstNormalFullTransactionId)
}
unsafe fn AssertTransactionIdInAllowableRange(_xid: TransactionId) {}     // TODO(pg-port): real fn in access/transam/varsup.c

// TransactionId comparison helpers
pub fn NormalTransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    // TODO(pg-port): real NormalTransactionIdPrecedes lives in access/transam.h
    // Wraparound-aware compare: (int32)(id1 - id2) < 0
    (id1 as i32).wrapping_sub(id2 as i32) < 0
}
pub fn TransactionIdOlder(a: TransactionId, b: TransactionId) -> TransactionId {
    if !TransactionIdIsValid(a) { return b; }
    if !TransactionIdIsValid(b) { return a; }
    if TransactionIdPrecedes(a, b) { a } else { b }
}
pub fn FullTransactionIdNewer(a: FullTransactionId, b: FullTransactionId) -> FullTransactionId {
    if FullTransactionIdFollows(a, b) { a } else { b }
}

// UINT32_ACCESS_ONCE: volatile read of a uint32
#[inline]
unsafe fn UINT32_ACCESS_ONCE(var: TransactionId) -> uint32 {
    core::ptr::read_volatile(&var as *const uint32)
}

// Shmem allocation -- TODO(pg-port): real fn in storage/ipc/shmem.c
unsafe fn ShmemInitStruct(_name: *const i8, _size: Size, _found: *mut bool) -> *mut c_void { unimplemented!() }
unsafe fn add_size(s1: Size, s2: Size) -> Size { s1 + s2 }
unsafe fn mul_size(s1: Size, s2: usize) -> Size { s1 * s2 }

// Memory management -- TODO(pg-port): real fns in utils/mmgr
unsafe fn palloc(size: Size) -> *mut c_void { unimplemented!() }
unsafe fn pfree(ptr: *mut c_void) { unimplemented!() }
unsafe fn palloc_extended(size: Size, flags: c_int) -> *mut c_void { unimplemented!() }

// List -- TODO(pg-port): real type in nodes/pg_list.h
pub struct List;
pub const NIL: *mut List = core::ptr::null_mut();
unsafe fn lappend_int(_list: *mut List, _datum: c_int) -> *mut List { unimplemented!() }
pub struct ListCell;
unsafe fn list_head(_list: *mut List) -> *mut ListCell { unimplemented!() }
unsafe fn lnext(_list: *mut List, _lc: *mut ListCell) -> *mut ListCell { unimplemented!() }
unsafe fn lfirst_int(_lc: *mut ListCell) -> c_int { unimplemented!() }
unsafe fn list_free(_list: *mut List) { unimplemented!() }

// Globals from miscadmin / auth
pub static mut MaxBackends: c_int = 0;            // TODO(pg-port): real value in storage/proc.h
pub static mut max_prepared_xacts: c_int = 0;     // TODO(pg-port): real value in access/twophase.c
pub static mut EnableHotStandby: bool = false;    // TODO(pg-port): real value in access/xlogdefs.h

pub static mut RecentXmin: TransactionId = InvalidTransactionId;     // TODO(pg-port): real global in utils/snapmgr.c
pub static mut TransactionXmin: TransactionId = InvalidTransactionId; // TODO(pg-port): real global in utils/snapmgr.c
pub static mut MyDatabaseId: Oid = InvalidOid;                        // TODO(pg-port): real global in utils/adt/acl.c
unsafe fn GetUserId() -> Oid { unimplemented!() }                    // TODO(pg-port): real fn in utils/adt/acl.c
unsafe fn GetCurrentCommandId(_increment: bool) -> uint32 { unimplemented!() } // TODO(pg-port): real fn in access/transam/xact.c
unsafe fn superuser_arg(_roleId: Oid) -> bool { unimplemented!() }              // TODO(pg-port): real fn in utils/adt/acl.c
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool { unimplemented!() } // TODO(pg-port): real fn in utils/adt/acl.c
pub const ROLE_PG_SIGNAL_BACKEND: Oid = 0;                                      // TODO(pg-port): real value in catalog/pg_authid.h
unsafe fn get_database_name(_db: Oid) -> *const i8 { unimplemented!() }        // TODO(pg-port): real fn in commands/dbcommands.c

// ProcSignal -- TODO(pg-port): real type in storage/ipc/procsignal.h
pub type ProcSignalReason = c_int;
unsafe fn SendProcSignal(_pid: c_int, _reason: ProcSignalReason, _procNumber: ProcNumber) -> c_int { unimplemented!() }

// subtrans -- TODO(pg-port): real fns in access/transam/subtrans.c
unsafe fn SubTransGetTopmostTransaction(_xid: TransactionId) -> TransactionId { unimplemented!() }
unsafe fn SubTransSetParent(_xid: TransactionId, _parent: TransactionId) { unimplemented!() }
unsafe fn ExtendSUBTRANS(_nextXid: TransactionId) { unimplemented!() }

// twophase -- TODO(pg-port): real fn in access/transam/twophase.c
unsafe fn StandbyTransactionIdIsPrepared(_xid: TransactionId) -> bool { unimplemented!() }

// varsup -- TODO(pg-port): real fn in access/transam/varsup.c
unsafe fn AdvanceNextFullTransactionIdPastXid(_xid: TransactionId) { unimplemented!() }

// standby/locks -- TODO(pg-port): real fn in access/transam/standby.c
unsafe fn StandbyReleaseOldLocks(_oldestXid: TransactionId) { unimplemented!() }

// clog -- TODO(pg-port): real fns in access/transam/clog.c
unsafe fn TransactionIdDidCommit(_xid: TransactionId) -> bool { unimplemented!() }
unsafe fn TransactionIdDidAbort(_xid: TransactionId) -> bool { unimplemented!() }

// xidLogicalComparator -- TODO(pg-port): real fn in utils/adt/xid.c / transam.c
unsafe fn xidLogicalComparator(a: *const c_void, b: *const c_void) -> c_int {
    let xa = *(a as *const TransactionId);
    let xb = *(b as *const TransactionId);
    if TransactionIdPrecedes(xa, xb) { -1 }
    else if TransactionIdPrecedes(xb, xa) { 1 }
    else { 0 }
}

// TransactionIdLatest -- TODO(pg-port): real fn in access/transam/transam.c
unsafe fn TransactionIdLatest(main: TransactionId, nxids: c_int, xids: *const TransactionId) -> TransactionId {
    let mut latest = main;
    for i in 0..nxids as usize {
        let x = *xids.add(i);
        if TransactionIdPrecedes(latest, x) { latest = x; }
    }
    latest
}

// qsort wrapper
unsafe fn qsort(base: *mut c_void, nmemb: usize, size: usize,
                 cmp: unsafe fn(*const c_void, *const c_void) -> c_int) {
    // TODO(pg-port): use libc qsort; stub defers
    unimplemented!()
}

// malloc/free wrappers
unsafe fn malloc(size: usize) -> *mut c_void { unimplemented!() }
unsafe fn free(ptr: *mut c_void) { unimplemented!() }

// kill syscall
unsafe fn kill(_pid: c_int, _sig: c_int) -> c_int { unimplemented!() }
pub const SIGTERM: c_int = 15;

// pg_usleep
unsafe fn pg_usleep(_usec: i64) { unimplemented!() }

// Timestamp helpers
pub type TimestampTz = int64;
unsafe fn GetCurrentTimestamp() -> TimestampTz { unimplemented!() }
unsafe fn TimestampTzPlusMilliseconds(ts: TimestampTz, ms: i64) -> TimestampTz { ts + ms * 1000 }

// StringInfo
pub struct StringInfoData { pub data: *mut i8 }
unsafe fn initStringInfo(_buf: *mut StringInfoData) { unimplemented!() }
unsafe fn appendStringInfo(_buf: *mut StringInfoData, _fmt: *const i8, _val: u32) { unimplemented!() }

// ereport / elog helpers (use crate macros)
use crate::elog;
use crate::ereport;

// CHECK_FOR_INTERRUPTS
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

// ---------------------------------------------------------------------------
// Macros from procarray.h merged in
// ---------------------------------------------------------------------------

/// PROCARRAY_MAXPROCS = MaxBackends + max_prepared_xacts (evaluated at runtime)
#[inline]
pub unsafe fn PROCARRAY_MAXPROCS() -> c_int {
    MaxBackends + max_prepared_xacts
}

/// TOTAL_MAX_CACHED_SUBXIDS = (PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS
#[inline]
pub unsafe fn TOTAL_MAX_CACHED_SUBXIDS() -> c_int {
    (PGPROC_MAX_CACHED_SUBXIDS + 1) * PROCARRAY_MAXPROCS()
}

// ---------------------------------------------------------------------------
// ProcArrayStruct -- the shared-memory structure
// ---------------------------------------------------------------------------

/// The in-shared-memory structure for the process array.
#[repr(C)]
pub struct ProcArrayStruct {
    pub numProcs: c_int,
    pub maxProcs: c_int,
    /// allocated size of KnownAssignedXids arrays
    pub maxKnownAssignedXids: c_int,
    /// current number of valid entries
    pub numKnownAssignedXids: c_int,
    /// index of oldest valid element
    pub tailKnownAssignedXids: c_int,
    /// index of newest element + 1
    pub headKnownAssignedXids: c_int,
    /// highest subxid removed from KnownAssignedXids to prevent overflow
    pub lastOverflowedXid: TransactionId,
    /// oldest xmin of any replication slot
    pub replication_slot_xmin: TransactionId,
    /// oldest catalog xmin of any replication slot
    pub replication_slot_catalog_xmin: TransactionId,
    /// indexes into allProcs[], PROCARRAY_MAXPROCS entries (flexible array) */
    pub pgprocnos: [c_int; 0],
}

// ---------------------------------------------------------------------------
// GlobalVisState
// ---------------------------------------------------------------------------

/// State for the GlobalVisTest* family of functions.
/// See the lengthy comment in the C source above struct GlobalVisState.
#[repr(C)]
pub struct GlobalVisState {
    /// XIDs >= are considered running by some backend
    pub definitely_needed: FullTransactionId,
    /// XIDs < are not considered to be running by any backend
    pub maybe_needed: FullTransactionId,
}

// ---------------------------------------------------------------------------
// ComputeXidHorizonsResult
// ---------------------------------------------------------------------------

#[repr(C)]
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

// ---------------------------------------------------------------------------
// GlobalVisHorizonKind
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, PartialEq)]
#[repr(C)]
pub enum GlobalVisHorizonKind {
    VISHORIZON_SHARED,
    VISHORIZON_CATALOG,
    VISHORIZON_DATA,
    VISHORIZON_TEMP,
}
pub use GlobalVisHorizonKind::*;

// ---------------------------------------------------------------------------
// KAXCompressReason
// ---------------------------------------------------------------------------

#[repr(C)]
pub enum KAXCompressReason {
    KAX_NO_SPACE,
    KAX_PRUNE,
    KAX_TRANSACTION_END,
    KAX_STARTUP_PROCESS_IDLE,
}
pub use KAXCompressReason::*;

// ---------------------------------------------------------------------------
// Module-level statics
// ---------------------------------------------------------------------------

static mut procArray: *mut ProcArrayStruct = core::ptr::null_mut();
static mut allProcs: *mut PGPROC = core::ptr::null_mut();

/// Cache to reduce overhead of repeated calls to TransactionIdIsInProgress()
static mut cachedXidIsNotInProgress: TransactionId = InvalidTransactionId;

/// Bookkeeping for tracking emulated transactions in recovery
static mut KnownAssignedXids: *mut TransactionId = core::ptr::null_mut();
static mut KnownAssignedXidsValid: *mut bool = core::ptr::null_mut();
static mut latestObservedXid: TransactionId = InvalidTransactionId;

/// If we're in STANDBY_SNAPSHOT_PENDING state, highest xid possibly still running
/// that we don't have in KnownAssignedXids.
static mut standbySnapshotPendingXmin: TransactionId = InvalidTransactionId;

/// Per-type global visibility states
pub static mut GlobalVisSharedRels: GlobalVisState   = GlobalVisState { definitely_needed: InvalidFullTransactionId, maybe_needed: InvalidFullTransactionId };
pub static mut GlobalVisCatalogRels: GlobalVisState  = GlobalVisState { definitely_needed: InvalidFullTransactionId, maybe_needed: InvalidFullTransactionId };
pub static mut GlobalVisDataRels: GlobalVisState     = GlobalVisState { definitely_needed: InvalidFullTransactionId, maybe_needed: InvalidFullTransactionId };
pub static mut GlobalVisTempRels: GlobalVisState     = GlobalVisState { definitely_needed: InvalidFullTransactionId, maybe_needed: InvalidFullTransactionId };

/// RecentXmin at the last accurate horizon recompute
static mut ComputeXidHorizonsResultLastXmin: TransactionId = InvalidTransactionId;

// XIDCACHE_DEBUG counters -- compiled out by default (#[cfg(any())] = always false)
#[cfg(any())] // XIDCACHE_DEBUG not enabled
mod xidcache_debug {
    pub static mut xc_by_recent_xmin: i64 = 0;
    pub static mut xc_by_known_xact: i64  = 0;
    pub static mut xc_by_my_xact: i64     = 0;
    pub static mut xc_by_latest_xid: i64  = 0;
    pub static mut xc_by_main_xid: i64    = 0;
    pub static mut xc_by_child_xid: i64   = 0;
    pub static mut xc_by_known_assigned: i64 = 0;
    pub static mut xc_no_overflow: i64    = 0;
    pub static mut xc_slow_answer: i64    = 0;
}

// Increment macros (no-ops unless XIDCACHE_DEBUG)
#[inline] fn xc_by_recent_xmin_inc() {}
#[inline] fn xc_by_known_xact_inc() {}
#[inline] fn xc_by_my_xact_inc() {}
#[inline] fn xc_by_latest_xid_inc() {}
#[inline] fn xc_by_main_xid_inc() {}
#[inline] fn xc_by_child_xid_inc() {}
#[inline] fn xc_by_known_assigned_inc() {}
#[inline] fn xc_no_overflow_inc() {}
#[inline] fn xc_slow_answer_inc() {}

// ---------------------------------------------------------------------------
// ProcArrayShmemSize / ProcArrayShmemInit
// ---------------------------------------------------------------------------

/// Report shared-memory space needed by ProcArrayShmemInit.
pub unsafe fn ProcArrayShmemSize() -> Size {
    let mut size: Size;

    size = core::mem::offset_of!(ProcArrayStruct, pgprocnos);
    size = add_size(size, mul_size(core::mem::size_of::<c_int>(), PROCARRAY_MAXPROCS() as usize));

    /*
     * During Hot Standby processing we have a data structure called
     * KnownAssignedXids, created in shared memory.  All of the main
     * structures created in those functions must be identically sized,
     * since we may at times copy the whole of the data structures around.
     * We refer to this size as TOTAL_MAX_CACHED_SUBXIDS.
     */
    if EnableHotStandby {
        size = add_size(size,
                        mul_size(core::mem::size_of::<TransactionId>(),
                                 TOTAL_MAX_CACHED_SUBXIDS() as usize));
        size = add_size(size,
                        mul_size(core::mem::size_of::<bool>(),
                                 TOTAL_MAX_CACHED_SUBXIDS() as usize));
    }

    size
}

/// Initialize the shared PGPROC array during postmaster startup.
pub unsafe fn ProcArrayShmemInit() {
    let mut found: bool = false;

    // Create or attach to the ProcArray shared structure
    procArray = ShmemInitStruct(
        b"Proc Array\0".as_ptr() as *const i8,
        add_size(core::mem::offset_of!(ProcArrayStruct, pgprocnos),
                 mul_size(core::mem::size_of::<c_int>(),
                          PROCARRAY_MAXPROCS() as usize)),
        &mut found,
    ) as *mut ProcArrayStruct;

    if !found {
        // We're the first - initialize.
        (*procArray).numProcs = 0;
        (*procArray).maxProcs = PROCARRAY_MAXPROCS();
        (*procArray).maxKnownAssignedXids = TOTAL_MAX_CACHED_SUBXIDS();
        (*procArray).numKnownAssignedXids = 0;
        (*procArray).tailKnownAssignedXids = 0;
        (*procArray).headKnownAssignedXids = 0;
        (*procArray).lastOverflowedXid = InvalidTransactionId;
        (*procArray).replication_slot_xmin = InvalidTransactionId;
        (*procArray).replication_slot_catalog_xmin = InvalidTransactionId;
        (*TransamVariables).xactCompletionCount = 1;
    }

    allProcs = (*ProcGlobal).allProcs;

    // Create or attach to the KnownAssignedXids arrays too, if needed
    if EnableHotStandby {
        KnownAssignedXids = ShmemInitStruct(
            b"KnownAssignedXids\0".as_ptr() as *const i8,
            mul_size(core::mem::size_of::<TransactionId>(),
                     TOTAL_MAX_CACHED_SUBXIDS() as usize),
            &mut found,
        ) as *mut TransactionId;
        KnownAssignedXidsValid = ShmemInitStruct(
            b"KnownAssignedXidsValid\0".as_ptr() as *const i8,
            mul_size(core::mem::size_of::<bool>(),
                     TOTAL_MAX_CACHED_SUBXIDS() as usize),
            &mut found,
        ) as *mut bool;
    }
}

// ---------------------------------------------------------------------------
// ProcArrayAdd / ProcArrayRemove
// ---------------------------------------------------------------------------

/// Add the specified PGPROC to the shared array.
pub unsafe fn ProcArrayAdd(proc_: *mut PGPROC) {
    let pgprocno = GetNumberFromPGProc(proc_);
    let array_p = procArray;
    let mut index: c_int;
    let movecount: c_int;

    // See ProcGlobal comment explaining why both locks are held
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);
    LWLockAcquire(XidGenLock(), LW_EXCLUSIVE);

    if (*array_p).numProcs >= (*array_p).maxProcs {
        /*
         * Oops, no room.  (This really shouldn't happen, since there is a
         * fixed supply of PGPROC structs too, and so we should have failed
         * earlier.)
         */
        ereport!(FATAL, errmsg!("sorry, too many clients already"));
    }

    /*
     * Keep the procs array sorted by (PGPROC *) so that we can utilize
     * locality of references much better.  Since the occurrence of
     * adding/removing a proc is much lower than the access to the ProcArray
     * itself, the overhead should be marginal.
     */
    index = 0;
    while index < (*array_p).numProcs {
        let this_procno = *pgprocnos_ptr(array_p).add(index as usize);
        Assert!(this_procno >= 0 && this_procno < ((*array_p).maxProcs + NUM_AUXILIARY_PROCS));
        Assert!((*allProcs.add(this_procno as usize)).pgxactoff == index);
        // If we have found our right position in the array, break
        if this_procno > pgprocno {
            break;
        }
        index += 1;
    }

    movecount = (*array_p).numProcs - index;
    core::ptr::copy(
        pgprocnos_ptr(array_p).add(index as usize),
        pgprocnos_ptr(array_p).add((index + 1) as usize),
        movecount as usize,
    );
    core::ptr::copy(
        (*ProcGlobal).xids.add(index as usize),
        (*ProcGlobal).xids.add((index + 1) as usize),
        movecount as usize,
    );
    core::ptr::copy(
        (*ProcGlobal).subxidStates.add(index as usize),
        (*ProcGlobal).subxidStates.add((index + 1) as usize),
        movecount as usize,
    );
    core::ptr::copy(
        (*ProcGlobal).statusFlags.add(index as usize),
        (*ProcGlobal).statusFlags.add((index + 1) as usize),
        movecount as usize,
    );

    *pgprocnos_ptr(array_p).add(index as usize) = GetNumberFromPGProc(proc_);
    (*proc_).pgxactoff = index;
    *(*ProcGlobal).xids.add(index as usize) = (*proc_).xid;
    *(*ProcGlobal).subxidStates.add(index as usize) = core::ptr::read(&(*proc_).subxidStatus);
    *(*ProcGlobal).statusFlags.add(index as usize) = (*proc_).statusFlags;

    (*array_p).numProcs += 1;

    // adjust pgxactoff for all following PGPROCs
    index += 1;
    while index < (*array_p).numProcs {
        let procno = *pgprocnos_ptr(array_p).add(index as usize);
        Assert!(procno >= 0 && procno < ((*array_p).maxProcs + NUM_AUXILIARY_PROCS));
        Assert!((*allProcs.add(procno as usize)).pgxactoff == index - 1);
        (*allProcs.add(procno as usize)).pgxactoff = index;
        index += 1;
    }

    /*
     * Release in reversed acquisition order, to reduce frequency of having to
     * wait for XidGenLock while holding ProcArrayLock.
     */
    LWLockRelease(XidGenLock());
    LWLockRelease(ProcArrayLock());
}

/// Remove the specified PGPROC from the shared array.
///
/// When latestXid is a valid XID, we are removing a live 2PC gxact from the
/// array, and thus causing it to appear as "not running" anymore.  In this
/// case we must advance latestCompletedXid.
pub unsafe fn ProcArrayRemove(proc_: *mut PGPROC, latestXid: TransactionId) {
    let array_p = procArray;
    let myoff: c_int;
    let movecount: c_int;

    // dump stats at backend shutdown, but not prepared-xact end (XIDCACHE_DEBUG only)
    #[cfg(any())] // XIDCACHE_DEBUG
    if (*proc_).pid != 0 { DisplayXidCache(); }

    // See ProcGlobal comment explaining why both locks are held
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);
    LWLockAcquire(XidGenLock(), LW_EXCLUSIVE);

    myoff = (*proc_).pgxactoff;

    Assert!(myoff >= 0 && myoff < (*array_p).numProcs);
    Assert!((*allProcs.add(*pgprocnos_ptr(array_p).add(myoff as usize) as usize)).pgxactoff == myoff);

    if TransactionIdIsValid(latestXid) {
        Assert!(TransactionIdIsValid(*(*ProcGlobal).xids.add(myoff as usize)));

        // Advance global latestCompletedXid while holding the lock
        MaintainLatestCompletedXid(latestXid);

        // Same with xactCompletionCount
        (*TransamVariables).xactCompletionCount += 1;

        *(*ProcGlobal).xids.add(myoff as usize) = InvalidTransactionId;
        (*(*ProcGlobal).subxidStates.add(myoff as usize)).overflowed = false;
        (*(*ProcGlobal).subxidStates.add(myoff as usize)).count = 0;
    } else {
        // Shouldn't be trying to remove a live transaction here
        Assert!(!TransactionIdIsValid(*(*ProcGlobal).xids.add(myoff as usize)));
    }

    Assert!(!TransactionIdIsValid(*(*ProcGlobal).xids.add(myoff as usize)));
    Assert!((*(*ProcGlobal).subxidStates.add(myoff as usize)).count == 0);
    Assert!((*(*ProcGlobal).subxidStates.add(myoff as usize)).overflowed == false);

    *(*ProcGlobal).statusFlags.add(myoff as usize) = 0;

    // Keep the PGPROC array sorted.  See notes above.
    movecount = (*array_p).numProcs - myoff - 1;
    core::ptr::copy(
        pgprocnos_ptr(array_p).add((myoff + 1) as usize),
        pgprocnos_ptr(array_p).add(myoff as usize),
        movecount as usize,
    );
    core::ptr::copy(
        (*ProcGlobal).xids.add((myoff + 1) as usize),
        (*ProcGlobal).xids.add(myoff as usize),
        movecount as usize,
    );
    core::ptr::copy(
        (*ProcGlobal).subxidStates.add((myoff + 1) as usize),
        (*ProcGlobal).subxidStates.add(myoff as usize),
        movecount as usize,
    );
    core::ptr::copy(
        (*ProcGlobal).statusFlags.add((myoff + 1) as usize),
        (*ProcGlobal).statusFlags.add(myoff as usize),
        movecount as usize,
    );

    *pgprocnos_ptr(array_p).add(((*array_p).numProcs - 1) as usize) = -1; // for debugging
    (*array_p).numProcs -= 1;

    /*
     * Adjust pgxactoff of following procs for removed PGPROC (note that
     * numProcs already has been decremented).
     */
    for index in myoff..(*array_p).numProcs {
        let procno = *pgprocnos_ptr(array_p).add(index as usize);
        Assert!(procno >= 0 && procno < ((*array_p).maxProcs + NUM_AUXILIARY_PROCS));
        Assert!((*allProcs.add(procno as usize)).pgxactoff - 1 == index);
        (*allProcs.add(procno as usize)).pgxactoff = index;
    }

    /*
     * Release in reversed acquisition order, to reduce frequency of having to
     * wait for XidGenLock while holding ProcArrayLock.
     */
    LWLockRelease(XidGenLock());
    LWLockRelease(ProcArrayLock());
}

// ---------------------------------------------------------------------------
// ProcArrayEndTransaction
// ---------------------------------------------------------------------------

/// ProcArrayEndTransaction -- mark a transaction as no longer running.
///
/// This is used interchangeably for commit and abort cases.  The transaction
/// commit/abort must already be reported to WAL and pg_xact.
pub unsafe fn ProcArrayEndTransaction(proc_: *mut PGPROC, latestXid: TransactionId) {
    if TransactionIdIsValid(latestXid) {
        /*
         * We must lock ProcArrayLock while clearing our advertised XID, so
         * that we do not exit the set of "running" transactions while someone
         * else is taking a snapshot.  See discussion in
         * src/backend/access/transam/README.
         */
        Assert!(TransactionIdIsValid((*proc_).xid));

        /*
         * If we can immediately acquire ProcArrayLock, we clear our own XID
         * and release the lock.  If not, use group XID clearing to improve
         * efficiency.
         */
        if LWLockConditionalAcquire(ProcArrayLock(), LW_EXCLUSIVE) {
            ProcArrayEndTransactionInternal(proc_, latestXid);
            LWLockRelease(ProcArrayLock());
        } else {
            ProcArrayGroupClearXid(proc_, latestXid);
        }
    } else {
        /*
         * If we have no XID, we don't need to lock, since we won't affect
         * anyone else's calculation of a snapshot.  We might change their
         * estimate of global xmin, but that's OK.
         */
        Assert!(!TransactionIdIsValid((*proc_).xid));
        Assert!((*proc_).subxidStatus.count == 0);
        Assert!(!(*proc_).subxidStatus.overflowed);

        (*proc_).vxid.lxid = InvalidLocalTransactionId;
        (*proc_).xmin = InvalidTransactionId;

        // be sure this is cleared in abort
        (*proc_).delayChkptFlags = 0;

        (*proc_).recoveryConflictPending = false;

        // must be cleared with xid/xmin:
        // avoid unnecessarily dirtying shared cachelines
        if ((*proc_).statusFlags & PROC_VACUUM_STATE_MASK) != 0 {
            Assert!(!LWLockHeldByMe(ProcArrayLock()));
            LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);
            Assert!((*proc_).statusFlags == *(*ProcGlobal).statusFlags.add((*proc_).pgxactoff as usize));
            (*proc_).statusFlags &= !PROC_VACUUM_STATE_MASK;
            *(*ProcGlobal).statusFlags.add((*proc_).pgxactoff as usize) = (*proc_).statusFlags;
            LWLockRelease(ProcArrayLock());
        }
    }
}

/// Mark a write transaction as no longer running.
/// We don't do any locking here; caller must handle that.
unsafe fn ProcArrayEndTransactionInternal(proc_: *mut PGPROC, latestXid: TransactionId) {
    let pgxactoff = (*proc_).pgxactoff;

    /*
     * Note: we need exclusive lock here because we're going to change other
     * processes' PGPROC entries.
     */
    Assert!(LWLockHeldByMeInMode(ProcArrayLock(), LW_EXCLUSIVE));
    Assert!(TransactionIdIsValid(*(*ProcGlobal).xids.add(pgxactoff as usize)));
    Assert!(*(*ProcGlobal).xids.add(pgxactoff as usize) == (*proc_).xid);

    *(*ProcGlobal).xids.add(pgxactoff as usize) = InvalidTransactionId;
    (*proc_).xid = InvalidTransactionId;
    (*proc_).vxid.lxid = InvalidLocalTransactionId;
    (*proc_).xmin = InvalidTransactionId;

    // be sure this is cleared in abort
    (*proc_).delayChkptFlags = 0;

    (*proc_).recoveryConflictPending = false;

    // must be cleared with xid/xmin:
    // avoid unnecessarily dirtying shared cachelines
    if ((*proc_).statusFlags & PROC_VACUUM_STATE_MASK) != 0 {
        (*proc_).statusFlags &= !PROC_VACUUM_STATE_MASK;
        *(*ProcGlobal).statusFlags.add((*proc_).pgxactoff as usize) = (*proc_).statusFlags;
    }

    // Clear the subtransaction-XID cache too while holding the lock
    Assert!((*(*ProcGlobal).subxidStates.add(pgxactoff as usize)).count == (*proc_).subxidStatus.count &&
            (*(*ProcGlobal).subxidStates.add(pgxactoff as usize)).overflowed == (*proc_).subxidStatus.overflowed);
    if (*proc_).subxidStatus.count > 0 || (*proc_).subxidStatus.overflowed {
        (*(*ProcGlobal).subxidStates.add(pgxactoff as usize)).count = 0;
        (*(*ProcGlobal).subxidStates.add(pgxactoff as usize)).overflowed = false;
        (*proc_).subxidStatus.count = 0;
        (*proc_).subxidStatus.overflowed = false;
    }

    // Also advance global latestCompletedXid while holding the lock
    MaintainLatestCompletedXid(latestXid);

    // Same with xactCompletionCount
    (*TransamVariables).xactCompletionCount += 1;
}

/// ProcArrayGroupClearXid -- group XID clearing.
///
/// When we cannot immediately acquire ProcArrayLock in exclusive mode at
/// commit time, add ourselves to a list of processes that need their XIDs
/// cleared.  The first process to add itself to the list will acquire
/// ProcArrayLock in exclusive mode and perform ProcArrayEndTransactionInternal
/// on behalf of all group members.
unsafe fn ProcArrayGroupClearXid(proc_: *mut PGPROC, latestXid: TransactionId) {
    let pgprocno = GetNumberFromPGProc(proc_);
    let procglobal: *mut PROC_HDR = ProcGlobal;
    let mut nextidx: uint32;
    let mut wakeidx: uint32;

    // We should definitely have an XID to clear.
    Assert!(TransactionIdIsValid((*proc_).xid));

    // Add ourselves to the list of processes needing a group XID clear.
    (*proc_).procArrayGroupMember = true;
    (*proc_).procArrayGroupMemberXid = latestXid;
    nextidx = pg_atomic_read_u32(&(*procglobal).procArrayGroupFirst);
    loop {
        pg_atomic_write_u32(&mut (*proc_).procArrayGroupNext, nextidx);
        if pg_atomic_compare_exchange_u32(&mut (*procglobal).procArrayGroupFirst,
                                          &mut nextidx,
                                          pgprocno as uint32) {
            break;
        }
    }

    /*
     * If the list was not empty, the leader will clear our XID.  It is
     * impossible to have followers without a leader because the first process
     * that has added itself to the list will always have nextidx as
     * INVALID_PROC_NUMBER.
     */
    if nextidx != INVALID_PROC_NUMBER as uint32 {
        let mut extra_waits: c_int = 0;

        // Sleep until the leader clears our XID.
        pgstat_report_wait_start(WAIT_EVENT_PROCARRAY_GROUP_UPDATE);
        loop {
            // acts as a read barrier
            PGSemaphoreLock((*proc_).sem);
            if !(*proc_).procArrayGroupMember {
                break;
            }
            extra_waits += 1;
        }
        pgstat_report_wait_end();

        Assert!(pg_atomic_read_u32(&(*proc_).procArrayGroupNext) == INVALID_PROC_NUMBER as uint32);

        // Fix semaphore count for any absorbed wakeups
        while extra_waits > 0 {
            extra_waits -= 1;
            PGSemaphoreUnlock((*proc_).sem);
        }
        return;
    }

    // We are the leader.  Acquire the lock on behalf of everyone.
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    /*
     * Now that we've got the lock, clear the list of processes waiting for
     * group XID clearing, saving a pointer to the head of the list.  Trying
     * to pop elements one at a time could lead to an ABA problem.
     */
    nextidx = pg_atomic_exchange_u32(&mut (*procglobal).procArrayGroupFirst,
                                     INVALID_PROC_NUMBER as uint32);

    // Remember head of list so we can perform wakeups after dropping lock.
    wakeidx = nextidx;

    // Walk the list and clear all XIDs.
    while nextidx != INVALID_PROC_NUMBER as uint32 {
        let nextproc: *mut PGPROC = allProcs.add(nextidx as usize);
        ProcArrayEndTransactionInternal(nextproc, (*nextproc).procArrayGroupMemberXid);
        // Move to next proc in list.
        nextidx = pg_atomic_read_u32(&(*nextproc).procArrayGroupNext);
    }

    // We're done with the lock now.
    LWLockRelease(ProcArrayLock());

    /*
     * Now that we've released the lock, go back and wake everybody up.  We
     * don't do this under the lock so as to keep lock hold times to a minimum.
     */
    while wakeidx != INVALID_PROC_NUMBER as uint32 {
        let nextproc: *mut PGPROC = allProcs.add(wakeidx as usize);
        wakeidx = pg_atomic_read_u32(&(*nextproc).procArrayGroupNext);
        pg_atomic_write_u32(&mut (*nextproc).procArrayGroupNext, INVALID_PROC_NUMBER as uint32);

        // ensure all previous writes are visible before follower continues.
        pg_write_barrier();

        (*nextproc).procArrayGroupMember = false;

        if nextproc != MyProc {
            PGSemaphoreUnlock((*nextproc).sem);
        }
    }
}

// pgstat wait event stubs -- TODO(pg-port): real values in utils/wait_event.h
pub const WAIT_EVENT_PROCARRAY_GROUP_UPDATE: uint32 = 0;
unsafe fn pgstat_report_wait_start(_wait_event_info: uint32) {} // TODO(pg-port): real fn in utils/activity/wait_event.c
unsafe fn pgstat_report_wait_end() {}                           // TODO(pg-port): real fn in utils/activity/wait_event.c

// ---------------------------------------------------------------------------
// ProcArrayClearTransaction
// ---------------------------------------------------------------------------

/// ProcArrayClearTransaction -- clear the transaction fields.
///
/// This is used after successfully preparing a 2-phase transaction.
pub unsafe fn ProcArrayClearTransaction(proc_: *mut PGPROC) {
    let pgxactoff: c_int;

    /*
     * Currently we need to lock ProcArrayLock exclusively here, as we
     * increment xactCompletionCount below.
     */
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    pgxactoff = (*proc_).pgxactoff;

    *(*ProcGlobal).xids.add(pgxactoff as usize) = InvalidTransactionId;
    (*proc_).xid = InvalidTransactionId;

    (*proc_).vxid.lxid = InvalidLocalTransactionId;
    (*proc_).xmin = InvalidTransactionId;
    (*proc_).recoveryConflictPending = false;

    Assert!(((*proc_).statusFlags & PROC_VACUUM_STATE_MASK) == 0);
    Assert!((*proc_).delayChkptFlags == 0);

    /*
     * Need to increment completion count even though transaction hasn't
     * really committed yet.  The reason for that is that GetSnapshotData()
     * omits the xid of the current transaction.
     */
    (*TransamVariables).xactCompletionCount += 1;

    // Clear the subtransaction-XID cache too
    Assert!((*(*ProcGlobal).subxidStates.add(pgxactoff as usize)).count == (*proc_).subxidStatus.count &&
            (*(*ProcGlobal).subxidStates.add(pgxactoff as usize)).overflowed == (*proc_).subxidStatus.overflowed);
    if (*proc_).subxidStatus.count > 0 || (*proc_).subxidStatus.overflowed {
        (*(*ProcGlobal).subxidStates.add(pgxactoff as usize)).count = 0;
        (*(*ProcGlobal).subxidStates.add(pgxactoff as usize)).overflowed = false;
        (*proc_).subxidStatus.count = 0;
        (*proc_).subxidStatus.overflowed = false;
    }

    LWLockRelease(ProcArrayLock());
}

// ---------------------------------------------------------------------------
// MaintainLatestCompletedXid helpers
// ---------------------------------------------------------------------------

/// Update TransamVariables->latestCompletedXid to point to latestXid if
/// currently older.
unsafe fn MaintainLatestCompletedXid(latestXid: TransactionId) {
    let cur_latest = (*TransamVariables).latestCompletedXid;

    Assert!(FullTransactionIdIsValid(cur_latest));
    Assert!(!RecoveryInProgress());
    Assert!(LWLockHeldByMe(ProcArrayLock()));

    if TransactionIdPrecedes(XidFromFullTransactionId(cur_latest), latestXid) {
        (*TransamVariables).latestCompletedXid =
            FullXidRelativeTo(cur_latest, latestXid);
    }

    Assert!(IsBootstrapProcessingMode() ||
            FullTransactionIdIsNormal((*TransamVariables).latestCompletedXid));
}

/// Same as MaintainLatestCompletedXid, except for use during WAL replay.
unsafe fn MaintainLatestCompletedXidRecovery(latestXid: TransactionId) {
    let cur_latest = (*TransamVariables).latestCompletedXid;
    let rel: FullTransactionId;

    Assert!(AmStartupProcess() || !IsUnderPostmaster);
    Assert!(LWLockHeldByMe(ProcArrayLock()));

    /*
     * Need a FullTransactionId to compare latestXid with.  Can't rely on
     * latestCompletedXid to be initialized in recovery.  But in recovery it's
     * safe to access nextXid without a lock for the startup process.
     */
    rel = (*TransamVariables).nextXid;
    Assert!(FullTransactionIdIsValid((*TransamVariables).nextXid));

    if !FullTransactionIdIsValid(cur_latest) ||
        TransactionIdPrecedes(XidFromFullTransactionId(cur_latest), latestXid)
    {
        (*TransamVariables).latestCompletedXid =
            FullXidRelativeTo(rel, latestXid);
    }

    Assert!(FullTransactionIdIsNormal((*TransamVariables).latestCompletedXid));
}

// ---------------------------------------------------------------------------
// ProcArrayInitRecovery / ProcArrayApplyRecoveryInfo
// ---------------------------------------------------------------------------

/// ProcArrayInitRecovery -- initialize recovery xid mgmt environment.
///
/// Remember up to where the startup process initialized the CLOG and subtrans
/// so we can ensure it's initialized gaplessly up to the point where necessary
/// while in recovery.
pub unsafe fn ProcArrayInitRecovery(initializedUptoXID: TransactionId) {
    Assert!(standbyState == STANDBY_INITIALIZED);
    Assert!(TransactionIdIsNormal(initializedUptoXID));

    /*
     * we set latestObservedXid to the xid SUBTRANS has been initialized up
     * to, so we can extend it from that point onwards in
     * RecordKnownAssignedTransactionIds, and when we get consistent in
     * ProcArrayApplyRecoveryInfo().
     */
    latestObservedXid = initializedUptoXID;
    TransactionIdRetreat(&mut latestObservedXid);
}

/// ProcArrayApplyRecoveryInfo -- apply recovery info about xids.
///
/// Takes us through 3 states: Initialized, Pending and Ready.
pub unsafe fn ProcArrayApplyRecoveryInfo(running: RunningTransactions) {
    let xids: *mut TransactionId;
    let mut advance_next_xid: TransactionId;
    let mut nxids: c_int;
    let mut i: c_int;

    Assert!(standbyState >= STANDBY_INITIALIZED);
    Assert!(TransactionIdIsValid((*running).nextXid));
    Assert!(TransactionIdIsValid((*running).oldestRunningXid));
    Assert!(TransactionIdIsNormal((*running).latestCompletedXid));

    // Remove stale transactions, if any.
    ExpireOldKnownAssignedTransactionIds((*running).oldestRunningXid);

    /*
     * Adjust TransamVariables->nextXid before StandbyReleaseOldLocks(),
     * because we will need it up to date for accessing two-phase transactions
     * in StandbyReleaseOldLocks().
     */
    advance_next_xid = (*running).nextXid;
    TransactionIdRetreat(&mut advance_next_xid);
    AdvanceNextFullTransactionIdPastXid(advance_next_xid);
    Assert!(FullTransactionIdIsValid((*TransamVariables).nextXid));

    // Remove stale locks, if any.
    StandbyReleaseOldLocks((*running).oldestRunningXid);

    // If our snapshot is already valid, nothing else to do...
    if standbyState == STANDBY_SNAPSHOT_READY {
        return;
    }

    /*
     * If our initial RunningTransactionsData had an overflowed snapshot then
     * we knew we were missing some subxids from our snapshot.
     */
    if standbyState == STANDBY_SNAPSHOT_PENDING {
        /*
         * If the snapshot isn't overflowed or if its empty we can reset our
         * pending state and use this snapshot instead.
         */
        match (*running).subxid_status {
            SUBXIDS_MISSING if (*running).xcnt != 0 => {
                if TransactionIdPrecedes(standbySnapshotPendingXmin,
                                         (*running).oldestRunningXid)
                {
                    standbyState = STANDBY_SNAPSHOT_READY;
                    elog!(DEBUG1, "recovery snapshots are now enabled");
                } else {
                    elog!(DEBUG1,
                         "recovery snapshot waiting for non-overflowed snapshot or \
until oldest active xid on standby is at least %u (now %u)");
                }
                return;
            }
            _ => {
                /*
                 * If we have already collected known assigned xids, we need to
                 * throw them away before we apply the recovery snapshot.
                 */
                KnownAssignedXidsReset();
                standbyState = STANDBY_INITIALIZED;
            }
        }
    }

    Assert!(standbyState == STANDBY_INITIALIZED);

    /*
     * Nobody else is running yet, but take locks anyhow.
     */
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    /*
     * KnownAssignedXids is sorted so we cannot just add the xids, we have to
     * sort them first.
     *
     * Allocate a temporary array to avoid modifying the array passed as argument.
     */
    xids = palloc(core::mem::size_of::<TransactionId>() as Size *
                  ((*running).xcnt + (*running).subxcnt) as Size) as *mut TransactionId;

    // Add to the temp array any xids which have not already completed.
    nxids = 0;
    i = 0;
    while i < (*running).xcnt + (*running).subxcnt {
        let xid = *(*running).xids.add(i as usize);

        /*
         * The running-xacts snapshot can contain xids that were still visible
         * in the procarray when the snapshot was taken, but were already
         * WAL-logged as completed.
         */
        if TransactionIdDidCommit(xid) || TransactionIdDidAbort(xid) {
            i += 1;
            continue;
        }

        *xids.add(nxids as usize) = xid;
        nxids += 1;
        i += 1;
    }

    if nxids > 0 {
        if (*procArray).numKnownAssignedXids != 0 {
            LWLockRelease(ProcArrayLock());
            elog!(ERROR, "KnownAssignedXids is not empty");
        }

        /*
         * Sort the array so that we can add them safely into KnownAssignedXids.
         */
        qsort(xids as *mut c_void, nxids as usize, core::mem::size_of::<TransactionId>(),
              xidLogicalComparator);

        /*
         * Add the sorted snapshot into KnownAssignedXids.  The running-xacts
         * snapshot may include duplicated xids because of prepared transactions.
         */
        i = 0;
        while i < nxids {
            if i > 0 && TransactionIdEquals(*xids.add((i - 1) as usize), *xids.add(i as usize)) {
                elog!(DEBUG1, "found duplicated transaction %u for KnownAssignedXids insertion");
                i += 1;
                continue;
            }
            KnownAssignedXidsAdd(*xids.add(i as usize), *xids.add(i as usize), true);
            i += 1;
        }

        KnownAssignedXidsDisplay(DEBUG3);
    }

    pfree(xids as *mut c_void);

    /*
     * latestObservedXid is at least set to the point where SUBTRANS was
     * started up to.  Initialize subtrans from thereon, up to nextXid - 1.
     */
    Assert!(TransactionIdIsNormal(latestObservedXid));
    TransactionIdAdvance(&mut latestObservedXid);
    while TransactionIdPrecedes(latestObservedXid, (*running).nextXid) {
        ExtendSUBTRANS(latestObservedXid);
        TransactionIdAdvance(&mut latestObservedXid);
    }
    TransactionIdRetreat(&mut latestObservedXid); // = running->nextXid - 1

    /*
     * Now we've got the running xids we need to set the global values that
     * are used to track snapshots as they evolve further.
     *
     * If the snapshot overflowed, then we still initialise with what we know,
     * but the recovery snapshot isn't fully valid yet.
     */
    match (*running).subxid_status {
        SUBXIDS_MISSING => {
            standbyState = STANDBY_SNAPSHOT_PENDING;
            standbySnapshotPendingXmin = latestObservedXid;
            (*procArray).lastOverflowedXid = latestObservedXid;
        }
        _ => {
            standbyState = STANDBY_SNAPSHOT_READY;
            standbySnapshotPendingXmin = InvalidTransactionId;

            /*
             * If the 'xids' array didn't include all subtransactions, we have to
             * mark any snapshots taken as overflowed.
             */
            match (*running).subxid_status {
                SUBXIDS_IN_SUBTRANS => {
                    (*procArray).lastOverflowedXid = latestObservedXid;
                }
                _ => {
                    // Assert!((*running).subxid_status == SUBXIDS_IN_ARRAY)
                    (*procArray).lastOverflowedXid = InvalidTransactionId;
                }
            }
        }
    }

    /*
     * If a transaction wrote a commit record in the gap between taking and
     * logging the snapshot then latestCompletedXid may already be higher.
     */
    MaintainLatestCompletedXidRecovery((*running).latestCompletedXid);

    /*
     * NB: No need to increment TransamVariables->xactCompletionCount here,
     * nobody can see it yet.
     */

    LWLockRelease(ProcArrayLock());

    KnownAssignedXidsDisplay(DEBUG3);
    if standbyState == STANDBY_SNAPSHOT_READY {
        elog!(DEBUG1, "recovery snapshots are now enabled");
    } else {
        elog!(DEBUG1,
              "recovery snapshot waiting for non-overflowed snapshot or until oldest active xid on standby is at least %u (now %u)");
    }
}

// elog level constants -- TODO(pg-port): real values in utils/elog.h
pub const DEBUG1: c_int = 15;
pub const DEBUG3: c_int = 13;
pub const DEBUG4: c_int = 12;
pub const LOG: c_int = 17;
pub const WARNING: c_int = 19;
pub const ERROR: c_int = 20;
pub const FATAL: c_int = 21;

// ---------------------------------------------------------------------------
// ProcArrayApplyXidAssignment
// ---------------------------------------------------------------------------

/// ProcArrayApplyXidAssignment -- Process an XLOG_XACT_ASSIGNMENT WAL record.
pub unsafe fn ProcArrayApplyXidAssignment(topxid: TransactionId,
                                          nsubxids: c_int,
                                          subxids: *mut TransactionId) {
    let max_xid: TransactionId;
    let mut i: c_int;

    Assert!(standbyState >= STANDBY_INITIALIZED);

    max_xid = TransactionIdLatest(topxid, nsubxids, subxids);

    /*
     * Mark all the subtransactions as observed.
     *
     * NOTE: This will fail if the subxid contains too many previously
     * unobserved xids to fit into known-assigned-xids.  That shouldn't happen
     * as the code stands, because xid-assignment records should never contain
     * more than PGPROC_MAX_CACHED_SUBXIDS entries.
     */
    RecordKnownAssignedTransactionIds(max_xid);

    /*
     * Notice that we update pg_subtrans with the top-level xid, rather than
     * the parent xid.
     */
    i = 0;
    while i < nsubxids {
        SubTransSetParent(*subxids.add(i as usize), topxid);
        i += 1;
    }

    // KnownAssignedXids isn't maintained yet, so we're done for now
    if standbyState == STANDBY_INITIALIZED {
        return;
    }

    // Uses same locking as transaction commit
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    // Remove subxids from known-assigned-xacts.
    KnownAssignedXidsRemoveTree(InvalidTransactionId, nsubxids, subxids);

    // Advance lastOverflowedXid to be at least the last of these subxids.
    if TransactionIdPrecedes((*procArray).lastOverflowedXid, max_xid) {
        (*procArray).lastOverflowedXid = max_xid;
    }

    LWLockRelease(ProcArrayLock());
}

// ---------------------------------------------------------------------------
// TransactionIdIsInProgress
// ---------------------------------------------------------------------------

/// TransactionIdIsInProgress -- is given transaction running in some backend.
///
/// Aside from some shortcuts such as checking RecentXmin and our own Xid,
/// there are four possibilities for finding a running transaction.
pub unsafe fn TransactionIdIsInProgress(xid: TransactionId) -> bool {
    static mut xids_static: *mut TransactionId = core::ptr::null_mut();
    static mut other_xids_static: *mut TransactionId = core::ptr::null_mut();

    let other_xids: *mut TransactionId;
    let other_subxidstates: *mut XidCacheStatus;
    let mut nxids: c_int = 0;
    let array_p = procArray;
    let topxid: TransactionId;
    let latest_completed_xid: TransactionId;
    let mypgxactoff: c_int;
    let numprocs: c_int;
    let mut j: c_int;

    /*
     * Don't bother checking a transaction older than RecentXmin; it could not
     * possibly still be running.  (Note: in particular, this guarantees that
     * we reject InvalidTransactionId, FrozenTransactionId, etc as not
     * running.)
     */
    if TransactionIdPrecedes(xid, RecentXmin) {
        xc_by_recent_xmin_inc();
        return false;
    }

    /*
     * We may have just checked the status of this transaction, so if it is
     * already known to be completed, we can fall out without any access to
     * shared memory.
     */
    if TransactionIdEquals(cachedXidIsNotInProgress, xid) {
        xc_by_known_xact_inc();
        return false;
    }

    /*
     * Also, we can handle our own transaction (and subtransactions) without
     * any access to shared memory.
     */
    if TransactionIdIsCurrentTransactionId(xid) {
        xc_by_my_xact_inc();
        return true;
    }

    /*
     * If first time through, get workspace to remember main XIDs in.  We
     * malloc it permanently to avoid repeated palloc/pfree overhead.
     */
    if xids_static.is_null() {
        /*
         * In hot standby mode, reserve enough space to hold all xids in the
         * known-assigned list.
         */
        let maxxids: c_int = if RecoveryInProgress() {
            TOTAL_MAX_CACHED_SUBXIDS()
        } else {
            (*array_p).maxProcs
        };

        xids_static = malloc(maxxids as usize * core::mem::size_of::<TransactionId>())
            as *mut TransactionId;
        if xids_static.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
        }
    }

    other_xids = (*ProcGlobal).xids;
    other_subxidstates = (*ProcGlobal).subxidStates;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    /*
     * Now that we have the lock, we can check latestCompletedXid; if the
     * target Xid is after that, it's surely still running.
     */
    latest_completed_xid =
        XidFromFullTransactionId((*TransamVariables).latestCompletedXid);
    if TransactionIdPrecedes(latest_completed_xid, xid) {
        LWLockRelease(ProcArrayLock());
        xc_by_latest_xid_inc();
        return true;
    }

    // No shortcuts, gotta grovel through the array
    mypgxactoff = (*MyProc).pgxactoff;
    numprocs = (*array_p).numProcs;
    for pgxactoff in 0..numprocs {
        let pgprocno: c_int;
        let proc_: *mut PGPROC;
        let pxid: TransactionId;
        let pxids: c_int;

        // Ignore ourselves --- dealt with it above
        if pgxactoff == mypgxactoff {
            continue;
        }

        // Fetch xid just once - see GetNewTransactionId
        pxid = UINT32_ACCESS_ONCE(*other_xids.add(pgxactoff as usize));

        if !TransactionIdIsValid(pxid) {
            continue;
        }

        // Step 1: check the main Xid
        if TransactionIdEquals(pxid, xid) {
            LWLockRelease(ProcArrayLock());
            xc_by_main_xid_inc();
            return true;
        }

        /*
         * We can ignore main Xids that are younger than the target Xid, since
         * the target could not possibly be their child.
         */
        if TransactionIdPrecedes(xid, pxid) {
            continue;
        }

        // Step 2: check the cached child-Xids arrays
        pxids = (*other_subxidstates.add(pgxactoff as usize)).count as c_int;
        pg_read_barrier(); // pairs with barrier in GetNewTransactionId()
        pgprocno = *pgprocnos_ptr(array_p).add(pgxactoff as usize);
        proc_ = allProcs.add(pgprocno as usize);
        j = pxids - 1;
        while j >= 0 {
            // Fetch xid just once - see GetNewTransactionId
            let cxid = UINT32_ACCESS_ONCE((*proc_).subxids.xids[j as usize]);

            if TransactionIdEquals(cxid, xid) {
                LWLockRelease(ProcArrayLock());
                xc_by_child_xid_inc();
                return true;
            }
            j -= 1;
        }

        /*
         * Save the main Xid for step 4.  We only need to remember main Xids
         * that have uncached children.  (Note: there is no race condition
         * here because the overflowed flag cannot be cleared, only set, while
         * we hold ProcArrayLock.)
         */
        if (*other_subxidstates.add(pgxactoff as usize)).overflowed {
            *xids_static.add(nxids as usize) = pxid;
            nxids += 1;
        }
    }

    /*
     * Step 3: in hot standby mode, check the known-assigned-xids list.
     */
    if RecoveryInProgress() {
        // none of the PGPROC entries should have XIDs in hot standby mode
        Assert!(nxids == 0);

        if KnownAssignedXidExists(xid) {
            LWLockRelease(ProcArrayLock());
            xc_by_known_assigned_inc();
            return true;
        }

        /*
         * If the KnownAssignedXids overflowed, we have to check pg_subtrans
         * too.
         */
        if TransactionIdPrecedesOrEquals(xid, (*procArray).lastOverflowedXid) {
            nxids = KnownAssignedXidsGet(xids_static, xid);
        }
    }

    LWLockRelease(ProcArrayLock());

    /*
     * If none of the relevant caches overflowed, we know the Xid is not
     * running without even looking at pg_subtrans.
     */
    if nxids == 0 {
        xc_no_overflow_inc();
        cachedXidIsNotInProgress = xid;
        return false;
    }

    /*
     * Step 4: have to check pg_subtrans.
     *
     * At this point, we know it's either a subtransaction of one of the Xids
     * in xids[], or it's not running.  If it's an already-failed
     * subtransaction, we want to say "not running" even though its parent may
     * still be running.
     */
    xc_slow_answer_inc();

    if TransactionIdDidAbort(xid) {
        cachedXidIsNotInProgress = xid;
        return false;
    }

    /*
     * It isn't aborted, so check whether the transaction tree it belongs to
     * is still running.
     */
    topxid = SubTransGetTopmostTransaction(xid);
    Assert!(TransactionIdIsValid(topxid));
    if !TransactionIdEquals(topxid, xid) &&
        pg_lfind32(topxid, xids_static, nxids)
    {
        return true;
    }

    cachedXidIsNotInProgress = xid;
    false
}

// ---------------------------------------------------------------------------
// TransactionIdIsActive
// ---------------------------------------------------------------------------

/// TransactionIdIsActive -- is xid the top-level XID of an active backend?
///
/// This differs from TransactionIdIsInProgress in that it ignores prepared
/// transactions, as well as transactions running on the primary if we're in
/// hot standby.
pub unsafe fn TransactionIdIsActive(xid: TransactionId) -> bool {
    let mut result = false;
    let array_p = procArray;
    let other_xids: *mut TransactionId = (*ProcGlobal).xids;
    let mut i: c_int;

    // Don't bother checking a transaction older than RecentXmin.
    if TransactionIdPrecedes(xid, RecentXmin) {
        return false;
    }

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    i = 0;
    while i < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(i as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);
        let pxid: TransactionId;

        // Fetch xid just once - see GetNewTransactionId
        pxid = UINT32_ACCESS_ONCE(*other_xids.add(i as usize));

        if !TransactionIdIsValid(pxid) {
            i += 1;
            continue;
        }

        if (*proc_).pid == 0 {
            i += 1;
            continue; // ignore prepared transactions
        }

        if TransactionIdEquals(pxid, xid) {
            result = true;
            break;
        }
        i += 1;
    }

    LWLockRelease(ProcArrayLock());

    result
}

// ---------------------------------------------------------------------------
// ComputeXidHorizons
// ---------------------------------------------------------------------------

/// Determine XID horizons.
///
/// Used by VACUUM, hot_standby_feedback, and GlobalVisUpdate().
/// See definition of ComputeXidHorizonsResult for the various computed horizons.
unsafe fn ComputeXidHorizons(h: *mut ComputeXidHorizonsResult) {
    let array_p = procArray;
    let mut kaxmin: TransactionId = InvalidTransactionId;
    let in_recovery = RecoveryInProgress();
    let other_xids: *mut TransactionId = (*ProcGlobal).xids;

    // inferred after ProcArrayLock is released
    (*h).catalog_oldest_nonremovable = InvalidTransactionId;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    (*h).latest_completed = (*TransamVariables).latestCompletedXid;

    /*
     * We initialize the MIN() calculation with latestCompletedXid + 1.
     */
    {
        let mut initial: TransactionId;

        initial = XidFromFullTransactionId((*h).latest_completed);
        Assert!(TransactionIdIsValid(initial));
        TransactionIdAdvance(&mut initial);

        (*h).oldest_considered_running = initial;
        (*h).shared_oldest_nonremovable = initial;
        (*h).data_oldest_nonremovable = initial;

        /*
         * Only modifications made by this backend affect the horizon for
         * temporary relations.
         */
        if TransactionIdIsValid((*MyProc).xid) {
            (*h).temp_oldest_nonremovable = (*MyProc).xid;
        } else {
            (*h).temp_oldest_nonremovable = initial;
        }
    }

    /*
     * Fetch slot horizons while ProcArrayLock is held.
     */
    (*h).slot_xmin = (*procArray).replication_slot_xmin;
    (*h).slot_catalog_xmin = (*procArray).replication_slot_catalog_xmin;

    for index in 0..(*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);
        let status_flags: int8 = *(*ProcGlobal).statusFlags.add(index as usize) as int8;
        let xid: TransactionId;
        let xmin: TransactionId;

        // Fetch xid just once - see GetNewTransactionId
        xid = UINT32_ACCESS_ONCE(*other_xids.add(index as usize));
        xmin = UINT32_ACCESS_ONCE((*proc_).xmin);

        /*
         * Consider both the transaction's Xmin, and its Xid.
         */
        let effective_xmin = TransactionIdOlder(xmin, xid);

        // if neither is set, this proc doesn't influence the horizon
        if !TransactionIdIsValid(effective_xmin) {
            continue;
        }

        /*
         * Don't ignore any procs when determining which transactions might be
         * considered running.
         */
        (*h).oldest_considered_running =
            TransactionIdOlder((*h).oldest_considered_running, effective_xmin);

        /*
         * Skip over backends either vacuuming or doing logical decoding.
         */
        if (status_flags as uint8) & (PROC_IN_VACUUM | PROC_IN_LOGICAL_DECODING) != 0 {
            continue;
        }

        // shared tables need to take backends in all databases into account
        (*h).shared_oldest_nonremovable =
            TransactionIdOlder((*h).shared_oldest_nonremovable, effective_xmin);

        /*
         * Normally sessions in other databases are ignored for anything but
         * the shared horizon.  However, include them when MyDatabaseId is not
         * (yet) set, or when in recovery.
         */
        if (*proc_).databaseId == MyDatabaseId ||
            MyDatabaseId == InvalidOid ||
            ((status_flags as uint8) & PROC_AFFECTS_ALL_HORIZONS) != 0 ||
            in_recovery
        {
            (*h).data_oldest_nonremovable =
                TransactionIdOlder((*h).data_oldest_nonremovable, effective_xmin);
        }
    }

    /*
     * If in recovery fetch oldest xid in KnownAssignedXids.
     */
    if in_recovery {
        kaxmin = KnownAssignedXidsGetOldestXmin();
    }

    LWLockRelease(ProcArrayLock());

    if in_recovery {
        (*h).oldest_considered_running =
            TransactionIdOlder((*h).oldest_considered_running, kaxmin);
        (*h).shared_oldest_nonremovable =
            TransactionIdOlder((*h).shared_oldest_nonremovable, kaxmin);
        (*h).data_oldest_nonremovable =
            TransactionIdOlder((*h).data_oldest_nonremovable, kaxmin);
        // temp relations cannot be accessed in recovery
    }

    Assert!(TransactionIdPrecedesOrEquals((*h).oldest_considered_running,
                                          (*h).shared_oldest_nonremovable));
    Assert!(TransactionIdPrecedesOrEquals((*h).shared_oldest_nonremovable,
                                          (*h).data_oldest_nonremovable));

    // Check whether there are replication slots requiring an older xmin.
    (*h).shared_oldest_nonremovable =
        TransactionIdOlder((*h).shared_oldest_nonremovable, (*h).slot_xmin);
    (*h).data_oldest_nonremovable =
        TransactionIdOlder((*h).data_oldest_nonremovable, (*h).slot_xmin);

    /*
     * The only difference between catalog / data horizons is that the slot's
     * catalog xmin is applied to the catalog one.
     */
    (*h).shared_oldest_nonremovable_raw = (*h).shared_oldest_nonremovable;
    (*h).shared_oldest_nonremovable =
        TransactionIdOlder((*h).shared_oldest_nonremovable, (*h).slot_catalog_xmin);
    (*h).catalog_oldest_nonremovable = (*h).data_oldest_nonremovable;
    (*h).catalog_oldest_nonremovable =
        TransactionIdOlder((*h).catalog_oldest_nonremovable, (*h).slot_catalog_xmin);

    /*
     * It's possible that slots backed up the horizons further than
     * oldest_considered_running.
     */
    (*h).oldest_considered_running =
        TransactionIdOlder((*h).oldest_considered_running, (*h).shared_oldest_nonremovable);
    (*h).oldest_considered_running =
        TransactionIdOlder((*h).oldest_considered_running, (*h).catalog_oldest_nonremovable);
    (*h).oldest_considered_running =
        TransactionIdOlder((*h).oldest_considered_running, (*h).data_oldest_nonremovable);

    Assert!(TransactionIdPrecedesOrEquals((*h).shared_oldest_nonremovable,
                                          (*h).data_oldest_nonremovable));
    Assert!(TransactionIdPrecedesOrEquals((*h).shared_oldest_nonremovable,
                                          (*h).catalog_oldest_nonremovable));
    Assert!(TransactionIdPrecedesOrEquals((*h).oldest_considered_running,
                                          (*h).shared_oldest_nonremovable));
    Assert!(TransactionIdPrecedesOrEquals((*h).oldest_considered_running,
                                          (*h).catalog_oldest_nonremovable));
    Assert!(TransactionIdPrecedesOrEquals((*h).oldest_considered_running,
                                          (*h).data_oldest_nonremovable));
    Assert!(TransactionIdPrecedesOrEquals((*h).oldest_considered_running,
                                          (*h).temp_oldest_nonremovable));
    Assert!(!TransactionIdIsValid((*h).slot_xmin) ||
            TransactionIdPrecedesOrEquals((*h).oldest_considered_running, (*h).slot_xmin));
    Assert!(!TransactionIdIsValid((*h).slot_catalog_xmin) ||
            TransactionIdPrecedesOrEquals((*h).oldest_considered_running, (*h).slot_catalog_xmin));

    // update approximate horizons with the computed horizons
    GlobalVisUpdateApply(h);
}

/// Determine what kind of visibility horizon needs to be used for a relation.
/// If rel is NULL, the most conservative horizon is used.
#[inline]
unsafe fn GlobalVisHorizonKindForRel(rel: Relation) -> GlobalVisHorizonKind {
    /*
     * Other relkinds currently don't contain xids, nor always the necessary
     * logical decoding markers.
     */
    // Assert: rel is NULL, RELATION, MATVIEW, or TOASTVALUE

    if rel.is_null() || (*(*rel).rd_rel).relisshared || RecoveryInProgress() {
        VISHORIZON_SHARED
    } else if IsCatalogRelation(rel) || RelationIsAccessibleInLogicalDecoding(rel) {
        VISHORIZON_CATALOG
    } else if !RELATION_IS_LOCAL(rel) {
        VISHORIZON_DATA
    } else {
        VISHORIZON_TEMP
    }
}

/// Return the oldest XID for which deleted tuples must be preserved in the
/// passed table.
pub unsafe fn GetOldestNonRemovableTransactionId(rel: Relation) -> TransactionId {
    let mut horizons: ComputeXidHorizonsResult = core::mem::zeroed();

    ComputeXidHorizons(&mut horizons);

    match GlobalVisHorizonKindForRel(rel) {
        VISHORIZON_SHARED  => horizons.shared_oldest_nonremovable,
        VISHORIZON_CATALOG => horizons.catalog_oldest_nonremovable,
        VISHORIZON_DATA    => horizons.data_oldest_nonremovable,
        VISHORIZON_TEMP    => horizons.temp_oldest_nonremovable,
    }
}

/// Return the oldest transaction id any currently running backend might still
/// consider running.
pub unsafe fn GetOldestTransactionIdConsideredRunning() -> TransactionId {
    let mut horizons: ComputeXidHorizonsResult = core::mem::zeroed();
    ComputeXidHorizons(&mut horizons);
    horizons.oldest_considered_running
}

/// Return the visibility horizons for a hot standby feedback message.
pub unsafe fn GetReplicationHorizons(xmin: *mut TransactionId, catalog_xmin: *mut TransactionId) {
    let mut horizons: ComputeXidHorizonsResult = core::mem::zeroed();
    ComputeXidHorizons(&mut horizons);

    /*
     * Don't want to use shared_oldest_nonremovable here, as that contains the
     * effect of replication slot's catalog_xmin.
     */
    *xmin = horizons.shared_oldest_nonremovable_raw;
    *catalog_xmin = horizons.slot_catalog_xmin;
}

// ---------------------------------------------------------------------------
// GetMaxSnapshot{Xid,Subxid}Count
// ---------------------------------------------------------------------------

/// GetMaxSnapshotXidCount -- get max size for snapshot XID array.
pub unsafe fn GetMaxSnapshotXidCount() -> c_int {
    (*procArray).maxProcs
}

/// GetMaxSnapshotSubxidCount -- get max size for snapshot sub-XID array.
pub unsafe fn GetMaxSnapshotSubxidCount() -> c_int {
    TOTAL_MAX_CACHED_SUBXIDS()
}

// ---------------------------------------------------------------------------
// GetSnapshotData (and helper GetSnapshotDataReuse)
// ---------------------------------------------------------------------------

/// Helper for GetSnapshotData() that checks if the bulk of the visibility
/// information in the snapshot is still valid.
unsafe fn GetSnapshotDataReuse(snapshot: Snapshot) -> bool {
    let cur_xact_completion_count: uint64;

    Assert!(LWLockHeldByMe(ProcArrayLock()));

    if unlikely((*snapshot).snapXactCompletionCount == 0) {
        return false;
    }

    cur_xact_completion_count = (*TransamVariables).xactCompletionCount;
    if cur_xact_completion_count != (*snapshot).snapXactCompletionCount {
        return false;
    }

    /*
     * If the current xactCompletionCount is still the same as it was at the
     * time the snapshot was built, we can be sure that rebuilding the
     * contents of the snapshot the hard way would result in the same snapshot
     * contents.
     */
    if !TransactionIdIsValid((*MyProc).xmin) {
        (*MyProc).xmin = (*snapshot).xmin;
        TransactionXmin = (*snapshot).xmin;
    }

    RecentXmin = (*snapshot).xmin;
    Assert!(TransactionIdPrecedesOrEquals(TransactionXmin, RecentXmin));

    (*snapshot).curcid = GetCurrentCommandId(false);
    (*snapshot).active_count = 0;
    (*snapshot).regd_count = 0;
    (*snapshot).copied = false;

    true
}

#[inline]
fn unlikely(b: bool) -> bool { b }

/// GetSnapshotData -- returns information about running transactions.
///
/// The returned snapshot includes xmin (lowest still-running xact ID),
/// xmax (highest completed xact ID + 1), and a list of running xact IDs
/// in the range xmin <= xid < xmax.
pub unsafe fn GetSnapshotData(snapshot: Snapshot) -> Snapshot {
    let array_p = procArray;
    let other_xids: *mut TransactionId = (*ProcGlobal).xids;
    let mut xmin: TransactionId;
    let mut xmax: TransactionId;
    let mut count: c_int = 0;
    let mut subcount: c_int = 0;
    let mut suboverflowed: bool = false;
    let latest_completed: FullTransactionId;
    let oldestxid: TransactionId;
    let mypgxactoff: c_int;
    let myxid: TransactionId;
    let cur_xact_completion_count: uint64;
    let mut replication_slot_xmin: TransactionId = InvalidTransactionId;
    let mut replication_slot_catalog_xmin: TransactionId = InvalidTransactionId;

    Assert!(!snapshot.is_null());

    /*
     * Allocating space for maxProcs xids is usually overkill; numProcs would
     * be sufficient.  But it seems better to do the malloc while not holding
     * the lock.
     */
    if (*snapshot).xip.is_null() {
        /*
         * First call for this snapshot.
         */
        (*snapshot).xip = malloc(GetMaxSnapshotXidCount() as usize *
                                  core::mem::size_of::<TransactionId>()) as *mut TransactionId;
        if (*snapshot).xip.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
        }
        Assert!((*snapshot).subxip.is_null());
        (*snapshot).subxip = malloc(GetMaxSnapshotSubxidCount() as usize *
                                     core::mem::size_of::<TransactionId>()) as *mut TransactionId;
        if (*snapshot).subxip.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
        }
    }

    /*
     * It is sufficient to get shared lock on ProcArrayLock, even if we are
     * going to set MyProc->xmin.
     */
    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    if GetSnapshotDataReuse(snapshot) {
        LWLockRelease(ProcArrayLock());
        return snapshot;
    }

    latest_completed = (*TransamVariables).latestCompletedXid;
    mypgxactoff = (*MyProc).pgxactoff;
    myxid = *other_xids.add(mypgxactoff as usize);
    Assert!(myxid == (*MyProc).xid);

    oldestxid = (*TransamVariables).oldestXid;
    cur_xact_completion_count = (*TransamVariables).xactCompletionCount;

    // xmax is always latestCompletedXid + 1
    xmax = XidFromFullTransactionId(latest_completed);
    TransactionIdAdvance(&mut xmax);
    Assert!(TransactionIdIsNormal(xmax));

    // initialize xmin calculation with xmax
    xmin = xmax;

    // take own xid into account, saves a check inside the loop
    if TransactionIdIsNormal(myxid) && NormalTransactionIdPrecedes(myxid, xmin) {
        xmin = myxid;
    }

    (*snapshot).takenDuringRecovery = RecoveryInProgress();

    if !(*snapshot).takenDuringRecovery {
        let numprocs = (*array_p).numProcs;
        let xip = (*snapshot).xip;
        let pgprocnos = pgprocnos_ptr(array_p);
        let subxid_states: *mut XidCacheStatus = (*ProcGlobal).subxidStates;
        let all_status_flags: *mut uint8 = (*ProcGlobal).statusFlags;

        /*
         * First collect set of pgxactoff/xids that need to be included in the
         * snapshot.
         */
        for pgxactoff in 0..numprocs {
            // Fetch xid just once - see GetNewTransactionId
            let xid = UINT32_ACCESS_ONCE(*other_xids.add(pgxactoff as usize));
            let status_flags: uint8;

            Assert!((*allProcs.add(*pgprocnos.add(pgxactoff as usize) as usize)).pgxactoff == pgxactoff);

            /*
             * If the transaction has no XID assigned, we can skip it.
             */
            if likely(xid == InvalidTransactionId) {
                continue;
            }

            /*
             * We don't include our own XIDs (if any) in the snapshot.
             */
            if pgxactoff == mypgxactoff {
                continue;
            }

            Assert!(TransactionIdIsNormal(xid));

            /*
             * If the XID is >= xmax, we can skip it.
             */
            if !NormalTransactionIdPrecedes(xid, xmax) {
                continue;
            }

            /*
             * Skip over backends doing logical decoding or running LAZY VACUUM.
             */
            status_flags = *all_status_flags.add(pgxactoff as usize);
            if status_flags & (PROC_IN_LOGICAL_DECODING | PROC_IN_VACUUM) != 0 {
                continue;
            }

            if NormalTransactionIdPrecedes(xid, xmin) {
                xmin = xid;
            }

            // Add XID to snapshot.
            *xip.add(count as usize) = xid;
            count += 1;

            /*
             * Save subtransaction XIDs if possible.
             */
            if !suboverflowed {
                if (*subxid_states.add(pgxactoff as usize)).overflowed {
                    suboverflowed = true;
                } else {
                    let nsubxids = (*subxid_states.add(pgxactoff as usize)).count as c_int;

                    if nsubxids > 0 {
                        let pgprocno = *pgprocnos.add(pgxactoff as usize);
                        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

                        pg_read_barrier(); // pairs with GetNewTransactionId

                        core::ptr::copy_nonoverlapping(
                            (*proc_).subxids.xids.as_ptr(),
                            (*snapshot).subxip.add(subcount as usize),
                            nsubxids as usize,
                        );
                        subcount += nsubxids;
                    }
                }
            }
        }
    } else {
        /*
         * We're in hot standby, so get XIDs from KnownAssignedXids.
         * We store all xids directly into subxip[].
         */
        subcount = KnownAssignedXidsGetAndSetXmin((*snapshot).subxip, &mut xmin, xmax);

        if TransactionIdPrecedesOrEquals(xmin, (*procArray).lastOverflowedXid) {
            suboverflowed = true;
        }
    }

    /*
     * Fetch into local variable while ProcArrayLock is held.
     */
    replication_slot_xmin = (*procArray).replication_slot_xmin;
    replication_slot_catalog_xmin = (*procArray).replication_slot_catalog_xmin;

    if !TransactionIdIsValid((*MyProc).xmin) {
        (*MyProc).xmin = xmin;
        TransactionXmin = xmin;
    }

    LWLockRelease(ProcArrayLock());

    // maintain state for GlobalVis*
    {
        let def_vis_xid: TransactionId;
        let def_vis_xid_data: TransactionId;
        let def_vis_fxid: FullTransactionId;
        let def_vis_fxid_data: FullTransactionId;
        let oldestfxid: FullTransactionId;

        /*
         * Converting oldestXid is only safe when xid horizon cannot advance.
         */
        oldestfxid = FullXidRelativeTo(latest_completed, oldestxid);

        // Check whether there's a replication slot requiring an older xmin.
        def_vis_xid_data = TransactionIdOlder(xmin, replication_slot_xmin);

        // Rows in non-shared, non-catalog tables possibly could be vacuumed if older.
        let mut def_vis_xid_tmp = def_vis_xid_data;

        // Check whether there's a replication slot requiring an older catalog xmin.
        def_vis_xid_tmp = TransactionIdOlder(replication_slot_catalog_xmin, def_vis_xid_tmp);
        def_vis_xid = def_vis_xid_tmp;

        def_vis_fxid = FullXidRelativeTo(latest_completed, def_vis_xid);
        def_vis_fxid_data = FullXidRelativeTo(latest_completed, def_vis_xid_data);

        /*
         * Check if we can increase upper bound.
         */
        GlobalVisSharedRels.definitely_needed =
            FullTransactionIdNewer(def_vis_fxid, GlobalVisSharedRels.definitely_needed);
        GlobalVisCatalogRels.definitely_needed =
            FullTransactionIdNewer(def_vis_fxid, GlobalVisCatalogRels.definitely_needed);
        GlobalVisDataRels.definitely_needed =
            FullTransactionIdNewer(def_vis_fxid_data, GlobalVisDataRels.definitely_needed);
        // See temp_oldest_nonremovable computation in ComputeXidHorizons()
        if TransactionIdIsNormal(myxid) {
            GlobalVisTempRels.definitely_needed =
                FullXidRelativeTo(latest_completed, myxid);
        } else {
            GlobalVisTempRels.definitely_needed = latest_completed;
            FullTransactionIdAdvance(&mut GlobalVisTempRels.definitely_needed);
        }

        /*
         * Check if we know that we can initialize or increase the lower bound.
         */
        GlobalVisSharedRels.maybe_needed =
            FullTransactionIdNewer(GlobalVisSharedRels.maybe_needed, oldestfxid);
        GlobalVisCatalogRels.maybe_needed =
            FullTransactionIdNewer(GlobalVisCatalogRels.maybe_needed, oldestfxid);
        GlobalVisDataRels.maybe_needed =
            FullTransactionIdNewer(GlobalVisDataRels.maybe_needed, oldestfxid);
        // accurate value known
        GlobalVisTempRels.maybe_needed = GlobalVisTempRels.definitely_needed;
    }

    RecentXmin = xmin;
    Assert!(TransactionIdPrecedesOrEquals(TransactionXmin, RecentXmin));

    (*snapshot).xmin = xmin;
    (*snapshot).xmax = xmax;
    (*snapshot).xcnt = count as u32;
    (*snapshot).subxcnt = subcount;
    (*snapshot).suboverflowed = suboverflowed;
    (*snapshot).snapXactCompletionCount = cur_xact_completion_count;

    (*snapshot).curcid = GetCurrentCommandId(false);

    // This is a new snapshot, so both refcounts are zero.
    (*snapshot).active_count = 0;
    (*snapshot).regd_count = 0;
    (*snapshot).copied = false;

    snapshot
}

#[inline]
fn likely(b: bool) -> bool { b }

// ---------------------------------------------------------------------------
// ProcArrayInstallImportedXmin / ProcArrayInstallRestoredXmin
// ---------------------------------------------------------------------------

/// ProcArrayInstallImportedXmin -- install imported xmin into MyProc->xmin.
///
/// This is called when installing a snapshot imported from another
/// transaction.
pub unsafe fn ProcArrayInstallImportedXmin(xmin: TransactionId,
                                           sourcevxid: *const VirtualTransactionId) -> bool {
    let mut result = false;
    let array_p = procArray;
    let mut index: c_int;

    Assert!(TransactionIdIsNormal(xmin));
    if sourcevxid.is_null() {
        return false;
    }

    // Get lock so source xact can't end while we're doing this
    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);
        let status_flags: c_int = *(*ProcGlobal).statusFlags.add(index as usize) as c_int;
        let xid: TransactionId;

        // Ignore procs running LAZY VACUUM
        if (status_flags as uint8) & PROC_IN_VACUUM != 0 {
            index += 1;
            continue;
        }

        // We are only interested in the specific virtual transaction.
        if (*proc_).vxid.procNumber != (*sourcevxid).procNumber {
            index += 1;
            continue;
        }
        if (*proc_).vxid.lxid != (*sourcevxid).localTransactionId {
            index += 1;
            continue;
        }

        /*
         * We check the transaction's database ID for paranoia's sake.
         */
        if (*proc_).databaseId != MyDatabaseId {
            index += 1;
            continue;
        }

        // Likewise, let's just make real sure its xmin does cover us.
        xid = UINT32_ACCESS_ONCE((*proc_).xmin);
        if !TransactionIdIsNormal(xid) ||
            !TransactionIdPrecedesOrEquals(xid, xmin)
        {
            index += 1;
            continue;
        }

        /*
         * We're good.  Install the new xmin.
         */
        (*MyProc).xmin = xmin;
        TransactionXmin = xmin;

        result = true;
        break;
    }

    LWLockRelease(ProcArrayLock());

    result
}

/// ProcArrayInstallRestoredXmin -- install restored xmin into MyProc->xmin.
///
/// This is like ProcArrayInstallImportedXmin, but we have a pointer to the
/// PGPROC of the transaction from which we imported the snapshot.
pub unsafe fn ProcArrayInstallRestoredXmin(xmin: TransactionId, proc_: *mut PGPROC) -> bool {
    let mut result = false;
    let xid: TransactionId;

    Assert!(TransactionIdIsNormal(xmin));
    Assert!(!proc_.is_null());

    // Get an exclusive lock so that we can copy statusFlags from source proc.
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    /*
     * Be certain that the referenced PGPROC has an advertised xmin which is
     * no later than the one we're installing.
     */
    xid = UINT32_ACCESS_ONCE((*proc_).xmin);
    if (*proc_).databaseId == MyDatabaseId &&
        TransactionIdIsNormal(xid) &&
        TransactionIdPrecedesOrEquals(xid, xmin)
    {
        /*
         * Install xmin and propagate the statusFlags that affect how the
         * value is interpreted by vacuum.
         */
        (*MyProc).xmin = xmin;
        TransactionXmin = xmin;
        (*MyProc).statusFlags = ((*MyProc).statusFlags & !PROC_XMIN_FLAGS) |
            ((*proc_).statusFlags & PROC_XMIN_FLAGS);
        *(*ProcGlobal).statusFlags.add((*MyProc).pgxactoff as usize) = (*MyProc).statusFlags;

        result = true;
    }

    LWLockRelease(ProcArrayLock());

    result
}

// ---------------------------------------------------------------------------
// GetRunningTransactionData
// ---------------------------------------------------------------------------

/// GetRunningTransactionData -- returns information about running transactions.
///
/// Similar to GetSnapshotData but returns more information.
pub unsafe fn GetRunningTransactionData() -> RunningTransactions {
    // result workspace
    static mut current_running_xacts_data: RunningTransactionsData = RunningTransactionsData {
        xcnt: 0, subxcnt: 0,
        subxid_status: SUBXIDS_IN_ARRAY,
        nextXid: 0, oldestRunningXid: 0, oldestDatabaseRunningXid: 0,
        latestCompletedXid: 0, xids: core::ptr::null_mut(),
    };

    let array_p = procArray;
    let other_xids: *mut TransactionId = (*ProcGlobal).xids;
    let current_running_xacts: RunningTransactions = &raw mut current_running_xacts_data;
    let latest_completed_xid: TransactionId;
    let mut oldest_running_xid: TransactionId;
    let mut oldest_database_running_xid: TransactionId;
    let xids: *mut TransactionId;
    let mut index: c_int;
    let mut count: c_int;
    let mut subcount: c_int;
    let mut suboverflowed: bool;

    Assert!(!RecoveryInProgress());

    /*
     * Allocating space for maxProcs xids is usually overkill.  Should only
     * be allocated in bgwriter, since only ever executed during checkpoints.
     */
    if (*current_running_xacts).xids.is_null() {
        (*current_running_xacts).xids = malloc(
            TOTAL_MAX_CACHED_SUBXIDS() as usize * core::mem::size_of::<TransactionId>()
        ) as *mut TransactionId;
        if (*current_running_xacts).xids.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
        }
    }

    xids = (*current_running_xacts).xids;

    count = 0;
    subcount = 0;
    suboverflowed = false;

    /*
     * Ensure that no xids enter or leave the procarray while we obtain snapshot.
     */
    LWLockAcquire(ProcArrayLock(), LW_SHARED);
    LWLockAcquire(XidGenLock(), LW_SHARED);

    latest_completed_xid =
        XidFromFullTransactionId((*TransamVariables).latestCompletedXid);
    oldest_database_running_xid = XidFromFullTransactionId((*TransamVariables).nextXid);
    oldest_running_xid = oldest_database_running_xid;

    // Spin over procArray collecting all xids
    index = 0;
    while index < (*array_p).numProcs {
        let xid: TransactionId;

        // Fetch xid just once - see GetNewTransactionId
        xid = UINT32_ACCESS_ONCE(*other_xids.add(index as usize));

        /*
         * We don't need to store transactions that don't have a TransactionId
         * yet because they will not show as running on a standby server.
         */
        if !TransactionIdIsValid(xid) {
            index += 1;
            continue;
        }

        if TransactionIdPrecedes(xid, oldest_running_xid) {
            oldest_running_xid = xid;
        }

        /*
         * Also, update the oldest running xid within the current database.
         */
        if TransactionIdPrecedes(xid, oldest_database_running_xid) {
            let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
            let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

            if (*proc_).databaseId == MyDatabaseId {
                oldest_database_running_xid = xid;
            }
        }

        if (*(*ProcGlobal).subxidStates.add(index as usize)).overflowed {
            suboverflowed = true;
        }

        *xids.add(count as usize) = xid;
        count += 1;
        index += 1;
    }

    // Spin over procArray collecting all subxids, but only if no suboverflow.
    if !suboverflowed {
        let other_subxidstates: *mut XidCacheStatus = (*ProcGlobal).subxidStates;

        index = 0;
        while index < (*array_p).numProcs {
            let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
            let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);
            let nsubxids: c_int;

            // Save subtransaction XIDs.
            nsubxids = (*other_subxidstates.add(index as usize)).count as c_int;
            if nsubxids > 0 {
                // barrier not really required, as XidGenLock is held, but ...
                pg_read_barrier(); // pairs with GetNewTransactionId

                core::ptr::copy_nonoverlapping(
                    (*proc_).subxids.xids.as_ptr(),
                    xids.add(count as usize),
                    nsubxids as usize,
                );
                count += nsubxids;
                subcount += nsubxids;

                /*
                 * Top-level XID of a transaction is always less than any of
                 * its subxids, so we don't need to check oldestRunningXid.
                 */
            }
            index += 1;
        }
    }

    (*current_running_xacts).xcnt = count - subcount;
    (*current_running_xacts).subxcnt = subcount;
    (*current_running_xacts).subxid_status =
        if suboverflowed { SUBXIDS_IN_SUBTRANS } else { SUBXIDS_IN_ARRAY };
    (*current_running_xacts).nextXid = XidFromFullTransactionId((*TransamVariables).nextXid);
    (*current_running_xacts).oldestRunningXid = oldest_running_xid;
    (*current_running_xacts).oldestDatabaseRunningXid = oldest_database_running_xid;
    (*current_running_xacts).latestCompletedXid = latest_completed_xid;

    Assert!(TransactionIdIsValid((*current_running_xacts).nextXid));
    Assert!(TransactionIdIsValid((*current_running_xacts).oldestRunningXid));
    Assert!(TransactionIdIsNormal((*current_running_xacts).latestCompletedXid));

    // We don't release the locks here, the caller is responsible for that

    current_running_xacts
}

// ---------------------------------------------------------------------------
// GetOldestActiveTransactionId / GetOldestSafeDecodingTransactionId
// ---------------------------------------------------------------------------

/// GetOldestActiveTransactionId() -- Similar to GetSnapshotData but returns
/// just oldestActiveXid.
pub unsafe fn GetOldestActiveTransactionId() -> TransactionId {
    let array_p = procArray;
    let other_xids: *mut TransactionId = (*ProcGlobal).xids;
    let mut oldest_running_xid: TransactionId;
    let mut index: c_int;

    Assert!(!RecoveryInProgress());

    /*
     * Read nextXid, as the upper bound of what's still active.
     */
    LWLockAcquire(XidGenLock(), LW_SHARED);
    oldest_running_xid = XidFromFullTransactionId((*TransamVariables).nextXid);
    LWLockRelease(XidGenLock());

    // Spin over procArray collecting all xids and subxids.
    LWLockAcquire(ProcArrayLock(), LW_SHARED);
    index = 0;
    while index < (*array_p).numProcs {
        let xid: TransactionId;

        // Fetch xid just once - see GetNewTransactionId
        xid = UINT32_ACCESS_ONCE(*other_xids.add(index as usize));

        if !TransactionIdIsNormal(xid) {
            index += 1;
            continue;
        }

        if TransactionIdPrecedes(xid, oldest_running_xid) {
            oldest_running_xid = xid;
        }

        /*
         * Top-level XID of a transaction is always less than any of its
         * subxids, so we don't need to check if any of the subxids are
         * smaller than oldestRunningXid.
         */
        index += 1;
    }
    LWLockRelease(ProcArrayLock());

    oldest_running_xid
}

/// GetOldestSafeDecodingTransactionId -- lowest xid not affected by vacuum.
///
/// Must be called with ProcArrayLock held.
pub unsafe fn GetOldestSafeDecodingTransactionId(catalog_only: bool) -> TransactionId {
    let array_p = procArray;
    let mut oldest_safe_xid: TransactionId;
    let mut index: c_int;
    let recovery_in_progress = RecoveryInProgress();

    Assert!(LWLockHeldByMe(ProcArrayLock()));

    /*
     * Acquire XidGenLock, so no transactions can acquire an xid while we're
     * running.  We initialize the computation to nextXid.
     */
    LWLockAcquire(XidGenLock(), LW_SHARED);
    oldest_safe_xid = XidFromFullTransactionId((*TransamVariables).nextXid);

    /*
     * If there's already a slot pegging the xmin horizon, we can start with
     * that value.
     */
    if TransactionIdIsValid((*procArray).replication_slot_xmin) &&
        TransactionIdPrecedes((*procArray).replication_slot_xmin, oldest_safe_xid)
    {
        oldest_safe_xid = (*procArray).replication_slot_xmin;
    }

    if catalog_only &&
        TransactionIdIsValid((*procArray).replication_slot_catalog_xmin) &&
        TransactionIdPrecedes((*procArray).replication_slot_catalog_xmin, oldest_safe_xid)
    {
        oldest_safe_xid = (*procArray).replication_slot_catalog_xmin;
    }

    if !recovery_in_progress {
        let other_xids: *mut TransactionId = (*ProcGlobal).xids;

        // Spin over procArray collecting min(ProcGlobal->xids[i])
        index = 0;
        while index < (*array_p).numProcs {
            let xid: TransactionId;

            // Fetch xid just once - see GetNewTransactionId
            xid = UINT32_ACCESS_ONCE(*other_xids.add(index as usize));

            if !TransactionIdIsNormal(xid) {
                index += 1;
                continue;
            }

            if TransactionIdPrecedes(xid, oldest_safe_xid) {
                oldest_safe_xid = xid;
            }
            index += 1;
        }
    }

    LWLockRelease(XidGenLock());

    oldest_safe_xid
}

// ---------------------------------------------------------------------------
// GetVirtualXIDsDelayingChkpt / HaveVirtualXIDsDelayingChkpt
// ---------------------------------------------------------------------------

/// GetVirtualXIDsDelayingChkpt -- Get the VXIDs of transactions that are
/// delaying checkpoint.
///
/// Returns a palloc'd array; *nvxids is the number of valid entries.
pub unsafe fn GetVirtualXIDsDelayingChkpt(nvxids: *mut c_int, type_: c_int) -> *mut VirtualTransactionId {
    let vxids: *mut VirtualTransactionId;
    let array_p = procArray;
    let mut count: c_int = 0;
    let mut index: c_int;

    Assert!(type_ != 0);

    // allocate what's certainly enough result space
    vxids = palloc(core::mem::size_of::<VirtualTransactionId>() as Size *
                   (*array_p).maxProcs as Size) as *mut VirtualTransactionId;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

        if ((*proc_).delayChkptFlags & type_) != 0 {
            let mut vxid: VirtualTransactionId = core::mem::zeroed();

            GET_VXID_FROM_PGPROC(&mut vxid, &*proc_);
            if VirtualTransactionIdIsValid(vxid) {
                *vxids.add(count as usize) = vxid;
                count += 1;
            }
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());

    *nvxids = count;
    vxids
}

/// HaveVirtualXIDsDelayingChkpt -- Are any of the specified VXIDs delaying?
pub unsafe fn HaveVirtualXIDsDelayingChkpt(vxids: *const VirtualTransactionId,
                                            nvxids: c_int,
                                            type_: c_int) -> bool {
    let mut result = false;
    let array_p = procArray;
    let mut index: c_int;

    Assert!(type_ != 0);

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    'outer: {
        index = 0;
        while index < (*array_p).numProcs {
            let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
            let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);
            let mut vxid: VirtualTransactionId = core::mem::zeroed();

            GET_VXID_FROM_PGPROC(&mut vxid, &*proc_);

            if ((*proc_).delayChkptFlags & type_) != 0 && VirtualTransactionIdIsValid(vxid) {
                let mut i: c_int = 0;
                while i < nvxids {
                    if VirtualTransactionIdEquals(vxid, *vxids.add(i as usize)) {
                        result = true;
                        break 'outer;
                    }
                    i += 1;
                }
            }
            index += 1;
        }
    }

    LWLockRelease(ProcArrayLock());

    result
}

// ---------------------------------------------------------------------------
// ProcNumberGetProc / ProcNumberGetTransactionIds
// ---------------------------------------------------------------------------

/// ProcNumberGetProc -- get a backend's PGPROC given its proc number.
pub unsafe fn ProcNumberGetProc(proc_number: ProcNumber) -> *mut PGPROC {
    let result: *mut PGPROC;

    if proc_number < 0 || proc_number >= (*ProcGlobal).allProcCount as c_int {
        return core::ptr::null_mut();
    }
    result = GetPGProcByNumber(proc_number);

    if (*result).pid == 0 {
        return core::ptr::null_mut();
    }

    result
}

/// ProcNumberGetTransactionIds -- get a backend's transaction status.
pub unsafe fn ProcNumberGetTransactionIds(proc_number: ProcNumber,
                                          xid: *mut TransactionId,
                                          xmin: *mut TransactionId,
                                          nsubxid: *mut c_int,
                                          overflowed: *mut bool) {
    let proc_: *mut PGPROC;

    *xid = InvalidTransactionId;
    *xmin = InvalidTransactionId;
    *nsubxid = 0;
    *overflowed = false;

    if proc_number < 0 || proc_number >= (*ProcGlobal).allProcCount as c_int {
        return;
    }
    proc_ = GetPGProcByNumber(proc_number);

    // Need to lock out additions/removals of backends
    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    if (*proc_).pid != 0 {
        *xid = (*proc_).xid;
        *xmin = (*proc_).xmin;
        *nsubxid = (*proc_).subxidStatus.count as c_int;
        *overflowed = (*proc_).subxidStatus.overflowed;
    }

    LWLockRelease(ProcArrayLock());
}

// ---------------------------------------------------------------------------
// BackendPidGetProc / BackendPidGetProcWithLock / BackendXidGetPid / IsBackendPid
// ---------------------------------------------------------------------------

/// BackendPidGetProc -- get a backend's PGPROC given its PID.
pub unsafe fn BackendPidGetProc(pid: c_int) -> *mut PGPROC {
    let result: *mut PGPROC;

    if pid == 0 { // never match dummy PGPROCs
        return core::ptr::null_mut();
    }

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    result = BackendPidGetProcWithLock(pid);

    LWLockRelease(ProcArrayLock());

    result
}

/// BackendPidGetProcWithLock -- get a backend's PGPROC given its PID.
/// Caller must be holding ProcArrayLock.
pub unsafe fn BackendPidGetProcWithLock(pid: c_int) -> *mut PGPROC {
    let mut result: *mut PGPROC = core::ptr::null_mut();
    let array_p = procArray;
    let mut index: c_int;

    if pid == 0 { // never match dummy PGPROCs
        return core::ptr::null_mut();
    }

    index = 0;
    while index < (*array_p).numProcs {
        let proc_: *mut PGPROC = allProcs.add(*pgprocnos_ptr(array_p).add(index as usize) as usize);

        if (*proc_).pid == pid {
            result = proc_;
            break;
        }
        index += 1;
    }

    result
}

/// BackendXidGetPid -- get a backend's pid given its XID.
pub unsafe fn BackendXidGetPid(xid: TransactionId) -> c_int {
    let mut result: c_int = 0;
    let array_p = procArray;
    let other_xids: *mut TransactionId = (*ProcGlobal).xids;
    let mut index: c_int;

    if xid == InvalidTransactionId { // never match invalid xid
        return 0;
    }

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        if *other_xids.add(index as usize) == xid {
            let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
            let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

            result = (*proc_).pid;
            break;
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());

    result
}

/// IsBackendPid -- is a given pid a running backend.
pub unsafe fn IsBackendPid(pid: c_int) -> bool {
    !BackendPidGetProc(pid).is_null()
}

// ---------------------------------------------------------------------------
// GetCurrentVirtualXIDs / GetConflictingVirtualXIDs
// ---------------------------------------------------------------------------

/// GetCurrentVirtualXIDs -- returns an array of currently active VXIDs.
///
/// The array is palloc'd.  The number of valid entries is returned into *nvxids.
pub unsafe fn GetCurrentVirtualXIDs(limit_xmin: TransactionId,
                                    exclude_xmin0: bool,
                                    all_dbs: bool,
                                    exclude_vacuum: c_int,
                                    nvxids: *mut c_int) -> *mut VirtualTransactionId {
    let vxids: *mut VirtualTransactionId;
    let array_p = procArray;
    let mut count: c_int = 0;
    let mut index: c_int;

    // allocate what's certainly enough result space
    vxids = palloc(core::mem::size_of::<VirtualTransactionId>() as Size *
                   (*array_p).maxProcs as Size) as *mut VirtualTransactionId;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);
        let status_flags: uint8 = *(*ProcGlobal).statusFlags.add(index as usize);

        if proc_ == MyProc {
            index += 1;
            continue;
        }

        if (exclude_vacuum as uint8) & status_flags != 0 {
            index += 1;
            continue;
        }

        if all_dbs || (*proc_).databaseId == MyDatabaseId {
            // Fetch xmin just once - might change on us
            let pxmin = UINT32_ACCESS_ONCE((*proc_).xmin);

            if exclude_xmin0 && !TransactionIdIsValid(pxmin) {
                index += 1;
                continue;
            }

            /*
             * InvalidTransactionId precedes all other XIDs, so a proc that
             * hasn't set xmin yet will not be rejected by this test.
             */
            if !TransactionIdIsValid(limit_xmin) ||
                TransactionIdPrecedesOrEquals(pxmin, limit_xmin)
            {
                let mut vxid: VirtualTransactionId = core::mem::zeroed();

                GET_VXID_FROM_PGPROC(&mut vxid, &*proc_);
                if VirtualTransactionIdIsValid(vxid) {
                    *vxids.add(count as usize) = vxid;
                    count += 1;
                }
            }
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());

    *nvxids = count;
    vxids
}

/// GetConflictingVirtualXIDs -- returns an array of currently active VXIDs.
///
/// Usage is limited to conflict resolution during recovery on standby servers.
pub unsafe fn GetConflictingVirtualXIDs(limit_xmin: TransactionId,
                                        db_oid: Oid) -> *mut VirtualTransactionId {
    static mut vxids_static: *mut VirtualTransactionId = core::ptr::null_mut();
    let array_p = procArray;
    let mut count: c_int = 0;
    let mut index: c_int;

    /*
     * If first time through, get workspace to remember main XIDs in.
     * We malloc it permanently.  Allow result space, remembering room for a terminator.
     */
    if vxids_static.is_null() {
        vxids_static = malloc(core::mem::size_of::<VirtualTransactionId>() *
                              ((*array_p).maxProcs as usize + 1)) as *mut VirtualTransactionId;
        if vxids_static.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
        }
    }

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

        // Exclude prepared transactions
        if (*proc_).pid == 0 {
            index += 1;
            continue;
        }

        if !OidIsValid(db_oid) || (*proc_).databaseId == db_oid {
            // Fetch xmin just once - can't change on us, but good coding
            let pxmin = UINT32_ACCESS_ONCE((*proc_).xmin);

            /*
             * We ignore an invalid pxmin because this means that backend has
             * no snapshot currently.
             */
            if !TransactionIdIsValid(limit_xmin) ||
                (TransactionIdIsValid(pxmin) && !TransactionIdFollows(pxmin, limit_xmin))
            {
                let mut vxid: VirtualTransactionId = core::mem::zeroed();

                GET_VXID_FROM_PGPROC(&mut vxid, &*proc_);
                if VirtualTransactionIdIsValid(vxid) {
                    *vxids_static.add(count as usize) = vxid;
                    count += 1;
                }
            }
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());

    // add the terminator
    (*vxids_static.add(count as usize)).procNumber = INVALID_PROC_NUMBER;
    (*vxids_static.add(count as usize)).localTransactionId = InvalidLocalTransactionId;

    vxids_static
}

// ---------------------------------------------------------------------------
// CancelVirtualTransaction / SignalVirtualTransaction
// ---------------------------------------------------------------------------

/// CancelVirtualTransaction - used in recovery conflict processing.
///
/// Returns pid of the process signaled, or 0 if not found.
pub unsafe fn CancelVirtualTransaction(vxid: VirtualTransactionId,
                                       sigmode: ProcSignalReason) -> c_int {
    SignalVirtualTransaction(vxid, sigmode, true)
}

pub unsafe fn SignalVirtualTransaction(vxid: VirtualTransactionId,
                                       sigmode: ProcSignalReason,
                                       conflict_pending: bool) -> c_int {
    let array_p = procArray;
    let mut index: c_int;
    let mut pid: c_int = 0;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);
        let mut procvxid: VirtualTransactionId = core::mem::zeroed();

        GET_VXID_FROM_PGPROC(&mut procvxid, &*proc_);

        if procvxid.procNumber == vxid.procNumber &&
            procvxid.localTransactionId == vxid.localTransactionId
        {
            (*proc_).recoveryConflictPending = conflict_pending;
            pid = (*proc_).pid;
            if pid != 0 {
                /*
                 * Kill the pid if it's still here.
                 */
                let _ = SendProcSignal(pid, sigmode, vxid.procNumber);
            }
            break;
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());

    pid
}

// ---------------------------------------------------------------------------
// MinimumActiveBackends
// ---------------------------------------------------------------------------

/// MinimumActiveBackends --- count backends (other than myself) that are
/// in active transactions.  Return true if the count exceeds the minimum threshold.
pub unsafe fn MinimumActiveBackends(min: c_int) -> bool {
    let array_p = procArray;
    let mut count: c_int = 0;
    let mut index: c_int;

    // Quick short-circuit if no minimum is specified
    if min == 0 {
        return true;
    }

    /*
     * Note: for speed, we don't acquire ProcArrayLock.  This is a little bit
     * bogus, but since we are only testing fields for zero or nonzero, it
     * should be OK.  The result is only used for heuristic purposes anyway...
     */
    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

        /*
         * Since we're not holding a lock, need to be prepared to deal with
         * garbage.
         */
        if pgprocno == -1 {
            index += 1;
            continue; // do not count deleted entries
        }
        if proc_ == MyProc {
            index += 1;
            continue; // do not count myself
        }
        if (*proc_).xid == InvalidTransactionId {
            index += 1;
            continue; // do not count if no XID assigned
        }
        if (*proc_).pid == 0 {
            index += 1;
            continue; // do not count prepared xacts
        }
        if !(*proc_).waitLock.is_null() {
            index += 1;
            continue; // do not count if blocked on a lock
        }
        count += 1;
        if count >= min {
            break;
        }
        index += 1;
    }

    count >= min
}

// ---------------------------------------------------------------------------
// CountDBBackends / CountDBConnections / CancelDBBackends / CountUserBackends
// ---------------------------------------------------------------------------

/// CountDBBackends --- count backends that are using specified database.
pub unsafe fn CountDBBackends(databaseid: Oid) -> c_int {
    let array_p = procArray;
    let mut count: c_int = 0;
    let mut index: c_int;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

        if (*proc_).pid == 0 {
            index += 1;
            continue; // do not count prepared xacts
        }
        if !OidIsValid(databaseid) || (*proc_).databaseId == databaseid {
            count += 1;
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());

    count
}

/// CountDBConnections --- counts database backends (only regular backends).
pub unsafe fn CountDBConnections(databaseid: Oid) -> c_int {
    let array_p = procArray;
    let mut count: c_int = 0;
    let mut index: c_int;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

        if (*proc_).pid == 0 {
            index += 1;
            continue; // do not count prepared xacts
        }
        if !(*proc_).isRegularBackend {
            index += 1;
            continue; // count only regular backend processes
        }
        if !OidIsValid(databaseid) || (*proc_).databaseId == databaseid {
            count += 1;
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());

    count
}

/// CancelDBBackends --- cancel backends that are using specified database.
pub unsafe fn CancelDBBackends(databaseid: Oid, sigmode: ProcSignalReason, conflict_pending: bool) {
    let array_p = procArray;
    let mut index: c_int;

    // tell all backends to die
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

        if databaseid == InvalidOid || (*proc_).databaseId == databaseid {
            let mut procvxid: VirtualTransactionId = core::mem::zeroed();
            let pid: c_int;

            GET_VXID_FROM_PGPROC(&mut procvxid, &*proc_);

            (*proc_).recoveryConflictPending = conflict_pending;
            pid = (*proc_).pid;
            if pid != 0 {
                // Kill the pid if it's still here.
                let _ = SendProcSignal(pid, sigmode, procvxid.procNumber);
            }
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());
}

/// CountUserBackends --- count backends that are used by specified user.
pub unsafe fn CountUserBackends(roleid: Oid) -> c_int {
    let array_p = procArray;
    let mut count: c_int = 0;
    let mut index: c_int;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    index = 0;
    while index < (*array_p).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

        if (*proc_).pid == 0 {
            index += 1;
            continue; // do not count prepared xacts
        }
        if !(*proc_).isRegularBackend {
            index += 1;
            continue; // count only regular backend processes
        }
        if (*proc_).roleId == roleid {
            count += 1;
        }
        index += 1;
    }

    LWLockRelease(ProcArrayLock());

    count
}

// ---------------------------------------------------------------------------
// CountOtherDBBackends / TerminateOtherDBBackends
// ---------------------------------------------------------------------------

/// CountOtherDBBackends -- check for other backends running in the given DB.
///
/// If there are other backends in the DB, we will wait a maximum of 5 seconds
/// for them to exit.
pub unsafe fn CountOtherDBBackends(database_id: Oid,
                                   nbackends: *mut c_int,
                                   nprepared: *mut c_int) -> bool {
    let array_p = procArray;

    const MAXAUTOVACPIDS: usize = 10; // max autovacs to SIGTERM per iteration
    let mut autovac_pids: [c_int; MAXAUTOVACPIDS] = [0; MAXAUTOVACPIDS];
    let mut tries: c_int;

    // 50 tries with 100ms sleep between tries makes 5 sec total wait
    tries = 0;
    while tries < 50 {
        let mut nautovacs: c_int = 0;
        let mut found = false;
        let mut index: c_int;

        CHECK_FOR_INTERRUPTS();

        *nbackends = 0;
        *nprepared = 0;

        LWLockAcquire(ProcArrayLock(), LW_SHARED);

        index = 0;
        while index < (*array_p).numProcs {
            let pgprocno = *pgprocnos_ptr(array_p).add(index as usize);
            let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);
            let status_flags: uint8 = *(*ProcGlobal).statusFlags.add(index as usize);

            if (*proc_).databaseId != database_id {
                index += 1;
                continue;
            }
            if proc_ == MyProc {
                index += 1;
                continue;
            }

            found = true;

            if (*proc_).pid == 0 {
                *nprepared += 1;
            } else {
                *nbackends += 1;
                if (status_flags & PROC_IS_AUTOVACUUM) != 0 &&
                    nautovacs < MAXAUTOVACPIDS as c_int
                {
                    autovac_pids[nautovacs as usize] = (*proc_).pid;
                    nautovacs += 1;
                }
            }
            index += 1;
        }

        LWLockRelease(ProcArrayLock());

        if !found {
            return false; // no conflicting backends, so done
        }

        /*
         * Send SIGTERM to any conflicting autovacuums before sleeping.
         */
        for av_index in 0..nautovacs as usize {
            let _ = kill(autovac_pids[av_index], SIGTERM); // ignore any error
        }

        // sleep, then try again
        pg_usleep(100 * 1000); // 100ms

        tries += 1;
    }

    true // timed out, still conflicts
}

/// TerminateOtherDBBackends - terminate existing connections to the specified
/// database.  Used by DROP DATABASE with FORCE.
pub unsafe fn TerminateOtherDBBackends(database_id: Oid) {
    let array_p = procArray;
    let mut pids: *mut List = NIL;
    let mut nprepared: c_int = 0;
    let mut i: c_int;

    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    i = 0;
    while i < (*procArray).numProcs {
        let pgprocno = *pgprocnos_ptr(array_p).add(i as usize);
        let proc_: *mut PGPROC = allProcs.add(pgprocno as usize);

        if (*proc_).databaseId != database_id {
            i += 1;
            continue;
        }
        if proc_ == MyProc {
            i += 1;
            continue;
        }

        if (*proc_).pid != 0 {
            pids = lappend_int(pids, (*proc_).pid);
        } else {
            nprepared += 1;
        }
        i += 1;
    }

    LWLockRelease(ProcArrayLock());

    if nprepared > 0 {
        ereport!(ERROR,
                 errmsg!("database is being used by prepared transactions"));
    }

    if !pids.is_null() {
        // Permissions checks
        let mut lc = list_head(pids);
        while !lc.is_null() {
            let pid = lfirst_int(lc);
            let proc_ = BackendPidGetProc(pid);

            if !proc_.is_null() {
                if superuser_arg((*proc_).roleId) && !superuser_arg(GetUserId()) {
                    ereport!(ERROR, errmsg!("permission denied to terminate process"));
                }

                if !has_privs_of_role(GetUserId(), (*proc_).roleId) &&
                    !has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_BACKEND)
                {
                    ereport!(ERROR, errmsg!("permission denied to terminate process"));
                }
            }
            lc = lnext(pids, lc);
        }

        /*
         * There's a race condition here: once we release the ProcArrayLock,
         * it's possible for the session to exit before we issue kill.
         */
        lc = list_head(pids);
        while !lc.is_null() {
            let pid = lfirst_int(lc);
            let proc_ = BackendPidGetProc(pid);

            if !proc_.is_null() {
                /*
                 * If we have setsid(), signal the backend's whole process group.
                 */
                // TODO(pg-port): HAVE_SETSID cfg not available yet; always use positive pid
                let _ = kill(pid, SIGTERM);
            }
            lc = lnext(pids, lc);
        }
        list_free(pids);
    }
}

// ---------------------------------------------------------------------------
// ProcArraySetReplicationSlotXmin / ProcArrayGetReplicationSlotXmin
// ---------------------------------------------------------------------------

/// ProcArraySetReplicationSlotXmin -- install limits to future computations
/// of the xmin horizon.
pub unsafe fn ProcArraySetReplicationSlotXmin(xmin: TransactionId,
                                              catalog_xmin: TransactionId,
                                              already_locked: bool) {
    Assert!(!already_locked || LWLockHeldByMe(ProcArrayLock()));

    if !already_locked {
        LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);
    }

    (*procArray).replication_slot_xmin = xmin;
    (*procArray).replication_slot_catalog_xmin = catalog_xmin;

    if !already_locked {
        LWLockRelease(ProcArrayLock());
    }

    elog!(DEBUG1, "xmin required by slots: data %u, catalog %u");
}

/// ProcArrayGetReplicationSlotXmin -- return the current slot xmin limits.
pub unsafe fn ProcArrayGetReplicationSlotXmin(xmin: *mut TransactionId,
                                              catalog_xmin: *mut TransactionId) {
    LWLockAcquire(ProcArrayLock(), LW_SHARED);

    if !xmin.is_null() {
        *xmin = (*procArray).replication_slot_xmin;
    }

    if !catalog_xmin.is_null() {
        *catalog_xmin = (*procArray).replication_slot_catalog_xmin;
    }

    LWLockRelease(ProcArrayLock());
}

// ---------------------------------------------------------------------------
// XidCacheRemoveRunningXids
// ---------------------------------------------------------------------------

/// XidCacheRemoveRunningXids -- Remove a bunch of TransactionIds from the
/// list of known-running subtransactions for my backend.
pub unsafe fn XidCacheRemoveRunningXids(xid: TransactionId,
                                        nxids: c_int,
                                        xids: *const TransactionId,
                                        latest_xid: TransactionId) {
    let mut i: c_int;
    let mut j: c_int;
    let mysubxidstat: *mut XidCacheStatus;

    Assert!(TransactionIdIsValid(xid));

    /*
     * We must hold ProcArrayLock exclusively in order to remove transactions
     * from the PGPROC array.
     */
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    mysubxidstat = (*ProcGlobal).subxidStates.add((*MyProc).pgxactoff as usize);

    /*
     * Under normal circumstances xid and xids[] will be in increasing order,
     * as will be the entries in subxids.  Scan backwards to avoid O(N^2)
     * behavior when removing a lot of xids.
     */
    i = nxids - 1;
    while i >= 0 {
        let anxid = *xids.add(i as usize);

        j = (*MyProc).subxidStatus.count as c_int - 1;
        while j >= 0 {
            if TransactionIdEquals((*MyProc).subxids.xids[j as usize], anxid) {
                (*MyProc).subxids.xids[j as usize] =
                    (*MyProc).subxids.xids[((*MyProc).subxidStatus.count - 1) as usize];
                pg_write_barrier();
                (*mysubxidstat).count -= 1;
                (*MyProc).subxidStatus.count -= 1;
                break;
            }
            j -= 1;
        }

        /*
         * Ordinarily we should have found it, unless the cache has overflowed.
         * However it's also possible for this routine to be invoked multiple
         * times for the same subtransaction.
         */
        if j < 0 && !(*MyProc).subxidStatus.overflowed {
            elog!(WARNING, "did not find subXID %u in MyProc");
        }
        i -= 1;
    }

    j = (*MyProc).subxidStatus.count as c_int - 1;
    while j >= 0 {
        if TransactionIdEquals((*MyProc).subxids.xids[j as usize], xid) {
            (*MyProc).subxids.xids[j as usize] =
                (*MyProc).subxids.xids[((*MyProc).subxidStatus.count - 1) as usize];
            pg_write_barrier();
            (*mysubxidstat).count -= 1;
            (*MyProc).subxidStatus.count -= 1;
            break;
        }
        j -= 1;
    }
    // Ordinarily we should have found it, unless the cache has overflowed
    if j < 0 && !(*MyProc).subxidStatus.overflowed {
        elog!(WARNING, "did not find subXID %u in MyProc");
    }

    // Also advance global latestCompletedXid while holding the lock
    MaintainLatestCompletedXid(latest_xid);

    // ... and xactCompletionCount
    (*TransamVariables).xactCompletionCount += 1;

    LWLockRelease(ProcArrayLock());
}

// ---------------------------------------------------------------------------
// GlobalVisTestFor / GlobalVisTestShouldUpdate / GlobalVisUpdateApply
// ---------------------------------------------------------------------------

/// GlobalVisTestFor -- return test state appropriate for relation.
///
/// If rel != NULL, return test state appropriate for relation; otherwise
/// return state usable for all relations.
pub unsafe fn GlobalVisTestFor(rel: Relation) -> *mut GlobalVisState {
    let state: *mut GlobalVisState;

    // XXX: we should assert that a snapshot is pushed or registered
    Assert!(RecentXmin != 0);

    state = match GlobalVisHorizonKindForRel(rel) {
        VISHORIZON_SHARED  => &raw mut GlobalVisSharedRels,
        VISHORIZON_CATALOG => &raw mut GlobalVisCatalogRels,
        VISHORIZON_DATA    => &raw mut GlobalVisDataRels,
        VISHORIZON_TEMP    => &raw mut GlobalVisTempRels,
    };

    Assert!(FullTransactionIdIsValid((*state).definitely_needed) &&
            FullTransactionIdIsValid((*state).maybe_needed));

    state
}

/// Return true if it's worth updating the accurate maybe_needed boundary.
unsafe fn GlobalVisTestShouldUpdate(state: *mut GlobalVisState) -> bool {
    // hasn't been updated yet
    if !TransactionIdIsValid(ComputeXidHorizonsResultLastXmin) {
        return true;
    }

    /*
     * If the maybe_needed/definitely_needed boundaries are the same, it's
     * unlikely to be beneficial to refresh boundaries.
     */
    if FullTransactionIdFollowsOrEquals((*state).maybe_needed, (*state).definitely_needed) {
        return false;
    }

    // does the last snapshot built have a different xmin?
    RecentXmin != ComputeXidHorizonsResultLastXmin
}

unsafe fn GlobalVisUpdateApply(horizons: *const ComputeXidHorizonsResult) {
    GlobalVisSharedRels.maybe_needed =
        FullXidRelativeTo((*horizons).latest_completed,
                          (*horizons).shared_oldest_nonremovable);
    GlobalVisCatalogRels.maybe_needed =
        FullXidRelativeTo((*horizons).latest_completed,
                          (*horizons).catalog_oldest_nonremovable);
    GlobalVisDataRels.maybe_needed =
        FullXidRelativeTo((*horizons).latest_completed,
                          (*horizons).data_oldest_nonremovable);
    GlobalVisTempRels.maybe_needed =
        FullXidRelativeTo((*horizons).latest_completed,
                          (*horizons).temp_oldest_nonremovable);

    /*
     * In longer running transactions it's possible that transactions we
     * previously needed to treat as running aren't around anymore.
     */
    GlobalVisSharedRels.definitely_needed =
        FullTransactionIdNewer(GlobalVisSharedRels.maybe_needed,
                               GlobalVisSharedRels.definitely_needed);
    GlobalVisCatalogRels.definitely_needed =
        FullTransactionIdNewer(GlobalVisCatalogRels.maybe_needed,
                               GlobalVisCatalogRels.definitely_needed);
    GlobalVisDataRels.definitely_needed =
        FullTransactionIdNewer(GlobalVisDataRels.maybe_needed,
                               GlobalVisDataRels.definitely_needed);
    GlobalVisTempRels.definitely_needed = GlobalVisTempRels.maybe_needed;

    ComputeXidHorizonsResultLastXmin = RecentXmin;
}

/// Update boundaries in GlobalVis{Shared,Catalog,Data}Rels using ComputeXidHorizons().
unsafe fn GlobalVisUpdate() {
    let mut horizons: ComputeXidHorizonsResult = core::mem::zeroed();
    // updates the horizons as a side-effect
    ComputeXidHorizons(&mut horizons);
}

/// Return true if no snapshot still considers fxid to be running.
pub unsafe fn GlobalVisTestIsRemovableFullXid(state: *mut GlobalVisState,
                                              fxid: FullTransactionId) -> bool {
    /*
     * If fxid is older than maybe_needed bound, it definitely is visible to everyone.
     */
    if FullTransactionIdPrecedes(fxid, (*state).maybe_needed) {
        return true;
    }

    /*
     * If fxid is >= definitely_needed bound, it is very likely to still be
     * considered running.
     */
    if FullTransactionIdFollowsOrEquals(fxid, (*state).definitely_needed) {
        return false;
    }

    /*
     * fxid is between maybe_needed and definitely_needed.  If it makes sense,
     * update boundaries and recheck.
     */
    if GlobalVisTestShouldUpdate(state) {
        GlobalVisUpdate();

        Assert!(FullTransactionIdPrecedes(fxid, (*state).definitely_needed));

        return FullTransactionIdPrecedes(fxid, (*state).maybe_needed);
    } else {
        false
    }
}

/// Wrapper around GlobalVisTestIsRemovableFullXid() for 32bit xids.
pub unsafe fn GlobalVisTestIsRemovableXid(state: *mut GlobalVisState,
                                          xid: TransactionId) -> bool {
    let fxid: FullTransactionId;

    /*
     * Convert 32 bit argument to FullTransactionId.
     */
    fxid = FullXidRelativeTo((*state).definitely_needed, xid);

    GlobalVisTestIsRemovableFullXid(state, fxid)
}

/// Convenience wrapper around GlobalVisTestFor() and
/// GlobalVisTestIsRemovableFullXid().
pub unsafe fn GlobalVisCheckRemovableFullXid(rel: Relation,
                                             fxid: FullTransactionId) -> bool {
    let state = GlobalVisTestFor(rel);
    GlobalVisTestIsRemovableFullXid(state, fxid)
}

/// Convenience wrapper around GlobalVisTestFor() and
/// GlobalVisTestIsRemovableXid().
pub unsafe fn GlobalVisCheckRemovableXid(rel: Relation, xid: TransactionId) -> bool {
    let state = GlobalVisTestFor(rel);
    GlobalVisTestIsRemovableXid(state, xid)
}

// ---------------------------------------------------------------------------
// FullXidRelativeTo
// ---------------------------------------------------------------------------

/// Convert a 32 bit transaction id into 64 bit transaction id, by assuming it
/// is within MaxTransactionId / 2 of XidFromFullTransactionId(rel).
///
/// Be very careful about when to use this function.
#[inline]
pub unsafe fn FullXidRelativeTo(rel: FullTransactionId, xid: TransactionId) -> FullTransactionId {
    let rel_xid = XidFromFullTransactionId(rel);

    Assert!(TransactionIdIsValid(xid));
    Assert!(TransactionIdIsValid(rel_xid));

    // not guaranteed to find issues, but likely to catch mistakes
    AssertTransactionIdInAllowableRange(xid);

    FullTransactionIdFromU64(U64FromFullTransactionId(rel)
                             .wrapping_add((xid.wrapping_sub(rel_xid)) as i32 as i64 as u64))
}

// ---------------------------------------------------------------------------
// KnownAssignedXids sub-module
// ---------------------------------------------------------------------------

/// RecordKnownAssignedTransactionIds -- Record the given XID in
/// KnownAssignedXids, as well as any preceding unobserved XIDs.
///
/// RecordKnownAssignedTransactionIds() should be run for *every* WAL record
/// associated with a transaction.  Must be called for each record after we
/// have executed StartupCLOG() et al.
pub unsafe fn RecordKnownAssignedTransactionIds(xid: TransactionId) {
    Assert!(standbyState >= STANDBY_INITIALIZED);
    Assert!(TransactionIdIsValid(xid));
    Assert!(TransactionIdIsValid(latestObservedXid));

    elog!(DEBUG4, "record known xact %u latestObservedXid %u");

    /*
     * When a newly observed xid arrives, it is frequently the case that it is
     * *not* the next xid in sequence.
     */
    if TransactionIdFollows(xid, latestObservedXid) {
        let mut next_expected_xid: TransactionId;

        /*
         * Extend subtrans like we do in GetNewTransactionId() during normal
         * operation.
         */
        next_expected_xid = latestObservedXid;
        while TransactionIdPrecedes(next_expected_xid, xid) {
            TransactionIdAdvance(&mut next_expected_xid);
            ExtendSUBTRANS(next_expected_xid);
        }
        Assert!(next_expected_xid == xid);

        /*
         * If the KnownAssignedXids machinery isn't up yet, there's nothing
         * more to do.
         */
        if standbyState <= STANDBY_INITIALIZED {
            latestObservedXid = xid;
            return;
        }

        /*
         * Add (latestObservedXid, xid] onto the KnownAssignedXids array.
         */
        next_expected_xid = latestObservedXid;
        TransactionIdAdvance(&mut next_expected_xid);
        KnownAssignedXidsAdd(next_expected_xid, xid, false);

        // Now we can advance latestObservedXid
        latestObservedXid = xid;

        // TransamVariables->nextXid must be beyond any observed xid
        AdvanceNextFullTransactionIdPastXid(latestObservedXid);
    }
}

/// ExpireTreeKnownAssignedTransactionIds -- Remove the given XIDs from
/// KnownAssignedXids.
pub unsafe fn ExpireTreeKnownAssignedTransactionIds(xid: TransactionId,
                                                    nsubxids: c_int,
                                                    subxids: *mut TransactionId,
                                                    max_xid: TransactionId) {
    Assert!(standbyState >= STANDBY_INITIALIZED);

    // Uses same locking as transaction commit
    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    KnownAssignedXidsRemoveTree(xid, nsubxids, subxids);

    // As in ProcArrayEndTransaction, advance latestCompletedXid
    MaintainLatestCompletedXidRecovery(max_xid);

    // ... and xactCompletionCount
    (*TransamVariables).xactCompletionCount += 1;

    LWLockRelease(ProcArrayLock());
}

/// ExpireAllKnownAssignedTransactionIds -- Remove all entries in
/// KnownAssignedXids and reset lastOverflowedXid.
pub unsafe fn ExpireAllKnownAssignedTransactionIds() {
    let mut latest_xid: FullTransactionId;

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);
    KnownAssignedXidsRemovePreceding(InvalidTransactionId);

    // Reset latestCompletedXid to nextXid - 1
    Assert!(FullTransactionIdIsValid((*TransamVariables).nextXid));
    latest_xid = (*TransamVariables).nextXid;
    FullTransactionIdRetreat(&mut latest_xid);
    (*TransamVariables).latestCompletedXid = latest_xid;

    /*
     * Any transactions that were in-progress were effectively aborted, so
     * advance xactCompletionCount.
     */
    (*TransamVariables).xactCompletionCount += 1;

    /*
     * Reset lastOverflowedXid.
     */
    (*procArray).lastOverflowedXid = InvalidTransactionId;
    LWLockRelease(ProcArrayLock());
}

/// ExpireOldKnownAssignedTransactionIds -- Remove KnownAssignedXids entries
/// preceding the given XID and potentially reset lastOverflowedXid.
pub unsafe fn ExpireOldKnownAssignedTransactionIds(xid: TransactionId) {
    let mut latest_xid: TransactionId;

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    // As in ProcArrayEndTransaction, advance latestCompletedXid
    latest_xid = xid;
    TransactionIdRetreat(&mut latest_xid);
    MaintainLatestCompletedXidRecovery(latest_xid);

    // ... and xactCompletionCount
    (*TransamVariables).xactCompletionCount += 1;

    /*
     * Reset lastOverflowedXid if we know all transactions that have been
     * possibly running are being gone.
     */
    if TransactionIdPrecedes((*procArray).lastOverflowedXid, xid) {
        (*procArray).lastOverflowedXid = InvalidTransactionId;
    }
    KnownAssignedXidsRemovePreceding(xid);
    LWLockRelease(ProcArrayLock());
}

/// KnownAssignedTransactionIdsIdleMaintenance -- Opportunistically do
/// maintenance work when the startup process is about to go idle.
pub unsafe fn KnownAssignedTransactionIdsIdleMaintenance() {
    KnownAssignedXidsCompress(KAX_STARTUP_PROCESS_IDLE, false);
}

// ---------------------------------------------------------------------------
// Private KnownAssignedXids functions
// ---------------------------------------------------------------------------

/// Compress KnownAssignedXids by shifting valid data down to the start of the
/// array, removing any gaps.
unsafe fn KnownAssignedXidsCompress(reason: KAXCompressReason, have_lock: bool) {
    let p_array = procArray;
    let head: c_int;
    let tail: c_int;
    let nelements: c_int;
    let mut compress_index: c_int;
    let mut i: c_int;

    // Counters for compression heuristics
    static mut transaction_ends_counter: u32 = 0;
    static mut last_compress_ts: TimestampTz = 0;

    // Tuning constants
    const KAX_COMPRESS_FREQUENCY: u32 = 128;        // in transactions
    const KAX_COMPRESS_IDLE_INTERVAL: i64 = 1000;   // in ms

    /*
     * Since only the startup process modifies the head/tail pointers, we
     * don't need a lock to read them here.
     */
    head = (*p_array).headKnownAssignedXids;
    tail = (*p_array).tailKnownAssignedXids;
    nelements = head - tail;

    /*
     * If we can choose whether to compress, use a heuristic to avoid
     * compressing too often or not often enough.
     */
    if nelements == (*p_array).numKnownAssignedXids {
        /*
         * When there are no gaps between head and tail, don't bother to
         * compress, except in the KAX_NO_SPACE case.
         */
        if !matches!(reason, KAX_NO_SPACE) {
            return;
        }
    } else if matches!(reason, KAX_TRANSACTION_END) {
        /*
         * Consider compressing only once every so many commits.
         */
        transaction_ends_counter += 1;
        if transaction_ends_counter % KAX_COMPRESS_FREQUENCY != 0 {
            return;
        }

        /*
         * Furthermore, compress only if the used part of the array is less
         * than 50% full.
         */
        if nelements < 2 * (*p_array).numKnownAssignedXids {
            return;
        }
    } else if matches!(reason, KAX_STARTUP_PROCESS_IDLE) {
        /*
         * We're about to go idle for lack of new WAL.
         */
        if last_compress_ts != 0 {
            let compress_after = TimestampTzPlusMilliseconds(last_compress_ts,
                                                              KAX_COMPRESS_IDLE_INTERVAL);
            if GetCurrentTimestamp() < compress_after {
                return;
            }
        }
    }

    // Need to compress, so get the lock if we don't have it.
    if !have_lock {
        LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);
    }

    /*
     * We compress the array by reading the valid values from tail to head,
     * re-aligning data to 0th element.
     */
    compress_index = 0;
    i = tail;
    while i < head {
        if *KnownAssignedXidsValid.add(i as usize) {
            *KnownAssignedXids.add(compress_index as usize) = *KnownAssignedXids.add(i as usize);
            *KnownAssignedXidsValid.add(compress_index as usize) = true;
            compress_index += 1;
        }
        i += 1;
    }
    Assert!(compress_index == (*p_array).numKnownAssignedXids);

    (*p_array).tailKnownAssignedXids = 0;
    (*p_array).headKnownAssignedXids = compress_index;

    if !have_lock {
        LWLockRelease(ProcArrayLock());
    }

    // Update timestamp for maintenance.  No need to hold lock for this.
    last_compress_ts = GetCurrentTimestamp();
}

/// Add xids into KnownAssignedXids at the head of the array.
///
/// xids from from_xid to to_xid, inclusive, are added to the array.
unsafe fn KnownAssignedXidsAdd(from_xid: TransactionId,
                                to_xid: TransactionId,
                                exclusive_lock: bool) {
    let p_array = procArray;
    let mut next_xid: TransactionId;
    let head: c_int;
    let tail: c_int;
    let nxids: c_int;
    let mut i: c_int;

    Assert!(TransactionIdPrecedesOrEquals(from_xid, to_xid));

    /*
     * Calculate how many array slots we'll need.  Normally this is cheap; in
     * the unusual case where the XIDs cross the wrap point, we do it the hard way.
     */
    if to_xid >= from_xid {
        nxids = (to_xid - from_xid + 1) as c_int;
    } else {
        let mut cnt: c_int = 1;
        next_xid = from_xid;
        while TransactionIdPrecedes(next_xid, to_xid) {
            cnt += 1;
            TransactionIdAdvance(&mut next_xid);
        }
        nxids = cnt;
    }

    /*
     * Since only the startup process modifies the head/tail pointers, we
     * don't need a lock to read them here.
     */
    head = (*p_array).headKnownAssignedXids;
    tail = (*p_array).tailKnownAssignedXids;

    Assert!(head >= 0 && head <= (*p_array).maxKnownAssignedXids);
    Assert!(tail >= 0 && tail < (*p_array).maxKnownAssignedXids);

    /*
     * Verify that insertions occur in TransactionId sequence.
     */
    if head > tail &&
        TransactionIdFollowsOrEquals(*KnownAssignedXids.add((head - 1) as usize), from_xid)
    {
        KnownAssignedXidsDisplay(LOG);
        elog!(ERROR, "out-of-order XID insertion in KnownAssignedXids");
    }

    /*
     * If our xids won't fit in the remaining space, compress out free space.
     */
    let mut cur_head = head;
    if cur_head + nxids > (*p_array).maxKnownAssignedXids {
        KnownAssignedXidsCompress(KAX_NO_SPACE, exclusive_lock);

        cur_head = (*p_array).headKnownAssignedXids;
        // note: we no longer care about the tail pointer

        /*
         * If it still won't fit then we're out of memory.
         */
        if cur_head + nxids > (*p_array).maxKnownAssignedXids {
            elog!(ERROR, "too many KnownAssignedXids");
        }
    }

    // Now we can insert the xids into the space starting at cur_head
    next_xid = from_xid;
    i = 0;
    while i < nxids {
        *KnownAssignedXids.add((cur_head + i) as usize) = next_xid;
        *KnownAssignedXidsValid.add((cur_head + i) as usize) = true;
        TransactionIdAdvance(&mut next_xid);
        i += 1;
    }

    // Adjust count of number of valid entries
    (*p_array).numKnownAssignedXids += nxids;

    /*
     * Now update the head pointer.  We use a write barrier to ensure that
     * other processors see the above array updates before they see the head
     * pointer change.
     */
    if !exclusive_lock {
        pg_write_barrier();
    }

    (*p_array).headKnownAssignedXids = cur_head + nxids;
}

/// KnownAssignedXidsSearch -- Searches KnownAssignedXids for a specific xid
/// and optionally removes it.
unsafe fn KnownAssignedXidsSearch(xid: TransactionId, remove: bool) -> bool {
    let p_array = procArray;
    let mut first: c_int;
    let mut last: c_int;
    let head: c_int;
    let tail: c_int;
    let mut result_index: c_int = -1;

    tail = (*p_array).tailKnownAssignedXids;
    head = (*p_array).headKnownAssignedXids;

    /*
     * Only the startup process removes entries, so we don't need the read
     * barrier in that case.
     */
    if !remove {
        pg_read_barrier(); // pairs with KnownAssignedXidsAdd
    }

    /*
     * Standard binary search.  Note we can ignore the KnownAssignedXidsValid
     * array here, since even invalid entries will contain sorted XIDs.
     */
    first = tail;
    last = head - 1;
    while first <= last {
        let mid_index = (first + last) / 2;
        let mid_xid = *KnownAssignedXids.add(mid_index as usize);

        if xid == mid_xid {
            result_index = mid_index;
            break;
        } else if TransactionIdPrecedes(xid, mid_xid) {
            last = mid_index - 1;
        } else {
            first = mid_index + 1;
        }
    }

    if result_index < 0 {
        return false; // not in array
    }

    if !*KnownAssignedXidsValid.add(result_index as usize) {
        return false; // in array, but invalid
    }

    if remove {
        *KnownAssignedXidsValid.add(result_index as usize) = false;

        (*p_array).numKnownAssignedXids -= 1;
        Assert!((*p_array).numKnownAssignedXids >= 0);

        /*
         * If we're removing the tail element then advance tail pointer over
         * any invalid elements.  This will speed future searches.
         */
        if result_index == tail {
            let mut new_tail = tail + 1;
            while new_tail < head && !*KnownAssignedXidsValid.add(new_tail as usize) {
                new_tail += 1;
            }
            if new_tail >= head {
                // Array is empty, so we can reset both pointers
                (*p_array).headKnownAssignedXids = 0;
                (*p_array).tailKnownAssignedXids = 0;
            } else {
                (*p_array).tailKnownAssignedXids = new_tail;
            }
        }
    }

    true
}

/// Is the specified XID present in KnownAssignedXids[]?
/// Caller must hold ProcArrayLock.
unsafe fn KnownAssignedXidExists(xid: TransactionId) -> bool {
    Assert!(TransactionIdIsValid(xid));
    KnownAssignedXidsSearch(xid, false)
}

/// Remove the specified XID from KnownAssignedXids[].
/// Caller must hold ProcArrayLock in exclusive mode.
unsafe fn KnownAssignedXidsRemove(xid: TransactionId) {
    Assert!(TransactionIdIsValid(xid));

    elog!(DEBUG4, "remove KnownAssignedXid %u");

    /*
     * Note: we cannot consider it an error to remove an XID that's not present.
     * We intentionally remove subxact IDs while processing XLOG_XACT_ASSIGNMENT.
     */
    let _ = KnownAssignedXidsSearch(xid, true);
}

/// KnownAssignedXidsRemoveTree -- Remove xid (if it's not InvalidTransactionId)
/// and all the subxids.
/// Caller must hold ProcArrayLock in exclusive mode.
unsafe fn KnownAssignedXidsRemoveTree(xid: TransactionId,
                                       nsubxids: c_int,
                                       subxids: *mut TransactionId) {
    let mut i: c_int;

    if TransactionIdIsValid(xid) {
        KnownAssignedXidsRemove(xid);
    }

    i = 0;
    while i < nsubxids {
        KnownAssignedXidsRemove(*subxids.add(i as usize));
        i += 1;
    }

    // Opportunistically compress the array
    KnownAssignedXidsCompress(KAX_TRANSACTION_END, true);
}

/// Prune KnownAssignedXids up to, but *not* including xid.  If xid is invalid
/// then clear the whole table.
/// Caller must hold ProcArrayLock in exclusive mode.
unsafe fn KnownAssignedXidsRemovePreceding(remove_xid: TransactionId) {
    let p_array = procArray;
    let mut count: c_int = 0;
    let head: c_int;
    let tail: c_int;
    let mut i: c_int;

    if !TransactionIdIsValid(remove_xid) {
        elog!(DEBUG4, "removing all KnownAssignedXids");
        (*p_array).numKnownAssignedXids = 0;
        (*p_array).headKnownAssignedXids = 0;
        (*p_array).tailKnownAssignedXids = 0;
        return;
    }

    elog!(DEBUG4, "prune KnownAssignedXids to %u");

    /*
     * Mark entries invalid starting at the tail.  Since array is sorted, we
     * can stop as soon as we reach an entry >= removeXid.
     */
    tail = (*p_array).tailKnownAssignedXids;
    head = (*p_array).headKnownAssignedXids;

    i = tail;
    while i < head {
        if *KnownAssignedXidsValid.add(i as usize) {
            let known_xid = *KnownAssignedXids.add(i as usize);

            if TransactionIdFollowsOrEquals(known_xid, remove_xid) {
                break;
            }

            if !StandbyTransactionIdIsPrepared(known_xid) {
                *KnownAssignedXidsValid.add(i as usize) = false;
                count += 1;
            }
        }
        i += 1;
    }

    (*p_array).numKnownAssignedXids -= count;
    Assert!((*p_array).numKnownAssignedXids >= 0);

    // Advance the tail pointer if we've marked the tail item invalid.
    i = tail;
    while i < head {
        if *KnownAssignedXidsValid.add(i as usize) {
            break;
        }
        i += 1;
    }
    if i >= head {
        // Array is empty, so we can reset both pointers
        (*p_array).headKnownAssignedXids = 0;
        (*p_array).tailKnownAssignedXids = 0;
    } else {
        (*p_array).tailKnownAssignedXids = i;
    }

    // Opportunistically compress the array
    KnownAssignedXidsCompress(KAX_PRUNE, true);
}

/// KnownAssignedXidsGet - Get an array of xids by scanning KnownAssignedXids.
/// We filter out anything >= xmax.
unsafe fn KnownAssignedXidsGet(xarray: *mut TransactionId, xmax: TransactionId) -> c_int {
    let mut xtmp: TransactionId = InvalidTransactionId;
    KnownAssignedXidsGetAndSetXmin(xarray, &mut xtmp, xmax)
}

/// KnownAssignedXidsGetAndSetXmin - as KnownAssignedXidsGet, plus
/// we reduce *xmin to the lowest xid value seen if not already lower.
/// Caller must hold ProcArrayLock.
unsafe fn KnownAssignedXidsGetAndSetXmin(xarray: *mut TransactionId,
                                          xmin: *mut TransactionId,
                                          xmax: TransactionId) -> c_int {
    let mut count: c_int = 0;
    let head: c_int;
    let tail: c_int;
    let mut i: c_int;

    /*
     * Fetch head just once, since it may change while we loop.
     */
    tail = (*procArray).tailKnownAssignedXids;
    head = (*procArray).headKnownAssignedXids;

    pg_read_barrier(); // pairs with KnownAssignedXidsAdd

    i = tail;
    while i < head {
        // Skip any gaps in the array
        if *KnownAssignedXidsValid.add(i as usize) {
            let known_xid = *KnownAssignedXids.add(i as usize);

            /*
             * Update xmin if required.  Only the first XID need be checked,
             * since the array is sorted.
             */
            if count == 0 && TransactionIdPrecedes(known_xid, *xmin) {
                *xmin = known_xid;
            }

            /*
             * Filter out anything >= xmax, again relying on sorted property.
             */
            if TransactionIdIsValid(xmax) &&
                TransactionIdFollowsOrEquals(known_xid, xmax)
            {
                break;
            }

            // Add knownXid into output array
            *xarray.add(count as usize) = known_xid;
            count += 1;
        }
        i += 1;
    }

    count
}

/// Get oldest XID in the KnownAssignedXids array, or InvalidTransactionId
/// if nothing there.
unsafe fn KnownAssignedXidsGetOldestXmin() -> TransactionId {
    let head: c_int;
    let tail: c_int;
    let mut i: c_int;

    // Fetch head just once, since it may change while we loop.
    tail = (*procArray).tailKnownAssignedXids;
    head = (*procArray).headKnownAssignedXids;

    pg_read_barrier(); // pairs with KnownAssignedXidsAdd

    i = tail;
    while i < head {
        // Skip any gaps in the array
        if *KnownAssignedXidsValid.add(i as usize) {
            return *KnownAssignedXids.add(i as usize);
        }
        i += 1;
    }

    InvalidTransactionId
}

/// Display KnownAssignedXids to provide debug trail.
unsafe fn KnownAssignedXidsDisplay(trace_level: c_int) {
    let p_array = procArray;
    let mut buf: StringInfoData = StringInfoData { data: core::ptr::null_mut() };
    let head: c_int;
    let tail: c_int;
    let mut i: c_int;
    let mut nxids: c_int = 0;

    tail = (*p_array).tailKnownAssignedXids;
    head = (*p_array).headKnownAssignedXids;

    initStringInfo(&mut buf);

    i = tail;
    while i < head {
        if *KnownAssignedXidsValid.add(i as usize) {
            nxids += 1;
            appendStringInfo(&mut buf,
                             b"[%d]=%u \0".as_ptr() as *const i8,
                             *KnownAssignedXids.add(i as usize));
        }
        i += 1;
    }

    elog!(trace_level, "%d KnownAssignedXids (num=%d tail=%d head=%d) %s");

    pfree(buf.data as *mut c_void);
}

/// KnownAssignedXidsReset -- Resets KnownAssignedXids to be empty.
unsafe fn KnownAssignedXidsReset() {
    let p_array = procArray;

    LWLockAcquire(ProcArrayLock(), LW_EXCLUSIVE);

    (*p_array).numKnownAssignedXids = 0;
    (*p_array).tailKnownAssignedXids = 0;
    (*p_array).headKnownAssignedXids = 0;

    LWLockRelease(ProcArrayLock());
}

// ---------------------------------------------------------------------------
// Helper: pgprocnos_ptr -- access the flexible array in ProcArrayStruct
// ---------------------------------------------------------------------------

/// Return a pointer to the pgprocnos flexible array embedded in a ProcArrayStruct.
/// In C this is just `arrayP->pgprocnos`, but in Rust we need to handle the
/// zero-length array by computing the offset manually.
#[inline]
unsafe fn pgprocnos_ptr(array_p: *mut ProcArrayStruct) -> *mut c_int {
    // pgprocnos is a [c_int; 0] at the end of the struct -- pointer arithmetic gives us
    // the address immediately after the fixed fields.
    let base = array_p as *mut u8;
    let offset = core::mem::offset_of!(ProcArrayStruct, pgprocnos);
    base.add(offset) as *mut c_int
}
