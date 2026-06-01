//! src/backend/storage/ipc/standby.c
//!
//! standby.c
//!   Misc functions used in Hot Standby mode.
//!
//!   All functions for handling RM_STANDBY_ID, which relate to
//!   AccessExclusiveLocks and starting snapshots for Hot Standby mode.
//!   Plus conflict recovery processing.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/ipc/standby.c

use crate::prelude::*;

use core::ffi::CStr;

use crate::storage::ipc::procsignal::ProcSignalReason;
use crate::storage::ipc::procsignal::ProcSignalReason::*;
use crate::storage::lmgr::lock::{
    AccessExclusiveLock, VirtualTransactionId, VirtualTransactionIdIsValid, xl_standby_lock,
    LOCKTAG, SET_LOCKTAG_RELATION,
};
use crate::utils::hash::dynahash::{
    hash_create, hash_destroy, hash_search, hash_seq_init, hash_seq_search, HASHCTL,
    HASH_BLOBS, HASH_ELEM, HASH_SEQ_STATUS, HTAB,
};
use crate::utils::hash::dynahash::HASHACTION::{HASH_ENTER, HASH_FIND, HASH_REMOVE};

extern "C" {
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
// Local stubs for symbols that don't have a stable home yet in the port.
// These belong to other .c files still being translated concurrently.
// ----------------------------------------------------------------------------

// TODO(pg-port): TransactionId/Oid/uint8/XLogRecPtr come from the prelude/c.h.

// TODO(pg-port): real TimestampTz lives in datatype/timestamp.h
pub type TimestampTz = int64;

// TODO(pg-port): real FullTransactionId lives in access/transam.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FullTransactionId {
    pub value: uint64,
}

// TODO(pg-port): real ProcNumber lives in storage/procnumber.h
pub type ProcNumber = c_int;

// TODO(pg-port): real LocalTransactionId lives in storage/lock.h
pub type LocalTransactionId = uint32;

// TODO(pg-port): real RelFileLocator lives in storage/relfilelocator.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: Oid,
}

// TODO(pg-port): real PGPROC lives in storage/proc.h (only .pid and .vxid/.waitStart used)
#[repr(C)]
pub struct PGPROC {
    pub pid: c_int,
    pub vxid: PGPROC_vxid,
    pub waitStart: pg_atomic_uint64,
}

#[repr(C)]
pub struct PGPROC_vxid {
    pub procNumber: ProcNumber,
}

// TODO(pg-port): real pg_atomic_uint64 lives in port/atomics.h
#[repr(C)]
pub struct pg_atomic_uint64 {
    pub value: uint64,
}

// TODO(pg-port): real StringInfoData lives in lib/stringinfo.h
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

// TODO(pg-port): real SharedInvalidationMessage lives in storage/sinval.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedInvalidationMessage {
    pub id: int8,
}

// TODO(pg-port): real XLogReaderState lives in access/xlogreader.h
pub type XLogReaderState = c_void;

// subxids_array_status -- storage/standby.h
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum subxids_array_status {
    SUBXIDS_IN_ARRAY,
    SUBXIDS_MISSING,
    SUBXIDS_IN_SUBTRANS,
}
use subxids_array_status::*;

// RunningTransactionsData -- storage/standby.h
#[repr(C)]
pub struct RunningTransactionsData {
    pub xcnt: c_int,
    pub subxcnt: c_int,
    pub subxid_status: subxids_array_status,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub oldestDatabaseRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
    pub xids: *mut TransactionId,
}
pub type RunningTransactions = *mut RunningTransactionsData;

// xl_standby_locks -- storage/standbydefs.h
#[repr(C)]
pub struct xl_standby_locks {
    pub nlocks: c_int,
    pub locks: [xl_standby_lock; 0], // FLEXIBLE_ARRAY_MEMBER
}

// xl_running_xacts -- storage/standbydefs.h
#[repr(C)]
pub struct xl_running_xacts {
    pub xcnt: c_int,
    pub subxcnt: c_int,
    pub subxid_overflow: bool,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
    pub xids: [TransactionId; 0], // FLEXIBLE_ARRAY_MEMBER
}

// xl_invalidations -- storage/standbydefs.h
#[repr(C)]
pub struct xl_invalidations {
    pub dbId: Oid,
    pub tsId: Oid,
    pub relcacheInitFileInval: bool,
    pub nmsgs: c_int,
    pub msgs: [SharedInvalidationMessage; 0], // FLEXIBLE_ARRAY_MEMBER
}

// EnableTimeoutParams -- utils/timeout.h
// TODO(pg-port): real EnableTimeoutParams lives in utils/timeout.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EnableTimeoutParams {
    pub id: TimeoutId,
    pub r#type: TimeoutType,
    pub fin_time: TimestampTz,
    pub delay_ms: c_int,
}
pub type TimeoutId = c_int;
pub type TimeoutType = c_int;
pub const STANDBY_TIMEOUT: TimeoutId = 0;
pub const STANDBY_DEADLOCK_TIMEOUT: TimeoutId = 0;
pub const STANDBY_LOCK_TIMEOUT: TimeoutId = 0;
pub const TMPARAM_AFTER: TimeoutType = 0;
pub const TMPARAM_AT: TimeoutType = 0;

// XLOG message types -- storage/standbydefs.h
pub const XLOG_STANDBY_LOCK: uint8 = 0x00;
pub const XLOG_RUNNING_XACTS: uint8 = 0x10;
pub const XLOG_INVALIDATIONS: uint8 = 0x20;

// standby states -- access/xlogutils.h (HotStandbyState)
pub const STANDBY_DISABLED: c_int = 0;
pub const STANDBY_INITIALIZED: c_int = 1;

// WAL levels -- access/xlog.h
pub const WAL_LEVEL_LOGICAL: c_int = 3;

// wait events -- utils/wait_event.h
pub const WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT: uint32 = 0;
pub const WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE: uint32 = 0;
pub const WAIT_EVENT_BUFFER_PIN: uint32 = 0;
pub const PG_WAIT_LOCK: uint32 = 0x03000000;

// replication slot invalidation cause -- replication/slot.h
pub const RS_INVAL_HORIZON: c_int = 0;

// resource managers -- access/rmgrlist.h
pub const RM_STANDBY_ID: uint8 = 0;

// XLog record flags -- access/xloginsert.h
pub const XLOG_MARK_UNIMPORTANT: uint8 = 0x04;

// xact flags -- access/xact.h
pub const XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK: c_int = 1 << 0;

// XLR info mask -- access/xlogrecord.h
pub const XLR_INFO_MASK: uint8 = 0x0F;

// MaxTransactionId -- access/transam.h
pub const MaxTransactionId: TransactionId = 0xFFFFFFFF;

// MinSizeOfXactRunningXacts -- storage/standby.h: offsetof(xl_running_xacts, xids)
pub const fn MinSizeOfXactRunningXacts() -> usize {
    core::mem::offset_of!(xl_running_xacts, xids)
}
// MinSizeOfInvalidations -- storage/standbydefs.h: offsetof(xl_invalidations, msgs)
pub const fn MinSizeOfInvalidations() -> usize {
    core::mem::offset_of!(xl_invalidations, msgs)
}

// ----------------------------------------------------------------------------
// Global variables that conceptually live elsewhere but are referenced here.
// ----------------------------------------------------------------------------

// TODO(pg-port): MyProc lives in storage/proc.h
pub static mut MyProc: *mut PGPROC = null_mut();
// TODO(pg-port): MyProcNumber lives in storage/procnumber.h
pub static mut MyProcNumber: ProcNumber = 0;
// TODO(pg-port): standbyState lives in access/xlogutils.c
pub static mut standbyState: c_int = STANDBY_DISABLED;
// TODO(pg-port): InRecovery lives in access/xlog.c
pub static mut InRecovery: bool = false;
// TODO(pg-port): wal_level lives in access/xlog.c
pub static mut wal_level: c_int = 0;
// TODO(pg-port): DeadlockTimeout lives in storage/lmgr/proc.c
pub static mut DeadlockTimeout: c_int = 1000;
// TODO(pg-port): update_process_title lives in utils/misc/ps_status.c
pub static mut update_process_title: bool = true;
// TODO(pg-port): MyDatabaseId / MyDatabaseTableSpace live in miscinit.c
pub static mut MyDatabaseId: Oid = 0;
pub static mut MyDatabaseTableSpace: Oid = 0;
// TODO(pg-port): MyXactFlags lives in access/xact.c
pub static mut MyXactFlags: c_int = 0;

// ----------------------------------------------------------------------------
// Stub functions for dependencies in other .c files.
// ----------------------------------------------------------------------------

// TODO(pg-port): SharedInvalBackendInit lives in storage/ipc/sinvaladt.c
unsafe fn SharedInvalBackendInit(_sendOnly: bool) { /* TODO(pg-port) */ }
// TODO(pg-port): GetNextLocalTransactionId lives in storage/lmgr/lock.c
unsafe fn GetNextLocalTransactionId() -> LocalTransactionId { 0 }
// TODO(pg-port): VirtualXactLockTableInsert lives in storage/lmgr/lock.c
unsafe fn VirtualXactLockTableInsert(_vxid: VirtualTransactionId) { /* TODO(pg-port) */ }
// TODO(pg-port): VirtualXactLockTableCleanup lives in storage/lmgr/lock.c
unsafe fn VirtualXactLockTableCleanup() { /* TODO(pg-port) */ }
// TODO(pg-port): ExpireAllKnownAssignedTransactionIds lives in storage/ipc/procarray.c
unsafe fn ExpireAllKnownAssignedTransactionIds() { /* TODO(pg-port) */ }
// TODO(pg-port): GetXLogReceiptTime lives in access/transam/xlogrecovery.c
unsafe fn GetXLogReceiptTime(rtime: *mut TimestampTz, fromStream: *mut bool) {
    *rtime = 0;
    *fromStream = false;
}
// TODO(pg-port): GetCurrentTimestamp lives in utils/adt/timestamp.c
unsafe fn GetCurrentTimestamp() -> TimestampTz { 0 }
// TODO(pg-port): TimestampTzPlusMilliseconds lives in utils/timestamp.h
unsafe fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: c_int) -> TimestampTz {
    tz + (ms as TimestampTz) * 1000
}
// TODO(pg-port): TimestampDifference lives in utils/adt/timestamp.c
unsafe fn TimestampDifference(_start: TimestampTz, _stop: TimestampTz, secs: *mut c_long, microsecs: *mut c_int) {
    *secs = 0;
    *microsecs = 0;
}
// TODO(pg-port): TimestampDifferenceExceeds lives in utils/adt/timestamp.c
unsafe fn TimestampDifferenceExceeds(_start: TimestampTz, _stop: TimestampTz, _msec: c_int) -> bool { false }
// TODO(pg-port): CHECK_FOR_INTERRUPTS lives in miscadmin.h
unsafe fn CHECK_FOR_INTERRUPTS() { /* TODO(pg-port) */ }
// TODO(pg-port): pg_usleep lives in port/pgsleep.c
unsafe fn pg_usleep(_microsec: c_long) { /* TODO(pg-port) */ }
// TODO(pg-port): pgstat_report_wait_start lives in utils/activity/wait_event.c
unsafe fn pgstat_report_wait_start(_wait_event_info: uint32) { /* TODO(pg-port) */ }
// TODO(pg-port): pgstat_report_wait_end lives in utils/activity/wait_event.c
unsafe fn pgstat_report_wait_end() { /* TODO(pg-port) */ }
// TODO(pg-port): pgstat_report_stat lives in utils/activity/pgstat.c
unsafe fn pgstat_report_stat(_force: bool) { /* TODO(pg-port) */ }
// TODO(pg-port): ProcNumberGetProc lives in storage/lmgr/proc.c
unsafe fn ProcNumberGetProc(_procNumber: ProcNumber) -> *mut PGPROC { null_mut() }
// TODO(pg-port): initStringInfo lives in lib/stringinfo.c
unsafe fn initStringInfo(_str: *mut StringInfoData) { /* TODO(pg-port) */ }
// TODO(pg-port): get_recovery_conflict_desc forward (defined below)
// TODO(pg-port): VirtualXactLock lives in storage/lmgr/lock.c
unsafe fn VirtualXactLock(_vxid: VirtualTransactionId, _wait: bool) -> bool { true }
// TODO(pg-port): CancelVirtualTransaction lives in storage/ipc/procarray.c
unsafe fn CancelVirtualTransaction(_vxid: VirtualTransactionId, _sigmode: ProcSignalReason) -> c_int { 0 }
// TODO(pg-port): SignalVirtualTransaction lives in storage/ipc/procarray.c
unsafe fn SignalVirtualTransaction(_vxid: VirtualTransactionId, _sigmode: ProcSignalReason, _conflictPending: bool) -> c_int { 0 }
// TODO(pg-port): set_ps_display_suffix lives in utils/misc/ps_status.c
unsafe fn set_ps_display_suffix(_suffix: *const c_char) { /* TODO(pg-port) */ }
// TODO(pg-port): set_ps_display_remove_suffix lives in utils/misc/ps_status.c
unsafe fn set_ps_display_remove_suffix() { /* TODO(pg-port) */ }
// TODO(pg-port): GetConflictingVirtualXIDs lives in storage/ipc/procarray.c
unsafe fn GetConflictingVirtualXIDs(_limitXmin: TransactionId, _dbOid: Oid) -> *mut VirtualTransactionId { null_mut() }
// TODO(pg-port): InvalidateObsoleteReplicationSlots lives in replication/slot.c
unsafe fn InvalidateObsoleteReplicationSlots(_cause: c_int, _segno: u64, _dboid: Oid, _snapshotConflictHorizon: TransactionId) { /* TODO(pg-port) */ }
// TODO(pg-port): ReadNextFullTransactionId lives in access/transam/varsup.c
unsafe fn ReadNextFullTransactionId() -> FullTransactionId { FullTransactionId { value: 0 } }
// TODO(pg-port): U64FromFullTransactionId lives in access/transam.h
unsafe fn U64FromFullTransactionId(x: FullTransactionId) -> uint64 { x.value }
// TODO(pg-port): XidFromFullTransactionId lives in access/transam.h
unsafe fn XidFromFullTransactionId(x: FullTransactionId) -> TransactionId { x.value as TransactionId }
// TODO(pg-port): CountDBBackends lives in storage/ipc/procarray.c
unsafe fn CountDBBackends(_databaseid: Oid) -> c_int { 0 }
// TODO(pg-port): CancelDBBackends lives in storage/ipc/procarray.c
unsafe fn CancelDBBackends(_databaseid: Oid, _sigmode: ProcSignalReason, _conflictPending: bool) { /* TODO(pg-port) */ }
// TODO(pg-port): GetLockConflicts lives in storage/lmgr/lock.c
unsafe fn GetLockConflicts(_locktag: *const LOCKTAG, _lockmode: c_int, _countp: *mut c_int) -> *mut VirtualTransactionId { null_mut() }
// TODO(pg-port): pg_atomic_read_u64 lives in port/atomics.h
unsafe fn pg_atomic_read_u64(ptr: *mut pg_atomic_uint64) -> uint64 { (*ptr).value }
// TODO(pg-port): pg_atomic_write_u64 lives in port/atomics.h
unsafe fn pg_atomic_write_u64(ptr: *mut pg_atomic_uint64, val: uint64) { (*ptr).value = val; }
// TODO(pg-port): enable_timeouts lives in utils/misc/timeout.c
unsafe fn enable_timeouts(_timeouts: *const EnableTimeoutParams, _count: c_int) { /* TODO(pg-port) */ }
// TODO(pg-port): disable_all_timeouts lives in utils/misc/timeout.c
unsafe fn disable_all_timeouts(_keep_indicators: bool) { /* TODO(pg-port) */ }
// TODO(pg-port): ProcWaitForSignal lives in storage/lmgr/proc.c
unsafe fn ProcWaitForSignal(_wait_event_info: uint32) { /* TODO(pg-port) */ }
// TODO(pg-port): SendRecoveryConflictWithBufferPin is defined below.
// TODO(pg-port): HoldingBufferPinThatDelaysRecovery lives in storage/buffer/bufmgr.c
unsafe fn HoldingBufferPinThatDelaysRecovery() -> bool { false }
// TODO(pg-port): TransactionIdIsValid lives in access/transam.h
unsafe fn TransactionIdIsValid(xid: TransactionId) -> bool { xid != 0 }
// TODO(pg-port): TransactionIdIsNormal lives in access/transam.h
unsafe fn TransactionIdIsNormal(xid: TransactionId) -> bool { xid >= 3 }
// TODO(pg-port): TransactionIdDidCommit lives in access/transam/transam.c
unsafe fn TransactionIdDidCommit(_xid: TransactionId) -> bool { false }
// TODO(pg-port): TransactionIdDidAbort lives in access/transam/transam.c
unsafe fn TransactionIdDidAbort(_xid: TransactionId) -> bool { false }
// TODO(pg-port): TransactionIdPrecedes lives in access/transam/transam.c
unsafe fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}
// TODO(pg-port): OidIsValid lives in c.h
unsafe fn OidIsValid(oid: Oid) -> bool { oid != 0 }
// TODO(pg-port): LockAcquire lives in storage/lmgr/lock.c
unsafe fn LockAcquire(_locktag: *const LOCKTAG, _lockmode: c_int, _sessionLock: bool, _dontWait: bool) -> c_int { 0 }
// TODO(pg-port): LockRelease lives in storage/lmgr/lock.c
unsafe fn LockRelease(_locktag: *const LOCKTAG, _lockmode: c_int, _sessionLock: bool) -> bool { true }
// TODO(pg-port): GetCurrentTransactionId lives in access/transam/xact.c
unsafe fn GetCurrentTransactionId() -> TransactionId { 0 }
// TODO(pg-port): StandbyTransactionIdIsPrepared lives in access/transam/twophase.c
unsafe fn StandbyTransactionIdIsPrepared(_xid: TransactionId) -> bool { false }
// TODO(pg-port): ProcArrayApplyRecoveryInfo lives in storage/ipc/procarray.c
unsafe fn ProcArrayApplyRecoveryInfo(_running: *mut RunningTransactionsData) { /* TODO(pg-port) */ }
// TODO(pg-port): ProcessCommittedInvalidationMessages lives in storage/ipc/inval.c
unsafe fn ProcessCommittedInvalidationMessages(_msgs: *mut SharedInvalidationMessage, _nmsgs: c_int, _relcacheInitFileInval: bool, _dbid: Oid, _tsid: Oid) { /* TODO(pg-port) */ }
// TODO(pg-port): GetRunningTransactionLocks lives in storage/lmgr/lock.c
unsafe fn GetRunningTransactionLocks(nlocks: *mut c_int) -> *mut xl_standby_lock { *nlocks = 0; null_mut() }
// TODO(pg-port): GetRunningTransactionData lives in storage/ipc/procarray.c
unsafe fn GetRunningTransactionData() -> RunningTransactions { null_mut() }
// TODO(pg-port): GetInsertRecPtr lives in access/transam/xlog.c
unsafe fn GetInsertRecPtr() -> XLogRecPtr { 0 }
// TODO(pg-port): LWLockRelease lives in storage/lmgr/lwlock.c
unsafe fn LWLockRelease(_lock: *mut c_void) { /* TODO(pg-port) */ }
// TODO(pg-port): ProcArrayLock / XidGenLock live in storage/lmgr/lwlock.c
static mut ProcArrayLock: *mut c_void = null_mut();
static mut XidGenLock: *mut c_void = null_mut();
// TODO(pg-port): XLogStandbyInfoActive lives in access/xlog.h
unsafe fn XLogStandbyInfoActive() -> bool { false }
// TODO(pg-port): XLogBeginInsert lives in access/transam/xloginsert.c
unsafe fn XLogBeginInsert() { /* TODO(pg-port) */ }
// TODO(pg-port): XLogSetRecordFlags lives in access/transam/xloginsert.c
unsafe fn XLogSetRecordFlags(_flags: uint8) { /* TODO(pg-port) */ }
// TODO(pg-port): XLogRegisterData lives in access/transam/xloginsert.c
unsafe fn XLogRegisterData(_data: *const c_void, _len: c_int) { /* TODO(pg-port) */ }
// TODO(pg-port): XLogInsert lives in access/transam/xloginsert.c
unsafe fn XLogInsert(_rmid: uint8, _info: uint8) -> XLogRecPtr { 0 }
// TODO(pg-port): XLogSetAsyncXactLSN lives in access/transam/xlog.c
unsafe fn XLogSetAsyncXactLSN(_asyncXactLSN: XLogRecPtr) { /* TODO(pg-port) */ }
// TODO(pg-port): XLogRecGetInfo lives in access/xlogreader.h
unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> uint8 { 0 }
// TODO(pg-port): XLogRecGetData lives in access/xlogreader.h
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_char { null_mut() }
// TODO(pg-port): XLogRecHasAnyBlockRefs lives in access/xlogreader.h
unsafe fn XLogRecHasAnyBlockRefs(_record: *mut XLogReaderState) -> bool { false }
// TODO(pg-port): IS_INJECTION_POINT_ATTACHED lives in utils/injection_point.h
unsafe fn IS_INJECTION_POINT_ATTACHED(_name: *const c_char) -> bool { false }
// TODO(pg-port): gettext marker _() lives in c.h
unsafe fn gettext(msgid: *const c_char) -> *const c_char { msgid }

// ============================================================================
// User-settable GUC parameters
// ============================================================================

pub static mut max_standby_archive_delay: c_int = 30 * 1000;
pub static mut max_standby_streaming_delay: c_int = 30 * 1000;
pub static mut log_recovery_conflict_waits: bool = false;

/*
 * Keep track of all the exclusive locks owned by original transactions.
 * For each known exclusive lock, there is a RecoveryLockEntry in the
 * RecoveryLockHash hash table.  All RecoveryLockEntrys belonging to a
 * given XID are chained together so that we can find them easily.
 * For each original transaction that is known to have any such locks,
 * there is a RecoveryLockXidEntry in the RecoveryLockXidHash hash table,
 * which stores the head of the chain of its locks.
 */
#[repr(C)]
pub struct RecoveryLockEntry {
    pub key: xl_standby_lock, // hash key: xid, dbOid, relOid
    pub next: *mut RecoveryLockEntry, // chain link
}

#[repr(C)]
pub struct RecoveryLockXidEntry {
    pub xid: TransactionId, // hash key -- must be first
    pub head: *mut RecoveryLockEntry, // chain head
}

static mut RecoveryLockHash: *mut HTAB = null_mut();
static mut RecoveryLockXidHash: *mut HTAB = null_mut();

/* Flags set by timeout handlers */
static mut got_standby_deadlock_timeout: bool = false;
static mut got_standby_delay_timeout: bool = false;
static mut got_standby_lock_timeout: bool = false;

/*
 * InitRecoveryTransactionEnvironment
 *		Initialize tracking of our primary's in-progress transactions.
 *
 * We need to issue shared invalidations and hold locks. Holding locks
 * means others may want to wait on us, so we need to make a lock table
 * vxact entry like a real transaction. We could create and delete
 * lock table entries for each transaction but its simpler just to create
 * one permanent entry and leave it there all the time. Locks are then
 * acquired and released as needed. Yes, this means you can see the
 * Startup process in pg_locks once we have run this.
 */
pub unsafe fn InitRecoveryTransactionEnvironment() {
    let mut vxid: VirtualTransactionId = core::mem::zeroed();
    let mut hash_ctl: HASHCTL = core::mem::zeroed();

    Assert!(RecoveryLockHash.is_null()); // don't run this twice

    /*
     * Initialize the hash tables for tracking the locks held by each
     * transaction.
     */
    hash_ctl.keysize = core::mem::size_of::<xl_standby_lock>();
    hash_ctl.entrysize = core::mem::size_of::<RecoveryLockEntry>();
    RecoveryLockHash = hash_create(
        c"RecoveryLockHash".as_ptr(),
        64,
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS,
    );
    hash_ctl.keysize = core::mem::size_of::<TransactionId>();
    hash_ctl.entrysize = core::mem::size_of::<RecoveryLockXidEntry>();
    RecoveryLockXidHash = hash_create(
        c"RecoveryLockXidHash".as_ptr(),
        64,
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS,
    );

    /*
     * Initialize shared invalidation management for Startup process, being
     * careful to register ourselves as a sendOnly process so we don't need to
     * read messages, nor will we get signaled when the queue starts filling
     * up.
     */
    SharedInvalBackendInit(true);

    /*
     * Lock a virtual transaction id for Startup process.
     *
     * We need to do GetNextLocalTransactionId() because
     * SharedInvalBackendInit() leaves localTransactionId invalid and the lock
     * manager doesn't like that at all.
     *
     * Note that we don't need to run XactLockTableInsert() because nobody
     * needs to wait on xids. That sounds a little strange, but table locks
     * are held by vxids and row level locks are held by xids. All queries
     * hold AccessShareLocks so never block while we write or lock new rows.
     */
    (*MyProc).vxid.procNumber = MyProcNumber;
    vxid.procNumber = MyProcNumber;
    vxid.localTransactionId = GetNextLocalTransactionId();
    VirtualXactLockTableInsert(vxid);

    standbyState = STANDBY_INITIALIZED;
}

/*
 * ShutdownRecoveryTransactionEnvironment
 *		Shut down transaction tracking
 *
 * Prepare to switch from hot standby mode to normal operation. Shut down
 * recovery-time transaction tracking.
 *
 * This must be called even in shutdown of startup process if transaction
 * tracking has been initialized. Otherwise some locks the tracked
 * transactions were holding will not be released and may interfere with
 * the processes still running (but will exit soon later) at the exit of
 * startup process.
 */
pub unsafe fn ShutdownRecoveryTransactionEnvironment() {
    /*
     * Do nothing if RecoveryLockHash is NULL because that means that
     * transaction tracking has not yet been initialized or has already been
     * shut down.  This makes it safe to have possibly-redundant calls of this
     * function during process exit.
     */
    if RecoveryLockHash.is_null() {
        return;
    }

    /* Mark all tracked in-progress transactions as finished. */
    ExpireAllKnownAssignedTransactionIds();

    /* Release all locks the tracked transactions were holding */
    StandbyReleaseAllLocks();

    /* Destroy the lock hash tables. */
    hash_destroy(RecoveryLockHash);
    hash_destroy(RecoveryLockXidHash);
    RecoveryLockHash = null_mut();
    RecoveryLockXidHash = null_mut();

    /* Cleanup our VirtualTransaction */
    VirtualXactLockTableCleanup();
}

/*
 * -----------------------------------------------------
 *		Standby wait timers and backend cancel logic
 * -----------------------------------------------------
 */

/*
 * Determine the cutoff time at which we want to start canceling conflicting
 * transactions.  Returns zero (a time safely in the past) if we are willing
 * to wait forever.
 */
unsafe fn GetStandbyLimitTime() -> TimestampTz {
    let mut rtime: TimestampTz = 0;
    let mut fromStream: bool = false;

    /*
     * The cutoff time is the last WAL data receipt time plus the appropriate
     * delay variable.  Delay of -1 means wait forever.
     */
    GetXLogReceiptTime(&mut rtime, &mut fromStream);
    if fromStream {
        if max_standby_streaming_delay < 0 {
            return 0; // wait forever
        }
        return TimestampTzPlusMilliseconds(rtime, max_standby_streaming_delay);
    } else {
        if max_standby_archive_delay < 0 {
            return 0; // wait forever
        }
        return TimestampTzPlusMilliseconds(rtime, max_standby_archive_delay);
    }
}

const STANDBY_INITIAL_WAIT_US: c_int = 1000;
static mut standbyWait_us: c_int = STANDBY_INITIAL_WAIT_US;

/*
 * Standby wait logic for ResolveRecoveryConflictWithVirtualXIDs.
 * We wait here for a while then return. If we decide we can't wait any
 * more then we return true, if we can wait some more return false.
 */
unsafe fn WaitExceedsMaxStandbyDelay(wait_event_info: uint32) -> bool {
    CHECK_FOR_INTERRUPTS();

    /* Are we past the limit time? */
    let ltime: TimestampTz = GetStandbyLimitTime();
    if ltime != 0 && GetCurrentTimestamp() >= ltime {
        return true;
    }

    /*
     * Sleep a bit (this is essential to avoid busy-waiting).
     */
    pgstat_report_wait_start(wait_event_info);
    pg_usleep(standbyWait_us as c_long);
    pgstat_report_wait_end();

    /*
     * Progressively increase the sleep times, but not to more than 1s, since
     * pg_usleep isn't interruptible on some platforms.
     */
    standbyWait_us *= 2;
    if standbyWait_us > 1000000 {
        standbyWait_us = 1000000;
    }

    false
}

/*
 * Log the recovery conflict.
 *
 * wait_start is the timestamp when the caller started to wait.
 * now is the timestamp when this function has been called.
 * wait_list is the list of virtual transaction ids assigned to
 * conflicting processes. still_waiting indicates whether
 * the startup process is still waiting for the recovery conflict
 * to be resolved or not.
 */
pub unsafe fn LogRecoveryConflict(
    reason: ProcSignalReason,
    wait_start: TimestampTz,
    now: TimestampTz,
    wait_list: *mut VirtualTransactionId,
    still_waiting: bool,
) {
    let mut secs: c_long = 0;
    let mut usecs: c_int = 0;
    let mut msecs: c_long;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut nprocs: c_int = 0;

    /*
     * There must be no conflicting processes when the recovery conflict has
     * already been resolved.
     */
    Assert!(still_waiting || wait_list.is_null());

    TimestampDifference(wait_start, now, &mut secs, &mut usecs);
    msecs = secs * 1000 + (usecs / 1000) as c_long;
    usecs = usecs % 1000;

    if !wait_list.is_null() {
        /* Construct a string of list of the conflicting processes */
        let mut vxids: *mut VirtualTransactionId = wait_list;
        while VirtualTransactionIdIsValid(*vxids) {
            let proc_: *mut PGPROC = ProcNumberGetProc((*vxids).procNumber);

            /* proc can be NULL if the target backend is not active */
            if !proc_.is_null() {
                if nprocs == 0 {
                    initStringInfo(&mut buf);
                    appendStringInfo(&mut buf, c"%d".as_ptr(), (*proc_).pid);
                } else {
                    appendStringInfo(&mut buf, c", %d".as_ptr(), (*proc_).pid);
                }

                nprocs += 1;
            }

            vxids = vxids.add(1);
        }
    }

    /*
     * If wait_list is specified, report the list of PIDs of active
     * conflicting backends in a detail message. Note that if all the backends
     * in the list are not active, no detail message is logged.
     */
    if still_waiting {
        // C also: errdetail_log_plural("Conflicting process: %s.",
        //   "Conflicting processes: %s.", nprocs, buf.data) appended when nprocs > 0.
        ereport!(
            LOG,
            errmsg!(
                "recovery still waiting after {}.{:03} ms: {}",
                msecs,
                usecs,
                CStr::from_ptr(get_recovery_conflict_desc(reason)).to_string_lossy()
            )
        );
    } else {
        ereport!(
            LOG,
            errmsg!(
                "recovery finished waiting after {}.{:03} ms: {}",
                msecs,
                usecs,
                CStr::from_ptr(get_recovery_conflict_desc(reason)).to_string_lossy()
            )
        );
    }

    if nprocs > 0 {
        pfree(buf.data as *mut c_void);
    }
}

// TODO(pg-port): appendStringInfo lives in lib/stringinfo.c (variadic)
unsafe fn appendStringInfo(_str: *mut StringInfoData, _fmt: *const c_char, _arg: c_int) { /* TODO(pg-port) */ }

/*
 * This is the main executioner for any query backend that conflicts with
 * recovery processing. Judgement has already been passed on it within
 * a specific rmgr. Here we just issue the orders to the procs. The procs
 * then throw the required error as instructed.
 *
 * If report_waiting is true, "waiting" is reported in PS display and the
 * wait for recovery conflict is reported in the log, if necessary. If
 * the caller is responsible for reporting them, report_waiting should be
 * false. Otherwise, both the caller and this function report the same
 * thing unexpectedly.
 */
unsafe fn ResolveRecoveryConflictWithVirtualXIDs(
    mut waitlist: *mut VirtualTransactionId,
    reason: ProcSignalReason,
    wait_event_info: uint32,
    report_waiting: bool,
) {
    let mut waitStart: TimestampTz = 0;
    let mut waiting: bool = false;
    let mut logged_recovery_conflict: bool = false;

    /* Fast exit, to avoid a kernel call if there's no work to be done. */
    if !VirtualTransactionIdIsValid(*waitlist) {
        return;
    }

    /* Set the wait start timestamp for reporting */
    if report_waiting && (log_recovery_conflict_waits || update_process_title) {
        waitStart = GetCurrentTimestamp();
    }

    while VirtualTransactionIdIsValid(*waitlist) {
        /* reset standbyWait_us for each xact we wait for */
        standbyWait_us = STANDBY_INITIAL_WAIT_US;

        /* wait until the virtual xid is gone */
        while !VirtualXactLock(*waitlist, false) {
            /* Is it time to kill it? */
            if WaitExceedsMaxStandbyDelay(wait_event_info) {
                /*
                 * Now find out who to throw out of the balloon.
                 */
                Assert!(VirtualTransactionIdIsValid(*waitlist));
                let pid: c_int = CancelVirtualTransaction(*waitlist, reason);

                /*
                 * Wait a little bit for it to die so that we avoid flooding
                 * an unresponsive backend when system is heavily loaded.
                 */
                if pid != 0 {
                    pg_usleep(5000);
                }
            }

            if waitStart != 0 && (!logged_recovery_conflict || !waiting) {
                let mut now: TimestampTz = 0;
                let maybe_log_conflict: bool;
                let maybe_update_title: bool;

                maybe_log_conflict = log_recovery_conflict_waits && !logged_recovery_conflict;
                maybe_update_title = update_process_title && !waiting;

                /* Get the current timestamp if not report yet */
                if maybe_log_conflict || maybe_update_title {
                    now = GetCurrentTimestamp();
                }

                /*
                 * Report via ps if we have been waiting for more than 500
                 * msec (should that be configurable?)
                 */
                if maybe_update_title && TimestampDifferenceExceeds(waitStart, now, 500) {
                    set_ps_display_suffix(c"waiting".as_ptr());
                    waiting = true;
                }

                /*
                 * Emit the log message if the startup process is waiting
                 * longer than deadlock_timeout for recovery conflict.
                 */
                if maybe_log_conflict
                    && TimestampDifferenceExceeds(waitStart, now, DeadlockTimeout)
                {
                    LogRecoveryConflict(reason, waitStart, now, waitlist, true);
                    logged_recovery_conflict = true;
                }
            }
        }

        /* The virtual transaction is gone now, wait for the next one */
        waitlist = waitlist.add(1);
    }

    /*
     * Emit the log message if recovery conflict was resolved but the startup
     * process waited longer than deadlock_timeout for it.
     */
    if logged_recovery_conflict {
        LogRecoveryConflict(reason, waitStart, GetCurrentTimestamp(), null_mut(), false);
    }

    /* reset ps display to remove the suffix if we added one */
    if waiting {
        set_ps_display_remove_suffix();
    }
}

/*
 * Generate whatever recovery conflicts are needed to eliminate snapshots that
 * might see XIDs <= snapshotConflictHorizon as still running.
 *
 * snapshotConflictHorizon cutoffs are our standard approach to generating
 * granular recovery conflicts.  Note that InvalidTransactionId values are
 * interpreted as "definitely don't need any conflicts" here, which is a
 * general convention that WAL records can (and often do) depend on.
 */
pub unsafe fn ResolveRecoveryConflictWithSnapshot(
    snapshotConflictHorizon: TransactionId,
    isCatalogRel: bool,
    locator: RelFileLocator,
) {
    /*
     * If we get passed InvalidTransactionId then we do nothing (no conflict).
     *
     * This can happen when replaying already-applied WAL records after a
     * standby crash or restart, or when replaying an XLOG_HEAP2_VISIBLE
     * record that marks as frozen a page which was already all-visible.  It's
     * also quite common with records generated during index deletion
     * (original execution of the deletion can reason that a recovery conflict
     * which is sufficient for the deletion operation must take place before
     * replay of the deletion record itself).
     */
    if !TransactionIdIsValid(snapshotConflictHorizon) {
        return;
    }

    Assert!(TransactionIdIsNormal(snapshotConflictHorizon));
    let backends: *mut VirtualTransactionId =
        GetConflictingVirtualXIDs(snapshotConflictHorizon, locator.dbOid);
    ResolveRecoveryConflictWithVirtualXIDs(
        backends,
        PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
        WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT,
        true,
    );

    /*
     * Note that WaitExceedsMaxStandbyDelay() is not taken into account here
     * (as opposed to ResolveRecoveryConflictWithVirtualXIDs() above). That
     * seems OK, given that this kind of conflict should not normally be
     * reached, e.g. due to using a physical replication slot.
     */
    if wal_level >= WAL_LEVEL_LOGICAL && isCatalogRel {
        InvalidateObsoleteReplicationSlots(
            RS_INVAL_HORIZON,
            0,
            locator.dbOid,
            snapshotConflictHorizon,
        );
    }
}

/*
 * Variant of ResolveRecoveryConflictWithSnapshot that works with
 * FullTransactionId values
 */
pub unsafe fn ResolveRecoveryConflictWithSnapshotFullXid(
    snapshotConflictHorizon: FullTransactionId,
    isCatalogRel: bool,
    locator: RelFileLocator,
) {
    /*
     * ResolveRecoveryConflictWithSnapshot operates on 32-bit TransactionIds,
     * so truncate the logged FullTransactionId.  If the logged value is very
     * old, so that XID wrap-around already happened on it, there can't be any
     * snapshots that still see it.
     */
    let nextXid: FullTransactionId = ReadNextFullTransactionId();
    let diff: uint64;

    diff = U64FromFullTransactionId(nextXid) - U64FromFullTransactionId(snapshotConflictHorizon);
    if diff < (MaxTransactionId / 2) as uint64 {
        let truncated: TransactionId;

        truncated = XidFromFullTransactionId(snapshotConflictHorizon);
        ResolveRecoveryConflictWithSnapshot(truncated, isCatalogRel, locator);
    }
}

pub unsafe fn ResolveRecoveryConflictWithTablespace(_tsid: Oid) {
    /*
     * Standby users may be currently using this tablespace for their
     * temporary files. We only care about current users because
     * temp_tablespace parameter will just ignore tablespaces that no longer
     * exist.
     *
     * Ask everybody to cancel their queries immediately so we can ensure no
     * temp files remain and we can remove the tablespace. Nuke the entire
     * site from orbit, it's the only way to be sure.
     *
     * XXX: We could work out the pids of active backends using this
     * tablespace by examining the temp filenames in the directory. We would
     * then convert the pids into VirtualXIDs before attempting to cancel
     * them.
     *
     * We don't wait for commit because drop tablespace is non-transactional.
     */
    let temp_file_users: *mut VirtualTransactionId =
        GetConflictingVirtualXIDs(InvalidTransactionId, InvalidOid);
    ResolveRecoveryConflictWithVirtualXIDs(
        temp_file_users,
        PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
        WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE,
        true,
    );
}

pub unsafe fn ResolveRecoveryConflictWithDatabase(dbid: Oid) {
    /*
     * We don't do ResolveRecoveryConflictWithVirtualXIDs() here since that
     * only waits for transactions and completely idle sessions would block
     * us. This is rare enough that we do this as simply as possible: no wait,
     * just force them off immediately.
     *
     * No locking is required here because we already acquired
     * AccessExclusiveLock. Anybody trying to connect while we do this will
     * block during InitPostgres() and then disconnect when they see the
     * database has been removed.
     */
    while CountDBBackends(dbid) > 0 {
        CancelDBBackends(dbid, PROCSIG_RECOVERY_CONFLICT_DATABASE, true);

        /*
         * Wait awhile for them to die so that we avoid flooding an
         * unresponsive backend when system is heavily loaded.
         */
        pg_usleep(10000);
    }
}

// TODO(pg-port): InvalidTransactionId lives in access/transam.h
pub const InvalidTransactionId: TransactionId = 0;
// TODO(pg-port): InvalidOid lives in postgres_ext.h
pub const InvalidOid: Oid = 0;

/*
 * ResolveRecoveryConflictWithLock is called from ProcSleep()
 * to resolve conflicts with other backends holding relation locks.
 *
 * The WaitLatch sleep normally done in ProcSleep()
 * (when not InHotStandby) is performed here, for code clarity.
 *
 * We either resolve conflicts immediately or set a timeout to wake us at
 * the limit of our patience.
 *
 * Resolve conflicts by canceling to all backends holding a conflicting
 * lock.  As we are already queued to be granted the lock, no new lock
 * requests conflicting with ours will be granted in the meantime.
 *
 * We also must check for deadlocks involving the Startup process and
 * hot-standby backend processes. If deadlock_timeout is reached in
 * this function, all the backends holding the conflicting locks are
 * requested to check themselves for deadlocks.
 *
 * logging_conflict should be true if the recovery conflict has not been
 * logged yet even though logging is enabled. After deadlock_timeout is
 * reached and the request for deadlock check is sent, we wait again to
 * be signaled by the release of the lock if logging_conflict is false.
 * Otherwise we return without waiting again so that the caller can report
 * the recovery conflict. In this case, then, this function is called again
 * with logging_conflict=false (because the recovery conflict has already
 * been logged) and we will wait again for the lock to be released.
 */
pub unsafe fn ResolveRecoveryConflictWithLock(mut locktag: LOCKTAG, logging_conflict: bool) {
    let ltime: TimestampTz;
    let now: TimestampTz;

    Assert!(InHotStandby());

    ltime = GetStandbyLimitTime();
    now = GetCurrentTimestamp();

    /*
     * Update waitStart if first time through after the startup process
     * started waiting for the lock. It should not be updated every time
     * ResolveRecoveryConflictWithLock() is called during the wait.
     *
     * Use the current time obtained for comparison with ltime as waitStart
     * (i.e., the time when this process started waiting for the lock). Since
     * getting the current time newly can cause overhead, we reuse the
     * already-obtained time to avoid that overhead.
     *
     * Note that waitStart is updated without holding the lock table's
     * partition lock, to avoid the overhead by additional lock acquisition.
     * This can cause "waitstart" in pg_locks to become NULL for a very short
     * period of time after the wait started even though "granted" is false.
     * This is OK in practice because we can assume that users are likely to
     * look at "waitstart" when waiting for the lock for a long time.
     */
    if pg_atomic_read_u64(&mut (*MyProc).waitStart) == 0 {
        pg_atomic_write_u64(&mut (*MyProc).waitStart, now as uint64);
    }

    if now >= ltime && ltime != 0 {
        /*
         * We're already behind, so clear a path as quickly as possible.
         */
        let backends: *mut VirtualTransactionId =
            GetLockConflicts(&locktag, AccessExclusiveLock, null_mut());

        /*
         * Prevent ResolveRecoveryConflictWithVirtualXIDs() from reporting
         * "waiting" in PS display by disabling its argument report_waiting
         * because the caller, WaitOnLock(), has already reported that.
         */
        ResolveRecoveryConflictWithVirtualXIDs(
            backends,
            PROCSIG_RECOVERY_CONFLICT_LOCK,
            PG_WAIT_LOCK | locktag.locktag_type as uint32,
            false,
        );
    } else {
        /*
         * Wait (or wait again) until ltime, and check for deadlocks as well
         * if we will be waiting longer than deadlock_timeout
         */
        let mut timeouts: [EnableTimeoutParams; 2] = core::mem::zeroed();
        let mut cnt: c_int = 0;

        if ltime != 0 {
            got_standby_lock_timeout = false;
            timeouts[cnt as usize].id = STANDBY_LOCK_TIMEOUT;
            timeouts[cnt as usize].r#type = TMPARAM_AT;
            timeouts[cnt as usize].fin_time = ltime;
            cnt += 1;
        }

        got_standby_deadlock_timeout = false;
        timeouts[cnt as usize].id = STANDBY_DEADLOCK_TIMEOUT;
        timeouts[cnt as usize].r#type = TMPARAM_AFTER;
        timeouts[cnt as usize].delay_ms = DeadlockTimeout;
        cnt += 1;

        enable_timeouts(timeouts.as_ptr(), cnt);
    }

    /* Wait to be signaled by the release of the Relation Lock */
    ProcWaitForSignal(PG_WAIT_LOCK | locktag.locktag_type as uint32);

    'cleanup: {
        /*
         * Exit if ltime is reached. Then all the backends holding conflicting
         * locks will be canceled in the next ResolveRecoveryConflictWithLock()
         * call.
         */
        if got_standby_lock_timeout {
            break 'cleanup;
        }

        if got_standby_deadlock_timeout {
            let mut backends: *mut VirtualTransactionId =
                GetLockConflicts(&locktag, AccessExclusiveLock, null_mut());

            /* Quick exit if there's no work to be done */
            if !VirtualTransactionIdIsValid(*backends) {
                break 'cleanup;
            }

            /*
             * Send signals to all the backends holding the conflicting locks, to
             * ask them to check themselves for deadlocks.
             */
            while VirtualTransactionIdIsValid(*backends) {
                SignalVirtualTransaction(
                    *backends,
                    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
                    false,
                );
                backends = backends.add(1);
            }

            /*
             * Exit if the recovery conflict has not been logged yet even though
             * logging is enabled, so that the caller can log that. Then
             * RecoveryConflictWithLock() is called again and we will wait again
             * for the lock to be released.
             */
            if logging_conflict {
                break 'cleanup;
            }

            /*
             * Wait again here to be signaled by the release of the Relation Lock,
             * to prevent the subsequent RecoveryConflictWithLock() from causing
             * deadlock_timeout and sending a request for deadlocks check again.
             * Otherwise the request continues to be sent every deadlock_timeout
             * until the relation locks are released or ltime is reached.
             */
            got_standby_deadlock_timeout = false;
            ProcWaitForSignal(PG_WAIT_LOCK | locktag.locktag_type as uint32);
        }
    }

    /*
     * Clear any timeout requests established above.  We assume here that the
     * Startup process doesn't have any other outstanding timeouts than those
     * used by this function. If that stops being true, we could cancel the
     * timeouts individually, but that'd be slower.
     */
    disable_all_timeouts(false);
    got_standby_lock_timeout = false;
    got_standby_deadlock_timeout = false;

    // silence unused-mut on locktag for the &locktag borrows
    let _ = &mut locktag;
}

// TODO(pg-port): InHotStandby lives in access/xlogutils.h (standbyState >= STANDBY_SNAPSHOT_PENDING)
unsafe fn InHotStandby() -> bool { standbyState >= STANDBY_INITIALIZED }

/*
 * ResolveRecoveryConflictWithBufferPin is called from LockBufferForCleanup()
 * to resolve conflicts with other backends holding buffer pins.
 *
 * The ProcWaitForSignal() sleep normally done in LockBufferForCleanup()
 * (when not InHotStandby) is performed here, for code clarity.
 *
 * We either resolve conflicts immediately or set a timeout to wake us at
 * the limit of our patience.
 *
 * Resolve conflicts by sending a PROCSIG signal to all backends to check if
 * they hold one of the buffer pins that is blocking Startup process. If so,
 * those backends will take an appropriate error action, ERROR or FATAL.
 *
 * We also must check for deadlocks.  Deadlocks occur because if queries
 * wait on a lock, that must be behind an AccessExclusiveLock, which can only
 * be cleared if the Startup process replays a transaction completion record.
 * If Startup process is also waiting then that is a deadlock. The deadlock
 * can occur if the query is waiting and then the Startup sleeps, or if
 * Startup is sleeping and the query waits on a lock. We protect against
 * only the former sequence here, the latter sequence is checked prior to
 * the query sleeping, in CheckRecoveryConflictDeadlock().
 *
 * Deadlocks are extremely rare, and relatively expensive to check for,
 * so we don't do a deadlock check right away ... only if we have had to wait
 * at least deadlock_timeout.
 */
pub unsafe fn ResolveRecoveryConflictWithBufferPin() {
    let ltime: TimestampTz;

    Assert!(InHotStandby());

    ltime = GetStandbyLimitTime();

    if GetCurrentTimestamp() >= ltime && ltime != 0 {
        /*
         * We're already behind, so clear a path as quickly as possible.
         */
        SendRecoveryConflictWithBufferPin(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN);
    } else {
        /*
         * Wake up at ltime, and check for deadlocks as well if we will be
         * waiting longer than deadlock_timeout
         */
        let mut timeouts: [EnableTimeoutParams; 2] = core::mem::zeroed();
        let mut cnt: c_int = 0;

        if ltime != 0 {
            timeouts[cnt as usize].id = STANDBY_TIMEOUT;
            timeouts[cnt as usize].r#type = TMPARAM_AT;
            timeouts[cnt as usize].fin_time = ltime;
            cnt += 1;
        }

        got_standby_deadlock_timeout = false;
        timeouts[cnt as usize].id = STANDBY_DEADLOCK_TIMEOUT;
        timeouts[cnt as usize].r#type = TMPARAM_AFTER;
        timeouts[cnt as usize].delay_ms = DeadlockTimeout;
        cnt += 1;

        enable_timeouts(timeouts.as_ptr(), cnt);
    }

    /*
     * Wait to be signaled by UnpinBuffer() or for the wait to be interrupted
     * by one of the timeouts established above.
     *
     * We assume that only UnpinBuffer() and the timeout requests established
     * above can wake us up here. WakeupRecovery() called by walreceiver or
     * SIGHUP signal handler, etc cannot do that because it uses the different
     * latch from that ProcWaitForSignal() waits on.
     */
    ProcWaitForSignal(WAIT_EVENT_BUFFER_PIN);

    if got_standby_delay_timeout {
        SendRecoveryConflictWithBufferPin(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN);
    } else if got_standby_deadlock_timeout {
        /*
         * Send out a request for hot-standby backends to check themselves for
         * deadlocks.
         *
         * XXX The subsequent ResolveRecoveryConflictWithBufferPin() will wait
         * to be signaled by UnpinBuffer() again and send a request for
         * deadlocks check if deadlock_timeout happens. This causes the
         * request to continue to be sent every deadlock_timeout until the
         * buffer is unpinned or ltime is reached. This would increase the
         * workload in the startup process and backends. In practice it may
         * not be so harmful because the period that the buffer is kept pinned
         * is basically no so long. But we should fix this?
         */
        SendRecoveryConflictWithBufferPin(PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK);
    }

    /*
     * Clear any timeout requests established above.  We assume here that the
     * Startup process doesn't have any other timeouts than what this function
     * uses.  If that stops being true, we could cancel the timeouts
     * individually, but that'd be slower.
     */
    disable_all_timeouts(false);
    got_standby_delay_timeout = false;
    got_standby_deadlock_timeout = false;
}

unsafe fn SendRecoveryConflictWithBufferPin(reason: ProcSignalReason) {
    Assert!(
        reason == PROCSIG_RECOVERY_CONFLICT_BUFFERPIN
            || reason == PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK
    );

    /*
     * We send signal to all backends to ask them if they are holding the
     * buffer pin which is delaying the Startup process. We must not set the
     * conflict flag yet, since most backends will be innocent. Let the
     * SIGUSR1 handling in each backend decide their own fate.
     */
    CancelDBBackends(InvalidOid, reason, false);
}

/*
 * In Hot Standby perform early deadlock detection.  We abort the lock
 * wait if we are about to sleep while holding the buffer pin that Startup
 * process is waiting for.
 *
 * Note: this code is pessimistic, because there is no way for it to
 * determine whether an actual deadlock condition is present: the lock we
 * need to wait for might be unrelated to any held by the Startup process.
 * Sooner or later, this mechanism should get ripped out in favor of somehow
 * accounting for buffer locks in DeadLockCheck().  However, errors here
 * seem to be very low-probability in practice, so for now it's not worth
 * the trouble.
 */
pub unsafe fn CheckRecoveryConflictDeadlock() {
    Assert!(!InRecovery); // do not call in Startup process

    if !HoldingBufferPinThatDelaysRecovery() {
        return;
    }

    /*
     * Error message should match ProcessInterrupts() but we avoid calling
     * that because we aren't handling an interrupt at this point. Note that
     * we only cancel the current transaction here, so if we are in a
     * subtransaction and the pin is held by a parent, then the Startup
     * process will continue to wait even though we have avoided deadlock.
     */
    // C also: errcode(ERRCODE_T_R_DEADLOCK_DETECTED),
    //   errdetail("User transaction caused buffer deadlock with recovery.")
    ereport!(
        ERROR,
        errmsg!("canceling statement due to conflict with recovery")
    );
}

/* --------------------------------
 *		timeout handler routines
 * --------------------------------
 */

/*
 * StandbyDeadLockHandler() will be called if STANDBY_DEADLOCK_TIMEOUT is
 * exceeded.
 */
pub unsafe fn StandbyDeadLockHandler() {
    got_standby_deadlock_timeout = true;
}

/*
 * StandbyTimeoutHandler() will be called if STANDBY_TIMEOUT is exceeded.
 */
pub unsafe fn StandbyTimeoutHandler() {
    got_standby_delay_timeout = true;
}

/*
 * StandbyLockTimeoutHandler() will be called if STANDBY_LOCK_TIMEOUT is exceeded.
 */
pub unsafe fn StandbyLockTimeoutHandler() {
    got_standby_lock_timeout = true;
}

/*
 * -----------------------------------------------------
 * Locking in Recovery Mode
 * -----------------------------------------------------
 *
 * All locks are held by the Startup process using a single virtual
 * transaction. This implementation is both simpler and in some senses,
 * more correct. The locks held mean "some original transaction held
 * this lock, so query access is not allowed at this time". So the Startup
 * process is the proxy by which the original locks are implemented.
 *
 * We only keep track of AccessExclusiveLocks, which are only ever held by
 * one transaction on one relation.
 *
 * We keep a table of known locks in the RecoveryLockHash hash table.
 * The point of that table is to let us efficiently de-duplicate locks,
 * which is important because checkpoints will re-report the same locks
 * already held.  There is also a RecoveryLockXidHash table with one entry
 * per xid, which allows us to efficiently find all the locks held by a
 * given original transaction.
 *
 * We use session locks rather than normal locks so we don't need
 * ResourceOwners.
 */

pub unsafe fn StandbyAcquireAccessExclusiveLock(xid: TransactionId, dbOid: Oid, relOid: Oid) {
    let xidentry: *mut RecoveryLockXidEntry;
    let lockentry: *mut RecoveryLockEntry;
    let mut key: xl_standby_lock = core::mem::zeroed();
    let mut locktag: LOCKTAG = core::mem::zeroed();
    let mut found: bool = false;

    /* Already processed? */
    if !TransactionIdIsValid(xid) || TransactionIdDidCommit(xid) || TransactionIdDidAbort(xid) {
        return;
    }

    elog!(DEBUG4, "adding recovery lock: db {} rel {}", dbOid, relOid);

    /* dbOid is InvalidOid when we are locking a shared relation. */
    Assert!(OidIsValid(relOid));

    /* Create a hash entry for this xid, if we don't have one already. */
    xidentry = hash_search(
        RecoveryLockXidHash,
        &xid as *const TransactionId as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut RecoveryLockXidEntry;
    if !found {
        Assert!((*xidentry).xid == xid); // dynahash should have set this
        (*xidentry).head = null_mut();
    }

    /* Create a hash entry for this lock, unless we have one already. */
    key.xid = xid;
    key.dbOid = dbOid;
    key.relOid = relOid;
    lockentry = hash_search(
        RecoveryLockHash,
        &key as *const xl_standby_lock as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut RecoveryLockEntry;
    if !found {
        /* It's new, so link it into the XID's list ... */
        (*lockentry).next = (*xidentry).head;
        (*xidentry).head = lockentry;

        /* ... and acquire the lock locally. */
        SET_LOCKTAG_RELATION(&mut locktag, dbOid, relOid);

        let _ = LockAcquire(&locktag, AccessExclusiveLock, true, false);
    }
}

/*
 * Release all the locks associated with this RecoveryLockXidEntry.
 */
unsafe fn StandbyReleaseXidEntryLocks(xidentry: *mut RecoveryLockXidEntry) {
    let mut entry: *mut RecoveryLockEntry;
    let mut next: *mut RecoveryLockEntry;

    entry = (*xidentry).head;
    while !entry.is_null() {
        let mut locktag: LOCKTAG = core::mem::zeroed();

        elog!(
            DEBUG4,
            "releasing recovery lock: xid {} db {} rel {}",
            (*entry).key.xid,
            (*entry).key.dbOid,
            (*entry).key.relOid
        );
        /* Release the lock ... */
        SET_LOCKTAG_RELATION(&mut locktag, (*entry).key.dbOid, (*entry).key.relOid);
        if !LockRelease(&locktag, AccessExclusiveLock, true) {
            elog!(
                LOG,
                "RecoveryLockHash contains entry for lock no longer recorded by lock manager: xid {} database {} relation {}",
                (*entry).key.xid,
                (*entry).key.dbOid,
                (*entry).key.relOid
            );
            Assert!(false);
        }
        /* ... and remove the per-lock hash entry */
        next = (*entry).next;
        hash_search(
            RecoveryLockHash,
            entry as *const c_void,
            HASH_REMOVE,
            null_mut(),
        );

        entry = next;
    }

    (*xidentry).head = null_mut(); // just for paranoia
}

/*
 * Release locks for specific XID, or all locks if it's InvalidXid.
 */
unsafe fn StandbyReleaseLocks(xid: TransactionId) {
    if TransactionIdIsValid(xid) {
        let entry = hash_search(
            RecoveryLockXidHash,
            &xid as *const TransactionId as *const c_void,
            HASH_FIND,
            null_mut(),
        ) as *mut RecoveryLockXidEntry;
        if !entry.is_null() {
            StandbyReleaseXidEntryLocks(entry);
            hash_search(
                RecoveryLockXidHash,
                entry as *const c_void,
                HASH_REMOVE,
                null_mut(),
            );
        }
    } else {
        StandbyReleaseAllLocks();
    }
}

/*
 * Release locks for a transaction tree, starting at xid down, from
 * RecoveryLockXidHash.
 *
 * Called during WAL replay of COMMIT/ROLLBACK when in hot standby mode,
 * to remove any AccessExclusiveLocks requested by a transaction.
 */
pub unsafe fn StandbyReleaseLockTree(xid: TransactionId, nsubxids: c_int, subxids: *mut TransactionId) {
    StandbyReleaseLocks(xid);

    for i in 0..nsubxids {
        StandbyReleaseLocks(*subxids.add(i as usize));
    }
}

/*
 * Called at end of recovery and when we see a shutdown checkpoint.
 */
pub unsafe fn StandbyReleaseAllLocks() {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut entry: *mut RecoveryLockXidEntry;

    elog!(DEBUG2, "release all standby locks");

    hash_seq_init(&mut status, RecoveryLockXidHash);
    loop {
        entry = hash_seq_search(&mut status) as *mut RecoveryLockXidEntry;
        if entry.is_null() {
            break;
        }
        StandbyReleaseXidEntryLocks(entry);
        hash_search(
            RecoveryLockXidHash,
            entry as *const c_void,
            HASH_REMOVE,
            null_mut(),
        );
    }
}

/*
 * StandbyReleaseOldLocks
 *		Release standby locks held by top-level XIDs that aren't running,
 *		as long as they're not prepared transactions.
 *
 * This is needed to prune the locks of crashed transactions, which didn't
 * write an ABORT/COMMIT record.
 */
pub unsafe fn StandbyReleaseOldLocks(oldxid: TransactionId) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut entry: *mut RecoveryLockXidEntry;

    hash_seq_init(&mut status, RecoveryLockXidHash);
    loop {
        entry = hash_seq_search(&mut status) as *mut RecoveryLockXidEntry;
        if entry.is_null() {
            break;
        }
        Assert!(TransactionIdIsValid((*entry).xid));

        /* Skip if prepared transaction. */
        if StandbyTransactionIdIsPrepared((*entry).xid) {
            continue;
        }

        /* Skip if >= oldxid. */
        if !TransactionIdPrecedes((*entry).xid, oldxid) {
            continue;
        }

        /* Remove all locks and hash table entry. */
        StandbyReleaseXidEntryLocks(entry);
        hash_search(
            RecoveryLockXidHash,
            entry as *const c_void,
            HASH_REMOVE,
            null_mut(),
        );
    }
}

/*
 * --------------------------------------------------------------------
 *		Recovery handling for Rmgr RM_STANDBY_ID
 *
 * These record types will only be created if XLogStandbyInfoActive()
 * --------------------------------------------------------------------
 */

pub unsafe fn standby_redo(record: *mut XLogReaderState) {
    let info: uint8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /* Backup blocks are not used in standby records */
    Assert!(!XLogRecHasAnyBlockRefs(record));

    /* Do nothing if we're not in hot standby mode */
    if standbyState == STANDBY_DISABLED {
        return;
    }

    if info == XLOG_STANDBY_LOCK {
        let xlrec: *mut xl_standby_locks = XLogRecGetData(record) as *mut xl_standby_locks;

        for i in 0..(*xlrec).nlocks {
            let lock = (*xlrec).locks.as_ptr().add(i as usize);
            StandbyAcquireAccessExclusiveLock((*lock).xid, (*lock).dbOid, (*lock).relOid);
        }
    } else if info == XLOG_RUNNING_XACTS {
        let xlrec: *mut xl_running_xacts = XLogRecGetData(record) as *mut xl_running_xacts;
        let mut running: RunningTransactionsData = core::mem::zeroed();

        running.xcnt = (*xlrec).xcnt;
        running.subxcnt = (*xlrec).subxcnt;
        running.subxid_status = if (*xlrec).subxid_overflow {
            SUBXIDS_MISSING
        } else {
            SUBXIDS_IN_ARRAY
        };
        running.nextXid = (*xlrec).nextXid;
        running.latestCompletedXid = (*xlrec).latestCompletedXid;
        running.oldestRunningXid = (*xlrec).oldestRunningXid;
        running.xids = (*xlrec).xids.as_mut_ptr();

        ProcArrayApplyRecoveryInfo(&mut running);

        /*
         * The startup process currently has no convenient way to schedule
         * stats to be reported. XLOG_RUNNING_XACTS records issued at a
         * regular cadence, making this a convenient location to report stats.
         * While these records aren't generated with wal_level=minimal, stats
         * also cannot be accessed during WAL replay.
         */
        pgstat_report_stat(true);
    } else if info == XLOG_INVALIDATIONS {
        let xlrec: *mut xl_invalidations = XLogRecGetData(record) as *mut xl_invalidations;

        ProcessCommittedInvalidationMessages(
            (*xlrec).msgs.as_mut_ptr(),
            (*xlrec).nmsgs,
            (*xlrec).relcacheInitFileInval,
            (*xlrec).dbId,
            (*xlrec).tsId,
        );
    } else {
        elog!(PANIC, "standby_redo: unknown op code {}", info);
    }
}

/*
 * Log details of the current snapshot to WAL. This allows the snapshot state
 * to be reconstructed on the standby and for logical decoding.
 *
 * This is used for Hot Standby as follows:
 *
 * We can move directly to STANDBY_SNAPSHOT_READY at startup if we
 * start from a shutdown checkpoint because we know nothing was running
 * at that time and our recovery snapshot is known empty. In the more
 * typical case of an online checkpoint we need to jump through a few
 * hoops to get a correct recovery snapshot and this requires a two or
 * sometimes a three stage process.
 *
 * The initial snapshot must contain all running xids and all current
 * AccessExclusiveLocks at a point in time on the standby. Assembling
 * that information while the server is running requires many and
 * various LWLocks, so we choose to derive that information piece by
 * piece and then re-assemble that info on the standby. When that
 * information is fully assembled we move to STANDBY_SNAPSHOT_READY.
 *
 * Since locking on the primary when we derive the information is not
 * strict, we note that there is a time window between the derivation and
 * writing to WAL of the derived information. That allows race conditions
 * that we must resolve, since xids and locks may enter or leave the
 * snapshot during that window. This creates the issue that an xid or
 * lock may start *after* the snapshot has been derived yet *before* the
 * snapshot is logged in the running xacts WAL record. We resolve this by
 * starting to accumulate changes at a point just prior to when we derive
 * the snapshot on the primary, then ignore duplicates when we later apply
 * the snapshot from the running xacts record. This is implemented during
 * CreateCheckPoint() where we use the logical checkpoint location as
 * our starting point and then write the running xacts record immediately
 * before writing the main checkpoint WAL record. Since we always start
 * up from a checkpoint and are immediately at our starting point, we
 * unconditionally move to STANDBY_INITIALIZED. After this point we
 * must do 4 things:
 *	* move shared nextXid forwards as we see new xids
 *	* extend the clog and subtrans with each new xid
 *	* keep track of uncommitted known assigned xids
 *	* keep track of uncommitted AccessExclusiveLocks
 *
 * When we see a commit/abort we must remove known assigned xids and locks
 * from the completing transaction. Attempted removals that cannot locate
 * an entry are expected and must not cause an error when we are in state
 * STANDBY_INITIALIZED. This is implemented in StandbyReleaseLocks() and
 * KnownAssignedXidsRemove().
 *
 * Later, when we apply the running xact data we must be careful to ignore
 * transactions already committed, since those commits raced ahead when
 * making WAL entries.
 *
 * For logical decoding only the running xacts information is needed;
 * there's no need to look at the locking information, but it's logged anyway,
 * as there's no independent knob to just enable logical decoding. For
 * details of how this is used, check snapbuild.c's introductory comment.
 *
 *
 * Returns the RecPtr of the last inserted record.
 */
pub unsafe fn LogStandbySnapshot() -> XLogRecPtr {
    let recptr: XLogRecPtr;
    let running: RunningTransactions;
    let locks: *mut xl_standby_lock;
    let mut nlocks: c_int = 0;

    Assert!(XLogStandbyInfoActive());

    // #ifdef USE_INJECTION_POINTS
    if IS_INJECTION_POINT_ATTACHED(c"skip-log-running-xacts".as_ptr()) {
        /*
         * This record could move slot's xmin forward during decoding, leading
         * to unpredictable results, so skip it when requested by the test.
         */
        return GetInsertRecPtr();
    }
    // #endif

    /*
     * Get details of any AccessExclusiveLocks being held at the moment.
     */
    locks = GetRunningTransactionLocks(&mut nlocks);
    if nlocks > 0 {
        LogAccessExclusiveLocks(nlocks, locks);
    }
    pfree(locks as *mut c_void);

    /*
     * Log details of all in-progress transactions. This should be the last
     * record we write, because standby will open up when it sees this.
     */
    running = GetRunningTransactionData();

    /*
     * GetRunningTransactionData() acquired ProcArrayLock, we must release it.
     * For Hot Standby this can be done before inserting the WAL record
     * because ProcArrayApplyRecoveryInfo() rechecks the commit status using
     * the clog. For logical decoding, though, the lock can't be released
     * early because the clog might be "in the future" from the POV of the
     * historic snapshot. This would allow for situations where we're waiting
     * for the end of a transaction listed in the xl_running_xacts record
     * which, according to the WAL, has committed before the xl_running_xacts
     * record. Fortunately this routine isn't executed frequently, and it's
     * only a shared lock.
     */
    if wal_level < WAL_LEVEL_LOGICAL {
        LWLockRelease(ProcArrayLock);
    }

    recptr = LogCurrentRunningXacts(running);

    /* Release lock if we kept it longer ... */
    if wal_level >= WAL_LEVEL_LOGICAL {
        LWLockRelease(ProcArrayLock);
    }

    /* GetRunningTransactionData() acquired XidGenLock, we must release it */
    LWLockRelease(XidGenLock);

    recptr
}

/*
 * Record an enhanced snapshot of running transactions into WAL.
 *
 * The definitions of RunningTransactionsData and xl_running_xacts are
 * similar. We keep them separate because xl_running_xacts is a contiguous
 * chunk of memory and never exists fully until it is assembled in WAL.
 * The inserted records are marked as not being important for durability,
 * to avoid triggering superfluous checkpoint / archiving activity.
 */
unsafe fn LogCurrentRunningXacts(CurrRunningXacts: RunningTransactions) -> XLogRecPtr {
    let mut xlrec: xl_running_xacts = core::mem::zeroed();
    let recptr: XLogRecPtr;

    xlrec.xcnt = (*CurrRunningXacts).xcnt;
    xlrec.subxcnt = (*CurrRunningXacts).subxcnt;
    xlrec.subxid_overflow = (*CurrRunningXacts).subxid_status != SUBXIDS_IN_ARRAY;
    xlrec.nextXid = (*CurrRunningXacts).nextXid;
    xlrec.oldestRunningXid = (*CurrRunningXacts).oldestRunningXid;
    xlrec.latestCompletedXid = (*CurrRunningXacts).latestCompletedXid;

    /* Header */
    XLogBeginInsert();
    XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);
    XLogRegisterData(
        &xlrec as *const xl_running_xacts as *const c_void,
        MinSizeOfXactRunningXacts() as c_int,
    );

    /* array of TransactionIds */
    if xlrec.xcnt > 0 {
        XLogRegisterData(
            (*CurrRunningXacts).xids as *const c_void,
            ((xlrec.xcnt + xlrec.subxcnt) as usize * core::mem::size_of::<TransactionId>()) as c_int,
        );
    }

    recptr = XLogInsert(RM_STANDBY_ID, XLOG_RUNNING_XACTS);

    if xlrec.subxid_overflow {
        elog!(
            DEBUG2,
            "snapshot of {} running transactions overflowed (lsn {:X}/{:X} oldest xid {} latest complete {} next xid {})",
            (*CurrRunningXacts).xcnt,
            (recptr >> 32) as uint32,
            recptr as uint32,
            (*CurrRunningXacts).oldestRunningXid,
            (*CurrRunningXacts).latestCompletedXid,
            (*CurrRunningXacts).nextXid
        );
    } else {
        elog!(
            DEBUG2,
            "snapshot of {}+{} running transaction ids (lsn {:X}/{:X} oldest xid {} latest complete {} next xid {})",
            (*CurrRunningXacts).xcnt,
            (*CurrRunningXacts).subxcnt,
            (recptr >> 32) as uint32,
            recptr as uint32,
            (*CurrRunningXacts).oldestRunningXid,
            (*CurrRunningXacts).latestCompletedXid,
            (*CurrRunningXacts).nextXid
        );
    }

    /*
     * Ensure running_xacts information is synced to disk not too far in the
     * future. We don't want to stall anything though (i.e. use XLogFlush()),
     * so we let the wal writer do it during normal operation.
     * XLogSetAsyncXactLSN() conveniently will mark the LSN as to-be-synced
     * and nudge the WALWriter into action if sleeping. Check
     * XLogBackgroundFlush() for details why a record might not be flushed
     * without it.
     */
    XLogSetAsyncXactLSN(recptr);

    recptr
}

/*
 * Wholesale logging of AccessExclusiveLocks. Other lock types need not be
 * logged, as described in backend/storage/lmgr/README.
 */
unsafe fn LogAccessExclusiveLocks(nlocks: c_int, locks: *mut xl_standby_lock) {
    let mut xlrec: xl_standby_locks = core::mem::zeroed();

    xlrec.nlocks = nlocks;

    XLogBeginInsert();
    XLogRegisterData(
        &xlrec as *const xl_standby_locks as *const c_void,
        core::mem::offset_of!(xl_standby_locks, locks) as c_int,
    );
    XLogRegisterData(
        locks as *const c_void,
        (nlocks as usize * core::mem::size_of::<xl_standby_lock>()) as c_int,
    );
    XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);

    let _ = XLogInsert(RM_STANDBY_ID, XLOG_STANDBY_LOCK);
}

/*
 * Individual logging of AccessExclusiveLocks for use during LockAcquire()
 */
pub unsafe fn LogAccessExclusiveLock(dbOid: Oid, relOid: Oid) {
    let mut xlrec: xl_standby_lock = core::mem::zeroed();

    xlrec.xid = GetCurrentTransactionId();

    xlrec.dbOid = dbOid;
    xlrec.relOid = relOid;

    LogAccessExclusiveLocks(1, &mut xlrec);
    MyXactFlags |= XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK;
}

/*
 * Prepare to log an AccessExclusiveLock, for use during LockAcquire()
 */
pub unsafe fn LogAccessExclusiveLockPrepare() {
    /*
     * Ensure that a TransactionId has been assigned to this transaction, for
     * two reasons, both related to lock release on the standby. First, we
     * must assign an xid so that RecordTransactionCommit() and
     * RecordTransactionAbort() do not optimise away the transaction
     * completion record which recovery relies upon to release locks. It's a
     * hack, but for a corner case not worth adding code for into the main
     * commit path. Second, we must assign an xid before the lock is recorded
     * in shared memory, otherwise a concurrently executing
     * GetRunningTransactionLocks() might see a lock associated with an
     * InvalidTransactionId which we later assert cannot happen.
     */
    let _ = GetCurrentTransactionId();
}

/*
 * Emit WAL for invalidations. This currently is only used for commits without
 * an xid but which contain invalidations.
 */
pub unsafe fn LogStandbyInvalidations(
    nmsgs: c_int,
    msgs: *mut SharedInvalidationMessage,
    relcacheInitFileInval: bool,
) {
    let mut xlrec: xl_invalidations = core::mem::zeroed();

    /* prepare record */
    memset(
        &mut xlrec as *mut xl_invalidations as *mut c_void,
        0,
        core::mem::size_of::<xl_invalidations>(),
    );
    xlrec.dbId = MyDatabaseId;
    xlrec.tsId = MyDatabaseTableSpace;
    xlrec.relcacheInitFileInval = relcacheInitFileInval;
    xlrec.nmsgs = nmsgs;

    /* perform insertion */
    XLogBeginInsert();
    XLogRegisterData(
        &xlrec as *const xl_invalidations as *const c_void,
        MinSizeOfInvalidations() as c_int,
    );
    XLogRegisterData(
        msgs as *const c_void,
        (nmsgs as usize * core::mem::size_of::<SharedInvalidationMessage>()) as c_int,
    );
    XLogInsert(RM_STANDBY_ID, XLOG_INVALIDATIONS);
}

/* Return the description of recovery conflict */
unsafe fn get_recovery_conflict_desc(reason: ProcSignalReason) -> *const c_char {
    let mut reasonDesc: *const c_char = gettext(c"unknown reason".as_ptr());

    match reason {
        PROCSIG_RECOVERY_CONFLICT_BUFFERPIN => {
            reasonDesc = gettext(c"recovery conflict on buffer pin".as_ptr());
        }
        PROCSIG_RECOVERY_CONFLICT_LOCK => {
            reasonDesc = gettext(c"recovery conflict on lock".as_ptr());
        }
        PROCSIG_RECOVERY_CONFLICT_TABLESPACE => {
            reasonDesc = gettext(c"recovery conflict on tablespace".as_ptr());
        }
        PROCSIG_RECOVERY_CONFLICT_SNAPSHOT => {
            reasonDesc = gettext(c"recovery conflict on snapshot".as_ptr());
        }
        PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT => {
            reasonDesc = gettext(c"recovery conflict on replication slot".as_ptr());
        }
        PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK => {
            reasonDesc = gettext(c"recovery conflict on buffer deadlock".as_ptr());
        }
        PROCSIG_RECOVERY_CONFLICT_DATABASE => {
            reasonDesc = gettext(c"recovery conflict on database".as_ptr());
        }
        _ => {}
    }

    reasonDesc
}
