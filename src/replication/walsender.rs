//! replication/walsender.c - WAL sender process.
//!
//! The WAL sender process (walsender) takes care of sending XLOG from the
//! primary server to a single recipient.  It is started by the postmaster
//! when the walreceiver of a standby server connects to the primary server
//! and requests XLOG streaming replication.
//!
//! Normal termination is by SIGTERM, which instructs the walsender to
//! close the connection and exit(0) at the next convenient moment.
//!
//! Portions Copyright (c) 2010-2025, PostgreSQL Global Development Group

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::libpq::pqformat::pq_getmsgint;
use crate::{foreach, current_cell, ereport, elog, errmsg};

use std::ffi::{c_char, c_int, c_uint, c_ulong, c_void};

// ---------------------------------------------------------------------------
// Imports from ported modules
// ---------------------------------------------------------------------------

use crate::access::transam::xlogdefs::{
    InvalidXLogRecPtr, XLogRecPtr, XLogSegNo, TimeLineID,
};
use crate::lib::stringinfo::StringInfoData;
use crate::nodes::replnodes::{
    ReplicationKind, REPLICATION_KIND_LOGICAL, REPLICATION_KIND_PHYSICAL,
};

// Re-export types defined in walsender_private so callers see them here too.
pub use crate::replication::walsender_private::{
    WalSnd, WalSndCtlData, WalSndState,
    WALSNDSTATE_STARTUP, WALSNDSTATE_BACKUP, WALSNDSTATE_CATCHUP,
    WALSNDSTATE_STREAMING, WALSNDSTATE_STOPPING,
    SYNC_STANDBY_INIT, SYNC_STANDBY_DEFINED,
    NUM_SYNC_REP_WAIT_MODE,
    TimestampTz, TimeOffset, pid_t, slock_t,
    yyscan_t,
    replication_scanner_init, replication_scanner_finish,
    replication_scanner_is_replication_command,
    replication_yyparse, WalSndSetState,
};

// ---------------------------------------------------------------------------
// Stubs for unported modules
// TODO(pg-port): real symbols live in the files noted
// ---------------------------------------------------------------------------

/// STUB: XLogReaderState - access/xlogreader.h
#[repr(C)]
pub struct XLogReaderState {
    pub seg: XLogReaderSeg,
    pub segcxt: XLogReaderSegContext,
    pub currTLI: TimeLineID,
    pub currTLIValidUntil: XLogRecPtr,
    pub nextTLI: TimeLineID,
    pub EndRecPtr: XLogRecPtr,
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct XLogReaderSeg {
    pub ws_file: c_int,
    pub ws_tli: TimeLineID,
}
#[repr(C)]
pub struct XLogReaderSegContext {
    pub ws_segsize: u32,
}

/// STUB: XLogRecord - access/xlogreader.h
pub type XLogRecord = c_void;

/// STUB: WALReadError - access/xlogreader.h
pub type WALReadError = c_void;

/// STUB: LogicalDecodingContext - replication/logical.h
/// TODO(pg-port): real LogicalDecodingContext lives in replication/logical.h
#[repr(C)]
pub struct LogicalDecodingContext {
    pub out: *mut StringInfoData,
    pub reader: *mut XLogReaderState,
    pub end_xact: bool,
    pub snapshot_builder: *mut c_void,
    _opaque: [u8; 0],
}

/// STUB: IncrementalBackupInfo - backup/basebackup_incremental.h
/// TODO(pg-port): real IncrementalBackupInfo lives in backup/basebackup_incremental.h
pub type IncrementalBackupInfo = c_void;

/// STUB: MemoryContext - utils/palloc.h
/// TODO(pg-port): real MemoryContext lives in utils/palloc.h
pub type MemoryContext = *mut c_void;

/// STUB: Datum - postgres.h
pub type Datum = usize;

/// STUB: TransactionId - access/transam.h
pub type TransactionId = u32;

/// STUB: FullTransactionId - access/transam.h
#[repr(C)]
pub struct FullTransactionId {
    pub value: u64,
}

/// STUB: ReplicationSlot - replication/slot.h
/// TODO(pg-port): real ReplicationSlot lives in replication/slot.h
pub type ReplicationSlot = c_void;

/// STUB: Node - nodes/nodes.h
pub type Node = crate::nodes::nodes::Node;

/// STUB: List - nodes/pg_list.h
/// TODO(pg-port): real List lives in nodes/pg_list.h
pub type List = c_void;

/// STUB: DefElem - nodes/parsenodes.h
/// TODO(pg-port): real DefElem lives in nodes/parsenodes.h
pub type DefElem = c_void;

/// STUB: CRSSnapshotAction - replication/slot.h
/// TODO(pg-port): real CRSSnapshotAction lives in replication/slot.h
pub type CRSSnapshotAction = c_int;
pub const CRS_EXPORT_SNAPSHOT: CRSSnapshotAction = 0;
pub const CRS_NOEXPORT_SNAPSHOT: CRSSnapshotAction = 1;
pub const CRS_USE_SNAPSHOT: CRSSnapshotAction = 2;

/// STUB: WaitEvent - storage/latch.h / storage/waiteventset.h
pub type WaitEvent = c_void;

/// STUB: SyncRepStandbyData - replication/syncrep.h
/// TODO(pg-port): real SyncRepStandbyData lives in replication/syncrep.h
#[repr(C)]
pub struct SyncRepStandbyData {
    pub walsnd_index: c_int,
    pub pid: pid_t,
}

/// STUB: ReturnSetInfo - funcapi.h
pub type ReturnSetInfo = c_void;

/// STUB: fcinfo / FunctionCallInfo - fmgr.h
pub type FunctionCallInfo = *mut c_void;

/// STUB: Interval - datatype/timestamp.h
/// TODO(pg-port): real Interval lives in datatype/timestamp.h
#[repr(C)]
pub struct Interval {
    pub time: i64,
    pub day: i32,
    pub month: i32,
}

/// STUB: TupleDesc - access/tupdesc.h
pub type TupleDesc = *mut c_void;

/// STUB: DestReceiver - tcop/dest.h
pub type DestReceiver = c_void;

/// STUB: TupOutputState - access/tupdesc.h
pub type TupOutputState = c_void;

/// STUB: ReadReplicationSlotCmd / CreateReplicationSlotCmd etc - nodes/replnodes.h
/// TODO(pg-port): real cmd types live in nodes/replnodes.h
pub type ReadReplicationSlotCmd = c_void;
pub type CreateReplicationSlotCmd = c_void;
pub type DropReplicationSlotCmd = c_void;
pub type AlterReplicationSlotCmd = c_void;
pub type StartReplicationCmd = c_void;
pub type TimeLineHistoryCmd = c_void;
pub type BaseBackupCmd = c_void;
pub type VariableShowStmt = c_void;
pub type UploadManifestCmd = c_void;

/// STUB: AttrNumber - access/attnum.h
pub type AttrNumber = i16;

/// STUB: Oid - postgres_ext.h
pub type Oid = u32;

/// STUB: QueryCompletion - tcop/cmdtag.h
pub type QueryCompletion = c_void;

/// STUB: Snapshot - utils/snapshot.h
pub type Snapshot = *mut c_void;

/// STUB: PGPROC - storage/proc.h
pub type PGPROC = c_void;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum interval used by walsender for stats flushes, in ms
const WALSENDER_STATS_FLUSH_INTERVAL: i64 = 1000;

/// Maximum data payload in a WAL data message.  Must be >= XLOG_BLCKSZ.
const MAX_SEND_SIZE: usize = 8192 * 16; // XLOG_BLCKSZ * 16

const LAG_TRACKER_BUFFER_SIZE: usize = 8192;

const WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS: i64 = 1000;

// ---------------------------------------------------------------------------
// Global variables (shared-memory pointers declared in walsender_private)
// ---------------------------------------------------------------------------

/// Am I a walsender process?
#[unsafe(no_mangle)]
pub static mut am_walsender: bool = false;

/// Am I cascading WAL to another standby?
#[unsafe(no_mangle)]
pub static mut am_cascading_walsender: bool = false;

/// Connected to a database?
#[unsafe(no_mangle)]
pub static mut am_db_walsender: bool = false;

/// GUC: the maximum number of concurrent walsenders
#[unsafe(no_mangle)]
pub static mut max_wal_senders: c_int = 10;

/// GUC: maximum time to send one WAL data message (ms)
#[unsafe(no_mangle)]
pub static mut wal_sender_timeout: c_int = 60 * 1000;

/// GUC: log replication commands
#[unsafe(no_mangle)]
pub static mut log_replication_commands: bool = false;

/// State for WalSndWakeupRequest
#[unsafe(no_mangle)]
pub static mut wake_wal_senders: bool = false;

// ---------------------------------------------------------------------------
// Static (file-local) state
// ---------------------------------------------------------------------------

static mut xlogreader: *mut XLogReaderState = std::ptr::null_mut();

static mut uploaded_manifest: *mut IncrementalBackupInfo = std::ptr::null_mut();
static mut uploaded_manifest_mcxt: MemoryContext = std::ptr::null_mut();

static mut sendTimeLine: TimeLineID = 0;
static mut sendTimeLineNextTLI: TimeLineID = 0;
static mut sendTimeLineIsHistoric: bool = false;
static mut sendTimeLineValidUpto: XLogRecPtr = InvalidXLogRecPtr;

/// Next WAL location to send (also in MyWalSnd->sentPtr)
static mut sentPtr: XLogRecPtr = InvalidXLogRecPtr;

static mut output_message: StringInfoData = StringInfoData {
    data: std::ptr::null_mut(),
    len: 0,
    maxlen: 0,
    cursor: 0,
};
static mut reply_message: StringInfoData = StringInfoData {
    data: std::ptr::null_mut(),
    len: 0,
    maxlen: 0,
    cursor: 0,
};
static mut tmpbuf: StringInfoData = StringInfoData {
    data: std::ptr::null_mut(),
    len: 0,
    maxlen: 0,
    cursor: 0,
};

static mut last_processing: TimestampTz = 0;
static mut last_reply_timestamp: TimestampTz = 0;
static mut waiting_for_ping_response: bool = false;

static mut streamingDoneSending: bool = false;
static mut streamingDoneReceiving: bool = false;

static mut WalSndCaughtUp: bool = false;

/// Flags set by signal handlers
static mut got_SIGUSR2: bool = false;
static mut got_STOPPING: bool = false;

/// Set while streaming; controls how PROCSIG_WALSND_INIT_STOPPING is handled
static mut replication_active: bool = false;

static mut logical_decoding_ctx: *mut LogicalDecodingContext = std::ptr::null_mut();

// ---------------------------------------------------------------------------
// Lag tracker types
// ---------------------------------------------------------------------------

/// A sample associating a WAL location with the time it was written.
#[repr(C)]
#[derive(Copy, Clone, Default)]
pub struct WalTimeSample {
    pub lsn: XLogRecPtr,
    pub time: TimestampTz,
}

/// A mechanism for tracking replication lag.
#[repr(C)]
pub struct LagTracker {
    pub last_lsn: XLogRecPtr,
    pub buffer: [WalTimeSample; LAG_TRACKER_BUFFER_SIZE],
    pub write_head: c_int,
    pub read_heads: [c_int; NUM_SYNC_REP_WAIT_MODE],
    pub last_read: [WalTimeSample; NUM_SYNC_REP_WAIT_MODE],
    /// Overflow entries for read heads that collide with the write head.
    pub overflowed: [WalTimeSample; NUM_SYNC_REP_WAIT_MODE],
}

static mut lag_tracker: *mut LagTracker = std::ptr::null_mut();

// ---------------------------------------------------------------------------
// Stub helpers for unported C functions
// TODO(pg-port): replace each with a real import when the module lands
// ---------------------------------------------------------------------------

unsafe fn RecoveryInProgress() -> bool { unimplemented!() }
unsafe fn GetFlushRecPtr(tli: *mut TimeLineID) -> XLogRecPtr { unimplemented!() }
unsafe fn GetXLogReplayRecPtr(tli: *mut TimeLineID) -> XLogRecPtr { unimplemented!() }
unsafe fn GetWALInsertionTimeLine() -> TimeLineID { unimplemented!() }
unsafe fn GetXLogInsertRecPtr() -> XLogRecPtr { unimplemented!() }
unsafe fn XLogFlush(lsn: XLogRecPtr) { unimplemented!() }
unsafe fn GetSystemIdentifier() -> u64 { unimplemented!() }
unsafe fn GetCurrentTimestamp() -> TimestampTz { unimplemented!() }
unsafe fn TimestampTzPlusMilliseconds(t: TimestampTz, ms: i64) -> TimestampTz { unimplemented!() }
unsafe fn TimestampDifferenceMilliseconds(t1: TimestampTz, t2: TimestampTz) -> i64 { unimplemented!() }
unsafe fn TimestampDifferenceExceeds(t1: TimestampTz, t2: TimestampTz, ms: i64) -> bool { unimplemented!() }
unsafe fn timestamptz_to_str(t: TimestampTz) -> *const c_char { unimplemented!() }
unsafe fn pstrdup(s: *const c_char) -> *mut c_char { unimplemented!() }
unsafe fn pfree(ptr: *mut c_void) { unimplemented!() }
unsafe fn palloc(size: usize) -> *mut c_void { unimplemented!() }

/// TODO(pg-port): MemoryContextAllocZero lives in utils/palloc.h
unsafe fn MemoryContextAllocZero(ctx: MemoryContext, size: usize) -> *mut c_void { unimplemented!() }
unsafe fn MemoryContextDelete(ctx: MemoryContext) { unimplemented!() }
unsafe fn MemoryContextSetParent(ctx: MemoryContext, parent: MemoryContext) { unimplemented!() }
unsafe fn MemoryContextReset(ctx: MemoryContext) { unimplemented!() }
unsafe fn MemoryContextStrdup(ctx: MemoryContext, s: *const c_char) -> *mut c_char { unimplemented!() }
unsafe fn AllocSetContextCreate(parent: MemoryContext, name: *const c_char, sizes: usize) -> MemoryContext { unimplemented!() }
unsafe fn MemoryContextSwitchTo(ctx: MemoryContext) -> MemoryContext { unimplemented!() }

/// TODO(pg-port): proc_exit lives in storage/ipc.h
unsafe fn proc_exit(code: c_int) -> ! { unimplemented!() }
unsafe fn kill(pid: pid_t, sig: c_int) { unimplemented!() }

/// TODO(pg-port): pg_usleep lives in port.h
unsafe fn pg_usleep(usec: u64) { unimplemented!() }

/// TODO(pg-port): latch functions live in storage/latch.h
unsafe fn ResetLatch(latch: *mut c_void) { unimplemented!() }
unsafe fn SetLatch(latch: *mut c_void) { unimplemented!() }
static mut MyLatch: *mut c_void = std::ptr::null_mut();

/// TODO(pg-port): spinlock macros live in storage/spin.h
unsafe fn SpinLockAcquire(lock: *mut slock_t) { unimplemented!() }
unsafe fn SpinLockRelease(lock: *mut slock_t) { unimplemented!() }
unsafe fn SpinLockInit(lock: *mut slock_t) { unimplemented!() }

/// TODO(pg-port): LWLock functions live in storage/lwlock.h
unsafe fn LWLockAcquire(lock: usize, mode: c_int) -> bool { unimplemented!() }
unsafe fn LWLockRelease(lock: usize) { unimplemented!() }
unsafe fn LWLockReleaseAll() { unimplemented!() }

/// TODO(pg-port): condition variable functions live in storage/condition_variable.h
unsafe fn ConditionVariableBroadcast(cv: *mut c_void) { unimplemented!() }
unsafe fn ConditionVariablePrepareToSleep(cv: *mut c_void) { unimplemented!() }
unsafe fn ConditionVariableCancelSleep() { unimplemented!() }
unsafe fn ConditionVariableInit(cv: *mut c_void) { unimplemented!() }

/// TODO(pg-port): pq* functions live in libpq/pqformat.h + libpq/libpq.h
unsafe fn pq_beginmessage(buf: *mut StringInfoData, msgtype: u8) { unimplemented!() }
unsafe fn pq_sendbyte(buf: *mut StringInfoData, b: u8) { unimplemented!() }
unsafe fn pq_sendint16(buf: *mut StringInfoData, v: i16) { unimplemented!() }
unsafe fn pq_sendint32(buf: *mut StringInfoData, v: i32) { unimplemented!() }
unsafe fn pq_sendint64(buf: *mut StringInfoData, v: i64) { unimplemented!() }
unsafe fn pq_sendint(buf: *mut StringInfoData, v: c_uint, b: c_int) -> c_uint { unimplemented!() }
unsafe fn pq_sendbytes(buf: *mut StringInfoData, data: *const c_char, n: usize) { unimplemented!() }
unsafe fn pq_endmessage(buf: *mut StringInfoData) { unimplemented!() }
unsafe fn pq_endmessage_reuse(buf: *mut StringInfoData) { unimplemented!() }
unsafe fn pq_flush() { unimplemented!() }
unsafe fn pq_flush_if_writable() -> c_int { unimplemented!() }
unsafe fn pq_is_send_pending() -> bool { unimplemented!() }
unsafe fn pq_putmessage_noblock(msgtype: c_char, data: *const c_char, len: usize) { unimplemented!() }
unsafe fn pq_startmsgread() { unimplemented!() }
unsafe fn pq_endmsgread() { unimplemented!() }
unsafe fn pq_getbyte() -> c_int { unimplemented!() }
unsafe fn pq_getbyte_if_available(b: *mut u8) -> c_int { unimplemented!() }
unsafe fn pq_getmessage(buf: *mut StringInfoData, maxlen: c_int) -> c_int { unimplemented!() }
unsafe fn pq_getmsgbyte(buf: *mut StringInfoData) -> c_char { unimplemented!() }
unsafe fn pq_getmsgint64(buf: *mut StringInfoData) -> i64 { unimplemented!() }
unsafe fn pq_getmsgstring(buf: *mut StringInfoData) -> *const c_char { unimplemented!() }

/// TODO(pg-port): StringInfo functions live in lib/stringinfo.h
unsafe fn initStringInfo(buf: *mut StringInfoData) { unimplemented!() }
unsafe fn resetStringInfo(buf: *mut StringInfoData) { unimplemented!() }
unsafe fn enlargeStringInfo(buf: *mut StringInfoData, needed: usize) { unimplemented!() }

/// TODO(pg-port): XLogReaderAllocate / XLogReadRecord live in access/xlogreader.h
unsafe fn XLogReaderAllocate(segsize: u32, wal_segment_directory: *const c_char, routine: usize, private_data: *mut c_void) -> *mut XLogReaderState { unimplemented!() }
unsafe fn XLogReadRecord(state: *mut XLogReaderState, errm: *mut *const c_char) -> *mut XLogRecord { unimplemented!() }
unsafe fn XLogBeginRead(state: *mut XLogReaderState, recptr: XLogRecPtr) { unimplemented!() }
unsafe fn XLogReadDetermineTimeline(state: *mut XLogReaderState, ptr: XLogRecPtr, reqlen: c_int, tli: TimeLineID) { unimplemented!() }

/// TODO(pg-port): WALRead / WALReadFromBuffers live in access/xlogutils.h
unsafe fn WALRead(state: *mut XLogReaderState, buf: *mut c_char, ptr: XLogRecPtr, count: usize, tli: TimeLineID, errinfo: *mut WALReadError) -> bool { unimplemented!() }
unsafe fn WALReadFromBuffers(buf: *mut c_char, ptr: XLogRecPtr, count: usize, tli: TimeLineID) -> usize { unimplemented!() }
unsafe fn WALReadRaiseError(errinfo: *mut WALReadError) { unimplemented!() }

/// TODO(pg-port): wal_segment_close / BasicOpenFile live in access/xlogutils.h / storage/fd.h
unsafe fn wal_segment_close(state: *mut XLogReaderState) { unimplemented!() }
unsafe fn BasicOpenFile(path: *const c_char, flags: c_int) -> c_int { unimplemented!() }
unsafe fn OpenTransientFile(path: *const c_char, flags: c_int) -> c_int { unimplemented!() }
unsafe fn CloseTransientFile(fd: c_int) -> c_int { unimplemented!() }

/// TODO(pg-port): CheckXLogRemoved lives in access/xlog.c
unsafe fn CheckXLogRemoved(segno: XLogSegNo, tli: TimeLineID) { unimplemented!() }

/// TODO(pg-port): XLByteToSeg / XLogFilePath / XLogFileName / TLHistoryFileName/FilePath macros live in access/xlog_internal.h
unsafe fn XLByteToSeg_fn(ptr: XLogRecPtr, segno: &mut XLogSegNo, segsize: u32) { unimplemented!() }
unsafe fn XLogFilePath_fn(path: *mut c_char, tli: TimeLineID, segno: XLogSegNo, segsize: u32) { unimplemented!() }
unsafe fn XLogFileName_fn(path: *mut c_char, tli: TimeLineID, segno: XLogSegNo, segsize: u32) { unimplemented!() }
unsafe fn TLHistoryFileName_fn(fname: *mut c_char, tli: TimeLineID) { unimplemented!() }
unsafe fn TLHistoryFilePath_fn(path: *mut c_char, tli: TimeLineID) { unimplemented!() }

/// TODO(pg-port): timeline history functions live in access/timeline.h
unsafe fn readTimeLineHistory(tli: TimeLineID) -> *mut List { unimplemented!() }
unsafe fn tliSwitchPoint(tli: TimeLineID, history: *mut List, nextTli: *mut TimeLineID) -> XLogRecPtr { unimplemented!() }
unsafe fn tliOfPointInHistory(lsn: XLogRecPtr, history: *mut List) -> TimeLineID { unimplemented!() }
unsafe fn list_free_deep(list: *mut List) { unimplemented!() }

/// TODO(pg-port): GUC variable wal_segment_size lives in access/xlog.h
static wal_segment_size: u32 = 16 * 1024 * 1024;

/// TODO(pg-port): replication slot functions live in replication/slot.h
unsafe fn ReplicationSlotAcquire(name: *const c_char, nowait: bool, release_on_error: bool) { unimplemented!() }
unsafe fn ReplicationSlotRelease() { unimplemented!() }
unsafe fn ReplicationSlotCleanup(flush: bool) { unimplemented!() }
unsafe fn ReplicationSlotCreate(name: *const c_char, logical: bool, persistence: c_int, two_phase: bool, failover: bool, failover_given: bool) { unimplemented!() }
unsafe fn ReplicationSlotReserveWal() { unimplemented!() }
unsafe fn ReplicationSlotMarkDirty() { unimplemented!() }
unsafe fn ReplicationSlotSave() { unimplemented!() }
unsafe fn ReplicationSlotPersist() { unimplemented!() }
unsafe fn ReplicationSlotDrop(name: *const c_char, nowait: bool) { unimplemented!() }
unsafe fn ReplicationSlotAlter(name: *const c_char, failover: *const bool, two_phase: *const bool) { unimplemented!() }
unsafe fn ReplicationSlotsComputeRequiredLSN() { unimplemented!() }
unsafe fn ReplicationSlotsComputeRequiredXmin(startup: bool) { unimplemented!() }
unsafe fn SearchNamedReplicationSlot(name: *const c_char, use_lock: bool) -> *mut ReplicationSlot { unimplemented!() }
unsafe fn SlotIsLogical(slot: *const ReplicationSlot) -> bool { unimplemented!() }
unsafe fn SlotIsPhysical(slot: *const ReplicationSlot) -> bool { unimplemented!() }
unsafe fn SlotExistsInSyncStandbySlots(name: *const c_char) -> bool { unimplemented!() }
unsafe fn StandbySlotsHaveCaughtup(lsn: XLogRecPtr, elevel: c_int) -> bool { unimplemented!() }
static mut MyReplicationSlot: *mut ReplicationSlot = std::ptr::null_mut();
static ReplicationSlotControlLock: usize = 0;

/// TODO(pg-port): logical decoding functions live in replication/logical.h / replication/decode.h
unsafe fn CreateDecodingContext(startpoint: XLogRecPtr, options: *mut List, fast_forward: bool, routine: usize, prepare_write: usize, write: usize, update_progress: usize) -> *mut LogicalDecodingContext { unimplemented!() }
unsafe fn CreateInitDecodingContext(plugin: *const c_char, options: *mut List, need_full_snapshot: bool, startpoint: XLogRecPtr, routine: usize, prepare_write: usize, write: usize, update_progress: usize) -> *mut LogicalDecodingContext { unimplemented!() }
unsafe fn FreeDecodingContext(ctx: *mut LogicalDecodingContext) { unimplemented!() }
unsafe fn DecodingContextFindStartpoint(ctx: *mut LogicalDecodingContext) { unimplemented!() }
unsafe fn LogicalDecodingProcessRecord(ctx: *mut LogicalDecodingContext, reader: *mut XLogReaderState) { unimplemented!() }
unsafe fn LogicalConfirmReceivedLocation(lsn: XLogRecPtr) { unimplemented!() }
unsafe fn CheckLogicalDecodingRequirements() { unimplemented!() }

/// TODO(pg-port): snapbuild functions live in replication/snapbuild.h
unsafe fn SnapBuildExportSnapshot(snapbuild: *mut c_void) -> *const c_char { unimplemented!() }
unsafe fn SnapBuildInitialSnapshot(snapbuild: *mut c_void) -> Snapshot { unimplemented!() }
unsafe fn SnapBuildClearExportedSnapshot() { unimplemented!() }
unsafe fn RestoreTransactionSnapshot(snap: Snapshot, proc_: *mut PGPROC) { unimplemented!() }

/// TODO(pg-port): syncrep functions live in replication/syncrep.h
unsafe fn SyncRepInitConfig() { unimplemented!() }
unsafe fn SyncRepReleaseWaiters() { unimplemented!() }
unsafe fn SyncRepRequested() -> bool { unimplemented!() }
unsafe fn SyncRepGetCandidateStandbys(standbys: *mut *mut SyncRepStandbyData) -> c_int { unimplemented!() }
/// STUB: SyncRepConfig is a global pointer in replication/syncrep.h
const SyncRepConfig: *const c_void = std::ptr::null();
const SYNC_REP_PRIORITY: c_int = 0;

/// TODO(pg-port): incremental backup functions live in backup/basebackup_incremental.h
unsafe fn CreateIncrementalBackupInfo(ctx: MemoryContext) -> *mut IncrementalBackupInfo { unimplemented!() }
unsafe fn AppendIncrementalManifestData(ib: *mut IncrementalBackupInfo, data: *const c_char, len: c_int) { unimplemented!() }
unsafe fn FinalizeIncrementalManifest(ib: *mut IncrementalBackupInfo) { unimplemented!() }
unsafe fn SendBaseBackup(cmd: *mut BaseBackupCmd, manifest: *mut IncrementalBackupInfo) { unimplemented!() }

/// TODO(pg-port): miscadmin functions
unsafe fn CreateAuxProcessResourceOwner() { unimplemented!() }
unsafe fn ReleaseAuxProcessResources(isCommit: bool) { unimplemented!() }
unsafe fn MarkPostmasterChildWalSender() { unimplemented!() }
unsafe fn IsTransactionOrTransactionBlock() -> bool { unimplemented!() }
unsafe fn IsTransactionBlock() -> bool { unimplemented!() }
unsafe fn IsSubTransaction() -> bool { unimplemented!() }
unsafe fn IsAbortedTransactionBlockState() -> bool { unimplemented!() }
unsafe fn StartTransactionCommand() { unimplemented!() }
unsafe fn CommitTransactionCommand() { unimplemented!() }
unsafe fn PreventInTransactionBlock(isTopLevel: bool, stmtType: *const c_char) { unimplemented!() }
unsafe fn get_database_name(dboid: Oid) -> *mut c_char { unimplemented!() }
unsafe fn GetUserId() -> Oid { unimplemented!() }
unsafe fn has_privs_of_role(roleid: Oid, priv_roleid: Oid) -> bool { unimplemented!() }
const ROLE_PG_READ_ALL_STATS: Oid = 0;

/// TODO(pg-port): proc signal functions live in storage/procsignal.h
unsafe fn SendProcSignal(pid: pid_t, signum: c_int, procnumber: c_int) { unimplemented!() }
const PROCSIG_WALSND_INIT_STOPPING: c_int = 0;
const INVALID_PROC_NUMBER: c_int = -1;

/// TODO(pg-port): postmaster signal lives in postmaster/pmsignal.h
unsafe fn SendPostmasterSignal(sig: c_int) { unimplemented!() }
const PMSIGNAL_ADVANCE_STATE_MACHINE: c_int = 0;

/// TODO(pg-port): ProcArrayLock / LW_EXCLUSIVE etc live in storage/proc.h / storage/lwlock.h
const ProcArrayLock: usize = 0;
const LW_EXCLUSIVE: c_int = 0;
const LW_SHARED: c_int = 1;

/// TODO(pg-port): PGPROC globals live in storage/proc.h
static mut MyProc: *mut PGPROC = std::ptr::null_mut();
unsafe fn InvalidTransactionId_val() -> TransactionId { 0 }
unsafe fn TransactionIdIsNormal(xid: TransactionId) -> bool { xid >= 3 }
unsafe fn TransactionIdPrecedes(a: TransactionId, b: TransactionId) -> bool { unimplemented!() }
unsafe fn TransactionIdPrecedesOrEquals(a: TransactionId, b: TransactionId) -> bool { unimplemented!() }
unsafe fn ReadNextFullTransactionId() -> FullTransactionId { unimplemented!() }
unsafe fn XidFromFullTransactionId(fxid: FullTransactionId) -> TransactionId { (fxid.value & 0xFFFFFFFF) as u32 }
unsafe fn EpochFromFullTransactionId(fxid: FullTransactionId) -> u32 { (fxid.value >> 32) as u32 }

/// TODO(pg-port): WaitEventSet lives in storage/waiteventset.h
static mut FeBeWaitSet: *mut c_void = std::ptr::null_mut();
const FeBeWaitSetSocketPos: usize = 0;
unsafe fn WaitEventSetWait(set: *mut c_void, timeout: i64, event: *mut WaitEvent, nevents: c_int, wait_event: u32) -> c_int { unimplemented!() }
unsafe fn ModifyWaitEvent(set: *mut c_void, pos: usize, events: u32, latch: *mut c_void) { unimplemented!() }

/// TODO(pg-port): pgstat functions live in utils/pgstat_internal.h
unsafe fn pgstat_report_wait_start(event: u32) { }
unsafe fn pgstat_report_wait_end() { }
unsafe fn pgstat_report_activity(state: c_int, cmd: *const c_char) { }
unsafe fn pgstat_flush_io(nowait: bool) { }
unsafe fn pgstat_flush_backend(nowait: bool, flags: u32) { }
const STATE_RUNNING: c_int = 0;
const PGSTAT_BACKEND_FLUSH_IO: u32 = 0;

/// TODO(pg-port): pgaio_error_cleanup lives in storage/aio_subsys.h
unsafe fn pgaio_error_cleanup() { }

/// TODO(pg-port): tuple output functions live in tcop/dest.h / access/printtup.h
unsafe fn CreateDestReceiver(dest: c_int) -> *mut DestReceiver { unimplemented!() }
unsafe fn CreateTemplateTupleDesc(natts: c_int) -> TupleDesc { unimplemented!() }
unsafe fn TupleDescInitBuiltinEntry(desc: TupleDesc, attnum: AttrNumber, name: *const c_char, typid: Oid, typmod: i32, attdim: c_int) { unimplemented!() }
unsafe fn begin_tup_output_tupdesc(dest: *mut DestReceiver, tupdesc: TupleDesc, ops: *const c_void) -> *mut TupOutputState { unimplemented!() }
unsafe fn do_tup_output(tstate: *mut TupOutputState, values: *const Datum, nulls: *const bool) { unimplemented!() }
unsafe fn end_tup_output(tstate: *mut TupOutputState) { unimplemented!() }
unsafe fn InitMaterializedSRF(fcinfo: FunctionCallInfo, flags: c_int) { unimplemented!() }
unsafe fn tuplestore_putvalues(store: *mut c_void, desc: TupleDesc, values: *const Datum, nulls: *const bool) { unimplemented!() }

/// Datum construction helpers
unsafe fn CStringGetTextDatum(s: *const c_char) -> Datum { unimplemented!() }
unsafe fn Int64GetDatum(v: i64) -> Datum { unimplemented!() }
unsafe fn Int32GetDatum(v: i32) -> Datum { unimplemented!() }
unsafe fn LSNGetDatum(lsn: XLogRecPtr) -> Datum { unimplemented!() }
unsafe fn TimestampTzGetDatum(t: TimestampTz) -> Datum { unimplemented!() }
unsafe fn IntervalPGetDatum(iv: *const Interval) -> Datum { unimplemented!() }

/// TODO(pg-port): signal handling stubs live in tcop/postgres.h / postmaster/interrupt.h
unsafe fn SignalHandlerForConfigReload(sig: c_int) { unimplemented!() }
unsafe fn StatementCancelHandler(sig: c_int) { unimplemented!() }
unsafe fn die(sig: c_int) { unimplemented!() }
unsafe fn procsignal_sigusr1_handler(sig: c_int) { unimplemented!() }
unsafe fn pqsignal(signum: c_int, handler: unsafe fn(c_int)) { unimplemented!() }
unsafe fn InitializeTimeouts() { unimplemented!() }
static mut ConfigReloadPending: bool = false;
unsafe fn ProcessConfigFile(context: c_int) { unimplemented!() }
const PGC_SIGHUP: c_int = 0;

/// TODO(pg-port): misc string/format helpers live in tcop/tcopprot.h / utils/ps_status.h
unsafe fn set_ps_display(activity: *const c_char) { }
unsafe fn update_process_title() -> bool { false }
unsafe fn defGetString(defel: *const DefElem) -> *const c_char { unimplemented!() }
unsafe fn defGetBoolean(defel: *const DefElem) -> bool { unimplemented!() }
unsafe fn NameStr(name: [c_char; 64]) -> *mut c_char { unimplemented!() }
static mut MyDatabaseId: Oid = 0;
const InvalidOid: Oid = 0;
static mut MyProcPid: pid_t = 0;

/// TODO(pg-port): walreceiver helper lives in replication/walreceiverfuncs.h
unsafe fn GetWalRcvFlushRecPtr(recptr: *mut XLogRecPtr, tli: *mut TimeLineID) -> XLogRecPtr { unimplemented!() }
unsafe fn IsSyncingReplicationSlots() -> bool { unimplemented!() }

/// TODO(pg-port): replication command helpers
unsafe fn EndReplicationCommand(cmdtag: *const c_char) { unimplemented!() }
unsafe fn EndCommand(qc: *mut QueryCompletion, dest: c_int, force_undecorated: bool) { unimplemented!() }
unsafe fn SetQueryCompletion(qc: *mut QueryCompletion, cmdtag: c_int, nprocessed: u64) { unimplemented!() }
unsafe fn GetPGVariable(name: *const c_char, dest: *mut DestReceiver) { unimplemented!() }
unsafe fn debug_query_string_set(s: *const c_char) { }

/// TODO(pg-port): macros from access/xlog_internal.h
unsafe fn XLogRecPtrIsInvalid(ptr: XLogRecPtr) -> bool { ptr == InvalidXLogRecPtr }

/// Wait event constants - TODO(pg-port): real values from utils/wait_event_types.h
const WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ: u32 = 0;
const WAIT_EVENT_WAL_SENDER_WAIT_FOR_WAL: u32 = 1;
const WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION: u32 = 2;
const WAIT_EVENT_WAL_SENDER_MAIN: u32 = 3;
const WAIT_EVENT_WAL_SENDER_WRITE_DATA: u32 = 4;

/// WaitEvent flags
const WL_SOCKET_READABLE: u32 = 0x01;
const WL_SOCKET_WRITEABLE: u32 = 0x02;
const WL_POSTMASTER_DEATH: u32 = 0x04;

/// DestRemote / DestNone / DestRemoteSimple constants
const DestRemote: c_int = 1;
const DestNone: c_int = 0;
const DestRemoteSimple: c_int = 2;
static mut whereToSendOutput: c_int = DestRemote;

/// TODO(pg-port): CMD_SELECT lives in nodes/parsenodes.h
const CMD_SELECT: c_int = 4;

/// PqMsg_* constants - TODO(pg-port): real values from libpq/pqmsgtype.h
const PqMsg_CopyBothResponse: u8 = b'W';
const PqMsg_CopyInResponse: u8 = b'G';
const PqMsg_DataRow: u8 = b'D';
const PqMsg_CopyData: u8 = b'd';
const PqMsg_CopyDone: u8 = b'c';
const PqMsg_Terminate: u8 = b'X';

/// Message size limits
const PQ_LARGE_MESSAGE_LIMIT: c_int = 0x3FFFFFFF;
const PQ_SMALL_MESSAGE_LIMIT: c_int = 4096;

/// OID constants
const TEXTOID: Oid = 25;
const INT8OID: Oid = 20;

/// TTSOpsVirtual opaque pointer
const TTSOpsVirtual: *const c_void = std::ptr::null();

/// PG_BINARY
const PG_BINARY: c_int = 0;

/// MAXFNAMELEN / MAXPGPATH
const MAXFNAMELEN: usize = 64;
const MAXPGPATH: usize = 1024;

/// RS_PERSISTENT / RS_TEMPORARY / RS_EPHEMERAL
const RS_PERSISTENT: c_int = 0;
const RS_TEMPORARY: c_int = 1;
const RS_EPHEMERAL: c_int = 2;

/// XACT_REPEATABLE_READ etc
const XACT_REPEATABLE_READ: c_int = 2;
static mut XactIsoLevel: c_int = 0;
static mut XactReadOnly: bool = false;
static mut FirstSnapshotSet: bool = false;

/// application_name GUC
static mut application_name: *const c_char = std::ptr::null();

/// message_level_is_interesting stub
unsafe fn message_level_is_interesting(level: c_int) -> bool { false }
const DEBUG2: c_int = 10;
const DEBUG1: c_int = 11;
const LOG: c_int = 17;
const ERROR: c_int = 21;
const FATAL: c_int = 22;
const COMMERROR: c_int = 25;
const WARNING: c_int = 19;

/// TODO(pg-port): CurrentMemoryContext / TopMemoryContext / CacheMemoryContext / AuxProcessResourceOwner / CurrentResourceOwner
static mut CurrentMemoryContext: MemoryContext = std::ptr::null_mut();
static mut TopMemoryContext: MemoryContext = std::ptr::null_mut();
static mut CacheMemoryContext: MemoryContext = std::ptr::null_mut();
static mut AuxProcessResourceOwner: *mut c_void = std::ptr::null_mut();
static mut CurrentResourceOwner: *mut c_void = std::ptr::null_mut();

/// TODO(pg-port): ShmemInitStruct / MemSet / dlist_init / add_size / mul_size / offsetof_fn live in storage/shmem.h / nodes/ilist.h
unsafe fn ShmemInitStruct(name: *const c_char, size: usize, found: *mut bool) -> *mut c_void { unimplemented!() }
unsafe fn MemSet(ptr: *mut c_void, val: c_int, n: usize) { unimplemented!() }
unsafe fn dlist_init(head: *mut c_void) { unimplemented!() }
unsafe fn add_size(a: usize, b: usize) -> usize { a + b }
unsafe fn mul_size(a: usize, b: usize) -> usize { a * b }

/// TODO(pg-port): on_shmem_exit lives in storage/ipc.h
unsafe fn on_shmem_exit(func: unsafe fn(c_int, Datum), arg: Datum) { unimplemented!() }

// ---------------------------------------------------------------------------
// Callback type for WalSndLoop
// ---------------------------------------------------------------------------

pub type WalSndSendDataCallback = unsafe fn();

// ---------------------------------------------------------------------------
// Part 1 ends here; function implementations follow in subsequent parts
// ---------------------------------------------------------------------------

// ===========================================================================
// Part 2: InitWalSender, WalSndErrorCleanup, WalSndShutdown,
//         IdentifySystem, ReadReplicationSlot, SendTimeLineHistory,
//         UploadManifest, HandleUploadManifestPacket
// ===========================================================================

/// Initialize walsender process before entering the main command loop.
pub unsafe fn InitWalSender() {
    am_cascading_walsender = RecoveryInProgress();

    // Create a per-walsender data structure in shared memory
    InitWalSenderSlot();

    // need resource owner for e.g. basebackups
    CreateAuxProcessResourceOwner();

    // Let postmaster know that we're a WAL sender.  Once we've declared us as
    // a WAL sender process, postmaster will let us outlive the bgwriter and
    // kill us last in the shutdown sequence, so we get a chance to stream all
    // remaining WAL at shutdown, including the shutdown checkpoint.  Note that
    // there's no going back, and we mustn't write any WAL records after this.
    MarkPostmasterChildWalSender();
    SendPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE);

    // If the client didn't specify a database to connect to, show in PGPROC
    // that our advertised xmin should affect vacuum horizons in all databases.
    if MyDatabaseId == InvalidOid {
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        // MyProc->statusFlags |= PROC_AFFECTS_ALL_HORIZONS (stub)
        LWLockRelease(ProcArrayLock);
    }

    // Initialize empty timestamp buffer for lag tracking.
    lag_tracker = MemoryContextAllocZero(TopMemoryContext, std::mem::size_of::<LagTracker>())
        as *mut LagTracker;
}

/// Clean up after an error.
///
/// WAL sender processes don't use transactions like regular backends do.
/// This function does any cleanup required after an error in a WAL sender
/// process, similar to what transaction abort does in a regular backend.
pub unsafe fn WalSndErrorCleanup() {
    LWLockReleaseAll();
    ConditionVariableCancelSleep();
    pgstat_report_wait_end();
    pgaio_error_cleanup();

    if !xlogreader.is_null() && (*xlogreader).seg.ws_file >= 0 {
        wal_segment_close(xlogreader);
    }

    if !MyReplicationSlot.is_null() {
        ReplicationSlotRelease();
    }

    ReplicationSlotCleanup(false);

    replication_active = false;

    // If there is a transaction in progress, it will clean up our
    // ResourceOwner, but if a replication command set up a resource owner
    // without a transaction, we've got to clean that up now.
    if !IsTransactionOrTransactionBlock() {
        ReleaseAuxProcessResources(false);
    }

    if got_STOPPING || got_SIGUSR2 {
        proc_exit(0);
    }

    // Revert back to startup state
    WalSndSetState(WALSNDSTATE_STARTUP);
}

/// Handle a client's connection abort in an orderly manner.
unsafe fn WalSndShutdown() -> ! {
    // Reset whereToSendOutput to prevent ereport from attempting to send any
    // more messages to the standby.
    if whereToSendOutput == DestRemote {
        whereToSendOutput = DestNone;
    }
    proc_exit(0);
}

/// Handle the IDENTIFY_SYSTEM command.
unsafe fn IdentifySystem() {
    let sysid: [c_char; 32] = [0; 32];
    let xloc: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let logptr: XLogRecPtr;
    let mut dbname: *mut c_char = std::ptr::null_mut();
    let dest: *mut DestReceiver;
    let tstate: *mut TupOutputState;
    let tupdesc: TupleDesc;
    let mut values: [Datum; 4] = [0; 4];
    let mut nulls: [bool; 4] = [false; 4];
    let mut currTLI: TimeLineID = 0;

    // Reply with a result set with one row, four columns.
    // First col: system ID, second: timeline ID, third: xlog location,
    // fourth: database name if we are connected to one.

    // snprintf(sysid, ..., UINT64_FORMAT, GetSystemIdentifier())
    // (stub: we skip the actual snprintf)
    let sysid_val = GetSystemIdentifier();

    am_cascading_walsender = RecoveryInProgress();
    if am_cascading_walsender {
        logptr = GetStandbyFlushRecPtr(&mut currTLI);
    } else {
        logptr = GetFlushRecPtr(&mut currTLI);
    }

    // snprintf(xloc, ..., "%X/%X", LSN_FORMAT_ARGS(logptr))
    // (stub: format elided)

    if MyDatabaseId != InvalidOid {
        let cur = CurrentMemoryContext;
        // syscache access needs a transaction env.
        StartTransactionCommand();
        dbname = get_database_name(MyDatabaseId);
        // copy dbname out of TX context
        dbname = MemoryContextStrdup(cur, dbname);
        CommitTransactionCommand();
    }

    dest = CreateDestReceiver(DestRemoteSimple);

    // need a tuple descriptor representing four columns
    tupdesc = CreateTemplateTupleDesc(4);
    // (AttrNumber casts elided for brevity; stubs used)
    TupleDescInitBuiltinEntry(tupdesc, 1, b"systemid\0".as_ptr() as *const c_char, TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2, b"timeline\0".as_ptr() as *const c_char, INT8OID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 3, b"xlogpos\0".as_ptr() as *const c_char, TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 4, b"dbname\0".as_ptr() as *const c_char, TEXTOID, -1, 0);

    // prepare for projection of tuples
    tstate = begin_tup_output_tupdesc(dest, tupdesc, TTSOpsVirtual);

    // column 1: system identifier
    values[0] = CStringGetTextDatum(sysid.as_ptr());
    // column 2: timeline
    values[1] = Int64GetDatum(currTLI as i64);
    // column 3: wal location
    values[2] = CStringGetTextDatum(xloc.as_ptr());
    // column 4: database name, or NULL if none
    if !dbname.is_null() {
        values[3] = CStringGetTextDatum(dbname);
    } else {
        nulls[3] = true;
    }

    do_tup_output(tstate, values.as_ptr(), nulls.as_ptr());
    end_tup_output(tstate);
}

/// Handle READ_REPLICATION_SLOT command.
unsafe fn ReadReplicationSlot(cmd: *mut ReadReplicationSlotCmd) {
    const READ_REPLICATION_SLOT_COLS: usize = 3;
    let dest: *mut DestReceiver;
    let tstate: *mut TupOutputState;
    let tupdesc: TupleDesc;
    let values: [Datum; READ_REPLICATION_SLOT_COLS] = [0; READ_REPLICATION_SLOT_COLS];
    let nulls: [bool; READ_REPLICATION_SLOT_COLS] = [true; READ_REPLICATION_SLOT_COLS];

    tupdesc = CreateTemplateTupleDesc(READ_REPLICATION_SLOT_COLS as c_int);
    TupleDescInitBuiltinEntry(tupdesc, 1, b"slot_type\0".as_ptr() as *const c_char, TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2, b"restart_lsn\0".as_ptr() as *const c_char, TEXTOID, -1, 0);
    // TimeLineID is unsigned, so int4 is not wide enough.
    TupleDescInitBuiltinEntry(tupdesc, 3, b"restart_tli\0".as_ptr() as *const c_char, INT8OID, -1, 0);

    LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    // (slot lookup and data fill elided; all stubs)
    // slot = SearchNamedReplicationSlot(cmd->slotname, false); ...
    LWLockRelease(ReplicationSlotControlLock);

    dest = CreateDestReceiver(DestRemoteSimple);
    tstate = begin_tup_output_tupdesc(dest, tupdesc, TTSOpsVirtual);
    do_tup_output(tstate, values.as_ptr(), nulls.as_ptr());
    end_tup_output(tstate);
}

/// Handle TIMELINE_HISTORY command.
unsafe fn SendTimeLineHistory(cmd: *mut TimeLineHistoryCmd) {
    let dest: *mut DestReceiver;
    let tupdesc: TupleDesc;
    let mut buf: StringInfoData = std::mem::zeroed();
    let histfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let fd: c_int;
    let histfilelen: i64;
    let mut bytesleft: i64;

    dest = CreateDestReceiver(DestRemoteSimple);

    // Reply with a result set with one row, and two columns.  The first col is
    // the name of the history file, 2nd is the contents.
    tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitBuiltinEntry(tupdesc, 1, b"filename\0".as_ptr() as *const c_char, TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2, b"content\0".as_ptr() as *const c_char, TEXTOID, -1, 0);

    // TLHistoryFileName / TLHistoryFilePath (stubs)
    // TLHistoryFileName_fn(histfname.as_mut_ptr(), cmd->timeline);
    // TLHistoryFilePath_fn(path.as_mut_ptr(), cmd->timeline);

    // Send a RowDescription message
    // dest->rStartup(dest, CMD_SELECT, tupdesc);

    // Send a DataRow message
    pq_beginmessage(&mut buf, PqMsg_DataRow);
    pq_sendint16(&mut buf, 2); // # of columns
    let len: usize = 0; // strlen(histfname) stub
    pq_sendint32(&mut buf, len as i32); // col1 len
    pq_sendbytes(&mut buf, histfname.as_ptr(), len);

    fd = OpenTransientFile(path.as_ptr(), libc_O_RDONLY());
    // error handling elided (stubs)

    // Determine file length (stub: 0)
    histfilelen = 0;
    pq_sendint32(&mut buf, histfilelen as i32); // col2 len

    bytesleft = histfilelen;
    while bytesleft > 0 {
        let rbuf: [u8; 8192] = [0; 8192];
        pgstat_report_wait_start(WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ);
        // nread = read(fd, rbuf, sizeof(rbuf)); (stub)
        pgstat_report_wait_end();
        // error handling elided (stub)
        pq_sendbytes(&mut buf, rbuf.as_ptr() as *const c_char, 0);
        bytesleft -= 0; // stub
    }

    CloseTransientFile(fd);
    pq_endmessage(&mut buf);
}

unsafe fn libc_O_RDONLY() -> c_int { 0 }

/// Handle UPLOAD_MANIFEST command.
unsafe fn UploadManifest() {
    let mcxt: MemoryContext;
    let ib: *mut IncrementalBackupInfo;
    let mut offset: i64 = 0;
    let mut buf: StringInfoData = std::mem::zeroed();

    // parsing the manifest will use the cryptohash stuff, which requires a
    // resource owner
    CurrentResourceOwner = AuxProcessResourceOwner;

    // Prepare to read manifest data into a temporary context.
    mcxt = AllocSetContextCreate(
        CurrentMemoryContext,
        b"incremental backup information\0".as_ptr() as *const c_char,
        0, // ALLOCSET_DEFAULT_SIZES stub
    );
    ib = CreateIncrementalBackupInfo(mcxt);

    // Send a CopyInResponse message
    pq_beginmessage(&mut buf, PqMsg_CopyInResponse);
    pq_sendbyte(&mut buf, 0);
    pq_sendint16(&mut buf, 0);
    pq_endmessage_reuse(&mut buf);
    pq_flush();

    // Receive packets from client until done.
    while HandleUploadManifestPacket(&mut buf, &mut offset, ib) {}

    // Finish up manifest processing.
    FinalizeIncrementalManifest(ib);

    // Discard any old manifest information and arrange to preserve the new
    // information we just got.
    if !uploaded_manifest_mcxt.is_null() {
        MemoryContextDelete(uploaded_manifest_mcxt);
    }
    MemoryContextSetParent(mcxt, CacheMemoryContext);
    uploaded_manifest = ib;
    uploaded_manifest_mcxt = mcxt;

    // clean up the resource owner we created
    ReleaseAuxProcessResources(true);
}

/// Process one packet received during the handling of an UPLOAD_MANIFEST
/// operation.
///
/// The return value is true if the caller should continue processing
/// additional packets and false if the UPLOAD_MANIFEST operation is complete.
unsafe fn HandleUploadManifestPacket(
    buf: *mut StringInfoData,
    offset: *mut i64,
    ib: *mut IncrementalBackupInfo,
) -> bool {
    let mtype: c_int;
    let maxmsglen: c_int;

    // HOLD_CANCEL_INTERRUPTS (stub)

    pq_startmsgread();
    mtype = pq_getbyte();
    if mtype < 0 {
        // unexpected EOF
        proc_exit(0);
    }

    let mtype_u8 = mtype as u8;
    maxmsglen = match mtype_u8 {
        b'd' => PQ_LARGE_MESSAGE_LIMIT, // CopyData
        b'c' | b'f' | b'H' | b'S' => PQ_SMALL_MESSAGE_LIMIT,
        _ => {
            // ereport ERRCODE_PROTOCOL_VIOLATION (stub)
            proc_exit(1);
        }
    };

    // Now collect the message body
    if pq_getmessage(buf, maxmsglen) != 0 {
        proc_exit(0);
    }
    // RESUME_CANCEL_INTERRUPTS (stub)

    match mtype_u8 {
        b'd' => {
            // CopyData
            AppendIncrementalManifestData(ib, (*buf).data, (*buf).len);
            true
        }
        b'c' => {
            // CopyDone
            false
        }
        b'H' | b'S' => {
            // Sync / Flush: ignore while in CopyOut mode
            true
        }
        b'f' => {
            // CopyFail
            // ereport ERRCODE_QUERY_CANCELED (stub)
            proc_exit(1);
        }
        _ => {
            // Not reached
            false
        }
    }
}

// ===========================================================================
// Part 3: StartReplication, logical_read_xlog_page,
//         parseCreateReplSlotOptions, CreateReplicationSlot,
//         DropReplicationSlot, AlterReplicationSlot,
//         StartLogicalReplication, WalSndPrepareWrite,
//         WalSndWriteData, ProcessPendingWrites, WalSndUpdateProgress
// ===========================================================================

/// Handle START_REPLICATION command.
///
/// At the moment, this never returns, but an ereport(ERROR) will take us back
/// to the main loop.
unsafe fn StartReplication(cmd: *mut StartReplicationCmd) {
    let mut buf: StringInfoData = std::mem::zeroed();
    let FlushPtr: XLogRecPtr;
    let mut FlushTLI: TimeLineID = 0;

    // create xlogreader for physical replication
    xlogreader = XLogReaderAllocate(wal_segment_size, std::ptr::null(), 0, std::ptr::null_mut());

    if xlogreader.is_null() {
        // ereport(ERROR, errmsg("out of memory"), ...)
        unimplemented!("ereport OOM");
    }

    // If a slot name was given, acquire the slot and verify it's physical.
    // (cmd->slotname / SlotIsLogical / ReplicationSlotAcquire stubs)

    // Select the timeline.
    am_cascading_walsender = RecoveryInProgress();
    if am_cascading_walsender {
        FlushPtr = GetStandbyFlushRecPtr(&mut FlushTLI);
    } else {
        FlushPtr = GetFlushRecPtr(&mut FlushTLI);
    }

    // cmd->timeline / sendTimeLine / sendTimeLineIsHistoric logic (stubs)
    // For the stub we just set defaults:
    sendTimeLine = FlushTLI;
    sendTimeLineValidUpto = InvalidXLogRecPtr;
    sendTimeLineIsHistoric = false;

    streamingDoneSending = false;
    streamingDoneReceiving = false;

    // If there is nothing to stream, don't even enter COPY mode
    if !sendTimeLineIsHistoric {
        // When we first start replication the standby will be behind the
        // primary.
        WalSndSetState(WALSNDSTATE_CATCHUP);

        // Send a CopyBothResponse message, and start streaming
        pq_beginmessage(&mut buf, PqMsg_CopyBothResponse);
        pq_sendbyte(&mut buf, 0);
        pq_sendint16(&mut buf, 0);
        pq_endmessage(&mut buf);
        pq_flush();

        // Start streaming from the requested point (cmd->startpoint stub)
        // sentPtr = cmd->startpoint;

        // Initialize shared memory status
        if !MyWalSnd_ptr().is_null() {
            SpinLockAcquire(&mut (*MyWalSnd_ptr()).mutex);
            (*MyWalSnd_ptr()).sentPtr = sentPtr;
            SpinLockRelease(&mut (*MyWalSnd_ptr()).mutex);
        }

        SyncRepInitConfig();

        // Main loop of walsender
        replication_active = true;
        WalSndLoop(XLogSendPhysical);
        replication_active = false;

        if got_STOPPING {
            proc_exit(0);
        }
        WalSndSetState(WALSNDSTATE_STARTUP);
    }

    // (slot release / historic timeline result set elided as stubs)

    // Send CommandComplete message
    EndReplicationCommand(b"START_STREAMING\0".as_ptr() as *const c_char);
}

/// Helper to dereference MyWalSnd (declared extern in walsender_private).
#[inline(always)]
unsafe fn MyWalSnd_ptr() -> *mut WalSnd {
    extern "C" { static mut MyWalSnd: *mut WalSnd; }
    MyWalSnd
}

/// Helper to dereference WalSndCtl (declared extern in walsender_private).
#[inline(always)]
unsafe fn WalSndCtl_ptr() -> *mut WalSndCtlData {
    extern "C" { static mut WalSndCtl: *mut WalSndCtlData; }
    WalSndCtl
}

/// XLogReaderRoutine->page_read callback for logical decoding contexts, as a
/// walsender process.
///
/// Inside the walsender we can do better than read_local_xlog_page,
/// which has to do a plain sleep/busy loop, because the walsender's latch gets
/// set every time WAL is flushed.
unsafe fn logical_read_xlog_page(
    state: *mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: c_int,
    targetRecPtr: XLogRecPtr,
    cur_page: *mut c_char,
) -> c_int {
    let flushptr: XLogRecPtr;
    let count: c_int;
    let mut errinfo: WALReadError = std::mem::zeroed();
    let mut segno: XLogSegNo = 0;
    let mut currTLI: TimeLineID = 0;

    // Make sure we have enough WAL available before retrieving the current timeline.
    flushptr = WalSndWaitForWal(targetPagePtr + reqLen as u64);

    // Fail if not enough (implies we are going to shut down)
    if flushptr < targetPagePtr + reqLen as u64 {
        return -1;
    }

    // Since logical decoding is also permitted on a standby server, we need
    // to check if the server is in recovery.
    am_cascading_walsender = RecoveryInProgress();

    if am_cascading_walsender {
        GetXLogReplayRecPtr(&mut currTLI);
    } else {
        currTLI = GetWALInsertionTimeLine();
    }

    XLogReadDetermineTimeline(state, targetPagePtr, reqLen, currTLI);
    sendTimeLineIsHistoric = (*state).currTLI != currTLI;
    sendTimeLine = (*state).currTLI;
    sendTimeLineValidUpto = (*state).currTLIValidUntil;
    sendTimeLineNextTLI = (*state).nextTLI;

    if targetPagePtr + 8192 <= flushptr {
        count = 8192 as c_int; // XLOG_BLCKSZ: more than one block available
    } else {
        count = (flushptr - targetPagePtr) as c_int; // part of the page available
    }

    // now actually read the data, we know it's there
    if !WALRead(state, cur_page, targetPagePtr, count as usize, currTLI, &mut errinfo) {
        WALReadRaiseError(&mut errinfo);
    }

    // After reading into the buffer, check that what we read was valid.
    XLByteToSeg_fn(targetPagePtr, &mut segno, (*state).segcxt.ws_segsize);
    CheckXLogRemoved(segno, (*state).seg.ws_tli);

    count
}

/// Process extra options given to CREATE_REPLICATION_SLOT.
// Field-access views over the opaque c_void cmd/DefElem stubs used elsewhere in
// this file. These mirror the real layouts in nodes/replnodes.h and
// nodes/parsenodes.h so parseCreateReplSlotOptions can read the fields it needs.
// TODO(pg-port): drop these once CreateReplicationSlotCmd/DefElem aliases are the
// real types from nodes/replnodes.rs and nodes/parsenodes.rs.
#[repr(C)]
struct CreateReplicationSlotCmdFields {
    r#type: crate::nodes::nodes::NodeTag,
    slotname: *mut c_char,
    kind: ReplicationKind,
    plugin: *mut c_char,
    temporary: bool,
    options: *mut crate::nodes::pg_list::List,
}

#[repr(C)]
struct DefElemFields {
    r#type: crate::nodes::nodes::NodeTag,
    defnamespace: *mut c_char,
    defname: *mut c_char,
    arg: *mut crate::nodes::nodes::Node,
    defaction: c_int,
    location: c_int,
}

unsafe fn parseCreateReplSlotOptions(
    cmd: *mut CreateReplicationSlotCmd,
    reserve_wal: *mut bool,
    snapshot_action: *mut CRSSnapshotAction,
    two_phase: *mut bool,
    failover: *mut bool,
) {
    let cmd = cmd as *mut CreateReplicationSlotCmdFields;
    let mut snapshot_action_given: bool = false;
    let mut reserve_wal_given: bool = false;
    let mut two_phase_given: bool = false;
    let mut failover_given: bool = false;

    /* Parse options */
    foreach!(lc, (*cmd).options, {
        let defel = crate::nodes::pg_list::lfirst(crate::current_cell!(lc))
            as *mut DefElemFields;
        let defel_void = defel as *const DefElem;

        if libc::strcmp((*defel).defname, b"snapshot\0".as_ptr() as *const c_char) == 0 {
            if snapshot_action_given || (*cmd).kind != REPLICATION_KIND_LOGICAL {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
            }

            let action = defGetString(defel_void);
            snapshot_action_given = true;

            if libc::strcmp(action, b"export\0".as_ptr() as *const c_char) == 0 {
                *snapshot_action = CRS_EXPORT_SNAPSHOT;
            } else if libc::strcmp(action, b"nothing\0".as_ptr() as *const c_char) == 0 {
                *snapshot_action = CRS_NOEXPORT_SNAPSHOT;
            } else if libc::strcmp(action, b"use\0".as_ptr() as *const c_char) == 0 {
                *snapshot_action = CRS_USE_SNAPSHOT;
            } else {
                ereport!(ERROR, errmsg!(
                    "unrecognized value for {} option \"{}\": \"{}\"",
                    "CREATE_REPLICATION_SLOT",
                    std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(action).to_string_lossy()));
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            }
        } else if libc::strcmp((*defel).defname, b"reserve_wal\0".as_ptr() as *const c_char) == 0 {
            if reserve_wal_given || (*cmd).kind != REPLICATION_KIND_PHYSICAL {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
            }

            reserve_wal_given = true;
            *reserve_wal = defGetBoolean(defel_void);
        } else if libc::strcmp((*defel).defname, b"two_phase\0".as_ptr() as *const c_char) == 0 {
            if two_phase_given || (*cmd).kind != REPLICATION_KIND_LOGICAL {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
            }
            two_phase_given = true;
            *two_phase = defGetBoolean(defel_void);
        } else if libc::strcmp((*defel).defname, b"failover\0".as_ptr() as *const c_char) == 0 {
            if failover_given || (*cmd).kind != REPLICATION_KIND_LOGICAL {
                ereport!(ERROR, errmsg!("conflicting or redundant options"));
                /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
            }
            failover_given = true;
            *failover = defGetBoolean(defel_void);
        } else {
            elog!(ERROR, "unrecognized option: {}",
                std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy());
        }
    });
}

/// Create a new replication slot.
unsafe fn CreateReplicationSlot(cmd: *mut CreateReplicationSlotCmd) {
    let xloc: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut reserve_wal: bool = false;
    let mut two_phase: bool = false;
    let mut failover: bool = false;
    let mut snapshot_action: CRSSnapshotAction = CRS_EXPORT_SNAPSHOT;
    let snapshot_name: *const c_char = std::ptr::null();
    let dest: *mut DestReceiver;
    let tstate: *mut TupOutputState;
    let tupdesc: TupleDesc;
    let values: [Datum; 4] = [0; 4];
    let nulls: [bool; 4] = [false; 4];

    parseCreateReplSlotOptions(cmd, &mut reserve_wal, &mut snapshot_action,
                               &mut two_phase, &mut failover);

    // cmd->kind == REPLICATION_KIND_PHYSICAL branch (stub)
    // cmd->kind == REPLICATION_KIND_LOGICAL branch uses logical decoding (stubs)

    // Prepare result tuple descriptor
    tupdesc = CreateTemplateTupleDesc(4);
    TupleDescInitBuiltinEntry(tupdesc, 1, b"slot_name\0".as_ptr() as *const c_char, TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2, b"consistent_point\0".as_ptr() as *const c_char, TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 3, b"snapshot_name\0".as_ptr() as *const c_char, TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 4, b"output_plugin\0".as_ptr() as *const c_char, TEXTOID, -1, 0);

    dest = CreateDestReceiver(DestRemoteSimple);
    tstate = begin_tup_output_tupdesc(dest, tupdesc, TTSOpsVirtual);
    do_tup_output(tstate, values.as_ptr(), nulls.as_ptr());
    end_tup_output(tstate);

    ReplicationSlotRelease();
}

/// Get rid of a replication slot that is no longer wanted.
unsafe fn DropReplicationSlot(cmd: *mut DropReplicationSlotCmd) {
    // ReplicationSlotDrop(cmd->slotname, !cmd->wait) (stub)
    ReplicationSlotDrop(std::ptr::null(), true);
}

/// Change the definition of a replication slot.
unsafe fn AlterReplicationSlot(cmd: *mut AlterReplicationSlotCmd) {
    // Parse options and call ReplicationSlotAlter (stub)
    ReplicationSlotAlter(std::ptr::null(), std::ptr::null(), std::ptr::null());
}

/// Load previously initiated logical slot and prepare for sending data
/// (via WalSndLoop).
unsafe fn StartLogicalReplication(cmd: *mut StartReplicationCmd) {
    let mut buf: StringInfoData = std::mem::zeroed();
    let mut qc: QueryCompletion = std::mem::zeroed();

    // make sure that our requirements are still fulfilled
    CheckLogicalDecodingRequirements();

    ReplicationSlotAcquire(std::ptr::null(), true, true);

    // Force a disconnect, so that the decoding code doesn't need to care
    // about an eventual switch from running in recovery, to running in a
    // normal environment.
    if am_cascading_walsender && !RecoveryInProgress() {
        // ereport(LOG, errmsg("terminating walsender process after promotion"))
        got_STOPPING = true;
    }

    // Create our decoding context (stubs)
    logical_decoding_ctx = CreateDecodingContext(
        InvalidXLogRecPtr,
        std::ptr::null_mut(),
        false,
        0, // XL_ROUTINE stub
        0, // WalSndPrepareWrite stub
        0, // WalSndWriteData stub
        0, // WalSndUpdateProgress stub
    );
    xlogreader = (*logical_decoding_ctx).reader;

    WalSndSetState(WALSNDSTATE_CATCHUP);

    // Send a CopyBothResponse message, and start streaming
    pq_beginmessage(&mut buf, PqMsg_CopyBothResponse);
    pq_sendbyte(&mut buf, 0);
    pq_sendint16(&mut buf, 0);
    pq_endmessage(&mut buf);
    pq_flush();

    // Start reading WAL from the oldest required WAL (stub: restart_lsn)
    XLogBeginRead((*logical_decoding_ctx).reader, InvalidXLogRecPtr);

    // sentPtr = MyReplicationSlot->data.confirmed_flush (stub)
    // Shared memory update (stub)
    if !MyWalSnd_ptr().is_null() {
        SpinLockAcquire(&mut (*MyWalSnd_ptr()).mutex);
        (*MyWalSnd_ptr()).sentPtr = InvalidXLogRecPtr; // stub: restart_lsn
        SpinLockRelease(&mut (*MyWalSnd_ptr()).mutex);
    }

    replication_active = true;
    SyncRepInitConfig();

    // Main loop of walsender
    WalSndLoop(XLogSendLogical);

    FreeDecodingContext(logical_decoding_ctx);
    ReplicationSlotRelease();

    replication_active = false;
    if got_STOPPING {
        proc_exit(0);
    }
    WalSndSetState(WALSNDSTATE_STARTUP);

    // Get out of COPY mode (CommandComplete).
    SetQueryCompletion(&mut qc, 0, 0); // CMDTAG_COPY = 0 stub
    EndCommand(&mut qc, DestRemote, false);
}

/// LogicalDecodingContext 'prepare_write' callback.
///
/// Prepare a write into a StringInfo.
unsafe fn WalSndPrepareWrite(
    ctx: *mut LogicalDecodingContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
    last_write: bool,
) {
    // can't have sync rep confused by sending the same LSN several times
    let lsn = if !last_write { InvalidXLogRecPtr } else { lsn };

    resetStringInfo((*ctx).out);
    pq_sendbyte((*ctx).out, b'w');
    pq_sendint64((*ctx).out, lsn as i64); // dataStart
    pq_sendint64((*ctx).out, lsn as i64); // walEnd
    // Fill out the sendtime later, just as it's done in XLogSendPhysical,
    // but reserve space here.
    pq_sendint64((*ctx).out, 0); // sendtime
}

/// LogicalDecodingContext 'write' callback.
///
/// Actually write out data previously prepared by WalSndPrepareWrite out to
/// the network.
unsafe fn WalSndWriteData(
    ctx: *mut LogicalDecodingContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
    last_write: bool,
) {
    let now: TimestampTz;

    // Fill the send timestamp last, so that it is taken as late as possible.
    resetStringInfo(&mut tmpbuf);
    now = GetCurrentTimestamp();
    pq_sendint64(&mut tmpbuf, now);
    // memcpy sendtime into ctx->out (stub)

    // output previously gathered data in a CopyData packet
    pq_putmessage_noblock(b'd' as c_char, (*(*ctx).out).data, (*(*ctx).out).len as usize);

    // CHECK_FOR_INTERRUPTS
    crate::miscadmin::CHECK_FOR_INTERRUPTS();

    // Try to flush pending output to the client
    if pq_flush_if_writable() != 0 {
        WalSndShutdown();
    }

    // Try taking fast path unless we get too close to walsender timeout.
    if now < TimestampTzPlusMilliseconds(last_reply_timestamp, wal_sender_timeout as i64 / 2)
        && !pq_is_send_pending()
    {
        return;
    }

    // If we have pending write here, go to slow path
    ProcessPendingWrites();
}

/// Wait until there is no pending write.  Also process replies from the other
/// side and check timeouts during that.
unsafe fn ProcessPendingWrites() {
    loop {
        // Check for input from the client
        ProcessRepliesIfAny();

        // die if timeout was reached
        WalSndCheckTimeOut();

        // Send keepalive if the time has come
        WalSndKeepaliveIfNecessary();

        if !pq_is_send_pending() {
            break;
        }

        let sleeptime = WalSndComputeSleeptime(GetCurrentTimestamp());

        // Sleep until something happens or we time out
        WalSndWait(
            WL_SOCKET_WRITEABLE | WL_SOCKET_READABLE,
            sleeptime,
            WAIT_EVENT_WAL_SENDER_WRITE_DATA,
        );

        // Clear any already-pending wakeups
        ResetLatch(MyLatch);

        // CHECK_FOR_INTERRUPTS
        crate::miscadmin::CHECK_FOR_INTERRUPTS();

        // Process any requests or signals received recently
        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
            SyncRepInitConfig();
        }

        // Try to flush pending output to the client
        if pq_flush_if_writable() != 0 {
            WalSndShutdown();
        }
    }

    // reactivate latch so WalSndLoop knows to continue
    SetLatch(MyLatch);
}

/// LogicalDecodingContext 'update_progress' callback.
///
/// Write the current position to the lag tracker.  When skipping empty
/// transactions, send a keepalive message if necessary.
unsafe fn WalSndUpdateProgress(
    ctx: *mut LogicalDecodingContext,
    lsn: XLogRecPtr,
    xid: TransactionId,
    skipped_xact: bool,
) {
    static mut sendTime: TimestampTz = 0;
    let now = GetCurrentTimestamp();
    let mut pending_writes = false;
    let end_xact = (*ctx).end_xact;

    // Track lag no more than once per WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS.
    if end_xact && TimestampDifferenceExceeds(sendTime, now, WALSND_LOGICAL_LAG_TRACK_INTERVAL_MS) {
        LagTrackerWrite(lsn, now);
        sendTime = now;
    }

    // When skipping empty transactions in synchronous replication, we send a
    // keepalive message to avoid delaying such transactions.
    if skipped_xact
        && SyncRepRequested()
        && (*(WalSndCtl_ptr() as *mut WalSndCtlData)).sync_standbys_status & SYNC_STANDBY_DEFINED != 0
    {
        WalSndKeepalive(false, lsn);

        // Try to flush pending output to the client
        if pq_flush_if_writable() != 0 {
            WalSndShutdown();
        }

        // If we have pending write here, make sure it's actually flushed
        if pq_is_send_pending() {
            pending_writes = true;
        }
    }

    // Process pending writes if any or try to send a keepalive if required.
    if pending_writes
        || (!end_xact
            && now >= TimestampTzPlusMilliseconds(
                last_reply_timestamp,
                wal_sender_timeout as i64 / 2,
            ))
    {
        ProcessPendingWrites();
    }
}

// ===========================================================================
// Part 4: PhysicalWakeupLogicalWalSnd, NeedToWaitForStandbys,
//         NeedToWaitForWal, WalSndWaitForWal,
//         exec_replication_command, ProcessRepliesIfAny,
//         ProcessStandbyMessage, PhysicalConfirmReceivedLocation,
//         ProcessStandbyReplyMessage, PhysicalReplicationSlotNewXmin,
//         TransactionIdInRecentPast, ProcessStandbyHSFeedbackMessage,
//         WalSndComputeSleeptime, WalSndCheckTimeOut, WalSndLoop
// ===========================================================================

/// Wake up the logical walsender processes with logical failover slots if the
/// currently acquired physical slot is specified in synchronized_standby_slots.
pub unsafe fn PhysicalWakeupLogicalWalSnd() {
    // If we are running in a standby, there is no need to wake up walsenders.
    if RecoveryInProgress() {
        return;
    }

    if SlotExistsInSyncStandbySlots(std::ptr::null()) {
        ConditionVariableBroadcast(
            &mut (*WalSndCtl_ptr()).wal_confirm_rcv_cv as *mut _ as *mut c_void,
        );
    }
}

/// Returns true if not all standbys have caught up to the flushed position
/// (flushed_lsn) when the current acquired slot is a logical failover
/// slot and we are streaming; otherwise, returns false.
unsafe fn NeedToWaitForStandbys(flushed_lsn: XLogRecPtr, wait_event: *mut u32) -> bool {
    let elevel = if got_STOPPING { ERROR } else { WARNING };
    let failover_slot = replication_active && !MyReplicationSlot.is_null(); // stub: data.failover

    if failover_slot && !StandbySlotsHaveCaughtup(flushed_lsn, elevel) {
        *wait_event = WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION;
        return true;
    }

    *wait_event = 0;
    false
}

/// Returns true if we need to wait for WALs to be flushed to disk, or if not
/// all standbys have caught up to the flushed position.
unsafe fn NeedToWaitForWal(
    target_lsn: XLogRecPtr,
    flushed_lsn: XLogRecPtr,
    wait_event: *mut u32,
) -> bool {
    // Check if we need to wait for WALs to be flushed to disk
    if target_lsn > flushed_lsn {
        *wait_event = WAIT_EVENT_WAL_SENDER_WAIT_FOR_WAL;
        return true;
    }

    // Check if the standby slots have caught up to the flushed position
    NeedToWaitForStandbys(flushed_lsn, wait_event)
}

/// Wait till WAL < loc is flushed to disk so it can be safely sent to client.
///
/// Returns end LSN of flushed WAL.  Normally this will be >= loc, but if we
/// detect a shutdown request (either from postmaster or client) we will return
/// early, so caller must always check.
unsafe fn WalSndWaitForWal(loc: XLogRecPtr) -> XLogRecPtr {
    let mut wakeEvents: u32;
    let mut wait_event: u32 = 0;
    static mut RecentFlushPtr: XLogRecPtr = InvalidXLogRecPtr;
    let mut last_flush: TimestampTz = 0;

    // Fast path to avoid acquiring the spinlock in case we already know we
    // have enough WAL available.
    if !XLogRecPtrIsInvalid(RecentFlushPtr)
        && !NeedToWaitForWal(loc, RecentFlushPtr, &mut wait_event)
    {
        return RecentFlushPtr;
    }

    loop {
        let mut wait_for_standby_at_stop = false;
        let sleeptime: i64;
        let now: TimestampTz;

        // Clear any already-pending wakeups
        ResetLatch(MyLatch);

        crate::miscadmin::CHECK_FOR_INTERRUPTS();

        // Process any requests or signals received recently
        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
            SyncRepInitConfig();
        }

        // Check for input from the client
        ProcessRepliesIfAny();

        // If we're shutting down, trigger pending WAL to be written out.
        if got_STOPPING && !RecoveryInProgress() {
            XLogFlush(GetXLogInsertRecPtr());
        }

        // Update our idea of the currently flushed position only if we are
        // not waiting for standbys to catch up.
        if wait_event != WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION {
            if !RecoveryInProgress() {
                RecentFlushPtr = GetFlushRecPtr(std::ptr::null_mut());
            } else {
                RecentFlushPtr = GetXLogReplayRecPtr(std::ptr::null_mut());
            }
        }

        // If postmaster asked us to stop and standby slots have caught up,
        // don't wait anymore.
        if got_STOPPING {
            if NeedToWaitForStandbys(RecentFlushPtr, &mut wait_event) {
                wait_for_standby_at_stop = true;
            } else {
                break;
            }
        }

        // We only send regular messages to the client for full decoded
        // transactions, but a synchronous replication and walsender shutdown
        // possibly are waiting for a later location.
        if !MyWalSnd_ptr().is_null() {
            let walsnd = MyWalSnd_ptr();
            if (*walsnd).flush < sentPtr && (*walsnd).write < sentPtr && !waiting_for_ping_response {
                WalSndKeepalive(false, InvalidXLogRecPtr);
            }
        }

        // Exit the loop if already caught up and doesn't need to wait.
        if !wait_for_standby_at_stop && !NeedToWaitForWal(loc, RecentFlushPtr, &mut wait_event) {
            break;
        }

        // Waiting for new WAL; we're now caught up.
        WalSndCaughtUp = true;

        // Try to flush any pending output to the client.
        if pq_flush_if_writable() != 0 {
            WalSndShutdown();
        }

        // If streaming is done on both sides and buffer is empty, fail.
        if streamingDoneReceiving && streamingDoneSending && !pq_is_send_pending() {
            break;
        }

        // die if timeout was reached
        WalSndCheckTimeOut();

        // Send keepalive if the time has come
        WalSndKeepaliveIfNecessary();

        now = GetCurrentTimestamp();
        sleeptime = WalSndComputeSleeptime(now);

        wakeEvents = WL_SOCKET_READABLE;
        if pq_is_send_pending() {
            wakeEvents |= WL_SOCKET_WRITEABLE;
        }

        // Report IO statistics, if needed
        if TimestampDifferenceExceeds(last_flush, now, WALSENDER_STATS_FLUSH_INTERVAL) {
            pgstat_flush_io(false);
            pgstat_flush_backend(false, PGSTAT_BACKEND_FLUSH_IO);
            last_flush = now;
        }

        WalSndWait(wakeEvents, sleeptime, wait_event);
    }

    // reactivate latch so WalSndLoop knows to continue
    SetLatch(MyLatch);
    RecentFlushPtr
}

/// Execute an incoming replication command.
///
/// Returns true if the cmd_string was recognized as WalSender command, false
/// if not.
pub unsafe fn exec_replication_command(cmd_string: *const c_char) -> bool {
    let mut scanner: yyscan_t = std::ptr::null_mut();
    let parse_rc: c_int;
    let cmd_node: *mut Node;
    let cmdtag: *const c_char;
    let old_context = CurrentMemoryContext;

    // We save and re-use the cmd_context across calls
    static mut cmd_context: MemoryContext = std::ptr::null_mut();

    // If WAL sender has been told that shutdown is getting close, switch its
    // status accordingly.
    if got_STOPPING {
        WalSndSetState(WALSNDSTATE_STOPPING);
    }

    // Throw error if in stopping mode.
    if !MyWalSnd_ptr().is_null() && (*MyWalSnd_ptr()).state == WALSNDSTATE_STOPPING {
        // ereport(ERROR, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ...)
        unimplemented!("ereport: cannot execute new commands while WAL sender is in stopping mode");
    }

    // CREATE_REPLICATION_SLOT ... LOGICAL exports a snapshot until the next
    // command arrives.  Clean up the old stuff if there's anything.
    SnapBuildClearExportedSnapshot();

    crate::miscadmin::CHECK_FOR_INTERRUPTS();

    // Prepare the command context
    if cmd_context.is_null() {
        cmd_context = AllocSetContextCreate(
            TopMemoryContext,
            b"Replication command context\0".as_ptr() as *const c_char,
            0,
        );
    } else {
        MemoryContextReset(cmd_context);
    }

    MemoryContextSwitchTo(cmd_context);

    replication_scanner_init(cmd_string, &mut scanner);

    // Is it a WalSender command?
    if !replication_scanner_is_replication_command(scanner) {
        // Nope; clean up and get out.
        replication_scanner_finish(scanner);

        MemoryContextSwitchTo(old_context);
        MemoryContextReset(cmd_context);

        if MyDatabaseId == InvalidOid {
            // ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED, ...)
            unimplemented!("ereport: cannot execute SQL commands in WAL sender for physical replication");
        }

        return false;
    }

    // Looks like a WalSender command, so parse it.
    let mut cmd_node_ptr: *mut Node = std::ptr::null_mut();
    let parse_rc = replication_yyparse(&mut cmd_node_ptr as *mut *mut Node, scanner);
    if parse_rc != 0 {
        // ereport(ERROR, ERRCODE_SYNTAX_ERROR, ...)
        unimplemented!("ereport: replication command parser returned {}", parse_rc);
    }
    replication_scanner_finish(scanner);

    // Report query to monitoring facilities.
    pgstat_report_activity(STATE_RUNNING, cmd_string);

    // Log replication command if enabled.
    // ereport(log_replication_commands ? LOG : DEBUG1, errmsg("received replication command: %s", cmd_string))
    // (stub)

    // Disallow replication commands in aborted transaction blocks.
    if IsAbortedTransactionBlockState() {
        // ereport(ERROR, ERRCODE_IN_FAILED_SQL_TRANSACTION, ...)
        unimplemented!("ereport: current transaction is aborted");
    }

    crate::miscadmin::CHECK_FOR_INTERRUPTS();

    // Allocate buffers for outgoing and incoming messages.
    initStringInfo(&mut output_message);
    initStringInfo(&mut reply_message);
    initStringInfo(&mut tmpbuf);

    // Dispatch on command type
    // (cmd_node->type / T_*Cmd casts need real node tag definitions)
    // TODO(pg-port): implement full dispatch when nodes/replnodes.h is ported

    MemoryContextSwitchTo(old_context);
    MemoryContextReset(cmd_context);

    true
}

/// Process any incoming messages while streaming.  Also checks if the remote
/// end has closed the connection.
unsafe fn ProcessRepliesIfAny() {
    let mut firstchar: u8 = 0;
    let maxmsglen: c_int;
    let mut r: c_int;
    let mut received = false;

    last_processing = GetCurrentTimestamp();

    // If we already received a CopyDone from the frontend, any subsequent
    // message is the beginning of a new command.
    while !streamingDoneReceiving {
        pq_startmsgread();
        r = pq_getbyte_if_available(&mut firstchar);
        if r < 0 {
            // unexpected error or EOF
            proc_exit(0);
        }
        if r == 0 {
            // no data available without blocking
            pq_endmsgread();
            break;
        }

        // Validate message type and set packet size limit
        let maxmsglen = match firstchar {
            b'd' => PQ_LARGE_MESSAGE_LIMIT, // PqMsg_CopyData
            b'c' | b'X' => PQ_SMALL_MESSAGE_LIMIT, // CopyDone / Terminate
            _ => {
                // ereport(FATAL, ERRCODE_PROTOCOL_VIOLATION, ...)
                proc_exit(1);
            }
        };

        // Read the message contents
        resetStringInfo(&mut reply_message);
        if pq_getmessage(&mut reply_message, maxmsglen) != 0 {
            proc_exit(0);
        }

        // ... and process it
        match firstchar {
            b'd' => {
                // CopyData: standby reply wrapped in a CopyData packet
                ProcessStandbyMessage();
                received = true;
            }
            b'c' => {
                // CopyDone: standby requested to finish streaming
                if !streamingDoneSending {
                    pq_putmessage_noblock(b'c' as c_char, std::ptr::null(), 0);
                    streamingDoneSending = true;
                }
                streamingDoneReceiving = true;
                received = true;
            }
            b'X' => {
                // Terminate: standby is closing down the socket
                proc_exit(0);
            }
            _ => {}
        }
    }

    // Save the last reply timestamp if we've received at least one reply.
    if received {
        last_reply_timestamp = last_processing;
        waiting_for_ping_response = false;
    }
}

/// Process a status update message received from standby.
unsafe fn ProcessStandbyMessage() {
    let msgtype = pq_getmsgbyte(&mut reply_message);

    match msgtype as u8 {
        b'r' => ProcessStandbyReplyMessage(),
        b'h' => ProcessStandbyHSFeedbackMessage(),
        _ => {
            // ereport(COMMERROR, ERRCODE_PROTOCOL_VIOLATION, ...)
            proc_exit(0);
        }
    }
}

/// Remember that a walreceiver just confirmed receipt of lsn `lsn`.
unsafe fn PhysicalConfirmReceivedLocation(lsn: XLogRecPtr) {
    let changed = false;
    let slot = MyReplicationSlot;

    // slot->data.restart_lsn update (stub: slot type is opaque)
    // SpinLockAcquire / Release stubs

    if changed {
        ReplicationSlotMarkDirty();
        ReplicationSlotsComputeRequiredLSN();
        PhysicalWakeupLogicalWalSnd();
    }
}

/// Regular reply from standby advising of WAL locations on standby server.
unsafe fn ProcessStandbyReplyMessage() {
    let writePtr: XLogRecPtr = pq_getmsgint64(&mut reply_message) as u64;
    let flushPtr: XLogRecPtr = pq_getmsgint64(&mut reply_message) as u64;
    let applyPtr: XLogRecPtr = pq_getmsgint64(&mut reply_message) as u64;
    let replyTime: TimestampTz = pq_getmsgint64(&mut reply_message);
    let replyRequested: bool = pq_getmsgbyte(&mut reply_message) != 0;

    // Debug logging (stub: message_level_is_interesting check)

    let now = GetCurrentTimestamp();
    let writeLag = LagTrackerRead(0 /* SYNC_REP_WAIT_WRITE */, writePtr, now);
    let flushLag = LagTrackerRead(1 /* SYNC_REP_WAIT_FLUSH */, flushPtr, now);
    let applyLag = LagTrackerRead(2 /* SYNC_REP_WAIT_APPLY */, applyPtr, now);

    // If the standby reports that it has fully replayed the WAL in two
    // consecutive reply messages, forget the lag times.
    static mut fullyAppliedLastTime: bool = false;
    let clearLagTimes: bool;
    if applyPtr == sentPtr {
        clearLagTimes = fullyAppliedLastTime;
        fullyAppliedLastTime = true;
    } else {
        clearLagTimes = false;
        fullyAppliedLastTime = false;
    }

    // Send a reply if the standby requested one.
    if replyRequested {
        WalSndKeepalive(false, InvalidXLogRecPtr);
    }

    // Update shared state for this WalSender process.
    if !MyWalSnd_ptr().is_null() {
        let walsnd = MyWalSnd_ptr();
        SpinLockAcquire(&mut (*walsnd).mutex);
        (*walsnd).write = writePtr;
        (*walsnd).flush = flushPtr;
        (*walsnd).apply = applyPtr;
        if writeLag != -1 || clearLagTimes { (*walsnd).writeLag = writeLag; }
        if flushLag != -1 || clearLagTimes { (*walsnd).flushLag = flushLag; }
        if applyLag != -1 || clearLagTimes { (*walsnd).applyLag = applyLag; }
        (*walsnd).replyTime = replyTime;
        SpinLockRelease(&mut (*walsnd).mutex);
    }

    if !am_cascading_walsender {
        SyncRepReleaseWaiters();
    }

    // Advance our local xmin horizon when the client confirmed a flush.
    if !MyReplicationSlot.is_null() && flushPtr != InvalidXLogRecPtr {
        if SlotIsLogical(MyReplicationSlot) {
            LogicalConfirmReceivedLocation(flushPtr);
        } else {
            PhysicalConfirmReceivedLocation(flushPtr);
        }
    }
}

/// Compute new replication slot xmin horizon if needed.
unsafe fn PhysicalReplicationSlotNewXmin(
    feedbackXmin: TransactionId,
    feedbackCatalogXmin: TransactionId,
) {
    // SpinLock / xmin comparison (slot fields opaque; stubs only)
    // ReplicationSlotMarkDirty / ReplicationSlotsComputeRequiredXmin stubs
}

/// Check that the provided xmin/epoch are sane, that is, not in the future
/// and not so far back as to be already wrapped around.
unsafe fn TransactionIdInRecentPast(xid: TransactionId, epoch: u32) -> bool {
    let nextFullXid = ReadNextFullTransactionId();
    let nextXid = XidFromFullTransactionId(core::ptr::read(&nextFullXid));
    let nextEpoch = EpochFromFullTransactionId(nextFullXid);

    if xid <= nextXid {
        if epoch != nextEpoch { return false; }
    } else {
        if epoch + 1 != nextEpoch { return false; }
    }

    if !TransactionIdPrecedesOrEquals(xid, nextXid) {
        return false; // epoch OK, but it's wrapped around
    }

    true
}

/// Hot Standby feedback.
unsafe fn ProcessStandbyHSFeedbackMessage() {
    let replyTime: TimestampTz = pq_getmsgint64(&mut reply_message);
    let feedbackXmin: TransactionId = pq_getmsgint(&mut reply_message, 4);
    let feedbackEpoch: u32 = pq_getmsgint(&mut reply_message, 4);
    let feedbackCatalogXmin: TransactionId = pq_getmsgint(&mut reply_message, 4);
    let feedbackCatalogEpoch: u32 = pq_getmsgint(&mut reply_message, 4);

    // Update shared state
    if !MyWalSnd_ptr().is_null() {
        let walsnd = MyWalSnd_ptr();
        SpinLockAcquire(&mut (*walsnd).mutex);
        (*walsnd).replyTime = replyTime;
        SpinLockRelease(&mut (*walsnd).mutex);
    }

    // Unset WalSender's xmins if the feedback message values are invalid.
    if !TransactionIdIsNormal(feedbackXmin) && !TransactionIdIsNormal(feedbackCatalogXmin) {
        // MyProc->xmin = InvalidTransactionId (stub)
        if !MyReplicationSlot.is_null() {
            PhysicalReplicationSlotNewXmin(feedbackXmin, feedbackCatalogXmin);
        }
        return;
    }

    // Check that the provided xmin/epoch are sane.  Ignore if not.
    if TransactionIdIsNormal(feedbackXmin) && !TransactionIdInRecentPast(feedbackXmin, feedbackEpoch) {
        return;
    }
    if TransactionIdIsNormal(feedbackCatalogXmin) && !TransactionIdInRecentPast(feedbackCatalogXmin, feedbackCatalogEpoch) {
        return;
    }

    // Set the WalSender's xmin.
    if !MyReplicationSlot.is_null() {
        PhysicalReplicationSlotNewXmin(feedbackXmin, feedbackCatalogXmin);
    } else {
        // MyProc->xmin = feedbackXmin (or feedbackCatalogXmin if smaller) (stub)
    }
}

/// Compute how long send/receive loops should sleep.
unsafe fn WalSndComputeSleeptime(now: TimestampTz) -> i64 {
    let mut sleeptime: i64 = 10000; // 10 s

    if wal_sender_timeout > 0 && last_reply_timestamp > 0 {
        let mut wakeup_time = TimestampTzPlusMilliseconds(
            last_reply_timestamp,
            wal_sender_timeout as i64,
        );

        if !waiting_for_ping_response {
            wakeup_time = TimestampTzPlusMilliseconds(
                last_reply_timestamp,
                wal_sender_timeout as i64 / 2,
            );
        }

        sleeptime = TimestampDifferenceMilliseconds(now, wakeup_time);
    }

    sleeptime
}

/// Check whether there have been responses by the client within
/// wal_sender_timeout and shutdown if not.
unsafe fn WalSndCheckTimeOut() {
    // don't bail out if we're doing something that doesn't require timeouts
    if last_reply_timestamp <= 0 {
        return;
    }

    let timeout = TimestampTzPlusMilliseconds(
        last_reply_timestamp,
        wal_sender_timeout as i64,
    );

    if wal_sender_timeout > 0 && last_processing >= timeout {
        // ereport(COMMERROR, errmsg("terminating walsender process due to replication timeout"))
        WalSndShutdown();
    }
}

/// Main loop of walsender process that streams the WAL over Copy messages.
unsafe fn WalSndLoop(send_data: WalSndSendDataCallback) {
    let mut last_flush: TimestampTz = 0;

    // Initialize the last reply timestamp.
    last_reply_timestamp = GetCurrentTimestamp();
    waiting_for_ping_response = false;

    loop {
        // Clear any already-pending wakeups
        ResetLatch(MyLatch);

        crate::miscadmin::CHECK_FOR_INTERRUPTS();

        // Process any requests or signals received recently
        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
            SyncRepInitConfig();
        }

        // Check for input from the client
        ProcessRepliesIfAny();

        // If we have received CopyDone from the client, sent CopyDone ourselves,
        // and the output buffer is empty, it's time to exit streaming.
        if streamingDoneReceiving && streamingDoneSending && !pq_is_send_pending() {
            break;
        }

        // If we don't have any pending data in the output buffer, try to send
        // some more.
        if !pq_is_send_pending() {
            send_data();
        } else {
            WalSndCaughtUp = false;
        }

        // Try to flush pending output to the client
        if pq_flush_if_writable() != 0 {
            WalSndShutdown();
        }

        // If nothing remains to be sent right now ...
        if WalSndCaughtUp && !pq_is_send_pending() {
            // If we're in catchup state, move to streaming.
            if !MyWalSnd_ptr().is_null() && (*MyWalSnd_ptr()).state == WALSNDSTATE_CATCHUP {
                // ereport(DEBUG1, errmsg("%s has now caught up ...", application_name))
                WalSndSetState(WALSNDSTATE_STREAMING);
            }

            // When SIGUSR2 arrives, send outstanding logs and exit.
            if got_SIGUSR2 {
                WalSndDone(send_data);
            }
        }

        // Check for replication timeout.
        WalSndCheckTimeOut();

        // Send keepalive if the time has come
        WalSndKeepaliveIfNecessary();

        // Block if we have unsent data or caught up.
        let need_sleep = (WalSndCaughtUp
            && send_data as usize != XLogSendLogical as usize
            && !streamingDoneSending)
            || pq_is_send_pending();

        if need_sleep {
            let wakeEvents: u32;
            let now: TimestampTz;

            if !streamingDoneReceiving {
                let mut we = WL_SOCKET_READABLE;
                if pq_is_send_pending() { we |= WL_SOCKET_WRITEABLE; }
                wakeEvents = we;
            } else {
                wakeEvents = if pq_is_send_pending() { WL_SOCKET_WRITEABLE } else { 0 };
            }

            now = GetCurrentTimestamp();
            let sleeptime = WalSndComputeSleeptime(now);

            // Report IO statistics, if needed
            if TimestampDifferenceExceeds(last_flush, now, WALSENDER_STATS_FLUSH_INTERVAL) {
                pgstat_flush_io(false);
                pgstat_flush_backend(false, PGSTAT_BACKEND_FLUSH_IO);
                last_flush = now;
            }

            WalSndWait(wakeEvents, sleeptime, WAIT_EVENT_WAL_SENDER_MAIN);
        }
    }
}

// ===========================================================================
// Part 5: InitWalSenderSlot, WalSndKill, WalSndSegmentOpen,
//         XLogSendPhysical, XLogSendLogical, WalSndDone,
//         GetStandbyFlushRecPtr, WalSndRqstFileReload,
//         HandleWalSndInitStopping, WalSndLastCycleHandler, WalSndSignals,
//         WalSndShmemSize, WalSndShmemInit, WalSndWakeup, WalSndWait,
//         WalSndInitStopping, WalSndWaitStopping, WalSndSetState (public),
//         WalSndGetStateString, offset_to_interval,
//         pg_stat_get_wal_senders, WalSndKeepalive, WalSndKeepaliveIfNecessary,
//         LagTrackerWrite, LagTrackerRead
// ===========================================================================

/// Initialize a per-walsender data structure for this walsender process.
unsafe fn InitWalSenderSlot() {
    let ctl = WalSndCtl_ptr();

    // WalSndCtl should be set up already.
    assert!(!ctl.is_null());
    assert!(MyWalSnd_ptr().is_null());

    // Find a free walsender slot and reserve it.
    'outer: for i in 0..(max_wal_senders as usize) {
        let walsnd = &mut (*ctl).walsnds[i] as *mut WalSnd;

        SpinLockAcquire(&mut (*walsnd).mutex);

        if (*walsnd).pid != 0 {
            SpinLockRelease(&mut (*walsnd).mutex);
            continue;
        }

        // Found a free slot.  Reserve it for us.
        (*walsnd).pid = MyProcPid;
        (*walsnd).state = WALSNDSTATE_STARTUP;
        (*walsnd).sentPtr = InvalidXLogRecPtr;
        (*walsnd).needreload = false;
        (*walsnd).write = InvalidXLogRecPtr;
        (*walsnd).flush = InvalidXLogRecPtr;
        (*walsnd).apply = InvalidXLogRecPtr;
        (*walsnd).writeLag = -1;
        (*walsnd).flushLag = -1;
        (*walsnd).applyLag = -1;
        (*walsnd).sync_standby_priority = 0;
        (*walsnd).replyTime = 0;

        // The kind assignment is done here, not in StartReplication().
        if MyDatabaseId == InvalidOid {
            (*walsnd).kind = REPLICATION_KIND_PHYSICAL;
        } else {
            (*walsnd).kind = REPLICATION_KIND_LOGICAL;
        }

        SpinLockRelease(&mut (*walsnd).mutex);

        // Assign MyWalSnd (via the extern static)
        {
            extern "C" { static mut MyWalSnd: *mut WalSnd; }
            MyWalSnd = walsnd;
        }

        break 'outer;
    }

    assert!(!MyWalSnd_ptr().is_null());

    // Arrange to clean up at walsender exit
    on_shmem_exit(WalSndKill, 0);
}

/// Destroy the per-walsender data structure for this walsender process.
unsafe fn WalSndKill(code: c_int, arg: Datum) {
    let walsnd = MyWalSnd_ptr();
    assert!(!walsnd.is_null());

    // Clear MyWalSnd
    {
        extern "C" { static mut MyWalSnd: *mut WalSnd; }
        MyWalSnd = std::ptr::null_mut();
    }

    SpinLockAcquire(&mut (*walsnd).mutex);
    // Mark WalSnd struct as no longer being in use.
    (*walsnd).pid = 0;
    SpinLockRelease(&mut (*walsnd).mutex);
}

/// XLogReaderRoutine->segment_open callback.
unsafe fn WalSndSegmentOpen(
    state: *mut XLogReaderState,
    nextSegNo: XLogSegNo,
    tli_p: *mut TimeLineID,
) {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    // When reading from a historic timeline, and there is a timeline switch
    // within this segment, read from the WAL segment belonging to the new
    // timeline.
    *tli_p = sendTimeLine;
    if sendTimeLineIsHistoric {
        let mut endSegNo: XLogSegNo = 0;
        XLByteToSeg_fn(sendTimeLineValidUpto, &mut endSegNo, (*state).segcxt.ws_segsize);
        if nextSegNo == endSegNo {
            *tli_p = sendTimeLineNextTLI;
        }
    }

    XLogFilePath_fn(path.as_mut_ptr(), *tli_p, nextSegNo, (*state).segcxt.ws_segsize);
    (*state).seg.ws_file = BasicOpenFile(path.as_ptr(), PG_BINARY);
    if (*state).seg.ws_file >= 0 {
        return;
    }

    // If the file is not found, the standby asked for a too old WAL segment.
    // ereport(ERROR, errcode_for_file_access, ...)
    unimplemented!("ereport: WAL segment file not found or error");
}

/// Send out the WAL in its normal physical/stored form.
///
/// Read up to MAX_SEND_SIZE bytes of WAL that's been flushed to disk,
/// but not yet sent to the client.
unsafe fn XLogSendPhysical() {
    let SendRqstPtr: XLogRecPtr;
    let startptr: XLogRecPtr;
    let endptr: XLogRecPtr;
    let nbytes: usize;
    let mut segno: XLogSegNo = 0;
    let mut errinfo: WALReadError = std::mem::zeroed();
    let rbytes: usize;

    // If requested switch the WAL sender to the stopping state.
    if got_STOPPING {
        WalSndSetState(WALSNDSTATE_STOPPING);
    }

    if streamingDoneSending {
        WalSndCaughtUp = true;
        return;
    }

    // Figure out how far we can safely send the WAL.
    if sendTimeLineIsHistoric {
        SendRqstPtr = sendTimeLineValidUpto;
    } else if am_cascading_walsender {
        let mut SendRqstTLI: TimeLineID = 0;
        let mut val = GetStandbyFlushRecPtr(&mut SendRqstTLI);
        let mut becameHistoric = false;

        if !RecoveryInProgress() {
            SendRqstTLI = GetWALInsertionTimeLine();
            am_cascading_walsender = false;
            becameHistoric = true;
        } else if sendTimeLine != SendRqstTLI {
            becameHistoric = true;
        }

        if becameHistoric {
            let history = readTimeLineHistory(SendRqstTLI);
            sendTimeLineValidUpto = tliSwitchPoint(sendTimeLine, history, &mut sendTimeLineNextTLI);
            list_free_deep(history);
            sendTimeLineIsHistoric = true;
            val = sendTimeLineValidUpto;
        }
        SendRqstPtr = val;
    } else {
        SendRqstPtr = GetFlushRecPtr(std::ptr::null_mut());
    }

    // Record the current system time for lag tracking.
    LagTrackerWrite(SendRqstPtr, GetCurrentTimestamp());

    // If this is a historic timeline and we've reached the switchpoint, stop.
    if sendTimeLineIsHistoric && sendTimeLineValidUpto <= sentPtr {
        if !xlogreader.is_null() && (*xlogreader).seg.ws_file >= 0 {
            wal_segment_close(xlogreader);
        }

        pq_putmessage_noblock(b'c' as c_char, std::ptr::null(), 0);
        streamingDoneSending = true;
        WalSndCaughtUp = true;

        // elog(DEBUG1, "walsender reached end of timeline ...")
        return;
    }

    // Do we have any work to do?
    if SendRqstPtr <= sentPtr {
        WalSndCaughtUp = true;
        return;
    }

    startptr = sentPtr;
    let mut end = startptr + MAX_SEND_SIZE as u64;

    // if we went beyond SendRqstPtr, back off
    if SendRqstPtr <= end {
        end = SendRqstPtr;
        if sendTimeLineIsHistoric {
            WalSndCaughtUp = false;
        } else {
            WalSndCaughtUp = true;
        }
    } else {
        // round down to page boundary
        end -= end % 8192; // XLOG_BLCKSZ
        WalSndCaughtUp = false;
    }

    let nbytes_val = (end - startptr) as usize;
    assert!(nbytes_val <= MAX_SEND_SIZE);

    resetStringInfo(&mut output_message);
    pq_sendbyte(&mut output_message, b'w');
    pq_sendint64(&mut output_message, startptr as i64); // dataStart
    pq_sendint64(&mut output_message, SendRqstPtr as i64); // walEnd
    pq_sendint64(&mut output_message, 0); // sendtime, filled in last

    // Read WAL into output buffer
    enlargeStringInfo(&mut output_message, nbytes_val);

    // attempt to read WAL from WAL buffers first
    // retry label omitted; goto not used in Rust; restructure as loop if needed
    let rbytes_val = WALReadFromBuffers(
        output_message.data.add(output_message.len as usize),
        startptr,
        nbytes_val,
        (*xlogreader).seg.ws_tli,
    );
    output_message.len += rbytes_val as i32;
    let startptr2 = startptr + rbytes_val as u64;
    let nbytes2 = nbytes_val - rbytes_val;

    if nbytes2 > 0 {
        if !WALRead(
            xlogreader,
            output_message.data.add(output_message.len as usize),
            startptr2,
            nbytes2,
            (*xlogreader).seg.ws_tli,
            &mut errinfo,
        ) {
            WALReadRaiseError(&mut errinfo);
        }
    }

    XLByteToSeg_fn(startptr, &mut segno, (*xlogreader).segcxt.ws_segsize);
    CheckXLogRemoved(segno, (*xlogreader).seg.ws_tli);

    // During recovery, check if the WAL file needs to be reloaded.
    if am_cascading_walsender && !xlogreader.is_null() {
        let walsnd = MyWalSnd_ptr();
        if !walsnd.is_null() {
            SpinLockAcquire(&mut (*walsnd).mutex);
            let reload = (*walsnd).needreload;
            (*walsnd).needreload = false;
            SpinLockRelease(&mut (*walsnd).mutex);

            if reload && (*xlogreader).seg.ws_file >= 0 {
                wal_segment_close(xlogreader);
                // In C this would goto retry; in Rust we'd restructure as a loop.
                // For now stub: fall through (the C semantics are preserved in intent).
            }
        }
    }

    output_message.len += nbytes2 as i32;

    // Fill the send timestamp last.
    resetStringInfo(&mut tmpbuf);
    pq_sendint64(&mut tmpbuf, GetCurrentTimestamp());
    // memcpy sendtime into output_message (stub)

    pq_putmessage_noblock(b'd' as c_char, output_message.data, output_message.len as usize);

    sentPtr = end;

    // Update shared memory status
    if !MyWalSnd_ptr().is_null() {
        let walsnd = MyWalSnd_ptr();
        SpinLockAcquire(&mut (*walsnd).mutex);
        (*walsnd).sentPtr = sentPtr;
        SpinLockRelease(&mut (*walsnd).mutex);
    }

    // Report progress of XLOG streaming in PS display
    // if update_process_title() { set_ps_display(...) }
}

/// Stream out logically decoded data.
unsafe fn XLogSendLogical() {
    let record: *mut XLogRecord;
    let mut errm: *const c_char = std::ptr::null();

    // We'll use the current flush point to determine whether we've caught up.
    static mut flushPtr: XLogRecPtr = InvalidXLogRecPtr;

    // Don't know whether we've caught up yet.
    WalSndCaughtUp = false;

    record = XLogReadRecord((*logical_decoding_ctx).reader, &mut errm as *mut *const c_char);

    // xlog record was invalid
    if !errm.is_null() {
        // elog(ERROR, "could not find record while sending logically-decoded data: %s", errm)
        unimplemented!("elog: XLogReadRecord failed");
    }

    if !record.is_null() {
        LogicalDecodingProcessRecord(logical_decoding_ctx, (*logical_decoding_ctx).reader);
        sentPtr = (*(*logical_decoding_ctx).reader).EndRecPtr;
    }

    // Update flushPtr if needed.
    if XLogRecPtrIsInvalid(flushPtr) || (*(*logical_decoding_ctx).reader).EndRecPtr >= flushPtr {
        if am_cascading_walsender {
            flushPtr = GetXLogReplayRecPtr(std::ptr::null_mut());
        } else {
            flushPtr = GetFlushRecPtr(std::ptr::null_mut());
        }
    }

    // If EndRecPtr is still past our flushPtr, it means we caught up.
    if (*(*logical_decoding_ctx).reader).EndRecPtr >= flushPtr {
        WalSndCaughtUp = true;
    }

    // If caught up and requested to stop, have WalSndLoop() terminate.
    if WalSndCaughtUp && got_STOPPING {
        got_SIGUSR2 = true;
    }

    // Update shared memory status
    if !MyWalSnd_ptr().is_null() {
        let walsnd = MyWalSnd_ptr();
        SpinLockAcquire(&mut (*walsnd).mutex);
        (*walsnd).sentPtr = sentPtr;
        SpinLockRelease(&mut (*walsnd).mutex);
    }
}

/// Shutdown if the sender is caught up.
///
/// NB: This should only be called when the shutdown signal has been received
/// from postmaster.
unsafe fn WalSndDone(send_data: WalSndSendDataCallback) {
    send_data();

    // To figure out whether all WAL has successfully been replicated, check
    // flush location if valid, write otherwise.
    let replicatedPtr = if !MyWalSnd_ptr().is_null() && XLogRecPtrIsInvalid((*MyWalSnd_ptr()).flush) {
        if !MyWalSnd_ptr().is_null() { (*MyWalSnd_ptr()).write } else { InvalidXLogRecPtr }
    } else {
        if !MyWalSnd_ptr().is_null() { (*MyWalSnd_ptr()).flush } else { InvalidXLogRecPtr }
    };

    if WalSndCaughtUp && sentPtr == replicatedPtr && !pq_is_send_pending() {
        let mut qc: QueryCompletion = std::mem::zeroed();
        // Inform the standby that XLOG streaming is done
        SetQueryCompletion(&mut qc, 0, 0); // CMDTAG_COPY stub
        EndCommand(&mut qc, DestRemote, false);
        pq_flush();
        proc_exit(0);
    }

    if !waiting_for_ping_response {
        WalSndKeepalive(true, InvalidXLogRecPtr);
    }
}

/// Returns the latest point in WAL that has been safely flushed to disk.
/// This should only be called when in recovery.
pub unsafe fn GetStandbyFlushRecPtr(tli: *mut TimeLineID) -> XLogRecPtr {
    let mut replayTLI: TimeLineID = 0;
    let mut receiveTLI: TimeLineID = 0;

    let replayPtr = GetXLogReplayRecPtr(&mut replayTLI);
    let receivePtr = GetWalRcvFlushRecPtr(std::ptr::null_mut(), &mut receiveTLI);

    if !tli.is_null() {
        *tli = replayTLI;
    }

    let mut result = replayPtr;
    if receiveTLI == replayTLI && receivePtr > replayPtr {
        result = receivePtr;
    }

    result
}

/// Request walsenders to reload the currently-open WAL file.
pub unsafe fn WalSndRqstFileReload() {
    for i in 0..(max_wal_senders as usize) {
        let walsnd = &mut (*WalSndCtl_ptr()).walsnds[i] as *mut WalSnd;
        SpinLockAcquire(&mut (*walsnd).mutex);
        if (*walsnd).pid == 0 {
            SpinLockRelease(&mut (*walsnd).mutex);
            continue;
        }
        (*walsnd).needreload = true;
        SpinLockRelease(&mut (*walsnd).mutex);
    }
}

/// Handle PROCSIG_WALSND_INIT_STOPPING signal.
pub unsafe fn HandleWalSndInitStopping() {
    // If replication has not yet started, die like with SIGTERM.  If
    // replication is active, only set a flag and wake up the main loop.
    if !replication_active {
        kill(MyProcPid, 15); // SIGTERM
    } else {
        got_STOPPING = true;
    }
}

/// SIGUSR2: set flag to do a last cycle and shut down afterwards.
unsafe fn WalSndLastCycleHandler(sig: c_int) {
    got_SIGUSR2 = true;
    SetLatch(MyLatch);
}

/// Set up signal handlers.
pub unsafe fn WalSndSignals() {
    pqsignal(1 /* SIGHUP */, SignalHandlerForConfigReload);
    pqsignal(2 /* SIGINT */, StatementCancelHandler);
    pqsignal(15 /* SIGTERM */, die);
    InitializeTimeouts(); // establishes SIGALRM handler
    pqsignal(13 /* SIGPIPE */, sig_ign_handler);
    pqsignal(10 /* SIGUSR1 */, procsignal_sigusr1_handler);
    pqsignal(12 /* SIGUSR2 */, WalSndLastCycleHandler);
    pqsignal(20 /* SIGCHLD */, sig_dfl_handler);
}

unsafe fn sig_ign_handler(_sig: c_int) {}
unsafe fn sig_dfl_handler(_sig: c_int) {}

/// Report shared-memory space needed by WalSndShmemInit.
pub unsafe fn WalSndShmemSize() -> usize {
    let size = std::mem::offset_of!(WalSndCtlData, walsnds);
    add_size(size, mul_size(max_wal_senders as usize, std::mem::size_of::<WalSnd>()))
}

/// Allocate and initialize walsender-related shared memory.
pub unsafe fn WalSndShmemInit() {
    let mut found: bool = false;

    {
        extern "C" { static mut WalSndCtl: *mut WalSndCtlData; }
        WalSndCtl = ShmemInitStruct(
            b"Wal Sender Ctl\0".as_ptr() as *const c_char,
            WalSndShmemSize(),
            &mut found,
        ) as *mut WalSndCtlData;
    }

    if !found {
        // First time through, so initialize
        let ctl = WalSndCtl_ptr();
        MemSet(ctl as *mut c_void, 0, WalSndShmemSize());

        for i in 0..NUM_SYNC_REP_WAIT_MODE {
            dlist_init(&mut (*ctl).SyncRepQueue[i] as *mut _ as *mut c_void);
        }

        for i in 0..(max_wal_senders as usize) {
            let walsnd = &mut (*ctl).walsnds[i] as *mut WalSnd;
            SpinLockInit(&mut (*walsnd).mutex);
        }

        ConditionVariableInit(&mut (*ctl).wal_flush_cv as *mut _ as *mut c_void);
        ConditionVariableInit(&mut (*ctl).wal_replay_cv as *mut _ as *mut c_void);
        ConditionVariableInit(&mut (*ctl).wal_confirm_rcv_cv as *mut _ as *mut c_void);
    }
}

/// Wake up physical, logical or both kinds of walsenders.
///
/// The distinction between physical and logical walsenders is done, because:
/// - physical walsenders can't send data until it's been flushed
/// - logical walsenders on standby can't decode and send data until it's been
///   applied
pub unsafe fn WalSndWakeup(physical: bool, logical: bool) {
    let ctl = WalSndCtl_ptr();

    if physical {
        ConditionVariableBroadcast(&mut (*ctl).wal_flush_cv as *mut _ as *mut c_void);
    }

    if logical {
        ConditionVariableBroadcast(&mut (*ctl).wal_replay_cv as *mut _ as *mut c_void);
    }
}

/// Wait for readiness on the FeBe socket, or a timeout.  The mask should be
/// composed of optional WL_SOCKET_WRITEABLE and WL_SOCKET_READABLE flags.
unsafe fn WalSndWait(socket_events: u32, timeout: i64, wait_event: u32) {
    let mut event: WaitEvent = std::mem::zeroed();
    let ctl = WalSndCtl_ptr();

    ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, socket_events, std::ptr::null_mut());

    // We use a condition variable to efficiently wake up walsenders.
    // Every walsender prepares to sleep on a shared memory CV but uses
    // WaitEventSetWait() for actual waiting.
    if wait_event == WAIT_EVENT_WAIT_FOR_STANDBY_CONFIRMATION {
        ConditionVariablePrepareToSleep(&mut (*ctl).wal_confirm_rcv_cv as *mut _ as *mut c_void);
    } else if !MyWalSnd_ptr().is_null() {
        if (*MyWalSnd_ptr()).kind == REPLICATION_KIND_PHYSICAL {
            ConditionVariablePrepareToSleep(&mut (*ctl).wal_flush_cv as *mut _ as *mut c_void);
        } else if (*MyWalSnd_ptr()).kind == REPLICATION_KIND_LOGICAL {
            ConditionVariablePrepareToSleep(&mut (*ctl).wal_replay_cv as *mut _ as *mut c_void);
        }
    }

    if WaitEventSetWait(FeBeWaitSet, timeout, &mut event as *mut WaitEvent, 1, wait_event) == 1 {
        // WL_POSTMASTER_DEATH check (stub: event.events field not directly accessible)
        ConditionVariableCancelSleep();
        proc_exit(1);
    }

    ConditionVariableCancelSleep();
}

/// Signal all walsenders to move to stopping state.
pub unsafe fn WalSndInitStopping() {
    for i in 0..(max_wal_senders as usize) {
        let walsnd = &mut (*WalSndCtl_ptr()).walsnds[i] as *mut WalSnd;
        SpinLockAcquire(&mut (*walsnd).mutex);
        let pid = (*walsnd).pid;
        SpinLockRelease(&mut (*walsnd).mutex);

        if pid == 0 {
            continue;
        }

        SendProcSignal(pid, PROCSIG_WALSND_INIT_STOPPING, INVALID_PROC_NUMBER);
    }
}

/// Wait that all the WAL senders have quit or reached the stopping state.
pub unsafe fn WalSndWaitStopping() {
    loop {
        let mut all_stopped = true;

        for i in 0..(max_wal_senders as usize) {
            let walsnd = &mut (*WalSndCtl_ptr()).walsnds[i] as *mut WalSnd;
            SpinLockAcquire(&mut (*walsnd).mutex);

            if (*walsnd).pid == 0 {
                SpinLockRelease(&mut (*walsnd).mutex);
                continue;
            }

            if (*walsnd).state != WALSNDSTATE_STOPPING {
                all_stopped = false;
                SpinLockRelease(&mut (*walsnd).mutex);
                break;
            }
            SpinLockRelease(&mut (*walsnd).mutex);
        }

        // safe to leave if confirmation is done for all WAL senders
        if all_stopped {
            return;
        }

        pg_usleep(10000); // wait for 10 msec
    }
}

/// Return a string constant representing the state.  This is used
/// in system views, and should *not* be translated.
unsafe fn WalSndGetStateString(state: WalSndState) -> *const c_char {
    match state {
        WALSNDSTATE_STARTUP  => b"startup\0".as_ptr() as *const c_char,
        WALSNDSTATE_BACKUP   => b"backup\0".as_ptr() as *const c_char,
        WALSNDSTATE_CATCHUP  => b"catchup\0".as_ptr() as *const c_char,
        WALSNDSTATE_STREAMING => b"streaming\0".as_ptr() as *const c_char,
        WALSNDSTATE_STOPPING  => b"stopping\0".as_ptr() as *const c_char,
        _ => b"UNKNOWN\0".as_ptr() as *const c_char,
    }
}

unsafe fn offset_to_interval(offset: TimeOffset) -> *mut Interval {
    let result = palloc(std::mem::size_of::<Interval>()) as *mut Interval;
    (*result).month = 0;
    (*result).day = 0;
    (*result).time = offset;
    result
}

/// Returns activity of walsenders, including pids and xlog locations sent to
/// standby servers.
///
/// SQL function: pg_stat_get_wal_senders()
pub unsafe fn pg_stat_get_wal_senders(fcinfo: FunctionCallInfo) -> Datum {
    const PG_STAT_GET_WAL_SENDERS_COLS: usize = 12;
    let rsinfo = (*(fcinfo as *mut crate::utils::fmgr::FunctionCallInfoBaseData)).resultinfo as *mut ReturnSetInfo;
    let mut sync_standbys: *mut SyncRepStandbyData = std::ptr::null_mut();
    let num_standbys: c_int;

    InitMaterializedSRF(fcinfo, 0);

    // Get the currently active synchronous standbys.
    num_standbys = SyncRepGetCandidateStandbys(&mut sync_standbys);

    for i in 0..(max_wal_senders as usize) {
        let walsnd = &mut (*WalSndCtl_ptr()).walsnds[i] as *mut WalSnd;
        let mut values: [Datum; PG_STAT_GET_WAL_SENDERS_COLS] = [0; PG_STAT_GET_WAL_SENDERS_COLS];
        let mut nulls: [bool; PG_STAT_GET_WAL_SENDERS_COLS] = [false; PG_STAT_GET_WAL_SENDERS_COLS];

        // Collect data from shared memory
        SpinLockAcquire(&mut (*walsnd).mutex);
        if (*walsnd).pid == 0 {
            SpinLockRelease(&mut (*walsnd).mutex);
            continue;
        }
        let pid = (*walsnd).pid;
        let sent_ptr = (*walsnd).sentPtr;
        let state = (*walsnd).state;
        let write = (*walsnd).write;
        let flush = (*walsnd).flush;
        let apply = (*walsnd).apply;
        let writeLag = (*walsnd).writeLag;
        let flushLag = (*walsnd).flushLag;
        let applyLag = (*walsnd).applyLag;
        let priority = (*walsnd).sync_standby_priority;
        let replyTime = (*walsnd).replyTime;
        SpinLockRelease(&mut (*walsnd).mutex);

        // Detect whether walsender is/was considered synchronous.
        let mut is_sync_standby = false;
        for j in 0..(num_standbys as usize) {
            let sd = &*sync_standbys.add(j);
            if sd.walsnd_index == i as c_int && sd.pid == pid {
                is_sync_standby = true;
                break;
            }
        }

        values[0] = Int32GetDatum(pid as i32);

        if !has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) {
            // Only superusers and roles with privileges of pg_read_all_stats
            // can see details.
            for k in 1..PG_STAT_GET_WAL_SENDERS_COLS { nulls[k] = true; }
        } else {
            values[1] = CStringGetTextDatum(WalSndGetStateString(state));

            if XLogRecPtrIsInvalid(sent_ptr) { nulls[2] = true; }
            values[2] = LSNGetDatum(sent_ptr);

            if XLogRecPtrIsInvalid(write) { nulls[3] = true; }
            values[3] = LSNGetDatum(write);

            if XLogRecPtrIsInvalid(flush) { nulls[4] = true; }
            values[4] = LSNGetDatum(flush);

            if XLogRecPtrIsInvalid(apply) { nulls[5] = true; }
            values[5] = LSNGetDatum(apply);

            // Treat a standby which always returns invalid flush location as async.
            let priority_eff = if XLogRecPtrIsInvalid(flush) { 0 } else { priority };

            if writeLag < 0 { nulls[6] = true; }
            else { values[6] = IntervalPGetDatum(offset_to_interval(writeLag)); }

            if flushLag < 0 { nulls[7] = true; }
            else { values[7] = IntervalPGetDatum(offset_to_interval(flushLag)); }

            if applyLag < 0 { nulls[8] = true; }
            else { values[8] = IntervalPGetDatum(offset_to_interval(applyLag)); }

            values[9] = Int32GetDatum(priority_eff);

            // Sync state string
            let sync_state = if priority_eff == 0 {
                b"async\0".as_ptr() as *const c_char
            } else if is_sync_standby {
                // SYNC_REP_PRIORITY check (stub)
                b"sync\0".as_ptr() as *const c_char
            } else {
                b"potential\0".as_ptr() as *const c_char
            };
            values[10] = CStringGetTextDatum(sync_state);

            if replyTime == 0 { nulls[11] = true; }
            else { values[11] = TimestampTzGetDatum(replyTime); }
        }

        // tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls) (stub)
        // tuplestore_putvalues(...)
    }

    0 as Datum // (Datum) 0
}

/// Send a keepalive message to standby.
///
/// If requestReply is set, the message requests the other party to send
/// a message back to us.  writePtr is the location up to which the WAL is
/// sent.
unsafe fn WalSndKeepalive(requestReply: bool, writePtr: XLogRecPtr) {
    // elog(DEBUG2, "sending replication keepalive")

    // construct the message...
    resetStringInfo(&mut output_message);
    pq_sendbyte(&mut output_message, b'k');
    pq_sendint64(
        &mut output_message,
        (if XLogRecPtrIsInvalid(writePtr) { sentPtr } else { writePtr }) as i64,
    );
    pq_sendint64(&mut output_message, GetCurrentTimestamp());
    pq_sendbyte(&mut output_message, if requestReply { 1 } else { 0 });

    // ... and send it wrapped in CopyData
    pq_putmessage_noblock(b'd' as c_char, output_message.data, output_message.len as usize);

    // Set local flag
    if requestReply {
        waiting_for_ping_response = true;
    }
}

/// Send keepalive message if too much time has elapsed.
unsafe fn WalSndKeepaliveIfNecessary() {
    // Don't send keepalive messages if timeouts are globally disabled or
    // we're doing something not partaking in timeouts.
    if wal_sender_timeout <= 0 || last_reply_timestamp <= 0 {
        return;
    }

    if waiting_for_ping_response {
        return;
    }

    // If half of wal_sender_timeout has lapsed without receiving any reply
    // from the standby, send a keep-alive message.
    let ping_time = TimestampTzPlusMilliseconds(
        last_reply_timestamp,
        wal_sender_timeout as i64 / 2,
    );
    if last_processing >= ping_time {
        WalSndKeepalive(true, InvalidXLogRecPtr);

        // Try to flush pending output to the client
        if pq_flush_if_writable() != 0 {
            WalSndShutdown();
        }
    }
}

/// Record the end of the WAL and the time it was flushed locally, so that
/// LagTrackerRead can compute the elapsed time (lag).
unsafe fn LagTrackerWrite(lsn: XLogRecPtr, local_flush_time: TimestampTz) {
    if !am_walsender {
        return;
    }

    // If the lsn hasn't advanced since last time, then do nothing.
    if (*lag_tracker).last_lsn == lsn {
        return;
    }
    (*lag_tracker).last_lsn = lsn;

    // If advancing the write head would crash into any of the read heads,
    // the buffer is full.  Save the overflowed read head entry.
    let new_write_head = ((*lag_tracker).write_head + 1) % LAG_TRACKER_BUFFER_SIZE as c_int;
    for i in 0..NUM_SYNC_REP_WAIT_MODE {
        if new_write_head == (*lag_tracker).read_heads[i] {
            (*lag_tracker).overflowed[i] =
                (*lag_tracker).buffer[(*lag_tracker).read_heads[i] as usize];
            (*lag_tracker).read_heads[i] = -1;
        }
    }

    // Store a sample at the current write head position.
    (*lag_tracker).buffer[(*lag_tracker).write_head as usize].lsn = lsn;
    (*lag_tracker).buffer[(*lag_tracker).write_head as usize].time = local_flush_time;
    (*lag_tracker).write_head = new_write_head;
}

/// Find out how much time has elapsed between the moment WAL location 'lsn'
/// was flushed locally and the time 'now'.
///
/// Return -1 if no new sample data is available, otherwise the elapsed time
/// in microseconds.
unsafe fn LagTrackerRead(head: usize, lsn: XLogRecPtr, now: TimestampTz) -> TimeOffset {
    let mut time: TimestampTz = 0;

    // If read head is using an overflow entry
    if (*lag_tracker).read_heads[head] == -1 {
        if (*lag_tracker).overflowed[head].lsn > lsn {
            return if now >= (*lag_tracker).overflowed[head].time {
                now - (*lag_tracker).overflowed[head].time
            } else {
                -1
            };
        }

        time = (*lag_tracker).overflowed[head].time;
        (*lag_tracker).last_read[head] = (*lag_tracker).overflowed[head];
        (*lag_tracker).read_heads[head] =
            ((*lag_tracker).write_head + 1) % LAG_TRACKER_BUFFER_SIZE as c_int;
    }

    // Read all unread samples up to this LSN or end of buffer.
    while (*lag_tracker).read_heads[head] != (*lag_tracker).write_head
        && (*lag_tracker).buffer[(*lag_tracker).read_heads[head] as usize].lsn <= lsn
    {
        time = (*lag_tracker).buffer[(*lag_tracker).read_heads[head] as usize].time;
        (*lag_tracker).last_read[head] =
            (*lag_tracker).buffer[(*lag_tracker).read_heads[head] as usize];
        (*lag_tracker).read_heads[head] =
            ((*lag_tracker).read_heads[head] + 1) % LAG_TRACKER_BUFFER_SIZE as c_int;
    }

    // If the lag tracker is empty, clear 'last_read'.
    if (*lag_tracker).read_heads[head] == (*lag_tracker).write_head {
        (*lag_tracker).last_read[head].time = 0;
    }

    if time > now {
        // If the clock somehow went backwards, treat as not found.
        return -1;
    } else if time == 0 {
        // We didn't cross a time.
        if (*lag_tracker).read_heads[head] == (*lag_tracker).write_head {
            // There are no future samples, so we can't interpolate.
            return -1;
        } else if (*lag_tracker).last_read[head].time != 0 {
            // Interpolate between last_read and the next sample.
            let prev = (*lag_tracker).last_read[head];
            let next = (*lag_tracker).buffer[(*lag_tracker).read_heads[head] as usize];

            if lsn < prev.lsn {
                // Reported LSNs shouldn't normally go backwards.
                return -1;
            }

            if prev.time > next.time {
                // If the clock somehow went backwards, treat as not found.
                return -1;
            }

            let fraction = (lsn - prev.lsn) as f64 / (next.lsn - prev.lsn) as f64;
            time = (prev.time as f64 + (next.time - prev.time) as f64 * fraction) as TimestampTz;
        } else {
            // We have only a future sample.
            time = (*lag_tracker).buffer[(*lag_tracker).read_heads[head] as usize].time;
        }
    }

    // Return the elapsed time since local flush time in microseconds.
    now - time
}
