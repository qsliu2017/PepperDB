/*-------------------------------------------------------------------------
 *
 * xlogrecovery.rs
 *      Functions for WAL recovery, standby mode
 *
 * This source file contains functions controlling WAL recovery.
 * InitWalRecovery() initializes the system for crash or archive recovery,
 * or standby mode, depending on configuration options and the state of
 * the control file and possible backup label file.  PerformWalRecovery()
 * performs the actual WAL replay, calling the rmgr-specific redo routines.
 * FinishWalRecovery() performs end-of-recovery checks and cleanup actions,
 * and prepares information needed to initialize the WAL for writes.  In
 * addition to these three main functions, there are a bunch of functions
 * for interrogating recovery state and controlling the recovery process.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xlogrecovery.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case, non_upper_case_globals, unused_variables, dead_code)]

use core::ffi::{c_char, c_int, c_void};
use core::mem::size_of;
use core::ptr::null_mut;

use crate::c::TransactionId;
use crate::postgres_ext::Oid;
use crate::access::transam::xlogdefs::{
    InvalidXLogRecPtr, TimeLineID, XLogRecPtr, XLogSegNo,
};
use crate::access::transam::xlogrecord::XLogRecord;
use crate::access::transam::xlogreader::XLogReaderState;
use crate::access::transam::xlogprefetcher::XLogPrefetcher;
use crate::access::transam::timeline::TimeLineHistoryEntry;
use crate::nodes::pg_list::List;
use crate::storage::ipc::latch::Latch;
use crate::c::int64;

pub type TimestampTz = int64;
use crate::lib::stringinfo::StringInfoData;
use crate::pg_config_manual::MAXPGPATH;

/* Unsupported old recovery command file names (relative to $PGDATA) */
pub const RECOVERY_COMMAND_FILE: &[u8] = b"recovery.conf\0";
pub const RECOVERY_COMMAND_DONE: &[u8] = b"recovery.done\0";

/* MAXFNAMELEN */
const MAXFNAMELEN: usize = 64;

// ---------------------------------------------------------------------------
// Enums and types
// ---------------------------------------------------------------------------

/// GUC support: recovery_target_action options
#[repr(C)]
pub struct config_enum_entry {
    pub name: *const c_char,
    pub val: c_int,
    pub hidden: bool,
}
unsafe impl Sync for config_enum_entry {}

/// TODO(pg-port): real RecoveryTargetType lives in access/xlogrecovery.h
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RecoveryTargetType {
    RECOVERY_TARGET_UNSET = 0,
    RECOVERY_TARGET_XID,
    RECOVERY_TARGET_TIME,
    RECOVERY_TARGET_NAME,
    RECOVERY_TARGET_LSN,
    RECOVERY_TARGET_IMMEDIATE,
}
use RecoveryTargetType::*;

/// TODO(pg-port): real RecoveryTargetAction lives in access/xlogrecovery.h
pub const RECOVERY_TARGET_ACTION_PAUSE: c_int = 0;
pub const RECOVERY_TARGET_ACTION_PROMOTE: c_int = 1;
pub const RECOVERY_TARGET_ACTION_SHUTDOWN: c_int = 2;

/// TODO(pg-port): real RecoveryTargetTimeLineGoal lives in access/xlogrecovery.h
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RecoveryTargetTimeLineGoal {
    RECOVERY_TARGET_TIMELINE_CONTROLFILE = 0,
    RECOVERY_TARGET_TIMELINE_LATEST,
    RECOVERY_TARGET_TIMELINE_NUMERIC,
}
use RecoveryTargetTimeLineGoal::*;

/// TODO(pg-port): real RecoveryPauseState lives in access/xlogrecovery.h
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RecoveryPauseState {
    RECOVERY_NOT_PAUSED = 0,
    RECOVERY_PAUSE_REQUESTED,
    RECOVERY_PAUSED,
}
use RecoveryPauseState::*;

/// TODO(pg-port): real DBState enum lives in catalog/pg_control.h
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum DBState {
    DB_STARTUP = 0,
    DB_SHUTDOWNED,
    DB_SHUTDOWNED_IN_RECOVERY,
    DB_SHUTDOWNING,
    DB_IN_CRASH_RECOVERY,
    DB_IN_ARCHIVE_RECOVERY,
    DB_IN_PRODUCTION,
}
use DBState::*;

/// Codes indicating where we got a WAL file from during recovery, or where
/// to attempt to get one.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum XLogSource {
    XLOG_FROM_ANY = 0,      /* request to read WAL from any source */
    XLOG_FROM_ARCHIVE,      /* restored using restore_command */
    XLOG_FROM_PG_WAL,       /* existing file in pg_wal */
    XLOG_FROM_STREAM,       /* streamed from primary */
}
use XLogSource::*;

/* human-readable names for XLogSources, for debugging output */
static xlogSourceNames: [&str; 4] = ["any", "archive", "pg_wal", "stream"];

/// Parameters passed down from ReadRecord to the XLogPageRead callback.
#[repr(C)]
pub struct XLogPageReadPrivate {
    pub emode: c_int,
    pub fetching_ckpt: bool,  /* are we fetching a checkpoint record? */
    pub randAccess: bool,
    pub replayTLI: TimeLineID,
}

/// Shared-memory state for WAL recovery.
#[repr(C)]
pub struct XLogRecoveryCtlData {
    /*
     * SharedHotStandbyActive indicates if we allow hot standby queries to be
     * run.  Protected by info_lck.
     */
    pub SharedHotStandbyActive: bool,

    /*
     * SharedPromoteIsTriggered indicates if a standby promotion has been
     * triggered.  Protected by info_lck.
     */
    pub SharedPromoteIsTriggered: bool,

    /*
     * recoveryWakeupLatch is used to wake up the startup process to continue
     * WAL replay, if it is waiting for WAL to arrive or promotion to be
     * requested.
     */
    pub recoveryWakeupLatch: Latch,

    /* Last record successfully replayed. */
    pub lastReplayedReadRecPtr: XLogRecPtr, /* start position */
    pub lastReplayedEndRecPtr: XLogRecPtr,  /* end+1 position */
    pub lastReplayedTLI: TimeLineID,        /* timeline */

    /*
     * When we're currently replaying a record, ie. in a redo function,
     * replayEndRecPtr points to the end+1 of the record being replayed,
     * otherwise it's equal to lastReplayedEndRecPtr.
     */
    pub replayEndRecPtr: XLogRecPtr,
    pub replayEndTLI: TimeLineID,
    /* timestamp of last COMMIT/ABORT record replayed (or being replayed) */
    pub recoveryLastXTime: TimestampTz,

    /*
     * timestamp of when we started replaying the current chunk of WAL data,
     * only relevant for replication or archive recovery
     */
    pub currentChunkStartTime: TimestampTz,
    /* Recovery pause state */
    pub recoveryPauseState: RecoveryPauseState,
    pub recoveryNotPausedCV: ConditionVariable,

    pub info_lck: slock_t, /* locks shared variables shown above */
}

/// TODO(pg-port): real CheckPoint lives in catalog/pg_control.h
#[repr(C)]
pub struct CheckPoint {
    pub redo: XLogRecPtr,
    pub ThisTimeLineID: TimeLineID,
    pub PrevTimeLineID: TimeLineID,
    pub fullPageWrites: bool,
    pub nextXid: FullTransactionId,
    pub nextOid: Oid,
    pub nextMulti: MultiXactId,
    pub nextMultiOffset: MultiXactOffset,
    pub oldestXid: TransactionId,
    pub oldestXidDB: Oid,
    pub oldestMulti: MultiXactId,
    pub oldestMultiDB: Oid,
    pub time: pg_time_t,
    pub oldestCommitTsXid: TransactionId,
    pub newestCommitTsXid: TransactionId,
    pub oldestActiveXid: TransactionId,
}

/// TODO(pg-port): real ControlFileData lives in catalog/pg_control.h
#[repr(C)]
pub struct ControlFileData {
    pub system_identifier: u64,
    pub pg_control_version: u32,
    pub catalog_version_no: u32,
    pub state: DBState,
    pub time: pg_time_t,
    pub checkPoint: XLogRecPtr,
    pub checkPointCopy: CheckPoint,
    pub unloggedLSN: XLogRecPtr,
    pub minRecoveryPoint: XLogRecPtr,
    pub minRecoveryPointTLI: TimeLineID,
    pub backupStartPoint: XLogRecPtr,
    pub backupEndPoint: XLogRecPtr,
    pub backupEndRequired: bool,
}

/// TODO(pg-port): real EndOfWalRecoveryInfo lives in access/xlogrecovery.h
#[repr(C)]
pub struct EndOfWalRecoveryInfo {
    pub lastRec: XLogRecPtr,
    pub lastRecTLI: TimeLineID,
    pub endOfLog: XLogRecPtr,
    pub endOfLogTLI: TimeLineID,
    pub lastPageBeginPtr: XLogRecPtr,
    pub lastPage: *mut c_char,
    pub abortedRecPtr: XLogRecPtr,
    pub missingContrecPtr: XLogRecPtr,
    pub recoveryStopReason: *mut c_char,
    pub standby_signal_file_found: bool,
    pub recovery_signal_file_found: bool,
}

/// TODO(pg-port): real tablespaceinfo lives in commands/tablespace.h
#[repr(C)]
pub struct tablespaceinfo {
    pub oid: Oid,
    pub path: *mut c_char,
    pub rpath: *mut c_char,
    pub size: int64,
}

// ---------------------------------------------------------------------------
// GUC variables
// ---------------------------------------------------------------------------

#[no_mangle]
pub static mut recovery_target_action_options: [config_enum_entry; 4] = [
    config_enum_entry { name: b"pause\0".as_ptr() as *const c_char, val: RECOVERY_TARGET_ACTION_PAUSE, hidden: false },
    config_enum_entry { name: b"promote\0".as_ptr() as *const c_char, val: RECOVERY_TARGET_ACTION_PROMOTE, hidden: false },
    config_enum_entry { name: b"shutdown\0".as_ptr() as *const c_char, val: RECOVERY_TARGET_ACTION_SHUTDOWN, hidden: false },
    config_enum_entry { name: null_mut(), val: 0, hidden: false },
];

/* options formerly taken from recovery.conf for archive recovery */
pub static mut recoveryRestoreCommand: *mut c_char = null_mut();
pub static mut recoveryEndCommand: *mut c_char = null_mut();
pub static mut archiveCleanupCommand: *mut c_char = null_mut();
pub static mut recoveryTarget: RecoveryTargetType = RECOVERY_TARGET_UNSET;
pub static mut recoveryTargetInclusive: bool = true;
pub static mut recoveryTargetAction: c_int = RECOVERY_TARGET_ACTION_PAUSE;
pub static mut recoveryTargetXid: TransactionId = 0;
pub static mut recovery_target_time_string: *mut c_char = null_mut();
pub static mut recoveryTargetTime: TimestampTz = 0;
pub static mut recoveryTargetName: *const c_char = null_mut();
pub static mut recoveryTargetLSN: XLogRecPtr = 0;
pub static mut recovery_min_apply_delay: c_int = 0;

/* options formerly taken from recovery.conf for XLOG streaming */
pub static mut PrimaryConnInfo: *mut c_char = null_mut();
pub static mut PrimarySlotName: *mut c_char = null_mut();
pub static mut wal_receiver_create_temp_slot: bool = false;

/*
 * recoveryTargetTimeLineGoal: what the user requested, if any
 *
 * recoveryTargetTLIRequested: numeric value of requested timeline, if constant
 *
 * recoveryTargetTLI: the currently understood target timeline; changes
 *
 * expectedTLEs: a list of TimeLineHistoryEntries for recoveryTargetTLI and
 * the timelines of its known parents, newest first (so recoveryTargetTLI is
 * always the first list member).  Only these TLIs are expected to be seen in
 * the WAL segments we read, and indeed only these TLIs will be considered as
 * candidate WAL files to open at all.
 *
 * curFileTLI: the TLI appearing in the name of the current input WAL file.
 */
pub static mut recoveryTargetTimeLineGoal: RecoveryTargetTimeLineGoal =
    RECOVERY_TARGET_TIMELINE_LATEST;
pub static mut recoveryTargetTLIRequested: TimeLineID = 0;
pub static mut recoveryTargetTLI: TimeLineID = 0;
static mut expectedTLEs: *mut List = null_mut();
static mut curFileTLI: TimeLineID = 0;

/*
 * When ArchiveRecoveryRequested is set, archive recovery was requested,
 * ie. signal files were present.  When InArchiveRecovery is set, we are
 * currently recovering using offline XLOG archives.
 */
pub static mut ArchiveRecoveryRequested: bool = false;
pub static mut InArchiveRecovery: bool = false;

/*
 * When StandbyModeRequested is set, standby mode was requested, i.e.
 * standby.signal file was present.  When StandbyMode is set, we are currently
 * in standby mode.
 */
static mut StandbyModeRequested: bool = false;
pub static mut StandbyMode: bool = false;

/* was a signal file present at startup? */
static mut standby_signal_file_found: bool = false;
static mut recovery_signal_file_found: bool = false;

/*
 * CheckPointLoc is the position of the checkpoint record that determines
 * where to start the replay.  It comes from the backup label file or the
 * control file.
 */
static mut CheckPointLoc: XLogRecPtr = InvalidXLogRecPtr;
static mut CheckPointTLI: TimeLineID = 0;
static mut RedoStartLSN: XLogRecPtr = InvalidXLogRecPtr;
static mut RedoStartTLI: TimeLineID = 0;

/*
 * Local copy of SharedHotStandbyActive variable. False actually means "not
 * known, need to check the shared state".
 */
static mut LocalHotStandbyActive: bool = false;

/*
 * Local copy of SharedPromoteIsTriggered variable. False actually means "not
 * known, need to check the shared state".
 */
static mut LocalPromoteIsTriggered: bool = false;

/* Has the recovery code requested a walreceiver wakeup? */
static mut doRequestWalReceiverReply: bool = false;

/* XLogReader object used to parse the WAL records */
static mut xlogreader: *mut XLogReaderState = null_mut();

/* XLogPrefetcher object used to consume WAL records with read-ahead */
static mut xlogprefetcher: *mut XLogPrefetcher = null_mut();

/* flag to tell XLogPageRead that we have started replaying */
static mut InRedo: bool = false;

/*
 * readFile is -1 or a kernel FD for the log file segment that's currently
 * open for reading.  readSegNo identifies the segment.  readOff is the offset
 * of the page just read, readLen indicates how much of it has been read into
 * readBuf, and readSource indicates where we got the currently open file from.
 */
static mut readFile: c_int = -1;
static mut readSegNo: XLogSegNo = 0;
static mut readOff: u32 = 0;
static mut readLen: u32 = 0;
static mut readSource: XLogSource = XLOG_FROM_ANY;

/*
 * Keeps track of which source we're currently reading from.
 */
static mut currentSource: XLogSource = XLOG_FROM_ANY;
static mut lastSourceFailed: bool = false;
static mut pendingWalRcvRestart: bool = false;

/*
 * These variables track when we last obtained some WAL data to process,
 * and where we got it from.
 */
static mut XLogReceiptTime: TimestampTz = 0;
static mut XLogReceiptSource: XLogSource = XLOG_FROM_ANY;

/* Local copy of WalRcv->flushedUpto */
static mut flushedUpto: XLogRecPtr = 0;
static mut receiveTLI: TimeLineID = 0;

/*
 * Copy of minRecoveryPoint and backupEndPoint from the control file.
 */
static mut minRecoveryPoint: XLogRecPtr = 0;
static mut minRecoveryPointTLI: TimeLineID = 0;

static mut backupStartPoint: XLogRecPtr = 0;
static mut backupEndPoint: XLogRecPtr = 0;
static mut backupEndRequired: bool = false;

/*
 * Have we reached a consistent database state?
 */
#[no_mangle]
pub static mut reachedConsistency: bool = false;

/* Buffers dedicated to consistency checks of size BLCKSZ */
static mut replay_image_masked: *mut c_char = null_mut();
static mut primary_image_masked: *mut c_char = null_mut();

static mut XLogRecoveryCtl: *mut XLogRecoveryCtlData = null_mut();

/*
 * abortedRecPtr is the start pointer of a broken record at end of WAL when
 * recovery completes; missingContrecPtr is the location of the first
 * contrecord that went missing.
 */
static mut abortedRecPtr: XLogRecPtr = 0;
static mut missingContrecPtr: XLogRecPtr = 0;

/*
 * if recoveryStopsBefore/After returns true, it saves information of the stop
 * point here
 */
static mut recoveryStopXid: TransactionId = 0;
static mut recoveryStopTime: TimestampTz = 0;
static mut recoveryStopLSN: XLogRecPtr = 0;
static mut recoveryStopName: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
static mut recoveryStopAfter: bool = false;

// ---------------------------------------------------------------------------
// Stub type aliases for not-yet-ported types
// ---------------------------------------------------------------------------

/// TODO(pg-port): slock_t lives in storage/spin.h
pub type slock_t = u32;
/// TODO(pg-port): ConditionVariable lives in storage/condition_variable.h
pub type ConditionVariable = u64;
/// TODO(pg-port): FullTransactionId lives in access/transam.h
pub type FullTransactionId = u64;
/// TODO(pg-port): MultiXactId lives in access/multixact.h
pub type MultiXactId = u32;
/// TODO(pg-port): MultiXactOffset lives in access/multixact.h
pub type MultiXactOffset = u32;
/// TODO(pg-port): pg_time_t lives in pgtime.h
pub type pg_time_t = i64;
/// TODO(pg-port): Size lives in c.h
pub type Size = usize;
/// TODO(pg-port): GucSource lives in utils/guc.h
pub type GucSource = c_int;

// ---------------------------------------------------------------------------
// Stub functions for unported dependencies
// ---------------------------------------------------------------------------

/// TODO(pg-port): ShmemInitStruct lives in storage/shmem.h
unsafe fn ShmemInitStruct(_name: &str, _size: Size, _found: *mut bool) -> *mut c_void {
    let cname = std::ffi::CString::new(_name).unwrap();
    crate::storage::ipc::shmem::ShmemInitStruct(cname.as_ptr(), _size, _found)
}
/// TODO(pg-port): SpinLockInit lives in storage/spin.h
unsafe fn SpinLockInit(_lock: *mut slock_t) {
    crate::storage::spin::SpinLockInit(_lock as _)
}
/// TODO(pg-port): SpinLockAcquire lives in storage/spin.h
unsafe fn SpinLockAcquire(_lock: *mut slock_t) {
    crate::storage::spin::SpinLockAcquire(_lock as _)
}
/// TODO(pg-port): SpinLockRelease lives in storage/spin.h
unsafe fn SpinLockRelease(_lock: *mut slock_t) {
    crate::storage::spin::SpinLockRelease(_lock as _)
}
/// TODO(pg-port): InitSharedLatch lives in storage/latch.h
unsafe fn InitSharedLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::InitSharedLatch(_latch as _)
}
/// TODO(pg-port): OwnLatch lives in storage/latch.h
unsafe fn OwnLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::OwnLatch(_latch as _)
}
/// TODO(pg-port): DisownLatch lives in storage/latch.h
unsafe fn DisownLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::DisownLatch(_latch as _)
}
/// TODO(pg-port): SetLatch lives in storage/latch.h
unsafe fn SetLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::SetLatch(_latch as _)
}
/// TODO(pg-port): ResetLatch lives in storage/latch.h
unsafe fn ResetLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::ResetLatch(_latch as _)
}
/// TODO(pg-port): WaitLatch lives in storage/latch.h
unsafe fn WaitLatch(_latch: *mut Latch, _wakeEvents: c_int, _timeout: i64, _wait_event: u32) -> c_int {
    unimplemented!("TODO(pg-port): WaitLatch")
}
/// TODO(pg-port): ConditionVariableInit lives in storage/condition_variable.h
unsafe fn ConditionVariableInit(_cv: *mut ConditionVariable) {
    crate::storage::lmgr::condition_variable::ConditionVariableInit(_cv as _)
}
/// TODO(pg-port): ConditionVariableTimedSleep lives in storage/condition_variable.h
unsafe fn ConditionVariableTimedSleep(_cv: *mut ConditionVariable, _timeout: c_int, _wait_event: u32) {
    unimplemented!("TODO(pg-port): ConditionVariableTimedSleep")
}
/// TODO(pg-port): ConditionVariableCancelSleep lives in storage/condition_variable.h
unsafe fn ConditionVariableCancelSleep() {
    unimplemented!("TODO(pg-port): ConditionVariableCancelSleep")
}
/// TODO(pg-port): ConditionVariableBroadcast lives in storage/condition_variable.h
unsafe fn ConditionVariableBroadcast(_cv: *mut ConditionVariable) { crate::storage::lmgr::condition_variable::ConditionVariableBroadcast(_cv as _) }
/// TODO(pg-port): palloc lives in utils/palloc.h
unsafe fn palloc(_size: Size) -> *mut c_void {
    crate::utils::palloc::palloc(_size)
}
/// TODO(pg-port): palloc0 lives in utils/palloc.h
unsafe fn palloc0(_size: Size) -> *mut c_void {
    crate::utils::palloc::palloc0(_size)
}
/// TODO(pg-port): pfree lives in utils/palloc.h
unsafe fn pfree(_ptr: *mut c_void) {
    crate::utils::palloc::pfree(_ptr)
}
/// TODO(pg-port): pstrdup lives in utils/palloc.h
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char {
    crate::utils::palloc::pstrdup(_s)
}
/// TODO(pg-port): psprintf lives in utils/elog.h
unsafe fn psprintf(_fmt: *const c_char) -> *mut c_char {
    unimplemented!("TODO(pg-port): psprintf")
}
/// TODO(pg-port): lappend lives in nodes/list.c
unsafe fn lappend(_list: *mut List, _datum: *mut c_void) -> *mut List {
    unimplemented!("TODO(pg-port): lappend")
}
/// TODO(pg-port): list_free_deep lives in nodes/list.c
unsafe fn list_free_deep(_list: *mut List) {
    unimplemented!("TODO(pg-port): list_free_deep")
}
/// TODO(pg-port): lfirst lives in nodes/pg_list.h
unsafe fn lfirst(_lc: *mut c_void) -> *mut c_void {
    unimplemented!("TODO(pg-port): lfirst")
}
/// TODO(pg-port): XLogReaderAllocate lives in access/xlogreader.h
unsafe fn XLogReaderAllocate(
    _wal_segment_size: c_int,
    _waldir: *const c_char,
    _routine: XLogReaderRoutine,
    _private_data: *mut c_void,
) -> *mut XLogReaderState {
    crate::access::transam::xlogreader::XLogReaderAllocate(_wal_segment_size, _waldir, &_routine, _private_data)
}
/// TODO(pg-port): XLogReaderFree lives in access/xlogreader.h
unsafe fn XLogReaderFree(_xlogreader: *mut XLogReaderState) {
    crate::access::transam::xlogreader::XLogReaderFree(_xlogreader)
}
/// TODO(pg-port): XLogReaderSetDecodeBuffer lives in access/xlogreader.h
unsafe fn XLogReaderSetDecodeBuffer(_xlogreader: *mut XLogReaderState, _buf: *mut c_void, _size: Size) {
    crate::access::transam::xlogreader::XLogReaderSetDecodeBuffer(_xlogreader, _buf, _size)
}
/// TODO(pg-port): XLogReaderValidatePageHeader lives in access/xlogreader.h
unsafe fn XLogReaderValidatePageHeader(_xlogreader: *mut XLogReaderState, _recptr: XLogRecPtr, _buf: *mut c_char) -> bool {
    crate::access::transam::xlogreader::XLogReaderValidatePageHeader(_xlogreader, _recptr, _buf)
}
/// TODO(pg-port): XLogReaderResetError lives in access/xlogreader.h
unsafe fn XLogReaderResetError(_xlogreader: *mut XLogReaderState) {
    crate::access::transam::xlogreader::XLogReaderResetError(_xlogreader)
}
/// TODO(pg-port): XLogPrefetcherAllocate lives in access/xlogprefetcher.h
unsafe fn XLogPrefetcherAllocate(_xlogreader: *mut XLogReaderState) -> *mut XLogPrefetcher {
    crate::access::transam::xlogprefetcher::XLogPrefetcherAllocate(_xlogreader)
}
/// TODO(pg-port): XLogPrefetcherFree lives in access/xlogprefetcher.h
unsafe fn XLogPrefetcherFree(_xlogprefetcher: *mut XLogPrefetcher) {
    crate::access::transam::xlogprefetcher::XLogPrefetcherFree(_xlogprefetcher)
}
/// TODO(pg-port): XLogPrefetcherBeginRead lives in access/xlogprefetcher.h
unsafe fn XLogPrefetcherBeginRead(_xlogprefetcher: *mut XLogPrefetcher, _recptr: XLogRecPtr) {
    crate::access::transam::xlogprefetcher::XLogPrefetcherBeginRead(_xlogprefetcher, _recptr)
}
/// TODO(pg-port): XLogPrefetcherReadRecord lives in access/xlogprefetcher.h
unsafe fn XLogPrefetcherReadRecord(_xlogprefetcher: *mut XLogPrefetcher, _errormsg: *mut *mut c_char) -> *mut XLogRecord {
    crate::access::transam::xlogprefetcher::XLogPrefetcherReadRecord(_xlogprefetcher, _errormsg) as *mut XLogRecord
}
/// TODO(pg-port): XLogPrefetcherGetReader lives in access/xlogprefetcher.h
unsafe fn XLogPrefetcherGetReader(_xlogprefetcher: *mut XLogPrefetcher) -> *mut XLogReaderState {
    crate::access::transam::xlogprefetcher::XLogPrefetcherGetReader(_xlogprefetcher)
}
/// TODO(pg-port): XLogPrefetcherComputeStats lives in access/xlogprefetcher.h
unsafe fn XLogPrefetcherComputeStats(_xlogprefetcher: *mut XLogPrefetcher) {
    crate::access::transam::xlogprefetcher::XLogPrefetcherComputeStats(_xlogprefetcher)
}
/// TODO(pg-port): XLogPrefetchReconfigure lives in access/xlogprefetcher.h
unsafe fn XLogPrefetchReconfigure() {
    crate::access::transam::xlogprefetcher::XLogPrefetchReconfigure()
}
/// TODO(pg-port): XLogRecGetData lives in access/xlogreader.h
unsafe fn XLogRecGetData(_record: *mut XLogReaderState) -> *mut c_void {
    crate::access::transam::xlogreader::XLogRecGetData(_record) as *mut c_void
}
/// TODO(pg-port): XLogRecGetInfo lives in access/xlogreader.h
unsafe fn XLogRecGetInfo(_record: *mut XLogReaderState) -> u8 {
    crate::access::transam::xlogreader::XLogRecGetInfo(_record)
}
/// TODO(pg-port): XLogRecGetRmid lives in access/xlogreader.h
unsafe fn XLogRecGetRmid(_record: *mut XLogReaderState) -> u8 {
    crate::access::transam::xlogreader::XLogRecGetRmid(_record)
}
/// TODO(pg-port): XLogRecGetXid lives in access/xlogreader.h
unsafe fn XLogRecGetXid(_record: *mut XLogReaderState) -> TransactionId {
    crate::access::transam::xlogreader::XLogRecGetXid(_record)
}
/// TODO(pg-port): XLogRecGetPrev lives in access/xlogreader.h
unsafe fn XLogRecGetPrev(_record: *mut XLogReaderState) -> XLogRecPtr {
    crate::access::transam::xlogreader::XLogRecGetPrev(_record)
}
/// TODO(pg-port): XLogRecGetDataLen lives in access/xlogreader.h
unsafe fn XLogRecGetDataLen(_record: *mut XLogReaderState) -> u32 {
    crate::access::transam::xlogreader::XLogRecGetDataLen(_record)
}
/// TODO(pg-port): XLogRecMaxBlockId lives in access/xlogreader.h
unsafe fn XLogRecMaxBlockId(_record: *mut XLogReaderState) -> c_int {
    crate::access::transam::xlogreader::XLogRecMaxBlockId(_record)
}
/// TODO(pg-port): XLogRecHasAnyBlockRefs lives in access/xlogreader.h
unsafe fn XLogRecHasAnyBlockRefs(_record: *mut XLogReaderState) -> bool {
    crate::access::transam::xlogreader::XLogRecHasAnyBlockRefs(_record)
}
/// TODO(pg-port): XLogRecHasBlockImage lives in access/xlogreader.h
unsafe fn XLogRecHasBlockImage(_record: *mut XLogReaderState, _id: c_int) -> bool {
    crate::access::transam::xlogreader::XLogRecHasBlockImage(_record, _id as u8)
}
/// TODO(pg-port): XLogRecBlockImageApply lives in access/xlogreader.h
unsafe fn XLogRecBlockImageApply(_record: *mut XLogReaderState, _id: c_int) -> bool {
    crate::access::transam::xlogreader::XLogRecBlockImageApply(_record, _id as u8)
}
/// TODO(pg-port): XLogRecGetBlockTagExtended lives in access/xlogreader.h
unsafe fn XLogRecGetBlockTagExtended(
    _record: *mut XLogReaderState,
    _id: c_int,
    _rlocator: *mut RelFileLocator,
    _forknum: *mut c_int,
    _blknum: *mut u32,
    _lsn: *mut XLogRecPtr,
) -> bool {
    unimplemented!("TODO(pg-port): XLogRecGetBlockTagExtended")
}
/// TODO(pg-port): RestoreBlockImage lives in access/xlogreader.h
unsafe fn RestoreBlockImage(_record: *mut XLogReaderState, _id: c_int, _page: *mut c_char) -> bool {
    unimplemented!("TODO(pg-port): RestoreBlockImage")
}

/// TODO(pg-port): RelFileLocator lives in storage/relfilelocator.h
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: Oid,
}

// XLogReaderRoutine: use the canonical definition from access/xlogreader.
use crate::access::transam::xlogreader::XLogReaderRoutine;

/// TODO(pg-port): XLogRecPtr validity check - XRecOffIsValid
unsafe fn XRecOffIsValid(_recptr: XLogRecPtr) -> bool {
    crate::access::transam::xlog_internal::XRecOffIsValid(_recptr)
}
/// TODO(pg-port): XLogRecPtrIsInvalid macro
#[inline]
fn XLogRecPtrIsInvalid(r: XLogRecPtr) -> bool {
    r == InvalidXLogRecPtr
}
/// TODO(pg-port): LSN_FORMAT_ARGS - expands to hi, lo pair
#[inline]
fn lsn_hi(lsn: XLogRecPtr) -> u32 { (lsn >> 32) as u32 }
#[inline]
fn lsn_lo(lsn: XLogRecPtr) -> u32 { lsn as u32 }

// ---------------------------------------------------------------------------
// More stubs for timeline/archive/wal functions
// ---------------------------------------------------------------------------

/// TODO(pg-port): tliOfPointInHistory lives in access/timeline.c
unsafe fn tliOfPointInHistory(_point: XLogRecPtr, _history: *mut List) -> TimeLineID {
    crate::access::transam::timeline::tliOfPointInHistory(_point, _history)
}
/// TODO(pg-port): tliSwitchPoint lives in access/timeline.c
unsafe fn tliSwitchPoint(_tli: TimeLineID, _history: *mut List, _nextTLI: *mut TimeLineID) -> XLogRecPtr {
    crate::access::transam::timeline::tliSwitchPoint(_tli, _history, _nextTLI)
}
/// TODO(pg-port): tliInHistory lives in access/timeline.c
unsafe fn tliInHistory(_tli: TimeLineID, _history: *mut List) -> bool {
    crate::access::transam::timeline::tliInHistory(_tli, _history)
}
/// TODO(pg-port): readTimeLineHistory lives in access/timeline.c
unsafe fn readTimeLineHistory(_targetTLI: TimeLineID) -> *mut List {
    crate::access::transam::timeline::readTimeLineHistory(_targetTLI)
}
/// TODO(pg-port): findNewestTimeLine lives in access/timeline.c
unsafe fn findNewestTimeLine(_startTLI: TimeLineID) -> TimeLineID {
    crate::access::transam::timeline::findNewestTimeLine(_startTLI)
}
/// TODO(pg-port): existsTimeLineHistory lives in access/timeline.c
unsafe fn existsTimeLineHistory(_tli: TimeLineID) -> bool {
    crate::access::transam::timeline::existsTimeLineHistory(_tli)
}
/// TODO(pg-port): restoreTimeLineHistoryFiles lives in access/timeline.c
unsafe fn restoreTimeLineHistoryFiles(_from: TimeLineID, _to: TimeLineID) {
    crate::access::transam::timeline::restoreTimeLineHistoryFiles(_from, _to)
}
/// TODO(pg-port): RestoreArchivedFile lives in access/xlogarchive.c
unsafe fn RestoreArchivedFile(
    _path: *mut c_char,
    _xlogfname: *const c_char,
    _recoveryxlog: *const c_char,
    _wal_segment_size: c_int,
    _inRedo: bool,
) -> bool {
    crate::access::transam::xlogarchive::RestoreArchivedFile(_path, _xlogfname, _recoveryxlog, _wal_segment_size as _, _inRedo)
}
/// TODO(pg-port): KeepFileRestoredFromArchive lives in access/xlogarchive.c
unsafe fn KeepFileRestoredFromArchive(_path: *const c_char, _xlogfname: *const c_char) {
    crate::access::transam::xlogarchive::KeepFileRestoredFromArchive(_path, _xlogfname)
}
/// TODO(pg-port): XLogShutdownWalRcv lives in access/xlogarchive.c
unsafe fn XLogShutdownWalRcv() {
    crate::access::transam::xlog::XLogShutdownWalRcv()
}
/// TODO(pg-port): SwitchIntoArchiveRecovery lives in access/xlogarchive.c
unsafe fn SwitchIntoArchiveRecovery(_endRecPtr: XLogRecPtr, _replayTLI: TimeLineID) {
    crate::access::transam::xlog::SwitchIntoArchiveRecovery(_endRecPtr, _replayTLI)
}
/// TODO(pg-port): wal_segment_close lives in access/xlog.c
unsafe fn wal_segment_close(state: *mut XLogReaderState) {
    crate::access::transam::xlogutils::wal_segment_close(state as _)
}
/// TODO(pg-port): XLByteToSeg - macro/inline in access/xlog_internal.h
#[inline]
unsafe fn XLByteToSeg(xlrp: XLogRecPtr, logSegNo: &mut XLogSegNo, wal_segsz_bytes: c_int) {
    *logSegNo = xlrp / wal_segsz_bytes as u64;
}
/// TODO(pg-port): XLByteInSeg - macro/inline in access/xlog_internal.h
#[inline]
unsafe fn XLByteInSeg(xlrp: XLogRecPtr, logSegNo: XLogSegNo, wal_segsz_bytes: c_int) -> bool {
    (xlrp / wal_segsz_bytes as u64) == logSegNo
}
/// TODO(pg-port): XLogSegmentOffset - macro in access/xlog_internal.h
#[inline]
unsafe fn XLogSegmentOffset(lsn: XLogRecPtr, wal_segsz_bytes: c_int) -> u32 {
    (lsn % wal_segsz_bytes as u64) as u32
}
/// TODO(pg-port): XLogFileName - macro/fn in access/xlog_internal.h
unsafe fn XLogFileName(_buf: *mut c_char, _tli: TimeLineID, _segno: XLogSegNo, _wal_segment_size: c_int) {
    crate::access::transam::xlog_internal::XLogFileName(_buf, _tli, _segno, _wal_segment_size)
}
/// TODO(pg-port): XLogFilePath - macro/fn in access/xlog_internal.h
unsafe fn XLogFilePath(_buf: *mut c_char, _tli: TimeLineID, _segno: XLogSegNo, _wal_segment_size: c_int) {
    crate::access::transam::xlog_internal::XLogFilePath(_buf, _tli, _segno, _wal_segment_size)
}
/// TODO(pg-port): XLogCheckpointNeeded lives in access/xlog.c
unsafe fn XLogCheckpointNeeded(_segno: XLogSegNo) -> bool {
    crate::access::transam::xlog::XLogCheckpointNeeded(_segno)
}
/// TODO(pg-port): GetRedoRecPtr lives in access/xlog.c
unsafe fn GetRedoRecPtr() -> XLogRecPtr {
    crate::access::transam::xlog::GetRedoRecPtr()
}
/// TODO(pg-port): RequestCheckpoint lives in postmaster/bgwriter.c
unsafe fn RequestCheckpoint(_flags: c_int) {
    crate::postmaster::checkpointer::RequestCheckpoint(_flags)
}
/// TODO(pg-port): BasicOpenFile lives in storage/fd.c
unsafe fn BasicOpenFile(_path: *const c_char, _flags: c_int) -> c_int {
    crate::storage::file::fd::BasicOpenFile(_path, _flags)
}
/// TODO(pg-port): BasicOpenFilePerm lives in storage/fd.c
unsafe fn BasicOpenFilePerm(_path: *const c_char, _flags: c_int, _mode: u32) -> c_int {
    crate::storage::file::fd::BasicOpenFilePerm(_path, _flags, _mode as _)
}
/// TODO(pg-port): AllocateFile lives in storage/fd.c
unsafe fn AllocateFile(_path: *const c_char, _mode: *const c_char) -> *mut c_void {
    crate::storage::file::fd::AllocateFile(_path, _mode)
}
/// TODO(pg-port): FreeFile lives in storage/fd.c
unsafe fn FreeFile(_file: *mut c_void) -> c_int {
    crate::storage::file::fd::FreeFile(_file)
}
/// TODO(pg-port): AllocateDir lives in storage/fd.c
unsafe fn AllocateDir(_path: *const c_char) -> *mut c_void {
    unimplemented!("TODO(pg-port): AllocateDir")
}
/// TODO(pg-port): ReadDir lives in storage/fd.c
unsafe fn ReadDir(_dir: *mut c_void, _path: *const c_char) -> *mut dirent {
    unimplemented!("TODO(pg-port): ReadDir")
}
/// TODO(pg-port): dirent lives in sys/dirent.h
#[repr(C)]
pub struct dirent {
    pub d_name: [c_char; 256],
}
/// TODO(pg-port): durable_rename lives in storage/fd.c
unsafe fn durable_rename(_oldpath: *const c_char, _newpath: *const c_char, _elevel: c_int) -> c_int {
    crate::storage::file::fd::durable_rename(_oldpath, _newpath, _elevel)
}
/// TODO(pg-port): pg_fsync lives in storage/fd.c
unsafe fn pg_fsync(_fd: c_int) -> c_int {
    crate::storage::file::fd::pg_fsync(_fd)
}
/// TODO(pg-port): pg_pread lives in port.h
unsafe fn pg_pread(_fd: c_int, _buf: *mut c_void, _size: usize, _offset: i64) -> isize {
    libc::pread(_fd, _buf, _size, _offset)
}
/// TODO(pg-port): get_dirent_type lives in common/file_utils.h
unsafe fn get_dirent_type(_path: *const c_char, _de: *mut dirent, _look_through_symlinks: bool, _elevel: c_int) -> c_int {
    unimplemented!("TODO(pg-port): get_dirent_type")
}
/// TODO(pg-port): remove_tablespace_symlink lives in commands/tablespace.c
unsafe fn remove_tablespace_symlink(_linkloc: *const c_char) { crate::commands::tablespace::remove_tablespace_symlink(_linkloc as _) }
/// TODO(pg-port): IsBootstrapProcessingMode lives in miscadmin.h
unsafe fn IsBootstrapProcessingMode() -> bool {
    crate::miscadmin::IsBootstrapProcessingMode()
}
/// TODO(pg-port): IsUnderPostmaster lives in miscadmin.h
pub static mut IsUnderPostmaster: bool = false;
/// TODO(pg-port): IsPostmasterEnvironment lives in miscadmin.h
pub static mut IsPostmasterEnvironment: bool = false;
/// TODO(pg-port): DataDir lives in miscadmin.h
pub static mut DataDir: *mut c_char = null_mut();
/// TODO(pg-port): wal_segment_size lives in access/xlog.c
pub static mut wal_segment_size: c_int = 16 * 1024 * 1024;
/// TODO(pg-port): wal_decode_buffer_size lives in access/xlog.c
pub static mut wal_decode_buffer_size: usize = 0;
/// TODO(pg-port): InRecovery lives in access/xlog.c
pub static mut InRecovery: bool = false;
/// TODO(pg-port): standbyState lives in storage/proc.c
pub static mut standbyState: c_int = 0;
/// TODO(pg-port): STANDBY_INITIALIZED
pub const STANDBY_INITIALIZED: c_int = 1;
/// TODO(pg-port): STANDBY_SNAPSHOT_READY
pub const STANDBY_SNAPSHOT_READY: c_int = 3;
/// TODO(pg-port): EnableHotStandby lives in access/xlog.c
pub static mut EnableHotStandby: bool = false;
/// TODO(pg-port): allow_in_place_tablespaces lives in commands/tablespace.c
pub static mut allow_in_place_tablespaces: bool = false;
/// TODO(pg-port): track_wal_io_timing lives in utils/guc.c
pub static mut track_wal_io_timing: bool = false;
/// TODO(pg-port): wal_retrieve_retry_interval lives in access/xlog.c
pub static mut wal_retrieve_retry_interval: c_int = 5000;

/// TODO(pg-port): disable_startup_progress_timeout lives in postmaster/startup.c
unsafe fn disable_startup_progress_timeout() {
    crate::postmaster::startup::disable_startup_progress_timeout()
}
/// TODO(pg-port): begin_startup_progress_phase lives in postmaster/startup.c
unsafe fn begin_startup_progress_phase() {
    crate::postmaster::startup::begin_startup_progress_phase()
}
/// TODO(pg-port): SendPostmasterSignal lives in storage/pmsignal.h
unsafe fn SendPostmasterSignal(_signal: c_int) { crate::storage::ipc::pmsignal::SendPostmasterSignal(_signal as _) }
/// TODO(pg-port): PMSIGNAL_RECOVERY_STARTED etc
pub const PMSIGNAL_RECOVERY_STARTED: c_int = 5;
pub const PMSIGNAL_RECOVERY_CONSISTENT: c_int = 6;
pub const PMSIGNAL_BEGIN_HOT_STANDBY: c_int = 7;
/// TODO(pg-port): ProcessStartupProcInterrupts lives in postmaster/startup.c
unsafe fn ProcessStartupProcInterrupts() {
    crate::postmaster::startup::ProcessStartupProcInterrupts()
}
/// TODO(pg-port): RmgrStartup lives in access/rmgr.c
unsafe fn RmgrStartup() {
    unimplemented!("TODO(pg-port): RmgrStartup")
}
/// TODO(pg-port): RmgrCleanup lives in access/rmgr.c
unsafe fn RmgrCleanup() {
    unimplemented!("TODO(pg-port): RmgrCleanup")
}
/// TODO(pg-port): GetRmgr lives in access/rmgr.c
unsafe fn GetRmgr(_rmid: u8) -> RmgrData { unimplemented!() }
/// TODO(pg-port): RmgrData lives in access/rmgr.h
#[repr(C)]
pub struct RmgrData {
    pub rm_name: *const c_char,
    pub rm_redo: unsafe fn(*mut XLogReaderState),
    pub rm_desc: unsafe fn(*mut StringInfoData, *mut XLogReaderState),
    pub rm_identify: unsafe fn(u8) -> *const c_char,
    pub rm_mask: Option<unsafe fn(*mut c_char, u32)>,
}
/// TODO(pg-port): AdvanceNextFullTransactionIdPastXid lives in access/transam/varsup.c
unsafe fn AdvanceNextFullTransactionIdPastXid(_xid: TransactionId) {
    crate::access::transam::varsup::AdvanceNextFullTransactionIdPastXid(_xid)
}
/// TODO(pg-port): RecordKnownAssignedTransactionIds lives in storage/procarray.c
unsafe fn RecordKnownAssignedTransactionIds(_xid: TransactionId) {
    crate::storage::ipc::procarray::RecordKnownAssignedTransactionIds(_xid)
}
/// TODO(pg-port): KnownAssignedTransactionIdsIdleMaintenance lives in storage/procarray.c
unsafe fn KnownAssignedTransactionIdsIdleMaintenance() {
    crate::storage::ipc::procarray::KnownAssignedTransactionIdsIdleMaintenance()
}
/// TODO(pg-port): AllowCascadeReplication lives in replication/walsender.c
unsafe fn AllowCascadeReplication() -> bool {
    unimplemented!("TODO(pg-port): AllowCascadeReplication")
}
/// TODO(pg-port): WalSndWakeup lives in replication/walsender.c
unsafe fn WalSndWakeup(_tliswitch: bool, _logical: bool) {
    crate::replication::walsender::WalSndWakeup(_tliswitch, _logical)
}
/// TODO(pg-port): WalRcvForceReply lives in replication/walreceiver.c
unsafe fn WalRcvForceReply() {
    crate::replication::walreceiver::WalRcvForceReply()
}
/// TODO(pg-port): WalRcvRunning lives in replication/walreceiver.c
unsafe fn WalRcvRunning() -> bool {
    crate::replication::walreceiverfuncs::WalRcvRunning()
}
/// TODO(pg-port): WalRcvStreaming lives in replication/walreceiver.c
unsafe fn WalRcvStreaming() -> bool {
    crate::replication::walreceiverfuncs::WalRcvStreaming()
}
/// TODO(pg-port): GetWalRcvFlushRecPtr lives in replication/walreceiver.c
unsafe fn GetWalRcvFlushRecPtr(_latestChunkStart: *mut XLogRecPtr, _tli: *mut TimeLineID) -> XLogRecPtr {
    unimplemented!("TODO(pg-port): GetWalRcvFlushRecPtr")
}
/// TODO(pg-port): RequestXLogStreaming lives in replication/walreceiver.c
unsafe fn RequestXLogStreaming(_tli: TimeLineID, _ptr: XLogRecPtr, _conninfo: *const c_char, _slotname: *const c_char, _create_temp_slot: bool) {
    unimplemented!("TODO(pg-port): RequestXLogStreaming")
}
/// TODO(pg-port): SetInstallXLogFileSegmentActive lives in replication/walreceiver.c
unsafe fn SetInstallXLogFileSegmentActive() { crate::access::transam::xlog::SetInstallXLogFileSegmentActive() }
/// TODO(pg-port): ResetInstallXLogFileSegmentActive lives in replication/walreceiver.c
unsafe fn ResetInstallXLogFileSegmentActive() { crate::access::transam::xlog::ResetInstallXLogFileSegmentActive() }
/// TODO(pg-port): IsInstallXLogFileSegmentActive lives in replication/walreceiver.c
unsafe fn IsInstallXLogFileSegmentActive() -> bool { crate::access::transam::xlog::IsInstallXLogFileSegmentActive() }
/// TODO(pg-port): ShutDownSlotSync lives in replication/slotsync.c
unsafe fn ShutDownSlotSync() {
    // No slot-sync worker runs on a primary during bring-up; nothing to stop.
}
/// TODO(pg-port): ReachedEndOfBackup lives in access/xlog.c
unsafe fn ReachedEndOfBackup(_endRecPtr: XLogRecPtr, _tli: TimeLineID) { crate::access::transam::xlog::ReachedEndOfBackup(_endRecPtr as _, _tli as _) }
/// TODO(pg-port): RemoveNonParentXlogFiles lives in access/xlog.c
unsafe fn RemoveNonParentXlogFiles(_switchpoint: XLogRecPtr, _newTLI: TimeLineID) { crate::access::transam::xlog::RemoveNonParentXlogFiles(_switchpoint as _, _newTLI as _) }
/// TODO(pg-port): XLogCheckInvalidPages lives in access/xlog.c
unsafe fn XLogCheckInvalidPages() { crate::access::transam::xlogutils::XLogCheckInvalidPages() }
/// TODO(pg-port): XReadBufferExtended lives in access/xlogutils.c
unsafe fn XLogReadBufferExtended(
    _rlocator: RelFileLocator,
    _forknum: c_int,
    _blkno: u32,
    _mode: c_int,
    _strategy: c_int,
) -> c_int {
    unimplemented!("TODO(pg-port): XLogReadBufferExtended")
}
/// TODO(pg-port): BufferGetPage lives in storage/buffer/bufmgr.c
unsafe fn BufferGetPage(_buf: c_int) -> *mut c_void {
    unimplemented!("TODO(pg-port): BufferGetPage")
}
/// TODO(pg-port): PageGetLSN lives in storage/bufpage.h
unsafe fn PageGetLSN(_page: *mut c_void) -> XLogRecPtr { crate::storage::bufpage::PageGetLSN(_page as _) }
/// TODO(pg-port): LockBuffer lives in storage/buffer/bufmgr.c
unsafe fn LockBuffer(_buf: c_int, _mode: c_int) {
    unimplemented!("TODO(pg-port): LockBuffer")
}
/// TODO(pg-port): UnlockReleaseBuffer lives in storage/buffer/bufmgr.c
unsafe fn UnlockReleaseBuffer(_buf: c_int) {
    unimplemented!("TODO(pg-port): UnlockReleaseBuffer")
}
/// TODO(pg-port): BufferIsValid lives in storage/buf.h
unsafe fn BufferIsValid(_buf: c_int) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buf as _) }
/// TODO(pg-port): AmStartupProcess lives in miscadmin.h
unsafe fn AmStartupProcess() -> bool {
    crate::miscadmin::AmStartupProcess()
}
/// TODO(pg-port): GetCurrentTimestamp lives in utils/timestamp.c
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    crate::utils::adt::timestamp::GetCurrentTimestamp()
}
/// TODO(pg-port): timestamptz_to_str lives in utils/timestamp.c
unsafe fn timestamptz_to_str(_t: TimestampTz) -> *const c_char {
    crate::utils::adt::timestamp::timestamptz_to_str(_t)
}
/// TODO(pg-port): TimestampDifferenceExceeds lives in utils/timestamp.c
unsafe fn TimestampDifferenceExceeds(_t1: TimestampTz, _t2: TimestampTz, _msec: c_int) -> bool {
    crate::utils::adt::timestamp::TimestampDifferenceExceeds(_t1, _t2, _msec)
}
/// TODO(pg-port): TimestampTzPlusMilliseconds lives in utils/timestamp.c
unsafe fn TimestampTzPlusMilliseconds(ts: TimestampTz, ms: i64) -> TimestampTz {
    ts + ms * 1000
}
/// TODO(pg-port): TimestampDifferenceMilliseconds lives in utils/timestamp.c
unsafe fn TimestampDifferenceMilliseconds(_start: TimestampTz, _stop: TimestampTz) -> i64 {
    crate::utils::adt::timestamp::TimestampDifferenceMilliseconds(_start, _stop)
}
/// TODO(pg-port): DirectFunctionCall3 lives in utils/fmgr.c
unsafe fn DirectFunctionCall3(_f: unsafe fn(), _a1: u64, _a2: u64, _a3: u64) -> u64 {
    unimplemented!("TODO(pg-port): DirectFunctionCall3")
}
/// TODO(pg-port): timestamptz_in, CStringGetDatum, ObjectIdGetDatum, Int32GetDatum, InvalidOid
unsafe fn timestamptz_in() {}
#[inline] fn CStringGetDatum(s: *const c_char) -> u64 { s as u64 }
#[inline] fn ObjectIdGetDatum(o: Oid) -> u64 { o as u64 }
#[inline] fn Int32GetDatum(i: i32) -> u64 { i as u64 }
pub const InvalidOid: Oid = 0;
/// TODO(pg-port): DatumGetTimestampTz
#[inline] fn DatumGetTimestampTz(d: u64) -> TimestampTz { d as TimestampTz }
/// TODO(pg-port): pg_lsn_in_internal lives in utils/adt/pg_lsn.c
unsafe fn pg_lsn_in_internal(_s: *const c_char, _have_error: *mut bool) -> XLogRecPtr { crate::utils::adt::pg_lsn::pg_lsn_in_internal(_s as _, _have_error as _) }
/// TODO(pg-port): guc_malloc lives in utils/guc.c
unsafe fn guc_malloc(_elevel: c_int, _size: Size) -> *mut c_void { crate::utils::misc::guc::guc_malloc(_elevel as _, _size as _) }
/// TODO(pg-port): GUC_check_errcode etc - from utils/guc.h
unsafe fn GUC_check_errcode(_sqlerrcode: c_int) {}
unsafe fn GUC_check_errdetail(_fmt: *const c_char) {}
unsafe fn GUC_check_errhint(_fmt: *const c_char) {}
/// TODO(pg-port): ReplicationSlotValidateNameInternal lives in replication/slot.c
unsafe fn ReplicationSlotValidateNameInternal(
    _name: *const c_char,
    _err_code: *mut c_int,
    _err_msg: *mut *mut c_char,
    _err_hint: *mut *mut c_char,
) -> bool {
    unimplemented!("TODO(pg-port): ReplicationSlotValidateNameInternal")
}
/// TODO(pg-port): strtoul / strtou64 libc wrappers
unsafe fn strtoul_wrapper(_s: *const c_char, _end: *mut *mut c_char, _base: c_int) -> u64 {
    unimplemented!("TODO(pg-port): strtoul wrapper")
}
/// TODO(pg-port): strlcpy lives in port/strlcpy.c
unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _size: usize) -> usize {
    crate::port::strlcpy::strlcpy(_dst, _src, _size)
}
/// TODO(pg-port): strspn, strlen - libc
unsafe fn c_strspn(_s: *const c_char, _accept: *const c_char) -> usize {
    libc::strspn(_s, _accept)
}
unsafe fn c_strlen(_s: *const c_char) -> usize {
    libc::strlen(_s)
}
/// TODO(pg-port): set_ps_display lives in utils/ps_status.c
unsafe fn set_ps_display(_activity: *const c_char) {
    crate::utils::misc::ps_status::set_ps_display(_activity)
}
/// TODO(pg-port): proc_exit lives in storage/ipc.c
unsafe fn proc_exit(_code: c_int) -> ! {
    crate::storage::ipc::ipc::proc_exit(_code)
}
/// TODO(pg-port): pg_rusage_init, pg_rusage_show lives in utils/pg_rusage.c
#[repr(C)] pub struct PGRUsage { _data: [u8; 128] }
unsafe fn pg_rusage_init(_ru: *mut PGRUsage) {}
unsafe fn pg_rusage_show(_ru: *const PGRUsage) -> *const c_char { null_mut() }
/// TODO(pg-port): xlog_outdesc lives in this file (declared later)
/// TODO(pg-port): ParseCommitRecord, ParseAbortRecord live in access/xact.c
#[repr(C)] pub struct xl_xact_commit { pub xact_time: TimestampTz }
#[repr(C)] pub struct xl_xact_abort  { pub xact_time: TimestampTz }
#[repr(C)] pub struct xl_xact_parsed_commit { pub twophase_xid: TransactionId }
#[repr(C)] pub struct xl_xact_parsed_abort  { pub twophase_xid: TransactionId }
unsafe fn ParseCommitRecord(_info: u8, _xlrec: *mut xl_xact_commit, _parsed: *mut xl_xact_parsed_commit) {
    unimplemented!("TODO(pg-port): ParseCommitRecord")
}
unsafe fn ParseAbortRecord(_info: u8, _xlrec: *mut xl_xact_abort, _parsed: *mut xl_xact_parsed_abort) {
    unimplemented!("TODO(pg-port): ParseAbortRecord")
}
/// TODO(pg-port): xl_restore_point lives in access/xlog.h
#[repr(C)] pub struct xl_restore_point {
    pub rp_time: TimestampTz,
    pub rp_name: [c_char; MAXFNAMELEN],
}
/// TODO(pg-port): xl_end_of_recovery lives in access/xlog.h
#[repr(C)] pub struct xl_end_of_recovery {
    pub end_time: pg_time_t,
    pub ThisTimeLineID: TimeLineID,
    pub PrevTimeLineID: TimeLineID,
    pub wal_level: c_int,
}
/// TODO(pg-port): xl_overwrite_contrecord lives in access/xlog.h
#[repr(C)] pub struct xl_overwrite_contrecord {
    pub overwritten_lsn: XLogRecPtr,
    pub overwrite_time: TimestampTz,
}
/// TODO(pg-port): xlog record info constants from access/xlog_internal.h
pub const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;
pub const XLOG_CHECKPOINT_ONLINE:   u8 = 0x10;
pub const XLOG_CHECKPOINT_REDO:     u8 = 0x20;
pub const XLOG_END_OF_RECOVERY:     u8 = 0x30;
pub const XLOG_OVERWRITE_CONTRECORD:u8 = 0x40;
pub const XLOG_BACKUP_END:          u8 = 0x50;
pub const XLOG_RESTORE_POINT:       u8 = 0x60;
pub const XLR_INFO_MASK:            u8 = 0x0F;
pub const XLR_CHECK_CONSISTENCY:    u8 = 0x02;
/// TODO(pg-port): RM_XLOG_ID, RM_XACT_ID from access/rmgr.h
pub const RM_XLOG_ID: u8 = 0;
pub const RM_XACT_ID: u8 = 1;
/// TODO(pg-port): XLOG_XACT_COMMIT etc from access/xact.h
pub const XLOG_XACT_COMMIT:          u8 = 0x00;
pub const XLOG_XACT_PREPARE:         u8 = 0x10;
pub const XLOG_XACT_ABORT:           u8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED:  u8 = 0x40;
pub const XLOG_XACT_OPMASK:          u8 = 0x70;
/// TODO(pg-port): BLCKSZ from pg_config.h
pub const BLCKSZ: usize = 8192;
/// TODO(pg-port): XLOG_BLCKSZ from access/xlog_internal.h
pub const XLOG_BLCKSZ: usize = 8192;
/// TODO(pg-port): SizeOfXLogRecord, SizeOfXLogRecordDataHeaderShort
pub const SizeOfXLogRecord: usize = 24;
pub const SizeOfXLogRecordDataHeaderShort: usize = 2;
/// TODO(pg-port): InvalidBuffer
pub const InvalidBuffer: c_int = 0;
/// TODO(pg-port): RBM_NORMAL_NO_LOG from storage/bufmgr.h
pub const RBM_NORMAL_NO_LOG: c_int = 2;
/// TODO(pg-port): BUFFER_LOCK_EXCLUSIVE from storage/bufmgr.h
pub const BUFFER_LOCK_EXCLUSIVE: c_int = 2;
/// TODO(pg-port): MAIN_FORKNUM from common/relpath.h
pub const MAIN_FORKNUM: c_int = 0;
/// TODO(pg-port): WL_LATCH_SET etc from storage/latch.h
pub const WL_LATCH_SET:       c_int = 0x01;
pub const WL_TIMEOUT:         c_int = 0x02;
pub const WL_EXIT_ON_PM_DEATH:c_int = 0x10;
/// TODO(pg-port): pgstat_prepare_io_time, pgstat_count_io_op_time, pgstat_report_wait_start/end
pub type instr_time = u64;
unsafe fn pgstat_prepare_io_time(_track: bool) -> instr_time { 0 }
unsafe fn pgstat_count_io_op_time(_obj: c_int, _ctx: c_int, _op: c_int, _start: instr_time, _cnt: c_int, _bytes: isize) {}
unsafe fn pgstat_report_wait_start(_event: u32) {}
unsafe fn pgstat_report_wait_end() {}
/// TODO(pg-port): PGFILETYPE_LNK from common/file_utils.h
pub const PGFILETYPE_LNK: c_int = 2;
/// TODO(pg-port): TABLESPACE_MAP, TABLESPACE_MAP_OLD, BACKUP_LABEL_FILE, etc
pub const TABLESPACE_MAP: *const u8 = b"tablespace_map\0" as *const u8;
pub const TABLESPACE_MAP_OLD: *const u8 = b"tablespace_map.old\0" as *const u8;
pub const BACKUP_LABEL_FILE: *const u8 = b"backup_label\0" as *const u8;
pub const PG_TBLSPC_DIR: *const u8 = b"pg_tblspc\0" as *const u8;
pub const XLOGDIR: *const u8 = b"pg_wal\0" as *const u8;
pub const STANDBY_SIGNAL_FILE: *const u8 = b"standby.signal\0" as *const u8;
pub const RECOVERY_SIGNAL_FILE: *const u8 = b"recovery.signal\0" as *const u8;
pub const PROMOTE_SIGNAL_FILE: *const u8 = b"promote\0" as *const u8;
/// TODO(pg-port): O_RDWR, O_RDONLY, PG_BINARY from port.h
pub const O_RDWR: c_int   = 0x0002;
pub const O_RDONLY: c_int = 0x0000;
pub const PG_BINARY: c_int = 0;
pub const S_IRUSR: u32 = 0o400;
pub const S_IWUSR: u32 = 0o200;
/// TODO(pg-port): ENOENT, EINVAL, ERANGE, errno from libc
pub const ENOENT: i32 = 2;
pub const EINVAL: i32 = 22;
pub const ERANGE: i32 = 34;
unsafe fn get_errno() -> i32 { *libc_errno() }
unsafe fn set_errno(v: i32) { *libc_errno() = v; }
extern "C" { fn __error() -> *mut c_int; }
#[inline] unsafe fn libc_errno() -> *mut i32 { __error() as *mut i32 }
/// TODO(pg-port): ferror, fgetc, fscanf, fclose - libc stdio
unsafe fn c_ferror(_f: *mut c_void) -> c_int { libc::ferror(_f as *mut libc::FILE) }
unsafe fn c_fgetc(_f: *mut c_void) -> c_int { libc::fgetc(_f as *mut libc::FILE) }
const EOF: c_int = -1;
/// TODO(pg-port): snprintf/unlink - libc
unsafe fn c_snprintf(_s: *mut c_char, _n: usize, _fmt: *const c_char) {}
unsafe fn c_unlink(_path: *const c_char) -> c_int { libc::unlink(_path) }
unsafe fn c_symlink(_target: *const c_char, _linkpath: *const c_char) -> c_int { libc::symlink(_target, _linkpath) }
unsafe fn c_stat(_path: *const c_char, _buf: *mut StatBuf) -> c_int {
    let mut st: libc::stat = core::mem::zeroed();
    let r = libc::stat(_path, &mut st);
    if r == 0 { (*_buf).st_size = st.st_size as i64; }
    r
}
unsafe fn c_close(_fd: c_int) -> c_int { libc::close(_fd) }
/// TODO(pg-port): stat struct
#[repr(C)] pub struct StatBuf { pub st_size: i64 }
/// TODO(pg-port): errcontext, error_context_stack from elog.h
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: unsafe fn(*mut c_void),
    pub arg: *mut c_void,
}
pub static mut error_context_stack: *mut ErrorContextCallback = null_mut();
/// TODO(pg-port): TransactionIdIsNormal, TransactionIdIsValid from access/transam.h
#[inline] fn TransactionIdIsNormal(xid: TransactionId) -> bool { xid >= 3 }
#[inline] fn TransactionIdIsValid(xid: TransactionId) -> bool { xid != 0 }
/// TODO(pg-port): XidFromFullTransactionId
#[inline] fn XidFromFullTransactionId(x: FullTransactionId) -> TransactionId { x as u32 }
/// TODO(pg-port): U64FromFullTransactionId
#[inline] fn U64FromFullTransactionId(x: FullTransactionId) -> u64 { x }
/// TODO(pg-port): IsPromoteSignaled, ResetPromoteSignaled from postmaster/startup.c
unsafe fn IsPromoteSignaled() -> bool { crate::postmaster::startup::IsPromoteSignaled() }
unsafe fn ResetPromoteSignaled() { crate::postmaster::startup::ResetPromoteSignaled() }
/// TODO(pg-port): CheckPromoteSignal, RemovePromoteSignalFiles declared later
/// TODO(pg-port): ereport_startup_progress
unsafe fn ereport_startup_progress(_fmt: *const c_char) {}
/// TODO(pg-port): pg_lsn macros
/// TODO(pg-port): initStringInfo, appendStringInfo, appendStringInfoChar, appendStringInfoString, pfree
unsafe fn initStringInfo(_buf: *mut StringInfoData) {
    unimplemented!("TODO(pg-port): initStringInfo")
}
unsafe fn appendStringInfo(_buf: *mut StringInfoData, _fmt: *const c_char) {
    unimplemented!("TODO(pg-port): appendStringInfo")
}
unsafe fn appendStringInfoChar(_buf: *mut StringInfoData, _ch: c_char) {
    unimplemented!("TODO(pg-port): appendStringInfoChar")
}
unsafe fn appendStringInfoString(_buf: *mut StringInfoData, _s: *const c_char) {
    unimplemented!("TODO(pg-port): appendStringInfoString")
}
/// TODO(pg-port): errcode_for_file_access lives in utils/elog.h
unsafe fn errcode_for_file_access() -> c_int { 0 }
/// TODO(pg-port): wait event constants
pub const WAIT_EVENT_RECOVERY_PAUSE: u32 = 0;
pub const WAIT_EVENT_RECOVERY_APPLY_DELAY: u32 = 1;
pub const WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL: u32 = 2;
pub const WAIT_EVENT_RECOVERY_WAL_STREAM: u32 = 3;
pub const WAIT_EVENT_WAL_READ: u32 = 4;
pub const IOOBJECT_WAL: c_int = 0;
pub const IOCONTEXT_NORMAL: c_int = 0;
pub const IOOP_READ: c_int = 0;
/// TODO(pg-port): PG_USED_FOR_ASSERTS_ONLY - suppresses unused warning
/// TODO(pg-port): Assert macro - no-op in non-debug mode
macro_rules! Assert {
    ($e:expr) => { let _ = $e; };
}

// ereport / elog stubs - forward args (matches GREEN xact.rs form)
macro_rules! ereport {
    ($level:expr, $msg:expr) => {{
        /* TODO(pg-port): ereport - errcode/errdetail folded as comment */
        eprintln!("[ereport level={}] {}", $level, $msg);
        if $level >= PANIC {
            panic!("ereport PANIC");
        }
    }};
}
macro_rules! elog {
    ($level:expr, $($arg:tt)*) => {{
        /* TODO(pg-port): elog */
        eprintln!("[elog level={}] {}", $level, format!($($arg)*));
        if $level >= PANIC {
            panic!("elog PANIC");
        }
    }};
}
macro_rules! errmsg {
    ($($arg:tt)*) => { format!($($arg)*) };
}
macro_rules! errmsg_internal {
    ($($arg:tt)*) => { format!($($arg)*) };
}
macro_rules! errdetail {
    ($fmt:literal $(, $arg:expr)*) => {};
}
macro_rules! errhint {
    ($fmt:literal $(, $arg:expr)*) => {};
}
macro_rules! errcode {
    ($code:ident) => { 0i32 };
}
macro_rules! errcontext {
    ($fmt:literal $(, $arg:expr)*) => {};
}

// Level constants used in ereport
pub const LOG: c_int = 15;
pub const DEBUG1: c_int = 14;
pub const DEBUG2: c_int = 13;
pub const WARNING: c_int = 19;
pub const PANIC: c_int = 22;
pub const FATAL: c_int = 21;
pub const ERROR: c_int = 20;
pub const INFO: c_int = 17;


// ---------------------------------------------------------------------------
// Part 3: XLogRecoveryShmemSize, XLogRecoveryShmemInit, EnableStandbyMode,
//         InitWalRecovery (first half)
// ---------------------------------------------------------------------------

/*
 * Initialization of shared memory for WAL recovery
 */
pub unsafe fn XLogRecoveryShmemSize() -> Size {
    /* XLogRecoveryCtl */
    size_of::<XLogRecoveryCtlData>()
}

pub unsafe fn XLogRecoveryShmemInit() {
    let mut found: bool = false;

    XLogRecoveryCtl = ShmemInitStruct("XLOG Recovery Ctl", XLogRecoveryShmemSize(), &mut found)
        as *mut XLogRecoveryCtlData;
    if found {
        return;
    }
    core::ptr::write_bytes(XLogRecoveryCtl as *mut u8, 0, size_of::<XLogRecoveryCtlData>());

    SpinLockInit(&mut (*XLogRecoveryCtl).info_lck);
    InitSharedLatch(&mut (*XLogRecoveryCtl).recoveryWakeupLatch);
    ConditionVariableInit(&mut (*XLogRecoveryCtl).recoveryNotPausedCV);
}

/*
 * A thin wrapper to enable StandbyMode and do other preparatory work as
 * needed.
 */
unsafe fn EnableStandbyMode() {
    StandbyMode = true;

    /*
     * To avoid server log bloat, we don't report recovery progress in a
     * standby as it will always be in recovery unless promoted. We disable
     * startup progress timeout in standby mode to avoid calling
     * startup_progress_timeout_handler() unnecessarily.
     */
    disable_startup_progress_timeout();
}

/*
 * Prepare the system for WAL recovery, if needed.
 *
 * This is called by StartupXLOG() which coordinates the server startup
 * sequence.  This function analyzes the control file and the backup label
 * file, if any, and figures out whether we need to perform crash recovery or
 * archive recovery, and how far we need to replay the WAL to reach a
 * consistent state.
 */
pub unsafe fn InitWalRecovery(
    ControlFile: *mut ControlFileData,
    wasShutdown_ptr: *mut bool,
    haveBackupLabel_ptr: *mut bool,
    haveTblspcMap_ptr: *mut bool,
) {
    let private: *mut XLogPageReadPrivate;
    let mut st = StatBuf { st_size: 0 };
    let wasShutdown: bool;
    let record: *mut XLogRecord;
    let dbstate_at_startup: DBState;
    let mut haveTblspcMap: bool = false;
    let mut haveBackupLabel: bool = false;
    let mut checkPoint: CheckPoint = core::mem::zeroed();
    let mut backupFromStandby: bool = false;

    dbstate_at_startup = (*ControlFile).state;

    /*
     * Initialize on the assumption we want to recover to the latest timeline
     * that's active according to pg_control.
     */
    if (*ControlFile).minRecoveryPointTLI > (*ControlFile).checkPointCopy.ThisTimeLineID {
        recoveryTargetTLI = (*ControlFile).minRecoveryPointTLI;
    } else {
        recoveryTargetTLI = (*ControlFile).checkPointCopy.ThisTimeLineID;
    }

    /*
     * Check for signal files, and if so set up state for offline recovery
     */
    readRecoverySignalFile();
    validateRecoveryParameters();

    /*
     * Take ownership of the wakeup latch if we're going to sleep during
     * recovery, if required.
     */
    if ArchiveRecoveryRequested {
        OwnLatch(&mut (*XLogRecoveryCtl).recoveryWakeupLatch);
    }

    /*
     * Set the WAL reading processor now, as it will be needed when reading
     * the checkpoint record required (backup_label or not).
     */
    private = palloc0(size_of::<XLogPageReadPrivate>()) as *mut XLogPageReadPrivate;
    xlogreader = XLogReaderAllocate(
        wal_segment_size,
        null_mut(),
        XLogReaderRoutine {
            page_read: Some(XLogPageRead_trampoline),
            segment_open: None,
            segment_close: Some(wal_segment_close),
        },
        private as *mut c_void,
    );
    if xlogreader.is_null() {
        ereport!(ERROR, errmsg!("out of memory"));
        /* C also: errdetail("Failed while allocating a WAL reading processor.") */
    }
    (*xlogreader).system_identifier = (*ControlFile).system_identifier;

    /*
     * Set the WAL decode buffer size.  This limits how far ahead we can read
     * in the WAL.
     */
    XLogReaderSetDecodeBuffer(xlogreader, null_mut(), wal_decode_buffer_size);

    /* Create a WAL prefetcher. */
    xlogprefetcher = XLogPrefetcherAllocate(xlogreader);

    /*
     * Allocate two page buffers dedicated to WAL consistency checks.
     */
    replay_image_masked = palloc(BLCKSZ) as *mut c_char;
    primary_image_masked = palloc(BLCKSZ) as *mut c_char;

    /*
     * Read the backup_label file.
     */
    let mut checkPointLoc_out: XLogRecPtr = 0;
    let mut checkPointTLI_out: TimeLineID = 0;
    let mut backupEndRequired_out: bool = false;
    let mut backupFromStandby_out: bool = false;
    if read_backup_label(
        &mut checkPointLoc_out,
        &mut checkPointTLI_out,
        &mut backupEndRequired_out,
        &mut backupFromStandby_out,
    ) {
        CheckPointLoc = checkPointLoc_out;
        CheckPointTLI = checkPointTLI_out;
        backupFromStandby = backupFromStandby_out;

        let mut tablespaces: *mut List = null_mut();

        /*
         * Archive recovery was requested, and thanks to the backup label
         * file, we know how far we need to replay to reach consistency. Enter
         * archive recovery directly.
         */
        InArchiveRecovery = true;
        if StandbyModeRequested {
            EnableStandbyMode();
        }

        /*
         * Omitting backup_label when creating a new replica, PITR node etc.
         * unfortunately is a common cause of corruption.
         */
        ereport!(LOG, errmsg!(
            "starting backup recovery with redo LSN {}/{:X}, checkpoint LSN {}/{:X}, on timeline ID {}",
            lsn_hi(RedoStartLSN), lsn_lo(RedoStartLSN),
            lsn_hi(CheckPointLoc), lsn_lo(CheckPointLoc),
            CheckPointTLI
        ));

        /*
         * When a backup_label file is present, we want to roll forward from
         * the checkpoint it identifies, rather than using pg_control.
         */
        record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc, CheckPointTLI);
        if !record.is_null() {
            core::ptr::copy_nonoverlapping(
                XLogRecGetData(xlogreader as *mut XLogReaderState) as *const u8,
                &mut checkPoint as *mut CheckPoint as *mut u8,
                size_of::<CheckPoint>(),
            );
            wasShutdown =
                ((*record).xl_info & !XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN;
            ereport!(DEBUG1, errmsg_internal!("checkpoint record is at {}/{:X}",
                lsn_hi(CheckPointLoc), lsn_lo(CheckPointLoc)));
            InRecovery = true; /* force recovery even if SHUTDOWNED */

            /*
             * Make sure that REDO location exists.
             */
            if checkPoint.redo < CheckPointLoc {
                XLogPrefetcherBeginRead(xlogprefetcher, checkPoint.redo);
                if ReadRecord(xlogprefetcher, LOG, false, checkPoint.ThisTimeLineID).is_null() {
                    ereport!(FATAL, errmsg!(
                        "could not find redo location {}/{:X} referenced by checkpoint record at {}/{:X}",
                        lsn_hi(checkPoint.redo), lsn_lo(checkPoint.redo),
                        lsn_hi(CheckPointLoc), lsn_lo(CheckPointLoc)
                    ));
                    /* C also: errhint(...) */
                }
            }
        } else {
            ereport!(FATAL, errmsg!(
                "could not locate required checkpoint record at {}/{:X}",
                lsn_hi(CheckPointLoc), lsn_lo(CheckPointLoc)
            ));
            /* C also: errhint(...) */
            wasShutdown = false; /* keep compiler quiet */
        }

        /* Read the tablespace_map file if present and create symlinks. */
        if read_tablespace_map(&mut tablespaces) {
            crate::foreach!(lc, tablespaces, {
                let ti = lfirst(crate::current_cell!(lc) as *mut c_void) as *mut tablespaceinfo;
                let linkloc = psprintf(
                    b"%s/%u\0".as_ptr() as *const c_char,
                );
                /* remove_tablespace_symlink(linkloc) then symlink */
                remove_tablespace_symlink(linkloc);
                if c_symlink((*ti).path, linkloc) < 0 {
                    ereport!(ERROR, errmsg!("could not create symbolic link"));
                    /* C also: errcode_for_file_access, errmsg(...linkloc) */
                }
                pfree((*ti).path as *mut c_void);
                pfree(ti as *mut c_void);
            });
            /* tell the caller to delete it later */
            haveTblspcMap = true;
        }

        /* tell the caller to delete it later */
        haveBackupLabel = true;
    } else {
        /* No backup_label file has been found if we are here. */

        /*
         * If tablespace_map file is present without backup_label file, there
         * is no use of such file.
         */
        if c_stat(TABLESPACE_MAP as *const c_char, &mut st) == 0 {
            c_unlink(TABLESPACE_MAP_OLD as *const c_char);
            if durable_rename(
                TABLESPACE_MAP as *const c_char,
                TABLESPACE_MAP_OLD as *const c_char,
                DEBUG1,
            ) == 0 {
                ereport!(LOG, errmsg!("ignoring file because no backup_label exists"));
                /* C also: errdetail */
            } else {
                ereport!(LOG, errmsg!("ignoring file because no backup_label exists"));
                /* C also: errdetail with rename failure */
            }
        }

        /*
         * It's possible that archive recovery was requested, but we don't
         * know how far we need to replay the WAL before we reach consistency.
         */
        if ArchiveRecoveryRequested
            && (!XLogRecPtrIsInvalid((*ControlFile).minRecoveryPoint)
                || (*ControlFile).backupEndRequired
                || !XLogRecPtrIsInvalid((*ControlFile).backupEndPoint)
                || (*ControlFile).state == DB_SHUTDOWNED)
        {
            InArchiveRecovery = true;
            if StandbyModeRequested {
                EnableStandbyMode();
            }
        }

        /*
         * For the same reason as when starting up with backup_label present,
         * emit a log message when we continue initializing from a base backup.
         */
        if !XLogRecPtrIsInvalid((*ControlFile).backupStartPoint) {
            ereport!(LOG, errmsg!(
                "restarting backup recovery with redo LSN {}/{:X}",
                lsn_hi((*ControlFile).backupStartPoint),
                lsn_lo((*ControlFile).backupStartPoint)
            ));
        }

        /* Get the last valid checkpoint record. */
        CheckPointLoc = (*ControlFile).checkPoint;
        CheckPointTLI = (*ControlFile).checkPointCopy.ThisTimeLineID;
        RedoStartLSN = (*ControlFile).checkPointCopy.redo;
        RedoStartTLI = (*ControlFile).checkPointCopy.ThisTimeLineID;
        record = ReadCheckpointRecord(xlogprefetcher, CheckPointLoc, CheckPointTLI);
        if !record.is_null() {
            ereport!(DEBUG1, errmsg_internal!("checkpoint record is at {}/{:X}",
                lsn_hi(CheckPointLoc), lsn_lo(CheckPointLoc)));
        } else {
            /*
             * We used to attempt to go back to a secondary checkpoint record
             * here, but only when not in standby mode. We now just fail if we
             * can't read the last checkpoint because this allows us to
             * simplify processing around checkpoints.
             */
            ereport!(PANIC, errmsg!(
                "could not locate a valid checkpoint record at {}/{:X}",
                lsn_hi(CheckPointLoc), lsn_lo(CheckPointLoc)
            ));
        }
        core::ptr::copy_nonoverlapping(
            XLogRecGetData(xlogreader as *mut XLogReaderState) as *const u8,
            &mut checkPoint as *mut CheckPoint as *mut u8,
            size_of::<CheckPoint>(),
        );
        wasShutdown = ((*record).xl_info & !XLR_INFO_MASK) == XLOG_CHECKPOINT_SHUTDOWN;

        /* Make sure that REDO location exists. */
        if checkPoint.redo < CheckPointLoc {
            XLogPrefetcherBeginRead(xlogprefetcher, checkPoint.redo);
            if ReadRecord(xlogprefetcher, LOG, false, checkPoint.ThisTimeLineID).is_null() {
                ereport!(PANIC, errmsg!(
                    "could not find redo location {}/{:08X} referenced by checkpoint record at {}/{:08X}",
                    lsn_hi(checkPoint.redo), lsn_lo(checkPoint.redo),
                    lsn_hi(CheckPointLoc), lsn_lo(CheckPointLoc)
                ));
            }
        }
    }

    if ArchiveRecoveryRequested {
        if StandbyModeRequested {
            ereport!(LOG, errmsg!("entering standby mode"));
        } else if recoveryTarget == RECOVERY_TARGET_XID {
            ereport!(LOG, errmsg!("starting point-in-time recovery to XID {}",
                recoveryTargetXid));
        } else if recoveryTarget == RECOVERY_TARGET_TIME {
            ereport!(LOG, errmsg!("starting point-in-time recovery to {}",
                core::ffi::CStr::from_ptr(timestamptz_to_str(recoveryTargetTime)).to_string_lossy()));
        } else if recoveryTarget == RECOVERY_TARGET_NAME {
            ereport!(LOG, errmsg!("starting point-in-time recovery to a named restore point"));
        } else if recoveryTarget == RECOVERY_TARGET_LSN {
            ereport!(LOG, errmsg!(
                "starting point-in-time recovery to WAL location (LSN) \"{}/{:X}\"",
                lsn_hi(recoveryTargetLSN), lsn_lo(recoveryTargetLSN)
            ));
        } else if recoveryTarget == RECOVERY_TARGET_IMMEDIATE {
            ereport!(LOG, errmsg!("starting point-in-time recovery to earliest consistent point"));
        } else {
            ereport!(LOG, errmsg!("starting archive recovery"));
        }
    }

    /*
     * If the location of the checkpoint record is not on the expected
     * timeline in the history of the requested timeline, we cannot proceed.
     */
    Assert!(expectedTLEs != null_mut()); /* was initialized by reading checkpoint record */
    if tliOfPointInHistory(CheckPointLoc, expectedTLEs) != CheckPointTLI {
        let switchpoint: XLogRecPtr;
        /*
         * tliSwitchPoint will throw an error if the checkpoint's timeline is
         * not in expectedTLEs at all.
         */
        switchpoint = tliSwitchPoint(CheckPointTLI, expectedTLEs, null_mut());
        ereport!(FATAL, errmsg!(
            "requested timeline {} is not a child of this server's history",
            recoveryTargetTLI
        ));
        /* C also: errdetail */
    }

    /*
     * The min recovery point should be part of the requested timeline's
     * history, too.
     */
    if !XLogRecPtrIsInvalid((*ControlFile).minRecoveryPoint)
        && tliOfPointInHistory((*ControlFile).minRecoveryPoint - 1, expectedTLEs)
            != (*ControlFile).minRecoveryPointTLI
    {
        ereport!(FATAL, errmsg!(
            "requested timeline {} does not contain minimum recovery point {}/{:X} on timeline {}",
            recoveryTargetTLI,
            lsn_hi((*ControlFile).minRecoveryPoint), lsn_lo((*ControlFile).minRecoveryPoint),
            (*ControlFile).minRecoveryPointTLI
        ));
    }

    ereport!(DEBUG1, errmsg_internal!("redo record is at {}/{:X}; shutdown {}",
        lsn_hi(checkPoint.redo), lsn_lo(checkPoint.redo),
        if wasShutdown { "true" } else { "false" }));
    ereport!(DEBUG1, errmsg_internal!("next transaction ID: {}; next OID: {}",
        U64FromFullTransactionId(checkPoint.nextXid), checkPoint.nextOid));
    ereport!(DEBUG1, errmsg_internal!("next MultiXactId: {}; next MultiXactOffset: {}",
        checkPoint.nextMulti, checkPoint.nextMultiOffset));
    ereport!(DEBUG1, errmsg_internal!("oldest unfrozen transaction ID: {}, in database {}",
        checkPoint.oldestXid, checkPoint.oldestXidDB));
    ereport!(DEBUG1, errmsg_internal!("oldest MultiXactId: {}, in database {}",
        checkPoint.oldestMulti, checkPoint.oldestMultiDB));
    ereport!(DEBUG1, errmsg_internal!("commit timestamp Xid oldest/newest: {}/{}",
        checkPoint.oldestCommitTsXid, checkPoint.newestCommitTsXid));
    if !TransactionIdIsNormal(XidFromFullTransactionId(checkPoint.nextXid)) {
        ereport!(PANIC, errmsg!("invalid next transaction ID"));
    }

    /* sanity check */
    if checkPoint.redo > CheckPointLoc {
        ereport!(PANIC, errmsg!("invalid redo in checkpoint record"));
    }

    /*
     * Check whether we need to force recovery from WAL.
     */
    if checkPoint.redo < CheckPointLoc {
        if wasShutdown {
            ereport!(PANIC, errmsg!("invalid redo record in shutdown checkpoint"));
        }
        InRecovery = true;
    } else if (*ControlFile).state != DB_SHUTDOWNED {
        InRecovery = true;
    } else if ArchiveRecoveryRequested {
        /* force recovery due to presence of recovery signal file */
        InRecovery = true;
    }

    /*
     * If recovery is needed, update our in-memory copy of pg_control to show
     * that we are recovering.
     */
    if InRecovery {
        if InArchiveRecovery {
            (*ControlFile).state = DB_IN_ARCHIVE_RECOVERY;
        } else {
            ereport!(LOG, errmsg!("database system was not properly shut down; automatic recovery in progress"));
            if recoveryTargetTLI > (*ControlFile).checkPointCopy.ThisTimeLineID {
                ereport!(LOG, errmsg!(
                    "crash recovery starts in timeline {} and has target timeline {}",
                    (*ControlFile).checkPointCopy.ThisTimeLineID,
                    recoveryTargetTLI
                ));
            }
            (*ControlFile).state = DB_IN_CRASH_RECOVERY;
        }
        (*ControlFile).checkPoint = CheckPointLoc;
        (*ControlFile).checkPointCopy = core::ptr::read(&checkPoint);
        if InArchiveRecovery {
            /* initialize minRecoveryPoint if not set yet */
            if (*ControlFile).minRecoveryPoint < checkPoint.redo {
                (*ControlFile).minRecoveryPoint = checkPoint.redo;
                (*ControlFile).minRecoveryPointTLI = checkPoint.ThisTimeLineID;
            }
        }

        /*
         * Set backupStartPoint if we're starting recovery from a base backup.
         */
        if haveBackupLabel {
            (*ControlFile).backupStartPoint = checkPoint.redo;
            (*ControlFile).backupEndRequired = backupEndRequired_out;

            if backupFromStandby {
                if dbstate_at_startup != DB_IN_ARCHIVE_RECOVERY
                    && dbstate_at_startup != DB_SHUTDOWNED_IN_RECOVERY
                {
                    ereport!(FATAL, errmsg!("backup_label contains data inconsistent with control file"));
                    /* C also: errhint */
                }
                (*ControlFile).backupEndPoint = (*ControlFile).minRecoveryPoint;
            }
        }
    }

    /* remember these, so that we know when we have reached consistency */
    backupStartPoint = (*ControlFile).backupStartPoint;
    backupEndRequired = (*ControlFile).backupEndRequired;
    backupEndPoint = (*ControlFile).backupEndPoint;
    if InArchiveRecovery {
        minRecoveryPoint = (*ControlFile).minRecoveryPoint;
        minRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;
    } else {
        minRecoveryPoint = InvalidXLogRecPtr;
        minRecoveryPointTLI = 0;
    }

    /*
     * Start recovery assuming that the final record isn't lost.
     */
    abortedRecPtr = InvalidXLogRecPtr;
    missingContrecPtr = InvalidXLogRecPtr;

    *wasShutdown_ptr = wasShutdown;
    *haveBackupLabel_ptr = haveBackupLabel;
    *haveTblspcMap_ptr = haveTblspcMap;
}

// trampoline so we can take address of XLogPageRead with the right signature
unsafe fn XLogPageRead_trampoline(
    xlogreader_arg: *mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: c_int,
    targetRecPtr: XLogRecPtr,
    readBuf: *mut c_char,
) -> c_int {
    XLogPageRead(xlogreader_arg, targetPagePtr, reqLen, targetRecPtr, readBuf)
}


// ---------------------------------------------------------------------------
// Part 4: readRecoverySignalFile, validateRecoveryParameters,
//         read_backup_label, read_tablespace_map, FinishWalRecovery,
//         ShutdownWalRecovery
// ---------------------------------------------------------------------------

/*
 * See if there are any recovery signal files and if so, set state for
 * recovery.
 */
unsafe fn readRecoverySignalFile() {
    let mut stat_buf = StatBuf { st_size: 0 };

    if IsBootstrapProcessingMode() {
        return;
    }

    /*
     * Check for old recovery API file: recovery.conf
     */
    if c_stat(RECOVERY_COMMAND_FILE.as_ptr() as *const c_char, &mut stat_buf) == 0 {
        ereport!(FATAL, errmsg!("using recovery command file \"recovery.conf\" is not supported"));
        /* C also: errcode_for_file_access */
    }

    /*
     * Remove unused .done file, if present. Ignore if absent.
     */
    c_unlink(RECOVERY_COMMAND_DONE.as_ptr() as *const c_char);

    /*
     * Check for recovery signal files and if found, fsync them since they
     * represent server state information.
     */
    if c_stat(STANDBY_SIGNAL_FILE as *const c_char, &mut stat_buf) == 0 {
        let fd = BasicOpenFilePerm(STANDBY_SIGNAL_FILE as *const c_char, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        if fd >= 0 {
            pg_fsync(fd);
            c_close(fd);
        }
        standby_signal_file_found = true;
    } else if c_stat(RECOVERY_SIGNAL_FILE as *const c_char, &mut stat_buf) == 0 {
        let fd = BasicOpenFilePerm(RECOVERY_SIGNAL_FILE as *const c_char, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        if fd >= 0 {
            pg_fsync(fd);
            c_close(fd);
        }
        recovery_signal_file_found = true;
    }

    StandbyModeRequested = false;
    ArchiveRecoveryRequested = false;
    if standby_signal_file_found {
        StandbyModeRequested = true;
        ArchiveRecoveryRequested = true;
    } else if recovery_signal_file_found {
        StandbyModeRequested = false;
        ArchiveRecoveryRequested = true;
    } else {
        return;
    }

    /*
     * We don't support standby mode in standalone backends.
     */
    if StandbyModeRequested && !IsUnderPostmaster {
        ereport!(FATAL, errmsg!("standby mode is not supported by single-user servers"));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }
}

unsafe fn validateRecoveryParameters() {
    if !ArchiveRecoveryRequested {
        return;
    }

    /*
     * Check for compulsory parameters
     */
    if StandbyModeRequested {
        let no_conninfo = PrimaryConnInfo.is_null()
            || *PrimaryConnInfo == 0;
        let no_restore_cmd = recoveryRestoreCommand.is_null()
            || *recoveryRestoreCommand == 0;
        if no_conninfo && no_restore_cmd {
            ereport!(WARNING, errmsg!(
                "specified neither \"primary_conninfo\" nor \"restore_command\""
            ));
            /* C also: errhint about pg_wal polling */
        }
    } else {
        if recoveryRestoreCommand.is_null() || *recoveryRestoreCommand == 0 {
            ereport!(FATAL, errmsg!(
                "must specify \"restore_command\" when standby mode is not enabled"
            ));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
    }

    /*
     * Override any inconsistent requests.
     */
    if recoveryTargetAction == RECOVERY_TARGET_ACTION_PAUSE && !EnableHotStandby {
        recoveryTargetAction = RECOVERY_TARGET_ACTION_SHUTDOWN;
    }

    /*
     * Final parsing of recovery_target_time string.
     */
    if recoveryTarget == RECOVERY_TARGET_TIME {
        recoveryTargetTime = DatumGetTimestampTz(DirectFunctionCall3(
            timestamptz_in,
            CStringGetDatum(recovery_target_time_string),
            ObjectIdGetDatum(InvalidOid),
            Int32GetDatum(-1),
        ));
    }

    /*
     * If user specified recovery_target_timeline, validate it or compute the
     * "latest" value.
     */
    if recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC {
        let rtli = recoveryTargetTLIRequested;
        /* Timeline 1 does not have a history file, all else should */
        if rtli != 1 && !existsTimeLineHistory(rtli) {
            ereport!(FATAL, errmsg!("recovery target timeline {} does not exist", rtli));
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        }
        recoveryTargetTLI = rtli;
    } else if recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST {
        /* We start the "latest" search from pg_control's timeline */
        recoveryTargetTLI = findNewestTimeLine(recoveryTargetTLI);
    } else {
        /*
         * else we just use the recoveryTargetTLI as already read from
         * ControlFile
         */
        Assert!(recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_CONTROLFILE);
    }
}

/*
 * read_backup_label: check to see if a backup_label file is present
 *
 * Returns true if a backup_label was found (and fills the checkpoint
 * location and TLI into *checkPointLoc and *backupLabelTLI, respectively);
 * returns false if not.
 */
unsafe fn read_backup_label(
    checkPointLoc: *mut XLogRecPtr,
    backupLabelTLI: *mut TimeLineID,
    backupEndRequired_p: *mut bool,
    backupFromStandby: *mut bool,
) -> bool {
    let startxlogfilename: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let tli_from_walseg: TimeLineID = 0;
    let tli_from_file: TimeLineID = 0;
    let lfp: *mut c_void;
    let ch: c_char = 0;
    let backuptype: [c_char; 20] = [0; 20];
    let backupfrom: [c_char; 20] = [0; 20];
    let backuplabel: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let backuptime: [c_char; 128] = [0; 128];
    let hi: u32 = 0;
    let lo: u32 = 0;

    /* suppress possible uninitialized-variable warnings */
    *checkPointLoc = InvalidXLogRecPtr;
    *backupLabelTLI = 0;
    *backupEndRequired_p = false;
    *backupFromStandby = false;

    /*
     * See if label file is present
     */
    lfp = AllocateFile(BACKUP_LABEL_FILE as *const c_char, b"r\0".as_ptr() as *const c_char);
    if lfp.is_null() {
        if get_errno() != ENOENT {
            ereport!(FATAL, errmsg!("could not read file \"backup_label\""));
            /* C also: errcode_for_file_access */
        }
        return false; /* it's not there, all is fine */
    }

    /*
     * Read and parse the START WAL LOCATION and CHECKPOINT lines.
     * (We rely on AllocateFile/fscanf via stub - real impl would use libc fscanf)
     */
    /* RedoStartLSN = ((uint64) hi) << 32 | lo; */
    /* RedoStartTLI = tli_from_walseg; */
    /* *checkPointLoc = ((uint64) hi) << 32 | lo; */
    /* *backupLabelTLI = tli_from_walseg; */
    /* BACKUP METHOD / BACKUP FROM / START TIME / LABEL / START TIMELINE parsing elided - */
    /* real impl calls fscanf on lfp; stubs cannot do that here */

    if FreeFile(lfp) != 0 {
        ereport!(FATAL, errmsg!("could not read file \"backup_label\""));
    }

    true
}

/*
 * read_tablespace_map: check to see if a tablespace_map file is present
 *
 * Returns true if a tablespace_map file was found (and fills *tablespaces
 * with a tablespaceinfo struct for each tablespace listed in the file);
 * returns false if not.
 */
unsafe fn read_tablespace_map(tablespaces: *mut *mut List) -> bool {
    let lfp: *mut c_void;
    let mut str_buf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut ch: c_int;
    let mut i: usize = 0;
    let mut n: usize;
    let mut was_backslash: bool = false;

    /*
     * See if tablespace_map file is present
     */
    lfp = AllocateFile(TABLESPACE_MAP as *const c_char, b"r\0".as_ptr() as *const c_char);
    if lfp.is_null() {
        if get_errno() != ENOENT {
            ereport!(FATAL, errmsg!("could not read file \"tablespace_map\""));
        }
        return false; /* it's not there, all is fine */
    }

    /*
     * Read and parse the link name and path lines from tablespace_map file.
     * De-escape any backslashes that were inserted.
     */
    i = 0;
    was_backslash = false;
    loop {
        ch = c_fgetc(lfp);
        if ch == EOF {
            break;
        }
        let c = ch as u8 as c_char;
        if !was_backslash && (c == b'\n' as c_char || c == b'\r' as c_char) {
            if i == 0 {
                continue; /* \r immediately followed by \n */
            }
            /*
             * The de-escaped line should contain an OID followed by exactly
             * one space followed by a path.
             */
            str_buf[i] = 0;
            n = 0;
            while n < i && str_buf[n] != b' ' as c_char {
                n += 1;
            }
            if n < 1 || n >= i - 1 {
                ereport!(FATAL, errmsg!("invalid data in file \"tablespace_map\""));
            }
            str_buf[n] = 0;
            n += 1;

            let ti = palloc0(size_of::<tablespaceinfo>()) as *mut tablespaceinfo;
            set_errno(0);
            (*ti).oid = {
                let s = core::str::from_utf8(core::slice::from_raw_parts(
                    str_buf.as_ptr() as *const u8, n - 1,
                )).unwrap_or("0");
                s.parse::<Oid>().unwrap_or(0)
            };
            let path_start = &str_buf[n] as *const c_char;
            (*ti).path = pstrdup(path_start);
            *tablespaces = lappend(*tablespaces, ti as *mut c_void);

            i = 0;
            continue;
        } else if !was_backslash && c == b'\\' as c_char {
            was_backslash = true;
        } else {
            if i < str_buf.len() - 1 {
                str_buf[i] = c;
                i += 1;
            }
            was_backslash = false;
        }
    }

    if i != 0 || was_backslash {
        /* last line not terminated? */
        ereport!(FATAL, errmsg!("invalid data in file \"tablespace_map\""));
    }

    if c_ferror(lfp) != 0 || FreeFile(lfp) != 0 {
        ereport!(FATAL, errmsg!("could not read file \"tablespace_map\""));
    }

    true
}

/*
 * Finish WAL recovery.
 *
 * Returns the position of the last valid or applied record, after which new
 * WAL should be appended, information about why recovery was ended, and some
 * other things.
 */
pub unsafe fn FinishWalRecovery() -> *mut EndOfWalRecoveryInfo {
    let result = palloc(size_of::<EndOfWalRecoveryInfo>()) as *mut EndOfWalRecoveryInfo;
    let lastRec: XLogRecPtr;
    let lastRecTLI: TimeLineID;
    let endOfLog: XLogRecPtr;

    /*
     * Kill WAL receiver, if it's still running, before we continue to write
     * the startup checkpoint and aborted-contrecord records.
     */
    XLogShutdownWalRcv();

    /*
     * Shutdown the slot sync worker to drop any temporary slots acquired by
     * it and to prevent it from keep trying to fetch the failover slots.
     */
    ShutDownSlotSync();

    /*
     * We are now done reading the xlog from stream. Turn off streaming
     * recovery to force fetching the files from archive or pg_wal.
     */
    Assert!(!WalRcvStreaming());
    StandbyMode = false;

    /*
     * Determine where to start writing WAL next.
     */
    if !InRecovery {
        lastRec = CheckPointLoc;
        lastRecTLI = CheckPointTLI;
    } else {
        lastRec = (*XLogRecoveryCtl).lastReplayedReadRecPtr;
        lastRecTLI = (*XLogRecoveryCtl).lastReplayedTLI;
    }
    XLogPrefetcherBeginRead(xlogprefetcher, lastRec);
    let _ = ReadRecord(xlogprefetcher, PANIC, false, lastRecTLI);
    endOfLog = (*xlogreader).EndRecPtr;

    /*
     * Remember the TLI in the filename of the XLOG segment containing the
     * end-of-log.
     */
    (*result).endOfLogTLI = (*xlogreader).seg.ws_tli;

    if ArchiveRecoveryRequested {
        /*
         * We are no longer in archive recovery state.
         */
        Assert!(InArchiveRecovery);
        InArchiveRecovery = false;

        /*
         * If the ending log segment is still open, close it.
         */
        if readFile >= 0 {
            c_close(readFile);
            readFile = -1;
        }
    }

    /*
     * Copy the last partial block to the caller, for initializing the WAL
     * buffer for appending new WAL.
     */
    if endOfLog % XLOG_BLCKSZ as u64 != 0 {
        let pageBeginPtr = endOfLog - (endOfLog % XLOG_BLCKSZ as u64);
        Assert!(readOff == XLogSegmentOffset(pageBeginPtr, wal_segment_size));

        /* Copy the valid part of the last block */
        let len = (endOfLog % XLOG_BLCKSZ as u64) as usize;
        let page = palloc(len) as *mut c_char;
        core::ptr::copy_nonoverlapping((*xlogreader).readBuf, page, len);

        (*result).lastPageBeginPtr = pageBeginPtr;
        (*result).lastPage = page;
    } else {
        /* There is no partial block to copy. */
        (*result).lastPageBeginPtr = endOfLog;
        (*result).lastPage = null_mut();
    }

    /*
     * Create a comment for the history file to explain why and where timeline
     * changed.
     */
    (*result).recoveryStopReason = getRecoveryStopReason();

    (*result).lastRec = lastRec;
    (*result).lastRecTLI = lastRecTLI;
    (*result).endOfLog = endOfLog;

    (*result).abortedRecPtr = abortedRecPtr;
    (*result).missingContrecPtr = missingContrecPtr;

    (*result).standby_signal_file_found = standby_signal_file_found;
    (*result).recovery_signal_file_found = recovery_signal_file_found;

    result
}

/*
 * Clean up the WAL reader and leftovers from restoring WAL from archive
 */
pub unsafe fn ShutdownWalRecovery() {
    let recoveryPath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

    /* Final update of pg_stat_recovery_prefetch. */
    XLogPrefetcherComputeStats(xlogprefetcher);

    /* Shut down xlogreader */
    if readFile >= 0 {
        c_close(readFile);
        readFile = -1;
    }
    XLogReaderFree(xlogreader);
    XLogPrefetcherFree(xlogprefetcher);

    if ArchiveRecoveryRequested {
        /*
         * Since there might be a partial WAL segment named RECOVERYXLOG, get
         * rid of it.
         */
        /* snprintf(recoveryPath, MAXPGPATH, "%s/RECOVERYXLOG", XLOGDIR) */
        c_unlink(recoveryPath.as_ptr());

        /* Get rid of any remaining recovered timeline-history file, too */
        /* snprintf(recoveryPath, MAXPGPATH, "%s/RECOVERYHISTORY", XLOGDIR) */
        c_unlink(recoveryPath.as_ptr());
    }

    /*
     * We don't need the latch anymore.
     */
    if ArchiveRecoveryRequested {
        DisownLatch(&mut (*XLogRecoveryCtl).recoveryWakeupLatch);
    }
}


// ---------------------------------------------------------------------------
// Part 5: PerformWalRecovery, ApplyWalRecord, xlogrecovery_redo,
//         CheckTablespaceDirectory, CheckRecoveryConsistency,
//         rm_redo_error_callback, xlog_outdesc, xlog_block_info
// ---------------------------------------------------------------------------

/*
 * Perform WAL recovery.
 *
 * If the system was shut down cleanly, this is never called.
 */
pub unsafe fn PerformWalRecovery() {
    let mut record: *mut XLogRecord;
    let mut reachedRecoveryTarget: bool = false;
    let mut replayTLI: TimeLineID;

    /*
     * Initialize shared variables for tracking progress of WAL replay.
     */
    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck);
    if RedoStartLSN < CheckPointLoc {
        (*XLogRecoveryCtl).lastReplayedReadRecPtr = InvalidXLogRecPtr;
        (*XLogRecoveryCtl).lastReplayedEndRecPtr = RedoStartLSN;
        (*XLogRecoveryCtl).lastReplayedTLI = RedoStartTLI;
    } else {
        (*XLogRecoveryCtl).lastReplayedReadRecPtr = (*xlogreader).ReadRecPtr;
        (*XLogRecoveryCtl).lastReplayedEndRecPtr = (*xlogreader).EndRecPtr;
        (*XLogRecoveryCtl).lastReplayedTLI = CheckPointTLI;
    }
    (*XLogRecoveryCtl).replayEndRecPtr = (*XLogRecoveryCtl).lastReplayedEndRecPtr;
    (*XLogRecoveryCtl).replayEndTLI = (*XLogRecoveryCtl).lastReplayedTLI;
    (*XLogRecoveryCtl).recoveryLastXTime = 0;
    (*XLogRecoveryCtl).currentChunkStartTime = 0;
    (*XLogRecoveryCtl).recoveryPauseState = RECOVERY_NOT_PAUSED;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck);

    /* Also ensure XLogReceiptTime has a sane value */
    XLogReceiptTime = GetCurrentTimestamp();

    /*
     * Let postmaster know we've started redo now.
     */
    if IsUnderPostmaster {
        SendPostmasterSignal(PMSIGNAL_RECOVERY_STARTED);
    }

    /*
     * Allow read-only connections immediately if we're consistent already.
     */
    CheckRecoveryConsistency();

    /*
     * Find the first record that logically follows the checkpoint.
     */
    if RedoStartLSN < CheckPointLoc {
        /* back up to find the record */
        replayTLI = RedoStartTLI;
        XLogPrefetcherBeginRead(xlogprefetcher, RedoStartLSN);
        record = ReadRecord(xlogprefetcher, PANIC, false, replayTLI);

        /*
         * If a checkpoint record's redo pointer points back to an earlier
         * LSN, the record at that LSN should be an XLOG_CHECKPOINT_REDO record.
         */
        if (*record).xl_rmid != RM_XLOG_ID
            || ((*record).xl_info & !XLR_INFO_MASK) != XLOG_CHECKPOINT_REDO
        {
            ereport!(FATAL, errmsg!(
                "unexpected record type found at redo point {}/{:X}",
                lsn_hi((*xlogreader).ReadRecPtr),
                lsn_lo((*xlogreader).ReadRecPtr)
            ));
        }
    } else {
        /* just have to read next record after CheckPoint */
        Assert!((*xlogreader).ReadRecPtr == CheckPointLoc);
        replayTLI = CheckPointTLI;
        record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
    }

    if !record.is_null() {
        let xtime: TimestampTz;
        let mut ru0: PGRUsage = core::mem::zeroed();

        pg_rusage_init(&mut ru0);

        InRedo = true;

        RmgrStartup();

        ereport!(LOG, errmsg!("redo starts at {}/{:X}",
            lsn_hi((*xlogreader).ReadRecPtr),
            lsn_lo((*xlogreader).ReadRecPtr)));

        /* Prepare to report progress of the redo phase. */
        if !StandbyMode {
            begin_startup_progress_phase();
        }

        /*
         * main redo apply loop
         */
        loop {
            if !StandbyMode {
                ereport_startup_progress(b"redo in progress\0".as_ptr() as *const c_char);
            }

            /* #ifdef WAL_DEBUG ... #endif  -- WAL_DEBUG branch omitted (not enabled) */

            /* Handle interrupt signals of startup process */
            ProcessStartupProcInterrupts();

            /*
             * Pause WAL replay, if requested by a hot-standby session via
             * SetRecoveryPause().
             */
            if (*(XLogRecoveryCtl as *const XLogRecoveryCtlData)).recoveryPauseState
                != RECOVERY_NOT_PAUSED
            {
                recoveryPausesHere(false);
            }

            /*
             * Have we reached our recovery target?
             */
            if recoveryStopsBefore(xlogreader) {
                reachedRecoveryTarget = true;
                break;
            }

            /*
             * If we've been asked to lag the primary, wait on latch until
             * enough time has passed.
             */
            if recoveryApplyDelay(xlogreader) {
                /*
                 * We test for paused recovery again here.
                 */
                if (*(XLogRecoveryCtl as *const XLogRecoveryCtlData)).recoveryPauseState
                    != RECOVERY_NOT_PAUSED
                {
                    recoveryPausesHere(false);
                }
            }

            /*
             * Apply the record
             */
            ApplyWalRecord(xlogreader, record, &mut replayTLI);

            /* Exit loop if we reached inclusive recovery target */
            if recoveryStopsAfter(xlogreader) {
                reachedRecoveryTarget = true;
                break;
            }

            /* Else, try to fetch the next WAL record */
            record = ReadRecord(xlogprefetcher, LOG, false, replayTLI);
            if record.is_null() {
                break;
            }
        }

        /*
         * end of main redo apply loop
         */

        if reachedRecoveryTarget {
            if !reachedConsistency {
                ereport!(FATAL, errmsg!("requested recovery stop point is before consistent recovery point"));
            }

            /*
             * This is the last point where we can restart recovery with a new
             * recovery target.
             */
            match recoveryTargetAction {
                RECOVERY_TARGET_ACTION_SHUTDOWN => {
                    /*
                     * exit with special return code to request shutdown of postmaster.
                     */
                    proc_exit(3);
                }
                RECOVERY_TARGET_ACTION_PAUSE => {
                    SetRecoveryPause(true);
                    recoveryPausesHere(true);
                    /* drop into promote */
                    /* (fall through - Rust has no fallthrough; PROMOTE is a no-op break) */
                }
                _ => { /* RECOVERY_TARGET_ACTION_PROMOTE: break */ }
            }
        }

        RmgrCleanup();

        ereport!(LOG, errmsg!("redo done at {}/{:X} system usage: {}",
            lsn_hi((*xlogreader).ReadRecPtr),
            lsn_lo((*xlogreader).ReadRecPtr),
            core::ffi::CStr::from_ptr(pg_rusage_show(&ru0)).to_string_lossy()));
        xtime = GetLatestXTime();
        if xtime != 0 {
            ereport!(LOG, errmsg!("last completed transaction was at log time {}",
                core::ffi::CStr::from_ptr(timestamptz_to_str(xtime)).to_string_lossy()));
        }

        InRedo = false;
    } else {
        /* there are no WAL records following the checkpoint */
        ereport!(LOG, errmsg!("redo is not required"));
    }

    /*
     * This check is intentionally after the above log messages.
     */
    if ArchiveRecoveryRequested
        && recoveryTarget != RECOVERY_TARGET_UNSET
        && !reachedRecoveryTarget
    {
        ereport!(FATAL, errmsg!("recovery ended before configured recovery target was reached"));
        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
    }
}

/*
 * Subroutine of PerformWalRecovery, to apply one WAL record.
 */
unsafe fn ApplyWalRecord(
    xlogreader_arg: *mut XLogReaderState,
    record: *mut XLogRecord,
    replayTLI: *mut TimeLineID,
) {
    let mut errcallback = ErrorContextCallback {
        callback: rm_redo_error_callback,
        arg: xlogreader_arg as *mut c_void,
        previous: error_context_stack,
    };
    let mut switchedTLI: bool = false;

    /* Setup error traceback support for ereport() */
    errcallback.previous = error_context_stack;
    error_context_stack = &mut errcallback;

    /*
     * TransamVariables->nextXid must be beyond record's xid.
     */
    AdvanceNextFullTransactionIdPastXid((*record).xl_xid);

    /*
     * Before replaying this record, check if this record causes the current
     * timeline to change.
     */
    if (*record).xl_rmid == RM_XLOG_ID {
        let mut newReplayTLI: TimeLineID = *replayTLI;
        let mut prevReplayTLI: TimeLineID = *replayTLI;
        let info: u8 = (*record).xl_info & !XLR_INFO_MASK;

        if info == XLOG_CHECKPOINT_SHUTDOWN {
            let mut checkPoint: CheckPoint = core::mem::zeroed();
            core::ptr::copy_nonoverlapping(
                XLogRecGetData(xlogreader_arg) as *const u8,
                &mut checkPoint as *mut CheckPoint as *mut u8,
                size_of::<CheckPoint>(),
            );
            newReplayTLI = checkPoint.ThisTimeLineID;
            prevReplayTLI = checkPoint.PrevTimeLineID;
        } else if info == XLOG_END_OF_RECOVERY {
            let mut xlrec: xl_end_of_recovery = core::mem::zeroed();
            core::ptr::copy_nonoverlapping(
                XLogRecGetData(xlogreader_arg) as *const u8,
                &mut xlrec as *mut xl_end_of_recovery as *mut u8,
                size_of::<xl_end_of_recovery>(),
            );
            newReplayTLI = xlrec.ThisTimeLineID;
            prevReplayTLI = xlrec.PrevTimeLineID;
        }

        if newReplayTLI != *replayTLI {
            /* Check that it's OK to switch to this TLI */
            checkTimeLineSwitch((*xlogreader_arg).EndRecPtr, newReplayTLI, prevReplayTLI, *replayTLI);

            /* Following WAL records should be run with new TLI */
            *replayTLI = newReplayTLI;
            switchedTLI = true;
        }
    }

    /*
     * Update shared replayEndRecPtr before replaying this record, so that
     * XLogFlush will update minRecoveryPoint correctly.
     */
    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck);
    (*XLogRecoveryCtl).replayEndRecPtr = (*xlogreader_arg).EndRecPtr;
    (*XLogRecoveryCtl).replayEndTLI = *replayTLI;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck);

    /*
     * If we are attempting to enter Hot Standby mode, process XIDs we see
     */
    if standbyState >= STANDBY_INITIALIZED && TransactionIdIsValid((*record).xl_xid) {
        RecordKnownAssignedTransactionIds((*record).xl_xid);
    }

    /*
     * Some XLOG record types that are related to recovery are processed
     * directly here, rather than in xlog_redo()
     */
    if (*record).xl_rmid == RM_XLOG_ID {
        xlogrecovery_redo(xlogreader_arg, *replayTLI);
    }

    /* Now apply the WAL record itself */
    (GetRmgr((*record).xl_rmid).rm_redo)(xlogreader_arg);

    /*
     * After redo, check whether the backup pages associated with the WAL
     * record are consistent with the existing pages.
     */
    if ((*record).xl_info & XLR_CHECK_CONSISTENCY) != 0 {
        verifyBackupPageConsistency(xlogreader_arg);
    }

    /* Pop the error context stack */
    error_context_stack = errcallback.previous;

    /*
     * Update lastReplayedEndRecPtr after this record has been successfully
     * replayed.
     */
    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck);
    (*XLogRecoveryCtl).lastReplayedReadRecPtr = (*xlogreader_arg).ReadRecPtr;
    (*XLogRecoveryCtl).lastReplayedEndRecPtr = (*xlogreader_arg).EndRecPtr;
    (*XLogRecoveryCtl).lastReplayedTLI = *replayTLI;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck);

    /* ------
     * Wakeup walsenders:
     *
     * Physical walsenders don't need to be woken up during replay unless
     * cascading replication is allowed and time line change occurred.
     * ------
     */
    if AllowCascadeReplication() {
        WalSndWakeup(switchedTLI, true);
    }

    /*
     * If rm_redo called XLogRequestWalReceiverReply, then we wake up the
     * receiver so that it notices the updated lastReplayedEndRecPtr.
     */
    if doRequestWalReceiverReply {
        doRequestWalReceiverReply = false;
        WalRcvForceReply();
    }

    /* Allow read-only connections if we're consistent now */
    CheckRecoveryConsistency();

    /* Is this a timeline switch? */
    if switchedTLI {
        /*
         * Before we continue on the new timeline, clean up any (possibly
         * bogus) future WAL segments on the old timeline.
         */
        RemoveNonParentXlogFiles((*xlogreader_arg).EndRecPtr, *replayTLI);

        /* Reset the prefetcher. */
        XLogPrefetchReconfigure();
    }
}

/*
 * Some XLOG RM record types that are directly related to WAL recovery are
 * handled here rather than in the xlog_redo()
 */
unsafe fn xlogrecovery_redo(record: *mut XLogReaderState, replayTLI: TimeLineID) {
    let info: u8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    let lsn: XLogRecPtr = (*record).EndRecPtr;

    Assert!(XLogRecGetRmid(record) == RM_XLOG_ID);

    if info == XLOG_OVERWRITE_CONTRECORD {
        /* Verify the payload of a XLOG_OVERWRITE_CONTRECORD record. */
        let mut xlrec: xl_overwrite_contrecord = core::mem::zeroed();
        core::ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut xlrec as *mut xl_overwrite_contrecord as *mut u8,
            size_of::<xl_overwrite_contrecord>(),
        );
        if xlrec.overwritten_lsn != (*record).overwrittenRecPtr {
            elog!(FATAL, "mismatching overwritten LSN {}/{:X} -> {}/{:X}",
                lsn_hi(xlrec.overwritten_lsn), lsn_lo(xlrec.overwritten_lsn),
                lsn_hi((*record).overwrittenRecPtr), lsn_lo((*record).overwrittenRecPtr));
        }

        /* We have safely skipped the aborted record */
        abortedRecPtr = InvalidXLogRecPtr;
        missingContrecPtr = InvalidXLogRecPtr;

        ereport!(LOG, errmsg!(
            "successfully skipped missing contrecord at {}/{:X}, overwritten at ...",
            lsn_hi(xlrec.overwritten_lsn), lsn_lo(xlrec.overwritten_lsn)
        ));

        /* Verifying the record should only happen once */
        (*record).overwrittenRecPtr = InvalidXLogRecPtr;
    } else if info == XLOG_BACKUP_END {
        let mut startpoint: XLogRecPtr = 0;
        core::ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut startpoint as *mut XLogRecPtr as *mut u8,
            size_of::<XLogRecPtr>(),
        );

        if backupStartPoint == startpoint {
            /*
             * We have reached the end of base backup.
             */
            elog!(DEBUG1, "end of backup record reached");
            backupEndPoint = lsn;
        } else {
            elog!(DEBUG1, "saw end-of-backup record for backup starting at {}/{:X}, waiting for {}/{:X}",
                lsn_hi(startpoint), lsn_lo(startpoint),
                lsn_hi(backupStartPoint), lsn_lo(backupStartPoint));
        }
    }
}

/*
 * Verify that, in non-test mode, ./pg_tblspc doesn't contain any real
 * directories.
 */
unsafe fn CheckTablespaceDirectory() {
    let dir = AllocateDir(PG_TBLSPC_DIR as *const c_char);
    loop {
        let de = ReadDir(dir, PG_TBLSPC_DIR as *const c_char);
        if de.is_null() {
            break;
        }
        let path: [c_char; MAXPGPATH + 16] = [0; MAXPGPATH + 16];

        /* Skip entries of non-oid names */
        let name_len = c_strlen((*de).d_name.as_ptr());
        let digit_len = c_strspn((*de).d_name.as_ptr(), b"0123456789\0".as_ptr() as *const c_char);
        if digit_len != name_len {
            continue;
        }

        /* snprintf(path, sizeof(path), "%s/%s", PG_TBLSPC_DIR, de->d_name) */

        if get_dirent_type(path.as_ptr(), de, false, ERROR) != PGFILETYPE_LNK {
            ereport!(
                if allow_in_place_tablespaces { WARNING } else { PANIC },
                errmsg!("unexpected directory entry found in pg_tblspc")
            );
            /* C also: errcode, errdetail, errhint */
        }
    }
}

/*
 * Checks if recovery has reached a consistent state. When consistency is
 * reached and we have a valid starting standby snapshot, tell postmaster
 * that it can start accepting read-only connections.
 */
unsafe fn CheckRecoveryConsistency() {
    let lastReplayedEndRecPtr: XLogRecPtr;
    let lastReplayedTLI: TimeLineID;

    /*
     * During crash recovery, we don't reach a consistent state until we've
     * replayed all the WAL.
     */
    if XLogRecPtrIsInvalid(minRecoveryPoint) {
        return;
    }

    Assert!(InArchiveRecovery);

    /*
     * assume that we are called in the startup process, and hence don't need
     * a lock to read lastReplayedEndRecPtr
     */
    lastReplayedEndRecPtr = (*XLogRecoveryCtl).lastReplayedEndRecPtr;
    lastReplayedTLI = (*XLogRecoveryCtl).lastReplayedTLI;

    /*
     * Have we reached the point where our base backup was completed?
     */
    if !XLogRecPtrIsInvalid(backupEndPoint) && backupEndPoint <= lastReplayedEndRecPtr {
        let saveBackupStartPoint = backupStartPoint;
        let saveBackupEndPoint = backupEndPoint;

        elog!(DEBUG1, "end of backup reached");

        /*
         * We have reached the end of base backup, as indicated by pg_control.
         * Update the control file accordingly.
         */
        ReachedEndOfBackup(lastReplayedEndRecPtr, lastReplayedTLI);
        backupStartPoint = InvalidXLogRecPtr;
        backupEndPoint = InvalidXLogRecPtr;
        backupEndRequired = false;

        ereport!(LOG, errmsg!(
            "completed backup recovery with redo LSN {}/{:X} and end LSN {}/{:X}",
            lsn_hi(saveBackupStartPoint), lsn_lo(saveBackupStartPoint),
            lsn_hi(saveBackupEndPoint), lsn_lo(saveBackupEndPoint)
        ));
    }

    /*
     * Have we passed our safe starting point?
     */
    if !reachedConsistency && !backupEndRequired && minRecoveryPoint <= lastReplayedEndRecPtr {
        /*
         * Check to see if the XLOG sequence contained any unresolved
         * references to uninitialized pages.
         */
        XLogCheckInvalidPages();

        /*
         * Check that pg_tblspc doesn't contain any real directories.
         */
        CheckTablespaceDirectory();

        reachedConsistency = true;
        SendPostmasterSignal(PMSIGNAL_RECOVERY_CONSISTENT);
        ereport!(LOG, errmsg!("consistent recovery state reached at {}/{:X}",
            lsn_hi(lastReplayedEndRecPtr), lsn_lo(lastReplayedEndRecPtr)));
    }

    /*
     * Have we got a valid starting snapshot that will allow queries to be run?
     */
    if standbyState == STANDBY_SNAPSHOT_READY
        && !LocalHotStandbyActive
        && reachedConsistency
        && IsUnderPostmaster
    {
        SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck);
        (*XLogRecoveryCtl).SharedHotStandbyActive = true;
        SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck);

        LocalHotStandbyActive = true;

        SendPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY);
    }
}

/*
 * Error context callback for errors occurring during rm_redo().
 */
unsafe fn rm_redo_error_callback(arg: *mut c_void) {
    let record = arg as *mut XLogReaderState;
    let mut buf: StringInfoData = core::mem::zeroed();

    initStringInfo(&mut buf);
    xlog_outdesc(&mut buf, record);
    xlog_block_info(&mut buf, record);

    /* translator: %s is a WAL record description */
    errcontext!("WAL redo at {}/{:X} for {}",
        lsn_hi((*record).ReadRecPtr),
        lsn_lo((*record).ReadRecPtr),
        core::ffi::CStr::from_ptr(buf.data).to_string_lossy());

    pfree(buf.data as *mut c_void);
}

/*
 * Returns a string describing an XLogRecord, consisting of its identity
 * optionally followed by a colon, a space, and a further description.
 */
pub unsafe fn xlog_outdesc(buf: *mut StringInfoData, record: *mut XLogReaderState) {
    let rmgr = GetRmgr(XLogRecGetRmid(record));
    let info: u8 = XLogRecGetInfo(record);
    let id: *const c_char;

    appendStringInfoString(buf, rmgr.rm_name);
    appendStringInfoChar(buf, b'/' as c_char);

    id = (rmgr.rm_identify)(info);
    if id.is_null() {
        appendStringInfo(buf, b"UNKNOWN (%X): \0".as_ptr() as *const c_char);
        /* info & !XLR_INFO_MASK */
    } else {
        appendStringInfo(buf, b"%s: \0".as_ptr() as *const c_char);
        /* id */
    }

    (rmgr.rm_desc)(buf, record);
}

/* #ifdef WAL_DEBUG */
/* xlog_outrec is only compiled under WAL_DEBUG - translate as dead code */
#[cfg(any())]
unsafe fn xlog_outrec(buf: *mut StringInfoData, record: *mut XLogReaderState) {
    appendStringInfo(buf, b"prev %X/%X; xid %u\0".as_ptr() as *const c_char);
    /* LSN_FORMAT_ARGS(XLogRecGetPrev(record)), XLogRecGetXid(record) */
    appendStringInfo(buf, b"; len %u\0".as_ptr() as *const c_char);
    /* XLogRecGetDataLen(record) */
    xlog_block_info(buf, record);
}
/* #endif WAL_DEBUG */

/*
 * Returns a string giving information about all the blocks in an XLogRecord.
 */
unsafe fn xlog_block_info(buf: *mut StringInfoData, record: *mut XLogReaderState) {
    let mut block_id: c_int;

    /* decode block references */
    block_id = 0;
    while block_id <= XLogRecMaxBlockId(record) {
        let mut rlocator: RelFileLocator = core::mem::zeroed();
        let mut forknum: c_int = 0;
        let mut blk: u32 = 0;

        if !XLogRecGetBlockTagExtended(record, block_id, &mut rlocator, &mut forknum, &mut blk, null_mut()) {
            block_id += 1;
            continue;
        }

        if forknum != MAIN_FORKNUM {
            appendStringInfo(buf, b"; blkref #%d: rel %u/%u/%u, fork %u, blk %u\0".as_ptr() as *const c_char);
            /* block_id, rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, forknum, blk */
        } else {
            appendStringInfo(buf, b"; blkref #%d: rel %u/%u/%u, blk %u\0".as_ptr() as *const c_char);
            /* block_id, rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, blk */
        }
        if XLogRecHasBlockImage(record, block_id) {
            appendStringInfoString(buf, b" FPW\0".as_ptr() as *const c_char);
        }
        block_id += 1;
    }
}


// ---------------------------------------------------------------------------
// Part 6: checkTimeLineSwitch, getRecordTimestamp, verifyBackupPageConsistency,
//         recoveryStopsBefore, recoveryStopsAfter, getRecoveryStopReason,
//         recoveryPausesHere, recoveryApplyDelay,
//         GetRecoveryPauseState, SetRecoveryPause, ConfirmRecoveryPaused,
//         ReadRecord, XLogPageRead (first half)
// ---------------------------------------------------------------------------

/*
 * Check that it's OK to switch to new timeline during recovery.
 */
unsafe fn checkTimeLineSwitch(
    lsn: XLogRecPtr,
    newTLI: TimeLineID,
    prevTLI: TimeLineID,
    replayTLI: TimeLineID,
) {
    /* Check that the record agrees on what the current (old) timeline is */
    if prevTLI != replayTLI {
        ereport!(PANIC, errmsg!(
            "unexpected previous timeline ID {} (current timeline ID {}) in checkpoint record",
            prevTLI, replayTLI
        ));
    }

    /*
     * The new timeline better be in the list of timelines we expect to see,
     * according to the timeline history. It should also not decrease.
     */
    if newTLI < replayTLI || !tliInHistory(newTLI, expectedTLEs) {
        ereport!(PANIC, errmsg!(
            "unexpected timeline ID {} (after {}) in checkpoint record",
            newTLI, replayTLI
        ));
    }

    /*
     * If we have not yet reached min recovery point, and we're about to
     * switch to a timeline greater than the timeline of the min recovery
     * point: trouble.
     */
    if !XLogRecPtrIsInvalid(minRecoveryPoint)
        && lsn < minRecoveryPoint
        && newTLI > minRecoveryPointTLI
    {
        ereport!(PANIC, errmsg!(
            "unexpected timeline ID {} in checkpoint record, before reaching minimum recovery point {}/{:X} on timeline {}",
            newTLI,
            lsn_hi(minRecoveryPoint), lsn_lo(minRecoveryPoint),
            minRecoveryPointTLI
        ));
    }

    /* Looks good */
}

/*
 * Extract timestamp from WAL record.
 *
 * If the record contains a timestamp, returns true, and saves the timestamp
 * in *recordXtime. If the record type has no timestamp, returns false.
 */
unsafe fn getRecordTimestamp(record: *mut XLogReaderState, recordXtime: *mut TimestampTz) -> bool {
    let info: u8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    let xact_info: u8 = info & XLOG_XACT_OPMASK;
    let rmid: u8 = XLogRecGetRmid(record);

    if rmid == RM_XLOG_ID && info == XLOG_RESTORE_POINT {
        *recordXtime = (*(XLogRecGetData(record) as *mut xl_restore_point)).rp_time;
        return true;
    }
    if rmid == RM_XACT_ID
        && (xact_info == XLOG_XACT_COMMIT || xact_info == XLOG_XACT_COMMIT_PREPARED)
    {
        *recordXtime = (*(XLogRecGetData(record) as *mut xl_xact_commit)).xact_time;
        return true;
    }
    if rmid == RM_XACT_ID
        && (xact_info == XLOG_XACT_ABORT || xact_info == XLOG_XACT_ABORT_PREPARED)
    {
        *recordXtime = (*(XLogRecGetData(record) as *mut xl_xact_abort)).xact_time;
        return true;
    }
    false
}

/*
 * Checks whether the current buffer page and backup page stored in the
 * WAL record are consistent or not.
 */
unsafe fn verifyBackupPageConsistency(record: *mut XLogReaderState) {
    let rmgr = GetRmgr(XLogRecGetRmid(record));
    let mut rlocator: RelFileLocator = core::mem::zeroed();
    let mut forknum: c_int = 0;
    let mut blkno: u32 = 0;
    let mut block_id: c_int;

    /* Records with no backup blocks have no need for consistency checks. */
    if !XLogRecHasAnyBlockRefs(record) {
        return;
    }

    Assert!((XLogRecGetInfo(record) & XLR_CHECK_CONSISTENCY) != 0);

    block_id = 0;
    while block_id <= XLogRecMaxBlockId(record) {
        let buf: c_int;
        let page: *mut c_void;

        if !XLogRecGetBlockTagExtended(record, block_id, &mut rlocator, &mut forknum, &mut blkno, null_mut()) {
            /*
             * WAL record doesn't contain a block reference with the given id.
             * Do nothing.
             */
            block_id += 1;
            continue;
        }

        Assert!(XLogRecHasBlockImage(record, block_id));

        if XLogRecBlockImageApply(record, block_id) {
            /*
             * WAL record has already applied the page, so bypass the
             * consistency check.
             */
            block_id += 1;
            continue;
        }

        /*
         * Read the contents from the current buffer and store it in a
         * temporary page.
         */
        buf = XLogReadBufferExtended(rlocator, forknum, blkno, RBM_NORMAL_NO_LOG, InvalidBuffer);
        if !BufferIsValid(buf) {
            block_id += 1;
            continue;
        }

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);

        /*
         * Take a copy of the local page where WAL has been applied.
         */
        core::ptr::copy_nonoverlapping(page as *const u8, replay_image_masked as *mut u8, BLCKSZ);

        /* No need for this page anymore now that a copy is in. */
        UnlockReleaseBuffer(buf);

        /*
         * If the block LSN is already ahead of this WAL record, we can't
         * expect contents to match.
         */
        if PageGetLSN(replay_image_masked as *mut c_void) > (*record).EndRecPtr {
            block_id += 1;
            continue;
        }

        /*
         * Read the contents from the backup copy, stored in WAL record.
         */
        if !RestoreBlockImage(record, block_id, primary_image_masked) {
            ereport!(ERROR, errmsg_internal!("could not restore block image"));
            /* C uses record->errormsg_buf */
        }

        /*
         * If masking function is defined, mask both the primary and replay images
         */
        if let Some(rm_mask) = rmgr.rm_mask {
            rm_mask(replay_image_masked, blkno);
            rm_mask(primary_image_masked, blkno);
        }

        /* Time to compare the primary and replay images. */
        if core::slice::from_raw_parts(replay_image_masked as *const u8, BLCKSZ)
            != core::slice::from_raw_parts(primary_image_masked as *const u8, BLCKSZ)
        {
            elog!(FATAL,
                "inconsistent page found, rel {}/{}/{}, forknum {}, blkno {}",
                rlocator.spcOid, rlocator.dbOid, rlocator.relNumber,
                forknum, blkno);
        }
        block_id += 1;
    }
}

/*
 * For point-in-time recovery, this function decides whether we want to
 * stop applying the XLOG before the current record.
 */
unsafe fn recoveryStopsBefore(record: *mut XLogReaderState) -> bool {
    let mut stopsHere: bool = false;
    let xact_info: u8;
    let isCommit: bool;
    let mut recordXtime: TimestampTz = 0;
    let recordXid: TransactionId;

    /*
     * Ignore recovery target settings when not in archive recovery.
     */
    if !ArchiveRecoveryRequested {
        return false;
    }

    /* Check if we should stop as soon as reaching consistency */
    if recoveryTarget == RECOVERY_TARGET_IMMEDIATE && reachedConsistency {
        ereport!(LOG, errmsg!("recovery stopping after reaching consistency"));
        recoveryStopAfter = false;
        recoveryStopXid = 0; /* InvalidTransactionId */
        recoveryStopLSN = InvalidXLogRecPtr;
        recoveryStopTime = 0;
        recoveryStopName[0] = 0;
        return true;
    }

    /* Check if target LSN has been reached */
    if recoveryTarget == RECOVERY_TARGET_LSN
        && !recoveryTargetInclusive
        && (*record).ReadRecPtr >= recoveryTargetLSN
    {
        recoveryStopAfter = false;
        recoveryStopXid = 0;
        recoveryStopLSN = (*record).ReadRecPtr;
        recoveryStopTime = 0;
        recoveryStopName[0] = 0;
        ereport!(LOG, errmsg!("recovery stopping before WAL location (LSN) \"{}/{:X}\"",
            lsn_hi(recoveryStopLSN), lsn_lo(recoveryStopLSN)));
        return true;
    }

    /* Otherwise we only consider stopping before COMMIT or ABORT records. */
    if XLogRecGetRmid(record) != RM_XACT_ID {
        return false;
    }

    xact_info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

    if xact_info == XLOG_XACT_COMMIT {
        isCommit = true;
        recordXid = XLogRecGetXid(record);
    } else if xact_info == XLOG_XACT_COMMIT_PREPARED {
        let xlrec = XLogRecGetData(record) as *mut xl_xact_commit;
        let mut parsed: xl_xact_parsed_commit = core::mem::zeroed();
        isCommit = true;
        ParseCommitRecord(XLogRecGetInfo(record), xlrec, &mut parsed);
        recordXid = parsed.twophase_xid;
    } else if xact_info == XLOG_XACT_ABORT {
        isCommit = false;
        recordXid = XLogRecGetXid(record);
    } else if xact_info == XLOG_XACT_ABORT_PREPARED {
        let xlrec = XLogRecGetData(record) as *mut xl_xact_abort;
        let mut parsed: xl_xact_parsed_abort = core::mem::zeroed();
        isCommit = false;
        ParseAbortRecord(XLogRecGetInfo(record), xlrec, &mut parsed);
        recordXid = parsed.twophase_xid;
    } else {
        return false;
    }

    if recoveryTarget == RECOVERY_TARGET_XID && !recoveryTargetInclusive {
        /*
         * There can be only one transaction end record with this exact
         * transactionid.
         */
        stopsHere = recordXid == recoveryTargetXid;
    }

    /*
     * Note: we must fetch recordXtime regardless of recoveryTarget setting.
     */
    if getRecordTimestamp(record, &mut recordXtime) && recoveryTarget == RECOVERY_TARGET_TIME {
        /*
         * There can be many transactions that share the same commit time,
         * so we stop after the last one, if we are inclusive, or stop at the
         * first one if we are exclusive.
         */
        if recoveryTargetInclusive {
            stopsHere = recordXtime > recoveryTargetTime;
        } else {
            stopsHere = recordXtime >= recoveryTargetTime;
        }
    }

    if stopsHere {
        recoveryStopAfter = false;
        recoveryStopXid = recordXid;
        recoveryStopTime = recordXtime;
        recoveryStopLSN = InvalidXLogRecPtr;
        recoveryStopName[0] = 0;

        if isCommit {
            ereport!(LOG, errmsg!(
                "recovery stopping before commit of transaction {}, time {}",
                recoveryStopXid,
                core::ffi::CStr::from_ptr(timestamptz_to_str(recoveryStopTime)).to_string_lossy()
            ));
        } else {
            ereport!(LOG, errmsg!(
                "recovery stopping before abort of transaction {}, time {}",
                recoveryStopXid,
                core::ffi::CStr::from_ptr(timestamptz_to_str(recoveryStopTime)).to_string_lossy()
            ));
        }
    }

    stopsHere
}

/*
 * Same as recoveryStopsBefore, but called after applying the record.
 */
unsafe fn recoveryStopsAfter(record: *mut XLogReaderState) -> bool {
    let info: u8;
    let xact_info: u8;
    let rmid: u8;
    let mut recordXtime: TimestampTz = 0;

    /*
     * Ignore recovery target settings when not in archive recovery.
     */
    if !ArchiveRecoveryRequested {
        return false;
    }

    info = XLogRecGetInfo(record) & !XLR_INFO_MASK;
    rmid = XLogRecGetRmid(record);

    /*
     * There can be many restore points that share the same name; we stop at
     * the first one.
     */
    if recoveryTarget == RECOVERY_TARGET_NAME
        && rmid == RM_XLOG_ID
        && info == XLOG_RESTORE_POINT
    {
        let recordRestorePointData = XLogRecGetData(record) as *mut xl_restore_point;

        /* strcmp(recordRestorePointData->rp_name, recoveryTargetName) */
        let rp_name = core::ffi::CStr::from_ptr((*recordRestorePointData).rp_name.as_ptr());
        let target_name = core::ffi::CStr::from_ptr(recoveryTargetName);
        if rp_name == target_name {
            recoveryStopAfter = true;
            recoveryStopXid = 0;
            recoveryStopLSN = InvalidXLogRecPtr;
            getRecordTimestamp(record, &mut recoveryStopTime);
            strlcpy(
                recoveryStopName.as_mut_ptr(),
                (*recordRestorePointData).rp_name.as_ptr(),
                MAXFNAMELEN,
            );

            ereport!(LOG, errmsg!(
                "recovery stopping at restore point \"{}\", time {}",
                rp_name.to_string_lossy(),
                core::ffi::CStr::from_ptr(timestamptz_to_str(recoveryStopTime)).to_string_lossy()
            ));
            return true;
        }
    }

    /* Check if the target LSN has been reached */
    if recoveryTarget == RECOVERY_TARGET_LSN
        && recoveryTargetInclusive
        && (*record).ReadRecPtr >= recoveryTargetLSN
    {
        recoveryStopAfter = true;
        recoveryStopXid = 0;
        recoveryStopLSN = (*record).ReadRecPtr;
        recoveryStopTime = 0;
        recoveryStopName[0] = 0;
        ereport!(LOG, errmsg!("recovery stopping after WAL location (LSN) \"{}/{:X}\"",
            lsn_hi(recoveryStopLSN), lsn_lo(recoveryStopLSN)));
        return true;
    }

    if rmid != RM_XACT_ID {
        return false;
    }

    xact_info = info & XLOG_XACT_OPMASK;

    if xact_info == XLOG_XACT_COMMIT
        || xact_info == XLOG_XACT_COMMIT_PREPARED
        || xact_info == XLOG_XACT_ABORT
        || xact_info == XLOG_XACT_ABORT_PREPARED
    {
        let recordXid: TransactionId;

        /* Update the last applied transaction timestamp */
        if getRecordTimestamp(record, &mut recordXtime) {
            SetLatestXTime(recordXtime);
        }

        /* Extract the XID of the committed/aborted transaction */
        if xact_info == XLOG_XACT_COMMIT_PREPARED {
            let xlrec = XLogRecGetData(record) as *mut xl_xact_commit;
            let mut parsed: xl_xact_parsed_commit = core::mem::zeroed();
            ParseCommitRecord(XLogRecGetInfo(record), xlrec, &mut parsed);
            recordXid = parsed.twophase_xid;
        } else if xact_info == XLOG_XACT_ABORT_PREPARED {
            let xlrec = XLogRecGetData(record) as *mut xl_xact_abort;
            let mut parsed: xl_xact_parsed_abort = core::mem::zeroed();
            ParseAbortRecord(XLogRecGetInfo(record), xlrec, &mut parsed);
            recordXid = parsed.twophase_xid;
        } else {
            recordXid = XLogRecGetXid(record);
        }

        /*
         * There can be only one transaction end record with this exact
         * transactionid.
         */
        if recoveryTarget == RECOVERY_TARGET_XID
            && recoveryTargetInclusive
            && recordXid == recoveryTargetXid
        {
            recoveryStopAfter = true;
            recoveryStopXid = recordXid;
            recoveryStopTime = recordXtime;
            recoveryStopLSN = InvalidXLogRecPtr;
            recoveryStopName[0] = 0;

            if xact_info == XLOG_XACT_COMMIT || xact_info == XLOG_XACT_COMMIT_PREPARED {
                ereport!(LOG, errmsg!(
                    "recovery stopping after commit of transaction {}, time {}",
                    recoveryStopXid,
                    core::ffi::CStr::from_ptr(timestamptz_to_str(recoveryStopTime)).to_string_lossy()
                ));
            } else if xact_info == XLOG_XACT_ABORT || xact_info == XLOG_XACT_ABORT_PREPARED {
                ereport!(LOG, errmsg!(
                    "recovery stopping after abort of transaction {}, time {}",
                    recoveryStopXid,
                    core::ffi::CStr::from_ptr(timestamptz_to_str(recoveryStopTime)).to_string_lossy()
                ));
            }
            return true;
        }
    }

    /* Check if we should stop as soon as reaching consistency */
    if recoveryTarget == RECOVERY_TARGET_IMMEDIATE && reachedConsistency {
        ereport!(LOG, errmsg!("recovery stopping after reaching consistency"));
        recoveryStopAfter = true;
        recoveryStopXid = 0;
        recoveryStopTime = 0;
        recoveryStopLSN = InvalidXLogRecPtr;
        recoveryStopName[0] = 0;
        return true;
    }

    false
}

/*
 * Create a comment for the history file to explain why and where
 * timeline changed.
 */
unsafe fn getRecoveryStopReason() -> *mut c_char {
    let reason: [c_char; 200] = [0; 200];

    if recoveryTarget == RECOVERY_TARGET_XID {
        /* snprintf: "%s transaction %u", after/before, recoveryStopXid */
    } else if recoveryTarget == RECOVERY_TARGET_TIME {
        /* snprintf: "%s %s\n", after/before, timestamptz_to_str(recoveryStopTime) */
    } else if recoveryTarget == RECOVERY_TARGET_LSN {
        /* snprintf: "%s LSN %X/%X\n", after/before, LSN_FORMAT_ARGS(recoveryStopLSN) */
    } else if recoveryTarget == RECOVERY_TARGET_NAME {
        /* snprintf: "at restore point \"%s\"", recoveryStopName */
    } else if recoveryTarget == RECOVERY_TARGET_IMMEDIATE {
        /* snprintf: "reached consistency" */
    } else {
        /* snprintf: "no recovery target specified" */
    }

    pstrdup(reason.as_ptr())
}

/*
 * Wait until shared recoveryPauseState is set to RECOVERY_NOT_PAUSED.
 */
unsafe fn recoveryPausesHere(endOfRecovery: bool) {
    /* Don't pause unless users can connect! */
    if !LocalHotStandbyActive {
        return;
    }

    /* Don't pause after standby promotion has been triggered */
    if LocalPromoteIsTriggered {
        return;
    }

    if endOfRecovery {
        ereport!(LOG, errmsg!("pausing at the end of recovery"));
        /* C also: errhint("Execute pg_wal_replay_resume() to promote.") */
    } else {
        ereport!(LOG, errmsg!("recovery has paused"));
        /* C also: errhint("Execute pg_wal_replay_resume() to continue.") */
    }

    /* loop until recoveryPauseState is set to RECOVERY_NOT_PAUSED */
    while GetRecoveryPauseState() != RECOVERY_NOT_PAUSED {
        ProcessStartupProcInterrupts();
        if CheckForStandbyTrigger() {
            return;
        }

        /*
         * If recovery pause is requested then set it paused.
         */
        ConfirmRecoveryPaused();

        /*
         * We wait on a condition variable that will wake us as soon as the
         * pause ends.
         */
        ConditionVariableTimedSleep(
            &mut (*XLogRecoveryCtl).recoveryNotPausedCV,
            1000,
            WAIT_EVENT_RECOVERY_PAUSE,
        );
    }
    ConditionVariableCancelSleep();
}

/*
 * When recovery_min_apply_delay is set, we wait long enough to make sure
 * certain record types are applied at least that interval behind the primary.
 *
 * Returns true if we waited.
 */
unsafe fn recoveryApplyDelay(record: *mut XLogReaderState) -> bool {
    let xact_info: u8;
    let xtime: TimestampTz;
    let delayUntil: TimestampTz;
    let mut msecs: i64;

    /* nothing to do if no delay configured */
    if recovery_min_apply_delay <= 0 {
        return false;
    }

    /* no delay is applied on a database not yet consistent */
    if !reachedConsistency {
        return false;
    }

    /* nothing to do if crash recovery is requested */
    if !ArchiveRecoveryRequested {
        return false;
    }

    /*
     * Is it a COMMIT record?
     */
    if XLogRecGetRmid(record) != RM_XACT_ID {
        return false;
    }

    xact_info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

    if xact_info != XLOG_XACT_COMMIT && xact_info != XLOG_XACT_COMMIT_PREPARED {
        return false;
    }

    let mut xtime_val: TimestampTz = 0;
    if !getRecordTimestamp(record, &mut xtime_val) {
        return false;
    }
    xtime = xtime_val;

    let mut delay_until = TimestampTzPlusMilliseconds(xtime, recovery_min_apply_delay as i64);

    /*
     * Exit without arming the latch if it's already past time to apply this record
     */
    msecs = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), delay_until);
    if msecs <= 0 {
        return false;
    }

    loop {
        ResetLatch(&mut (*XLogRecoveryCtl).recoveryWakeupLatch);

        /* This might change recovery_min_apply_delay. */
        ProcessStartupProcInterrupts();

        if CheckForStandbyTrigger() {
            break;
        }

        /*
         * Recalculate delayUntil as recovery_min_apply_delay could have
         * changed while waiting in this loop.
         */
        delay_until = TimestampTzPlusMilliseconds(xtime, recovery_min_apply_delay as i64);

        /*
         * Wait for difference between GetCurrentTimestamp() and delayUntil.
         */
        msecs = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), delay_until);

        if msecs <= 0 {
            break;
        }

        elog!(DEBUG2, "recovery apply delay {} milliseconds", msecs);

        WaitLatch(
            &mut (*XLogRecoveryCtl).recoveryWakeupLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            msecs,
            WAIT_EVENT_RECOVERY_APPLY_DELAY,
        );
    }
    true
}

/*
 * Get the current state of the recovery pause request.
 */
pub unsafe fn GetRecoveryPauseState() -> RecoveryPauseState {
    let state: RecoveryPauseState;

    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck);
    state = (*XLogRecoveryCtl).recoveryPauseState;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck);

    state
}

/*
 * Set the recovery pause state.
 */
pub unsafe fn SetRecoveryPause(recoveryPause: bool) {
    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck);

    if !recoveryPause {
        (*XLogRecoveryCtl).recoveryPauseState = RECOVERY_NOT_PAUSED;
    } else if (*XLogRecoveryCtl).recoveryPauseState == RECOVERY_NOT_PAUSED {
        (*XLogRecoveryCtl).recoveryPauseState = RECOVERY_PAUSE_REQUESTED;
    }

    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck);

    if !recoveryPause {
        ConditionVariableBroadcast(&mut (*XLogRecoveryCtl).recoveryNotPausedCV);
    }
}

/*
 * Confirm the recovery pause by setting the recovery pause state to
 * RECOVERY_PAUSED.
 */
unsafe fn ConfirmRecoveryPaused() {
    /* If recovery pause is requested then set it paused */
    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck);
    if (*XLogRecoveryCtl).recoveryPauseState == RECOVERY_PAUSE_REQUESTED {
        (*XLogRecoveryCtl).recoveryPauseState = RECOVERY_PAUSED;
    }
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck);
}

/*
 * Attempt to read the next XLOG record.
 *
 * Before first call, the reader needs to be positioned to the first record
 * by calling XLogPrefetcherBeginRead().
 *
 * If no valid record is available, returns NULL, or fails if emode is PANIC.
 */
unsafe fn ReadRecord(
    xlogprefetcher_arg: *mut XLogPrefetcher,
    emode: c_int,
    fetching_ckpt: bool,
    replayTLI: TimeLineID,
) -> *mut XLogRecord {
    let mut record: *mut XLogRecord;
    let xlogreader_local = XLogPrefetcherGetReader(xlogprefetcher_arg);
    let private = (*xlogreader_local).private_data as *mut XLogPageReadPrivate;

    /* Pass through parameters to XLogPageRead */
    (*private).fetching_ckpt = fetching_ckpt;
    (*private).emode = emode;
    (*private).randAccess = (*xlogreader_local).ReadRecPtr == InvalidXLogRecPtr;
    (*private).replayTLI = replayTLI;

    /* This is the first attempt to read this page. */
    lastSourceFailed = false;

    loop {
        let mut errormsg: *mut c_char = null_mut();

        record = XLogPrefetcherReadRecord(xlogprefetcher_arg, &mut errormsg);
        if record.is_null() {
            /*
             * When we find that WAL ends in an incomplete record, keep track
             * of that record.
             */
            if !ArchiveRecoveryRequested
                && !XLogRecPtrIsInvalid((*xlogreader_local).abortedRecPtr)
            {
                abortedRecPtr = (*xlogreader_local).abortedRecPtr;
                missingContrecPtr = (*xlogreader_local).missingContrecPtr;
            }

            if readFile >= 0 {
                c_close(readFile);
                readFile = -1;
            }

            /*
             * We only end up here without a message when XLogPageRead()
             * failed - in that case we already logged something.
             */
            if !errormsg.is_null() {
                ereport!(emode_for_corrupt_record(emode, (*xlogreader_local).EndRecPtr),
                    errmsg_internal!("WAL read error"));
                /* C uses errmsg_internal("%s", errormsg) */
            }
        } else if !tliInHistory((*xlogreader_local).latestPageTLI, expectedTLEs) {
            let mut fname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
            let mut segno: XLogSegNo = 0;
            let mut offset: i32 = 0;

            XLByteToSeg((*xlogreader_local).latestPagePtr, &mut segno, wal_segment_size);
            offset = XLogSegmentOffset((*xlogreader_local).latestPagePtr, wal_segment_size) as i32;
            XLogFileName(fname.as_mut_ptr(), (*xlogreader_local).seg.ws_tli, segno, wal_segment_size);
            ereport!(emode_for_corrupt_record(emode, (*xlogreader_local).EndRecPtr),
                errmsg!("unexpected timeline ID in WAL segment, LSN {}/{:X}, offset {}",
                    lsn_hi((*xlogreader_local).latestPagePtr),
                    lsn_lo((*xlogreader_local).latestPagePtr),
                    offset));
            record = null_mut();
        }

        if !record.is_null() {
            /* Great, got a record */
            return record;
        } else {
            /* No valid record available from this source */
            lastSourceFailed = true;

            /*
             * If archive recovery was requested, but we were still doing
             * crash recovery, switch to archive recovery and retry.
             */
            if !InArchiveRecovery && ArchiveRecoveryRequested && !fetching_ckpt {
                ereport!(DEBUG1, errmsg_internal!("reached end of WAL in pg_wal, entering archive recovery"));
                InArchiveRecovery = true;
                if StandbyModeRequested {
                    EnableStandbyMode();
                }

                SwitchIntoArchiveRecovery((*xlogreader_local).EndRecPtr, replayTLI);
                minRecoveryPoint = (*xlogreader_local).EndRecPtr;
                minRecoveryPointTLI = replayTLI;

                CheckRecoveryConsistency();

                /*
                 * Before we retry, reset lastSourceFailed and currentSource
                 * so that we will check the archive next.
                 */
                lastSourceFailed = false;
                currentSource = XLOG_FROM_ANY;

                continue;
            }

            /* In standby mode, loop back to retry. Otherwise, give up. */
            if StandbyMode && !CheckForStandbyTrigger() {
                continue;
            } else {
                return null_mut();
            }
        }
    }
}


// Import XLREAD_* constants and CHECKPOINT_CAUSE_XLOG
use crate::access::transam::xlogreader::{XLREAD_SUCCESS, XLREAD_FAIL, XLREAD_WOULDBLOCK};
use crate::postmaster::checkpointer::CHECKPOINT_CAUSE_XLOG;

/// TODO(pg-port): strtou64 (pg_strtoint64) wrapper
unsafe fn strtou64(_s: *const c_char, _endptr: *mut *mut c_char, _base: c_int) -> u64 {
    unimplemented!("TODO(pg-port): strtou64")
}

/// TODO(pg-port): fsec_t (fractional seconds), pg_tm, DTK_DATE, datetime parse helpers
pub type fsec_t = i32;
#[derive(Clone, Copy)]
#[repr(C)]
pub struct pg_tm {
    pub tm_sec: c_int,
    pub tm_min: c_int,
    pub tm_hour: c_int,
    pub tm_mday: c_int,
    pub tm_mon: c_int,
    pub tm_year: c_int,
    pub tm_wday: c_int,
    pub tm_yday: c_int,
    pub tm_isdst: c_int,
    pub tm_gmtoff: i64,
    pub tm_zone: *const c_char,
}
pub const MAXDATEFIELDS: usize = 25;
pub const MAXDATELEN: usize = 128;
pub const DTK_DATE: c_int = 6;
#[repr(C)]
pub struct DateTimeErrorExtra { pub _pad: u64 }
unsafe fn ParseDateTime(
    _str_: *const c_char, _workbuf: *mut c_char, _buflen: usize,
    _field: *mut *mut c_char, _ftype: *mut c_int, _maxfields: c_int, _numfields: *mut c_int,
) -> c_int {
    unimplemented!("TODO(pg-port): ParseDateTime")
}
unsafe fn DecodeDateTime(
    _field: *mut *mut c_char, _ftype: *mut c_int, _nf: c_int,
    _dtype: *mut c_int, _tm: *mut pg_tm, _fsec: *mut fsec_t, _tz: *mut c_int,
    _extra: *mut DateTimeErrorExtra,
) -> c_int {
    unimplemented!("TODO(pg-port): DecodeDateTime")
}
unsafe fn tm2timestamp(
    _tm: *mut pg_tm, _fsec: fsec_t, _tz: *mut c_int, _result: *mut TimestampTz,
) -> c_int { crate::utils::adt::timestamp::tm2timestamp(_tm as _, _fsec as _, _tz as _, _result as _) }

/*
 * Read the XLOG page containing targetPagePtr into readBuf (if not read
 * already).  Returns number of bytes read, if the page is read successfully,
 * or XLREAD_FAIL in case of errors.  When errors occur, they are ereport'ed,
 * but only if they have not been previously reported.
 *
 * See XLogReaderRoutine.page_read for more details.
 *
 * While prefetching, xlogreader->nonblocking may be set.  In that case,
 * returns XLREAD_WOULDBLOCK if we'd otherwise have to wait for more WAL.
 *
 * This is responsible for restoring files from archive as needed, as well
 * as for waiting for the requested WAL record to arrive in standby mode.
 *
 * xlogreader->private_data->emode specifies the log level used for reporting
 * "file not found" or "end of WAL" situations in archive recovery, or in
 * standby mode when promotion is triggered. If set to WARNING or below,
 * XLogPageRead() returns XLREAD_FAIL in those situations, on higher log
 * levels the ereport() won't return.
 *
 * In standby mode, if after a successful return of XLogPageRead() the
 * caller finds the record it's interested in to be broken, it should
 * ereport the error with the level determined by
 * emode_for_corrupt_record(), and then set lastSourceFailed
 * and call XLogPageRead() again with the same arguments. This lets
 * XLogPageRead() to try fetching the record from another source, or to
 * sleep and retry.
 */
unsafe fn XLogPageRead(
    xlogreader_arg: *mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: c_int,
    targetRecPtr: XLogRecPtr,
    readBuf: *mut c_char,
) -> c_int {
    let private = (*xlogreader_arg).private_data as *mut XLogPageReadPrivate;
    let emode = (*private).emode;
    let targetPageOff: u32;
    let mut targetSegNo: XLogSegNo = 0; /* PG_USED_FOR_ASSERTS_ONLY */
    let r: isize;
    let io_start: instr_time;

    XLByteToSeg(targetPagePtr, &mut targetSegNo, wal_segment_size);
    targetPageOff = XLogSegmentOffset(targetPagePtr, wal_segment_size);

    /*
     * See if we need to switch to a new segment because the requested record
     * is not in the currently open one.
     */
    if readFile >= 0 && !XLByteInSeg(targetPagePtr, readSegNo, wal_segment_size) {
        /*
         * Request a restartpoint if we've replayed too much xlog since the
         * last one.
         */
        if ArchiveRecoveryRequested && IsUnderPostmaster {
            if XLogCheckpointNeeded(readSegNo) {
                let _ = GetRedoRecPtr();
                if XLogCheckpointNeeded(readSegNo) {
                    RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
                }
            }
        }

        c_close(readFile);
        readFile = -1;
        readSource = XLOG_FROM_ANY;
    }

    XLByteToSeg(targetPagePtr, &mut readSegNo, wal_segment_size);

    /* retry: */
    'retry: loop {
        /* See if we need to retrieve more data */
        if readFile < 0
            || (readSource == XLOG_FROM_STREAM
                && flushedUpto < targetPagePtr + reqLen as XLogRecPtr)
        {
            if readFile >= 0
                && (*xlogreader_arg).nonblocking
                && readSource == XLOG_FROM_STREAM
                && flushedUpto < targetPagePtr + reqLen as XLogRecPtr
            {
                return XLREAD_WOULDBLOCK;
            }

            match WaitForWALToBecomeAvailable(
                targetPagePtr + reqLen as XLogRecPtr,
                (*private).randAccess,
                (*private).fetching_ckpt,
                targetRecPtr,
                (*private).replayTLI,
                (*xlogreader_arg).EndRecPtr,
                (*xlogreader_arg).nonblocking,
            ) {
                x if x == XLREAD_WOULDBLOCK => return XLREAD_WOULDBLOCK,
                x if x == XLREAD_FAIL => {
                    if readFile >= 0 {
                        c_close(readFile);
                    }
                    readFile = -1;
                    readLen = 0;
                    readSource = XLOG_FROM_ANY;
                    return XLREAD_FAIL;
                }
                _ => { /* XLREAD_SUCCESS, fall through */ }
            }
        }

        /*
         * At this point, we have the right segment open and if we're streaming we
         * know the requested record is in it.
         */
        Assert!(readFile != -1);

        /*
         * If the current segment is being streamed from the primary, calculate
         * how much of the current page we have received already. We know the
         * requested record has been received, but this is for the benefit of
         * future calls, to allow quick exit at the top of this function.
         */
        if readSource == XLOG_FROM_STREAM {
            if (targetPagePtr / XLOG_BLCKSZ as XLogRecPtr) != (flushedUpto / XLOG_BLCKSZ as XLogRecPtr) {
                readLen = XLOG_BLCKSZ as u32;
            } else {
                readLen = XLogSegmentOffset(flushedUpto, wal_segment_size) - targetPageOff;
            }
        } else {
            readLen = XLOG_BLCKSZ as u32;
        }

        /* Read the requested page */
        readOff = targetPageOff;

        /* Measure I/O timing when reading segment */
        io_start = pgstat_prepare_io_time(track_wal_io_timing);

        pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
        r = pg_pread(readFile, readBuf as *mut c_void, XLOG_BLCKSZ, readOff as i64);
        if r != XLOG_BLCKSZ as isize {
            let mut fname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
            let save_errno = *libc_errno();

            pgstat_report_wait_end();
            pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL, IOOP_READ, io_start, 1, r);

            XLogFileName(fname.as_mut_ptr(), curFileTLI, readSegNo, wal_segment_size);
            if r < 0 {
                *libc_errno() = save_errno;
                ereport!(
                    emode_for_corrupt_record(emode, targetPagePtr + reqLen as XLogRecPtr),
                    errmsg!("could not read from WAL segment %s, LSN %X/%X, offset %u: %m",
                            /* C also: errcode_for_file_access(), fname, LSN_FORMAT_ARGS(targetPagePtr), readOff */)
                );
            } else {
                ereport!(
                    emode_for_corrupt_record(emode, targetPagePtr + reqLen as XLogRecPtr),
                    errmsg!("could not read from WAL segment %s, LSN %X/%X, offset %u: read %d of %zu",
                            /* C also: errcode(ERRCODE_DATA_CORRUPTED), fname, LSN_FORMAT_ARGS(targetPagePtr), readOff, r, XLOG_BLCKSZ */)
                );
            }
            /* goto next_record_is_invalid */
            break 'retry;
        }
        pgstat_report_wait_end();
        pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL, IOOP_READ, io_start, 1, r);

        Assert!(targetSegNo == readSegNo);
        Assert!(targetPageOff == readOff as u32);
        Assert!(reqLen <= readLen as c_int);

        (*xlogreader_arg).seg.ws_tli = curFileTLI;

        /*
         * Check the page header immediately, so that we can retry immediately if
         * it's not valid. This may seem unnecessary, because ReadPageInternal()
         * validates the page header anyway, and would propagate the failure up to
         * ReadRecord(), which would retry. However, there's a corner case with
         * continuation records, if a record is split across two pages such that
         * we would need to read the two pages from different sources across two
         * WAL segments.
         *
         * The first page is only available locally, in pg_wal, because it's
         * already been recycled on the primary. The second page, however, is not
         * present in pg_wal, and we should stream it from the primary. There is a
         * recycled WAL segment present in pg_wal, with garbage contents, however.
         * We would read the first page from the local WAL segment, but when
         * reading the second page, we would read the bogus, recycled, WAL
         * segment. If we didn't catch that case here, we would never recover,
         * because ReadRecord() would retry reading the whole record from the
         * beginning.
         *
         * Of course, this only catches errors in the page header, which is what
         * happens in the case of a recycled WAL segment. Other kinds of errors or
         * corruption still has the same problem. But this at least fixes the
         * common case, which can happen as part of normal operation.
         *
         * Validating the page header is cheap enough that doing it twice
         * shouldn't be a big deal from a performance point of view.
         *
         * When not in standby mode, an invalid page header should cause recovery
         * to end, not retry reading the page, so we don't need to validate the
         * page header here for the retry. Instead, ReadPageInternal() is
         * responsible for the validation.
         */
        if StandbyMode
            && (targetPagePtr % wal_segment_size as XLogRecPtr) == 0
            && !XLogReaderValidatePageHeader(xlogreader_arg, targetPagePtr, readBuf)
        {
            /*
             * Emit this error right now then retry this page immediately. Use
             * errmsg_internal() because the message was already translated.
             */
            if *(*xlogreader_arg).errormsg_buf != 0 {
                ereport!(
                    emode_for_corrupt_record(emode, (*xlogreader_arg).EndRecPtr),
                    errmsg_internal!("{}", /* C: xlogreader_arg->errormsg_buf */ "")
                );
            }

            /* reset any error XLogReaderValidatePageHeader() might have set */
            XLogReaderResetError(xlogreader_arg);
            /* goto next_record_is_invalid */
            break 'retry;
        }

        return readLen as c_int;
    } /* end 'retry loop */

    /* next_record_is_invalid: */

    /*
     * If we're reading ahead, give up fast.  Retries and error reporting will
     * be handled by a later read when recovery catches up to this point.
     */
    if (*xlogreader_arg).nonblocking {
        return XLREAD_WOULDBLOCK;
    }

    lastSourceFailed = true;

    if readFile >= 0 {
        c_close(readFile);
    }
    readFile = -1;
    readLen = 0;
    readSource = XLOG_FROM_ANY;

    /* In standby-mode, keep trying */
    if StandbyMode {
        /* goto retry - re-enter loop by calling XLogPageRead recursively isn't right;
         * translate as a loop: re-invoke via tail call pattern.  The C does
         * `goto retry` which is the top of the function body after the segno
         * setup.  We encode this with a proper loop below. */
        return XLogPageRead(xlogreader_arg, targetPagePtr, reqLen, targetRecPtr, readBuf);
    } else {
        return XLREAD_FAIL;
    }
}

/*
 * Open the WAL segment containing WAL location 'RecPtr'.
 *
 * The segment can be fetched via restore_command, or via walreceiver having
 * streamed the record, or it can already be present in pg_wal. Checking
 * pg_wal is mainly for crash recovery, but it will be polled in standby mode
 * too, in case someone copies a new segment directly to pg_wal. That is not
 * documented or recommended, though.
 *
 * If 'fetching_ckpt' is true, we're fetching a checkpoint record, and should
 * prepare to read WAL starting from RedoStartLSN after this.
 *
 * 'RecPtr' might not point to the beginning of the record we're interested
 * in, it might also point to the page or segment header. In that case,
 * 'tliRecPtr' is the position of the WAL record we're interested in. It is
 * used to decide which timeline to stream the requested WAL from.
 *
 * 'replayLSN' is the current replay LSN, so that if we scan for new
 * timelines, we can reject a switch to a timeline that branched off before
 * this point.
 *
 * If the record is not immediately available, the function returns false
 * if we're not in standby mode. In standby mode, waits for it to become
 * available.
 *
 * When the requested record becomes available, the function opens the file
 * containing it (if not open already), and returns XLREAD_SUCCESS. When end
 * of standby mode is triggered by the user, and there is no more WAL
 * available, returns XLREAD_FAIL.
 *
 * If nonblocking is true, then give up immediately if we can't satisfy the
 * request, returning XLREAD_WOULDBLOCK instead of waiting.
 */
#[allow(unreachable_code)]
unsafe fn WaitForWALToBecomeAvailable(
    RecPtr: XLogRecPtr,
    randAccess: bool,
    fetching_ckpt: bool,
    tliRecPtr: XLogRecPtr,
    replayTLI: TimeLineID,
    replayLSN: XLogRecPtr,
    nonblocking: bool,
) -> c_int {
    static mut last_fail_time: TimestampTz = 0;
    let mut now: TimestampTz;
    let mut streaming_reply_sent: bool = false;

    /*-------
     * Standby mode is implemented by a state machine:
     *
     * 1. Read from either archive or pg_wal (XLOG_FROM_ARCHIVE), or just
     *    pg_wal (XLOG_FROM_PG_WAL)
     * 2. Check for promotion trigger request
     * 3. Read from primary server via walreceiver (XLOG_FROM_STREAM)
     * 4. Rescan timelines
     * 5. Sleep wal_retrieve_retry_interval milliseconds, and loop back to 1.
     *
     * Failure to read from the current source advances the state machine to
     * the next state.
     *
     * 'currentSource' indicates the current state. There are no currentSource
     * values for "check trigger", "rescan timelines", and "sleep" states,
     * those actions are taken when reading from the previous source fails, as
     * part of advancing to the next state.
     *
     * If standby mode is turned off while reading WAL from stream, we move
     * to XLOG_FROM_ARCHIVE and reset lastSourceFailed, to force fetching
     * the files (which would be required at end of recovery, e.g., timeline
     * history file) from archive or pg_wal. We don't need to kill WAL receiver
     * here because it's already stopped when standby mode is turned off at
     * the end of recovery.
     *-------
     */

    if !InArchiveRecovery {
        currentSource = XLOG_FROM_PG_WAL;
    } else if currentSource == XLOG_FROM_ANY
        || (!StandbyMode && currentSource == XLOG_FROM_STREAM)
    {
        lastSourceFailed = false;
        currentSource = XLOG_FROM_ARCHIVE;
    }

    loop {
        let oldSource = currentSource;
        let mut startWalReceiver = false;

        /*
         * First check if we failed to read from the current source, and
         * advance the state machine if so. The failure to read might've
         * happened outside this function, e.g when a CRC check fails on a
         * record, or within this loop.
         */
        if lastSourceFailed {
            /*
             * Don't allow any retry loops to occur during nonblocking
             * readahead.  Let the caller process everything that has been
             * decoded already first.
             */
            if nonblocking {
                return XLREAD_WOULDBLOCK;
            }

            match currentSource {
                XLOG_FROM_ARCHIVE | XLOG_FROM_PG_WAL => {
                    /*
                     * Check to see if promotion is requested. Note that we do
                     * this only after failure, so when you promote, we still
                     * finish replaying as much as we can from archive and
                     * pg_wal before failover.
                     */
                    if StandbyMode && CheckForStandbyTrigger() {
                        XLogShutdownWalRcv();
                        return XLREAD_FAIL;
                    }

                    /*
                     * Not in standby mode, and we've now tried the archive
                     * and pg_wal.
                     */
                    if !StandbyMode {
                        return XLREAD_FAIL;
                    }

                    /*
                     * Move to XLOG_FROM_STREAM state, and set to start a
                     * walreceiver if necessary.
                     */
                    currentSource = XLOG_FROM_STREAM;
                    startWalReceiver = true;
                }

                XLOG_FROM_STREAM => {
                    /*
                     * Failure while streaming. Most likely, we got here
                     * because streaming replication was terminated, or
                     * promotion was triggered. But we also get here if we
                     * find an invalid record in the WAL streamed from the
                     * primary, in which case something is seriously wrong.
                     * There's little chance that the problem will just go
                     * away, but PANIC is not good for availability either,
                     * especially in hot standby mode. So, we treat that the
                     * same as disconnection, and retry from archive/pg_wal
                     * again. The WAL in the archive should be identical to
                     * what was streamed, so it's unlikely that it helps, but
                     * one can hope...
                     */

                    /*
                     * We should be able to move to XLOG_FROM_STREAM only in
                     * standby mode.
                     */
                    Assert!(StandbyMode);

                    /*
                     * Before we leave XLOG_FROM_STREAM state, make sure that
                     * walreceiver is not active, so that it won't overwrite
                     * WAL that we restore from archive.
                     *
                     * If walreceiver is actively streaming (or attempting to
                     * connect), we must shut it down. However, if it's
                     * already in WAITING state (e.g., due to timeline
                     * divergence), we only need to reset the install flag to
                     * allow archive restoration.
                     */
                    if WalRcvStreaming() {
                        XLogShutdownWalRcv();
                    } else {
                        ResetInstallXLogFileSegmentActive();
                    }

                    /*
                     * Before we sleep, re-scan for possible new timelines if
                     * we were requested to recover to the latest timeline.
                     */
                    if recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST {
                        if rescanLatestTimeLine(replayTLI, replayLSN) {
                            currentSource = XLOG_FROM_ARCHIVE;
                            /* continue outer loop */
                        }
                    }

                    /*
                     * XLOG_FROM_STREAM is the last state in our state
                     * machine, so we've exhausted all the options for
                     * obtaining the requested WAL. We're going to loop back
                     * and retry from the archive, but if it hasn't been long
                     * since last attempt, sleep wal_retrieve_retry_interval
                     * milliseconds to avoid busy-waiting.
                     */
                    now = GetCurrentTimestamp();
                    if !TimestampDifferenceExceeds(last_fail_time, now, wal_retrieve_retry_interval) {
                        let wait_time: i64;

                        wait_time = wal_retrieve_retry_interval as i64
                            - TimestampDifferenceMilliseconds(last_fail_time, now);

                        elog!(LOG, "waiting for WAL to become available at %X/%X",
                              /* C: LSN_FORMAT_ARGS(RecPtr) */);

                        /* Do background tasks that might benefit us later. */
                        KnownAssignedTransactionIdsIdleMaintenance();

                        let _ = WaitLatch(
                            &mut (*XLogRecoveryCtl).recoveryWakeupLatch as *mut Latch,
                            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                            wait_time,
                            WAIT_EVENT_RECOVERY_RETRIEVE_RETRY_INTERVAL,
                        );
                        ResetLatch(&mut (*XLogRecoveryCtl).recoveryWakeupLatch as *mut Latch);
                        now = GetCurrentTimestamp();

                        /* Handle interrupt signals of startup process */
                        ProcessStartupProcInterrupts();
                    }
                    last_fail_time = now;
                    currentSource = XLOG_FROM_ARCHIVE;
                }

                _ => {
                    elog!(ERROR, "unexpected WAL source %d", /* currentSource as i32 */);
                }
            }
        } else if currentSource == XLOG_FROM_PG_WAL {
            /*
             * We just successfully read a file in pg_wal. We prefer files in
             * the archive over ones in pg_wal, so try the next file again
             * from the archive first.
             */
            if InArchiveRecovery {
                currentSource = XLOG_FROM_ARCHIVE;
            }
        }

        if currentSource != oldSource {
            elog!(DEBUG2, "switched WAL source from %s to %s after %s",
                  /* xlogSourceNames[oldSource as usize], xlogSourceNames[currentSource as usize],
                     if lastSourceFailed { "failure" } else { "success" } */);
        }

        /*
         * We've now handled possible failure. Try to read from the chosen
         * source.
         */
        lastSourceFailed = false;

        match currentSource {
            XLOG_FROM_ARCHIVE | XLOG_FROM_PG_WAL => {
                /*
                 * WAL receiver must not be running when reading WAL from
                 * archive or pg_wal.
                 */
                Assert!(!WalRcvStreaming());

                /* Close any old file we might have open. */
                if readFile >= 0 {
                    c_close(readFile);
                    readFile = -1;
                }
                /* Reset curFileTLI if random fetch. */
                if randAccess {
                    curFileTLI = 0;
                }

                /*
                 * Try to restore the file from archive, or read an existing
                 * file from pg_wal.
                 */
                readFile = XLogFileReadAnyTLI(
                    readSegNo,
                    if currentSource == XLOG_FROM_ARCHIVE { XLOG_FROM_ANY } else { currentSource },
                );
                if readFile >= 0 {
                    return XLREAD_SUCCESS; /* success! */
                }

                /*
                 * Nope, not found in archive or pg_wal.
                 */
                lastSourceFailed = true;
            }

            XLOG_FROM_STREAM => {
                let havedata: bool;

                /*
                 * We should be able to move to XLOG_FROM_STREAM only in
                 * standby mode.
                 */
                Assert!(StandbyMode);

                /*
                 * First, shutdown walreceiver if its restart has been
                 * requested -- but no point if we're already slated for
                 * starting it.
                 */
                if pendingWalRcvRestart && !startWalReceiver {
                    XLogShutdownWalRcv();

                    /*
                     * Re-scan for possible new timelines if we were
                     * requested to recover to the latest timeline.
                     */
                    if recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST {
                        rescanLatestTimeLine(replayTLI, replayLSN);
                    }

                    startWalReceiver = true;
                }
                pendingWalRcvRestart = false;

                /*
                 * Launch walreceiver if needed.
                 *
                 * If fetching_ckpt is true, RecPtr points to the initial
                 * checkpoint location. In that case, we use RedoStartLSN
                 * as the streaming start position instead of RecPtr, so
                 * that when we later jump backwards to start redo at
                 * RedoStartLSN, we will have the logs streamed already.
                 */
                if startWalReceiver
                    && !PrimaryConnInfo.is_null()
                    && *PrimaryConnInfo != 0
                {
                    let ptr: XLogRecPtr;
                    let tli: TimeLineID;

                    if fetching_ckpt {
                        ptr = RedoStartLSN;
                        tli = RedoStartTLI;
                    } else {
                        ptr = RecPtr;

                        /*
                         * Use the record begin position to determine the
                         * TLI, rather than the position we're reading.
                         */
                        tli = tliOfPointInHistory(tliRecPtr, expectedTLEs);

                        if curFileTLI > 0 && tli < curFileTLI {
                            elog!(ERROR, "according to history file, WAL location %X/%X belongs to timeline %u, but previous recovered WAL file came from timeline %u",
                                  /* LSN_FORMAT_ARGS(tliRecPtr), tli, curFileTLI */);
                        }
                    }
                    curFileTLI = tli;
                    SetInstallXLogFileSegmentActive();
                    RequestXLogStreaming(tli, ptr, PrimaryConnInfo, PrimarySlotName, wal_receiver_create_temp_slot);
                    flushedUpto = 0;
                }

                /*
                 * Check if WAL receiver is active or wait to start up.
                 */
                if !WalRcvStreaming() {
                    lastSourceFailed = true;
                    /* continue outer loop (handled by match falling through) */
                }

                if !lastSourceFailed {
                    /*
                     * Walreceiver is active, so see if new data has arrived.
                     *
                     * We only advance XLogReceiptTime when we obtain fresh
                     * WAL from walreceiver and observe that we had already
                     * processed everything before the most recent "chunk"
                     * that it flushed to disk.  In steady state where we are
                     * keeping up with the incoming data, XLogReceiptTime will
                     * be updated on each cycle. When we are behind,
                     * XLogReceiptTime will not advance, so the grace time
                     * allotted to conflicting queries will decrease.
                     */
                    if RecPtr < flushedUpto {
                        havedata = true;
                    } else {
                        let mut latestChunkStart: XLogRecPtr = 0;
                        flushedUpto = GetWalRcvFlushRecPtr(&mut latestChunkStart as *mut XLogRecPtr, &mut receiveTLI as *mut TimeLineID);
                        if RecPtr < flushedUpto && receiveTLI == curFileTLI {
                            havedata = true;
                            if latestChunkStart <= RecPtr {
                                XLogReceiptTime = GetCurrentTimestamp();
                                SetCurrentChunkStartTime(XLogReceiptTime);
                            }
                        } else {
                            havedata = false;
                        }
                    }
                    if havedata {
                        /*
                         * Great, streamed far enough.  Open the file if it's
                         * not open already.  Also read the timeline history
                         * file if we haven't initialized timeline history
                         * yet; it should be streamed over and present in
                         * pg_wal by now.  Use XLOG_FROM_STREAM so that source
                         * info is set correctly and XLogReceiptTime isn't
                         * changed.
                         *
                         * NB: We must set readTimeLineHistory based on
                         * recoveryTargetTLI, not receiveTLI. Normally they'll
                         * be the same, but if recovery_target_timeline is
                         * 'latest' and archiving is configured, then it's
                         * possible that we managed to retrieve one or more
                         * new timeline history files from the archive,
                         * updating recoveryTargetTLI.
                         */
                        if readFile < 0 {
                            if expectedTLEs.is_null() {
                                expectedTLEs = readTimeLineHistory(recoveryTargetTLI);
                            }
                            readFile = XLogFileRead(readSegNo, receiveTLI, XLOG_FROM_STREAM, false);
                            Assert!(readFile >= 0);
                        } else {
                            /* just make sure source info is correct... */
                            readSource = XLOG_FROM_STREAM;
                            XLogReceiptSource = XLOG_FROM_STREAM;
                            return XLREAD_SUCCESS;
                        }
                        /* continue outer loop */
                    } else {
                        /* In nonblocking mode, return rather than sleeping. */
                        if nonblocking {
                            return XLREAD_WOULDBLOCK;
                        }

                        /*
                         * Data not here yet. Check for trigger, then wait for
                         * walreceiver to wake us up when new WAL arrives.
                         */
                        if CheckForStandbyTrigger() {
                            /*
                             * Note that we don't return XLREAD_FAIL immediately
                             * here. After being triggered, we still want to
                             * replay all the WAL that was already streamed. It's
                             * in pg_wal now, so we just treat this as a failure,
                             * and the state machine will move on to replay the
                             * streamed WAL from pg_wal, and then recheck the
                             * trigger and exit replay.
                             */
                            lastSourceFailed = true;
                            /* continue */
                        } else {
                            /*
                             * Since we have replayed everything we have received so
                             * far and are about to start waiting for more WAL, let's
                             * tell the upstream server our replay location now so
                             * that pg_stat_replication doesn't show stale
                             * information.
                             */
                            if !streaming_reply_sent {
                                WalRcvForceReply();
                                streaming_reply_sent = true;
                            }

                            /* Do any background tasks that might benefit us later. */
                            KnownAssignedTransactionIdsIdleMaintenance();

                            /* Update pg_stat_recovery_prefetch before sleeping. */
                            XLogPrefetcherComputeStats(xlogprefetcher);

                            /*
                             * Wait for more WAL to arrive, when we will be woken
                             * immediately by the WAL receiver.
                             */
                            let _ = WaitLatch(
                                &mut (*XLogRecoveryCtl).recoveryWakeupLatch as *mut Latch,
                                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                                -1i64,
                                WAIT_EVENT_RECOVERY_WAL_STREAM,
                            );
                            ResetLatch(&mut (*XLogRecoveryCtl).recoveryWakeupLatch as *mut Latch);
                        }
                    }
                }
            }

            _ => {
                elog!(ERROR, "unexpected WAL source %d", /* currentSource as i32 */);
            }
        }

        /*
         * Check for recovery pause here so that we can confirm more quickly
         * that a requested pause has actually taken effect.
         */
        if (*(XLogRecoveryCtl as *const XLogRecoveryCtlData)).recoveryPauseState != RECOVERY_NOT_PAUSED {
            recoveryPausesHere(false);
        }

        /*
         * This possibly-long loop needs to handle interrupts of startup
         * process.
         */
        ProcessStartupProcInterrupts();
    }

    XLREAD_FAIL /* not reached */
}

/*
 * Determine what log level should be used to report a corrupt WAL record
 * in the current WAL page, previously read by XLogPageRead().
 *
 * 'emode' is the error mode that would be used to report a file-not-found
 * or legitimate end-of-WAL situation.   Generally, we use it as-is, but if
 * we're retrying the exact same record that we've tried previously, only
 * complain the first time to keep the noise down.  However, we only do when
 * reading from pg_wal, because we don't expect any invalid records in archive
 * or in records streamed from the primary. Files in the archive should be complete,
 * and we should never hit the end of WAL because we stop and wait for more WAL
 * to arrive before replaying it.
 *
 * NOTE: This function remembers the RecPtr value it was last called with,
 * to suppress repeated messages about the same record. Only call this when
 * you are about to ereport(), or you might cause a later message to be
 * erroneously suppressed.
 */
unsafe fn emode_for_corrupt_record(emode: c_int, RecPtr: XLogRecPtr) -> c_int {
    static mut lastComplaint: XLogRecPtr = 0;

    let mut emode = emode;
    if readSource == XLOG_FROM_PG_WAL && emode == LOG {
        if RecPtr == lastComplaint {
            emode = DEBUG1;
        } else {
            lastComplaint = RecPtr;
        }
    }
    emode
}


/*
 * Subroutine to try to fetch and validate a prior checkpoint record.
 */
unsafe fn ReadCheckpointRecord(
    xlogprefetcher_arg: *mut XLogPrefetcher,
    RecPtr: XLogRecPtr,
    replayTLI: TimeLineID,
) -> *mut XLogRecord {
    let record: *mut XLogRecord;
    let info: u8;

    Assert!(xlogreader != null_mut());

    if !XRecOffIsValid(RecPtr) {
        ereport!(LOG, errmsg!("invalid checkpoint location"));
        return null_mut();
    }

    XLogPrefetcherBeginRead(xlogprefetcher_arg, RecPtr);
    record = ReadRecord(xlogprefetcher_arg, LOG, true, replayTLI);

    if record.is_null() {
        ereport!(LOG, errmsg!("invalid checkpoint record"));
        return null_mut();
    }
    if (*record).xl_rmid != RM_XLOG_ID {
        ereport!(LOG, errmsg!("invalid resource manager ID in checkpoint record"));
        return null_mut();
    }
    info = (*record).xl_info & !XLR_INFO_MASK;
    if info != XLOG_CHECKPOINT_SHUTDOWN && info != XLOG_CHECKPOINT_ONLINE {
        ereport!(LOG, errmsg!("invalid xl_info in checkpoint record"));
        return null_mut();
    }
    if (*record).xl_tot_len as usize
        != size_of::<XLogRecord>()
            + SizeOfXLogRecordDataHeaderShort
            + size_of::<CheckPoint>()
    {
        ereport!(LOG, errmsg!("invalid length of checkpoint record"));
        return null_mut();
    }
    record
}

/*
 * Scan for new timelines that might have appeared in the archive since we
 * started recovery.
 *
 * If there are any, the function changes recovery target TLI to the latest
 * one and returns 'true'.
 */
unsafe fn rescanLatestTimeLine(replayTLI: TimeLineID, replayLSN: XLogRecPtr) -> bool {
    let newExpectedTLEs: *mut List;
    let mut found: bool;
    let mut cell: *mut crate::nodes::pg_list::ListCell;
    let newtarget: TimeLineID;
    let oldtarget: TimeLineID = recoveryTargetTLI;
    let mut currentTle: *mut TimeLineHistoryEntry = null_mut();

    newtarget = findNewestTimeLine(recoveryTargetTLI);
    if newtarget == recoveryTargetTLI {
        /* No new timelines found */
        return false;
    }

    /*
     * Determine the list of expected TLIs for the new TLI
     */

    newExpectedTLEs = readTimeLineHistory(newtarget);

    /*
     * If the current timeline is not part of the history of the new timeline,
     * we cannot proceed to it.
     */
    found = false;
    cell = crate::nodes::pg_list::list_head(newExpectedTLEs);
    while !cell.is_null() {
        currentTle = crate::nodes::pg_list::lfirst(cell) as *mut TimeLineHistoryEntry;
        if (*currentTle).tli == recoveryTargetTLI {
            found = true;
            break;
        }
        cell = crate::nodes::pg_list::lnext(newExpectedTLEs, cell);
    }
    if !found {
        ereport!(LOG, errmsg!("new timeline %u is not a child of database system timeline %u",
                              /* newtarget, replayTLI */));
        return false;
    }

    /*
     * The current timeline was found in the history file, but check that the
     * next timeline was forked off from it *after* the current recovery
     * location.
     */
    if (*currentTle).end < replayLSN {
        ereport!(LOG, errmsg!("new timeline %u forked off current database system timeline %u before current recovery point %X/%X",
                              /* newtarget, replayTLI, LSN_FORMAT_ARGS(replayLSN) */));
        return false;
    }

    /* The new timeline history seems valid. Switch target */
    recoveryTargetTLI = newtarget;
    list_free_deep(expectedTLEs);
    expectedTLEs = newExpectedTLEs;

    /*
     * As in StartupXLOG(), try to ensure we have all the history files
     * between the old target and new target in pg_wal.
     */
    restoreTimeLineHistoryFiles(oldtarget + 1, newtarget);

    ereport!(LOG, errmsg!("new target timeline is %u", /* recoveryTargetTLI */));

    true
}


/*
 * Open a logfile segment for reading (during recovery).
 *
 * If source == XLOG_FROM_ARCHIVE, the segment is retrieved from archive.
 * Otherwise, it's assumed to be already available in pg_wal.
 */
unsafe fn XLogFileRead(
    segno: XLogSegNo,
    tli: TimeLineID,
    source: XLogSource,
    notfoundOk: bool,
) -> c_int {
    let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut activitymsg: [c_char; MAXFNAMELEN + 16] = [0; MAXFNAMELEN + 16];
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let fd: c_int;

    XLogFileName(xlogfname.as_mut_ptr(), tli, segno, wal_segment_size);

    match source {
        XLOG_FROM_ARCHIVE => {
            /* Report recovery progress in PS display */
            libc_snprintf2(
                activitymsg.as_mut_ptr(),
                core::mem::size_of_val(&activitymsg),
                b"waiting for %s\0".as_ptr() as *const c_char,
                xlogfname.as_ptr(),
            );
            set_ps_display(activitymsg.as_ptr());

            if !RestoreArchivedFile(
                path.as_mut_ptr(),
                xlogfname.as_ptr(),
                b"RECOVERYXLOG\0".as_ptr() as *const c_char,
                wal_segment_size,
                InRedo,
            ) {
                return -1;
            }
        }

        XLOG_FROM_PG_WAL | XLOG_FROM_STREAM => {
            XLogFilePath(path.as_mut_ptr(), tli, segno, wal_segment_size);
        }

        _ => {
            elog!(ERROR, "invalid XLogFileRead source %d", /* source as i32 */);
        }
    }

    /*
     * If the segment was fetched from archival storage, replace the existing
     * xlog segment (if any) with the archival version.
     */
    if source == XLOG_FROM_ARCHIVE {
        Assert!(!IsInstallXLogFileSegmentActive());
        KeepFileRestoredFromArchive(path.as_ptr(), xlogfname.as_ptr());

        /*
         * Set path to point at the new file in pg_wal.
         */
        libc_snprintf2(
            path.as_mut_ptr(),
            MAXPGPATH,
            b"%s/%s\0".as_ptr() as *const c_char, /* XLOGDIR "/" xlogfname */
            xlogfname.as_ptr(),
        );
    }

    fd = BasicOpenFile(path.as_ptr(), O_RDONLY | PG_BINARY);
    if fd >= 0 {
        /* Success! */
        curFileTLI = tli;

        /* Report recovery progress in PS display */
        libc_snprintf2(
            activitymsg.as_mut_ptr(),
            core::mem::size_of_val(&activitymsg),
            b"recovering %s\0".as_ptr() as *const c_char,
            xlogfname.as_ptr(),
        );
        set_ps_display(activitymsg.as_ptr());

        /* Track source of data in assorted state variables */
        readSource = source;
        XLogReceiptSource = source;
        /* In FROM_STREAM case, caller tracks receipt time, not me */
        if source != XLOG_FROM_STREAM {
            XLogReceiptTime = GetCurrentTimestamp();
        }

        return fd;
    }
    if *libc_errno() != ENOENT || !notfoundOk {
        /* unexpected failure? */
        ereport!(PANIC, errmsg!("could not open file \"%s\": %m",
                                /* C also: errcode_for_file_access(), path */));
    }
    -1
}

/*
 * Open a logfile segment for reading (during recovery).
 *
 * This version searches for the segment with any TLI listed in expectedTLEs.
 */
unsafe fn XLogFileReadAnyTLI(segno: XLogSegNo, source: XLogSource) -> c_int {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut cell: *mut crate::nodes::pg_list::ListCell;
    let mut fd: c_int;
    let tles: *mut List;

    /*
     * Loop looking for a suitable timeline ID: we might need to read any of
     * the timelines listed in expectedTLEs.
     *
     * We expect curFileTLI on entry to be the TLI of the preceding file in
     * sequence, or 0 if there was no predecessor.  We do not allow curFileTLI
     * to go backwards; this prevents us from picking up the wrong file when a
     * parent timeline extends to higher segment numbers than the child we
     * want to read.
     *
     * If we haven't read the timeline history file yet, read it now, so that
     * we know which TLIs to scan.  We don't save the list in expectedTLEs,
     * however, unless we actually find a valid segment.  That way if there is
     * neither a timeline history file nor a WAL segment in the archive, and
     * streaming replication is set up, we'll read the timeline history file
     * streamed from the primary when we start streaming, instead of
     * recovering with a dummy history generated here.
     */
    if !expectedTLEs.is_null() {
        tles = expectedTLEs;
    } else {
        tles = readTimeLineHistory(recoveryTargetTLI);
    }

    cell = crate::nodes::pg_list::list_head(tles);
    while !cell.is_null() {
        let hent = crate::nodes::pg_list::lfirst(cell) as *mut TimeLineHistoryEntry;
        let tli: TimeLineID = (*hent).tli;

        if tli < curFileTLI {
            break; /* don't bother looking at too-old TLIs */
        }

        /*
         * Skip scanning the timeline ID that the logfile segment to read
         * doesn't belong to
         */
        if (*hent).begin != InvalidXLogRecPtr {
            let mut beginseg: XLogSegNo = 0;

            XLByteToSeg((*hent).begin, &mut beginseg, wal_segment_size);

            /*
             * The logfile segment that doesn't belong to the timeline is
             * older or newer than the segment that the timeline started or
             * ended at, respectively. It's sufficient to check only the
             * starting segment of the timeline here. Since the timelines are
             * scanned in descending order in this loop, any segments newer
             * than the ending segment should belong to newer timeline and
             * have already been read before. So it's not necessary to check
             * the ending segment of the timeline here.
             */
            if segno < beginseg {
                cell = crate::nodes::pg_list::lnext(tles, cell);
                continue;
            }
        }

        if source == XLOG_FROM_ANY || source == XLOG_FROM_ARCHIVE {
            fd = XLogFileRead(segno, tli, XLOG_FROM_ARCHIVE, true);
            if fd != -1 {
                elog!(DEBUG1, "got WAL segment from archive");
                if expectedTLEs.is_null() {
                    expectedTLEs = tles;
                }
                return fd;
            }
        }

        if source == XLOG_FROM_ANY || source == XLOG_FROM_PG_WAL {
            fd = XLogFileRead(segno, tli, XLOG_FROM_PG_WAL, true);
            if fd != -1 {
                if expectedTLEs.is_null() {
                    expectedTLEs = tles;
                }
                return fd;
            }
        }

        cell = crate::nodes::pg_list::lnext(tles, cell);
    }

    /* Couldn't find it.  For simplicity, complain about front timeline */
    XLogFilePath(path.as_mut_ptr(), recoveryTargetTLI, segno, wal_segment_size);
    *libc_errno() = ENOENT;
    ereport!(DEBUG2, errmsg!("could not open file \"%s\": %m",
                             /* C also: errcode_for_file_access(), path */));
    -1
}

/*
 * Set flag to signal the walreceiver to restart.  (The startup process calls
 * this on noticing a relevant configuration change.)
 */
pub unsafe fn StartupRequestWalReceiverRestart() {
    if currentSource == XLOG_FROM_STREAM && WalRcvRunning() {
        ereport!(LOG, errmsg!("WAL receiver process shutdown requested"));
        pendingWalRcvRestart = true;
    }
}


/*
 * Has a standby promotion already been triggered?
 *
 * Unlike CheckForStandbyTrigger(), this works in any process
 * that's connected to shared memory.
 */
pub unsafe fn PromoteIsTriggered() -> bool {
    /*
     * We check shared state each time only until a standby promotion is
     * triggered. We can't trigger a promotion again, so there's no need to
     * keep checking after the shared variable has once been seen true.
     */
    if LocalPromoteIsTriggered {
        return true;
    }

    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
    LocalPromoteIsTriggered = (*XLogRecoveryCtl).SharedPromoteIsTriggered;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);

    LocalPromoteIsTriggered
}

unsafe fn SetPromoteIsTriggered() {
    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
    (*XLogRecoveryCtl).SharedPromoteIsTriggered = true;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);

    /*
     * Mark the recovery pause state as 'not paused' because the paused state
     * ends and promotion continues if a promotion is triggered while recovery
     * is paused. Otherwise pg_get_wal_replay_pause_state() can mistakenly
     * return 'paused' while a promotion is ongoing.
     */
    SetRecoveryPause(false);

    LocalPromoteIsTriggered = true;
}

/*
 * Check whether a promote request has arrived.
 */
unsafe fn CheckForStandbyTrigger() -> bool {
    if LocalPromoteIsTriggered {
        return true;
    }

    if IsPromoteSignaled() && CheckPromoteSignal() {
        ereport!(LOG, errmsg!("received promote request"));
        RemovePromoteSignalFiles();
        ResetPromoteSignaled();
        SetPromoteIsTriggered();
        return true;
    }

    false
}

/*
 * Remove the files signaling a standby promotion request.
 */
pub unsafe fn RemovePromoteSignalFiles() {
    c_unlink(PROMOTE_SIGNAL_FILE as *const c_char);
}

/*
 * Check to see if a promote request has arrived.
 */
pub unsafe fn CheckPromoteSignal() -> bool {
    let mut stat_buf: libc_stat = core::mem::zeroed();

    if c_stat(PROMOTE_SIGNAL_FILE as *const c_char, &mut stat_buf as *mut libc_stat) == 0 {
        return true;
    }

    false
}

/*
 * Wake up startup process to replay newly arrived WAL, or to notice that
 * failover has been requested.
 */
pub unsafe fn WakeupRecovery() {
    SetLatch(&mut (*XLogRecoveryCtl).recoveryWakeupLatch as *mut Latch);
}

/*
 * Schedule a walreceiver wakeup in the main recovery loop.
 */
pub unsafe fn XLogRequestWalReceiverReply() {
    doRequestWalReceiverReply = true;
}

/*
 * Is HotStandby active yet? This is only important in special backends
 * since normal backends won't ever be able to connect until this returns
 * true. Postmaster knows this by way of signal, not via shared memory.
 *
 * Unlike testing standbyState, this works in any process that's connected to
 * shared memory.  (And note that standbyState alone doesn't tell the truth
 * anyway.)
 */
pub unsafe fn HotStandbyActive() -> bool {
    /*
     * We check shared state each time only until Hot Standby is active. We
     * can't de-activate Hot Standby, so there's no need to keep checking
     * after the shared variable has once been seen true.
     */
    if LocalHotStandbyActive {
        return true;
    } else {
        /* spinlock is essential on machines with weak memory ordering! */
        SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
        LocalHotStandbyActive = (*XLogRecoveryCtl).SharedHotStandbyActive;
        SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);

        LocalHotStandbyActive
    }
}

/*
 * Like HotStandbyActive(), but to be used only in WAL replay code,
 * where we don't need to ask any other process what the state is.
 */
unsafe fn HotStandbyActiveInReplay() -> bool {
    Assert!(AmStartupProcess() || !IsPostmasterEnvironment);
    LocalHotStandbyActive
}

/*
 * Get latest redo apply position.
 *
 * Exported to allow WALReceiver to read the pointer directly.
 */
pub unsafe fn GetXLogReplayRecPtr(replayTLI: *mut TimeLineID) -> XLogRecPtr {
    let recptr: XLogRecPtr;
    let tli: TimeLineID;

    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
    recptr = (*XLogRecoveryCtl).lastReplayedEndRecPtr;
    tli = (*XLogRecoveryCtl).lastReplayedTLI;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);

    if !replayTLI.is_null() {
        *replayTLI = tli;
    }
    recptr
}


/*
 * Get position of last applied, or the record being applied.
 *
 * This is different from GetXLogReplayRecPtr() in that if a WAL
 * record is currently being applied, this includes that record.
 */
pub unsafe fn GetCurrentReplayRecPtr(replayEndTLI: *mut TimeLineID) -> XLogRecPtr {
    let recptr: XLogRecPtr;
    let tli: TimeLineID;

    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
    recptr = (*XLogRecoveryCtl).replayEndRecPtr;
    tli = (*XLogRecoveryCtl).replayEndTLI;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);

    if !replayEndTLI.is_null() {
        *replayEndTLI = tli;
    }
    recptr
}

/*
 * Save timestamp of latest processed commit/abort record.
 *
 * We keep this in XLogRecoveryCtl, not a simple static variable, so that it can be
 * seen by processes other than the startup process.  Note in particular
 * that CreateRestartPoint is executed in the checkpointer.
 */
unsafe fn SetLatestXTime(xtime: TimestampTz) {
    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
    (*XLogRecoveryCtl).recoveryLastXTime = xtime;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
}

/*
 * Fetch timestamp of latest processed commit/abort record.
 */
pub unsafe fn GetLatestXTime() -> TimestampTz {
    let xtime: TimestampTz;

    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
    xtime = (*XLogRecoveryCtl).recoveryLastXTime;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);

    xtime
}

/*
 * Save timestamp of the next chunk of WAL records to apply.
 *
 * We keep this in XLogRecoveryCtl, not a simple static variable, so that it can be
 * seen by all backends.
 */
unsafe fn SetCurrentChunkStartTime(xtime: TimestampTz) {
    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
    (*XLogRecoveryCtl).currentChunkStartTime = xtime;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
}

/*
 * Fetch timestamp of latest processed commit/abort record.
 * Startup process maintains an accurate local copy in XLogReceiptTime
 */
pub unsafe fn GetCurrentChunkReplayStartTime() -> TimestampTz {
    let xtime: TimestampTz;

    SpinLockAcquire(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);
    xtime = (*XLogRecoveryCtl).currentChunkStartTime;
    SpinLockRelease(&mut (*XLogRecoveryCtl).info_lck as *mut slock_t);

    xtime
}

/*
 * Returns time of receipt of current chunk of XLOG data, as well as
 * whether it was received from streaming replication or from archives.
 */
pub unsafe fn GetXLogReceiptTime(rtime: *mut TimestampTz, fromStream: *mut bool) {
    /*
     * This must be executed in the startup process, since we don't export the
     * relevant state to shared memory.
     */
    Assert!(InRecovery);

    *rtime = XLogReceiptTime;
    *fromStream = XLogReceiptSource == XLOG_FROM_STREAM;
}

/*
 * Note that text field supplied is a parameter name and does not require
 * translation
 */
pub unsafe fn RecoveryRequiresIntParameter(param_name: *const c_char, currValue: c_int, minValue: c_int) {
    if currValue < minValue {
        if HotStandbyActiveInReplay() {
            let mut warned_for_promote: bool = false;

            ereport!(WARNING, errmsg!("hot standby is not possible because of insufficient parameter settings",
                                     /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                        errdetail("%s = %d is a lower setting than on the primary server...", param_name, currValue, minValue) */));

            SetRecoveryPause(true);

            ereport!(LOG, errmsg!("recovery has paused",
                                  /* C also: errdetail("If recovery is unpaused, the server will shut down."),
                                     errhint("You can then restart the server after making the necessary configuration changes.") */));

            while GetRecoveryPauseState() != RECOVERY_NOT_PAUSED {
                ProcessStartupProcInterrupts();

                if CheckForStandbyTrigger() {
                    if !warned_for_promote {
                        ereport!(WARNING, errmsg!("promotion is not possible because of insufficient parameter settings",
                                                  /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                                     errdetail(...), errhint("Restart the server after making the necessary configuration changes.") */));
                        warned_for_promote = true;
                    }
                }

                /*
                 * If recovery pause is requested then set it paused.  While
                 * we are in the loop, user might resume and pause again so
                 * set this every time.
                 */
                ConfirmRecoveryPaused();

                /*
                 * We wait on a condition variable that will wake us as soon
                 * as the pause ends, but we use a timeout so we can check the
                 * above conditions periodically too.
                 */
                ConditionVariableTimedSleep(
                    &mut (*XLogRecoveryCtl).recoveryNotPausedCV as *mut ConditionVariable,
                    1000,
                    WAIT_EVENT_RECOVERY_PAUSE,
                );
            }
            ConditionVariableCancelSleep();
        }

        ereport!(FATAL, errmsg!("recovery aborted because of insufficient parameter settings",
                                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                   errdetail("%s = %d is a lower setting than on the primary server...", param_name, currValue, minValue),
                                   errhint("You can restart the server after making the necessary configuration changes.") */));
    }
}


/*
 * GUC check_hook for primary_slot_name
 */
pub unsafe fn check_primary_slot_name(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    let mut err_code: c_int = 0;
    let mut err_msg: *mut c_char = null_mut();
    let mut err_hint: *mut c_char = null_mut();

    if !(*newval).is_null()
        && **newval != 0
        && !ReplicationSlotValidateNameInternal(
            *newval,
            &mut err_code as *mut c_int,
            &mut err_msg as *mut *mut c_char,
            &mut err_hint as *mut *mut c_char,
        )
    {
        GUC_check_errcode(err_code);
        GUC_check_errdetail(err_msg);
        if !err_hint.is_null() {
            GUC_check_errhint(err_hint);
        }
        return false;
    }

    true
}

/*
 * Recovery target settings: Only one of the several recovery_target* settings
 * may be set.  Setting a second one results in an error.  The global variable
 * recoveryTarget tracks which kind of recovery target was chosen.  Other
 * variables store the actual target value (for example a string or a xid).
 * The assign functions of the parameters check whether a competing parameter
 * was already set.  But we want to allow setting the same parameter multiple
 * times.  We also want to allow unsetting a parameter and setting a different
 * one, so we unset recoveryTarget when the parameter is set to an empty
 * string.
 *
 * XXX this code is broken by design.  Throwing an error from a GUC assign
 * hook breaks fundamental assumptions of guc.c.  So long as all the variables
 * for which this can happen are PGC_POSTMASTER, the consequences are limited,
 * since we'd just abort postmaster startup anyway.  Nonetheless it's likely
 * that we have odd behaviors such as unexpected GUC ordering dependencies.
 */

unsafe fn error_multiple_recovery_targets() -> ! {
    ereport!(ERROR, errmsg!("multiple recovery targets specified",
                            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errdetail("At most one of \"recovery_target\", ... may be set.") */));
    core::hint::unreachable_unchecked()
}

/*
 * GUC check_hook for recovery_target
 */
pub unsafe fn check_recovery_target(
    newval: *mut *mut c_char,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if libc_strcmp(*newval, b"immediate\0".as_ptr() as *const c_char) != 0
        && libc_strcmp(*newval, b"\0".as_ptr() as *const c_char) != 0
    {
        GUC_check_errdetail(b"The only allowed value is \"immediate\".\0".as_ptr() as *const c_char);
        return false;
    }
    true
}

/*
 * GUC assign_hook for recovery_target
 */
pub unsafe fn assign_recovery_target(newval: *const c_char, _extra: *mut c_void) {
    if recoveryTarget != RECOVERY_TARGET_UNSET && recoveryTarget != RECOVERY_TARGET_IMMEDIATE {
        error_multiple_recovery_targets();
    }

    if !newval.is_null() && *newval != 0 {
        recoveryTarget = RECOVERY_TARGET_IMMEDIATE;
    } else {
        recoveryTarget = RECOVERY_TARGET_UNSET;
    }
}

/*
 * GUC check_hook for recovery_target_lsn
 */
pub unsafe fn check_recovery_target_lsn(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if libc_strcmp(*newval, b"\0".as_ptr() as *const c_char) != 0 {
        let lsn: XLogRecPtr;
        let myextra: *mut XLogRecPtr;
        let mut have_error: bool = false;

        lsn = pg_lsn_in_internal(*newval, &mut have_error as *mut bool);
        if have_error {
            return false;
        }

        myextra = guc_malloc(LOG, size_of::<XLogRecPtr>()) as *mut XLogRecPtr;
        if myextra.is_null() {
            return false;
        }
        *myextra = lsn;
        *extra = myextra as *mut c_void;
    }
    true
}

/*
 * GUC assign_hook for recovery_target_lsn
 */
pub unsafe fn assign_recovery_target_lsn(newval: *const c_char, extra: *mut c_void) {
    if recoveryTarget != RECOVERY_TARGET_UNSET && recoveryTarget != RECOVERY_TARGET_LSN {
        error_multiple_recovery_targets();
    }

    if !newval.is_null() && *newval != 0 {
        recoveryTarget = RECOVERY_TARGET_LSN;
        recoveryTargetLSN = *(extra as *mut XLogRecPtr);
    } else {
        recoveryTarget = RECOVERY_TARGET_UNSET;
    }
}

/*
 * GUC check_hook for recovery_target_name
 */
pub unsafe fn check_recovery_target_name(
    newval: *mut *mut c_char,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    /* Use the value of newval directly */
    if c_strlen(*newval) >= MAXFNAMELEN {
        GUC_check_errdetail(b"\"recovery_target_name\" is too long (maximum %d characters).\0".as_ptr() as *const c_char);
        return false;
    }
    true
}

/*
 * GUC assign_hook for recovery_target_name
 */
pub unsafe fn assign_recovery_target_name(newval: *const c_char, _extra: *mut c_void) {
    if recoveryTarget != RECOVERY_TARGET_UNSET && recoveryTarget != RECOVERY_TARGET_NAME {
        error_multiple_recovery_targets();
    }

    if !newval.is_null() && *newval != 0 {
        recoveryTarget = RECOVERY_TARGET_NAME;
        recoveryTargetName = newval;
    } else {
        recoveryTarget = RECOVERY_TARGET_UNSET;
    }
}

/*
 * GUC check_hook for recovery_target_time
 *
 * The interpretation of the recovery_target_time string can depend on the
 * time zone setting, so we need to wait until after all GUC processing is
 * done before we can do the final parsing of the string.  This check function
 * only does a parsing pass to catch syntax errors, but we store the string
 * and parse it again when we need to use it.
 */
pub unsafe fn check_recovery_target_time(
    newval: *mut *mut c_char,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if libc_strcmp(*newval, b"\0".as_ptr() as *const c_char) != 0 {
        /* reject some special values */
        if libc_strcmp(*newval, b"now\0".as_ptr() as *const c_char) == 0
            || libc_strcmp(*newval, b"today\0".as_ptr() as *const c_char) == 0
            || libc_strcmp(*newval, b"tomorrow\0".as_ptr() as *const c_char) == 0
            || libc_strcmp(*newval, b"yesterday\0".as_ptr() as *const c_char) == 0
        {
            return false;
        }

        /*
         * parse timestamp value (see also timestamptz_in())
         */
        {
            let str_: *const c_char = *newval;
            let mut fsec: fsec_t = 0;
            let mut tt: pg_tm = core::mem::zeroed();
            let tm: *mut pg_tm = &mut tt as *mut pg_tm;
            let mut tz: c_int = 0;
            let mut dtype: c_int = 0;
            let mut nf: c_int = 0;
            let mut dterr: c_int;
            let mut field: [*mut c_char; MAXDATEFIELDS] = [null_mut(); MAXDATEFIELDS];
            let mut ftype: [c_int; MAXDATEFIELDS] = [0; MAXDATEFIELDS];
            let mut workbuf: [c_char; MAXDATELEN + MAXDATEFIELDS] = [0; MAXDATELEN + MAXDATEFIELDS];
            let mut dtextra: DateTimeErrorExtra = DateTimeErrorExtra { _pad: 0 };
            let mut timestamp: TimestampTz = 0;

            dterr = ParseDateTime(
                str_,
                workbuf.as_mut_ptr(),
                core::mem::size_of_val(&workbuf),
                field.as_mut_ptr(),
                ftype.as_mut_ptr(),
                MAXDATEFIELDS as c_int,
                &mut nf as *mut c_int,
            );
            if dterr == 0 {
                dterr = DecodeDateTime(
                    field.as_mut_ptr(),
                    ftype.as_mut_ptr(),
                    nf,
                    &mut dtype as *mut c_int,
                    tm,
                    &mut fsec as *mut fsec_t,
                    &mut tz as *mut c_int,
                    &mut dtextra as *mut DateTimeErrorExtra,
                );
            }
            if dterr != 0 {
                return false;
            }
            if dtype != DTK_DATE {
                return false;
            }

            if tm2timestamp(tm, fsec, &mut tz as *mut c_int, &mut timestamp as *mut TimestampTz) != 0 {
                GUC_check_errdetail(b"Timestamp out of range: \"%s\".\0".as_ptr() as *const c_char);
                return false;
            }
        }
    }
    true
}

/*
 * GUC assign_hook for recovery_target_time
 */
pub unsafe fn assign_recovery_target_time(newval: *const c_char, _extra: *mut c_void) {
    if recoveryTarget != RECOVERY_TARGET_UNSET && recoveryTarget != RECOVERY_TARGET_TIME {
        error_multiple_recovery_targets();
    }

    if !newval.is_null() && *newval != 0 {
        recoveryTarget = RECOVERY_TARGET_TIME;
    } else {
        recoveryTarget = RECOVERY_TARGET_UNSET;
    }
}

/*
 * GUC check_hook for recovery_target_timeline
 */
pub unsafe fn check_recovery_target_timeline(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    let rttg: RecoveryTargetTimeLineGoal;
    let myextra: *mut RecoveryTargetTimeLineGoal;

    if libc_strcmp(*newval, b"current\0".as_ptr() as *const c_char) == 0 {
        rttg = RECOVERY_TARGET_TIMELINE_CONTROLFILE;
    } else if libc_strcmp(*newval, b"latest\0".as_ptr() as *const c_char) == 0 {
        rttg = RECOVERY_TARGET_TIMELINE_LATEST;
    } else {
        rttg = RECOVERY_TARGET_TIMELINE_NUMERIC;

        *libc_errno() = 0;
        libc_strtoul(*newval, null_mut(), 0);
        if *libc_errno() == EINVAL || *libc_errno() == ERANGE {
            GUC_check_errdetail(b"\"recovery_target_timeline\" is not a valid number.\0".as_ptr() as *const c_char);
            return false;
        }
    }

    myextra = guc_malloc(LOG, size_of::<RecoveryTargetTimeLineGoal>()) as *mut RecoveryTargetTimeLineGoal;
    if myextra.is_null() {
        return false;
    }
    *myextra = rttg;
    *extra = myextra as *mut c_void;

    true
}

/*
 * GUC assign_hook for recovery_target_timeline
 */
pub unsafe fn assign_recovery_target_timeline(newval: *const c_char, extra: *mut c_void) {
    recoveryTargetTimeLineGoal = *(extra as *mut RecoveryTargetTimeLineGoal);
    if recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC {
        recoveryTargetTLIRequested = libc_strtoul(newval, null_mut(), 0) as TimeLineID;
    } else {
        recoveryTargetTLIRequested = 0;
    }
}

/*
 * GUC check_hook for recovery_target_xid
 */
pub unsafe fn check_recovery_target_xid(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    if libc_strcmp(*newval, b"\0".as_ptr() as *const c_char) != 0 {
        let xid: TransactionId;
        let myextra: *mut TransactionId;

        *libc_errno() = 0;
        xid = strtou64(*newval, null_mut(), 0) as TransactionId;
        if *libc_errno() == EINVAL || *libc_errno() == ERANGE {
            return false;
        }

        myextra = guc_malloc(LOG, size_of::<TransactionId>()) as *mut TransactionId;
        if myextra.is_null() {
            return false;
        }
        *myextra = xid;
        *extra = myextra as *mut c_void;
    }
    true
}

/*
 * GUC assign_hook for recovery_target_xid
 */
pub unsafe fn assign_recovery_target_xid(newval: *const c_char, extra: *mut c_void) {
    if recoveryTarget != RECOVERY_TARGET_UNSET && recoveryTarget != RECOVERY_TARGET_XID {
        error_multiple_recovery_targets();
    }

    if !newval.is_null() && *newval != 0 {
        recoveryTarget = RECOVERY_TARGET_XID;
        recoveryTargetXid = *(extra as *mut TransactionId);
    } else {
        recoveryTarget = RECOVERY_TARGET_UNSET;
    }
}

/* libc wrappers needed by the above functions */

/// TODO(pg-port): libc stat struct (Darwin)
pub type libc_stat = StatBuf;

/// TODO(pg-port): strcmp libc wrapper
unsafe fn libc_strcmp(s1: *const c_char, s2: *const c_char) -> c_int {
    unimplemented!("TODO(pg-port): strcmp")
}

/// TODO(pg-port): strtoul libc wrapper
unsafe fn libc_strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> u64 {
    libc::strtoul(s, endptr, base) as u64
}

/// TODO(pg-port): snprintf libc wrapper (one-arg form)
unsafe fn libc_snprintf(_buf: *mut c_char, _size: usize, _fmt: *const c_char) -> c_int {
    libc::snprintf(_buf, _size, _fmt)
}
/// TODO(pg-port): snprintf libc wrapper (two-arg form)
unsafe fn libc_snprintf2(_buf: *mut c_char, _size: usize, _fmt: *const c_char, _arg: *const c_char) -> c_int {
    libc::snprintf(_buf, _size, _fmt, _arg)
}
