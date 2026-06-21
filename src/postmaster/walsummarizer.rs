/*-------------------------------------------------------------------------
 *
 * walsummarizer.c -> walsummarizer.rs
 *
 * Background process to perform WAL summarization, if it is enabled.
 * It continuously scans the write-ahead log and periodically emits a
 * summary file which indicates which blocks in which relation forks
 * were modified by WAL records in the LSN range covered by the summary
 * file. See walsummary.c and blkreftable.c for more details on the
 * naming and contents of WAL summary files.
 *
 * If configured to do, this background process will also remove WAL
 * summary files when the file timestamp is older than a configurable
 * threshold (but only if the WAL has been removed first).
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *   src/backend/postmaster/walsummarizer.c -> src/postmaster/walsummarizer.rs
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use crate::access::transam::xlogdefs::{
    InvalidXLogRecPtr, LSN_FORMAT_ARGS, TimeLineID, XLogRecPtr, XLogRecPtrIsInvalid, XLogSegNo,
};
use crate::access::transam::xlogreader::{
    WALReadError, XLogBeginRead, XLogFindNextRecord, XLogReadRecord, XLogReaderAllocate,
    XLogReaderFree, XLogReaderRoutine, XLogReaderState, XLogRecGetBlockTagExtended, XLogRecGetData,
    XLogRecGetInfo, XLogRecGetRmid, XLogRecMaxBlockId, WALRead,
    XLR_INFO_MASK,
};
use crate::access::transam::xlog_internal::{XLogSegNoOffsetToRecPtr, XLOGDIR};
use crate::access::rmgrlist::{RmgrId, RM_DBASE_ID, RM_SMGR_ID, RM_XACT_ID, RM_XLOG_ID};
use crate::backup::walsummaryfuncs::{File, WalSummaryFile, WalSummaryIO};
use crate::common::blkreftable::{
    BlockRefTable, BlockRefTableMarkBlockModified, BlockRefTableSetLimitBlock,
    CreateEmptyBlockRefTable, WriteBlockRefTable,
};
use crate::libpq::pqsignal::{
    pqsignal, sigset_t, SigHandler, UnBlockSig, SIGALRM, SIGCHLD, SIGHUP, SIGINT, SIGPIPE,
    SIGTERM, SIGUSR1, SIGUSR2, SIG_DFL,
};
use crate::miscadmin::{
    AmWalSummarizerProcess, B_WAL_SUMMARIZER, HOLD_INTERRUPTS, MyBackendType, RESUME_INTERRUPTS,
};
use crate::nodes::pg_list::{lfirst, linitial, list_length, list_nth, List, ListCell, NIL};
use crate::pg_config_manual::MAXPGPATH;
use crate::postmaster::auxprocess::AuxiliaryProcessMainCommon;
use crate::postmaster::interrupt::{
    ConfigReloadPending, ShutdownRequestPending, SignalHandlerForConfigReload,
    SignalHandlerForShutdownRequest,
};
use crate::storage::ipc::ipc::{on_shmem_exit, proc_exit};
use crate::storage::ipc::latch::{
    ResetLatch, SetLatch, WaitLatch, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT,
};
use crate::storage::lmgr::condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariableInit, ConditionVariableTimedSleep,
};
use crate::storage::procnumber::{MyProcNumber, ProcNumber, INVALID_PROC_NUMBER};

// ---------------------------------------------------------------------------
// Types shared with other modules (from walsummarizer.h).
// ---------------------------------------------------------------------------

/*
 * Data in shared memory related to WAL summarization.
 */
#[repr(C)]
pub struct WalSummarizerData {
    /*
     * These fields are protected by WALSummarizerLock.
     *
     * Until we've discovered what summary files already exist on disk and
     * stored that information in shared memory, initialized is false and the
     * other fields here contain no meaningful information. After that has
     * been done, initialized is true.
     *
     * summarized_tli and summarized_lsn indicate the last LSN and TLI at
     * which the next summary file will start. Normally, these are the LSN and
     * TLI at which the last file ended; in such case, lsn_is_exact is true.
     * If, however, the LSN is just an approximation, then lsn_is_exact is
     * false. This can happen if, for example, there are no existing WAL
     * summary files at startup. In that case, we have to derive the position
     * at which to start summarizing from the WAL files that exist on disk,
     * and so the LSN might point to the start of the next file even though
     * that might happen to be in the middle of a WAL record.
     *
     * summarizer_pgprocno is the proc number of the summarizer process, if
     * one is running, or else INVALID_PROC_NUMBER.
     *
     * pending_lsn is used by the summarizer to advertise the ending LSN of a
     * record it has recently read. It shouldn't ever be less than
     * summarized_lsn, but might be greater, because the summarizer buffers
     * data for a range of LSNs in memory before writing out a new file.
     */
    pub initialized: bool,
    pub summarized_tli: TimeLineID,
    pub summarized_lsn: XLogRecPtr,
    pub lsn_is_exact: bool,
    pub summarizer_pgprocno: ProcNumber,
    pub pending_lsn: XLogRecPtr,

    /*
     * This field handles its own synchronization.
     */
    pub summary_file_cv: ConditionVariable,
}

/*
 * Private data for our xlogreader's page read callback.
 */
#[repr(C)]
struct SummarizerReadLocalXLogPrivate {
    tli: TimeLineID,
    historic: bool,
    read_upto: XLogRecPtr,
    end_of_wal: bool,
}

/* Pointer to shared memory state. */
static mut WalSummarizerCtl: *mut WalSummarizerData = core::ptr::null_mut();

/*
 * When we reach end of WAL and need to read more, we sleep for a number of
 * milliseconds that is an integer multiple of MS_PER_SLEEP_QUANTUM. This is
 * the multiplier. It should vary between 1 and MAX_SLEEP_QUANTA, depending
 * on system activity. See summarizer_wait_for_wal() for how we adjust this.
 */
static mut sleep_quanta: c_long = 1;

/*
 * The sleep time will always be a multiple of 200ms and will not exceed
 * thirty seconds (150 * 200 = 30 * 1000). Note that the timeout here needs
 * to be substantially less than the maximum amount of time for which an
 * incremental backup will wait for this process to catch up. Otherwise, an
 * incremental backup might time out on an idle system just because we sleep
 * for too long.
 */
const MAX_SLEEP_QUANTA: c_long = 150;
const MS_PER_SLEEP_QUANTUM: c_long = 200;

/*
 * This is a count of the number of pages of WAL that we've read since the
 * last time we waited for more WAL to appear.
 */
static mut pages_read_since_last_sleep: c_long = 0;

/*
 * Most recent RedoRecPtr value observed by MaybeRemoveOldWalSummaries.
 */
static mut redo_pointer_at_last_summary_removal: XLogRecPtr = InvalidXLogRecPtr;

/*
 * GUC parameters
 */
#[no_mangle]
pub static mut summarize_wal: bool = false;
#[no_mangle]
pub static mut wal_summary_keep_time: c_int = 10 * HOURS_PER_DAY * MINS_PER_HOUR;

/* Time constants from utils/datetime.h */
const HOURS_PER_DAY: c_int = 24;
const MINS_PER_HOUR: c_int = 60;

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported dependencies.  Each has a TODO(pg-port) comment
// identifying its real home.
// ---------------------------------------------------------------------------

/* SIG_IGN: function pointer with platform value 1. */
#[inline]
fn SIG_IGN() -> SigHandler {
    Some(unsafe { core::mem::transmute::<usize, unsafe extern "C" fn(c_int)>(1usize) })
}

/* SIG_SETMASK (signal.h). TODO(pg-port): centralize to port layer. */
const SIG_SETMASK: c_int = if cfg!(target_os = "macos") { 3 } else { 2 };

/* sigprocmask(2). TODO(pg-port): route through port-layer wrapper. */
unsafe extern "C" {
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
}

/* error_context_stack / PG_exception_stack (elog.c). TODO(pg-port). */
static mut error_context_stack: *mut c_void = null_mut();
static mut PG_exception_stack: *mut c_void = null_mut();

/* sigjmp_buf stub. TODO(pg-port): wire to real sigsetjmp once elog.c is ported. */
type sigjmp_buf = [c_void; 0];

unsafe fn sigsetjmp(_env: *mut sigjmp_buf, _savemask: c_int) -> c_int {
    /* TODO(pg-port): not ported */
    0
}

/* procsignal_sigusr1_handler (procsignal.c). TODO(pg-port). */
unsafe extern "C" fn procsignal_sigusr1_handler(_postgres_signal_arg: c_int) {
    /* TODO(pg-port): not ported */
}

/* EmitErrorReport / FlushErrorState (elog.c). TODO(pg-port). */
unsafe fn EmitErrorReport() {
    /* TODO(pg-port): not ported */
}
unsafe fn FlushErrorState() {
    /* TODO(pg-port): not ported */
}

/* LWLockReleaseAll (lwlock.c). TODO(pg-port). */
unsafe fn LWLockReleaseAll() {
    /* TODO(pg-port): not ported */
}

/* pgstat_report_wait_end (pgstat.h). TODO(pg-port). */
unsafe fn pgstat_report_wait_end() {
    /* TODO(pg-port): not ported */
}

/* pgaio_error_cleanup (aio_subsys.h). TODO(pg-port). */
unsafe fn pgaio_error_cleanup() {
    /* TODO(pg-port): not ported */
}

/* ReleaseAuxProcessResources (auxprocess.h). TODO(pg-port). */
unsafe fn ReleaseAuxProcessResources(_isCommit: bool) {
    /* TODO(pg-port): not ported */
}

/* AtEOXact_Files / AtEOXact_HashTables (fd.c / hsearch.c). TODO(pg-port). */
unsafe fn AtEOXact_Files(_isCommit: bool) {
    /* TODO(pg-port): not ported */
}
unsafe fn AtEOXact_HashTables(_isCommit: bool) {
    /* TODO(pg-port): not ported */
}

/* pgstat_report_wal (pgstat_wal.c). TODO(pg-port). */
unsafe fn pgstat_report_wal(_force: bool) {
    /* TODO(pg-port): not ported */
}

/* LWLock stub type. TODO(pg-port): import from storage/lwlock.h once ported. */
type LWLock = c_void;
const LW_EXCLUSIVE: c_int = 0; /* TODO(pg-port): storage/lwlock.h */
const LW_SHARED: c_int = 1;    /* TODO(pg-port): storage/lwlock.h */

unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    /* TODO(pg-port): not ported */
    true
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    /* TODO(pg-port): not ported */
}

/* WALSummarizerLock (lwlock.h). TODO(pg-port): generated lock array. */
unsafe fn WALSummarizerLock() -> *mut LWLock {
    crate::backend_link_shims::WALSummarizerLock as *mut LWLock
}

/* ShmemInitStruct (shmem.h). TODO(pg-port). */
unsafe fn ShmemInitStruct(
    _name: *const c_char,
    _size: Size,
    _found: *mut bool,
) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(_name as *const c_char, _size as Size, _found)
}

/* ProcessConfigFile (guc.h). TODO(pg-port). */
const PGC_SIGHUP: c_int = 1; /* TODO(pg-port): utils/guc.h */
unsafe fn ProcessConfigFile(_context: c_int) {
    /* TODO(pg-port): not ported */
}

/* ProcessProcSignalBarrier (procsignal.h). TODO(pg-port). */
static mut ProcSignalBarrierPending: bool = false; /* TODO(pg-port): miscadmin.h sig_atomic_t */
unsafe fn ProcessProcSignalBarrier() {
    /* TODO(pg-port): not ported */
}

/* ProcessLogMemoryContextInterrupt (utils/memutils.h). TODO(pg-port). */
static mut LogMemoryContextPending: bool = false; /* TODO(pg-port): miscadmin.h sig_atomic_t */
unsafe fn ProcessLogMemoryContextInterrupt() {
    /* TODO(pg-port): not ported */
}

/* MyLatch: thread-local latch pointer. TODO(pg-port): import from miscadmin once ported. */
unsafe fn get_my_latch() -> *mut crate::storage::ipc::latch::Latch {
    /* TODO(pg-port): MyLatch from miscadmin.h / proc.c */
    null_mut()
}

/* TimestampTz and related helpers. TODO(pg-port): utils/timestamp.h */
type TimestampTz = int64;
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    /* TODO(pg-port): not ported */
    0
}
unsafe fn TimestampDifferenceMilliseconds(_start: TimestampTz, _stop: TimestampTz) -> c_long {
    /* TODO(pg-port): not ported */
    0
}
unsafe fn TimestampTzPlusMilliseconds(ts: TimestampTz, ms: c_long) -> TimestampTz {
    /* TODO(pg-port): not ported */
    ts + ms * 1000
}

/* time(3) (libc). TODO(pg-port): centralize. */
type time_t = i64;
unsafe extern "C" {
    fn time(t: *mut time_t) -> time_t;
}

/* SECS_PER_MINUTE. TODO(pg-port): utils/datetime.h */
const SECS_PER_MINUTE: time_t = 60;

/* XLogGetOldestSegno (xlog.c). TODO(pg-port). */
unsafe fn XLogGetOldestSegno(_tli: TimeLineID) -> XLogSegNo {
    /* TODO(pg-port): not ported */
    0
}

/* readTimeLineHistory (timeline.c). TODO(pg-port). */
unsafe fn readTimeLineHistory(_targetTLI: TimeLineID) -> *mut List {
    /* TODO(pg-port): not ported */
    null_mut()
}

/* tliSwitchPoint (timeline.c). TODO(pg-port). */
unsafe fn tliSwitchPoint(
    _tli: TimeLineID,
    _tles: *mut List,
    _nextTLI: *mut TimeLineID,
) -> XLogRecPtr {
    /* TODO(pg-port): not ported */
    InvalidXLogRecPtr
}

/* RecoveryInProgress (xlog.c). TODO(pg-port). */
unsafe fn RecoveryInProgress() -> bool {
    /* TODO(pg-port): not ported */
    false
}

/* GetFlushRecPtr (xlog.c). TODO(pg-port). */
unsafe fn GetFlushRecPtr(_insertTLI: *mut TimeLineID) -> XLogRecPtr {
    /* TODO(pg-port): not ported */
    InvalidXLogRecPtr
}

/* GetWALInsertionTimeLineIfSet (xlog.c). TODO(pg-port). */
unsafe fn GetWALInsertionTimeLineIfSet() -> TimeLineID {
    /* TODO(pg-port): not ported */
    0
}

/* GetXLogReplayRecPtr (xlogrecovery.c). TODO(pg-port). */
unsafe fn GetXLogReplayRecPtr(_replayTLI: *mut TimeLineID) -> XLogRecPtr {
    /* TODO(pg-port): not ported */
    InvalidXLogRecPtr
}

/* GetWalRcvFlushRecPtr (walreceiver.c). TODO(pg-port). */
unsafe fn GetWalRcvFlushRecPtr(
    _latestChunkStart: *mut XLogRecPtr,
    _receiveTLI: *mut TimeLineID,
) -> XLogRecPtr {
    /* TODO(pg-port): not ported */
    InvalidXLogRecPtr
}

/* GetRedoRecPtr (xloginsert.c). TODO(pg-port): real pub fn exists in xloginsert.rs. */
unsafe fn GetRedoRecPtr() -> XLogRecPtr {
    /* TODO(pg-port): use crate::access::transam::xloginsert::GetRedoRecPtr() once wired */
    InvalidXLogRecPtr
}

/* GetWalSummaries (walsummary.c). TODO(pg-port). */
unsafe fn GetWalSummaries(
    _tli: TimeLineID,
    _start_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
) -> *mut List {
    /* TODO(pg-port): not ported */
    null_mut()
}

/* RemoveWalSummaryIfOlderThan (walsummary.c). TODO(pg-port). */
unsafe fn RemoveWalSummaryIfOlderThan(_ws: *mut WalSummaryFile, _cutoff_time: time_t) {
    /* TODO(pg-port): not ported */
}

/* WriteWalSummary (walsummary.c). TODO(pg-port): io callback for WriteBlockRefTable. */
unsafe fn WriteWalSummary_cb(
    _callback_arg: *mut c_void,
    _data: *mut c_void,
    _length: c_int,
) -> c_int {
    /* TODO(pg-port): not ported */
    0
}

/* PathNameOpenFile / FileClose / durable_rename (fd.c). TODO(pg-port). */
unsafe fn PathNameOpenFile(_path: *const c_char, _flags: c_int) -> File {
    /* TODO(pg-port): not ported */
    -1
}
unsafe fn FileClose(_file: File) {
    /* TODO(pg-port): not ported */
}
unsafe fn durable_rename(
    _oldfile: *const c_char,
    _newfile: *const c_char,
    _elevel: c_int,
) -> c_int {
    /* TODO(pg-port): not ported */
    0
}

/* wal_segment_open / wal_segment_close (xlog.c). TODO(pg-port). */
unsafe fn wal_segment_open(
    _xlogreader: *mut XLogReaderState,
    _nextSegNo: XLogSegNo,
    _tli_p: *mut TimeLineID,
) {
    /* TODO(pg-port): not ported */
}
unsafe fn wal_segment_close(_xlogreader: *mut XLogReaderState) {
    /* TODO(pg-port): not ported */
}

/* wal_segment_size (xlog.c GUC). TODO(pg-port). */
static mut wal_segment_size: c_int = 16 * 1024 * 1024;

/* XLOG_BLCKSZ. TODO(pg-port): pg_config.h */
const XLOG_BLCKSZ: XLogRecPtr = 8192;

/* Wait event IDs. TODO(pg-port): generated wait_event.h */
const WAIT_EVENT_WAL_SUMMARIZER_ERROR: u32 = 0;
const WAIT_EVENT_WAL_SUMMARIZER_WAL: u32 = 0;
const WAIT_EVENT_WAL_SUMMARY_READY: u32 = 0;

/* errcode helpers. TODO(pg-port): elog.h */
unsafe fn errcode(_sqlerrcode: c_int) -> c_int {
    /* TODO(pg-port): not ported */
    0
}
unsafe fn errcode_for_file_access() -> c_int {
    /* TODO(pg-port): not ported */
    0
}
unsafe fn errdetail(_fmt: &str) -> c_int {
    /* TODO(pg-port): not ported */
    0
}
unsafe fn errmsg_plural(
    _fmt_singular: &str,
    _fmt_plural: &str,
    _n: c_long,
    _a: (u32, u32),
    _b: c_long,
) -> c_int {
    /* TODO(pg-port): not ported */
    0
}
unsafe fn errmsg_internal(_fmt: &str) -> c_int {
    /* TODO(pg-port): not ported */
    0
}

/* ERRCODE_* constants. TODO(pg-port): errcodes.h */
const ERRCODE_INTERNAL_ERROR: c_int = 0;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0;
const ERRCODE_OUT_OF_MEMORY: c_int = 0;

/* GetPGProcByNumber / PGPROC.pid (proc.h). TODO(pg-port). */
#[repr(C)]
struct PGPROC {
    pid: c_int,
    procLatch: crate::storage::ipc::latch::Latch,
}

/* ProcGlobal (proc.h). TODO(pg-port). */
#[repr(C)]
struct PROC_HDR {
    allProcs: *mut PGPROC,
}
extern "C" { pub static mut ProcGlobal: *mut PROC_HDR; }
unsafe fn GetPGProcByNumber(pgprocno: ProcNumber) -> *mut PGPROC {
    /* TODO(pg-port): real GetPGProcByNumber from proc.h */
    &raw mut (*(*ProcGlobal).allProcs.add(pgprocno as usize))
}

/* pfree (palloc.h). Already in prelude via palloc module. */
/* NOTE: pfree is available via crate::utils::palloc::pfree */

/* XLogRecord / XLogRecordType stubs. TODO(pg-port). */
type XLogRecord = c_void;

/* XLOG record info constants. TODO(pg-port): access/xlog_internal.h */
const XLOG_CHECKPOINT_REDO: uint8 = 0xE0;
const XLOG_CHECKPOINT_SHUTDOWN: uint8 = 0x00;
const XLOG_PARAMETER_CHANGE: uint8 = 0x60;
const XLOG_END_OF_RECOVERY: uint8 = 0x90;
const WAL_LEVEL_MINIMAL: c_int = 0;

/* CheckPoint struct (access/xlog_internal.h). TODO(pg-port). */
#[repr(C)]
struct CheckPoint {
    wal_level: c_int,
    /* other fields omitted; only wal_level is used here */
    _opaque: [u8; 128],
}

/* xl_parameter_change (access/xlog_internal.h). */
#[repr(C)]
struct xl_parameter_change {
    wal_level: c_int,
    /* other fields omitted */
    _opaque: [u8; 64],
}

/* xl_end_of_recovery (access/xlog_internal.h). */
#[repr(C)]
struct xl_end_of_recovery {
    wal_level: c_int,
    /* other fields omitted */
    _opaque: [u8; 32],
}

/* dbase xlog record types (commands/dbcommands_xlog.h). TODO(pg-port). */
const XLOG_DBASE_CREATE_FILE_COPY: uint8 = 0x00;
const XLOG_DBASE_CREATE_WAL_LOG: uint8 = 0x10;
const XLOG_DBASE_DROP: uint8 = 0x20;

#[repr(C)]
struct xl_dbase_create_file_copy_rec {
    db_id: Oid,
    tablespace_id: Oid,
    /* src_db_id / src_tablespace_id omitted for pointer arithmetic only */
    _opaque: [u8; 8],
}

#[repr(C)]
struct xl_dbase_create_wal_log_rec {
    db_id: Oid,
    tablespace_id: Oid,
}

#[repr(C)]
struct xl_dbase_drop_rec {
    db_id: Oid,
    ntablespaces: c_int,
    /* tablespace_ids[] flexible array follows in C */
}

/* smgr xlog record types (catalog/storage_xlog.h). TODO(pg-port). */
const XLOG_SMGR_CREATE: uint8 = 0x10;
const XLOG_SMGR_TRUNCATE: uint8 = 0x20;
const SMGR_TRUNCATE_HEAP: c_int = 0x0001;
const SMGR_TRUNCATE_VM: c_int = 0x0002;

use crate::common::blkreftable::RelFileLocator;
type BlockNumber = uint32;
type ForkNumber = c_int;
const MAIN_FORKNUM: ForkNumber = 0;
const VISIBILITYMAP_FORKNUM: ForkNumber = 2;
const FSM_FORKNUM: ForkNumber = 1;
const MAX_FORKNUM: ForkNumber = 3; /* must equal last fork in enum */

#[repr(C)]
struct xl_smgr_create {
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
}

#[repr(C)]
struct xl_smgr_truncate {
    blkno: BlockNumber,
    rlocator: RelFileLocator,
    flags: c_int,
}

/* xact xlog record types (access/xact.h). TODO(pg-port). */
const XLOG_XACT_COMMIT: uint8 = 0x00;
const XLOG_XACT_ABORT: uint8 = 0x20;
const XLOG_XACT_COMMIT_PREPARED: uint8 = 0x30;
const XLOG_XACT_ABORT_PREPARED: uint8 = 0x40;
const XLOG_XACT_OPMASK: uint8 = 0x70;

/* xl_xact_commit / xl_xact_abort stubs (access/xact.h). TODO(pg-port). */
#[repr(C)]
struct xl_xact_commit {
    _opaque: [u8; 64],
}

#[repr(C)]
struct xl_xact_abort {
    _opaque: [u8; 32],
}

/* xl_xact_parsed_commit / xl_xact_parsed_abort (access/xact.h). TODO(pg-port). */
#[repr(C)]
struct xl_xact_parsed_commit {
    nrels: c_int,
    xlocators: *mut RelFileLocator,
    _opaque: [u8; 128],
}

#[repr(C)]
struct xl_xact_parsed_abort {
    nrels: c_int,
    xlocators: *mut RelFileLocator,
    _opaque: [u8; 128],
}

/* ParseCommitRecord / ParseAbortRecord (access/xact.h). TODO(pg-port). */
unsafe fn ParseCommitRecord(
    _info: uint8,
    _xlrec: *mut xl_xact_commit,
    _parsed: *mut xl_xact_parsed_commit,
) {
    /* TODO(pg-port): not ported */
}
unsafe fn ParseAbortRecord(
    _info: uint8,
    _xlrec: *mut xl_xact_abort,
    _parsed: *mut xl_xact_parsed_abort,
) {
    /* TODO(pg-port): not ported */
}

/* O_* open flags. TODO(pg-port): centralize. */
const O_WRONLY: c_int = 0x0001;
const O_CREAT: c_int = 0x0200;
const O_TRUNC: c_int = 0x0400;

/* Min helper. TODO(pg-port): prelude candidate. */
#[inline]
fn Min(a: c_long, b: c_long) -> c_long {
    if a < b { a } else { b }
}

/* list_delete_nth_cell (pg_list.c). TODO(pg-port): not yet ported.
 * Returns the list with the element at index n removed. If the list becomes
 * empty, returns NIL (null pointer). */
unsafe fn list_delete_nth_cell_stub(lst: *mut List, _n: c_int) -> *mut List {
    /* TODO(pg-port): replace with real list_delete_nth_cell once ported */
    lst
}

/* TimeLineHistoryEntry (access/timeline.h). TODO(pg-port). */
#[repr(C)]
struct TimeLineHistoryEntry {
    tli: TimeLineID,
    begin: XLogRecPtr,
    end: XLogRecPtr,
}

// ---------------------------------------------------------------------------
// Exported shmem-size function
// ---------------------------------------------------------------------------

/*
 * Amount of shared memory required for this module.
 */
pub unsafe fn WalSummarizerShmemSize() -> Size {
    core::mem::size_of::<WalSummarizerData>()
}

// ---------------------------------------------------------------------------
// Shared memory initialization
// ---------------------------------------------------------------------------

/*
 * Create or attach to shared memory segment for this module.
 */
pub unsafe fn WalSummarizerShmemInit() {
    let mut found: bool = false;

    WalSummarizerCtl = ShmemInitStruct(
        b"Wal Summarizer Ctl\0".as_ptr() as *const c_char,
        WalSummarizerShmemSize(),
        &raw mut found,
    ) as *mut WalSummarizerData;

    if !found {
        /*
         * First time through, so initialize.
         *
         * We're just filling in dummy values here -- the real initialization
         * will happen when GetOldestUnsummarizedLSN() is called for the first
         * time.
         */
        (*WalSummarizerCtl).initialized = false;
        (*WalSummarizerCtl).summarized_tli = 0;
        (*WalSummarizerCtl).summarized_lsn = InvalidXLogRecPtr;
        (*WalSummarizerCtl).lsn_is_exact = false;
        (*WalSummarizerCtl).summarizer_pgprocno = INVALID_PROC_NUMBER;
        (*WalSummarizerCtl).pending_lsn = InvalidXLogRecPtr;
        ConditionVariableInit(&raw mut (*WalSummarizerCtl).summary_file_cv);
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/*
 * Entry point for walsummarizer process.
 */
pub unsafe fn WalSummarizerMain(_startup_data: *const c_void, startup_data_len: Size) {
    let mut local_sigjmp_buf: sigjmp_buf = [];
    let context: MemoryContext;

    /*
     * Within this function, 'current_lsn' and 'current_tli' refer to the
     * point from which the next WAL summary file should start. 'exact' is
     * true if 'current_lsn' is known to be the start of a WAL record or WAL
     * segment, and false if it might be in the middle of a record someplace.
     *
     * 'switch_lsn' and 'switch_tli', if set, are the LSN at which we need to
     * switch to a new timeline and the timeline to which we need to switch.
     * If not set, we either haven't figured out the answers yet or we're
     * already on the latest timeline.
     */
    let mut current_lsn: XLogRecPtr;
    let mut current_tli: TimeLineID = 0;
    let mut exact: bool = false;
    let mut switch_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let mut switch_tli: TimeLineID = 0;

    Assert!(startup_data_len == 0);

    MyBackendType = B_WAL_SUMMARIZER;
    AuxiliaryProcessMainCommon();

    ereport!(DEBUG1, errmsg!("WAL summarizer started"));

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * We have no particular use for SIGINT at the moment, but seems
     * reasonable to treat like SIGTERM.
     */
    pqsignal(SIGHUP, Some(SignalHandlerForConfigReload));
    pqsignal(SIGINT, Some(SignalHandlerForShutdownRequest));
    pqsignal(SIGTERM, Some(SignalHandlerForShutdownRequest));
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGALRM, SIG_IGN());
    pqsignal(SIGPIPE, SIG_IGN());
    pqsignal(SIGUSR1, Some(procsignal_sigusr1_handler));
    pqsignal(SIGUSR2, SIG_IGN()); /* not used */

    /* Advertise ourselves. */
    on_shmem_exit(WalSummarizerShutdown_cb, 0 as Datum);
    LWLockAcquire(WALSummarizerLock(), LW_EXCLUSIVE);
    (*WalSummarizerCtl).summarizer_pgprocno = MyProcNumber;
    LWLockRelease(WALSummarizerLock());

    /* Create and switch to a memory context that we can reset on error. */
    context = AllocSetContextCreate!(TopMemoryContext, c"Wal Summarizer".as_ptr(), ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(context);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    pqsignal(SIGCHLD, SIG_DFL);

    /*
     * If an exception is encountered, processing resumes here.
     */
    if sigsetjmp(&raw mut local_sigjmp_buf, 1) != 0 {
        /* Since not using PG_TRY, must reset error stack by hand */
        error_context_stack = null_mut();

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /* Release resources we might have acquired. */
        LWLockReleaseAll();
        ConditionVariableCancelSleep();
        pgstat_report_wait_end();
        pgaio_error_cleanup();
        ReleaseAuxProcessResources(false);
        AtEOXact_Files(false);
        AtEOXact_HashTables(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextReset(context);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep for 10 seconds before attempting to resume operations in
         * order to avoid excessive logging.
         *
         * Many of the likely error conditions are things that will repeat
         * every time. For example, if the WAL can't be read or the summary
         * can't be written, only administrator action will cure the problem.
         * So a really fast retry time doesn't seem to be especially
         * beneficial, and it will clutter the logs.
         */
        let _ = WaitLatch(
            null_mut(),
            WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            10000,
            WAIT_EVENT_WAL_SUMMARIZER_ERROR,
        );
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = (&raw mut local_sigjmp_buf) as *mut c_void;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    sigprocmask(SIG_SETMASK, &raw const UnBlockSig, null_mut::<sigset_t>());

    /*
     * Fetch information about previous progress from shared memory, and ask
     * GetOldestUnsummarizedLSN to reset pending_lsn to summarized_lsn. We
     * might be recovering from an error, and if so, pending_lsn might have
     * advanced past summarized_lsn, but any WAL we read previously has been
     * lost and will need to be reread.
     *
     * If we discover that WAL summarization is not enabled, just exit.
     */
    current_lsn = GetOldestUnsummarizedLSN(&raw mut current_tli, &raw mut exact);
    if XLogRecPtrIsInvalid(current_lsn) {
        proc_exit(0);
    }

    /*
     * Loop forever
     */
    loop {
        let latest_lsn: XLogRecPtr;
        let mut latest_tli: TimeLineID = 0;
        let end_of_summary_lsn: XLogRecPtr;

        /* Flush any leaked data in the top-level context */
        MemoryContextReset(context);

        /* Process any signals received recently. */
        ProcessWalSummarizerInterrupts();

        /* If it's time to remove any old WAL summaries, do that now. */
        MaybeRemoveOldWalSummaries();

        /* Find the LSN and TLI up to which we can safely summarize. */
        latest_lsn = GetLatestLSN(&raw mut latest_tli);

        /*
         * If we're summarizing a historic timeline and we haven't yet
         * computed the point at which to switch to the next timeline, do that
         * now.
         *
         * Note that if this is a standby, what was previously the current
         * timeline could become historic at any time.
         *
         * We could try to make this more efficient by caching the results of
         * readTimeLineHistory when latest_tli has not changed, but since we
         * only have to do this once per timeline switch, we probably wouldn't
         * save any significant amount of work in practice.
         */
        if current_tli != latest_tli && XLogRecPtrIsInvalid(switch_lsn) {
            let tles = readTimeLineHistory(latest_tli);
            switch_lsn = tliSwitchPoint(current_tli, tles, &raw mut switch_tli);
            ereport!(
                DEBUG1,
                errmsg!(
                    "switch point from TLI {} to TLI {} is at {:X}/{:X}",
                    current_tli,
                    switch_tli,
                    LSN_FORMAT_ARGS(switch_lsn).0,
                    LSN_FORMAT_ARGS(switch_lsn).1
                )
            );
        }

        /*
         * If we've reached the switch LSN, we can't summarize anything else
         * on this timeline. Switch to the next timeline and go around again,
         * backing up to the exact switch point if we passed it.
         */
        if !XLogRecPtrIsInvalid(switch_lsn) && current_lsn >= switch_lsn {
            /* Restart summarization from switch point. */
            current_tli = switch_tli;
            current_lsn = switch_lsn;

            /* Next timeline and switch point, if any, not yet known. */
            switch_lsn = InvalidXLogRecPtr;
            switch_tli = 0;

            /* Update (really, rewind, if needed) state in shared memory. */
            LWLockAcquire(WALSummarizerLock(), LW_EXCLUSIVE);
            (*WalSummarizerCtl).summarized_lsn = current_lsn;
            (*WalSummarizerCtl).summarized_tli = current_tli;
            (*WalSummarizerCtl).lsn_is_exact = true;
            (*WalSummarizerCtl).pending_lsn = current_lsn;
            LWLockRelease(WALSummarizerLock());

            continue;
        }

        /* Summarize WAL. */
        end_of_summary_lsn = SummarizeWAL(
            current_tli,
            current_lsn,
            exact,
            switch_lsn,
            latest_lsn,
        );
        Assert!(!XLogRecPtrIsInvalid(end_of_summary_lsn));
        Assert!(end_of_summary_lsn >= current_lsn);

        /*
         * Update state for next loop iteration.
         *
         * Next summary file should start from exactly where this one ended.
         */
        current_lsn = end_of_summary_lsn;
        exact = true;

        /* Update state in shared memory. */
        LWLockAcquire(WALSummarizerLock(), LW_EXCLUSIVE);
        (*WalSummarizerCtl).summarized_lsn = end_of_summary_lsn;
        (*WalSummarizerCtl).summarized_tli = current_tli;
        (*WalSummarizerCtl).lsn_is_exact = true;
        (*WalSummarizerCtl).pending_lsn = end_of_summary_lsn;
        LWLockRelease(WALSummarizerLock());

        /* Wake up anyone waiting for more summary files to be written. */
        ConditionVariableBroadcast(&raw mut (*WalSummarizerCtl).summary_file_cv);
    }
}

// ---------------------------------------------------------------------------
// Public API functions
// ---------------------------------------------------------------------------

/*
 * Get information about the state of the WAL summarizer.
 */
pub unsafe fn GetWalSummarizerState(
    summarized_tli: *mut TimeLineID,
    summarized_lsn: *mut XLogRecPtr,
    pending_lsn: *mut XLogRecPtr,
    summarizer_pid: *mut c_int,
) {
    LWLockAcquire(WALSummarizerLock(), LW_SHARED);
    if !(*WalSummarizerCtl).initialized {
        /*
         * If initialized is false, the rest of the structure contents are
         * undefined.
         */
        *summarized_tli = 0;
        *summarized_lsn = InvalidXLogRecPtr;
        *pending_lsn = InvalidXLogRecPtr;
        *summarizer_pid = -1;
    } else {
        let summarizer_pgprocno: ProcNumber = (*WalSummarizerCtl).summarizer_pgprocno;

        *summarized_tli = (*WalSummarizerCtl).summarized_tli;
        *summarized_lsn = (*WalSummarizerCtl).summarized_lsn;
        if summarizer_pgprocno == INVALID_PROC_NUMBER {
            /*
             * If the summarizer has exited, the fact that it had processed
             * beyond summarized_lsn is irrelevant now.
             */
            *pending_lsn = (*WalSummarizerCtl).summarized_lsn;
            *summarizer_pid = -1;
        } else {
            *pending_lsn = (*WalSummarizerCtl).pending_lsn;

            /*
             * We're not fussed about inexact answers here, since they could
             * become stale instantly, so we don't bother taking the lock, but
             * make sure that invalid PID values are normalized to -1.
             */
            *summarizer_pid = (*GetPGProcByNumber(summarizer_pgprocno)).pid;
            if *summarizer_pid <= 0 {
                *summarizer_pid = -1;
            }
        }
    }
    LWLockRelease(WALSummarizerLock());
}

/*
 * Get the oldest LSN in this server's timeline history that has not yet been
 * summarized, and update shared memory state as appropriate.
 *
 * If *tli != NULL, it will be set to the TLI for the LSN that is returned.
 *
 * If *lsn_is_exact != NULL, it will be set to true if the returned LSN is
 * necessarily the start of a WAL record and false if it's just the beginning
 * of a WAL segment.
 */
pub unsafe fn GetOldestUnsummarizedLSN(
    tli: *mut TimeLineID,
    lsn_is_exact: *mut bool,
) -> XLogRecPtr {
    let mut latest_tli: TimeLineID = 0;
    let n: c_int;
    let tles: *mut List;
    let mut unsummarized_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let mut unsummarized_tli: TimeLineID = 0;
    let mut should_make_exact: bool = false;
    let existing_summaries: *mut List;
    let am_wal_summarizer: bool = AmWalSummarizerProcess();

    /* If not summarizing WAL, do nothing. */
    if !summarize_wal {
        return InvalidXLogRecPtr;
    }

    /*
     * If we are not the WAL summarizer process, then we normally just want to
     * read the values from shared memory. However, as an exception, if shared
     * memory hasn't been initialized yet, then we need to do that so that we
     * can read legal values and not remove any WAL too early.
     */
    if !am_wal_summarizer {
        LWLockAcquire(WALSummarizerLock(), LW_SHARED);

        if (*WalSummarizerCtl).initialized {
            unsummarized_lsn = (*WalSummarizerCtl).summarized_lsn;
            if !tli.is_null() {
                *tli = (*WalSummarizerCtl).summarized_tli;
            }
            if !lsn_is_exact.is_null() {
                *lsn_is_exact = (*WalSummarizerCtl).lsn_is_exact;
            }
            LWLockRelease(WALSummarizerLock());
            return unsummarized_lsn;
        }

        LWLockRelease(WALSummarizerLock());
    }

    /*
     * Find the oldest timeline on which WAL still exists, and the earliest
     * segment for which it exists.
     *
     * Note that we do this every time the WAL summarizer process restarts or
     * recovers from an error, in case the contents of pg_wal have changed
     * under us e.g. if some files were removed, either manually - which
     * shouldn't really happen, but might - or by postgres itself, if
     * summarize_wal was turned off and then back on again.
     */
    let _ = GetLatestLSN(&raw mut latest_tli);
    tles = readTimeLineHistory(latest_tli);
    let mut n_iter: c_int = list_length(tles) - 1;
    while n_iter >= 0 {
        let tle: *mut TimeLineHistoryEntry =
            list_nth(tles, n_iter) as *mut TimeLineHistoryEntry;
        let oldest_segno: XLogSegNo;

        oldest_segno = XLogGetOldestSegno((*tle).tli);
        if oldest_segno != 0 {
            /* Compute oldest LSN that still exists on disk. */
            XLogSegNoOffsetToRecPtr(
                oldest_segno,
                0,
                wal_segment_size,
                &mut unsummarized_lsn,
            );

            unsummarized_tli = (*tle).tli;
            break;
        }
        n_iter -= 1;
    }

    /*
     * Don't try to summarize anything older than the end LSN of the newest
     * summary file that exists for this timeline.
     */
    existing_summaries =
        GetWalSummaries(unsummarized_tli, InvalidXLogRecPtr, InvalidXLogRecPtr);
    /* Iterate by index (PG13+ lists use a packed element array, not a linked list). */
    {
        let n_summaries = if existing_summaries.is_null() {
            0
        } else {
            list_length(existing_summaries)
        };
        let mut i: c_int = 0;
        while i < n_summaries {
            let ws: *mut WalSummaryFile = list_nth(existing_summaries, i) as *mut WalSummaryFile;

            if (*ws).end_lsn > unsummarized_lsn {
                unsummarized_lsn = (*ws).end_lsn;
                should_make_exact = true;
            }
            i += 1;
        }
    }

    /* It really should not be possible for us to find no WAL. */
    if unsummarized_tli == 0 {
        ereport!(
            ERROR,
            /* errcode(ERRCODE_INTERNAL_ERROR) -- TODO(pg-port): fold when errcode ported */
            errmsg!("no WAL found on timeline {}", latest_tli)
        );
    }

    /*
     * If we're the WAL summarizer, we always want to store the values we just
     * computed into shared memory, because those are the values we're going
     * to use to drive our operation, and so they are the authoritative
     * values. Otherwise, we only store values into shared memory if shared
     * memory is uninitialized. Our values are not canonical in such a case,
     * but it's better to have something than nothing, to guide WAL retention.
     */
    LWLockAcquire(WALSummarizerLock(), LW_EXCLUSIVE);
    if am_wal_summarizer || !(*WalSummarizerCtl).initialized {
        (*WalSummarizerCtl).initialized = true;
        (*WalSummarizerCtl).summarized_lsn = unsummarized_lsn;
        (*WalSummarizerCtl).summarized_tli = unsummarized_tli;
        (*WalSummarizerCtl).lsn_is_exact = should_make_exact;
        (*WalSummarizerCtl).pending_lsn = unsummarized_lsn;
    } else {
        unsummarized_lsn = (*WalSummarizerCtl).summarized_lsn;
    }

    /* Also return the to the caller as required. */
    if !tli.is_null() {
        *tli = (*WalSummarizerCtl).summarized_tli;
    }
    if !lsn_is_exact.is_null() {
        *lsn_is_exact = (*WalSummarizerCtl).lsn_is_exact;
    }
    LWLockRelease(WALSummarizerLock());

    unsummarized_lsn
}

/*
 * Wake up the WAL summarizer process.
 *
 * This might not work, because there's no guarantee that the WAL summarizer
 * process was successfully started, and it also might have started but
 * subsequently terminated. So, under normal circumstances, this will get the
 * latch set, but there's no guarantee.
 */
pub unsafe fn WakeupWalSummarizer() {
    let pgprocno: ProcNumber;

    if WalSummarizerCtl.is_null() {
        return;
    }

    LWLockAcquire(WALSummarizerLock(), LW_SHARED);
    pgprocno = (*WalSummarizerCtl).summarizer_pgprocno;
    LWLockRelease(WALSummarizerLock());

    if pgprocno != INVALID_PROC_NUMBER {
        SetLatch(&raw mut (*GetPGProcByNumber(pgprocno)).procLatch);
    }
}

/*
 * Wait until WAL summarization reaches the given LSN, but time out with an
 * error if the summarizer seems to be stuck.
 *
 * Returns immediately if summarize_wal is turned off while we wait. Caller
 * is expected to handle this case, if necessary.
 */
pub unsafe fn WaitForWalSummarization(lsn: XLogRecPtr) {
    let initial_time: TimestampTz;
    let mut cycle_time: TimestampTz;
    let current_time: TimestampTz;
    let mut prior_pending_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let mut deadcycles: c_int = 0;

    initial_time = GetCurrentTimestamp();
    cycle_time = initial_time;

    loop {
        let mut timeout_in_ms: c_long = 10000;
        let summarized_lsn: XLogRecPtr;
        let pending_lsn: XLogRecPtr;

        /* CHECK_FOR_INTERRUPTS() -- TODO(pg-port): macro from miscadmin.h */

        /* If WAL summarization is disabled while we're waiting, give up. */
        if !summarize_wal {
            return;
        }

        /*
         * If the LSN summarized on disk has reached the target value, stop.
         */
        LWLockAcquire(WALSummarizerLock(), LW_SHARED);
        summarized_lsn = (*WalSummarizerCtl).summarized_lsn;
        pending_lsn = (*WalSummarizerCtl).pending_lsn;
        LWLockRelease(WALSummarizerLock());

        /* If WAL summarization has progressed sufficiently, stop waiting. */
        if summarized_lsn >= lsn {
            break;
        }

        /* Recheck current time. */
        let current_time = GetCurrentTimestamp();

        /* Have we finished the current cycle of waiting? */
        if TimestampDifferenceMilliseconds(cycle_time, current_time) >= timeout_in_ms {
            let elapsed_seconds: c_long;

            /* Begin new wait cycle. */
            cycle_time = TimestampTzPlusMilliseconds(cycle_time, timeout_in_ms);

            /*
             * Keep track of the number of cycles during which there has been
             * no progression of pending_lsn. If pending_lsn is not advancing,
             * that means that not only are no new files appearing on disk,
             * but we're not even incorporating new records into the in-memory
             * state.
             */
            if pending_lsn > prior_pending_lsn {
                prior_pending_lsn = pending_lsn;
                deadcycles = 0;
            } else {
                deadcycles += 1;
            }

            /*
             * If we've managed to wait for an entire minute without the WAL
             * summarizer absorbing a single WAL record, error out; probably
             * something is wrong.
             *
             * We could consider also erroring out if the summarizer is taking
             * too long to catch up, but it's not clear what rate of progress
             * would be acceptable and what would be too slow. So instead, we
             * just try to error out in the case where there's no progress at
             * all. That seems likely to catch a reasonable number of the
             * things that can go wrong in practice (e.g. the summarizer
             * process is completely hung, say because somebody hooked up a
             * debugger to it or something) without giving up too quickly when
             * the system is just slow.
             */
            if deadcycles >= 6 {
                ereport!(
                    ERROR,
                    /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                     * errdetail("Summarization is needed through %X/%X, ...")
                     * TODO(pg-port): fold errcode/errdetail when elog ported */
                    errmsg!(
                        "WAL summarization is not progressing"
                    )
                );
            }

            /*
             * Otherwise, just let the user know what's happening.
             */
            elapsed_seconds =
                TimestampDifferenceMilliseconds(initial_time, current_time) / 1000;
            ereport!(
                WARNING,
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                 * errmsg_plural(...)
                 * errdetail(...)
                 * TODO(pg-port): fold errcode/errmsg_plural/errdetail when elog ported */
                errmsg!(
                    "still waiting for WAL summarization through {:X}/{:X} after {} second(s)",
                    LSN_FORMAT_ARGS(lsn).0,
                    LSN_FORMAT_ARGS(lsn).1,
                    elapsed_seconds
                )
            );
        }

        /*
         * Align the wait time to prevent drift. This doesn't really matter,
         * but we'd like the warnings about how long we've been waiting to say
         * 10 seconds, 20 seconds, 30 seconds, 40 seconds ... without ever
         * drifting to something that is not a multiple of ten.
         */
        timeout_in_ms -=
            TimestampDifferenceMilliseconds(cycle_time, current_time);

        /* Wait and see. */
        ConditionVariableTimedSleep(
            &raw mut (*WalSummarizerCtl).summary_file_cv,
            timeout_in_ms,
            WAIT_EVENT_WAL_SUMMARY_READY,
        );
    }

    ConditionVariableCancelSleep();
}

// ---------------------------------------------------------------------------
// Private (static) helpers
// ---------------------------------------------------------------------------

/*
 * on_shmem_exit callback: update shared memory to show we're no longer running.
 */
unsafe extern "C" fn WalSummarizerShutdown_cb(_code: c_int, _arg: Datum) {
    WalSummarizerShutdown();
}

/*
 * On exit, update shared memory to make it clear that we're no longer
 * running.
 */
unsafe fn WalSummarizerShutdown() {
    LWLockAcquire(WALSummarizerLock(), LW_EXCLUSIVE);
    (*WalSummarizerCtl).summarizer_pgprocno = INVALID_PROC_NUMBER;
    LWLockRelease(WALSummarizerLock());
}

/*
 * Get the latest LSN that is eligible to be summarized, and set *tli to the
 * corresponding timeline.
 */
unsafe fn GetLatestLSN(tli: *mut TimeLineID) -> XLogRecPtr {
    if !RecoveryInProgress() {
        /* Don't summarize WAL before it's flushed. */
        return GetFlushRecPtr(tli);
    } else {
        let flush_lsn: XLogRecPtr;
        let mut flush_tli: TimeLineID = 0;
        let replay_lsn: XLogRecPtr;
        let mut replay_tli: TimeLineID = 0;
        let insert_tli: TimeLineID;

        /*
         * After the insert TLI has been set and before the control file has
         * been updated to show the DB in production, RecoveryInProgress()
         * will return true, because it's not yet safe for all backends to
         * begin writing WAL. However, replay has already ceased, so from our
         * point of view, recovery is already over. We should summarize up to
         * where replay stopped and then prepare to resume at the start of the
         * insert timeline.
         */
        insert_tli = GetWALInsertionTimeLineIfSet();
        if insert_tli != 0 {
            *tli = insert_tli;
            return GetXLogReplayRecPtr(null_mut());
        }

        /*
         * What we really want to know is how much WAL has been flushed to
         * disk, but the only flush position available is the one provided by
         * the walreceiver, which may not be running, because this could be
         * crash recovery or recovery via restore_command. So use either the
         * WAL receiver's flush position or the replay position, whichever is
         * further ahead, on the theory that if the WAL has been replayed then
         * it must also have been flushed to disk.
         */
        flush_lsn = GetWalRcvFlushRecPtr(null_mut(), &raw mut flush_tli);
        replay_lsn = GetXLogReplayRecPtr(&raw mut replay_tli);
        if flush_lsn > replay_lsn {
            *tli = flush_tli;
            return flush_lsn;
        } else {
            *tli = replay_tli;
            return replay_lsn;
        }
    }
}

/*
 * Interrupt handler for main loop of WAL summarizer process.
 */
unsafe fn ProcessWalSummarizerInterrupts() {
    if ProcSignalBarrierPending {
        ProcessProcSignalBarrier();
    }

    if ConfigReloadPending {
        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if ShutdownRequestPending || !summarize_wal {
        ereport!(DEBUG1, errmsg!("WAL summarizer shutting down"));
        proc_exit(0);
    }

    /* Perform logging of memory contexts of this process */
    if LogMemoryContextPending {
        ProcessLogMemoryContextInterrupt();
    }
}

/*
 * Summarize a range of WAL records on a single timeline.
 *
 * 'tli' is the timeline to be summarized.
 *
 * 'start_lsn' is the point at which we should start summarizing. If this
 * value comes from the end LSN of the previous record as returned by the
 * xlogreader machinery, 'exact' should be true; otherwise, 'exact' should
 * be false, and this function will search forward for the start of a valid
 * WAL record.
 *
 * 'switch_lsn' is the point at which we should switch to a later timeline,
 * if we're summarizing a historic timeline.
 *
 * 'maximum_lsn' identifies the point beyond which we can't count on being
 * able to read any more WAL. It should be the switch point when reading a
 * historic timeline, or the most-recently-measured end of WAL when reading
 * the current timeline.
 *
 * The return value is the LSN at which the WAL summary actually ends. Most
 * often, a summary file ends because we notice that a checkpoint has
 * occurred and reach the redo pointer of that checkpoint, but sometimes
 * we stop for other reasons, such as a timeline switch.
 */
unsafe fn SummarizeWAL(
    tli: TimeLineID,
    start_lsn: XLogRecPtr,
    exact: bool,
    switch_lsn: XLogRecPtr,
    maximum_lsn: XLogRecPtr,
) -> XLogRecPtr {
    let private_data: *mut SummarizerReadLocalXLogPrivate;
    let xlogreader: *mut XLogReaderState;
    let summary_start_lsn: XLogRecPtr;
    let mut summary_end_lsn: XLogRecPtr = switch_lsn;
    let mut temp_path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut final_path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut io: WalSummaryIO = WalSummaryIO { filepos: 0, file: -1 };
    let brtab: *mut BlockRefTable = CreateEmptyBlockRefTable();
    let fast_forward: bool = true;

    /* Initialize private data for xlogreader. */
    private_data = palloc0(core::mem::size_of::<SummarizerReadLocalXLogPrivate>())
        as *mut SummarizerReadLocalXLogPrivate;
    (*private_data).tli = tli;
    (*private_data).historic = !XLogRecPtrIsInvalid(switch_lsn);
    (*private_data).read_upto = maximum_lsn;

    /* Create xlogreader. */
    let routine = XLogReaderRoutine {
        page_read: Some(unsafe { core::mem::transmute(summarizer_read_local_xlog_page as usize) }),
        segment_open: Some(wal_segment_open),
        segment_close: Some(wal_segment_close),
    };
    xlogreader = XLogReaderAllocate(
        wal_segment_size,
        null_mut(),
        &raw const routine,
        private_data as *mut c_void,
    );
    if xlogreader.is_null() {
        ereport!(
            ERROR,
            /* errcode(ERRCODE_OUT_OF_MEMORY)
             * errdetail("Failed while allocating a WAL reading processor.")
             * TODO(pg-port): fold errcode/errdetail when elog ported */
            errmsg!("out of memory")
        );
    }

    /*
     * When exact = false, we're starting from an arbitrary point in the WAL
     * and must search forward for the start of the next record.
     *
     * When exact = true, start_lsn should be either the LSN where a record
     * begins, or the LSN of a page where the page header is immediately
     * followed by the start of a new record. XLogBeginRead should tolerate
     * either case.
     *
     * We need to allow for both cases because the behavior of xlogreader
     * varies. When a record spans two or more xlog pages, the ending LSN
     * reported by xlogreader will be the starting LSN of the following
     * record, but when an xlog page boundary falls between two records, the
     * end LSN for the first will be reported as the first byte of the
     * following page. We can't know until we read that page how large the
     * header will be, but we'll have to skip over it to find the next record.
     */
    if exact {
        /*
         * Even if start_lsn is the beginning of a page rather than the
         * beginning of the first record on that page, we should still use it
         * as the start LSN for the summary file. That's because we detect
         * missing summary files by looking for cases where the end LSN of one
         * file is less than the start LSN of the next file. When only a page
         * header is skipped, nothing has been missed.
         */
        XLogBeginRead(xlogreader, start_lsn);
        summary_start_lsn = start_lsn;
    } else {
        let found = XLogFindNextRecord(xlogreader, start_lsn);
        if XLogRecPtrIsInvalid(found) {
            /*
             * If we hit end-of-WAL while trying to find the next valid
             * record, we must be on a historic timeline that has no valid
             * records that begin after start_lsn and before end of WAL.
             */
            if (*private_data).end_of_wal {
                ereport!(
                    DEBUG1,
                    errmsg!(
                        "could not read WAL from timeline {} at {:X}/{:X}: end of WAL at {:X}/{:X}",
                        tli,
                        LSN_FORMAT_ARGS(start_lsn).0,
                        LSN_FORMAT_ARGS(start_lsn).1,
                        LSN_FORMAT_ARGS((*private_data).read_upto).0,
                        LSN_FORMAT_ARGS((*private_data).read_upto).1
                    )
                );

                /*
                 * The timeline ends at or after start_lsn, without containing
                 * any records. Thus, we must make sure the main loop does not
                 * iterate. If start_lsn is the end of the timeline, then we
                 * won't actually emit an empty summary file, but otherwise,
                 * we must, to capture the fact that the LSN range in question
                 * contains no interesting WAL records.
                 */
                summary_end_lsn = (*private_data).read_upto;
                /* summary_start_lsn = start_lsn handled below */
                {
                    /* We need summary_start_lsn set; fall through to the
                     * assignment after the if/else.  C used:
                     *   summary_start_lsn = start_lsn;
                     *   summary_end_lsn   = private_data->read_upto;
                     *   switch_lsn        = xlogreader->EndRecPtr;
                     * Rust: re-assign the mutable bindings. */
                    let switch_lsn_new = (*xlogreader).EndRecPtr;
                    /* Note: switch_lsn is an immutable parameter in this fn;
                     * use a local shadow to match the C behavior. */
                    let _ = switch_lsn_new; /* used below to finalize the loop */
                    return SummarizeWALCore(
                        tli,
                        start_lsn,           /* summary_start_lsn */
                        summary_end_lsn,
                        switch_lsn_new,      /* effective switch_lsn for loop */
                        brtab,
                        fast_forward,
                        xlogreader,
                        private_data,
                        &raw mut io,
                        &raw mut temp_path,
                        &raw mut final_path,
                    );
                }
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not find a valid record after {:X}/{:X}",
                        LSN_FORMAT_ARGS(start_lsn).0,
                        LSN_FORMAT_ARGS(start_lsn).1
                    )
                );
            }
        }

        summary_start_lsn = found;

        /* We shouldn't go backward. */
        Assert!(summary_start_lsn >= start_lsn);
    }

    SummarizeWALCore(
        tli,
        summary_start_lsn,
        summary_end_lsn,
        switch_lsn,
        brtab,
        fast_forward,
        xlogreader,
        private_data,
        &raw mut io,
        &raw mut temp_path,
        &raw mut final_path,
    )
}

/*
 * Inner loop for SummarizeWAL -- extracted to share the early-exit paths.
 */
unsafe fn SummarizeWALCore(
    tli: TimeLineID,
    summary_start_lsn: XLogRecPtr,
    mut summary_end_lsn: XLogRecPtr,
    switch_lsn: XLogRecPtr,
    brtab: *mut BlockRefTable,
    mut fast_forward: bool,
    xlogreader: *mut XLogReaderState,
    private_data: *mut SummarizerReadLocalXLogPrivate,
    io: *mut WalSummaryIO,
    temp_path: *mut [c_char; MAXPGPATH],
    final_path: *mut [c_char; MAXPGPATH],
) -> XLogRecPtr {
    use core::ffi::CStr;

    /*
     * Main loop: read xlog records one by one.
     */
    loop {
        let mut errormsg: *mut c_char = null_mut();
        let record: *mut XLogRecord;
        let rmid: RmgrId;

        ProcessWalSummarizerInterrupts();

        /* We shouldn't go backward. */
        Assert!(summary_start_lsn <= (*xlogreader).EndRecPtr);

        /* Now read the next record. */
        record = XLogReadRecord(xlogreader, &raw mut errormsg) as *mut c_void;
        if record.is_null() {
            if (*private_data).end_of_wal {
                /*
                 * This timeline must be historic and must end before we were
                 * able to read a complete record.
                 */
                ereport!(
                    DEBUG1,
                    errmsg!(
                        "could not read WAL from timeline {} at {:X}/{:X}: end of WAL at {:X}/{:X}",
                        tli,
                        LSN_FORMAT_ARGS((*xlogreader).EndRecPtr).0,
                        LSN_FORMAT_ARGS((*xlogreader).EndRecPtr).1,
                        LSN_FORMAT_ARGS((*private_data).read_upto).0,
                        LSN_FORMAT_ARGS((*private_data).read_upto).1
                    )
                );
                /* Summary ends at end of WAL. */
                summary_end_lsn = (*private_data).read_upto;
                break;
            }
            if !errormsg.is_null() {
                let msg = CStr::from_ptr(errormsg).to_string_lossy();
                ereport!(
                    ERROR,
                    /* errcode_for_file_access() -- TODO(pg-port) */
                    errmsg!(
                        "could not read WAL from timeline {} at {:X}/{:X}: {}",
                        tli,
                        LSN_FORMAT_ARGS((*xlogreader).EndRecPtr).0,
                        LSN_FORMAT_ARGS((*xlogreader).EndRecPtr).1,
                        msg
                    )
                );
            } else {
                ereport!(
                    ERROR,
                    /* errcode_for_file_access() -- TODO(pg-port) */
                    errmsg!(
                        "could not read WAL from timeline {} at {:X}/{:X}",
                        tli,
                        LSN_FORMAT_ARGS((*xlogreader).EndRecPtr).0,
                        LSN_FORMAT_ARGS((*xlogreader).EndRecPtr).1
                    )
                );
            }
        }

        /* We shouldn't go backward. */
        Assert!(summary_start_lsn <= (*xlogreader).EndRecPtr);

        if !XLogRecPtrIsInvalid(switch_lsn)
            && (*xlogreader).ReadRecPtr >= switch_lsn
        {
            /*
             * Whoops! We've read a record that *starts* after the switch LSN,
             * contrary to our goal of reading only until we hit the first
             * record that ends at or after the switch LSN. Pretend we didn't
             * read it after all by bailing out of this loop right here,
             * before we do anything with this record.
             *
             * This can happen because the last record before the switch LSN
             * might be continued across multiple pages, and then we might
             * come to a page with XLP_FIRST_IS_OVERWRITE_CONTRECORD set. In
             * that case, the record that was continued across multiple pages
             * is incomplete and will be disregarded, and the read will
             * restart from the beginning of the page that is flagged
             * XLP_FIRST_IS_OVERWRITE_CONTRECORD.
             *
             * If this case occurs, we can fairly say that the current summary
             * file ends at the switch LSN exactly. The first record on the
             * page marked XLP_FIRST_IS_OVERWRITE_CONTRECORD will be
             * discovered when generating the next summary file.
             */
            summary_end_lsn = switch_lsn;
            break;
        }

        /*
         * Certain types of records require special handling. Redo points and
         * shutdown checkpoints trigger creation of new summary files and can
         * also cause us to enter or exit "fast forward" mode. Other types of
         * records can require special updates to the block reference table.
         */
        rmid = XLogRecGetRmid(xlogreader);
        if rmid == RM_XLOG_ID {
            let mut new_fast_forward: bool = false;

            /*
             * If we've already processed some WAL records when we hit a redo
             * point or shutdown checkpoint, then we stop summarization before
             * including this record in the current file, so that it will be
             * the first record in the next file.
             *
             * When we hit one of those record types as the first record in a
             * file, we adjust our notion of whether we're fast-forwarding.
             * Any WAL generated with wal_level=minimal must be skipped
             * without actually generating any summary file, because an
             * incremental backup that crosses such WAL would be unsafe.
             */
            if SummarizeXlogRecord(xlogreader, &raw mut new_fast_forward) {
                if (*xlogreader).ReadRecPtr > summary_start_lsn {
                    summary_end_lsn = (*xlogreader).ReadRecPtr;
                    break;
                } else {
                    fast_forward = new_fast_forward;
                }
            }
        } else if !fast_forward {
            /*
             * This switch handles record types that require extra updates to
             * the contents of the block reference table.
             */
            match rmid {
                RM_DBASE_ID => {
                    SummarizeDbaseRecord(xlogreader, brtab);
                }
                RM_SMGR_ID => {
                    SummarizeSmgrRecord(xlogreader, brtab);
                }
                RM_XACT_ID => {
                    SummarizeXactRecord(xlogreader, brtab);
                }
                _ => {}
            }
        }

        /*
         * If we're in fast-forward mode, we don't really need to do anything.
         * Otherwise, feed block references from xlog record to block
         * reference table.
         */
        if !fast_forward {
            let mut block_id: c_int = 0;
            while block_id <= XLogRecMaxBlockId(xlogreader) {
                let mut rlocator = RelFileLocator {
                    spcOid: 0,
                    dbOid: 0,
                    relNumber: 0,
                };
                let mut forknum: ForkNumber = 0;
                let mut blocknum: BlockNumber = 0;

                if !XLogRecGetBlockTagExtended(
                    xlogreader,
                    block_id as u8,
                    &raw mut rlocator as *mut crate::access::transam::xlogreader::RelFileLocator,
                    &raw mut forknum,
                    &raw mut blocknum,
                    null_mut(),
                ) {
                    block_id += 1;
                    continue;
                }

                /*
                 * As we do elsewhere, ignore the FSM fork, because it's not
                 * fully WAL-logged.
                 */
                if forknum != FSM_FORKNUM {
                    BlockRefTableMarkBlockModified(brtab, &raw const rlocator, forknum, blocknum);
                }
                block_id += 1;
            }
        }

        /* Update our notion of where this summary file ends. */
        summary_end_lsn = (*xlogreader).EndRecPtr;

        /* Also update shared memory. */
        LWLockAcquire(WALSummarizerLock(), LW_EXCLUSIVE);
        Assert!(summary_end_lsn >= (*WalSummarizerCtl).summarized_lsn);
        (*WalSummarizerCtl).pending_lsn = summary_end_lsn;
        LWLockRelease(WALSummarizerLock());

        /*
         * If we have a switch LSN and have reached it, stop before reading
         * the next record.
         */
        if !XLogRecPtrIsInvalid(switch_lsn) && (*xlogreader).EndRecPtr >= switch_lsn {
            break;
        }
    }

    /* Destroy xlogreader. */
    pfree((*xlogreader).private_data);
    XLogReaderFree(xlogreader);

    /*
     * If a timeline switch occurs, we may fail to make any progress at all
     * before exiting the loop above. If that happens, we don't write a WAL
     * summary file at all. We can also skip writing a file if we're in
     * fast-forward mode.
     */
    if summary_end_lsn > summary_start_lsn && !fast_forward {
        use core::fmt::Write;

        /* Generate temporary and final path name. */
        let temp_str = format!("{}/summaries/temp.summary\0", XLOGDIR);
        let final_str = format!(
            "{}/summaries/{:08X}{:08X}{:08X}{:08X}{:08X}.summary\0",
            XLOGDIR,
            tli,
            LSN_FORMAT_ARGS(summary_start_lsn).0,
            LSN_FORMAT_ARGS(summary_start_lsn).1,
            LSN_FORMAT_ARGS(summary_end_lsn).0,
            LSN_FORMAT_ARGS(summary_end_lsn).1
        );

        /* Open the temporary file for writing. */
        (*io).filepos = 0;
        (*io).file = PathNameOpenFile(temp_str.as_ptr() as *const c_char, O_WRONLY | O_CREAT | O_TRUNC);
        if (*io).file < 0 {
            ereport!(
                ERROR,
                /* errcode_for_file_access() -- TODO(pg-port) */
                errmsg!("could not create file \"{}\"", temp_str.trim_end_matches('\0'))
            );
        }

        /* Write the data. */
        WriteBlockRefTable(brtab, WriteWalSummary_cb, io as *mut c_void);

        /* Close temporary file and shut down xlogreader. */
        FileClose((*io).file);

        /* Tell the user what we did. */
        ereport!(
            DEBUG1,
            errmsg!(
                "summarized WAL on TLI {} from {:X}/{:X} to {:X}/{:X}",
                tli,
                LSN_FORMAT_ARGS(summary_start_lsn).0,
                LSN_FORMAT_ARGS(summary_start_lsn).1,
                LSN_FORMAT_ARGS(summary_end_lsn).0,
                LSN_FORMAT_ARGS(summary_end_lsn).1
            )
        );

        /* Durably rename the new summary into place. */
        durable_rename(
            temp_str.as_ptr() as *const c_char,
            final_str.as_ptr() as *const c_char,
            ERROR as c_int,
        );
    }

    /* If we skipped a non-zero amount of WAL, log a debug message. */
    if summary_end_lsn > summary_start_lsn && fast_forward {
        ereport!(
            DEBUG1,
            errmsg!(
                "skipped summarizing WAL on TLI {} from {:X}/{:X} to {:X}/{:X}",
                tli,
                LSN_FORMAT_ARGS(summary_start_lsn).0,
                LSN_FORMAT_ARGS(summary_start_lsn).1,
                LSN_FORMAT_ARGS(summary_end_lsn).0,
                LSN_FORMAT_ARGS(summary_end_lsn).1
            )
        );
    }

    summary_end_lsn
}

/*
 * Special handling for WAL records with RM_DBASE_ID.
 */
unsafe fn SummarizeDbaseRecord(xlogreader: *mut XLogReaderState, brtab: *mut BlockRefTable) {
    let info: uint8 = XLogRecGetInfo(xlogreader) & !XLR_INFO_MASK;

    /*
     * We use relfilenode zero for a given database OID and tablespace OID to
     * indicate that all relations with that pair of IDs have been recreated
     * if they exist at all. Effectively, we're setting a limit block of 0 for
     * all such relfilenodes.
     *
     * Technically, this special handling is only needed in the case of
     * XLOG_DBASE_CREATE_FILE_COPY, because that can create a whole bunch of
     * relation files in a directory without logging anything specific to each
     * one. If we didn't mark the whole DB OID/TS OID combination in some way,
     * then a tablespace that was dropped after the reference backup and
     * recreated using the FILE_COPY method prior to the incremental backup
     * would look just like one that was never touched at all, which would be
     * catastrophic.
     *
     * But it seems best to adopt this treatment for all records that drop or
     * create a DB OID/TS OID combination. That's similar to how we treat the
     * limit block for individual relations, and it's an extra layer of safety
     * here. We can never lose data by marking more stuff as needing to be
     * backed up in full.
     */
    if info == XLOG_DBASE_CREATE_FILE_COPY {
        let xlrec: *mut xl_dbase_create_file_copy_rec =
            XLogRecGetData(xlogreader) as *mut xl_dbase_create_file_copy_rec;
        let rlocator = RelFileLocator {
            spcOid: (*xlrec).tablespace_id,
            dbOid: (*xlrec).db_id,
            relNumber: 0,
        };
        BlockRefTableSetLimitBlock(brtab, &raw const rlocator, MAIN_FORKNUM, 0);
    } else if info == XLOG_DBASE_CREATE_WAL_LOG {
        let xlrec: *mut xl_dbase_create_wal_log_rec =
            XLogRecGetData(xlogreader) as *mut xl_dbase_create_wal_log_rec;
        let rlocator = RelFileLocator {
            spcOid: (*xlrec).tablespace_id,
            dbOid: (*xlrec).db_id,
            relNumber: 0,
        };
        BlockRefTableSetLimitBlock(brtab, &raw const rlocator, MAIN_FORKNUM, 0);
    } else if info == XLOG_DBASE_DROP {
        let xlrec: *mut xl_dbase_drop_rec =
            XLogRecGetData(xlogreader) as *mut xl_dbase_drop_rec;
        let db_id = (*xlrec).db_id;
        let ntablespaces = (*xlrec).ntablespaces;
        /* tablespace_ids[] is a flexible C array immediately after the struct */
        let tablespace_ids: *const Oid =
            (xlrec as *const u8).add(core::mem::size_of::<xl_dbase_drop_rec>()) as *const Oid;
        let mut i: c_int = 0;
        while i < ntablespaces {
            let rlocator = RelFileLocator {
                spcOid: *tablespace_ids.add(i as usize),
                dbOid: db_id,
                relNumber: 0,
            };
            BlockRefTableSetLimitBlock(brtab, &raw const rlocator, MAIN_FORKNUM, 0);
            i += 1;
        }
    }
}

/*
 * Special handling for WAL records with RM_SMGR_ID.
 */
unsafe fn SummarizeSmgrRecord(xlogreader: *mut XLogReaderState, brtab: *mut BlockRefTable) {
    let info: uint8 = XLogRecGetInfo(xlogreader) & !XLR_INFO_MASK;

    if info == XLOG_SMGR_CREATE {
        let xlrec: *mut xl_smgr_create =
            XLogRecGetData(xlogreader) as *mut xl_smgr_create;

        /*
         * If a new relation fork is created on disk, there is no point
         * tracking anything about which blocks have been modified, because
         * the whole thing will be new. Hence, set the limit block for this
         * fork to 0.
         *
         * Ignore the FSM fork, which is not fully WAL-logged.
         */
        if (*xlrec).forkNum != FSM_FORKNUM {
            BlockRefTableSetLimitBlock(brtab, &raw const (*xlrec).rlocator, (*xlrec).forkNum, 0);
        }
    } else if info == XLOG_SMGR_TRUNCATE {
        let xlrec: *mut xl_smgr_truncate =
            XLogRecGetData(xlogreader) as *mut xl_smgr_truncate;

        /*
         * If a relation fork is truncated on disk, there is no point in
         * tracking anything about block modifications beyond the truncation
         * point.
         *
         * We ignore SMGR_TRUNCATE_FSM here because the FSM isn't fully
         * WAL-logged and thus we can't track modified blocks for it anyway.
         */
        if ((*xlrec).flags & SMGR_TRUNCATE_HEAP) != 0 {
            BlockRefTableSetLimitBlock(
                brtab,
                &raw const (*xlrec).rlocator,
                MAIN_FORKNUM,
                (*xlrec).blkno,
            );
        }
        if ((*xlrec).flags & SMGR_TRUNCATE_VM) != 0 {
            BlockRefTableSetLimitBlock(
                brtab,
                &raw const (*xlrec).rlocator,
                VISIBILITYMAP_FORKNUM,
                (*xlrec).blkno,
            );
        }
    }
}

/*
 * Special handling for WAL records with RM_XACT_ID.
 */
unsafe fn SummarizeXactRecord(xlogreader: *mut XLogReaderState, brtab: *mut BlockRefTable) {
    let info: uint8 = XLogRecGetInfo(xlogreader) & !XLR_INFO_MASK;
    let xact_info: uint8 = info & XLOG_XACT_OPMASK;

    if xact_info == XLOG_XACT_COMMIT || xact_info == XLOG_XACT_COMMIT_PREPARED {
        let xlrec: *mut xl_xact_commit =
            XLogRecGetData(xlogreader) as *mut xl_xact_commit;
        let mut parsed: xl_xact_parsed_commit = core::mem::zeroed();
        let mut i: c_int;

        /*
         * Don't track modified blocks for any relations that were removed on
         * commit.
         */
        ParseCommitRecord(XLogRecGetInfo(xlogreader), xlrec, &raw mut parsed);
        i = 0;
        while i < parsed.nrels {
            let mut forknum: ForkNumber = 0;
            while forknum <= MAX_FORKNUM {
                if forknum != FSM_FORKNUM {
                    BlockRefTableSetLimitBlock(
                        brtab,
                        &raw const *parsed.xlocators.add(i as usize),
                        forknum,
                        0,
                    );
                }
                forknum += 1;
            }
            i += 1;
        }
    } else if xact_info == XLOG_XACT_ABORT || xact_info == XLOG_XACT_ABORT_PREPARED {
        let xlrec: *mut xl_xact_abort =
            XLogRecGetData(xlogreader) as *mut xl_xact_abort;
        let mut parsed: xl_xact_parsed_abort = core::mem::zeroed();
        let mut i: c_int;

        /*
         * Don't track modified blocks for any relations that were removed on
         * abort.
         */
        ParseAbortRecord(XLogRecGetInfo(xlogreader), xlrec, &raw mut parsed);
        i = 0;
        while i < parsed.nrels {
            let mut forknum: ForkNumber = 0;
            while forknum <= MAX_FORKNUM {
                if forknum != FSM_FORKNUM {
                    BlockRefTableSetLimitBlock(
                        brtab,
                        &raw const *parsed.xlocators.add(i as usize),
                        forknum,
                        0,
                    );
                }
                forknum += 1;
            }
            i += 1;
        }
    }
}

/*
 * Special handling for WAL records with RM_XLOG_ID.
 *
 * The return value is true if WAL summarization should stop before this
 * record and false otherwise. When the return value is true,
 * *new_fast_forward indicates whether future processing should be done
 * in fast forward mode (i.e. read WAL without emitting summaries) or not.
 */
unsafe fn SummarizeXlogRecord(
    xlogreader: *mut XLogReaderState,
    new_fast_forward: *mut bool,
) -> bool {
    let info: uint8 = XLogRecGetInfo(xlogreader) & !XLR_INFO_MASK;
    let record_wal_level: c_int;

    if info == XLOG_CHECKPOINT_REDO {
        /* Payload is wal_level at the time record was written. */
        let mut wl: c_int = 0;
        core::ptr::copy_nonoverlapping(
            XLogRecGetData(xlogreader) as *const u8,
            &raw mut wl as *mut u8,
            core::mem::size_of::<c_int>(),
        );
        record_wal_level = wl;
    } else if info == XLOG_CHECKPOINT_SHUTDOWN {
        let mut rec_ckpt: CheckPoint = core::mem::zeroed();
        /* Extract wal_level at time record was written from payload. */
        core::ptr::copy_nonoverlapping(
            XLogRecGetData(xlogreader) as *const u8,
            &raw mut rec_ckpt as *mut u8,
            core::mem::size_of::<CheckPoint>(),
        );
        record_wal_level = rec_ckpt.wal_level;
    } else if info == XLOG_PARAMETER_CHANGE {
        let mut xlrec: xl_parameter_change = core::mem::zeroed();
        /* Extract wal_level at time record was written from payload. */
        core::ptr::copy_nonoverlapping(
            XLogRecGetData(xlogreader) as *const u8,
            &raw mut xlrec as *mut u8,
            core::mem::size_of::<xl_parameter_change>(),
        );
        record_wal_level = xlrec.wal_level;
    } else if info == XLOG_END_OF_RECOVERY {
        let mut xlrec: xl_end_of_recovery = core::mem::zeroed();
        /* Extract wal_level at time record was written from payload. */
        core::ptr::copy_nonoverlapping(
            XLogRecGetData(xlogreader) as *const u8,
            &raw mut xlrec as *mut u8,
            core::mem::size_of::<xl_end_of_recovery>(),
        );
        record_wal_level = xlrec.wal_level;
    } else {
        /* No special handling required. Return false. */
        return false;
    }

    /*
     * Redo can only begin at an XLOG_CHECKPOINT_REDO or
     * XLOG_CHECKPOINT_SHUTDOWN record, so we want WAL summarization to begin
     * at those points. Hence, when those records are encountered, return
     * true, so that we stop just before summarizing either of those records.
     *
     * We also reach here if we just saw XLOG_END_OF_RECOVERY or
     * XLOG_PARAMETER_CHANGE. These are not places where recovery can start,
     * but they're still relevant here. A new timeline can begin with
     * XLOG_END_OF_RECOVERY, so we need to confirm the WAL level at that
     * point; and a restart can provoke XLOG_PARAMETER_CHANGE after an
     * intervening change to postgresql.conf, which might force us to stop
     * summarizing.
     */
    *new_fast_forward = record_wal_level == WAL_LEVEL_MINIMAL;
    true
}

/*
 * Similar to read_local_xlog_page, but limited to read from one particular
 * timeline. If the end of WAL is reached, it will wait for more if reading
 * from the current timeline, or give up if reading from a historic timeline.
 * In the latter case, it will also set private_data->end_of_wal = true.
 *
 * Caller must set private_data->tli to the TLI of interest,
 * private_data->read_upto to the lowest LSN that is not known to be safe
 * to read on that timeline, and private_data->historic to true if and only
 * if the timeline is not the current timeline. This function will update
 * private_data->read_upto and private_data->historic if more WAL appears
 * on the current timeline or if the current timeline becomes historic.
 */
unsafe extern "C" fn summarizer_read_local_xlog_page(
    state: *mut XLogReaderState,
    targetPagePtr: XLogRecPtr,
    reqLen: c_int,
    _targetRecPtr: XLogRecPtr,
    cur_page: *mut c_char,
) -> c_int {
    let count: c_int;
    let mut errinfo: WALReadError = core::mem::zeroed();
    let private_data: *mut SummarizerReadLocalXLogPrivate;

    ProcessWalSummarizerInterrupts();

    private_data = (*state).private_data as *mut SummarizerReadLocalXLogPrivate;

    loop {
        if targetPagePtr + XLOG_BLCKSZ <= (*private_data).read_upto {
            /*
             * more than one block available; read only that block, have
             * caller come back if they need more.
             */
            count = XLOG_BLCKSZ as c_int;
            break;
        } else if targetPagePtr + reqLen as XLogRecPtr > (*private_data).read_upto {
            /* We don't seem to have enough data. */
            if (*private_data).historic {
                /*
                 * This is a historic timeline, so there will never be any
                 * more data than we have currently.
                 */
                (*private_data).end_of_wal = true;
                return -1;
            } else {
                let latest_lsn: XLogRecPtr;
                let mut latest_tli: TimeLineID = 0;

                /*
                 * This is - or at least was up until very recently - the
                 * current timeline, so more data might show up.  Delay here
                 * so we don't tight-loop.
                 */
                ProcessWalSummarizerInterrupts();
                summarizer_wait_for_wal();

                /* Recheck end-of-WAL. */
                latest_lsn = GetLatestLSN(&raw mut latest_tli);
                if (*private_data).tli == latest_tli {
                    /* Still the current timeline, update max LSN. */
                    Assert!(latest_lsn >= (*private_data).read_upto);
                    (*private_data).read_upto = latest_lsn;
                } else {
                    let tles = readTimeLineHistory(latest_tli);
                    let switchpoint: XLogRecPtr;

                    /*
                     * The timeline we're scanning is no longer the latest
                     * one. Figure out when it ended.
                     */
                    (*private_data).historic = true;
                    switchpoint = tliSwitchPoint((*private_data).tli, tles, null_mut());

                    /*
                     * Allow reads up to exactly the switch point.
                     *
                     * It's possible that this will cause read_upto to move
                     * backwards, because we might have been promoted before
                     * reaching the end of the previous timeline. In that
                     * case, the next loop iteration will likely conclude that
                     * we've reached end of WAL.
                     */
                    (*private_data).read_upto = switchpoint;

                    /* Debugging output. */
                    ereport!(
                        DEBUG1,
                        errmsg!(
                            "timeline {} became historic, can read up to {:X}/{:X}",
                            (*private_data).tli,
                            LSN_FORMAT_ARGS((*private_data).read_upto).0,
                            LSN_FORMAT_ARGS((*private_data).read_upto).1
                        )
                    );
                }

                /* Go around and try again. */
            }
        } else {
            /* enough bytes available to satisfy the request */
            count = ((*private_data).read_upto - targetPagePtr) as c_int;
            break;
        }
    }

    if !WALRead(state, cur_page, targetPagePtr, count as Size, (*private_data).tli, &raw mut errinfo) {
        /* TODO(pg-port): WALReadRaiseError from access/transam/xlogutils.rs */
        crate::access::transam::xlogutils::WALReadRaiseError(&raw mut errinfo as *mut _);
    }

    /* Track that we read a page, for sleep time calculation. */
    pages_read_since_last_sleep += 1;

    /* number of valid bytes in the buffer */
    count
}

/*
 * Sleep for long enough that we believe it's likely that more WAL will
 * be available afterwards.
 */
unsafe fn summarizer_wait_for_wal() {
    if pages_read_since_last_sleep == 0 {
        /*
         * No pages were read since the last sleep, so double the sleep time,
         * but not beyond the maximum allowable value.
         */
        sleep_quanta = Min(sleep_quanta * 2, MAX_SLEEP_QUANTA);
    } else if pages_read_since_last_sleep > 1 {
        /*
         * Multiple pages were read since the last sleep, so reduce the sleep
         * time.
         *
         * A large burst of activity should be able to quickly reduce the
         * sleep time to the minimum, but we don't want a handful of extra WAL
         * records to provoke a strong reaction. We choose to reduce the sleep
         * time by 1 quantum for each page read beyond the first, which is a
         * fairly arbitrary way of trying to be reactive without overreacting.
         */
        if pages_read_since_last_sleep > sleep_quanta - 1 {
            sleep_quanta = 1;
        } else {
            sleep_quanta -= pages_read_since_last_sleep;
        }
    }

    /* Report pending statistics to the cumulative stats system. */
    pgstat_report_wal(false);

    /* OK, now sleep. */
    let _ = WaitLatch(
        get_my_latch(),
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        sleep_quanta * MS_PER_SLEEP_QUANTUM,
        WAIT_EVENT_WAL_SUMMARIZER_WAL,
    );
    ResetLatch(get_my_latch());

    /* Reset count of pages read. */
    pages_read_since_last_sleep = 0;
}

/*
 * Remove WAL summaries whose mtimes are older than wal_summary_keep_time.
 */
unsafe fn MaybeRemoveOldWalSummaries() {
    let redo_pointer: XLogRecPtr = GetRedoRecPtr();
    let mut wslist: *mut List;
    let cutoff_time: time_t;

    /* If WAL summary removal is disabled, don't do anything. */
    if wal_summary_keep_time == 0 {
        return;
    }

    /*
     * If the redo pointer has not advanced, don't do anything.
     *
     * This has the effect that we only try to remove old WAL summary files
     * once per checkpoint cycle.
     */
    if redo_pointer == redo_pointer_at_last_summary_removal {
        return;
    }
    redo_pointer_at_last_summary_removal = redo_pointer;

    /*
     * Files should only be removed if the last modification time precedes the
     * cutoff time we compute here.
     */
    cutoff_time = time(null_mut()) - wal_summary_keep_time as time_t * SECS_PER_MINUTE;

    /* Get all the summaries that currently exist. */
    wslist = GetWalSummaries(0, InvalidXLogRecPtr, InvalidXLogRecPtr);

    /* Loop until all summaries have been considered for removal. */
    while !wslist.is_null() /* NIL == null_mut() */ {
        let mut lc: *mut ListCell;
        let oldest_segno: XLogSegNo;
        let mut oldest_lsn: XLogRecPtr = InvalidXLogRecPtr;
        let selected_tli: TimeLineID;

        ProcessWalSummarizerInterrupts();

        /*
         * Pick a timeline for which some summary files still exist on disk,
         * and find the oldest LSN that still exists on disk for that
         * timeline.
         */
        selected_tli = (*(linitial(wslist) as *mut WalSummaryFile)).tli;
        oldest_segno = XLogGetOldestSegno(selected_tli);
        if oldest_segno != 0 {
            XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size, &mut oldest_lsn);
        }

        /* Consider each WAL file on the selected timeline in turn.
         * We iterate by index in reverse so that removal doesn't disrupt
         * subsequent indices (list_delete_nth_cell compacts the array).
         * We keep a separate pass and rebuild the iteration state each time.
         */
        {
            let mut i: c_int = 0;
            while i < list_length(wslist) {
                let ws: *mut WalSummaryFile = list_nth(wslist, i) as *mut WalSummaryFile;

                ProcessWalSummarizerInterrupts();

                /* If it's not on this timeline, it's not time to consider it. */
                if selected_tli != (*ws).tli {
                    i += 1;
                    continue;
                }

                /*
                 * If the WAL doesn't exist any more, we can remove it if the file
                 * modification time is old enough.
                 */
                if XLogRecPtrIsInvalid(oldest_lsn) || (*ws).end_lsn <= oldest_lsn {
                    RemoveWalSummaryIfOlderThan(ws, cutoff_time);
                }

                /*
                 * Whether we removed the file or not, we need not consider it
                 * again. Use list_delete_nth_cell to remove the current element;
                 * it compacts the array so the next element is now at index i.
                 */
                /* TODO(pg-port): list_delete_nth_cell not yet ported; use stub list removal. */
                wslist = list_delete_nth_cell_stub(wslist, i);
                pfree(ws as *mut c_void);
                /* do NOT increment i: the next element slid into position i */
            }
        }
    }
}
