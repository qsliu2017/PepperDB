/*-------------------------------------------------------------------------
 *
 * checkpointer.rs
 *
 * The checkpointer is new as of Postgres 9.2.  It handles all checkpoints.
 * Checkpoints are automatically dispatched after a certain amount of time has
 * elapsed since the last one, and it can be signaled to perform requested
 * checkpoints as well.  (The GUC parameter that mandates a checkpoint every
 * so many WAL segments is implemented by having backends signal when they
 * fill WAL segments; the checkpointer itself doesn't watch for the
 * condition.)
 *
 * The normal termination sequence is that checkpointer is instructed to
 * execute the shutdown checkpoint by SIGINT.  After that checkpointer waits
 * to be terminated via SIGUSR2, which instructs the checkpointer to exit(0).
 * All backends must be stopped before SIGINT or SIGUSR2 is issued!
 *
 * Emergency termination is by SIGQUIT; like any backend, the checkpointer
 * will simply abort and exit on SIGQUIT.
 *
 * If the checkpointer exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.  (Even if
 * shared memory isn't corrupted, we have lost information about which
 * files need to be fsync'd for the next checkpoint, and so a system
 * restart needs to be forced.)
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *   src/backend/postmaster/checkpointer.c -> src/postmaster/checkpointer.rs
 *
 *-------------------------------------------------------------------------
 */

//! Merged declarations from postmaster/bgwriter.h (checkpointer parts):
//!
//! GUC options: CheckPointTimeout, CheckPointWarning, CheckPointCompletionTarget.
//!
//! Public API:
//!   CheckpointerMain, RequestCheckpoint, CheckpointWriteDelay,
//!   ForwardSyncRequest, AbsorbSyncRequests,
//!   CheckpointerShmemSize, CheckpointerShmemInit,
//!   FirstCallSinceLastCheckpoint.

use crate::prelude::*;
use crate::utils::init::globals::MyLatch;

use crate::libpq::pqsignal::{
    pqsignal, sigset_t, SigHandler, UnBlockSig, SIGALRM, SIGCHLD, SIGHUP, SIGINT, SIGPIPE, SIGTERM,
    SIGUSR1, SIGUSR2, SIG_DFL,
};
use crate::miscadmin::{
    AmCheckpointerProcess, B_CHECKPOINTER, CHECK_FOR_INTERRUPTS, CritSectionCount,
    END_CRIT_SECTION, ExitOnAnyError, HOLD_INTERRUPTS, IsPostmasterEnvironment, IsUnderPostmaster,
    Latch, MyBackendType, MyProcPid, NBuffers, RESUME_INTERRUPTS, START_CRIT_SECTION,
    pg_time_t,
};
use crate::postmaster::auxprocess::AuxiliaryProcessMainCommon;
use crate::postmaster::interrupt::{
    ConfigReloadPending, ShutdownRequestPending, SignalHandlerForConfigReload,
    SignalHandlerForShutdownRequest,
};
use crate::storage::ipc::ipc::{before_shmem_exit, proc_exit};
use crate::storage::ipc::latch::{ResetLatch, SetLatch, WaitLatch, WL_EXIT_ON_PM_DEATH,
    WL_LATCH_SET, WL_TIMEOUT};
use crate::storage::lmgr::condition_variable::{
    ConditionVariable, ConditionVariableBroadcast, ConditionVariableCancelSleep,
    ConditionVariableInit, ConditionVariablePrepareToSleep, ConditionVariableSleep,
};
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::storage::sync::sync::{FileTag, RememberSyncRequest, SyncRequestType};
use crate::utils::activity::pgstat_checkpointer::{
    pgstat_report_checkpointer, PendingCheckpointerStats,
};
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::pgtime::pg_time_t as PgTimet; // same underlying type

// ---------------------------------------------------------------------------
// Stubs for not-yet-translated dependencies.
// ---------------------------------------------------------------------------

// SIG_IGN: function pointer value 1 (platform ABI).
#[inline]
fn SIG_IGN() -> SigHandler {
    Some(unsafe { core::mem::transmute::<usize, unsafe extern "C" fn(c_int)>(1usize) })
}

// SIG_SETMASK (signal.h). TODO: centralize to port layer.
const SIG_SETMASK: c_int = if cfg!(target_os = "macos") { 3 } else { 2 };

// sigprocmask(2). TODO: route through port-layer wrapper.
extern "C" {
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
    fn time(t: *mut pg_time_t) -> pg_time_t;
    fn gettimeofday(tv: *mut timeval, tz: *mut c_void) -> c_int;
}

// struct timeval (sys/time.h). Used locally in IsCheckpointOnSchedule.
#[repr(C)]
struct timeval {
    tv_sec: pg_time_t,
    tv_usec: c_long,
}

// error_context_stack / PG_exception_stack (elog.c). TODO: import once elog.c is ported.
static mut error_context_stack: *mut c_void = null_mut();
static mut PG_exception_stack: *mut c_void = null_mut();

// sigjmp_buf stub. TODO: wire to real sigsetjmp once elog.c is ported.
type sigjmp_buf = [c_void; 0];

unsafe fn sigsetjmp(_env: *mut sigjmp_buf, _savemask: c_int) -> c_int {
    /* TODO: not ported */
    0
}

// procsignal_sigusr1_handler (storage/procsignal.c). TODO.
unsafe extern "C" fn procsignal_sigusr1_handler(_postgres_signal_arg: c_int) {
    /* TODO(pg-port): real procsignal_sigusr1_handler lives in storage/ipc/procsignal.c */
}

// Interrupt-handling stubs -------------------------------------------------

// ProcSignalBarrierPending is re-declared here because interrupt.rs has its own
// module-private copy; checkpointer needs the one from miscadmin/globals.c.
// TODO: unify once storage/ipc/procsignal.c is ported.
extern "C" {
    static mut ProcSignalBarrierPending: crate::miscadmin::sig_atomic_t;
    static mut LogMemoryContextPending: crate::miscadmin::sig_atomic_t;
}

// TODO(pg-port): real ProcessProcSignalBarrier lives in storage/ipc/procsignal.c
unsafe fn ProcessProcSignalBarrier() { /* TODO */ }

// TODO(pg-port): real ProcessLogMemoryContextInterrupt lives in utils/mmgr/mcxt.c
unsafe fn ProcessLogMemoryContextInterrupt() { /* TODO */ }

// GUC / config stubs -------------------------------------------------------

const PGC_SIGHUP: c_int = 0;

// TODO(pg-port): real ProcessConfigFile lives in utils/misc/guc.c
unsafe fn ProcessConfigFile(_context: c_int) { /* TODO */ }

// xlog stubs ----------------------------------------------------------------

// TODO(pg-port): real CreateCheckPoint lives in access/transam/xlog.c
unsafe fn CreateCheckPoint(_flags: c_int) -> bool {
    /* TODO(pg-port): real CreateCheckPoint lives in access/transam/xlog.c */
    false
}

// TODO(pg-port): real CreateRestartPoint lives in access/transam/xlog.c
unsafe fn CreateRestartPoint(_flags: c_int) -> bool {
    /* TODO(pg-port): real CreateRestartPoint lives in access/transam/xlog.c */
    false
}

// TODO(pg-port): real ShutdownXLOG lives in access/transam/xlog.c
unsafe extern "C" fn ShutdownXLOG(_code: c_int, _arg: Datum) {
    /* TODO(pg-port): real ShutdownXLOG lives in access/transam/xlog.c */
}

// TODO(pg-port): real RecoveryInProgress lives in access/transam/xlog.c
unsafe fn RecoveryInProgress() -> bool {
    /* TODO(pg-port): real RecoveryInProgress lives in access/transam/xlog.c */
    false
}

// TODO(pg-port): real GetInsertRecPtr lives in access/transam/xlog.c
unsafe fn GetInsertRecPtr() -> XLogRecPtr {
    /* TODO(pg-port): real GetInsertRecPtr lives in access/transam/xlog.c */
    0
}

// TODO(pg-port): real GetXLogReplayRecPtr lives in access/transam/xlogrecovery.c
unsafe fn GetXLogReplayRecPtr(_replayTLI: *mut c_int) -> XLogRecPtr {
    /* TODO(pg-port): real GetXLogReplayRecPtr lives in access/transam/xlogrecovery.c */
    0
}

// TODO(pg-port): real GetLastSegSwitchData lives in access/transam/xlog.c
unsafe fn GetLastSegSwitchData(_lsn: *mut XLogRecPtr) -> pg_time_t {
    /* TODO(pg-port): real GetLastSegSwitchData lives in access/transam/xlog.c */
    0
}

// TODO(pg-port): real GetLastImportantRecPtr lives in access/transam/xlog.c
unsafe fn GetLastImportantRecPtr() -> XLogRecPtr {
    /* TODO(pg-port): real GetLastImportantRecPtr lives in access/transam/xlog.c */
    0
}

// TODO(pg-port): real RequestXLogSwitch lives in access/transam/xlog.c
unsafe fn RequestXLogSwitch(_mark_unimportant: bool) -> XLogRecPtr {
    /* TODO(pg-port): real RequestXLogSwitch lives in access/transam/xlog.c */
    0
}

// TODO(pg-port): real XLogSegmentOffset macro lives in access/transam/xlogdefs.h
#[inline]
unsafe fn XLogSegmentOffset(ptr: XLogRecPtr, segsz: c_int) -> u64 {
    ptr % segsz as u64
}

// TODO(pg-port): real XLogArchiveTimeout GUC lives in access/transam/xlog.c
static mut XLogArchiveTimeout: c_int = 0;

// TODO(pg-port): real wal_segment_size GUC lives in access/transam/xlogutils.c (already ported, import when ready)
static mut wal_segment_size: c_int = 16 * 1024 * 1024;

// TODO(pg-port): real CheckPointSegments GUC lives in access/transam/xlog.c
static mut CheckPointSegments: c_int = 3;

// smgr / sync stubs --------------------------------------------------------

// TODO(pg-port): real smgrdestroyall lives in storage/smgr/smgr.c
unsafe fn smgrdestroyall() { /* TODO(pg-port): real smgrdestroyall lives in storage/smgr/smgr.c */ }

// AbortTransaction-subset cleanup helpers ----------------------------------
// TODO: import from real ported modules once available.

unsafe fn EmitErrorReport() { /* TODO(pg-port): real EmitErrorReport lives in utils/error/elog.c */ }
unsafe fn FlushErrorState() { /* TODO(pg-port): real FlushErrorState lives in utils/error/elog.c */ }
unsafe fn LWLockReleaseAll() { /* TODO(pg-port): real LWLockReleaseAll lives in storage/lmgr/lwlock.c */ }
unsafe fn pgstat_report_wait_end() { /* TODO(pg-port): real pgstat_report_wait_end lives in utils/activity/pgstat.c */ }
unsafe fn pgaio_error_cleanup() { /* TODO(pg-port): real pgaio_error_cleanup lives in storage/aio/aio.c */ }
unsafe fn UnlockBuffers() { /* TODO(pg-port): real UnlockBuffers lives in storage/buffer/bufmgr.c */ }
unsafe fn ReleaseAuxProcessResources(_is_commit: bool) { /* TODO(pg-port): real ReleaseAuxProcessResources lives in postmaster/auxprocess.c */ }
unsafe fn AtEOXact_Buffers(_is_commit: bool) { /* TODO(pg-port): real AtEOXact_Buffers lives in storage/buffer/bufmgr.c */ }
unsafe fn AtEOXact_SMgr() { /* TODO(pg-port): real AtEOXact_SMgr lives in storage/smgr/smgr.c */ }
unsafe fn AtEOXact_Files(_is_commit: bool) { /* TODO(pg-port): real AtEOXact_Files lives in storage/file/fd.c */ }
unsafe fn AtEOXact_HashTables(_is_commit: bool) { /* TODO(pg-port): real AtEOXact_HashTables lives in utils/hash/dynahash.c */ }

// pgstat stubs -------------------------------------------------------------

// TODO(pg-port): real pgstat_before_server_shutdown lives in utils/activity/pgstat.c
unsafe extern "C" fn pgstat_before_server_shutdown(_code: c_int, _arg: Datum) {
    /* TODO(pg-port): real pgstat_before_server_shutdown lives in utils/activity/pgstat.c */
}

// TODO(pg-port): real pgstat_report_wal lives in utils/activity/pgstat_wal.c
unsafe fn pgstat_report_wal(_force: bool) { /* TODO */ }

// Postmaster signal stubs --------------------------------------------------

// TODO(pg-port): real SendPostmasterSignal lives in storage/ipc/pmsignal.c
unsafe fn SendPostmasterSignal(_reason: c_int) { /* TODO(pg-port): real SendPostmasterSignal lives in storage/ipc/pmsignal.c */ }

// PMSIGNAL_XLOG_IS_SHUTDOWN (storage/pmsignal.h).
// TODO(pg-port): real constant lives in storage/ipc/pmsignal.c
const PMSIGNAL_XLOG_IS_SHUTDOWN: c_int = 4;

// proc stubs ----------------------------------------------------------------

/// Minimal PROC_HDR stub: only the checkpointerProc field is used here.
/// TODO(pg-port): real PROC_HDR lives in storage/lmgr/proc.c
#[repr(C)]
struct PROC_HDR {
    checkpointerProc: ProcNumber,
    // ... remaining fields not yet ported
}

/// Minimal PGPROC stub: only procLatch is accessed here.
/// TODO(pg-port): real PGPROC lives in storage/lmgr/proc.c
#[repr(C)]
struct PGPROC {
    procLatch: Latch,
    // ... remaining fields not yet ported
}

/// TODO(pg-port): real ProcGlobal lives in storage/lmgr/proc.c
static mut ProcGlobal: *mut PROC_HDR = null_mut();

/// TODO(pg-port): real GetPGProcByNumber lives in storage/lmgr/proc.h
unsafe fn GetPGProcByNumber(_n: ProcNumber) -> *mut PGPROC {
    /* TODO(pg-port): real GetPGProcByNumber lives in storage/lmgr/proc.h */
    null_mut()
}

// LWLock stubs -------------------------------------------------------------

/// Opaque LWLock type. TODO(pg-port): real def in storage/lmgr/lwlock.h
type LWLock = c_void;
const LW_EXCLUSIVE: c_int = 2;

/// TODO(pg-port): real CheckpointerCommLock lives in storage/lmgr/lwlock.c
unsafe fn CheckpointerCommLock() -> *mut LWLock {
    /* TODO(pg-port): real CheckpointerCommLock lives in storage/lmgr/lwlock.c */
    null_mut()
}

unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    /* TODO(pg-port): real LWLockAcquire lives in storage/lmgr/lwlock.c */
    true
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    /* TODO(pg-port): real LWLockRelease lives in storage/lmgr/lwlock.c */
}
unsafe fn LWLockHeldByMe(_lock: *mut LWLock) -> bool {
    /* TODO(pg-port): real LWLockHeldByMe lives in storage/lmgr/lwlock.c */
    true
}

// hash table stubs ----------------------------------------------------------
// TODO(pg-port): import from utils/hash/dynahash.rs once HTAB/HASHCTL are exported there.

type HTAB = c_void;

#[repr(C)]
struct HASHCTL {
    keysize: Size,
    entrysize: Size,
    hcxt: MemoryContext,
}

const HASH_ELEM: c_int = 0x0008;
const HASH_BLOBS: c_int = 0x0010;
const HASH_CONTEXT: c_int = 0x0200;
const HASH_ENTER: c_int = 1;

unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    /* TODO(pg-port): real hash_create lives in utils/hash/dynahash.c */
    null_mut()
}

unsafe fn hash_search(
    _hashp: *mut HTAB,
    _keyPtr: *const c_void,
    _action: c_int,
    _foundPtr: *mut bool,
) -> *mut c_void {
    /* TODO(pg-port): real hash_search lives in utils/hash/dynahash.c */
    null_mut()
}

unsafe fn hash_destroy(_hashp: *mut HTAB) {
    /* TODO(pg-port): real hash_destroy lives in utils/hash/dynahash.c */
}

// sync rep / xlog config stubs ---------------------------------------------

// TODO(pg-port): real SyncRepUpdateSyncStandbysDefined lives in replication/syncrep.c
unsafe fn SyncRepUpdateSyncStandbysDefined() {
    /* TODO(pg-port): real SyncRepUpdateSyncStandbysDefined lives in replication/syncrep.c */
}

// TODO(pg-port): real UpdateFullPageWrites lives in access/transam/xlog.c
unsafe fn UpdateFullPageWrites() {
    /* TODO(pg-port): real UpdateFullPageWrites lives in access/transam/xlog.c */
}

// wait-event constants (wait_event.h). TODO: import from generated wait events.
const WAIT_EVENT_CHECKPOINTER_MAIN: u32 = 0;
const WAIT_EVENT_CHECKPOINTER_SHUTDOWN: u32 = 0;
const WAIT_EVENT_CHECKPOINT_WRITE_DELAY: u32 = 0;
const WAIT_EVENT_CHECKPOINT_START: u32 = 0;
const WAIT_EVENT_CHECKPOINT_DONE: u32 = 0;

// add_size / mul_size. TODO(pg-port): real ones live in storage/ipc/shmem.c
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1.saturating_add(s2)
}
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1.saturating_mul(s2)
}

// ShmemInitStruct. TODO(pg-port): real one lives in storage/ipc/shmem.c
unsafe fn ShmemInitStruct(
    _name: *const c_char,
    _size: Size,
    _found: *mut bool,
) -> *mut c_void {
    /* TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c */
    null_mut()
}

// pg_usleep. TODO: import from port layer.
unsafe fn pg_usleep(_microsec: c_long) {
    /* TODO(pg-port): real pg_usleep lives in port/pgsleep.c */
}

/*----------
 * Shared memory area for communication between checkpointer and backends
 *
 * The ckpt counters allow backends to watch for completion of a checkpoint
 * request they send.  Here's how it works:
 *   * At start of a checkpoint, checkpointer reads (and clears) the request
 *     flags and increments ckpt_started, while holding ckpt_lck.
 *   * On completion of a checkpoint, checkpointer sets ckpt_done to
 *     equal ckpt_started.
 *   * On failure of a checkpoint, checkpointer increments ckpt_failed
 *     and sets ckpt_done to equal ckpt_started.
 *
 * The algorithm for backends is:
 *   1. Record current values of ckpt_failed and ckpt_started, and
 *      set request flags, while holding ckpt_lck.
 *   2. Send signal to request checkpoint.
 *   3. Sleep until ckpt_started changes.  Now you know a checkpoint has
 *      begun since you started this algorithm (although *not* that it was
 *      specifically initiated by your signal), and that it is using your flags.
 *   4. Record new value of ckpt_started.
 *   5. Sleep until ckpt_done >= saved value of ckpt_started.  (Use modulo
 *      arithmetic here in case counters wrap around.)  Now you know a
 *      checkpoint has started and completed, but not whether it was
 *      successful.
 *   6. If ckpt_failed is different from the originally saved value,
 *      assume request failed; otherwise it was definitely successful.
 *
 * ckpt_flags holds the OR of the checkpoint request flags sent by all
 * requesting backends since the last checkpoint start.  The flags are
 * chosen so that OR'ing is the correct way to combine multiple requests.
 *
 * The requests array holds fsync requests sent by backends and not yet
 * absorbed by the checkpointer.
 *
 * Unlike the checkpoint fields, requests related fields are protected by
 * CheckpointerCommLock.
 *----------
 */

/// A single pending fsync request forwarded by a backend.
#[repr(C)]
struct CheckpointerRequest {
    type_: SyncRequestType, /* request type */
    ftag: FileTag,          /* file identifier */
}

/// Shared memory structure for checkpointer <-> backend communication.
#[repr(C)]
struct CheckpointerShmemStruct {
    checkpointer_pid: crate::miscadmin::pid_t, /* PID (0 if not started) */

    ckpt_lck: slock_t, /* protects all the ckpt_* fields */

    ckpt_started: c_int, /* advances when checkpoint starts */
    ckpt_done: c_int,    /* advances when checkpoint done */
    ckpt_failed: c_int,  /* advances when checkpoint fails */

    ckpt_flags: c_int, /* checkpoint flags, as defined in xlog.h */

    start_cv: ConditionVariable, /* signaled when ckpt_started advances */
    done_cv: ConditionVariable,  /* signaled when ckpt_done advances */

    num_requests: c_int, /* current # of requests */
    max_requests: c_int, /* allocated array size */
    /* requests[] is a flexible array member -- accessed via raw pointer arithmetic */
}

static mut CheckpointerShmem: *mut CheckpointerShmemStruct = null_mut();

/* interval for calling AbsorbSyncRequests in CheckpointWriteDelay */
const WRITES_PER_ABSORB: c_int = 1000;

/* Max number of requests the checkpointer request queue can hold */
const MAX_CHECKPOINT_REQUESTS: c_int = 10000000;

/*
 * GUC parameters
 */
#[no_mangle]
pub static mut CheckPointTimeout: c_int = 300;
#[no_mangle]
pub static mut CheckPointWarning: c_int = 30;
#[no_mangle]
pub static mut CheckPointCompletionTarget: f64 = 0.9;

/*
 * Private state
 */
static mut ckpt_active: bool = false;
static mut ShutdownXLOGPending: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);

/* these values are valid when ckpt_active is true: */
static mut ckpt_start_time: pg_time_t = 0;
static mut ckpt_start_recptr: XLogRecPtr = 0;
static mut ckpt_cached_elapsed: f64 = 0.0;

static mut last_checkpoint_time: pg_time_t = 0;
static mut last_xlog_switch_time: pg_time_t = 0;

/* Checkpoint flags (access/xlog.h) */
pub const CHECKPOINT_IS_SHUTDOWN: c_int = 0x0001; /* Checkpoint is for shutdown */
pub const CHECKPOINT_END_OF_RECOVERY: c_int = 0x0002; /* Like shutdown checkpoint */
pub const CHECKPOINT_IMMEDIATE: c_int = 0x0004; /* Do it without delays */
pub const CHECKPOINT_FORCE: c_int = 0x0008; /* Force even if no activity */
pub const CHECKPOINT_FLUSH_ALL: c_int = 0x0010; /* Flush all pages including hint bits */
pub const CHECKPOINT_WAIT: c_int = 0x0020; /* Wait for completion */
pub const CHECKPOINT_REQUESTED: c_int = 0x0040; /* Checkpoint request has been made */
pub const CHECKPOINT_CAUSE_XLOG: c_int = 0x0080; /* XLOG consumption */
pub const CHECKPOINT_CAUSE_TIME: c_int = 0x0100; /* Elapsed time */

/* Helpers to access the flexible-array requests[] after the struct */
#[inline]
unsafe fn requests_ptr(shmem: *mut CheckpointerShmemStruct) -> *mut CheckpointerRequest {
    (shmem as *mut u8).add(core::mem::size_of::<CheckpointerShmemStruct>())
        as *mut CheckpointerRequest
}

/* --------------------------------
 *      signal handler routines
 * --------------------------------
 */

/* SIGINT: set flag to trigger writing of shutdown checkpoint */
unsafe extern "C" fn ReqShutdownXLOG(_postgres_signal_arg: c_int) {
    ShutdownXLOGPending.store(true, core::sync::atomic::Ordering::Relaxed);
    SetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
}

/*
 * Main entry point for checkpointer process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
pub unsafe fn CheckpointerMain(_startup_data: *const c_void, startup_data_len: Size) {
    let mut local_sigjmp_buf: sigjmp_buf = [];
    let checkpointer_context: MemoryContext;

    Assert!(startup_data_len == 0);

    MyBackendType = B_CHECKPOINTER;
    AuxiliaryProcessMainCommon();

    (*CheckpointerShmem).checkpointer_pid = MyProcPid;

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * Note: we deliberately ignore SIGTERM, because during a standard Unix
     * system shutdown cycle, init will SIGTERM all processes at once.  We
     * want to wait for the backends to exit, whereupon the postmaster will
     * tell us it's okay to shut down (via SIGUSR2).
     */
    pqsignal(SIGHUP, Some(SignalHandlerForConfigReload));
    pqsignal(SIGINT, Some(ReqShutdownXLOG));
    pqsignal(SIGTERM, SIG_IGN()); /* ignore SIGTERM */
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGALRM, SIG_IGN());
    pqsignal(SIGPIPE, SIG_IGN());
    pqsignal(SIGUSR1, Some(procsignal_sigusr1_handler));
    pqsignal(SIGUSR2, Some(SignalHandlerForShutdownRequest));

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    pqsignal(SIGCHLD, SIG_DFL);

    /*
     * Initialize so that first time-driven event happens at the correct time.
     */
    last_checkpoint_time = time(null_mut());
    last_xlog_switch_time = last_checkpoint_time;

    /*
     * Write out stats after shutdown. This needs to be called by exactly one
     * process during a normal shutdown, and since checkpointer is shut down
     * very late...
     *
     * While e.g. walsenders are active after the shutdown checkpoint has been
     * written (and thus could produce more stats), checkpointer stays around
     * after the shutdown checkpoint has been written. postmaster will only
     * signal checkpointer to exit after all processes that could emit stats
     * have been shut down.
     */
    before_shmem_exit(pgstat_before_server_shutdown, 0 as Datum);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * TopMemoryContext, but resetting that would be a really bad idea.
     */
    checkpointer_context = AllocSetContextCreate!(
        TopMemoryContext,
        "Checkpointer",
        ALLOCSET_DEFAULT_SIZES
    );
    MemoryContextSwitchTo(checkpointer_context);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * You might wonder why this isn't coded as an infinite loop around a
     * PG_TRY construct.  The reason is that this is the bottom of the
     * exception stack, and so with PG_TRY there would be no exception handler
     * in force at all during the CATCH part.  By leaving the outermost setjmp
     * always active, we have at least some chance of recovering from an error
     * during error recovery.  (If we get into an infinite loop thereby, it
     * will soon be stopped by overflow of elog.c's internal state stack.)
     *
     * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
     * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
     * signals other than SIGQUIT will be blocked until we complete error
     * recovery.  It might seem that this policy makes the HOLD_INTERRUPTS()
     * call redundant, but it is not since InterruptPending might be set
     * already.
     */
    if sigsetjmp(&raw mut local_sigjmp_buf, 1) != 0 {
        /* Since not using PG_TRY, must reset error stack by hand */
        error_context_stack = null_mut();

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * These operations are really just a minimal subset of
         * AbortTransaction().  We don't have very many resources to worry
         * about in checkpointer, but we do have LWLocks, buffers, and temp
         * files.
         */
        LWLockReleaseAll();
        ConditionVariableCancelSleep();
        pgstat_report_wait_end();
        pgaio_error_cleanup();
        UnlockBuffers();
        ReleaseAuxProcessResources(false);
        AtEOXact_Buffers(false);
        AtEOXact_SMgr();
        AtEOXact_Files(false);
        AtEOXact_HashTables(false);

        /* Warn any waiting backends that the checkpoint failed. */
        if ckpt_active {
            SpinLockAcquire(&raw mut (*CheckpointerShmem).ckpt_lck);
            (*CheckpointerShmem).ckpt_failed += 1;
            (*CheckpointerShmem).ckpt_done = (*CheckpointerShmem).ckpt_started;
            SpinLockRelease(&raw mut (*CheckpointerShmem).ckpt_lck);

            ConditionVariableBroadcast(&raw mut (*CheckpointerShmem).done_cv);

            ckpt_active = false;
        }

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(checkpointer_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextReset(checkpointer_context);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely
         * to be repeated, and we don't want to be filling the error logs as
         * fast as we can.
         */
        pg_usleep(1000000);
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = (&raw mut local_sigjmp_buf) as *mut c_void;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    sigprocmask(SIG_SETMASK, &raw const UnBlockSig, null_mut::<sigset_t>());

    /*
     * Ensure all shared memory values are set correctly for the config. Doing
     * this here ensures no race conditions from other concurrent updaters.
     */
    UpdateSharedMemoryConfig();

    /*
     * Advertise our proc number that backends can use to wake us up while
     * we're sleeping.
     */
    (*ProcGlobal).checkpointerProc = crate::storage::procnumber::MyProcNumber;

    /*
     * Loop until we've been asked to write the shutdown checkpoint or
     * terminate.
     */
    'main_loop: loop {
        let mut do_checkpoint: bool = false;
        let mut flags: c_int = 0;
        let mut now: pg_time_t;
        let mut elapsed_secs: c_int;
        let cur_timeout: c_int;
        let mut chkpt_or_rstpt_requested: bool = false;
        let mut chkpt_or_rstpt_timed: bool = false;

        /* Clear any already-pending wakeups */
        ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);

        /*
         * Process any requests or signals received recently.
         */
        AbsorbSyncRequests();

        ProcessCheckpointerInterrupts();
        if ShutdownXLOGPending.load(core::sync::atomic::Ordering::Relaxed) || ShutdownRequestPending {
            break 'main_loop;
        }

        /*
         * Detect a pending checkpoint request by checking whether the flags
         * word in shared memory is nonzero.  We shouldn't need to acquire the
         * ckpt_lck for this.
         */
        if (*(CheckpointerShmem as *const CheckpointerShmemStruct)).ckpt_flags != 0 {
            do_checkpoint = true;
            chkpt_or_rstpt_requested = true;
        }

        /*
         * Force a checkpoint if too much time has elapsed since the last one.
         * Note that we count a timed checkpoint in stats only when this
         * occurs without an external request, but we set the CAUSE_TIME flag
         * bit even if there is also an external request.
         */
        now = time(null_mut());
        elapsed_secs = (now - last_checkpoint_time) as c_int;
        if elapsed_secs >= CheckPointTimeout {
            if !do_checkpoint {
                chkpt_or_rstpt_timed = true;
            }
            do_checkpoint = true;
            flags |= CHECKPOINT_CAUSE_TIME;
        }

        /*
         * Do a checkpoint if requested.
         */
        if do_checkpoint {
            let mut ckpt_performed: bool = false;
            let do_restartpoint: bool;

            /* Check if we should perform a checkpoint or a restartpoint. */
            do_restartpoint = RecoveryInProgress();

            /*
             * Atomically fetch the request flags to figure out what kind of a
             * checkpoint we should perform, and increase the started-counter
             * to acknowledge that we've started a new checkpoint.
             */
            SpinLockAcquire(&raw mut (*CheckpointerShmem).ckpt_lck);
            flags |= (*CheckpointerShmem).ckpt_flags;
            (*CheckpointerShmem).ckpt_flags = 0;
            (*CheckpointerShmem).ckpt_started += 1;
            SpinLockRelease(&raw mut (*CheckpointerShmem).ckpt_lck);

            ConditionVariableBroadcast(&raw mut (*CheckpointerShmem).start_cv);

            /*
             * The end-of-recovery checkpoint is a real checkpoint that's
             * performed while we're still in recovery.
             */
            let mut do_restartpoint = do_restartpoint;
            if (flags & CHECKPOINT_END_OF_RECOVERY) != 0 {
                do_restartpoint = false;
            }

            if chkpt_or_rstpt_timed {
                chkpt_or_rstpt_timed = false;
                if do_restartpoint {
                    PendingCheckpointerStats.restartpoints_timed += 1;
                } else {
                    PendingCheckpointerStats.num_timed += 1;
                }
            }

            if chkpt_or_rstpt_requested {
                chkpt_or_rstpt_requested = false;
                if do_restartpoint {
                    PendingCheckpointerStats.restartpoints_requested += 1;
                } else {
                    PendingCheckpointerStats.num_requested += 1;
                }
            }

            /*
             * We will warn if (a) too soon since last checkpoint (whatever
             * caused it) and (b) somebody set the CHECKPOINT_CAUSE_XLOG flag
             * since the last checkpoint start.  Note in particular that this
             * implementation will not generate warnings caused by
             * CheckPointTimeout < CheckPointWarning.
             */
            if !do_restartpoint
                && (flags & CHECKPOINT_CAUSE_XLOG) != 0
                && elapsed_secs < CheckPointWarning
            {
                ereport!(
                    LOG,
                    errmsg!(
                        "checkpoints are occurring too frequently ({} second(s) apart); \
                         consider increasing max_wal_size",
                        elapsed_secs
                    )
                );
            }

            /*
             * Initialize checkpointer-private variables used during
             * checkpoint.
             */
            ckpt_active = true;
            if do_restartpoint {
                ckpt_start_recptr = GetXLogReplayRecPtr(null_mut());
            } else {
                ckpt_start_recptr = GetInsertRecPtr();
            }
            ckpt_start_time = now;
            ckpt_cached_elapsed = 0.0;

            /*
             * Do the checkpoint.
             */
            if !do_restartpoint {
                ckpt_performed = CreateCheckPoint(flags);
            } else {
                ckpt_performed = CreateRestartPoint(flags);
            }

            /*
             * After any checkpoint, free all smgr objects.  Otherwise we
             * would never do so for dropped relations, as the checkpointer
             * does not process shared invalidation messages or call
             * AtEOXact_SMgr().
             */
            smgrdestroyall();

            /*
             * Indicate checkpoint completion to any waiting backends.
             */
            SpinLockAcquire(&raw mut (*CheckpointerShmem).ckpt_lck);
            (*CheckpointerShmem).ckpt_done = (*CheckpointerShmem).ckpt_started;
            SpinLockRelease(&raw mut (*CheckpointerShmem).ckpt_lck);

            ConditionVariableBroadcast(&raw mut (*CheckpointerShmem).done_cv);

            if !do_restartpoint {
                /*
                 * Note we record the checkpoint start time not end time as
                 * last_checkpoint_time.  This is so that time-driven
                 * checkpoints happen at a predictable spacing.
                 */
                last_checkpoint_time = now;

                if ckpt_performed {
                    PendingCheckpointerStats.num_performed += 1;
                }
            } else {
                if ckpt_performed {
                    /*
                     * The same as for checkpoint. Please see the
                     * corresponding comment.
                     */
                    last_checkpoint_time = now;

                    PendingCheckpointerStats.restartpoints_performed += 1;
                } else {
                    /*
                     * We were not able to perform the restartpoint
                     * (checkpoints throw an ERROR in case of error).  Most
                     * likely because we have not received any new checkpoint
                     * WAL records since the last restartpoint. Try again in
                     * 15 s.
                     */
                    last_checkpoint_time = now - CheckPointTimeout as pg_time_t + 15;
                }
            }

            ckpt_active = false;

            /*
             * We may have received an interrupt during the checkpoint and the
             * latch might have been reset (e.g. in CheckpointWriteDelay).
             */
            ProcessCheckpointerInterrupts();
            if ShutdownXLOGPending.load(core::sync::atomic::Ordering::Relaxed)
                || ShutdownRequestPending
            {
                break 'main_loop;
            }
        }

        /* Check for archive_timeout and switch xlog files if necessary. */
        CheckArchiveTimeout();

        /* Report pending statistics to the cumulative stats system */
        pgstat_report_checkpointer();
        pgstat_report_wal(true);

        /*
         * If any checkpoint flags have been set, redo the loop to handle the
         * checkpoint without sleeping.
         */
        if (*(CheckpointerShmem as *const CheckpointerShmemStruct)).ckpt_flags != 0 {
            continue 'main_loop;
        }

        /*
         * Sleep until we are signaled or it's time for another checkpoint or
         * xlog file switch.
         */
        now = time(null_mut());
        elapsed_secs = (now - last_checkpoint_time) as c_int;
        if elapsed_secs >= CheckPointTimeout {
            continue 'main_loop; /* no sleep for us ... */
        }
        let mut cur_timeout = CheckPointTimeout - elapsed_secs;
        if XLogArchiveTimeout > 0 && !RecoveryInProgress() {
            let elapsed_secs2 = (now - last_xlog_switch_time) as c_int;
            if elapsed_secs2 >= XLogArchiveTimeout {
                continue 'main_loop; /* no sleep for us ... */
            }
            cur_timeout = Min(cur_timeout, XLogArchiveTimeout - elapsed_secs2);
        }

        let _ = WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            (cur_timeout as c_long) * 1000, /* convert to ms */
            WAIT_EVENT_CHECKPOINTER_MAIN,
        );
    } /* end of main_loop */

    /*
     * From here on, elog(ERROR) should end with exit(1), not send control
     * back to the sigsetjmp block above.
     */
    ExitOnAnyError = true;

    if ShutdownXLOGPending.load(core::sync::atomic::Ordering::Relaxed) {
        /*
         * Close down the database.
         *
         * Since ShutdownXLOG() creates restartpoint or checkpoint, and
         * updates the statistics, increment the checkpoint request and flush
         * out pending statistic.
         */
        PendingCheckpointerStats.num_requested += 1;
        ShutdownXLOG(0, 0 as Datum);
        pgstat_report_checkpointer();
        pgstat_report_wal(true);

        /*
         * Tell postmaster that we're done.
         */
        SendPostmasterSignal(PMSIGNAL_XLOG_IS_SHUTDOWN);
        ShutdownXLOGPending.store(false, core::sync::atomic::Ordering::Relaxed);
    }

    /*
     * Wait until we're asked to shut down. By separating the writing of the
     * shutdown checkpoint from checkpointer exiting, checkpointer can perform
     * some should-be-as-late-as-possible work like writing out stats.
     */
    loop {
        /* Clear any already-pending wakeups */
        ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);

        ProcessCheckpointerInterrupts();

        if ShutdownRequestPending {
            break;
        }

        let _ = WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_CHECKPOINTER_SHUTDOWN,
        );
    }

    /* Normal exit from the checkpointer is here */
    proc_exit(0); /* done */
}

/*
 * Process any new interrupts.
 */
unsafe fn ProcessCheckpointerInterrupts() {
    if ProcSignalBarrierPending != 0 {
        ProcessProcSignalBarrier();
    }

    if ConfigReloadPending {
        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);

        /*
         * Checkpointer is the last process to shut down, so we ask it to hold
         * the keys for a range of other tasks required most of which have
         * nothing to do with checkpointing at all.
         *
         * For various reasons, some config values can change dynamically so
         * the primary copy of them is held in shared memory to make sure all
         * backends see the same value.  We make Checkpointer responsible for
         * updating the shared memory copy if the parameter setting changes
         * because of SIGHUP.
         */
        UpdateSharedMemoryConfig();
    }

    /* Perform logging of memory contexts of this process */
    if LogMemoryContextPending != 0 {
        ProcessLogMemoryContextInterrupt();
    }
}

/*
 * CheckArchiveTimeout -- check for archive_timeout and switch xlog files
 *
 * This will switch to a new WAL file and force an archive file write if
 * meaningful activity is recorded in the current WAL file. This includes most
 * writes, including just a single checkpoint record, but excludes WAL records
 * that were inserted with the XLOG_MARK_UNIMPORTANT flag being set (like
 * snapshots of running transactions).  Such records, depending on
 * configuration, occur on regular intervals and don't contain important
 * information.  This avoids generating archives with a few unimportant
 * records.
 */
unsafe fn CheckArchiveTimeout() {
    let now: pg_time_t;
    let last_time: pg_time_t;
    let mut last_switch_lsn: XLogRecPtr = 0;

    if XLogArchiveTimeout <= 0 || RecoveryInProgress() {
        return;
    }

    now = time(null_mut());

    /* First we do a quick check using possibly-stale local state. */
    if ((now - last_xlog_switch_time) as c_int) < XLogArchiveTimeout {
        return;
    }

    /*
     * Update local state ... note that last_xlog_switch_time is the last time
     * a switch was performed *or requested*.
     */
    last_time = GetLastSegSwitchData(&raw mut last_switch_lsn);

    last_xlog_switch_time = Max(last_xlog_switch_time, last_time);

    /* Now we can do the real checks */
    if ((now - last_xlog_switch_time) as c_int) >= XLogArchiveTimeout {
        /*
         * Switch segment only when "important" WAL has been logged since the
         * last segment switch (last_switch_lsn points to end of segment
         * switch occurred in).
         */
        if GetLastImportantRecPtr() > last_switch_lsn {
            let switchpoint: XLogRecPtr;

            /* mark switch as unimportant, avoids triggering checkpoints */
            switchpoint = RequestXLogSwitch(true);

            /*
             * If the returned pointer points exactly to a segment boundary,
             * assume nothing happened.
             */
            if XLogSegmentOffset(switchpoint, wal_segment_size) != 0 {
                elog!(
                    DEBUG1,
                    "write-ahead log switch forced (\"archive_timeout\"={})",
                    XLogArchiveTimeout
                );
            }
        }

        /*
         * Update state in any case, so we don't retry constantly when the
         * system is idle.
         */
        last_xlog_switch_time = now;
    }
}

/*
 * Returns true if an immediate checkpoint request is pending.  (Note that
 * this does not check the *current* checkpoint's IMMEDIATE flag, but whether
 * there is one pending behind it.)
 */
unsafe fn ImmediateCheckpointRequested() -> bool {
    let cps: *const CheckpointerShmemStruct = CheckpointerShmem;

    /*
     * We don't need to acquire the ckpt_lck in this case because we're only
     * looking at a single flag bit.
     */
    ((*cps).ckpt_flags & CHECKPOINT_IMMEDIATE) != 0
}

/*
 * CheckpointWriteDelay -- control rate of checkpoint
 *
 * This function is called after each page write performed by BufferSync().
 * It is responsible for throttling BufferSync()'s write rate to hit
 * checkpoint_completion_target.
 *
 * The checkpoint request flags should be passed in; currently the only one
 * examined is CHECKPOINT_IMMEDIATE, which disables delays between writes.
 *
 * 'progress' is an estimate of how much of the work has been done, as a
 * fraction between 0.0 meaning none, and 1.0 meaning all done.
 */
pub unsafe fn CheckpointWriteDelay(flags: c_int, progress: f64) {
    static mut absorb_counter: c_int = WRITES_PER_ABSORB;

    /* Do nothing if checkpoint is being executed by non-checkpointer process */
    if !AmCheckpointerProcess() {
        return;
    }

    /*
     * Perform the usual duties and take a nap, unless we're behind schedule,
     * in which case we just try to catch up as quickly as possible.
     */
    if (flags & CHECKPOINT_IMMEDIATE) == 0
        && !ShutdownXLOGPending.load(core::sync::atomic::Ordering::Relaxed)
        && !ShutdownRequestPending
        && !ImmediateCheckpointRequested()
        && IsCheckpointOnSchedule(progress)
    {
        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
            /* update shmem copies of config variables */
            UpdateSharedMemoryConfig();
        }

        AbsorbSyncRequests();
        absorb_counter = WRITES_PER_ABSORB;

        CheckArchiveTimeout();

        /* Report interim statistics to the cumulative stats system */
        pgstat_report_checkpointer();

        /*
         * This sleep used to be connected to bgwriter_delay, typically 200ms.
         * That resulted in more frequent wakeups if not much work to do.
         * Checkpointer and bgwriter are no longer related so take the Big
         * Sleep.
         */
        WaitLatch(
            MyLatch as *mut crate::storage::ipc::latch::Latch,
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
            100,
            WAIT_EVENT_CHECKPOINT_WRITE_DELAY,
        );
        ResetLatch(MyLatch as *mut crate::storage::ipc::latch::Latch);
    } else {
        absorb_counter -= 1;
        if absorb_counter <= 0 {
            /*
             * Absorb pending fsync requests after each WRITES_PER_ABSORB write
             * operations even when we don't sleep, to prevent overflow of the
             * fsync request queue.
             */
            AbsorbSyncRequests();
            absorb_counter = WRITES_PER_ABSORB;
        }
    }

    /* Check for barrier events. */
    if ProcSignalBarrierPending != 0 {
        ProcessProcSignalBarrier();
    }
}

/*
 * IsCheckpointOnSchedule -- are we on schedule to finish this checkpoint
 *       (or restartpoint) in time?
 *
 * Compares the current progress against the time/segments elapsed since last
 * checkpoint, and returns true if the progress we've made this far is greater
 * than the elapsed time/segments.
 */
unsafe fn IsCheckpointOnSchedule(mut progress: f64) -> bool {
    let recptr: XLogRecPtr;
    let mut now: timeval = core::mem::zeroed();
    let elapsed_xlogs: f64;
    let elapsed_time: f64;

    Assert!(ckpt_active);

    /* Scale progress according to checkpoint_completion_target. */
    progress *= CheckPointCompletionTarget;

    /*
     * Check against the cached value first. Only do the more expensive
     * calculations once we reach the target previously calculated. Since
     * neither time or WAL insert pointer moves backwards, a freshly
     * calculated value can only be greater than or equal to the cached value.
     */
    if progress < ckpt_cached_elapsed {
        return false;
    }

    /*
     * Check progress against WAL segments written and CheckPointSegments.
     */
    if RecoveryInProgress() {
        recptr = GetXLogReplayRecPtr(null_mut());
    } else {
        recptr = GetInsertRecPtr();
    }
    elapsed_xlogs = ((recptr.wrapping_sub(ckpt_start_recptr) as f64)
        / wal_segment_size as f64)
        / CheckPointSegments as f64;

    if progress < elapsed_xlogs {
        ckpt_cached_elapsed = elapsed_xlogs;
        return false;
    }

    /*
     * Check progress against time elapsed and checkpoint_timeout.
     */
    gettimeofday(&raw mut now, null_mut());
    elapsed_time = (((now.tv_sec - ckpt_start_time) as f64)
        + now.tv_usec as f64 / 1_000_000.0)
        / CheckPointTimeout as f64;

    if progress < elapsed_time {
        ckpt_cached_elapsed = elapsed_time;
        return false;
    }

    /* It looks like we're on schedule. */
    true
}

/* --------------------------------
 *      communication with backends
 * --------------------------------
 */

/*
 * CheckpointerShmemSize
 *      Compute space needed for checkpointer-related shared memory
 */
pub unsafe fn CheckpointerShmemSize() -> Size {
    let size: Size;

    /*
     * The size of the requests[] array is arbitrarily set equal to NBuffers.
     * But there is a cap of MAX_CHECKPOINT_REQUESTS to prevent accumulating
     * too many checkpoint requests in the ring buffer.
     */
    let size = core::mem::offset_of!(CheckpointerShmemStruct, ckpt_lck); // placeholder for offsetof requests
    // Use raw offset: size_of the struct fields before the flexible array.
    let base = core::mem::size_of::<CheckpointerShmemStruct>();
    let nreqs = Min(NBuffers, MAX_CHECKPOINT_REQUESTS) as Size;
    add_size(base, mul_size(nreqs, core::mem::size_of::<CheckpointerRequest>()))
}

/*
 * CheckpointerShmemInit
 *      Allocate and initialize checkpointer-related shared memory
 */
pub unsafe fn CheckpointerShmemInit() {
    let size: Size = CheckpointerShmemSize();
    let mut found: bool = false;

    CheckpointerShmem = ShmemInitStruct(
        b"Checkpointer Data\0".as_ptr() as *const c_char,
        size,
        &raw mut found,
    ) as *mut CheckpointerShmemStruct;

    if !found {
        /*
         * First time through, so initialize.  Note that we zero the whole
         * requests array; this is so that CompactCheckpointerRequestQueue can
         * assume that any pad bytes in the request structs are zeroes.
         */
        crate::c::MemSet(CheckpointerShmem as *mut c_void, 0, size);
        SpinLockInit(&raw mut (*CheckpointerShmem).ckpt_lck);
        (*CheckpointerShmem).max_requests = Min(NBuffers, MAX_CHECKPOINT_REQUESTS);
        ConditionVariableInit(&raw mut (*CheckpointerShmem).start_cv);
        ConditionVariableInit(&raw mut (*CheckpointerShmem).done_cv);
    }
}

/*
 * RequestCheckpoint
 *      Called in backend processes to request a checkpoint
 *
 * flags is a bitwise OR of the following:
 *   CHECKPOINT_IS_SHUTDOWN: checkpoint is for database shutdown.
 *   CHECKPOINT_END_OF_RECOVERY: checkpoint is for end of WAL recovery.
 *   CHECKPOINT_IMMEDIATE: finish the checkpoint ASAP,
 *     ignoring checkpoint_completion_target parameter.
 *   CHECKPOINT_FORCE: force a checkpoint even if no XLOG activity has occurred
 *     since the last one (implied by CHECKPOINT_IS_SHUTDOWN or
 *     CHECKPOINT_END_OF_RECOVERY).
 *   CHECKPOINT_WAIT: wait for completion before returning (otherwise,
 *     just signal checkpointer to do it, and return).
 *   CHECKPOINT_CAUSE_XLOG: checkpoint is requested due to xlog filling.
 *     (This affects logging, and in particular enables CheckPointWarning.)
 */
pub unsafe fn RequestCheckpoint(flags: c_int) {
    let mut ntries: c_int;
    let old_failed: c_int;
    let old_started: c_int;

    /*
     * If in a standalone backend, just do it ourselves.
     */
    if !IsPostmasterEnvironment {
        /*
         * There's no point in doing slow checkpoints in a standalone backend,
         * because there's no other backends the checkpoint could disrupt.
         */
        CreateCheckPoint(flags | CHECKPOINT_IMMEDIATE);

        /* Free all smgr objects, as CheckpointerMain() normally would. */
        smgrdestroyall();

        return;
    }

    /*
     * Atomically set the request flags, and take a snapshot of the counters.
     * When we see ckpt_started > old_started, we know the flags we set here
     * have been seen by checkpointer.
     *
     * Note that we OR the flags with any existing flags, to avoid overriding
     * a "stronger" request by another backend.  The flag senses must be
     * chosen to make this work!
     */
    SpinLockAcquire(&raw mut (*CheckpointerShmem).ckpt_lck);

    old_failed = (*CheckpointerShmem).ckpt_failed;
    old_started = (*CheckpointerShmem).ckpt_started;
    (*CheckpointerShmem).ckpt_flags |= flags | CHECKPOINT_REQUESTED;

    SpinLockRelease(&raw mut (*CheckpointerShmem).ckpt_lck);

    /*
     * Set checkpointer's latch to request checkpoint.  It's possible that the
     * checkpointer hasn't started yet, so we will retry a few times if
     * needed.  (Actually, more than a few times, since on slow or overloaded
     * buildfarm machines, it's been observed that the checkpointer can take
     * several seconds to start.)  However, if not told to wait for the
     * checkpoint to occur, we consider failure to set the latch to be
     * nonfatal and merely LOG it.  The checkpointer should see the request
     * when it does start, with or without the SetLatch().
     */
    /* MAX_SIGNAL_TRIES: max wait 60.0 sec */
    const MAX_SIGNAL_TRIES: c_int = 600;
    ntries = 0;
    loop {
        let procglobal: *const PROC_HDR = ProcGlobal;
        let checkpointer_proc: ProcNumber = (*procglobal).checkpointerProc;

        if checkpointer_proc == INVALID_PROC_NUMBER {
            if ntries >= MAX_SIGNAL_TRIES || (flags & CHECKPOINT_WAIT) == 0 {
                elog!(
                    if (flags & CHECKPOINT_WAIT) != 0 { ERROR } else { LOG },
                    "could not notify checkpoint: checkpointer is not running"
                );
                break;
            }
        } else {
            SetLatch(&raw mut (*GetPGProcByNumber(checkpointer_proc)).procLatch as *mut crate::storage::ipc::latch::Latch);
            /* notified successfully */
            break;
        }

        CHECK_FOR_INTERRUPTS();
        pg_usleep(100000); /* wait 0.1 sec, then retry */
        ntries += 1;
    }

    /*
     * If requested, wait for completion.  We detect completion according to
     * the algorithm given above.
     */
    if (flags & CHECKPOINT_WAIT) != 0 {
        let new_started: c_int;
        let new_failed: c_int;

        /* Wait for a new checkpoint to start. */
        ConditionVariablePrepareToSleep(&raw mut (*CheckpointerShmem).start_cv);
        loop {
            SpinLockAcquire(&raw mut (*CheckpointerShmem).ckpt_lck);
            let ns = (*CheckpointerShmem).ckpt_started;
            SpinLockRelease(&raw mut (*CheckpointerShmem).ckpt_lck);

            if ns != old_started {
                new_started = ns;
                break;
            }

            ConditionVariableSleep(
                &raw mut (*CheckpointerShmem).start_cv,
                WAIT_EVENT_CHECKPOINT_START,
            );
        }
        ConditionVariableCancelSleep();

        /*
         * We are waiting for ckpt_done >= new_started, in a modulo sense.
         */
        ConditionVariablePrepareToSleep(&raw mut (*CheckpointerShmem).done_cv);
        loop {
            SpinLockAcquire(&raw mut (*CheckpointerShmem).ckpt_lck);
            let nd = (*CheckpointerShmem).ckpt_done;
            let nf = (*CheckpointerShmem).ckpt_failed;
            SpinLockRelease(&raw mut (*CheckpointerShmem).ckpt_lck);

            if nd.wrapping_sub(new_started) >= 0 {
                new_failed = nf;
                break;
            }

            ConditionVariableSleep(
                &raw mut (*CheckpointerShmem).done_cv,
                WAIT_EVENT_CHECKPOINT_DONE,
            );
        }
        ConditionVariableCancelSleep();

        if new_failed != old_failed {
            ereport!(
                ERROR,
                errmsg!("checkpoint request failed; consult recent messages in the server log for details")
            );
        }
    }
}

/*
 * ForwardSyncRequest
 *      Forward a file-fsync request from a backend to the checkpointer
 *
 * Whenever a backend is compelled to write directly to a relation
 * (which should be seldom, if the background writer is getting its job done),
 * the backend calls this routine to pass over knowledge that the relation
 * is dirty and must be fsync'd before next checkpoint.  We also use this
 * opportunity to count such writes for statistical purposes.
 *
 * To avoid holding the lock for longer than necessary, we normally write
 * to the requests[] queue without checking for duplicates.  The checkpointer
 * will have to eliminate dups internally anyway.  However, if we discover
 * that the queue is full, we make a pass over the entire queue to compact
 * it.  This is somewhat expensive, but the alternative is for the backend
 * to perform its own fsync, which is far more expensive in practice.  It
 * is theoretically possible a backend fsync might still be necessary, if
 * the queue is full and contains no duplicate entries.  In that case, we
 * let the backend know by returning false.
 */
pub unsafe fn ForwardSyncRequest(ftag: *const FileTag, type_: SyncRequestType) -> bool {
    let request: *mut CheckpointerRequest;
    let too_full: bool;

    if !IsUnderPostmaster {
        return false; /* probably shouldn't even get here */
    }

    if AmCheckpointerProcess() {
        elog!(ERROR, "ForwardSyncRequest must not be called in checkpointer");
    }

    LWLockAcquire(CheckpointerCommLock(), LW_EXCLUSIVE);

    /*
     * If the checkpointer isn't running or the request queue is full, the
     * backend will have to perform its own fsync request.  But before forcing
     * that to happen, we can try to compact the request queue.
     */
    if (*CheckpointerShmem).checkpointer_pid == 0
        || ((*CheckpointerShmem).num_requests >= (*CheckpointerShmem).max_requests
            && !CompactCheckpointerRequestQueue())
    {
        LWLockRelease(CheckpointerCommLock());
        return false;
    }

    /* OK, insert request */
    let idx = (*CheckpointerShmem).num_requests as usize;
    (*CheckpointerShmem).num_requests += 1;
    request = requests_ptr(CheckpointerShmem).add(idx);
    (*request).ftag = *ftag;
    (*request).type_ = type_;

    /* If queue is more than half full, nudge the checkpointer to empty it */
    too_full = (*CheckpointerShmem).num_requests
        >= (*CheckpointerShmem).max_requests / 2;

    LWLockRelease(CheckpointerCommLock());

    /* ... but not till after we release the lock */
    if too_full {
        let procglobal: *const PROC_HDR = ProcGlobal;
        let checkpointer_proc: ProcNumber = (*procglobal).checkpointerProc;

        if checkpointer_proc != INVALID_PROC_NUMBER {
            SetLatch(&raw mut (*GetPGProcByNumber(checkpointer_proc)).procLatch as *mut crate::storage::ipc::latch::Latch);
        }
    }

    true
}

/*
 * CompactCheckpointerRequestQueue
 *      Remove duplicates from the request queue to avoid backend fsyncs.
 *      Returns "true" if any entries were removed.
 *
 * Although a full fsync request queue is not common, it can lead to severe
 * performance problems when it does happen.  So far, this situation has
 * only been observed to occur when the system is under heavy write load,
 * and especially during the "sync" phase of a checkpoint.  Without this
 * logic, each backend begins doing an fsync for every block written, which
 * gets very expensive and can slow down the whole system.
 *
 * Trying to do this every time the queue is full could lose if there
 * aren't any removable entries.  But that should be vanishingly rare in
 * practice: there's one queue entry per shared buffer.
 */
unsafe fn CompactCheckpointerRequestQueue() -> bool {
    /// Entry in the temporary dedup hash table: maps a request value to the
    /// slot index of its latest occurrence.
    #[repr(C)]
    struct CheckpointerSlotMapping {
        request: CheckpointerRequest,
        slot: c_int,
    }

    let n: c_int;
    let preserve_count: c_int;
    let mut num_skipped: c_int = 0;
    let mut ctl: HASHCTL = core::mem::zeroed();
    let htab: *mut HTAB;
    let skip_slot: *mut bool;

    /* must hold CheckpointerCommLock in exclusive mode */
    Assert!(LWLockHeldByMe(CheckpointerCommLock()));

    /* Avoid memory allocations in a critical section. */
    if CritSectionCount > 0 {
        return false;
    }

    /* Initialize skip_slot array */
    let nreqs = (*CheckpointerShmem).num_requests as usize;
    skip_slot = palloc0(nreqs * core::mem::size_of::<bool>()) as *mut bool;

    /* Initialize temporary hash table */
    ctl.keysize = core::mem::size_of::<CheckpointerRequest>();
    ctl.entrysize = core::mem::size_of::<CheckpointerSlotMapping>();
    ctl.hcxt = CurrentMemoryContext;

    htab = hash_create(
        b"CompactCheckpointerRequestQueue\0".as_ptr() as *const c_char,
        (*CheckpointerShmem).num_requests as c_long,
        &raw mut ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    /*
     * The basic idea here is that a request can be skipped if it's followed
     * by a later, identical request.  It might seem more sensible to work
     * backwards from the end of the queue and check whether a request is
     * *preceded* by an earlier, identical request, in the hopes of doing less
     * copying.  But that might change the semantics, if there's an
     * intervening SYNC_FORGET_REQUEST or SYNC_FILTER_REQUEST, so we do it
     * this way.  It would be possible to be even smarter if we made the code
     * below understand the specific semantics of such requests (it could blow
     * away preceding entries that would end up being canceled anyhow), but
     * it's not clear that the extra complexity would buy us anything.
     */
    let req_base = requests_ptr(CheckpointerShmem);
    let mut i: c_int = 0;
    while i < (*CheckpointerShmem).num_requests {
        let request: *mut CheckpointerRequest = req_base.add(i as usize);
        let mut found: bool = false;
        let slotmap: *mut CheckpointerSlotMapping = hash_search(
            htab,
            request as *const c_void,
            HASH_ENTER,
            &raw mut found,
        ) as *mut CheckpointerSlotMapping;

        if found {
            /* Duplicate, so mark the previous occurrence as skippable */
            *skip_slot.add((*slotmap).slot as usize) = true;
            num_skipped += 1;
        }
        /* Remember slot containing latest occurrence of this request value */
        (*slotmap).slot = i;
        i += 1;
    }

    /* Done with the hash table. */
    hash_destroy(htab);

    /* If no duplicates, we're out of luck. */
    if num_skipped == 0 {
        pfree(skip_slot as *mut c_void);
        return false;
    }

    /* We found some duplicates; remove them. */
    let mut preserve_count: c_int = 0;
    let mut i: c_int = 0;
    while i < (*CheckpointerShmem).num_requests {
        if *skip_slot.add(i as usize) {
            i += 1;
            continue;
        }
        *req_base.add(preserve_count as usize) = core::ptr::read(req_base.add(i as usize));
        preserve_count += 1;
        i += 1;
    }
    elog!(
        DEBUG1,
        "compacted fsync request queue from {} entries to {} entries",
        (*CheckpointerShmem).num_requests,
        preserve_count
    );
    (*CheckpointerShmem).num_requests = preserve_count;

    /* Cleanup. */
    pfree(skip_slot as *mut c_void);
    true
}

/*
 * AbsorbSyncRequests
 *      Retrieve queued sync requests and pass them to sync mechanism.
 *
 * This is exported because it must be called during CreateCheckPoint;
 * we have to be sure we have accepted all pending requests just before
 * we start fsync'ing.  Since CreateCheckPoint sometimes runs in
 * non-checkpointer processes, do nothing if not checkpointer.
 */
pub unsafe fn AbsorbSyncRequests() {
    let mut requests: *mut CheckpointerRequest = null_mut();
    let n: c_int;

    if !AmCheckpointerProcess() {
        return;
    }

    LWLockAcquire(CheckpointerCommLock(), LW_EXCLUSIVE);

    /*
     * We try to avoid holding the lock for a long time by copying the request
     * array, and processing the requests after releasing the lock.
     *
     * Once we have cleared the requests from shared memory, we have to PANIC
     * if we then fail to absorb them (eg, because our hashtable runs out of
     * memory).  This is because the system cannot run safely if we are unable
     * to fsync what we have been told to fsync.  Fortunately, the hashtable
     * is so small that the problem is quite unlikely to arise in practice.
     */
    n = (*CheckpointerShmem).num_requests;
    if n > 0 {
        requests = palloc(
            (n as usize) * core::mem::size_of::<CheckpointerRequest>()
        ) as *mut CheckpointerRequest;
        core::ptr::copy_nonoverlapping(
            requests_ptr(CheckpointerShmem),
            requests,
            n as usize,
        );
    }

    START_CRIT_SECTION();

    (*CheckpointerShmem).num_requests = 0;

    LWLockRelease(CheckpointerCommLock());

    let mut i: c_int = 0;
    while i < n {
        let req: *mut CheckpointerRequest = requests.add(i as usize);
        RememberSyncRequest(&raw const (*req).ftag, (*req).type_);
        i += 1;
    }

    END_CRIT_SECTION();

    if !requests.is_null() {
        pfree(requests as *mut c_void);
    }
}

/*
 * Update any shared memory configurations based on config parameters
 */
unsafe fn UpdateSharedMemoryConfig() {
    /* update global shmem state for sync rep */
    SyncRepUpdateSyncStandbysDefined();

    /*
     * If full_page_writes has been changed by SIGHUP, we update it in shared
     * memory and write an XLOG_FPW_CHANGE record.
     */
    UpdateFullPageWrites();

    elog!(DEBUG2, "checkpointer updated shared memory configuration values");
}

/*
 * FirstCallSinceLastCheckpoint allows a process to take an action once
 * per checkpoint cycle by asynchronously checking for checkpoint completion.
 */
pub unsafe fn FirstCallSinceLastCheckpoint() -> bool {
    static mut local_ckpt_done: c_int = 0;
    let new_done: c_int;
    let first_call: bool;

    SpinLockAcquire(&raw mut (*CheckpointerShmem).ckpt_lck);
    new_done = (*CheckpointerShmem).ckpt_done;
    SpinLockRelease(&raw mut (*CheckpointerShmem).ckpt_lck);

    first_call = new_done != local_ckpt_done;

    local_ckpt_done = new_done;

    first_call
}
