//! postmaster/bgwriter.c - background writer (bgwriter) process main loop.
//!
//! The background writer attempts to keep regular backends from having to
//! write out dirty shared buffers.  As of Postgres 9.2 the bgwriter no longer
//! handles checkpoints.

use crate::prelude::*;

use crate::libpq::pqsignal::{
    pqsignal, sigset_t, SigHandler, UnBlockSig, SIGALRM, SIGCHLD, SIGHUP, SIGINT, SIGPIPE, SIGTERM,
    SIGUSR1, SIGUSR2, SIG_DFL,
};
use crate::miscadmin::{HOLD_INTERRUPTS, Latch, MyLatch, RESUME_INTERRUPTS, B_BG_WRITER};
use crate::port::pgsleep::pg_usleep;
use crate::postmaster::auxprocess::AuxiliaryProcessMainCommon;
use crate::postmaster::interrupt::{
    ProcessMainLoopInterrupts, SignalHandlerForConfigReload, SignalHandlerForShutdownRequest,
};
use crate::storage::buf_internals::{StrategyNotifyBgWriter, WritebackContext, WritebackContextInit};
use crate::storage::procnumber::MyProcNumber;
use crate::utils::activity::pgstat::GetCurrentTimestamp;
use crate::utils::activity::pgstat_bgwriter::pgstat_report_bgwriter;

// TimestampTz / XLogRecPtr / InvalidXLogRecPtr (timestamp.h / xlogdefs.h).
use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, XLogRecPtr};
use crate::miscadmin::TimestampTz;

// MyBackendType is an extern mutable global (miscadmin.h).
extern "C" {
    static mut MyBackendType: c_int;
}

// sigprocmask(2) - libc. TODO: route through a ported port-layer wrapper.
extern "C" {
    fn sigprocmask(how: c_int, set: *const sigset_t, oldset: *mut sigset_t) -> c_int;
}

/*
 * GUC parameters
 */
#[no_mangle]
pub static mut BgWriterDelay: c_int = 200;

/*
 * Multiplier to apply to BgWriterDelay when we decide to hibernate.
 * (Perhaps this needs to be configurable?)
 */
const HIBERNATE_FACTOR: c_int = 50;

/*
 * Interval in which standby snapshots are logged into the WAL stream, in
 * milliseconds.
 */
const LOG_SNAPSHOT_INTERVAL_MS: c_int = 15000;

/*
 * LSN and timestamp at which we last issued a LogStandbySnapshot(), to avoid
 * doing so too often or repeatedly if there has been no other write activity
 * in the system.
 */
static mut last_snapshot_ts: TimestampTz = 0;
static mut last_snapshot_lsn: XLogRecPtr = InvalidXLogRecPtr;

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported dependencies.
// ---------------------------------------------------------------------------

// SIG_IGN is the function pointer with value 1; pqsignal.rs models SIG_DFL as
// None and leaves SIG_IGN to callers. We construct it here as a fn-ptr from 1.
#[inline]
fn SIG_IGN() -> SigHandler {
    Some(unsafe { core::mem::transmute::<usize, unsafe extern "C" fn(c_int)>(1usize) })
}

// Wait-event flags (latch.h). TODO: import from ported latch.c.
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 2;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

// Wait event ids (wait_event.h). TODO: import from generated wait events.
const WAIT_EVENT_BGWRITER_MAIN: u32 = 0;
const WAIT_EVENT_BGWRITER_HIBERNATE: u32 = 0;

// SIG_SETMASK (signal.h). TODO: centralize.
const SIG_SETMASK: c_int = if cfg!(target_os = "macos") { 3 } else { 2 };

// error_context_stack / PG_exception_stack (elog.c). TODO: import once elog.c
// is ported. Modeled as raw pointers as in C.
static mut error_context_stack: *mut c_void = null_mut();
static mut PG_exception_stack: *mut c_void = null_mut();

// bgwriter_flush_after GUC (bufmgr.h, defined in bufmgr.c). Default 512KB /
// BLCKSZ = 64 blocks.  TODO: import once bufmgr.c GUC is ported.
static mut bgwriter_flush_after: c_int = 64;

// procsignal_sigusr1_handler (procsignal.c). TODO.
unsafe extern "C" fn procsignal_sigusr1_handler(_postgres_signal_arg: c_int) {
    /* TODO: not ported */
}

// EmitErrorReport / FlushErrorState (elog.c). TODO.
unsafe fn EmitErrorReport() {
    /* TODO: not ported */
}
unsafe fn FlushErrorState() {
    /* TODO: not ported */
}

// AbortTransaction-subset cleanup helpers. All TODO (not yet ported).
unsafe fn LWLockReleaseAll() {
    /* TODO: not ported */
}
unsafe fn ConditionVariableCancelSleep() {
    /* TODO: not ported */
}
unsafe fn pgstat_report_wait_end() {
    /* TODO: not ported */
}
unsafe fn pgaio_error_cleanup() {
    /* TODO: not ported */
}
unsafe fn UnlockBuffers() {
    /* TODO: not ported */
}
unsafe fn ReleaseAuxProcessResources(_isCommit: bool) {
    /* TODO: not ported */
}
unsafe fn AtEOXact_Buffers(_isCommit: bool) {
    /* TODO: not ported */
}
unsafe fn AtEOXact_SMgr() {
    /* TODO: not ported */
}
unsafe fn AtEOXact_Files(_isCommit: bool) {
    /* TODO: not ported */
}
unsafe fn AtEOXact_HashTables(_isCommit: bool) {
    /* TODO: not ported */
}

// pgstat_report_wal (pgstat_wal.c). TODO.
unsafe fn pgstat_report_wal(_force: bool) {
    /* TODO: not ported */
}

// BgBufferSync (bufmgr.c). Returns whether the bgwriter may hibernate. TODO.
unsafe fn BgBufferSync(_wb_context: *mut WritebackContext) -> bool {
    /* TODO: not ported */
    false
}

// FirstCallSinceLastCheckpoint (checkpointer.c). TODO.
unsafe fn FirstCallSinceLastCheckpoint() -> bool {
    /* TODO: not ported */
    false
}

// smgrdestroyall (smgr.c). TODO.
unsafe fn smgrdestroyall() {
    /* TODO: not ported */
}

// XLogStandbyInfoActive (xlog.h). TODO.
unsafe fn XLogStandbyInfoActive() -> bool {
    /* TODO: not ported */
    false
}

// RecoveryInProgress (xlog.c). TODO.
unsafe fn RecoveryInProgress() -> bool {
    /* TODO: not ported */
    false
}

// GetLastImportantRecPtr (xlog.c). TODO.
unsafe fn GetLastImportantRecPtr() -> XLogRecPtr {
    /* TODO: not ported */
    InvalidXLogRecPtr
}

// LogStandbySnapshot (standby.c). TODO.
unsafe fn LogStandbySnapshot() -> XLogRecPtr {
    /* TODO: not ported */
    InvalidXLogRecPtr
}

// TimestampTzPlusMilliseconds (timestamp.h). msec is added directly to a
// TimestampTz, scaled to microseconds.
#[inline]
unsafe fn TimestampTzPlusMilliseconds(tz: TimestampTz, ms: c_int) -> TimestampTz {
    tz + (ms as TimestampTz) * 1000
}

// Latch operations (latch.c). TODO.
unsafe fn ResetLatch(_latch: *mut Latch) {
    /* TODO: not ported */
}
unsafe fn WaitLatch(
    _latch: *mut Latch,
    _wakeEvents: c_int,
    _timeout: c_long,
    _wait_event_info: u32,
) -> c_int {
    /* TODO: not ported */
    0
}

// sigjmp_buf: a fixed-size opaque buffer matching the platform's jmp_buf. We do
// not actually perform a setjmp here (elog.c machinery not ported); the loop's
// recovery path is preserved structurally for a faithful translation.
// TODO: wire to real sigsetjmp once elog.c is ported.
type sigjmp_buf = [c_void; 0];

// sigsetjmp stub: returns 0 (the "set up" return). TODO.
unsafe fn sigsetjmp(_env: *mut sigjmp_buf, _savemask: c_int) -> c_int {
    /* TODO: not ported */
    0
}

/*
 * Main entry point for bgwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
pub unsafe fn BackgroundWriterMain(_startup_data: *const c_void, startup_data_len: Size) {
    let mut local_sigjmp_buf: sigjmp_buf = [];
    let bgwriter_context: MemoryContext;
    let mut prev_hibernate: bool;
    let mut wb_context: WritebackContext = core::mem::zeroed();

    Assert!(startup_data_len == 0);

    MyBackendType = B_BG_WRITER;
    AuxiliaryProcessMainCommon();

    /*
     * Properly accept or ignore signals that might be sent to us.
     */
    pqsignal(SIGHUP, Some(SignalHandlerForConfigReload));
    pqsignal(SIGINT, SIG_IGN());
    pqsignal(SIGTERM, Some(SignalHandlerForShutdownRequest));
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGALRM, SIG_IGN());
    pqsignal(SIGPIPE, SIG_IGN());
    pqsignal(SIGUSR1, Some(procsignal_sigusr1_handler));
    pqsignal(SIGUSR2, SIG_IGN());

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    pqsignal(SIGCHLD, SIG_DFL);

    /*
     * We just started, assume there has been either a shutdown or
     * end-of-recovery snapshot.
     */
    last_snapshot_ts = GetCurrentTimestamp();

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in TopMemoryContext,
     * but resetting that would be a really bad idea.
     */
    bgwriter_context = AllocSetContextCreate!(
        TopMemoryContext,
        c"Background Writer".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    MemoryContextSwitchTo(bgwriter_context);

    WritebackContextInit(&raw mut wb_context, &raw mut bgwriter_flush_after);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See the C original for the rationale of keeping the outermost setjmp
     * always active rather than using PG_TRY.
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
         * about in bgwriter, but we do have LWLocks, buffers, and temp files.
         */
        LWLockReleaseAll();
        ConditionVariableCancelSleep();
        pgaio_error_cleanup();
        UnlockBuffers();
        ReleaseAuxProcessResources(false);
        AtEOXact_Buffers(false);
        AtEOXact_SMgr();
        AtEOXact_Files(false);
        AtEOXact_HashTables(false);

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(bgwriter_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextReset(bgwriter_context);

        /* re-initialize to avoid repeated errors causing problems */
        WritebackContextInit(&raw mut wb_context, &raw mut bgwriter_flush_after);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely to
         * be repeated, and we don't want to be filling the error logs as fast
         * as we can.
         */
        pg_usleep(1000000 as c_long);

        /* Report wait end here, when there is no further possibility of wait */
        pgstat_report_wait_end();
    }

    /* We can now handle ereport(ERROR) */
    PG_exception_stack = (&raw mut local_sigjmp_buf) as *mut c_void;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    sigprocmask(SIG_SETMASK, &raw const UnBlockSig, null_mut::<sigset_t>());

    /*
     * Reset hibernation state after any error.
     */
    prev_hibernate = false;

    /*
     * Loop forever
     */
    loop {
        let can_hibernate: bool;
        let rc: c_int;

        /* Clear any already-pending wakeups */
        ResetLatch(MyLatch);

        ProcessMainLoopInterrupts();

        /*
         * Do one cycle of dirty-buffer writing.
         */
        can_hibernate = BgBufferSync(&raw mut wb_context);

        /* Report pending statistics to the cumulative stats system */
        pgstat_report_bgwriter();
        pgstat_report_wal(true);

        if FirstCallSinceLastCheckpoint() {
            /*
             * After any checkpoint, free all smgr objects.  Otherwise we would
             * never do so for dropped relations, as the bgwriter does not
             * process shared invalidation messages or call AtEOXact_SMgr().
             */
            smgrdestroyall();
        }

        /*
         * Log a new xl_running_xacts every now and then so replication can get
         * into a consistent state faster and clean up resources more
         * frequently.  See the C original for the full rationale.
         */
        if XLogStandbyInfoActive() && !RecoveryInProgress() {
            let timeout: TimestampTz;
            let now: TimestampTz = GetCurrentTimestamp();

            timeout = TimestampTzPlusMilliseconds(last_snapshot_ts, LOG_SNAPSHOT_INTERVAL_MS);

            /*
             * Only log if enough time has passed and interesting records have
             * been inserted since the last snapshot.  Have to compare with <=
             * instead of < because GetLastImportantRecPtr() points at the
             * start of a record, whereas last_snapshot_lsn points just past
             * the end of the record.
             */
            if now >= timeout && last_snapshot_lsn <= GetLastImportantRecPtr() {
                last_snapshot_lsn = LogStandbySnapshot();
                last_snapshot_ts = now;
            }
        }

        /*
         * Sleep until we are signaled or BgWriterDelay has elapsed.
         *
         * Note: the feedback control loop in BgBufferSync() expects that we
         * will call it every BgWriterDelay msec.
         */
        rc = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            BgWriterDelay as c_long, /* ms */
            WAIT_EVENT_BGWRITER_MAIN,
        );

        /*
         * If no latch event and BgBufferSync says nothing's happening, extend
         * the sleep in "hibernation" mode, where we sleep for much longer than
         * bgwriter_delay says.  See the C original for the full rationale and
         * the race-condition discussion.
         */
        if rc == WL_TIMEOUT && can_hibernate && prev_hibernate {
            /* Ask for notification at next buffer allocation */
            StrategyNotifyBgWriter(MyProcNumber);
            /* Sleep ... */
            let _ = WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                (BgWriterDelay * HIBERNATE_FACTOR) as c_long,
                WAIT_EVENT_BGWRITER_HIBERNATE,
            );
            /* Reset the notification request in case we timed out */
            StrategyNotifyBgWriter(-1);
        }

        prev_hibernate = can_hibernate;
    }
}
