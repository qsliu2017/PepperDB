//! postmaster/walwriter.c - WAL writer background process main loop.
//!
//! The WAL writer background process attempts to keep regular backends from
//! having to write out (and fsync) WAL pages, and guarantees that async-commit
//! transaction commit records reach disk within a knowable time.

use crate::prelude::*;
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

use crate::libpq::pqsignal::{
    pqsignal, sigset_t, SigHandler, UnBlockSig, SIGALRM, SIGCHLD, SIGHUP, SIGINT, SIGPIPE, SIGTERM,
    SIGUSR1, SIGUSR2, SIG_DFL,
};
use crate::miscadmin::{HOLD_INTERRUPTS, Latch, MyLatch, RESUME_INTERRUPTS, B_WAL_WRITER};
use crate::port::pgsleep::pg_usleep;
use crate::postmaster::auxprocess::AuxiliaryProcessMainCommon;
use crate::postmaster::interrupt::{
    ProcessMainLoopInterrupts, SignalHandlerForConfigReload, SignalHandlerForShutdownRequest,
};
use crate::storage::procnumber::MyProcNumber;
// MemoryContextReset comes from the prelude.

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
pub static mut WalWriterDelay: c_int = 200;
#[no_mangle]
pub static mut WalWriterFlushAfter: c_int = DEFAULT_WAL_WRITER_FLUSH_AFTER;

/* Default value for WalWriterFlushAfter (walwriter.h): 1MB / XLOG_BLCKSZ */
const DEFAULT_WAL_WRITER_FLUSH_AFTER: c_int = (1024 * 1024) / 8192;

/*
 * Number of do-nothing loops before lengthening the delay time, and the
 * multiplier to apply to WalWriterDelay when we do decide to hibernate.
 * (Perhaps these need to be configurable?)
 */
const LOOPS_UNTIL_HIBERNATE: c_int = 50;
const HIBERNATE_FACTOR: c_int = 25;

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported dependencies.
// ---------------------------------------------------------------------------

// SIG_IGN is the function pointer with value 1; pqsignal.rs models SIG_DFL as
// None and leaves SIG_IGN to callers. We construct it here as a fn-ptr from 1.
#[inline]
fn SIG_IGN() -> SigHandler {
    // SIG_IGN is the platform handler with value 1; build it at runtime (a const
    // fn-pointer from an integer fails const-eval validation).
    Some(unsafe { core::mem::transmute::<usize, unsafe extern "C" fn(c_int)>(1usize) })
}

// Wait-event flags (latch.h). TODO: import from ported latch.c.
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 2;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

// Wait event id (wait_event.h). TODO: import from generated wait events.
const WAIT_EVENT_WAL_WRITER_MAIN: u32 = 0;

// SIG_SETMASK (signal.h). TODO: centralize.
const SIG_SETMASK: c_int = if cfg!(target_os = "macos") { 3 } else { 2 };

// error_context_stack / PG_exception_stack (elog.c). TODO: import once elog.c
// is ported. Modeled as raw pointers as in C.
static mut error_context_stack: *mut c_void = null_mut();
static mut PG_exception_stack: *mut c_void = null_mut();

// procsignal_sigusr1_handler (procsignal.c). TODO.
unsafe extern "C" fn procsignal_sigusr1_handler(_postgres_signal_arg: c_int) {
    /* TODO: not ported */
}

// XLogBackgroundFlush (xlog.c). TODO.
unsafe fn XLogBackgroundFlush() -> bool {
    /* TODO: not ported */
    false
}

// SetWalWriterSleeping (xlog.c). TODO.
unsafe fn SetWalWriterSleeping(_sleeping: bool) {
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

// AllocSetContextCreate-produced context type is MemoryContext (from prelude).
// MemoryContextSwitchTo / TopMemoryContext / AllocSetContextCreate are in prelude.

// ProcGlobal (proc.h): PROC_HDR with walwriterProc. TODO: import once proc.c
// is ported. Modeled as a single global record with the field we touch.
#[repr(C)]
struct PROC_HDR {
    walwriterProc: ProcNumber,
}
static mut ProcGlobal_storage: PROC_HDR = PROC_HDR {
    walwriterProc: INVALID_PROC_NUMBER,
};
// In C, ProcGlobal is a pointer; mirror via a static pointer into our storage.
extern "C" { pub static mut ProcGlobal: *mut PROC_HDR; }
// sigjmp_buf: a fixed-size opaque buffer matching the platform's jmp_buf. We do
// not actually perform a setjmp here (elog.c machinery not ported); the loop's
// recovery path is preserved structurally for a faithful translation. The
// branch is taken when sigsetjmp returns nonzero, which never happens in this
// stub (returns 0). TODO: wire to real sigsetjmp once elog.c is ported.
type sigjmp_buf = [c_void; 0];

// sigsetjmp stub: returns 0 (the "set up" return). TODO.
unsafe fn sigsetjmp(_env: *mut sigjmp_buf, _savemask: c_int) -> c_int {
    /* TODO: not ported */
    0
}

/*
 * Main entry point for walwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
pub unsafe fn WalWriterMain(_startup_data: *const c_void, startup_data_len: Size) {
    let mut local_sigjmp_buf: sigjmp_buf = [];
    let walwriter_context: MemoryContext;
    let mut left_till_hibernate: c_int;
    let mut hibernating: bool;

    Assert!(startup_data_len == 0);

    MyBackendType = B_WAL_WRITER;
    AuxiliaryProcessMainCommon();

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * We have no particular use for SIGINT at the moment, but seems reasonable
     * to treat like SIGTERM.
     */
    pqsignal(SIGHUP, Some(SignalHandlerForConfigReload));
    pqsignal(SIGINT, Some(SignalHandlerForShutdownRequest));
    pqsignal(SIGTERM, Some(SignalHandlerForShutdownRequest));
    /* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGALRM, SIG_IGN());
    pqsignal(SIGPIPE, SIG_IGN());
    pqsignal(SIGUSR1, Some(procsignal_sigusr1_handler));
    pqsignal(SIGUSR2, SIG_IGN()); /* not used */

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    pqsignal(SIGCHLD, SIG_DFL);

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in TopMemoryContext,
     * but resetting that would be a really bad idea.
     */
    walwriter_context = AllocSetContextCreate!(
        TopMemoryContext,
        c"Wal Writer".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    MemoryContextSwitchTo(walwriter_context);

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
         * about in walwriter, but we do have LWLocks, and perhaps buffers?
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

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(walwriter_context);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextReset(walwriter_context);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /*
         * Sleep at least 1 second after any error.  A write error is likely to
         * be repeated, and we don't want to be filling the error logs as fast
         * as we can.
         */
        pg_usleep(1000000 as c_long);
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
    left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
    hibernating = false;
    SetWalWriterSleeping(false);

    /*
     * Advertise our proc number that backends can use to wake us up while we're
     * sleeping.
     */
    // local PROC_HDR stub field offset != canonical; write through canonical layout.
    (*(ProcGlobal as *mut crate::storage::lmgr::proc::PROC_HDR)).walwriterProc = MyProcNumber as _;

    /*
     * Loop forever
     */
    loop {
        let cur_timeout: c_long;

        /*
         * Advertise whether we might hibernate in this cycle.  We do this
         * before resetting the latch to ensure that any async commits will see
         * the flag set if they might possibly need to wake us up, and that we
         * won't miss any signal they send us.  But avoid touching the global
         * flag if it doesn't need to change.
         */
        if hibernating != (left_till_hibernate <= 1) {
            hibernating = left_till_hibernate <= 1;
            SetWalWriterSleeping(hibernating);
        }

        /* Clear any already-pending wakeups */
        ResetLatch(MyLatch);

        /* Process any signals received recently */
        ProcessMainLoopInterrupts();

        /*
         * Do what we're here for; then, if XLogBackgroundFlush() found useful
         * work to do, reset hibernation counter.
         */
        if XLogBackgroundFlush() {
            left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
        } else if left_till_hibernate > 0 {
            left_till_hibernate -= 1;
        }

        /* report pending statistics to the cumulative stats system */
        pgstat_report_wal(false);

        /*
         * Sleep until we are signaled or WalWriterDelay has elapsed.  If we
         * haven't done anything useful for quite some time, lengthen the sleep
         * time so as to reduce the server's idle power consumption.
         */
        if left_till_hibernate > 0 {
            cur_timeout = WalWriterDelay as c_long; /* in ms */
        } else {
            cur_timeout = (WalWriterDelay * HIBERNATE_FACTOR) as c_long;
        }

        let _ = WaitLatch(
            MyLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            cur_timeout,
            WAIT_EVENT_WAL_WRITER_MAIN,
        );
    }
}
