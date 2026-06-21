//! Translation of postgres/src/backend/postmaster/interrupt.c
//! (with declarations from src/include/postmaster/interrupt.h)
//!
//! Interrupt handling routines.  Shared interrupt/signal-flag handling for the
//! postmaster and auxiliary processes.  Responses to interrupts are fairly
//! varied and many types of backends have their own implementations, but a few
//! generic things live here to facilitate code reuse.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

// In C these are `volatile sig_atomic_t` process globals declared
// `PGDLLIMPORT`.  Per the porting convention a process-global C static maps to
// a Rust `static mut`, read/written only under `unsafe`.  The flags are pure
// booleans (set in a signal handler, polled in the main loop).

/// Set by SIGHUP handler; polled to trigger a configuration reload.
#[no_mangle]
pub static mut ConfigReloadPending: bool = false;

/// Set by the shutdown signal handler; polled to trigger process exit.
pub static mut ShutdownRequestPending: bool = false;

// ---------------------------------------------------------------------------
// Stubs for unported dependencies.  Each is noted; replace when the owning
// module is translated.
// ---------------------------------------------------------------------------

/// STUB (unported: storage/latch).  In C this is the process-local latch
/// `MyLatch`; the signal handlers SetLatch(MyLatch) to wake the main loop.
/// Modeled here as a unit value so SetLatch has something to take.
static mut MyLatch: () = ();

/// STUB (unported: storage/latch).  No-op; real SetLatch wakes a waiting
/// process via its latch.
unsafe fn SetLatch(_latch: *mut ()) {
    /* TODO: storage/latch not yet ported */
}

/// STUB (unported: utils/guc).  GUC context value passed to ProcessConfigFile
/// when reloading on SIGHUP.
const PGC_SIGHUP: c_int = 0;

/// STUB (unported: utils/guc).  No-op; real ProcessConfigFile re-reads
/// postgresql.conf and applies SIGHUP-reloadable settings.
unsafe fn ProcessConfigFile(_context: c_int) {
    /* TODO: utils/guc not yet ported */
}

/// STUB (unported: storage/ipc).  No-op (kept non-terminating so it stays
/// testable); real proc_exit runs before-shmem-exit / on-proc-exit callbacks
/// then exits the process.
unsafe fn proc_exit(_code: c_int) {
    /* TODO: storage/ipc not yet ported */
}

/// STUB (unported: storage/procsignal).  In C `ProcSignalBarrierPending` is a
/// volatile flag polled in the main loop.
static mut ProcSignalBarrierPending: bool = false;

/// STUB (unported: storage/procsignal).  No-op.
unsafe fn ProcessProcSignalBarrier() {
    /* TODO: storage/procsignal not yet ported */
}

/// STUB (unported: miscadmin).  Volatile flag set when a log-memory-contexts
/// request is pending.
static mut LogMemoryContextPending: bool = false;

/// STUB (unported: utils/mmgr).  No-op; real handler logs this process's memory
/// contexts.
unsafe fn ProcessLogMemoryContextInterrupt() {
    /* TODO: utils/mmgr not yet ported */
}

// ---------------------------------------------------------------------------
// Interrupt handling.
// ---------------------------------------------------------------------------

/// Simple interrupt handler for main loops of background processes.
pub unsafe fn ProcessMainLoopInterrupts() {
    if ProcSignalBarrierPending {
        ProcessProcSignalBarrier();
    }

    if ConfigReloadPending {
        ConfigReloadPending = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if ShutdownRequestPending {
        proc_exit(0);
    }

    /* Perform logging of memory contexts of this process */
    if LogMemoryContextPending {
        ProcessLogMemoryContextInterrupt();
    }
}

/// Simple signal handler for triggering a configuration reload.
///
/// Normally, this handler would be used for SIGHUP. The idea is that code which
/// uses it would arrange to check the ConfigReloadPending flag at convenient
/// places inside main loops, or else call ProcessMainLoopInterrupts.
///
/// C `SIGNAL_ARGS` expands to `int postgres_signal_arg`; the handler keeps the
/// C ABI so it can be installed as an OS signal handler.
///
/// errno save/restore (C does `int save_errno = errno; ...; errno = save_errno`)
/// is omitted here: errno is not yet touched by the no-op SetLatch stub.
#[no_mangle]
pub unsafe extern "C" fn SignalHandlerForConfigReload(_postgres_signal_arg: c_int) {
    ConfigReloadPending = true;
    SetLatch(&raw mut MyLatch);
}

/// Simple signal handler for exiting quickly as if due to a crash.
///
/// Normally, this would be used for handling SIGQUIT.
///
/// We DO NOT want to run proc_exit() or atexit() callbacks -- we're here because
/// shared memory may be corrupted, so we don't want to try to clean up our
/// transaction.  Just nail the windows shut and get out of town.
///
/// Note we do _exit(2) not _exit(0).  This forces the postmaster into a system
/// reset cycle if someone sends a manual SIGQUIT to a random backend.
pub unsafe extern "C" fn SignalHandlerForCrashExit(_postgres_signal_arg: c_int) {
    extern "C" {
        fn _exit(status: c_int) -> !;
    }
    _exit(2);
}

/// Simple signal handler for triggering a long-running background process to
/// shut down and exit.
///
/// Typically used for SIGTERM, but some processes use other signals. In
/// particular, the checkpointer and parallel apply worker exit on SIGUSR2, and
/// the WAL writer exits on either SIGINT or SIGTERM.
///
/// ShutdownRequestPending should be checked at a convenient place within the
/// main loop, or else the main loop should call ProcessMainLoopInterrupts.
///
/// errno save/restore omitted (see SignalHandlerForConfigReload).
pub unsafe extern "C" fn SignalHandlerForShutdownRequest(_postgres_signal_arg: c_int) {
    ShutdownRequestPending = true;
    SetLatch(&raw mut MyLatch);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Serialize all tests that touch the process-global static mut flags.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn config_reload_handler_sets_flag() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            ConfigReloadPending = false;
            SignalHandlerForConfigReload(1 /* SIGHUP; arg is ignored */);
            assert!(ConfigReloadPending);
            ConfigReloadPending = false;
        }
    }

    #[test]
    fn shutdown_handler_sets_flag() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            ShutdownRequestPending = false;
            SignalHandlerForShutdownRequest(15 /* SIGTERM; arg is ignored */);
            assert!(ShutdownRequestPending);
            ShutdownRequestPending = false;
        }
    }

    #[test]
    fn process_main_loop_clears_config_reload() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            ConfigReloadPending = true;
            ShutdownRequestPending = false;
            ProcessMainLoopInterrupts();
            // ProcessConfigFile is a no-op stub, but the flag must be cleared.
            assert!(!ConfigReloadPending);
        }
    }
}
