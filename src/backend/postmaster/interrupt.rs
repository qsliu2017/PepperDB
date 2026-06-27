//! Translated from PostgreSQL src/backend/postmaster/interrupt.c
//!
//! Interrupt handling for the main loops of long-running (auxiliary) tasks.
//! PG's `volatile sig_atomic_t` flags set from OS signal handlers become process
//! atomics fed by tokio::signal tasks; `SetLatch(MyLatch)` becomes a latch wake.
//!
//! Non-type-centric: these are free functions over the flags, so the header
//! `src/postmaster/interrupt.rs` re-exports them (`pub use`) rather than holding
//! deprecated shims.

use std::sync::atomic::Ordering;

use crate::postmaster::interrupt::{CONFIG_RELOAD_PENDING, SHUTDOWN_REQUEST_PENDING};
use crate::storage::latch::Latch;

// TODO(procsignal): these are still process-global. PG sets them per-aux-process;
// the per-task home is the ProcSignalSlot. Keep global for now -- aux tasks are
// not yet multiplexed.

/// PG's `ProcessMainLoopInterrupts`. Checks the config-reload and
/// shutdown-request flags and acts. Minimal: the config-reload and
/// memory-context hooks are TODOs; shutdown signals the caller to exit.
///
/// Returns `true` if a shutdown was requested (the caller should break its loop
/// and begin graceful exit) -- PG calls `proc_exit(0)` here, but under the async
/// model exit is supervisor-driven, so we surface the request instead.
#[must_use]
pub fn process_main_loop_interrupts() -> bool {
    // TODO(procsignal): if ProcSignalBarrierPending -> ProcessProcSignalBarrier().

    if CONFIG_RELOAD_PENDING.swap(false, Ordering::AcqRel) {
        // TODO: ProcessConfigFile(PGC_SIGHUP) -- GUC reload not yet ported.
    }

    if SHUTDOWN_REQUEST_PENDING.load(Ordering::Acquire) {
        return true;
    }

    // TODO(pgstat): if LogMemoryContextPending -> ProcessLogMemoryContextInterrupt().
    false
}

/// PG's `SignalHandlerForConfigReload` (SIGHUP). Sets the config-reload flag and
/// wakes `latch`. Synchronous; callable from a signal-listener task.
pub fn signal_handler_for_config_reload(latch: &Latch) {
    CONFIG_RELOAD_PENDING.store(true, Ordering::Release);
    latch.set();
}

/// PG's `SignalHandlerForShutdownRequest` (SIGTERM). Sets the shutdown flag and
/// wakes `latch`.
pub fn signal_handler_for_shutdown_request(latch: &Latch) {
    SHUTDOWN_REQUEST_PENDING.store(true, Ordering::Release);
    latch.set();
}

/// PG's `SignalHandlerForCrashExit` (SIGQUIT). Exit immediately without running
/// any Drop/atexit cleanup -- shared state may be corrupt. PG does `_exit(2)`;
/// `process::abort` is the closest no-cleanup, non-zero-status equivalent.
pub fn signal_handler_for_crash_exit() -> ! {
    std::process::abort()
}

/// PROVISIONAL. Spawn a task that translates OS signals into the interrupt
/// flags, waking `latch`: SIGHUP -> config reload, SIGTERM -> shutdown,
/// SIGQUIT -> crash exit. The supervisor (step 09) calls this once at startup;
/// nothing is spawned at import time. Returns the spawned task's handle.
#[cfg(unix)]
pub fn install_signal_handlers(latch: std::sync::Arc<Latch>) -> tokio::task::JoinHandle<()> {
    use tokio::signal::unix::{SignalKind, signal};
    tokio::spawn(async move {
        let mut sighup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigquit = signal(SignalKind::quit()).expect("install SIGQUIT handler");
        loop {
            tokio::select! {
                _ = sighup.recv() => signal_handler_for_config_reload(&latch),
                _ = sigterm.recv() => signal_handler_for_shutdown_request(&latch),
                _ = sigquit.recv() => signal_handler_for_crash_exit(),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    // Flags are process-global; keep these serialized within one test to avoid
    // cross-test interference.
    #[tokio::test]
    async fn config_reload_then_shutdown_flow() {
        let latch = Arc::new(Latch::new());
        CONFIG_RELOAD_PENDING.store(false, Ordering::Release);
        SHUTDOWN_REQUEST_PENDING.store(false, Ordering::Release);

        signal_handler_for_config_reload(&latch);
        assert!(CONFIG_RELOAD_PENDING.load(Ordering::Acquire));
        // latch was set by the handler.
        tokio::time::timeout(Duration::from_secs(1), latch.wait())
            .await
            .expect("config-reload should have set the latch");

        // process_main_loop_interrupts clears config-reload and reports no shutdown.
        assert!(!process_main_loop_interrupts());
        assert!(!CONFIG_RELOAD_PENDING.load(Ordering::Acquire));

        signal_handler_for_shutdown_request(&latch);
        assert!(process_main_loop_interrupts(), "shutdown should be reported");

        SHUTDOWN_REQUEST_PENDING.store(false, Ordering::Release);
    }
}
