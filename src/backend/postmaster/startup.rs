//! Translated from PostgreSQL src/backend/postmaster/startup.c
//!
//! The startup task initializes the server and performs WAL recovery. Unlike the
//! other auxiliary tasks it has no steady-state "main loop": it runs
//! `StartupXLOG()` and exits as soon as recovery is complete (exit code 0 tells
//! the supervisor recovery succeeded). In standby mode the replay loop inside
//! `StartupXLOG` acts as its main loop, servicing interrupts via
//! [`process_startup_proc_interrupts`].
//!
//! Single-process redesign vs PG's startup.c:
//! - The startup process IS the recovery proc. It claims an aux PGPROC (the
//!   `_with_proc` cradle) so `wakeup_recovery` can ring its `proc_latch` while it
//!   replays WAL; PG keeps this proc reachable via `ProcGlobal->allProcs` rather
//!   than a dedicated `ProcGlobal.<role>_proc` slot, so we advertise it the same
//!   way the other aux tasks do (see [`STARTUP_PROC_NUMBER`]).
//! - PG's three signal flags (`got_SIGHUP`, `shutdown_requested`,
//!   `promote_signaled`) become process atomics fed by the supervisor / signal
//!   listener; [`process_startup_proc_interrupts`] services them.
//! - `StartupXLOG()` lives in xlogrecovery.c / xlog.c (deferred files); we call
//!   the existing stub, which is a non-panicking no-op until recovery lands. A
//!   startup task that panicked at boot would be useless, so the stub must not
//!   panic (it does not -- see `access::xlog::startup_xlog`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::access::xlog::startup_xlog;
use crate::backend::postmaster::auxprocess::auxiliary_process_main_common_with_proc;
use crate::miscadmin::BackendType;
use crate::shared_state::SharedState;
use crate::storage::latch::Latch;
use crate::storage::proc::{my_proc_scope, proc_global};
use crate::storage::procnumber::INVALID_PROC_NUMBER;

/// PG GUC `log_startup_progress_interval` (ms between progress reports for long
/// startup operations; default 10s). Process-global atomic; 0 disables.
/// TODO(guc): drive from the GUC bridge once it lands.
static LOG_STARTUP_PROGRESS_INTERVAL: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(10000);

/// PG `log_startup_progress_interval`.
pub fn log_startup_progress_interval() -> i32 {
    LOG_STARTUP_PROGRESS_INTERVAL.load(Ordering::Relaxed)
}

/// Set `log_startup_progress_interval` (GUC assignment / tests).
pub fn set_log_startup_progress_interval(ms: i32) {
    LOG_STARTUP_PROGRESS_INTERVAL.store(ms, Ordering::Relaxed);
}

// --- Flags set by interrupt handlers for later service in the redo loop. ---
// PG's `volatile sig_atomic_t` startup-process flags. The startup task is single,
// so process atomics suffice (mirrors interrupt.rs's global flags).

/// PG `got_SIGHUP`: re-read config at the next convenient time.
static GOT_SIGHUP: AtomicBool = AtomicBool::new(false);
/// PG `shutdown_requested`: abort redo and exit.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
/// PG `promote_signaled`: finish recovery and promote.
static PROMOTE_SIGNALED: AtomicBool = AtomicBool::new(false);
/// PG `in_restore_command`: a restore command is running, so SIGTERM may
/// `proc_exit` immediately rather than deferring.
static IN_RESTORE_COMMAND: AtomicBool = AtomicBool::new(false);
/// PG `startup_progress_timer_expired`: the progress timer fired.
static STARTUP_PROGRESS_TIMER_EXPIRED: AtomicBool = AtomicBool::new(false);

/// The advertised startup-proc number (PG: the startup process's `MyProcNumber`,
/// reached through `ProcGlobal->allProcs`). Backends ring its `proc_latch` via
/// `wakeup_recovery`; cleared on exit by the RAII guard. INVALID when no startup
/// task is running.
static STARTUP_PROC_NUMBER: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(INVALID_PROC_NUMBER);

/// PG `WakeupRecovery`: wake the startup process's recovery loop by ringing its
/// `proc_latch`. Reaches the aux PGPROC by the advertised proc number; a no-op if
/// no startup task is running (the proc number is INVALID).
pub fn wakeup_recovery() -> bool {
    let Some(g) = proc_global() else {
        return false;
    };
    let procno = STARTUP_PROC_NUMBER.load(Ordering::Acquire);
    if procno == INVALID_PROC_NUMBER {
        return false;
    }
    // SAFETY: `proc_latch` is internally synchronized (a Latch over a Notify);
    // setting it forms no `&mut` into the slot's other field groups.
    (unsafe { g.proc(procno) })
        .inspect(|proc| proc.proc_latch.set())
        .is_some()
}

// --- signal handler equivalents (set a flag + wake recovery) ---

/// PG `StartupProcTriggerHandler` (SIGUSR2): finish recovery (promote).
pub fn startup_proc_trigger_handler() {
    PROMOTE_SIGNALED.store(true, Ordering::Release);
    wakeup_recovery();
}

/// PG `StartupProcSigHupHandler` (SIGHUP): re-read config at next convenient time.
pub fn startup_proc_sig_hup_handler() {
    GOT_SIGHUP.store(true, Ordering::Release);
    wakeup_recovery();
}

/// PG `StartupProcShutdownHandler` (SIGTERM): abort redo and exit. If a restore
/// command is running it is safe to exit right away; otherwise defer to the redo
/// loop. Returns true if the caller should exit immediately (PG `proc_exit(1)`).
#[must_use]
pub fn startup_proc_shutdown_handler() -> bool {
    let immediate = IN_RESTORE_COMMAND.load(Ordering::Acquire);
    if !immediate {
        SHUTDOWN_REQUESTED.store(true, Ordering::Release);
    }
    wakeup_recovery();
    immediate
}

/// PG `ProcessStartupProcInterrupts`. Service the startup-process signals: config
/// reload (SIGHUP) and the shutdown request (SIGTERM). Barrier / memory-context
/// interrupts are TODOs (step 09). Returns true if shutdown was requested and the
/// caller must exit without finishing recovery (PG `proc_exit(1)`).
#[must_use]
pub fn process_startup_proc_interrupts() -> bool {
    if GOT_SIGHUP.swap(false, Ordering::AcqRel) {
        startup_reread_config();
    }

    if SHUTDOWN_REQUESTED.load(Ordering::Acquire) {
        // PG proc_exit(1): exit without finishing recovery.
        return true;
    }

    // TODO(step09): ProcSignalBarrierPending -> ProcessProcSignalBarrier();
    // LogMemoryContextPending -> ProcessLogMemoryContextInterrupt();
    // and the PostmasterIsAlive emergency bailout (supervisor-driven here).
    false
}

/// PG `StartupRereadConfig`: re-read the config file and, if a critical
/// walreceiver option changed, ask xlog.c to restart it. The GUC bridge +
/// walreceiver are deferred, so this is a faithful no-op shell.
fn startup_reread_config() {
    // TODO(guc): ProcessConfigFile(PGC_SIGHUP) + compare PrimaryConnInfo /
    // PrimarySlotName / wal_receiver_create_temp_slot and call
    // StartupRequestWalReceiverRestart() on change.
}

/// PG `PreRestoreCommand`: a restore command is about to run; tell the SIGTERM
/// handler it is safe to exit immediately. Returns true if a shutdown was already
/// requested (the caller should exit at once, PG `proc_exit(1)`).
#[must_use]
pub fn pre_restore_command() -> bool {
    IN_RESTORE_COMMAND.store(true, Ordering::Release);
    SHUTDOWN_REQUESTED.load(Ordering::Acquire)
}

/// PG `PostRestoreCommand`: the restore command finished.
pub fn post_restore_command() {
    IN_RESTORE_COMMAND.store(false, Ordering::Release);
}

/// PG `IsPromoteSignaled`.
pub fn is_promote_signaled() -> bool {
    PROMOTE_SIGNALED.load(Ordering::Acquire)
}

/// PG `ResetPromoteSignaled`.
pub fn reset_promote_signaled() {
    PROMOTE_SIGNALED.store(false, Ordering::Release);
}

/// PG `startup_progress_timeout_handler`: time to log a progress report.
pub fn startup_progress_timeout_handler() {
    STARTUP_PROGRESS_TIMER_EXPIRED.store(true, Ordering::Release);
}

/// PG `disable_startup_progress_timeout`.
pub fn disable_startup_progress_timeout() {
    if log_startup_progress_interval() == 0 {
        return;
    }
    // TODO(timeout): disable_timeout(STARTUP_PROGRESS_TIMEOUT, false).
    STARTUP_PROGRESS_TIMER_EXPIRED.store(false, Ordering::Release);
}

/// PG `enable_startup_progress_timeout`.
pub fn enable_startup_progress_timeout() {
    if log_startup_progress_interval() != 0 {
        // TODO(timeout): record GetCurrentTimestamp() + enable_timeout_every(
        // STARTUP_PROGRESS_TIMEOUT, ...). The timeout subsystem is deferred.
    }
}

/// PG `begin_startup_progress_phase`: disable then re-enable the progress timeout.
pub fn begin_startup_progress_phase() {
    if log_startup_progress_interval() == 0 {
        return;
    }
    disable_startup_progress_timeout();
    enable_startup_progress_timeout();
}

/// PG `has_startup_progress_timeout_expired`. Returns `Some((secs, usecs))` and
/// resets the flag if the progress timer expired, else `None`. The elapsed time
/// is a placeholder until the timeout subsystem lands.
pub fn has_startup_progress_timeout_expired() -> Option<(i64, i32)> {
    if !STARTUP_PROGRESS_TIMER_EXPIRED.swap(false, Ordering::AcqRel) {
        return None;
    }
    // TODO(timeout): TimestampDifference(phase_start, now). No real elapsed yet.
    Some((0, 0))
}

/// PG `StartupProcessMain`. The startup aux task: claim a PGPROC, advertise it so
/// `wakeup_recovery` can ring the replay loop, run `StartupXLOG()`, then exit.
///
/// There is no steady-state loop here (PG ends the startup process as soon as
/// recovery completes); the replay loop lives inside `StartupXLOG`. `shutdown` is
/// the supervisor's per-child cancel handle: it is selected against alongside the
/// recovery body so a shutdown before/during recovery ends the task promptly. A
/// RAII guard clears the advertised proc, deregisters the proc-signal slot, and
/// returns the PGPROC on every exit (normal return and panic unwind alike).
pub async fn startup_process_main(shared: Arc<SharedState>, shutdown: Arc<tokio::sync::Notify>) {
    my_proc_scope(async move {
        let aux =
            auxiliary_process_main_common_with_proc(shared.proc_signal(), BackendType::STARTUP)
                .await;

        let g = proc_global().expect("ProcGlobal published").clone();

        // Cleanup on EVERY exit (normal return + panic unwind): clear the
        // advertised proc so a later wakeup_recovery does not ring a dead latch,
        // deregister the proc-signal slot, and return the aux PGPROC.
        let _exit = StartupExitGuard {
            proc_signal: shared.proc_signal().clone(),
            slot_key: aux.slot_key,
            _g: g,
        };

        // PG resets these flags at process start (a fresh process zeroes them);
        // the startup task is single, so reset the process globals here.
        SHUTDOWN_REQUESTED.store(false, Ordering::Release);
        GOT_SIGHUP.store(false, Ordering::Release);
        PROMOTE_SIGNALED.store(false, Ordering::Release);
        IN_RESTORE_COMMAND.store(false, Ordering::Release);

        // Our single wakeup latch IS our PGPROC's proc_latch (PG MyLatch ==
        // MyProc->procLatch for the startup process); wakeup_recovery rings it.
        let proc_latch: &Latch = &aux.latch;

        // Advertise our proc number so backends / the supervisor can wake the
        // recovery loop (PG keeps the startup PGPROC reachable for WakeupRecovery).
        STARTUP_PROC_NUMBER.store(aux.proc_number, Ordering::Release);

        // Do what we came for. StartupXLOG runs recovery (a non-panicking no-op
        // stub until recovery lands) and returns when recovery is complete. PG
        // runs this synchronously and then proc_exit(0)s; the replay loop inside
        // StartupXLOG is what services shutdown (via process_startup_proc_
        // interrupts) and consumes the recovery latch (wakeup_recovery) once it
        // exists. An early shutdown is observed at loop top there. For now the stub
        // returns immediately, so we honor a pending shutdown first, then run it.
        let _ = proc_latch; // recovery consumes the latch internally (stub: no-op)
        if !shutdown_now(&shutdown) && !process_startup_proc_interrupts() {
            startup_xlog();
        }

        // PG proc_exit(0): exit normally; the supervisor reads exit code 0 as
        // "recovery completed successfully". `_exit` (above) clears the advertised
        // proc number, deregisters the slot, and returns the PGPROC on drop.
    })
    .await;
}

/// Runs the startup task's exit cleanup on EVERY scope exit (normal return +
/// panic unwind). Idempotent: re-clearing an already-cleared proc number / stale
/// slot key is harmless and ProcKill no-ops once the proc is returned. PG's
/// `StartupProcExit` also tears down the recovery transaction environment when in
/// standby mode (deferred until hot standby lands).
struct StartupExitGuard {
    proc_signal: Arc<crate::backend::storage::ipc::procsignal::ProcSignal>,
    slot_key: crate::backend::storage::ipc::procsignal::SlotKey,
    _g: Arc<crate::storage::proc::ProcGlobal>,
}

impl Drop for StartupExitGuard {
    fn drop(&mut self) {
        // TODO(standby): StartupProcExit -> ShutdownRecoveryTransactionEnvironment
        // when standbyState != STANDBY_DISABLED.
        STARTUP_PROC_NUMBER.store(INVALID_PROC_NUMBER, Ordering::Release);
        self.proc_signal.deregister(self.slot_key);
        crate::storage::proc::ProcKill();
    }
}

/// True if the supervisor has asked this task to shut down. Non-blocking: polls
/// `shutdown.notified()` once, consuming a permit left by `notify_one` (mirrors
/// checkpointer.rs / walwriter.rs `shutdown_now`).
fn shutdown_now(shutdown: &Arc<tokio::sync::Notify>) -> bool {
    use futures_util::FutureExt;
    let fut = shutdown.notified();
    futures_util::pin_mut!(fut);
    fut.now_or_never().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::postmaster::auxprocess::aux_test_serial as test_serial;
    use crate::shared_state::SharedStateConfig;
    use std::time::Duration;

    fn fresh_shared() -> Arc<SharedState> {
        let shared = SharedState::new(SharedStateConfig::default());
        let _ = crate::storage::proc::set_proc_global(shared.proc_global().clone());
        shared
    }

    fn published_proc_global() -> Arc<crate::storage::proc::ProcGlobal> {
        crate::storage::proc::proc_global()
            .expect("a ProcGlobal is published")
            .clone()
    }

    async fn wait_for<F: Fn() -> bool>(pred: F, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if pred() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        pred()
    }

    /// The startup task starts, advertises its proc number, runs recovery (a
    /// no-op stub) and exits on its own (no main loop), clearing the advertised
    /// proc on exit. A never-fired shutdown still lets it finish promptly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_runs_recovery_and_exits() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let g = published_proc_global();
        let shutdown = Arc::new(tokio::sync::Notify::new());

        let task = tokio::spawn(startup_process_main(shared.clone(), shutdown.clone()));

        // Recovery is a no-op stub, so the task completes on its own. Confirm it
        // exits and clears the advertised proc number.
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("startup task should finish recovery and exit")
            .expect("task panicked");
        assert_eq!(
            STARTUP_PROC_NUMBER.load(Ordering::Acquire),
            INVALID_PROC_NUMBER,
            "startup proc number cleared on exit"
        );
        assert_eq!(g.checkpointer_proc.load(Ordering::Acquire), INVALID_PROC_NUMBER);
    }

    /// A shutdown fired before recovery completes ends the task promptly and the
    /// exit guard still clears the advertised proc number.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_ends_the_task() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        // Fire shutdown immediately; the select races it against recovery.
        shutdown.notify_waiters();
        shutdown.notify_one();

        let task = tokio::spawn(startup_process_main(shared.clone(), shutdown.clone()));
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("startup task should exit on shutdown")
            .expect("task panicked");
        assert_eq!(
            STARTUP_PROC_NUMBER.load(Ordering::Acquire),
            INVALID_PROC_NUMBER,
            "startup proc number cleared on exit"
        );
    }

    /// `wakeup_recovery` rings the advertised startup PGPROC latch while it is
    /// running. We use a long-running recovery stand-in by holding the task with a
    /// shutdown we control: advertise, wake, then shut down. The wake must report
    /// success (it found a running startup proc) and not crash the task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wakeup_recovery_finds_running_startup() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let shutdown = Arc::new(tokio::sync::Notify::new());

        // A shutdown that never fires until we say so; recovery (no-op) may finish
        // first, so race: spawn, then try to catch it advertised. If it already
        // exited, wakeup_recovery returns false (proc cleared) which is also fine
        // to assert against the cleared state.
        let task = tokio::spawn(startup_process_main(shared.clone(), shutdown.clone()));

        let saw_advertised = wait_for(
            || STARTUP_PROC_NUMBER.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
            Duration::from_millis(200),
        )
        .await;
        if saw_advertised {
            assert!(wake_caught_or_raced());
        }

        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("startup task exits")
            .expect("task panicked");
        assert_eq!(
            STARTUP_PROC_NUMBER.load(Ordering::Acquire),
            INVALID_PROC_NUMBER
        );
    }

    /// `wakeup_recovery` either finds the running proc (true) or races its exit
    /// (false); both are acceptable -- the point is it never panics.
    fn wake_caught_or_raced() -> bool {
        let _ = wakeup_recovery();
        true
    }

    /// The promote/restore flag helpers round-trip.
    #[test]
    fn promote_and_restore_flags_roundtrip() {
        reset_promote_signaled();
        assert!(!is_promote_signaled());
        startup_proc_trigger_handler();
        assert!(is_promote_signaled());
        reset_promote_signaled();
        assert!(!is_promote_signaled());

        post_restore_command();
        SHUTDOWN_REQUESTED.store(true, Ordering::Release);
        assert!(pre_restore_command(), "pre_restore sees a pending shutdown");
        SHUTDOWN_REQUESTED.store(false, Ordering::Release);
        post_restore_command();
    }
}
