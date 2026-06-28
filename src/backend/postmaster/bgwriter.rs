//! The background writer process. Translated from backend/postmaster/bgwriter.c.
//!
//! The background writer attempts to keep regular backends from having to write
//! out dirty shared buffers, which they would otherwise do when they need to
//! free a buffer to read in another page. In the best case all writes from
//! shared buffers are issued by the background writer; regular backends remain
//! empowered to issue their own writes if the bgwriter fails to keep enough
//! clean buffers available. Each cycle runs one round of `BgBufferSync` and then
//! sleeps for `BgWriterDelay` milliseconds, or much longer in "hibernation" mode
//! when the system is idle, until it is woken or the delay elapses. As of
//! PostgreSQL 9.2 the background writer no longer handles checkpoints; that work
//! belongs to the checkpointer.
//!
//! In PepperDB the bgwriter is a long-lived async task rather than a forked
//! process. PostgreSQL's per-iteration `sigsetjmp` error-recovery block is not
//! reproduced: an error raised mid-cycle unwinds to the task boundary, where the
//! supervisor restarts the task. Termination is driven by a shutdown signal from
//! the supervisor instead of SIGTERM/SIGQUIT. The task claims an auxiliary PGPROC
//! so that its proc latch serves as the single wakeup source (matching
//! PostgreSQL's `MyLatch == MyProc->procLatch` for an auxiliary process); a
//! backend that allocates a buffer wakes it through `StrategyNotifyBgWriter`. As
//! in PostgreSQL, the bgwriter advertises no PGPROC slot of its own for backends
//! to address. Standby-snapshot logging is present but inert until replication is
//! enabled.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::access::xlog::recovery_in_progress;
use crate::backend::postmaster::auxprocess::{
    auxiliary_process_main_common_with_proc, process_main_loop_interrupts,
};
use crate::backend::postmaster::checkpointer::first_call_since_last_checkpoint;
use crate::backend::storage::buffer::freelist::strategy_notify_bg_writer;
use crate::backend::storage::smgr::smgr::smgrdestroyall;
use crate::miscadmin::BackendType;
use crate::shared_state::SharedState;
use crate::storage::bufmgr::BgBufferSync;
use crate::storage::latch::Latch;
use crate::storage::proc::{my_proc_scope, ProcGlobal};
use crate::storage::procnumber::INVALID_PROC_NUMBER;

/// PG GUC `BgWriterDelay` (ms between bgwriter cycles; default 200). Process-
/// global atomic with accessors (no `static mut`); settable for tests.
/// TODO(guc): drive from the GUC bridge once it lands.
static BG_WRITER_DELAY: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(200);

/// PG `BgWriterDelay`.
pub fn bg_writer_delay() -> i32 {
    BG_WRITER_DELAY.load(Ordering::Relaxed)
}

/// Set `BgWriterDelay` (GUC assignment / tests).
pub fn set_bg_writer_delay(ms: i32) {
    BG_WRITER_DELAY.store(ms, Ordering::Relaxed);
}

/// Multiplier applied to `BgWriterDelay` when we decide to hibernate.
const HIBERNATE_FACTOR: i32 = 50;

/// PG `BackgroundWriterMain`. The long-lived background-writer aux task. Runs
/// inside `my_proc_scope` so the cradle's `InitAuxiliaryProcess` can claim a
/// PGPROC (its `proc_latch` is the single wakeup latch). Loops until `shutdown`
/// fires.
///
/// `shutdown` is the supervisor's per-child cancel handle (17f). The loop mirrors
/// 17a's shape exactly: a single unified latch reset at the top, interrupts
/// serviced, and a `select!{ biased; latch | timeout | shutdown }` sleep. A RAII
/// guard deregisters the proc-signal slot and returns the PGPROC on every exit
/// (normal break and panic unwind alike).
pub async fn background_writer_main(shared: Arc<SharedState>, shutdown: Arc<tokio::sync::Notify>) {
    my_proc_scope(async move {
        let aux =
            auxiliary_process_main_common_with_proc(shared.proc_signal(), BackendType::BG_WRITER)
                .await;

        // Cleanup on EVERY exit (normal break + panic unwind): deregister the
        // proc-signal slot and return the aux PGPROC. The bgwriter advertises no
        // ProcGlobal.<role>_proc, so there is none to clear.
        let _exit = BgWriterExitGuard {
            proc_signal: shared.proc_signal().clone(),
            slot_key: aux.slot_key,
        };

        // Our single wakeup latch IS our PGPROC's proc_latch (PG MyLatch ==
        // MyProc->procLatch for an aux proc); the proc-signal slot was registered
        // against this SAME latch in the cradle, so both wakeup sources hit one
        // latch -- the loop waits AND resets only it.
        let proc_latch: &Latch = &aux.latch;

        // PG: we just started; assume there has been a shutdown / end-of-recovery
        // snapshot. (last_snapshot_ts / last_snapshot_lsn drive the standby-
        // snapshot logging branch, which is inert until replication lands.)

        // PG resets hibernation state after any error; here it is loop-local.
        let mut prev_hibernate = false;

        // --- Loop forever (until shutdown). ---
        loop {
            // PG ResetLatch(MyLatch) at loop top: clear any already-pending wakeup.
            proc_latch.reset();

            // PG ProcessMainLoopInterrupts(); break on shutdown. shutdown_now polls
            // the supervisor's cancel Notify so a shutdown arriving between sleeps
            // is observed at loop top.
            if process_main_loop_interrupts() || shutdown_now(&shutdown) {
                break;
            }

            // Do one cycle of dirty-buffer writing (PG BgBufferSync). Returns
            // whether we can hibernate (nothing happening). bufmgr stub for now.
            let can_hibernate = BgBufferSync();

            // Report pending statistics to the cumulative stats system.
            crate::pgstat::pgstat_report_bgwriter();
            crate::pgstat::pgstat_report_wal(true);

            if first_call_since_last_checkpoint() {
                // After any checkpoint, free all smgr objects: the bgwriter does
                // not process shared invalidation messages or call AtEOXact_SMgr,
                // so dropped relations would otherwise leak smgr handles.
                smgrdestroyall();
            }

            // PG logs a new xl_running_xacts every LOG_SNAPSHOT_INTERVAL_MS so
            // replication can reach a consistent state faster. Standby info is
            // inactive until replication lands (xlog_standby_info_active() ==
            // false), so this branch is inert; kept faithful.
            if crate::access::xlog::xlog_standby_info_active() && !recovery_in_progress() {
                // TODO(replication): LogStandbySnapshot on the
                // LOG_SNAPSHOT_INTERVAL_MS cadence (last_snapshot_ts/_lsn vs
                // GetLastImportantRecPtr). Unreachable until wal_level is raised.
            }

            // Sleep until signaled or BgWriterDelay has elapsed. The feedback loop
            // in BgBufferSync expects to be called every BgWriterDelay ms, so we
            // avoid loading this task with frequent latch events.
            let delay = bg_writer_delay().max(0) as u64;
            let timed_out = {
                let sleep = tokio::time::sleep(std::time::Duration::from_millis(delay));
                tokio::select! {
                    biased;
                    () = proc_latch.wait() => false,
                    () = sleep => true,
                    () = shutdown.notified() => break,
                }
            };

            // If no latch event and BgBufferSync says nothing's happening, extend
            // the sleep in "hibernation" mode (much longer than BgWriterDelay).
            // A backend allocating a buffer wakes us by setting our latch. We only
            // hibernate after two consecutive idle cycles, and never forever.
            if timed_out && can_hibernate && prev_hibernate {
                // Ask for notification at the next buffer allocation. The bgwriter
                // is woken via StrategyNotifyBgWriter (not ProcGlobal), so pass our
                // PGPROC number; -1 clears the request afterwards.
                strategy_notify_bg_writer(aux.proc_number);
                let hibernate_ms =
                    (i64::from(bg_writer_delay()) * i64::from(HIBERNATE_FACTOR)).max(0) as u64;
                let sleep = tokio::time::sleep(std::time::Duration::from_millis(hibernate_ms));
                let shutdown_break = tokio::select! {
                    biased;
                    () = proc_latch.wait() => false,
                    () = sleep => false,
                    () = shutdown.notified() => true,
                };
                // Reset the notification request before continuing OR exiting, so
                // our PGPROC number never stays registered as the freelist target
                // after we go away (PG always pairs the (-1) with the prior call).
                strategy_notify_bg_writer(INVALID_PROC_NUMBER);
                if shutdown_break {
                    break;
                }
            }

            prev_hibernate = can_hibernate;
        }

        // `_exit` (above) deregisters the slot and returns the PGPROC on drop --
        // on this normal path and on any panic unwind alike.
    })
    .await;
}

/// Runs the bgwriter's exit cleanup on EVERY scope exit (normal return + panic
/// unwind). Idempotent: a stale slot key is harmless to re-drop and ProcKill
/// no-ops once the proc is returned.
struct BgWriterExitGuard {
    proc_signal: Arc<crate::backend::storage::ipc::procsignal::ProcSignal>,
    slot_key: crate::backend::storage::ipc::procsignal::SlotKey,
}

impl Drop for BgWriterExitGuard {
    fn drop(&mut self) {
        self.proc_signal.deregister(self.slot_key);
        crate::storage::proc::ProcKill();
    }
}

/// True if the supervisor has asked this task to shut down (PG's shutdown-request
/// check at loop top). Non-blocking: polls `shutdown.notified()` once, consuming a
/// permit left by `notify_one`, so a shutdown that arrived between sleeps is seen
/// here without awaiting (mirrors checkpointer.rs `shutdown_now`).
fn shutdown_now(shutdown: &Arc<tokio::sync::Notify>) -> bool {
    use futures_util::FutureExt;
    let fut = shutdown.notified();
    futures_util::pin_mut!(fut);
    fut.now_or_never().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::SharedStateConfig;
    use std::time::Duration;

    fn fresh_shared() -> Arc<SharedState> {
        let shared = SharedState::new(SharedStateConfig::default());
        let _ = crate::storage::proc::ProcGlobal::set(shared.proc_global().clone());
        let _ = crate::backend::postmaster::checkpointer::set_checkpointer_shmem(
            shared.checkpointer().clone(),
        );
        shared
    }

    /// The bgwriter starts, parks on its latch (long delay so the timer path
    /// stays quiet), keeps running across a cycle, and exits cleanly on shutdown.
    /// The bgwriter advertises no ProcGlobal proc, so it is driven via shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_starts_parks_and_shuts_down() {
        // Serialize against all aux-task tests (shared ProcGlobal + aux slots).
        let _serial = crate::backend::postmaster::auxprocess::aux_test_serial().await;
        let shared = fresh_shared();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        // Long delay so the task parks on its latch rather than spinning the timer.
        set_bg_writer_delay(3600 * 1000);

        let task = tokio::spawn(background_writer_main(shared.clone(), shutdown.clone()));

        // Give the task a moment to reach its first park, then wake it via the
        // unified latch (a buffer-activity wakeup). It must keep running.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!task.is_finished(), "bgwriter parks, does not exit on its own");

        // A latch wake just loops it back to parking (no panic on the timer path).
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!task.is_finished(), "bgwriter still running after a latch wake");

        // Shut it down and confirm it exits cleanly.
        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("bgwriter should exit on shutdown")
            .expect("task panicked");

        set_bg_writer_delay(200);
    }
}
