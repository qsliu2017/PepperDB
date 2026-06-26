//! Translated from PostgreSQL src/backend/postmaster/walwriter.c
//!
//! The WAL writer: a long-lived auxiliary task that keeps regular backends from
//! having to write out (and fsync) WAL pages, and that guarantees asynchronously
//! committed transactions reach disk within a bounded time (at most three
//! `wal_writer_delay` cycles). Each cycle runs `XLogBackgroundFlush`, then sleeps
//! `WalWriterDelay` ms (or `WalWriterDelay * HIBERNATE_FACTOR` once it has been
//! idle for `LOOPS_UNTIL_HIBERNATE` cycles) until its latch is rung or the delay
//! elapses.
//!
//! Single-process redesign vs PG's walwriter.c:
//! - The walwriter advertises its ProcNumber in `ProcGlobal.walwriter_proc` so
//!   async-commit backends can wake it by `ProcNumber` (PG `ProcGlobal->
//!   walwriterProc`). [`wake_walwriter`] rings that PGPROC's `proc_latch`; the
//!   advertisement is cleared on exit by the RAII guard.
//! - PG's in-loop `sigsetjmp` error recovery is NOT reproduced: an
//!   `elog(ERROR)`-as-panic propagates to the task boundary, where the supervisor
//!   (17f) restarts the task (mirrors the checkpointer, 17a).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::access::xlog::{set_wal_writer_sleeping, xlog_background_flush};
use crate::backend::postmaster::auxprocess::{
    auxiliary_process_main_common_with_proc, process_main_loop_interrupts,
};
use crate::miscadmin::BackendType;
use crate::postmaster::walwriter::DEFAULT_WAL_WRITER_FLUSH_AFTER;
use crate::shared_state::SharedState;
use crate::storage::latch::Latch;
use crate::storage::proc::{my_proc_scope, proc_global};
use crate::storage::procnumber::INVALID_PROC_NUMBER;

/// PG GUC `WalWriterDelay` (ms between walwriter cycles; default 200). Process-
/// global atomic with accessors (no `static mut`); settable for tests.
/// TODO(guc): drive from the GUC bridge once it lands.
static WAL_WRITER_DELAY: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(200);

/// PG GUC `WalWriterFlushAfter` (XLOG blocks; default `DEFAULT_WAL_WRITER_FLUSH_
/// AFTER`). Consulted by `XLogBackgroundFlush` (an xlog stub for now).
static WAL_WRITER_FLUSH_AFTER: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(DEFAULT_WAL_WRITER_FLUSH_AFTER);

/// PG `WalWriterDelay`.
pub fn wal_writer_delay() -> i32 {
    WAL_WRITER_DELAY.load(Ordering::Relaxed)
}

/// Set `WalWriterDelay` (GUC assignment / tests).
pub fn set_wal_writer_delay(ms: i32) {
    WAL_WRITER_DELAY.store(ms, Ordering::Relaxed);
}

/// PG `WalWriterFlushAfter`.
pub fn wal_writer_flush_after() -> i32 {
    WAL_WRITER_FLUSH_AFTER.load(Ordering::Relaxed)
}

/// Number of do-nothing loops before lengthening the delay time.
const LOOPS_UNTIL_HIBERNATE: i32 = 50;
/// Multiplier applied to `WalWriterDelay` when we decide to hibernate.
const HIBERNATE_FACTOR: i32 = 25;

/// Ring the walwriter's wakeup latch (PG `SetLatch(&GetPGProcByNumber(
/// walwriterProc)->procLatch)`). Reaches the aux PGPROC by the advertised
/// `ProcGlobal.walwriter_proc` and sets its `proc_latch`. Returns false if no
/// walwriter is running (the proc number is INVALID). Used by async-commit
/// backends to flush WAL promptly.
pub fn wake_walwriter() -> bool {
    let Some(g) = proc_global() else {
        return false;
    };
    let procno = g.walwriter_proc.load(Ordering::Acquire);
    if procno == INVALID_PROC_NUMBER {
        return false;
    }
    // SAFETY: `proc_latch` is internally synchronized (a Latch over a Notify);
    // setting it forms no `&mut` into the slot's other field groups.
    (unsafe { g.proc(procno) })
        .inspect(|proc| proc.proc_latch.set())
        .is_some()
}

/// PG `WalWriterMain`. The long-lived WAL-writer aux task. Runs inside
/// `my_proc_scope` so the cradle's `InitAuxiliaryProcess` can claim a PGPROC
/// (its `proc_latch` is the single wakeup latch); advertises `ProcGlobal.
/// walwriter_proc`; loops until `shutdown` fires.
///
/// `shutdown` is the supervisor's per-child cancel handle (17f). The loop mirrors
/// 17a's shape exactly: a single unified latch reset at the top, interrupts
/// serviced, and a `select!{ biased; latch | timeout | shutdown }` sleep. A RAII
/// guard clears the advertised proc, deregisters the slot, and returns the PGPROC
/// on every exit (normal break and panic unwind alike).
pub async fn wal_writer_main(shared: Arc<SharedState>, shutdown: Arc<tokio::sync::Notify>) {
    my_proc_scope(async move {
        let aux =
            auxiliary_process_main_common_with_proc(shared.proc_signal(), BackendType::WAL_WRITER)
                .await;

        let g = proc_global().expect("ProcGlobal published").clone();

        // Cleanup on EVERY exit (normal break + panic unwind): clear the advertised
        // proc so a later wake_walwriter does not ring a dead latch, deregister the
        // proc-signal slot, and return the aux PGPROC.
        let _exit = WalWriterExitGuard {
            g: g.clone(),
            proc_signal: shared.proc_signal().clone(),
            slot_key: aux.slot_key,
        };

        // Our single wakeup latch IS our PGPROC's proc_latch (PG MyLatch ==
        // MyProc->procLatch for an aux proc); the proc-signal slot was registered
        // against this SAME latch in the cradle, so wake_walwriter (PGPROC latch)
        // AND a proc-signal/barrier send both hit one latch.
        let proc_latch: &Latch = &aux.latch;

        // PG resets hibernation state after any error; here it is loop-local.
        let mut left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
        let mut hibernating = false;
        set_wal_writer_sleeping(false);

        // Advertise our proc number so async-commit backends can wake us (PG:
        // ProcGlobal->walwriterProc = MyProcNumber).
        g.walwriter_proc.store(aux.proc_number, Ordering::Release);

        // --- Loop forever (until shutdown). ---
        loop {
            // Advertise whether we might hibernate this cycle, BEFORE resetting the
            // latch, so any async commit sees the flag set if it might need to wake
            // us and we won't miss its signal. Avoid touching the flag needlessly.
            if hibernating != (left_till_hibernate <= 1) {
                hibernating = left_till_hibernate <= 1;
                set_wal_writer_sleeping(hibernating);
            }

            // PG ResetLatch(MyLatch) at loop top: clear any already-pending wakeup.
            proc_latch.reset();

            // PG ProcessMainLoopInterrupts(); break on shutdown. shutdown_now polls
            // the supervisor's cancel Notify so a shutdown arriving between sleeps
            // is observed at loop top.
            if process_main_loop_interrupts() || shutdown_now(&shutdown) {
                break;
            }

            // Do what we're here for; if XLogBackgroundFlush found useful work,
            // reset the hibernation counter (xlog stub for now: reports no work).
            if xlog_background_flush() {
                left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
            } else if left_till_hibernate > 0 {
                left_till_hibernate -= 1;
            }

            // Report pending statistics to the cumulative stats system.
            crate::pgstat::pgstat_report_wal(false);

            // Sleep until signaled or WalWriterDelay has elapsed. If we have not
            // done anything useful for a while, lengthen the sleep to reduce idle
            // power consumption.
            let cur_timeout = if left_till_hibernate > 0 {
                wal_writer_delay()
            } else {
                wal_writer_delay() * HIBERNATE_FACTOR
            };

            let sleep =
                tokio::time::sleep(std::time::Duration::from_millis(cur_timeout.max(0) as u64));
            tokio::select! {
                biased;
                () = proc_latch.wait() => {}
                () = sleep => {}
                () = shutdown.notified() => break,
            }
        }

        // `_exit` (above) clears the advertised proc number, deregisters the slot,
        // and returns the PGPROC on drop -- on this normal path and on any panic
        // unwind alike.
    })
    .await;
}

/// Runs the walwriter's exit cleanup on EVERY scope exit (normal return + panic
/// unwind). Idempotent: re-clearing an already-cleared proc number / stale slot
/// key is harmless and ProcKill no-ops once the proc is returned.
struct WalWriterExitGuard {
    g: Arc<crate::storage::proc::ProcGlobal>,
    proc_signal: Arc<crate::backend::storage::ipc::procsignal::ProcSignal>,
    slot_key: crate::backend::storage::ipc::procsignal::SlotKey,
}

impl Drop for WalWriterExitGuard {
    fn drop(&mut self) {
        // Stop backends from waking a dead proc, then drop the slot + PGPROC.
        self.g
            .walwriter_proc
            .store(INVALID_PROC_NUMBER, Ordering::Release);
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
        let _ = crate::storage::proc::set_proc_global(shared.proc_global().clone());
        shared
    }

    fn published_proc_global() -> Arc<crate::storage::proc::ProcGlobal> {
        crate::storage::proc::proc_global()
            .expect("a ProcGlobal is published")
            .clone()
    }

    /// Serialize across ALL aux-task tests: they share the single process-wide
    /// `ProcGlobal` + aux PGPROC slots + the `ProcGlobal.<role>_proc`
    /// advertisements, so no two aux tasks (any module) may run concurrently.
    use crate::backend::postmaster::auxprocess::aux_test_serial as test_serial;

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

    /// The walwriter starts, advertises walwriter_proc, parks on its latch, and
    /// clears the advertisement + exits cleanly on shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_advertises_parks_and_shuts_down() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let g = published_proc_global();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        // Long delay so the task parks on its latch rather than spinning the timer.
        set_wal_writer_delay(3600 * 1000);

        let task = tokio::spawn(wal_writer_main(shared.clone(), shutdown.clone()));

        // The task advertises ProcGlobal.walwriter_proc.
        assert!(
            wait_for(
                || g.walwriter_proc.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(2),
            )
            .await,
            "walwriter should advertise its proc number"
        );

        // Shut it down and confirm it exits + clears its proc number.
        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("walwriter should exit on shutdown")
            .expect("task panicked");
        assert_eq!(
            g.walwriter_proc.load(Ordering::Acquire),
            INVALID_PROC_NUMBER,
            "proc number cleared on exit"
        );

        set_wal_writer_delay(200);
    }

    /// wake_walwriter rings the advertised PGPROC latch, so a parked walwriter
    /// loops back (and keeps running). With a long delay the only wakeup is the
    /// latch, proving the wake path reaches the task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wake_walwriter_wakes_the_task() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let g = published_proc_global();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        set_wal_writer_delay(3600 * 1000);

        let task = tokio::spawn(wal_writer_main(shared.clone(), shutdown.clone()));
        assert!(
            wait_for(
                || g.walwriter_proc.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(2),
            )
            .await,
            "walwriter running"
        );

        // Wake it via the advertised proc; it loops back to parking, not exits.
        assert!(wake_walwriter(), "wake_walwriter finds the running walwriter");
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!task.is_finished(), "walwriter keeps running after a wake");

        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("walwriter exits")
            .expect("task panicked");

        set_wal_writer_delay(200);
    }
}
