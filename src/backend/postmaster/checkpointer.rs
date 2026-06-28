//! The checkpointer auxiliary task: performs all checkpoints. Translated from backend/postmaster/checkpointer.c.
//!
//! The checkpointer is a long-lived auxiliary process that handles every
//! checkpoint. Checkpoints are dispatched automatically once a configured
//! interval (`CheckPointTimeout`) has elapsed since the last one, and the
//! checkpointer can also be signaled to perform requested checkpoints on demand.
//! A backend requests a checkpoint by setting request flags and waking the
//! checkpointer, then watches a set of counters -- `started`, `done`, and
//! `failed`, guarded by the checkpointer's lock -- to learn that its checkpoint
//! began, completed, or failed. On the normal shutdown path the checkpointer is
//! instructed to write the shutdown checkpoint, then to exit only after the other
//! auxiliary tasks have stopped, so it is first to start and last to stop.
//!
//! In PostgreSQL the checkpointer also serves as the central collector for fsync
//! requests: any backend that writes a relation forwards an fsync request to the
//! checkpointer over a shared-memory ring buffer, which the checkpointer absorbs
//! into its pending-operations table and flushes during the next checkpoint.
//!
//! Because PepperDB runs as a single process, that cross-process request
//! forwarding does not exist. Every task enqueues fsync and unlink requests
//! directly into one shared sync-request queue (already deduplicated by file tag),
//! so the ring buffer, its lock, the forward/compact helpers, and the
//! request struct are all gone, and `absorb_sync_requests` is a no-op. The
//! shared state that connects the checkpointer with requesting backends is an
//! `Arc`-shared struct holding the counters under a `parking_lot` mutex and two
//! condition variables, published once at startup, rather than a fixed
//! shared-memory segment. Wakeups travel over a `tokio`-backed latch instead of a
//! signal, and the checkpointer is a spawned task rather than a forked process.
//! PostgreSQL's in-loop `sigsetjmp` error recovery is not reproduced: a failed
//! checkpoint surfaces as an unwinding panic, which still bumps the `failed`
//! counter and wakes any waiter before propagating to the task boundary, where the
//! supervisor restarts the task. Several deeper paths -- WAL-segment switching for
//! `archive_timeout`, the buffer-sync write throttle, and the shared-memory config
//! push -- are present as faithful shells but are inert until the WAL and buffer
//! manager subsystems they depend on are implemented.

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, MutexGuard};

use crate::access::xlog::{create_check_point, create_restart_point, recovery_in_progress, CheckpointFlags};
use crate::backend::postmaster::auxprocess::{
    auxiliary_process_main_common_with_proc, process_main_loop_interrupts,
};
use crate::backend::storage::smgr::smgr::smgrdestroyall;
use crate::backend::storage::sync::sync::{ProcessSyncRequests, SyncPostCheckpoint, SyncPreCheckpoint};
use crate::miscadmin::BackendType;
use crate::pgtime::pg_time_t;
use crate::shared_state::SharedState;
use crate::storage::condition_variable::ConditionVariable;
use crate::storage::latch::Latch;
use crate::storage::proc::{my_proc_scope, ProcGlobal};
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

/// GUC parameters (PG globals; default values from checkpointer.c). Settable for
/// tests via the process-global setters below. TODO(guc): drive from the GUC
/// bridge once it lands.
static CHECK_POINT_TIMEOUT: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(300);
static CHECK_POINT_WARNING: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(30);
/// PG `XLogArchiveTimeout` (xlog.c). 0 = disabled (the default), so
/// `CheckArchiveTimeout` short-circuits and never reaches the xlog-switch stubs.
static XLOG_ARCHIVE_TIMEOUT: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// PG `CheckPointTimeout`.
pub fn check_point_timeout() -> i32 {
    CHECK_POINT_TIMEOUT.load(Ordering::Relaxed)
}

/// Set `CheckPointTimeout` (GUC assignment / tests).
pub fn set_check_point_timeout(secs: i32) {
    CHECK_POINT_TIMEOUT.store(secs, Ordering::Relaxed);
}

fn check_point_warning() -> i32 {
    CHECK_POINT_WARNING.load(Ordering::Relaxed)
}

fn xlog_archive_timeout() -> i32 {
    XLOG_ARCHIVE_TIMEOUT.load(Ordering::Relaxed)
}

// deleted by redesign: single-process uses the shared SyncRequests queue
//   - the requests[]/num_requests/max_requests ring buffer
//   - CheckpointerCommLock (the LWLock guarding it)
//   - ForwardSyncRequest / CompactCheckpointerRequestQueue
//   - the CheckpointerRequest struct + WRITES_PER_ABSORB / MAX_CHECKPOINT_REQUESTS

/// The ckpt request counters (PG's `ckpt_*` fields under `ckpt_lck`). All `i32`
/// with modulo-arithmetic compare so they stay wraparound-safe (PG's design).
#[derive(Default)]
pub struct CkptCounters {
    /// Advances when a checkpoint starts.
    pub started: i32,
    /// Advances (set equal to `started`) when a checkpoint finishes.
    pub done: i32,
    /// Advances when a checkpoint fails.
    pub failed: i32,
    /// OR of the checkpoint request flags since the last checkpoint start
    /// (`CheckpointFlags` bits, as in xlog.h).
    pub flags: i32,
}

/// PG `CheckpointerShmemStruct` -- the checkpointer<->backend shared state.
/// An `Arc` on [`SharedState`] (the ipci.c `CheckpointerShmemInit` slot) AND
/// published process-wide via [`set_checkpointer_shmem`] so `request_checkpoint`,
/// called by arbitrary backends, reaches it without a `SharedState` handle.
pub struct CheckpointerShmem {
    /// PG `ckpt_lck` + the `ckpt_*` fields.
    pub ckpt: Mutex<CkptCounters>,
    /// Signaled when `started` advances (PG `start_cv`).
    pub start_cv: ConditionVariable,
    /// Signaled when `done` advances (PG `done_cv`).
    pub done_cv: ConditionVariable,
}

impl CheckpointerShmem {
    /// PG `CheckpointerShmemInit`: a zeroed counters struct + two fresh CVs.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ckpt: Mutex::new(CkptCounters::default()),
            start_cv: ConditionVariable::new(),
            done_cv: ConditionVariable::new(),
        })
    }

    /// Lock the counters. PG guards them with a SpinLock, which has no poison
    /// concept; `CkptCounters` is plain integer state and every critical section
    /// is a complete, short update, so it is never left inconsistent. A panic on
    /// the checkpointer task (which the supervisor restarts) must therefore NOT
    /// brick the shared counters via poison -- recover the guard if poisoned.
    fn lock_ckpt(&self) -> MutexGuard<'_, CkptCounters> {
        self.ckpt.lock()
    }
}

/// The process-wide `CheckpointerShmem`, published by `SharedState::new`
/// (single-process model: exactly one). Replaces C's `static CheckpointerShmem`.
static CHECKPOINTER_SHMEM: OnceLock<Arc<CheckpointerShmem>> = OnceLock::new();

/// Publish the process-wide `CheckpointerShmem`. First publish wins (tests build
/// multiple `SharedState`s); returns whether this call won.
pub fn set_checkpointer_shmem(shmem: Arc<CheckpointerShmem>) -> bool {
    CHECKPOINTER_SHMEM.set(shmem).is_ok()
}

/// The process-wide `CheckpointerShmem`, if `SharedState::new` has run.
pub fn checkpointer_shmem() -> Option<&'static Arc<CheckpointerShmem>> {
    CHECKPOINTER_SHMEM.get()
}

/// Ring the checkpointer's wakeup latch (PG `SetLatch(&GetPGProcByNumber(
/// checkpointerProc)->procLatch)`). Reaches the aux PGPROC by the advertised
/// `ProcGlobal.checkpointer_proc` and sets its `proc_latch`. Returns false if no
/// checkpointer is running (the proc number is INVALID).
fn wake_checkpointer() -> bool {
    let Some(g) = ProcGlobal::get() else {
        return false;
    };
    let procno = g.checkpointer_proc.load(Ordering::Acquire);
    if procno == INVALID_PROC_NUMBER {
        return false;
    }
    // SAFETY: `proc_latch` is internally synchronized (a Latch over a Notify);
    // setting it forms no `&mut` into the slot's other field groups.
    (unsafe { g.proc(procno) })
        .inspect(|proc| proc.proc_latch.set())
        .is_some()
}

/// PG `RequestCheckpoint`: ask the checkpointer to perform a checkpoint.
///
/// `flags` is a bitwise OR of `CheckpointFlags` bits. With `WAIT` set this awaits
/// completion via the start/done CVs (modulo-compare on the counters) and errors
/// (panics, PG `ereport(ERROR)`) if the checkpoint failed. Async because the CV
/// waits `.await`.
///
/// Standalone-backend path (no checkpointer running) does the checkpoint inline,
/// mirroring PG's `!IsPostmasterEnvironment` branch.
pub async fn request_checkpoint(flags: i32) {
    let Some(shmem) = checkpointer_shmem() else {
        // No shmem published: behave like a standalone backend (do it inline).
        create_check_point(CheckpointFlags::from_bits_truncate(flags) | CheckpointFlags::IMMEDIATE);
        // Free all smgr objects, as CheckpointerMain would (checkpointer.c:1021).
        smgrdestroyall();
        return;
    };

    let wait = flags & CheckpointFlags::WAIT.bits() != 0;

    // Atomically set the request flags and snapshot the counters. Seeing
    // started > old_started later means the checkpointer has seen our flags.
    let (old_started, old_failed) = {
        let mut c = shmem.lock_ckpt();
        let old = (c.started, c.failed);
        c.flags |= flags | CheckpointFlags::REQUESTED.bits();
        old
    };

    // Wake the checkpointer. It may not have started yet; retry a few times. If
    // not waiting, a failure to wake is nonfatal (it will see the request when it
    // starts). PG retries up to MAX_SIGNAL_TRIES (600 * 0.1s).
    let mut ntries = 0;
    loop {
        if wake_checkpointer() {
            break;
        }
        if ntries >= MAX_SIGNAL_TRIES || !wait {
            // PG: elog(WAIT ? ERROR : LOG, "could not notify checkpoint: ...").
            assert!(
                !wait,
                "could not notify checkpoint: checkpointer is not running"
            );
            crate::elog!(
                crate::utils::elog::LOG,
                "could not notify checkpoint: checkpointer is not running".to_string()
            );
            break;
        }
        ntries += 1;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    if !wait {
        return;
    }

    // Wait for a new checkpoint to START (started advances past old_started).
    let new_started;
    {
        let mut s = shmem.start_cv.prepare_to_sleep();
        loop {
            let started_now = shmem.lock_ckpt().started;
            if started_now != old_started {
                new_started = started_now;
                break;
            }
            s.sleep(0).await;
        }
    }

    // Wait for it to be DONE: ckpt_done >= new_started (modulo). We reuse the
    // exact `started` value the START loop broke on (PG checkpointer.c:1093,1117)
    // -- NOT a fresh read -- so a second checkpoint starting in the gap does not
    // make us wait for the later checkpoint.
    let new_failed;
    {
        let mut s = shmem.done_cv.prepare_to_sleep();
        loop {
            let (new_done, failed_now) = {
                let c = shmem.lock_ckpt();
                (c.done, c.failed)
            };
            if new_done.wrapping_sub(new_started) >= 0 {
                new_failed = failed_now;
                break;
            }
            s.sleep(0).await;
        }
    }

    // PG ereport(ERROR) on failure. Surface as a panic (the elog-as-panic model).
    assert!(new_failed == old_failed, "checkpoint request failed");
}

/// PG `AbsorbSyncRequests`. No-op under the single-process model: backends
/// enqueue fsync requests directly into the shared `SyncRequests` queue, so there
/// is nothing to absorb here. The symbol is kept because bgwriter references it.
// PG AbsorbSyncRequests
pub fn absorb_sync_requests() {}

/// PG `FirstCallSinceLastCheckpoint`: true once per checkpoint cycle (detects
/// `done` advancing since the last call). Per-task state (PG used a function
/// `static`); the checkpointer is single, so a process-global suffices.
pub fn first_call_since_last_checkpoint() -> bool {
    let Some(shmem) = checkpointer_shmem() else {
        return false;
    };
    let new_done = shmem.lock_ckpt().done;
    let prev = FIRST_CALL_LAST_DONE.swap(new_done, Ordering::AcqRel);
    new_done != prev
}

/// PG `FirstCallSinceLastCheckpoint`'s function `static ckpt_done`.
static FIRST_CALL_LAST_DONE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// PG `RequestCheckpoint`'s `MAX_SIGNAL_TRIES` (max wait 60.0s).
const MAX_SIGNAL_TRIES: i32 = 600;

/// Private per-task checkpointer state (PG's file `static`s). Lives on the
/// `checkpointer_main` stack; threaded into the schedule helpers.
struct CheckpointerState {
    last_checkpoint_time: pg_time_t,
    last_xlog_switch_time: pg_time_t,
}

/// PG `CheckpointerMain`. The long-lived checkpointer aux task. Runs inside
/// `my_proc_scope` so `InitAuxiliaryProcess` (in the cradle) can publish its
/// PGPROC; advertises `ProcGlobal.checkpointer_proc`; then runs the timed /
/// requested checkpoint loop until `shutdown` fires.
///
/// `shutdown` is the supervisor's per-child cancel handle. A single notify breaks
/// the loop, the shutdown checkpoint is written, and the task exits immediately
/// (one-phase). The supervisor's sequenced drain uses [`checkpointer_main_phased`]
/// to split write-checkpoint (PG SIGINT/ShutdownXLOGPending) from exit (PG
/// SIGUSR2/ShutdownRequestPending).
pub async fn checkpointer_main(shared: Arc<SharedState>, shutdown: Arc<tokio::sync::Notify>) {
    // One-phase: phase-2 fires together with phase-1 so the task exits as soon as
    // the shutdown checkpoint is written. Keeps the standalone entry simple. The
    // xlog-is-shutdown completion signal has no waiter here, so use a throwaway.
    let phase2 = Arc::new(tokio::sync::Notify::new());
    phase2.notify_waiters();
    phase2.notify_one();
    let xlog_shutdown_done = Arc::new(tokio::sync::Notify::new());
    checkpointer_main_phased(shared, shutdown, phase2, xlog_shutdown_done).await;
}

/// PG's two-phase checkpointer shutdown. `phase1` (PG SIGINT, ShutdownXLOGPending)
/// breaks the main loop and triggers the shutdown checkpoint; once the checkpoint
/// is written the task fires `xlog_shutdown_done` (PG
/// `SendPostmasterSignal(PMSIGNAL_XLOG_IS_SHUTDOWN)`) so the supervisor may stop
/// the other aux tasks, then waits on `phase2` (PG SIGUSR2, ShutdownRequestPending)
/// before exiting. The supervisor (17f) drives this so the checkpointer is
/// FIRST-started, LAST-stopped: it writes the shutdown checkpoint, signals
/// completion, and only after the other aux tasks have drained does `phase2` let
/// it exit.
pub async fn checkpointer_main_phased(
    shared: Arc<SharedState>,
    phase1: Arc<tokio::sync::Notify>,
    phase2: Arc<tokio::sync::Notify>,
    xlog_shutdown_done: Arc<tokio::sync::Notify>,
) {
    let shutdown = phase1;
    my_proc_scope(async move {
        let aux = auxiliary_process_main_common_with_proc(
            shared.proc_signal(),
            BackendType::CHECKPOINTER,
        )
        .await;

        // Use the PROCESS-published CheckpointerShmem (the one arbitrary backends
        // reach via request_checkpoint), not the SharedState handle, so the
        // request<->checkpointer counter protocol always agrees on one struct.
        // SharedState::new publishes its own at construction; first publish wins.
        let shmem = checkpointer_shmem()
            .cloned()
            .unwrap_or_else(|| shared.checkpointer().clone());

        // Advertise our proc number so backends can wake us (PG:
        // ProcGlobal->checkpointerProc = MyProcNumber).
        let g = ProcGlobal::expect().clone();
        g.checkpointer_proc.store(aux.proc_number, Ordering::Release);

        // Cleanup runs on EVERY exit -- normal break, early break, AND a panic
        // unwind out of the loop (e.g. a checkpoint-failure re-raise). Without
        // this an unwind would leave ProcGlobal.checkpointer_proc pointing at a
        // dead proc, so a later request_checkpoint(WAIT) would ring a dead latch
        // and hang. The guard clears the advertised proc, deregisters the
        // proc-signal slot, and returns the aux PGPROC (ProcKill).
        let _exit = CheckpointerExitGuard {
            g: g.clone(),
            proc_signal: shared.proc_signal().clone(),
            slot_key: aux.slot_key,
        };

        // Our single wakeup latch IS our PGPROC's proc_latch (PG MyLatch ==
        // MyProc->procLatch for an aux proc). request_checkpoint rings it via
        // ProcGlobal.checkpointer_proc; the proc-signal slot (barrier/config
        // sends) was registered against this SAME latch in the cradle, so both
        // wakeup sources hit one latch -- the loop waits AND resets only it (no
        // second sticky latch to busy-spin on).
        let proc_latch: &Latch = &aux.latch;

        let now = current_time();
        let mut st = CheckpointerState {
            last_checkpoint_time: now,
            last_xlog_switch_time: now,
        };

        // PG UpdateSharedMemoryConfig() at startup.
        update_shared_memory_config();

        // --- main loop: until asked to write the shutdown checkpoint / exit ---
        // Every exit from this loop is a shutdown (PG breaks only on
        // ShutdownXLOGPending / ShutdownRequestPending), so the loop yields the
        // shutdown_pending flag.
        let shutdown_pending: bool = loop {
            // PG ResetLatch(MyLatch) at loop top.
            proc_latch.reset();

            // PG AbsorbSyncRequests() -- no-op (shared queue).
            absorb_sync_requests();

            // PG ProcessCheckpointerInterrupts(); break on shutdown. The cancel
            // Notify is the supervisor's shutdown signal (PG SIGINT/SIGUSR2); a
            // non-blocking poll consumes a permit left by `notify_one` so a
            // shutdown that arrives between sleeps is not missed at loop top.
            if process_checkpointer_interrupts() || shutdown_now(&shutdown) {
                break true;
            }

            let mut do_checkpoint = false;
            let mut flags = CheckpointFlags::empty();

            // Pending request? (flags word nonzero.)
            if shmem.lock_ckpt().flags != 0 {
                do_checkpoint = true;
            }

            // Force a checkpoint if too much time has elapsed.
            let now = current_time();
            let elapsed_secs = (now - st.last_checkpoint_time) as i32;
            if elapsed_secs >= check_point_timeout() {
                do_checkpoint = true;
                flags |= CheckpointFlags::CAUSE_TIME;
            }

            if do_checkpoint {
                do_checkpoint_cycle(&shared, &shmem, flags, elapsed_secs, now, &mut st).await;

                // We may have been asked to shut down during the checkpoint.
                if process_checkpointer_interrupts() || shutdown_now(&shutdown) {
                    break true;
                }
            }

            // Check for archive_timeout and switch xlog files if necessary.
            check_archive_timeout(&mut st);

            // If a request arrived, redo the loop without sleeping.
            if shmem.lock_ckpt().flags != 0 {
                continue;
            }

            // Sleep until signaled or it's time for another checkpoint / xlog
            // switch.
            let now = current_time();
            let elapsed_secs = (now - st.last_checkpoint_time) as i32;
            if elapsed_secs >= check_point_timeout() {
                continue; // no sleep
            }
            let mut cur_timeout = check_point_timeout() - elapsed_secs;
            if xlog_archive_timeout() > 0 && !recovery_in_progress() {
                let elapsed = (now - st.last_xlog_switch_time) as i32;
                if elapsed >= xlog_archive_timeout() {
                    continue;
                }
                cur_timeout = cur_timeout.min(xlog_archive_timeout() - elapsed);
            }

            let sleep = tokio::time::sleep(std::time::Duration::from_secs(cur_timeout.max(0) as u64));
            tokio::select! {
                biased;
                () = proc_latch.wait() => {}
                () = sleep => {}
                () = shutdown.notified() => { break true; }
            }
        };

        // From here on an error should end the task, not loop (PG ExitOnAnyError).
        if shutdown_pending {
            // PG ShutdownXLOGPending: write the shutdown checkpoint, then tell the
            // postmaster we're done. ShutdownXLOG is an xlog stub; we drain the
            // real fsync queue as CheckPointGuts would.
            // TODO(xlog): normally inside ShutdownXLOG -> CreateCheckPoint.
            // SyncPreCheckpoint bumps the unlink cycle (CreateCheckPoint top,
            // xlog.c:6970) so SyncPostCheckpoint unlinks the PRIOR cycle's files.
            SyncPreCheckpoint(&shared);
            create_check_point(CheckpointFlags::IS_SHUTDOWN | CheckpointFlags::IMMEDIATE);
            ProcessSyncRequests(&shared).await;
            SyncPostCheckpoint(&shared).await;
            #[cfg(test)]
            tests::SHUTDOWN_CKPT_WRITTEN.store(true, Ordering::Release);
        }

        // PG SendPostmasterSignal(PMSIGNAL_XLOG_IS_SHUTDOWN): tell the supervisor
        // WAL is shut down so it may transition PM_WAIT_XLOG_SHUTDOWN ->
        // PM_WAIT_XLOG_ARCHIVAL and stop the other aux tasks. Sticky (waiters +
        // permit) so a drain that awaits after this still observes it.
        xlog_shutdown_done.notify_waiters();
        xlog_shutdown_done.notify_one();

        // PG's second loop waits for SIGUSR2 (ShutdownRequestPending) before exit.
        // `phase2` is that signal: the supervisor fires it only after the other
        // aux tasks have drained, so the checkpointer is LAST to stop. The
        // one-phase `checkpointer_main` pre-fires it, so this returns at once.
        // `_exit` (above) clears the advertised proc number, deregisters the slot,
        // and returns the PGPROC on drop -- on this path and on any panic unwind.
        phase2.notified().await;
    })
    .await;
}

/// Runs the checkpointer's exit cleanup on EVERY scope exit -- normal return and
/// panic unwind. Idempotent: ProcKill no-ops once the proc is returned, and a
/// stale slot key / already-cleared proc number are harmless to re-clear, so a
/// future explicit-cleanup path could not double-free.
struct CheckpointerExitGuard {
    g: Arc<crate::storage::proc::ProcGlobal>,
    proc_signal: Arc<crate::backend::storage::ipc::procsignal::ProcSignal>,
    slot_key: crate::backend::storage::ipc::procsignal::SlotKey,
}

impl Drop for CheckpointerExitGuard {
    fn drop(&mut self) {
        // Stop backends from waking a dead proc, then drop the slot.
        self.g
            .checkpointer_proc
            .store(INVALID_PROC_NUMBER, Ordering::Release);
        self.proc_signal.deregister(self.slot_key);
        // Return the aux PGPROC to its pool (no-op if already returned).
        crate::storage::proc::ProcKill();
    }
}

/// One do-checkpoint cycle: bump `started` + broadcast `start_cv`, run the
/// checkpoint (guarded so a failure bumps `failed`), drain the real fsync queue,
/// set `done = started` + broadcast `done_cv`. Mirrors the `if (do_checkpoint)`
/// body of CheckpointerMain.
async fn do_checkpoint_cycle(
    shared: &Arc<SharedState>,
    shmem: &Arc<CheckpointerShmem>,
    extra_flags: CheckpointFlags,
    elapsed_secs: i32,
    now: pg_time_t,
    st: &mut CheckpointerState,
) {
    let do_restartpoint = recovery_in_progress();

    // Atomically fetch the request flags and bump started.
    let flags = {
        let mut c = shmem.lock_ckpt();
        let flags = extra_flags | CheckpointFlags::from_bits_truncate(c.flags);
        c.flags = 0;
        c.started = c.started.wrapping_add(1);
        flags
    };
    shmem.start_cv.broadcast();

    let do_restartpoint = do_restartpoint && !flags.contains(CheckpointFlags::END_OF_RECOVERY);

    // Warn if checkpoints occur too frequently (CAUSE_XLOG + too soon).
    if !do_restartpoint
        && flags.contains(CheckpointFlags::CAUSE_XLOG)
        && elapsed_secs < check_point_warning()
    {
        crate::elog!(
            crate::utils::elog::LOG,
            format!("checkpoints are occurring too frequently ({elapsed_secs} seconds apart)")
        );
    }

    // Run the checkpoint. PG's sigsetjmp catch bumps ckpt_failed on error; we
    // guard with catch_unwind so a panicking stub still leaves the counters
    // consistent and wakes waiters with a failure.
    let ckpt_result = {
        use futures_util::FutureExt;
        let fut = async {
            // Test-only fault injection: make the checkpoint body panic to
            // exercise the ckpt_failed path (see failure-path test).
            #[cfg(test)]
            assert!(
                !tests::FAIL_NEXT_CHECKPOINT.swap(false, Ordering::AcqRel),
                "injected checkpoint failure"
            );
            if do_restartpoint {
                create_restart_point(flags)
            } else {
                // SyncPreCheckpoint bumps the unlink cycle (CreateCheckPoint top,
                // xlog.c:6970) so SyncPostCheckpoint unlinks only PRIOR-cycle files.
                SyncPreCheckpoint(shared);
                create_check_point(flags)
            }
            // TODO(xlog): normally inside CreateCheckPoint->CheckPointGuts->smgrsync.
            // Drain the REAL fsync/unlink queue (single-process: the shared
            // SyncRequests queue that backends enqueue into directly).
            ;
            ProcessSyncRequests(shared).await;
            SyncPostCheckpoint(shared).await;
            true
        };
        std::panic::AssertUnwindSafe(fut).catch_unwind().await
    };

    match ckpt_result {
        Ok(_ckpt_performed) => {
            // After any checkpoint free all smgr objects, else dropped-relation
            // handles leak (the checkpointer sees no invalidation; checkpointer.c:490).
            smgrdestroyall();
            // Indicate completion (done = started) and wake waiters.
            {
                let mut c = shmem.lock_ckpt();
                c.done = c.started;
            }
            shmem.done_cv.broadcast();
            if !do_restartpoint {
                st.last_checkpoint_time = now;
            }
        }
        Err(payload) => {
            // PG's ckpt_failed path: bump failed, set done = started, wake waiters.
            {
                let mut c = shmem.lock_ckpt();
                c.failed = c.failed.wrapping_add(1);
                c.done = c.started;
            }
            shmem.done_cv.broadcast();
            crate::elog!(
                crate::utils::elog::LOG,
                "checkpoint failed".to_string()
            );
            // Re-raise after notifying waiters so the task boundary / supervisor
            // sees it (PG would have longjmp'd and napped 1s before retrying).
            std::panic::resume_unwind(payload);
        }
    }
}

/// PG `ProcessCheckpointerInterrupts`. Returns true if shutdown was requested
/// (the caller breaks the loop). Config-reload re-runs UpdateSharedMemoryConfig.
fn process_checkpointer_interrupts() -> bool {
    let shutdown = process_main_loop_interrupts();
    if shutdown {
        return true;
    }
    // PG: on SIGHUP, ProcessConfigFile + UpdateSharedMemoryConfig. The config
    // reload flag is cleared inside process_main_loop_interrupts; mirror the
    // shmem-config update unconditionally is wrong, so only the shutdown signal
    // is surfaced here. UpdateSharedMemoryConfig runs at startup + on reload (17
    // GUC wiring).
    false
}

/// True if the supervisor has asked this task to shut down (PG's
/// `ShutdownRequestPending` check at loop top). Non-blocking: polls
/// `shutdown.notified()` once, consuming a permit left by `notify_one`, so a
/// shutdown that arrived between sleeps is observed here without awaiting.
fn shutdown_now(shutdown: &Arc<tokio::sync::Notify>) -> bool {
    use futures_util::FutureExt;
    let fut = shutdown.notified();
    futures_util::pin_mut!(fut);
    fut.now_or_never().is_some()
}

/// PG `CheckArchiveTimeout`. Switch to a new WAL file and force an archive write
/// if meaningful activity occurred. Returns early when archiving is disabled (the
/// default), so the xlog-switch stubs are never reached on a timer.
fn check_archive_timeout(st: &mut CheckpointerState) {
    if xlog_archive_timeout() <= 0 || recovery_in_progress() {
        return;
    }
    let now = current_time();
    if (now - st.last_xlog_switch_time) < i64::from(xlog_archive_timeout()) {
        return;
    }
    // TODO(xlog): GetLastSegSwitchData / GetLastImportantRecPtr / RequestXLogSwitch
    // are xlog stubs; archive_timeout is off by default so this branch is unused.
    st.last_xlog_switch_time = now;
}

/// PG `UpdateSharedMemoryConfig`: push config-derived shared values (sync-rep
/// standbys-defined, full_page_writes). The underlying calls are syncrep/xlog
/// stubs; this is a faithful no-op shell until those land (17 GUC wiring).
fn update_shared_memory_config() {
    // TODO(syncrep): SyncRepUpdateSyncStandbysDefined().
    // TODO(xlog): UpdateFullPageWrites().
}

/// PG `IsCheckpointOnSchedule`: whether progress is ahead of elapsed time/WAL.
/// The WAL-insert progress source is an xlog stub; until it lands this reports
/// "on schedule" (no throttling), matching create_check_point being a no-op.
#[allow(dead_code, reason = "called by CheckpointWriteDelay once BufferSync lands (17/bufmgr)")]
fn is_checkpoint_on_schedule(_progress: f64) -> bool {
    true
}

/// PG `CheckpointWriteDelay`: throttle BufferSync's write rate. Called from
/// BufferSync (bufmgr, deferred). Kept as the public symbol; the throttling body
/// is a no-op until BufferSync lands.
#[allow(dead_code, reason = "called by BufferSync (17/bufmgr) once it lands")]
pub async fn checkpoint_write_delay(_flags: i32, _progress: f64) {
    // TODO(bufmgr): the on-schedule nap + AbsorbSyncRequests cadence.
}

/// PG `(pg_time_t) time(NULL)`: current wall-clock seconds.
fn current_time() -> pg_time_t {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as pg_time_t)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::shared_state::SharedStateConfig;
    use crate::storage::sync::{FileTag, SyncRequestHandler, SyncRequestType};
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    /// Test-only fault injection: when set, the next checkpoint body panics
    /// (consumed by the swap). Drives the ckpt_failed path in `do_checkpoint_cycle`.
    pub(super) static FAIL_NEXT_CHECKPOINT: AtomicBool = AtomicBool::new(false);

    /// Test-only flag: set once the shutdown checkpoint has been written (post-
    /// loop, before phase-2 exit). Lets the 17f two-phase test assert ordering.
    pub static SHUTDOWN_CKPT_WRITTEN: AtomicBool = AtomicBool::new(false);

    /// Build a fresh SharedState and best-effort publish its ProcGlobal +
    /// CheckpointerShmem to the process OnceLocks. The OnceLocks ignore a second
    /// publish (first SharedState in the test process wins), so the checkpointer
    /// task + request_checkpoint operate on the PUBLISHED arena/shmem, which the
    /// tests therefore read via `ProcGlobal::get()` / `checkpointer_shmem()` rather
    /// than `shared.*`. The per-test `shared` still owns the sync_requests queue
    /// the task drains, which is independent of the OnceLocks.
    fn fresh_shared() -> Arc<SharedState> {
        let shared = SharedState::new(SharedStateConfig::default());
        let _ = crate::storage::proc::ProcGlobal::set(shared.proc_global().clone());
        let _ = set_checkpointer_shmem(shared.checkpointer().clone());
        shared
    }

    /// The process-published ProcGlobal the checkpointer advertises on.
    fn published_proc_global() -> Arc<crate::storage::proc::ProcGlobal> {
        crate::storage::proc::ProcGlobal::get()
            .expect("a ProcGlobal is published")
            .clone()
    }

    fn md_tag(rel: u32) -> FileTag {
        FileTag {
            handler: SyncRequestHandler::Md as i16,
            forknum: crate::common::relpath::ForkNumber::MAIN_FORKNUM as i16,
            rlocator: crate::storage::relfilelocator::RelFileLocator {
                spcOid: crate::postgres_ext::Oid(1663),
                dbOid: crate::postgres_ext::Oid(5),
                relNumber: crate::postgres_ext::Oid(rel),
            },
            segno: 0,
        }
    }

    /// Serialize across ALL aux-task tests (not just checkpointer): they share the
    /// single process-wide `ProcGlobal` + aux PGPROC slots + the
    /// `ProcGlobal.<role>_proc` advertisements, so a bgwriter/walwriter task in
    /// another module must not run concurrently with a checkpointer WAIT.
    use crate::backend::postmaster::auxprocess::aux_test_serial as test_serial;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_starts_advertises_and_parks() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let g = published_proc_global();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        // Long timeout so the task parks on its latch (no 300s default nap path).
        set_check_point_timeout(3600);

        let task = tokio::spawn(checkpointer_main(shared.clone(), shutdown.clone()));

        // The task advertises ProcGlobal.checkpointer_proc.
        let advertised = wait_for(
            || g.checkpointer_proc.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
            Duration::from_secs(2),
        )
        .await;
        assert!(advertised, "checkpointer should advertise its proc number");

        // Shut it down and confirm it exits + clears its proc number.
        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("checkpointer should exit on shutdown")
            .expect("task panicked");
        assert_eq!(
            g.checkpointer_proc.load(Ordering::Acquire),
            INVALID_PROC_NUMBER,
            "proc number cleared on exit"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn request_wait_wakes_and_completes_and_drains_queue() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let g = published_proc_global();
        let shmem = checkpointer_shmem().expect("a CheckpointerShmem is published").clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        set_check_point_timeout(3600);

        // Enqueue a sync request a checkpoint must drain (the task drains THIS
        // test's shared queue, independent of the OnceLocks).
        crate::backend::storage::sync::sync::RegisterSyncRequest(
            &shared,
            &md_tag(42),
            SyncRequestType::SyncRequest,
            false,
        );
        assert_eq!(shared.sync_requests().pending_op_count(), 1);

        let task = tokio::spawn(checkpointer_main(shared.clone(), shutdown.clone()));
        assert!(
            wait_for(
                || g.checkpointer_proc.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(2)
            )
            .await,
            "checkpointer running"
        );

        // Baselines: the process-wide CheckpointerShmem persists across tests, so
        // compare counters RELATIVE to a snapshot, not absolute zero.
        let (started_before, failed_before) = {
            let c = shmem.lock_ckpt();
            (c.started, c.failed)
        };

        // Request a checkpoint and WAIT for it.
        tokio::time::timeout(
            Duration::from_secs(5),
            request_checkpoint(CheckpointFlags::WAIT.bits() | CheckpointFlags::IMMEDIATE.bits()),
        )
        .await
        .expect("request_checkpoint(WAIT) should return after a checkpoint");

        // started/done advanced.
        {
            let c = shmem.lock_ckpt();
            assert!(c.started.wrapping_sub(started_before) >= 1, "started advanced");
            assert_eq!(c.done, c.started, "done caught up to started");
            assert_eq!(c.failed, failed_before, "no failure");
        }

        // The fsync queue was drained by ProcessSyncRequests inside the checkpoint.
        // (md sync of a nonexistent file is treated as NotFound -> forgotten.)
        assert_eq!(
            shared.sync_requests().pending_op_count(),
            0,
            "checkpoint drains the shared SyncRequests queue"
        );

        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("checkpointer exits")
            .expect("task panicked");
    }

    /// MINOR-1 regression: two sequential WAIT requests each advance
    /// started/done by exactly one; the second waiter does not return on the
    /// first checkpoint (it carries its own observed `started` into the DONE
    /// wait).
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn two_sequential_waits_each_advance_once() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let g = published_proc_global();
        let shmem = checkpointer_shmem().expect("a CheckpointerShmem is published").clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        set_check_point_timeout(3600);

        let task = tokio::spawn(checkpointer_main(shared.clone(), shutdown.clone()));
        assert!(
            wait_for(
                || g.checkpointer_proc.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(2)
            )
            .await,
            "checkpointer running"
        );

        // Baselines: the process-wide CheckpointerShmem persists across tests.
        let (s0, d0, f0) = {
            let c = shmem.lock_ckpt();
            (c.started, c.done, c.failed)
        };

        for i in 1..=2 {
            tokio::time::timeout(
                Duration::from_secs(5),
                request_checkpoint(CheckpointFlags::WAIT.bits() | CheckpointFlags::IMMEDIATE.bits()),
            )
            .await
            .expect("request_checkpoint(WAIT) returns after a checkpoint");
            let c = shmem.lock_ckpt();
            assert_eq!(c.started.wrapping_sub(s0), i, "started advanced once per request");
            assert_eq!(c.done.wrapping_sub(d0), i, "done advanced once per request");
            assert_eq!(c.failed, f0, "no failure");
        }

        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("checkpointer exits")
            .expect("task panicked");
    }

    /// MINOR-3 gap: a checkpoint body panic bumps failed+done+broadcasts, so a
    /// concurrent WAIT observes the failure (panics with "checkpoint request
    /// failed") rather than hanging. Also confirms the exit guard cleared the
    /// advertised proc on the unwind path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn request_wait_observes_checkpoint_failure() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let g = published_proc_global();
        let shmem = checkpointer_shmem().expect("a CheckpointerShmem is published").clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        set_check_point_timeout(3600);

        // Arm the fault: the next checkpoint body panics.
        FAIL_NEXT_CHECKPOINT.store(true, Ordering::Release);

        // The checkpointer task re-raises the failure; swallow its panic.
        let task = tokio::spawn(checkpointer_main(shared.clone(), shutdown.clone()));
        assert!(
            wait_for(
                || g.checkpointer_proc.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(2)
            )
            .await,
            "checkpointer running"
        );

        let failed_before = shmem.lock_ckpt().failed;

        // The WAIT must NOT hang: it returns by panicking ("checkpoint request
        // failed") once the checkpointer bumps failed+done and broadcasts.
        let waiter = tokio::spawn(request_checkpoint(
            CheckpointFlags::WAIT.bits() | CheckpointFlags::IMMEDIATE.bits(),
        ));
        let joined = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("WAIT must not hang on a failed checkpoint");
        assert!(joined.is_err(), "WAIT surfaces the failure as a panic");

        // The failed counter advanced (failure path ran).
        assert!(
            shmem.lock_ckpt().failed.wrapping_sub(failed_before) >= 1,
            "ckpt_failed advanced"
        );

        // The exit guard cleared the advertised proc on the panic unwind path.
        assert!(
            wait_for(
                || g.checkpointer_proc.load(Ordering::Acquire) == INVALID_PROC_NUMBER,
                Duration::from_secs(5)
            )
            .await,
            "checkpointer_proc cleared on the panic exit path"
        );

        // The checkpointer task ended by panicking (re-raised after notifying).
        shutdown.notify_waiters();
        shutdown.notify_one();
        let task_join = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("checkpointer task ends");
        assert!(task_join.is_err(), "checkpointer task ended via panic");
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
}
