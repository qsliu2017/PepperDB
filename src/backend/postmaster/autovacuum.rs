//! The integrated autovacuum daemon. Translated from backend/postmaster/autovacuum.c.
//!
//! Autovacuum is structured around two kinds of long-lived workers: the
//! launcher and the worker. The launcher is an always-running task, started
//! while the `autovacuum` setting is enabled. It maintains a per-database
//! schedule ordered by next wakeup and, once it decides a database is due,
//! arranges for a worker to be started on it. The launcher does not start
//! workers directly: it records the database it wants vacuumed and requests a
//! worker, leaving the actual launch to the surrounding supervisor. A worker
//! claims the slot the launcher set up, connects to the chosen database,
//! examines the catalogs to pick the tables to vacuum, and runs the per-table
//! vacuum and analyze. When a worker finishes it returns its slot to the free
//! list and wakes the launcher, which can then launch another worker if the
//! schedule is tight and can rebalance the cost-based vacuum delay across the
//! remaining workers.
//!
//! More than one worker can be active in a single database at once. Each worker
//! records the table it is currently vacuuming in shared state so that the
//! others avoid blocking on the same relation, and consults the latest vacuum
//! statistics just before each table to skip work another worker has already
//! done.
//!
//! Unlike PostgreSQL, PepperDB runs as a single process, so the multi-process
//! machinery collapses. The shared-memory autovacuum area becomes an
//! `Arc`-shared state struct published process-wide through a `OnceLock`; the
//! launcher's and workers' intrusive worker lists become index lists over a
//! fixed slot array, all guarded by one mutex that stands in for PostgreSQL's
//! separate autovacuum locks. There is no postmaster fork-and-signal handshake
//! to start a worker: the launcher records the request and asks the supervisor
//! to spawn the worker task; if no spawn hook is installed (as in unit tests)
//! the request is still recorded faithfully but no worker runs. The launcher
//! advertises its identity in the shared process table rather than by process
//! id, and a finishing worker notifies it directly. PostgreSQL's per-iteration
//! `sigsetjmp` error recovery is not reproduced: an error propagates as a panic
//! to the task boundary, where the supervisor restarts the task.
//!
//! The catalog-driven core - building the database list, choosing a database,
//! and the per-table vacuum and analyze scan - is translated in full but
//! depends on catalog access, vacuum, and statistics machinery that is not yet
//! implemented and will panic if reached. To keep the launcher from being
//! driven into those paths, autovacuum defaults off and the launcher is only
//! spawned when it is explicitly enabled and a real catalog is present.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct; the real GETSTRUCT returns a MAXALIGN'd pointer (staged until it lands)"
)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, MutexGuard};

use crate::access::transam::ReadNextTransactionId;
use crate::backend::postmaster::auxprocess::{
    auxiliary_process_main_common_with_proc, process_main_loop_interrupts,
};
use crate::c::{FirstMultiXactId, FirstNormalTransactionId, MultiXactId, TransactionId};
use crate::miscadmin::BackendType;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::postmaster::autovacuum::AutoVacuumWorkItemType;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::latch::Latch;
use crate::storage::proc::{my_proc_scope, ProcGlobal};
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};
use crate::utils::timestamp::GetCurrentTimestamp;

use crate::datatype::timestamp::{TimestampTz, USECS_PER_SEC};

/// PG `MIN_AUTOVAC_SLEEPTIME` (ms): minimum time between launcher wakeups.
const MIN_AUTOVAC_SLEEPTIME: f64 = 100.0;
/// PG `MAX_AUTOVAC_SLEEPTIME` (s): clamp on the launcher's nap.
const MAX_AUTOVAC_SLEEPTIME: i64 = 300;
/// PG `NUM_WORKITEMS`: size of the work-item array.
const NUM_WORKITEMS: usize = 256;

// ---------------------------------------------------------------------------
// GUC parameters (PG globals). Process-global atomics with accessors (no
// `static mut`); settable for tests. TODO(guc): drive from the GUC bridge.
// ---------------------------------------------------------------------------

/// PG `autovacuum_start_daemon`.
static AUTOVACUUM_START_DAEMON: AtomicBool = AtomicBool::new(false);
/// PG `autovacuum_worker_slots` (total worker slots; default 16).
static AUTOVACUUM_WORKER_SLOTS: AtomicI32 = AtomicI32::new(16);
/// PG `autovacuum_max_workers` (concurrent workers; default 3).
static AUTOVACUUM_MAX_WORKERS: AtomicI32 = AtomicI32::new(3);
/// PG `autovacuum_naptime` (s between launcher runs; default 60).
static AUTOVACUUM_NAPTIME: AtomicI32 = AtomicI32::new(60);
/// PG `autovacuum_freeze_max_age` (default 200M).
static AUTOVACUUM_FREEZE_MAX_AGE: AtomicI32 = AtomicI32::new(200_000_000);
/// PG `autovacuum_vac_thresh` (default 50).
static AUTOVACUUM_VAC_THRESH: AtomicI32 = AtomicI32::new(50);
/// PG `autovacuum_anl_thresh` (default 50).
static AUTOVACUUM_ANL_THRESH: AtomicI32 = AtomicI32::new(50);
/// PG `pgstat_track_counts`. AutoVacuumingActive requires it.
static PGSTAT_TRACK_COUNTS: AtomicBool = AtomicBool::new(true);

/// PG `recentXid` / `recentMulti`: the worker refreshes these before do_autovacuum
/// (and do_start_worker sets them on the launcher) so relation_needs_vacanalyze can
/// compute the anti-wraparound force limits. File statics in C; process atomics
/// here (only the running worker/launcher reads/writes them).
static RECENT_XID: AtomicU32 = AtomicU32::new(0);
static RECENT_MULTI: AtomicU32 = AtomicU32::new(0);

/// PG `autovacuum_vac_scale` (default 0.2).
static AUTOVACUUM_VAC_SCALE_MILLI: AtomicI32 = AtomicI32::new(200);
/// PG `autovacuum_anl_scale` (default 0.1).
static AUTOVACUUM_ANL_SCALE_MILLI: AtomicI32 = AtomicI32::new(100);

/// PG `autovacuum_freeze_max_age`.
fn autovacuum_freeze_max_age() -> i32 {
    AUTOVACUUM_FREEZE_MAX_AGE.load(Ordering::Relaxed)
}
/// PG `autovacuum_vac_thresh`.
fn autovacuum_vac_thresh() -> i32 {
    AUTOVACUUM_VAC_THRESH.load(Ordering::Relaxed)
}
/// PG `autovacuum_anl_thresh`.
fn autovacuum_anl_thresh() -> i32 {
    AUTOVACUUM_ANL_THRESH.load(Ordering::Relaxed)
}
/// PG `autovacuum_vac_scale` (stored as integer permille).
fn autovacuum_vac_scale() -> f64 {
    f64::from(AUTOVACUUM_VAC_SCALE_MILLI.load(Ordering::Relaxed)) / 1000.0
}
/// PG `autovacuum_anl_scale` (stored as integer permille).
fn autovacuum_anl_scale() -> f64 {
    f64::from(AUTOVACUUM_ANL_SCALE_MILLI.load(Ordering::Relaxed)) / 1000.0
}

/// PG `autovacuum_naptime`.
pub fn autovacuum_naptime() -> i32 {
    AUTOVACUUM_NAPTIME.load(Ordering::Relaxed)
}

/// Set `autovacuum_naptime` (GUC assignment / tests).
pub fn set_autovacuum_naptime(secs: i32) {
    AUTOVACUUM_NAPTIME.store(secs, Ordering::Relaxed);
}

/// PG `autovacuum_max_workers`.
pub fn autovacuum_max_workers() -> i32 {
    AUTOVACUUM_MAX_WORKERS.load(Ordering::Relaxed)
}

/// PG `autovacuum_worker_slots`.
pub fn autovacuum_worker_slots() -> i32 {
    AUTOVACUUM_WORKER_SLOTS.load(Ordering::Relaxed)
}

/// Set `autovacuum_start_daemon` (GUC assignment / tests).
pub fn set_autovacuum_start_daemon(on: bool) {
    AUTOVACUUM_START_DAEMON.store(on, Ordering::Relaxed);
}

/// PG `AutoVacuumingActive`: whether the autovacuum daemon should be running.
pub fn auto_vacuuming_active() -> bool {
    AUTOVACUUM_START_DAEMON.load(Ordering::Relaxed) && PGSTAT_TRACK_COUNTS.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Shared structs
// ---------------------------------------------------------------------------

/// PG `WorkerInfoData`. One per worker slot. `wi_links` (the C dlist node) is
/// gone -- membership is tracked by the index lists in [`AutoVacuumShmemInner`].
/// `wi_proc` becomes the worker's `ProcNumber` (INVALID when not started).
#[derive(Clone, Copy)]
pub struct WorkerInfoData {
    /// OID of the database this worker is to work on.
    pub wi_dboid: Oid,
    /// OID of the table currently being vacuumed, if any.
    pub wi_tableoid: Oid,
    /// ProcNumber of the running worker (PG `wi_proc`), INVALID if not started.
    pub wi_proc: ProcNumber,
    /// Time at which this worker was launched.
    pub wi_launchtime: TimestampTz,
    /// Whether this worker is included in cost-balance calculations
    /// (PG `wi_dobalance`, an atomic flag; here a plain bool under the lock).
    pub wi_dobalance: bool,
    /// Whether the current table is marked relisshared.
    pub wi_sharedrel: bool,
}

impl WorkerInfoData {
    const fn empty() -> Self {
        Self {
            wi_dboid: InvalidOid,
            wi_tableoid: InvalidOid,
            wi_proc: INVALID_PROC_NUMBER,
            wi_launchtime: 0,
            wi_dobalance: false,
            wi_sharedrel: false,
        }
    }
}

/// PG `AutoVacuumSignal` indices into `av_signal[]`.
#[derive(Clone, Copy)]
pub enum AutoVacuumSignal {
    /// Failed trying to start a worker.
    ForkFailed = 0,
    /// Rebalance the cost limits.
    Rebalance = 1,
}
/// PG `AutoVacNumSignals`.
const AUTOVAC_NUM_SIGNALS: usize = 2;

/// PG `AutoVacuumWorkItem`. An entry in `av_workItems`.
#[derive(Clone, Copy)]
pub struct AutoVacuumWorkItem {
    pub avw_type: AutoVacuumWorkItemType,
    /// Below data is valid.
    pub avw_used: bool,
    /// Being processed.
    pub avw_active: bool,
    pub avw_database: Oid,
    pub avw_relation: Oid,
    pub avw_block_number: BlockNumber,
}

impl AutoVacuumWorkItem {
    const fn empty() -> Self {
        Self {
            avw_type: AutoVacuumWorkItemType::AVW_BRINSummarizeRange,
            avw_used: false,
            avw_active: false,
            avw_database: InvalidOid,
            avw_relation: InvalidOid,
            avw_block_number: 0,
        }
    }
}

/// The lock-guarded interior of [`AutoVacuumShmem`] (PG `AutovacuumLock`). The C
/// `dclist av_freeWorkers` / `dlist av_runningWorkers` / `WorkerInfo
/// av_startingWorker` become index lists over `workers`; the work-item array
/// lives here too.
pub struct AutoVacuumShmemInner {
    /// PG `av_freeWorkers`: indices of free worker slots.
    pub free_workers: Vec<usize>,
    /// PG `av_runningWorkers`: indices of running worker slots.
    pub running_workers: Vec<usize>,
    /// PG `av_startingWorker`: index of the slot a worker is being started in.
    pub starting_worker: Option<usize>,
    /// The worker slot array (PG's `WorkerInfoData[]` after the shmem struct).
    pub workers: Vec<WorkerInfoData>,
    /// PG `av_workItems`.
    pub work_items: Vec<AutoVacuumWorkItem>,
}

/// PG `AutoVacuumShmemStruct`. An `Arc` on [`SharedState`] AND published process-
/// wide via [`set_autovacuum_shmem`] so the launcher / workers / backends
/// (`AutoVacuumRequestWork`) reach one struct without a `SharedState` handle.
pub struct AutoVacuumShmem {
    /// PG `av_signal[]`: flags set by other processes without locking.
    pub av_signal: [AtomicBool; AUTOVAC_NUM_SIGNALS],
    /// PG `av_nworkersForBalance`.
    pub av_nworkers_for_balance: AtomicU32,
    /// Everything protected by `AutovacuumLock`.
    inner: Mutex<AutoVacuumShmemInner>,
}

impl AutoVacuumShmem {
    /// PG `AutoVacuumShmemInit`: build the worker slot array and seed the freelist
    /// to `autovacuum_worker_slots` (PG's `IsUnderPostmaster` init branch).
    pub fn new() -> Arc<Self> {
        let nslots = autovacuum_worker_slots().max(0) as usize;
        let workers = vec![WorkerInfoData::empty(); nslots];
        // PG pushes onto the freelist head in slot order, so the list ends up in
        // reverse; the order does not matter functionally (dclist_pop_head picks
        // any free slot). Keep ascending for readability.
        let free_workers = (0..nslots).collect();
        Arc::new(Self {
            av_signal: [const { AtomicBool::new(false) }; AUTOVAC_NUM_SIGNALS],
            av_nworkers_for_balance: AtomicU32::new(0),
            inner: Mutex::new(AutoVacuumShmemInner {
                free_workers,
                running_workers: Vec::new(),
                starting_worker: None,
                workers,
                work_items: vec![AutoVacuumWorkItem::empty(); NUM_WORKITEMS],
            }),
        })
    }

    /// Lock the interior. PG guards it with `AutovacuumLock`; a panic on a worker
    /// task (which the supervisor restarts) must not brick the lists via poison,
    /// so recover the guard if poisoned (every critical section is a complete,
    /// short update).
    fn lock(&self) -> MutexGuard<'_, AutoVacuumShmemInner> {
        self.inner.lock()
    }
}

/// The process-wide `AutoVacuumShmem` (PG's `static AutoVacuumShmem`). First
/// publish wins (tests build multiple `SharedState`s).
static AUTOVACUUM_SHMEM: OnceLock<Arc<AutoVacuumShmem>> = OnceLock::new();

/// Publish the process-wide `AutoVacuumShmem`. First publish wins; returns whether
/// this call won.
pub fn set_autovacuum_shmem(shmem: Arc<AutoVacuumShmem>) -> bool {
    AUTOVACUUM_SHMEM.set(shmem).is_ok()
}

/// The process-wide `AutoVacuumShmem`, if `SharedState::new` has run.
pub fn autovacuum_shmem() -> Option<&'static Arc<AutoVacuumShmem>> {
    AUTOVACUUM_SHMEM.get()
}

// ---------------------------------------------------------------------------
// Worker-launch hook (PG's SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER))
// ---------------------------------------------------------------------------

/// A hook the supervisor (17f) installs so the launcher can request a worker task
/// be spawned. Mirrors PG asking the postmaster to fork the worker (the launcher
/// must not fork directly). `dbid` is the database the worker should process.
pub type AutovacWorkerSpawner = Box<dyn Fn(Oid) + Send + Sync + 'static>;

static WORKER_SPAWNER: OnceLock<AutovacWorkerSpawner> = OnceLock::new();

/// Install the worker-spawn hook (17f). First install wins.
pub fn set_autovac_worker_spawner(spawner: AutovacWorkerSpawner) -> bool {
    WORKER_SPAWNER.set(spawner).is_ok()
}

/// PG `SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER)`. Ask for a worker to be
/// started on `dbid`. If the supervisor hook is installed it spawns the worker
/// task; otherwise (unit tests) the request was already recorded in shmem
/// (`av_startingWorker`) and this just logs -- faithful: PG's launcher likewise
/// only sets shmem state and signals; the worker appears asynchronously.
fn request_autovac_worker(dbid: Oid) {
    if let Some(spawn) = WORKER_SPAWNER.get() {
        spawn(dbid);
    } else {
        crate::elog!(
            crate::utils::elog::DEBUG1,
            format!("autovacuum worker requested for db {} (no spawner installed)", dbid.0)
        );
    }
}

// ---------------------------------------------------------------------------
// Launcher-side per-task state (PG's file statics, scoped to the launcher task)
// ---------------------------------------------------------------------------

/// PG `avl_dbase`: a database in the launcher's schedule. (C `adl_*` fields.)
#[derive(Clone, Copy)]
struct AvlDbase {
    datid: Oid,
    next_worker: TimestampTz,
}

/// PG `avw_dbase`: a database fetched from pg_database by `get_database_list`.
/// (C `adw_*` fields.)
#[derive(Clone)]
struct AvwDbase {
    datid: Oid,
    #[allow(dead_code, reason = "C adw_name; for ps display / logging once the connect path lands")]
    name: String,
    /// C `adw_frozenxid`.
    frozenxid: TransactionId,
    /// C `adw_minmulti`.
    minmulti: MultiXactId,
}

/// Launcher per-task state: the schedule (PG `DatabaseList`, a dlist ordered by
/// decreasing next_worker so the tail is the next-due database). PG pushes onto the
/// head (`dlist_push_head`), so a `VecDeque` with `push_front` matches.
struct LauncherState {
    database_list: VecDeque<AvlDbase>,
}

// ---------------------------------------------------------------------------
// LAUNCHER
// ---------------------------------------------------------------------------

/// PG `AutoVacLauncherMain`. The long-lived autovacuum launcher aux task. Claims a
/// PGPROC, advertises it in `ProcGlobal.autovacuum_launcher_proc`, then runs the
/// schedule loop until `shutdown` fires.
///
/// `shutdown` is the supervisor's per-child cancel handle (17f). The loop mirrors
/// the 17a/17b/17c shape: a single unified latch reset at the top, interrupts
/// serviced, then a `select!{ biased; latch | naptime | shutdown }` sleep. A RAII
/// guard clears the advertised proc, deregisters the slot, and returns the PGPROC
/// on every exit (normal break and panic unwind alike).
pub async fn auto_vac_launcher_main(shared: Arc<SharedState>, shutdown: Arc<tokio::sync::Notify>) {
    my_proc_scope(async move {
        let aux = auxiliary_process_main_common_with_proc(
            shared.proc_signal(),
            BackendType::AUTOVAC_LAUNCHER,
        )
        .await;

        let g = ProcGlobal::expect().clone();

        // Cleanup on EVERY exit (PG AutoVacLauncherShutdown sets av_launcherpid=0):
        // clear the advertised proc so a worker does not ring a dead latch,
        // deregister the proc-signal slot, and return the aux PGPROC.
        let _exit = AutoVacLauncherExitGuard {
            g: g.clone(),
            proc_signal: shared.proc_signal().clone(),
            slot_key: aux.slot_key,
        };

        // Our single wakeup latch IS our PGPROC's proc_latch (PG MyLatch ==
        // MyProc->procLatch); a worker wakes us via this proc number, and the
        // proc-signal slot was registered against this SAME latch in the cradle.
        let proc_latch: &Latch = &aux.latch;

        // PG: in emergency mode (!AutoVacuumingActive) start one worker and exit.
        // Under our model the supervisor only spawns the launcher when autovacuum
        // is active, so this matches PG's normal path; the emergency one-shot is
        // not reproduced (the supervisor decides whether to run us at all).

        // Advertise our proc number so workers can wake us on exit (PG advertises
        // av_launcherpid; we use the proc number for the latch wake).
        g.autovacuum_launcher_proc.store(aux.proc_number, Ordering::Release);

        let shmem = autovacuum_shmem()
            .cloned()
            .unwrap_or_else(|| shared.autovacuum().clone());

        let mut st = LauncherState { database_list: VecDeque::new() };

        // PG: rebuild_database_list(InvalidOid) before the loop.
        rebuild_database_list(&shared, &mut st, InvalidOid).await;

        // --- main loop: until shutdown ---
        loop {
            // PG launcher_determine_sleep -> nap; we wait that long (or until woken).
            let can_launch = av_worker_available(&shmem);
            let nap_us = launcher_determine_sleep(&shared, &mut st, can_launch, false).await;

            // PG ResetLatch(MyLatch) AFTER WaitLatch in C; here we reset before the
            // select so a wake that lands during the work below is not lost (the
            // loop shape used by all 17 aux tasks).
            proc_latch.reset();

            // Sleep until naptime expires or we are woken / shut down.
            let sleep = tokio::time::sleep(std::time::Duration::from_micros(nap_us.max(0) as u64));
            tokio::select! {
                biased;
                () = proc_latch.wait() => {}
                () = sleep => {}
                () = shutdown.notified() => break,
            }

            // PG ProcessAutoVacLauncherInterrupts(); break on shutdown.
            if process_auto_vac_launcher_interrupts(&shared, &mut st).await
                || shutdown_now(&shutdown)
            {
                break;
            }

            // A worker finished, or the postmaster reported a fork failure.
            // PG multiplexes both via got_SIGUSR2 + av_signal[]; here we poll the
            // signal flags directly each wakeup.
            if shmem.av_signal[AutoVacuumSignal::Rebalance as usize].swap(false, Ordering::AcqRel) {
                autovac_recalculate_workers_for_balance(&shmem);
            }
            if shmem.av_signal[AutoVacuumSignal::ForkFailed as usize].swap(false, Ordering::AcqRel) {
                // PG sleeps 1s and resends the start signal. Re-request the worker
                // (the WorkerInfo state is still in shmem) and restart the loop.
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let dbid = {
                    let inner = shmem.lock();
                    inner.starting_worker.map(|idx| inner.workers[idx].wi_dboid)
                };
                if let Some(dbid) = dbid {
                    request_autovac_worker(dbid);
                }
                continue;
            }

            // Conditions to check before launching: a free slot, and no other
            // worker stuck while starting up. PG evaluates both under a single
            // AutovacuumLock hold (shared, upgraded to exclusive only to reclaim a
            // stuck slot); we inline av_worker_available's freelist check under the
            // one shmem guard so the count and the starting-worker decision are
            // observed atomically.
            let current_time = GetCurrentTimestamp();
            let mut can_launch;

            {
                let mut inner = shmem.lock();
                let reserved = (autovacuum_worker_slots() - autovacuum_max_workers()).max(0);
                can_launch = inner.free_workers.len() as i32 > reserved;
                if let Some(idx) = inner.starting_worker {
                    // A worker is still starting. Wait up to min(naptime, 60)s for
                    // it; if it took too long, reclaim its slot (PG: the only cause
                    // is an early error in AutoVacWorkerMain before it clears the
                    // starting pointer).
                    let waittime = i64::from(autovacuum_naptime().min(60)) * 1000;
                    let launchtime = inner.workers[idx].wi_launchtime;
                    if timestamp_diff_exceeds_ms(launchtime, current_time, waittime) {
                        let w = &mut inner.workers[idx];
                        w.wi_dboid = InvalidOid;
                        w.wi_tableoid = InvalidOid;
                        w.wi_sharedrel = false;
                        w.wi_proc = INVALID_PROC_NUMBER;
                        w.wi_launchtime = 0;
                        inner.free_workers.push(idx);
                        inner.starting_worker = None;
                        crate::elog!(
                            crate::utils::elog::WARNING,
                            "autovacuum worker took too long to start; canceled".to_string()
                        );
                    } else {
                        can_launch = false;
                    }
                }
            }

            if !can_launch {
                continue;
            }

            // We're OK to start a new worker.
            if st.database_list.is_empty() {
                // Special case: empty list -> start a worker right away (initial
                // case; launcher_determine_sleep throttles us to <= once/naptime).
                launch_worker(&shared, &mut st, &shmem, current_time).await;
            } else {
                // The list is ordered with the most-distant next_worker first, so
                // the next-due database is at the tail.
                let avdb = *st.database_list.back().expect("non-empty");
                if timestamp_diff_exceeds_ms(avdb.next_worker, current_time, 0) {
                    launch_worker(&shared, &mut st, &shmem, current_time).await;
                }
            }
        }

        // `_exit` clears the advertised proc, deregisters the slot, returns the
        // PGPROC on drop -- on this path and on any panic unwind alike.
    })
    .await;
}

/// PG `ProcessAutoVacLauncherInterrupts`. Returns true if shutdown was requested.
/// On config reload, rebuild the schedule (the naptime may have changed).
async fn process_auto_vac_launcher_interrupts(
    shared: &Arc<SharedState>,
    st: &mut LauncherState,
) -> bool {
    let shutdown = process_main_loop_interrupts();
    if shutdown {
        return true;
    }
    // PG: on SIGHUP, ProcessConfigFile then (if autovacuum now off) shut down,
    // emit the worker-gucs warning, and rebuild the schedule (the naptime may have
    // changed). process_main_loop_interrupts consumes the reload flag without
    // exposing it; whether autovacuum should run at all is the supervisor's call
    // (17f), which stops us if the GUC turns it off. We rebuild defensively each
    // tick (cheap: an empty list until the catalog scan lands).
    // TODO(catalog): C rebuilds ONLY inside `if (ConfigReloadPending)`. Once
    // get_database_list returns real rows, rebuilding every tick pushes every
    // next_worker forward each nap so the next-due db stays in the future
    // -> worker starvation. Gate on a reload signal the interrupt layer exposes.
    rebuild_database_list(shared, st, InvalidOid).await;
    false
}

/// PG `launcher_determine_sleep`. Compute the launcher nap in microseconds, based
/// on the schedule. `recursing` guards the single self-recursion PG allows.
async fn launcher_determine_sleep(
    shared: &Arc<SharedState>,
    st: &mut LauncherState,
    canlaunch: bool,
    recursing: bool,
) -> i64 {
    let mut nap_us: i64 = if !canlaunch {
        i64::from(autovacuum_naptime()) * USECS_PER_SEC
    } else if let Some(avdb) = st.database_list.back() {
        // Sleep until the next-due database's next_worker.
        let current_time = GetCurrentTimestamp();
        (avdb.next_worker - current_time).max(0)
    } else {
        // List empty: sleep a whole naptime.
        i64::from(autovacuum_naptime()) * USECS_PER_SEC
    };

    // If exactly zero, a database had a time in the past: rebuild and recompute
    // once (PG only recurses once).
    if nap_us == 0 && !recursing {
        rebuild_database_list(shared, st, InvalidOid).await;
        return Box::pin(launcher_determine_sleep(shared, st, canlaunch, true)).await;
    }

    // Clamp to the minimum sleep time.
    let min_us = (MIN_AUTOVAC_SLEEPTIME * 1000.0) as i64;
    if nap_us <= min_us {
        nap_us = min_us;
    }
    // Clamp to the maximum (avoids an effectively infinite sleep on clock skew).
    let max_us = MAX_AUTOVAC_SLEEPTIME * USECS_PER_SEC;
    if nap_us > max_us {
        nap_us = max_us;
    }
    nap_us
}

/// PG `rebuild_database_list`. Build the schedule from the existing list + the
/// databases in pg_database, ordered by next_worker (most distant first), spread
/// across the next naptime interval.
///
/// PG builds a hash of "scored" databases (new db lowest score, then the existing
/// list in order, then all of `get_database_list`), keeping only those with a
/// pgstat entry, sorts by score, and lays them out across the naptime. We keep the
/// score order via a dedup Vec (the pgstat filter lands on the deferred stub). Each
/// element is pushed to the FRONT (PG `dlist_push_head`), so the tail is next-due.
async fn rebuild_database_list(shared: &Arc<SharedState>, st: &mut LauncherState, newdb: Oid) {
    // Build the scored oid list in PG's order: new db, existing list, then the
    // pg_database scan, skipping duplicates and InvalidOid.
    let mut scored: Vec<Oid> = Vec::new();
    let push_unique = |oid: Oid, v: &mut Vec<Oid>| {
        if oid != InvalidOid && !v.contains(&oid) {
            v.push(oid);
        }
    };

    push_unique(newdb, &mut scored);
    for avdb in &st.database_list {
        push_unique(avdb.datid, &mut scored);
    }
    for avdb in get_database_list(shared).await {
        push_unique(avdb.datid, &mut scored);
    }

    let nelems = scored.len();
    st.database_list.clear();
    if nelems == 0 {
        return;
    }

    // Time interval between databases in the schedule.
    let mut millis_increment = 1000.0 * f64::from(autovacuum_naptime()) / nelems as f64;
    if millis_increment <= MIN_AUTOVAC_SLEEPTIME {
        millis_increment = MIN_AUTOVAC_SLEEPTIME * 1.1;
    }

    let mut current_time = GetCurrentTimestamp();
    // PG walks the score-sorted array pushing each onto the list head, so later
    // (higher-score) elements land closer to the head (most-distant next_worker
    // first); push_front matches, leaving the tail = next-due.
    for datid in scored {
        current_time = crate::utils::timestamp::TimestampTzPlusMilliseconds(
            current_time,
            millis_increment as i64,
        );
        st.database_list.push_front(AvlDbase { datid, next_worker: current_time });
    }
}

/// PG `get_database_list`. Return all databases in pg_database.
///
/// Faithful (s4): opens a transaction, table_open(pg_database) + a catalog seqscan
/// (`table_beginscan_catalog` / `heap_getnext`), GETSTRUCT of each Form_pg_database,
/// skipping partially-dropped databases (`database_is_invalid_form`). All of those
/// are deferred catalog/access stubs that `unimplemented!()`, so this only runs when
/// autovacuum is enabled and a real catalog exists (the supervisor does not start
/// the launcher otherwise). The `!Send` raw `Relation`/`HeapTuple` handles live
/// entirely inside the synchronous `scan_pg_database` between the two transaction
/// awaits, so the returned future stays `Send` (no raw pointer crosses an `.await`).
async fn get_database_list(shared: &Arc<SharedState>) -> Vec<AvwDbase> {
    use crate::backend::access::transam::xact::{CommitTransactionCommand, StartTransactionCommand};

    // Start a transaction so we can access pg_database.
    StartTransactionCommand(shared).await;
    let dblist = scan_pg_database();
    CommitTransactionCommand(shared).await;
    dblist
}

/// The synchronous pg_database seqscan body of [`get_database_list`]. Kept separate
/// so the `!Send` raw `Relation`/`HeapTuple`/`TableScanDesc` handles never enter an
/// `async` state machine (rules s5/s15 sync-outer pattern).
fn scan_pg_database() -> Vec<AvwDbase> {
    use crate::access::heapam::heap_getnext;
    use crate::access::htup::HeapTupleIsValid;
    use crate::access::htup_details::GETSTRUCT;
    use crate::access::sdir::ScanDirection;
    use crate::access::table::{table_close, table_open};
    use crate::access::tableam::{table_beginscan_catalog, table_endscan};
    use crate::c::NameStr;
    use crate::catalog::pg_database::{
        database_is_invalid_form, DatabaseRelationId, FormData_pg_database,
    };
    use crate::storage::lockdefs::LockMode;

    let mut dblist: Vec<AvwDbase> = Vec::new();

    let rel = table_open(DatabaseRelationId, LockMode::AccessShareLock);
    let scan = table_beginscan_catalog(&rel, 0, &mut []);

    loop {
        let tup = heap_getnext(scan, ScanDirection::Forward);
        // SAFETY: heap_getnext returns the scan-owned tuple (or null at EOF);
        // HeapTupleIsValid / GETSTRUCT read it before the next heap_getnext, while
        // the scan keeps it live. No await between fetch and read.
        if !HeapTupleIsValid(unsafe { tup.as_ref() }) {
            break;
        }
        let pgdatabase = GETSTRUCT(unsafe { &*tup }).cast::<FormData_pg_database>();

        // Skip partially-dropped databases (we can't, nor need to, vacuum them).
        if database_is_invalid_form(pgdatabase) {
            continue;
        }

        // SAFETY: `pgdatabase` points into the live scan tuple's fixed part.
        let db = unsafe { &*pgdatabase };
        dblist.push(AvwDbase {
            datid: db.oid,
            name: name_to_string(NameStr(&db.datname)),
            frozenxid: db.datfrozenxid,
            minmulti: db.datminmxid,
        });
    }

    table_endscan(scan);
    table_close(rel, LockMode::AccessShareLock);
    dblist
}

/// PG `NameStr(name)` -> owned `String` (the NUL-padded fixed-width name).
fn name_to_string(name: &[u8]) -> String {
    let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    String::from_utf8_lossy(&name[..end]).into_owned()
}

/// PG `do_start_worker`. Choose a database and set up the `av_startingWorker` slot,
/// then signal for the worker to be started. Returns the chosen database OID, or
/// InvalidOid if no worker was started.
///
/// Faithful (s4): picks the database least-recently autovacuumed, or one at risk of
/// xid/multixact wraparound (wraparound wins; xid danger over multi danger). The
/// `recentXid`/`recentMulti` limits and the pgstat last-autovac filter land on the
/// deferred xid/pgstat stubs; reached only when autovacuum is enabled.
async fn do_start_worker(
    shared: &Arc<SharedState>,
    st: &LauncherState,
    shmem: &Arc<AutoVacuumShmem>,
) -> Oid {
    // Return quickly when there are no free workers.
    if !av_worker_available(shmem) {
        return InvalidOid;
    }

    // Get a list of databases.
    let dblist = get_database_list(shared).await;

    // Determine the oldest datfrozenxid we will allow to pass without forcing a
    // vacuum (this limit can be tightened per-table, but not loosened).
    let recent_xid = ReadNextTransactionId(shared);
    RECENT_XID.store(recent_xid.0, Ordering::Relaxed);
    let mut xid_force_limit =
        TransactionId(recent_xid.0.wrapping_sub(autovacuum_freeze_max_age() as u32));
    // Ensure it's a "normal" XID, else TransactionIdPrecedes misbehaves (this can
    // back the limit up by 3, but that's OK).
    if xid_force_limit.0 < FirstNormalTransactionId.0 {
        xid_force_limit.0 = xid_force_limit.0.wrapping_sub(FirstNormalTransactionId.0);
    }

    // Also determine the oldest datminmxid we will consider.
    let recent_multi = crate::access::multixact::ReadNextMultiXactId();
    RECENT_MULTI.store(recent_multi.0, Ordering::Relaxed);
    // MultiXactId is a TransactionId alias (same 32-bit space); construct via it.
    let mut multi_force_limit: MultiXactId = TransactionId(
        recent_multi
            .0
            .wrapping_sub(crate::access::multixact::MultiXactMemberFreezeThreshold() as u32),
    );
    if multi_force_limit.0 < FirstMultiXactId.0 {
        multi_force_limit.0 = multi_force_limit.0.wrapping_sub(FirstMultiXactId.0);
    }

    // Choose a database to connect to (least-recently autovacuumed, or one at risk
    // of wraparound -- xid danger ranks above multi danger).
    let mut avdb: Option<&AvwDbase> = None;
    let mut for_xid_wrap = false;
    let mut for_multi_wrap = false;
    let current_time = GetCurrentTimestamp();

    for tmp in &dblist {
        // Check whether this one is at risk of xid wraparound.
        if tmp.frozenxid.precedes(xid_force_limit) {
            if avdb.is_none() || tmp.frozenxid.precedes(avdb.expect("set").frozenxid) {
                avdb = Some(tmp);
            }
            for_xid_wrap = true;
            continue;
        } else if for_xid_wrap {
            continue; // ignore not-at-risk DBs
        } else if crate::access::multixact::MultiXactIdPrecedes(tmp.minmulti, multi_force_limit) {
            if avdb.is_none()
                || crate::access::multixact::MultiXactIdPrecedes(
                    tmp.minmulti,
                    avdb.expect("set").minmulti,
                )
            {
                avdb = Some(tmp);
            }
            for_multi_wrap = true;
            continue;
        } else if for_multi_wrap {
            continue; // ignore not-at-risk DBs
        }

        // Find the pgstat entry; skip a database with none (no activity since the
        // stats were initialized) -- PG autovacuum.c:1204 `if (!tmp->adw_entry) continue;`.
        let Some(entry) = crate::pgstat::pgstat_fetch_stat_dbentry(tmp.datid) else {
            continue;
        };

        // Skip a database that the schedule shows was processed recently (its
        // next_worker falls within [now, now + naptime]): we may have just picked
        // it but pgstat has not yet updated last_autovac_time. PG walks DatabaseList
        // in reverse looking for this datid.
        let naptime_ms = i64::from(autovacuum_naptime()) * 1000;
        let skipit = st.database_list.iter().rev().any(|dbp| {
            dbp.datid == tmp.datid
                && !timestamp_diff_exceeds_ms(dbp.next_worker, current_time, 0)
                && !timestamp_diff_exceeds_ms(current_time, dbp.next_worker, naptime_ms)
        });
        // PG, if ALL databases were skipped here, rebuilds DatabaseList (it may
        // contain a dropped db). Our launcher rebuilds every tick
        // (process_auto_vac_launcher_interrupts), so that case is already covered;
        // we simply skip this candidate.
        if skipit {
            continue;
        }

        // Remember the db with the oldest autovac time.
        if avdb.is_none_or(|cur| entry.last_autovac_time < avdb_last_autovac(cur)) {
            avdb = Some(tmp);
        }
    }

    // Found a database -- claim a worker slot and request its start.
    let Some(avdb) = avdb else {
        return InvalidOid;
    };

    let mut inner = shmem.lock();
    let Some(idx) = inner.free_workers.pop() else {
        return InvalidOid;
    };
    let now = GetCurrentTimestamp();
    let w = &mut inner.workers[idx];
    w.wi_dboid = avdb.datid;
    w.wi_proc = INVALID_PROC_NUMBER;
    w.wi_launchtime = now;
    inner.starting_worker = Some(idx);
    drop(inner);

    request_autovac_worker(avdb.datid);
    avdb.datid
}

/// Helper for the least-recently-autovacuumed comparison in [`do_start_worker`]:
/// the current best candidate's `last_autovac_time` (its pgstat entry). Lands on
/// the deferred pgstat stub. PG invariant (autovacuum.c:1240): reached only in the
/// non-wraparound path, where every candidate passed the entry guard, so the entry
/// is always present here.
fn avdb_last_autovac(avdb: &AvwDbase) -> TimestampTz {
    let Some(entry) = crate::pgstat::pgstat_fetch_stat_dbentry(avdb.datid) else {
        crate::assert!(false, "candidate db lost its pgstat entry");
        return TimestampTz::MIN;
    };
    entry.last_autovac_time
}

/// PG `launch_worker`. Start a worker (via `do_start_worker`) and update the
/// schedule entry for the chosen database (rebuild if it was absent).
async fn launch_worker(
    shared: &Arc<SharedState>,
    st: &mut LauncherState,
    shmem: &Arc<AutoVacuumShmem>,
    now: TimestampTz,
) {
    let dbid = do_start_worker(shared, st, shmem).await;
    if dbid == InvalidOid {
        return;
    }

    // Update the schedule entry: next_worker = now + naptime; move it to the head
    // (most distant). If absent, rebuild the list with this db as "new".
    if let Some(pos) = st.database_list.iter().position(|d| d.datid == dbid) {
        let mut entry = st.database_list.remove(pos).expect("position is in range");
        entry.next_worker = crate::utils::timestamp::TimestampTzPlusMilliseconds(
            now,
            i64::from(autovacuum_naptime()) * 1000,
        );
        st.database_list.push_front(entry);
    } else {
        rebuild_database_list(shared, st, dbid).await;
    }
}

/// PG `AutoVacWorkerFailed`. Called (by the supervisor in our model) to report a
/// failure to start a worker; the launcher notices the flag next wakeup.
pub fn auto_vac_worker_failed() {
    if let Some(shmem) = autovacuum_shmem() {
        shmem.av_signal[AutoVacuumSignal::ForkFailed as usize].store(true, Ordering::Release);
    }
}

/// PG `autovac_recalculate_workers_for_balance`. Recount the workers that
/// participate in cost balancing (those with a live proc and dobalance set).
fn autovac_recalculate_workers_for_balance(shmem: &Arc<AutoVacuumShmem>) {
    let inner = shmem.lock();
    let n = inner
        .running_workers
        .iter()
        .filter(|&&idx| inner.workers[idx].wi_proc != INVALID_PROC_NUMBER && inner.workers[idx].wi_dobalance)
        .count() as u32;
    drop(inner);
    if n != shmem.av_nworkers_for_balance.load(Ordering::Relaxed) {
        shmem.av_nworkers_for_balance.store(n, Ordering::Relaxed);
    }
}

/// PG `av_worker_available`. Whether a free worker slot is available, honoring the
/// reservation of `worker_slots - max_workers` slots.
fn av_worker_available(shmem: &Arc<AutoVacuumShmem>) -> bool {
    let free_slots = shmem.lock().free_workers.len() as i32;
    let reserved = (autovacuum_worker_slots() - autovacuum_max_workers()).max(0);
    free_slots > reserved
}

/// PG `check_av_worker_gucs`. Warn if worker_slots < max_workers.
fn check_av_worker_gucs() {
    if autovacuum_worker_slots() < autovacuum_max_workers() {
        crate::elog!(
            crate::utils::elog::WARNING,
            format!(
                "\"autovacuum_max_workers\" ({}) should be less than or equal to \"autovacuum_worker_slots\" ({})",
                autovacuum_max_workers(),
                autovacuum_worker_slots()
            )
        );
    }
}

// ---------------------------------------------------------------------------
// WORKER
// ---------------------------------------------------------------------------

/// PG `AutoVacWorkerMain`. The autovacuum worker aux task. Claims the
/// `av_startingWorker` slot the launcher set up, moves it onto the running list,
/// then (if it got a database) runs [`do_autovacuum`]. On exit it returns the slot
/// to the freelist ([`free_worker_info`]) and wakes the launcher.
///
/// Unlike the always-running aux tasks, a worker runs once and exits (PG: "we
/// don't attempt to continue after an error; the launcher spawns another later").
/// `do_autovacuum` reaches vacuum/catalog stubs -- correct staging (s4). The
/// launcher only requests a worker when a database is due (none in tests), so this
/// is not driven into those stubs on a timer.
pub async fn auto_vac_worker_main(shared: Arc<SharedState>, _dbid: Oid) {
    my_proc_scope(async move {
        let aux = auxiliary_process_main_common_with_proc(
            shared.proc_signal(),
            BackendType::AUTOVAC_WORKER,
        )
        .await;

        let shmem = autovacuum_shmem()
            .cloned()
            .unwrap_or_else(|| shared.autovacuum().clone());

        // RAII: return the WorkerInfo to the freelist + wake the launcher on EVERY
        // exit (PG's on_shmem_exit(FreeWorkerInfo) + the ProcKill launcher wake),
        // then deregister the slot + return the PGPROC.
        let exit = AutoVacWorkerExitGuard {
            shmem: shmem.clone(),
            my_worker: std::cell::Cell::new(None),
            proc_signal: shared.proc_signal().clone(),
            slot_key: aux.slot_key,
        };

        // Claim the starting-worker slot (PG: under AutovacuumLock, move
        // av_startingWorker onto av_runningWorkers, record my proc, clear the
        // starting pointer, wake the launcher).
        let dbid = {
            let mut inner = shmem.lock();
            if let Some(idx) = inner.starting_worker.take() {
                inner.workers[idx].wi_proc = aux.proc_number;
                inner.running_workers.push(idx);
                let dbid = inner.workers[idx].wi_dboid;
                drop(inner);
                exit.my_worker.set(Some(idx));
                // Wake the launcher so it can start another worker if needed.
                wake_launcher();
                dbid
            } else {
                // No worker entry for me -- go away (PG WARNING).
                crate::elog!(
                    crate::utils::elog::WARNING,
                    "autovacuum worker started without a worker entry".to_string()
                );
                InvalidOid
            }
        };

        if dbid != InvalidOid {
            // PG reports autovac startup to pgstat BEFORE InitPostgres so
            // last_autovac_time advances even if the connection attempt fails (so
            // we don't get "stuck" repeatedly selecting an unopenable database).
            crate::pgstat::pgstat_report_autovac(dbid);

            // Connect to the selected database (no particular user, ignoring
            // datallowconn). PG: InitPostgres(NULL, dbid, NULL, InvalidOid,
            // INIT_PG_OVERRIDE_ALLOW_CONNS, dbname) then SetProcessingMode(Normal).
            crate::miscadmin::InitPostgres(
                None,
                dbid,
                None,
                InvalidOid,
                crate::miscadmin::InitPgFlags::OVERRIDE_ALLOW_CONNS,
            );
            crate::miscadmin::set_processing_mode(
                crate::miscadmin::ProcessingMode::NormalProcessing,
            );

            // PG refreshes recentXid/recentMulti right before do_autovacuum so the
            // anti-wraparound force limits are current.
            RECENT_XID.store(ReadNextTransactionId(&shared).0, Ordering::Relaxed);
            RECENT_MULTI.store(crate::access::multixact::ReadNextMultiXactId().0, Ordering::Relaxed);

            // And do an appropriate amount of work.
            do_autovacuum(&shared, &shmem, exit.my_worker.get()).await;
        }

        // `exit` runs FreeWorkerInfo + wakes the launcher + returns the PGPROC.
        drop(exit);
    })
    .await;
}

/// PG `FreeWorkerInfo`. Return my WorkerInfo to the freelist, clear it, and flag a
/// rebalance. Called from the worker exit guard (PG's on_shmem_exit callback).
fn free_worker_info(shmem: &Arc<AutoVacuumShmem>, idx: usize) {
    let mut inner = shmem.lock();
    if let Some(pos) = inner.running_workers.iter().position(|&i| i == idx) {
        inner.running_workers.swap_remove(pos);
    }
    let w = &mut inner.workers[idx];
    w.wi_dboid = InvalidOid;
    w.wi_tableoid = InvalidOid;
    w.wi_sharedrel = false;
    w.wi_proc = INVALID_PROC_NUMBER;
    w.wi_launchtime = 0;
    w.wi_dobalance = false;
    inner.free_workers.push(idx);
    drop(inner);
    // Cause a rebalance of the surviving workers.
    shmem.av_signal[AutoVacuumSignal::Rebalance as usize].store(true, Ordering::Release);
}

/// Wake the autovacuum launcher by ringing its `proc_latch` (PG: kill(SIGUSR2,
/// av_launcherpid)). Reaches the launcher PGPROC via `ProcGlobal.autovacuum_
/// launcher_proc`; a no-op if no launcher is running.
pub fn wake_launcher() -> bool {
    let Some(g) = ProcGlobal::get() else {
        return false;
    };
    let procno = g.autovacuum_launcher_proc.load(Ordering::Acquire);
    if procno == INVALID_PROC_NUMBER {
        return false;
    }
    // SAFETY: `proc_latch` is internally synchronized (a Latch over a Notify);
    // setting it forms no `&mut` into the slot's other field groups.
    (unsafe { g.proc(procno) })
        .inspect(|proc| proc.proc_latch.set())
        .is_some()
}

/// PG `autovac_table`: a table the worker has decided to vacuum/analyze, with the
/// resolved per-table VACUUM parameters.
struct AutovacTable {
    /// C `at_relid`.
    relid: Oid,
    /// C `at_params`.
    params: crate::commands::vacuum::VacuumParams,
    #[allow(dead_code, reason = "C at_storage_param_vac_cost_delay; consumed by VacuumUpdateCosts")]
    storage_param_vac_cost_delay: f64,
    #[allow(dead_code, reason = "C at_storage_param_vac_cost_limit; consumed by VacuumUpdateCosts")]
    storage_param_vac_cost_limit: i32,
    /// C `at_dobalance`.
    dobalance: bool,
    /// C `at_sharedrel`; consumed by the concurrent-worker claim.
    sharedrel: bool,
    /// C `at_relname`.
    relname: Option<String>,
    /// C `at_nspname`.
    nspname: Option<String>,
    /// C `at_datname`.
    datname: Option<String>,
}

/// PG `do_autovacuum`. Process a database table-by-table: scan pg_class (two passes:
/// plain rels + matviews, then TOAST tables), decide which need vacuum/analyze, then
/// vacuum each; then process backend-requested work items; then update
/// pg_database.datfrozenxid.
///
/// Faithful (s4): every catalog/syscache/vacuum/pgstat call lands on a deferred stub
/// that `unimplemented!()`. Reached only when a worker actually connected to a
/// database (autovacuum enabled + a real catalog), never on the launcher's timer.
/// The `!Send` raw catalog handles are confined to the synchronous `collect_*`
/// helpers so this future stays `Send`.
async fn do_autovacuum(
    shared: &Arc<SharedState>,
    shmem: &Arc<AutoVacuumShmem>,
    my_worker: Option<usize>,
) {
    use crate::backend::access::transam::xact::{CommitTransactionCommand, StartTransactionCommand};

    // PG creates AutovacMemCxt to keep the table list across transactions; under
    // Rust ownership the `Vec`s outlive the per-table transactions naturally.

    // Start a transaction so our commands have one to play into.
    StartTransactionCommand(shared).await;

    // Compute the multixact age for which freezing is urgent.
    let effective_multixact_freeze_max_age =
        crate::access::multixact::MultiXactMemberFreezeThreshold();

    // PG selects per-database default freeze ages from the pg_database row
    // (zeroed for template / non-connectable databases). That syscache lookup is
    // deferred; the two-pass scan below collects the work list.
    let table_oids = collect_tables_to_vacuum(effective_multixact_freeze_max_age);

    CommitTransactionCommand(shared).await;

    // Perform operations on collected tables, one transaction per table (PG runs
    // each vacuum in its own transaction so a failure aborts only that table).
    let mut did_vacuum = false;
    let mut found_concurrent_worker = false;
    let my_db = my_database_id();
    for relid in table_oids {
        crate::miscadmin::check_for_interrupts();

        StartTransactionCommand(shared).await;

        // Skip the table if another worker is vacuuming it concurrently, else
        // claim it by publishing wi_tableoid (PG holds AutovacuumScheduleLock +
        // AutovacuumLock(SHARED); both collapse into our single shmem Mutex). Only
        // a worker that owns a slot participates in the claim protocol.
        if let Some(idx) = my_worker {
            let skipit = {
                let mut inner = shmem.lock();
                let skip = inner.running_workers.iter().any(|&o| {
                    o != idx
                        && (inner.workers[o].wi_sharedrel || inner.workers[o].wi_dboid == my_db)
                        && inner.workers[o].wi_tableoid == relid
                });
                if !skip {
                    inner.workers[idx].wi_tableoid = relid;
                }
                skip
            };
            if skipit {
                found_concurrent_worker = true;
                CommitTransactionCommand(shared).await;
                continue;
            }
        }

        // Recheck pgstat: the table may have been processed since we looked.
        let tab = table_recheck_autovac(relid, effective_multixact_freeze_max_age);
        let Some(mut tab) = tab else {
            // Someone else vacuumed it, or it went away: drop our claim.
            if let Some(idx) = my_worker {
                let mut inner = shmem.lock();
                inner.workers[idx].wi_tableoid = InvalidOid;
                inner.workers[idx].wi_sharedrel = false;
            }
            CommitTransactionCommand(shared).await;
            continue;
        };

        // Finish the claim now that recheck gave us relisshared, set the worker's
        // cost-balance participation from at_dobalance, then recount (PG:
        // wi_sharedrel / pg_atomic_*_flag(wi_dobalance) under AutovacuumLock,
        // then autovac_recalculate_workers_for_balance + VacuumUpdateCosts).
        if let Some(idx) = my_worker {
            let mut inner = shmem.lock();
            inner.workers[idx].wi_sharedrel = tab.sharedrel;
            inner.workers[idx].wi_dobalance = tab.dobalance;
        }
        autovac_recalculate_workers_for_balance(shmem);

        // Save the relation name for a possible error message (a NULL means the rel
        // was dropped since we checked; skip it). PG get_rel_name /
        // get_namespace_name(get_rel_namespace) / get_database_name.
        tab.relname = crate::utils::lsyscache::get_rel_name(tab.relid);
        tab.nspname =
            crate::utils::lsyscache::get_namespace_name(crate::utils::lsyscache::get_rel_namespace(
                tab.relid,
            ));
        tab.datname = crate::commands::dbcommands::get_database_name(my_database_id());
        if tab.relname.is_none() || tab.nspname.is_none() || tab.datname.is_none() {
            release_table_claim(shmem, my_worker);
            CommitTransactionCommand(shared).await;
            continue;
        }

        // Have at it. PG wraps this in PG_TRY so an error aborts only this table;
        // here the per-table transaction boundary + the task-level catch_unwind
        // provide the equivalent isolation.
        autovacuum_do_vac_analyze(&tab);
        did_vacuum = true;

        // Release our claim on the table (PG :2517-2520: clear wi_tableoid /
        // wi_sharedrel, then optimistically re-set wi_dobalance for the next table).
        release_table_claim(shmem, my_worker);
        CommitTransactionCommand(shared).await;
    }

    // Perform additional work items requested by backends.
    perform_requested_work_items(shared).await;

    // Update pg_database.datfrozenxid and truncate pg_xact if possible. PG skips
    // this only when it did no work AND skipped a table due to a concurrent worker
    // (else an autovacuum=off restart loop can result).
    if did_vacuum || !found_concurrent_worker {
        StartTransactionCommand(shared).await;
        crate::commands::vacuum::vac_update_datfrozenxid();
        CommitTransactionCommand(shared).await;
    }
}

/// PG's per-table cleanup (autovacuum.c:2517-2520): clear the worker's table claim
/// (wi_tableoid / wi_sharedrel) and optimistically re-set wi_dobalance for the next
/// table, under the shmem lock. No-op when the worker owns no slot (tests).
fn release_table_claim(shmem: &Arc<AutoVacuumShmem>, my_worker: Option<usize>) {
    let Some(idx) = my_worker else {
        return;
    };
    let mut inner = shmem.lock();
    inner.workers[idx].wi_tableoid = InvalidOid;
    inner.workers[idx].wi_sharedrel = false;
    inner.workers[idx].wi_dobalance = true;
}

/// The synchronous two-pass pg_class scan of [`do_autovacuum`]: collect the OIDs of
/// plain relations / matviews (pass 1) and TOAST tables (pass 2) that need vacuum or
/// analyze. The `!Send` raw catalog handles stay inside this sync function. Lands on
/// deferred catalog/access/pgstat stubs.
fn collect_tables_to_vacuum(effective_multixact_freeze_max_age: i32) -> Vec<Oid> {
    use crate::access::heapam::heap_getnext;
    use crate::access::htup::HeapTupleIsValid;
    use crate::access::htup_details::GETSTRUCT;
    use crate::access::sdir::ScanDirection;
    use crate::access::table::{table_close, table_open};
    use crate::access::tableam::{table_beginscan_catalog, table_endscan};
    use crate::catalog::pg_class::{FormData_pg_class, RelationRelationId};
    use crate::storage::lockdefs::LockMode;

    const RELKIND_RELATION: i8 = b'r' as i8;
    const RELKIND_MATVIEW: i8 = b'm' as i8;
    const RELKIND_TOASTVALUE: i8 = b't' as i8;
    const RELPERSISTENCE_TEMP: i8 = b't' as i8;

    let mut table_oids: Vec<Oid> = Vec::new();

    let class_rel = table_open(RelationRelationId, LockMode::AccessShareLock);

    // Pass 1: plain relations + matviews.
    let scan = table_beginscan_catalog(&class_rel, 0, &mut []);
    loop {
        let tuple = heap_getnext(scan, ScanDirection::Forward);
        // SAFETY: scan-owned tuple read before the next heap_getnext; no await.
        if !HeapTupleIsValid(unsafe { tuple.as_ref() }) {
            break;
        }
        let class_form = GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_class>();
        // SAFETY: points into the live scan tuple.
        let cf = unsafe { &*class_form };
        if cf.relkind != RELKIND_RELATION && cf.relkind != RELKIND_MATVIEW {
            continue;
        }
        // We cannot safely process another backend's temp table.
        if cf.relpersistence == RELPERSISTENCE_TEMP {
            continue;
        }
        let (dovacuum, doanalyze, _wraparound) =
            relation_needs_vacanalyze(cf.oid, cf, effective_multixact_freeze_max_age);
        if dovacuum || doanalyze {
            table_oids.push(cf.oid);
        }
    }
    table_endscan(scan);

    // Pass 2: TOAST tables (PG uses a scan key on relkind; the catalog scan stub
    // ignores keys, so we filter in the loop).
    let scan = table_beginscan_catalog(&class_rel, 0, &mut []);
    loop {
        let tuple = heap_getnext(scan, ScanDirection::Forward);
        // SAFETY: as above.
        if !HeapTupleIsValid(unsafe { tuple.as_ref() }) {
            break;
        }
        let class_form = GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_class>();
        let cf = unsafe { &*class_form };
        if cf.relkind != RELKIND_TOASTVALUE {
            continue;
        }
        if cf.relpersistence == RELPERSISTENCE_TEMP {
            continue;
        }
        let (dovacuum, _doanalyze, _wraparound) =
            relation_needs_vacanalyze(cf.oid, cf, effective_multixact_freeze_max_age);
        // Ignore analyze for toast tables.
        if dovacuum {
            table_oids.push(cf.oid);
        }
    }
    table_endscan(scan);

    table_close(class_rel, LockMode::AccessShareLock);
    table_oids
}

/// PG `relation_needs_vacanalyze`. Decide whether a relation needs vacuum and/or
/// analyze from its pg_class row + pgstat counters + the wraparound limits. The
/// reloptions / pgstat fetch land on deferred stubs; returns
/// `(dovacuum, doanalyze, wraparound)` (PG's three out-params).
fn relation_needs_vacanalyze(
    relid: Oid,
    class_form: &crate::catalog::pg_class::FormData_pg_class,
    effective_multixact_freeze_max_age: i32,
) -> (bool, bool, bool) {
    // Force vacuum if the table is at risk of xid/multixact wraparound. PG reads the
    // autovacuum_freeze_max_age GUC (+ per-table reloptions); reloptions are
    // deferred, so use the GUC defaults.
    let recent_xid = current_recent_xid();
    let mut xid_force_limit =
        TransactionId(recent_xid.0.wrapping_sub(autovacuum_freeze_max_age() as u32));
    if xid_force_limit.0 < FirstNormalTransactionId.0 {
        xid_force_limit.0 = xid_force_limit.0.wrapping_sub(FirstNormalTransactionId.0);
    }
    let relfrozenxid = class_form.relfrozenxid;
    let mut force_vacuum = relfrozenxid.is_normal() && relfrozenxid.precedes(xid_force_limit);

    if !force_vacuum {
        let relminmxid = class_form.relminmxid;
        let recent_multi = current_recent_multi();
        let mut multi_force_limit: MultiXactId = TransactionId(
            recent_multi.0.wrapping_sub(effective_multixact_freeze_max_age as u32),
        );
        if multi_force_limit.0 < FirstMultiXactId.0 {
            multi_force_limit.0 = multi_force_limit.0.wrapping_sub(FirstMultiXactId.0);
        }
        force_vacuum = crate::access::multixact::MultiXactIdIsValid(relminmxid)
            && crate::access::multixact::MultiXactIdPrecedes(relminmxid, multi_force_limit);
    }

    // Fetch pgstat counters (deferred). With stats present, apply the threshold
    // equations; without them, only forced (anti-wraparound) vacuums proceed.
    let tabentry =
        crate::pgstat::pgstat_fetch_stat_tabentry_ext(class_form.relisshared, relid);
    let Some(tabentry) = tabentry else {
        return (force_vacuum, false, force_vacuum);
    };

    let reltuples = f64::from(class_form.reltuples).max(0.0);
    let vacthresh = autovacuum_vac_scale().mul_add(reltuples, f64::from(autovacuum_vac_thresh()));
    let anlthresh = autovacuum_anl_scale().mul_add(reltuples, f64::from(autovacuum_anl_thresh()));
    let vactuples = tabentry.dead_tuples as f64;
    let anltuples = tabentry.mod_since_analyze as f64;

    let dovacuum = force_vacuum || vactuples > vacthresh;
    let doanalyze = anltuples > anlthresh;
    (dovacuum, doanalyze, force_vacuum)
}

/// PG `table_recheck_autovac`. Re-fetch the relation and re-check whether it still
/// needs vacuum/analyze (the picture may have changed since the first scan); if so,
/// build the [`AutovacTable`] with the resolved VACUUM parameters. The syscache
/// fetch + reloptions land on deferred stubs; `None` means nothing to do.
fn table_recheck_autovac(relid: Oid, effective_multixact_freeze_max_age: i32) -> Option<AutovacTable> {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_class::FormData_pg_class;
    use crate::commands::vacuum::{VacOpt, VacOptValue, VacuumParams};
    use crate::utils::syscache::{SearchSysCacheCopy1, SysCacheIdentifier};

    let class_tup = SearchSysCacheCopy1(
        SysCacheIdentifier::RELOID,
        crate::postgres::Datum(relid.0 as usize),
    )?;
    // SAFETY: a valid syscache tuple; read its fixed part before it is freed.
    let class_form = GETSTRUCT(unsafe { &*class_tup }).cast::<FormData_pg_class>();
    let cf = unsafe { &*class_form };

    let (dovacuum, doanalyze, wraparound) =
        relation_needs_vacanalyze(relid, cf, effective_multixact_freeze_max_age);
    if !dovacuum && !doanalyze {
        return None;
    }

    // Build the VACUUM parameters (PG fills freeze ages from reloptions/GUC; we use
    // the autovac/vacuum GUC defaults since reloptions are deferred). Select VACUUM
    // options: don't process toast (vacuum() skips it), skip the database-stats
    // update (we do vac_update_datfrozenxid ourselves), and skip-locked unless
    // anti-wraparound.
    let mut options = VacOpt::empty();
    if dovacuum {
        options |= VacOpt::VACUUM | VacOpt::PROCESS_MAIN | VacOpt::SKIP_DATABASE_STATS;
    }
    if doanalyze {
        options |= VacOpt::ANALYZE;
    }
    if !wraparound {
        options |= VacOpt::SKIP_LOCKED;
    }

    let params = VacuumParams {
        options,
        freeze_min_age: -1,
        freeze_table_age: -1,
        multixact_freeze_min_age: -1,
        multixact_freeze_table_age: -1,
        is_wraparound: wraparound,
        log_min_duration: -1,
        index_cleanup: VacOptValue::Unspecified,
        truncate: VacOptValue::Unspecified,
        toast_parent: InvalidOid,
        max_eager_freeze_failure_rate: 0.0,
        nworkers: -1,
    };

    let tab = AutovacTable {
        relid,
        params,
        storage_param_vac_cost_delay: -1.0,
        storage_param_vac_cost_limit: 0,
        dobalance: true,
        sharedrel: cf.relisshared,
        relname: None,
        nspname: None,
        datname: None,
    };
    Some(tab)
}

/// PG `autovacuum_do_vac_analyze`. Build a one-relation VACUUM target and invoke
/// `vacuum()` for the table. The makeRangeVar / makeVacuumRelation / vacuum() calls
/// land on deferred stubs.
fn autovacuum_do_vac_analyze(tab: &AutovacTable) {
    // PG: autovac_report_activity(tab) (pgstat), then build a VacuumRelation target
    // identified by OID and call vacuum(rel_list, &tab->params, bstrategy,
    // vac_context, true). The relation-target node construction + vacuum() are
    // deferred; pass an empty target list to the stub.
    crate::commands::vacuum::vacuum(
        &[],
        &tab.params,
        None,
        std::ptr::null_mut(),
        true,
    );
}

/// PG `do_autovacuum`'s work-item loop + `perform_work_item`. Claim each used,
/// inactive work item for this database, run it, and mark it done. The BRIN
/// summarize dispatch lands on a deferred stub.
async fn perform_requested_work_items(shared: &Arc<SharedState>) {
    use crate::backend::access::transam::xact::{CommitTransactionCommand, StartTransactionCommand};

    let Some(shmem) = autovacuum_shmem() else {
        return;
    };
    let my_db = my_database_id();

    loop {
        // Claim the next pending work item for my database under the lock.
        let claimed = {
            let mut inner = shmem.lock();
            let found = inner.work_items.iter_mut().enumerate().find(|(_, w)| {
                w.avw_used && !w.avw_active && w.avw_database == my_db
            });
            match found {
                Some((i, w)) => {
                    w.avw_active = true;
                    Some((i, w.avw_type, w.avw_relation, w.avw_block_number))
                }
                None => None,
            }
        };
        let Some((idx, wtype, relation, blkno)) = claimed else {
            break;
        };

        StartTransactionCommand(shared).await;
        perform_work_item(wtype, relation, blkno);
        CommitTransactionCommand(shared).await;

        crate::miscadmin::check_for_interrupts();

        // Mark it done.
        let mut inner = shmem.lock();
        inner.work_items[idx].avw_active = false;
        inner.work_items[idx].avw_used = false;
    }
}

/// PG `perform_work_item`. Dispatch one autovacuum work item. The BRIN
/// summarize-range path lands on a deferred stub.
fn perform_work_item(wtype: AutoVacuumWorkItemType, relation: Oid, blkno: BlockNumber) {
    match wtype {
        AutoVacuumWorkItemType::AVW_BRINSummarizeRange => {
            brin_summarize_range(relation, blkno);
        }
    }
}

/// PG `brin_summarize_range` (DirectFunctionCall2 from `perform_work_item`). A SQL-
/// callable BRIN function not yet ported; a deferred stub so the dispatch is
/// faithful. Reached only when a real BRIN work item is processed.
fn brin_summarize_range(_relation: Oid, _blkno: BlockNumber) {
    unimplemented!("brin_summarize_range: BRIN AM deferred (s4 staging)")
}

/// PG `MyDatabaseId`: the database this worker connected to. The per-task session
/// identity; the worker set it during InitPostgres.
fn my_database_id() -> Oid {
    crate::session::current().database_id()
}

/// PG `recentXid` (file static refreshed by the worker before do_autovacuum).
fn current_recent_xid() -> TransactionId {
    TransactionId(RECENT_XID.load(Ordering::Relaxed))
}

/// PG `recentMulti` (file static). See [`current_recent_xid`].
fn current_recent_multi() -> MultiXactId {
    TransactionId(RECENT_MULTI.load(Ordering::Relaxed))
}

/// PG `AutoVacuumRequestWork`. Record one work item for the next autovacuum run on
/// the caller's database. Returns false if no free work-item slot.
pub fn auto_vacuum_request_work(
    type_: AutoVacuumWorkItemType,
    my_database_id: Oid,
    relation_id: Oid,
    blkno: BlockNumber,
) -> bool {
    let Some(shmem) = autovacuum_shmem() else {
        return false;
    };
    let mut inner = shmem.lock();
    inner
        .work_items
        .iter_mut()
        .find(|item| !item.avw_used)
        .map(|item| {
            item.avw_used = true;
            item.avw_active = false;
            item.avw_type = type_;
            item.avw_database = my_database_id;
            item.avw_relation = relation_id;
            item.avw_block_number = blkno;
        })
        .is_some()
}

/// PG `autovac_init`. Postmaster-time sanity check: warn on misconfiguration.
pub fn autovac_init() {
    if !AUTOVACUUM_START_DAEMON.load(Ordering::Relaxed) {
        return;
    }
    if PGSTAT_TRACK_COUNTS.load(Ordering::Relaxed) {
        check_av_worker_gucs();
    } else {
        crate::elog!(
            crate::utils::elog::WARNING,
            "autovacuum not started because of misconfiguration".to_string()
        );
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// PG `TimestampDifferenceExceeds(start, stop, msec)`: stop - start > msec ms.
/// Inlined (the shared `TimestampDifferenceExceeds` is an unimplemented stub, and
/// the launcher calls this on its naptime path so it must not panic).
const fn timestamp_diff_exceeds_ms(start: TimestampTz, stop: TimestampTz, msec: i64) -> bool {
    let diff = stop - start; // microseconds
    diff >= msec * 1000
}

/// True if the supervisor has asked this task to shut down. Non-blocking: polls
/// `shutdown.notified()` once, consuming a permit left by `notify_one`, so a
/// shutdown that arrived between sleeps is seen here (mirrors the other aux tasks).
fn shutdown_now(shutdown: &Arc<tokio::sync::Notify>) -> bool {
    use futures_util::FutureExt;
    let fut = shutdown.notified();
    futures_util::pin_mut!(fut);
    fut.now_or_never().is_some()
}

/// Runs the launcher's exit cleanup on EVERY scope exit (normal + panic unwind).
/// Idempotent: re-clearing an already-cleared proc / stale slot key is harmless;
/// ProcKill no-ops once the proc is returned.
struct AutoVacLauncherExitGuard {
    g: Arc<crate::storage::proc::ProcGlobal>,
    proc_signal: Arc<crate::backend::storage::ipc::procsignal::ProcSignal>,
    slot_key: crate::backend::storage::ipc::procsignal::SlotKey,
}

impl Drop for AutoVacLauncherExitGuard {
    fn drop(&mut self) {
        // PG AutoVacLauncherShutdown: av_launcherpid = 0.
        self.g
            .autovacuum_launcher_proc
            .store(INVALID_PROC_NUMBER, Ordering::Release);
        self.proc_signal.deregister(self.slot_key);
        crate::storage::proc::ProcKill();
    }
}

/// Runs the worker's exit cleanup on EVERY scope exit (PG on_shmem_exit(
/// FreeWorkerInfo) + the ProcKill launcher wake). `my_worker` is set once the
/// worker claimed its slot; if it never did, there is nothing to free.
struct AutoVacWorkerExitGuard {
    shmem: Arc<AutoVacuumShmem>,
    my_worker: std::cell::Cell<Option<usize>>,
    proc_signal: Arc<crate::backend::storage::ipc::procsignal::ProcSignal>,
    slot_key: crate::backend::storage::ipc::procsignal::SlotKey,
}

impl Drop for AutoVacWorkerExitGuard {
    fn drop(&mut self) {
        if let Some(idx) = self.my_worker.get() {
            free_worker_info(&self.shmem, idx);
            // PG: the launcher is notified of my death in ProcKill, if I got a slot.
            wake_launcher();
        }
        self.proc_signal.deregister(self.slot_key);
        crate::storage::proc::ProcKill();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::postmaster::auxprocess::aux_test_serial as test_serial;
    use crate::shared_state::SharedStateConfig;
    use std::time::Duration;

    fn fresh_shared() -> Arc<SharedState> {
        let shared = SharedState::new(SharedStateConfig::default());
        let _ = crate::storage::proc::ProcGlobal::set(shared.proc_global().clone());
        let _ = set_autovacuum_shmem(shared.autovacuum().clone());
        shared
    }

    fn published_proc_global() -> Arc<crate::storage::proc::ProcGlobal> {
        crate::storage::proc::ProcGlobal::get()
            .expect("a ProcGlobal is published")
            .clone()
    }

    fn published_shmem() -> Arc<AutoVacuumShmem> {
        autovacuum_shmem().expect("an AutoVacuumShmem is published").clone()
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

    /// AutoVacuumShmem init: the freelist is seeded to autovacuum_worker_slots and
    /// the running list / starting pointer start empty.
    #[test]
    fn shmem_init_populates_freelist() {
        let shmem = AutoVacuumShmem::new();
        let inner = shmem.lock();
        assert_eq!(inner.free_workers.len(), autovacuum_worker_slots() as usize);
        assert!(inner.running_workers.is_empty());
        assert!(inner.starting_worker.is_none());
        assert_eq!(inner.work_items.len(), NUM_WORKITEMS);
    }

    /// `launcher_determine_sleep` clamps to [MIN, MAX] without touching the catalog:
    /// an empty schedule sleeps a whole naptime; !canlaunch likewise. This exercises
    /// the schedule arithmetic without driving `get_database_list` into the deferred
    /// pg_database stub (the launcher's real loop requires a live catalog, so it is
    /// only started by the supervisor when autovacuum is enabled).
    #[tokio::test]
    async fn determine_sleep_clamps_to_min_and_max() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        set_autovacuum_naptime(60);

        // Empty schedule: a whole naptime (60s), clamped under MAX (300s).
        let mut st = LauncherState { database_list: VecDeque::new() };
        let nap = launcher_determine_sleep(&shared, &mut st, true, true).await;
        assert_eq!(nap, 60 * USECS_PER_SEC);

        // A next-due entry far in the future is clamped to MAX_AUTOVAC_SLEEPTIME.
        st.database_list.push_front(AvlDbase {
            datid: Oid(1),
            next_worker: GetCurrentTimestamp() + 10_000 * USECS_PER_SEC,
        });
        let nap = launcher_determine_sleep(&shared, &mut st, true, true).await;
        assert_eq!(nap, MAX_AUTOVAC_SLEEPTIME * USECS_PER_SEC);

        // A due entry in the past is clamped UP to the minimum sleep time.
        st.database_list.clear();
        st.database_list.push_front(AvlDbase {
            datid: Oid(1),
            next_worker: GetCurrentTimestamp() - USECS_PER_SEC,
        });
        let nap = launcher_determine_sleep(&shared, &mut st, true, true).await;
        assert_eq!(nap, (MIN_AUTOVAC_SLEEPTIME * 1000.0) as i64);

        drop(shared);
    }

    /// The launcher exit guard clears the advertised proc on drop (the cleanup that
    /// runs on every launcher exit, including panic unwind), tested directly without
    /// running the catalog-dependent launcher loop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exit_guard_clears_advertised_proc() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let g = published_proc_global();

        my_proc_scope(async move {
            let aux = auxiliary_process_main_common_with_proc(
                shared.proc_signal(),
                BackendType::AUTOVAC_LAUNCHER,
            )
            .await;
            g.autovacuum_launcher_proc.store(aux.proc_number, Ordering::Release);
            assert_ne!(
                g.autovacuum_launcher_proc.load(Ordering::Acquire),
                INVALID_PROC_NUMBER
            );

            let guard = AutoVacLauncherExitGuard {
                g: g.clone(),
                proc_signal: shared.proc_signal().clone(),
                slot_key: aux.slot_key,
            };
            drop(guard);

            assert_eq!(
                g.autovacuum_launcher_proc.load(Ordering::Acquire),
                INVALID_PROC_NUMBER,
                "exit guard clears the advertised proc"
            );
        })
        .await;
    }

    /// The worker-claim/release handshake on WorkerInfo, WITHOUT driving
    /// do_autovacuum: simulate the launcher setting av_startingWorker, then a worker
    /// claiming it (moved to running, proc recorded, starting cleared) and releasing
    /// it via free_worker_info (back on the freelist, rebalance flagged).
    #[test]
    fn worker_claim_and_release_handshake() {
        let shmem = AutoVacuumShmem::new();
        let total = shmem.lock().free_workers.len();

        // Launcher: do_start_worker would pop a free slot and set starting_worker.
        let idx = {
            let mut inner = shmem.lock();
            let idx = inner.free_workers.pop().expect("a free slot");
            inner.workers[idx].wi_dboid = Oid(12345);
            inner.workers[idx].wi_launchtime = GetCurrentTimestamp();
            inner.starting_worker = Some(idx);
            idx
        };
        assert_eq!(shmem.lock().free_workers.len(), total - 1);

        // Worker: claim the starting slot.
        {
            let mut inner = shmem.lock();
            let claimed = inner.starting_worker.take().expect("starting worker present");
            assert_eq!(claimed, idx);
            inner.workers[claimed].wi_proc = 7;
            inner.running_workers.push(claimed);
        }
        {
            let inner = shmem.lock();
            assert!(inner.starting_worker.is_none());
            assert_eq!(inner.running_workers, vec![idx]);
            assert_eq!(inner.workers[idx].wi_proc, 7);
            assert_eq!(inner.workers[idx].wi_dboid, Oid(12345));
        }

        // Worker exit: free_worker_info returns the slot and flags a rebalance.
        shmem.av_signal[AutoVacuumSignal::Rebalance as usize].store(false, Ordering::Release);
        free_worker_info(&shmem, idx);
        let inner = shmem.lock();
        assert!(inner.running_workers.is_empty());
        assert_eq!(inner.free_workers.len(), total);
        assert_eq!(inner.workers[idx].wi_proc, INVALID_PROC_NUMBER);
        assert_eq!(inner.workers[idx].wi_dboid, InvalidOid);
        drop(inner);
        assert!(
            shmem.av_signal[AutoVacuumSignal::Rebalance as usize].load(Ordering::Acquire),
            "free_worker_info flags a rebalance"
        );
    }

    /// av_worker_available honors the worker_slots - max_workers reservation, and
    /// AutoVacuumRequestWork records a work item.
    #[test]
    fn worker_available_and_request_work() {
        let shared = fresh_shared();
        let shmem = published_shmem();

        // With a full freelist there are free slots beyond the reservation.
        assert!(av_worker_available(&shmem));

        // Record a work item.
        let ok = auto_vacuum_request_work(
            AutoVacuumWorkItemType::AVW_BRINSummarizeRange,
            Oid(99),
            Oid(100),
            0,
        );
        assert!(ok, "a fresh shmem has free work-item slots");
        let used = shmem.lock().work_items.iter().filter(|w| w.avw_used).count();
        assert_eq!(used, 1);
        drop(shared);
    }
}
