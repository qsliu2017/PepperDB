//! Pluggable background workers implementation. Translated from backend/postmaster/bgworker.c.
//!
//! Background workers are auxiliary processes that the server starts on behalf
//! of extensions or core subsystems (parallel query, logical replication, and
//! so on). Workers are either registered statically at startup or requested
//! dynamically by a running backend. Each registered worker occupies a slot in
//! a fixed-size table; the registering backend receives a handle and tracks the
//! worker's lifecycle through the slot's `pid`, `generation`, and `in_use`
//! fields. In PostgreSQL that slot table lives in shared memory and is
//! coordinated with a lockless protocol so the postmaster, which must never
//! take a lock, can hand slots back and forth with regular backends: a backend
//! claims a slot by fully initializing it and setting `in_use`, after which the
//! postmaster owns it; a backend may still set `terminate` to ask that the
//! worker not be restarted. A separate pair of registered/terminated counters
//! caps the number of concurrent parallel workers without locking.
//!
//! In the single-process model there is no separate postmaster and no shared
//! memory, so the slot table is a `Mutex`-guarded vector held in shared process
//! state and reached through a process-wide handle. The lockless constraint no
//! longer applies, but the `in_use`/`terminate`/`pid`/`generation` handoff
//! semantics are kept intact because callers across tasks still depend on them.
//! Dynamic library loading is not supported: a single binary cannot load an
//! external function by name, so the only entry-point source is the in-core
//! registry mapping a worker name to a Rust function. Spawning a worker is
//! delegated to a hook installed by the supervisor; absent a hook the request
//! is still recorded faithfully in the slot and the worker simply never
//! appears, mirroring PostgreSQL, where the registering backend only writes the
//! request and the child arrives asynchronously. The child entry point,
//! signal-based death handling, and `sigsetjmp` recovery are not reproduced: a
//! worker is an async task whose panic propagates to the supervisor, and the
//! signal-mask block/unblock helpers are no-ops.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, MutexGuard};

use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::postmaster::bgworker::{
    BackgroundWorker, BackgroundWorkerHandle, BgWorkerStartTime, BgworkerBypassFlags,
    BgworkerFlags, BgworkerMainType, BgwHandleStatus, BGW_NEVER_RESTART,
};

/// PG `InvalidPid` (miscadmin.h): a slot that is in use but whose worker has not
/// been started yet. `0` means a previously-running worker has exited.
pub const INVALID_PID: i32 = -1;

/// PG `MAX_PARALLEL_WORKER_LIMIT` (bgworker_internals.h).
pub const MAX_PARALLEL_WORKER_LIMIT: u32 = 1024;

// ---------------------------------------------------------------------------
// GUC parameters (PG globals). Process-global atomics with accessors; settable
// for tests. TODO(guc): drive from the GUC bridge once it lands.
// ---------------------------------------------------------------------------

/// PG `max_worker_processes` (default 8): the slot-table size.
static MAX_WORKER_PROCESSES: AtomicU32 = AtomicU32::new(8);
/// PG `max_parallel_workers` (default 8): cap on concurrent parallel workers.
static MAX_PARALLEL_WORKERS: AtomicU32 = AtomicU32::new(8);

/// PG `max_worker_processes`.
pub fn max_worker_processes() -> u32 {
    MAX_WORKER_PROCESSES.load(Ordering::Relaxed)
}

/// Set `max_worker_processes` (GUC assignment / tests). Must be set BEFORE
/// `SharedState::new` so the slot table is sized correctly.
pub fn set_max_worker_processes(n: u32) {
    MAX_WORKER_PROCESSES.store(n, Ordering::Relaxed);
}

/// PG `max_parallel_workers`.
pub fn max_parallel_workers() -> u32 {
    MAX_PARALLEL_WORKERS.load(Ordering::Relaxed)
}

/// Set `max_parallel_workers` (GUC assignment / tests).
pub fn set_max_parallel_workers(n: u32) {
    MAX_PARALLEL_WORKERS.store(n, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Slot table (PG `BackgroundWorkerArray` / `BackgroundWorkerSlot`)
// ---------------------------------------------------------------------------

/// PG `BackgroundWorkerSlot`. One entry of the shared slot table. The
/// `in_use`/`terminate`/`pid`/`generation` handoff fields keep PG's semantics:
/// `in_use=false` -> a backend may claim it; `in_use=true` -> the postmaster
/// (here: the spawner/supervisor) owns it; `terminate` may be set even while in
/// use; `generation` bumps on every (re)use so a stale handle is detectable.
#[derive(Clone)]
pub struct BackgroundWorkerSlot {
    /// PG `in_use`.
    pub in_use: bool,
    /// PG `terminate`: ask the supervisor not to restart this worker.
    pub terminate: bool,
    /// PG `pid`: `INVALID_PID` = not started yet; `0` = dead; `>0` = running.
    pub pid: i32,
    /// PG `generation`: incremented when the slot is recycled.
    pub generation: u64,
    /// PG `worker`: the registration descriptor.
    pub worker: BackgroundWorker,
}

impl BackgroundWorkerSlot {
    /// A free (not-in-use) slot. PG zeroes the array and only sets `in_use`.
    fn empty() -> Self {
        Self {
            in_use: false,
            terminate: false,
            pid: INVALID_PID,
            generation: 0,
            worker: empty_worker(),
        }
    }
}

/// A placeholder `BackgroundWorker` for an unused slot (the descriptor is only
/// meaningful when `in_use`). Avoids `Option` so the slot layout matches PG's
/// inline `BackgroundWorker worker`.
fn empty_worker() -> BackgroundWorker {
    BackgroundWorker {
        bgw_name: String::new(),
        bgw_type: String::new(),
        bgw_flags: BgworkerFlags::empty(),
        bgw_start_time: BgWorkerStartTime::PostmasterStart,
        bgw_restart_time: BGW_NEVER_RESTART,
        bgw_library_name: String::new(),
        bgw_function_name: String::new(),
        bgw_main_arg: Datum(0),
        bgw_extra: String::new(),
        bgw_notify_pid: 0,
    }
}

/// PG `BackgroundWorkerArray` interior, guarded by `BackgroundWorkerLock`.
struct BackgroundWorkerArray {
    /// PG `total_slots` (== `max_worker_processes` at init).
    total_slots: usize,
    /// PG `slot[]`.
    slots: Vec<BackgroundWorkerSlot>,
}

/// PG `BackgroundWorkerData`. An `Arc` on [`SharedState`] (the ipci.c
/// `BackgroundWorkerShmemInit` slot) AND published process-wide via
/// [`set_bgworker_shmem`] so dynamic registration / handle polling, called by
/// arbitrary backends, reach one struct without a `SharedState` handle.
pub struct BackgroundWorkerShmem {
    /// Everything under `BackgroundWorkerLock`.
    array: Mutex<BackgroundWorkerArray>,
    /// PG `parallel_register_count` (bumped under the lock by registrants).
    parallel_register_count: AtomicU32,
    /// PG `parallel_terminate_count` (bumped by the postmaster, lockless in PG).
    parallel_terminate_count: AtomicU32,
}

impl BackgroundWorkerShmem {
    /// PG `BackgroundWorkerShmemInit`: a slot table sized to
    /// `max_worker_processes`, all slots free, counters zeroed. (PG's
    /// `!IsUnderPostmaster` branch copies the static `BackgroundWorkerList` into
    /// the leading slots; here static workers register into the table directly
    /// via [`register_background_worker`], so init just produces empty slots.)
    pub fn new() -> Arc<Self> {
        let total_slots = max_worker_processes() as usize;
        Arc::new(Self {
            array: Mutex::new(BackgroundWorkerArray {
                total_slots,
                slots: vec![BackgroundWorkerSlot::empty(); total_slots],
            }),
            parallel_register_count: AtomicU32::new(0),
            parallel_terminate_count: AtomicU32::new(0),
        })
    }

    /// Lock the slot table. PG uses `BackgroundWorkerLock`; a panic on a worker or
    /// registering task (the supervisor restarts tasks) must not brick the table
    /// via poison -- every critical section is a complete, short update, so
    /// recover the guard if poisoned.
    fn lock(&self) -> MutexGuard<'_, BackgroundWorkerArray> {
        self.array.lock()
    }

    /// PG `parallel_register_count - parallel_terminate_count`: the active count.
    /// Wraparound-safe (PG relies on the subtraction wrapping correctly).
    fn parallel_active(&self) -> u32 {
        self.parallel_register_count
            .load(Ordering::Relaxed)
            .wrapping_sub(self.parallel_terminate_count.load(Ordering::Relaxed))
    }
}

/// The process-wide `BackgroundWorkerShmem` (PG's `static BackgroundWorkerData`).
/// First publish wins (tests build multiple `SharedState`s).
static BGWORKER_SHMEM: OnceLock<Arc<BackgroundWorkerShmem>> = OnceLock::new();

/// Publish the process-wide `BackgroundWorkerShmem`. First publish wins; returns
/// whether this call won.
pub fn set_bgworker_shmem(shmem: Arc<BackgroundWorkerShmem>) -> bool {
    BGWORKER_SHMEM.set(shmem).is_ok()
}

/// The process-wide `BackgroundWorkerShmem`, if `SharedState::new` has run.
pub fn bgworker_shmem() -> Option<&'static Arc<BackgroundWorkerShmem>> {
    BGWORKER_SHMEM.get()
}

/// Construct a handle for an existing (slot, generation). PG builds the
/// `BackgroundWorkerHandle` inline in `RegisterDynamicBackgroundWorker`; exposed
/// as a named ctor (PG `GetBackgroundWorkerHandle`, used by parallel.c) so other
/// modules can rebuild a handle from a known slot.
///
/// PG `GetBackgroundWorkerHandle`.
pub fn get_background_worker_handle(slot: usize, generation: u64) -> BackgroundWorkerHandle {
    BackgroundWorkerHandle { slot, generation }
}

// ---------------------------------------------------------------------------
// In-core entry-point registry (PG `InternalBGWorkers[]`, generalized)
// ---------------------------------------------------------------------------

// deleted by redesign: no dynamic library loading in a single binary
//   - LookupBackgroundWorkerFunction's load_external_function() path
//   - the "postgres"-vs-external library distinction
// The supported path is this name -> fn registry (PG's InternalBGWorkers table,
// here a process-global map so subsystems register their entry points at init).

/// The process-wide entry-point registry (PG `InternalBGWorkers[]`). Maps a
/// worker's `bgw_function_name` to its in-core Rust entry point. Subsystems
/// (parallel.c, logicallauncher.c, ...) install their entries at init.
static INTERNAL_BGWORKERS: OnceLock<Mutex<Vec<(String, BgworkerMainType)>>> = OnceLock::new();

fn internal_bgworkers() -> &'static Mutex<Vec<(String, BgworkerMainType)>> {
    INTERNAL_BGWORKERS.get_or_init(|| Mutex::new(Vec::new()))
}

/// Register an in-core bgworker entry point under `fn_name`. Replaces PG's
/// compile-time `InternalBGWorkers[]` array (a single binary can populate it at
/// startup). Idempotent: re-registering the same name overwrites.
pub fn register_internal_bgworker(fn_name: &str, fn_addr: BgworkerMainType) {
    let mut tbl = internal_bgworkers().lock();
    if let Some(e) = tbl.iter_mut().find(|(n, _)| n == fn_name) {
        e.1 = fn_addr;
    } else {
        tbl.push((fn_name.to_string(), fn_addr));
    }
}

/// PG `LookupBackgroundWorkerFunction`: resolve a worker entry point by name. The
/// library name is ignored (no dynamic loading); all entry points are in-core.
/// Returns `None` if unknown (PG `elog(ERROR)`s; callers here decide).
pub fn lookup_background_worker_function(funcname: &str) -> Option<BgworkerMainType> {
    let tbl = internal_bgworkers().lock();
    tbl.iter().find(|(n, _)| n == funcname).map(|(_, f)| *f)
}

// ---------------------------------------------------------------------------
// Worker-spawn hook (PG SendPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE))
// ---------------------------------------------------------------------------

/// A hook the supervisor (17f) installs so a dynamic registration can request the
/// worker task be spawned. Mirrors PG signalling the postmaster to fork the
/// child; the registrant never spawns directly. The handle locates the claimed
/// slot.
pub type BgworkerSpawner = Box<dyn Fn(BackgroundWorkerHandle) + Send + Sync + 'static>;

static BGWORKER_SPAWNER: OnceLock<BgworkerSpawner> = OnceLock::new();

/// Install the worker-spawn hook (17f). First install wins.
pub fn set_bgworker_spawner(spawner: BgworkerSpawner) -> bool {
    BGWORKER_SPAWNER.set(spawner).is_ok()
}

/// PG `SendPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE)`. If the supervisor
/// hook is installed it spawns the worker task; otherwise (unit tests) the
/// request was already recorded in the slot (`in_use=true`, `pid=InvalidPid`) and
/// this just logs -- faithful: PG's registrant only writes shmem + signals; the
/// child appears asynchronously.
fn notify_postmaster_worker_change(handle: BackgroundWorkerHandle) {
    if let Some(spawn) = BGWORKER_SPAWNER.get() {
        spawn(handle);
    } else {
        crate::elog!(
            crate::utils::elog::DEBUG1,
            format!(
                "background worker requested in slot {} (no spawner installed)",
                handle.slot
            )
        );
    }
}

// ---------------------------------------------------------------------------
// Static registration (PG `BackgroundWorkerList` + `RegisterBackgroundWorker`)
// ---------------------------------------------------------------------------

/// PG `RegisteredBgWorker`: a postmaster-private registry entry. The static list
/// here keeps only the registration descriptor + restart bookkeeping; the
/// per-running PID is tracked in the shmem slot once the worker starts.
#[derive(Clone)]
pub struct RegisteredBgWorker {
    /// PG `rw_worker`.
    pub rw_worker: BackgroundWorker,
    /// PG `rw_crashed_at` (TimestampTz; 0 = never).
    pub rw_crashed_at: i64,
    /// PG `rw_terminate`.
    pub rw_terminate: bool,
}

/// PG `BackgroundWorkerList` (postmaster-private, static-registered workers).
/// Process-global: static workers register here before shmem init.
static BACKGROUND_WORKER_LIST: OnceLock<Mutex<Vec<RegisteredBgWorker>>> = OnceLock::new();

fn background_worker_list() -> &'static Mutex<Vec<RegisteredBgWorker>> {
    BACKGROUND_WORKER_LIST.get_or_init(|| Mutex::new(Vec::new()))
}

/// PG `SanityCheckBackgroundWorker`: validate the registration. Returns `Ok(())`
/// if it looks valid, `Err(msg)` otherwise (PG `ereport`s at `elevel`; here the
/// caller decides whether to log or panic). May normalize `bgw_type` to
/// `bgw_name` if empty (PG does this in place).
fn sanity_check_background_worker(worker: &mut BackgroundWorker) -> Result<(), String> {
    // We used to support workers not connected to shared memory; SHMEM_ACCESS is
    // a required flag now.
    if !worker.bgw_flags.contains(BgworkerFlags::SHMEM_ACCESS) {
        return Err(format!(
            "background worker \"{}\": background workers without shared memory access are not supported",
            worker.bgw_name
        ));
    }

    if worker.bgw_flags.contains(BgworkerFlags::BACKEND_DATABASE_CONNECTION)
        && worker.bgw_start_time == BgWorkerStartTime::PostmasterStart
    {
        return Err(format!(
            "background worker \"{}\": cannot request database access if starting at postmaster start",
            worker.bgw_name
        ));
    }

    // restart interval: >= 0 (seconds) or BGW_NEVER_RESTART, and <= one day.
    if (worker.bgw_restart_time < 0 && worker.bgw_restart_time != BGW_NEVER_RESTART)
        || i64::from(worker.bgw_restart_time) > USECS_PER_DAY / 1000
    {
        return Err(format!(
            "background worker \"{}\": invalid restart interval",
            worker.bgw_name
        ));
    }

    // Parallel workers may not be configured for restart (the
    // register/terminate-count accounting can't survive a crash-restart cycle).
    if worker.bgw_restart_time != BGW_NEVER_RESTART
        && worker.bgw_flags.contains(BgworkerFlags::CLASS_PARALLEL)
    {
        return Err(format!(
            "background worker \"{}\": parallel workers may not be configured for restart",
            worker.bgw_name
        ));
    }

    if worker.bgw_type.is_empty() {
        worker.bgw_type = worker.bgw_name.clone();
    }

    Ok(())
}

/// PG `USECS_PER_DAY` (timestamp.h): microseconds per day.
const USECS_PER_DAY: i64 = 86_400_000_000;

/// PG `RegisterBackgroundWorker`: register a static background worker. Valid only
/// before shmem init (PG: only from the postmaster / `_PG_init` of a
/// shared_preload_libraries module). Adds to the process-global list after a
/// sanity check + the `max_worker_processes` cap.
///
/// PG `RegisterBackgroundWorker`.
pub fn register_background_worker(worker: &BackgroundWorker) {
    let mut w = worker.clone();

    crate::elog!(
        crate::utils::elog::DEBUG1,
        format!("registering background worker \"{}\"", w.bgw_name)
    );

    if let Err(msg) = sanity_check_background_worker(&mut w) {
        crate::elog!(crate::utils::elog::LOG, msg.clone());
        return;
    }

    // Only dynamic workers can request start/stop notification.
    if w.bgw_notify_pid != 0 {
        crate::elog!(
            crate::utils::elog::LOG,
            format!(
                "background worker \"{}\": only dynamic background workers can request notification",
                w.bgw_name
            )
        );
        return;
    }

    let mut list = background_worker_list().lock();

    // Enforce the maximum number of workers.
    if list.len() + 1 > max_worker_processes() as usize {
        crate::elog!(
            crate::utils::elog::LOG,
            format!(
                "too many background workers (up to {} can be registered)",
                max_worker_processes()
            )
        );
        return;
    }

    list.push(RegisteredBgWorker {
        rw_worker: w,
        rw_crashed_at: 0,
        rw_terminate: false,
    });
}

/// Read-only snapshot of the static-registered worker list (PG iterates
/// `BackgroundWorkerList`). For the supervisor (17f) / tests.
pub fn registered_background_workers() -> Vec<RegisteredBgWorker> {
    background_worker_list().lock().clone()
}

// ---------------------------------------------------------------------------
// Dynamic registration + handle polling (backend side)
// ---------------------------------------------------------------------------

/// PG `RegisterDynamicBackgroundWorker`: register a worker from a regular backend.
/// Claims a free shmem slot, bumps its generation, and (if a spawner is
/// installed) requests the worker task be started. Returns the handle on success
/// or `None` if no slot is free / the parallel cap is hit / shmem is absent.
///
/// PG `RegisterDynamicBackgroundWorker`.
pub fn register_dynamic_background_worker(
    worker: &BackgroundWorker,
) -> Option<BackgroundWorkerHandle> {
    let shmem = bgworker_shmem()?;

    let mut w = worker.clone();
    if let Err(msg) = sanity_check_background_worker(&mut w) {
        crate::elog!(crate::utils::elog::LOG, msg.clone());
        return None;
    }

    let parallel = w.bgw_flags.contains(BgworkerFlags::CLASS_PARALLEL);

    let handle = {
        let mut arr = shmem.lock();

        // If parallel, refuse if there are already too many parallel workers.
        if parallel && shmem.parallel_active() >= max_parallel_workers() {
            return None;
        }

        // Look for an unused slot; if found, grab it.
        let found = arr.slots.iter_mut().enumerate().find(|(_, s)| !s.in_use);
        let (slotno, slot) = found?;

        slot.worker = w;
        slot.pid = INVALID_PID; // not started yet
        slot.generation += 1;
        slot.terminate = false;
        let generation = slot.generation;
        slot.in_use = true;
        if parallel {
            shmem.parallel_register_count.fetch_add(1, Ordering::Relaxed);
        }
        BackgroundWorkerHandle { slot: slotno, generation }
    };

    // Tell the supervisor to notice the change (spawn the worker task).
    notify_postmaster_worker_change(handle);
    Some(handle)
}

/// PG `StartBackgroundWorker` (the child side). The supervisor's spawner hook
/// (17f) runs this in a tokio task: resolve the slot's in-core entry point, mark
/// the slot running (a synthetic positive pid), invoke the worker body, then
/// record the exit. Collapses PG's fork + `BackgroundWorkerMain` into one task;
/// the worker's panic propagates to the supervisor's `catch_unwind` (no in-task
/// sigsetjmp recovery -- see module header). No-op if the handle is stale.
pub fn run_background_worker(handle: BackgroundWorkerHandle) {
    let Some(shmem) = bgworker_shmem() else {
        return;
    };
    let (funcname, main_arg) = {
        let arr = shmem.lock();
        match arr.slots.get(handle.slot) {
            Some(slot) if handle.generation == slot.generation && slot.in_use => {
                (slot.worker.bgw_function_name.clone(), slot.worker.bgw_main_arg)
            }
            _ => return, // recycled slot: nothing to start
        }
    };
    let Some(entry) = lookup_background_worker_function(&funcname) else {
        crate::elog!(
            crate::utils::elog::LOG,
            format!("background worker entry point \"{funcname}\" not found")
        );
        report_background_worker_exit(handle.slot);
        return;
    };

    // Mark running. PG assigns the child's real pid; we use the slot index + 1 so
    // it is positive (>0 == running) and distinct from INVALID_PID / 0.
    report_background_worker_pid(handle.slot, handle.slot as i32 + 1);
    entry(main_arg);
    report_background_worker_exit(handle.slot);
}

/// PG `GetBackgroundWorkerPid`: report a dynamic worker's status + pid. The
/// handle's generation must still match the slot and the slot be in use, else the
/// slot was recycled and the worker is treated as stopped.
///
/// PG `GetBackgroundWorkerPid`.
pub fn get_background_worker_pid(handle: &BackgroundWorkerHandle) -> (BgwHandleStatus, i32) {
    let Some(shmem) = bgworker_shmem() else {
        return (BgwHandleStatus::Stopped, 0);
    };
    let arr = shmem.lock();
    let pid = match arr.slots.get(handle.slot) {
        Some(slot) if handle.generation == slot.generation && slot.in_use => slot.pid,
        _ => 0,
    };
    drop(arr);

    if pid == 0 {
        (BgwHandleStatus::Stopped, 0)
    } else if pid == INVALID_PID {
        (BgwHandleStatus::NotYetStarted, 0)
    } else {
        (BgwHandleStatus::Started, pid)
    }
}

/// PG `WaitForBackgroundWorkerStartup`: poll until the worker leaves the
/// not-yet-started state. Async (PG sleeps on `MyLatch`); a short backoff poll
/// over the shmem slot. Never returns `NotYetStarted`.
///
/// PG `WaitForBackgroundWorkerStartup`.
pub async fn wait_for_background_worker_startup(
    handle: &BackgroundWorkerHandle,
) -> (BgwHandleStatus, i32) {
    loop {
        let (status, pid) = get_background_worker_pid(handle);
        if status != BgwHandleStatus::NotYetStarted {
            return (status, pid);
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// PG `WaitForBackgroundWorkerShutdown`: poll until the worker is stopped.
///
/// PG `WaitForBackgroundWorkerShutdown`.
pub async fn wait_for_background_worker_shutdown(
    handle: &BackgroundWorkerHandle,
) -> BgwHandleStatus {
    loop {
        let (status, _pid) = get_background_worker_pid(handle);
        if status == BgwHandleStatus::Stopped {
            return status;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// PG `TerminateBackgroundWorker`: ask the supervisor to terminate the worker by
/// setting the slot's `terminate` flag (unless the slot was already recycled).
///
/// PG `TerminateBackgroundWorker`.
pub fn terminate_background_worker(handle: &BackgroundWorkerHandle) {
    let Some(shmem) = bgworker_shmem() else {
        return;
    };
    let signal = {
        let mut arr = shmem.lock();
        match arr.slots.get_mut(handle.slot) {
            Some(slot) if handle.generation == slot.generation => {
                slot.terminate = true;
                true
            }
            _ => false,
        }
    };
    if signal {
        notify_postmaster_worker_change(*handle);
    }
}

/// PG `GetBackgroundWorkerTypeByPid`: the `bgw_type` of the worker with this pid,
/// or `None` if no slot matches. (PG returns a pointer into a static buffer; here
/// an owned `String`.)
///
/// PG `GetBackgroundWorkerTypeByPid`.
pub fn get_background_worker_type_by_pid(pid: i32) -> Option<String> {
    let shmem = bgworker_shmem()?;
    let arr = shmem.lock();
    arr.slots
        .iter()
        .find(|s| s.pid > 0 && s.pid == pid)
        .map(|s| s.worker.bgw_type.clone())
}

// ---------------------------------------------------------------------------
// Postmaster-side bookkeeping (supervisor side, 17f)
// ---------------------------------------------------------------------------
//
// PG threads these through the postmaster-private `RegisteredBgWorker` list and
// the lockless slot protocol. Under one process the supervisor (17f) drives them
// directly against the shmem slot table by slot index; the `pid`/`in_use`/
// `terminate`/`generation` transitions are kept faithful. `notify_pid` SIGUSR1
// signalling is dropped (waiters poll the slot via the async waiters above).

/// PG `BackgroundWorkerStateChange`: notice changes other tasks made to the slot
/// table. In PG the postmaster scans for newly-registered or to-be-terminated
/// workers; here the spawner hook already runs at registration, so this is the
/// reconcile pass the supervisor (17f) calls. De-fanged: it never panics; it
/// frees slots marked `terminate` whose worker is not running and returns the
/// freed slot indices for the supervisor to act on. `allow_new_workers=false`
/// (shutdown) marks every newly-seen slot for termination.
///
/// PG `BackgroundWorkerStateChange`.
pub fn background_worker_state_change(allow_new_workers: bool) -> Vec<usize> {
    let Some(shmem) = bgworker_shmem() else {
        return Vec::new();
    };
    let mut freed = Vec::new();
    let mut arr = shmem.lock();
    for slotno in 0..arr.total_slots {
        let slot = &mut arr.slots[slotno];
        if !slot.in_use {
            continue;
        }
        // During shutdown, refuse not-yet-started workers by marking them.
        if !allow_new_workers && slot.pid == INVALID_PID {
            slot.terminate = true;
        }
        // Free a to-be-terminated slot whenever its worker is not running
        // (never-started INVALID_PID or already-exited 0); a positive pid is
        // still running and must wait. PG frees the slot unconditionally so any
        // waiter is awoken even if the worker never ran.
        if slot.terminate && slot.pid <= 0 {
            if slot.worker.bgw_flags.contains(BgworkerFlags::CLASS_PARALLEL) {
                shmem.parallel_terminate_count.fetch_add(1, Ordering::Relaxed);
            }
            slot.pid = 0;
            slot.in_use = false;
            freed.push(slotno);
        }
    }
    freed
}

/// PG `ForgetBackgroundWorker`: release a slot for reuse (the supervisor calls
/// this once a worker is gone). Bumps `parallel_terminate_count` for parallel
/// workers, then clears `in_use`. No-op for an already-free / out-of-range slot.
///
/// PG `ForgetBackgroundWorker`.
pub fn forget_background_worker(slotno: usize) {
    let Some(shmem) = bgworker_shmem() else {
        return;
    };
    let mut arr = shmem.lock();
    let Some(slot) = arr.slots.get_mut(slotno) else {
        return;
    };
    if !slot.in_use {
        return;
    }
    if slot.worker.bgw_flags.contains(BgworkerFlags::CLASS_PARALLEL) {
        shmem.parallel_terminate_count.fetch_add(1, Ordering::Relaxed);
    }
    slot.in_use = false;
}

/// PG `ReportBackgroundWorkerPID`: record a newly-launched worker's pid in its
/// slot (the supervisor calls this after spawning). PG also SIGUSR1s the
/// notify_pid; here waiters poll, so signalling is dropped.
///
/// PG `ReportBackgroundWorkerPID`.
pub fn report_background_worker_pid(slotno: usize, pid: i32) {
    let Some(shmem) = bgworker_shmem() else {
        return;
    };
    let mut arr = shmem.lock();
    if let Some(slot) = arr.slots.get_mut(slotno) {
        slot.pid = pid;
    }
}

/// PG `ReportBackgroundWorkerExit`: record that a worker exited (pid -> 0). If the
/// slot is marked `terminate` or the worker is never-restart, free the slot too
/// (PG deregisters it before notifying the waiter).
///
/// PG `ReportBackgroundWorkerExit`.
pub fn report_background_worker_exit(slotno: usize) {
    let Some(shmem) = bgworker_shmem() else {
        return;
    };
    let forget = {
        let mut arr = shmem.lock();
        match arr.slots.get_mut(slotno) {
            Some(slot) => {
                slot.pid = 0;
                slot.terminate || slot.worker.bgw_restart_time == BGW_NEVER_RESTART
            }
            None => false,
        }
    };
    if forget {
        forget_background_worker(slotno);
    }
}

/// PG `BackgroundWorkerStopNotifications`: cancel SIGUSR1 notifications for an
/// exiting backend's pid. Single-process waiters poll, so this just clears the
/// stored `notify_pid` on matching slots for fidelity.
///
/// PG `BackgroundWorkerStopNotifications`.
pub fn background_worker_stop_notifications(pid: i32) {
    let Some(shmem) = bgworker_shmem() else {
        return;
    };
    let mut arr = shmem.lock();
    for slot in &mut arr.slots {
        if slot.in_use && slot.worker.bgw_notify_pid == pid {
            slot.worker.bgw_notify_pid = 0;
        }
    }
}

/// PG `ForgetUnstartedBackgroundWorkers`: during a normal shutdown, cancel any
/// not-yet-started worker whose registrant is waiting, so the waiter is released.
/// Frees every in-use slot still at `INVALID_PID`. Returns the freed indices.
///
/// PG `ForgetUnstartedBackgroundWorkers`.
pub fn forget_unstarted_background_workers() -> Vec<usize> {
    let Some(shmem) = bgworker_shmem() else {
        return Vec::new();
    };
    let mut freed = Vec::new();
    {
        let arr = shmem.lock();
        for (slotno, slot) in arr.slots.iter().enumerate() {
            if slot.in_use && slot.pid == INVALID_PID {
                freed.push(slotno);
            }
        }
    }
    for &slotno in &freed {
        forget_background_worker(slotno);
    }
    freed
}

/// PG `ResetBackgroundWorkerCrashTimes`: after a crash-restart cycle, forget
/// never-restart workers and clear the crash time on the rest so they relaunch
/// immediately. Operates on in-use slots; returns the freed (never-restart)
/// indices.
///
/// PG `ResetBackgroundWorkerCrashTimes`.
pub fn reset_background_worker_crash_times() -> Vec<usize> {
    let Some(shmem) = bgworker_shmem() else {
        return Vec::new();
    };
    let mut to_forget = Vec::new();
    {
        let mut arr = shmem.lock();
        for (slotno, slot) in arr.slots.iter_mut().enumerate() {
            if !slot.in_use {
                continue;
            }
            if slot.worker.bgw_restart_time == BGW_NEVER_RESTART {
                to_forget.push(slotno);
            } else {
                // Parallel workers must be never-restart; non-parallel here.
                slot.pid = INVALID_PID;
                slot.worker.bgw_notify_pid = 0;
            }
        }
    }
    for &slotno in &to_forget {
        forget_background_worker(slotno);
    }
    to_forget
}

// ---------------------------------------------------------------------------
// Worker-side helpers (connection init + signal blocking)
// ---------------------------------------------------------------------------

/// PG `BackgroundWorkerInitializeConnection`: connect a worker to a database by
/// name. The `BackgroundWorker` argument (PG reads `MyBgworkerEntry`) is passed
/// explicitly since there is no per-process global here.
///
/// PG `BackgroundWorkerInitializeConnection`.
pub fn background_worker_initialize_connection(
    worker: &BackgroundWorker,
    dbname: Option<&str>,
    username: Option<&str>,
    flags: BgworkerBypassFlags,
) {
    let init_flags = bypass_to_init_flags(flags);

    // XXX is this the right errcode?
    if !worker
        .bgw_flags
        .contains(BgworkerFlags::BACKEND_DATABASE_CONNECTION)
    {
        // PG ereport(FATAL); the panic propagates to the task boundary.
        crate::elog!(
            crate::utils::elog::FATAL,
            "database connection requirement not indicated during registration".to_string()
        );
    }

    // PG: InitPostgres(dbname, InvalidOid, username, InvalidOid, init_flags, NULL).
    crate::miscadmin::InitPostgres(dbname, InvalidOid, username, InvalidOid, init_flags);

    // It had better not have gotten out of "init" mode yet.
    if !crate::miscadmin::is_init_processing_mode() {
        crate::elog!(
            crate::utils::elog::ERROR,
            "invalid processing mode in background worker".to_string()
        );
    }
    crate::miscadmin::set_processing_mode(crate::miscadmin::ProcessingMode::NormalProcessing);
}

/// PG `BackgroundWorkerInitializeConnectionByOid`: connect a worker to a database
/// by OID.
///
/// PG `BackgroundWorkerInitializeConnectionByOid`.
pub fn background_worker_initialize_connection_by_oid(
    worker: &BackgroundWorker,
    dboid: Oid,
    useroid: Oid,
    flags: BgworkerBypassFlags,
) {
    let init_flags = bypass_to_init_flags(flags);

    // XXX is this the right errcode?
    if !worker
        .bgw_flags
        .contains(BgworkerFlags::BACKEND_DATABASE_CONNECTION)
    {
        crate::elog!(
            crate::utils::elog::FATAL,
            "database connection requirement not indicated during registration".to_string()
        );
    }

    // PG: InitPostgres(NULL, dboid, NULL, useroid, init_flags, NULL).
    crate::miscadmin::InitPostgres(None, dboid, None, useroid, init_flags);

    if !crate::miscadmin::is_init_processing_mode() {
        crate::elog!(
            crate::utils::elog::ERROR,
            "invalid processing mode in background worker".to_string()
        );
    }
    crate::miscadmin::set_processing_mode(crate::miscadmin::ProcessingMode::NormalProcessing);
}

/// Translate the `BGWORKER_BYPASS_*` flags into `InitPostgres`' `INIT_PG_*` flags
/// (PG `BackgroundWorkerInitializeConnection*` prologue). `session_preload_libraries`
/// is never honored for a bgworker, so that flag is always off.
fn bypass_to_init_flags(flags: BgworkerBypassFlags) -> crate::miscadmin::InitPgFlags {
    let mut init = crate::miscadmin::InitPgFlags::empty();
    if flags.contains(BgworkerBypassFlags::ALLOWCONN) {
        init |= crate::miscadmin::InitPgFlags::OVERRIDE_ALLOW_CONNS;
    }
    if flags.contains(BgworkerBypassFlags::ROLELOGINCHECK) {
        init |= crate::miscadmin::InitPgFlags::OVERRIDE_ROLE_LOGIN;
    }
    init
}

/// PG `BackgroundWorkerBlockSignals`. No-op: the single-process model has no
/// per-process signal mask.
///
/// PG `BackgroundWorkerBlockSignals`.
pub fn background_worker_block_signals() {
    // deleted by redesign: no per-process signal masks
}

/// PG `BackgroundWorkerUnblockSignals`. No-op (see above).
///
/// PG `BackgroundWorkerUnblockSignals`.
pub fn background_worker_unblock_signals() {
    // deleted by redesign: no per-process signal masks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::postmaster::auxprocess::aux_test_serial as test_serial;
    use crate::shared_state::{SharedState, SharedStateConfig};

    /// Build (or reuse) the process-wide shmem and clear its slots/counters so a
    /// serial test starts from a known state. `bgworker_shmem()` is a first-wins
    /// `OnceLock`, so once a `SharedState` is built the same instance persists.
    fn fresh_published_shmem() -> Arc<BackgroundWorkerShmem> {
        let shared = SharedState::new(SharedStateConfig::default());
        let _ = set_bgworker_shmem(shared.bgworker().clone());
        let shmem = bgworker_shmem()
            .expect("a BackgroundWorkerShmem is published")
            .clone();
        {
            let mut arr = shmem.lock();
            for s in &mut arr.slots {
                *s = BackgroundWorkerSlot::empty();
            }
        }
        shmem.parallel_register_count.store(0, Ordering::Relaxed);
        shmem.parallel_terminate_count.store(0, Ordering::Relaxed);
        shmem
    }

    fn a_worker(name: &str) -> BackgroundWorker {
        BackgroundWorker {
            bgw_name: name.to_string(),
            bgw_type: String::new(),
            bgw_flags: BgworkerFlags::SHMEM_ACCESS,
            bgw_start_time: BgWorkerStartTime::RecoveryFinished,
            bgw_restart_time: BGW_NEVER_RESTART,
            bgw_library_name: "postgres".to_string(),
            bgw_function_name: "SomeWorkerMain".to_string(),
            bgw_main_arg: Datum(0),
            bgw_extra: String::new(),
            bgw_notify_pid: 0,
        }
    }

    /// Shmem init builds a slot table sized to max_worker_processes, all free, with
    /// the parallel counters at zero.
    #[test]
    fn shmem_init_empty_slots() {
        let shmem = BackgroundWorkerShmem::new();
        let arr = shmem.lock();
        assert_eq!(arr.slots.len(), max_worker_processes() as usize);
        assert_eq!(arr.total_slots, max_worker_processes() as usize);
        assert!(arr.slots.iter().all(|s| !s.in_use));
        assert_eq!(shmem.parallel_active(), 0);
    }

    /// RegisterDynamicBackgroundWorker claims a free slot (in_use=true,
    /// pid=INVALID_PID, generation bumped) and returns a usable handle.
    #[tokio::test]
    async fn register_dynamic_claims_slot_and_handle() {
        let _serial = test_serial().await;
        let shmem = fresh_published_shmem();

        let handle =
            register_dynamic_background_worker(&a_worker("w1")).expect("a slot is available");
        assert_eq!(handle.slot, 0);
        assert_eq!(handle.generation, 1);

        let arr = shmem.lock();
        assert!(arr.slots[0].in_use);
        assert_eq!(arr.slots[0].pid, INVALID_PID);
        assert_eq!(arr.slots[0].worker.bgw_name, "w1");
        drop(arr);

        // GetBackgroundWorkerPid on the fresh handle reports NotYetStarted.
        let (status, pid) = get_background_worker_pid(&handle);
        assert_eq!(status, BgwHandleStatus::NotYetStarted);
        assert_eq!(pid, 0);
    }

    /// ReportBackgroundWorkerPID then Exit drive the slot status transitions:
    /// NotYetStarted -> Started -> Stopped, and a never-restart worker frees the
    /// slot on exit.
    #[tokio::test]
    async fn report_pid_and_exit_transitions() {
        let _serial = test_serial().await;
        let shmem = fresh_published_shmem();

        let handle =
            register_dynamic_background_worker(&a_worker("w2")).expect("a slot is available");

        report_background_worker_pid(handle.slot, 4242);
        let (status, pid) = get_background_worker_pid(&handle);
        assert_eq!(status, BgwHandleStatus::Started);
        assert_eq!(pid, 4242);

        // GetBackgroundWorkerTypeByPid finds it (bgw_type defaulted to bgw_name).
        assert_eq!(get_background_worker_type_by_pid(4242).as_deref(), Some("w2"));

        report_background_worker_exit(handle.slot);
        let (status, _pid) = get_background_worker_pid(&handle);
        assert_eq!(status, BgwHandleStatus::Stopped);
        // never-restart: the slot was freed on exit.
        assert!(!shmem.lock().slots[handle.slot].in_use);
    }

    /// A stale handle (generation no longer matches) reports Stopped even though a
    /// new worker may occupy the slot.
    #[tokio::test]
    async fn stale_handle_reports_stopped() {
        let _serial = test_serial().await;
        let _shmem = fresh_published_shmem();

        let h1 = register_dynamic_background_worker(&a_worker("a")).unwrap();
        report_background_worker_exit(h1.slot); // frees slot (never-restart)
        let h2 = register_dynamic_background_worker(&a_worker("b")).unwrap();
        assert_eq!(h1.slot, h2.slot);
        assert_ne!(h1.generation, h2.generation);

        let (status, _) = get_background_worker_pid(&h1);
        assert_eq!(status, BgwHandleStatus::Stopped);
    }

    /// WaitForBackgroundWorkerStartup observes a status change made via the shmem
    /// helpers (no real spawned worker): a concurrent ReportBackgroundWorkerPID
    /// unblocks the await with Started.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_startup_via_shmem_helper() {
        let _serial = test_serial().await;
        let _shmem = fresh_published_shmem();

        let handle =
            register_dynamic_background_worker(&a_worker("w3")).expect("a slot is available");

        let h = handle;
        let reporter = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            report_background_worker_pid(h.slot, 999);
        });

        let (status, pid) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            wait_for_background_worker_startup(&handle),
        )
        .await
        .expect("startup wait should not time out");
        assert_eq!(status, BgwHandleStatus::Started);
        assert_eq!(pid, 999);
        reporter.await.unwrap();
    }

    /// TerminateBackgroundWorker sets the slot's terminate flag for the matching
    /// generation.
    #[tokio::test]
    async fn terminate_sets_flag() {
        let _serial = test_serial().await;
        let shmem = fresh_published_shmem();

        let handle = register_dynamic_background_worker(&a_worker("w4")).unwrap();
        terminate_background_worker(&handle);
        assert!(shmem.lock().slots[handle.slot].terminate);
    }

    /// Static RegisterBackgroundWorker accumulates into the process-global list and
    /// defaults bgw_type to bgw_name.
    #[tokio::test]
    async fn static_register_accumulates() {
        let _serial = test_serial().await;
        // Clear the static list for a known baseline.
        background_worker_list().lock().clear();

        register_background_worker(&a_worker("static1"));
        register_background_worker(&a_worker("static2"));

        let list = registered_background_workers();
        assert_eq!(list.len(), 2);
        // bgw_type was empty -> defaulted to bgw_name by the sanity check.
        assert_eq!(list[0].rw_worker.bgw_type, "static1");
        assert_eq!(list[1].rw_worker.bgw_type, "static2");
    }

    /// A worker missing SHMEM_ACCESS is rejected by static registration.
    #[tokio::test]
    async fn static_register_rejects_no_shmem_access() {
        let _serial = test_serial().await;
        background_worker_list().lock().clear();

        let mut w = a_worker("bad");
        w.bgw_flags = BgworkerFlags::empty();
        register_background_worker(&w);
        assert!(registered_background_workers().is_empty());
    }

    /// The in-core entry-point registry resolves a registered name and misses an
    /// unknown one.
    #[test]
    fn internal_registry_lookup() {
        fn dummy(_arg: Datum) {}
        register_internal_bgworker("DummyMain", dummy);
        assert!(lookup_background_worker_function("DummyMain").is_some());
        assert!(lookup_background_worker_function("NoSuchMain").is_none());
    }
}
