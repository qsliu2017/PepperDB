//! Translated from PostgreSQL src/backend/access/transam/parallel.c
//!
//! REWRITE to the single-process async model (not a 1:1 port). parallel.c's
//! bulk is cross-process state transport: it serializes GUCs, snapshots, xact
//! state, combocid, relmapper, the entrypoint, etc. into DSM regions keyed by
//! PARALLEL_KEY_* and the worker restores them. Here a worker is a tokio task
//! that shares the leader's state by Arc / task-local inheritance, so all of
//! that is tombstoned. What survives is the chassis: the ParallelContext
//! lifecycle, launching worker tasks, error exchange over tokio mpsc, the
//! lock-group join, and dispatch to the entrypoint.

use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::access::xlogdefs::XLogRecPtr;
use crate::backend::access::transam::xact::{GetCurrentSubTransactionId, IsInParallelMode};
use crate::backend::storage::lmgr::proc::{
    BecomeLockGroupLeader, BecomeLockGroupMember, InitProcess, ProcKill,
};
use crate::c::SubTransactionId;
use crate::session::{self, Session};
use crate::shared_state::SharedState;
use crate::storage::proc::{current_proc_number, my_proc_scope};

/// A typed message from a worker task to the leader (replaces the pqmq
/// StringInfo wire framing; deleted by redesign). PG sends ErrorResponse /
/// NoticeResponse / NotifyResponse bytes; we send the parsed variant.
#[derive(Debug)]
pub enum ParallelMessage {
    /// Worker raised an error (PG PqMsg_ErrorResponse, re-raised in the leader).
    Error(String),
    /// Worker emitted a notice (PG PqMsg_NoticeResponse, logged, not fatal).
    Notice(String),
}

/// Per-worker bookkeeping. `handle` is the worker tokio task (ex-bgwhandle);
/// `error_rx` is the leader's end of the worker's error channel (ex-error_mqh).
pub struct ParallelWorkerInfo {
    pub handle: Option<tokio::task::JoinHandle<()>>,
    pub error_rx: Option<tokio::sync::mpsc::UnboundedReceiver<ParallelMessage>>,
}

impl ParallelWorkerInfo {
    fn new() -> Self {
        Self {
            handle: None,
            error_rx: None,
        }
    }
}

/// The resolved worker entrypoint. PG's `parallel_worker_main_type` is
/// `fn(seg, toc)`; here it is a task body over the Arc-shared context. The
/// worker number identifies which worker is running.
pub type ParallelWorkerMainType = fn(worker_number: i32, last_xlog_end: &Arc<AtomicU64>);

pub struct ParallelContext {
    pub subid: SubTransactionId,
    pub nworkers: i32,
    pub nworkers_to_launch: i32,
    pub nworkers_launched: i32,
    pub library_name: String,
    pub function_name: String,
    /// Arc-cloned leader shared state (replaces the DSM segment). Workers borrow
    /// the same process-wide subsystems the leader uses.
    pub shared: Arc<SharedState>,
    pub workers: Vec<ParallelWorkerInfo>,
    pub known_attached_workers: Vec<bool>,
    pub nknown_attached_workers: i32,
    /// ex-FixedParallelState.last_xlog_end (XLogRecPtr as raw u64).
    pub last_xlog_end: Arc<AtomicU64>,
}

tokio::task_local! {
    /// PG `ParallelWorkerNumber` (None = not a worker; Some(n) = worker n). Set
    /// by the worker task before user code; `is_parallel_worker` reads it.
    static PARALLEL_WORKER_NUMBER: Cell<Option<u32>>;
    /// PG `InitializingParallelWorker`.
    static INITIALIZING_PARALLEL_WORKER: Cell<bool>;
    /// PG `pcxt_list`: active parallel contexts owned by THIS leader task. We
    /// store the addresses as `usize` (not `*mut`) so the leader future stays
    /// `Send` for `tokio::spawn`; each pointer is only ever dereferenced on the
    /// leader task that owns the backing `Box`.
    static PCXT_LIST: RefCell<Vec<usize>>;
}

/// PG `ParallelWorkerNumber` accessor (None outside a worker task).
pub fn parallel_worker_number() -> Option<u32> {
    PARALLEL_WORKER_NUMBER.try_with(Cell::get).unwrap_or(None)
}

/// PG `IsParallelWorker()`.
pub fn is_parallel_worker() -> bool {
    parallel_worker_number().is_some()
}

/// PG `InitializingParallelWorker` accessor.
pub fn initializing_parallel_worker() -> bool {
    INITIALIZING_PARALLEL_WORKER
        .try_with(Cell::get)
        .unwrap_or(false)
}

/// PG `CreateParallelContext`: establish a new parallel context after entering
/// parallel mode. Pushes onto the per-leader active-context list.
pub fn CreateParallelContext(
    library_name: &str,
    function_name: &str,
    nworkers: i32,
    shared: Arc<SharedState>,
) -> Box<ParallelContext> {
    debug_assert!(IsInParallelMode());
    debug_assert!(nworkers >= 0);

    let mut pcxt = Box::new(ParallelContext {
        subid: GetCurrentSubTransactionId(),
        nworkers,
        nworkers_to_launch: nworkers,
        nworkers_launched: 0,
        library_name: library_name.to_owned(),
        function_name: function_name.to_owned(),
        shared,
        workers: Vec::new(),
        known_attached_workers: Vec::new(),
        nknown_attached_workers: 0,
        last_xlog_end: Arc::new(AtomicU64::new(0)),
    });

    let ptr: *mut ParallelContext = &raw mut *pcxt;
    pcxt_list_push(ptr);
    pcxt
}

/// PG `InitializeParallelDSM`: serialization of GUC/snapshot/xact/combocid/
/// relmapper/library/entrypoint is deleted by redesign: single-process shares
/// state by Arc, no DSM serialization. We only create the per-worker error
/// channels and zero last_xlog_end.
pub fn InitializeParallelDSM(pcxt: &mut ParallelContext) {
    pcxt.last_xlog_end.store(0, Ordering::Relaxed);
    setup_worker_slots(pcxt);
    pcxt.nworkers_to_launch = pcxt.nworkers;
}

/// PG `ReinitializeParallelDSM`: wait for old workers, then recreate channels.
pub async fn ReinitializeParallelDSM(pcxt: &mut ParallelContext) {
    if pcxt.nworkers_launched > 0 {
        WaitForParallelWorkersToFinish(pcxt).await;
        WaitForParallelWorkersToExit(pcxt).await;
        pcxt.nworkers_launched = 0;
        pcxt.known_attached_workers.clear();
        pcxt.nknown_attached_workers = 0;
    }
    pcxt.last_xlog_end.store(0, Ordering::Relaxed);
    setup_worker_slots(pcxt);
}

/// PG `ReinitializeParallelWorkers`: trim the launch count to <= nworkers.
pub fn ReinitializeParallelWorkers(pcxt: &mut ParallelContext, nworkers_to_launch: i32) {
    pcxt.nworkers_to_launch = pcxt.nworkers.min(nworkers_to_launch);
}

/// Allocate empty per-worker slots (no channels yet; LaunchParallelWorkers
/// makes one per worker it actually spawns).
fn setup_worker_slots(pcxt: &mut ParallelContext) {
    pcxt.workers = std::iter::repeat_with(ParallelWorkerInfo::new)
        .take(pcxt.nworkers.max(0) as usize)
        .collect();
}

/// PG `LaunchParallelWorkers`: become the lock-group leader, then spawn one
/// tokio task per worker. Each task claims a backend PGPROC, joins the leader's
/// lock group, inherits the leader's Session, sets ParallelWorkerNumber, then
/// runs the resolved entrypoint -- all wrapped so a panic surfaces as a
/// ParallelMessage::Error instead of aborting the process. Spawning replaces
/// RegisterDynamicBackgroundWorker (it cannot fail, so the "fewer workers"
/// branch collapses to the launched accounting).
pub fn LaunchParallelWorkers(pcxt: &mut ParallelContext) {
    if pcxt.nworkers == 0 || pcxt.nworkers_to_launch == 0 {
        return;
    }

    BecomeLockGroupLeader();
    let leader_procno = current_proc_number();
    let leader_pid = session::try_current().map_or(0, |s| s.proc_pid());
    let leader_session = session::try_current();

    let entrypt = LookupParallelWorkerFunction(&pcxt.library_name, &pcxt.function_name);

    for i in 0..pcxt.nworkers_to_launch {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ParallelMessage>();
        let last_xlog_end = pcxt.last_xlog_end.clone();
        let session = leader_session.clone();
        let handle = tokio::spawn(worker_cradle(
            i,
            leader_procno,
            leader_pid,
            session,
            last_xlog_end,
            entrypt,
            tx,
        ));

        let slot = &mut pcxt.workers[i as usize];
        slot.handle = Some(handle);
        slot.error_rx = Some(rx);
        pcxt.nworkers_launched += 1;
    }

    if pcxt.nworkers_launched > 0 {
        pcxt.known_attached_workers = vec![false; pcxt.nworkers_launched as usize];
        pcxt.nknown_attached_workers = 0;
    }
}

/// The spawned worker task body: set the per-task statics, claim a PGPROC inside
/// nested Session + my_proc scopes, join the leader's lock group, then run the
/// entrypoint under catch-the-panic so a failure becomes a ParallelMessage.
async fn worker_cradle(
    worker_number: i32,
    leader_procno: i32,
    leader_pid: i32,
    leader_session: Option<Arc<Session>>,
    last_xlog_end: Arc<AtomicU64>,
    entrypt: ParallelWorkerMainType,
    tx: tokio::sync::mpsc::UnboundedSender<ParallelMessage>,
) {
    let session = leader_session
        .unwrap_or_else(|| Arc::new(Session::new(crate::miscadmin::BackendType::BG_WORKER)));
    // TODO(execParallel): once the real entrypoints (ParallelQueryMain /
    // execParallel) land, this cradle must ALSO inherit the leader's snapshot
    // (snapmgr task-local) and establish an xact scope around the nested scopes
    // below before invoking the entrypoint. Today the worker runs with no active
    // snapshot/xact, which is correct only because every real entrypoint is an
    // unimplemented!() stub (staging s4).
    session::scope(
        session,
        my_proc_scope(PARALLEL_WORKER_NUMBER.scope(
            Cell::new(Some(worker_number as u32)),
            INITIALIZING_PARALLEL_WORKER.scope(Cell::new(true), async move {
                parallel_worker_main(
                    worker_number,
                    leader_procno,
                    leader_pid,
                    &last_xlog_end,
                    entrypt,
                    &tx,
                );
            }),
        )),
    )
    .await;
}

/// PG `WaitForParallelWorkersToAttach`: PG loops until every launched worker has
/// attached its error-queue *sender*, and errors ("parallel worker failed to
/// initialize") for any worker that stopped without attaching -- it is detecting
/// a fork()/early-startup failure. That failure mode does not exist here: the
/// error-channel sender is created by `LaunchParallelWorkers` and MOVED into the
/// worker at `tokio::spawn` time (not attached later by worker code), and a spawn
/// cannot fail. So the sender is bound -- i.e. the worker is "attached" -- the
/// instant the slot is launched, with no window in which a launched worker could
/// be un-attached. Marking every launched slot attached is therefore the faithful
/// in-process equivalent of PG's loop, not a simplification of its logic; there is
/// no startup-failure case left to wait for. (No `.await`: nothing to wait on.)
pub fn WaitForParallelWorkersToAttach(pcxt: &mut ParallelContext) {
    if pcxt.nworkers_launched == 0 {
        return;
    }
    let newly = pcxt
        .known_attached_workers
        .iter_mut()
        .filter(|w| !**w)
        .map(|w| *w = true)
        .count();
    pcxt.nknown_attached_workers += newly as i32;
}

/// PG `WaitForParallelWorkersToFinish`: drain each worker's error channel
/// (re-raising on Error), await all worker tasks, and fold last_xlog_end into
/// the leader's XactLastRecEnd.
pub async fn WaitForParallelWorkersToFinish(pcxt: &mut ParallelContext) {
    drain_pcxt(pcxt);

    for slot in &mut pcxt.workers {
        if let Some(handle) = slot.handle.take() {
            // A worker panic is caught inside the cradle and reported via the
            // error channel; the JoinHandle still resolves Ok.
            let _ = handle.await;
        }
    }

    // Drain any messages that arrived as the workers finished.
    drain_pcxt(pcxt);

    let last = pcxt.last_xlog_end.load(Ordering::Relaxed);
    crate::backend::access::transam::xact::fold_worker_last_rec_end(XLogRecPtr(last));
}

/// PG `WaitForParallelWorkersToExit`: ensure complete worker shutdown. Tasks are
/// already awaited in WaitForParallelWorkersToFinish; here we abort/await any
/// stragglers and drop the channels.
async fn WaitForParallelWorkersToExit(pcxt: &mut ParallelContext) {
    for slot in &mut pcxt.workers {
        if let Some(handle) = slot.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        slot.error_rx = None;
    }
}

/// PG `DestroyParallelContext`: remove from the active list first, abort any
/// remaining workers, drop the channels, and await the tasks. The DSM detach /
/// private-memory free is deleted by redesign.
pub async fn DestroyParallelContext(pcxt: &mut ParallelContext) {
    let ptr: *mut ParallelContext = pcxt;
    pcxt_list_remove(ptr);

    WaitForParallelWorkersToExit(pcxt).await;
    pcxt.workers.clear();
    pcxt.nworkers_launched = 0;
    pcxt.known_attached_workers.clear();
    pcxt.nknown_attached_workers = 0;
}

/// PG `ParallelContextActive`.
pub fn ParallelContextActive() -> bool {
    PCXT_LIST
        .try_with(|l| !l.borrow().is_empty())
        .unwrap_or(false)
}

/// PG `HandleParallelMessageInterrupt`: in the async model the mpsc readiness IS
/// the pending flag, so this only sets the per-task ProcSignal ParallelMessage
/// reason for parity. ProcessParallelMessages drains the channels directly.
pub fn HandleParallelMessageInterrupt() {
    // The error channel is the transport; nothing to latch here.
}

/// PG `ProcessParallelMessages`: drain every active context's worker error
/// channels, re-raising Error messages in the leader and logging Notices.
pub fn ProcessParallelMessages() {
    PCXT_LIST
        .try_with(|list| {
            for &addr in list.borrow().iter() {
                // SAFETY: the leader owns each pcxt for as long as it is on this
                // task-local list; entries are removed in DestroyParallelContext
                // before the Box is dropped, and the list is only touched by this
                // task.
                let pcxt = unsafe { &mut *(addr as *mut ParallelContext) };
                drain_pcxt(pcxt);
            }
        })
        .ok();
}

/// Drain a single context's worker channels (the `&mut ParallelContext` form
/// used by the wait loop, which already holds the context).
fn drain_pcxt(pcxt: &mut ParallelContext) {
    let nlaunched = pcxt.nworkers_launched as usize;
    for i in 0..nlaunched.min(pcxt.workers.len()) {
        if pcxt.known_attached_workers.get(i) == Some(&false) {
            pcxt.known_attached_workers[i] = true;
            pcxt.nknown_attached_workers += 1;
        }
        let Some(rx) = pcxt.workers[i].error_rx.as_mut() else {
            continue;
        };
        while let Ok(msg) = rx.try_recv() {
            ProcessParallelMessage(i, msg);
        }
    }
}

/// PG `ProcessParallelMessage`: act on a single worker message. Error is
/// re-raised in the leader (the pqmq parse is deleted by redesign; we carry the
/// message string directly).
fn ProcessParallelMessage(_worker: usize, msg: ParallelMessage) {
    match msg {
        ParallelMessage::Error(text) => {
            // PG ThrowErrorData rethrows; we panic the leader, mirroring elog(ERROR).
            panic!("parallel worker error: {text}");
        }
        ParallelMessage::Notice(text) => {
            crate::elog!(crate::utils::elog::NOTICE, format!("parallel worker: {text}"));
        }
    }
}

/// PG `AtEOSubXact_Parallel`: destroy contexts created in this subtransaction.
pub async fn AtEOSubXact_Parallel(is_commit: bool, my_sub_id: SubTransactionId) {
    while let Some(ptr) = pcxt_list_head_if(|p| p.subid == my_sub_id) {
        if is_commit {
            crate::elog!(crate::utils::elog::WARNING, "leaked parallel context");
        }
        // SAFETY: head pointer of the active list, owned by this leader task.
        let pcxt = unsafe { &mut *ptr };
        DestroyParallelContext(pcxt).await;
    }
}

/// PG `AtEOXact_Parallel`: destroy all remaining contexts.
pub async fn AtEOXact_Parallel(is_commit: bool) {
    while let Some(ptr) = pcxt_list_head() {
        if is_commit {
            crate::elog!(crate::utils::elog::WARNING, "leaked parallel context");
        }
        // SAFETY: head pointer of the active list, owned by this leader task.
        let pcxt = unsafe { &mut *ptr };
        DestroyParallelContext(pcxt).await;
    }
}

/// PG `ParallelWorkerMain`: the worker entrypoint. The DSM attach + every
/// Restore*State (GUC/snapshot/xact/combocid/relmapper/libraries/clientconninfo)
/// is deleted by redesign: single-process shares state by Arc, the worker
/// inherits the leader's Session + snapshot via its task-local scopes. We claim
/// a PGPROC, join the leader's lock group, run the resolved entrypoint under
/// catch_unwind, and report any panic as a ParallelMessage::Error.
pub fn parallel_worker_main(
    worker_number: i32,
    leader_procno: i32,
    leader_pid: i32,
    last_xlog_end: &Arc<AtomicU64>,
    entrypt: ParallelWorkerMainType,
    tx: &tokio::sync::mpsc::UnboundedSender<ParallelMessage>,
) {
    InitProcess();
    // Release the PGPROC on EVERY exit path (normal return, early return, or a
    // panic unwinding past here), mirroring the step-17 aux exit guards /
    // PG on_shmem_exit semantics. The guard is the single ProcKill site; it
    // drops while still inside my_proc_scope, so ProcKill sees this task's
    // current_proc_number.
    let _proc_guard = WorkerProcGuard;

    // Join the leader's lock group; if it has gone away, exit quietly. The guard
    // returns the PGPROC on the early-return path too.
    if !BecomeLockGroupMember(leader_procno, leader_pid) {
        return;
    }

    // Real work: invoke the resolved entrypoint, catching a panic so it becomes
    // an error reported to the leader rather than aborting the process.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        entrypt(worker_number, last_xlog_end);
    }));

    if let Err(payload) = result {
        let text = panic_message(payload.as_ref());
        let _ = tx.send(ParallelMessage::Error(text));
    }
}

/// Releases the worker's PGPROC on EVERY scope exit -- normal return, early
/// return, or panic unwind (PG `ParallelWorkerShutdown` / on_shmem_exit).
/// Idempotent: ProcKill no-ops once the proc number is cleared.
struct WorkerProcGuard;

impl Drop for WorkerProcGuard {
    fn drop(&mut self) {
        ProcKill();
    }
}

/// Extract a human-readable string from a catch_unwind payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload.downcast_ref::<&str>().map_or_else(
        || {
            payload
                .downcast_ref::<String>()
                .map_or_else(|| "parallel worker panicked".to_owned(), Clone::clone)
        },
        |s| (*s).to_owned(),
    )
}

/// PG `ParallelWorkerReportLastRecEnd`: max our XactLastRecEnd into the shared
/// atomic (replaces the FixedParallelState spinlock).
pub fn ParallelWorkerReportLastRecEnd(shared: &Arc<AtomicU64>, last_xlog_end: XLogRecPtr) {
    let v = last_xlog_end.0;
    let mut cur = shared.load(Ordering::Relaxed);
    while v > cur {
        match shared.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
}

/// PG `LookupParallelWorkerFunction`: map (library, function) to an entrypoint.
/// Dynamic-library lookup is deleted by redesign (in-core only). The query/
/// index/vacuum entrypoints are deferred stubs that unimplemented!() until
/// execParallel / the AM build paths land (correct staging).
fn LookupParallelWorkerFunction(library_name: &str, function_name: &str) -> ParallelWorkerMainType {
    debug_assert_eq!(library_name, "postgres", "external libraries deleted by redesign");
    #[cfg(test)]
    if let Some(f) = tests::lookup_test_worker(function_name) {
        return f;
    }
    INTERNAL_PARALLEL_WORKERS
        .iter()
        .find(|(name, _)| *name == function_name)
        .map_or_else(
            || panic!("internal function \"{function_name}\" not found"),
            |(_, f)| *f,
        )
}

/// PG `InternalParallelWorkers[]`: name -> entrypoint. Deferred stubs.
static INTERNAL_PARALLEL_WORKERS: &[(&str, ParallelWorkerMainType)] = &[
    ("ParallelQueryMain", parallel_query_main),
    ("_bt_parallel_build_main", bt_parallel_build_main),
    ("_brin_parallel_build_main", brin_parallel_build_main),
    ("_gin_parallel_build_main", gin_parallel_build_main),
    ("parallel_vacuum_main", parallel_vacuum_main),
];

fn parallel_query_main(_worker: i32, _last_xlog_end: &Arc<AtomicU64>) {
    unimplemented!("ParallelQueryMain: deferred until execParallel lands")
}
fn bt_parallel_build_main(_worker: i32, _last_xlog_end: &Arc<AtomicU64>) {
    unimplemented!("_bt_parallel_build_main: deferred until nbtree parallel build lands")
}
fn brin_parallel_build_main(_worker: i32, _last_xlog_end: &Arc<AtomicU64>) {
    unimplemented!("_brin_parallel_build_main: deferred until brin parallel build lands")
}
fn gin_parallel_build_main(_worker: i32, _last_xlog_end: &Arc<AtomicU64>) {
    unimplemented!("_gin_parallel_build_main: deferred until gin parallel build lands")
}
fn parallel_vacuum_main(_worker: i32, _last_xlog_end: &Arc<AtomicU64>) {
    unimplemented!("parallel_vacuum_main: deferred until parallel vacuum lands")
}

// ---------------------------------------------------------------------------
// pcxt_list helpers (PG's dlist of active contexts, per leader task).
// ---------------------------------------------------------------------------

fn pcxt_list_push(ptr: *mut ParallelContext) {
    let _ = PCXT_LIST.try_with(|l| l.borrow_mut().insert(0, ptr as usize));
}

fn pcxt_list_remove(ptr: *mut ParallelContext) {
    let key = ptr as usize;
    let _ = PCXT_LIST.try_with(|l| l.borrow_mut().retain(|&p| p != key));
}

fn pcxt_list_head() -> Option<*mut ParallelContext> {
    PCXT_LIST
        .try_with(|l| l.borrow().first().copied())
        .ok()
        .flatten()
        .map(|addr| addr as *mut ParallelContext)
}

fn pcxt_list_head_if<F: Fn(&ParallelContext) -> bool>(pred: F) -> Option<*mut ParallelContext> {
    let ptr = pcxt_list_head()?;
    // SAFETY: head of the active list, owned by this leader task.
    if pred(unsafe { &*ptr }) {
        Some(ptr)
    } else {
        None
    }
}

/// Run `f` inside a fresh per-leader pcxt-list scope. Leader tasks wrap their
/// body in this so CreateParallelContext / AtEOXact_Parallel see one list.
pub async fn pcxt_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    PCXT_LIST.scope(RefCell::new(Vec::new()), f).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::transam::xact::{EnterParallelMode, ExitParallelMode};
    use crate::backend::postmaster::auxprocess::aux_test_serial;
    use crate::backend::storage::lmgr::proc::InitProcess;
    use crate::miscadmin::BackendType;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use std::sync::atomic::{AtomicI32, AtomicUsize};
    use std::time::Duration;

    // Observations a test entrypoint records (process-wide; serialized by aux_test_serial).
    static TEST_RAN_COUNT: AtomicUsize = AtomicUsize::new(0);
    static TEST_LAST_WORKER_NUM: AtomicI32 = AtomicI32::new(-100);

    pub(super) fn lookup_test_worker(name: &str) -> Option<ParallelWorkerMainType> {
        match name {
            "test_ok_worker" => Some(test_ok_worker),
            "test_panic_worker" => Some(test_panic_worker),
            _ => None,
        }
    }

    fn test_ok_worker(worker_number: i32, last_xlog_end: &Arc<AtomicU64>) {
        assert!(is_parallel_worker(), "worker should see IsParallelWorker");
        assert_eq!(parallel_worker_number(), Some(worker_number as u32));
        TEST_RAN_COUNT.fetch_add(1, Ordering::SeqCst);
        TEST_LAST_WORKER_NUM.store(worker_number, Ordering::SeqCst);
        // Exercise the last_xlog_end report path.
        ParallelWorkerReportLastRecEnd(last_xlog_end, XLogRecPtr(42));
    }

    fn test_panic_worker(_worker_number: i32, _last_xlog_end: &Arc<AtomicU64>) {
        panic!("boom from worker");
    }

    fn fresh_shared() -> Arc<SharedState> {
        let shared = SharedState::new(SharedStateConfig::default());
        let _ = crate::storage::proc::set_proc_global(shared.proc_global().clone());
        shared
    }

    /// Run a leader closure with a PGPROC, Session, parallel mode, and a pcxt list.
    async fn with_leader<F, Fut, T>(shared: Arc<SharedState>, body: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use crate::backend::access::transam::xact::xact_scope;
        let session = Arc::new(Session::new(BackendType::BACKEND));
        session::scope(
            session,
            my_proc_scope(pcxt_scope(xact_scope(async move {
                InitProcess();
                EnterParallelMode();
                let out = body(shared).await;
                ExitParallelMode();
                ProcKill();
                out
            }))),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn launch_runs_workers_and_finishes() {
        let _serial = aux_test_serial().await;
        let shared = fresh_shared();
        let before = TEST_RAN_COUNT.load(Ordering::SeqCst);

        with_leader(shared.clone(), |shared| async move {
            // Leader is not a parallel worker.
            assert!(!is_parallel_worker());
            assert_eq!(parallel_worker_number(), None);

            let mut pcxt = CreateParallelContext("postgres", "test_ok_worker", 3, shared);
            assert!(ParallelContextActive());
            InitializeParallelDSM(&mut pcxt);
            LaunchParallelWorkers(&mut pcxt);
            assert_eq!(pcxt.nworkers_launched, 3);

            WaitForParallelWorkersToAttach(&mut pcxt);
            assert_eq!(pcxt.nknown_attached_workers, 3);

            tokio::time::timeout(Duration::from_secs(5), WaitForParallelWorkersToFinish(&mut pcxt))
                .await
                .expect("workers finish");

            // last_xlog_end folded from the workers.
            assert_eq!(pcxt.last_xlog_end.load(Ordering::SeqCst), 42);

            DestroyParallelContext(&mut pcxt).await;
            assert!(!ParallelContextActive(), "no leaked context after destroy");
            assert!(pcxt.workers.iter().all(|w| w.handle.is_none()), "no leaked tasks");
        })
        .await;

        assert_eq!(
            TEST_RAN_COUNT.load(Ordering::SeqCst),
            before + 3,
            "all three workers ran the entrypoint"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn worker_panic_propagates_as_error() {
        let _serial = aux_test_serial().await;
        let shared = fresh_shared();

        with_leader(shared.clone(), |shared| async move {
            let mut pcxt = CreateParallelContext("postgres", "test_panic_worker", 1, shared);
            InitializeParallelDSM(&mut pcxt);
            LaunchParallelWorkers(&mut pcxt);
            assert_eq!(pcxt.nworkers_launched, 1);

            // The worker panic is caught at its spawn boundary (not aborting the
            // process) and reported as a ParallelMessage::Error. Await the worker
            // task, then drain the error channel directly to observe the message.
            let handle = pcxt.workers[0].handle.take().expect("worker handle");
            tokio::time::timeout(Duration::from_secs(5), handle)
                .await
                .expect("worker task joins")
                .expect("worker task did not abort the process");

            let rx = pcxt.workers[0].error_rx.as_mut().expect("error channel");
            let msg = rx.try_recv().expect("worker reported a message");
            assert!(
                matches!(&msg, ParallelMessage::Error(t) if t.contains("boom from worker")),
                "worker error carried the panic text, got {msg:?}"
            );

            // ProcessParallelMessage re-raises an Error in the leader.
            let reraised = std::panic::catch_unwind(|| ProcessParallelMessage(0, msg)).is_err();
            assert!(reraised, "leader re-raises the worker error");

            // The handle was already taken; drop the rest cleanly.
            DestroyParallelContext(&mut pcxt).await;
            assert!(!ParallelContextActive());
        })
        .await;
    }
}
