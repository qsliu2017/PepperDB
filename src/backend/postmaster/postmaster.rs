//! Translated from PostgreSQL src/backend/postmaster/postmaster.c
//!
//! The supervisor. In PG the postmaster is a process that `fork`s a child per
//! connection and reaps them via SIGCHLD/waitpid, driving a state machine
//! (`PostmasterStateMachine`/`UpdatePMState`) for startup and shutdown. Under
//! the single-process async model the postmaster is a supervisor TASK:
//!
//! - `PostmasterMain` -> [`postmaster_main`]: build `SharedState`, bind the
//!   listener, run the accept loop, then drain.
//! - `ServerLoop` -> the `tokio::select!` accept loop inside
//!   [`postmaster_main`].
//! - `BackendStartup`/fork -> `tokio::spawn` of the backend task onto a
//!   `JoinSet`, wrapped in `catch_unwind`.
//! - SIGCHLD/waitpid reap -> draining finished tasks off the `JoinSet`.
//! - `PMChild` fixed slots -> a generational [`GenSlab`] child registry keyed by
//!   a [`ChildKey`].
//! - the shutdown state machine -> [`Shutdown`] + the drain at the end of
//!   [`postmaster_main`].
//!
//! DELETED (single-process redesign):
//! - `fork`/`exec`, `BackendStartup`, `postmaster_child_launch`, and all of
//!   `launch_backend.c` / `fork_process.c` (TOMBSTONED -- replaced by
//!   `tokio::spawn` + the `BackendType` match-dispatch below).
//! - `pmchild.c` fixed-slot pools (`AssignPostmasterChildSlot`, ...) -- folded
//!   into the generational [`ChildRegistry`] (TOMBSTONED).
//! - `EXEC_BACKEND` child-variable marshalling and the Windows paths.
//! - the postmaster-death watch pipe (`postmaster_alive_fds`) -- task lifetime
//!   replaces it; a dropped supervisor drops its children.
//! - `sig_atomic_t` signal handlers -- replaced by `tokio::signal` (production)
//!   and a programmatic [`Shutdown`] handle (tests).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::backend::postmaster::{
    autovacuum, bgwriter, checkpointer, pgarch, walwriter,
};
use crate::backend::tcop::backend_startup::backend_main;
use crate::miscadmin::BackendType;
use crate::shared_state::{SharedState, SharedStateConfig};
use crate::storage::procnumber::{GenSlab, Key};

/// Default listen port (PG's compiled-in `DEF_PGPORT`).
pub const DEFAULT_PG_PORT: u16 = 5432;

/// Maximum time to wait for backends to drain on shutdown before abandoning
/// them. PG's smart/fast shutdown waits indefinitely; we cap it for the demo.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// One active supervisor child (a spawned backend or, later, an aux task).
/// Replaces PG's `PMChild`. The `JoinHandle` lives in the `JoinSet`, not here;
/// this is the lifecycle/identity side of the registry.
pub struct ChildEntry {
    /// Child process flavor (PG `PMChild.bkend_type`).
    pub backend_type: BackendType,
    /// The peer address, for logging / diagnostics.
    pub peer: SocketAddr,
    /// Termination request handle. Firing it asks the backend to exit (the
    /// analogue of PG signalling SIGTERM to the child). Part B wires the
    /// backend's command loop to observe this alongside its proc-signal latch.
    pub cancel: Arc<Notify>,
}

/// Generational handle into the [`ChildRegistry`]; the per-task identity that
/// replaces PG's `MyPMChildSlot`. Carried into the backend task as `identity`.
pub type ChildKey = Key<ChildEntry>;

/// The supervisor-owned child registry. Replaces `pmchild.c`'s fixed slot pool
/// with a generational slab so a freed slot's stale key can never alias a new
/// occupant. Plain data behind the supervisor's own `Mutex` (per the rule that
/// each shared structure owns its locking); the lock is held only for short,
/// non-`.await` mutations.
#[derive(Default)]
pub struct ChildRegistry {
    inner: Mutex<GenSlab<ChildEntry>>,
}

impl ChildRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a child; returns its generational key.
    pub fn register(&self, entry: ChildEntry) -> ChildKey {
        self.inner.lock().insert(entry)
    }

    /// Remove a child on exit (stale key is a no-op).
    pub fn remove(&self, key: ChildKey) -> Option<ChildEntry> {
        self.inner.lock().remove(key)
    }

    /// Live child count (PG's `CountChildren`), for the admission check.
    pub fn count(&self) -> usize {
        self.inner.lock().len()
    }

    /// Collect every live child's cancel handle (PG's `SignalChildren` target
    /// list). Cloned out so the lock is not held while firing them.
    pub fn cancel_handles(&self) -> Vec<Arc<Notify>> {
        self.inner
            .lock()
            .iter()
            .map(|(_, e)| e.cancel.clone())
            .collect()
    }
}

/// Programmatic shutdown handle so the supervisor can be stopped without an OS
/// signal (used by tests; the production path also wires `tokio::signal` to fire
/// this). One `Notify` is enough -- shutdown mode collapses to a single graceful
/// drain for now (TODO: smart/fast/immediate distinction).
#[derive(Clone, Default)]
pub struct Shutdown {
    notify: Arc<Notify>,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }

    /// Request shutdown. Idempotent; wakes a current or future waiter.
    pub fn trigger(&self) {
        self.notify.notify_waiters();
        // Also store a permit so a `wait` that arrives after `trigger` still
        // returns (notify_waiters alone does not store a permit).
        self.notify.notify_one();
    }

    async fn wait(&self) {
        self.notify.notified().await;
    }
}

/// Map a [`BackendType`] to its async entry point. Replaces
/// `launch_backend.c`'s `child_process_kinds[]` dispatch table. A client
/// connection is always a regular backend; the aux tasks are spawned separately
/// by `launch_missing_background_tasks`, not via the connection-admission path.
fn backend_type_for_connection() -> BackendType {
    BackendType::BACKEND
}

/// Result/handle returned to a caller that wants to drive the supervisor
/// programmatically (tests). Production uses [`postmaster_main`] directly.
pub struct Supervisor {
    /// The bound address (useful when binding to port 0 for tests).
    pub local_addr: SocketAddr,
    /// Fire to request graceful shutdown.
    pub shutdown: Shutdown,
    /// The shared child registry (for assertions / introspection).
    pub registry: Arc<ChildRegistry>,
    /// The shared state (for assertions / introspection -- e.g. the proc-signal
    /// registry the backend registers its slot in; step 09 Part B tests).
    pub shared: Arc<SharedState>,
}

/// PG `PostmasterMain`. Build shared state, bind the listener on the default
/// port, and run to completion. Production entry: blocks until a shutdown signal
/// drains all backends. This is the thin wrapper the bin calls.
pub async fn postmaster_main(config: SharedStateConfig) {
    let shared = SharedState::new(config);
    let addr: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, DEFAULT_PG_PORT).into();
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("could not bind {addr}: {e}"));

    let shutdown = Shutdown::new();
    // Production: translate OS signals into the shutdown request.
    spawn_signal_shutdown(shutdown.clone());

    server_loop(listener, shared, shutdown, Arc::new(ChildRegistry::new())).await;
}

/// Bind on `bind_addr` (use port 0 for an ephemeral test port) and start the
/// supervisor on a background task, returning a [`Supervisor`] handle. The
/// returned `JoinHandle` resolves when the accept loop has stopped and all
/// backends have drained. Test-facing; production uses [`postmaster_main`].
pub async fn start_supervisor(
    bind_addr: SocketAddr,
    config: SharedStateConfig,
) -> (Supervisor, tokio::task::JoinHandle<()>) {
    let shared = SharedState::new(config);
    let listener = TcpListener::bind(bind_addr)
        .await
        .unwrap_or_else(|e| panic!("could not bind {bind_addr}: {e}"));
    let local_addr = listener.local_addr().expect("listener has a local addr");

    let shutdown = Shutdown::new();
    let registry = Arc::new(ChildRegistry::new());

    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        let registry = registry.clone();
        let shared = shared.clone();
        async move { server_loop(listener, shared, shutdown, registry).await }
    });

    (Supervisor { local_addr, shutdown, registry, shared }, handle)
}

/// PG `ServerLoop`. The accept loop plus the shutdown drain. Selects over:
///   (a) `listener.accept()`        -- new connection -> admit + spawn backend.
///   (b) `shutdown.wait()`          -- begin graceful drain (stop accepting).
///   (c) `join_set.join_next()`     -- a backend finished -> reap + deregister.
/// SIGHUP (config reload) is a TODO hook (see `spawn_signal_shutdown`).
async fn server_loop(
    listener: TcpListener,
    shared: Arc<SharedState>,
    shutdown: Shutdown,
    registry: Arc<ChildRegistry>,
) {
    let mut backends: JoinSet<ChildKey> = JoinSet::new();

    // Start the long-lived auxiliary tasks (PG launches them right after shared
    // memory is up) and install the on-demand spawner hooks.
    let mut aux = AuxTasks::new();
    launch_missing_background_tasks(&shared, &mut aux);

    loop {
        tokio::select! {
            // (a) accept a new connection.
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        admit_and_spawn(&mut backends, &shared, &registry, stream, peer);
                    }
                    Err(e) => {
                        // Transient accept error: log and keep serving (PG logs
                        // and continues; it does not tear the postmaster down).
                        crate::elog!(crate::utils::elog::LOG, format!("accept failed: {e}"));
                    }
                }
            }

            // (b) shutdown requested: stop accepting, break to the drain.
            () = shutdown.wait() => {
                crate::elog!(crate::utils::elog::LOG, "postmaster: shutdown requested".to_string());
                break;
            }

            // (c) a backend exited: reap it and remove from the registry.
            Some(joined) = backends.join_next() => {
                reap(&registry, joined);
            }

            // (d) an aux task exited unexpectedly (not during shutdown): respawn
            // it (PG's LaunchMissingBackgroundProcesses restart policy).
            Some(joined) = aux.join_set.join_next() => {
                respawn_aux(&mut aux, &shared, joined);
            }
        }
    }

    // --- shutdown state machine -------------------------------------------
    // PG's PostmasterStateMachine sequences the shutdown; see `drain` for the
    // exact order (backends -> shutdown checkpoint -> other aux -> checkpointer).
    drain(&mut backends, &registry, &mut aux).await;
}

/// An aux task ended during NORMAL operation (PG treats an aux exit outside
/// shutdown as a crash and relaunches it). Identify the role and respawn it.
fn respawn_aux(
    aux: &mut AuxTasks,
    shared: &Arc<SharedState>,
    joined: Result<AuxRole, tokio::task::JoinError>,
) {
    match joined {
        Ok(role) => {
            crate::elog!(
                crate::utils::elog::LOG,
                format!("aux task {role:?} exited unexpectedly; restarting")
            );
            aux.spawn_role(role, shared);
        }
        Err(e) => {
            // Aborted before yielding its role; cannot tell which one. The next
            // launch_missing pass on a future revision could reconcile; for now
            // log it (the abort path is only hit on runtime cancel).
            crate::elog!(crate::utils::elog::LOG, format!("aux task aborted: {e}"));
        }
    }
}

/// PG `BackendStartup` + admission (`canAcceptConnections`/`CountChildren`).
/// Enforce `MaxConnections`, register the child, and spawn its task wrapped in
/// `catch_unwind`. The `JoinSet` yields the child's `ChildKey` so the reaper can
/// deregister it.
fn admit_and_spawn(
    backends: &mut JoinSet<ChildKey>,
    shared: &Arc<SharedState>,
    registry: &Arc<ChildRegistry>,
    stream: TcpStream,
    peer: SocketAddr,
) {
    // Admission: refuse past MaxConnections (PG returns CAC_TOOMANY and the
    // dead-end backend just sends an error). We close the socket instead.
    let max = shared.config().max_connections.max(0) as usize;
    if registry.count() >= max {
        crate::elog!(
            crate::utils::elog::LOG,
            format!("connection refused (max_connections={max} reached): {peer}")
        );
        drop(stream);
        return;
    }

    let cancel = Arc::new(Notify::new());
    let entry = ChildEntry {
        backend_type: backend_type_for_connection(),
        peer,
        cancel: cancel.clone(),
    };
    let key = registry.register(entry);

    let shared = shared.clone();
    // type -> async-entry dispatch (replaces launch_backend's child_process_kinds[]).
    // A connection is always a client backend; the aux tasks (checkpointer,
    // bgwriter, walwriter, autovacuum, archiver) are spawned by
    // launch_missing_background_tasks, never through connection admission -- so any
    // other type here is unreachable.
    // `cancel` is the per-child termination Notify (PG's SIGTERM target); the
    // backend selects on it to set ProcDie (step 09 Part B).
    let entry_fut = match backend_type_for_connection() {
        BackendType::BACKEND => backend_main(stream, peer, shared, key, cancel),
        other => unreachable!("supervisor only spawns client backends, got {other:?}"),
    };

    // catch_unwind at the task boundary (design-000 error model): an
    // elog(ERROR)-as-panic inside the backend is contained here so the
    // supervisor never crashes. The critical-section abort path uses
    // process::abort and correctly bypasses this.
    backends.spawn(async move {
        use futures_util::FutureExt;
        let result = std::panic::AssertUnwindSafe(entry_fut).catch_unwind().await;
        if let Err(payload) = result {
            log_caught_panic(&*payload);
        }
        // Yield the key so the supervisor reaper can deregister this child,
        // whether it returned normally or panicked.
        key
    });
}

/// Log a panic payload caught at the task boundary. If it is an
/// `elog(ERROR)`-style `ErrorData`, log it as such; otherwise log a generic
/// panic. Either way the connection task ends without taking down the supervisor.
fn log_caught_panic(payload: &(dyn std::any::Any + Send)) {
    if let Some(edata) = payload.downcast_ref::<crate::utils::elog::ErrorData>() {
        let msg = edata.message.clone().unwrap_or_else(|| "(no message)".to_string());
        crate::elog!(crate::utils::elog::LOG, format!("backend terminated by error: {msg}"));
    } else if let Some(s) = payload.downcast_ref::<&str>() {
        crate::elog!(crate::utils::elog::LOG, format!("backend panicked: {s}"));
    } else if let Some(s) = payload.downcast_ref::<String>() {
        crate::elog!(crate::utils::elog::LOG, format!("backend panicked: {s}"));
    } else {
        crate::elog!(crate::utils::elog::LOG, "backend panicked (unknown payload)".to_string());
    }
}

/// Reap one finished backend: deregister it from the child registry. A JoinSet
/// error here is the runtime cancelling/aborting the task; the catch_unwind
/// inside the task means a panic is already turned into a normal `Ok(key)`
/// return, so `Err` only occurs on abort.
fn reap(registry: &ChildRegistry, joined: Result<ChildKey, tokio::task::JoinError>) {
    match joined {
        Ok(key) => {
            registry.remove(key);
        }
        Err(e) => {
            // Aborted (not a contained panic). We cannot recover the key, but
            // the entry will be cleaned up by the final drain; log it.
            crate::elog!(crate::utils::elog::LOG, format!("backend task aborted: {e}"));
        }
    }
}

/// PG's shutdown drain (the tail of `PostmasterStateMachine`). Encodes PG's
/// shutdown ORDER (fast-shutdown PM_WAIT_BACKENDS -> PM_WAIT_XLOG_SHUTDOWN ->
/// PM_WAIT_AUX -> checkpointer exit):
///
///   1. Stop accepting (the caller already broke the accept loop).
///   2. Signal regular BACKENDS to terminate and await them.
///   3. Tell the checkpointer to write the SHUTDOWN checkpoint (phase-1) and AWAIT
///      its PMSIGNAL_XLOG_IS_SHUTDOWN completion -- nothing else may stop until
///      WAL is shut down (PG PM_WAIT_XLOG_SHUTDOWN -> PM_WAIT_XLOG_ARCHIVAL).
///   4. Shut down the OTHER aux tasks (bgwriter / walwriter / pgarch / autovac
///      launcher) and the on-demand workers.
///   5. Tell the checkpointer to EXIT (phase-2): it is FIRST-started, LAST-stopped.
///   6. Await the aux JoinSet with the deadline; abandon stragglers past it.
async fn drain(backends: &mut JoinSet<ChildKey>, registry: &ChildRegistry, aux: &mut AuxTasks) {
    let deadline = tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT);
    tokio::pin!(deadline);

    // (2) Signal all backends to terminate (PG SignalChildren(SIGTERM)) and wait.
    for cancel in registry.cancel_handles() {
        cancel.notify_waiters();
    }
    drain_backends(backends, registry, &mut deadline).await;

    // (3) PM_WAIT_XLOG_SHUTDOWN: ask the checkpointer to write the shutdown
    // checkpoint (PG SIGINT) and WAIT until it has done so. PG's
    // PostmasterStateMachine only leaves PM_WAIT_XLOG_SHUTDOWN for
    // PM_WAIT_XLOG_ARCHIVAL once the checkpointer signals
    // PMSIGNAL_XLOG_IS_SHUTDOWN; nothing else may stop until WAL is shut down. The
    // phase-1 notify is sticky (notify_waiters + notify_one); we then await the
    // checkpointer's xlog-is-shutdown completion, bounded by the drain deadline so
    // a dead checkpointer cannot hang us (we log and proceed, like a straggler).
    aux.ckpt_phase1.notify_waiters();
    aux.ckpt_phase1.notify_one();
    tokio::select! {
        () = aux.ckpt_xlog_done.notified() => {}
        () = &mut deadline => {
            crate::elog!(
                crate::utils::elog::LOG,
                "shutdown drain timed out waiting for shutdown checkpoint; proceeding".to_string()
            );
        }
    }

    // (4) PM_WAIT_AUX: shut down the non-checkpointer aux tasks. Each per-role
    // notify wakes bgwriter/walwriter/pgarch/autovac-launcher exactly once.
    for notify in aux.role_shutdown.values() {
        notify.notify_waiters();
        notify.notify_one();
    }

    // (5) Tell the checkpointer to EXIT (PG SIGUSR2). Sticky so a checkpointer
    // that reaches phase-2 after this still sees it.
    aux.ckpt_phase2.notify_waiters();
    aux.ckpt_phase2.notify_one();

    // (6) Await every aux task with the (remaining) deadline.
    loop {
        tokio::select! {
            joined = aux.join_set.join_next() => {
                match joined {
                    Some(Ok(role)) => {
                        crate::elog!(crate::utils::elog::LOG, format!("aux task {role:?} exited"));
                    }
                    Some(Err(e)) => {
                        crate::elog!(crate::utils::elog::LOG, format!("aux task join error: {e}"));
                    }
                    None => break, // all aux drained
                }
            }
            () = &mut deadline => {
                crate::elog!(
                    crate::utils::elog::LOG,
                    format!("shutdown drain timed out; abandoning {} aux task(s)", aux.join_set.len())
                );
                aux.join_set.shutdown().await; // abort the stragglers
                break;
            }
        }
    }
}

/// Await the backend JoinSet until empty or the deadline fires (step 2 of the
/// drain). On timeout, abort the stragglers (PG escalates to immediate / SIGKILL).
async fn drain_backends(
    backends: &mut JoinSet<ChildKey>,
    registry: &ChildRegistry,
    deadline: &mut std::pin::Pin<&mut tokio::time::Sleep>,
) {
    loop {
        tokio::select! {
            joined = backends.join_next() => {
                match joined {
                    Some(j) => reap(registry, j),
                    None => break, // all backends drained
                }
            }
            () = deadline.as_mut() => {
                crate::elog!(
                    crate::utils::elog::LOG,
                    format!("shutdown drain timed out; abandoning {} backend(s)", backends.len())
                );
                backends.shutdown().await; // abort the stragglers
                return;
            }
        }
    }
}

/// Translate OS signals into a shutdown request (production path). SIGTERM /
/// SIGINT / SIGQUIT all trigger the graceful drain for now; SIGHUP (config
/// reload) is a TODO hook. Thin wrapper over `tokio::signal`.
#[cfg(unix)]
fn spawn_signal_shutdown(shutdown: Shutdown) {
    use tokio::signal::unix::{SignalKind, signal};
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        let mut sigquit = signal(SignalKind::quit()).expect("install SIGQUIT handler");
        let mut sighup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
        loop {
            tokio::select! {
                _ = sigterm.recv() => { shutdown.trigger(); break; }
                _ = sigint.recv()  => { shutdown.trigger(); break; }
                _ = sigquit.recv() => { shutdown.trigger(); break; }
                // SIGHUP: config reload. TODO: ProcessConfigFile(PGC_SIGHUP).
                _ = sighup.recv()  => { /* TODO(guc): reload config */ }
            }
        }
    });
}

#[cfg(not(unix))]
fn spawn_signal_shutdown(_shutdown: Shutdown) {
    // TODO: non-unix signal wiring; tests use the programmatic Shutdown handle.
}

/// The long-lived auxiliary roles the supervisor keeps running and restarts on
/// unexpected exit (PG's `LaunchMissingBackgroundProcesses` set). The startup
/// process is a boot-time one-shot and is NOT in this set; autovac/bgworkers are
/// spawned on demand via the spawner hooks, not restarted here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum AuxRole {
    Checkpointer,
    BgWriter,
    WalWriter,
    PgArch,
    AutoVacLauncher,
}

impl AuxRole {
    /// The restartable roles, in first-started order. The checkpointer is first
    /// (PG starts it first and stops it last; see the sequenced drain).
    const RESTARTABLE: [Self; 5] = [
        Self::Checkpointer,
        Self::BgWriter,
        Self::WalWriter,
        Self::PgArch,
        Self::AutoVacLauncher,
    ];
}

/// Supervisor-side control block for the auxiliary tasks. Holds a DEDICATED aux
/// `JoinSet` (separate from the backend `JoinSet`), a PER-ROLE shutdown signal,
/// and the checkpointer's two-phase shutdown handles so the drain can sequence
/// write-shutdown-checkpoint then exit.
struct AuxTasks {
    join_set: JoinSet<AuxRole>,
    /// One shutdown `Notify` per non-checkpointer role (PG's SIGTERM to that aux
    /// child). Each task owns its own handle so a single drain wakes it exactly
    /// once -- a shared notify could be consumed by the wrong task's loop-top
    /// poll. The checkpointer is driven by its two-phase handles instead.
    role_shutdown: HashMap<AuxRole, Arc<Notify>>,
    /// Checkpointer phase-1 (PG SIGINT / ShutdownXLOGPending): write the shutdown
    /// checkpoint.
    ckpt_phase1: Arc<Notify>,
    /// Checkpointer phase-2 (PG SIGUSR2 / ShutdownRequestPending): exit.
    ckpt_phase2: Arc<Notify>,
    /// Checkpointer xlog-is-shutdown completion (PG
    /// PMSIGNAL_XLOG_IS_SHUTDOWN): fired by the checkpointer once the shutdown
    /// checkpoint is written. The drain AWAITS this between phase-1 and stopping
    /// the other aux tasks (PM_WAIT_XLOG_SHUTDOWN -> PM_WAIT_XLOG_ARCHIVAL).
    ckpt_xlog_done: Arc<Notify>,
}

impl AuxTasks {
    fn new() -> Self {
        Self {
            join_set: JoinSet::new(),
            role_shutdown: HashMap::new(),
            ckpt_phase1: Arc::new(Notify::new()),
            ckpt_phase2: Arc::new(Notify::new()),
            ckpt_xlog_done: Arc::new(Notify::new()),
        }
    }

    /// The shutdown handle for a non-checkpointer role, created on first use so a
    /// restart reuses the same handle.
    fn role_shutdown(&mut self, role: AuxRole) -> Arc<Notify> {
        self.role_shutdown.entry(role).or_insert_with(|| Arc::new(Notify::new())).clone()
    }

    /// Spawn one aux role onto the aux JoinSet, wrapped in `catch_unwind` (the
    /// task-boundary error model) and yielding its `AuxRole` so the restart logic
    /// can identify which role exited. The checkpointer gets its two-phase
    /// handles; the others get their per-role shutdown notify.
    fn spawn_role(&mut self, role: AuxRole, shared: &Arc<SharedState>) {
        let shared = shared.clone();
        let shutdown = self.role_shutdown(role);
        let ckpt_phase1 = self.ckpt_phase1.clone();
        let ckpt_phase2 = self.ckpt_phase2.clone();
        let ckpt_xlog_done = self.ckpt_xlog_done.clone();
        self.join_set.spawn(async move {
            use futures_util::FutureExt;
            let fut = async move {
                match role {
                    AuxRole::Checkpointer => {
                        checkpointer::checkpointer_main_phased(
                            shared,
                            ckpt_phase1,
                            ckpt_phase2,
                            ckpt_xlog_done,
                        )
                        .await;
                    }
                    AuxRole::BgWriter => bgwriter::background_writer_main(shared, shutdown).await,
                    AuxRole::WalWriter => walwriter::wal_writer_main(shared, shutdown).await,
                    AuxRole::PgArch => pgarch::pgarch_main(shared, shutdown).await,
                    AuxRole::AutoVacLauncher => {
                        autovacuum::auto_vac_launcher_main(shared, shutdown).await;
                    }
                }
            };
            if let Err(payload) = std::panic::AssertUnwindSafe(fut).catch_unwind().await {
                log_caught_panic(&*payload);
            }
            role
        });
    }
}

/// PG `LaunchMissingBackgroundProcesses`. Spawn the long-lived auxiliary tasks
/// onto the aux JoinSet and install the on-demand spawner hooks (autovac worker,
/// bgworker) so the launcher->worker and register->worker paths reach the
/// supervisor. Idempotent re-launch happens in `server_loop` (restart policy).
fn launch_missing_background_tasks(shared: &Arc<SharedState>, aux: &mut AuxTasks) {
    for role in AuxRole::RESTARTABLE {
        // PG's postmaster only forks the autovacuum launcher when autovacuum is
        // enabled (the for-wraparound-emergency case aside, which is deferred). With
        // autovacuum off by default, the now-faithful get_database_list /
        // do_autovacuum bodies are not driven into the deferred catalog stubs on the
        // launcher's naptime timer.
        if role == AuxRole::AutoVacLauncher
            && !crate::backend::postmaster::autovacuum::auto_vacuuming_active()
        {
            continue;
        }
        aux.spawn_role(role, shared);
    }
    install_spawner_hooks(shared);
}

/// Install the autovac-worker and bgworker spawn hooks. The closures spawn the
/// on-demand worker tasks onto a detached task (they are short-lived and not part
/// of the restart set); each is wrapped in `catch_unwind` so a worker panic is
/// contained. First install wins (the hooks are process-global `OnceLock`s), so
/// re-launch after a restart is a no-op.
fn install_spawner_hooks(shared: &Arc<SharedState>) {
    let shared_av = shared.clone();
    autovacuum::set_autovac_worker_spawner(Box::new(move |dbid| {
        let shared = shared_av.clone();
        tokio::spawn(async move {
            use futures_util::FutureExt;
            let fut = autovacuum::auto_vac_worker_main(shared, dbid);
            if let Err(payload) = std::panic::AssertUnwindSafe(fut).catch_unwind().await {
                log_caught_panic(&*payload);
            }
        });
    }));

    // bgworkers observe their slot `terminate` flag, not an aux shutdown notify.
    crate::backend::postmaster::bgworker::set_bgworker_spawner(Box::new(move |handle| {
        tokio::spawn(async move {
            use futures_util::FutureExt;
            let fut = async move {
                crate::backend::postmaster::bgworker::run_background_worker(handle);
            };
            if let Err(payload) = std::panic::AssertUnwindSafe(fut).catch_unwind().await {
                log_caught_panic(&*payload);
            }
        });
    }));
}

// Compatibility note: the old header-stub `PostmasterMain(argc, argv) -> !` and
// the pmchild/launch_backend prototypes in `crate::postmaster::postmaster`
// remain as unimplemented `// TODO(panic)` shims; this module is the real
// supervisor. The bin (`src/main.rs`) calls `postmaster_main` here.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::postmaster::auxprocess::aux_test_serial;
    use crate::backend::tcop::backend_startup::test_hook;
    use crate::storage::proc::ProcGlobal;
    use crate::storage::procnumber::INVALID_PROC_NUMBER;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream as ClientStream;

    fn loopback_port0() -> SocketAddr {
        (std::net::Ipv4Addr::LOCALHOST, 0).into()
    }

    // `test_hook::PANIC_ON_CONNECT` / `CONNECTED` are process-global; tests that
    // read or write them must not run concurrently or one test's panic flag
    // bleeds into another's backend. Serialize via the shared hook mutex (also
    // used by the backend-startup test module).
    use test_hook::serial as hook_serial;

    // Every test that spawns the supervisor brings up the aux tasks, which claim
    // process-wide aux PGPROCs + advertise ProcGlobal.<role>_proc. They MUST hold
    // `aux_test_serial` (shared with checkpointer/walwriter/autovacuum tests) so
    // those shared slots never interleave. When both guards are needed, take
    // `aux_test_serial` FIRST (consistent order avoids deadlock).

    async fn wait_until<F: Fn() -> bool>(pred: F, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if pred() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        pred()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accepts_and_spawns_backend() {
        let _aux = aux_test_serial().await;
        let _hook = hook_serial().await;
        test_hook::PANIC_ON_CONNECT.store(false, Ordering::SeqCst);
        let before = test_hook::CONNECTED.load(Ordering::SeqCst);

        let (sup, handle) =
            start_supervisor(loopback_port0(), SharedStateConfig::default()).await;

        let mut client = ClientStream::connect(sup.local_addr).await.expect("connect");
        client.write_all(b"hello").await.expect("write");

        // The placeholder backend ran (connection observed).
        assert!(
            wait_until(
                || test_hook::CONNECTED.load(Ordering::SeqCst) > before,
                Duration::from_secs(2)
            )
            .await,
            "backend placeholder should have observed the connection"
        );

        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor should drain and return")
            .expect("supervisor task panicked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_and_returns() {
        let _aux = aux_test_serial().await;
        let (sup, handle) =
            start_supervisor(loopback_port0(), SharedStateConfig::default()).await;

        // No connections; shutdown should drain an empty JoinSet and return.
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor should return promptly on empty drain")
            .expect("supervisor task panicked");

        assert_eq!(sup.registry.count(), 0, "registry should be empty after drain");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_tracks_and_clears_a_backend() {
        let _aux = aux_test_serial().await;
        let _hook = hook_serial().await;
        // The backend blocks reading the startup-packet length prefix; if we
        // never send and never close, it stays parked there, so the registry
        // holds it. Closing the client makes the read return EOF and the task
        // ends, so the reaper deregisters it.
        let (sup, handle) =
            start_supervisor(loopback_port0(), SharedStateConfig::default()).await;

        let client = ClientStream::connect(sup.local_addr).await.expect("connect");

        assert!(
            wait_until(|| sup.registry.count() == 1, Duration::from_secs(2)).await,
            "registry should hold the live backend"
        );

        // Closing the client makes the placeholder's read return 0 -> task ends
        // -> reaper deregisters it.
        drop(client);
        assert!(
            wait_until(|| sup.registry.count() == 0, Duration::from_secs(2)).await,
            "registry should clear after the backend exits"
        );

        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor should drain and return")
            .expect("supervisor task panicked");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catch_unwind_contains_backend_panic() {
        let _aux = aux_test_serial().await;
        let _hook = hook_serial().await;
        // A panicking backend must NOT take down the supervisor runtime.
        test_hook::PANIC_ON_CONNECT.store(true, Ordering::SeqCst);

        let (sup, handle) =
            start_supervisor(loopback_port0(), SharedStateConfig::default()).await;

        // First connection: the placeholder panics; catch_unwind contains it.
        {
            let mut c = ClientStream::connect(sup.local_addr).await.expect("connect");
            let _ = c.write_all(b"x").await;
        }

        // The supervisor is still alive: a second connection is still accepted
        // and its (panicking) backend is again contained -- count returns to 0.
        assert!(
            wait_until(|| sup.registry.count() == 0, Duration::from_secs(2)).await,
            "panicking backends should be reaped, supervisor still serving"
        );
        {
            let mut c = ClientStream::connect(sup.local_addr).await.expect("connect");
            let _ = c.write_all(b"y").await;
        }
        assert!(
            wait_until(|| sup.registry.count() == 0, Duration::from_secs(2)).await,
            "supervisor should keep reaping after a contained panic"
        );

        test_hook::PANIC_ON_CONNECT.store(false, Ordering::SeqCst);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor should drain and return")
            .expect("supervisor task panicked");
    }

    #[test]
    fn admission_check_uses_registry_count_vs_max() {
        // Unit-test the admission predicate directly: it refuses once the live
        // child count reaches max_connections. (The networked path is covered by
        // the accept/registry tests; this isolates the cap arithmetic.)
        let registry = ChildRegistry::new();
        let max = 1usize;
        assert!(registry.count() < max, "empty registry admits");

        let cancel = Arc::new(Notify::new());
        let key = registry.register(ChildEntry {
            backend_type: BackendType::BACKEND,
            peer: (std::net::Ipv4Addr::LOCALHOST, 1).into(),
            cancel,
        });
        assert!(registry.count() >= max, "at cap, admission must refuse");

        registry.remove(key);
        assert!(registry.count() < max, "after exit, admits again");
    }

    // --- step 17f: supervisor aux integration --------------------------------
    use std::sync::atomic::Ordering as AtomicOrdering;

    /// start_supervisor brings the aux tasks up: the checkpointer, walwriter, and
    /// autovac launcher each advertise their ProcGlobal proc number. Triggering
    /// shutdown drains every aux task and clears those advertisements.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supervisor_starts_and_drains_aux_tasks() {
        let _serial = aux_test_serial().await;
        // Autovacuum defaults OFF, so the supervisor does NOT start the launcher
        // (its now-faithful get_database_list / do_autovacuum bodies would drive the
        // deferred pg_database/pg_class catalog stubs on the launcher's naptime
        // timer). We therefore assert only the always-on roles that advertise a
        // ProcGlobal slot: the checkpointer and the walwriter.
        let (sup, handle) =
            start_supervisor(loopback_port0(), SharedStateConfig::default()).await;
        let g = ProcGlobal::expect().clone();

        // The always-on roles that advertise a ProcGlobal slot come up.
        assert!(
            wait_until(
                || g.checkpointer_proc.load(AtomicOrdering::Acquire) != INVALID_PROC_NUMBER
                    && g.walwriter_proc.load(AtomicOrdering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(3)
            )
            .await,
            "checkpointer/walwriter should advertise their procs"
        );

        // Shutdown drains the aux JoinSet within the timeout and the supervisor
        // task returns.
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(8), handle)
            .await
            .expect("supervisor should drain aux tasks and return")
            .expect("supervisor task panicked");

        // Every advertised aux proc is cleared on exit.
        assert!(
            wait_until(
                || g.checkpointer_proc.load(AtomicOrdering::Acquire) == INVALID_PROC_NUMBER
                    && g.walwriter_proc.load(AtomicOrdering::Acquire) == INVALID_PROC_NUMBER,
                Duration::from_secs(2)
            )
            .await,
            "aux proc advertisements should be cleared after drain"
        );
    }

    /// The two-phase checkpointer shutdown writes the SHUTDOWN checkpoint (phase
    /// 1) BEFORE it exits (phase 2). Drives `checkpointer_main_phased` directly
    /// and asserts the ordering via the test flag the post-loop sets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn checkpointer_two_phase_writes_shutdown_ckpt_before_exit() {
        use crate::backend::postmaster::checkpointer;
        let _serial = aux_test_serial().await;

        let shared = SharedState::new(SharedStateConfig::default());
        let _ = crate::storage::proc::ProcGlobal::set(shared.proc_global().clone());
        let g = ProcGlobal::expect().clone();

        checkpointer::tests::SHUTDOWN_CKPT_WRITTEN.store(false, AtomicOrdering::Release);
        let phase1 = Arc::new(Notify::new());
        let phase2 = Arc::new(Notify::new());
        let xlog_shutdown_done = Arc::new(Notify::new());

        let task = tokio::spawn(checkpointer::checkpointer_main_phased(
            shared.clone(),
            phase1.clone(),
            phase2.clone(),
            xlog_shutdown_done.clone(),
        ));

        assert!(
            wait_until(
                || g.checkpointer_proc.load(AtomicOrdering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(2)
            )
            .await,
            "checkpointer should advertise its proc"
        );

        // Phase 1: write the shutdown checkpoint. The task must NOT exit yet -- it
        // parks on phase 2 -- but it must record that the checkpoint was written.
        phase1.notify_waiters();
        phase1.notify_one();
        assert!(
            wait_until(
                || checkpointer::tests::SHUTDOWN_CKPT_WRITTEN.load(AtomicOrdering::Acquire),
                Duration::from_secs(2)
            )
            .await,
            "phase 1 should write the shutdown checkpoint"
        );
        // The xlog-is-shutdown completion fires AFTER the checkpoint is written and
        // BEFORE phase-2 (we have not fired phase-2 yet), so it must be observable
        // now -- this is what the drain awaits before stopping the other aux tasks.
        tokio::time::timeout(Duration::from_secs(2), xlog_shutdown_done.notified())
            .await
            .expect("xlog-is-shutdown completion should fire after the shutdown checkpoint");
        assert!(!task.is_finished(), "checkpointer must wait for phase 2 to exit");

        // Phase 2: now the task may exit.
        phase2.notify_waiters();
        phase2.notify_one();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("checkpointer should exit after phase 2")
            .expect("checkpointer task panicked");
        assert_eq!(
            g.checkpointer_proc.load(AtomicOrdering::Acquire),
            INVALID_PROC_NUMBER,
            "checkpointer proc cleared on exit"
        );
    }

    /// An aux task that exits during NORMAL operation is respawned (PG's
    /// LaunchMissingBackgroundProcesses restart policy). Drive `respawn_aux`
    /// directly: a synthetic "exited" result for a role re-populates the JoinSet.
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn unexpected_aux_exit_is_respawned() {
        let _serial = aux_test_serial().await;
        let shared = SharedState::new(SharedStateConfig::default());
        let _ = crate::storage::proc::ProcGlobal::set(shared.proc_global().clone());

        let mut aux = AuxTasks::new();
        // Simulate the bgwriter having just exited unexpectedly.
        respawn_aux(&mut aux, &shared, Ok(AuxRole::BgWriter));
        assert_eq!(aux.join_set.len(), 1, "respawn should put a task back on the set");

        // Drain it so the test leaves no live task behind.
        for notify in aux.role_shutdown.values() {
            notify.notify_waiters();
            notify.notify_one();
        }
        let _ = tokio::time::timeout(Duration::from_secs(3), aux.join_set.join_next()).await;
    }
}
