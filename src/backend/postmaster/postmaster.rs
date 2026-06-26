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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinSet;

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
        self.inner.lock().unwrap().insert(entry)
    }

    /// Remove a child on exit (stale key is a no-op).
    pub fn remove(&self, key: ChildKey) -> Option<ChildEntry> {
        self.inner.lock().unwrap().remove(key)
    }

    /// Live child count (PG's `CountChildren`), for the admission check.
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Collect every live child's cancel handle (PG's `SignalChildren` target
    /// list). Cloned out so the lock is not held while firing them.
    pub fn cancel_handles(&self) -> Vec<Arc<Notify>> {
        self.inner
            .lock()
            .unwrap()
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
/// `launch_backend.c`'s `child_process_kinds[]` dispatch table. For Part A only
/// the regular backend is real; aux types are TODO(step17) and currently
/// rejected at admission (the supervisor only accepts client connections).
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
        }
    }

    // --- shutdown state machine (simplified) -------------------------------
    // PG's PostmasterStateMachine sequences smart/fast/immediate; we collapse to
    // one graceful drain. TODO: distinguish modes.
    drain(&mut backends, &registry, &shutdown).await;
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
    // Only the regular backend is real in Part A; aux arms are TODO(step17).
    // `cancel` is the per-child termination Notify (PG's SIGTERM target); the
    // backend selects on it to set ProcDie (step 09 Part B).
    let entry_fut = match backend_type_for_connection() {
        BackendType::BACKEND => backend_main(stream, peer, shared, key, cancel),
        // TODO(step17): Checkpointer/BgWriter/WalWriter/Autovacuum/Archiver aux
        // entries are not spawned here yet (see launch_missing_background_tasks).
        other => unreachable!("supervisor only spawns client backends in Part A, got {other:?}"),
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

/// PG's shutdown drain (the tail of `PostmasterStateMachine`). Stop accepting
/// (the caller already broke the loop), signal every live child to terminate,
/// then await the `JoinSet` with a deadline. After the deadline, abandon the
/// stragglers (PG escalates to immediate shutdown / SIGKILL).
async fn drain(backends: &mut JoinSet<ChildKey>, registry: &ChildRegistry, _shutdown: &Shutdown) {
    // Signal all children to terminate (PG's SignalChildren(SIGTERM)). Part B's
    // backend loop selects on this Notify; the placeholder ignores it and exits
    // on its own.
    for cancel in registry.cancel_handles() {
        cancel.notify_waiters();
    }

    let deadline = tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            joined = backends.join_next() => {
                match joined {
                    Some(j) => reap(registry, j),
                    None => break, // all drained
                }
            }
            () = &mut deadline => {
                crate::elog!(
                    crate::utils::elog::LOG,
                    format!("shutdown drain timed out; abandoning {} backend(s)", backends.len())
                );
                backends.shutdown().await; // abort the stragglers
                break;
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

/// PG `LaunchMissingBackgroundProcesses`. Aux tasks (checkpointer, bgwriter, WAL
/// writer, autovacuum, archiver) are spawned here in step 17. Currently a no-op
/// hook so the call site exists for Part B / step 17 to fill.
#[allow(dead_code)]
fn launch_missing_background_tasks(_shared: &Arc<SharedState>) {
    // TODO(step17): spawn the long-lived auxiliary tasks as their own JoinSet
    // children, restarting them on exit per PG's policy.
}

// Compatibility note: the old header-stub `PostmasterMain(argc, argv) -> !` and
// the pmchild/launch_backend prototypes in `crate::postmaster::postmaster`
// remain as unimplemented `// TODO(panic)` shims; this module is the real
// supervisor. The bin (`src/main.rs`) calls `postmaster_main` here.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::tcop::backend_startup::test_hook;
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
}
