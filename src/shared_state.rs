//! `SharedState`: the typed `Arc`-field container that replaces PostgreSQL's
//! shared-memory segment (no C counterpart).
//!
//! Under the single-process async model the shared segment is gone: state that
//! PostgreSQL kept there lives on the heap and is shared by cloning an
//! `Arc<SharedState>` into each task. Every subsystem owns its own locking, so
//! sharing is just `Arc` reference counting -- no segment sizing, no
//! `ShmemInitStruct`, no pointer fixups.
//!
//! `SharedState::new` constructs the subsystems in the SAME order as ipci.c's
//! `CreateOrAttachShmemStructs` (see `backend::storage::ipc::ipci`). That order
//! encodes real initialization dependencies, so later steps must insert their
//! field at the position marked by the matching `TODO(stepNN)` placeholder
//! rather than appending at the end.

use std::sync::Arc;

use crate::backend::storage::file::fd::FdManager;
use crate::backend::storage::ipc::procsignal::ProcSignal;
use crate::backend::utils::init::globals::ProcessConfig;
use crate::storage::io_backend::{self, IoBackend};

/// Construction parameters for [`SharedState`]. Sizing knobs for the I/O leaf.
pub struct SharedStateConfig {
    /// Hard kernel-fd budget: the max simultaneously-open OS fds (IoBackend
    /// semaphore). The hard enforcer.
    pub fd_budget: usize,
    /// Soft cap on simultaneously-open vfds, driving proactive LRU closing in
    /// `FdManager`. Must be `<= fd_budget`: the LRU layer trims before the hard
    /// budget blocks, so a soft cap above the budget would let opens stall on
    /// the semaphore instead of LRU-closing first (see step-05 fd.rs).
    pub max_open_files: usize,
    /// Shared buffer count (PG `NBuffers`). Small test-friendly default; the real
    /// size comes from the GUC at startup. TODO(guc): drive from ProcessConfig.
    pub nbuffers: usize,
    /// Data directory (PG `DataDir`). Applied to `ProcessConfig` BEFORE the
    /// clog/subtrans SLRUs are built so they resolve their segment dirs against
    /// it. `None` leaves the compiled-in default (relative paths). Production
    /// sets this from the startup `-D`/PGDATA; tests pass a tempdir.
    pub data_dir: Option<String>,
}

impl Default for SharedStateConfig {
    fn default() -> Self {
        Self {
            fd_budget: io_backend::DEFAULT_FD_BUDGET,
            // Leave headroom below the hard budget so transient/durable-op opens
            // and parent-dir fsyncs do not contend with the managed vfd pool.
            max_open_files: io_backend::DEFAULT_FD_BUDGET / 2,
            // Small default keeps SharedState construction cheap; production
            // sizing is a GUC concern (TODO(guc)).
            nbuffers: 1024,
            data_dir: None,
        }
    }
}

/// Declare `SharedState`'s `Arc` fields once and derive, for each:
///   - a PRIVATE `name: Arc<Type>` struct field,
///   - a `pub(crate) fn name(&self) -> &Arc<Type>` accessor,
///   - a `Send + Sync + 'static` compile-time assertion on `Type`, so a field
///     whose subsystem is not thread-shareable fails HERE with a clear bound
///     error instead of later at a `tokio::spawn` site.
///
/// Per-field doc comments apply to both the field and its accessor.
/// (No per-field visibility override: every accessor is `pub(crate)`, matching
/// the existing API; an `@ vis` token was deemed unnecessary complexity.)
macro_rules! shared_state {
    ( $( $(#[$meta:meta])* $name:ident : $ty:ty ),+ $(,)? ) => {
        /// The shared heap state, replacing the shared-memory segment. Clone the
        /// `Arc<SharedState>` into a task to share the SAME subsystem instances;
        /// each field is itself `Arc` so reaching through any clone hits the same
        /// subsystem.
        ///
        /// Fields are added by later steps at the position dictated by ipci.c
        /// order (see `SharedState::new`).
        pub struct SharedState {
            $( $(#[$meta])* $name: Arc<$ty>, )+
        }

        impl SharedState {
            $(
                $(#[$meta])*
                pub(crate) fn $name(&self) -> &Arc<$ty> {
                    &self.$name
                }
            )+
        }

        // Per-field guard: each subsystem must be `Send + Sync + 'static` to be
        // shared across tasks via `Arc<SharedState>`. Fails at the struct, not
        // at a downstream spawn.
        const _: () = {
            fn _assert_shared<T: Send + Sync + 'static>() {}
            fn _assert_all() {
                $( let _ = _assert_shared::<$ty>; )+
            }
        };
    };
}

shared_state! {
    /// Process-wide startup config (PG globals.c config half: DataDir, sizing
    /// GUCs). No ipci.c line -- it is process config, not a shmem struct.
    config: ProcessConfig,

    /// VFD pool over the async I/O leaf. Holds the `IoBackend` internally;
    /// reach the raw leaf via [`SharedState::io`] for WAL/smgr append paths.
    fd: FdManager,

    /// Inter-task signaling registry (PG `ProcSignal`).
    proc_signal: ProcSignal,

    /// WAL write pipeline state (PG `XLogCtl`; ipci.c `XLOGShmemInit` slot). The
    /// buffer ring, insert reservation, write/flush LSNs, and the flushed-LSN
    /// watch.
    xlog: crate::backend::access::transam::xlog::XLogCtl,

    /// Pending-fsync / pending-unlink queue (PG sync.c `pendingOps`). Storage
    /// tasks (smgr/md) enqueue fsync/unlink requests; the checkpointer drains it
    /// (step 17). Single-process: one shared structure instead of the
    /// per-checkpointer table + cross-process forward queue.
    sync_requests: crate::storage::sync::SyncRequests,

    /// Shared buffer pool (PG `BufferManagerShmemInit`): the page array,
    /// descriptors, the tag->buffer map, and the clock-sweep strategy. Replaces
    /// the C shmem buffer cache.
    buffers: crate::backend::storage::buffer::buf_init::BufferPool,

    /// OID/XID generation state (PG `TransamVariables`; ipci.c `VarsupShmemInit`
    /// slot). One `Mutex` over the whole struct (low contention).
    variable_cache: crate::backend::access::transam::transam::VariableCache,

    /// Commit-log SLRU (PG clog.c `XactCtl`; ipci.c `CLOGShmemInit` slot).
    clog: crate::backend::access::transam::slru::SlruCtl,

    /// Subtransaction-parent SLRU (PG subtrans.c `SubTransCtl`; ipci.c
    /// `SUBTRANSShmemInit` slot).
    subtrans: crate::backend::access::transam::slru::SlruCtl,

    /// Heavyweight lock manager (PG lock.c `LockMethodLockHash`/
    /// `LockMethodProcLockHash`; ipci.c `LockManagerShmemInit` slot, before the
    /// proc table). The sharded LOCK/PROCLOCK tables + the fast-path strong-lock
    /// counts.
    lock_manager: crate::backend::storage::lmgr::lock::LockManager,

    /// PGPROC arena (PG proc.c `PROC_HDR`/ProcGlobal; ipci.c `InitProcGlobal`,
    /// called from CreateSharedMemoryAndSemaphores after the lock tables). The
    /// fixed, process-lifetime arena indexed by ProcNumber + the dense MVCC mirror
    /// arrays the procarray scans.
    proc_global: crate::storage::proc::ProcGlobal,

    /// Process array (PG procarray.c `ProcArrayStruct`; ipci.c
    /// `ProcArrayShmemInit` slot). The snapshot/horizon source; `ProcArrayLock`
    /// is its internal `RwLock`.
    proc_array: crate::backend::storage::ipc::procarray::ProcArray,

    /// Shared cache-invalidation transport (PG sinvaladt.c `SISeg`; ipci.c
    /// `SharedInvalShmemInit` slot). The SI ring buffer + per-backend read state.
    sinval: crate::backend::storage::ipc::sinvaladt::SInvalBuffer,

    /// Checkpointer<->backend shared state (PG checkpointer.c
    /// `CheckpointerShmemStruct`; ipci.c `CheckpointerShmemInit` slot). The
    /// checkpoint-request counters + the start/done condition variables. The
    /// cross-process fsync forwarding is gone (single-process drains the shared
    /// `sync_requests` queue directly).
    checkpointer: crate::backend::postmaster::checkpointer::CheckpointerShmem,

    /// Autovacuum launcher<->worker shared state (PG autovacuum.c
    /// `AutoVacuumShmemStruct`; ipci.c `AutoVacuumShmemInit` slot). The worker
    /// freelist / running list / starting-worker pointer + the WorkerInfo array +
    /// the work-item array, all under one Mutex (PG `AutovacuumLock`).
    autovacuum: crate::backend::postmaster::autovacuum::AutoVacuumShmem,

    /// Background-worker slot table (PG bgworker.c `BackgroundWorkerData`; ipci.c
    /// `BackgroundWorkerShmemInit` slot, between TwoPhase and SharedInval). The
    /// fixed slot array + the parallel register/terminate counters, all under one
    /// Mutex (PG `BackgroundWorkerLock`). Published process-wide so dynamic
    /// registration / handle polling reach one struct without a SharedState handle.
    bgworker: crate::backend::postmaster::bgworker::BackgroundWorkerShmem,

    /// In-memory rewrite-rule action store (no C shmem analog; stands in for the
    /// `pg_rewrite.ev_action` pg_node_tree until nodeToString/stringToNode land).
    /// Keyed by relation OID; the relcache reads it to build `rd_rules`.
    rule_registry: crate::backend::rewrite::rule_registry::RuleRegistry,
}

/// Default `PROCARRAY_MAXPROCS` (MaxBackends + max_prepared_xacts) when the
/// startup-computed `max_backends` is not yet set. Test-friendly; production
/// sizing comes from the GUC. TODO(guc): drive from ProcessConfig.max_backends.
const DEFAULT_PROCARRAY_MAXPROCS: usize = 128;

impl SharedState {
    /// Build the shared state once, then `Arc::clone` it into tasks.
    ///
    /// The body mirrors ipci.c `CreateOrAttachShmemStructs` ordering. Each
    /// `TODO(stepNN)` marks where that subsystem's `*ShmemInit` sits in the C
    /// roster; insert the corresponding `Arc` field construction at that exact
    /// point so init dependencies stay faithful. `deferred` = no step assigned
    /// yet (multixact, twophase, aio, and the replication/stats/sync-scan tail).
    #[allow(clippy::needless_pass_by_value, reason = "callers in src/backend/ pass by value; changing to &ref would ripple to all callers")]
    pub fn new(config: SharedStateConfig) -> Arc<Self> {
        // max_open must stay <= the fd budget: the VFD LRU frees a permit by
        // closing an idle fd before acquiring, so a larger soft cap can wedge on
        // the budget semaphore (see step-05 fd.rs).
        debug_assert!(config.max_open_files <= config.fd_budget);
        // --- fd / I/O leaf (no ipci.c line) -----------------------------------
        // The fd budget is process I/O infrastructure, not a shmem struct, so it
        // has no `*ShmemInit` call in ipci.c. It must exist before any storage
        // subsystem (smgr, bufmgr, xlog) since they all do file I/O through it,
        // so it is constructed first.
        let io = IoBackend::new(config.fd_budget);
        let fd = FdManager::new(io, config.max_open_files);

        // Process-wide startup config (DataDir, sizing GUCs). Not a shmem
        // struct; constructed with compiled-in defaults, populated from GUC at
        // startup (TODO(guc)). DataDir is settable early via `config.set_data_dir`.
        let process_config = Arc::new(ProcessConfig::new());
        // DataDir must be set before the clog/subtrans SLRUs below capture it.
        if let Some(d) = &config.data_dir {
            process_config.set_data_dir(d.clone());
        }
        // Publish for the deprecated miscadmin `DataDir` shims (one per process).
        crate::backend::utils::init::globals::set_process_config(process_config.clone());

        // --- CreateOrAttachShmemStructs roster (ipci.c order) -----------------
        // CreateLWLocks / InitShmemIndex / dsm_shmem_init / DSMRegistryShmemInit:
        //   tombstoned -- no LWLock arena, no shmem index, no DSM (Arc-shared).

        // Set up xlog, clog, and buffers:
        // VarsupShmemInit -- step14. The OID/XID generation state; one Mutex over
        // the ex-TransamVariables struct.
        let variable_cache =
            Arc::new(crate::backend::access::transam::transam::VariableCache::new());
        // Publish process-wide so proc.c (ProcKill) can reach it like PG's shmem
        // TransamVariables (first SharedState wins; tests build their own).
        crate::backend::access::transam::transam::VariableCache::set(variable_cache.clone());
        // XLOGShmemInit -- step13 (part A), DONE. The WAL buffer ring, insert
        // reservation, write/flush LSNs, and the flushed-LSN watch. Bound to the
        // I/O leaf and process config (both built above) for segment I/O.
        let xlog = crate::backend::access::transam::xlog::XLogCtl::new(
            fd.io().clone(),
            process_config.clone(),
        );
        //   (XLogPrefetchShmemInit -- deferred: prefetch is an aio concern)
        //   (XLogRecoveryShmemInit -- step13/recovery)
        // The sync request queue (sync.c InitSync) is constructed early here so
        // the clog/subtrans SLRUs can hold a handle to it (their physical writes
        // enqueue fsync requests). It is logically the checkpointer-adjacent
        // structure; the checkpointer task drains it (ProcessSyncRequests).
        let sync_requests = Arc::new(crate::storage::sync::SyncRequests::new());
        // CLOGShmemInit -- step14. The commit-log SLRU over pg_xact. Holds I/O
        // handles directly (not Arc<SharedState>) to avoid a reference cycle.
        let clog = crate::backend::access::transam::clog::clog_shmem_init_handles(
            config.nbuffers.max(1),
            fd.clone(),
            xlog.clone(),
            sync_requests.clone(),
            process_config.data_dir(),
        );
        //   (CommitTsShmemInit -- deferred)
        // SUBTRANSShmemInit -- step14. The subtransaction-parent SLRU over
        // pg_subtrans.
        let subtrans = crate::backend::access::transam::subtrans::subtrans_shmem_init_handles(
            config.nbuffers.max(1),
            fd.clone(),
            xlog.clone(),
            sync_requests.clone(),
            process_config.data_dir(),
        );
        //   (MultiXactShmemInit -- deferred)
        // BufferManagerShmemInit -- step12 (part A), DONE. The page pool is
        // sized from the NBuffers GUC carried on ProcessConfig.
        let buffers = Arc::new(crate::backend::storage::buffer::buf_init::BufferPool::new(
            config.nbuffers.max(1),
        ));

        // Set up lock manager:
        // LockManagerShmemInit -- step15. The sharded LOCK/PROCLOCK tables +
        // fast-path strong-lock counts. Built before the proc table (ipci.c
        // order) and published process-wide so LockAcquire/Release reach the
        // same tables.
        let lock_manager = crate::backend::storage::lmgr::lock::lock_manager_shared();
        //   (PredicateLockShmemInit -- deferred: SSI/serializable)

        // Set up process table:
        // InitProcGlobal -- step15. The PGPROC arena + the dense MVCC mirror arrays
        // the procarray scans. Built before the ProcArray (which reads it) and
        // published process-wide so InitProcess/ProcKill reach the same arena.
        let proc_global = crate::backend::storage::lmgr::proc::init_proc_global_shared();
        // ProcArrayShmemInit -- step14. The snapshot/horizon source. Sized from
        // the arena (MaxBackends + max_prepared_xacts); falls back to a default
        // until the startup-computed value lands.
        let procarray_maxprocs = if process_config.max_backends > 0 {
            process_config.max_backends as usize
        } else {
            DEFAULT_PROCARRAY_MAXPROCS
        };
        let proc_array =
            crate::backend::storage::ipc::procarray::proc_array_shmem_init(procarray_maxprocs);
        //   (BackendStatusShmemInit -- pgstat, deferred)
        //   (TwoPhaseShmemInit -- deferred)
        // BackgroundWorkerShmemInit -- step17. The bgworker slot table + the
        // parallel register/terminate counters. Sized from max_worker_processes.
        // Published process-wide so RegisterDynamicBackgroundWorker / handle
        // polling, called by arbitrary backends, reach one struct.
        let bgworker = crate::backend::postmaster::bgworker::BackgroundWorkerShmem::new();
        crate::backend::postmaster::bgworker::set_bgworker_shmem(bgworker.clone());

        // Set up shared-inval messaging:
        // SharedInvalShmemInit -- step16. The SI ring transport; published
        // process-wide so sinval.c's Send/Receive reach it without a handle.
        let sinval = crate::backend::storage::ipc::sinvaladt::shared_inval_shmem_init();

        // Set up interprocess signaling mechanisms:
        //   (PMSignalShmemInit -- postmaster signaling, supervisor/step17)
        // ProcSignalShmemInit -- step04, DONE (constructed below at this slot).
        // Published process-wide so a foreign sender (sinval catchup) reaches it.
        let proc_signal = crate::backend::storage::ipc::procsignal::proc_signal_shared();
        // (sync_requests is constructed earlier, before the clog/subtrans SLRUs.)
        // CheckpointerShmemInit -- step17. The checkpoint-request counters + the
        // start/done CVs. Published process-wide so RequestCheckpoint, called by
        // arbitrary backends, reaches it without a SharedState handle.
        let checkpointer = crate::backend::postmaster::checkpointer::CheckpointerShmem::new();
        crate::backend::postmaster::checkpointer::set_checkpointer_shmem(checkpointer.clone());
        // AutoVacuumShmemInit -- step17. The launcher<->worker worker freelist /
        // running list / work items. Published process-wide so the launcher, the
        // workers, and backends (AutoVacuumRequestWork) reach one struct without a
        // SharedState handle.
        let autovacuum = crate::backend::postmaster::autovacuum::AutoVacuumShmem::new();
        crate::backend::postmaster::autovacuum::set_autovacuum_shmem(autovacuum.clone());

        // Rewrite-rule action store (no ipci.c line; a heap structure standing in
        // for the pg_rewrite action-tree storage). Built last among the modules.
        // A fresh per-database rule store, published process-wide so query_rewrite
        // reaches it without a SharedState handle. The publish is re-bound on every
        // SharedState::new (a new tempdir in tests gets a clean store), and the same
        // Arc is kept in the field so the field and the global never diverge.
        let rule_registry =
            Arc::new(crate::backend::rewrite::rule_registry::RuleRegistry::new());
        crate::backend::rewrite::rule_registry::RuleRegistry::set(rule_registry.clone());
        //   (Replication* / WalSnd / WalRcv / WalSummarizer / PgArch /
        //    ApplyLauncher / SlotSync -- deferred)

        // Set up other modules:
        //   (BTreeShmemInit -- nbtree vacuum cycle id, deferred)
        //   (SyncScanShmemInit -- deferred)
        //   (AsyncShmemInit -- LISTEN/NOTIFY, deferred)
        //   (StatsShmemInit -- pgstat, deferred)
        //   (WaitEventCustomShmemInit / InjectionPointShmemInit -- deferred)
        //   (AioShmemInit -- deferred: tokio I/O leaf replaces the aio subsys)

        Arc::new(Self {
            config: process_config,
            fd,
            proc_signal,
            xlog,
            sync_requests,
            buffers,
            variable_cache,
            clog,
            subtrans,
            lock_manager,
            proc_global,
            proc_array,
            sinval,
            checkpointer,
            autovacuum,
            bgworker,
            rule_registry,
        })
    }

    /// The raw async I/O leaf, for WAL/smgr append paths that need positional
    /// I/O without going through the vfd pool. Reached through `FdManager` so
    /// `SharedState` stores a single I/O owner. Derived (not a field), so it
    /// stays hand-written.
    pub fn io(&self) -> &Arc<IoBackend> {
        self.fd.io()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::latch::Latch;

    #[test]
    fn new_default_has_live_fields() {
        let s = SharedState::new(SharedStateConfig::default());
        // fd budget reaches the I/O leaf and reflects the configured budget.
        assert_eq!(s.io().available_permits(), io_backend::DEFAULT_FD_BUDGET);
        // proc_signal registry starts empty but live.
        assert!(s.proc_signal.is_empty());
    }

    #[test]
    fn arc_clone_shares_same_instances() {
        let a = SharedState::new(SharedStateConfig::default());
        let b = a.clone();
        assert!(Arc::ptr_eq(&a.proc_signal, &b.proc_signal));
        assert!(Arc::ptr_eq(&a.fd, &b.fd));
        // The I/O leaf is the same instance through both clones.
        assert!(Arc::ptr_eq(a.io(), b.io()));
    }

    #[test]
    fn registration_visible_through_other_clone() {
        let a = SharedState::new(SharedStateConfig::default());
        let b = a.clone();
        let latch = Arc::new(Latch::new());
        let (_key, _slot) = a.proc_signal.register(7, b"cancel-key", latch);
        // The registration done via `a` is observed via `b`: same registry.
        assert_eq!(b.proc_signal.len(), 1);
    }

    #[test]
    fn independent_states_have_distinct_fields() {
        let a = SharedState::new(SharedStateConfig::default());
        let b = SharedState::new(SharedStateConfig::default());
        assert!(!Arc::ptr_eq(&a.proc_signal, &b.proc_signal));
        assert!(!Arc::ptr_eq(&a.fd, &b.fd));
        // A registration in one must not leak into the other.
        let latch = Arc::new(Latch::new());
        a.proc_signal.register(1, b"k", latch);
        assert_eq!(a.proc_signal.len(), 1);
        assert_eq!(b.proc_signal.len(), 0);
    }

    #[test]
    fn config_respects_custom_budget() {
        let s = SharedState::new(SharedStateConfig {
            fd_budget: 42,
            max_open_files: 8,
            nbuffers: 16,
            data_dir: None,
        });
        assert_eq!(s.io().available_permits(), 42);
        assert_eq!(s.buffers().nbuffers(), 16);
    }
}
