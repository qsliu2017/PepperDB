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

    /// Process array (PG procarray.c `ProcArrayStruct`; ipci.c
    /// `ProcArrayShmemInit` slot). The snapshot/horizon source; `ProcArrayLock`
    /// is its internal `RwLock`.
    proc_array: crate::backend::storage::ipc::procarray::ProcArray,
    // Future Arc fields (lockmgr, sinval, checkpointer, ...) are inserted by
    // later steps -- see new().
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
    pub fn new(config: SharedStateConfig) -> Arc<SharedState> {
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
        // structure; the checkpointer task drains it (TODO(step17)).
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
        // TODO(step15): LockManagerShmemInit  here
        //   (PredicateLockShmemInit -- deferred: SSI/serializable)

        // Set up process table:
        // ProcArrayShmemInit -- step14. The snapshot/horizon source. Sized from
        // MaxBackends (+ max_prepared_xacts); falls back to a default until the
        // startup-computed value lands (InitProcGlobal itself is step 15).
        let procarray_maxprocs = if process_config.max_backends > 0 {
            process_config.max_backends as usize
        } else {
            DEFAULT_PROCARRAY_MAXPROCS
        };
        let proc_array =
            crate::backend::storage::ipc::procarray::proc_array_shmem_init(procarray_maxprocs);
        //   (BackendStatusShmemInit -- pgstat, deferred)
        //   (TwoPhaseShmemInit -- deferred)
        // TODO(step17): BackgroundWorkerShmemInit  here

        // Set up shared-inval messaging:
        // TODO(step16): SharedInvalShmemInit  here

        // Set up interprocess signaling mechanisms:
        //   (PMSignalShmemInit -- postmaster signaling, supervisor/step17)
        // ProcSignalShmemInit -- step04, DONE (constructed below at this slot).
        let proc_signal = Arc::new(ProcSignal::new());
        // (sync_requests is constructed earlier, before the clog/subtrans SLRUs.)
        // TODO(step17): CheckpointerShmemInit  here
        // TODO(step17): AutoVacuumShmemInit  here
        //   (Replication* / WalSnd / WalRcv / WalSummarizer / PgArch /
        //    ApplyLauncher / SlotSync -- deferred)

        // Set up other modules:
        //   (BTreeShmemInit -- nbtree vacuum cycle id, deferred)
        //   (SyncScanShmemInit -- deferred)
        //   (AsyncShmemInit -- LISTEN/NOTIFY, deferred)
        //   (StatsShmemInit -- pgstat, deferred)
        //   (WaitEventCustomShmemInit / InjectionPointShmemInit -- deferred)
        //   (AioShmemInit -- deferred: tokio I/O leaf replaces the aio subsys)

        Arc::new(SharedState {
            config: process_config,
            fd,
            proc_signal,
            xlog,
            sync_requests,
            buffers,
            variable_cache,
            clog,
            subtrans,
            proc_array,
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
