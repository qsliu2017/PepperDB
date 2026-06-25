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
}

impl Default for SharedStateConfig {
    fn default() -> Self {
        Self {
            fd_budget: io_backend::DEFAULT_FD_BUDGET,
            // Leave headroom below the hard budget so transient/durable-op opens
            // and parent-dir fsyncs do not contend with the managed vfd pool.
            max_open_files: io_backend::DEFAULT_FD_BUDGET / 2,
        }
    }
}

/// The shared heap state, replacing the shared-memory segment. Clone the
/// `Arc<SharedState>` into a task to share the SAME subsystem instances; each
/// field is itself `Arc` so reaching through any clone hits the same subsystem.
///
/// Fields are added by later steps at the position dictated by ipci.c order
/// (see `SharedState::new`).
pub struct SharedState {
    /// Process-wide startup config (PG globals.c config half: DataDir, sizing
    /// GUCs). No ipci.c line -- it is process config, not a shmem struct.
    pub config: Arc<ProcessConfig>,

    /// VFD pool over the async I/O leaf. Holds the `IoBackend` internally;
    /// reach the raw leaf via [`SharedState::io`] for WAL/smgr append paths.
    pub fd: Arc<FdManager>,

    /// Inter-task signaling registry (PG `ProcSignal`).
    pub proc_signal: Arc<ProcSignal>,
    // Future Arc fields (varsup, xlog, clog, bufmgr, lockmgr, procarray,
    // sinval, checkpointer, ...) are inserted by later steps -- see new().
}

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
        // Publish for the deprecated miscadmin `DataDir` shims (one per process).
        crate::backend::utils::init::globals::set_process_config(process_config.clone());

        // --- CreateOrAttachShmemStructs roster (ipci.c order) -----------------
        // CreateLWLocks / InitShmemIndex / dsm_shmem_init / DSMRegistryShmemInit:
        //   tombstoned -- no LWLock arena, no shmem index, no DSM (Arc-shared).

        // Set up xlog, clog, and buffers:
        // TODO(step14): VarsupShmemInit  here
        // TODO(step13): XLOGShmemInit  here
        //   (XLogPrefetchShmemInit -- deferred: prefetch is an aio concern)
        //   (XLogRecoveryShmemInit -- step13/recovery)
        // TODO(step14): CLOGShmemInit  here
        //   (CommitTsShmemInit -- deferred)
        // TODO(step14): SUBTRANSShmemInit  here
        //   (MultiXactShmemInit -- deferred)
        // TODO(step12): BufferManagerShmemInit  here

        // Set up lock manager:
        // TODO(step15): LockManagerShmemInit  here
        //   (PredicateLockShmemInit -- deferred: SSI/serializable)

        // Set up process table:
        // TODO(step14): ProcArrayShmemInit  here  (InitProcGlobal + ProcArray)
        //   (BackendStatusShmemInit -- pgstat, deferred)
        //   (TwoPhaseShmemInit -- deferred)
        // TODO(step17): BackgroundWorkerShmemInit  here

        // Set up shared-inval messaging:
        // TODO(step16): SharedInvalShmemInit  here

        // Set up interprocess signaling mechanisms:
        //   (PMSignalShmemInit -- postmaster signaling, supervisor/step17)
        // ProcSignalShmemInit -- step04, DONE (constructed below at this slot).
        let proc_signal = Arc::new(ProcSignal::new());
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

        Arc::new(SharedState { config: process_config, fd, proc_signal })
    }

    /// Process-wide startup config (PG globals.c config half).
    pub fn config(&self) -> &Arc<ProcessConfig> {
        &self.config
    }

    /// The raw async I/O leaf, for WAL/smgr append paths that need positional
    /// I/O without going through the vfd pool. Reached through `FdManager` so
    /// `SharedState` stores a single I/O owner.
    pub fn io(&self) -> &Arc<IoBackend> {
        self.fd.io()
    }

    pub fn fd(&self) -> &Arc<FdManager> {
        &self.fd
    }

    pub fn proc_signal(&self) -> &Arc<ProcSignal> {
        &self.proc_signal
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
        let s = SharedState::new(SharedStateConfig { fd_budget: 42, max_open_files: 8 });
        assert_eq!(s.io().available_permits(), 42);
    }
}
