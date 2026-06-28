//! Process-wide global state and startup configuration. Translated from backend/utils/init/globals.c.
//!
//! In PostgreSQL, globals.c is the single home for variables that are used all
//! over the backend rather than belonging to any one module: the frontend
//! protocol version, the volatile interrupt-pending flags, per-backend identity
//! (process id, start time, backend type, database and tablespace ids, the
//! authenticated and current user ids), the data directory path and its
//! permission mode, and the compiled-in defaults for shared-structure sizing
//! such as the buffer count and the backend/connection/worker limits.
//!
//! Under PepperDB's single-process async model that flat list no longer fits in
//! one place, so it is split by lifetime and scope. Per-task identity and
//! user/database state live on the backend's session object and are reached
//! through a task-local rather than as process-wide `static mut`s, since many
//! backends share one address space. The interrupt-pending and critical-section
//! counters, processing mode, and the related accessor shims live with the rest
//! of the miscellaneous backend administration code.
//!
//! What remains here is the genuinely process-wide startup configuration,
//! gathered into [`ProcessConfig`]: the data directory and its mode plus the
//! shared-structure sizing parameters. One instance exists per process and is
//! shared read-only through an `Arc` held by the shared state; the data
//! directory is the exception, kept behind a mutex because it is assigned very
//! early (before the shared state is fully wired) and read by the lock-file and
//! version-check code. The sizing fields currently hold only PostgreSQL's
//! compiled-in defaults; populating them from configuration is not yet wired up,
//! and the maximum backend count, which PostgreSQL computes once background
//! workers have registered, is likewise still a placeholder.

/// PostgreSQL data-directory default mode (`PG_DIR_MODE_OWNER`, 0700).
pub const PG_DIR_MODE_OWNER: u32 = 0o700;

/// Process-wide startup configuration, replacing the config half of globals.c.
/// Shared (read-only after startup) via `Arc<ProcessConfig>` inside
/// [`crate::shared_state::SharedState`]. `DataDir` is the exception: it must be
/// settable very early (before/at startup, before `SharedState` is fully wired)
/// and is read by the lock-file code, so it lives behind a `Mutex`.
pub struct ProcessConfig {
    /// Absolute path to the PGDATA root (PG `DataDir`). `None` until set by
    /// startup. Behind a `Mutex` because it is assigned early and read by
    /// `miscinit`'s lock-file / version-check code.
    data_dir: parking_lot::Mutex<Option<String>>,

    /// Mode of the data directory (PG `data_directory_mode`); 0700, or 0750 if
    /// the datadir grants group read/execute (set by `checkDataDir`).
    pub data_directory_mode: u32,

    // --- Shared-structure sizing GUCs (TODO(guc): populate from GUC) ---
    // TODO(guc): these duplicate the `miscadmin` static-muts (NBuffers/
    // MaxBackends/MaxConnections/*_buffers/...); only the miscadmin static-muts
    // are read today. Consolidate readers onto ProcessConfig when GUC lands.
    /// PG `NBuffers`.
    pub nbuffers: i32,
    /// PG `MaxBackends` (computed at startup).
    pub max_backends: i32,
    /// PG `MaxConnections`.
    pub max_connections: i32,
    /// PG `max_worker_processes`.
    pub max_worker_processes: i32,
    /// PG `max_parallel_workers`.
    pub max_parallel_workers: i32,
}

impl ProcessConfig {
    /// Construct with PostgreSQL's compiled-in defaults (globals.c initializers).
    /// `DataDir` starts unset. TODO(guc): overwrite from parsed GUC at startup.
    pub fn new() -> Self {
        Self {
            data_dir: parking_lot::Mutex::new(None),
            data_directory_mode: PG_DIR_MODE_OWNER,
            nbuffers: 16384,
            max_backends: 0,
            max_connections: 100,
            max_worker_processes: 8,
            max_parallel_workers: 8,
        }
    }

    /// Current data directory, if set (PG `DataDir`).
    pub fn data_dir(&self) -> Option<String> {
        self.data_dir.lock().clone()
    }

    /// Set the data directory (PG `SetDataDir`). The caller is responsible for
    /// passing an absolute path.
    pub fn set_data_dir(&self, dir: impl Into<String>) {
        *self.data_dir.lock() = Some(dir.into());
    }
}

impl Default for ProcessConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Process-wide accessor
// ---------------------------------------------------------------------------
//
// `ProcessConfig` lives in `SharedState`, but the deprecated C-named shims in
// `miscadmin` (e.g. `DataDir`) have no `SharedState` handle. They read it through
// this process-wide pointer, which `SharedState::new` publishes once. There is
// exactly one `SharedState` per process under the single-process model, so a
// `OnceLock` is the right shape.

use std::sync::{Arc, OnceLock};

static PROCESS_CONFIG: OnceLock<Arc<ProcessConfig>> = OnceLock::new();

/// Publish the process-wide [`ProcessConfig`] (called once by `SharedState::new`).
/// Ignores a second publish so tests constructing multiple `SharedState`s do not
/// panic; the first one wins.
pub fn set_process_config(config: Arc<ProcessConfig>) {
    let _ = PROCESS_CONFIG.set(config);
}

/// The process-wide [`ProcessConfig`], if it has been published. Used by the
/// deprecated `miscadmin` shims; new code should reach it via `SharedState`.
pub fn process_config() -> Option<Arc<ProcessConfig>> {
    PROCESS_CONFIG.get().cloned()
}
