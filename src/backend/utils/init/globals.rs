//! Translated from PostgreSQL src/backend/utils/init/globals.c
//!
//! globals.c was a flat list of process-global variables. Under the
//! single-process async model that list is split by lifetime and scope:
//!
//! - Per-task identity + user/database state (`MyProcPid`, `MyStartTime[stamp]`,
//!   `MyBackendType`, `MyDatabaseId`, `MyDatabaseTableSpace`, the user-id stack)
//!   moved to [`crate::session::Session`], published as a task-local.
//! - Process-wide startup config (`DataDir`, `data_directory_mode`, and the
//!   shared-structure sizing GUCs `NBuffers` / `MaxBackends` / ...) is collected
//!   in [`ProcessConfig`], reachable through
//!   [`crate::shared_state::SharedState::config`].
//! - The interrupt-flag / holdoff / crit-section counters and the
//!   processing-mode + miscadmin function shims stay in
//!   [`crate::miscadmin`] (header-origin), where the C-named accessors now read
//!   `Session` / `ProcessConfig` instead of `static mut`s.
//!
//! What remains genuinely process-global with no better home is translated as
//! constants here; the rest of globals.c is therefore a tombstone pointing at
//! the modules above.
//!
//! NOTE(guc): the GUC sizing fields below currently carry only their compiled-in
//! defaults. They are populated from the GUC machinery at startup (TODO(guc));
//! `MaxBackends` in particular is computed by the supervisor after background
//! workers register (PG `InitializeMaxBackends`).

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
