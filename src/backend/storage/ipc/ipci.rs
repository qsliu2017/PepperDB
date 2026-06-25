//! Translated from PostgreSQL src/backend/storage/ipc/ipci.c
//!
//! ipci.c set up the shared-memory segment and ran the ordered roster of
//! `*ShmemInit` calls. Under the single-process async model there is no segment:
//! `CreateSharedMemoryAndSemaphores` becomes the constructor for the
//! `Arc`-shared [`SharedState`] (the typed replacement for the segment), and the
//! segment-sizing machinery is tombstoned.
//!
//! The ordered roster itself lives in [`SharedState::new`] -- ipci.c's
//! `CreateOrAttachShmemStructs` ordering encodes real init dependencies and is
//! reproduced there with `TODO(stepNN)` placeholders.
//!
//! Tombstoned siblings (headers only, no backend files): storage/ipc/shmem.c,
//! dsm.c, dsm_impl.c, dsm_registry.c -- all replaced by Arc-shared heap state
//! and tokio channels; no shared-memory segment exists to size or attach.

use std::sync::Arc;

use crate::shared_state::{SharedState, SharedStateConfig};

/// Create and initialize the shared state (PG `CreateSharedMemoryAndSemaphores`).
///
/// The real entry point: instead of sizing/allocating a segment it builds the
/// `Arc<SharedState>` whose fields are the per-subsystem shared structures. The
/// supervisor calls this once at startup and `Arc::clone`s the result into each
/// spawned task.
pub fn CreateSharedMemoryAndSemaphores() -> Arc<SharedState> {
    SharedState::new(SharedStateConfig::default())
}

/// As above, with an explicit config (sizing knobs for the I/O leaf).
pub fn CreateSharedMemoryAndSemaphoresWithConfig(config: SharedStateConfig) -> Arc<SharedState> {
    SharedState::new(config)
}

// --- Tombstoned segment machinery -----------------------------------------
// No shared-memory segment under single-process; state is Arc-shared (see
// src/shared_state.rs). These existed only to size/allocate/attach the segment.

/// Tombstone: no shared-memory segment to size.
#[deprecated(note = "no shared-memory segment under single-process; state is Arc-shared (see src/shared_state.rs)")]
pub fn CalculateShmemSize() {}

/// Tombstone: extensions request shmem via the dropped dynamic-loader path.
#[deprecated(note = "no shared-memory segment under single-process; state is Arc-shared (see src/shared_state.rs)")]
pub fn RequestAddinShmemSpace() {}

/// Tombstone: EXEC_BACKEND attach has no analogue (no fork, no segment).
#[deprecated(note = "no shared-memory segment under single-process; state is Arc-shared (see src/shared_state.rs)")]
pub fn AttachSharedMemoryStructs() {}

/// Tombstone: the roster now lives in `SharedState::new`.
#[deprecated(note = "no shared-memory segment under single-process; the init roster is SharedState::new (see src/shared_state.rs)")]
pub fn CreateOrAttachShmemStructs() {}

// Dropped entirely (no tombstone fn): shmem_startup_hook / dsm_postmaster_startup
// (loadable-module shmem setup + DSM startup) -- the dynamic loader is removed by
// redesign and there is no DSM. InitializeShmemGUCs is dropped: the
// shared_memory_size / huge_pages GUCs describe a segment that does not exist.
