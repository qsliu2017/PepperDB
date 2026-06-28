//! Inter-process communication initialization. Translated from backend/storage/ipc/ipci.c.
//!
//! In PostgreSQL, this is where the postmaster creates the shared-memory
//! segment and brings every shared subsystem online. `CalculateShmemSize`
//! sums each subsystem's space request (plus extension requests) to size the
//! segment; `CreateSharedMemoryAndSemaphores` allocates the segment and the
//! semaphore set and then runs `CreateOrAttachShmemStructs`, a fixed ordered
//! roster of per-subsystem `*ShmemInit` calls (LWLocks, the shmem index, the
//! transaction log, buffers, the lock manager, the process array, shared-
//! invalidation messaging, and so on). The roster order encodes real
//! initialization dependencies. Under `EXEC_BACKEND`, `AttachSharedMemoryStructs`
//! re-runs the same roster so a freshly exec'd child can rebuild its local
//! pointers into the already-existing segment.
//!
//! PepperDB runs as a single process, so there is no shared-memory segment to
//! size, allocate, or attach. The shared state is an `Arc`-shared `SharedState`
//! value living on the heap, whose fields are the per-subsystem structures that
//! PostgreSQL would place in the segment. `CreateSharedMemoryAndSemaphores`
//! therefore becomes the constructor for that value: it builds the `SharedState`
//! and hands back an `Arc` that the supervisor clones into each spawned task.
//! The dependency-ordered roster of subsystem initializations is reproduced
//! inside `SharedState`'s constructor rather than here.
//!
//! Because there is no segment and no dynamic loader, several PostgreSQL entry
//! points have no counterpart and remain as deprecated stubs: segment sizing
//! (`CalculateShmemSize`), extension shared-memory requests
//! (`RequestAddinShmemSpace`), the `EXEC_BACKEND` re-attach
//! (`AttachSharedMemoryStructs`), and the standalone roster
//! (`CreateOrAttachShmemStructs`). The loadable-module startup hook, dynamic
//! shared memory startup, and the runtime shared-memory-size GUCs describe a
//! segment that does not exist here and are dropped entirely.

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
