//! Translated from PostgreSQL src/include/access/parallel.h
//!
//! Single-process simplification: parallel workers become tokio tasks; the DSM
//! segment + shm_toc keyed regions collapse into Arc-shared state + tokio mpsc
//! channels. The backend bodies live in
//! `crate::backend::access::transam::parallel`; this header re-exports the type
//! surface and the C-named entry points.

// Types (reshaped for the in-process model).
pub use crate::backend::access::transam::parallel::{
    ParallelContext, ParallelMessage, ParallelWorkerInfo, ParallelWorkerMainType,
};

// Tombstoned shmem types, kept as opaque placeholders for the deferred executor /
// index-build parallel stubs (execParallel, _bt/_brin/_gin build mains) whose
// signatures still mention them. They carry no fields under single-process.
/// Opaque; tombstoned DSM segment (single-process has no shared memory).
pub struct dsm_segment;
/// Opaque; tombstoned shm_toc (keyed regions become struct fields / Arc state).
pub struct shm_toc;
/// Opaque; tombstoned shm_mq handle (replaced by a tokio channel endpoint).
pub struct shm_mq_handle;

/// Tombstoned worker-side (seg, toc) handoff, kept for deferred executor stubs
/// (`Exec*InitializeWorker`). Carries no state under single-process.
pub struct ParallelWorkerContext {
    pub seg: Option<Box<dsm_segment>>,
    pub toc: Option<Box<shm_toc>>,
}

// PG `CreateParallelContext`
pub use crate::backend::access::transam::parallel::CreateParallelContext;
// PG `InitializeParallelDSM`
pub use crate::backend::access::transam::parallel::InitializeParallelDSM;
// PG `ReinitializeParallelDSM`
pub use crate::backend::access::transam::parallel::ReinitializeParallelDSM;
// PG `ReinitializeParallelWorkers`
pub use crate::backend::access::transam::parallel::ReinitializeParallelWorkers;
// PG `LaunchParallelWorkers`
pub use crate::backend::access::transam::parallel::LaunchParallelWorkers;
// PG `WaitForParallelWorkersToAttach`
pub use crate::backend::access::transam::parallel::WaitForParallelWorkersToAttach;
// PG `WaitForParallelWorkersToFinish`
pub use crate::backend::access::transam::parallel::WaitForParallelWorkersToFinish;
// PG `DestroyParallelContext`
pub use crate::backend::access::transam::parallel::DestroyParallelContext;
// PG `ParallelContextActive`
pub use crate::backend::access::transam::parallel::ParallelContextActive;
// PG `HandleParallelMessageInterrupt`
pub use crate::backend::access::transam::parallel::HandleParallelMessageInterrupt;
// PG `ProcessParallelMessages`
pub use crate::backend::access::transam::parallel::ProcessParallelMessages;
// PG `AtEOXact_Parallel`
pub use crate::backend::access::transam::parallel::AtEOXact_Parallel;
// PG `AtEOSubXact_Parallel`
pub use crate::backend::access::transam::parallel::AtEOSubXact_Parallel;
// PG `ParallelWorkerReportLastRecEnd`
pub use crate::backend::access::transam::parallel::ParallelWorkerReportLastRecEnd;
// PG `ParallelWorkerMain`: the C bgworker string-entrypoint; in-process this is
// the backend worker body the leader's spawn cradle invokes.
pub use crate::backend::access::transam::parallel::parallel_worker_main as ParallelWorkerMain;

// PG `ParallelWorkerNumber` / `IsParallelWorker` / `InitializingParallelWorker`:
// the `static mut` process globals become per-task accessors. The accessor now
// returns `Option<u32>` (None = not a worker) instead of the C `-1` sentinel.
pub use crate::backend::access::transam::parallel::{
    initializing_parallel_worker, is_parallel_worker as IsParallelWorker,
    parallel_worker_number as ParallelWorkerNumber, parallel_worker_number,
};
