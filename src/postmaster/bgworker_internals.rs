//! Translated from PostgreSQL src/include/postmaster/bgworker_internals.h
//! Postmaster-private background-worker bookkeeping.
//!
//! The definitions live in the .c translation (src/backend/postmaster/bgworker.rs);
//! this header re-exposes them under their C names. The intrusive `dlist`
//! machinery (`BackgroundWorkerList` as a `dlist_head`, `rw_lnode`) is dropped:
//! the static-registered list is a process-global `Vec` owned by the impl, and
//! the postmaster-side bookkeeping operates on the shmem slot table by index.

/// PG `MAX_PARALLEL_WORKER_LIMIT`.
pub use crate::backend::postmaster::bgworker::MAX_PARALLEL_WORKER_LIMIT;

/// PG `RegisteredBgWorker`. Re-exported from the impl (a `dlist`-free owned
/// entry).
pub use crate::backend::postmaster::bgworker::RegisteredBgWorker;

// Shmem sizing/init: shmem -> Arc-shared heap state in a single process. Sizing
// is implicit (the Vec length), so `BackgroundWorkerShmemSize` is dropped; init
// is `BackgroundWorkerShmem::new`, called from `SharedState::new`.

// Postmaster-side bookkeeping (the reconcile/poll path the supervisor 17f drives
// against the shmem slot table). Re-exposed under their C names; the signatures
// take a slot index rather than `&mut RegisteredBgWorker` (no intrusive list).
pub use crate::backend::postmaster::bgworker::{
    background_worker_state_change as BackgroundWorkerStateChange,
    background_worker_stop_notifications as BackgroundWorkerStopNotifications,
    forget_background_worker as ForgetBackgroundWorker,
    forget_unstarted_background_workers as ForgetUnstartedBackgroundWorkers,
    report_background_worker_exit as ReportBackgroundWorkerExit,
    report_background_worker_pid as ReportBackgroundWorkerPID,
    reset_background_worker_crash_times as ResetBackgroundWorkerCrashTimes,
};

// deleted by redesign: BackgroundWorkerMain (the forked-child entry point) +
// bgworker_die + the sigsetjmp recovery -- a worker is a tokio task whose panic
// propagates to the supervisor (17f), which restarts it.
