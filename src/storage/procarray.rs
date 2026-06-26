//! Translated from PostgreSQL src/include/storage/procarray.h
//!
//! POSTGRES process array. Under the single-process port the shmem-resident
//! `ProcArrayStruct` collapses to ordinary `Arc`-shared state (`ProcArray` on
//! `SharedState`, reached via `shared.proc_array()`); `ProcArrayLock` becomes
//! that struct's internal `RwLock`.
//!
//! The header declares; the backend module
//! (`backend::storage::ipc::procarray`) defines (rules s2). After R-A the
//! snapshot/horizon/running-xact entry points are inherent methods on
//! `ProcArray`, taking the narrow handles they need (`&VariableCache`, and
//! `&SlruCtl` for the clog/subtrans probes) rather than `&SharedState`. They are
//! reached through the owning type (`shared.proc_array().method(...)`), so they
//! are NOT re-exported as free functions here. Only the genuinely free
//! functions, the per-task `TransactionXmin`/`RecentXmin` accessors, the
//! construction helpers, and the public types are re-exported. `PGPROC` pointers
//! become references; not-found / invalid-xid sentinels become `Option`;
//! pointer out-params become tuples or owned `Vec`s / structs.

// Construction + recovery-init free fns, MyProc/pid lookups (staged), per-task
// TransactionXmin/RecentXmin accessors, and the public types.
pub use crate::backend::storage::ipc::procarray::{
    ComputeXidHorizonsResult, ProcArray, ProcNumberXids, backend_pid_get_proc,
    backend_pid_get_proc_with_lock, is_backend_pid, proc_array_init_recovery,
    proc_array_shmem_init, proc_array_shmem_size, proc_number_get_proc, recent_xmin,
    snapshot_globals_scope, transaction_xmin,
};
