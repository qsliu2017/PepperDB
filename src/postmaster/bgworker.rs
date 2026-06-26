//! Translated from PostgreSQL src/include/postmaster/bgworker.h

use bitflags::bitflags;

use crate::postgres::Datum;

// bgw_flags values (BGWORKER_*). BGWORKER_CLASS_PARALLEL is internal.
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BgworkerFlags: i32 {
        const SHMEM_ACCESS = 0x0001;
        const BACKEND_DATABASE_CONNECTION = 0x0002;
        const CLASS_PARALLEL = 0x0010;
    }
}

// Flags to BackgroundWorkerInitializeConnection et al (BGWORKER_BYPASS_*).
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct BgworkerBypassFlags: u32 {
        const ALLOWCONN = 0x0001;
        const ROLELOGINCHECK = 0x0002;
    }
}

pub const BGW_DEFAULT_RESTART_INTERVAL: i32 = 60;
pub const BGW_NEVER_RESTART: i32 = -1;
pub const BGW_MAXLEN: usize = 96;
pub const BGW_EXTRALEN: usize = 128;

pub use crate::pg_config_manual::MAXPGPATH;

/// C: `void (*bgworker_main_type)(Datum main_arg)`.
pub type BgworkerMainType = fn(main_arg: Datum);

// Points in time at which a bgworker can request to be started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BgWorkerStartTime {
    PostmasterStart,
    ConsistentState,
    RecoveryFinished,
}

// In-memory registration descriptor (not on-disk; fixed char[] buffers map to
// owned strings).
#[derive(Debug, Clone)]
pub struct BackgroundWorker {
    pub bgw_name: String,
    pub bgw_type: String,
    pub bgw_flags: BgworkerFlags,
    pub bgw_start_time: BgWorkerStartTime,
    pub bgw_restart_time: i32, // seconds, or BGW_NEVER_RESTART
    pub bgw_library_name: String,
    pub bgw_function_name: String,
    pub bgw_main_arg: Datum,
    pub bgw_extra: String,
    pub bgw_notify_pid: i32, // SIGUSR1 this backend on start/stop
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BgwHandleStatus {
    Started,          // worker is running
    NotYetStarted,    // worker hasn't been started yet
    Stopped,          // worker has exited
    PostmasterDied,   // postmaster died; worker status unclear
}

// Opaque handle to a dynamically-registered worker: a (slot, generation) pair.
// The fields are pub(crate) so the implementation (src/backend/.../bgworker.rs)
// reads them while external callers treat the handle as opaque.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackgroundWorkerHandle {
    pub(crate) slot: usize,
    pub(crate) generation: u64,
}

// Definitions live in src/backend/postmaster/bgworker.rs (the .c translation);
// these `pub use`s expose them under their C names.
pub use crate::backend::postmaster::bgworker::{
    background_worker_block_signals, background_worker_initialize_connection,
    background_worker_initialize_connection_by_oid, background_worker_unblock_signals,
    get_background_worker_handle as GetBackgroundWorkerHandle,
    get_background_worker_pid as GetBackgroundWorkerPid,
    get_background_worker_type_by_pid as GetBackgroundWorkerTypeByPid,
    register_background_worker as RegisterBackgroundWorker,
    register_dynamic_background_worker as RegisterDynamicBackgroundWorker,
    terminate_background_worker as TerminateBackgroundWorker,
    wait_for_background_worker_shutdown as WaitForBackgroundWorkerShutdown,
    wait_for_background_worker_startup as WaitForBackgroundWorkerStartup,
};
pub use background_worker_block_signals as BackgroundWorkerBlockSignals;
pub use background_worker_initialize_connection as BackgroundWorkerInitializeConnection;
pub use background_worker_initialize_connection_by_oid as BackgroundWorkerInitializeConnectionByOid;
pub use background_worker_unblock_signals as BackgroundWorkerUnblockSignals;
