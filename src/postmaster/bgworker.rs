//! Translated from PostgreSQL src/include/postmaster/bgworker.h

use bitflags::bitflags;

use crate::postgres::Datum;
use crate::postgres_ext::Oid;

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

// Opaque handle to a dynamically-registered worker.
pub struct BackgroundWorkerHandle {
    _private: (),
}

pub fn register_background_worker(_worker: &BackgroundWorker) {
    unimplemented!()
}

/// C: `bool RegisterDynamicBackgroundWorker(BackgroundWorker *, BackgroundWorkerHandle **)`.
/// Returns the handle on success, `None` on failure.
pub fn register_dynamic_background_worker(
    _worker: &BackgroundWorker,
) -> Option<BackgroundWorkerHandle> {
    unimplemented!()
}

/// C: `BgwHandleStatus GetBackgroundWorkerPid(handle, pid_t *pidp)`.
/// Pairs the status with the worker pid when started.
pub fn get_background_worker_pid(_handle: &BackgroundWorkerHandle) -> (BgwHandleStatus, i32) {
    unimplemented!()
}

pub fn wait_for_background_worker_startup(_handle: &BackgroundWorkerHandle) -> (BgwHandleStatus, i32) {
    unimplemented!()
}

pub fn wait_for_background_worker_shutdown(_handle: &BackgroundWorkerHandle) -> BgwHandleStatus {
    unimplemented!()
}

pub fn get_background_worker_type_by_pid(_pid: i32) -> Option<String> {
    unimplemented!()
}

pub fn terminate_background_worker(_handle: &BackgroundWorkerHandle) {
    unimplemented!()
}

pub fn background_worker_initialize_connection(
    _dbname: Option<&str>,
    _username: Option<&str>,
    _flags: BgworkerBypassFlags,
) {
    unimplemented!()
}

pub fn background_worker_initialize_connection_by_oid(
    _dboid: Oid,
    _useroid: Oid,
    _flags: BgworkerBypassFlags,
) {
    unimplemented!()
}

pub fn background_worker_block_signals() {
    unimplemented!()
}

pub fn background_worker_unblock_signals() {
    unimplemented!()
}
