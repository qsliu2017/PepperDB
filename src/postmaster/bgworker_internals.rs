//! Translated from PostgreSQL src/include/postmaster/bgworker_internals.h
//! Postmaster-private background-worker bookkeeping.

use crate::datatype::timestamp::TimestampTz;
use crate::postmaster::bgworker::BackgroundWorker;

/// Maximum possible value of parallel workers.
pub const MAX_PARALLEL_WORKER_LIMIT: i32 = 1024;

/// A registered background worker. The C `dlist_node rw_lnode` (intrusive list
/// link) is dropped; the registry below owns entries in a `Vec`.
pub struct RegisteredBgWorker {
    pub worker: BackgroundWorker, // its registry entry
    pub pid: i32,                 // 0 if not running
    pub crashed_at: TimestampTz,  // if not 0, time it last crashed
    pub shmem_slot: i32,
    pub terminate: bool,
}

// C: `dlist_head BackgroundWorkerList` (intrusive list) -> an owned Vec. TODO(global)
pub static mut BackgroundWorkerList: Vec<RegisteredBgWorker> = Vec::new();

// Shared-memory sizing/init: shmem -> Arc-shared heap state in single process.
pub fn BackgroundWorkerShmemSize() -> usize {
    unimplemented!()
}
pub fn BackgroundWorkerShmemInit() {
    unimplemented!()
}
pub fn BackgroundWorkerStateChange(allow_new_workers: bool) {
    unimplemented!()
}
pub fn ForgetBackgroundWorker(rw: &mut RegisteredBgWorker) {
    unimplemented!()
}
pub fn ReportBackgroundWorkerPID(rw: &mut RegisteredBgWorker) {
    unimplemented!()
}
pub fn ReportBackgroundWorkerExit(rw: &mut RegisteredBgWorker) {
    unimplemented!()
}
pub fn BackgroundWorkerStopNotifications(pid: i32) {
    unimplemented!()
}
pub fn ForgetUnstartedBackgroundWorkers() {
    unimplemented!()
}
pub fn ResetBackgroundWorkerCrashTimes() {
    unimplemented!()
}

/// C: `pg_noreturn ... BackgroundWorkerMain(const void*, size_t)`.
pub fn BackgroundWorkerMain(startup_data: &[u8]) -> ! {
    unimplemented!()
}
