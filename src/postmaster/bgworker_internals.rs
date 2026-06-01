//! postmaster/bgworker_internals.h - POSTGRES pluggable background workers internals

use std::ffi::{c_int, c_void};

use crate::c::Size;
use crate::lib::ilist::{dlist_head, dlist_node};

// timestamp.h not yet translated: TimestampTz = int64.
// TODO: dedup with datatype/timestamp.h once translated.
pub type TimestampTz = crate::c::int64;

// bgworker.h not yet translated: minimal stub for BackgroundWorker registry entry.
// TODO: dedup with postmaster/bgworker.h once translated.
pub type BackgroundWorker = c_void;

// pid_t: defined in storage/pg_shmem.rs as well.
// TODO: dedup.
pub type pid_t = c_int;

/* GUC options */

/*
 * Maximum possible value of parallel workers.
 */
pub const MAX_PARALLEL_WORKER_LIMIT: c_int = 1024;

/*
 * List of background workers, private to postmaster.
 *
 * All workers that are currently running will also have an entry in
 * ActiveChildList.
 */
#[repr(C)]
pub struct RegisteredBgWorker {
    pub rw_worker: BackgroundWorker, /* its registry entry */
    pub rw_pid: pid_t,               /* 0 if not running */
    pub rw_crashed_at: TimestampTz,  /* if not 0, time it last crashed */
    pub rw_shmem_slot: c_int,
    pub rw_terminate: bool,
    pub rw_lnode: dlist_node, /* list link */
}

extern "C" {
    pub static mut BackgroundWorkerList: dlist_head;
}

pub unsafe fn BackgroundWorkerShmemSize() -> Size {
    unimplemented!()
}

pub unsafe fn BackgroundWorkerShmemInit() {
    unimplemented!()
}

pub unsafe fn BackgroundWorkerStateChange(allow_new_workers: bool) {
    unimplemented!()
}

pub unsafe fn ForgetBackgroundWorker(rw: *mut RegisteredBgWorker) {
    unimplemented!()
}

pub unsafe fn ReportBackgroundWorkerPID(rw: *mut RegisteredBgWorker) {
    unimplemented!()
}

pub unsafe fn ReportBackgroundWorkerExit(rw: *mut RegisteredBgWorker) {
    unimplemented!()
}

pub unsafe fn BackgroundWorkerStopNotifications(pid: pid_t) {
    unimplemented!()
}

pub unsafe fn ForgetUnstartedBackgroundWorkers() {
    unimplemented!()
}

pub unsafe fn ResetBackgroundWorkerCrashTimes() {
    unimplemented!()
}

/* Entry point for background worker processes */
/* pg_noreturn */
pub unsafe fn BackgroundWorkerMain(startup_data: *const c_void, startup_data_len: crate::c::Size) -> ! {
    unimplemented!()
}
