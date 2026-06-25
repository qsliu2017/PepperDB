//! Translated from PostgreSQL src/include/access/parallel.h

// Single-process simplification: parallel workers become tokio tasks; the
// DSM segment + shm_toc keyed regions collapse into ordinary struct fields on an
// Arc-shared context. dsm_segment / shm_toc / shm_mq are tombstoned shmem types.

use crate::access::xlogdefs::XLogRecPtr;
use crate::c::SubTransactionId;
use crate::postmaster::bgworker::BackgroundWorkerHandle;
use crate::postgres::Datum;
use crate::utils::elog::ErrorContextCallback;

/// Opaque; tombstoned DSM segment, single-process port has no shared memory.
pub struct dsm_segment;
/// Opaque; tombstoned shm_toc, keyed regions become struct fields.
pub struct shm_toc;
/// Opaque; tombstoned shm_toc estimator.
pub struct shm_toc_estimator;
/// Opaque; tombstoned shm_mq, replaced by a tokio channel endpoint later.
pub struct shm_mq_handle;

// worker main: (seg, toc) shmem handoff -> a task body over Arc-shared state.
pub type parallel_worker_main_type = fn(seg: &dsm_segment, toc: &shm_toc);

pub struct ParallelWorkerInfo {
    pub bgwhandle: Option<Box<BackgroundWorkerHandle>>, // tokio JoinHandle later
    pub error_mqh: Option<Box<shm_mq_handle>>,          // error channel
}

pub struct ParallelContext {
    // dlist_node link -> owning collection holds this; field dropped.
    pub subid: SubTransactionId,
    pub nworkers: i32,           // maximum number of workers to launch
    pub nworkers_to_launch: i32, // actual number of workers to launch
    pub nworkers_launched: i32,
    pub library_name: String,
    pub function_name: String,
    pub error_context_stack: Option<Box<ErrorContextCallback>>,
    pub estimator: shm_toc_estimator,
    pub seg: Option<Box<dsm_segment>>,
    pub private_memory: Option<Box<()>>, // void* private region -> owned Box later
    pub toc: Option<Box<shm_toc>>,
    pub worker: Option<Box<ParallelWorkerInfo>>,
    pub nknown_attached_workers: i32,
    pub known_attached_workers: Vec<bool>,
}

pub struct ParallelWorkerContext {
    pub seg: Option<Box<dsm_segment>>,
    pub toc: Option<Box<shm_toc>>,
}

// Process-globals -> task-local later.
pub static mut ParallelMessagePending: i32 = 0; // volatile sig_atomic_t
pub static mut ParallelWorkerNumber: i32 = -1;
pub static mut InitializingParallelWorker: bool = false;

#[allow(static_mut_refs)]
pub fn IsParallelWorker() -> bool {
    unsafe { ParallelWorkerNumber >= 0 }
}

pub fn CreateParallelContext(
    _library_name: &str,
    _function_name: &str,
    _nworkers: i32,
) -> ParallelContext {
    unimplemented!()
}

pub fn InitializeParallelDSM(_pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ReinitializeParallelDSM(_pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ReinitializeParallelWorkers(_pcxt: &mut ParallelContext, _nworkers_to_launch: i32) {
    unimplemented!()
}

pub fn LaunchParallelWorkers(_pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn WaitForParallelWorkersToAttach(_pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn WaitForParallelWorkersToFinish(_pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn DestroyParallelContext(_pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ParallelContextActive() -> bool {
    unimplemented!()
}

pub fn HandleParallelMessageInterrupt() {
    unimplemented!()
}

pub fn ProcessParallelMessages() {
    unimplemented!()
}

pub fn AtEOXact_Parallel(_is_commit: bool) {
    unimplemented!()
}

pub fn AtEOSubXact_Parallel(_is_commit: bool, _my_sub_id: SubTransactionId) {
    unimplemented!()
}

pub fn ParallelWorkerReportLastRecEnd(_last_xlog_end: XLogRecPtr) {
    unimplemented!()
}

// Worker entry; Datum main_arg is the dsm handle key under shmem -> task arg later.
pub fn ParallelWorkerMain(_main_arg: Datum) {
    unimplemented!()
}
