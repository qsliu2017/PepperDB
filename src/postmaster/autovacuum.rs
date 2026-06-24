//! Translated from PostgreSQL src/include/postmaster/autovacuum.h
//! Integrated autovacuum daemon.

use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;

/// Work that other processes can request from autovacuum. (C enum.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoVacuumWorkItemType {
    AVW_BRINSummarizeRange,
}

// GUC variables. TODO(global)
pub static mut autovacuum_start_daemon: bool = false;
pub static mut autovacuum_worker_slots: i32 = 0;
pub static mut autovacuum_max_workers: i32 = 0;
pub static mut autovacuum_work_mem: i32 = 0;
pub static mut autovacuum_naptime: i32 = 0;
pub static mut autovacuum_vac_thresh: i32 = 0;
pub static mut autovacuum_vac_max_thresh: i32 = 0;
pub static mut autovacuum_vac_scale: f64 = 0.0;
pub static mut autovacuum_vac_ins_thresh: i32 = 0;
pub static mut autovacuum_vac_ins_scale: f64 = 0.0;
pub static mut autovacuum_anl_thresh: i32 = 0;
pub static mut autovacuum_anl_scale: f64 = 0.0;
pub static mut autovacuum_freeze_max_age: i32 = 0;
pub static mut autovacuum_multixact_freeze_max_age: i32 = 0;
pub static mut autovacuum_vac_cost_delay: f64 = 0.0;
pub static mut autovacuum_vac_cost_limit: i32 = 0;

pub static mut AutovacuumLauncherPid: i32 = 0;
pub static mut Log_autovacuum_min_duration: i32 = 0;

pub fn AutoVacuumingActive() -> bool {
    unimplemented!()
}
pub fn autovac_init() {
    unimplemented!()
}
pub fn AutoVacWorkerFailed() {
    unimplemented!()
}

/// C: `pg_noreturn ... AutoVacLauncherMain(const void*, size_t)`.
pub fn AutoVacLauncherMain(startup_data: &[u8]) -> ! {
    unimplemented!()
}
pub fn AutoVacWorkerMain(startup_data: &[u8]) -> ! {
    unimplemented!()
}

pub fn AutoVacuumRequestWork(
    type_: AutoVacuumWorkItemType,
    relation_id: Oid,
    blkno: BlockNumber,
) -> bool {
    unimplemented!()
}

// Shared-memory sizing/init: shmem -> Arc-shared heap state in single process.
pub fn AutoVacuumShmemSize() -> usize {
    unimplemented!()
}
pub fn AutoVacuumShmemInit() {
    unimplemented!()
}
