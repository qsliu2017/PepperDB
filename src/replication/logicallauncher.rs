//! Translated from PostgreSQL src/include/replication/logicallauncher.h

use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// GUCs.
pub static mut MAX_LOGICAL_REPLICATION_WORKERS: i32 = 0;
pub static mut MAX_SYNC_WORKERS_PER_SUBSCRIPTION: i32 = 0;
pub static mut MAX_PARALLEL_APPLY_WORKERS_PER_SUBSCRIPTION: i32 = 0;

pub fn apply_launcher_register() {
    unimplemented!()
}

pub fn apply_launcher_main(_main_arg: Datum) {
    unimplemented!()
}

pub fn apply_launcher_shmem_size() -> usize {
    unimplemented!()
}

pub fn apply_launcher_shmem_init() {
    unimplemented!()
}

pub fn apply_launcher_forget_worker_start_time(_subid: Oid) {
    unimplemented!()
}

pub fn apply_launcher_wakeup_at_commit() {
    unimplemented!()
}

pub fn at_eo_xact_apply_launcher(_is_commit: bool) {
    unimplemented!()
}

pub fn is_logical_launcher() -> bool {
    unimplemented!()
}

pub fn get_leader_apply_worker_pid(_pid: i32) -> i32 {
    unimplemented!()
}
