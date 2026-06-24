//! Translated from PostgreSQL src/include/replication/logicalworker.h

use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// volatile sig_atomic_t set from the parallel-apply signal handler.
pub static PARALLEL_APPLY_MESSAGE_PENDING: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);

pub fn apply_worker_main(_main_arg: Datum) {
    unimplemented!()
}

pub fn parallel_apply_worker_main(_main_arg: Datum) {
    unimplemented!()
}

pub fn tablesync_worker_main(_main_arg: Datum) {
    unimplemented!()
}

pub fn is_logical_worker() -> bool {
    unimplemented!()
}

pub fn is_logical_parallel_apply_worker() -> bool {
    unimplemented!()
}

pub fn handle_parallel_apply_message_interrupt() {
    unimplemented!()
}

pub fn process_parallel_apply_messages() {
    unimplemented!()
}

pub fn logical_rep_workers_wakeup_at_commit(_subid: Oid) {
    unimplemented!()
}

pub fn at_eo_xact_logical_rep_workers(_is_commit: bool) {
    unimplemented!()
}
