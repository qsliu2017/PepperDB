//! Translated from PostgreSQL src/include/utils/backend_progress.h

use crate::postgres_ext::Oid;

/// Command type for progress reporting purposes.
pub enum ProgressCommandType {
    Invalid,
    Vacuum,
    Analyze,
    Cluster,
    CreateIndex,
    Basebackup,
    Copy,
}

pub const PGSTAT_NUM_PROGRESS_PARAM: usize = 20;

pub fn pgstat_progress_start_command(cmdtype: ProgressCommandType, relid: Oid) {
    let _ = (cmdtype, relid);
    unimplemented!()
}

pub fn pgstat_progress_update_param(index: i32, val: i64) {
    let _ = (index, val);
    unimplemented!()
}

pub fn pgstat_progress_incr_param(index: i32, incr: i64) {
    let _ = (index, incr);
    unimplemented!()
}

pub fn pgstat_progress_parallel_incr_param(index: i32, incr: i64) {
    let _ = (index, incr);
    unimplemented!()
}

pub fn pgstat_progress_update_multi_param(index: &[i32], val: &[i64]) {
    let _ = (index, val);
    unimplemented!()
}

pub fn pgstat_progress_end_command() {
    unimplemented!()
}
