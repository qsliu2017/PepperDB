//! Translated from PostgreSQL src/include/libpq/pqmq.h

// Tombstoned: redirects the FE/BE protocol output of a parallel worker over a
// shm_mq back to the leader. Under single-process, shm_mq -> a tokio channel and
// dsm_segment drops out; these are leader/worker plumbing stubs.

use crate::storage::procnumber::ProcNumber;

#[deprecated(note = "TODO(shm_mq): replace dsm_segment with owned Arc-shared state in Phase 2")]
pub struct dsm_segment; // TODO(struct-forward): tombstoned dsm
#[deprecated(note = "TODO(shm_mq): replace shm_mq_handle with a tokio channel endpoint in Phase 2")]
pub struct shm_mq_handle; // TODO(struct-forward): tombstoned shm_mq
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::elog::ErrorData in Phase 2")]
pub struct ErrorData; // TODO(struct-forward)

#[allow(deprecated)]
pub fn pq_redirect_to_shm_mq(_seg: &dsm_segment, _mqh: &shm_mq_handle) {
    unimplemented!()
}

pub fn pq_set_parallel_leader(_pid: i32, _proc_number: ProcNumber) {
    unimplemented!()
}

// StringInfo tombstoned -> &mut Vec<u8> message buffer.
#[allow(deprecated)]
pub fn pq_parse_errornotice(_msg: &mut Vec<u8>, _edata: &mut ErrorData) {
    unimplemented!()
}
