//! Translated from PostgreSQL src/include/libpq/pqmq.h

// Tombstoned: redirects the FE/BE protocol output of a parallel worker over a
// shm_mq back to the leader. Under single-process, shm_mq -> a tokio channel and
// dsm_segment drops out; these are leader/worker plumbing stubs.

use crate::storage::procnumber::ProcNumber;
use crate::utils::elog::ErrorData;

/// Opaque; dynamic shared memory subsystem not ported (single-process).
pub struct dsm_segment;
/// Opaque; shm_mq subsystem not ported (single-process).
pub struct shm_mq_handle;

pub fn pq_redirect_to_shm_mq(_seg: &dsm_segment, _mqh: &shm_mq_handle) {
    unimplemented!()
}

pub fn pq_set_parallel_leader(_pid: i32, _proc_number: ProcNumber) {
    unimplemented!()
}

// StringInfo tombstoned -> &mut Vec<u8> message buffer.
pub fn pq_parse_errornotice(_msg: &mut Vec<u8>, _edata: &mut ErrorData) {
    unimplemented!()
}
