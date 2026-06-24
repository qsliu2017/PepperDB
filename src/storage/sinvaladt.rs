//! Translated from PostgreSQL src/include/storage/sinvaladt.h
//!
//! POSTGRES shared cache invalidation data manager.
//!
//! STUB. The shared cache invalidation manager transmits invalidation messages
//! between backends: any message sent by any backend must be delivered to all
//! already-running backends before it can be forgotten (or a "RESET" message is
//! delivered to backends that fell too far behind). PG backs this with a
//! shared-memory ring buffer (the SI message queue). Under the single-process
//! async model that becomes a shared queue plus per-task wakeups; the message
//! shapes live in `crate::storage::sinval`, the transport is not implemented yet.

use crate::c::LocalTransactionId;
use crate::storage::sinval::SharedInvalidationMessage;

// storage/lock.h, storage/sinval.h are the C includes; we reference the latter.

/// Report the shared-memory space needed by the SI message queue.
// TODO(invalidation): shared queue + per-task wakeups; shmem sizing is moot
// under the single-process model (no shmem segment to size).
pub fn shared_inval_shmem_size() -> usize {
    unimplemented!()
}

/// Create and initialize the SI message buffer.
// TODO(invalidation): shared queue + per-task wakeups
pub fn shared_inval_shmem_init() {
    unimplemented!()
}

/// Register the current backend with the SI message buffer. `send_only` backends
/// only send invalidations and never need to receive them.
// TODO(invalidation): shared queue + per-task wakeups
pub fn shared_inval_backend_init(_send_only: bool) {
    unimplemented!()
}

/// Add `data` to the buffer, to be read by all backends.
// TODO(invalidation): shared queue + per-task wakeups
pub fn si_insert_data_entries(_data: &[SharedInvalidationMessage]) {
    unimplemented!()
}

/// Get next SI messages for current backend, copying into `data` (capacity
/// `data.len()`). C returned the count plus -1 to signal "must RESET"; in Rust
/// that maps to a count-or-reset result.
// TODO(invalidation): shared queue + per-task wakeups
pub fn si_get_data_entries(_data: &mut [SharedInvalidationMessage]) -> i32 {
    unimplemented!()
}

/// Remove messages that have been consumed by all active backends, freeing space
/// and (if needed) signalling lagging backends. `min_free` is the minimum number
/// of free message slots the caller wants available afterward.
// TODO(invalidation): shared queue + per-task wakeups
pub fn si_cleanup_queue(_caller_has_write_lock: bool, _min_free: i32) {
    unimplemented!()
}

/// Allocate the next LocalTransactionId for the current backend.
// TODO(invalidation): shared queue + per-task wakeups
pub fn get_next_local_transaction_id() -> LocalTransactionId {
    unimplemented!()
}
