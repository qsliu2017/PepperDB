//! Translated from PostgreSQL src/include/executor/tqueue.h

// shm_mq tombstoned: parallel tuple transport between backends becomes a
// tokio::sync::mpsc channel of MinimalTuple between tasks (single process).
// The shm_mq_handle args below stand in for the channel endpoints.

#[deprecated(note = "TODO(shm_mq): replace shm_mq_handle with a tokio::sync::mpsc endpoint in Phase 2")]
pub struct shm_mq_handle; // TODO(struct-forward): tombstoned shm_mq -> tokio mpsc
#[deprecated(note = "TODO(struct-forward): repoint to crate::tcop::dest::DestReceiver in Phase 2")]
pub struct DestReceiver; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::access::htup::MinimalTuple in Phase 2")]
pub struct MinimalTuple; // TODO(struct-forward)

// Opaque struct, only known inside tqueue.c.
pub struct TupleQueueReader {
    _private: (),
}

#[allow(deprecated)]
pub fn CreateTupleQueueDestReceiver(_handle: &shm_mq_handle) -> DestReceiver {
    unimplemented!()
}

#[allow(deprecated)]
pub fn CreateTupleQueueReader(_handle: &shm_mq_handle) -> TupleQueueReader {
    unimplemented!()
}

pub fn DestroyTupleQueueReader(_reader: &mut TupleQueueReader) {
    unimplemented!()
}

// `bool nowait` selects blocking; `bool *done` out-param folds into Option
// (None = queue done/no tuple available).
#[allow(deprecated)]
pub fn TupleQueueReaderNext(_reader: &mut TupleQueueReader, _nowait: bool) -> Option<MinimalTuple> {
    unimplemented!()
}
