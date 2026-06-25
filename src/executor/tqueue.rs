//! Translated from PostgreSQL src/include/executor/tqueue.h

use crate::executor::tuptable::MinimalTuple;

// shm_mq tombstoned: parallel tuple transport between backends becomes a
// tokio::sync::mpsc channel of MinimalTuple between tasks (single process).
// The shm_mq_handle args below stand in for the channel endpoints.

/// Opaque; tombstoned shm_mq -> tokio mpsc endpoint (not ported).
pub struct shm_mq_handle;
/// Opaque; DestReceiver not ported (tcop/dest absent in this port).
pub struct DestReceiver;

// Opaque struct, only known inside tqueue.c.
pub struct TupleQueueReader {
    _private: (),
}

pub fn CreateTupleQueueDestReceiver(_handle: &shm_mq_handle) -> DestReceiver {
    unimplemented!()
}

pub fn CreateTupleQueueReader(_handle: &shm_mq_handle) -> TupleQueueReader {
    unimplemented!()
}

pub fn DestroyTupleQueueReader(_reader: &mut TupleQueueReader) {
    unimplemented!()
}

// `bool nowait` selects blocking; `bool *done` out-param folds into Option
// (None = queue done/no tuple available).
pub fn TupleQueueReaderNext(_reader: &mut TupleQueueReader, _nowait: bool) -> Option<MinimalTuple> {
    unimplemented!()
}
