//! Translated from PostgreSQL src/include/commands/explain_dr.h

use crate::commands::explain_state::ExplainState;
use crate::executor::instrument::BufferUsage;
use crate::tcop::dest::DestReceiver;
use std::time::Duration;

/// Instrumentation data for EXPLAIN's SERIALIZE option. In-memory.
pub struct SerializeMetrics {
    pub bytesSent: u64,         // # of bytes serialized
    pub timeSpent: Duration,    // instr_time: time spent serializing
    pub bufferUsage: BufferUsage, // buffers accessed during serialization
}

// TODO(ptr): the concrete receiver type is chosen at runtime; revisit ownership.
pub fn CreateExplainSerializeDestReceiver(_es: &mut ExplainState) -> Box<dyn DestReceiver> {
    unimplemented!()
}

pub fn GetSerializationMetrics(_dest: &dyn DestReceiver) -> SerializeMetrics {
    unimplemented!()
}
