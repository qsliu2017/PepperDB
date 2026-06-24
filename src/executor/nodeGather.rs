//! Translated from PostgreSQL src/include/executor/nodeGather.h

use crate::nodes::execnodes::{EState, GatherState};
use crate::nodes::plannodes::Gather;

// TODO(ptr)
pub fn ExecInitGather(_node: &Gather, _estate: &mut EState, _eflags: i32) -> *mut GatherState {
    unimplemented!()
}

pub fn ExecEndGather(_node: &mut GatherState) {
    unimplemented!()
}

pub fn ExecShutdownGather(_node: &mut GatherState) {
    unimplemented!()
}

pub fn ExecReScanGather(_node: &mut GatherState) {
    unimplemented!()
}
