//! Translated from PostgreSQL src/include/executor/nodeGatherMerge.h

use crate::nodes::execnodes::{EState, GatherMergeState};
use crate::nodes::plannodes::GatherMerge;

// TODO(ptr)
pub fn ExecInitGatherMerge(
    _node: &GatherMerge,
    _estate: &mut EState<'_>,
    _eflags: i32,
) -> *mut GatherMergeState {
    unimplemented!()
}

pub fn ExecEndGatherMerge(_node: &mut GatherMergeState) {
    unimplemented!()
}

pub fn ExecReScanGatherMerge(_node: &mut GatherMergeState) {
    unimplemented!()
}

pub fn ExecShutdownGatherMerge(_node: &mut GatherMergeState) {
    unimplemented!()
}
