//! Translated from PostgreSQL src/include/executor/nodeGroup.h

use crate::nodes::execnodes::{EState, GroupState};
use crate::nodes::plannodes::Group;

// TODO(ptr)
pub fn ExecInitGroup(_node: &Group, _estate: &mut EState<'_>, _eflags: i32) -> *mut GroupState {
    unimplemented!()
}

pub fn ExecEndGroup(_node: &mut GroupState) {
    unimplemented!()
}

pub fn ExecReScanGroup(_node: &mut GroupState) {
    unimplemented!()
}
