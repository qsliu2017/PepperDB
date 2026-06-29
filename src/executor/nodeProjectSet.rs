//! Translated from PostgreSQL src/include/executor/nodeProjectSet.h

use crate::nodes::execnodes::{EState, ProjectSetState};
use crate::nodes::plannodes::ProjectSet;

pub fn ExecInitProjectSet(_node: &ProjectSet, _estate: &mut EState<'_>, _eflags: i32) -> *mut ProjectSetState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndProjectSet(_node: &mut ProjectSetState) {
    unimplemented!()
}
pub fn ExecReScanProjectSet(_node: &mut ProjectSetState) {
    unimplemented!()
}
