//! Translated from PostgreSQL src/include/executor/nodeCtescan.h

use crate::nodes::execnodes::{CteScanState, EState};
use crate::nodes::plannodes::CteScan;

// TODO(ptr)
pub fn ExecInitCteScan(_node: &CteScan, _estate: &mut EState, _eflags: i32) -> *mut CteScanState {
    unimplemented!()
}

pub fn ExecEndCteScan(_node: &mut CteScanState) {
    unimplemented!()
}

pub fn ExecReScanCteScan(_node: &mut CteScanState) {
    unimplemented!()
}
