//! Translated from PostgreSQL src/include/executor/nodeValuesscan.h

use crate::nodes::execnodes::{EState, ValuesScanState};
use crate::nodes::plannodes::ValuesScan;

pub fn ExecInitValuesScan(_node: &ValuesScan, _estate: &mut EState, _eflags: i32) -> *mut ValuesScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecReScanValuesScan(_node: &mut ValuesScanState) {
    unimplemented!()
}
