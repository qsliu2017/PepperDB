//! Translated from PostgreSQL src/include/executor/nodeFunctionscan.h

use crate::nodes::execnodes::{EState, FunctionScanState};
use crate::nodes::plannodes::FunctionScan;

// TODO(ptr)
pub fn ExecInitFunctionScan(
    _node: &FunctionScan,
    _estate: &mut EState,
    _eflags: i32,
) -> *mut FunctionScanState {
    unimplemented!()
}

pub fn ExecEndFunctionScan(_node: &mut FunctionScanState) {
    unimplemented!()
}

pub fn ExecReScanFunctionScan(_node: &mut FunctionScanState) {
    unimplemented!()
}
