//! Translated from PostgreSQL src/include/executor/nodeWorktablescan.h

use crate::nodes::execnodes::{EState, WorkTableScanState};
use crate::nodes::plannodes::WorkTableScan;

pub fn ExecInitWorkTableScan(_node: &WorkTableScan, _estate: &mut EState<'_>, _eflags: i32) -> *mut WorkTableScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecReScanWorkTableScan(_node: &mut WorkTableScanState) {
    unimplemented!()
}
