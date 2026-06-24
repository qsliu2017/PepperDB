//! Translated from PostgreSQL src/include/executor/nodeTableFuncscan.h

use crate::nodes::execnodes::{EState, TableFuncScanState};
use crate::nodes::plannodes::TableFuncScan;

pub fn ExecInitTableFuncScan(_node: &TableFuncScan, _estate: &mut EState, _eflags: i32) -> *mut TableFuncScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndTableFuncScan(_node: &mut TableFuncScanState) {
    unimplemented!()
}
pub fn ExecReScanTableFuncScan(_node: &mut TableFuncScanState) {
    unimplemented!()
}
