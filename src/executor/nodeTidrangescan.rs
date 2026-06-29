//! Translated from PostgreSQL src/include/executor/nodeTidrangescan.h

use crate::nodes::execnodes::{EState, TidRangeScanState};
use crate::nodes::plannodes::TidRangeScan;

pub fn ExecInitTidRangeScan(_node: &TidRangeScan, _estate: &mut EState<'_>, _eflags: i32) -> *mut TidRangeScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndTidRangeScan(_node: &mut TidRangeScanState) {
    unimplemented!()
}
pub fn ExecReScanTidRangeScan(_node: &mut TidRangeScanState) {
    unimplemented!()
}
