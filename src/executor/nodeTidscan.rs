//! Translated from PostgreSQL src/include/executor/nodeTidscan.h

use crate::nodes::execnodes::{EState, TidScanState};
use crate::nodes::plannodes::TidScan;

pub fn ExecInitTidScan(_node: &TidScan, _estate: &mut EState<'_>, _eflags: i32) -> *mut TidScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndTidScan(_node: &mut TidScanState) {
    unimplemented!()
}
pub fn ExecReScanTidScan(_node: &mut TidScanState) {
    unimplemented!()
}
