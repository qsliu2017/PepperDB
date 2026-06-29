//! Translated from PostgreSQL src/include/executor/nodeSamplescan.h

use crate::nodes::execnodes::{EState, SampleScanState};
use crate::nodes::plannodes::SampleScan;

pub fn ExecInitSampleScan(_node: &SampleScan, _estate: &mut EState<'_>, _eflags: i32) -> *mut SampleScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndSampleScan(_node: &mut SampleScanState) {
    unimplemented!()
}
pub fn ExecReScanSampleScan(_node: &mut SampleScanState) {
    unimplemented!()
}
