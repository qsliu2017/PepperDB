//! Translated from PostgreSQL src/include/executor/nodeSeqscan.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{EState, SeqScanState};
use crate::nodes::plannodes::SeqScan;

pub fn ExecInitSeqScan(_node: &SeqScan, _estate: &mut EState, _eflags: i32) -> *mut SeqScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndSeqScan(_node: &mut SeqScanState) {
    unimplemented!()
}
pub fn ExecReScanSeqScan(_node: &mut SeqScanState) {
    unimplemented!()
}

// parallel scan support
pub fn ExecSeqScanEstimate(_node: &mut SeqScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecSeqScanInitializeDSM(_node: &mut SeqScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecSeqScanReInitializeDSM(_node: &mut SeqScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecSeqScanInitializeWorker(_node: &mut SeqScanState, _pwcxt: &mut ParallelWorkerContext) {
    unimplemented!()
}
