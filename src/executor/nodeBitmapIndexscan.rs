//! Translated from PostgreSQL src/include/executor/nodeBitmapIndexscan.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{BitmapIndexScanState, EState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::BitmapIndexScan;

// TODO(ptr)
pub fn ExecInitBitmapIndexScan(
    _node: &BitmapIndexScan,
    _estate: &mut EState,
    _eflags: i32,
) -> *mut BitmapIndexScanState {
    unimplemented!()
}

// MultiExec returns a Node* (a TIDBitmap, tagged via NodeTag).
// TODO(ptr)
pub fn MultiExecBitmapIndexScan(_node: &mut BitmapIndexScanState) -> *mut Node {
    unimplemented!()
}

pub fn ExecEndBitmapIndexScan(_node: &mut BitmapIndexScanState) {
    unimplemented!()
}

pub fn ExecReScanBitmapIndexScan(_node: &mut BitmapIndexScanState) {
    unimplemented!()
}

pub fn ExecBitmapIndexScanEstimate(_node: &mut BitmapIndexScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecBitmapIndexScanInitializeDSM(
    _node: &mut BitmapIndexScanState,
    _pcxt: &mut ParallelContext,
) {
    unimplemented!()
}

pub fn ExecBitmapIndexScanInitializeWorker(
    _node: &mut BitmapIndexScanState,
    _pwcxt: &mut ParallelWorkerContext,
) {
    unimplemented!()
}

pub fn ExecBitmapIndexScanRetrieveInstrumentation(_node: &mut BitmapIndexScanState) {
    unimplemented!()
}
