//! Translated from PostgreSQL src/include/executor/nodeBitmapHeapscan.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{BitmapHeapScanState, EState};
use crate::nodes::plannodes::BitmapHeapScan;

// TODO(ptr)
pub fn ExecInitBitmapHeapScan(
    _node: &BitmapHeapScan,
    _estate: &mut EState<'_>,
    _eflags: i32,
) -> *mut BitmapHeapScanState {
    unimplemented!()
}

pub fn ExecEndBitmapHeapScan(_node: &mut BitmapHeapScanState) {
    unimplemented!()
}

pub fn ExecReScanBitmapHeapScan(_node: &mut BitmapHeapScanState) {
    unimplemented!()
}

pub fn ExecBitmapHeapEstimate(_node: &mut BitmapHeapScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecBitmapHeapInitializeDSM(_node: &mut BitmapHeapScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecBitmapHeapReInitializeDSM(_node: &mut BitmapHeapScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecBitmapHeapInitializeWorker(
    _node: &mut BitmapHeapScanState,
    _pwcxt: &mut ParallelWorkerContext,
) {
    unimplemented!()
}

pub fn ExecBitmapHeapRetrieveInstrumentation(_node: &mut BitmapHeapScanState) {
    unimplemented!()
}
