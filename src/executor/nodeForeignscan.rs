//! Translated from PostgreSQL src/include/executor/nodeForeignscan.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{AsyncRequest, EState, ForeignScanState};
use crate::nodes::plannodes::ForeignScan;

// TODO(ptr)
pub fn ExecInitForeignScan(
    _node: &ForeignScan,
    _estate: &mut EState<'_>,
    _eflags: i32,
) -> *mut ForeignScanState {
    unimplemented!()
}

pub fn ExecEndForeignScan(_node: &mut ForeignScanState) {
    unimplemented!()
}

pub fn ExecReScanForeignScan(_node: &mut ForeignScanState) {
    unimplemented!()
}

pub fn ExecForeignScanEstimate(_node: &mut ForeignScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecForeignScanInitializeDSM(_node: &mut ForeignScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecForeignScanReInitializeDSM(_node: &mut ForeignScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecForeignScanInitializeWorker(
    _node: &mut ForeignScanState,
    _pwcxt: &mut ParallelWorkerContext,
) {
    unimplemented!()
}

pub fn ExecShutdownForeignScan(_node: &mut ForeignScanState) {
    unimplemented!()
}

pub fn ExecAsyncForeignScanRequest(_areq: &mut AsyncRequest) {
    unimplemented!()
}

pub fn ExecAsyncForeignScanConfigureWait(_areq: &mut AsyncRequest) {
    unimplemented!()
}

pub fn ExecAsyncForeignScanNotify(_areq: &mut AsyncRequest) {
    unimplemented!()
}
