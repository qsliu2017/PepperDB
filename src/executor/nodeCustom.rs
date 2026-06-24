//! Translated from PostgreSQL src/include/executor/nodeCustom.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{CustomScanState, EState};
use crate::nodes::plannodes::CustomScan;

// TODO(ptr)
pub fn ExecInitCustomScan(
    _cscan: &CustomScan,
    _estate: &mut EState,
    _eflags: i32,
) -> *mut CustomScanState {
    unimplemented!()
}

pub fn ExecEndCustomScan(_node: &mut CustomScanState) {
    unimplemented!()
}

pub fn ExecReScanCustomScan(_node: &mut CustomScanState) {
    unimplemented!()
}

pub fn ExecCustomMarkPos(_node: &mut CustomScanState) {
    unimplemented!()
}

pub fn ExecCustomRestrPos(_node: &mut CustomScanState) {
    unimplemented!()
}

pub fn ExecCustomScanEstimate(_node: &mut CustomScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecCustomScanInitializeDSM(_node: &mut CustomScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecCustomScanReInitializeDSM(_node: &mut CustomScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecCustomScanInitializeWorker(
    _node: &mut CustomScanState,
    _pwcxt: &mut ParallelWorkerContext,
) {
    unimplemented!()
}

pub fn ExecShutdownCustomScan(_node: &mut CustomScanState) {
    unimplemented!()
}
