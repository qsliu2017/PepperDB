//! Translated from PostgreSQL src/include/executor/nodeIndexonlyscan.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{EState, IndexOnlyScanState};
use crate::nodes::plannodes::IndexOnlyScan;

// TODO(ptr)
pub fn ExecInitIndexOnlyScan(
    _node: &IndexOnlyScan,
    _estate: &mut EState,
    _eflags: i32,
) -> *mut IndexOnlyScanState {
    unimplemented!()
}

pub fn ExecEndIndexOnlyScan(_node: &mut IndexOnlyScanState) {
    unimplemented!()
}

pub fn ExecIndexOnlyMarkPos(_node: &mut IndexOnlyScanState) {
    unimplemented!()
}

pub fn ExecIndexOnlyRestrPos(_node: &mut IndexOnlyScanState) {
    unimplemented!()
}

pub fn ExecReScanIndexOnlyScan(_node: &mut IndexOnlyScanState) {
    unimplemented!()
}

pub fn ExecIndexOnlyScanEstimate(_node: &mut IndexOnlyScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecIndexOnlyScanInitializeDSM(_node: &mut IndexOnlyScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecIndexOnlyScanReInitializeDSM(
    _node: &mut IndexOnlyScanState,
    _pcxt: &mut ParallelContext,
) {
    unimplemented!()
}

pub fn ExecIndexOnlyScanInitializeWorker(
    _node: &mut IndexOnlyScanState,
    _pwcxt: &mut ParallelWorkerContext,
) {
    unimplemented!()
}

pub fn ExecIndexOnlyScanRetrieveInstrumentation(_node: &mut IndexOnlyScanState) {
    unimplemented!()
}
