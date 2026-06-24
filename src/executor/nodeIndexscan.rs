//! Translated from PostgreSQL src/include/executor/nodeIndexscan.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{EState, ExprContext, IndexScanState, PlanState};
use crate::nodes::plannodes::IndexScan;

// TODO(ptr)
pub fn ExecInitIndexScan(
    _node: &IndexScan,
    _estate: &mut EState,
    _eflags: i32,
) -> *mut IndexScanState {
    unimplemented!()
}

pub fn ExecEndIndexScan(_node: &mut IndexScanState) {
    unimplemented!()
}

pub fn ExecIndexMarkPos(_node: &mut IndexScanState) {
    unimplemented!()
}

pub fn ExecIndexRestrPos(_node: &mut IndexScanState) {
    unimplemented!()
}

pub fn ExecReScanIndexScan(_node: &mut IndexScanState) {
    unimplemented!()
}

pub fn ExecIndexScanEstimate(_node: &mut IndexScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecIndexScanInitializeDSM(_node: &mut IndexScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecIndexScanReInitializeDSM(_node: &mut IndexScanState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecIndexScanInitializeWorker(
    _node: &mut IndexScanState,
    _pwcxt: &mut ParallelWorkerContext,
) {
    unimplemented!()
}

pub fn ExecIndexScanRetrieveInstrumentation(_node: &mut IndexScanState) {
    unimplemented!()
}

// Exported to share code with nodeIndexonlyscan.c and nodeBitmapIndexscan.c.
// Out-params (scanKeys, numScanKeys, runtimeKeys, numRuntimeKeys, arrayKeys,
// numArrayKeys) folded into a returned struct.
pub struct IndexScanKeys {
    pub scan_keys: Vec<ScanKey>,
    pub runtime_keys: Vec<IndexRuntimeKeyInfo>,
    pub array_keys: Vec<IndexArrayKeyInfo>,
}

// TODO(struct-forward): repoint to crate::access::genam / crate::executor::execnodes in Phase 2
#[deprecated(note = "TODO(struct-forward): repoint to crate::access::genam in Phase 2")]
pub type ScanKey = usize;
// TODO(struct-forward): repoint to crate::nodes::execnodes in Phase 2
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::execnodes in Phase 2")]
#[derive(Debug, Clone)]
pub struct IndexRuntimeKeyInfo;
// TODO(struct-forward): repoint to crate::nodes::execnodes in Phase 2
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::execnodes in Phase 2")]
#[derive(Debug, Clone)]
pub struct IndexArrayKeyInfo;

// TODO(struct-forward): repoint Relation / List to real types in Phase 2
#[allow(deprecated)]
pub fn ExecIndexBuildScanKeys(
    _planstate: &mut PlanState,
    _index: usize,
    _quals: &[usize],
    _isorderby: bool,
) -> IndexScanKeys {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecIndexEvalRuntimeKeys(
    _econtext: &mut ExprContext,
    _runtime_keys: &mut [IndexRuntimeKeyInfo],
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecIndexEvalArrayKeys(
    _econtext: &mut ExprContext,
    _array_keys: &mut [IndexArrayKeyInfo],
) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ExecIndexAdvanceArrayKeys(_array_keys: &mut [IndexArrayKeyInfo]) -> bool {
    unimplemented!()
}
