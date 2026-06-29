//! Translated from PostgreSQL src/include/executor/nodeMemoize.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{EState, MemoizeState};
use crate::nodes::plannodes::Memoize;

pub fn ExecInitMemoize(_node: &Memoize, _estate: &mut EState<'_>, _eflags: i32) -> *mut MemoizeState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndMemoize(_node: &mut MemoizeState) {
    unimplemented!()
}
pub fn ExecReScanMemoize(_node: &mut MemoizeState) {
    unimplemented!()
}
pub fn ExecEstimateCacheEntryOverheadBytes(_ntuples: f64) -> f64 {
    unimplemented!()
}

// parallel instrumentation support
pub fn ExecMemoizeEstimate(_node: &mut MemoizeState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecMemoizeInitializeDSM(_node: &mut MemoizeState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecMemoizeInitializeWorker(_node: &mut MemoizeState, _pwcxt: &mut ParallelWorkerContext) {
    unimplemented!()
}
pub fn ExecMemoizeRetrieveInstrumentation(_node: &mut MemoizeState) {
    unimplemented!()
}
