//! Translated from PostgreSQL src/include/executor/nodeIncrementalSort.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{EState, IncrementalSortState};
use crate::nodes::plannodes::IncrementalSort;

// TODO(ptr)
pub fn ExecInitIncrementalSort(
    _node: &IncrementalSort,
    _estate: &mut EState<'_>,
    _eflags: i32,
) -> *mut IncrementalSortState {
    unimplemented!()
}

pub fn ExecEndIncrementalSort(_node: &mut IncrementalSortState) {
    unimplemented!()
}

pub fn ExecReScanIncrementalSort(_node: &mut IncrementalSortState) {
    unimplemented!()
}

pub fn ExecIncrementalSortEstimate(_node: &mut IncrementalSortState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecIncrementalSortInitializeDSM(
    _node: &mut IncrementalSortState,
    _pcxt: &mut ParallelContext,
) {
    unimplemented!()
}

pub fn ExecIncrementalSortInitializeWorker(
    _node: &mut IncrementalSortState,
    _pwcxt: &mut ParallelWorkerContext,
) {
    unimplemented!()
}

pub fn ExecIncrementalSortRetrieveInstrumentation(_node: &mut IncrementalSortState) {
    unimplemented!()
}
