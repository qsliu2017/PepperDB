//! Translated from PostgreSQL src/include/executor/nodeSort.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{EState, SortState};
use crate::nodes::plannodes::Sort;

pub fn ExecInitSort(_node: &Sort, _estate: &mut EState, _eflags: i32) -> *mut SortState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndSort(_node: &mut SortState) {
    unimplemented!()
}
pub fn ExecSortMarkPos(_node: &mut SortState) {
    unimplemented!()
}
pub fn ExecSortRestrPos(_node: &mut SortState) {
    unimplemented!()
}
pub fn ExecReScanSort(_node: &mut SortState) {
    unimplemented!()
}

// parallel instrumentation support
pub fn ExecSortEstimate(_node: &mut SortState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecSortInitializeDSM(_node: &mut SortState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}
pub fn ExecSortInitializeWorker(_node: &mut SortState, _pwcxt: &mut ParallelWorkerContext) {
    unimplemented!()
}
pub fn ExecSortRetrieveInstrumentation(_node: &mut SortState) {
    unimplemented!()
}
