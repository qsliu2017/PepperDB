//! Translated from PostgreSQL src/include/executor/nodeAppend.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{AppendState, AsyncRequest, EState};
use crate::nodes::plannodes::Append;

// TODO(ptr)
pub fn ExecInitAppend(_node: &Append, _estate: &mut EState, _eflags: i32) -> *mut AppendState {
    unimplemented!()
}

pub fn ExecEndAppend(_node: &mut AppendState) {
    unimplemented!()
}

pub fn ExecReScanAppend(_node: &mut AppendState) {
    unimplemented!()
}

pub fn ExecAppendEstimate(_node: &mut AppendState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecAppendInitializeDSM(_node: &mut AppendState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecAppendReInitializeDSM(_node: &mut AppendState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecAppendInitializeWorker(_node: &mut AppendState, _pwcxt: &mut ParallelWorkerContext) {
    unimplemented!()
}

pub fn ExecAsyncAppendResponse(_areq: &mut AsyncRequest) {
    unimplemented!()
}
