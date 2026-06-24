//! Translated from PostgreSQL src/include/executor/nodeHashjoin.h

use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::execnodes::{EState, HashJoinState, MinimalTuple};
use crate::nodes::plannodes::HashJoin;
use crate::storage::buffile::BufFile;

// HashJoinTable is defined in executor/hashjoin.h (not in this batch).
// TODO(struct-forward): repoint to crate::executor::hashjoin::HashJoinTable in Phase 2
#[deprecated(note = "TODO(struct-forward): repoint to crate::executor::hashjoin in Phase 2")]
pub type HashJoinTable = usize;

// TODO(ptr)
pub fn ExecInitHashJoin(_node: &HashJoin, _estate: &mut EState, _eflags: i32) -> *mut HashJoinState {
    unimplemented!()
}

pub fn ExecEndHashJoin(_node: &mut HashJoinState) {
    unimplemented!()
}

pub fn ExecReScanHashJoin(_node: &mut HashJoinState) {
    unimplemented!()
}

pub fn ExecShutdownHashJoin(_node: &mut HashJoinState) {
    unimplemented!()
}

pub fn ExecHashJoinEstimate(_state: &mut HashJoinState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecHashJoinInitializeDSM(_state: &mut HashJoinState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecHashJoinReInitializeDSM(_state: &mut HashJoinState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecHashJoinInitializeWorker(_state: &mut HashJoinState, _pwcxt: &mut ParallelWorkerContext) {
    unimplemented!()
}

// C `BufFile **fileptr` is an in/out param (lazily allocates the file).
#[allow(deprecated)]
pub fn ExecHashJoinSaveTuple(
    _tuple: MinimalTuple,
    _hashvalue: u32,
    _fileptr: &mut Option<Box<BufFile>>,
    _hashtable: HashJoinTable,
) {
    unimplemented!()
}
