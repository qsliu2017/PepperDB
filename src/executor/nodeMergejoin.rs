//! Translated from PostgreSQL src/include/executor/nodeMergejoin.h

use crate::nodes::execnodes::{EState, MergeJoinState};
use crate::nodes::plannodes::MergeJoin;

pub fn ExecInitMergeJoin(_node: &MergeJoin, _estate: &mut EState<'_>, _eflags: i32) -> *mut MergeJoinState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndMergeJoin(_node: &mut MergeJoinState) {
    unimplemented!()
}
pub fn ExecReScanMergeJoin(_node: &mut MergeJoinState) {
    unimplemented!()
}
