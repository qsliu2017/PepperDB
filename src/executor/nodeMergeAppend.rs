//! Translated from PostgreSQL src/include/executor/nodeMergeAppend.h

use crate::nodes::execnodes::{EState, MergeAppendState};
use crate::nodes::plannodes::MergeAppend;

pub fn ExecInitMergeAppend(_node: &MergeAppend, _estate: &mut EState, _eflags: i32) -> *mut MergeAppendState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndMergeAppend(_node: &mut MergeAppendState) {
    unimplemented!()
}
pub fn ExecReScanMergeAppend(_node: &mut MergeAppendState) {
    unimplemented!()
}
