//! Translated from PostgreSQL src/include/executor/nodeLimit.h

use crate::nodes::execnodes::{EState, LimitState};
use crate::nodes::plannodes::Limit;

// TODO(ptr)
pub fn ExecInitLimit(_node: &Limit, _estate: &mut EState<'_>, _eflags: i32) -> *mut LimitState {
    unimplemented!()
}

pub fn ExecEndLimit(_node: &mut LimitState) {
    unimplemented!()
}

pub fn ExecReScanLimit(_node: &mut LimitState) {
    unimplemented!()
}
