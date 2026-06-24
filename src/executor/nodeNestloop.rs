//! Translated from PostgreSQL src/include/executor/nodeNestloop.h

use crate::nodes::execnodes::{EState, NestLoopState};
use crate::nodes::plannodes::NestLoop;

pub fn ExecInitNestLoop(_node: &NestLoop, _estate: &mut EState, _eflags: i32) -> *mut NestLoopState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndNestLoop(_node: &mut NestLoopState) {
    unimplemented!()
}
pub fn ExecReScanNestLoop(_node: &mut NestLoopState) {
    unimplemented!()
}
