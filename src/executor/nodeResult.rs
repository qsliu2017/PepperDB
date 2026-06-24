//! Translated from PostgreSQL src/include/executor/nodeResult.h

use crate::nodes::execnodes::{EState, ResultState};
use crate::nodes::plannodes::Result;

pub fn ExecInitResult(_node: &Result, _estate: &mut EState, _eflags: i32) -> *mut ResultState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndResult(_node: &mut ResultState) {
    unimplemented!()
}
pub fn ExecResultMarkPos(_node: &mut ResultState) {
    unimplemented!()
}
pub fn ExecResultRestrPos(_node: &mut ResultState) {
    unimplemented!()
}
pub fn ExecReScanResult(_node: &mut ResultState) {
    unimplemented!()
}
