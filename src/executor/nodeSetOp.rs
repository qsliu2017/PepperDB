//! Translated from PostgreSQL src/include/executor/nodeSetOp.h

use crate::nodes::execnodes::{EState, SetOpState};
use crate::nodes::plannodes::SetOp;

pub fn ExecInitSetOp(_node: &SetOp, _estate: &mut EState<'_>, _eflags: i32) -> *mut SetOpState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndSetOp(_node: &mut SetOpState) {
    unimplemented!()
}
pub fn ExecReScanSetOp(_node: &mut SetOpState) {
    unimplemented!()
}
