//! Translated from PostgreSQL src/include/executor/nodeRecursiveunion.h

use crate::nodes::execnodes::{EState, RecursiveUnionState};
use crate::nodes::plannodes::RecursiveUnion;

pub fn ExecInitRecursiveUnion(_node: &RecursiveUnion, _estate: &mut EState, _eflags: i32) -> *mut RecursiveUnionState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndRecursiveUnion(_node: &mut RecursiveUnionState) {
    unimplemented!()
}
pub fn ExecReScanRecursiveUnion(_node: &mut RecursiveUnionState) {
    unimplemented!()
}
