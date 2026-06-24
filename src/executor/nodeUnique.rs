//! Translated from PostgreSQL src/include/executor/nodeUnique.h

use crate::nodes::execnodes::{EState, UniqueState};
use crate::nodes::plannodes::Unique;

pub fn ExecInitUnique(_node: &Unique, _estate: &mut EState, _eflags: i32) -> *mut UniqueState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndUnique(_node: &mut UniqueState) {
    unimplemented!()
}
pub fn ExecReScanUnique(_node: &mut UniqueState) {
    unimplemented!()
}
