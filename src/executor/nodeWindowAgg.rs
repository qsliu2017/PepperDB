//! Translated from PostgreSQL src/include/executor/nodeWindowAgg.h

use crate::nodes::execnodes::{EState, WindowAggState};
use crate::nodes::plannodes::WindowAgg;

pub fn ExecInitWindowAgg(_node: &WindowAgg, _estate: &mut EState, _eflags: i32) -> *mut WindowAggState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndWindowAgg(_node: &mut WindowAggState) {
    unimplemented!()
}
pub fn ExecReScanWindowAgg(_node: &mut WindowAggState) {
    unimplemented!()
}
