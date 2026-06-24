//! Translated from PostgreSQL src/include/executor/nodeSubqueryscan.h

use crate::nodes::execnodes::{EState, SubqueryScanState};
use crate::nodes::plannodes::SubqueryScan;

pub fn ExecInitSubqueryScan(_node: &SubqueryScan, _estate: &mut EState, _eflags: i32) -> *mut SubqueryScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndSubqueryScan(_node: &mut SubqueryScanState) {
    unimplemented!()
}
pub fn ExecReScanSubqueryScan(_node: &mut SubqueryScanState) {
    unimplemented!()
}
