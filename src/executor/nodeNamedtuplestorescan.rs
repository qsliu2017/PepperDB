//! Translated from PostgreSQL src/include/executor/nodeNamedtuplestorescan.h

use crate::nodes::execnodes::{EState, NamedTuplestoreScanState};
use crate::nodes::plannodes::NamedTuplestoreScan;

pub fn ExecInitNamedTuplestoreScan(_node: &NamedTuplestoreScan, _estate: &mut EState<'_>, _eflags: i32) -> *mut NamedTuplestoreScanState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecReScanNamedTuplestoreScan(_node: &mut NamedTuplestoreScanState) {
    unimplemented!()
}
