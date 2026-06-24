//! Translated from PostgreSQL src/include/executor/nodeLockRows.h

use crate::nodes::execnodes::{EState, LockRowsState};
use crate::nodes::plannodes::LockRows;

// TODO(ptr)
pub fn ExecInitLockRows(
    _node: &LockRows,
    _estate: &mut EState,
    _eflags: i32,
) -> *mut LockRowsState {
    unimplemented!()
}

pub fn ExecEndLockRows(_node: &mut LockRowsState) {
    unimplemented!()
}

pub fn ExecReScanLockRows(_node: &mut LockRowsState) {
    unimplemented!()
}
