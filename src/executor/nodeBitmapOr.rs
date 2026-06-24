//! Translated from PostgreSQL src/include/executor/nodeBitmapOr.h

use crate::nodes::execnodes::{BitmapOrState, EState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::BitmapOr;

// TODO(ptr)
pub fn ExecInitBitmapOr(_node: &BitmapOr, _estate: &mut EState, _eflags: i32) -> *mut BitmapOrState {
    unimplemented!()
}

// MultiExec returns a Node* (a TIDBitmap, tagged via NodeTag).
// TODO(ptr)
pub fn MultiExecBitmapOr(_node: &mut BitmapOrState) -> *mut Node {
    unimplemented!()
}

pub fn ExecEndBitmapOr(_node: &mut BitmapOrState) {
    unimplemented!()
}

pub fn ExecReScanBitmapOr(_node: &mut BitmapOrState) {
    unimplemented!()
}
