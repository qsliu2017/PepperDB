//! Translated from PostgreSQL src/include/executor/nodeBitmapAnd.h

use crate::nodes::execnodes::{BitmapAndState, EState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::BitmapAnd;

// TODO(ptr)
pub fn ExecInitBitmapAnd(
    _node: &BitmapAnd,
    _estate: &mut EState<'_>,
    _eflags: i32,
) -> *mut BitmapAndState {
    unimplemented!()
}

// MultiExec returns a Node* (a TIDBitmap, tagged via NodeTag).
// TODO(ptr)
pub fn MultiExecBitmapAnd(_node: &mut BitmapAndState) -> *mut Node {
    unimplemented!()
}

pub fn ExecEndBitmapAnd(_node: &mut BitmapAndState) {
    unimplemented!()
}

pub fn ExecReScanBitmapAnd(_node: &mut BitmapAndState) {
    unimplemented!()
}
