//! Translated from PostgreSQL src/include/executor/nodeModifyTable.h

use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::{EState, ModifyTableState, ResultRelInfo};
use crate::nodes::nodes::CmdType;
use crate::nodes::plannodes::ModifyTable;

pub fn ExecInitGenerated(_result_rel_info: &mut ResultRelInfo, _estate: &mut EState, _cmdtype: CmdType) {
    unimplemented!()
}
pub fn ExecComputeStoredGenerated(
    _result_rel_info: &mut ResultRelInfo,
    _estate: &mut EState,
    _slot: &mut TupleTableSlot,
    _cmdtype: CmdType,
) {
    unimplemented!()
}
pub fn ExecInitModifyTable(_node: &ModifyTable, _estate: &mut EState, _eflags: i32) -> *mut ModifyTableState {
    unimplemented!() // TODO(ptr)
}
pub fn ExecEndModifyTable(_node: &mut ModifyTableState) {
    unimplemented!()
}
pub fn ExecReScanModifyTable(_node: &mut ModifyTableState) {
    unimplemented!()
}
pub fn ExecInitMergeTupleSlots(_mtstate: &mut ModifyTableState, _result_rel_info: &mut ResultRelInfo) {
    unimplemented!()
}
