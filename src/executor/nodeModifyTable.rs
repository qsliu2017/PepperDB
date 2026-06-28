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
/// PG `ExecInitModifyTable`. Returns the owned `Box<ModifyTableRun>` (the C
/// `ModifyTableState*` plus its child plan-state and target relation).
pub use crate::backend::executor::nodeModifyTable::exec_init_modify_table as ExecInitModifyTable;
/// PG `ExecEndModifyTable`.
pub use crate::backend::executor::nodeModifyTable::exec_end_modify_table as ExecEndModifyTable;
/// PG `ExecModifyTable`: the `ExecProcNodeMtd` that runs the modification.
pub use crate::backend::executor::nodeModifyTable::exec_modify_table as ExecModifyTable;
pub fn ExecReScanModifyTable(_node: &mut ModifyTableState) {
    unimplemented!()
}
pub fn ExecInitMergeTupleSlots(_mtstate: &mut ModifyTableState, _result_rel_info: &mut ResultRelInfo) {
    unimplemented!()
}
