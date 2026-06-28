//! Translated from PostgreSQL src/include/executor/nodeResult.h

use crate::nodes::execnodes::ResultState;

/// PG `ExecInitResult`. Returns an owned `Box<ResultState>` (the C `ResultState*`
/// is owned by the plan-state enum in this port).
pub use crate::backend::executor::nodeResult::exec_init_result as ExecInitResult;
pub use crate::backend::executor::nodeResult::exec_end_result as ExecEndResult;
pub fn ExecResultMarkPos(_node: &mut ResultState) {
    unimplemented!()
}
pub fn ExecResultRestrPos(_node: &mut ResultState) {
    unimplemented!()
}
pub fn ExecReScanResult(_node: &mut ResultState) {
    unimplemented!()
}
