//! Executor access-method glue. Translated from
//! backend/executor/execAmi.c (disposition: grow).
//!
//! `ExecReScan` resets a node subtree's scan position; M2 wires the `T_SeqScan`
//! arm (re-open the heap scan) and the trivial `T_Result`/`T_ModifyTable` arms.
//! `ExecMarkPos`/`ExecRestrPos` (mark/restore) and the materialize/sort/append/...
//! arms grow at later milestones (rules.md s4).

use std::sync::Arc;

use crate::backend::executor::execProcnode::PlanStateNode;
use crate::backend::executor::nodeSeqscan::exec_rescan_seq_scan;
use crate::shared_state::SharedState;

/// PG `ExecReScan`: reset a node subtree so the next `ExecProcNode` re-reads from
/// the start. M2: a SeqScan drops + re-opens its heap scan; a childless Result
/// re-arms its one-row flag on the next call (handled by ExecReScanResult, a
/// grow guard); a ModifyTable rescan is not reachable (it runs to completion).
pub fn exec_rescan(shared: &Arc<SharedState>, node: &mut PlanStateNode) {
    match node {
        PlanStateNode::SeqScan(ss) => exec_rescan_seq_scan(shared, ss),
        PlanStateNode::Result(_) => {
            unimplemented!("ExecReScan: ExecReScanResult not yet reachable")
        }
        PlanStateNode::ModifyTable(_) => {
            unimplemented!("ExecReScan: ModifyTable rescan not reachable")
        }
    }
}
