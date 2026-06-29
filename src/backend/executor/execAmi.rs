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
        // M5 upper nodes: rescan resets the node (and forgets/rewinds buffered
        // output); the child rescan is driven by the node's own rescan helper.
        PlanStateNode::Sort(s) => crate::backend::executor::nodeSort::exec_rescan_sort(s),
        PlanStateNode::Limit(l) => crate::backend::executor::nodeLimit::exec_rescan_limit(l),
        PlanStateNode::Material(m) => crate::backend::executor::nodeMaterial::exec_rescan_material(m),
        PlanStateNode::Unique(u) => crate::backend::executor::nodeUnique::exec_rescan_unique(u),
        PlanStateNode::Group(g) => crate::backend::executor::nodeGroup::exec_rescan_group(g),
        PlanStateNode::Agg(a) => crate::backend::executor::nodeAgg::exec_rescan_agg(a),
        #[cfg(test)]
        PlanStateNode::TupleSource(_) => {
            unimplemented!("ExecReScan: test tuple source is not rescannable")
        }
    }
}
