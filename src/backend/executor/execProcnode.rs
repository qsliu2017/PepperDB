//! Node dispatch: init / run / end an executor plan-state tree. Translated from
//! backend/executor/execProcnode.c (disposition: grow).
//!
//! PG dispatches on a `NodeTag` and (for ExecProcNode) through a per-node
//! function pointer that downcasts `PlanState*` to the concrete `*State`. Rust
//! cannot downcast a shared base struct, so the concrete node states are held in
//! a `PlanStateNode` enum and the three dispatchers (`ExecInitNode`,
//! `ExecProcNode`, `ExecEndNode`) `match` on it. M2 adds the `T_SeqScan` and
//! `T_ModifyTable` arms; every other node kind is a clean `not_yet_reachable`
//! arm that grows per milestone (rules.md s4).
//!
//! Slot ownership (the step-08 carry-forward, settled here): a node OWNS its
//! result/scan slot and `ExecProcNode` returns a BORROW of it
//! (`Option<&mut TupleTableSlot>`), not a fresh clone -- PG's `ExecProcNode`
//! returns a `TupleTableSlot*` the node reuses each call. The caller consumes the
//! borrow before the next `ExecProcNode`. There is no per-tuple deep clone on the
//! hot path; the scan node deforms the heap tuple into its own slot and Var eval
//! reads it through `econtext->ecxt_scantuple` (see execScan).

use crate::nodes::execnodes::{EState, ResultState, TupleTableSlot};
use crate::nodes::nodes::Node;

use crate::backend::executor::nodeModifyTable::{
    exec_end_modify_table, exec_init_modify_table, exec_modify_table, ModifyTableRun,
};
use crate::backend::executor::nodeResult::{exec_end_result, exec_init_result, exec_result};
use crate::backend::executor::nodeSeqscan::{
    exec_end_seq_scan, exec_init_seq_scan, exec_seq_scan, SeqScanRun,
};
use crate::shared_state::SharedState;
use std::sync::Arc;

/// The executor plan-state tree. Replaces PG's `PlanState*` + per-node
/// `ExecProcNode` function pointer (which relied on downcasting). One variant per
/// node kind; M1/M2 live `Result`, `SeqScan`, `ModifyTable`, the rest grow.
///
/// `SeqScan`/`ModifyTable` use small wrapper run-states (`SeqScanRun`/
/// `ModifyTableRun`) that pair the PG node state with the AM scan handle / child
/// plan-state -- state the C node struct holds by pointer but the Rust node
/// struct (in `nodes/execnodes.rs`, outside this island) has no field for.
///
/// `'rel` is the lifetime of the open range-table relations borrowed from the
/// command frame (relation-ownership-plan §1.3): the scan/modify nodes hold
/// `&'rel RelationData` (and the snapshot borrow shares it), never an owned `Arc`.
pub enum PlanStateNode<'rel> {
    /// T_ResultState.
    Result(Box<ResultState>),
    /// T_SeqScanState (+ its open heap scan descriptor borrowing from EState).
    SeqScan(Box<SeqScanRun<'rel>>),
    /// T_ModifyTableState (+ its child plan-state).
    ModifyTable(Box<ModifyTableRun<'rel>>),
}

/// PG `ExecInitNode`: build the plan-state subtree for `node`. The nodeTag switch
/// lives the `T_Result`/`T_SeqScan`/`T_ModifyTable` arms; other tags grow per
/// milestone.
pub fn exec_init_node<'rel>(
    node: Option<&Node>,
    estate: &mut EState<'rel>,
    eflags: i32,
) -> Option<PlanStateNode<'rel>> {
    let node = node?;
    match node {
        Node::Result(r) => Some(PlanStateNode::Result(exec_init_result(r, estate, eflags))),
        Node::SeqScan(s) => Some(PlanStateNode::SeqScan(exec_init_seq_scan(s, estate, eflags))),
        Node::ModifyTable(m) => Some(PlanStateNode::ModifyTable(exec_init_modify_table(
            m, estate, eflags,
        ))),
        other => unimplemented!("ExecInitNode: {other:?} not yet translated for this milestone"),
    }
}

/// PG `ExecProcNode`: pull the next tuple from a node, returning a BORROW of the
/// node-owned result/scan slot (the C `ExecProcNodeMtd` returns the reused
/// `TupleTableSlot*`). `None` is `TupIsNull` (end of data). Async because the
/// scan path reaches the table AM's buffer reads (rules.md s5).
///
/// `shared` is `Option` so the childless-const path (the M1 `SELECT 1` wire path)
/// drives the executor without an `Arc<SharedState>` (it reaches no I/O leaf); the
/// scan/insert arms require it (`expect`). The full wire wiring that always
/// supplies a SharedState is step 18B.
pub async fn exec_proc_node<'n>(
    shared: Option<&Arc<SharedState>>,
    node: &'n mut PlanStateNode<'_>,
) -> Option<&'n mut TupleTableSlot> {
    match node {
        PlanStateNode::Result(rs) => exec_result(rs),
        PlanStateNode::SeqScan(ss) => exec_seq_scan(expect_shared(shared), ss).await,
        PlanStateNode::ModifyTable(mt) => exec_modify_table(expect_shared(shared), mt).await,
    }
}

/// The SharedState a storage-touching node needs; absent only on the const path.
fn expect_shared(shared: Option<&Arc<SharedState>>) -> &Arc<SharedState> {
    shared.unwrap_or_else(|| {
        unimplemented!("ExecProcNode: a scan/modify node requires a SharedState (the const path has none)")
    })
}

/// PG `ExecEndNode`: recursively tear down a node subtree. `shared` is `Option`
/// (the const path needs none); scan/modify teardown require it.
pub fn exec_end_node(shared: Option<&Arc<SharedState>>, node: &mut PlanStateNode<'_>) {
    match node {
        PlanStateNode::Result(rs) => exec_end_result(rs),
        PlanStateNode::SeqScan(ss) => exec_end_seq_scan(expect_shared(shared), ss),
        PlanStateNode::ModifyTable(mt) => exec_end_modify_table(expect_shared(shared), mt),
    }
}
