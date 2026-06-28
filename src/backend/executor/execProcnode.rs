//! Node dispatch: init / run / end an executor plan-state tree. Translated from
//! backend/executor/execProcnode.c (disposition: grow).
//!
//! PG dispatches on a `NodeTag` and (for ExecProcNode) through a per-node
//! function pointer that downcasts `PlanState*` to the concrete `*State`. Rust
//! cannot downcast a shared base struct, so the concrete node states are held in
//! a `PlanStateNode` enum and the three dispatchers (`ExecInitNode`,
//! `ExecProcNode`, `ExecEndNode`) `match` on it. The `T_Result` arm is COMPLETE;
//! every other node kind is a clean `not_yet_reachable` arm that grows per
//! milestone (rules.md s4).

use crate::nodes::execnodes::{EState, ResultState, TupleTableSlot};
use crate::nodes::nodes::Node;

use crate::backend::executor::nodeResult::{exec_end_result, exec_init_result, exec_result};

/// The executor plan-state tree. Replaces PG's `PlanState*` + per-node
/// `ExecProcNode` function pointer (which relied on downcasting). One variant per
/// node kind; M1 lives `Result`, the rest grow.
pub enum PlanStateNode {
    /// T_ResultState.
    Result(Box<ResultState>),
}

/// PG `ExecInitNode`: build the plan-state subtree for `node`. The nodeTag switch
/// lives the `T_Result` arm; other tags grow per milestone.
pub fn exec_init_node(
    node: Option<&Node>,
    estate: &mut EState,
    eflags: i32,
) -> Option<PlanStateNode> {
    let node = node?;
    match node {
        Node::Result(r) => Some(PlanStateNode::Result(exec_init_result(r, estate, eflags))),
        other => unimplemented!("ExecInitNode: {other:?} not yet translated for this milestone"),
    }
}

/// PG `ExecProcNode`: pull the next tuple from a node. Dispatches on the concrete
/// node state (PG's `ExecProcNodeFirst`/function-pointer indirection collapses to
/// this match; instrumentation wrapping grows with EXPLAIN ANALYZE).
pub fn exec_proc_node(node: &mut PlanStateNode) -> Option<Box<TupleTableSlot>> {
    match node {
        PlanStateNode::Result(rs) => exec_result(rs),
    }
}

/// PG `ExecEndNode`: recursively tear down a node subtree.
pub fn exec_end_node(node: &mut PlanStateNode) {
    match node {
        PlanStateNode::Result(rs) => exec_end_result(rs),
    }
}
