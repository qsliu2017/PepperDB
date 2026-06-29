//! Node dispatch: init / run / end an executor plan-state tree. Translated from
//! backend/executor/execProcnode.c (disposition: grow).
//!
//! PG dispatches on a `NodeTag` and (for ExecProcNode) through a per-node
//! function pointer that downcasts `PlanState*` to the concrete `*State`. Rust
//! cannot downcast a shared base struct, so the concrete node states are held in
//! a `PlanStateNode` enum and the three dispatchers (`ExecInitNode`,
//! `ExecProcNode`, `ExecEndNode`) `match` on it. M2 adds the `T_SeqScan` and
//! `T_ModifyTable` arms; M5 (step 25A) adds the `T_Sort`/`T_Limit`/`T_Material`/
//! `T_Unique`/`T_Group` upper-plan arms (each drives a CHILD subplan). The
//! `T_Agg` arm (step 25B, separate agent) slots in alongside them. Every other
//! node kind is a clean `not_yet_reachable` arm that grows per milestone
//! (rules.md s4).
//!
//! Slot ownership (the step-08 carry-forward, settled here): a node OWNS its
//! result/scan slot and `ExecProcNode` returns a BORROW of it
//! (`Option<&mut TupleTableSlot>`), not a fresh clone -- PG's `ExecProcNode`
//! returns a `TupleTableSlot*` the node reuses each call. The caller consumes the
//! borrow before the next `ExecProcNode`. There is no per-tuple deep clone on the
//! hot path; the scan node deforms the heap tuple into its own slot and Var eval
//! reads it through `econtext->ecxt_scantuple` (see execScan).

use crate::access::tupdesc::TupleDesc;
use crate::nodes::execnodes::{EState, ResultState, TupleTableSlot};
use crate::nodes::nodes::Node;

use crate::backend::executor::nodeGroup::{exec_end_group, exec_group, exec_init_group, GroupRun};
use crate::backend::executor::nodeLimit::{exec_end_limit, exec_init_limit, exec_limit, LimitRun};
use crate::backend::executor::nodeMaterial::{
    exec_end_material, exec_init_material, exec_material, MaterialRun,
};
use crate::backend::executor::nodeModifyTable::{
    exec_end_modify_table, exec_init_modify_table, exec_modify_table, ModifyTableRun,
};
use crate::backend::executor::nodeResult::{exec_end_result, exec_init_result, exec_result};
use crate::backend::executor::nodeSeqscan::{
    exec_end_seq_scan, exec_init_seq_scan, exec_seq_scan, SeqScanRun,
};
use crate::backend::executor::nodeSort::{exec_end_sort, exec_init_sort, exec_sort, SortRun};
use crate::backend::executor::nodeUnique::{
    exec_end_unique, exec_init_unique, exec_unique, UniqueRun,
};
use crate::shared_state::SharedState;
use std::sync::Arc;

/// The executor plan-state tree. Replaces PG's `PlanState*` + per-node
/// `ExecProcNode` function pointer (which relied on downcasting). One variant per
/// node kind; M1/M2 live `Result`, `SeqScan`, `ModifyTable`; M5 adds the
/// `Sort`/`Limit`/`Material`/`Unique`/`Group` upper plans (each owns a CHILD
/// `PlanStateNode`). The `T_Agg` arm slots in here next (step 25B).
///
/// `SeqScan`/`ModifyTable`/the M5 upper nodes use small wrapper run-states
/// (`SeqScanRun`/`ModifyTableRun`/`SortRun`/...) that pair the PG node state with
/// the AM scan handle / child plan-state -- state the C node struct holds by
/// pointer but the Rust node struct (in `nodes/execnodes.rs`, outside this island)
/// has no field for.
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
    /// T_SortState (+ child + tuplesort).
    Sort(Box<SortRun<'rel>>),
    /// T_LimitState (+ child).
    Limit(Box<LimitRun<'rel>>),
    /// T_MaterialState (+ child + tuplestore).
    Material(Box<MaterialRun<'rel>>),
    /// T_UniqueState (+ child).
    Unique(Box<UniqueRun<'rel>>),
    /// T_GroupState (+ child).
    Group(Box<GroupRun<'rel>>),
    // NOTE: the T_Agg arm (step 25B, separate agent) is added here. It pairs an
    // AggState with its child; do not collapse this enum or reorder the arms.
    /// Test-only: an in-memory list of pre-built tuples served one per
    /// ExecProcNode. Lets the upper-node unit tests feed a deterministic child
    /// without a SeqScan/initdb dependency (the planner wires real children at
    /// step 26). Carries a result descriptor so `result_type_of` works.
    #[cfg(test)]
    TupleSource(Box<TupleSource>),
}

/// Test-only synthetic tuple source backing `PlanStateNode::TupleSource`.
#[cfg(test)]
pub struct TupleSource {
    desc: TupleDesc,
    rows: std::collections::VecDeque<Box<TupleTableSlot>>,
    current: Option<Box<TupleTableSlot>>,
}

#[cfg(test)]
impl PlanStateNode<'_> {
    /// Build a test tuple-source child from a rowtype + pre-filled slots.
    pub fn test_tuple_source(desc: TupleDesc, rows: Vec<Box<TupleTableSlot>>) -> Self {
        PlanStateNode::TupleSource(Box::new(TupleSource {
            desc,
            rows: rows.into(),
            current: None,
        }))
    }
}

/// The result TupleDesc of a plan-state node (PG `ExecGetResultType`). Used by the
/// upper nodes (Sort/Limit/...) to size their slots from the child's rowtype and
/// by `standard_executor_start` to publish the root result descriptor.
#[must_use]
pub fn result_type_of(node: &PlanStateNode<'_>) -> Option<TupleDesc> {
    match node {
        PlanStateNode::Result(rs) => rs.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::SeqScan(ss) => ss.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::ModifyTable(mt) => mt.state.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Sort(s) => s.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Limit(l) => l.state.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Material(m) => m.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Unique(u) => u.state.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Group(g) => g.state.ss.ps.ps_result_tuple_desc.clone(),
        #[cfg(test)]
        PlanStateNode::TupleSource(t) => Some(t.desc.clone()),
    }
}

/// PG `ExecInitNode`: build the plan-state subtree for `node`. The nodeTag switch
/// lives the `T_Result`/`T_SeqScan`/`T_ModifyTable` arms + the M5 upper-plan arms;
/// other tags grow per milestone. Upper nodes init their `lefttree` child first,
/// then build the node over it.
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
        Node::Sort(s) => {
            // We shield the child from REWIND/BACKWARD/MARK (the sort materializes).
            let child = init_child(s.plan.lefttree.as_ref(), estate, child_eflags(eflags));
            Some(PlanStateNode::Sort(exec_init_sort(s, estate, eflags, child)))
        }
        Node::Limit(l) => {
            let child = init_child(l.plan.lefttree.as_ref(), estate, eflags);
            Some(PlanStateNode::Limit(exec_init_limit(l, estate, child)))
        }
        Node::Material(m) => {
            let child = init_child(m.plan.lefttree.as_ref(), estate, child_eflags(eflags));
            Some(PlanStateNode::Material(exec_init_material(m, estate, eflags, child)))
        }
        Node::Unique(u) => {
            let child = init_child(u.plan.lefttree.as_ref(), estate, eflags);
            Some(PlanStateNode::Unique(exec_init_unique(u, estate, child)))
        }
        Node::Group(g) => {
            let child = init_child(g.plan.lefttree.as_ref(), estate, eflags);
            Some(PlanStateNode::Group(exec_init_group(g, estate, child)))
        }
        other => unimplemented!("ExecInitNode: {other:?} not yet translated for this milestone"),
    }
}

/// Initialize a required child subplan (the upper nodes always have a lefttree).
fn init_child<'rel>(child: Option<&Node>, estate: &mut EState<'rel>, eflags: i32) -> PlanStateNode<'rel> {
    exec_init_node(child, estate, eflags)
        .unwrap_or_else(|| unimplemented!("ExecInitNode: upper node without a child subplan"))
}

/// Sort/Material shield the child from REWIND/BACKWARD/MARK (they materialize).
fn child_eflags(eflags: i32) -> i32 {
    eflags
        & !(crate::utils::tuplestore::EXEC_FLAG_REWIND
            | crate::utils::tuplestore::EXEC_FLAG_BACKWARD
            | crate::utils::tuplestore::EXEC_FLAG_MARK)
}

/// PG `ExecProcNode`: pull the next tuple from a node, returning a BORROW of the
/// node-owned result/scan slot (the C `ExecProcNodeMtd` returns the reused
/// `TupleTableSlot*`). `None` is `TupIsNull` (end of data). Async because the
/// scan path reaches the table AM's buffer reads, and the upper nodes drive their
/// (async) children (rules.md s5).
///
/// `shared` is `Option` so the childless-const path (the M1 `SELECT 1` wire path)
/// drives the executor without an `Arc<SharedState>` (it reaches no I/O leaf); the
/// scan/insert arms require it (`expect`). The upper nodes pass `shared` straight
/// through to their child.
pub async fn exec_proc_node<'n>(
    shared: Option<&Arc<SharedState>>,
    node: &'n mut PlanStateNode<'_>,
) -> Option<&'n mut TupleTableSlot> {
    match node {
        PlanStateNode::Result(rs) => exec_result(rs),
        PlanStateNode::SeqScan(ss) => exec_seq_scan(expect_shared(shared), ss).await,
        PlanStateNode::ModifyTable(mt) => exec_modify_table(expect_shared(shared), mt).await,
        PlanStateNode::Sort(s) => exec_sort(shared, s).await,
        PlanStateNode::Limit(l) => exec_limit(shared, l).await,
        PlanStateNode::Material(m) => exec_material(shared, m).await,
        PlanStateNode::Unique(u) => exec_unique(shared, u).await,
        PlanStateNode::Group(g) => exec_group(shared, g).await,
        #[cfg(test)]
        PlanStateNode::TupleSource(t) => {
            t.current = t.rows.pop_front();
            t.current.as_deref_mut()
        }
    }
}

/// The SharedState a storage-touching node needs; absent only on the const path.
fn expect_shared(shared: Option<&Arc<SharedState>>) -> &Arc<SharedState> {
    shared.unwrap_or_else(|| {
        unimplemented!("ExecProcNode: a scan/modify node requires a SharedState (the const path has none)")
    })
}

/// PG `ExecEndNode`: recursively tear down a node subtree. `shared` is `Option`
/// (the const path needs none); scan/modify teardown require it; the upper nodes
/// pass it through to the child.
pub fn exec_end_node(shared: Option<&Arc<SharedState>>, node: &mut PlanStateNode<'_>) {
    match node {
        PlanStateNode::Result(rs) => exec_end_result(rs),
        PlanStateNode::SeqScan(ss) => exec_end_seq_scan(expect_shared(shared), ss),
        PlanStateNode::ModifyTable(mt) => exec_end_modify_table(expect_shared(shared), mt),
        PlanStateNode::Sort(s) => exec_end_sort(shared, s),
        PlanStateNode::Limit(l) => exec_end_limit(shared, l),
        PlanStateNode::Material(m) => exec_end_material(shared, m),
        PlanStateNode::Unique(u) => exec_end_unique(shared, u),
        PlanStateNode::Group(g) => exec_end_group(shared, g),
        #[cfg(test)]
        PlanStateNode::TupleSource(_) => {}
    }
}
