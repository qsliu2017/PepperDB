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

use crate::backend::executor::nodeAgg::{exec_agg, exec_end_agg, exec_init_agg, AggRun};
use crate::backend::executor::nodeAppend::{exec_append, exec_end_append, exec_init_append, AppendRun};
use crate::backend::executor::nodeCtescan::{
    exec_cte_scan, exec_end_cte_scan, exec_init_cte_scan, CteScanRun,
};
use crate::backend::executor::nodeValuesscan::{
    exec_end_values_scan, exec_init_values_scan, exec_values_scan,
};
use crate::backend::executor::nodeRecursiveunion::{
    exec_end_recursive_union, exec_init_recursive_union, exec_recursive_union, make_worktable_ref,
    RecursiveUnionRun,
};
use crate::backend::executor::nodeSetOp::{exec_end_setop, exec_init_setop, exec_setop, SetOpRun};
use crate::backend::executor::nodeWorktablescan::{
    exec_end_work_table_scan, exec_init_work_table_scan, exec_rescan_work_table_scan,
    exec_work_table_scan, WorkTableScanRun,
};
use crate::backend::executor::nodeBitmapAnd::{
    exec_end_bitmap_and, exec_init_bitmap_and, multi_exec_bitmap_and, BitmapAndRun,
};
use crate::backend::executor::nodeBitmapHeapscan::{
    exec_bitmap_heap_scan, exec_end_bitmap_heap_scan, exec_init_bitmap_heap_scan,
    BitmapHeapScanRun,
};
use crate::backend::executor::nodeBitmapIndexscan::{
    exec_end_bitmap_index_scan, exec_init_bitmap_index_scan, multi_exec_bitmap_index_scan,
    BitmapIndexScanRun,
};
use crate::backend::executor::nodeBitmapOr::{
    exec_end_bitmap_or, exec_init_bitmap_or, multi_exec_bitmap_or, BitmapOrRun,
};
use crate::backend::executor::nodeGroup::{exec_end_group, exec_group, exec_init_group, GroupRun};
use crate::backend::executor::nodeIndexonlyscan::{
    exec_end_index_only_scan, exec_index_only_scan, exec_init_index_only_scan, IndexOnlyScanRun,
};
use crate::backend::executor::nodeIndexscan::{
    exec_end_index_scan, exec_index_scan, exec_init_index_scan, IndexScanRun,
};
use crate::backend::executor::nodeLimit::{exec_end_limit, exec_init_limit, exec_limit, LimitRun};
use crate::backend::executor::nodeLockRows::{
    exec_end_lock_rows, exec_init_lock_rows, exec_lock_rows, LockRowsRun,
};
use crate::backend::executor::nodeMaterial::{
    exec_end_material, exec_init_material, exec_material, MaterialRun,
};
use crate::backend::executor::nodeTidscan::{
    exec_end_tid_scan, exec_init_tid_scan, exec_tid_scan, TidScanRun,
};
use crate::backend::executor::nodeHash::{exec_end_hash, exec_init_hash, HashRun};
use crate::backend::executor::nodeHashjoin::{
    exec_end_hash_join, exec_hash_join, exec_init_hash_join, HashJoinRun,
};
use crate::backend::executor::nodeMergejoin::{
    exec_end_merge_join, exec_init_merge_join, exec_merge_join, MergeJoinRun,
};
use crate::backend::executor::nodeModifyTable::{
    exec_end_modify_table, exec_init_modify_table, exec_modify_table, ModifyTableRun,
};
use crate::backend::executor::nodeNestloop::{
    exec_end_nest_loop, exec_init_nest_loop, exec_nest_loop, NestLoopRun,
};
use crate::backend::executor::nodeResult::{exec_end_result, exec_init_result, exec_result};
use crate::backend::executor::nodeSeqscan::{
    exec_end_seq_scan, exec_init_seq_scan, exec_seq_scan, SeqScanRun,
};
use crate::backend::executor::nodeSort::{exec_end_sort, exec_init_sort, exec_sort, SortRun};
use crate::backend::executor::nodeUnique::{
    exec_end_unique, exec_init_unique, exec_unique, UniqueRun,
};
use crate::backend::executor::nodeWindowAgg::{
    exec_end_window_agg, exec_init_window_agg, exec_window_agg, WindowAggRun,
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
    /// T_IndexScanState (+ its open AM index-scan descriptor borrowing from EState).
    IndexScan(Box<IndexScanRun<'rel>>),
    /// T_IndexOnlyScanState (+ its open AM index-scan descriptor).
    IndexOnlyScan(Box<IndexOnlyScanRun<'rel>>),
    /// T_BitmapHeapScanState (+ its bitmap-producer child + iteration cursor).
    BitmapHeapScan(Box<BitmapHeapScanRun<'rel>>),
    /// T_BitmapIndexScanState. A bitmap PRODUCER: driven via `multi_exec_proc_node`
    /// (yields a `TIDBitmap`, not a slot); its `ExecProcNode` arm is an error.
    BitmapIndexScan(Box<BitmapIndexScanRun<'rel>>),
    /// T_BitmapAndState. A bitmap producer (intersects child bitmaps).
    BitmapAnd(Box<BitmapAndRun<'rel>>),
    /// T_BitmapOrState. A bitmap producer (unions child bitmaps).
    BitmapOr(Box<BitmapOrRun<'rel>>),
    /// T_ModifyTableState (+ its child plan-state).
    ModifyTable(Box<ModifyTableRun<'rel>>),
    /// T_LockRowsState (+ its child plan-state + the resolved row marks).
    LockRows(Box<LockRowsRun<'rel>>),
    /// T_TidScanState (+ the borrowed relation/snapshot + the TID list).
    TidScan(Box<TidScanRun<'rel>>),
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
    /// T_AggState (+ child + resolved per-aggregate metadata). PLAIN/SORTED/HASHED.
    Agg(Box<AggRun<'rel>>),
    /// T_WindowAggState (+ child + per-window-function metadata + partition spool).
    WindowAgg(Box<WindowAggRun<'rel>>),
    /// T_AppendState (+ ordered subplan states). UNION ALL concatenation.
    Append(Box<AppendRun<'rel>>),
    /// T_SetOpState (+ left/right children). INTERSECT/EXCEPT [ALL].
    SetOp(Box<SetOpRun<'rel>>),
    /// T_ValuesScanState (+ per-row compiled expression lists). Owns no borrow.
    ValuesScan(Box<crate::backend::executor::nodeValuesscan::ValuesScanRun>),
    /// T_CteScanState (+ the CTE subplan). Materializes the CTE once.
    CteScan(Box<CteScanRun<'rel>>),
    /// T_RecursiveUnionState (+ non-recursive/recursive terms + working table).
    RecursiveUnion(Box<RecursiveUnionRun<'rel>>),
    /// T_WorkTableScanState (+ shared working table). The recursive term's scan.
    WorkTableScan(Box<WorkTableScanRun>),
    /// T_NestLoopState (+ outer/inner children, joinqual, projection).
    NestLoop(Box<NestLoopRun<'rel>>),
    /// T_HashJoinState (+ outer child + Hash inner child, hashclauses, projection).
    HashJoin(Box<HashJoinRun<'rel>>),
    /// T_HashState. The inner build side of a HashJoin: driven by ExecHashJoin (not
    /// a tuple-returning ExecProcNode); its proc arm is an error.
    Hash(Box<HashRun<'rel>>),
    /// T_MergeJoinState (+ outer/inner children, mergeclauses, projection).
    MergeJoin(Box<MergeJoinRun<'rel>>),
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
        PlanStateNode::IndexScan(is) => is.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::IndexOnlyScan(ios) => ios.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::BitmapHeapScan(bhs) => bhs.ss.ps.ps_result_tuple_desc.clone(),
        // The bitmap producers yield a TIDBitmap, not tuples; no result rowtype.
        PlanStateNode::BitmapIndexScan(_)
        | PlanStateNode::BitmapAnd(_)
        | PlanStateNode::BitmapOr(_) => None,
        PlanStateNode::ModifyTable(mt) => mt.state.ps.ps_result_tuple_desc.clone(),
        // LockRows projects its child unchanged -> the child's rowtype.
        PlanStateNode::LockRows(l) => result_type_of(&l.subplan),
        PlanStateNode::TidScan(t) => t.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Sort(s) => s.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Limit(l) => l.state.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Material(m) => m.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Unique(u) => u.state.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Group(g) => g.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Agg(a) => a.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::WindowAgg(w) => w.state.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::Append(a) => a.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::SetOp(s) => s.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::ValuesScan(v) => v.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::CteScan(c) => c.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::RecursiveUnion(r) => r.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::WorkTableScan(w) => w.ss.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::NestLoop(n) => n.state.js.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::HashJoin(h) => h.state.js.ps.ps_result_tuple_desc.clone(),
        PlanStateNode::MergeJoin(m) => m.state.js.ps.ps_result_tuple_desc.clone(),
        // The Hash node is a build side; it yields no tuples, so no result rowtype.
        PlanStateNode::Hash(_) => None,
        #[cfg(test)]
        PlanStateNode::TupleSource(t) => Some(t.desc.clone()),
    }
}

/// PG `ExecInitNode`: build the plan-state subtree for `node`. The nodeTag switch
/// lives the `T_Result`/`T_SeqScan`/`T_ModifyTable` arms + the M5 upper-plan arms;
/// other tags grow per milestone. Upper nodes init their `lefttree` child first,
/// then build the node over it.
#[allow(
    clippy::too_many_lines,
    reason = "1:1 PG ExecInitNode nodeTag dispatch; one arm per node kind, grows per milestone"
)]
pub fn exec_init_node<'rel>(
    node: Option<&Node>,
    estate: &mut EState<'rel>,
    eflags: i32,
) -> Option<PlanStateNode<'rel>> {
    let node = node?;
    match node {
        Node::Result(r) => Some(PlanStateNode::Result(exec_init_result(r, estate, eflags))),
        Node::SeqScan(s) => Some(PlanStateNode::SeqScan(exec_init_seq_scan(s, estate, eflags))),
        Node::IndexScan(s) => Some(PlanStateNode::IndexScan(exec_init_index_scan(s, estate, eflags))),
        Node::IndexOnlyScan(s) => Some(PlanStateNode::IndexOnlyScan(exec_init_index_only_scan(
            s, estate, eflags,
        ))),
        Node::BitmapHeapScan(s) => {
            // The bitmap-producer child is the scan node's lefttree (PG `outerPlan`).
            let child = init_child(s.scan.plan.lefttree.as_ref(), estate, eflags);
            Some(PlanStateNode::BitmapHeapScan(exec_init_bitmap_heap_scan(
                s, estate, eflags, child,
            )))
        }
        Node::BitmapIndexScan(s) => Some(PlanStateNode::BitmapIndexScan(
            exec_init_bitmap_index_scan(s, estate, eflags),
        )),
        Node::BitmapAnd(a) => {
            let children = init_bitmap_children(&a.bitmapplans, estate, eflags);
            Some(PlanStateNode::BitmapAnd(exec_init_bitmap_and(a, children)))
        }
        Node::BitmapOr(o) => {
            let children = init_bitmap_children(&o.bitmapplans, estate, eflags);
            Some(PlanStateNode::BitmapOr(exec_init_bitmap_or(o, children)))
        }
        Node::ModifyTable(m) => Some(PlanStateNode::ModifyTable(exec_init_modify_table(
            m, estate, eflags,
        ))),
        Node::LockRows(l) => Some(PlanStateNode::LockRows(exec_init_lock_rows(l, estate, eflags))),
        Node::TidScan(t) => Some(PlanStateNode::TidScan(exec_init_tid_scan(t, estate, eflags))),
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
        Node::Agg(a) => {
            // The agg materializes its input each call; the child needs no
            // REWIND/BACKWARD/MARK (PG passes eflags through, dropping those).
            let child = init_child(a.plan.lefttree.as_ref(), estate, child_eflags(eflags));
            Some(PlanStateNode::Agg(exec_init_agg(a, estate, child)))
        }
        Node::WindowAgg(w) => {
            // The window agg spools each partition; the child (a Sort) materializes,
            // so it needs no REWIND/BACKWARD/MARK.
            let child = init_child(w.plan.lefttree.as_ref(), estate, child_eflags(eflags));
            Some(PlanStateNode::WindowAgg(exec_init_window_agg(w, estate, child)))
        }
        Node::NestLoop(n) => {
            // Two children: outer=lefttree, inner=righttree (PG outer/innerPlan).
            // No nestloop params (M7) -> the inner is materialized here, so neither
            // child needs special eflags.
            let outer = init_child(n.join.plan.lefttree.as_ref(), estate, eflags);
            let inner = init_child(n.join.plan.righttree.as_ref(), estate, eflags);
            Some(PlanStateNode::NestLoop(exec_init_nest_loop(n, estate, eflags, outer, inner)))
        }
        Node::HashJoin(h) => {
            // outer=lefttree; inner=righttree is a Hash node (the build side).
            let outer = init_child(h.join.plan.lefttree.as_ref(), estate, eflags);
            let inner = init_child(h.join.plan.righttree.as_ref(), estate, eflags);
            Some(PlanStateNode::HashJoin(exec_init_hash_join(h, estate, eflags, outer, inner)))
        }
        Node::Hash(h) => {
            let child = init_child(h.plan.lefttree.as_ref(), estate, eflags);
            Some(PlanStateNode::Hash(exec_init_hash(h, estate, child)))
        }
        Node::MergeJoin(m) => {
            let outer = init_child(m.join.plan.lefttree.as_ref(), estate, eflags);
            let inner = init_child(m.join.plan.righttree.as_ref(), estate, eflags);
            Some(PlanStateNode::MergeJoin(exec_init_merge_join(m, estate, eflags, outer, inner)))
        }
        Node::Append(a) => {
            // The branch subplans live in `appendplans`; init each in order.
            let children: Vec<PlanStateNode<'rel>> = a
                .appendplans
                .iter()
                .map(|p| init_child(Some(p), estate, eflags))
                .collect();
            Some(PlanStateNode::Append(exec_init_append(a, estate, children)))
        }
        Node::SetOp(s) => {
            // PG 18.4: two inputs via the plan's left/right tree (outer=left).
            let left = init_child(s.plan.lefttree.as_ref(), estate, eflags);
            let right = init_child(s.plan.righttree.as_ref(), estate, eflags);
            Some(PlanStateNode::SetOp(exec_init_setop(s, estate, left, right)))
        }
        Node::ValuesScan(v) => {
            Some(PlanStateNode::ValuesScan(exec_init_values_scan(v, estate)))
        }
        Node::CteScan(c) => {
            // The CTE subplan is embedded as the CteScan's lefttree (the port has no
            // es_subplanstates registry yet; see nodeCtescan.rs).
            let cteplan = init_child(c.scan.plan.lefttree.as_ref(), estate, child_eflags(eflags));
            Some(PlanStateNode::CteScan(exec_init_cte_scan(c, estate, cteplan)))
        }
        Node::RecursiveUnion(r) => {
            // Init the non-recursive term first to learn the output rowtype; register
            // the shared working table (keyed by wt_param) BEFORE the recursive term
            // is initialized, so its WorkTableScan can pick the handle up.
            let left = init_child(r.plan.lefttree.as_ref(), estate, child_eflags(eflags));
            let desc = result_type_of(&left)
                .unwrap_or_else(|| unimplemented!("ExecInitNode: RecursiveUnion non-recursive term has no rowtype"));
            let wt = make_worktable_ref();
            estate
                .worktables
                .push((r.wt_param, std::sync::Arc::clone(&wt), desc));
            let right = init_child(r.plan.righttree.as_ref(), estate, child_eflags(eflags));
            // The handle stays registered for the node's lifetime (rescans re-read it).
            Some(PlanStateNode::RecursiveUnion(exec_init_recursive_union(
                r, estate, left, right, wt,
            )))
        }
        Node::WorkTableScan(w) => {
            // Pick up the shared working table + rowtype the enclosing RecursiveUnion
            // registered on the EState.
            let Some((_, handle, desc)) =
                estate.worktables.iter().find(|(p, _, _)| *p == w.wt_param)
            else {
                unimplemented!("ExecInitNode: WorkTableScan without a registered working table")
            };
            let wt = std::sync::Arc::clone(handle);
            let desc = desc.clone();
            Some(PlanStateNode::WorkTableScan(exec_init_work_table_scan(w, &desc, estate, wt)))
        }
        other => unimplemented!("ExecInitNode: {other:?} not yet translated for this milestone"),
    }
}

/// Initialize a required child subplan (the upper nodes always have a lefttree).
fn init_child<'rel>(child: Option<&Node>, estate: &mut EState<'rel>, eflags: i32) -> PlanStateNode<'rel> {
    exec_init_node(child, estate, eflags)
        .unwrap_or_else(|| unimplemented!("ExecInitNode: upper node without a child subplan"))
}

/// Init the bitmap subplans of a BitmapAnd/BitmapOr (each a bitmap producer).
fn init_bitmap_children<'rel>(
    plans: &[Node],
    estate: &mut EState<'rel>,
    eflags: i32,
) -> Vec<PlanStateNode<'rel>> {
    plans
        .iter()
        .map(|p| {
            exec_init_node(Some(p), estate, eflags)
                .unwrap_or_else(|| unimplemented!("ExecInitNode: empty bitmap subplan"))
        })
        .collect()
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
        PlanStateNode::IndexScan(is) => exec_index_scan(expect_shared(shared), is).await,
        PlanStateNode::IndexOnlyScan(ios) => {
            exec_index_only_scan(expect_shared(shared), ios).await
        }
        PlanStateNode::BitmapHeapScan(bhs) => {
            exec_bitmap_heap_scan(expect_shared(shared), bhs).await
        }
        // The bitmap producers do not return tuples; they go through MultiExecProcNode.
        PlanStateNode::BitmapIndexScan(_)
        | PlanStateNode::BitmapAnd(_)
        | PlanStateNode::BitmapOr(_) => {
            unimplemented!("ExecProcNode: a bitmap producer is driven via MultiExecProcNode")
        }
        PlanStateNode::ModifyTable(mt) => exec_modify_table(expect_shared(shared), mt).await,
        PlanStateNode::LockRows(l) => exec_lock_rows(shared, l).await,
        PlanStateNode::TidScan(t) => exec_tid_scan(expect_shared(shared), t).await,
        PlanStateNode::Sort(s) => exec_sort(shared, s).await,
        PlanStateNode::Limit(l) => exec_limit(shared, l).await,
        PlanStateNode::Material(m) => exec_material(shared, m).await,
        PlanStateNode::Unique(u) => exec_unique(shared, u).await,
        PlanStateNode::Group(g) => exec_group(shared, g).await,
        PlanStateNode::Agg(a) => exec_agg(shared, a).await,
        PlanStateNode::WindowAgg(w) => exec_window_agg(shared, w).await,
        PlanStateNode::Append(a) => Box::pin(exec_append(shared, a)).await,
        PlanStateNode::SetOp(s) => Box::pin(exec_setop(shared, s)).await,
        PlanStateNode::ValuesScan(v) => exec_values_scan(v).await,
        PlanStateNode::CteScan(c) => Box::pin(exec_cte_scan(shared, c)).await,
        PlanStateNode::RecursiveUnion(r) => Box::pin(exec_recursive_union(shared, r)).await,
        PlanStateNode::WorkTableScan(w) => Box::pin(exec_work_table_scan(w)).await,
        PlanStateNode::NestLoop(n) => exec_nest_loop(shared, n).await,
        PlanStateNode::HashJoin(h) => exec_hash_join(shared, h).await,
        PlanStateNode::MergeJoin(m) => exec_merge_join(shared, m).await,
        // The Hash node is the HashJoin's build side; it is driven internally by
        // ExecHashJoin (MultiExecHash), not pulled as a tuple source.
        PlanStateNode::Hash(_) => {
            unimplemented!("ExecProcNode: a Hash node is driven by its HashJoin parent")
        }
        #[cfg(test)]
        PlanStateNode::TupleSource(t) => {
            t.current = t.rows.pop_front();
            t.current.as_deref_mut()
        }
    }
}

/// PG `MultiExecProcNode`: run a bitmap-producer node to completion and return its
/// `TIDBitmap` (the alternate return path PG uses for nodes whose `ExecProcNode` is
/// an error). `result` is an optional pre-made accumulator a parent stashed for a
/// BitmapIndexScan child to OR directly into (the BitmapOr fast path); BitmapAnd /
/// BitmapOr build their own accumulator and ignore it. Async (the index descent +
/// child MultiExecs reach buffer reads).
pub async fn multi_exec_proc_node(
    shared: &Arc<SharedState>,
    node: &mut PlanStateNode<'_>,
    result: Option<Box<crate::backend::nodes::tidbitmap::TIDBitmap>>,
) -> Box<crate::backend::nodes::tidbitmap::TIDBitmap> {
    match node {
        PlanStateNode::BitmapIndexScan(bis) => {
            multi_exec_bitmap_index_scan(shared, bis, result).await
        }
        PlanStateNode::BitmapAnd(ba) => Box::pin(multi_exec_bitmap_and(shared, ba)).await,
        PlanStateNode::BitmapOr(bo) => Box::pin(multi_exec_bitmap_or(shared, bo)).await,
        _ => unimplemented!("MultiExecProcNode: node is not a bitmap producer"),
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
        PlanStateNode::IndexScan(is) => exec_end_index_scan(expect_shared(shared), is),
        PlanStateNode::IndexOnlyScan(ios) => exec_end_index_only_scan(expect_shared(shared), ios),
        PlanStateNode::BitmapHeapScan(bhs) => exec_end_bitmap_heap_scan(shared, bhs),
        PlanStateNode::BitmapIndexScan(bis) => {
            exec_end_bitmap_index_scan(expect_shared(shared), bis);
        }
        PlanStateNode::BitmapAnd(ba) => exec_end_bitmap_and(shared, ba),
        PlanStateNode::BitmapOr(bo) => exec_end_bitmap_or(shared, bo),
        PlanStateNode::ModifyTable(mt) => exec_end_modify_table(expect_shared(shared), mt),
        PlanStateNode::LockRows(l) => exec_end_lock_rows(shared, l),
        PlanStateNode::TidScan(t) => exec_end_tid_scan(expect_shared(shared), t),
        PlanStateNode::Sort(s) => exec_end_sort(shared, s),
        PlanStateNode::Limit(l) => exec_end_limit(shared, l),
        PlanStateNode::Material(m) => exec_end_material(shared, m),
        PlanStateNode::Unique(u) => exec_end_unique(shared, u),
        PlanStateNode::Group(g) => exec_end_group(shared, g),
        PlanStateNode::Agg(a) => exec_end_agg(shared, a),
        PlanStateNode::WindowAgg(w) => exec_end_window_agg(shared, w),
        PlanStateNode::Append(a) => exec_end_append(shared, a),
        PlanStateNode::SetOp(s) => exec_end_setop(shared, s),
        PlanStateNode::ValuesScan(v) => exec_end_values_scan(v),
        PlanStateNode::CteScan(c) => exec_end_cte_scan(shared, c),
        PlanStateNode::RecursiveUnion(r) => exec_end_recursive_union(shared, r),
        PlanStateNode::WorkTableScan(w) => exec_end_work_table_scan(w),
        PlanStateNode::NestLoop(n) => exec_end_nest_loop(shared, n),
        PlanStateNode::HashJoin(h) => exec_end_hash_join(shared, h),
        PlanStateNode::MergeJoin(m) => exec_end_merge_join(shared, m),
        PlanStateNode::Hash(h) => exec_end_hash(shared, h),
        #[cfg(test)]
        PlanStateNode::TupleSource(_) => {}
    }
}

/// PG `ExecReScan` (the subset the recursive-CTE term needs): reset a node subtree
/// so the next `ExecProcNode` re-reads from the start. RecursiveUnion calls this on
/// its recursive term after swapping the working table. The recursive term M12
/// reaches is a `Result` (projection + qual over the working table) whose input is
/// the WorkTableScan, threaded via the Result run-state's child; recurse into the
/// handled wrappers down to the WorkTableScan, which reloads the swapped table.
pub fn exec_rescan_node(node: &mut PlanStateNode<'_>) {
    match node {
        PlanStateNode::WorkTableScan(w) => exec_rescan_work_table_scan(w),
        PlanStateNode::Append(a) => {
            for sub in &mut a.subplans {
                exec_rescan_node(sub);
            }
            a.which = 0;
        }
        other => {
            let _ = other;
            unimplemented!("ExecReScan: node kind not supported in a recursive term yet");
        }
    }
}
