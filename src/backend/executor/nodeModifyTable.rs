//! ModifyTable node executor. Translated from
//! backend/executor/nodeModifyTable.c (disposition: grow -- M2 lands INSERT; the
//! UPDATE/DELETE/MERGE, ON CONFLICT, RETURNING, partition-routing, and trigger
//! arms are grow guards, rules.md s4).
//!
//! `ExecInitModifyTable` builds the ModifyTableState over the source subplan and
//! the result relation(s) (set up by InitPlan). `ExecModifyTable` pulls each row
//! from the subplan and `ExecInsert` stores it through the table AM. A
//! non-RETURNING INSERT yields no tuples to the destination; the row count lives
//! in `es_processed`.
//!
//! Async coloring: `ExecInsert` reaches the heap insert (WAL), so
//! `ExecModifyTable` is `async` (rules.md s5). The caller must be inside a WAL
//! insertion scope (`with_insertion`), as `heap_insert` requires.


use std::sync::Arc;

use crate::backend::access::common::heaptuple::heap_form_tuple;
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::executor::execProcnode::{
    exec_end_node, exec_init_node, exec_proc_node, PlanStateNode,
};
use crate::nodes::execnodes::{EState, ModifyTableState, PlanState};
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::plannodes::ModifyTable;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// Run-state pairing the PG `ModifyTableState` with its child (source) plan-state.
/// The C node holds the subplan-state through `ps.lefttree` (a `PlanState*`); the
/// Rust `PlanState.lefttree` is typed `Option<Box<PlanState>>` (the base struct,
/// not the `PlanStateNode` dispatch enum), so the executable child is kept here.
///
/// Borrow-based ownership (relation-ownership-plan step 5): the result relation is
/// BORROWED from the `EState` range-table (`&'rel RelationData`), whose owner is the
/// command frame's `Arc` -- no node-owned `Arc`.
pub struct ModifyTableRun<'rel> {
    pub state: Box<ModifyTableState>,
    /// the source-plan state yielding rows to insert.
    pub subplan: Box<PlanStateNode<'rel>>,
    /// the target relation borrowed from `EState.es_range_table_rels` (PG keeps it
    /// in `resultRelInfo->ri_RelationDesc`).
    pub result_relation: &'rel RelationData,
    /// rows inserted by `ExecModifyTable` (PG bumps `es_processed` directly; here
    /// the EState lives on the QueryDesc, so the driver reads this after the run).
    pub processed: u64,
}

/// PG `ExecInitModifyTable` (M2: INSERT). Builds the ModifyTableState, validates
/// it is an INSERT with a single source subplan, and initializes that subplan. The
/// per-result-rel slot/projection/trigger/WCO setup grows at later milestones.
pub fn exec_init_modify_table<'rel>(node: &ModifyTable, estate: &mut EState<'rel>, eflags: i32) -> Box<ModifyTableRun<'rel>> {
    crate::assert!(
        node.operation == CmdType::INSERT,
        "ExecInitModifyTable: only INSERT is reachable in M2"
    );
    crate::assert!(node.returning_lists.is_empty(), "ExecInitModifyTable: RETURNING not yet reachable");
    crate::assert!(
        node.on_conflict_action == crate::nodes::nodes::OnConflictAction::NONE,
        "ExecInitModifyTable: ON CONFLICT not yet reachable"
    );
    crate::assert!(
        node.merge_action_lists.is_empty(),
        "ExecInitModifyTable: MERGE not yet reachable"
    );
    crate::assert!(node.result_relations.len() == 1, "ExecInitModifyTable: M2 has one result relation");

    let subplan_node = node
        .plan
        .lefttree
        .as_ref()
        .unwrap_or_else(|| unimplemented!("ExecInitModifyTable: INSERT without a source subplan"));
    let subplan = exec_init_node(Some(subplan_node), estate, eflags)
        .unwrap_or_else(|| unimplemented!("ExecInitModifyTable: null source subplan"));

    // ExecInitResultRelation: the (single) target relation, BORROWED from the
    // EState range-table slots (PG's `es_relations`, indexed by RTI) -- the command
    // frame opened it into its owning `Arc` and published the borrow on the EState.
    let rti = node.result_relations[0];
    crate::assert!(rti > 0);
    let result_relation = estate
        .es_range_table_rels
        .get((rti - 1) as usize)
        .copied()
        .flatten()
        .unwrap_or_else(|| unimplemented!("ExecInitModifyTable: result relation not registered for RTI"));

    let ps = PlanState {
        plan: Some(Node::ModifyTable(Box::new(node.clone()))),
        ..PlanState::default()
    };
    let mt = ModifyTableState {
        ps,
        operation: Some(node.operation),
        can_set_tag: node.can_set_tag,
        mt_done: false,
        mt_nrels: 1,
        ..ModifyTableState::default()
    };

    Box::new(ModifyTableRun {
        state: Box::new(mt),
        subplan: Box::new(subplan),
        result_relation,
        processed: 0,
    })
}

/// PG `ExecModifyTable` (M2: INSERT). Pull each row from the source subplan and
/// `ExecInsert` it through the table AM, counting inserts in `es_processed`. A
/// non-RETURNING INSERT returns no tuples (None), so the driving ExecutePlan loop
/// ends after this one drive.
pub async fn exec_modify_table<'r>(
    shared: &Arc<SharedState>,
    run: &'r mut ModifyTableRun<'_>,
) -> Option<&'r mut crate::nodes::execnodes::TupleTableSlot> {
    if run.state.mt_done {
        return None;
    }

    let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
    let mut inserted: u64 = 0;

    loop {
        // Pull the next source row (a borrow of the subplan's node-owned slot).
        // Box::pin the recursive async dispatch (ExecProcNode -> ... -> here).
        let Some(slot) = Box::pin(exec_proc_node(Some(shared), &mut run.subplan)).await else {
            break;
        };

        // ExecInsert: form a heap tuple from the (virtual) source slot and store
        // it through the table AM. The TID is patched into the tuple by heap_insert.
        exec_insert(shared, run.result_relation, slot, cid).await;
        inserted += 1;
    }

    run.state.mt_done = true;
    run.processed = inserted;
    // M2 ModifyTable returns no tuples to the destination; the row count is read
    // off `run.processed` by ExecutePlan (which owns the EState) and folded into
    // es_processed.
    None
}

/// PG `ExecInsert` (M2 subset): store the row in `slot` into `relation` through
/// the table AM. The executor-slot insert path (`table_tuple_insert` ->
/// `ExecFetchSlotHeapTuple`) is staged, so M2 forms the heap tuple here from the
/// virtual slot's value/null arrays and calls `heap_insert` directly (the complete
/// M2 storage path). BEFORE-ROW/INSTEAD triggers, partition routing, index
/// insert, WCO/constraint checks, and RETURNING grow at later milestones.
async fn exec_insert(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    slot: &crate::nodes::execnodes::TupleTableSlot,
    cid: crate::c::CommandId,
) {
    // ExecGetInsertNewTuple is the identity here (no per-rel new-tuple projection
    // in M2: the source tlist already matches the target rowtype, ordered by
    // attno via transformInsertStmt). Form the heap tuple from the slot.
    let desc = relation_tupdesc(relation);
    let natts = desc.natts as usize;
    crate::assert!(slot.values.len() >= natts && slot.isnull.len() >= natts);
    let mut tuple = heap_form_tuple(&desc, &slot.values[..natts], &slot.isnull[..natts]);

    // table_tuple_insert -> heap_insert. heap_insert stamps xmin/cmin, places the
    // tuple, emits WAL, and patches t_self.
    heap_insert(shared, relation, &mut tuple, cid, 0).await;

    // heap_freetuple: the in-memory tuple body is reclaimed (the on-page copy is
    // what persists). tuple drops here; free its body.
    crate::backend::access::common::heaptuple::heap_freetuple(tuple);
}

/// PG `ExecEndModifyTable`: tear down the subplan; result relations are
/// caller-owned (M2), closed by the EState teardown.
pub fn exec_end_modify_table(shared: &Arc<SharedState>, run: &mut ModifyTableRun<'_>) {
    exec_end_node(Some(shared), &mut run.subplan);
}

/// The rowtype descriptor of a relation (`RelationGetDescr`).
fn relation_tupdesc(relation: &RelationData) -> crate::access::tupdesc::TupleDesc {
    relation.rd_att
        .clone()
        .unwrap_or_else(|| unimplemented!("relation_tupdesc: relation has no rowtype descriptor"))
}
