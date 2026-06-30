//! ModifyTable node executor. Translated from
//! backend/executor/nodeModifyTable.c (disposition: grow -- M2 lands INSERT; M8
//! (step 34) lands UPDATE/DELETE + RETURNING + the basic MERGE matched/not-matched
//! dispatch. ON CONFLICT, partition routing, cross-partition update, FDW, WCO, and
//! the heavy MERGE corners stay grow guards, rules.md s4).
//!
//! `ExecInitModifyTable` builds the ModifyTableState over the source subplan and the
//! result relation. `ExecModifyTable` pulls each row from the subplan and applies the
//! command through the table AM (heap_insert / heap_update / heap_delete via the
//! heapam_handler callbacks from step 33). RETURNING, when present, projects the
//! modified row and the node yields it; otherwise the node yields no tuples and the
//! row count lives in `run.processed`.
//!
//! Row identity (PG carries a `ctid` resjunk Var the executor reads via
//! ExecGetJunkAttribute): the system-column-Var executor path is staged, so the row
//! identity is read directly off the subplan slot's `tts_tid` (stamped by the scan).
//!
//! Async coloring: the heap update/delete/insert reach WAL, so `ExecModifyTable` is
//! `async` (rules.md s5). The caller must be inside a WAL insertion scope.

use std::sync::Arc;

use crate::access::tableam::TM_Result;
use crate::backend::access::common::heaptuple::heap_form_tuple;
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::access::heap::heapam_handler::{heapam_tuple_delete, heapam_tuple_update};
use crate::backend::executor::execExpr::exec_build_projection_info;
use crate::backend::executor::execProcnode::{
    exec_end_node, exec_init_node, exec_proc_node, PlanStateNode,
};
use crate::backend::executor::execTuples::{make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::nodes::execnodes::{EState, ExprContext, ModifyTableState, PlanState, ProjectionInfo, TupleTableSlot};
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::plannodes::ModifyTable;
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::RelationData;

/// Run-state pairing the PG `ModifyTableState` with its child (source) plan-state.
/// Borrow-based ownership: the result relation is BORROWED from the EState range
/// table (`&'rel RelationData`), whose owner is the command frame's `Arc`.
pub struct ModifyTableRun<'rel> {
    pub state: Box<ModifyTableState>,
    /// the source-plan state yielding rows to modify.
    pub subplan: Box<PlanStateNode<'rel>>,
    /// the target relation borrowed from `EState.es_range_table_rels`.
    pub result_relation: &'rel RelationData,
    /// the command operation (INSERT/UPDATE/DELETE/MERGE).
    pub operation: CmdType,
    /// rows modified by `ExecModifyTable` (folded into es_processed by the driver).
    pub processed: u64,
    /// the RETURNING projection (None when there is no RETURNING clause). Projects
    /// the modified-row slot into the result slot.
    pub returning: Option<ReturningProj>,
    /// the merge action list (one MergeAction per WHEN clause), carried for MERGE.
    pub merge_actions: Vec<Node>,
}

/// The RETURNING projection state: an owned exprcontext (whose ecxt_scantuple points
/// at the modified-row slot) + the projection info producing the result row.
pub struct ReturningProj {
    pub econtext: Box<ExprContext>,
    pub projection: Box<ProjectionInfo>,
}

/// PG `ExecInitModifyTable`: build the ModifyTableState, init the source subplan, set
/// up the result relation, and (if RETURNING) build the RETURNING projection.
pub fn exec_init_modify_table<'rel>(
    node: &ModifyTable,
    estate: &mut EState<'rel>,
    eflags: i32,
) -> Box<ModifyTableRun<'rel>> {
    crate::assert!(
        node.on_conflict_action == crate::nodes::nodes::OnConflictAction::NONE,
        "ExecInitModifyTable: ON CONFLICT not yet reachable"
    );
    crate::assert!(node.result_relations.len() == 1, "ExecInitModifyTable: one result relation");

    let operation = node.operation;

    let subplan_node = node
        .plan
        .lefttree
        .as_ref()
        .unwrap_or_else(|| unimplemented!("ExecInitModifyTable: ModifyTable without a source subplan"));
    let subplan = exec_init_node(Some(subplan_node), estate, eflags)
        .unwrap_or_else(|| unimplemented!("ExecInitModifyTable: null source subplan"));

    // ExecInitResultRelation: the (single) target relation, BORROWED from the EState.
    let rti = node.result_relations[0];
    crate::assert!(rti > 0);
    let result_relation = estate
        .es_range_table_rels
        .get((rti - 1) as usize)
        .copied()
        .flatten()
        .unwrap_or_else(|| unimplemented!("ExecInitModifyTable: result relation not registered for RTI"));

    // RETURNING projection (the plan's targetlist is the RETURNING list when present).
    let (returning, returning_desc) = if node.returning_lists.is_empty() {
        (None, None)
    } else {
        let desc = crate::backend::executor::execTuples::exec_type_from_tl(&node.returning_lists);
        (
            Some(build_returning_projection(&node.returning_lists, result_relation, &desc)),
            Some(desc),
        )
    };

    let ps = PlanState {
        plan: Some(Node::ModifyTable(Box::new(node.clone()))),
        ps_result_tuple_desc: returning_desc,
        ..PlanState::default()
    };
    let mt = ModifyTableState {
        ps,
        operation: Some(operation),
        can_set_tag: node.can_set_tag,
        mt_done: false,
        mt_nrels: 1,
        ..ModifyTableState::default()
    };

    Box::new(ModifyTableRun {
        state: Box::new(mt),
        subplan: Box::new(subplan),
        result_relation,
        operation,
        processed: 0,
        returning,
        merge_actions: node.merge_action_lists.clone(),
    })
}

/// Build the RETURNING projection: an exprcontext + a projection over the RETURNING
/// target list, with input descriptor = the result relation's rowtype (so the
/// RETURNING Vars resolve to the modified-row slot's attributes).
fn build_returning_projection(
    returning_list: &[Node],
    relation: &RelationData,
    result_desc: &crate::access::tupdesc::TupleDesc,
) -> ReturningProj {
    let input_desc = relation_tupdesc(relation);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(result_desc)), &TTS_OPS_VIRTUAL);
    let mut econtext = Box::new(ExprContext {
        case_value_is_null: true,
        domain_value_is_null: true,
        ..ExprContext::default()
    });
    let projection = exec_build_projection_info(
        returning_list,
        &mut econtext,
        result_slot,
        None,
        Some(input_desc),
    );
    ReturningProj { econtext, projection }
}

/// PG `ExecModifyTable`: the modify driver. For a non-RETURNING command, pulls every
/// source row, applies the command, counts it, and returns `None` once (the row count
/// is read off `run.processed`). For RETURNING, returns one projected row per
/// `ExecProcNode` call (the driver's ExecutePlan loop sends each to the destination).
pub async fn exec_modify_table<'r>(
    shared: &Arc<SharedState>,
    run: &'r mut ModifyTableRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    if run.state.mt_done {
        return None;
    }

    if run.operation == CmdType::MERGE {
        // MERGE execution is staged: the matched-update / not-matched-insert action
        // dispatch over the joined source needs the join subplan's matched/unmatched
        // signaling, which is heavier than the milestone budget. The parse+plan path
        // is complete (transformMergeStmt + the ModifyTable carries the actions).
        unimplemented!("ExecMerge: MERGE execution not yet reachable for this milestone");
    }

    let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
    let has_returning = run.returning.is_some();

    // Build the result relation's trigger descriptor (PG keeps this on the relcache
    // entry; the milestone builds it on demand for the ModifyTable run). AFTER ROW
    // triggers (the RI system triggers) are queued during the run and fired at its
    // end. `relhastriggers` short-circuits relations with no triggers.
    let relid = run.result_relation.rd_id;
    let has_triggers = run
        .result_relation
        .rd_rel
        .as_ref()
        .is_some_and(|r| r.relhastriggers);
    let trigdesc = if has_triggers {
        crate::backend::commands::trigger::relation_build_triggers(shared, relid).await
    } else {
        None
    };

    // AfterTriggerBeginQuery / AfterTriggerEndQuery are NOT driven here: PG opens
    // the after-trigger query level in standard_ExecutorStart and fires the queued
    // events in standard_ExecutorFinish (ExecPostprocessPlan + AfterTriggerEndQuery),
    // so they run regardless of how the run terminates -- a full drain, a RETURNING
    // fetch that stops at a row-count limit, or an early portal stop. Firing them
    // from this node would skip the queue (the RI FK check) whenever the run breaks
    // out before this node returns None. See execMain `standard_executor_finish`.

    loop {
        // Pull the next source row, snapshotting its values + TID before the modify
        // (the subplan-slot borrow ends here, freeing `run` for the RETURNING project).
        let snapshot = {
            let Some(slot) = Box::pin(exec_proc_node(Some(shared), &mut run.subplan)).await else {
                break;
            };
            let n = slot.nvalid.max(0) as usize;
            RowSnapshot {
                tid: slot.tid,
                values: slot.values[..n].to_vec(),
                isnull: slot.isnull[..n].to_vec(),
                desc: slot.tupleDescriptor.clone(),
            }
        };

        match run.operation {
            CmdType::INSERT => {
                exec_insert(shared, run.result_relation, &snapshot, cid).await;
                // ExecARInsertTriggers: queue AFTER ROW INSERT triggers (RI check).
                crate::backend::commands::trigger::exec_ar_insert_triggers(
                    trigdesc.as_ref(), relid, &snapshot.values, &snapshot.isnull, &snapshot.desc,
                );
            }
            CmdType::UPDATE => {
                exec_update(shared, run.result_relation, &snapshot, cid).await;
                // ExecARUpdateTriggers: queue AFTER ROW UPDATE triggers.
                crate::backend::commands::trigger::exec_ar_update_triggers(
                    trigdesc.as_ref(), relid, &snapshot.values, &snapshot.isnull, &snapshot.desc,
                );
            }
            CmdType::DELETE => {
                // The DELETE subplan slot carries only the row identity (ctid), not
                // the full column values an AFTER DELETE trigger reads. When delete
                // triggers exist, fetch the OLD tuple at the TID before deleting it.
                let old_row = if trigdesc.as_ref().is_some_and(|d| d.trig_delete_after_row) {
                    fetch_old_row(shared, run.result_relation, &snapshot.tid).await
                } else {
                    None
                };
                exec_delete(shared, run.result_relation, &snapshot.tid, cid).await;
                // ExecARDeleteTriggers: queue AFTER ROW DELETE triggers (RI action).
                if let Some((vals, nulls, desc)) = old_row {
                    crate::backend::commands::trigger::exec_ar_delete_triggers(
                        trigdesc.as_ref(), relid, &vals, &nulls, &desc,
                    );
                }
            }
            other => unimplemented!("ExecModifyTable: operation {other:?} not reachable"),
        }
        run.processed += 1;

        if has_returning {
            // RETURNING: project the modified row's snapshot (the new tuple for
            // INSERT/UPDATE, the deleted row for DELETE) through the projection.
            return Some(project_returning(run, &snapshot.values, &snapshot.isnull, snapshot.desc));
        }
    }

    run.state.mt_done = true;
    None
}

/// Fetch the full column values of the (about-to-be-deleted) row at `tid`, so an
/// AFTER DELETE trigger sees the OLD tuple's columns. Returns (values, isnull, desc),
/// or None if the row cannot be fetched.
async fn fetch_old_row(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    tid: &ItemPointerData,
) -> Option<(
    Vec<crate::postgres::Datum>,
    Vec<bool>,
    Option<crate::access::tupdesc::TupleDesc>,
)> {
    use crate::backend::utils::time::snapmgr::{ActiveSnapshotSet, GetActiveSnapshot};
    let snap = if ActiveSnapshotSet() { GetActiveSnapshot()? } else { return None };
    let tup = crate::backend::access::heap::heapam::heap_fetch_tid(shared, relation, tid, &snap).await?;
    let desc = relation.rd_att.clone()?;
    // SAFETY: live fetched tuple + the relation's descriptor.
    let (vals, nulls) =
        unsafe { crate::backend::access::common::heaptuple::heap_deform_tuple(&tup, &desc) };
    Some((vals, nulls, Some(desc)))
}

/// A snapshot of the subplan row (its values + TID), taken before the modify so the
/// subplan-slot borrow can be released (freeing the run-state for RETURNING).
struct RowSnapshot {
    tid: ItemPointerData,
    values: Vec<crate::postgres::Datum>,
    isnull: Vec<bool>,
    desc: Option<crate::access::tupdesc::TupleDesc>,
}

/// Project the modified-row snapshot through the RETURNING projection and return a
/// borrow of the projection result slot. The snapshot is stored into the projection's
/// scan-tuple slot (its `ecxt_scantuple`) so the RETURNING scan Vars read it.
fn project_returning<'r>(
    run: &'r mut ModifyTableRun<'_>,
    values: &[crate::postgres::Datum],
    isnull: &[bool],
    desc: Option<crate::access::tupdesc::TupleDesc>,
) -> &'r mut TupleTableSlot {
    let ret = run
        .returning
        .as_mut()
        .unwrap_or_else(|| unreachable!("project_returning: RETURNING projection present"));

    let mut scan_slot = make_tuple_table_slot(desc, &TTS_OPS_VIRTUAL);
    let n = values.len();
    if n > 0 {
        scan_slot.values[..n].copy_from_slice(values);
        scan_slot.isnull[..n].copy_from_slice(isnull);
    }
    crate::backend::executor::execTuples::exec_store_virtual_tuple(&mut scan_slot);
    ret.econtext.ecxt_scantuple = Some(scan_slot);

    run_projection(&mut ret.projection.state, &mut ret.econtext);

    ret.econtext.ecxt_scantuple = None;
    ret.projection
        .state
        .resultslot
        .as_deref_mut()
        .unwrap_or_else(|| unimplemented!("project_returning: projection lost its result slot"))
}

/// Run a projection ExprState into its result slot (mirrors execScan::run_projection).
fn run_projection(
    state: &mut crate::nodes::execnodes::ExprState,
    econtext: &mut ExprContext,
) {
    if let Some(slot) = state.resultslot.as_mut() {
        crate::executor::tuptable::ExecClearTuple(slot);
    }
    let evalfunc = state
        .evalfunc
        .unwrap_or_else(|| unimplemented!("RETURNING projection not ready"));
    let mut is_null = false;
    let _ = evalfunc(state, econtext, &mut is_null);
    if let Some(slot) = state.resultslot.as_mut() {
        crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
    }
}

/// PG `ExecInsert` (M2 subset): form a heap tuple from `slot` and store it through
/// the table AM. AFTER ROW triggers fire in the caller's loop (ExecARInsertTriggers,
/// step 41); BEFORE ROW triggers, indexes, WCO and partition routing grow later.
async fn exec_insert(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    row: &RowSnapshot,
    cid: crate::c::CommandId,
) {
    let desc = relation_tupdesc(relation);
    let natts = desc.natts as usize;
    crate::assert!(row.values.len() >= natts && row.isnull.len() >= natts);
    let mut tuple = heap_form_tuple(&desc, &row.values[..natts], &row.isnull[..natts]);
    heap_insert(shared, relation, &mut tuple, cid, 0).await;
    crate::backend::access::common::heaptuple::heap_freetuple(tuple);
}

/// PG `ExecUpdate` (M8 subset): form the new tuple from the subplan slot (the
/// preptlist-expanded new-tuple values) and `table_tuple_update` it at `old_tid`. The
/// TM_Result is handled: TM_Ok succeeds; TM_SelfModified (the row was already touched
/// by this command, only via join-DML/triggers) is staged; TM_Updated/TM_Deleted are
/// concurrent-update outcomes staged for EPQ. BEFORE/AFTER triggers, indexes, WCO grow later.
async fn exec_update(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    row: &RowSnapshot,
    cid: crate::c::CommandId,
) {
    let old_tid = &row.tid;
    crate::assert!(old_tid.is_valid(), "ExecUpdate: subplan row has no TID");
    let desc = relation_tupdesc(relation);
    let natts = desc.natts as usize;
    crate::assert!(row.values.len() >= natts && row.isnull.len() >= natts);
    let mut newtup = heap_form_tuple(&desc, &row.values[..natts], &row.isnull[..natts]);

    let (res, _lockmode, _update_indexes) =
        heapam_tuple_update(shared, relation, old_tid, &mut newtup, cid, None, None, true).await;
    match res {
        TM_Result::Ok => {}
        TM_Result::SelfModified => {
            // Row already touched by this command: PG returns NULL without counting
            // (and raises TRIGGERED_DATA_CHANGE_VIOLATION on cmax != output cid). Only
            // join-DML / triggers can produce it; both are staged.
            unimplemented!("ExecUpdate: TM_SelfModified (needs join-DML/trigger handling) not yet reachable");
        }
        TM_Result::Updated | TM_Result::Deleted => {
            // Concurrent update/delete: EPQ recheck (EvalPlanQual) lands here. The EPQ
            // scaffolding exists (execMain); resolving the conflict beyond reporting is
            // staged for the concurrent-writer milestone.
            unimplemented!("ExecUpdate: concurrent update (TM_Updated/TM_Deleted) -> EPQ recheck not yet reachable");
        }
        other => unimplemented!("ExecUpdate: unexpected table_tuple_update result {other:?}"),
    }
    crate::backend::access::common::heaptuple::heap_freetuple(newtup);
}

/// PG `ExecDelete` (M8 subset): `table_tuple_delete` the row at `old_tid`. TM_Result
/// handled as in ExecUpdate. BEFORE/AFTER triggers, indexes grow later.
async fn exec_delete(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    old_tid: &ItemPointerData,
    cid: crate::c::CommandId,
) {
    crate::assert!(old_tid.is_valid(), "ExecDelete: subplan row has no TID");
    let (res, _tmfd) =
        heapam_tuple_delete(shared, relation, old_tid, cid, None, None, true, false).await;
    match res {
        TM_Result::Ok => {}
        TM_Result::SelfModified => {
            // Row already touched by this command: PG returns NULL without counting.
            // Only join-DML / triggers can produce it; both are staged.
            unimplemented!("ExecDelete: TM_SelfModified (needs join-DML/trigger handling) not yet reachable");
        }
        TM_Result::Updated | TM_Result::Deleted => {
            unimplemented!("ExecDelete: concurrent update (TM_Updated/TM_Deleted) -> EPQ recheck not yet reachable");
        }
        other => unimplemented!("ExecDelete: unexpected table_tuple_delete result {other:?}"),
    }
}

/// PG `ExecEndModifyTable`: tear down the subplan; the result relation is
/// caller-owned (closed by the EState teardown).
pub fn exec_end_modify_table(shared: &Arc<SharedState>, run: &mut ModifyTableRun<'_>) {
    exec_end_node(Some(shared), &mut run.subplan);
}

/// The rowtype descriptor of a relation (`RelationGetDescr`).
fn relation_tupdesc(relation: &RelationData) -> crate::access::tupdesc::TupleDesc {
    relation
        .rd_att
        .clone()
        .unwrap_or_else(|| unimplemented!("relation_tupdesc: relation has no rowtype descriptor"))
}
