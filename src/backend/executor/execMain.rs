//! The executor driver. Translated from
//! backend/executor/execMain.c (disposition: grow).
//!
//! `standard_ExecutorStart` -> InitPlan -> ExecInitNode; `standard_ExecutorRun`
//! -> ExecutePlan -> ExecProcNode -> DestReceiver; `standard_ExecutorEnd` ->
//! ExecEndPlan -> FreeExecutorState. ExecutePlan's command arm lives the SELECT
//! retrieval loop; the ModifyTable/INTO/parallel/returning arms grow per
//! milestone (rules.md s4). The whole M1 const path is synchronous (rules.md s5):
//! no node reaches an I/O leaf, so ExecutorRun does not `.await`.

use crate::utils::rel::RelationData;

use std::sync::Arc;

use crate::access::sdir::{scan_direction_is_no_movement, ScanDirection};
use crate::executor::execdesc::QueryDesc;
use crate::executor::executor::ExecFlag;
use crate::nodes::nodes::CmdType;
use crate::shared_state::SharedState;

use crate::backend::executor::execProcnode::{exec_end_node, exec_init_node, exec_proc_node, PlanStateNode};
use crate::backend::executor::execTuples::exec_reset_tuple_table;
use crate::backend::executor::execUtils::{create_executor_state, free_executor_state};
use crate::nodes::execnodes::RangeTableRels;

// ---------------------------------------------------------------------------
// Executor range-table relations (PG `es_relations`).
//
// PG's `ExecInitRangeTable` sizes `estate->es_relations` and the scan/result-rel
// openers (`ExecGetRangeTableRelation`) fill each slot with the open relation,
// already opened under the right locks by the caller before InitPlan. Here the
// command/statement frame opens them into its owning `Arc<RelationData>`s (the
// `'rel` root that strictly encloses ExecutorStart..End) and passes a BORROW --
// `RangeTableRels<'rel>` indexed by RT index -- into `standard_executor_start`,
// which publishes it on `EState.es_range_table_rels`. No task-local, no per-node
// `Arc` clone: the executor borrows the relations from a suspended ancestor frame.
// ---------------------------------------------------------------------------

/// PG `standard_ExecutorStart`: set up the EState and the plan-state tree. The
/// borrowed range-table relations + query snapshot are published on the EState
/// (the `'rel` owners are the command frame's `Arc`s, see the module note).
pub fn standard_executor_start<'rel>(
    query_desc: &mut QueryDesc<'rel>,
    range_table_rels: RangeTableRels<'rel>,
    snapshot_ref: Option<&'rel crate::utils::snapshot::SnapshotData>,
    eflags: i32,
) {
    standard_executor_start_indexed(query_desc, range_table_rels, &[], snapshot_ref, eflags);
}

/// `standard_ExecutorStart` with the borrowed open index relations also published
/// (PG resolves these via `index_open`; here the command frame passes the borrows).
/// The plain `standard_executor_start` forwards here with no index relations.
pub fn standard_executor_start_indexed<'rel>(
    query_desc: &mut QueryDesc<'rel>,
    range_table_rels: RangeTableRels<'rel>,
    index_rels: &'rel [Option<&'rel crate::utils::rel::RelationData>],
    snapshot_ref: Option<&'rel crate::utils::snapshot::SnapshotData>,
    eflags: i32,
) {
    crate::assert!(query_desc.estate.is_none());

    let estate = create_executor_state();
    query_desc.estate = Some(estate);
    let estate = query_desc
        .estate
        .as_mut()
        .unwrap_or_else(|| unreachable!("estate just set"));

    estate.es_range_table_rels = range_table_rels;
    estate.es_index_rels = index_rels;
    estate.es_snapshot_ref = snapshot_ref;
    estate.top_eflags = eflags;
    // es_snapshot / es_crosscheck_snapshot: the scan path (M2) reads tuples under
    // the query snapshot, so copy it from the QueryDesc (the caller registers the
    // active snapshot before ExecutorStart, mirroring PG's es_snapshot setup).
    if let Some(snap) = query_desc.snapshot.as_deref() {
        estate.snapshot.clone_from(snap);
    }
    if let Some(cc) = query_desc.crosscheck_snapshot.as_deref() {
        estate.crosscheck_snapshot.clone_from(cc);
    }
    // es_output_cid: the inserting/deleting command id for a data-modifying query.
    if query_desc.operation != CmdType::SELECT {
        estate.output_cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
    }
    // Param setup, junkfilter and trigger setup grow with their subsystems.

    init_plan(query_desc, eflags);
}

/// PG `InitPlan`: initialize the plan-state tree and result tupdesc. Sets up the
/// executor range table (`ExecInitRangeTable`) and, for a data-modifying command,
/// the result relation(s) (`ExecInitResultRelation`), then `ExecInitNode`. The
/// rowmark/pruning/subplan/junkfilter setup grows with those features.
fn init_plan(query_desc: &mut QueryDesc<'_>, eflags: i32) {
    let plannedstmt = query_desc
        .plannedstmt
        .as_ref()
        .unwrap_or_else(|| unimplemented!("InitPlan: no planned statement"));
    let operation = plannedstmt.command_type;
    let plan_tree = plannedstmt.plan_tree.clone();
    let rtable = plannedstmt.rtable.clone();
    let result_relations = plannedstmt.result_relations.clone();

    let estate = query_desc
        .estate
        .as_mut()
        .unwrap_or_else(|| unreachable!("estate set by ExecutorStart"));

    // ExecInitRangeTable: publish the rangetable. The open relations for the
    // RTE_RELATION entries were borrowed onto `estate.es_range_table_rels` by
    // standard_executor_start (PG's es_relations; see the module-level note), read
    // by ExecGetRangeTableRelation during node init. The scan/result-rel openers
    // index that borrowed slice by RT index.
    estate.range_table_size = rtable.len();
    estate.range_table = rtable;
    // Result relations are surfaced to ExecInitModifyTable via the planned
    // `result_relations` RT indices (also stashed on the EState as integers).
    estate.result_relations.clear();
    crate::assert!(
        operation == CmdType::SELECT || !result_relations.is_empty(),
        "InitPlan: data-modifying command without a result relation"
    );

    let planstate = exec_init_node(Some(&plan_tree), estate, eflags)
        .unwrap_or_else(|| unimplemented!("InitPlan: null plan tree"));

    // ExecGetResultType: the root node's result tupdesc (an Arc clone the
    // QueryDesc co-owns alongside the planstate's slot). A non-RETURNING
    // ModifyTable yields no tuples, hence no result descriptor.
    query_desc.tupDesc = result_type_of(&planstate);
    query_desc.planstate = Some(Box::new(planstate));
}

/// The result TupleDesc of a plan-state node (PG `ExecGetResultType`).
use crate::backend::executor::execProcnode::result_type_of;

/// PG `standard_ExecutorRun`: drive the plan, sending tuples to the destination.
/// Async because the scan path reaches the table AM's buffer reads (rules.md s5);
/// the M1 const path still resolves immediately (no `.await` on the inner futures
/// hits an I/O leaf). `dest.receive_slot` stays synchronous (printtup buffers into
/// the send buffer synchronously, step 09).
pub async fn standard_executor_run(
    shared: Option<&Arc<SharedState>>,
    query_desc: &mut QueryDesc<'_>,
    direction: ScanDirection,
    count: u64,
) {
    let operation = query_desc.operation;
    let send_tuples =
        operation == CmdType::SELECT || query_desc.plannedstmt.as_ref().is_some_and(|p| p.has_returning);

    if let Some(estate) = query_desc.estate.as_mut() {
        estate.processed = 0;
    }

    // dest->rStartup(operation, tupDesc). A tuple-returning run always has a
    // result descriptor; hand the receiver an Arc clone (a co-owner).
    if send_tuples {
        let tup_desc = query_desc
            .tupDesc
            .clone()
            .unwrap_or_else(|| unimplemented!("ExecutorRun: tuple-returning run without a result descriptor"));
        let dest = query_desc
            .dest
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecutorRun: no destination receiver"));
        dest.r_startup(operation, tup_desc);
    }

    if !scan_direction_is_no_movement(direction) {
        execute_plan(shared, query_desc, operation, send_tuples, count, direction).await;
    }

    // dest->rShutdown().
    if send_tuples {
        if let Some(dest) = query_desc.dest.as_mut() {
            dest.r_shutdown();
        } else {
            unimplemented!("ExecutorRun: no destination receiver");
        }
    }

    query_desc.already_executed = true;
}

/// PG `ExecutePlan`: the retrieval loop. Pull a tuple, (junk-filter,) send it,
/// honor `count` and direction. The scan node returns a BORROW of its node-owned
/// slot each call; the borrow is consumed (sent to the destination) before the
/// next `ExecProcNode` so there is no per-tuple clone. A non-RETURNING
/// ModifyTable returns None on its single drive (the count lives in es_processed),
/// so the loop ends immediately.
async fn execute_plan(
    shared: Option<&Arc<SharedState>>,
    query_desc: &mut QueryDesc<'_>,
    operation: CmdType,
    send_tuples: bool,
    number_tuples: u64,
    direction: ScanDirection,
) {
    // Split the QueryDesc into disjoint field borrows up front: the plan-state
    // (which yields the borrowed slot), the destination, and the estate are
    // distinct fields, so borrowing them separately lets the slot borrowed from
    // `planstate` coexist with the `dest`/`estate` accesses in one loop iteration.
    let QueryDesc {
        planstate,
        dest,
        estate,
        ..
    } = query_desc;
    let planstate = planstate
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecutePlan: no plan state"));
    if let Some(estate) = estate.as_mut() {
        estate.direction = direction;
    }

    let mut current_tuple_count: u64 = 0;
    loop {
        // ResetPerTupleExprContext(estate): no-op (memory tombstoned).
        let Some(slot) = exec_proc_node(shared, planstate).await else {
            break; // TupIsNull -> done
        };

        // es_junkFilter is NULL for a plain SELECT; junk filtering grows.

        if send_tuples {
            let dest = dest
                .as_mut()
                .unwrap_or_else(|| unimplemented!("ExecutePlan: no destination receiver"));
            if !dest.receive_slot(slot) {
                break; // receiver asked to stop
            }
        }

        // es_processed is only bumped for SELECT (RETURNING grows later).
        if operation == CmdType::SELECT
            && let Some(estate) = estate.as_mut()
        {
            estate.processed += 1;
        }

        current_tuple_count += 1;
        if number_tuples != 0 && number_tuples == current_tuple_count {
            break;
        }
    }

    // For a data-modifying ModifyTable, PG bumps es_processed inside the node; the
    // EState lives on the QueryDesc here, so fold the node's count in afterwards.
    if let PlanStateNode::ModifyTable(mt) = planstate.as_mut()
        && let Some(estate) = estate.as_mut()
    {
        estate.processed += mt.processed;
    }

    // ExecShutdownNode grows with parallel/async nodes; nothing to do on M1/M2.
}

/// PG `standard_ExecutorFinish`: run any post-processing (ModifyTable to
/// completion). Nothing to do for a SELECT.
pub fn standard_executor_finish(query_desc: &mut QueryDesc<'_>) {
    if let Some(estate) = query_desc.estate.as_mut() {
        estate.finished = true;
    }
}

/// PG `standard_ExecutorEnd`: tear down the plan and free the EState. Takes
/// `shared` so node teardown can release buffers/scans (heap_endscan). Stays
/// synchronous (buffer release does not `.await`).
pub fn standard_executor_end(shared: Option<&Arc<SharedState>>, query_desc: &mut QueryDesc<'_>) {
    if let Some(mut planstate) = query_desc.planstate.take() {
        exec_end_plan(shared, &mut planstate, query_desc);
    }

    if let Some(estate) = query_desc.estate.take() {
        free_executor_state(estate);
    }
    query_desc.tupDesc = None;
}

/// PG `ExecEndPlan`: end the node tree and release the tuple table / relations.
fn exec_end_plan(shared: Option<&Arc<SharedState>>, planstate: &mut PlanStateNode<'_>, query_desc: &mut QueryDesc<'_>) {
    exec_end_node(shared, planstate);
    if let Some(estate) = query_desc.estate.as_mut() {
        exec_reset_tuple_table(&mut estate.tuple_table, false);
    }
    // ExecCloseResultRelations / ExecCloseRangeTableRelations grow with relations
    // (M2 relations are caller-owned, not closed here).
}

/// `ExecFlag` is kept referenced so the eflags type stays wired as start/run grow.
#[allow(dead_code)]
fn eflag_marker() -> ExecFlag {
    ExecFlag::empty()
}

// ===========================================================================
//  EvalPlanQual (EPQ): re-check a concurrently-updated row (M8, step 34).
//
//  When an UPDATE/DELETE/SELECT-FOR-UPDATE finds the target row was updated by a
//  concurrent transaction (TM_Updated), PG re-fetches the latest row version and
//  re-runs the query's quals over it (`EvalPlanQual`) to decide whether the new
//  version still qualifies. The state lives in an `EPQState` threaded by the
//  ModifyTable / LockRows node.
//
//  M8 builds the EPQ SCAFFOLDING -- the EPQState lifecycle (Init / Slot / SetPlan /
//  End), the per-rel test-tuple slots, the row-identity helpers, and the
//  non-concurrent recheck path (no concurrent writer -> the origslot row is returned
//  unchanged). The full recheck-subtree rebuild (`EvalPlanQualStart`, a child EState
//  over the parent's range table) is reached only on an actual concurrent-update
//  conflict; that path is a clear grow guard (the concurrent-writer milestone).
// ===========================================================================

use crate::nodes::execnodes::{EPQState, EState, ExecRowMark, TupleTableSlot};
use crate::nodes::lockoptions::LockTupleMode;
use crate::nodes::nodes::Node;

/// PG `EvalPlanQualInit`: initialize an EPQState over a parent EState's subplan and
/// the (non-locking) auxiliary row marks. Allocates the per-rti test-tuple slot
/// vector and marks the EPQ inactive.
pub fn eval_plan_qual_init(
    epqstate: &mut EPQState,
    range_table_size: usize,
    subplan: Option<Node>,
    epq_param: i32,
    result_relations: Vec<i32>,
) {
    epqstate.epq_param = epq_param;
    epqstate.result_relations = result_relations;
    epqstate.tuple_table = Vec::new();
    epqstate.relsubs_slot = Vec::with_capacity(range_table_size);
    epqstate.plan = subplan;
    epqstate.origslot = None;
    epqstate.recheckestate = None;
    epqstate.recheckplanstate = None;
    epqstate.relsubs_rowmark = Vec::new();
    epqstate.relsubs_done = Vec::new();
    epqstate.relsubs_blocked = Vec::new();
}

/// PG `EvalPlanQualSetPlan`: set or change an EPQState's subplan (shutting down any
/// live EPQ tree first).
pub fn eval_plan_qual_set_plan(epqstate: &mut EPQState, subplan: Option<Node>) {
    eval_plan_qual_end(epqstate);
    epqstate.plan = subplan;
}

/// PG `EvalPlanQualSetSlot`: record the top-level result row to recheck (origslot).
pub fn eval_plan_qual_set_slot(epqstate: &mut EPQState, slot: Option<Box<TupleTableSlot>>) {
    epqstate.origslot = slot;
}

/// PG `EvalPlanQualNext`: fetch the next row from the EPQ recheck plan. With no
/// concurrent writer (the common, M8-reachable case) there is no recheck subtree
/// running and the origslot row stands -- return it unchanged. The recheck-subtree
/// drive (a re-run of the plan over the re-fetched row) is reached only after
/// `EvalPlanQualBegin` built a child plan, which is staged.
pub fn eval_plan_qual_next(epqstate: &mut EPQState) -> Option<&mut TupleTableSlot> {
    if epqstate.recheckplanstate.is_some() {
        // A live recheck tree means a concurrent conflict was detected; driving it
        // (ExecProcNode on the child EState) is the concurrent-writer path.
        unimplemented!("EvalPlanQualNext: recheck-subtree drive not yet reachable for this milestone");
    }
    // Non-concurrent case: the origslot row is the recheck result, returned unchanged.
    epqstate.origslot.as_deref_mut()
}

/// PG `EvalPlanQualBegin`: (re)initialize the EPQ recheck plan tree. Reached only on
/// an actual concurrent-update conflict; the child-EState rebuild over the parent's
/// borrowed range table is staged (the concurrent-writer milestone).
pub fn eval_plan_qual_begin(_epqstate: &mut EPQState) {
    unimplemented!("EvalPlanQualBegin: recheck-subtree rebuild not yet reachable for this milestone");
}

/// PG `EvalPlanQualEnd`: tear down a live EPQ recheck tree (no-op when inactive).
pub fn eval_plan_qual_end(epqstate: &mut EPQState) {
    epqstate.recheckplanstate = None;
    epqstate.recheckestate = None;
    epqstate.origslot = None;
    epqstate.tuple_table.clear();
    epqstate.relsubs_slot.clear();
}

/// PG `EvalPlanQualFetchRowMark`: fetch the current row value for a non-locked
/// relation that the EPQ recheck must scan. Reached only inside an active EPQ
/// recheck (concurrent-writer path); staged.
pub fn eval_plan_qual_fetch_row_mark(
    _epqstate: &mut EPQState,
    _rti: usize,
    _slot: &mut TupleTableSlot,
) -> bool {
    unimplemented!("EvalPlanQualFetchRowMark: not yet reachable for this milestone");
}

/// PG `ExecUpdateLockMode`: the tuple-lock strength for an UPDATE. If a key column
/// was modified, FOR UPDATE strength (exclusive); otherwise the weaker NO KEY
/// UPDATE. M8 has no index-key bitmap wired (no unique indexes on the test tables),
/// so the conservative LockTupleExclusive is returned (matches PG when key columns
/// are or may be updated); the key-column narrowing grows with the index attr bitmap.
pub fn exec_update_lock_mode(_estate: &EState<'_>, _relinfo: &ExecRowMark) -> LockTupleMode {
    LockTupleMode::LockTupleExclusive
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::access::sdir::ScanDirection;
    use crate::access::tupdesc::TupleDesc;
    use crate::catalog::genbki::INT4OID;
    use crate::executor::execdesc::QueryDesc;
    use crate::executor::executor::{ExecutorEnd, ExecutorFinish, ExecutorRun, ExecutorStart};
    use crate::executor::instrument::InstrumentOption;
    use crate::executor::tuptable::{slot_getattr, TupleTableSlot};
    use crate::nodes::nodes::{CmdType, Node};
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;
    use crate::postgres::{Datum, DatumGetInt32};
    use crate::tcop::dest::{CommandDest, DestReceiver};

    /// Shared sink the test inspects after the run.
    #[derive(Default)]
    struct Collected {
        rows: Vec<Vec<(Datum, bool)>>,
        startups: u32,
        shutdowns: u32,
    }

    /// In-memory DestReceiver writing each row's (datum, isnull) into a shared
    /// `Collected` the test keeps a handle to.
    struct CollectingDest {
        sink: Arc<Mutex<Collected>>,
    }

    impl DestReceiver for CollectingDest {
        fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool {
            let natts = i32::from(slot.nvalid);
            let row = (1..=natts)
                .map(|attno| {
                    let v = slot_getattr(slot, attno);
                    (v.unwrap_or(Datum(0)), v.is_none())
                })
                .collect();
            self.sink.lock().unwrap().rows.push(row);
            true
        }
        fn r_startup(&mut self, _operation: CmdType, _typeinfo: TupleDesc) {
            self.sink.lock().unwrap().startups += 1;
        }
        fn r_shutdown(&mut self) {
            self.sink.lock().unwrap().shutdowns += 1;
        }
        fn mydest(&self) -> CommandDest {
            CommandDest::DestNone
        }
    }

    /// Run the front half of the pipeline: raw parse + analyze + rewrite + plan.
    fn plan(s: &str) -> crate::nodes::plannodes::PlannedStmt {
        let mut list = crate::backend::parser::parser::raw_parser(s, RawParseMode::Default);
        assert_eq!(list.len(), 1);
        let Node::RawStmt(rs) = list.remove(0) else {
            panic!("not a RawStmt")
        };
        let rs: RawStmt = *rs;
        let q = crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, s, &[], 0, None);
        let mut rewritten = crate::backend::rewrite::rewriteHandler::query_rewrite(*q);
        assert_eq!(rewritten.len(), 1);
        let mut parse = rewritten.remove(0);
        crate::backend::optimizer::plan::planner::standard_planner(&mut parse, s, 0, None)
    }

    /// Build a QueryDesc with a collecting receiver writing into `sink`.
    fn query_desc(sql: &str, sink: &Arc<Mutex<Collected>>) -> QueryDesc<'static> {
        let stmt = plan(sql);
        #[allow(deprecated)]
        QueryDesc {
            operation: stmt.command_type,
            plannedstmt: Some(Box::new(stmt)),
            sourceText: sql.to_string(),
            snapshot: None,
            crosscheck_snapshot: None,
            dest: Some(Box::new(CollectingDest {
                sink: Arc::clone(sink),
            })),
            params: None,
            queryEnv: None,
            instrument_options: InstrumentOption::empty(),
            tupDesc: None,
            estate: None,
            planstate: None,
            already_executed: false,
            totaltime: None,
        }
    }

    #[test]
    fn select_one_executes_to_single_int4_row() {
        let sink = Arc::new(Mutex::new(Collected::default()));
        let mut qd = query_desc("SELECT 1", &sink);
        ExecutorStart(&mut qd, 0);

        // ExecutorStart built the result tupdesc (one int4 attr).
        let result_desc = qd.tupDesc.as_ref().expect("tupDesc set by ExecutorStart");
        assert_eq!(result_desc.natts, 1);
        assert_eq!(result_desc.attr(0).atttypid, INT4OID);

        block_on_ready(ExecutorRun(None, &mut qd, ScanDirection::Forward, 0));
        ExecutorFinish(&mut qd);

        {
            let dest = sink.lock().unwrap();
            assert_eq!(dest.startups, 1);
            assert_eq!(dest.shutdowns, 1);
            assert_eq!(dest.rows.len(), 1, "exactly one row");
            assert_eq!(dest.rows[0].len(), 1, "one attr");
            assert!(!dest.rows[0][0].1, "not null");
            assert_eq!(DatumGetInt32(dest.rows[0][0].0), 1);
        }
        assert_eq!(qd.estate.as_ref().unwrap().processed, 1);

        ExecutorEnd(None, &mut qd);
        assert!(qd.estate.is_none());
    }

    #[test]
    fn second_exec_proc_node_returns_no_slot() {
        // The Result returns one row, then None on the next pull.
        let stmt = plan("SELECT 1");
        let mut estate = create_executor_state();
        let mut ps = exec_init_node(Some(&stmt.plan_tree), &mut estate, 0).expect("a Result node");

        assert!(block_on_ready(exec_proc_node(None, &mut ps)).is_some(), "first pull yields the row");
        assert!(block_on_ready(exec_proc_node(None, &mut ps)).is_none(), "second pull yields nothing");
        // A third pull is still None (idempotent EOF).
        assert!(block_on_ready(exec_proc_node(None, &mut ps)).is_none());
    }

    #[test]
    fn select_42_executes_to_42() {
        let sink = Arc::new(Mutex::new(Collected::default()));
        let mut qd = query_desc("SELECT 42", &sink);
        ExecutorStart(&mut qd, 0);
        block_on_ready(ExecutorRun(None, &mut qd, ScanDirection::Forward, 0));
        ExecutorFinish(&mut qd);
        assert_eq!(DatumGetInt32(sink.lock().unwrap().rows[0][0].0), 42);
        ExecutorEnd(None, &mut qd);
    }

    #[test]
    fn select_two_constants_one_row_two_attrs() {
        let sink = Arc::new(Mutex::new(Collected::default()));
        let mut qd = query_desc("SELECT 1, 2", &sink);
        ExecutorStart(&mut qd, 0);
        block_on_ready(ExecutorRun(None, &mut qd, ScanDirection::Forward, 0));
        ExecutorFinish(&mut qd);
        {
            let dest = sink.lock().unwrap();
            assert_eq!(dest.rows.len(), 1);
            assert_eq!(dest.rows[0].len(), 2);
            assert_eq!(DatumGetInt32(dest.rows[0][0].0), 1);
            assert_eq!(DatumGetInt32(dest.rows[0][1].0), 2);
        }
        ExecutorEnd(None, &mut qd);
    }

    /// Drive a const-path executor future to completion synchronously (it reaches
    /// no I/O leaf, so it is Ready on the first poll).
    fn block_on_ready<F: std::future::Future<Output = T>, T>(fut: F) -> T {
        use std::task::{Context, Poll};
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = std::pin::pin!(fut);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("const executor future suspended unexpectedly"),
        }
    }

    /// EPQ scaffolding: with no concurrent writer, EvalPlanQualNext returns the
    /// recorded top-level row (origslot) unchanged -- the common, non-conflict case.
    #[test]
    fn eval_plan_qual_next_returns_origslot_unchanged() {
        use crate::backend::executor::execTuples::{make_tuple_table_slot, TTS_OPS_VIRTUAL};
        use crate::postgres::{DatumGetInt32, Int32GetDatum};

        let mut epq = EPQState::default();
        eval_plan_qual_init(&mut epq, 1, None, -1, vec![1]);

        // Record a top-level row (the row being rechecked).
        let mut slot = make_tuple_table_slot(None, &TTS_OPS_VIRTUAL);
        slot.values = vec![Int32GetDatum(7)];
        slot.isnull = vec![false];
        slot.nvalid = 1;
        eval_plan_qual_set_slot(&mut epq, Some(slot));

        // No recheck subtree is running -> EvalPlanQualNext returns the origslot row.
        let out = eval_plan_qual_next(&mut epq).expect("origslot returned unchanged");
        assert_eq!(DatumGetInt32(out.values[0]), 7, "the row is returned unchanged");

        // EvalPlanQualEnd clears the state (idempotent).
        eval_plan_qual_end(&mut epq);
        assert!(epq.origslot.is_none(), "EvalPlanQualEnd cleared the origslot");
    }
}
