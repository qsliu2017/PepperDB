//! The executor driver. Translated from
//! backend/executor/execMain.c (disposition: grow).
//!
//! `standard_ExecutorStart` -> InitPlan -> ExecInitNode; `standard_ExecutorRun`
//! -> ExecutePlan -> ExecProcNode -> DestReceiver; `standard_ExecutorEnd` ->
//! ExecEndPlan -> FreeExecutorState. ExecutePlan's command arm lives the SELECT
//! retrieval loop; the ModifyTable/INTO/parallel/returning arms grow per
//! milestone (rules.md s4). The whole M1 const path is synchronous (rules.md s5):
//! no node reaches an I/O leaf, so ExecutorRun does not `.await`.

use crate::access::sdir::{scan_direction_is_no_movement, ScanDirection};
use crate::executor::execdesc::QueryDesc;
use crate::executor::executor::ExecFlag;
use crate::nodes::nodes::CmdType;

use crate::backend::executor::execProcnode::{exec_end_node, exec_init_node, exec_proc_node, PlanStateNode};
use crate::backend::executor::execTuples::exec_reset_tuple_table;
use crate::backend::executor::execUtils::{create_executor_state, free_executor_state};

/// PG `standard_ExecutorStart`: set up the EState and the plan-state tree.
pub fn standard_executor_start(query_desc: &mut QueryDesc, eflags: i32) {
    crate::assert!(query_desc.estate.is_none());

    let estate = create_executor_state();
    query_desc.estate = Some(estate);
    let estate = query_desc
        .estate
        .as_mut()
        .unwrap_or_else(|| unreachable!("estate just set"));

    estate.top_eflags = eflags;
    // Snapshot registration, param setup, junkfilter and trigger setup grow with
    // their subsystems; none is reachable on the const SELECT path.

    init_plan(query_desc, eflags);
}

/// PG `InitPlan`: initialize the plan-state tree and result tupdesc for a
/// non-modifying SELECT. The rangetable/pruning/rowmark/subplan/junkfilter setup
/// is empty on the const path and grows with those features.
fn init_plan(query_desc: &mut QueryDesc, eflags: i32) {
    let plannedstmt = query_desc
        .plannedstmt
        .as_ref()
        .unwrap_or_else(|| unimplemented!("InitPlan: no planned statement"));
    crate::assert!(plannedstmt.rtable.is_empty());
    crate::assert!(plannedstmt.result_relations.is_empty());

    let plan_tree = plannedstmt.plan_tree.clone();

    let estate = query_desc
        .estate
        .as_mut()
        .unwrap_or_else(|| unreachable!("estate set by ExecutorStart"));

    let planstate = exec_init_node(Some(&plan_tree), estate, eflags)
        .unwrap_or_else(|| unimplemented!("InitPlan: null plan tree"));

    // ExecGetResultType: the root node's result tupdesc.
    let tup_desc = result_type_of(&planstate);
    query_desc.tupDesc = (!tup_desc.is_null()).then_some(tup_desc);
    query_desc.planstate = Some(Box::new(planstate));
}

/// The result TupleDesc of a plan-state node (PG `ExecGetResultType`).
fn result_type_of(node: &PlanStateNode) -> crate::access::tupdesc::TupleDesc {
    match node {
        PlanStateNode::Result(rs) => rs.ps.ps_result_tuple_desc,
    }
}

/// PG `standard_ExecutorRun`: drive the plan, sending tuples to the destination.
/// Synchronous on the M1 path.
pub fn standard_executor_run(query_desc: &mut QueryDesc, direction: ScanDirection, count: u64) {
    let operation = query_desc.operation;
    let send_tuples =
        operation == CmdType::SELECT || query_desc.plannedstmt.as_ref().is_some_and(|p| p.has_returning);

    if let Some(estate) = query_desc.estate.as_mut() {
        estate.processed = 0;
    }

    // dest->rStartup(operation, tupDesc).
    if send_tuples {
        let tup_desc = query_desc.tupDesc.unwrap_or(core::ptr::null_mut());
        let dest = query_desc
            .dest
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecutorRun: no destination receiver"));
        dest.r_startup(operation, tup_desc);
    }

    if !scan_direction_is_no_movement(direction) {
        execute_plan(query_desc, operation, send_tuples, count, direction);
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
/// honor `count` and direction. The Result node returns one row then None.
fn execute_plan(
    query_desc: &mut QueryDesc,
    operation: CmdType,
    send_tuples: bool,
    number_tuples: u64,
    direction: ScanDirection,
) {
    if let Some(estate) = query_desc.estate.as_mut() {
        estate.direction = direction;
    }

    let mut current_tuple_count: u64 = 0;
    loop {
        // ResetPerTupleExprContext(estate): no-op (memory tombstoned).
        let planstate = query_desc
            .planstate
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecutePlan: no plan state"));

        let Some(mut slot) = exec_proc_node(planstate) else {
            break; // TupIsNull -> done
        };

        // es_junkFilter is NULL for a plain SELECT; junk filtering grows.

        if send_tuples {
            let dest = query_desc
                .dest
                .as_mut()
                .unwrap_or_else(|| unimplemented!("ExecutePlan: no destination receiver"));
            if !dest.receive_slot(&mut slot) {
                break; // receiver asked to stop
            }
        }

        // es_processed is only bumped for SELECT (RETURNING grows later).
        if operation == CmdType::SELECT
            && let Some(estate) = query_desc.estate.as_mut()
        {
            estate.processed += 1;
        }

        current_tuple_count += 1;
        if number_tuples != 0 && number_tuples == current_tuple_count {
            break;
        }
    }

    // ExecShutdownNode grows with parallel/async nodes; nothing to do on M1.
}

/// PG `standard_ExecutorFinish`: run any post-processing (ModifyTable to
/// completion). Nothing to do for a SELECT.
pub fn standard_executor_finish(query_desc: &mut QueryDesc) {
    if let Some(estate) = query_desc.estate.as_mut() {
        estate.finished = true;
    }
}

/// PG `standard_ExecutorEnd`: tear down the plan and free the EState.
pub fn standard_executor_end(query_desc: &mut QueryDesc) {
    if let Some(mut planstate) = query_desc.planstate.take() {
        exec_end_plan(&mut planstate, query_desc);
    }

    if let Some(estate) = query_desc.estate.take() {
        free_executor_state(estate);
    }
    query_desc.tupDesc = None;
}

/// PG `ExecEndPlan`: end the node tree and release the tuple table / relations.
fn exec_end_plan(planstate: &mut PlanStateNode, query_desc: &mut QueryDesc) {
    exec_end_node(planstate);
    if let Some(estate) = query_desc.estate.as_mut() {
        exec_reset_tuple_table(&mut estate.tuple_table, false);
    }
    // ExecCloseResultRelations / ExecCloseRangeTableRelations grow with relations.
}

/// `ExecFlag` is kept referenced so the eflags type stays wired as start/run grow.
#[allow(dead_code)]
fn eflag_marker() -> ExecFlag {
    ExecFlag::empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

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
        sink: Rc<RefCell<Collected>>,
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
            self.sink.borrow_mut().rows.push(row);
            true
        }
        fn r_startup(&mut self, _operation: CmdType, _typeinfo: TupleDesc) {
            self.sink.borrow_mut().startups += 1;
        }
        fn r_shutdown(&mut self) {
            self.sink.borrow_mut().shutdowns += 1;
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
    fn query_desc(sql: &str, sink: &Rc<RefCell<Collected>>) -> QueryDesc {
        let stmt = plan(sql);
        #[allow(deprecated)]
        QueryDesc {
            operation: stmt.command_type,
            plannedstmt: Some(Box::new(stmt)),
            sourceText: sql.to_string(),
            snapshot: None,
            crosscheck_snapshot: None,
            dest: Some(Box::new(CollectingDest {
                sink: Rc::clone(sink),
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
        let sink = Rc::new(RefCell::new(Collected::default()));
        let mut qd = query_desc("SELECT 1", &sink);
        ExecutorStart(&mut qd, 0);

        // ExecutorStart built the result tupdesc (one int4 attr).
        let result_desc = qd.tupDesc.expect("tupDesc set by ExecutorStart");
        // SAFETY: live descriptor for the duration of the test.
        unsafe {
            assert_eq!((*result_desc).natts, 1);
            assert_eq!((*result_desc).attr(0).atttypid, INT4OID);
        }

        ExecutorRun(&mut qd, ScanDirection::Forward, 0);
        ExecutorFinish(&mut qd);

        {
            let dest = sink.borrow();
            assert_eq!(dest.startups, 1);
            assert_eq!(dest.shutdowns, 1);
            assert_eq!(dest.rows.len(), 1, "exactly one row");
            assert_eq!(dest.rows[0].len(), 1, "one attr");
            assert!(!dest.rows[0][0].1, "not null");
            assert_eq!(DatumGetInt32(dest.rows[0][0].0), 1);
        }
        assert_eq!(qd.estate.as_ref().unwrap().processed, 1);

        ExecutorEnd(&mut qd);
        assert!(qd.estate.is_none());
    }

    #[test]
    fn second_exec_proc_node_returns_no_slot() {
        // The Result returns one row, then None on the next pull.
        let stmt = plan("SELECT 1");
        let mut estate = create_executor_state();
        let mut ps = exec_init_node(Some(&stmt.plan_tree), &mut estate, 0).expect("a Result node");

        assert!(exec_proc_node(&mut ps).is_some(), "first pull yields the row");
        assert!(exec_proc_node(&mut ps).is_none(), "second pull yields nothing");
        // A third pull is still None (idempotent EOF).
        assert!(exec_proc_node(&mut ps).is_none());
    }

    #[test]
    fn select_42_executes_to_42() {
        let sink = Rc::new(RefCell::new(Collected::default()));
        let mut qd = query_desc("SELECT 42", &sink);
        ExecutorStart(&mut qd, 0);
        ExecutorRun(&mut qd, ScanDirection::Forward, 0);
        ExecutorFinish(&mut qd);
        assert_eq!(DatumGetInt32(sink.borrow().rows[0][0].0), 42);
        ExecutorEnd(&mut qd);
    }

    #[test]
    fn select_two_constants_one_row_two_attrs() {
        let sink = Rc::new(RefCell::new(Collected::default()));
        let mut qd = query_desc("SELECT 1, 2", &sink);
        ExecutorStart(&mut qd, 0);
        ExecutorRun(&mut qd, ScanDirection::Forward, 0);
        ExecutorFinish(&mut qd);
        {
            let dest = sink.borrow();
            assert_eq!(dest.rows.len(), 1);
            assert_eq!(dest.rows[0].len(), 2);
            assert_eq!(DatumGetInt32(dest.rows[0][0].0), 1);
            assert_eq!(DatumGetInt32(dest.rows[0][1].0), 2);
        }
        ExecutorEnd(&mut qd);
    }
}
