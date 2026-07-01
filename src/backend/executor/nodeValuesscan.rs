//! ValuesScan node executor. Translated from
//! backend/executor/nodeValuesscan.c (disposition: full for the core int/text
//! literal `VALUES (...), ...` scan).
//!
//! `ExecInitValuesScan` builds the scan tuple slot from the first row's expression
//! list (`ExecTypeFromExprList`), the scan projection from the plan targetlist, and
//! the per-node exprcontext; it pre-compiles every row's expression list into an
//! `ExprState` array. `ExecValuesScan` drives `ValuesNext`: evaluate the current
//! row's expressions into the scan slot, then qual + project (`ExecScan`).
//! `ExecReScanValuesScan` resets the cursor to the start.
//!
//! Slot ownership mirrors nodeSeqscan: the node OWNS its scan tuple slot; each
//! `ValuesNext` fills it, `exec_scan` aliases it into `ecxt_scantuple`, projects,
//! and returns a borrow of the projection result slot -- no per-tuple deep clone.
//!
//! Async coloring: evaluating VALUES expressions reaches no table AM, but the
//! `exec_proc_node` dispatch is `async`, so `exec_values_scan` matches (it never
//! awaits). PG's `ExecValuesScan` is synchronous.

use std::sync::Arc;

use crate::access::tupdesc::TupleDescData;
use crate::backend::executor::execExpr::exec_init_expr;
use crate::backend::executor::execScan::exec_scan;
use crate::backend::executor::execTuples::{
    exec_store_virtual_tuple, exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL,
};
use crate::backend::executor::execUtils::{
    create_expr_context, exec_assign_projection_info, reset_expr_context,
};
use crate::backend::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};
use crate::executor::tuptable::ExecClearTuple;
use crate::nodes::execnodes::{EState, ExprState, PlanState, ScanState, TupleTableSlot};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::ValuesScan;

/// Run-state pairing the PG `ValuesScanState` with the per-row compiled expression
/// lists. `exprstatelists[i]` is the compiled `ExprState` array for row `i`; each
/// entry evaluates one output column into the scan slot.
pub struct ValuesScanRun {
    pub ss: Box<ScanState>,
    pub rowcontext: Box<crate::nodes::execnodes::ExprContext>,
    /// one compiled column-expr array per VALUES row.
    exprstatelists: Vec<Vec<ExprState>>,
    /// number of VALUES rows.
    array_len: usize,
    /// current 0-based row cursor (`-1` before the first fetch, as a signed guard).
    curr_idx: i64,
}

/// The per-column expression args of a VALUES row. The parse side wraps each row's
/// exprs in a `RowExpr`, so a plan `values_lists` entry is a `Node::RowExpr`.
fn row_args(row: &Node) -> &[Node] {
    match row {
        Node::RowExpr(r) => &r.args,
        _ => unimplemented!("ExecValuesScan: VALUES row is not a RowExpr"),
    }
}

/// PG `ExecTypeFromExprList`: build the scan tuple descriptor from one row's
/// expression list. Column names default to `column1`, `column2`, ... (the parse
/// side sets the same eref names).
fn desc_from_row(exprs: &[Node]) -> crate::access::tupdesc::TupleDesc {
    let len = i32::try_from(exprs.len()).unwrap_or(0);
    let mut desc = TupleDescData::create_template(len);
    for (i, e) in exprs.iter().enumerate() {
        let resno = (i + 1) as i16;
        desc.init_builtin_entry(resno, "", exprType(e), exprTypmod(e), 0);
        desc.init_entry_collation(resno, exprCollation(e));
    }
    Arc::new(desc)
}

/// PG `ExecInitValuesScan`: build the ValuesScanState. The scan slot's rowtype is
/// `ExecTypeFromExprList(linitial(values_lists))`; the projection is built from the
/// plan targetlist over that scan desc; every row's expression list is compiled up
/// front (this port has no SubPlans in VALUES, so the deferred-init special case
/// PG uses for sublists WITH subplans does not apply).
pub fn exec_init_values_scan(node: &ValuesScan, estate: &mut EState<'_>) -> Box<ValuesScanRun> {
    crate::assert!(
        node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none(),
        "ExecInitValuesScan: a scan node is childless"
    );

    let first = node
        .values_lists
        .first()
        .unwrap_or_else(|| unimplemented!("ExecInitValuesScan: VALUES with no rows"));
    let scan_desc = desc_from_row(row_args(first));
    let scan_slot = make_tuple_table_slot(Some(Arc::clone(&scan_desc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::ValuesScan(Box::new(node.clone()))),
        scandesc: Some(Arc::clone(&scan_desc)),
        scanops: Some(&TTS_OPS_VIRTUAL),
        scanopsset: true,
        scanopsfixed: true,
        ..PlanState::default()
    };
    // Two exprcontexts: rowcontext for per-row expr eval, ps_expr_context for the
    // qual/projection (PG builds both via ExecAssignExprContext).
    let rowcontext = create_expr_context(estate);
    ps.ps_expr_context = Some(create_expr_context(estate));

    // ExecInitResultTypeTL + result slot from the plan targetlist.
    let result_desc = exec_type_from_tl(&node.scan.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);
    ps.ps_result_tuple_desc = Some(result_desc);
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;

    // ExecAssignScanProjectionInfo: projection from the plan tlist, input desc =
    // the scan slot's descriptor (so scan Vars resolve).
    exec_assign_projection_info(&mut ps, Some(Arc::clone(&scan_desc)));

    // ExecInitQual: the WHERE qual (None when absent).
    ps.qual = crate::backend::executor::execExpr::exec_init_qual(&node.scan.plan.qual, None);

    // Pre-compile every row's column expressions.
    let natts = scan_desc.natts as usize;
    let exprstatelists: Vec<Vec<ExprState>> = node
        .values_lists
        .iter()
        .map(|row| {
            let exprs = row_args(row);
            crate::assert!(
                exprs.len() == natts,
                "ExecValuesScan: VALUES sublists differ in length"
            );
            exprs
                .iter()
                .map(|e| {
                    *exec_init_expr(Some(e), None)
                        .unwrap_or_else(|| unimplemented!("ExecInitValuesScan: null VALUES expr"))
                })
                .collect()
        })
        .collect();

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(ValuesScanRun {
        ss: Box::new(ss),
        rowcontext,
        array_len: exprstatelists.len(),
        exprstatelists,
        curr_idx: -1,
    })
}

/// PG `ValuesNext`: advance the cursor, evaluate the current row's column
/// expressions into the scan slot, and store it. Returns false at end of data.
fn values_next(run: &mut ValuesScanRun) -> bool {
    if run.curr_idx < run.array_len as i64 {
        run.curr_idx += 1;
    }

    let slot = run
        .ss
        .ss_scan_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ValuesNext: scan node has no scan tuple slot"));
    ExecClearTuple(slot);

    let idx = run.curr_idx;
    if idx < 0 || idx >= run.array_len as i64 {
        return false;
    }

    // ReScanExprContext: reset the per-row context before this row's eval.
    reset_expr_context(&mut run.rowcontext);

    let states = &mut run.exprstatelists[idx as usize];
    for (col, state) in states.iter_mut().enumerate() {
        let evalfunc = state
            .evalfunc
            .unwrap_or_else(|| unimplemented!("ValuesNext: VALUES expr not ready"));
        let mut isnull = false;
        let value = evalfunc(state, &mut run.rowcontext, &mut isnull);
        slot.values[col] = value;
        slot.isnull[col] = isnull;
    }
    exec_store_virtual_tuple(slot);
    true
}

/// PG `ExecValuesScan` -> `ExecScan(ValuesNext)`: fetch the next row into the scan
/// slot, then qual + project. Returns a borrow of the projection result slot, or
/// None at end of data. Async to match the `exec_proc_node` dispatch (never awaits).
pub async fn exec_values_scan(run: &mut ValuesScanRun) -> Option<&mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    loop {
        if !values_next(run) {
            return None;
        }
        if exec_scan(&mut run.ss).is_some() {
            return run
                .ss
                .ps
                .ps_proj_info
                .as_mut()
                .and_then(|p| p.state.resultslot.as_deref_mut());
        }
    }
}

/// PG `ExecEndValuesScan`: owned state drops with the box; clear the result slot.
pub fn exec_end_values_scan(run: &mut ValuesScanRun) {
    if let Some(slot) = run.ss.ps.ps_result_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
}

/// PG `ExecReScanValuesScan`: reset the cursor so the next scan re-reads from the
/// start.
pub fn exec_rescan_values_scan(run: &mut ValuesScanRun) {
    if let Some(slot) = run.ss.ps.ps_result_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
    run.curr_idx = -1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::nodes::makefuncs::{make_const, make_target_entry, make_var};
    use crate::backend::optimizer::plan::planner::standard_planner;
    use crate::catalog::genbki::INT4OID;
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt};
    use crate::nodes::parsenodes::RawStmt;
    use crate::nodes::plannodes::{Plan, Scan};
    use crate::nodes::primnodes::{CoercionForm, RowExpr};
    use crate::parser::parser::RawParseMode;
    use crate::postgres::Int32GetDatum;
    use crate::postgres_ext::{InvalidOid, Oid};

    /// One VALUES row `(v)` as a `Node::RowExpr` of one int4 `Const` (the shape the
    /// parse side produces).
    fn row(v: i32) -> Node {
        let con = make_const(INT4OID, -1, InvalidOid, 4, Int32GetDatum(v), false, true);
        Node::RowExpr(Box::new(RowExpr {
            args: vec![Node::Const(Box::new(con))],
            row_typeid: Oid::new(2249),
            row_format: CoercionForm::IMPLICIT_CAST,
            colnames: Vec::new(),
            location: -1,
        }))
    }

    fn empty_plan(tlist: Vec<Node>) -> Plan {
        Plan {
            disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
            parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
            targetlist: tlist, qual: Vec::new(), lefttree: None, righttree: None,
            init_plan: Vec::new(), ext_param: None, all_param: None,
        }
    }

    /// A `VALUES (1),(2),(3)` scan node with a single-Var passthrough targetlist.
    fn valuesscan_node() -> ValuesScan {
        let var = make_var(1, 1, INT4OID, -1, InvalidOid, 0);
        let tle = make_target_entry(Some(Node::Var(Box::new(var))), 1, Some("column1".into()), false);
        ValuesScan {
            scan: Scan {
                plan: empty_plan(vec![Node::TargetEntry(Box::new(tle))]),
                scanrelid: 1,
            },
            values_lists: vec![row(1), row(2), row(3)],
        }
    }

    /// Execute a `VALUES (1),(2),(3)` ValuesScan node and collect the column.
    #[tokio::test(flavor = "multi_thread")]
    async fn valuesscan_yields_three_rows() {
        let mut estate = EState::default();
        let mut run = exec_init_values_scan(&valuesscan_node(), &mut estate);

        let mut out = Vec::new();
        loop {
            let Some(slot) = exec_values_scan(&mut run).await else { break };
            out.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
        }
        assert_eq!(out, vec![1, 2, 3]);

        // Rescan replays from the start.
        exec_rescan_values_scan(&mut run);
        let mut again = Vec::new();
        loop {
            let Some(slot) = exec_values_scan(&mut run).await else { break };
            again.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
        }
        assert_eq!(again, vec![1, 2, 3]);
        exec_end_values_scan(&mut run);
    }

    // ----- end-to-end (parse -> analyze -> rewrite -> plan -> execute) harness -----
    // Mirrors analyze.rs's test harness: a bootstrapped catalog warms the syscache
    // that transformValuesClause's common-type resolution needs.

    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-valuesscan-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 256,
            ..Default::default()
        })
    }

    async fn in_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use crate::backend::access::transam::xloginsert::with_insertion;
        use crate::backend::catalog::indexing::scope_async as catalog_index_scope;
        use crate::backend::utils::cache::catcache::scope_async as catcache_scope;
        use crate::backend::utils::cache::relcache::scope_async as relcache_scope;
        use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};

        let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
        sess.set_database_id(DB_OID);
        sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");

        let body = Box::pin(catalog_index_scope(Box::pin(relcache_scope(Box::pin(f(shared))))));
        let body = Box::pin(catcache_scope(body));
        let body = Box::pin(with_insertion(body));
        let body = Box::pin(combocid_scope(body));
        let body = Box::pin(snapmgr_scope(body));
        let body = Box::pin(crate::backend::access::transam::xact::xact_scope(body));
        crate::session::scope(
            sess,
            crate::backend::utils::resowner::resowner::scope(owner, body),
        )
        .await
    }

    async fn init_db(shared: &Arc<SharedState>) {
        use crate::backend::access::transam::xact::{
            CommandCounterIncrement, GetCurrentCommandId, StartTransactionCommand,
        };
        use crate::backend::utils::time::snapmgr::{
            GetTransactionSnapshot, InvalidateCatalogSnapshot, PopActiveSnapshot, PushActiveSnapshot,
        };

        StartTransactionCommand(shared).await;
        let mut snap = GetTransactionSnapshot(shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);
        crate::backend::bootstrap::bootstrap::bootstrap_catalogs(shared).await;
        CommandCounterIncrement();
        InvalidateCatalogSnapshot();
        PopActiveSnapshot();
        let mut snap = GetTransactionSnapshot(shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);
    }

    /// Bare `VALUES (1),(2),(3)` plans to a top-level `ValuesScan` (3 rows) and
    /// executes to the three rows through the real planner + executor.
    #[tokio::test(flavor = "multi_thread")]
    async fn bare_values_plans_and_executes() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;

            let sql = "VALUES (1),(2),(3)";
            let mut list =
                crate::backend::parser::parser::raw_parser(sql, RawParseMode::Default);
            let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
            let rs: RawStmt = *rs;
            let q =
                crate::backend::parser::analyze::parse_analyze_fixedparams_async(&shared, &rs, sql, &[], 0)
                    .await;
            // query_rewrite is a pass-through for a bare VALUES (no views/rules);
            // fireRIRrules' range-table expansion is not yet translated, so plan the
            // analyzed Query directly (as analyze.rs's own plan() helper does for the
            // non-rewriter path).
            let mut parse = *q;
            let stmt = standard_planner(&mut parse, sql, 0, None);

            // Planning: the top plan node is a ValuesScan carrying three rows.
            assert!(
                matches!(stmt.plan_tree, Node::ValuesScan(_)),
                "top plan is a ValuesScan, got {:?}",
                stmt.plan_tree
            );
            let Node::ValuesScan(vs) = &stmt.plan_tree else { unreachable!() };
            assert_eq!(vs.values_lists.len(), 3);

            // Execution: init + drive the node to completion -> rows 1, 2, 3.
            let mut estate = EState::default();
            let node = (**vs).clone();
            let mut run = exec_init_values_scan(&node, &mut estate);
            let mut out = Vec::new();
            loop {
                let Some(slot) = exec_values_scan(&mut run).await else { break };
                out.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
            }
            exec_end_values_scan(&mut run);
            assert_eq!(out, vec![1, 2, 3]);
        }))
        .await;
    }
}
