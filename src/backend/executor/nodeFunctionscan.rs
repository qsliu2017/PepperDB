//! FunctionScan node executor. Translated from
//! backend/executor/nodeFunctionscan.c + the ValuePerCall SRF driver of
//! backend/executor/execSRF.c (`ExecMakeTableFunctionResult`).
//!
//! `ExecInitFunctionScan` builds the scan tuple slot from the function's result
//! tupdesc, the scan projection from the plan targetlist, the per-node
//! exprcontext, and pre-compiles the function's argument expressions.
//! `FunctionNext` materializes the set on first call: it runs the function's
//! ValuePerCall protocol (evaluating args once, then calling the function
//! repeatedly, collecting `(Datum, isnull)` rows) into a tuplestore, then fetches
//! rows from it. `ExecFunctionScan` drives `FunctionNext` through `ExecScan`
//! (qual + projection). `ExecReScanFunctionScan` drops the tuplestore so the next
//! scan re-runs the function.
//!
//! MILESTONE (step 08): the SIMPLE case only -- one function, no WITH ORDINALITY.
//! The scan result tupdesc is the function's result tupdesc; results are fetched
//! straight into the scan slot. Multiple functions, the ordinality column, and
//! LATERAL argument Vars are grow guards (rules.md s4).

#![allow(clippy::similar_names, reason = "fcinfo/flinfo mirror PG's identifiers")]

use std::sync::Arc;

use crate::access::tupdesc::{TupleDesc, TupleDescData};
use crate::backend::executor::execExpr::exec_init_expr;
use crate::backend::executor::execScan::exec_scan;
use crate::backend::executor::execTuples::{
    exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL,
};
use crate::backend::executor::execUtils::{
    create_expr_context, exec_assign_projection_info, reset_expr_context,
};
use crate::backend::nodes::nodeFuncs::exprCollation;
use crate::backend::utils::fmgr::fmgr::fmgr_info;
use crate::backend::utils::sort::tuplestore::{
    tuplestore_begin_heap, tuplestore_gettupleslot, tuplestore_putvalues, tuplestore_rescan,
    Tuplestorestate,
};
use crate::executor::tuptable::ExecClearTuple;
use crate::fmgr::{FmgrInfo, FunctionCallInfoBaseData, InitFunctionCallInfoData};
use crate::funcapi::{get_expr_result_type, TypeFuncClass};
use crate::nodes::execnodes::{
    EState, ExprDoneCond, ExprState, PlanState, ReturnSetInfo, ScanState, SetFunctionReturnMode,
    TupleTableSlot,
};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::FunctionScan;
use crate::postgres::{Datum, NullableDatum};
use crate::postgres_ext::{InvalidOid, Oid};

/// Run-state for a (simple) FunctionScan: the ScanState, the per-tuple
/// exprcontext, the function's result tupdesc, the compiled argument expressions,
/// and the materialized tuplestore (None until the first fetch).
pub struct FunctionScanRun {
    pub ss: Box<ScanState>,
    /// the funcexpr (a FuncExpr) driving the scan.
    funcexpr: Node,
    /// resolved fmgr info for the function.
    flinfo: Box<FmgrInfo>,
    /// compiled argument expressions (evaluated once, before the ValuePerCall loop).
    arg_states: Vec<ExprState>,
    /// the function's result tupdesc (== the scan tupdesc in the simple case).
    tupdesc: TupleDesc,
    /// materialized set (None until FunctionNext runs the function).
    tstore: Option<Box<Tuplestorestate>>,
}

/// The single funcexpr node of a (simple) function RTE.
fn single_funcexpr(node: &FunctionScan) -> &Node {
    let Some(Node::RangeTblFunction(rtf)) = node.functions.first() else {
        unimplemented!("ExecInitFunctionScan: function list is not a RangeTblFunction");
    };
    rtf.funcexpr
        .as_ref()
        .unwrap_or_else(|| unimplemented!("ExecInitFunctionScan: RangeTblFunction has no funcexpr"))
}

/// Build the function's result tupdesc (simple case). Scalar -> a one-column desc
/// named "column"; composite/record -> the resolved rowtype tupdesc.
fn function_result_tupdesc(funcexpr: &Node) -> TupleDesc {
    let info = get_expr_result_type(funcexpr);
    match info.class {
        TypeFuncClass::Composite | TypeFuncClass::CompositeDomain => info
            .result_tuple_desc
            .unwrap_or_else(|| unimplemented!("FunctionScan: composite result has no tupdesc")),
        TypeFuncClass::Scalar => {
            let funcrettype = info
                .result_type_id
                .unwrap_or_else(|| unimplemented!("FunctionScan: scalar result has no type OID"));
            let mut desc = TupleDescData::create_template(1);
            desc.init_builtin_entry(1, "column", funcrettype, -1, 0);
            desc.init_entry_collation(1, exprCollation(funcexpr));
            Arc::new(desc)
        }
        TypeFuncClass::Record | TypeFuncClass::Other => {
            unimplemented!("FunctionScan: function has unsupported (record/pseudo) return type")
        }
    }
}

/// PG `ExecInitFunctionScan` (simple case): build the FunctionScanState.
pub fn exec_init_function_scan(node: &FunctionScan, estate: &mut EState<'_>) -> Box<FunctionScanRun> {
    crate::assert!(
        node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none(),
        "ExecInitFunctionScan: a scan node is childless"
    );
    if node.funcordinality {
        unimplemented!("ExecInitFunctionScan: WITH ORDINALITY");
    }
    if node.functions.len() != 1 {
        unimplemented!("ExecInitFunctionScan: multiple functions (ROWS FROM)");
    }

    let funcexpr = single_funcexpr(node).clone();
    let Node::FuncExpr(func) = &funcexpr else {
        unimplemented!("ExecInitFunctionScan: function-in-FROM expr is not a FuncExpr (constant-folded)");
    };

    // Resolve the function and compile its argument expressions. `flinfo.expr` is
    // the FuncExpr so a composite-returning function's `get_call_result_type`
    // (fcinfo->flinfo->fn_expr) can resolve its result rowtype.
    let mut flinfo = empty_flinfo();
    fmgr_info(func.funcid, &mut flinfo);
    flinfo.expr = Some(Box::new(funcexpr.clone()));
    let arg_states: Vec<ExprState> = func
        .args
        .iter()
        .map(|a| {
            *exec_init_expr(Some(a), None)
                .unwrap_or_else(|| unimplemented!("ExecInitFunctionScan: null function argument"))
        })
        .collect();

    // The scan tupdesc == the function's result tupdesc (simple case).
    let tupdesc = function_result_tupdesc(&funcexpr);
    let scan_slot = make_tuple_table_slot(Some(Arc::clone(&tupdesc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::FunctionScan(Box::new(node.clone()))),
        scandesc: Some(Arc::clone(&tupdesc)),
        scanops: Some(&TTS_OPS_VIRTUAL),
        scanopsset: true,
        scanopsfixed: true,
        ..PlanState::default()
    };
    ps.ps_expr_context = Some(create_expr_context(estate));

    // Result slot + projection from the plan targetlist.
    let result_desc = exec_type_from_tl(&node.scan.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);
    ps.ps_result_tuple_desc = Some(result_desc);
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;

    exec_assign_projection_info(&mut ps, Some(Arc::clone(&tupdesc)));
    ps.qual = crate::backend::executor::execExpr::exec_init_qual(&node.scan.plan.qual, None);

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(FunctionScanRun {
        ss: Box::new(ss),
        funcexpr,
        flinfo: Box::new(flinfo),
        arg_states,
        tupdesc,
        tstore: None,
    })
}

/// An empty FmgrInfo (the fields `fmgr_info` fills).
fn empty_flinfo() -> FmgrInfo {
    FmgrInfo {
        fn_addr: None,
        oid: Oid::new(0),
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: (),
        expr: None,
    }
}

/// PG `ExecMakeTableFunctionResult` (ValuePerCall path): evaluate the function's
/// argument list once, then call the function repeatedly under a `ReturnSetInfo`
/// in ValuePerCall mode, collecting each returned Datum into a tuplestore. A
/// non-set function yields a single row (or an all-null row on NULL). Returns the
/// filled tuplestore.
#[allow(clippy::too_many_lines, reason = "1:1 port of ExecMakeTableFunctionResult's ValuePerCall loop")]
#[allow(clippy::unnecessary_box_returns, reason = "tuplestore_begin_heap returns Box<Tuplestorestate>; the store is threaded by Box in this port")]
fn exec_make_table_function_result(run: &mut FunctionScanRun) -> Box<Tuplestorestate> {
    use crate::catalog::genbki::RECORDOID;

    let Node::FuncExpr(func) = &run.funcexpr else {
        unimplemented!("ExecMakeTableFunctionResult: setexpr is not a FuncExpr");
    };
    let funcrettype = func.funcresulttype;
    let returns_set = run.flinfo.retset;
    // A rowtype (composite/record) result returns whole tuples; a scalar result is
    // stored as a single-column row.
    let returns_tuple = funcrettype == RECORDOID || run.tupdesc.natts > 1;

    let nargs = run.arg_states.len();
    let mut fcinfo = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: func.inputcollid,
        isnull: false,
        nargs: 0,
        args: vec![NullableDatum { value: Datum(0), isnull: true }; nargs],
    };

    // Evaluate the argument list into the fcinfo (once).
    let econtext = run
        .ss
        .ps
        .ps_expr_context
        .as_mut()
        .unwrap_or_else(|| unimplemented!("FunctionScan: no exprcontext"));
    reset_expr_context(econtext);
    let mut any_arg_null = false;
    for (i, state) in run.arg_states.iter_mut().enumerate() {
        let evalfunc = state
            .evalfunc
            .unwrap_or_else(|| unimplemented!("FunctionScan: argument expr not compiled"));
        let mut isnull = false;
        let value = evalfunc(state, econtext, &mut isnull);
        fcinfo.args[i] = NullableDatum { value, isnull };
        any_arg_null |= isnull;
    }

    let mut tstore = tuplestore_begin_heap(false, false, work_mem_kb());

    // Strict function with a NULL argument: no rows (empty set) / one null row.
    if run.flinfo.strict && any_arg_null {
        if !returns_set {
            put_null_row(&mut tstore, &run.tupdesc);
        }
        return tstore;
    }

    // The function needs a live flinfo + ReturnSetInfo across the calls.
    let flinfo = std::mem::replace(&mut run.flinfo, Box::new(empty_flinfo()));
    InitFunctionCallInfoData(
        &mut fcinfo,
        Some(flinfo),
        i16::try_from(nargs).unwrap_or(0),
        func.inputcollid,
        None,
        Some(Box::new(rsinfo(&run.tupdesc))),
    );

    let fn_addr = fcinfo
        .flinfo
        .as_ref()
        .and_then(|fi| fi.fn_addr)
        .unwrap_or_else(|| unimplemented!("FunctionScan: function has no bound implementation"));

    let mut produced = false;
    loop {
        crate::miscadmin::check_for_interrupts();
        if let Some(rsi) = fcinfo.resultinfo.as_mut() {
            rsi.is_done = Some(ExprDoneCond::ExprSingleResult);
        }
        fcinfo.isnull = false;
        let result = fn_addr(&mut fcinfo);
        let is_done = fcinfo
            .resultinfo
            .as_ref()
            .and_then(|rsi| rsi.is_done)
            .unwrap_or(ExprDoneCond::ExprSingleResult);

        if is_done == ExprDoneCond::ExprEndResult {
            break;
        }

        // Store this result item.
        if returns_tuple {
            if fcinfo.isnull {
                put_null_row(&mut tstore, &run.tupdesc);
            } else {
                // The record Datum is a leaked HeapTupleData (funcapi's rowtype-Datum
                // representation); deform it against the expected desc and store the row.
                // SAFETY: `result` is a record Datum from a composite-returning function
                // (HeapTupleGetDatum); reclaim it exactly once.
                let tuple = unsafe { crate::funcapi::datum_get_heap_tuple(result) };
                let (values, nulls) = unsafe {
                    crate::backend::access::common::heaptuple::heap_deform_tuple(&tuple, &run.tupdesc)
                };
                tuplestore_putvalues(&mut tstore, &run.tupdesc, &values, &nulls);
                // The deformed pass-by-ref Datums point INTO the tuple body; keep it
                // alive for the query (the tuplestore + downstream output read it).
                // A per-row record is small and query-scoped, so the leak is bounded.
                Box::leak(tuple);
            }
        } else {
            tuplestore_putvalues(&mut tstore, &run.tupdesc, &[result], &[fcinfo.isnull]);
        }
        produced = true;

        if is_done != ExprDoneCond::ExprMultipleResult {
            break;
        }
        if !returns_set {
            crate::elog!(
                crate::utils::elog::ERROR,
                "table-function protocol for value-per-call mode was not followed".to_string()
            );
        }
    }

    // Give the flinfo back (its `extra` was reclaimed by SRF_RETURN_DONE).
    if let Some(fi) = fcinfo.flinfo.take() {
        run.flinfo = fi;
    }

    // Empty non-set result: a single all-null row.
    if !produced && !returns_set {
        put_null_row(&mut tstore, &run.tupdesc);
    }

    tstore
}

/// A `ReturnSetInfo` for a ValuePerCall SRF call over `expected_desc`.
fn rsinfo(expected_desc: &TupleDesc) -> ReturnSetInfo {
    ReturnSetInfo {
        econtext: None,
        expected_desc: Some(Arc::clone(expected_desc)),
        allowed_modes: i32::from(
            (SetFunctionReturnMode::VALUE_PER_CALL | SetFunctionReturnMode::MATERIALIZE).bits(),
        ),
        return_mode: SetFunctionReturnMode::VALUE_PER_CALL,
        is_done: Some(ExprDoneCond::ExprSingleResult),
        set_result: None,
        set_desc: None,
    }
}

/// Store a row of all NULLs (the empty non-set-function result).
fn put_null_row(tstore: &mut Tuplestorestate, tupdesc: &TupleDesc) {
    let natts = tupdesc.natts as usize;
    let values = vec![Datum(0); natts];
    let nulls = vec![true; natts];
    tuplestore_putvalues(tstore, tupdesc, &values, &nulls);
}

/// PG `work_mem` in kilobytes (the tuplestore allowance). This port has no GUC
/// yet; use PG's default 4 MB.
fn work_mem_kb() -> i32 {
    4096
}

/// PG `FunctionNext` (simple case): on first call, materialize the function's
/// results into the tuplestore; then fetch the next tuple from it into the scan
/// slot. Returns false at end of data.
fn function_next(run: &mut FunctionScanRun) -> bool {
    if run.tstore.is_none() {
        let mut tstore = exec_make_table_function_result(run);
        tuplestore_rescan(&mut tstore);
        run.tstore = Some(tstore);
    }

    let slot = run
        .ss
        .ss_scan_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("FunctionNext: scan node has no scan tuple slot"));
    let tstore = run
        .tstore
        .as_mut()
        .unwrap_or_else(|| unreachable!("tuplestore just built"));
    tuplestore_gettupleslot(tstore, true, false, slot)
}

/// PG `ExecFunctionScan` -> `ExecScan(FunctionNext)`: fetch the next row into the
/// scan slot, then qual + project. Async to match the `exec_proc_node` dispatch
/// (never awaits -- the function ran synchronously into the tuplestore).
#[allow(clippy::unused_async, reason = "async colors to match the exec_proc_node dispatch (rules.md s5); the SRF ran synchronously into the tuplestore")]
pub async fn exec_function_scan(run: &mut FunctionScanRun) -> Option<&mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    loop {
        if !function_next(run) {
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

/// PG `ExecEndFunctionScan`: owned state drops with the box; clear the result slot.
pub fn exec_end_function_scan(run: &mut FunctionScanRun) {
    if let Some(slot) = run.ss.ps.ps_result_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
}

/// PG `ExecReScanFunctionScan`: drop the tuplestore so the next scan re-runs the
/// function from the start.
pub fn exec_rescan_function_scan(run: &mut FunctionScanRun) {
    if let Some(slot) = run.ss.ps.ps_result_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
    run.tstore = None;
    let _ = InvalidOid;
}
