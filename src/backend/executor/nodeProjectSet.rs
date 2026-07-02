//! ProjectSet node executor: evaluate a targetlist containing set-returning
//! functions. Translated from backend/executor/nodeProjectSet.c + the
//! targetlist-SRF driver of backend/executor/execSRF.c
//! (`ExecInitFunctionResultSet` / `ExecMakeFunctionResultSet`).
//!
//! The planner guarantees every SRF sits at the TOP level of a targetlist item
//! (create_project_set_plan rejects nesting). `ExecProjectSet` pulls one input
//! tuple from the outer plan, then `ExecProjectSRF` emits one output row per
//! ValuePerCall SRF result, re-evaluating the plain (non-SRF) tlist expressions
//! for each row; when several SRFs are present the shorter ones pad with NULLs
//! until the longest is exhausted (`elemdone` + the `continuing` protocol).
//!
//! MILESTONE (plan 004, unblocker pass C): ValuePerCall SRFs only (the
//! `generate_series` family / funcapi FuncCallContext protocol from step 08);
//! materialize-mode SRFs are a grow guard. Memory-context resets (`argcontext`,
//! per-tuple) are no-ops in this port.
//!
//! Slot ownership: the run-state owns the result slot (in `ps`) and the outer
//! snapshot slot the child tuple is copied into; `ecxt_outertuple` keeps
//! pointing at that snapshot across calls so the pending-SRF continuation and
//! the per-row plain-expression re-evaluation read the same input tuple, as in
//! C (where the slot pointer simply stays set).

#![allow(clippy::similar_names, reason = "fcinfo/flinfo mirror PG's identifiers")]

use std::sync::Arc;

use crate::backend::executor::execExpr::exec_init_expr;
use crate::backend::executor::execProcnode::{
    exec_end_node, exec_proc_node, result_type_of, PlanStateNode,
};
use crate::backend::executor::execTuples::{
    exec_store_virtual_tuple, exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL,
};
use crate::backend::executor::execUtils::{create_expr_context, reset_expr_context};
use crate::backend::executor::nodeFunctionscan::function_result_tupdesc;
use crate::backend::executor::nodeUnique::snapshot_slot;
use crate::backend::utils::fmgr::fmgr::fmgr_info;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::fmgr::{FmgrInfo, FunctionCallInfoBaseData, InitFunctionCallInfoData};
use crate::nodes::execnodes::{
    EState, ExprContext, ExprDoneCond, ExprState, PlanState, ProjectSetState, ReturnSetInfo,
    SetFunctionReturnMode,
};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::ProjectSet;
use crate::postgres::{Datum, NullableDatum};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// Run-state pairing the PG `ProjectSetState` with its child plan-state and the
/// per-tlist-entry element states (PG's `elems` array of `ExprState` /
/// `SetExprState`, typed here instead of downcast).
pub struct ProjectSetRun<'rel> {
    pub state: Box<ProjectSetState>,
    pub child: Box<PlanStateNode<'rel>>,
    elems: Vec<ElemState>,
}

/// One targetlist element: a set-returning function driven by the ValuePerCall
/// protocol, or a plain expression evaluated once per output row.
enum ElemState {
    /// PG `SetExprState` (IsA(elem, SetExprState) in ExecProjectSRF).
    Srf(Box<SetElem>),
    /// PG `ExprState` (a non-SRF tlist expression).
    Expr(Box<ExprState>),
}

/// The SRF half of PG's `SetExprState`: the resolved function (inside `fcinfo`),
/// its compiled argument expressions, and the cross-row call state.
struct SetElem {
    /// compiled argument expressions (evaluated once per input tuple).
    arg_states: Vec<ExprState>,
    /// the persistent call frame: owns the FmgrInfo (whose `extra` carries the
    /// funcapi FuncCallContext across calls) and the ReturnSetInfo.
    fcinfo: FunctionCallInfoBaseData,
    /// function is strict (NULL argument -> empty set).
    strict: bool,
    /// PG `setArgsValid`: the args in fcinfo are live from an unfinished set.
    args_valid: bool,
}

/// PG `ExecInitProjectSet`: build the ProjectSetState over an initialized child.
pub fn exec_init_project_set<'rel>(
    node: &ProjectSet,
    estate: &mut EState<'rel>,
    child: PlanStateNode<'rel>,
) -> Box<ProjectSetRun<'rel>> {
    crate::assert!(node.plan.qual.is_empty(), "ExecInitProjectSet: no qual on ProjectSet");

    let outer_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitProjectSet: child has no result descriptor"));

    // Result slot / rowtype from the SRF-bearing targetlist
    // (ExecInitResultTupleSlotTL over &TTSOpsVirtual).
    let result_desc = exec_type_from_tl(&node.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::ProjectSet(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_expr_context = Some(create_expr_context(estate));
    ps.ps_result_tuple_desc = Some(result_desc);
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;

    // The owned outer snapshot slot Var evaluation reads via ecxt_outertuple.
    if let Some(ec) = ps.ps_expr_context.as_mut() {
        ec.ecxt_outertuple = Some(make_tuple_table_slot(Some(outer_desc), &TTS_OPS_VIRTUAL));
    }

    // Compile each tlist expression separately (PG: "can't use
    // ExecBuildProjectionInfo, since that doesn't deal with SRFs").
    let nelems = node.plan.targetlist.len();
    let mut elems: Vec<ElemState> = Vec::with_capacity(nelems);
    for n in &node.plan.targetlist {
        let Node::TargetEntry(te) = n else {
            unimplemented!("ExecInitProjectSet: tlist entry is not a TargetEntry");
        };
        let expr = te
            .expr
            .as_ref()
            .unwrap_or_else(|| unimplemented!("ExecInitProjectSet: empty target expr"));
        let is_srf = matches!(expr, Node::FuncExpr(f) if f.funcretset)
            || matches!(expr, Node::OpExpr(o) if o.opretset);
        if is_srf {
            elems.push(ElemState::Srf(Box::new(exec_init_function_result_set(expr))));
        } else {
            crate::assert!(
                !crate::backend::nodes::nodeFuncs::expression_returns_set(expr),
                "ExecInitProjectSet: non-top-level SRF (planner should have rejected)"
            );
            let es = exec_init_expr(Some(expr), None)
                .unwrap_or_else(|| unreachable!("target expr is present"));
            elems.push(ElemState::Expr(es));
        }
    }

    let state = ProjectSetState {
        ps,
        elemdone: vec![ExprDoneCond::ExprSingleResult; nelems],
        nelems: i32::try_from(nelems).unwrap_or(0),
        pending_srf_tuples: false,
        ..ProjectSetState::default()
    };

    Box::new(ProjectSetRun { state: Box::new(state), child: Box::new(child), elems })
}

/// PG `ExecInitFunctionResultSet` (execSRF.c): resolve the SRF, compile its
/// argument expressions, and build the persistent fcinfo + ReturnSetInfo.
fn exec_init_function_result_set(expr: &Node) -> SetElem {
    let (funcid, inputcollid, args) = match expr {
        Node::FuncExpr(f) => (f.funcid, f.inputcollid, &f.args),
        Node::OpExpr(o) => (o.opfuncid, o.inputcollid, &o.args),
        other => unimplemented!("ExecInitFunctionResultSet: unrecognized node type {other:?}"),
    };

    // init_sexpr: fmgr_info + fn_expr (so a composite SRF can resolve its
    // rowtype through get_call_result_type).
    let mut flinfo = empty_flinfo();
    fmgr_info(funcid, &mut flinfo);
    flinfo.expr = Some(Box::new(expr.clone()));
    crate::assert!(flinfo.retset, "ExecInitFunctionResultSet: selected function returns set");
    let strict = flinfo.strict;

    let arg_states: Vec<ExprState> = args
        .iter()
        .map(|a| {
            *exec_init_expr(Some(a), None)
                .unwrap_or_else(|| unimplemented!("ExecInitFunctionResultSet: null SRF argument"))
        })
        .collect();

    // needDescForSRF: the expectedDesc funcapi-mode SRFs may consult.
    let expected_desc = function_result_tupdesc(expr);

    let nargs = arg_states.len();
    let mut fcinfo = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: inputcollid,
        isnull: false,
        nargs: 0,
        args: vec![NullableDatum { value: Datum(0), isnull: true }; nargs],
    };
    InitFunctionCallInfoData(
        &mut fcinfo,
        Some(Box::new(flinfo)),
        i16::try_from(nargs).unwrap_or(0),
        inputcollid,
        None,
        Some(Box::new(ReturnSetInfo {
            econtext: None,
            expected_desc: Some(expected_desc),
            allowed_modes: i32::from(
                (SetFunctionReturnMode::VALUE_PER_CALL | SetFunctionReturnMode::MATERIALIZE)
                    .bits(),
            ),
            return_mode: SetFunctionReturnMode::VALUE_PER_CALL,
            is_done: Some(ExprDoneCond::ExprSingleResult),
            set_result: None,
            set_desc: None,
        })),
    );

    SetElem { arg_states, fcinfo, strict, args_valid: false }
}

/// A zeroed FmgrInfo for the init-time `fmgr_info` lookup.
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

/// PG `ExecProjectSet`: return the next tuple of the SRF-expanded projection.
/// Continues an in-progress set first; otherwise pulls the next input tuple from
/// the outer plan and projects it (looping past inputs that yield no rows).
pub async fn exec_project_set<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut ProjectSetRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    // Still projecting rows from the previous input tuple?
    if run.state.pending_srf_tuples {
        let produced = {
            let state = &mut *run.state;
            let econtext = state
                .ps
                .ps_expr_context
                .as_deref_mut()
                .unwrap_or_else(|| unimplemented!("ExecProjectSet: no exprcontext"));
            reset_expr_context(econtext);
            let result_slot = state
                .ps
                .ps_result_tuple_slot
                .as_deref_mut()
                .unwrap_or_else(|| unimplemented!("ExecProjectSet: no result slot"));
            exec_project_srf(
                &mut run.elems,
                &mut state.elemdone,
                &mut state.pending_srf_tuples,
                econtext,
                result_slot,
                true,
            )
        };
        if produced {
            return run.state.ps.ps_result_tuple_slot.as_deref_mut();
        }
    }

    // Get another input tuple and project SRFs from it.
    loop {
        // (C resets argcontext here; memory contexts are no-ops in this port.)
        let slot = Box::pin(exec_proc_node(shared, &mut run.child)).await?;
        let (vals, nulls) = snapshot_slot(slot);

        let produced = {
            let state = &mut *run.state;
            let econtext = state
                .ps
                .ps_expr_context
                .as_deref_mut()
                .unwrap_or_else(|| unimplemented!("ExecProjectSet: no exprcontext"));
            // econtext->ecxt_outertuple = outerTupleSlot (the owned snapshot).
            let outer = econtext
                .ecxt_outertuple
                .as_deref_mut()
                .unwrap_or_else(|| unimplemented!("ExecProjectSet: no outer snapshot slot"));
            store_into(outer, &vals, &nulls);
            let result_slot = state
                .ps
                .ps_result_tuple_slot
                .as_deref_mut()
                .unwrap_or_else(|| unimplemented!("ExecProjectSet: no result slot"));
            exec_project_srf(
                &mut run.elems,
                &mut state.elemdone,
                &mut state.pending_srf_tuples,
                econtext,
                result_slot,
                false,
            )
        };
        // Return the tuple unless the projection produced no rows (an empty
        // set), in which case loop back for the next input tuple.
        if produced {
            return run.state.ps.ps_result_tuple_slot.as_deref_mut();
        }
    }
}

/// PG `ExecProjectSRF`: project the targetlist once. `continuing` = keep
/// emitting rows for the SAME input tuple (exhausted SRFs pad with NULLs);
/// otherwise start the SRFs afresh for a new input tuple. Returns whether an
/// output row was produced (false = every SRF returned an empty/ended set).
fn exec_project_srf(
    elems: &mut [ElemState],
    elemdone: &mut [ExprDoneCond],
    pending_srf_tuples: &mut bool,
    econtext: &mut ExprContext,
    result_slot: &mut TupleTableSlot,
    continuing: bool,
) -> bool {
    ExecClearTuple(result_slot);

    // Assume no further tuples unless an ExprMultipleResult shows up.
    *pending_srf_tuples = false;

    let mut hasresult = false;
    for (i, elem) in elems.iter_mut().enumerate() {
        match elem {
            ElemState::Srf(se) => {
                if continuing && elemdone[i] == ExprDoneCond::ExprEndResult {
                    // This SRF is exhausted: pad with NULLs while others emit.
                    result_slot.values[i] = Datum(0);
                    result_slot.isnull[i] = true;
                    continue;
                }
                let (value, isnull, isdone) = exec_make_function_result_set(se, econtext);
                elemdone[i] = isdone;
                if isdone != ExprDoneCond::ExprEndResult {
                    hasresult = true;
                }
                if isdone == ExprDoneCond::ExprMultipleResult {
                    *pending_srf_tuples = true;
                }
                result_slot.values[i] = value;
                result_slot.isnull[i] = isnull;
            }
            ElemState::Expr(es) => {
                // Non-SRF tlist expression: evaluate normally (per output row).
                let evalfunc = es
                    .evalfunc
                    .unwrap_or_else(|| unimplemented!("ExecProjectSRF: expr not compiled"));
                let mut isnull = false;
                let value = evalfunc(es, econtext, &mut isnull);
                result_slot.values[i] = value;
                result_slot.isnull[i] = isnull;
                elemdone[i] = ExprDoneCond::ExprSingleResult;
            }
        }
    }

    // If all SRFs returned ExprEndResult, no row is produced.
    if hasresult {
        exec_store_virtual_tuple(result_slot);
        return true;
    }
    false
}

/// PG `ExecMakeFunctionResultSet` (execSRF.c, ValuePerCall path): evaluate the
/// SRF's arguments (unless continuing an unfinished set), call the function
/// under the ValuePerCall protocol, and return (value, isnull, isDone). A strict
/// SRF with a NULL argument yields an empty set without calling the function.
fn exec_make_function_result_set(
    se: &mut SetElem,
    econtext: &mut ExprContext,
) -> (Datum, bool, ExprDoneCond) {
    // check_stack_depth() analogue: the interrupt poll.
    crate::miscadmin::check_for_interrupts();

    // Skip argument evaluation if continuing the previous set's calls
    // (ValuePerCall SRFs may re-read the args on every returned row).
    if se.args_valid {
        se.args_valid = false; // may be set again below
    } else {
        for (i, st) in se.arg_states.iter_mut().enumerate() {
            let evalfunc = st
                .evalfunc
                .unwrap_or_else(|| unimplemented!("ExecMakeFunctionResultSet: arg not compiled"));
            let mut isnull = false;
            let value = evalfunc(st, econtext, &mut isnull);
            se.fcinfo.args[i] = NullableDatum { value, isnull };
        }
    }

    // Prepare the resultinfo node for this call.
    {
        let rsi = se
            .fcinfo
            .resultinfo
            .as_mut()
            .unwrap_or_else(|| unreachable!("rsinfo installed at init"));
        rsi.return_mode = SetFunctionReturnMode::VALUE_PER_CALL;
        rsi.is_done = Some(ExprDoneCond::ExprSingleResult);
        rsi.set_result = None;
        rsi.set_desc = None;
    }

    // Strict function with a NULL argument: the result is an empty set.
    if se.strict && se.fcinfo.args.iter().any(|a| a.isnull) {
        return (Datum(0), true, ExprDoneCond::ExprEndResult);
    }

    se.fcinfo.isnull = false;
    let fn_addr = se
        .fcinfo
        .flinfo
        .as_ref()
        .and_then(|fi| fi.fn_addr)
        .unwrap_or_else(|| {
            unimplemented!("ExecMakeFunctionResultSet: SRF has no bound implementation")
        });
    let result = fn_addr(&mut se.fcinfo);
    let isnull = se.fcinfo.isnull;

    let rsi = se
        .fcinfo
        .resultinfo
        .as_ref()
        .unwrap_or_else(|| unreachable!("rsinfo installed at init"));
    if rsi.return_mode != SetFunctionReturnMode::VALUE_PER_CALL {
        // Materialize-mode SRFs (funcResultStore) grow later (rules.md s4).
        unimplemented!("ExecMakeFunctionResultSet: materialize-mode SRF not yet translated");
    }
    let isdone = rsi.is_done.unwrap_or(ExprDoneCond::ExprSingleResult);

    // Keep the argument values for the next call of an unfinished set.
    if isdone == ExprDoneCond::ExprMultipleResult {
        se.args_valid = true;
    }

    (result, isnull, isdone)
}

/// Store deformed values into an owned virtual slot.
fn store_into(slot: &mut TupleTableSlot, values: &[Datum], isnull: &[bool]) {
    ExecClearTuple(slot);
    let n = values.len();
    slot.values[..n].copy_from_slice(values);
    slot.isnull[..n].copy_from_slice(isnull);
    exec_store_virtual_tuple(slot);
}

/// PG `ExecEndProjectSet`: tear down the child subtree; owned state drops.
pub fn exec_end_project_set(shared: Option<&Arc<SharedState>>, run: &mut ProjectSetRun<'_>) {
    // Reclaim any in-progress funcapi FuncCallContext (the ShutdownSetExpr
    // callback in C); a set abandoned mid-stream (e.g. under a LIMIT) would
    // otherwise leak its leaked-Box context.
    for elem in &mut run.elems {
        if let ElemState::Srf(se) = elem {
            crate::funcapi::end_MultiFuncCall(&mut se.fcinfo);
        }
    }
    exec_end_node(shared, &mut run.child);
    if let Some(slot) = run.state.ps.ps_result_tuple_slot.as_deref_mut() {
        ExecClearTuple(slot);
    }
}

/// PG `ExecReScanProjectSet`: forget any incompletely-evaluated SRFs (including
/// their cross-call funcapi contexts -- C's ShutdownSetExpr on rescan) and
/// rescan the child.
pub fn exec_rescan_project_set(shared: &Arc<SharedState>, run: &mut ProjectSetRun<'_>) {
    run.state.pending_srf_tuples = false;
    for elem in &mut run.elems {
        if let ElemState::Srf(se) = elem {
            se.args_valid = false;
            crate::funcapi::end_MultiFuncCall(&mut se.fcinfo);
        }
    }
    crate::backend::executor::execAmi::exec_rescan(shared, &mut run.child);
}
