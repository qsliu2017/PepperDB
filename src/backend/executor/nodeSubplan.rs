//! SubPlan node executor: sub-selects appearing in expressions. Translated from
//! backend/executor/nodeSubplan.c (disposition: full leaf for the per-row scan path
//! + the uncorrelated InitPlan path; the hashed-IN and ROWCOMPARE/ARRAY paths stage).
//!
//! A SubPlan is NOT a tuple-returning plan node; it is an EXPRESSION node evaluated
//! while a parent node runs its qual/projection. PG calls `ExecSubPlan` from
//! `ExecEvalSubPlan` during synchronous expression eval. The port's expression eval
//! is synchronous but running a child plan is async (it reaches the table AM) and
//! needs the `SharedState`. So the port DECOUPLES the two: a parent node, in its
//! async loop, runs each referenced SubPlan BEFORE the sync qual/projection
//! (`run_subplans_for`), depositing the result into the parent exprcontext's
//! `ecxt_param_exec_vals[setParam]`; the sync qual/projection then reads it via the
//! ordinary `EEOP_PARAM_EXEC` step (the SubLink was replaced by that Param in the
//! planner). This is exactly PG's InitPlan mechanism, extended to correlated plans by
//! re-running per outer row.
//!
//! Each SubPlan's child plan-state subtree (`SubPlanRun.planstate`) borrows the
//! EState range-table relations (`'rel`), just like the top plan; both share the one
//! flattened rangetable, so the subplan's scan nodes index it by their offset
//! scanrelid (set in make_subplan).

use std::sync::Arc;

use crate::backend::executor::execExpr::exec_init_expr;
use crate::backend::executor::execProcnode::{
    exec_end_node, exec_init_node, exec_proc_node, exec_rescan_node, PlanStateNode,
};
use crate::nodes::execnodes::{EState, ExprContext, ExprState, ParamExecData};
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{SubLinkType, SubPlan};
use crate::postgres::{BoolGetDatum, Datum, DatumGetBool};
use crate::shared_state::SharedState;
use crate::executor::tuptable::slot_getattr;

/// Run-state for one SubPlan: the SubPlan node plus its initialized child plan-state
/// subtree and the compiled testexpr / correlation-arg expressions. Held by the
/// parent node that references the subplan (built in InitPlan from the EState).
pub struct SubPlanRun<'rel> {
    pub subplan: SubPlan,
    pub planstate: PlanStateNode<'rel>,
    /// Compiled testexpr (ANY/ALL combining op over the per-row output Params).
    pub testexpr: Option<Box<ExprState>>,
    /// Compiled correlation-arg expressions (the parParam outer Vars), evaluated in
    /// the PARENT exprcontext to set the parParam slots before each (re)scan.
    pub args: Vec<Box<ExprState>>,
    /// InitPlan caching: an uncorrelated subplan runs once; `computed` guards re-run.
    pub computed: bool,
}

/// PG `ExecInitSubPlan` (per subplan): build a SubPlanRun from a SubPlan node + its
/// child plan (looked up by `plan_id` in the EState's subplan list). Compiles the
/// testexpr + arg expressions and initializes the child plan-state subtree.
pub fn exec_init_subplan<'rel>(
    subplan: &SubPlan,
    subplan_tree: &Node,
    estate: &mut EState<'rel>,
    eflags: i32,
) -> SubPlanRun<'rel> {
    let planstate = exec_init_node(Some(subplan_tree), estate, eflags)
        .unwrap_or_else(|| unimplemented!("ExecInitSubPlan: subplan tree did not initialize"));

    let testexpr = subplan
        .testexpr
        .as_ref()
        .and_then(|t| exec_init_expr(Some(t), None));

    let args = subplan
        .args
        .iter()
        .map(|a| {
            exec_init_expr(Some(a), None)
                .unwrap_or_else(|| unimplemented!("ExecInitSubPlan: subplan arg did not compile"))
        })
        .collect();

    SubPlanRun { subplan: subplan.clone(), planstate, testexpr, args, computed: false }
}

/// Run every SubPlan a parent node references, depositing each result into
/// `econtext.ecxt_param_exec_vals`. Correlated subplans (non-empty parParam) re-run
/// each call against the current outer tuple (read through `econtext`); uncorrelated
/// subplans (InitPlans) run once and cache. Called from a parent node's async loop
/// before the synchronous qual/projection.
pub async fn run_subplans_for(
    shared: Option<&Arc<SharedState>>,
    subplans: &mut [SubPlanRun<'_>],
    econtext: &mut ExprContext,
) {
    for sp in subplans.iter_mut() {
        // A subplan caches (runs once as an InitPlan) only when it is uncorrelated
        // (empty parParam) AND its result does not depend on the outer row. ANY/ALL
        // always depend on the outer row via the testexpr LHS, so they re-run every
        // call even when uncorrelated (PG: ANY/ALL are never InitPlans).
        let is_init_plan = sp.subplan.parParam.is_empty()
            && !matches!(
                sp.subplan.subLinkType,
                SubLinkType::ANY_SUBLINK | SubLinkType::ALL_SUBLINK
            );
        if is_init_plan && sp.computed {
            continue;
        }
        exec_sub_plan(shared, sp, econtext).await;
        sp.computed = true;
    }
}

/// PG `ExecSubPlan` + `ExecScanSubPlan`/`ExecSetParamPlan` combined: run one SubPlan
/// and deposit its result into `econtext.ecxt_param_exec_vals[setParam...]`.
///
/// For a correlated subplan the parParam values are first loaded from the args
/// (evaluated in the parent econtext), the child is rescanned, then run. The
/// per-row combination depends on the subLinkType (3-valued for ANY/ALL).
async fn exec_sub_plan(
    shared: Option<&Arc<SharedState>>,
    sp: &mut SubPlanRun<'_>,
    econtext: &mut ExprContext,
) {
    // 1. Load the parParam slots from the args (evaluated against the parent tuple).
    if !sp.subplan.parParam.is_empty() {
        load_par_params(sp, econtext);
    }
    // Rescan the child before re-running: a correlated subplan re-runs with new
    // params; an (uncorrelated) ANY/ALL re-runs each outer row too. The first run
    // (computed == false) opens the scan lazily, so only rescan on later runs.
    if sp.computed {
        rescan_subplan_tree(shared, &mut sp.planstate);
    }

    let kind = sp.subplan.subLinkType;
    let param_ids = sp.subplan.paramIds.clone();
    let mut found = false;
    // Defaults for the empty-subplan result: ANY -> FALSE, ALL -> TRUE.
    let mut result = BoolGetDatum(kind == SubLinkType::ALL_SUBLINK);
    let mut result_null = false;
    // EXPR scalar accumulator.
    let mut expr_val = Datum(0);
    let mut expr_null = true;

    loop {
        // Box::pin to break the async recursion cycle (a parent SeqScan runs subplans,
        // whose own scan calls back into exec_proc_node).
        let slot = Box::pin(exec_proc_node(shared, &mut sp.planstate)).await;
        let Some(slot) = slot else { break };

        match kind {
            SubLinkType::EXISTS_SUBLINK => {
                found = true;
                result = BoolGetDatum(true);
                result_null = false;
                break;
            }
            SubLinkType::EXPR_SUBLINK => {
                if found {
                    cardinality_error();
                }
                found = true;
                let v = slot_getattr(slot, 1);
                expr_val = v.unwrap_or(Datum(0));
                expr_null = v.is_none();
                // keep scanning to enforce single-row (fall through to next iteration)
            }
            SubLinkType::ANY_SUBLINK | SubLinkType::ALL_SUBLINK => {
                found = true;
                // Read the per-column output values out of the subquery row FIRST
                // (this ends the `slot`/`sp.planstate` borrow), then set the testexpr
                // input Params + evaluate the combining op.
                let row: Vec<(Datum, bool)> = (1..=param_ids.len())
                    .map(|col| {
                        let v = slot_getattr(slot, col as i32);
                        (v.unwrap_or(Datum(0)), v.is_none())
                    })
                    .collect();
                for (i, &paramid) in param_ids.iter().enumerate() {
                    set_param(econtext, paramid, row[i].0, row[i].1);
                }
                let (rowresult, rownull) = eval_testexpr(sp, econtext);
                if kind == SubLinkType::ANY_SUBLINK {
                    // OR semantics (3-valued).
                    if rownull {
                        result_null = true;
                    } else if DatumGetBool(rowresult) {
                        result = BoolGetDatum(true);
                        result_null = false;
                        break;
                    }
                } else {
                    // ALL: AND semantics (3-valued).
                    if rownull {
                        result_null = true;
                    } else if !DatumGetBool(rowresult) {
                        result = BoolGetDatum(false);
                        result_null = false;
                        break;
                    }
                }
            }
            other => {
                let _ = other;
                unimplemented!("ExecSubPlan: subLinkType not yet reachable for this milestone");
            }
        }
    }

    // Finalize the result for EXISTS/EXPR empty cases.
    match kind {
        SubLinkType::EXISTS_SUBLINK => {
            if !found {
                result = BoolGetDatum(false);
                result_null = false;
            }
        }
        SubLinkType::EXPR_SUBLINK => {
            // EXPR with no rows -> NULL; else the single scalar.
            result = expr_val;
            result_null = if found { expr_null } else { true };
        }
        _ => {}
    }

    // Deposit into the single output (setParam) slot.
    set_param(econtext, output_param(sp), result, result_null);
}

/// The single output PARAM_EXEC id for this subplan (EXISTS/EXPR/ANY/ALL each have
/// exactly one setParam in the port's encoding).
fn output_param(sp: &SubPlanRun<'_>) -> i32 {
    *sp.subplan
        .setParam
        .first()
        .unwrap_or_else(|| unimplemented!("ExecSubPlan: subplan has no output param"))
}

/// Evaluate the correlation args (in the parent econtext) into the parParam slots.
fn load_par_params(sp: &mut SubPlanRun<'_>, econtext: &mut ExprContext) {
    let ids = sp.subplan.parParam.clone();
    for (i, &paramid) in ids.iter().enumerate() {
        let (v, isnull) = eval_scalar(&mut sp.args[i], econtext);
        set_param(econtext, paramid, v, isnull);
    }
}

/// Evaluate the testexpr (ANY/ALL combining op) in `econtext`, returning (value, isnull).
fn eval_testexpr(sp: &mut SubPlanRun<'_>, econtext: &mut ExprContext) -> (Datum, bool) {
    let state = sp
        .testexpr
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecSubPlan: ANY/ALL without a testexpr"));
    eval_scalar(state, econtext)
}

/// Evaluate a scalar ExprState in `econtext`.
fn eval_scalar(state: &mut ExprState, econtext: &mut ExprContext) -> (Datum, bool) {
    let evalfunc = state
        .evalfunc
        .unwrap_or_else(|| unimplemented!("ExecSubPlan: expression not ready"));
    let mut isnull = false;
    let v = evalfunc(state, econtext, &mut isnull);
    (v, isnull)
}

/// Write a PARAM_EXEC slot in the query's shared value array (sizing it if needed).
fn set_param(econtext: &ExprContext, paramid: i32, value: Datum, isnull: bool) {
    let vals = econtext
        .ecxt_param_exec_vals
        .as_ref()
        .unwrap_or_else(|| unimplemented!("ExecSubPlan: no ecxt_param_exec_vals to set"));
    let mut guard = vals.lock();
    let idx = paramid as usize;
    if guard.len() <= idx {
        guard.resize_with(idx + 1, || ParamExecData { exec_plan: 0, value: Datum(0), isnull: true });
    }
    guard[idx] = ParamExecData { exec_plan: 0, value, isnull };
}

#[cold]
fn cardinality_error() -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_CARDINALITY_VIOLATION)
            .errmsg("more than one row returned by a subquery used as an expression".to_owned());
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `ExecEndSubPlan`: tear down a subplan's child plan-state subtree.
pub fn exec_end_subplan(shared: Option<&Arc<SharedState>>, sp: &mut SubPlanRun<'_>) {
    exec_end_node(shared, &mut sp.planstate);
}

/// PG `ExecReScan` for a subplan's child tree: reset it so the next `ExecProcNode`
/// re-reads from the start (the correlated subplan re-runs with new parParams). The
/// shared-aware variant handles the SeqScan leaf (drop the open descriptor) the
/// generic `exec_rescan_node` cannot reach; other shapes route through it.
fn rescan_subplan_tree(shared: Option<&Arc<SharedState>>, node: &mut PlanStateNode<'_>) {
    use crate::backend::executor::execProcnode::PlanStateNode as PSN;
    match node {
        PSN::SeqScan(ss) => {
            if let Some(s) = shared {
                crate::backend::executor::nodeSeqscan::exec_rescan_seq_scan(s, ss);
            } else {
                ss.scan = None;
            }
        }
        PSN::Agg(a) => {
            rescan_subplan_tree(shared, &mut a.child);
            crate::backend::executor::nodeAgg::exec_rescan_agg(a);
        }
        PSN::Sort(s) => {
            rescan_subplan_tree(shared, &mut s.child);
            crate::backend::executor::nodeSort::exec_rescan_sort(s);
        }
        PSN::Limit(l) => rescan_subplan_tree(shared, &mut l.child),
        PSN::Material(m) => rescan_subplan_tree(shared, &mut m.child),
        other => exec_rescan_node(other),
    }
}
