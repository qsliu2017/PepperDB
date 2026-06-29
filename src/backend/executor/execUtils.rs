//! Executor utility routines. Translated from
//! backend/executor/execUtils.c (disposition: full for the spine helpers the M1
//! Result/Const path uses).
//!
//! MemoryContext is tombstoned in this port (rules.md s6.4): the per-query and
//! per-tuple "contexts" become Rust ownership + `Drop`, so `CreateExecutorState`/
//! `CreateExprContext` only initialize the live fields, `ResetExprContext` is a
//! no-op (no arena to reset for by-val data), and `FreeExecutorState` is `Drop`.

use crate::access::tupdesc::TupleDesc;
use crate::nodes::execnodes::{EState, ExprContext, PlanState};

use crate::backend::executor::execExpr::exec_build_projection_info;
use crate::backend::executor::execTuples::TTS_OPS_VIRTUAL;

/// PG `CreateExecutorState`: a fresh EState. `EState::default()` mirrors PG's
/// palloc0 + `es_direction = ForwardScanDirection` (see execnodes.rs). The range
/// table relations default empty (`&[]`, `'static`-coercible), so `'rel` is
/// unconstrained until `standard_executor_start` publishes the borrowed relations.
pub fn create_executor_state<'rel>() -> Box<EState<'rel>> {
    Box::new(EState::default())
}

/// PG `FreeExecutorState`: release the executor state. The exprcontexts, tuple
/// table and subplan states are owned by the EState and freed when the `Box`
/// drops; nothing else needs explicit teardown on the M1 path.
pub fn free_executor_state(estate: Box<EState<'_>>) {
    drop(estate);
}

/// PG `CreateExprContext`: a per-node expression context linked to `estate`.
/// The per-query/per-tuple memory contexts are tombstoned; the rest of the
/// fields default to their zero/empty values, matching CreateExprContextInternal.
pub fn create_expr_context(_estate: &mut EState<'_>) -> Box<ExprContext> {
    Box::new(ExprContext {
        case_value_is_null: true,
        domain_value_is_null: true,
        ..ExprContext::default()
    })
}

/// PG `MakePerTupleExprContext`: get/create the EState<'_>'s per-output-tuple
/// exprcontext. Returns the index of the (owned) context in `es_per_tuple`... in
/// this port the context is stored directly in `per_tuple_exprcontext`.
pub fn make_per_tuple_expr_context<'e>(estate: &'e mut EState<'_>) -> &'e mut ExprContext {
    if estate.per_tuple_exprcontext.is_none() {
        let ec = create_expr_context(estate);
        estate.per_tuple_exprcontext = Some(ec);
    }
    estate
        .per_tuple_exprcontext
        .as_mut()
        .unwrap_or_else(|| unreachable!("per_tuple_exprcontext just set"))
}

/// PG `ResetExprContext`: reset the per-tuple memory context. Memory is
/// tombstoned and the const path holds only by-val datums (nothing arena-
/// allocated per tuple), so this is a no-op. By-ref attr lifetimes grow with
/// varlena support.
pub fn reset_expr_context(_econtext: &mut ExprContext) {}

/// PG `ExecAssignExprContext`: give a PlanState its own per-node exprcontext.
pub fn exec_assign_expr_context(estate: &mut EState<'_>, planstate: &mut PlanState) {
    planstate.ps_expr_context = Some(create_expr_context(estate));
}

/// PG `ExecGetResultType`: the PlanState's result tuple descriptor (an `Arc`
/// clone; `None` if the node has none).
pub fn exec_get_result_type(planstate: &mut PlanState) -> Option<TupleDesc> {
    planstate.ps_result_tuple_desc.clone()
}

/// PG `ExecAssignProjectionInfo`: build the PlanState's projection from its
/// plan targetlist into its result slot. The C version threads the result slot
/// by pointer (it stays in es_tupleTable); here the result slot is owned by the
/// PlanState (`ps_result_tuple_slot`), so it is moved into the ProjectionInfo,
/// which becomes the live owner of the projected row for the node's lifetime.
pub fn exec_assign_projection_info(planstate: &mut PlanState, input_desc: Option<TupleDesc>) {
    let targetlist = plan_targetlist(planstate);

    let slot = planstate
        .ps_result_tuple_slot
        .take()
        .unwrap_or_else(|| unimplemented!("ExecAssignProjectionInfo: result slot not initialized"));

    // The exprcontext is owned by the PlanState; build against a throwaway clone
    // of the per-node context handle is unnecessary on the const path (the
    // projection reads no Var), so pass a transient context.
    let mut transient = ExprContext::default();
    let proj = exec_build_projection_info(&targetlist, &mut transient, slot, None, input_desc);
    planstate.ps_proj_info = Some(proj);
    planstate.resultops = Some(&TTS_OPS_VIRTUAL);
    planstate.resultopsset = true;
    planstate.resultopsfixed = true;
}

/// Extract a clone of a PlanState's plan targetlist (the plan is a `Node`).
fn plan_targetlist(planstate: &PlanState) -> Vec<crate::nodes::nodes::Node> {
    use crate::nodes::nodes::Node;
    let plan = planstate
        .plan
        .as_ref()
        .unwrap_or_else(|| unimplemented!("plan_targetlist: PlanState has no plan"));
    match plan {
        Node::Result(r) => r.plan.targetlist.clone(),
        Node::SeqScan(s) => s.scan.plan.targetlist.clone(),
        Node::IndexScan(s) => s.scan.plan.targetlist.clone(),
        Node::IndexOnlyScan(s) => s.scan.plan.targetlist.clone(),
        other => unimplemented!("plan_targetlist: {other:?} not reachable for this milestone"),
    }
}
