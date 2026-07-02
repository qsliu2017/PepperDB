//! Result node executor. Translated from
//! backend/executor/nodeResult.c (disposition: full for the childless-const
//! case; the with-outer-plan branch grows when scan nodes arrive).
//!
//! A Result evaluates a variable-free targetlist (optionally gated by a one-time
//! `resconstantqual`) and returns one projected row, or projects each row of an
//! outer plan. The M1 path is the childless, qual-free const case: it returns
//! exactly one row (then NULL), via `rs_done`.

use crate::nodes::execnodes::{EState, PlanState, ResultState};
use crate::nodes::plannodes::Result as ResultPlan;
use crate::nodes::nodes::Node;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};

use crate::backend::executor::execExpr::{exec_init_qual, exec_qual};
use crate::backend::nodes::makefuncs::make_ands_implicit;
use crate::backend::executor::execTuples::{exec_store_virtual_tuple, exec_type_from_tl, TTS_OPS_VIRTUAL};
use crate::backend::executor::execUtils::{
    create_expr_context, exec_assign_projection_info, reset_expr_context,
};

/// PG `ExecInitResult`: build the ResultState for a Result plan.
///
/// Childless + no resconstantqual on the M1 path: `rs_done = false`,
/// `rs_checkqual = false`. Builds the result tupdesc + a virtual result slot from
/// the targetlist, then the projection. Holds the per-node exprcontext.
pub fn exec_init_result(node: &ResultPlan, estate: &mut EState<'_>, eflags: i32) -> Box<ResultState> {
    let _ = eflags;
    // outer/inner plans must be absent on the M1 const path.
    if node.plan.lefttree.is_some() || node.plan.righttree.is_some() {
        unimplemented!("ExecInitResult: Result with child plan not yet reachable");
    }

    let mut ps = PlanState {
        plan: Some(Node::Result(Box::new(node.clone()))),
        ..PlanState::default()
    };

    // ExecAssignExprContext: per-node expression context.
    ps.ps_expr_context = Some(create_expr_context(estate));

    // ExecInitResultTupleSlotTL: result tupdesc + virtual result slot. The desc
    // is an Arc shared between the PlanState's `ps_result_tuple_desc` and the
    // result slot's `tupleDescriptor` (co-owners; freed when the last drops).
    let desc = exec_type_from_tl(&node.plan.targetlist);
    let slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(std::sync::Arc::clone(&desc)),
        &TTS_OPS_VIRTUAL,
    );
    ps.ps_result_tuple_desc = Some(desc);
    ps.ps_result_tuple_slot = Some(slot);
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;

    // ExecAssignProjectionInfo: build the projection (inputDesc = None).
    exec_assign_projection_info(&mut ps, None);

    // ExecInitQual on plan.qual (empty) and resconstantqual. resconstantqual is
    // stored as a single boolean expr; split to implicit-AND form for ExecInitQual.
    ps.qual = exec_init_qual(&node.plan.qual, None);
    let resconstantqual = node
        .resconstantqual
        .as_ref()
        .and_then(|q| exec_init_qual(&make_ands_implicit(Some(q.clone())), None));

    Box::new(ResultState {
        ps,
        resconstantqual,
        rs_done: false,
        rs_checkqual: node.resconstantqual.is_some(),
    })
}

/// PG `ExecResult`: return the next projected tuple, or None at EOF.
///
/// Childless + qual-free: the first call sets `rs_done = true` and returns the
/// projected const row; the second call sees `rs_done` and returns None.
/// Returns a BORROW of the node-owned projection result slot (the canonical
/// result row lives in the projection's result slot; `ExecProcNodeMtd` returns
/// the reused `TupleTableSlot*`).
pub fn exec_result(node: &mut ResultState) -> Option<&mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    // Check constant qualifications like (2 > 1), if not already done. On failure
    // the Result emits no rows.
    if node.rs_checkqual {
        let qual_result = {
            let econtext = node
                .ps
                .ps_expr_context
                .as_mut()
                .unwrap_or_else(|| unimplemented!("ExecResult: no exprcontext"));
            exec_qual(node.resconstantqual.as_deref_mut(), econtext)
        };
        node.rs_checkqual = false;
        if !qual_result {
            node.rs_done = true;
            return None;
        }
    }

    if let Some(ec) = node.ps.ps_expr_context.as_mut() {
        reset_expr_context(ec);
    }

    if node.rs_done {
        return None;
    }

    // No outer plan on the M1 path -> mark done, project the one row.
    node.rs_done = true;
    Some(exec_project(node))
}

/// Inlined `ExecProject` for the Result node: clear the result slot, run the
/// projection's interpreter (which deposits the const into the slot's value
/// arrays), then mark the virtual tuple stored. Returns a borrow of the
/// node-owned result slot (no per-tuple clone).
fn exec_project(node: &mut ResultState) -> &mut TupleTableSlot {
    let econtext = node
        .ps
        .ps_expr_context
        .take()
        .unwrap_or_else(|| unimplemented!("ExecProject: no exprcontext"));
    let proj = node
        .ps
        .ps_proj_info
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecResult: no projection info"));

    let mut econtext = econtext;
    project_const_into(&mut proj.state, &mut econtext);
    node.ps.ps_expr_context = Some(econtext);

    // Re-borrow the result slot (now filled + stored) to hand back.
    node.ps
        .ps_proj_info
        .as_mut()
        .and_then(|p| p.state.resultslot.as_deref_mut())
        .unwrap_or_else(|| unimplemented!("ExecProject: projection lost its result slot"))
}

/// Run a variable-free (const) projection: clear the result slot, run the
/// interpreter (it deposits the const into the slot's value arrays), mark the
/// virtual tuple stored. A childless Result reads no Var, so `ecxt_scantuple` is
/// untouched.
fn project_const_into(
    state: &mut crate::nodes::execnodes::ExprState,
    econtext: &mut crate::nodes::execnodes::ExprContext,
) {
    {
        let slot = state
            .resultslot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecProject: projection has no result slot"));
        ExecClearTuple(slot);
    }
    let evalfunc = state
        .evalfunc
        .unwrap_or_else(|| unimplemented!("ExecProject: projection not ready"));
    let mut is_null = false;
    let _ = evalfunc(state, econtext, &mut is_null);
    let slot = state
        .resultslot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecProject: projection lost its result slot"));
    exec_store_virtual_tuple(slot);
}

/// PG `ExecEndResult`: tear down the Result node. Childless, so the only C work
/// (`ExecEndNode(outerPlanState)`) is a no-op; owned state drops with the box.
pub fn exec_end_result(_node: &mut ResultState) {}
