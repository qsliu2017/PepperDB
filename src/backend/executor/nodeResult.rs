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

use crate::backend::executor::execExpr::exec_init_qual;
use crate::backend::executor::execTuples::{exec_store_virtual_tuple, exec_type_from_tl, TTS_OPS_VIRTUAL};
use crate::backend::executor::execUtils::{
    create_expr_context, exec_assign_projection_info, reset_expr_context,
};

/// PG `ExecInitResult`: build the ResultState for a Result plan.
///
/// Childless + no resconstantqual on the M1 path: `rs_done = false`,
/// `rs_checkqual = false`. Builds the result tupdesc + a virtual result slot from
/// the targetlist, then the projection. Holds the per-node exprcontext.
pub fn exec_init_result(node: &ResultPlan, estate: &mut EState, eflags: i32) -> Box<ResultState> {
    let _ = eflags;
    // outer/inner plans must be absent on the M1 const path.
    if node.plan.lefttree.is_some() || node.plan.righttree.is_some() {
        unimplemented!("ExecInitResult: Result with child plan not yet reachable");
    }

    let mut ps = PlanState {
        plan: Some(Box::new(Node::Result(Box::new(node.clone())))),
        ..PlanState::default()
    };

    // ExecAssignExprContext: per-node expression context.
    ps.ps_expr_context = Some(create_expr_context(estate));

    // ExecInitResultTupleSlotTL: result tupdesc + virtual result slot.
    let desc = exec_type_from_tl(&node.plan.targetlist);
    ps.ps_result_tuple_desc = desc;
    let slot = crate::backend::executor::execTuples::make_tuple_table_slot(desc, &TTS_OPS_VIRTUAL);
    ps.ps_result_tuple_slot = Some(slot);
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;

    // ExecAssignProjectionInfo: build the projection (inputDesc = NULL).
    exec_assign_projection_info(&mut ps, core::ptr::null_mut());

    // ExecInitQual on plan.qual (empty) and resconstantqual.
    ps.qual = exec_init_qual(&node.plan.qual, None);
    let resconstantqual = match &node.resconstantqual {
        None => None,
        Some(_) => unimplemented!("ExecInitResult: resconstantqual not yet reachable"),
    };

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
/// Returns an owned slot clone (the canonical result row lives in the
/// projection's result slot; `ExecProcNodeMtd` hands callers an owned slot).
pub fn exec_result(node: &mut ResultState) -> Option<Box<TupleTableSlot>> {
    crate::miscadmin::check_for_interrupts();

    // rs_checkqual is false on the M1 path (no resconstantqual); the one-time
    // qual test grows with that field.
    if node.rs_checkqual {
        unimplemented!("ExecResult: resconstantqual evaluation not yet reachable");
    }

    if let Some(ec) = node.ps.ps_expr_context.as_mut() {
        reset_expr_context(ec);
    }

    if node.rs_done {
        return None;
    }

    // No outer plan on the M1 path -> mark done, project the one row.
    node.rs_done = true;
    Some(Box::new(exec_project(node)))
}

/// Inlined `ExecProject` for the Result node: clear the result slot, run the
/// projection's interpreter (which deposits the const into the slot's value
/// arrays), then mark the virtual tuple stored. Returns an owned clone.
fn exec_project(node: &mut ResultState) -> TupleTableSlot {
    let proj = node
        .ps
        .ps_proj_info
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecResult: no projection info"));

    // Clear the result slot held by the projection.
    {
        let slot = proj
            .state
            .resultslot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecProject: projection has no result slot"));
        ExecClearTuple(slot);
    }

    // Run the projection steps (ExecEvalExprNoReturn: no scalar return).
    let mut econtext = node
        .ps
        .ps_expr_context
        .take()
        .unwrap_or_else(|| unimplemented!("ExecProject: no exprcontext"));
    let evalfunc = proj
        .state
        .evalfunc
        .unwrap_or_else(|| unimplemented!("ExecProject: projection not ready"));
    let mut is_null = false;
    let _ = evalfunc(&mut proj.state, &mut econtext, &mut is_null);
    node.ps.ps_expr_context = Some(econtext);

    // Inlined ExecStoreVirtualTuple: mark the (filled) virtual slot valid, then
    // hand back an owned clone of the row.
    let slot = proj
        .state
        .resultslot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecProject: projection lost its result slot"));
    exec_store_virtual_tuple(slot);
    (**slot).clone()
}

/// PG `ExecEndResult`: tear down the Result node. Childless, so the only C work
/// (`ExecEndNode(outerPlanState)`) is a no-op; owned state drops with the box.
pub fn exec_end_result(_node: &mut ResultState) {}
