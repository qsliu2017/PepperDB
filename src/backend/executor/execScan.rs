//! Generic scan-node machinery. Translated from
//! backend/executor/execScan.c (disposition: full for the M2 qual-free scan +
//! projection; the qual-eval and EPQ-recheck arms grow at later milestones).
//!
//! `ExecScan`/`ExecScanExtended` is the generic loop shared by every relation
//! scan node: fetch a tuple from the access method, set `ecxt_scantuple`, evaluate
//! the qual, and project. In this port the access-method fetch + scan-slot deform
//! is done by the caller (`SeqNext` in nodeSeqscan) before `exec_scan` is called,
//! so `exec_scan` here is the qual+project tail: it aliases the node-owned scan
//! slot into `econtext->ecxt_scantuple`, projects (whose Vars read it), restores
//! the slot, and returns a borrow of the projection result slot.
//!
//! Slot ownership: aliasing `ecxt_scantuple` to the scan slot is an O(1) `Box`
//! move (`Option::take`) in each direction -- no per-tuple data copy, and Var eval
//! reads the live scanned row through the exprcontext.

use crate::nodes::execnodes::{PlanState, ScanState, TupleTableSlot};

use crate::backend::executor::execExpr::exec_qual;
use crate::backend::executor::execTuples::exec_store_virtual_tuple;
use crate::executor::tuptable::ExecClearTuple;

/// PG `ExecScan` (qual + project tail): the scan slot already holds the current
/// tuple (filled by the node's access method). Evaluate the qual against the scan
/// slot; if it FAILS, return `None` (the caller fetches the next tuple). If it
/// passes (or there is no qual), project and return a borrow of the projection
/// result slot.
///
/// PG's `ExecScan` runs the fetch loop internally; here the async fetch lives in
/// the caller (`exec_seq_scan`), which loops while this returns `None` for a
/// qual-failed row -- the same semantics, split across the sync/async boundary.
pub fn exec_scan(ss: &mut ScanState) -> Option<&mut TupleTableSlot> {
    // Alias the node-owned scan slot into econtext->ecxt_scantuple (O(1) Box move)
    // so the qual's and projection's EEOP_SCAN_VAR read the live row.
    let scan_slot = ss
        .ss_scan_tuple_slot
        .take()
        .unwrap_or_else(|| unimplemented!("ExecScan: scan node has no scan tuple slot"));
    let mut econtext = ss
        .ps
        .ps_expr_context
        .take()
        .unwrap_or_else(|| unimplemented!("ExecScan: scan node has no exprcontext"));
    econtext.ecxt_scantuple = Some(scan_slot);

    // ExecQual(node->ps.qual, econtext): None qual is always-true.
    let passes = exec_qual(ss.ps.qual.as_deref_mut(), &mut econtext);

    if passes {
        // The scan tuple's TID (PG `tts_tid`), carried onto the projection output slot
        // so a ModifyTable/LockRows parent reads the row identity off it (PG threads
        // ctid via a junk Var; here the slot carries it -- see nodeModifyTable).
        let scan_tid = econtext.ecxt_scantuple.as_ref().map(|s| s.tid);
        let proj = ss
            .ps
            .ps_proj_info
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecScan: scan node has no projection"));
        run_projection(&mut proj.state, &mut econtext);
        if let (Some(tid), Some(result)) = (scan_tid, proj.state.resultslot.as_mut()) {
            result.tid = tid;
        }
    }

    // Restore the scan slot to the node and the exprcontext to the PlanState.
    ss.ss_scan_tuple_slot = econtext.ecxt_scantuple.take();
    ss.ps.ps_expr_context = Some(econtext);

    if !passes {
        return None;
    }
    Some(
        ss.ps
            .ps_proj_info
            .as_mut()
            .and_then(|p| p.state.resultslot.as_deref_mut())
            .unwrap_or_else(|| unimplemented!("ExecScan: projection lost its result slot")),
    )
}

/// Run a projection ExprState: clear the result slot, run the interpreter (it
/// deposits each target into the result slot's arrays via ASSIGN_SCAN_VAR /
/// ASSIGN_TMP), mark the virtual tuple stored.
fn run_projection(
    state: &mut crate::nodes::execnodes::ExprState,
    econtext: &mut crate::nodes::execnodes::ExprContext,
) {
    {
        let slot = state
            .resultslot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecScan: projection has no result slot"));
        ExecClearTuple(slot);
    }
    let evalfunc = state
        .evalfunc
        .unwrap_or_else(|| unimplemented!("ExecScan: projection not ready"));
    let mut is_null = false;
    let _ = evalfunc(state, econtext, &mut is_null);
    let slot = state
        .resultslot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecScan: projection lost its result slot"));
    exec_store_virtual_tuple(slot);
}

/// PG `ExecAssignScanProjectionInfo`: build the scan node's projection from its
/// plan targetlist, with input descriptor = the scan tuple descriptor. Delegates
/// to the shared `exec_assign_projection_info` (execUtils), which threads the
/// input desc so scan Vars resolve to the scan slot.
pub fn exec_assign_scan_projection_info(planstate: &mut PlanState) {
    let input_desc = planstate.scandesc.clone();
    crate::backend::executor::execUtils::exec_assign_projection_info(planstate, input_desc);
}
