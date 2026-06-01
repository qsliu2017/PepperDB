//! nodeSubqueryscan.c - Support routines for scanning subqueries (subselects in rangetable).
//!
//! This is just enough different from sublinks (nodeSubplan.c) to mean that
//! we need two sets of code.  Ought to look at trying to unify the cases.
//!
//! INTERFACE ROUTINES
//!     ExecSubqueryScan            scans a subquery.
//!     ExecSubqueryNext            retrieve next tuple in sequential order.
//!     ExecInitSubqueryScan        creates and initializes a subqueryscan node.
//!     ExecEndSubqueryScan         releases any storage allocated.
//!     ExecReScanSubqueryScan      rescans the relation

use crate::prelude::*;

use std::ffi::c_int;
use std::ptr;

use crate::nodes::execnodes::{EState, PlanState, SubqueryScanState};
use crate::nodes::plannodes::{innerPlan, outerPlan, SubqueryScan};
use crate::nodes::nodes::NodeTag;
use crate::executor::tuptable::TupleTableSlot;
use crate::{castNode, makeNode};

use crate::executor::executor::{
    ExecEndNode, ExecInitNode, ExecInitQual, ExecInitResultTypeTL, ExecInitScanTupleSlot,
    ExecProcNode, ExecReScan, ExecScanAccessMtd, ExecScanRecheckMtd, UpdateChangedParamSet,
    EXEC_FLAG_MARK,
};
use crate::executor::execScan::{ExecAssignScanProjectionInfo, ExecScan, ExecScanReScan};
use crate::executor::execUtils::{
    ExecAssignExprContext, ExecGetResultSlotOps, ExecGetResultType,
};

/* ----------------------------------------------------------------
 *                      Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *      SubqueryNext
 *
 *      This is a workhorse for ExecSubqueryScan
 * ----------------------------------------------------------------
 */
unsafe fn SubqueryNext(node: *mut SubqueryScanState) -> *mut TupleTableSlot {
    let slot: *mut TupleTableSlot;

    /*
     * Get the next tuple from the sub-query.
     */
    slot = ExecProcNode((*node).subplan);

    /*
     * We just return the subplan's result slot, rather than expending extra
     * cycles for ExecCopySlot().  (Our own ScanTupleSlot is used only for
     * EvalPlanQual rechecks.)
     */
    slot
}

/*
 * SubqueryRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn SubqueryRecheck(_node: *mut SubqueryScanState, _slot: *mut TupleTableSlot) -> bool {
    /* nothing to check */
    true
}

/* ----------------------------------------------------------------
 *      ExecSubqueryScan(node)
 *
 *      Scans the subquery sequentially and returns the next qualifying
 *      tuple.
 *      We call the ExecScan() routine and pass it the appropriate
 *      access method functions.
 * ----------------------------------------------------------------
 */
unsafe fn ExecSubqueryScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SubqueryScanState = castNode!(SubqueryScanState, T_SubqueryScanState, pstate);

    ExecScan(
        &mut (*node).ss,
        Some(SubqueryNext_access),
        Some(SubqueryRecheck_recheck),
    )
}

/*
 * The C code casts SubqueryNext/SubqueryRecheck (which take SubqueryScanState*)
 * to ExecScanAccessMtd/ExecScanRecheckMtd (which take ScanState*).  Since
 * ScanState is the first member of SubqueryScanState, the pointer values are
 * identical; provide thin shim functions with the ScanState* signature.
 */
unsafe fn SubqueryNext_access(node: *mut crate::nodes::execnodes::ScanState) -> *mut TupleTableSlot {
    SubqueryNext(node as *mut SubqueryScanState)
}

unsafe fn SubqueryRecheck_recheck(
    node: *mut crate::nodes::execnodes::ScanState,
    slot: *mut TupleTableSlot,
) -> bool {
    SubqueryRecheck(node as *mut SubqueryScanState, slot)
}

/* ----------------------------------------------------------------
 *      ExecInitSubqueryScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitSubqueryScan(
    node: *mut SubqueryScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut SubqueryScanState {
    let subquerystate: *mut SubqueryScanState;

    /* check for unsupported flags */
    Assert!((eflags & EXEC_FLAG_MARK) == 0);

    /* SubqueryScan should not have any "normal" children */
    Assert!(outerPlan(node as *mut crate::nodes::plannodes::Plan).is_null());
    Assert!(innerPlan(node as *mut crate::nodes::plannodes::Plan).is_null());

    /*
     * create state structure
     */
    subquerystate = makeNode!(SubqueryScanState, T_SubqueryScanState);
    (*subquerystate).ss.ps.plan = node as *mut crate::nodes::plannodes::Plan;
    (*subquerystate).ss.ps.state = estate;
    (*subquerystate).ss.ps.ExecProcNode = Some(ExecSubqueryScan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*subquerystate).ss.ps);

    /*
     * initialize subquery
     */
    (*subquerystate).subplan = ExecInitNode((*node).subplan, estate, eflags);

    /*
     * Initialize scan slot and type (needed by ExecAssignScanProjectionInfo)
     */
    ExecInitScanTupleSlot(
        estate,
        &mut (*subquerystate).ss,
        ExecGetResultType((*subquerystate).subplan),
        ExecGetResultSlotOps((*subquerystate).subplan, ptr::null_mut()),
    );

    /*
     * The slot used as the scantuple isn't the slot above (outside of EPQ),
     * but the one from the node below.
     */
    (*subquerystate).ss.ps.scanopsset = true;
    (*subquerystate).ss.ps.scanops = ExecGetResultSlotOps(
        (*subquerystate).subplan,
        &mut (*subquerystate).ss.ps.scanopsfixed,
    );
    (*subquerystate).ss.ps.resultopsset = true;
    (*subquerystate).ss.ps.resultops = (*subquerystate).ss.ps.scanops;
    (*subquerystate).ss.ps.resultopsfixed = (*subquerystate).ss.ps.scanopsfixed;

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*subquerystate).ss.ps);
    ExecAssignScanProjectionInfo(&mut (*subquerystate).ss);

    /*
     * initialize child expressions
     */
    (*subquerystate).ss.ps.qual = ExecInitQual(
        (*node).scan.plan.qual,
        subquerystate as *mut PlanState,
    );

    subquerystate
}

/* ----------------------------------------------------------------
 *      ExecEndSubqueryScan
 *
 *      frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndSubqueryScan(node: *mut SubqueryScanState) {
    /*
     * close down subquery
     */
    ExecEndNode((*node).subplan);
}

/* ----------------------------------------------------------------
 *      ExecReScanSubqueryScan
 *
 *      Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanSubqueryScan(node: *mut SubqueryScanState) {
    ExecScanReScan(&mut (*node).ss);

    /*
     * ExecReScan doesn't know about my subplan, so I have to do
     * changed-parameter signaling myself.  This is just as well, because the
     * subplan has its own memory context in which its chgParam state lives.
     */
    if !(*node).ss.ps.chgParam.is_null() {
        UpdateChangedParamSet((*node).subplan, (*node).ss.ps.chgParam);
    }

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*(*node).subplan).chgParam.is_null() {
        ExecReScan((*node).subplan);
    }
}
