//! nodeNamedtuplestorescan.c - routines to handle NamedTuplestoreScan nodes.

use crate::prelude::*;

use crate::makeNode;
use crate::access::sdir::ScanDirectionIsForward;
use crate::executor::execScan::{ExecScan, ExecScanReScan};
use crate::executor::execTuples::TTSOpsMinimalTuple;
use crate::executor::execUtils::ExecAssignExprContext;
use crate::executor::executor::{
    ExecInitQual, ExecInitResultTypeTL, ExecInitScanTupleSlot, EXEC_FLAG_BACKWARD,
    EXEC_FLAG_MARK, EXEC_FLAG_REWIND,
};
use crate::executor::execScan::ExecAssignScanProjectionInfo;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{
    EState, NamedTuplestoreScanState, PlanState, ScanState, Tuplestorestate,
};
use crate::nodes::nodes::{castNodeImpl, NodeTag};
use crate::nodes::plannodes::{innerPlan, outerPlan, NamedTuplestoreScan, Plan};
use crate::utils::misc::queryenvironment::{
    get_ENR, EphemeralNamedRelation, ENRMetadataGetTupDesc,
};

// ----------------------------------------------------------------
// Locally-stubbed tuplestore routines (utils/sort/tuplestore.c not yet ported).
// ----------------------------------------------------------------

// TODO: tuplestore.c not yet ported.
unsafe fn tuplestore_select_read_pointer(_state: *mut Tuplestorestate, _ptr: c_int) {
    crate::utils::sort::tuplestore::tuplestore_select_read_pointer(_state as _, _ptr as _)
}

// TODO: tuplestore.c not yet ported.
unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    crate::utils::sort::tuplestore::tuplestore_gettupleslot(_state as _, _forward as _, _copy as _, _slot as _) as _
}

// TODO: tuplestore.c not yet ported.
unsafe fn tuplestore_alloc_read_pointer(_state: *mut Tuplestorestate, _eflags: c_int) -> c_int {
    crate::utils::sort::tuplestore::tuplestore_alloc_read_pointer(_state as _, _eflags as _) as _
}

// TODO: tuplestore.c not yet ported.
unsafe fn tuplestore_rescan(_state: *mut Tuplestorestate) {
    crate::utils::sort::tuplestore::tuplestore_rescan(_state as _)
}

/* ----------------------------------------------------------------
 *		NamedTuplestoreScanNext
 *
 *		This is a workhorse for ExecNamedTuplestoreScan
 * ----------------------------------------------------------------
 */
unsafe fn NamedTuplestoreScanNext(node: *mut NamedTuplestoreScanState) -> *mut TupleTableSlot {
    let slot: *mut TupleTableSlot;

    /* We intentionally do not support backward scan. */
    Assert!(ScanDirectionIsForward(
        (*(*node).ss.ps.state).es_direction
    ));

    /*
     * Get the next tuple from tuplestore. Return NULL if no more tuples.
     */
    slot = (*node).ss.ss_ScanTupleSlot;
    tuplestore_select_read_pointer((*node).relation, (*node).readptr);
    let _ = tuplestore_gettupleslot((*node).relation, true, false, slot);
    slot
}

/*
 * NamedTuplestoreScanRecheck -- access method routine to recheck a tuple in
 * EvalPlanQual
 */
unsafe fn NamedTuplestoreScanRecheck(
    _node: *mut NamedTuplestoreScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    /* nothing to check */
    true
}

/*
 * Trampolines matching the ExecScanAccessMtd / ExecScanRecheckMtd signatures
 * (which take *mut ScanState). The C code casts NamedTuplestoreScan{Next,Recheck}
 * to those types; here we adapt explicitly since ScanState is the first field.
 */
unsafe fn NamedTuplestoreScanNext_mtd(node: *mut ScanState) -> *mut TupleTableSlot {
    NamedTuplestoreScanNext(node as *mut NamedTuplestoreScanState)
}

unsafe fn NamedTuplestoreScanRecheck_mtd(
    node: *mut ScanState,
    slot: *mut TupleTableSlot,
) -> bool {
    NamedTuplestoreScanRecheck(node as *mut NamedTuplestoreScanState, slot)
}

/* ----------------------------------------------------------------
 *		ExecNamedTuplestoreScan(node)
 *
 *		Scans the CTE sequentially and returns the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
unsafe fn ExecNamedTuplestoreScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut NamedTuplestoreScanState = castNodeImpl(
        NodeTag::T_NamedTuplestoreScanState,
        pstate as *mut c_void,
    ) as *mut NamedTuplestoreScanState;

    ExecScan(
        &mut (*node).ss,
        Some(NamedTuplestoreScanNext_mtd),
        Some(NamedTuplestoreScanRecheck_mtd),
    )
}

/* ----------------------------------------------------------------
 *		ExecInitNamedTuplestoreScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitNamedTuplestoreScan(
    node: *mut NamedTuplestoreScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut NamedTuplestoreScanState {
    let scanstate: *mut NamedTuplestoreScanState;
    let enr: EphemeralNamedRelation;

    /* check for unsupported flags */
    Assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    /*
     * NamedTuplestoreScan should not have any children.
     */
    Assert!(outerPlan(node as *mut Plan).is_null());
    Assert!(innerPlan(node as *mut Plan).is_null());

    /*
     * create new NamedTuplestoreScanState for node
     */
    scanstate = makeNode!(NamedTuplestoreScanState, T_NamedTuplestoreScanState);
    (*scanstate).ss.ps.plan = node as *mut Plan;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecNamedTuplestoreScan);

    enr = get_ENR((*estate).es_queryEnv, (*node).enrname);
    if enr.is_null() {
        elog!(
            ERROR,
            "executor could not find named tuplestore"
        );
    }

    Assert!(!(*enr).reldata.is_null());
    (*scanstate).relation = (*enr).reldata as *mut Tuplestorestate;
    (*scanstate).tupdesc = ENRMetadataGetTupDesc(&mut (*enr).md);
    (*scanstate).readptr =
        tuplestore_alloc_read_pointer((*scanstate).relation, EXEC_FLAG_REWIND);

    /*
     * The new read pointer copies its position from read pointer 0, which
     * could be anywhere, so explicitly rewind it.
     */
    tuplestore_select_read_pointer((*scanstate).relation, (*scanstate).readptr);
    tuplestore_rescan((*scanstate).relation);

    /*
     * XXX: Should we add a function to free that read pointer when done?
     *
     * This was attempted, but it did not improve performance or memory usage
     * in any tested cases.
     */

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*scanstate).ss.ps);

    /*
     * The scan tuple type is specified for the tuplestore.
     */
    ExecInitScanTupleSlot(
        estate,
        &mut (*scanstate).ss,
        (*scanstate).tupdesc,
        &TTSOpsMinimalTuple,
    );

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*scanstate).ss.ps);
    ExecAssignScanProjectionInfo(&mut (*scanstate).ss);

    /*
     * initialize child expressions
     */
    (*scanstate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual, scanstate as *mut PlanState);

    scanstate
}

/* ----------------------------------------------------------------
 *		ExecReScanNamedTuplestoreScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanNamedTuplestoreScan(node: *mut NamedTuplestoreScanState) {
    let tuplestorestate: *mut Tuplestorestate = (*node).relation;

    if !(*node).ss.ps.ps_ResultTupleSlot.is_null() {
        ExecClearTuple((*node).ss.ps.ps_ResultTupleSlot);
    }

    ExecScanReScan(&mut (*node).ss);

    /*
     * Rewind my own pointer.
     */
    tuplestore_select_read_pointer(tuplestorestate, (*node).readptr);
    tuplestore_rescan(tuplestorestate);
}
