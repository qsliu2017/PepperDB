//! nodeWorktablescan.c - routines to handle WorkTableScan nodes.

use crate::prelude::*;

use crate::access::sdir::ScanDirectionIsForward;
use crate::executor::execScan::{ExecAssignScanProjectionInfo, ExecScan, ExecScanReScan};
use crate::executor::executor::{
    ExecAssignExprContext, ExecAssignScanType, ExecGetResultType, ExecInitQual,
    ExecInitResultTypeTL, ExecInitScanTupleSlot, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::execTuples::TTSOpsMinimalTuple;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot, TupleTableSlotOps};
use crate::nodes::execnodes::{
    EState, PlanState, RecursiveUnionState, ScanState, Tuplestorestate, WorkTableScanState,
};
use crate::nodes::params::ParamExecData;
use crate::nodes::plannodes::{innerPlan, outerPlan, Plan, WorkTableScan};
use crate::postgres::DatumGetPointer;
use crate::{castNode, makeNode, Assert};

use std::ffi::c_int;

// tuplestore.c routines - not yet ported.
unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    // TODO(pg-port): port src/backend/utils/sort/tuplestore.c
    unimplemented!()
}

unsafe fn tuplestore_rescan(_state: *mut Tuplestorestate) {
    // TODO(pg-port): port src/backend/utils/sort/tuplestore.c
    unimplemented!()
}

/* ----------------------------------------------------------------
 *		WorkTableScanNext
 *
 *		This is a workhorse for ExecWorkTableScan
 * ----------------------------------------------------------------
 */
unsafe fn WorkTableScanNext(node: *mut WorkTableScanState) -> *mut TupleTableSlot {
    let slot: *mut TupleTableSlot;
    let tuplestorestate: *mut Tuplestorestate;

    /*
     * get information from the estate and scan state
     *
     * Note: we intentionally do not support backward scan.  Although it would
     * take only a couple more lines here, it would force nodeRecursiveunion.c
     * to create the tuplestore with backward scan enabled, which has a
     * performance cost.  In practice backward scan is never useful for a
     * worktable plan node, since it cannot appear high enough in the plan
     * tree of a scrollable cursor to be exposed to a backward-scan
     * requirement.  So it's not worth expending effort to support it.
     *
     * Note: we are also assuming that this node is the only reader of the
     * worktable.  Therefore, we don't need a private read pointer for the
     * tuplestore, nor do we need to tell tuplestore_gettupleslot to copy.
     */
    Assert!(ScanDirectionIsForward(
        (*(*node).ss.ps.state).es_direction
    ));

    tuplestorestate = (*(*node).rustate).working_table;

    /*
     * Get the next tuple from tuplestore. Return NULL if no more tuples.
     */
    slot = (*node).ss.ss_ScanTupleSlot;
    let _ = tuplestore_gettupleslot(tuplestorestate, true, false, slot);
    slot
}

/*
 * WorkTableScanRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn WorkTableScanRecheck(
    _node: *mut WorkTableScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    /* nothing to check */
    true
}

/* ----------------------------------------------------------------
 *		ExecWorkTableScan(node)
 *
 *		Scans the worktable sequentially and returns the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
unsafe fn ExecWorkTableScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut WorkTableScanState =
        castNode!(WorkTableScanState, T_WorkTableScanState, pstate);

    /*
     * On the first call, find the ancestor RecursiveUnion's state via the
     * Param slot reserved for it.  (We can't do this during node init because
     * there are corner cases where we'll get the init call before the
     * RecursiveUnion does.)
     */
    if (*node).rustate.is_null() {
        let plan: *mut WorkTableScan = (*node).ss.ps.plan as *mut WorkTableScan;
        let estate: *mut EState = (*node).ss.ps.state;
        let param: *mut ParamExecData;

        param = &mut *(*estate)
            .es_param_exec_vals
            .add((*plan).wtParam as usize);
        Assert!((*param).execPlan.is_null());
        Assert!(!(*param).isnull);
        (*node).rustate = castNode!(
            RecursiveUnionState,
            T_RecursiveUnionState,
            DatumGetPointer((*param).value)
        );
        Assert!(!(*node).rustate.is_null());

        /*
         * The scan tuple type (ie, the rowtype we expect to find in the work
         * table) is the same as the result rowtype of the ancestor
         * RecursiveUnion node.  Note this depends on the assumption that
         * RecursiveUnion doesn't allow projection.
         */
        ExecAssignScanType(
            &mut (*node).ss,
            ExecGetResultType(&mut (*(*node).rustate).ps),
        );

        /*
         * Now we can initialize the projection info.  This must be completed
         * before we can call ExecScan().
         */
        ExecAssignScanProjectionInfo(&mut (*node).ss);
    }

    /*
     * C casts the function pointers to (ExecScanAccessMtd) / (ExecScanRecheckMtd);
     * the access/recheck mtds are declared over WorkTableScanState* but the
     * typedefs take ScanState*.  Mirror that cast with a transmute.
     */
    ExecScan(
        &mut (*node).ss,
        Some(std::mem::transmute::<_, unsafe fn(*mut ScanState) -> *mut TupleTableSlot>(
            WorkTableScanNext as unsafe fn(*mut WorkTableScanState) -> *mut TupleTableSlot,
        )),
        Some(std::mem::transmute::<
            _,
            unsafe fn(*mut ScanState, *mut TupleTableSlot) -> bool,
        >(
            WorkTableScanRecheck
                as unsafe fn(*mut WorkTableScanState, *mut TupleTableSlot) -> bool,
        )),
    )
}

/* ----------------------------------------------------------------
 *		ExecInitWorkTableScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitWorkTableScan(
    node: *mut WorkTableScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut WorkTableScanState {
    let scanstate: *mut WorkTableScanState;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * WorkTableScan should not have any children.
     */
    Assert!(outerPlan(node as *mut Plan).is_null());
    Assert!(innerPlan(node as *mut Plan).is_null());

    /*
     * create new WorkTableScanState for node
     */
    scanstate = makeNode!(WorkTableScanState, T_WorkTableScanState);
    (*scanstate).ss.ps.plan = node as *mut Plan;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecWorkTableScan);
    (*scanstate).rustate = null_mut(); /* we'll set this later */

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*scanstate).ss.ps);

    /*
     * tuple table initialization
     */
    ExecInitResultTypeTL(&mut (*scanstate).ss.ps);

    /* signal that return type is not yet known */
    (*scanstate).ss.ps.resultopsset = true;
    (*scanstate).ss.ps.resultopsfixed = false;

    ExecInitScanTupleSlot(estate, &mut (*scanstate).ss, null_mut(), &TTSOpsMinimalTuple);

    /*
     * initialize child expressions
     */
    (*scanstate).ss.ps.qual = ExecInitQual(
        (*node).scan.plan.qual,
        scanstate as *mut PlanState,
    );

    /*
     * Do not yet initialize projection info, see ExecWorkTableScan() for
     * details.
     */

    scanstate
}

/* ----------------------------------------------------------------
 *		ExecReScanWorkTableScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanWorkTableScan(node: *mut WorkTableScanState) {
    if !(*node).ss.ps.ps_ResultTupleSlot.is_null() {
        ExecClearTuple((*node).ss.ps.ps_ResultTupleSlot);
    }

    ExecScanReScan(&mut (*node).ss);

    /* No need (or way) to rescan if ExecWorkTableScan not called yet */
    if !(*node).rustate.is_null() {
        tuplestore_rescan((*(*node).rustate).working_table);
    }
}
