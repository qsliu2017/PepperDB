//! nodeUnique.c - filter out duplicate tuples from a stream of sorted tuples.
//!
//! Unique is a very simple node type that just filters out duplicate tuples
//! from a stream of sorted tuples from its subplan.  It's essentially a
//! dumbed-down form of Group: the duplicate-removal functionality is
//! identical.  However, Unique doesn't do projection nor qual checking, so
//! it's marginally more efficient for cases where neither is needed.
//!
//! Assumes tuples returned from subplan arrive in sorted order.

use crate::prelude::*;

use std::ptr::null_mut;

use crate::nodes::execnodes::{
    outerPlanState, EState, ExprContext, PlanState, UniqueState,
};
use crate::nodes::plannodes::{outerPlan, Plan, Unique};

use crate::executor::tuptable::TupleTableSlot;

use crate::executor::execGrouping::execTuplesMatchPrepare;
use crate::executor::execTuples::TTSOpsMinimalTuple;
use crate::executor::executor::{
    ExecAssignExprContext, ExecEndNode, ExecGetResultType, ExecInitNode,
    ExecInitResultTupleSlotTL, ExecProcNode, ExecQualAndReset, ExecReScan,
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::tuptable::{ExecClearTuple, ExecCopySlot, TupIsNull};
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::{castNode, makeNode, Assert};

/* ----------------------------------------------------------------
 *		ExecUnique
 * ----------------------------------------------------------------
 */
unsafe fn ExecUnique(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut UniqueState = castNode!(UniqueState, T_UniqueState, pstate);
    let econtext: *mut ExprContext = (*node).ps.ps_ExprContext;
    let resultTupleSlot: *mut TupleTableSlot;
    let mut slot: *mut TupleTableSlot;
    let outerPlan: *mut PlanState;

    CHECK_FOR_INTERRUPTS();

    /*
     * get information from the node
     */
    outerPlan = outerPlanState(node as *mut PlanState);
    resultTupleSlot = (*node).ps.ps_ResultTupleSlot;

    /*
     * now loop, returning only non-duplicate tuples. We assume that the
     * tuples arrive in sorted order so we can detect duplicates easily. The
     * first tuple of each group is returned.
     */
    loop {
        /*
         * fetch a tuple from the outer subplan
         */
        slot = ExecProcNode(outerPlan);
        if TupIsNull(slot) {
            /* end of subplan, so we're done */
            ExecClearTuple(resultTupleSlot);
            return null_mut();
        }

        /*
         * Always return the first tuple from the subplan.
         */
        if TupIsNull(resultTupleSlot) {
            break;
        }

        /*
         * Else test if the new tuple and the previously returned tuple match.
         * If so then we loop back and fetch another new tuple from the
         * subplan.
         */
        (*econtext).ecxt_innertuple = slot;
        (*econtext).ecxt_outertuple = resultTupleSlot;
        if !ExecQualAndReset((*node).eqfunction, econtext) {
            break;
        }
    }

    /*
     * We have a new tuple different from the previous saved tuple (if any).
     * Save it and return it.  We must copy it because the source subplan
     * won't guarantee that this source tuple is still accessible after
     * fetching the next source tuple.
     */
    ExecCopySlot(resultTupleSlot, slot)
}

/* ----------------------------------------------------------------
 *		ExecInitUnique
 *
 *		This initializes the unique node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitUnique(
    node: *mut Unique,
    estate: *mut EState,
    eflags: c_int,
) -> *mut UniqueState {
    let uniquestate: *mut UniqueState;

    /* check for unsupported flags */
    Assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    /*
     * create state structure
     */
    uniquestate = makeNode!(UniqueState, T_UniqueState);
    (*uniquestate).ps.plan = node as *mut Plan;
    (*uniquestate).ps.state = estate;
    (*uniquestate).ps.ExecProcNode = Some(ExecUnique);

    /*
     * create expression context
     */
    ExecAssignExprContext(estate, &mut (*uniquestate).ps);

    /*
     * then initialize outer plan
     */
    (*(uniquestate as *mut PlanState)).lefttree =
        ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);

    /*
     * Initialize result slot and type. Unique nodes do no projections, so
     * initialize projection info for this node appropriately.
     */
    ExecInitResultTupleSlotTL(&mut (*uniquestate).ps, &TTSOpsMinimalTuple);
    (*uniquestate).ps.ps_ProjInfo = null_mut();

    /*
     * Precompute fmgr lookup data for inner loop
     */
    (*uniquestate).eqfunction = execTuplesMatchPrepare(
        ExecGetResultType(outerPlanState(uniquestate as *mut PlanState)) as *mut _,
        (*node).numCols,
        (*node).uniqColIdx,
        (*node).uniqOperators,
        (*node).uniqCollations,
        &mut (*uniquestate).ps as *mut PlanState as *mut _,
    ) as *mut _;

    uniquestate
}

/* ----------------------------------------------------------------
 *		ExecEndUnique
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndUnique(node: *mut UniqueState) {
    ExecEndNode(outerPlanState(node as *mut PlanState));
}

pub unsafe fn ExecReScanUnique(node: *mut UniqueState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    /* must clear result tuple so first input tuple is returned */
    ExecClearTuple((*node).ps.ps_ResultTupleSlot);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}
