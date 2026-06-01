//! nodeGroup.c - routines to handle group nodes (used for queries with GROUP BY clause).
//!
//! The Group node is designed for handling queries with a GROUP BY clause.
//! Its outer plan must deliver tuples that are sorted in the order specified
//! by the grouping columns (ie. tuples from the same group are consecutive).
//! That way, we just have to compare adjacent tuples to locate group
//! boundaries.

use crate::prelude::*;

use std::ptr::null_mut;

use crate::nodes::execnodes::{
    outerPlanState, EState, ExprContext, GroupState, PlanState, ScanState,
};
use crate::nodes::plannodes::{outerPlan, Group, Plan};

use crate::executor::instrument::Instrumentation;

use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};

use crate::executor::execGrouping::execTuplesMatchPrepare;
use crate::executor::execTuples::TTSOpsVirtual;
use crate::executor::executor::{
    ExecAssignExprContext, ExecAssignProjectionInfo, ExecCreateScanSlotFromOuterPlan, ExecEndNode,
    ExecGetResultSlotOps, ExecGetResultType, ExecInitNode, ExecInitQual,
    ExecInitResultTupleSlotTL, ExecProcNode, ExecProject, ExecQual, ExecQualAndReset, ExecReScan,
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::tuptable::{ExecClearTuple, ExecCopySlot, TupIsNull};
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::{castNode, makeNode, Assert};

/*
 * InstrCountFiltered1(node, delta) -- accumulate `delta` into the node's
 * per-tuple "filtered by qual" counter, but only when instrumentation is
 * active for the node.
 *
 *   #define InstrCountFiltered1(node, delta) \
 *       do { \
 *           if (((PlanState *)(node))->instrument) \
 *               ((PlanState *)(node))->instrument->nfiltered1 += (delta); \
 *       } while(0)
 */
#[inline]
unsafe fn InstrCountFiltered1(node: *mut GroupState, delta: f64) {
    let instrument: *mut Instrumentation = (*node).ss.ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered1 += delta;
    }
}

/*
 *	 ExecGroup -
 *
 *		Return one tuple for each group of matching input tuples.
 */
unsafe fn ExecGroup(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut GroupState = castNode!(GroupState, T_GroupState, pstate);
    let econtext: *mut ExprContext;
    let firsttupleslot: *mut TupleTableSlot;
    let mut outerslot: *mut TupleTableSlot;

    CHECK_FOR_INTERRUPTS();

    /*
     * get state info from node
     */
    if (*node).grp_done {
        return null_mut();
    }
    econtext = (*node).ss.ps.ps_ExprContext;

    /*
     * The ScanTupleSlot holds the (copied) first tuple of each group.
     */
    firsttupleslot = (*node).ss.ss_ScanTupleSlot;

    /*
     * We need not call ResetExprContext here because ExecQualAndReset() will
     * reset the per-tuple memory context once per input tuple.
     */

    /*
     * If first time through, acquire first input tuple and determine whether
     * to return it or not.
     */
    if TupIsNull(firsttupleslot) {
        outerslot = ExecProcNode(outerPlanState(node as *mut PlanState));
        if TupIsNull(outerslot) {
            /* empty input, so return nothing */
            (*node).grp_done = true;
            return null_mut();
        }
        /* Copy tuple into firsttupleslot */
        ExecCopySlot(firsttupleslot, outerslot);

        /*
         * Set it up as input for qual test and projection.  The expressions
         * will access the input tuple as varno OUTER.
         */
        (*econtext).ecxt_outertuple = firsttupleslot;

        /*
         * Check the qual (HAVING clause); if the group does not match, ignore
         * it and fall into scan loop.
         */
        if ExecQual((*node).ss.ps.qual, econtext) {
            /*
             * Form and return a projection tuple using the first input tuple.
             */
            return ExecProject((*node).ss.ps.ps_ProjInfo);
        } else {
            InstrCountFiltered1(node, 1.0);
        }
    }

    /*
     * This loop iterates once per input tuple group.  At the head of the
     * loop, we have finished processing the first tuple of the group and now
     * need to scan over all the other group members.
     */
    loop {
        /*
         * Scan over all remaining tuples that belong to this group
         */
        loop {
            outerslot = ExecProcNode(outerPlanState(node as *mut PlanState));
            if TupIsNull(outerslot) {
                /* no more groups, so we're done */
                (*node).grp_done = true;
                return null_mut();
            }

            /*
             * Compare with first tuple and see if this tuple is of the same
             * group.  If so, ignore it and keep scanning.
             */
            (*econtext).ecxt_innertuple = firsttupleslot;
            (*econtext).ecxt_outertuple = outerslot;
            if !ExecQualAndReset((*node).eqfunction, econtext) {
                break;
            }
        }

        /*
         * We have the first tuple of the next input group.  See if we want to
         * return it.
         */
        /* Copy tuple, set up as input for qual test and projection */
        ExecCopySlot(firsttupleslot, outerslot);
        (*econtext).ecxt_outertuple = firsttupleslot;

        /*
         * Check the qual (HAVING clause); if the group does not match, ignore
         * it and loop back to scan the rest of the group.
         */
        if ExecQual((*node).ss.ps.qual, econtext) {
            /*
             * Form and return a projection tuple using the first input tuple.
             */
            return ExecProject((*node).ss.ps.ps_ProjInfo);
        } else {
            InstrCountFiltered1(node, 1.0);
        }
    }
}

/* -----------------
 * ExecInitGroup
 *
 *	Creates the run-time information for the group node produced by the
 *	planner and initializes its outer subtree
 * -----------------
 */
pub unsafe fn ExecInitGroup(
    node: *mut Group,
    estate: *mut EState,
    eflags: c_int,
) -> *mut GroupState {
    let grpstate: *mut GroupState;
    let tts_ops: *const TupleTableSlotOps;

    /* check for unsupported flags */
    Assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    /*
     * create state structure
     */
    grpstate = makeNode!(GroupState, T_GroupState);
    (*grpstate).ss.ps.plan = node as *mut Plan;
    (*grpstate).ss.ps.state = estate;
    (*grpstate).ss.ps.ExecProcNode = Some(ExecGroup);
    (*grpstate).grp_done = false;

    /*
     * create expression context
     */
    ExecAssignExprContext(estate, &mut (*grpstate).ss.ps);

    /*
     * initialize child nodes
     */
    *(&mut (*(grpstate as *mut PlanState)).lefttree) =
        ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);

    /*
     * Initialize scan slot and type.
     */
    tts_ops = ExecGetResultSlotOps(
        outerPlanState(&mut (*grpstate).ss as *mut ScanState as *mut PlanState),
        null_mut(),
    );
    ExecCreateScanSlotFromOuterPlan(estate, &mut (*grpstate).ss as *mut ScanState as *mut _, tts_ops);

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlotTL(&mut (*grpstate).ss.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&mut (*grpstate).ss.ps, null_mut());

    /*
     * initialize child expressions
     */
    (*grpstate).ss.ps.qual = ExecInitQual(
        (*node).plan.qual,
        grpstate as *mut PlanState,
    );

    /*
     * Precompute fmgr lookup data for inner loop
     */
    (*grpstate).eqfunction = execTuplesMatchPrepare(
        ExecGetResultType(outerPlanState(grpstate as *mut PlanState)) as *mut _,
        (*node).numCols,
        (*node).grpColIdx,
        (*node).grpOperators,
        (*node).grpCollations,
        &mut (*grpstate).ss.ps as *mut PlanState as *mut _,
    ) as *mut _;

    grpstate
}

/* ------------------------
 *		ExecEndGroup(node)
 *
 * -----------------------
 */
pub unsafe fn ExecEndGroup(node: *mut GroupState) {
    let outerPlan: *mut PlanState;

    outerPlan = outerPlanState(node as *mut PlanState);
    ExecEndNode(outerPlan);
}

pub unsafe fn ExecReScanGroup(node: *mut GroupState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    (*node).grp_done = false;
    /* must clear first tuple */
    ExecClearTuple((*node).ss.ss_ScanTupleSlot);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}
