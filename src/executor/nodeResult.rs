//! nodeResult.c - support for constant nodes needing special code.
//!
//! postgres source: src/backend/executor/nodeResult.c
//! companion header: src/include/executor/nodeResult.h
//!
//! DESCRIPTION
//!
//!     Result nodes are used in queries where no relations are scanned.
//!     Examples of such queries are:
//!
//!             select 1 * 2
//!
//!             insert into emp values ('mike', 15000)
//!
//!     (Remember that in an INSERT or UPDATE, we need a plan tree that
//!     generates the new rows.)
//!
//!     Result nodes are also used to optimise queries with constant
//!     qualifications (ie, quals that do not depend on the scanned data),
//!     such as:
//!
//!             select * from emp where 2 > 1
//!
//!     In this case, the plan generated is
//!
//!                     Result  (with 2 > 1 qual)
//!                     /
//!                SeqScan (emp.*)
//!
//!     At runtime, the Result node evaluates the constant qual once,
//!     which is shown by EXPLAIN as a One-Time Filter.  If it's
//!     false, we can return an empty result set without running the
//!     controlled plan at all.  If it's true, we run the controlled
//!     plan normally and pass back the results.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::c_int;
use std::ptr::null_mut;

use crate::nodes::execnodes::{
    outerPlanState, EState, ExprContext, PlanState, ResultState,
};
use crate::nodes::nodes::NodeTag;
use crate::nodes::plannodes::{innerPlan, outerPlan, Plan, Result};
use crate::nodes::pg_list::List;
use crate::executor::tuptable::{TupIsNull, TupleTableSlot};
use crate::{castNode, makeNode, Assert};

use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::executor::executor::{
    ExecAssignExprContext, ExecAssignProjectionInfo, ExecEndNode, ExecInitNode,
    ExecInitQual, ExecInitResultTupleSlotTL, ExecMarkPos, ExecProcNode, ExecProject,
    ExecQual, ExecReScan, ExecRestrPos, ResetExprContext, EXEC_FLAG_BACKWARD,
    EXEC_FLAG_MARK,
};
use crate::executor::execTuples::TTSOpsVirtual;

use crate::utils::elog::{DEBUG2, ERROR};
use crate::elog;

/* ----------------------------------------------------------------
 *		ExecResult(node)
 *
 *		returns the tuples from the outer plan which satisfy the
 *		qualification clause.  Since result nodes with right
 *		subtrees are never planned, we ignore the right subtree
 *		entirely (for now).. -cim 10/7/89
 *
 *		The qualification containing only constant clauses are
 *		checked first before any processing is done. It always returns
 *		'nil' if the constant qualification is not satisfied.
 * ----------------------------------------------------------------
 */
unsafe fn ExecResult(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut ResultState = castNode!(ResultState, T_ResultState, pstate);
    let outerTupleSlot: *mut TupleTableSlot;
    let outerPlan: *mut PlanState;
    let econtext: *mut ExprContext;

    CHECK_FOR_INTERRUPTS();

    econtext = (*node).ps.ps_ExprContext;

    /*
     * check constant qualifications like (2 > 1), if not already done
     */
    if (*node).rs_checkqual {
        let qualResult: bool = ExecQual((*node).resconstantqual, econtext);

        (*node).rs_checkqual = false;
        if !qualResult {
            (*node).rs_done = true;
            return null_mut();
        }
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * if rs_done is true then it means that we were asked to return a
     * constant tuple and we already did the last time ExecResult() was
     * called, OR that we failed the constant qual check. Either way, now we
     * are through.
     */
    if !(*node).rs_done {
        outerPlan = outerPlanState(node as *mut PlanState);

        if !outerPlan.is_null() {
            /*
             * retrieve tuples from the outer plan until there are no more.
             */
            outerTupleSlot = ExecProcNode(outerPlan);

            if TupIsNull(outerTupleSlot) {
                return null_mut();
            }

            /*
             * prepare to compute projection expressions, which will expect to
             * access the input tuples as varno OUTER.
             */
            (*econtext).ecxt_outertuple = outerTupleSlot;
        } else {
            /*
             * if we don't have an outer plan, then we are just generating the
             * results from a constant target list.  Do it only once.
             */
            (*node).rs_done = true;
        }

        /* form the result tuple using ExecProject(), and return it */
        return ExecProject((*node).ps.ps_ProjInfo);
    }

    null_mut()
}

/* ----------------------------------------------------------------
 *		ExecResultMarkPos
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecResultMarkPos(node: *mut ResultState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    if !outerPlan.is_null() {
        ExecMarkPos(outerPlan);
    } else {
        elog!(DEBUG2, "Result nodes do not support mark/restore");
    }
}

/* ----------------------------------------------------------------
 *		ExecResultRestrPos
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecResultRestrPos(node: *mut ResultState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    if !outerPlan.is_null() {
        ExecRestrPos(outerPlan);
    } else {
        elog!(ERROR, "Result nodes do not support mark/restore");
    }
}

/* ----------------------------------------------------------------
 *		ExecInitResult
 *
 *		Creates the run-time state information for the result node
 *		produced by the planner and initializes outer relations
 *		(child nodes).
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitResult(
    node: *mut Result,
    estate: *mut EState,
    eflags: c_int,
) -> *mut ResultState {
    let resstate: *mut ResultState;

    /* check for unsupported flags */
    Assert!(
        (eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) == 0
            || !outerPlan(node as *mut Plan).is_null()
    );

    /*
     * create state structure
     */
    resstate = makeNode!(ResultState, T_ResultState);
    (*resstate).ps.plan = node as *mut Plan;
    (*resstate).ps.state = estate;
    (*resstate).ps.ExecProcNode = Some(ExecResult);

    (*resstate).rs_done = false;
    (*resstate).rs_checkqual = !(*node).resconstantqual.is_null();

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*resstate).ps);

    /*
     * initialize child nodes
     */
    *(&mut (*(resstate as *mut PlanState)).lefttree) =
        ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);

    /*
     * we don't use inner plan
     */
    Assert!(innerPlan(node as *mut Plan).is_null());

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlotTL(&mut (*resstate).ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&mut (*resstate).ps, null_mut());

    /*
     * initialize child expressions
     */
    (*resstate).ps.qual =
        ExecInitQual((*node).plan.qual, resstate as *mut PlanState);
    (*resstate).resconstantqual =
        ExecInitQual((*node).resconstantqual as *mut List, resstate as *mut PlanState);

    resstate
}

/* ----------------------------------------------------------------
 *		ExecEndResult
 *
 *		frees up storage allocated through C routines
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndResult(node: *mut ResultState) {
    /*
     * shut down subplans
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));
}

pub unsafe fn ExecReScanResult(node: *mut ResultState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    (*node).rs_done = false;
    (*node).rs_checkqual = !(*node).resconstantqual.is_null();

    /*
     * If chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if !outerPlan.is_null() && (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}
