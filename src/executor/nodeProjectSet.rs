//! nodeProjectSet.c
//!   support for evaluating targetlists containing set-returning functions
//!
//! DESCRIPTION
//!
//!     ProjectSet nodes are inserted by the planner to evaluate set-returning
//!     functions in the targetlist.  It's guaranteed that all set-returning
//!     functions are directly at the top level of the targetlist, i.e. they
//!     can't be inside more-complex expressions.  If that'd otherwise be
//!     the case, the planner adds additional ProjectSet nodes.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!     src/backend/executor/nodeProjectSet.c
//!     src/include/executor/nodeProjectSet.h

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::ptr;

use crate::{castNode, foreach, current_cell, makeNode, IsA};

use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::nodes::execnodes::{
    outerPlanState, EState, ExprDoneCond, ExprState, PlanState, ProjectSetState, SetExprState,
};
use crate::nodes::execnodes::ExprDoneCond::*;
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{lfirst, list_length, List, ListCell, NIL};
use crate::nodes::plannodes::{innerPlan, outerPlan, Plan, ProjectSet};
use crate::nodes::primnodes::{Expr, FuncExpr, OpExpr, TargetEntry};
use crate::nodes::nodeFuncs::expression_returns_set;

use crate::executor::tuptable::{TupIsNull, TupleTableSlot};
use crate::executor::execTuples::{ExecStoreVirtualTuple, TTSOpsVirtual};
use crate::executor::executor::{
    ExecEndNode, ExecEvalExpr, ExecInitExpr, ExecInitFunctionResultSet, ExecInitNode,
    ExecInitResultTupleSlotTL, ExecMakeFunctionResultSet, ExecProcNode, ExecReScan,
    ResetExprContext, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::tuptable::ExecClearTuple;
use crate::executor::execUtils::ExecAssignExprContext;

use crate::nodes::execnodes::ExprContext;
use crate::utils::mmgr::mcxt::CurrentMemoryContext;

/* ----------------------------------------------------------------
 *      ExecProjectSet(node)
 *
 *      Return tuples after evaluating the targetlist (which contains set
 *      returning functions).
 * ----------------------------------------------------------------
 */
unsafe fn ExecProjectSet(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut ProjectSetState = castNode!(ProjectSetState, T_ProjectSetState, pstate);
    let mut outerTupleSlot: *mut TupleTableSlot;
    let mut resultSlot: *mut TupleTableSlot;
    let mut outerPlan: *mut PlanState;
    let econtext: *mut ExprContext;

    CHECK_FOR_INTERRUPTS();

    econtext = (*node).ps.ps_ExprContext;

    /*
     * Reset per-tuple context to free expression-evaluation storage allocated
     * for a potentially previously returned tuple. Note that the SRF argument
     * context has a different lifetime and is reset below.
     */
    ResetExprContext(econtext);

    /*
     * Check to see if we're still projecting out tuples from a previous scan
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (*node).pending_srf_tuples {
        resultSlot = ExecProjectSRF(node, true);

        if resultSlot != ptr::null_mut() {
            return resultSlot;
        }
    }

    /*
     * Get another input tuple and project SRFs from it.
     */
    loop {
        /*
         * Reset argument context to free any expression evaluation storage
         * allocated in the previous tuple cycle.  Note this can't happen
         * until we're done projecting out tuples from a scan tuple, as
         * ValuePerCall functions are allowed to reference the arguments for
         * each returned tuple.  However, if we loop around after finding that
         * no rows are produced from a scan tuple, we should reset, to avoid
         * leaking memory when many successive scan tuples produce no rows.
         */
        MemoryContextReset((*node).argcontext as *mut _);

        /*
         * Retrieve tuples from the outer plan until there are no more.
         */
        outerPlan = outerPlanState(node as *mut PlanState);
        outerTupleSlot = ExecProcNode(outerPlan);

        if TupIsNull(outerTupleSlot) {
            return ptr::null_mut();
        }

        /*
         * Prepare to compute projection expressions, which will expect to
         * access the input tuples as varno OUTER.
         */
        (*econtext).ecxt_outertuple = outerTupleSlot;

        /* Evaluate the expressions */
        resultSlot = ExecProjectSRF(node, false);

        /*
         * Return the tuple unless the projection produced no rows (due to an
         * empty set), in which case we must loop back to see if there are
         * more outerPlan tuples.
         */
        if !resultSlot.is_null() {
            return resultSlot;
        }

        /*
         * When we do loop back, we'd better reset the econtext again, just in
         * case the SRF leaked some memory there.
         */
        ResetExprContext(econtext);
    }
}

/* ----------------------------------------------------------------
 *      ExecProjectSRF
 *
 *      Project a targetlist containing one or more set-returning functions.
 *
 *      'continuing' indicates whether to continue projecting rows for the
 *      same input tuple; or whether a new input tuple is being projected.
 *
 *      Returns NULL if no output tuple has been produced.
 *
 * ----------------------------------------------------------------
 */
unsafe fn ExecProjectSRF(node: *mut ProjectSetState, continuing: bool) -> *mut TupleTableSlot {
    let resultSlot: *mut TupleTableSlot = (*node).ps.ps_ResultTupleSlot;
    let econtext: *mut ExprContext = (*node).ps.ps_ExprContext;
    let oldcontext: MemoryContext;
    #[allow(unused_assignments)]
    let mut hassrf: bool; /* PG_USED_FOR_ASSERTS_ONLY */
    let mut hasresult: bool;
    let mut argno: c_int;

    ExecClearTuple(resultSlot);

    /* Call SRFs, as well as plain expressions, in per-tuple context */
    oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory as *mut _);

    /*
     * Assume no further tuples are produced unless an ExprMultipleResult is
     * encountered from a set returning function.
     */
    (*node).pending_srf_tuples = false;

    hassrf = false;
    hasresult = false;
    argno = 0;
    while argno < (*node).nelems {
        let elem: *mut Node = *(*node).elems.offset(argno as isize);
        let isdone: *mut ExprDoneCond = (*node).elemdone.offset(argno as isize);
        let result: *mut Datum = (*resultSlot).tts_values.offset(argno as isize);
        let isnull: *mut bool = (*resultSlot).tts_isnull.offset(argno as isize);

        if continuing && *isdone == ExprEndResult {
            /*
             * If we're continuing to project output rows from a source tuple,
             * return NULLs once the SRF has been exhausted.
             */
            *result = 0 as Datum;
            *isnull = true;
            hassrf = true;
        } else if IsA!(elem, T_SetExprState) {
            /*
             * Evaluate SRF - possibly continuing previously started output.
             */
            *result = ExecMakeFunctionResultSet(
                elem as *mut SetExprState,
                econtext,
                (*node).argcontext as *mut _,
                isnull,
                isdone,
            );

            if *isdone != ExprEndResult {
                hasresult = true;
            }
            if *isdone == ExprMultipleResult {
                (*node).pending_srf_tuples = true;
            }
            hassrf = true;
        } else {
            /* Non-SRF tlist expression, just evaluate normally. */
            *result = ExecEvalExpr(elem as *mut ExprState, econtext, isnull);
            *isdone = ExprSingleResult;
        }

        argno += 1;
    }

    MemoryContextSwitchTo(oldcontext);

    /* ProjectSet should not be used if there's no SRFs */
    Assert!(hassrf);

    /*
     * If all the SRFs returned ExprEndResult, we consider that as no row
     * being produced.
     */
    if hasresult {
        ExecStoreVirtualTuple(resultSlot);
        return resultSlot;
    }

    ptr::null_mut()
}

/* ----------------------------------------------------------------
 *      ExecInitProjectSet
 *
 *      Creates the run-time state information for the ProjectSet node
 *      produced by the planner and initializes outer relations
 *      (child nodes).
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitProjectSet(
    node: *mut ProjectSet,
    estate: *mut EState,
    eflags: c_int,
) -> *mut ProjectSetState {
    let state: *mut ProjectSetState;
    let mut off: c_int;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD)) == 0);

    /*
     * create state structure
     */
    state = makeNode!(ProjectSetState, T_ProjectSetState);
    (*state).ps.plan = node as *mut Plan;
    (*state).ps.state = estate;
    (*state).ps.ExecProcNode = Some(ExecProjectSet);

    (*state).pending_srf_tuples = false;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*state).ps);

    /*
     * initialize child nodes
     */
    /* outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags); */
    (*state).ps.lefttree = ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);

    /*
     * we don't use inner plan
     */
    Assert!(innerPlan(node as *mut Plan).is_null());

    /*
     * tuple table and result type initialization
     */
    ExecInitResultTupleSlotTL(&mut (*state).ps, &TTSOpsVirtual);

    /* Create workspace for per-tlist-entry expr state & SRF-is-done state */
    (*state).nelems = list_length((*node).plan.targetlist);
    (*state).elems = palloc(size_of::<*mut Node>() * (*state).nelems as usize) as *mut *mut Node;
    (*state).elemdone =
        palloc(size_of::<ExprDoneCond>() * (*state).nelems as usize) as *mut ExprDoneCond;

    /*
     * Build expressions to evaluate targetlist.  We can't use
     * ExecBuildProjectionInfo here, since that doesn't deal with SRFs.
     * Instead compile each expression separately, using
     * ExecInitFunctionResultSet where applicable.
     */
    off = 0;
    foreach!(lc, (*node).plan.targetlist, {
        let te: *mut TargetEntry = lfirst(current_cell!(lc)) as *mut TargetEntry;
        let expr: *mut Expr = (*te).expr;

        if (IsA!(expr, T_FuncExpr) && (*(expr as *mut FuncExpr)).funcretset)
            || (IsA!(expr, T_OpExpr) && (*(expr as *mut OpExpr)).opretset)
        {
            *(*state).elems.offset(off as isize) = ExecInitFunctionResultSet(
                expr,
                (*state).ps.ps_ExprContext,
                &mut (*state).ps,
            ) as *mut Node;
        } else {
            Assert!(!expression_returns_set(expr as *mut Node));
            *(*state).elems.offset(off as isize) =
                ExecInitExpr(expr, &mut (*state).ps) as *mut Node;
        }

        off += 1;
    });

    /* We don't support any qual on ProjectSet nodes */
    Assert!((*node).plan.qual == NIL);

    /*
     * Create a memory context that ExecMakeFunctionResultSet can use to
     * evaluate function arguments in.  We can't use the per-tuple context for
     * this because it gets reset too often; but we don't want to leak
     * evaluation results into the query-lifespan context either.  We use one
     * context for the arguments of all tSRFs, as they have roughly equivalent
     * lifetimes.
     */
    (*state).argcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"tSRF function arguments".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    ) as *mut _;

    state
}

/* ----------------------------------------------------------------
 *      ExecEndProjectSet
 *
 *      frees up storage allocated through C routines
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndProjectSet(node: *mut ProjectSetState) {
    /*
     * shut down subplans
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));
}

pub unsafe fn ExecReScanProjectSet(node: *mut ProjectSetState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    /* Forget any incompletely-evaluated SRFs */
    (*node).pending_srf_tuples = false;

    /*
     * If chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam == ptr::null_mut() {
        ExecReScan(outerPlan);
    }
}
