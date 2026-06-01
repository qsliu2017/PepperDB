//! nodeNestloop.c - routines to support nest-loop joins
//!
//! Postgres source: src/backend/executor/nodeNestloop.c
//! Companion header: src/include/executor/nodeNestloop.h
//!
//! INTERFACE ROUTINES
//!     ExecNestLoop     - process a nestloop join of two plans
//!     ExecInitNestLoop - initialize the join
//!     ExecEndNestLoop  - shut down the join

use crate::prelude::*;

use crate::nodes::bitmapset::bms_add_member;
use crate::nodes::execnodes::{
    innerPlanState, outerPlanState, EState, ExprContext, ExprState, NestLoopState, PlanState,
    ProjectionInfo,
};
use crate::nodes::nodes::JoinType::{self, JOIN_ANTI, JOIN_INNER, JOIN_LEFT, JOIN_SEMI};
use crate::nodes::params::ParamExecData;
use crate::nodes::pg_list::{ListCell, NIL, lfirst};
use crate::nodes::plannodes::{innerPlan, outerPlan, NestLoop, NestLoopParam, Plan};
use crate::nodes::primnodes::{Var, OUTER_VAR};

use crate::executor::execTuples::TTSOpsVirtual;
use crate::executor::execUtils::{
    ExecAssignExprContext, ExecAssignProjectionInfo, ExecGetResultType,
};
use crate::executor::executor::{
    ExecEndNode, ExecInitNode, ExecInitNullTupleSlot, ExecInitQual, ExecInitResultTupleSlotTL,
    ExecProcNode, ExecProject, ExecQual, ExecReScan, ResetExprContext, EXEC_FLAG_BACKWARD,
    EXEC_FLAG_MARK, EXEC_FLAG_REWIND,
};
use crate::executor::instrument::Instrumentation;
use crate::executor::tuptable::{slot_getattr, TupIsNull, TupleTableSlot};
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::{castNode, foreach, current_cell, makeNode, Assert, IsA};

/*
 * Debug tracing macros from executor/execdebug.h.  These compile to nothing
 * unless EXEC_NESTLOOPDEBUG / EXEC_INITDEINIT_DEBUG are defined.
 */
macro_rules! ENL1_printf {
    ($($arg:tt)*) => {{}};
}
macro_rules! NL1_printf {
    ($($arg:tt)*) => {{}};
}

/*
 * Instrumentation helpers; see executor/instrument.h.
 *
 *   #define InstrCountFiltered1(node, delta) \
 *       do { \
 *           if (((PlanState *)(node))->instrument) \
 *               ((PlanState *)(node))->instrument->nfiltered1 += (delta); \
 *       } while(0)
 */
#[inline]
unsafe fn InstrCountFiltered1(node: *mut NestLoopState, delta: f64) {
    let instrument: *mut Instrumentation = (*node).js.ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered1 += delta;
    }
}

#[inline]
unsafe fn InstrCountFiltered2(node: *mut NestLoopState, delta: f64) {
    let instrument: *mut Instrumentation = (*node).js.ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered2 += delta;
    }
}

/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
unsafe fn ExecNestLoop(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut NestLoopState = castNode!(NestLoopState, T_NestLoopState, pstate);
    let nl: *mut NestLoop;
    let innerPlan: *mut PlanState;
    let outerPlan: *mut PlanState;
    let mut outerTupleSlot: *mut TupleTableSlot;
    let mut innerTupleSlot: *mut TupleTableSlot;
    let joinqual: *mut ExprState;
    let otherqual: *mut ExprState;
    let econtext: *mut ExprContext;

    CHECK_FOR_INTERRUPTS();

    /*
     * get information from the node
     */
    ENL1_printf!("getting info from node");

    nl = (*node).js.ps.plan as *mut NestLoop;
    joinqual = (*node).js.joinqual;
    otherqual = (*node).js.ps.qual;
    outerPlan = outerPlanState(node as *mut PlanState);
    innerPlan = innerPlanState(node as *mut PlanState);
    econtext = (*node).js.ps.ps_ExprContext;

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * Ok, everything is setup for the join so now loop until we return a
     * qualifying join tuple.
     */
    ENL1_printf!("entering main loop");

    loop {
        /*
         * If we don't have an outer tuple, get the next one and reset the
         * inner scan.
         */
        if (*node).nl_NeedNewOuter {
            ENL1_printf!("getting new outer tuple");
            outerTupleSlot = ExecProcNode(outerPlan);

            /*
             * if there are no more outer tuples, then the join is complete..
             */
            if TupIsNull(outerTupleSlot) {
                ENL1_printf!("no outer tuple, ending join");
                return std::ptr::null_mut();
            }

            ENL1_printf!("saving new outer tuple information");
            (*econtext).ecxt_outertuple = outerTupleSlot;
            (*node).nl_NeedNewOuter = false;
            (*node).nl_MatchedOuter = false;

            /*
             * fetch the values of any outer Vars that must be passed to the
             * inner scan, and store them in the appropriate PARAM_EXEC slots.
             */
            foreach!(lc, (*nl).nestParams, {
                let nlp: *mut NestLoopParam = lfirst(current_cell!(lc)) as *mut NestLoopParam;
                let paramno: c_int = (*nlp).paramno;
                let prm: *mut ParamExecData;

                prm = &mut (*(*econtext).ecxt_param_exec_vals.offset(paramno as isize));
                /* Param value should be an OUTER_VAR var */
                Assert!(IsA!((*nlp).paramval, T_Var));
                Assert!((*(*nlp).paramval).varno == OUTER_VAR);
                Assert!((*(*nlp).paramval).varattno > 0);
                (*prm).value = slot_getattr(
                    outerTupleSlot,
                    (*(*nlp).paramval).varattno as c_int,
                    &mut (*prm).isnull,
                );
                /* Flag parameter value as changed */
                (*innerPlan).chgParam = bms_add_member((*innerPlan).chgParam, paramno);
            });

            /*
             * now rescan the inner plan
             */
            ENL1_printf!("rescanning inner plan");
            ExecReScan(innerPlan);
        }

        /*
         * we have an outerTuple, try to get the next inner tuple.
         */
        ENL1_printf!("getting new inner tuple");

        innerTupleSlot = ExecProcNode(innerPlan);
        (*econtext).ecxt_innertuple = innerTupleSlot;

        if TupIsNull(innerTupleSlot) {
            ENL1_printf!("no inner tuple, need new outer tuple");

            (*node).nl_NeedNewOuter = true;

            if !(*node).nl_MatchedOuter
                && ((*node).js.jointype == JOIN_LEFT || (*node).js.jointype == JOIN_ANTI)
            {
                /*
                 * We are doing an outer join and there were no join matches
                 * for this outer tuple.  Generate a fake join tuple with
                 * nulls for the inner tuple, and return it if it passes the
                 * non-join quals.
                 */
                (*econtext).ecxt_innertuple = (*node).nl_NullInnerTupleSlot;

                ENL1_printf!("testing qualification for outer-join tuple");

                if otherqual.is_null() || ExecQual(otherqual, econtext) {
                    /*
                     * qualification was satisfied so we project and return
                     * the slot containing the result tuple using
                     * ExecProject().
                     */
                    ENL1_printf!("qualification succeeded, projecting tuple");

                    return ExecProject((*node).js.ps.ps_ProjInfo);
                } else {
                    InstrCountFiltered2(node, 1.0);
                }
            }

            /*
             * Otherwise just return to top of loop for a new outer tuple.
             */
            continue;
        }

        /*
         * at this point we have a new pair of inner and outer tuples so we
         * test the inner and outer tuples to see if they satisfy the node's
         * qualification.
         *
         * Only the joinquals determine MatchedOuter status, but all quals
         * must pass to actually return the tuple.
         */
        ENL1_printf!("testing qualification");

        if ExecQual(joinqual, econtext) {
            (*node).nl_MatchedOuter = true;

            /* In an antijoin, we never return a matched tuple */
            if (*node).js.jointype == JOIN_ANTI {
                (*node).nl_NeedNewOuter = true;
                continue; /* return to top of loop */
            }

            /*
             * If we only need to join to the first matching inner tuple, then
             * consider returning this one, but after that continue with next
             * outer tuple.
             */
            if (*node).js.single_match {
                (*node).nl_NeedNewOuter = true;
            }

            if otherqual.is_null() || ExecQual(otherqual, econtext) {
                /*
                 * qualification was satisfied so we project and return the
                 * slot containing the result tuple using ExecProject().
                 */
                ENL1_printf!("qualification succeeded, projecting tuple");

                return ExecProject((*node).js.ps.ps_ProjInfo);
            } else {
                InstrCountFiltered2(node, 1.0);
            }
        } else {
            InstrCountFiltered1(node, 1.0);
        }

        /*
         * Tuple fails qual, so free per-tuple memory and try again.
         */
        ResetExprContext(econtext);

        ENL1_printf!("qualification failed, looping");
    }
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitNestLoop(
    node: *mut NestLoop,
    estate: *mut EState,
    mut eflags: c_int,
) -> *mut NestLoopState {
    let nlstate: *mut NestLoopState;

    /* check for unsupported flags */
    Assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    NL1_printf!("ExecInitNestLoop: {}\n", "initializing node");

    /*
     * create state structure
     */
    nlstate = makeNode!(NestLoopState, T_NestLoopState);
    (*nlstate).js.ps.plan = node as *mut Plan;
    (*nlstate).js.ps.state = estate;
    (*nlstate).js.ps.ExecProcNode = Some(ExecNestLoop);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*nlstate).js.ps);

    /*
     * initialize child nodes
     *
     * If we have no parameters to pass into the inner rel from the outer,
     * tell the inner child that cheap rescans would be good.  If we do have
     * such parameters, then there is no point in REWIND support at all in the
     * inner child, because it will always be rescanned with fresh parameter
     * values.
     */
    (*nlstate).js.ps.lefttree = ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);
    if (*node).nestParams == NIL {
        eflags |= EXEC_FLAG_REWIND;
    } else {
        eflags &= !EXEC_FLAG_REWIND;
    }
    (*nlstate).js.ps.righttree = ExecInitNode(innerPlan(node as *mut Plan), estate, eflags);

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlotTL(&mut (*nlstate).js.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&mut (*nlstate).js.ps, std::ptr::null_mut());

    /*
     * initialize child expressions
     */
    (*nlstate).js.ps.qual = ExecInitQual((*node).join.plan.qual, nlstate as *mut PlanState);
    (*nlstate).js.jointype = (*node).join.jointype;
    (*nlstate).js.joinqual = ExecInitQual((*node).join.joinqual, nlstate as *mut PlanState);

    /*
     * detect whether we need only consider the first matching inner tuple
     */
    (*nlstate).js.single_match =
        (*node).join.inner_unique || (*node).join.jointype == JOIN_SEMI;

    /* set up null tuples for outer joins, if needed */
    match (*node).join.jointype {
        JOIN_INNER | JOIN_SEMI => {}
        JOIN_LEFT | JOIN_ANTI => {
            (*nlstate).nl_NullInnerTupleSlot = ExecInitNullTupleSlot(
                estate,
                ExecGetResultType(innerPlanState(nlstate as *mut PlanState)),
                &TTSOpsVirtual,
            );
        }
        _ => {
            elog!(
                ERROR,
                "unrecognized join type: {}",
                (*node).join.jointype as c_int
            );
        }
    }

    /*
     * finally, wipe the current outer tuple clean.
     */
    (*nlstate).nl_NeedNewOuter = true;
    (*nlstate).nl_MatchedOuter = false;

    NL1_printf!("ExecInitNestLoop: {}\n", "node initialized");

    nlstate
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndNestLoop(node: *mut NestLoopState) {
    NL1_printf!("ExecEndNestLoop: {}\n", "ending node processing");

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));
    ExecEndNode(innerPlanState(node as *mut PlanState));

    NL1_printf!("ExecEndNestLoop: {}\n", "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanNestLoop(node: *mut NestLoopState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    /*
     * If outerPlan->chgParam is not null then plan will be automatically
     * re-scanned by first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }

    /*
     * innerPlan is re-scanned for each new outer tuple and MUST NOT be
     * re-scanned from here or you'll get troubles from inner index scans when
     * outer Vars are used as run-time keys...
     */

    (*node).nl_NeedNewOuter = true;
    (*node).nl_MatchedOuter = false;
}
