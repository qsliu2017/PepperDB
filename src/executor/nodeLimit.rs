//! nodeLimit.c - Routines to handle limiting of query results where appropriate.
//!
//! INTERFACE ROUTINES
//!     ExecLimit       - extract a limited range of tuples
//!     ExecInitLimit   - initialize node and subnodes
//!     ExecEndLimit    - shutdown node and subnodes

use crate::prelude::*;

use std::ptr::null_mut;

use crate::access::sdir::{ScanDirection, ScanDirectionIsForward};

use crate::nodes::execnodes::{
    outerPlanState, EState, ExprContext, ExprState, LimitState, PlanState, TupleDesc,
    LIMIT_EMPTY, LIMIT_INITIAL, LIMIT_INWINDOW, LIMIT_RESCAN, LIMIT_SUBPLANEOF, LIMIT_WINDOWEND,
    LIMIT_WINDOWEND_TIES, LIMIT_WINDOWSTART,
};
use crate::nodes::nodes::{LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES};
use crate::nodes::plannodes::{outerPlan, Limit, Plan};
use crate::nodes::primnodes::Expr;

use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};
use crate::executor::tuptable::{ExecCopySlot, TupIsNull};

use crate::executor::execGrouping::execTuplesMatchPrepare;
use crate::executor::executor::{
    ExecAssignExprContext, ExecEndNode, ExecEvalExprSwitchContext, ExecGetResultSlotOps,
    ExecGetResultType, ExecInitExpr, ExecInitExtraTupleSlot, ExecInitNode, ExecInitResultTypeTL,
    ExecProcNode, ExecQualAndReset, ExecReScan, ExecSetTupleBound, EXEC_FLAG_MARK,
};

use crate::postgres::DatumGetInt64;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::utils::elog::ERROR;

use crate::{castNode, ereport, elog, makeNode, Assert};

/*
 * SQL error classification codes for the OFFSET/LIMIT clauses.  Not yet present
 * in the errcodes table, so define them locally.
 */
const ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE: c_int = 0;
const ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE: c_int = 0;

/* ----------------------------------------------------------------
 *		ExecLimit
 *
 *		This is a very simple node which just performs LIMIT/OFFSET
 *		filtering on the stream of tuples returned by a subplan.
 * ----------------------------------------------------------------
 */
unsafe fn ExecLimit(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut LimitState = castNode!(LimitState, T_LimitState, pstate);
    let econtext: *mut ExprContext = (*node).ps.ps_ExprContext;
    let direction: ScanDirection;
    let slot: *mut TupleTableSlot;
    let outerPlan: *mut PlanState;

    CHECK_FOR_INTERRUPTS();

    /*
     * get information from the node
     */
    direction = (*(*node).ps.state).es_direction;
    outerPlan = outerPlanState(node as *mut PlanState);

    /*
     * The main logic is a simple state machine.
     */
    match (*node).lstate {
        LIMIT_INITIAL => {
            /*
             * First call for this node, so compute limit/offset. (We can't do
             * this any earlier, because parameters from upper nodes will not
             * be set during ExecInitLimit.)  This also sets position = 0 and
             * changes the state to LIMIT_RESCAN.
             */
            recompute_limits(node);

            /* FALL THRU */
            ExecLimit_rescan(node, econtext, direction, outerPlan)
        }

        LIMIT_RESCAN => ExecLimit_rescan(node, econtext, direction, outerPlan),

        LIMIT_EMPTY => {
            /*
             * The subplan is known to return no tuples (or not more than
             * OFFSET tuples, in general).  So we return no tuples.
             */
            null_mut()
        }

        LIMIT_INWINDOW => {
            if ScanDirectionIsForward(direction) {
                /*
                 * Forwards scan, so check for stepping off end of window.  At
                 * the end of the window, the behavior depends on whether WITH
                 * TIES was specified: if so, we need to change the state
                 * machine to WINDOWEND_TIES, and fall through to the code for
                 * that case.  If not (nothing was specified, or ONLY was)
                 * return NULL without advancing the subplan or the position
                 * variable, but change the state machine to record having
                 * done so.
                 */
                if !(*node).noCount
                    && (*node).position - (*node).offset >= (*node).count
                {
                    if (*node).limitOption == LIMIT_OPTION_COUNT {
                        (*node).lstate = LIMIT_WINDOWEND;
                        return null_mut();
                    } else {
                        (*node).lstate = LIMIT_WINDOWEND_TIES;
                        /* we'll fall through to the next case */
                        return ExecLimit_windowend_ties(
                            node, econtext, direction, outerPlan,
                        );
                    }
                } else {
                    /*
                     * Get next tuple from subplan, if any.
                     */
                    slot = ExecProcNode(outerPlan);
                    if TupIsNull(slot) {
                        (*node).lstate = LIMIT_SUBPLANEOF;
                        return null_mut();
                    }

                    /*
                     * If WITH TIES is active, and this is the last in-window
                     * tuple, save it to be used in subsequent WINDOWEND_TIES
                     * processing.
                     */
                    if (*node).limitOption == LIMIT_OPTION_WITH_TIES
                        && (*node).position - (*node).offset == (*node).count - 1
                    {
                        ExecCopySlot((*node).last_slot, slot);
                    }
                    (*node).subSlot = slot;
                    (*node).position += 1;
                    /* break */
                }
            } else {
                /*
                 * Backwards scan, so check for stepping off start of window.
                 * As above, only change state-machine status if so.
                 */
                if (*node).position <= (*node).offset + 1 {
                    (*node).lstate = LIMIT_WINDOWSTART;
                    return null_mut();
                }

                /*
                 * Get previous tuple from subplan; there should be one!
                 */
                slot = ExecProcNode(outerPlan);
                if TupIsNull(slot) {
                    elog!(ERROR, "LIMIT subplan failed to run backwards");
                }
                (*node).subSlot = slot;
                (*node).position -= 1;
                /* break */
            }

            /* Return the current tuple */
            Assert!(!TupIsNull(slot));
            slot
        }

        LIMIT_WINDOWEND_TIES => {
            ExecLimit_windowend_ties(node, econtext, direction, outerPlan)
        }

        LIMIT_SUBPLANEOF => {
            if ScanDirectionIsForward(direction) {
                return null_mut();
            }

            /*
             * Backing up from subplan EOF, so re-fetch previous tuple; there
             * should be one!  Note previous tuple must be in window.
             */
            slot = ExecProcNode(outerPlan);
            if TupIsNull(slot) {
                elog!(ERROR, "LIMIT subplan failed to run backwards");
            }
            (*node).subSlot = slot;
            (*node).lstate = LIMIT_INWINDOW;
            /* position does not change 'cause we didn't advance it before */

            Assert!(!TupIsNull(slot));
            slot
        }

        LIMIT_WINDOWEND => {
            if ScanDirectionIsForward(direction) {
                return null_mut();
            }

            /*
             * We already past one position to detect ties so re-fetch
             * previous tuple; there should be one!  Note previous tuple must
             * be in window.
             */
            if (*node).limitOption == LIMIT_OPTION_WITH_TIES {
                slot = ExecProcNode(outerPlan);
                if TupIsNull(slot) {
                    elog!(ERROR, "LIMIT subplan failed to run backwards");
                }
                (*node).subSlot = slot;
                (*node).lstate = LIMIT_INWINDOW;
            } else {
                /*
                 * Backing up from window end: simply re-return the last tuple
                 * fetched from the subplan.
                 */
                slot = (*node).subSlot;
                (*node).lstate = LIMIT_INWINDOW;
                /* position does not change 'cause we didn't advance it before */
            }

            Assert!(!TupIsNull(slot));
            slot
        }

        LIMIT_WINDOWSTART => {
            if !ScanDirectionIsForward(direction) {
                return null_mut();
            }

            /*
             * Advancing after having backed off window start: simply
             * re-return the last tuple fetched from the subplan.
             */
            slot = (*node).subSlot;
            (*node).lstate = LIMIT_INWINDOW;
            /* position does not change 'cause we didn't change it before */

            Assert!(!TupIsNull(slot));
            slot
        }

        #[allow(unreachable_patterns)]
        _ => {
            elog!(ERROR, "impossible LIMIT state: {}", (*node).lstate as c_int);
            #[allow(unreachable_code)]
            {
                null_mut() /* keep compiler quiet */
            }
        }
    }
}

/*
 * Code for the LIMIT_INITIAL / LIMIT_RESCAN fall-through path.
 */
unsafe fn ExecLimit_rescan(
    node: *mut LimitState,
    _econtext: *mut ExprContext,
    direction: ScanDirection,
    outerPlan: *mut PlanState,
) -> *mut TupleTableSlot {
    let mut slot: *mut TupleTableSlot;

    /*
     * If backwards scan, just return NULL without changing state.
     */
    if !ScanDirectionIsForward(direction) {
        return null_mut();
    }

    /*
     * Check for empty window; if so, treat like empty subplan.
     */
    if (*node).count <= 0 && !(*node).noCount {
        (*node).lstate = LIMIT_EMPTY;
        return null_mut();
    }

    /*
     * Fetch rows from subplan until we reach position > offset.
     */
    loop {
        slot = ExecProcNode(outerPlan);
        if TupIsNull(slot) {
            /*
             * The subplan returns too few tuples for us to produce any output
             * at all.
             */
            (*node).lstate = LIMIT_EMPTY;
            return null_mut();
        }

        /*
         * Tuple at limit is needed for comparison in subsequent execution to
         * detect ties.
         */
        if (*node).limitOption == LIMIT_OPTION_WITH_TIES
            && (*node).position - (*node).offset == (*node).count - 1
        {
            ExecCopySlot((*node).last_slot, slot);
        }
        (*node).subSlot = slot;
        (*node).position += 1;
        if (*node).position > (*node).offset {
            break;
        }
    }

    /*
     * Okay, we have the first tuple of the window.
     */
    (*node).lstate = LIMIT_INWINDOW;

    /* Return the current tuple */
    Assert!(!TupIsNull(slot));
    slot
}

/*
 * Code for the LIMIT_WINDOWEND_TIES case (also reached as a fall-through from
 * LIMIT_INWINDOW when WITH TIES is active).
 */
unsafe fn ExecLimit_windowend_ties(
    node: *mut LimitState,
    econtext: *mut ExprContext,
    direction: ScanDirection,
    outerPlan: *mut PlanState,
) -> *mut TupleTableSlot {
    let slot: *mut TupleTableSlot;

    if ScanDirectionIsForward(direction) {
        /*
         * Advance the subplan until we find the first row with different
         * ORDER BY pathkeys.
         */
        slot = ExecProcNode(outerPlan);
        if TupIsNull(slot) {
            (*node).lstate = LIMIT_SUBPLANEOF;
            return null_mut();
        }

        /*
         * Test if the new tuple and the last tuple match. If so we return the
         * tuple.
         */
        (*econtext).ecxt_innertuple = slot;
        (*econtext).ecxt_outertuple = (*node).last_slot;
        if ExecQualAndReset((*node).eqfunction, econtext) {
            (*node).subSlot = slot;
            (*node).position += 1;
        } else {
            (*node).lstate = LIMIT_WINDOWEND;
            return null_mut();
        }
    } else {
        /*
         * Backwards scan, so check for stepping off start of window.  Change
         * only state-machine status if so.
         */
        if (*node).position <= (*node).offset + 1 {
            (*node).lstate = LIMIT_WINDOWSTART;
            return null_mut();
        }

        /*
         * Get previous tuple from subplan; there should be one! And change
         * state-machine status.
         */
        slot = ExecProcNode(outerPlan);
        if TupIsNull(slot) {
            elog!(ERROR, "LIMIT subplan failed to run backwards");
        }
        (*node).subSlot = slot;
        (*node).position -= 1;
        (*node).lstate = LIMIT_INWINDOW;
    }

    /* Return the current tuple */
    Assert!(!TupIsNull(slot));
    slot
}

/*
 * Evaluate the limit/offset expressions --- done at startup or rescan.
 *
 * This is also a handy place to reset the current-position state info.
 */
unsafe fn recompute_limits(node: *mut LimitState) {
    let econtext: *mut ExprContext = (*node).ps.ps_ExprContext;
    let mut val: Datum;
    let mut isNull: bool = false;

    if !(*node).limitOffset.is_null() {
        val = ExecEvalExprSwitchContext((*node).limitOffset, econtext, &mut isNull);
        /* Interpret NULL offset as no offset */
        if isNull {
            (*node).offset = 0;
        } else {
            (*node).offset = DatumGetInt64(val);
            if (*node).offset < 0 {
                ereport!(ERROR, "OFFSET must not be negative");
            }
        }
    } else {
        /* No OFFSET supplied */
        (*node).offset = 0;
    }

    if !(*node).limitCount.is_null() {
        val = ExecEvalExprSwitchContext((*node).limitCount, econtext, &mut isNull);
        /* Interpret NULL count as no count (LIMIT ALL) */
        if isNull {
            (*node).count = 0;
            (*node).noCount = true;
        } else {
            (*node).count = DatumGetInt64(val);
            if (*node).count < 0 {
                ereport!(ERROR, "LIMIT must not be negative");
            }
            (*node).noCount = false;
        }
    } else {
        /* No COUNT supplied */
        (*node).count = 0;
        (*node).noCount = true;
    }

    /* Reset position to start-of-scan */
    (*node).position = 0;
    (*node).subSlot = null_mut();

    /* Set state-machine state */
    (*node).lstate = LIMIT_RESCAN;

    /*
     * Notify child node about limit.  Note: think not to "optimize" by
     * skipping ExecSetTupleBound if compute_tuples_needed returns < 0.  We
     * must update the child node anyway, in case this is a rescan and the
     * previous time we got a different result.
     */
    ExecSetTupleBound(
        compute_tuples_needed(node),
        outerPlanState(node as *mut PlanState),
    );
}

/*
 * Compute the maximum number of tuples needed to satisfy this Limit node.
 * Return a negative value if there is not a determinable limit.
 */
unsafe fn compute_tuples_needed(node: *mut LimitState) -> int64 {
    if (*node).noCount || (*node).limitOption == LIMIT_OPTION_WITH_TIES {
        return -1;
    }
    /* Note: if this overflows, we'll return a negative value, which is OK */
    (*node).count + (*node).offset
}

/* ----------------------------------------------------------------
 *		ExecInitLimit
 *
 *		This initializes the limit node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitLimit(
    node: *mut Limit,
    estate: *mut EState,
    eflags: c_int,
) -> *mut LimitState {
    let limitstate: *mut LimitState;
    let outerPlan: *mut Plan;

    /* check for unsupported flags */
    Assert!((eflags & EXEC_FLAG_MARK) == 0);

    /*
     * create state structure
     */
    limitstate = makeNode!(LimitState, T_LimitState);
    (*limitstate).ps.plan = node as *mut Plan;
    (*limitstate).ps.state = estate;
    (*limitstate).ps.ExecProcNode = Some(ExecLimit);

    (*limitstate).lstate = LIMIT_INITIAL;

    /*
     * Miscellaneous initialization
     *
     * Limit nodes never call ExecQual or ExecProject, but they need an
     * exprcontext anyway to evaluate the limit/offset parameters in.
     */
    ExecAssignExprContext(estate, &mut (*limitstate).ps);

    /*
     * initialize outer plan
     */
    outerPlan = crate::nodes::plannodes::outerPlan(node as *mut Plan);
    *(outerPlanState_mut(limitstate)) = ExecInitNode(outerPlan, estate, eflags);

    /*
     * initialize child expressions
     */
    (*limitstate).limitOffset =
        ExecInitExpr((*node).limitOffset as *mut Expr, limitstate as *mut PlanState);
    (*limitstate).limitCount =
        ExecInitExpr((*node).limitCount as *mut Expr, limitstate as *mut PlanState);
    (*limitstate).limitOption = (*node).limitOption;

    /*
     * Initialize result type.
     */
    ExecInitResultTypeTL(&mut (*limitstate).ps);

    (*limitstate).ps.resultopsset = true;
    (*limitstate).ps.resultops = ExecGetResultSlotOps(
        outerPlanState(limitstate as *mut PlanState),
        &mut (*limitstate).ps.resultopsfixed,
    );

    /*
     * limit nodes do no projections, so initialize projection info for this
     * node appropriately
     */
    (*limitstate).ps.ps_ProjInfo = null_mut();

    /*
     * Initialize the equality evaluation, to detect ties.
     */
    if (*node).limitOption == LIMIT_OPTION_WITH_TIES {
        let desc: TupleDesc;
        let ops: *const TupleTableSlotOps;

        desc = ExecGetResultType(outerPlanState(limitstate as *mut PlanState));
        ops = ExecGetResultSlotOps(
            outerPlanState(limitstate as *mut PlanState),
            null_mut(),
        );

        (*limitstate).last_slot = ExecInitExtraTupleSlot(estate, desc, ops);
        (*limitstate).eqfunction = execTuplesMatchPrepare(
            desc as *mut _,
            (*node).uniqNumCols,
            (*node).uniqColIdx,
            (*node).uniqOperators,
            (*node).uniqCollations,
            &mut (*limitstate).ps as *mut PlanState as *mut _,
        ) as *mut _;
    }

    limitstate
}

/*
 * Helper to obtain a mutable reference to the outer PlanState slot, mirroring
 * the C lvalue assignment `outerPlanState(limitstate) = ...`.
 */
#[inline]
unsafe fn outerPlanState_mut(node: *mut LimitState) -> *mut *mut PlanState {
    &mut (*node).ps.lefttree
}

/* ----------------------------------------------------------------
 *		ExecEndLimit
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndLimit(node: *mut LimitState) {
    ExecEndNode(outerPlanState(node as *mut PlanState));
}

pub unsafe fn ExecReScanLimit(node: *mut LimitState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    /*
     * Recompute limit/offset in case parameters changed, and reset the state
     * machine.  We must do this before rescanning our child node, in case
     * it's a Sort that we are passing the parameters down to.
     */
    recompute_limits(node);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by first
     * ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}
