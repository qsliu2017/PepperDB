/*-------------------------------------------------------------------------
 *
 * nodeMergejoin.rs
 *	  routines supporting merge joins
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMergejoin.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecMergeJoin			mergejoin outer and inner relations.
 *		ExecInitMergeJoin		creates and initializes run time states
 *		ExecEndMergeJoin		cleans up the node.
 *
 * NOTES
 *
 *		Merge-join is done by joining the inner and outer tuples satisfying
 *		join clauses of the form ((= outerKey innerKey) ...).
 *		The join clause list is provided by the query planner and may contain
 *		more than one (= outerKey innerKey) clause (for composite sort key).
 *
 *		However, the query executor needs to know whether an outer
 *		tuple is "greater/smaller" than an inner tuple so that it can
 *		"synchronize" the two relations. For example, consider the following
 *		relations:
 *
 *				outer: (0 ^1 1 2 5 5 5 6 6 7)	current tuple: 1
 *				inner: (1 ^3 5 5 5 5 6)			current tuple: 3
 *
 *		To continue the merge-join, the executor needs to scan both inner
 *		and outer relations till the matching tuples 5. It needs to know
 *		that currently inner tuple 3 is "greater" than outer tuple 1 and
 *		therefore it should scan the outer relation first to find a
 *		matching tuple and so on.
 *
 *		Therefore, rather than directly executing the merge join clauses,
 *		we evaluate the left and right key expressions separately and then
 *		compare the columns one at a time (see MJCompare).  The planner
 *		passes us enough information about the sort ordering of the inputs
 *		to allow us to determine how to make the comparison.  We may use the
 *		appropriate btree comparison function, since Postgres' only notion
 *		of ordering is specified by btree opfamilies.
 *
 *
 *		Consider the above relations and suppose that the executor has
 *		just joined the first outer "5" with the last inner "5". The
 *		next step is of course to join the second outer "5" with all
 *		the inner "5's". This requires repositioning the inner "cursor"
 *		to point at the first inner "5". This is done by "marking" the
 *		first inner 5 so we can restore the "cursor" to it before joining
 *		with the second outer 5. The access method interface provides
 *		routines to mark and restore to a tuple.
 *
 *
 *		Essential operation of the merge join algorithm is as follows:
 *
 *		Join {
 *			get initial outer and inner tuples				INITIALIZE
 *			do forever {
 *				while (outer != inner) {					SKIP_TEST
 *					if (outer < inner)
 *						advance outer						SKIPOUTER_ADVANCE
 *					else
 *						advance inner						SKIPINNER_ADVANCE
 *				}
 *				mark inner position							SKIP_TEST
 *				do forever {
 *					while (outer == inner) {
 *						join tuples							JOINTUPLES
 *						advance inner position				NEXTINNER
 *					}
 *					advance outer position					NEXTOUTER
 *					if (outer == mark)						TESTOUTER
 *						restore inner position to mark		TESTOUTER
 *					else
 *						break	// return to top of outer loop
 *				}
 *			}
 *		}
 *
 *		The merge join operation is coded in the fashion
 *		of a state machine.  At each state, we do something and then
 *		proceed to another state.  This state is stored in the node's
 *		execution state information and is preserved across calls to
 *		ExecMergeJoin. -cim 10/31/89
 */
// #include "postgres.h"
use crate::prelude::*;

use core::ptr::null_mut;

use crate::nodes::execnodes::{
    innerPlanState, outerPlanState, EState, ExprContext, ExprState, MergeJoinState, PlanState,
};
use crate::nodes::nodes::JoinType::{
    JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_SEMI,
};
use crate::nodes::pg_list::{lfirst, linitial, lsecond, list_length, List, ListCell};
use crate::nodes::plannodes::{innerPlan, outerPlan, MergeJoin, Plan};
use crate::nodes::primnodes::{Const, Expr, OpExpr};

use crate::access::cmptype::COMPARE_EQ;
use crate::access::index::amapi::IndexAmTranslateStrategy;
use crate::access::nbtree::nbtutils::BTORDER_PROC;
use crate::access::nbtree::nbtvalidate::BTSORTSUPPORT_PROC;

use crate::executor::execTuples::TTSOpsVirtual;
use crate::executor::execUtils::{
    ExecAssignExprContext, ExecAssignProjectionInfo, ExecGetResultSlotOps, ExecGetResultType,
    CreateExprContext,
};
use crate::executor::executor::{
    ExecEndNode, ExecEvalExpr, ExecInitExpr, ExecInitExtraTupleSlot, ExecInitNode,
    ExecInitNullTupleSlot, ExecInitQual, ExecInitResultTupleSlotTL, ExecMarkPos, ExecProcNode,
    ExecProject, ExecQual, ExecReScan, ExecRestrPos, ResetExprContext, EXEC_FLAG_BACKWARD,
    EXEC_FLAG_MARK, EXEC_FLAG_REWIND,
};
use crate::executor::instrument::Instrumentation;
use crate::executor::tuptable::{ExecClearTuple, ExecCopySlot, TupIsNull, TupleTableSlot};
use crate::executor::tuptable::TupleTableSlotOps;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::utils::cache::lsyscache::{
    get_op_opfamily_properties, get_opfamily_method, get_opfamily_proc,
};
use crate::utils::sort::sortsupport::{
    ApplySortComparator, PrepareSortSupportComparisonShim, SortSupport, SortSupportData,
};

use crate::access::common::tupdesc::TupleDesc;
use crate::nodes::execnodes::ProjectionInfo;

use crate::{castNode, foreach, current_cell, makeNode, Assert, IsA};

/*
 * States of the ExecMergeJoin state machine
 */
const EXEC_MJ_INITIALIZE_OUTER: c_int = 1;
const EXEC_MJ_INITIALIZE_INNER: c_int = 2;
const EXEC_MJ_JOINTUPLES: c_int = 3;
const EXEC_MJ_NEXTOUTER: c_int = 4;
const EXEC_MJ_TESTOUTER: c_int = 5;
const EXEC_MJ_NEXTINNER: c_int = 6;
const EXEC_MJ_SKIP_TEST: c_int = 7;
const EXEC_MJ_SKIPOUTER_ADVANCE: c_int = 8;
const EXEC_MJ_SKIPINNER_ADVANCE: c_int = 9;
const EXEC_MJ_ENDOUTER: c_int = 10;
const EXEC_MJ_ENDINNER: c_int = 11;

/*
 * Runtime data for each mergejoin clause
 */
#[repr(C)]
pub struct MergeJoinClauseData {
    /* Executable expression trees */
    pub lexpr: *mut ExprState, /* left-hand (outer) input expression */
    pub rexpr: *mut ExprState, /* right-hand (inner) input expression */

    /*
     * If we have a current left or right input tuple, the values of the
     * expressions are loaded into these fields:
     */
    pub ldatum: Datum, /* current left-hand value */
    pub rdatum: Datum, /* current right-hand value */
    pub lisnull: bool, /* and their isnull flags */
    pub risnull: bool,

    /*
     * Everything we need to know to compare the left and right values is
     * stored here.
     */
    pub ssup: SortSupportData,
}

pub type MergeJoinClause = *mut MergeJoinClauseData;

/* Result type for MJEvalOuterValues and MJEvalInnerValues */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum MJEvalResult {
    MJEVAL_MATCHABLE,    /* normal, potentially matchable tuple */
    MJEVAL_NONMATCHABLE, /* tuple cannot join because it has a null */
    MJEVAL_ENDOFJOIN,    /* end of input (physical or effective) */
}
use MJEvalResult::*;

/*
 * #define MarkInnerTuple(innerTupleSlot, mergestate) \
 *	ExecCopySlot((mergestate)->mj_MarkedTupleSlot, (innerTupleSlot))
 */
#[inline]
unsafe fn MarkInnerTuple(
    innerTupleSlot: *mut TupleTableSlot,
    mergestate: *mut MergeJoinState,
) -> *mut TupleTableSlot {
    ExecCopySlot((*mergestate).mj_MarkedTupleSlot, innerTupleSlot)
}

/*
 * Debug tracing macros from executor/execdebug.h.  These compile to nothing
 * unless EXEC_MERGEJOINDEBUG / EXEC_INITDEINIT_DEBUG are defined.
 */
macro_rules! MJ_printf {
    ($($arg:tt)*) => {{}};
}
macro_rules! MJ1_printf {
    ($($arg:tt)*) => {{}};
}
macro_rules! MJ_dump {
    ($($arg:tt)*) => {{}};
}
macro_rules! MJ_DEBUG_QUAL {
    ($($arg:tt)*) => {{}};
}
macro_rules! MJ_DEBUG_COMPARE {
    ($($arg:tt)*) => {{}};
}
macro_rules! MJ_DEBUG_PROC_NODE {
    ($($arg:tt)*) => {{}};
}

/*
 * Instrumentation helpers; see executor/instrument.h.
 */
#[inline]
unsafe fn InstrCountFiltered1(node: *mut MergeJoinState, delta: f64) {
    let instrument: *mut Instrumentation = (*node).js.ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered1 += delta;
    }
}

#[inline]
unsafe fn InstrCountFiltered2(node: *mut MergeJoinState, delta: f64) {
    let instrument: *mut Instrumentation = (*node).js.ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered2 += delta;
    }
}

// ---------------------------------------------------------------------------
// Dependency stubs (defined in other .c files; given stub bodies here).
// ---------------------------------------------------------------------------

/// TODO(pg-port): real def in utils/fmgr/fmgr.c
unsafe fn OidFunctionCall1(_functionId: Oid, _arg1: Datum) -> Datum {
    unimplemented!("TODO(pg-port): OidFunctionCall1")
}

#[inline]
unsafe fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

#[inline]
unsafe fn PointerGetDatum(p: *const c_void) -> Datum {
    p as Datum
}

#[inline]
unsafe fn DatumGetBool(d: Datum) -> bool {
    (d & 0x1) != 0
}

/*
 * MJExamineQuals
 *
 * This deconstructs the list of mergejoinable expressions, which is given
 * to us by the planner in the form of a list of "leftexpr = rightexpr"
 * expression trees in the order matching the sort columns of the inputs.
 * We build an array of MergeJoinClause structs containing the information
 * we will need at runtime.  Each struct essentially tells us how to compare
 * the two expressions from the original clause.
 *
 * In addition to the expressions themselves, the planner passes the btree
 * opfamily OID, collation OID, btree strategy number (BTLessStrategyNumber or
 * BTGreaterStrategyNumber), and nulls-first flag that identify the intended
 * sort ordering for each merge key.  The mergejoinable operator is an
 * equality operator in the opfamily, and the two inputs are guaranteed to be
 * ordered in either increasing or decreasing (respectively) order according
 * to the opfamily and collation, with nulls at the indicated end of the range.
 * This allows us to obtain the needed comparison function from the opfamily.
 */
unsafe fn MJExamineQuals(
    mergeclauses: *mut List,
    mergefamilies: *mut Oid,
    mergecollations: *mut Oid,
    mergereversals: *mut bool,
    mergenullsfirst: *mut bool,
    parent: *mut PlanState,
) -> MergeJoinClause {
    let clauses: MergeJoinClause;
    let nClauses: c_int = list_length(mergeclauses);
    let mut iClause: c_int;
    let cl: *mut ListCell;

    clauses = palloc0(nClauses as usize * core::mem::size_of::<MergeJoinClauseData>())
        as MergeJoinClause;

    iClause = 0;
    foreach!(cl, mergeclauses, {
        let qual: *mut OpExpr = lfirst(current_cell!(cl)) as *mut OpExpr;
        let clause: MergeJoinClause = clauses.add(iClause as usize);
        let opfamily: Oid = *mergefamilies.add(iClause as usize);
        let collation: Oid = *mergecollations.add(iClause as usize);
        let reversed: bool = *mergereversals.add(iClause as usize);
        let nulls_first: bool = *mergenullsfirst.add(iClause as usize);
        let mut op_strategy: c_int = 0;
        let mut op_lefttype: Oid = 0;
        let mut op_righttype: Oid = 0;
        let mut sortfunc: Oid;

        if !IsA!(qual, T_OpExpr) {
            elog!(ERROR, "mergejoin clause is not an OpExpr");
        }

        /*
         * Prepare the input expressions for execution.
         */
        (*clause).lexpr = ExecInitExpr(linitial((*qual).args) as *mut Expr, parent);
        (*clause).rexpr = ExecInitExpr(lsecond((*qual).args) as *mut Expr, parent);

        /* Set up sort support data */
        (*clause).ssup.ssup_cxt = CurrentMemoryContext;
        (*clause).ssup.ssup_collation = collation;
        (*clause).ssup.ssup_reverse = reversed;
        (*clause).ssup.ssup_nulls_first = nulls_first;

        /* Extract the operator's declared left/right datatypes */
        get_op_opfamily_properties(
            (*qual).opno,
            opfamily,
            false,
            &mut op_strategy,
            &mut op_lefttype,
            &mut op_righttype,
        );
        if IndexAmTranslateStrategy(
            op_strategy as _,
            get_opfamily_method(opfamily),
            opfamily,
            true,
        ) != COMPARE_EQ
        {
            /* should not happen */
            elog!(
                ERROR,
                "cannot merge using non-equality operator {}",
                (*qual).opno
            );
        }

        /*
         * sortsupport routine must know if abbreviation optimization is
         * applicable in principle.  It is never applicable for merge joins
         * because there is no convenient opportunity to convert to
         * alternative representation.
         */
        (*clause).ssup.abbreviate = false;

        /* And get the matching support or comparison function */
        Assert!((*clause).ssup.comparator.is_none());
        sortfunc = get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTSORTSUPPORT_PROC as i16);
        if OidIsValid(sortfunc) {
            /* The sort support function can provide a comparator */
            OidFunctionCall1(sortfunc, PointerGetDatum(&(*clause).ssup as *const _ as *const c_void));
        }
        if (*clause).ssup.comparator.is_none() {
            /* support not available, get comparison func */
            sortfunc = get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTORDER_PROC as i16);
            if !OidIsValid(sortfunc) {
                /* should not happen */
                elog!(
                    ERROR,
                    "missing support function {}({},{}) in opfamily {}",
                    BTORDER_PROC,
                    op_lefttype,
                    op_righttype,
                    opfamily
                );
            }
            /* We'll use a shim to call the old-style btree comparator */
            PrepareSortSupportComparisonShim(sortfunc, &mut (*clause).ssup as SortSupport);
        }

        iClause += 1;
    });

    clauses
}

/*
 * MJEvalOuterValues
 *
 * Compute the values of the mergejoined expressions for the current
 * outer tuple.  We also detect whether it's impossible for the current
 * outer tuple to match anything --- this is true if it yields a NULL
 * input, since we assume mergejoin operators are strict.  If the NULL
 * is in the first join column, and that column sorts nulls last, then
 * we can further conclude that no following tuple can match anything
 * either, since they must all have nulls in the first column.  However,
 * that case is only interesting if we're not in FillOuter mode, else
 * we have to visit all the tuples anyway.
 *
 * For the convenience of callers, we also make this routine responsible
 * for testing for end-of-input (null outer tuple), and returning
 * MJEVAL_ENDOFJOIN when that's seen.  This allows the same code to be used
 * for both real end-of-input and the effective end-of-input represented by
 * a first-column NULL.
 *
 * We evaluate the values in OuterEContext, which can be reset each
 * time we move to a new tuple.
 */
unsafe fn MJEvalOuterValues(mergestate: *mut MergeJoinState) -> MJEvalResult {
    let econtext: *mut ExprContext = (*mergestate).mj_OuterEContext;
    let mut result: MJEvalResult = MJEVAL_MATCHABLE;
    let mut i: c_int;
    let oldContext: MemoryContext;

    /* Check for end of outer subplan */
    if TupIsNull((*mergestate).mj_OuterTupleSlot) {
        return MJEVAL_ENDOFJOIN;
    }

    ResetExprContext(econtext);

    oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    (*econtext).ecxt_outertuple = (*mergestate).mj_OuterTupleSlot;

    i = 0;
    while i < (*mergestate).mj_NumClauses {
        let clause: MergeJoinClause =
            ((*mergestate).mj_Clauses as MergeJoinClause).add(i as usize);

        (*clause).ldatum = ExecEvalExpr((*clause).lexpr, econtext, &mut (*clause).lisnull);
        if (*clause).lisnull {
            /* match is impossible; can we end the join early? */
            if i == 0 && !(*clause).ssup.ssup_nulls_first && !(*mergestate).mj_FillOuter {
                result = MJEVAL_ENDOFJOIN;
            } else if result == MJEVAL_MATCHABLE {
                result = MJEVAL_NONMATCHABLE;
            }
        }

        i += 1;
    }

    MemoryContextSwitchTo(oldContext);

    result
}

/*
 * MJEvalInnerValues
 *
 * Same as above, but for the inner tuple.  Here, we have to be prepared
 * to load data from either the true current inner, or the marked inner,
 * so caller must tell us which slot to load from.
 */
unsafe fn MJEvalInnerValues(
    mergestate: *mut MergeJoinState,
    innerslot: *mut TupleTableSlot,
) -> MJEvalResult {
    let econtext: *mut ExprContext = (*mergestate).mj_InnerEContext;
    let mut result: MJEvalResult = MJEVAL_MATCHABLE;
    let mut i: c_int;
    let oldContext: MemoryContext;

    /* Check for end of inner subplan */
    if TupIsNull(innerslot) {
        return MJEVAL_ENDOFJOIN;
    }

    ResetExprContext(econtext);

    oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    (*econtext).ecxt_innertuple = innerslot;

    i = 0;
    while i < (*mergestate).mj_NumClauses {
        let clause: MergeJoinClause =
            ((*mergestate).mj_Clauses as MergeJoinClause).add(i as usize);

        (*clause).rdatum = ExecEvalExpr((*clause).rexpr, econtext, &mut (*clause).risnull);
        if (*clause).risnull {
            /* match is impossible; can we end the join early? */
            if i == 0 && !(*clause).ssup.ssup_nulls_first && !(*mergestate).mj_FillInner {
                result = MJEVAL_ENDOFJOIN;
            } else if result == MJEVAL_MATCHABLE {
                result = MJEVAL_NONMATCHABLE;
            }
        }

        i += 1;
    }

    MemoryContextSwitchTo(oldContext);

    result
}

/*
 * MJCompare
 *
 * Compare the mergejoinable values of the current two input tuples
 * and return 0 if they are equal (ie, the mergejoin equalities all
 * succeed), >0 if outer > inner, <0 if outer < inner.
 *
 * MJEvalOuterValues and MJEvalInnerValues must already have been called
 * for the current outer and inner tuples, respectively.
 */
unsafe fn MJCompare(mergestate: *mut MergeJoinState) -> c_int {
    let mut result: c_int = 0;
    let mut nulleqnull: bool = false;
    let econtext: *mut ExprContext = (*mergestate).js.ps.ps_ExprContext;
    let mut i: c_int;
    let oldContext: MemoryContext;

    /*
     * Call the comparison functions in short-lived context, in case they leak
     * memory.
     */
    ResetExprContext(econtext);

    oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

    i = 0;
    while i < (*mergestate).mj_NumClauses {
        let clause: MergeJoinClause =
            ((*mergestate).mj_Clauses as MergeJoinClause).add(i as usize);

        /*
         * Special case for NULL-vs-NULL, else use standard comparison.
         */
        if (*clause).lisnull && (*clause).risnull {
            nulleqnull = true; /* NULL "=" NULL */
            i += 1;
            continue;
        }

        result = ApplySortComparator(
            (*clause).ldatum,
            (*clause).lisnull,
            (*clause).rdatum,
            (*clause).risnull,
            &mut (*clause).ssup as SortSupport,
        );

        if result != 0 {
            break;
        }

        i += 1;
    }

    /*
     * If we had any NULL-vs-NULL inputs, we do not want to report that the
     * tuples are equal.  Instead, if result is still 0, change it to +1. This
     * will result in advancing the inner side of the join.
     *
     * Likewise, if there was a constant-false joinqual, do not report
     * equality.  We have to check this as part of the mergequals, else the
     * rescan logic will do the wrong thing.
     */
    if result == 0 && (nulleqnull || (*mergestate).mj_ConstFalseJoin) {
        result = 1;
    }

    MemoryContextSwitchTo(oldContext);

    result
}

/*
 * Generate a fake join tuple with nulls for the inner tuple,
 * and return it if it passes the non-join quals.
 */
unsafe fn MJFillOuter(node: *mut MergeJoinState) -> *mut TupleTableSlot {
    let econtext: *mut ExprContext = (*node).js.ps.ps_ExprContext;
    let otherqual: *mut ExprState = (*node).js.ps.qual;

    ResetExprContext(econtext);

    (*econtext).ecxt_outertuple = (*node).mj_OuterTupleSlot;
    (*econtext).ecxt_innertuple = (*node).mj_NullInnerTupleSlot;

    if ExecQual(otherqual, econtext) {
        /*
         * qualification succeeded.  now form the desired projection tuple and
         * return the slot containing it.
         */
        MJ_printf!("ExecMergeJoin: returning outer fill tuple\n");

        return ExecProject((*node).js.ps.ps_ProjInfo);
    } else {
        InstrCountFiltered2(node, 1.0);
    }

    null_mut()
}

/*
 * Generate a fake join tuple with nulls for the outer tuple,
 * and return it if it passes the non-join quals.
 */
unsafe fn MJFillInner(node: *mut MergeJoinState) -> *mut TupleTableSlot {
    let econtext: *mut ExprContext = (*node).js.ps.ps_ExprContext;
    let otherqual: *mut ExprState = (*node).js.ps.qual;

    ResetExprContext(econtext);

    (*econtext).ecxt_outertuple = (*node).mj_NullOuterTupleSlot;
    (*econtext).ecxt_innertuple = (*node).mj_InnerTupleSlot;

    if ExecQual(otherqual, econtext) {
        /*
         * qualification succeeded.  now form the desired projection tuple and
         * return the slot containing it.
         */
        MJ_printf!("ExecMergeJoin: returning inner fill tuple\n");

        return ExecProject((*node).js.ps.ps_ProjInfo);
    } else {
        InstrCountFiltered2(node, 1.0);
    }

    null_mut()
}

/*
 * Check that a qual condition is constant true or constant false.
 * If it is constant false (or null), set *is_const_false to true.
 *
 * Constant true would normally be represented by a NIL list, but we allow an
 * actual bool Const as well.  We do expect that the planner will have thrown
 * away any non-constant terms that have been ANDed with a constant false.
 */
unsafe fn check_constant_qual(qual: *mut List, is_const_false: *mut bool) -> bool {
    let lc: *mut ListCell;

    foreach!(lc, qual, {
        let con: *mut Const = lfirst(current_cell!(lc)) as *mut Const;

        if con.is_null() || !IsA!(con, T_Const) {
            return false;
        }
        if (*con).constisnull || !DatumGetBool((*con).constvalue) {
            *is_const_false = true;
        }
    });
    true
}

/* ----------------------------------------------------------------
 *		ExecMergeTupleDump
 *
 *		This function is called through the MJ_dump() macro
 *		when EXEC_MERGEJOINDEBUG is defined
 * ----------------------------------------------------------------
 */
// #ifdef EXEC_MERGEJOINDEBUG
// The dump routines below are compiled only when EXEC_MERGEJOINDEBUG is defined.
// They are reached solely through the MJ_dump!() macro, which expands to nothing
// in this port, so they are gated off via cfg(EXEC_MERGEJOINDEBUG) and never built.
#[cfg(EXEC_MERGEJOINDEBUG)]
unsafe fn ExecMergeTupleDumpOuter(mergestate: *mut MergeJoinState) {
    let outerSlot: *mut TupleTableSlot = (*mergestate).mj_OuterTupleSlot;

    print!("==== outer tuple ====\n");
    if TupIsNull(outerSlot) {
        print!("(nil)\n");
    } else {
        MJ_debugtup(outerSlot);
    }
}

#[cfg(EXEC_MERGEJOINDEBUG)]
unsafe fn ExecMergeTupleDumpInner(mergestate: *mut MergeJoinState) {
    let innerSlot: *mut TupleTableSlot = (*mergestate).mj_InnerTupleSlot;

    print!("==== inner tuple ====\n");
    if TupIsNull(innerSlot) {
        print!("(nil)\n");
    } else {
        MJ_debugtup(innerSlot);
    }
}

#[cfg(EXEC_MERGEJOINDEBUG)]
unsafe fn ExecMergeTupleDumpMarked(mergestate: *mut MergeJoinState) {
    let markedSlot: *mut TupleTableSlot = (*mergestate).mj_MarkedTupleSlot;

    print!("==== marked tuple ====\n");
    if TupIsNull(markedSlot) {
        print!("(nil)\n");
    } else {
        MJ_debugtup(markedSlot);
    }
}

#[cfg(EXEC_MERGEJOINDEBUG)]
unsafe fn ExecMergeTupleDump(mergestate: *mut MergeJoinState) {
    print!("******** ExecMergeTupleDump ********\n");

    ExecMergeTupleDumpOuter(mergestate);
    ExecMergeTupleDumpInner(mergestate);
    ExecMergeTupleDumpMarked(mergestate);

    print!("********\n");
}
// #endif /* EXEC_MERGEJOINDEBUG */

/* ----------------------------------------------------------------
 *		ExecMergeJoin
 * ----------------------------------------------------------------
 */
unsafe fn ExecMergeJoin(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut MergeJoinState = castNode!(MergeJoinState, T_MergeJoinState, pstate);
    let joinqual: *mut ExprState;
    let otherqual: *mut ExprState;
    let mut qualResult: bool;
    let mut compareResult: c_int;
    let innerPlan: *mut PlanState;
    let mut innerTupleSlot: *mut TupleTableSlot;
    let outerPlan: *mut PlanState;
    let mut outerTupleSlot: *mut TupleTableSlot;
    let econtext: *mut ExprContext;
    let doFillOuter: bool;
    let doFillInner: bool;

    CHECK_FOR_INTERRUPTS();

    /*
     * get information from node
     */
    innerPlan = innerPlanState(node as *mut PlanState);
    outerPlan = outerPlanState(node as *mut PlanState);
    econtext = (*node).js.ps.ps_ExprContext;
    joinqual = (*node).js.joinqual;
    otherqual = (*node).js.ps.qual;
    doFillOuter = (*node).mj_FillOuter;
    doFillInner = (*node).mj_FillInner;

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * ok, everything is setup.. let's go to work
     */
    loop {
        MJ_dump!(node);

        /*
         * get the current state of the join and do things accordingly.
         */
        match (*node).mj_JoinState {
            /*
             * EXEC_MJ_INITIALIZE_OUTER means that this is the first time
             * ExecMergeJoin() has been called and so we have to fetch the
             * first matchable tuple for both outer and inner subplans. We
             * do the outer side in INITIALIZE_OUTER state, then advance
             * to INITIALIZE_INNER state for the inner subplan.
             */
            EXEC_MJ_INITIALIZE_OUTER => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_INITIALIZE_OUTER\n");

                outerTupleSlot = ExecProcNode(outerPlan);
                (*node).mj_OuterTupleSlot = outerTupleSlot;

                /* Compute join values and check for unmatchability */
                match MJEvalOuterValues(node) {
                    MJEVAL_MATCHABLE => {
                        /* OK to go get the first inner tuple */
                        (*node).mj_JoinState = EXEC_MJ_INITIALIZE_INNER;
                    }
                    MJEVAL_NONMATCHABLE => {
                        /* Stay in same state to fetch next outer tuple */
                        if doFillOuter {
                            /*
                             * Generate a fake join tuple with nulls for the
                             * inner tuple, and return it if it passes the
                             * non-join quals.
                             */
                            let result: *mut TupleTableSlot;

                            result = MJFillOuter(node);
                            if !result.is_null() {
                                return result;
                            }
                        }
                    }
                    MJEVAL_ENDOFJOIN => {
                        /* No more outer tuples */
                        MJ_printf!("ExecMergeJoin: nothing in outer subplan\n");
                        if doFillInner {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples. We set MatchedInner = true to
                             * force the ENDOUTER state to advance inner.
                             */
                            (*node).mj_JoinState = EXEC_MJ_ENDOUTER;
                            (*node).mj_MatchedInner = true;
                        } else {
                            /* Otherwise we're done. */
                            return null_mut();
                        }
                    }
                }
            }

            EXEC_MJ_INITIALIZE_INNER => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_INITIALIZE_INNER\n");

                innerTupleSlot = ExecProcNode(innerPlan);
                (*node).mj_InnerTupleSlot = innerTupleSlot;

                /* Compute join values and check for unmatchability */
                match MJEvalInnerValues(node, innerTupleSlot) {
                    MJEVAL_MATCHABLE => {
                        /*
                         * OK, we have the initial tuples.  Begin by skipping
                         * non-matching tuples.
                         */
                        (*node).mj_JoinState = EXEC_MJ_SKIP_TEST;
                    }
                    MJEVAL_NONMATCHABLE => {
                        /* Mark before advancing, if wanted */
                        if (*node).mj_ExtraMarks {
                            ExecMarkPos(innerPlan);
                        }
                        /* Stay in same state to fetch next inner tuple */
                        if doFillInner {
                            /*
                             * Generate a fake join tuple with nulls for the
                             * outer tuple, and return it if it passes the
                             * non-join quals.
                             */
                            let result: *mut TupleTableSlot;

                            result = MJFillInner(node);
                            if !result.is_null() {
                                return result;
                            }
                        }
                    }
                    MJEVAL_ENDOFJOIN => {
                        /* No more inner tuples */
                        MJ_printf!("ExecMergeJoin: nothing in inner subplan\n");
                        if doFillOuter {
                            /*
                             * Need to emit left-join tuples for all outer
                             * tuples, including the one we just fetched.  We
                             * set MatchedOuter = false to force the ENDINNER
                             * state to emit first tuple before advancing
                             * outer.
                             */
                            (*node).mj_JoinState = EXEC_MJ_ENDINNER;
                            (*node).mj_MatchedOuter = false;
                        } else {
                            /* Otherwise we're done. */
                            return null_mut();
                        }
                    }
                }
            }

            /*
             * EXEC_MJ_JOINTUPLES means we have two tuples which satisfied
             * the merge clause so we join them and then proceed to get
             * the next inner tuple (EXEC_MJ_NEXTINNER).
             */
            EXEC_MJ_JOINTUPLES => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_JOINTUPLES\n");

                /*
                 * Set the next state machine state.  The right things will
                 * happen whether we return this join tuple or just fall
                 * through to continue the state machine execution.
                 */
                (*node).mj_JoinState = EXEC_MJ_NEXTINNER;

                /*
                 * Check the extra qual conditions to see if we actually want
                 * to return this join tuple.  If not, can proceed with merge.
                 * We must distinguish the additional joinquals (which must
                 * pass to consider the tuples "matched" for outer-join logic)
                 * from the otherquals (which must pass before we actually
                 * return the tuple).
                 *
                 * We don't bother with a ResetExprContext here, on the
                 * assumption that we just did one while checking the merge
                 * qual.  One per tuple should be sufficient.  We do have to
                 * set up the econtext links to the tuples for ExecQual to
                 * use.
                 */
                outerTupleSlot = (*node).mj_OuterTupleSlot;
                (*econtext).ecxt_outertuple = outerTupleSlot;
                innerTupleSlot = (*node).mj_InnerTupleSlot;
                (*econtext).ecxt_innertuple = innerTupleSlot;

                qualResult = joinqual.is_null() || ExecQual(joinqual, econtext);
                MJ_DEBUG_QUAL!(joinqual, qualResult);

                if qualResult {
                    (*node).mj_MatchedOuter = true;
                    (*node).mj_MatchedInner = true;

                    /* In an antijoin, we never return a matched tuple */
                    if (*node).js.jointype == JOIN_ANTI {
                        (*node).mj_JoinState = EXEC_MJ_NEXTOUTER;
                        // break out of this case
                    } else {
                        /*
                         * If we only need to consider the first matching inner
                         * tuple, then advance to next outer tuple after we've
                         * processed this one.
                         */
                        if (*node).js.single_match {
                            (*node).mj_JoinState = EXEC_MJ_NEXTOUTER;
                        }

                        /*
                         * In a right-antijoin, we never return a matched tuple.
                         * If it's not an inner_unique join, we need to stay on
                         * the current outer tuple to continue scanning the inner
                         * side for matches.
                         */
                        if (*node).js.jointype == JOIN_RIGHT_ANTI {
                            // break (fall through to bottom of loop)
                        } else {
                            qualResult = otherqual.is_null() || ExecQual(otherqual, econtext);
                            MJ_DEBUG_QUAL!(otherqual, qualResult);

                            if qualResult {
                                /*
                                 * qualification succeeded.  now form the desired
                                 * projection tuple and return the slot containing it.
                                 */
                                MJ_printf!("ExecMergeJoin: returning tuple\n");

                                return ExecProject((*node).js.ps.ps_ProjInfo);
                            } else {
                                InstrCountFiltered2(node, 1.0);
                            }
                        }
                    }
                } else {
                    InstrCountFiltered1(node, 1.0);
                }
            }

            /*
             * EXEC_MJ_NEXTINNER means advance the inner scan to the next
             * tuple. If the tuple is not nil, we then proceed to test it
             * against the join qualification.
             *
             * Before advancing, we check to see if we must emit an
             * outer-join fill tuple for this inner tuple.
             */
            EXEC_MJ_NEXTINNER => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_NEXTINNER\n");

                if doFillInner && !(*node).mj_MatchedInner {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    let result: *mut TupleTableSlot;

                    (*node).mj_MatchedInner = true; /* do it only once */

                    result = MJFillInner(node);
                    if !result.is_null() {
                        return result;
                    }
                }

                /*
                 * now we get the next inner tuple, if any.  If there's none,
                 * advance to next outer tuple (which may be able to join to
                 * previously marked tuples).
                 *
                 * NB: must NOT do "extraMarks" here, since we may need to
                 * return to previously marked tuples.
                 */
                innerTupleSlot = ExecProcNode(innerPlan);
                (*node).mj_InnerTupleSlot = innerTupleSlot;
                MJ_DEBUG_PROC_NODE!(innerTupleSlot);
                (*node).mj_MatchedInner = false;

                /* Compute join values and check for unmatchability */
                match MJEvalInnerValues(node, innerTupleSlot) {
                    MJEVAL_MATCHABLE => {
                        /*
                         * Test the new inner tuple to see if it matches
                         * outer.
                         *
                         * If they do match, then we join them and move on to
                         * the next inner tuple (EXEC_MJ_JOINTUPLES).
                         *
                         * If they do not match then advance to next outer
                         * tuple.
                         */
                        compareResult = MJCompare(node);
                        MJ_DEBUG_COMPARE!(compareResult);

                        if compareResult == 0 {
                            (*node).mj_JoinState = EXEC_MJ_JOINTUPLES;
                        } else if compareResult < 0 {
                            (*node).mj_JoinState = EXEC_MJ_NEXTOUTER;
                        } else {
                            /* compareResult > 0 should not happen */
                            elog!(ERROR, "mergejoin input data is out of order");
                        }
                    }
                    MJEVAL_NONMATCHABLE => {
                        /*
                         * It contains a NULL and hence can't match any outer
                         * tuple, so we can skip the comparison and assume the
                         * new tuple is greater than current outer.
                         */
                        (*node).mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }
                    MJEVAL_ENDOFJOIN => {
                        /*
                         * No more inner tuples.  However, this might be only
                         * effective and not physical end of inner plan, so
                         * force mj_InnerTupleSlot to null to make sure we
                         * don't fetch more inner tuples.  (We need this hack
                         * because we are not transiting to a state where the
                         * inner plan is assumed to be exhausted.)
                         */
                        (*node).mj_InnerTupleSlot = null_mut();
                        (*node).mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }
                }
            }

            /*-------------------------------------------
             * EXEC_MJ_NEXTOUTER means
             *
             *				outer inner
             * outer tuple -  5		5  - marked tuple
             *				  5		5
             *				  6		6  - inner tuple
             *				  7		7
             *
             * we know we just bumped into the
             * first inner tuple > current outer tuple (or possibly
             * the end of the inner stream)
             * so get a new outer tuple and then
             * proceed to test it against the marked tuple
             * (EXEC_MJ_TESTOUTER)
             *
             * Before advancing, we check to see if we must emit an
             * outer-join fill tuple for this outer tuple.
             *------------------------------------------------
             */
            EXEC_MJ_NEXTOUTER => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_NEXTOUTER\n");

                if doFillOuter && !(*node).mj_MatchedOuter {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    let result: *mut TupleTableSlot;

                    (*node).mj_MatchedOuter = true; /* do it only once */

                    result = MJFillOuter(node);
                    if !result.is_null() {
                        return result;
                    }
                }

                /*
                 * now we get the next outer tuple, if any
                 */
                outerTupleSlot = ExecProcNode(outerPlan);
                (*node).mj_OuterTupleSlot = outerTupleSlot;
                MJ_DEBUG_PROC_NODE!(outerTupleSlot);
                (*node).mj_MatchedOuter = false;

                /* Compute join values and check for unmatchability */
                match MJEvalOuterValues(node) {
                    MJEVAL_MATCHABLE => {
                        /* Go test the new tuple against the marked tuple */
                        (*node).mj_JoinState = EXEC_MJ_TESTOUTER;
                    }
                    MJEVAL_NONMATCHABLE => {
                        /* Can't match, so fetch next outer tuple */
                        (*node).mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }
                    MJEVAL_ENDOFJOIN => {
                        /* No more outer tuples */
                        MJ_printf!("ExecMergeJoin: end of outer subplan\n");
                        innerTupleSlot = (*node).mj_InnerTupleSlot;
                        if doFillInner && !TupIsNull(innerTupleSlot) {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples.
                             */
                            (*node).mj_JoinState = EXEC_MJ_ENDOUTER;
                        } else {
                            /* Otherwise we're done. */
                            return null_mut();
                        }
                    }
                }
            }

            /*--------------------------------------------------------
             * EXEC_MJ_TESTOUTER If the new outer tuple and the marked
             * tuple satisfy the merge clause then we know we have
             * duplicates in the outer scan so we have to restore the
             * inner scan to the marked tuple and proceed to join the
             * new outer tuple with the inner tuples.
             *
             * This is the case when
             *						  outer inner
             *							4	  5  - marked tuple
             *			 outer tuple -	5	  5
             *		 new outer tuple -	5	  5
             *							6	  8  - inner tuple
             *							7	 12
             *
             *				new outer tuple == marked tuple
             *
             * If the outer tuple fails the test, then we are done
             * with the marked tuples, and we have to look for a
             * match to the current inner tuple.  So we will
             * proceed to skip outer tuples until outer >= inner
             * (EXEC_MJ_SKIP_TEST).
             *
             *		This is the case when
             *
             *						  outer inner
             *							5	  5  - marked tuple
             *			 outer tuple -	5	  5
             *		 new outer tuple -	6	  8  - inner tuple
             *							7	 12
             *
             *				new outer tuple > marked tuple
             *
             *---------------------------------------------------------
             */
            EXEC_MJ_TESTOUTER => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_TESTOUTER\n");

                /*
                 * Here we must compare the outer tuple with the marked inner
                 * tuple.  (We can ignore the result of MJEvalInnerValues,
                 * since the marked inner tuple is certainly matchable.)
                 */
                innerTupleSlot = (*node).mj_MarkedTupleSlot;
                let _ = MJEvalInnerValues(node, innerTupleSlot);

                compareResult = MJCompare(node);
                MJ_DEBUG_COMPARE!(compareResult);

                if compareResult == 0 {
                    /*
                     * the merge clause matched so now we restore the inner
                     * scan position to the first mark, and go join that tuple
                     * (and any following ones) to the new outer.
                     *
                     * If we were able to determine mark and restore are not
                     * needed, then we don't have to back up; the current
                     * inner is already the first possible match.
                     *
                     * NOTE: we do not need to worry about the MatchedInner
                     * state for the rescanned inner tuples.  We know all of
                     * them will match this new outer tuple and therefore
                     * won't be emitted as fill tuples.  This works *only*
                     * because we require the extra joinquals to be constant
                     * when doing a right, right-anti or full join ---
                     * otherwise some of the rescanned tuples might fail the
                     * extra joinquals.  This obviously won't happen for a
                     * constant-true extra joinqual, while the constant-false
                     * case is handled by forcing the merge clause to never
                     * match, so we never get here.
                     */
                    if !(*node).mj_SkipMarkRestore {
                        ExecRestrPos(innerPlan);

                        /*
                         * ExecRestrPos probably should give us back a new
                         * Slot, but since it doesn't, use the marked slot.
                         * (The previously returned mj_InnerTupleSlot cannot
                         * be assumed to hold the required tuple.)
                         */
                        (*node).mj_InnerTupleSlot = innerTupleSlot;
                        /* we need not do MJEvalInnerValues again */
                    }

                    (*node).mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if compareResult > 0 {
                    /* ----------------
                     *	if the new outer tuple didn't match the marked inner
                     *	tuple then we have a case like:
                     *
                     *			 outer inner
                     *			   4	 4	- marked tuple
                     * new outer - 5	 4
                     *			   6	 5	- inner tuple
                     *			   7
                     *
                     *	which means that all subsequent outer tuples will be
                     *	larger than our marked inner tuples.  So we need not
                     *	revisit any of the marked tuples but can proceed to
                     *	look for a match to the current inner.  If there's
                     *	no more inners, no more matches are possible.
                     * ----------------
                     */
                    innerTupleSlot = (*node).mj_InnerTupleSlot;

                    /* reload comparison data for current inner */
                    match MJEvalInnerValues(node, innerTupleSlot) {
                        MJEVAL_MATCHABLE => {
                            /* proceed to compare it to the current outer */
                            (*node).mj_JoinState = EXEC_MJ_SKIP_TEST;
                        }
                        MJEVAL_NONMATCHABLE => {
                            /*
                             * current inner can't possibly match any outer;
                             * better to advance the inner scan than the
                             * outer.
                             */
                            (*node).mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                        }
                        MJEVAL_ENDOFJOIN => {
                            /* No more inner tuples */
                            if doFillOuter {
                                /*
                                 * Need to emit left-join tuples for remaining
                                 * outer tuples.
                                 */
                                (*node).mj_JoinState = EXEC_MJ_ENDINNER;
                            } else {
                                /* Otherwise we're done. */
                                return null_mut();
                            }
                        }
                    }
                } else {
                    /* compareResult < 0 should not happen */
                    elog!(ERROR, "mergejoin input data is out of order");
                }
            }

            /*----------------------------------------------------------
             * EXEC_MJ_SKIP_TEST means compare tuples and if they do not
             * match, skip whichever is lesser.
             *
             * For example:
             *
             *				outer inner
             *				  5		5
             *				  5		5
             * outer tuple -  6		8  - inner tuple
             *				  7    12
             *				  8    14
             *
             * we have to advance the outer scan
             * until we find the outer 8.
             *
             * On the other hand:
             *
             *				outer inner
             *				  5		5
             *				  5		5
             * outer tuple - 12		8  - inner tuple
             *				 14    10
             *				 17    12
             *
             * we have to advance the inner scan
             * until we find the inner 12.
             *----------------------------------------------------------
             */
            EXEC_MJ_SKIP_TEST => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_SKIP_TEST\n");

                /*
                 * before we advance, make sure the current tuples do not
                 * satisfy the mergeclauses.  If they do, then we update the
                 * marked tuple position and go join them.
                 */
                compareResult = MJCompare(node);
                MJ_DEBUG_COMPARE!(compareResult);

                if compareResult == 0 {
                    if !(*node).mj_SkipMarkRestore {
                        ExecMarkPos(innerPlan);
                    }

                    MarkInnerTuple((*node).mj_InnerTupleSlot, node);

                    (*node).mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if compareResult < 0 {
                    (*node).mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                } else {
                    /* compareResult > 0 */
                    (*node).mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                }
            }

            /*
             * EXEC_MJ_SKIPOUTER_ADVANCE: advance over an outer tuple that
             * is known not to join to any inner tuple.
             *
             * Before advancing, we check to see if we must emit an
             * outer-join fill tuple for this outer tuple.
             */
            EXEC_MJ_SKIPOUTER_ADVANCE => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_SKIPOUTER_ADVANCE\n");

                if doFillOuter && !(*node).mj_MatchedOuter {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    let result: *mut TupleTableSlot;

                    (*node).mj_MatchedOuter = true; /* do it only once */

                    result = MJFillOuter(node);
                    if !result.is_null() {
                        return result;
                    }
                }

                /*
                 * now we get the next outer tuple, if any
                 */
                outerTupleSlot = ExecProcNode(outerPlan);
                (*node).mj_OuterTupleSlot = outerTupleSlot;
                MJ_DEBUG_PROC_NODE!(outerTupleSlot);
                (*node).mj_MatchedOuter = false;

                /* Compute join values and check for unmatchability */
                match MJEvalOuterValues(node) {
                    MJEVAL_MATCHABLE => {
                        /* Go test the new tuple against the current inner */
                        (*node).mj_JoinState = EXEC_MJ_SKIP_TEST;
                    }
                    MJEVAL_NONMATCHABLE => {
                        /* Can't match, so fetch next outer tuple */
                        (*node).mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                    }
                    MJEVAL_ENDOFJOIN => {
                        /* No more outer tuples */
                        MJ_printf!("ExecMergeJoin: end of outer subplan\n");
                        innerTupleSlot = (*node).mj_InnerTupleSlot;
                        if doFillInner && !TupIsNull(innerTupleSlot) {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples.
                             */
                            (*node).mj_JoinState = EXEC_MJ_ENDOUTER;
                        } else {
                            /* Otherwise we're done. */
                            return null_mut();
                        }
                    }
                }
            }

            /*
             * EXEC_MJ_SKIPINNER_ADVANCE: advance over an inner tuple that
             * is known not to join to any outer tuple.
             *
             * Before advancing, we check to see if we must emit an
             * outer-join fill tuple for this inner tuple.
             */
            EXEC_MJ_SKIPINNER_ADVANCE => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_SKIPINNER_ADVANCE\n");

                if doFillInner && !(*node).mj_MatchedInner {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    let result: *mut TupleTableSlot;

                    (*node).mj_MatchedInner = true; /* do it only once */

                    result = MJFillInner(node);
                    if !result.is_null() {
                        return result;
                    }
                }

                /* Mark before advancing, if wanted */
                if (*node).mj_ExtraMarks {
                    ExecMarkPos(innerPlan);
                }

                /*
                 * now we get the next inner tuple, if any
                 */
                innerTupleSlot = ExecProcNode(innerPlan);
                (*node).mj_InnerTupleSlot = innerTupleSlot;
                MJ_DEBUG_PROC_NODE!(innerTupleSlot);
                (*node).mj_MatchedInner = false;

                /* Compute join values and check for unmatchability */
                match MJEvalInnerValues(node, innerTupleSlot) {
                    MJEVAL_MATCHABLE => {
                        /* proceed to compare it to the current outer */
                        (*node).mj_JoinState = EXEC_MJ_SKIP_TEST;
                    }
                    MJEVAL_NONMATCHABLE => {
                        /*
                         * current inner can't possibly match any outer;
                         * better to advance the inner scan than the outer.
                         */
                        (*node).mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                    }
                    MJEVAL_ENDOFJOIN => {
                        /* No more inner tuples */
                        MJ_printf!("ExecMergeJoin: end of inner subplan\n");
                        outerTupleSlot = (*node).mj_OuterTupleSlot;
                        if doFillOuter && !TupIsNull(outerTupleSlot) {
                            /*
                             * Need to emit left-join tuples for remaining
                             * outer tuples.
                             */
                            (*node).mj_JoinState = EXEC_MJ_ENDINNER;
                        } else {
                            /* Otherwise we're done. */
                            return null_mut();
                        }
                    }
                }
            }

            /*
             * EXEC_MJ_ENDOUTER means we have run out of outer tuples, but
             * are doing a right/right-anti/full join and therefore must
             * null-fill any remaining unmatched inner tuples.
             */
            EXEC_MJ_ENDOUTER => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_ENDOUTER\n");

                Assert!(doFillInner);

                if !(*node).mj_MatchedInner {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    let result: *mut TupleTableSlot;

                    (*node).mj_MatchedInner = true; /* do it only once */

                    result = MJFillInner(node);
                    if !result.is_null() {
                        return result;
                    }
                }

                /* Mark before advancing, if wanted */
                if (*node).mj_ExtraMarks {
                    ExecMarkPos(innerPlan);
                }

                /*
                 * now we get the next inner tuple, if any
                 */
                innerTupleSlot = ExecProcNode(innerPlan);
                (*node).mj_InnerTupleSlot = innerTupleSlot;
                MJ_DEBUG_PROC_NODE!(innerTupleSlot);
                (*node).mj_MatchedInner = false;

                if TupIsNull(innerTupleSlot) {
                    MJ_printf!("ExecMergeJoin: end of inner subplan\n");
                    return null_mut();
                }

                /* Else remain in ENDOUTER state and process next tuple. */
            }

            /*
             * EXEC_MJ_ENDINNER means we have run out of inner tuples, but
             * are doing a left/full join and therefore must null- fill
             * any remaining unmatched outer tuples.
             */
            EXEC_MJ_ENDINNER => {
                MJ_printf!("ExecMergeJoin: EXEC_MJ_ENDINNER\n");

                Assert!(doFillOuter);

                if !(*node).mj_MatchedOuter {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    let result: *mut TupleTableSlot;

                    (*node).mj_MatchedOuter = true; /* do it only once */

                    result = MJFillOuter(node);
                    if !result.is_null() {
                        return result;
                    }
                }

                /*
                 * now we get the next outer tuple, if any
                 */
                outerTupleSlot = ExecProcNode(outerPlan);
                (*node).mj_OuterTupleSlot = outerTupleSlot;
                MJ_DEBUG_PROC_NODE!(outerTupleSlot);
                (*node).mj_MatchedOuter = false;

                if TupIsNull(outerTupleSlot) {
                    MJ_printf!("ExecMergeJoin: end of outer subplan\n");
                    return null_mut();
                }

                /* Else remain in ENDINNER state and process next tuple. */
            }

            /*
             * broken state value?
             */
            _ => {
                elog!(
                    ERROR,
                    "unrecognized mergejoin state: {}",
                    (*node).mj_JoinState
                );
            }
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecInitMergeJoin
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitMergeJoin(
    node: *mut MergeJoin,
    estate: *mut EState,
    eflags: c_int,
) -> *mut MergeJoinState {
    let mergestate: *mut MergeJoinState;
    let outerDesc: TupleDesc;
    let innerDesc: TupleDesc;
    let innerOps: *const TupleTableSlotOps;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    MJ1_printf!("ExecInitMergeJoin: {}\n", "initializing node");

    /*
     * create state structure
     */
    mergestate = makeNode!(MergeJoinState, T_MergeJoinState);
    (*mergestate).js.ps.plan = node as *mut Plan;
    (*mergestate).js.ps.state = estate;
    (*mergestate).js.ps.ExecProcNode = Some(ExecMergeJoin);
    (*mergestate).js.jointype = (*node).join.jointype;
    (*mergestate).mj_ConstFalseJoin = false;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*mergestate).js.ps);

    /*
     * we need two additional econtexts in which we can compute the join
     * expressions from the left and right input tuples.  The node's regular
     * econtext won't do because it gets reset too often.
     */
    (*mergestate).mj_OuterEContext = CreateExprContext(estate);
    (*mergestate).mj_InnerEContext = CreateExprContext(estate);

    /*
     * initialize child nodes
     *
     * inner child must support MARK/RESTORE, unless we have detected that we
     * don't need that.  Note that skip_mark_restore must never be set if
     * there are non-mergeclause joinquals, since the logic wouldn't work.
     */
    Assert!((*node).join.joinqual.is_null() || !(*node).skip_mark_restore);
    (*mergestate).mj_SkipMarkRestore = (*node).skip_mark_restore;

    (*mergestate).js.ps.lefttree = ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);
    outerDesc = ExecGetResultType(outerPlanState(mergestate as *mut PlanState));
    (*mergestate).js.ps.righttree = ExecInitNode(
        innerPlan(node as *mut Plan),
        estate,
        if (*mergestate).mj_SkipMarkRestore {
            eflags
        } else {
            eflags | EXEC_FLAG_MARK
        },
    );
    innerDesc = ExecGetResultType(innerPlanState(mergestate as *mut PlanState));

    /*
     * For certain types of inner child nodes, it is advantageous to issue
     * MARK every time we advance past an inner tuple we will never return to.
     * For other types, MARK on a tuple we cannot return to is a waste of
     * cycles.  Detect which case applies and set mj_ExtraMarks if we want to
     * issue "unnecessary" MARK calls.
     *
     * Currently, only Material wants the extra MARKs, and it will be helpful
     * only if eflags doesn't specify REWIND.
     *
     * Note that for IndexScan and IndexOnlyScan, it is *necessary* that we
     * not set mj_ExtraMarks; otherwise we might attempt to set a mark before
     * the first inner tuple, which they do not support.
     */
    if IsA!(innerPlan(node as *mut Plan), T_Material)
        && (eflags & EXEC_FLAG_REWIND) == 0
        && !(*mergestate).mj_SkipMarkRestore
    {
        (*mergestate).mj_ExtraMarks = true;
    } else {
        (*mergestate).mj_ExtraMarks = false;
    }

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlotTL(&mut (*mergestate).js.ps, &TTSOpsVirtual);
    ExecAssignProjectionInfo(&mut (*mergestate).js.ps, null_mut());

    /*
     * tuple table initialization
     */
    innerOps = ExecGetResultSlotOps(innerPlanState(mergestate as *mut PlanState), null_mut());
    (*mergestate).mj_MarkedTupleSlot =
        ExecInitExtraTupleSlot(estate, innerDesc, innerOps);

    /*
     * initialize child expressions
     */
    (*mergestate).js.ps.qual =
        ExecInitQual((*node).join.plan.qual, mergestate as *mut PlanState);
    (*mergestate).js.joinqual =
        ExecInitQual((*node).join.joinqual, mergestate as *mut PlanState);
    /* mergeclauses are handled below */

    /*
     * detect whether we need only consider the first matching inner tuple
     */
    (*mergestate).js.single_match =
        (*node).join.inner_unique || (*node).join.jointype == JOIN_SEMI;

    /* set up null tuples for outer joins, if needed */
    match (*node).join.jointype {
        JOIN_INNER | JOIN_SEMI => {
            (*mergestate).mj_FillOuter = false;
            (*mergestate).mj_FillInner = false;
        }
        JOIN_LEFT | JOIN_ANTI => {
            (*mergestate).mj_FillOuter = true;
            (*mergestate).mj_FillInner = false;
            (*mergestate).mj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
        }
        JOIN_RIGHT | JOIN_RIGHT_ANTI => {
            (*mergestate).mj_FillOuter = false;
            (*mergestate).mj_FillInner = true;
            (*mergestate).mj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);

            /*
             * Can't handle right, right-anti or full join with non-constant
             * extra joinclauses.  This should have been caught by planner.
             */
            if !check_constant_qual(
                (*node).join.joinqual,
                &mut (*mergestate).mj_ConstFalseJoin,
            ) {
                ereport!(
                    ERROR,
                    errmsg!("RIGHT JOIN is only supported with merge-joinable join conditions")
                );
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            }
        }
        JOIN_FULL => {
            (*mergestate).mj_FillOuter = true;
            (*mergestate).mj_FillInner = true;
            (*mergestate).mj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
            (*mergestate).mj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);

            /*
             * Can't handle right, right-anti or full join with non-constant
             * extra joinclauses.  This should have been caught by planner.
             */
            if !check_constant_qual(
                (*node).join.joinqual,
                &mut (*mergestate).mj_ConstFalseJoin,
            ) {
                ereport!(
                    ERROR,
                    errmsg!("FULL JOIN is only supported with merge-joinable join conditions")
                );
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            }
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
     * preprocess the merge clauses
     */
    (*mergestate).mj_NumClauses = list_length((*node).mergeclauses);
    (*mergestate).mj_Clauses = MJExamineQuals(
        (*node).mergeclauses,
        (*node).mergeFamilies,
        (*node).mergeCollations,
        (*node).mergeReversals,
        (*node).mergeNullsFirst,
        mergestate as *mut PlanState,
    ) as crate::nodes::execnodes::MergeJoinClause;

    /*
     * initialize join state
     */
    (*mergestate).mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    (*mergestate).mj_MatchedOuter = false;
    (*mergestate).mj_MatchedInner = false;
    (*mergestate).mj_OuterTupleSlot = null_mut();
    (*mergestate).mj_InnerTupleSlot = null_mut();

    /*
     * initialization successful
     */
    MJ1_printf!("ExecInitMergeJoin: {}\n", "node initialized");

    mergestate
}

/* ----------------------------------------------------------------
 *		ExecEndMergeJoin
 *
 * old comments
 *		frees storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndMergeJoin(node: *mut MergeJoinState) {
    MJ1_printf!("ExecEndMergeJoin: {}\n", "ending node processing");

    /*
     * shut down the subplans
     */
    ExecEndNode(innerPlanState(node as *mut PlanState));
    ExecEndNode(outerPlanState(node as *mut PlanState));

    MJ1_printf!("ExecEndMergeJoin: {}\n", "node processing ended");
}

pub unsafe fn ExecReScanMergeJoin(node: *mut MergeJoinState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);
    let innerPlan: *mut PlanState = innerPlanState(node as *mut PlanState);

    ExecClearTuple((*node).mj_MarkedTupleSlot);

    (*node).mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    (*node).mj_MatchedOuter = false;
    (*node).mj_MatchedInner = false;
    (*node).mj_OuterTupleSlot = null_mut();
    (*node).mj_InnerTupleSlot = null_mut();

    /*
     * if chgParam of subnodes is not null then plans will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
    if (*innerPlan).chgParam.is_null() {
        ExecReScan(innerPlan);
    }
}
