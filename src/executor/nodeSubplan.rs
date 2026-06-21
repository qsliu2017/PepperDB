//! nodeSubplan.rs
//!   routines to support sub-selects appearing in expressions
//!
//! Translated 1:1 from postgres/src/backend/executor/nodeSubplan.c
//!
//! This module is concerned with executing SubPlan expression nodes, which
//! should not be confused with sub-SELECTs appearing in FROM.  SubPlans are
//! divided into "initplans", which are those that need only one evaluation per
//! query (among other restrictions, this requires that they don't use any
//! direct correlation variables from the parent plan level), and "regular"
//! subplans, which are re-evaluated every time their result is required.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/executor/nodeSubplan.c
//!
//! INTERFACE ROUTINES
//!     ExecSubPlan  - process a subselect
//!     ExecInitSubPlan - initialize a subselect

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::nodes::nodes::Node;

use std::ffi::{c_int, c_long, c_void};

use crate::postgres_ext::Oid;

use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::access::common::heaptuple::heap_freetuple;
use crate::access::htup_details::{heap_getattr, HeapTuple};
use crate::access::sdir::{ForwardScanDirection, ScanDirection};

use crate::nodes::bitmapset::{bms_add_member, bms_is_empty, bms_next_member, Bitmapset};
use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, PlanState, ProjectionInfo, SubPlanState, TupleHashIterator,
};
use crate::nodes::makefuncs::makeTargetEntry;
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::nodes::params::ParamExecData;
use crate::nodes::pg_list::{
    lappend, lfirst_int, linitial, linitial_int, list_length, list_nth, lsecond,
    List, ListCell, NIL,
};
use crate::nodes::plannodes::Plan;

use crate::optimizer::optimizer::clamp_cardinality_to_long;
use crate::nodes::primnodes::{
    BoolExpr, Expr, OpExpr, SubLinkType, SubPlan, TargetEntry, ALL_SUBLINK, ANY_SUBLINK,
    ARRAY_SUBLINK, CTE_SUBLINK, EXISTS_SUBLINK, EXPR_SUBLINK, MULTIEXPR_SUBLINK,
    ROWCOMPARE_SUBLINK,
};

use crate::executor::execGrouping::{
    BuildTupleHashTable, FindTupleHashEntry, LookupTupleHashEntry, ResetTupleHashTable,
    TupleHashEntry, TupleHashEntryGetTuple, TupleHashTable,
};
use crate::executor::execTuples::{ExecStoreMinimalTuple, TTSOpsMinimalTuple, TTSOpsVirtual};
use crate::executor::executor::{
    ExecBuildGroupingEqual, ExecBuildHash32FromAttrs, ExecBuildProjectionInfo, ExecEvalExprSwitchContext,
    ExecInitExpr, ExecInitExtraTupleSlot, ExecProcNode, ExecProject, ExecReScan, ExecTypeFromTL,
    ResetExprContext,
};
use crate::executor::execUtils::CreateExprContext;
use crate::executor::tuptable::{
    slot_attisnull, slot_getattr, ExecClearTuple, ExecCopySlotHeapTuple, TupIsNull, TupleTableSlot,
};

use crate::utils::adt::arrayfuncs::{
    accumArrayResultAny, initArrayResultAny, makeArrayResultAny, ArrayBuildStateAny,
};
use crate::utils::fmgr::{fmgr_info, FmgrInfo, FunctionCall2Coll};
use crate::utils::palloc::palloc;

use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::{castNode, foreach, current_cell, makeNode, Assert, IsA, lfirst_node, list_make1};

// ---------------------------------------------------------------------------
// Local stubs for symbols whose real homes are not yet ported.
// ---------------------------------------------------------------------------

/// TODO(pg-port): real `is_andclause` lives in nodes/nodeFuncs.c
unsafe fn is_andclause(clause: *const c_void) -> bool {
    if clause.is_null() {
        return false;
    }
    nodeTag(clause as *const NodeTag) == NodeTag::T_BoolExpr
        && (*(clause as *const BoolExpr)).boolop == crate::nodes::primnodes::AND_EXPR
}

/// TODO(pg-port): real `fmgr_info_set_expr` lives in utils/fmgr/fmgr.c (macro
/// setting flinfo->fn_expr).
unsafe fn fmgr_info_set_expr(expr: *mut c_void, finfo: *mut FmgrInfo) {
    (*finfo).fn_expr = expr as *mut Node;
}

/// TODO(pg-port): real `get_opcode` lives in utils/cache/lsyscache.c
unsafe fn get_opcode(opno: Oid) -> Oid {
    unimplemented!("get_opcode: lsyscache.c not ported")
}

/// TODO(pg-port): real `get_compatible_hash_operators` lives in utils/cache/lsyscache.c
unsafe fn get_compatible_hash_operators(
    opno: Oid,
    lhs_opno: *mut Oid,
    rhs_opno: *mut Oid,
) -> bool {
    crate::utils::cache::lsyscache::get_compatible_hash_operators(opno as _, lhs_opno as _, rhs_opno as _) as _
}

/// TODO(pg-port): real `get_op_hash_functions` lives in utils/cache/lsyscache.c
unsafe fn get_op_hash_functions(
    opno: Oid,
    lhs_procno: *mut Oid,
    rhs_procno: *mut Oid,
) -> bool {
    crate::utils::cache::lsyscache::get_op_hash_functions(opno as _, lhs_procno as _, rhs_procno as _) as _
}

/// TODO(pg-port): real FindTupleHashEntry hash-table iterator support lives in
/// executor/execGrouping.c (simplehash iterator macros).
unsafe fn InitTupleHashIterator(hashtable: TupleHashTable, iter: *mut TupleHashIterator) {
    unimplemented!("InitTupleHashIterator: simplehash iterator not ported")
}

/// TODO(pg-port): real ScanTupleHashTable lives in executor/execGrouping.c.
unsafe fn ScanTupleHashTable(
    hashtable: TupleHashTable,
    iter: *mut TupleHashIterator,
) -> TupleHashEntry {
    unimplemented!("ScanTupleHashTable: simplehash iterator not ported")
}

/// TODO(pg-port): real TermTupleHashIterator lives in executor/execGrouping.c.
unsafe fn TermTupleHashIterator(iter: *mut TupleHashIterator) {
    unimplemented!("TermTupleHashIterator: simplehash iterator not ported")
}

// ---------------------------------------------------------------------------

/* ----------------------------------------------------------------
 *		ExecSubPlan
 *
 * This is the main entry point for execution of a regular SubPlan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSubPlan(
    node: *mut SubPlanState,
    econtext: *mut ExprContext,
    isNull: *mut bool,
) -> Datum {
    let subplan: *mut SubPlan = (*node).subplan;
    let estate: *mut EState = (*(*node).planstate).state;
    let dir: ScanDirection = (*estate).es_direction;
    let retval: Datum;

    CHECK_FOR_INTERRUPTS();

    /* Set non-null as default */
    *isNull = false;

    /* Sanity checks */
    if (*subplan).subLinkType == CTE_SUBLINK {
        elog!(ERROR, "CTE subplans should not be executed via ExecSubPlan");
    }
    if (*subplan).setParam != NIL && (*subplan).subLinkType != MULTIEXPR_SUBLINK {
        elog!(ERROR, "cannot set parent params from subquery");
    }

    /* Force forward-scan mode for evaluation */
    (*estate).es_direction = ForwardScanDirection;

    /* Select appropriate evaluation strategy */
    if (*subplan).useHashTable {
        retval = ExecHashSubPlan(node, econtext, isNull);
    } else {
        retval = ExecScanSubPlan(node, econtext, isNull);
    }

    /* restore scan direction */
    (*estate).es_direction = dir;

    retval
}

/*
 * ExecHashSubPlan: store subselect result in an in-memory hash table
 */
unsafe fn ExecHashSubPlan(
    node: *mut SubPlanState,
    econtext: *mut ExprContext,
    isNull: *mut bool,
) -> Datum {
    let mut result: bool = false;
    let subplan: *mut SubPlan = (*node).subplan;
    let planstate: *mut PlanState = (*node).planstate;
    let slot: *mut TupleTableSlot;

    /* Shouldn't have any direct correlation Vars */
    if (*subplan).parParam != NIL || (*subplan).args != NIL {
        elog!(ERROR, "hashed subplan with direct correlation not supported");
    }

    /*
     * If first time through or we need to rescan the subplan, build the hash
     * table.
     */
    if (*node).hashtable.is_null() || !(*planstate).chgParam.is_null() {
        buildSubPlanHash(node, econtext);
    }

    /*
     * The result for an empty subplan is always FALSE; no need to evaluate
     * lefthand side.
     */
    *isNull = false;
    if !(*node).havehashrows && !(*node).havenullrows {
        return BoolGetDatum(false);
    }

    /*
     * Evaluate lefthand expressions and form a projection tuple. First we
     * have to set the econtext to use (hack alert!).
     */
    (*(*node).projLeft).pi_exprContext = econtext;
    slot = ExecProject((*node).projLeft);

    /*
     * If the LHS is all non-null, probe for an exact match in the main hash
     * table.  If we find one, the result is TRUE. Otherwise, scan the
     * partly-null table to see if there are any rows that aren't provably
     * unequal to the LHS; if so, the result is UNKNOWN.  (We skip that part
     * if we don't care about UNKNOWN.) Otherwise, the result is FALSE.
     *
     * Note: the reason we can avoid a full scan of the main hash table is
     * that the combining operators are assumed never to yield NULL when both
     * inputs are non-null.  If they were to do so, we might need to produce
     * UNKNOWN instead of FALSE because of an UNKNOWN result in comparing the
     * LHS to some main-table entry --- which is a comparison we will not even
     * make, unless there's a chance match of hash keys.
     */
    if slotNoNulls(slot) {
        if (*node).havehashrows
            && !FindTupleHashEntry(
                (*node).hashtable,
                slot,
                (*node).cur_eq_comp as _,
                (*node).lhs_hash_expr as _,
            )
            .is_null()
        {
            result = true;
        } else if (*node).havenullrows
            && findPartialMatch((*node).hashnulls, slot, (*node).cur_eq_funcs)
        {
            *isNull = true;
        }
    }
    /*
     * When the LHS is partly or wholly NULL, we can never return TRUE. If we
     * don't care about UNKNOWN, just return FALSE.  Otherwise, if the LHS is
     * wholly NULL, immediately return UNKNOWN.  (Since the combining
     * operators are strict, the result could only be FALSE if the sub-select
     * were empty, but we already handled that case.) Otherwise, we must scan
     * both the main and partly-null tables to see if there are any rows that
     * aren't provably unequal to the LHS; if so, the result is UNKNOWN.
     * Otherwise, the result is FALSE.
     */
    else if (*node).hashnulls.is_null() {
        /* just return FALSE */
    } else if slotAllNulls(slot) {
        *isNull = true;
    }
    /* Scan partly-null table first, since more likely to get a match */
    else if (*node).havenullrows
        && findPartialMatch((*node).hashnulls, slot, (*node).cur_eq_funcs)
    {
        *isNull = true;
    } else if (*node).havehashrows
        && findPartialMatch((*node).hashtable, slot, (*node).cur_eq_funcs)
    {
        *isNull = true;
    }

    /*
     * Note: because we are typically called in a per-tuple context, we have
     * to explicitly clear the projected tuple before returning. Otherwise,
     * we'll have a double-free situation: the per-tuple context will probably
     * be reset before we're called again, and then the tuple slot will think
     * it still needs to free the tuple.
     */
    ExecClearTuple(slot);

    /* Also must reset the hashtempcxt after each hashtable lookup. */
    MemoryContextReset((*node).hashtempcxt);

    BoolGetDatum(result)
}

/*
 * ExecScanSubPlan: default case where we have to rescan subplan each time
 */
unsafe fn ExecScanSubPlan(
    node: *mut SubPlanState,
    econtext: *mut ExprContext,
    isNull: *mut bool,
) -> Datum {
    let subplan: *mut SubPlan = (*node).subplan;
    let planstate: *mut PlanState = (*node).planstate;
    let subLinkType: SubLinkType = (*subplan).subLinkType;
    let oldcontext: MemoryContext;
    let mut slot: *mut TupleTableSlot;
    let mut result: Datum;
    let mut found: bool = false; /* true if got at least one subplan tuple */
    let mut l: *mut ListCell;
    let mut astate: *mut ArrayBuildStateAny = null_mut();

    /* Initialize ArrayBuildStateAny in caller's context, if needed */
    if subLinkType == ARRAY_SUBLINK {
        astate = initArrayResultAny((*subplan).firstColType, CurrentMemoryContext, true);
    }

    /*
     * We are probably in a short-lived expression-evaluation context. Switch
     * to the per-query context for manipulating the child plan's chgParam,
     * calling ExecProcNode on it, etc.
     */
    oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);

    /*
     * We rely on the caller to evaluate plan correlation values, if
     * necessary. However we still need to record the fact that the values
     * (might have) changed, otherwise the ExecReScan() below won't know that
     * nodes need to be rescanned.
     */
    foreach!(l, (*subplan).parParam, {
        let paramid: c_int = lfirst_int(current_cell!(l));

        (*planstate).chgParam = bms_add_member((*planstate).chgParam, paramid);
    });

    /* with that done, we can reset the subplan */
    ExecReScan(planstate);

    /*
     * For all sublink types except EXPR_SUBLINK and ARRAY_SUBLINK, the result
     * is boolean as are the results of the combining operators. We combine
     * results across tuples (if the subplan produces more than one) using OR
     * semantics for ANY_SUBLINK or AND semantics for ALL_SUBLINK.
     * (ROWCOMPARE_SUBLINK doesn't allow multiple tuples from the subplan.)
     * NULL results from the combining operators are handled according to the
     * usual SQL semantics for OR and AND.  The result for no input tuples is
     * FALSE for ANY_SUBLINK, TRUE for ALL_SUBLINK, NULL for
     * ROWCOMPARE_SUBLINK.
     *
     * For EXPR_SUBLINK we require the subplan to produce no more than one
     * tuple, else an error is raised.  If zero tuples are produced, we return
     * NULL.  Assuming we get a tuple, we just use its first column (there can
     * be only one non-junk column in this case).
     *
     * For MULTIEXPR_SUBLINK, we push the per-column subplan outputs out to
     * the setParams and then return a dummy false value.  There must not be
     * multiple tuples returned from the subplan; if zero tuples are produced,
     * set the setParams to NULL.
     *
     * For ARRAY_SUBLINK we allow the subplan to produce any number of tuples,
     * and form an array of the first column's values.  Note in particular
     * that we produce a zero-element array if no tuples are produced (this is
     * a change from pre-8.3 behavior of returning NULL).
     */
    result = BoolGetDatum(subLinkType == ALL_SUBLINK);
    *isNull = false;

    slot = ExecProcNode(planstate);
    while !TupIsNull(slot) {
        let tdesc: TupleDesc = (*slot).tts_tupleDescriptor;
        let rowresult: Datum;
        let mut rownull: bool = false;
        let mut col: c_int;
        let mut plst: *mut ListCell;

        if subLinkType == EXISTS_SUBLINK {
            found = true;
            result = BoolGetDatum(true);
            break;
        }

        if subLinkType == EXPR_SUBLINK {
            /* cannot allow multiple input tuples for EXPR sublink */
            if found {
                ereport!(
                    ERROR,
                    "more than one row returned by a subquery used as an expression"
                );
            }
            found = true;

            /*
             * We need to copy the subplan's tuple in case the result is of
             * pass-by-ref type --- our return value will point into this
             * copied tuple!  Can't use the subplan's instance of the tuple
             * since it won't still be valid after next ExecProcNode() call.
             * node->curTuple keeps track of the copied tuple for eventual
             * freeing.
             */
            if !(*node).curTuple.is_null() {
                heap_freetuple((*node).curTuple);
            }
            (*node).curTuple = ExecCopySlotHeapTuple(slot);

            result = heap_getattr((*node).curTuple, 1, tdesc, isNull);
            /* keep scanning subplan to make sure there's only one tuple */
            slot = ExecProcNode(planstate);
            continue;
        }

        if subLinkType == MULTIEXPR_SUBLINK {
            /* cannot allow multiple input tuples for MULTIEXPR sublink */
            if found {
                ereport!(
                    ERROR,
                    "more than one row returned by a subquery used as an expression"
                );
            }
            found = true;

            /*
             * We need to copy the subplan's tuple in case any result is of
             * pass-by-ref type --- our output values will point into this
             * copied tuple!  Can't use the subplan's instance of the tuple
             * since it won't still be valid after next ExecProcNode() call.
             * node->curTuple keeps track of the copied tuple for eventual
             * freeing.
             */
            if !(*node).curTuple.is_null() {
                heap_freetuple((*node).curTuple);
            }
            (*node).curTuple = ExecCopySlotHeapTuple(slot);

            /*
             * Now set all the setParam params from the columns of the tuple
             */
            col = 1;
            foreach!(plst, (*subplan).setParam, {
                let paramid: c_int = lfirst_int(current_cell!(plst));
                let prmdata: *mut ParamExecData;

                prmdata = &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));
                Assert!((*prmdata).execPlan.is_null());
                (*prmdata).value =
                    heap_getattr((*node).curTuple, col, tdesc, &raw mut (*prmdata).isnull);
                col += 1;
            });

            /* keep scanning subplan to make sure there's only one tuple */
            slot = ExecProcNode(planstate);
            continue;
        }

        if subLinkType == ARRAY_SUBLINK {
            let dvalue: Datum;
            let mut disnull: bool = false;

            found = true;
            /* stash away current value */
            Assert!((*subplan).firstColType == (*TupleDescAttr(tdesc, 0)).atttypid);
            dvalue = slot_getattr(slot, 1, &raw mut disnull);
            astate = accumArrayResultAny(
                astate,
                dvalue,
                disnull,
                (*subplan).firstColType,
                oldcontext,
            );
            /* keep scanning subplan to collect all values */
            slot = ExecProcNode(planstate);
            continue;
        }

        /* cannot allow multiple input tuples for ROWCOMPARE sublink either */
        if subLinkType == ROWCOMPARE_SUBLINK && found {
            ereport!(
                ERROR,
                "more than one row returned by a subquery used as an expression"
            );
        }

        found = true;

        /*
         * For ALL, ANY, and ROWCOMPARE sublinks, load up the Params
         * representing the columns of the sub-select, and then evaluate the
         * combining expression.
         */
        col = 1;
        foreach!(plst, (*subplan).paramIds, {
            let paramid: c_int = lfirst_int(current_cell!(plst));
            let prmdata: *mut ParamExecData;

            prmdata = &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));
            Assert!((*prmdata).execPlan.is_null());
            (*prmdata).value = slot_getattr(slot, col, &raw mut (*prmdata).isnull);
            col += 1;
        });

        rowresult = ExecEvalExprSwitchContext((*node).testexpr, econtext, &raw mut rownull);

        if subLinkType == ANY_SUBLINK {
            /* combine across rows per OR semantics */
            if rownull {
                *isNull = true;
            } else if DatumGetBool(rowresult) {
                result = BoolGetDatum(true);
                *isNull = false;
                break; /* needn't look at any more rows */
            }
        } else if subLinkType == ALL_SUBLINK {
            /* combine across rows per AND semantics */
            if rownull {
                *isNull = true;
            } else if !DatumGetBool(rowresult) {
                result = BoolGetDatum(false);
                *isNull = false;
                break; /* needn't look at any more rows */
            }
        } else {
            /* must be ROWCOMPARE_SUBLINK */
            result = rowresult;
            *isNull = rownull;
        }

        slot = ExecProcNode(planstate);
    }

    MemoryContextSwitchTo(oldcontext);

    if subLinkType == ARRAY_SUBLINK {
        /* We return the result in the caller's context */
        result = makeArrayResultAny(astate, oldcontext, true);
    } else if !found {
        /*
         * deal with empty subplan result.  result/isNull were previously
         * initialized correctly for all sublink types except EXPR and
         * ROWCOMPARE; for those, return NULL.
         */
        if subLinkType == EXPR_SUBLINK || subLinkType == ROWCOMPARE_SUBLINK {
            result = 0 as Datum;
            *isNull = true;
        } else if subLinkType == MULTIEXPR_SUBLINK {
            /* We don't care about function result, but set the setParams */
            foreach!(l, (*subplan).setParam, {
                let paramid: c_int = lfirst_int(current_cell!(l));
                let prmdata: *mut ParamExecData;

                prmdata = &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));
                Assert!((*prmdata).execPlan.is_null());
                (*prmdata).value = 0 as Datum;
                (*prmdata).isnull = true;
            });
        }
    }

    result
}

/*
 * buildSubPlanHash: load hash table by scanning subplan output.
 */
unsafe fn buildSubPlanHash(node: *mut SubPlanState, econtext: *mut ExprContext) {
    let subplan: *mut SubPlan = (*node).subplan;
    let planstate: *mut PlanState = (*node).planstate;
    let ncols: c_int = (*node).numCols;
    let innerecontext: *mut ExprContext = (*node).innerecontext;
    let oldcontext: MemoryContext;
    let mut nbuckets: c_long;
    let mut slot: *mut TupleTableSlot;

    Assert!((*subplan).subLinkType == ANY_SUBLINK);

    /*
     * If we already had any hash tables, reset 'em; otherwise create empty
     * hash table(s).
     *
     * If we need to distinguish accurately between FALSE and UNKNOWN (i.e.,
     * NULL) results of the IN operation, then we have to store subplan output
     * rows that are partly or wholly NULL.  We store such rows in a separate
     * hash table that we expect will be much smaller than the main table. (We
     * can use hashing to eliminate partly-null rows that are not distinct. We
     * keep them separate to minimize the cost of the inevitable full-table
     * searches; see findPartialMatch.)
     *
     * If it's not necessary to distinguish FALSE and UNKNOWN, then we don't
     * need to store subplan output rows that contain NULL.
     *
     * Because the input slot for each hash table is always the slot resulting
     * from an ExecProject(), we can use TTSOpsVirtual for the input ops. This
     * saves a needless fetch inner op step for the hashing ExprState created
     * in BuildTupleHashTable().
     */
    MemoryContextReset((*node).hashtablecxt);
    (*node).havehashrows = false;
    (*node).havenullrows = false;

    nbuckets = clamp_cardinality_to_long((*(*planstate).plan).plan_rows);
    if nbuckets < 1 {
        nbuckets = 1;
    }

    if !(*node).hashtable.is_null() {
        ResetTupleHashTable((*node).hashtable);
    } else {
        (*node).hashtable = BuildTupleHashTable(
            (*node).parent as *mut c_void,
            (*node).descRight as *mut c_void,
            &TTSOpsVirtual,
            ncols,
            (*node).keyColIdx,
            (*node).tab_eq_funcoids,
            (*node).tab_hash_funcs as _,
            (*node).tab_collations,
            nbuckets,
            0,
            (*(*(*node).planstate).state).es_query_cxt,
            (*node).hashtablecxt,
            (*node).hashtempcxt,
            false,
        );
    }

    if !(*subplan).unknownEqFalse {
        if ncols == 1 {
            nbuckets = 1; /* there can only be one entry */
        } else {
            nbuckets /= 16;
            if nbuckets < 1 {
                nbuckets = 1;
            }
        }

        if !(*node).hashnulls.is_null() {
            ResetTupleHashTable((*node).hashnulls);
        } else {
            (*node).hashnulls = BuildTupleHashTable(
                (*node).parent as *mut c_void,
                (*node).descRight as *mut c_void,
                &TTSOpsVirtual,
                ncols,
                (*node).keyColIdx,
                (*node).tab_eq_funcoids,
                (*node).tab_hash_funcs as _,
                (*node).tab_collations,
                nbuckets,
                0,
                (*(*(*node).planstate).state).es_query_cxt,
                (*node).hashtablecxt,
                (*node).hashtempcxt,
                false,
            );
        }
    } else {
        (*node).hashnulls = null_mut();
    }

    /*
     * We are probably in a short-lived expression-evaluation context. Switch
     * to the per-query context for manipulating the child plan.
     */
    oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);

    /*
     * Reset subplan to start.
     */
    ExecReScan(planstate);

    /*
     * Scan the subplan and load the hash table(s).  Note that when there are
     * duplicate rows coming out of the sub-select, only one copy is stored.
     */
    slot = ExecProcNode(planstate);
    while !TupIsNull(slot) {
        let mut col: c_int = 1;
        let mut plst: *mut ListCell;
        let mut isnew: bool = false;

        /*
         * Load up the Params representing the raw sub-select outputs, then
         * form the projection tuple to store in the hashtable.
         */
        foreach!(plst, (*subplan).paramIds, {
            let paramid: c_int = lfirst_int(current_cell!(plst));
            let prmdata: *mut ParamExecData;

            prmdata = &raw mut (*(*innerecontext).ecxt_param_exec_vals.offset(paramid as isize));
            Assert!((*prmdata).execPlan.is_null());
            (*prmdata).value = slot_getattr(slot, col, &raw mut (*prmdata).isnull);
            col += 1;
        });
        slot = ExecProject((*node).projRight);

        /*
         * If result contains any nulls, store separately or not at all.
         */
        if slotNoNulls(slot) {
            let _ = LookupTupleHashEntry((*node).hashtable, slot, &raw mut isnew, null_mut());
            (*node).havehashrows = true;
        } else if !(*node).hashnulls.is_null() {
            let _ = LookupTupleHashEntry((*node).hashnulls, slot, &raw mut isnew, null_mut());
            (*node).havenullrows = true;
        }

        /*
         * Reset innerecontext after each inner tuple to free any memory used
         * during ExecProject.
         */
        ResetExprContext(innerecontext);

        /* Also must reset the hashtempcxt after each hashtable lookup. */
        MemoryContextReset((*node).hashtempcxt);

        slot = ExecProcNode(planstate);
    }

    /*
     * Since the projected tuples are in the sub-query's context and not the
     * main context, we'd better clear the tuple slot before there's any
     * chance of a reset of the sub-query's context.  Else we will have the
     * potential for a double free attempt.  (XXX possibly no longer needed,
     * but can't hurt.)
     */
    ExecClearTuple((*(*node).projRight).pi_state.resultslot);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * execTuplesUnequal
 *		Return true if two tuples are definitely unequal in the indicated
 *		fields.
 *
 * Nulls are neither equal nor unequal to anything else.  A true result
 * is obtained only if there are non-null fields that compare not-equal.
 *
 * slot1, slot2: the tuples to compare (must have same columns!)
 * numCols: the number of attributes to be examined
 * matchColIdx: array of attribute column numbers
 * eqFunctions: array of fmgr lookup info for the equality functions to use
 * evalContext: short-term memory context for executing the functions
 */
unsafe fn execTuplesUnequal(
    slot1: *mut TupleTableSlot,
    slot2: *mut TupleTableSlot,
    numCols: c_int,
    matchColIdx: *mut AttrNumber,
    eqfunctions: *mut FmgrInfo,
    collations: *const Oid,
    evalContext: MemoryContext,
) -> bool {
    let oldContext: MemoryContext;
    let mut result: bool;
    let mut i: c_int;

    /* Reset and switch into the temp context. */
    MemoryContextReset(evalContext);
    oldContext = MemoryContextSwitchTo(evalContext);

    /*
     * We cannot report a match without checking all the fields, but we can
     * report a non-match as soon as we find unequal fields.  So, start
     * comparing at the last field (least significant sort key). That's the
     * most likely to be different if we are dealing with sorted input.
     */
    result = false;

    i = numCols;
    loop {
        i -= 1;
        if i < 0 {
            break;
        }
        let att: AttrNumber = *matchColIdx.offset(i as isize);
        let attr1: Datum;
        let attr2: Datum;
        let mut isNull1: bool = false;
        let mut isNull2: bool = false;

        attr1 = slot_getattr(slot1, att as c_int, &raw mut isNull1);

        if isNull1 {
            continue; /* can't prove anything here */
        }

        attr2 = slot_getattr(slot2, att as c_int, &raw mut isNull2);

        if isNull2 {
            continue; /* can't prove anything here */
        }

        /* Apply the type-specific equality function */
        if !DatumGetBool(FunctionCall2Coll(
            eqfunctions.offset(i as isize),
            *collations.offset(i as isize),
            attr1,
            attr2,
        )) {
            result = true; /* they are unequal */
            break;
        }
    }

    MemoryContextSwitchTo(oldContext);

    result
}

/*
 * findPartialMatch: does the hashtable contain an entry that is not
 * provably distinct from the tuple?
 *
 * We have to scan the whole hashtable; we can't usefully use hashkeys
 * to guide probing, since we might get partial matches on tuples with
 * hashkeys quite unrelated to what we'd get from the given tuple.
 *
 * Caller must provide the equality functions to use, since in cross-type
 * cases these are different from the hashtable's internal functions.
 */
unsafe fn findPartialMatch(
    hashtable: TupleHashTable,
    slot: *mut TupleTableSlot,
    eqfunctions: *mut FmgrInfo,
) -> bool {
    let numCols: c_int = (*hashtable).numCols;
    let keyColIdx: *mut AttrNumber = (*hashtable).keyColIdx;
    let mut hashiter: TupleHashIterator = std::mem::zeroed();
    let mut entry: TupleHashEntry;

    InitTupleHashIterator(hashtable, &raw mut hashiter);
    loop {
        entry = ScanTupleHashTable(hashtable, &raw mut hashiter);
        if entry.is_null() {
            break;
        }
        CHECK_FOR_INTERRUPTS();

        ExecStoreMinimalTuple(
            TupleHashEntryGetTuple(entry),
            (*hashtable).tableslot,
            false,
        );
        if !execTuplesUnequal(
            slot,
            (*hashtable).tableslot,
            numCols,
            keyColIdx,
            eqfunctions,
            (*hashtable).tab_collations,
            (*hashtable).tempcxt,
        ) {
            TermTupleHashIterator(&raw mut hashiter);
            return true;
        }
    }
    /* No TermTupleHashIterator call needed here */
    false
}

/*
 * slotAllNulls: is the slot completely NULL?
 *
 * This does not test for dropped columns, which is OK because we only
 * use it on projected tuples.
 */
unsafe fn slotAllNulls(slot: *mut TupleTableSlot) -> bool {
    let ncols: c_int = (*(*slot).tts_tupleDescriptor).natts;
    let mut i: c_int;

    i = 1;
    while i <= ncols {
        if !slot_attisnull(slot, i) {
            return false;
        }
        i += 1;
    }
    true
}

/*
 * slotNoNulls: is the slot entirely not NULL?
 *
 * This does not test for dropped columns, which is OK because we only
 * use it on projected tuples.
 */
unsafe fn slotNoNulls(slot: *mut TupleTableSlot) -> bool {
    let ncols: c_int = (*(*slot).tts_tupleDescriptor).natts;
    let mut i: c_int;

    i = 1;
    while i <= ncols {
        if slot_attisnull(slot, i) {
            return false;
        }
        i += 1;
    }
    true
}

/* ----------------------------------------------------------------
 *		ExecInitSubPlan
 *
 * Create a SubPlanState for a SubPlan; this is the SubPlan-specific part
 * of ExecInitExpr().  We split it out so that it can be used for InitPlans
 * as well as regular SubPlans.  Note that we don't link the SubPlan into
 * the parent's subPlan list, because that shouldn't happen for InitPlans.
 * Instead, ExecInitExpr() does that one part.
 *
 * We also rely on ExecInitExpr(), more precisely ExecInitSubPlanExpr(), to
 * evaluate input parameters, as that allows them to be evaluated as part of
 * the expression referencing the SubPlan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitSubPlan(subplan: *mut SubPlan, parent: *mut PlanState) -> *mut SubPlanState {
    let sstate: *mut SubPlanState = makeNode!(SubPlanState, T_SubPlanState);
    let estate: *mut EState = (*parent).state;

    (*sstate).subplan = subplan;

    /* Link the SubPlanState to already-initialized subplan */
    (*sstate).planstate = list_nth(
        (*estate).es_subplanstates,
        (*subplan).plan_id - 1,
    ) as *mut PlanState;

    /*
     * This check can fail if the planner mistakenly puts a parallel-unsafe
     * subplan into a parallelized subquery; see ExecSerializePlan.
     */
    if (*sstate).planstate.is_null() {
        elog!(
            ERROR,
            "subplan \"{:?}\" was not initialized",
            (*subplan).plan_name
        );
    }

    /* Link to parent's state, too */
    (*sstate).parent = parent;

    /* Initialize subexpressions */
    (*sstate).testexpr = ExecInitExpr((*subplan).testexpr as *mut Expr, parent);

    /*
     * initialize my state
     */
    (*sstate).curTuple = null_mut();
    (*sstate).curArray = PointerGetDatum(null_mut());
    (*sstate).projLeft = null_mut();
    (*sstate).projRight = null_mut();
    (*sstate).hashtable = null_mut();
    (*sstate).hashnulls = null_mut();
    (*sstate).hashtablecxt = null_mut();
    (*sstate).hashtempcxt = null_mut();
    (*sstate).innerecontext = null_mut();
    (*sstate).keyColIdx = null_mut();
    (*sstate).tab_eq_funcoids = null_mut();
    (*sstate).tab_hash_funcs = null_mut();
    (*sstate).tab_collations = null_mut();
    (*sstate).cur_eq_funcs = null_mut();

    /*
     * If this is an initplan, it has output parameters that the parent plan
     * will use, so mark those parameters as needing evaluation.  We don't
     * actually run the subplan until we first need one of its outputs.
     *
     * A CTE subplan's output parameter is never to be evaluated in the normal
     * way, so skip this in that case.
     *
     * Note that we don't set parent->chgParam here: the parent plan hasn't
     * been run yet, so no need to force it to re-run.
     */
    if (*subplan).setParam != NIL
        && (*subplan).parParam == NIL
        && (*subplan).subLinkType != CTE_SUBLINK
    {
        let mut lst: *mut ListCell;

        foreach!(lst, (*subplan).setParam, {
            let paramid: c_int = lfirst_int(current_cell!(lst));
            let prm: *mut ParamExecData =
                &raw mut (*(*estate).es_param_exec_vals.offset(paramid as isize));

            (*prm).execPlan = sstate as *mut c_void;
        });
    }

    /*
     * If we are going to hash the subquery output, initialize relevant stuff.
     * (We don't create the hashtable until needed, though.)
     */
    if (*subplan).useHashTable {
        let ncols: c_int;
        let mut i: c_int;
        let tupDescLeft: TupleDesc;
        let tupDescRight: TupleDesc;
        let cross_eq_funcoids: *mut Oid;
        let mut slot: *mut TupleTableSlot;
        let lhs_hash_funcs: *mut FmgrInfo;
        let oplist: *mut List;
        let mut lefttlist: *mut List;
        let mut righttlist: *mut List;
        let mut l: *mut ListCell;

        /* We need a memory context to hold the hash table(s) */
        (*sstate).hashtablecxt = AllocSetContextCreate!(
            CurrentMemoryContext,
            "Subplan HashTable Context",
            ALLOCSET_DEFAULT_SIZES
        );
        /* and a small one for the hash tables to use as temp storage */
        (*sstate).hashtempcxt = AllocSetContextCreate!(
            CurrentMemoryContext,
            "Subplan HashTable Temp Context",
            ALLOCSET_SMALL_SIZES
        );
        /* and a short-lived exprcontext for function evaluation */
        (*sstate).innerecontext = CreateExprContext(estate);

        /*
         * We use ExecProject to evaluate the lefthand and righthand
         * expression lists and form tuples.  (You might think that we could
         * use the sub-select's output tuples directly, but that is not the
         * case if we had to insert any run-time coercions of the sub-select's
         * output datatypes; anyway this avoids storing any resjunk columns
         * that might be in the sub-select's output.)  Run through the
         * combining expressions to build tlists for the lefthand and
         * righthand sides.
         *
         * We also extract the combining operators themselves to initialize
         * the equality and hashing functions for the hash tables.
         */
        if IsA!((*subplan).testexpr, T_OpExpr) {
            /* single combining operator */
            oplist = list_make1!((*subplan).testexpr as *mut c_void);
        } else if is_andclause((*subplan).testexpr as *const c_void) {
            /* multiple combining operators */
            oplist = (*castNode!(BoolExpr, T_BoolExpr, (*subplan).testexpr)).args;
        } else {
            /* shouldn't see anything else in a hashable subplan */
            elog!(
                ERROR,
                "unrecognized testexpr type: {}",
                nodeTag((*subplan).testexpr) as c_int
            );
            #[allow(unreachable_code)]
            {
                oplist = NIL; /* keep compiler quiet */
            }
        }
        ncols = list_length(oplist);

        lefttlist = NIL;
        righttlist = NIL;
        (*sstate).numCols = ncols;
        (*sstate).keyColIdx =
            palloc(ncols as usize * std::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
        (*sstate).tab_eq_funcoids =
            palloc(ncols as usize * std::mem::size_of::<Oid>()) as *mut Oid;
        (*sstate).tab_collations =
            palloc(ncols as usize * std::mem::size_of::<Oid>()) as *mut Oid;
        (*sstate).tab_hash_funcs =
            palloc(ncols as usize * std::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
        lhs_hash_funcs =
            palloc(ncols as usize * std::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
        (*sstate).cur_eq_funcs =
            palloc(ncols as usize * std::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;
        /* we'll need the cross-type equality fns below, but not in sstate */
        cross_eq_funcoids = palloc(ncols as usize * std::mem::size_of::<Oid>()) as *mut Oid;

        i = 1;
        foreach!(l, oplist, {
            let opexpr: *mut OpExpr = lfirst_node!(OpExpr, T_OpExpr, current_cell!(l));
            let mut expr: *mut Expr;
            let mut tle: *mut TargetEntry;
            let mut rhs_eq_oper: Oid = 0;
            let mut left_hashfn: Oid = 0;
            let mut right_hashfn: Oid = 0;

            Assert!(list_length((*opexpr).args) == 2);

            /* Process lefthand argument */
            expr = linitial((*opexpr).args) as *mut Expr;
            tle = makeTargetEntry(expr, i as AttrNumber, null_mut(), false);
            lefttlist = lappend(lefttlist, tle as *mut c_void);

            /* Process righthand argument */
            expr = lsecond((*opexpr).args) as *mut Expr;
            tle = makeTargetEntry(expr, i as AttrNumber, null_mut(), false);
            righttlist = lappend(righttlist, tle as *mut c_void);

            /* Lookup the equality function (potentially cross-type) */
            *cross_eq_funcoids.offset((i - 1) as isize) = (*opexpr).opfuncid;
            fmgr_info((*opexpr).opfuncid, (*sstate).cur_eq_funcs.offset((i - 1) as isize));
            fmgr_info_set_expr(
                opexpr as *mut c_void,
                (*sstate).cur_eq_funcs.offset((i - 1) as isize),
            );

            /* Look up the equality function for the RHS type */
            if !get_compatible_hash_operators((*opexpr).opno, null_mut(), &raw mut rhs_eq_oper) {
                elog!(
                    ERROR,
                    "could not find compatible hash operator for operator {}",
                    (*opexpr).opno
                );
            }
            *(*sstate).tab_eq_funcoids.offset((i - 1) as isize) = get_opcode(rhs_eq_oper);

            /* Lookup the associated hash functions */
            if !get_op_hash_functions(
                (*opexpr).opno,
                &raw mut left_hashfn,
                &raw mut right_hashfn,
            ) {
                elog!(
                    ERROR,
                    "could not find hash function for hash operator {}",
                    (*opexpr).opno
                );
            }
            fmgr_info(left_hashfn, lhs_hash_funcs.offset((i - 1) as isize));
            fmgr_info(
                right_hashfn,
                (*sstate).tab_hash_funcs.offset((i - 1) as isize),
            );

            /* Set collation */
            *(*sstate).tab_collations.offset((i - 1) as isize) = (*opexpr).inputcollid;

            /* keyColIdx is just column numbers 1..n */
            *(*sstate).keyColIdx.offset((i - 1) as isize) = i as AttrNumber;

            i += 1;
        });

        /*
         * Construct tupdescs, slots and projection nodes for left and right
         * sides.  The lefthand expressions will be evaluated in the parent
         * plan node's exprcontext, which we don't have access to here.
         * Fortunately we can just pass NULL for now and fill it in later
         * (hack alert!).  The righthand expressions will be evaluated in our
         * own innerecontext.
         */
        tupDescLeft = ExecTypeFromTL(lefttlist);
        slot = ExecInitExtraTupleSlot(estate, tupDescLeft, &TTSOpsVirtual);
        (*sstate).projLeft =
            ExecBuildProjectionInfo(lefttlist, null_mut(), slot, parent, null_mut());

        tupDescRight = ExecTypeFromTL(righttlist);
        (*sstate).descRight = tupDescRight;
        slot = ExecInitExtraTupleSlot(estate, tupDescRight, &TTSOpsVirtual);
        (*sstate).projRight = ExecBuildProjectionInfo(
            righttlist,
            (*sstate).innerecontext,
            slot,
            (*sstate).planstate,
            null_mut(),
        );

        /* Build the ExprState for generating hash values */
        (*sstate).lhs_hash_expr = ExecBuildHash32FromAttrs(
            tupDescLeft,
            &TTSOpsVirtual,
            lhs_hash_funcs,
            (*sstate).tab_collations,
            (*sstate).numCols,
            (*sstate).keyColIdx,
            parent,
            0,
        );

        /*
         * Create comparator for lookups of rows in the table (potentially
         * cross-type comparisons).
         */
        (*sstate).cur_eq_comp = ExecBuildGroupingEqual(
            tupDescLeft,
            tupDescRight,
            &TTSOpsVirtual,
            &TTSOpsMinimalTuple,
            ncols,
            (*sstate).keyColIdx,
            cross_eq_funcoids,
            (*sstate).tab_collations,
            parent,
        );
    }

    sstate
}

/* ----------------------------------------------------------------
 *		ExecSetParamPlan
 *
 *		Executes a subplan and sets its output parameters.
 *
 * This is called from ExecEvalParamExec() when the value of a PARAM_EXEC
 * parameter is requested and the param's execPlan field is set (indicating
 * that the param has not yet been evaluated).  This allows lazy evaluation
 * of initplans: we don't run the subplan until/unless we need its output.
 * Note that this routine MUST clear the execPlan fields of the plan's
 * output parameters after evaluating them!
 *
 * The results of this function are stored in the EState associated with the
 * ExprContext (particularly, its ecxt_param_exec_vals); any pass-by-ref
 * result Datums are allocated in the EState's per-query memory.  The passed
 * econtext can be any ExprContext belonging to that EState; which one is
 * important only to the extent that the ExprContext's per-tuple memory
 * context is used to evaluate any parameters passed down to the subplan.
 * (Thus in principle, the shorter-lived the ExprContext the better, since
 * that data isn't needed after we return.  In practice, because initplan
 * parameters are never more complex than Vars, Aggrefs, etc, evaluating them
 * currently never leaks any memory anyway.)
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSetParamPlan(node: *mut SubPlanState, econtext: *mut ExprContext) {
    let subplan: *mut SubPlan = (*node).subplan;
    let planstate: *mut PlanState = (*node).planstate;
    let subLinkType: SubLinkType = (*subplan).subLinkType;
    let estate: *mut EState = (*planstate).state;
    let dir: ScanDirection = (*estate).es_direction;
    let oldcontext: MemoryContext;
    let mut slot: *mut TupleTableSlot;
    let mut l: *mut ListCell;
    let mut found: bool = false;
    let mut astate: *mut ArrayBuildStateAny = null_mut();

    if subLinkType == ANY_SUBLINK || subLinkType == ALL_SUBLINK {
        elog!(ERROR, "ANY/ALL subselect unsupported as initplan");
    }
    if subLinkType == CTE_SUBLINK {
        elog!(
            ERROR,
            "CTE subplans should not be executed via ExecSetParamPlan"
        );
    }
    if !(*subplan).parParam.is_null() || !(*subplan).args.is_null() {
        elog!(
            ERROR,
            "correlated subplans should not be executed via ExecSetParamPlan"
        );
    }

    /*
     * Enforce forward scan direction regardless of caller. It's hard but not
     * impossible to get here in backward scan, so make it work anyway.
     */
    (*estate).es_direction = ForwardScanDirection;

    /* Initialize ArrayBuildStateAny in caller's context, if needed */
    if subLinkType == ARRAY_SUBLINK {
        astate = initArrayResultAny((*subplan).firstColType, CurrentMemoryContext, true);
    }

    /*
     * Must switch to per-query memory context.
     */
    oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_query_memory);

    /*
     * Run the plan.  (If it needs to be rescanned, the first ExecProcNode
     * call will take care of that.)
     */
    slot = ExecProcNode(planstate);
    while !TupIsNull(slot) {
        let tdesc: TupleDesc = (*slot).tts_tupleDescriptor;
        let mut i: c_int = 1;

        if subLinkType == EXISTS_SUBLINK {
            /* There can be only one setParam... */
            let paramid: c_int = linitial_int((*subplan).setParam);
            let prm: *mut ParamExecData =
                &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));

            (*prm).execPlan = null_mut();
            (*prm).value = BoolGetDatum(true);
            (*prm).isnull = false;
            found = true;
            break;
        }

        if subLinkType == ARRAY_SUBLINK {
            let dvalue: Datum;
            let mut disnull: bool = false;

            found = true;
            /* stash away current value */
            Assert!((*subplan).firstColType == (*TupleDescAttr(tdesc, 0)).atttypid);
            dvalue = slot_getattr(slot, 1, &raw mut disnull);
            astate = accumArrayResultAny(
                astate,
                dvalue,
                disnull,
                (*subplan).firstColType,
                oldcontext,
            );
            /* keep scanning subplan to collect all values */
            slot = ExecProcNode(planstate);
            continue;
        }

        if found
            && (subLinkType == EXPR_SUBLINK
                || subLinkType == MULTIEXPR_SUBLINK
                || subLinkType == ROWCOMPARE_SUBLINK)
        {
            ereport!(
                ERROR,
                "more than one row returned by a subquery used as an expression"
            );
        }

        found = true;

        /*
         * We need to copy the subplan's tuple into our own context, in case
         * any of the params are pass-by-ref type --- the pointers stored in
         * the param structs will point at this copied tuple! node->curTuple
         * keeps track of the copied tuple for eventual freeing.
         */
        if !(*node).curTuple.is_null() {
            heap_freetuple((*node).curTuple);
        }
        (*node).curTuple = ExecCopySlotHeapTuple(slot);

        /*
         * Now set all the setParam params from the columns of the tuple
         */
        foreach!(l, (*subplan).setParam, {
            let paramid: c_int = lfirst_int(current_cell!(l));
            let prm: *mut ParamExecData =
                &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));

            (*prm).execPlan = null_mut();
            (*prm).value = heap_getattr((*node).curTuple, i, tdesc, &raw mut (*prm).isnull);
            i += 1;
        });

        slot = ExecProcNode(planstate);
    }

    if subLinkType == ARRAY_SUBLINK {
        /* There can be only one setParam... */
        let paramid: c_int = linitial_int((*subplan).setParam);
        let prm: *mut ParamExecData =
            &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));

        /*
         * We build the result array in query context so it won't disappear;
         * to avoid leaking memory across repeated calls, we have to remember
         * the latest value, much as for curTuple above.
         */
        if (*node).curArray != PointerGetDatum(null_mut()) {
            pfree(DatumGetPointer((*node).curArray) as *mut c_void);
        }
        (*node).curArray =
            makeArrayResultAny(astate, (*econtext).ecxt_per_query_memory, true);
        (*prm).execPlan = null_mut();
        (*prm).value = (*node).curArray;
        (*prm).isnull = false;
    } else if !found {
        if subLinkType == EXISTS_SUBLINK {
            /* There can be only one setParam... */
            let paramid: c_int = linitial_int((*subplan).setParam);
            let prm: *mut ParamExecData =
                &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));

            (*prm).execPlan = null_mut();
            (*prm).value = BoolGetDatum(false);
            (*prm).isnull = false;
        } else {
            /* For other sublink types, set all the output params to NULL */
            foreach!(l, (*subplan).setParam, {
                let paramid: c_int = lfirst_int(current_cell!(l));
                let prm: *mut ParamExecData =
                    &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));

                (*prm).execPlan = null_mut();
                (*prm).value = 0 as Datum;
                (*prm).isnull = true;
            });
        }
    }

    MemoryContextSwitchTo(oldcontext);

    /* restore scan direction */
    (*estate).es_direction = dir;
}

/*
 * ExecSetParamPlanMulti
 *
 * Apply ExecSetParamPlan to evaluate any not-yet-evaluated initplan output
 * parameters whose ParamIDs are listed in "params".  Any listed params that
 * are not initplan outputs are ignored.
 *
 * As with ExecSetParamPlan, any ExprContext belonging to the current EState
 * can be used, but in principle a shorter-lived ExprContext is better than a
 * longer-lived one.
 */
pub unsafe fn ExecSetParamPlanMulti(params: *const Bitmapset, econtext: *mut ExprContext) {
    let mut paramid: c_int;

    paramid = -1;
    loop {
        paramid = bms_next_member(params, paramid);
        if paramid < 0 {
            break;
        }
        let prm: *mut ParamExecData =
            &raw mut (*(*econtext).ecxt_param_exec_vals.offset(paramid as isize));

        if !(*prm).execPlan.is_null() {
            /* Parameter not evaluated yet, so go do it */
            ExecSetParamPlan((*prm).execPlan as *mut SubPlanState, econtext);
            /* ExecSetParamPlan should have processed this param... */
            Assert!((*prm).execPlan.is_null());
        }
    }
}

/*
 * Mark an initplan as needing recalculation
 */
pub unsafe fn ExecReScanSetParamPlan(node: *mut SubPlanState, parent: *mut PlanState) {
    let planstate: *mut PlanState = (*node).planstate;
    let subplan: *mut SubPlan = (*node).subplan;
    let estate: *mut EState = (*parent).state;
    let mut l: *mut ListCell;

    /* sanity checks */
    if (*subplan).parParam != NIL {
        elog!(ERROR, "direct correlated subquery unsupported as initplan");
    }
    if (*subplan).setParam == NIL {
        elog!(ERROR, "setParam list of initplan is empty");
    }
    if bms_is_empty((*(*planstate).plan).extParam) {
        elog!(ERROR, "extParam set of initplan is empty");
    }

    /*
     * Don't actually re-scan: it'll happen inside ExecSetParamPlan if needed.
     */

    /*
     * Mark this subplan's output parameters as needing recalculation.
     *
     * CTE subplans are never executed via parameter recalculation; instead
     * they get run when called by nodeCtescan.c.  So don't mark the output
     * parameter of a CTE subplan as dirty, but do set the chgParam bit for it
     * so that dependent plan nodes will get told to rescan.
     */
    foreach!(l, (*subplan).setParam, {
        let paramid: c_int = lfirst_int(current_cell!(l));
        let prm: *mut ParamExecData =
            &raw mut (*(*estate).es_param_exec_vals.offset(paramid as isize));

        if (*subplan).subLinkType != CTE_SUBLINK {
            (*prm).execPlan = node as *mut c_void;
        }

        (*parent).chgParam = bms_add_member((*parent).chgParam, paramid);
    });
}
