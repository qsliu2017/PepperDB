//! nodeValuesscan.c - Support routines for scanning Values lists
//!   ("VALUES (...), (...), ..." in rangetable).
//!
//! postgres source: src/backend/executor/nodeValuesscan.c
//! companion header: src/include/executor/nodeValuesscan.h
//!
//! INTERFACE ROUTINES
//!     ExecValuesScan          scans a values list.
//!     ExecValuesNext          retrieve next tuple in sequential order.
//!     ExecInitValuesScan      creates and initializes a valuesscan node.
//!     ExecReScanValuesScan    rescans the values list

use crate::prelude::*;

use std::ffi::c_int;

use crate::access::sdir::{ScanDirection, ScanDirectionIsForward};
use crate::access::common::tupdesc::{CompactAttribute, TupleDesc, TupleDescCompactAttr};
use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, PlanState, ScanState, ValuesScanState,
};
use crate::nodes::nodes::NodeTag;
use crate::nodes::nodes::Node;
use crate::nodes::pg_list::{lfirst, linitial, list_length, List};
use crate::nodes::plannodes::{innerPlan, outerPlan, Plan, ValuesScan};
use crate::executor::tuptable::TupleTableSlot;
use crate::{castNode, current_cell, foreach, lfirst_node, makeNode};

use crate::executor::executor::{
    ExecInitExprList, ExecInitQual, ExecInitResultTypeTL, ExecInitScanTupleSlot,
    ExecTypeFromExprList,
};
use crate::executor::execScan::{ExecAssignScanProjectionInfo, ExecScan, ExecScanReScan};
use crate::executor::execTuples::{ExecStoreVirtualTuple, TTSOpsVirtual};
use crate::executor::execUtils::{ExecAssignExprContext, ReScanExprContext};
use crate::executor::tuptable::ExecClearTuple;

/* ----------------------------------------------------------------
 *                      Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *      ValuesNext
 *
 *      This is a workhorse for ExecValuesScan
 * ----------------------------------------------------------------
 */
unsafe fn ValuesNext(node: *mut ValuesScanState) -> *mut TupleTableSlot {
    let slot: *mut TupleTableSlot;
    let estate: *mut EState;
    let econtext: *mut ExprContext;
    let direction: ScanDirection;
    let curr_idx: c_int;

    /*
     * get information from the estate and scan state
     */
    estate = (*node).ss.ps.state;
    direction = (*estate).es_direction;
    slot = (*node).ss.ss_ScanTupleSlot;
    econtext = (*node).rowcontext;

    /*
     * Get the next tuple. Return NULL if no more tuples.
     */
    if ScanDirectionIsForward(direction) {
        if (*node).curr_idx < (*node).array_len {
            (*node).curr_idx += 1;
        }
    } else {
        if (*node).curr_idx >= 0 {
            (*node).curr_idx -= 1;
        }
    }

    /*
     * Always clear the result slot; this is appropriate if we are at the end
     * of the data, and if we're not, we still need it as the first step of
     * the store-virtual-tuple protocol.  It seems wise to clear the slot
     * before we reset the context it might have pointers into.
     */
    ExecClearTuple(slot);

    curr_idx = (*node).curr_idx;
    if curr_idx >= 0 && curr_idx < (*node).array_len {
        let exprlist: *mut List = *(*node).exprlists.offset(curr_idx as isize);
        let mut exprstatelist: *mut List = *(*node).exprstatelists.offset(curr_idx as isize);
        let oldContext: MemoryContext;
        let values: *mut Datum;
        let isnull: *mut bool;
        let mut resind: c_int;

        /*
         * Get rid of any prior cycle's leftovers.  We use ReScanExprContext
         * not just ResetExprContext because we want any registered shutdown
         * callbacks to be called.
         */
        ReScanExprContext(econtext);

        /*
         * Do per-VALUES-row work in the per-tuple context.
         */
        oldContext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);

        /*
         * Unless we already made the expression eval state for this row,
         * build it in the econtext's per-tuple memory.  This is a tad
         * unusual, but we want to delete the eval state again when we move to
         * the next row, to avoid growth of memory requirements over a long
         * values list.  For rows in which that won't work, we already built
         * the eval state at plan startup.
         */
        if exprstatelist.is_null() {
            /*
             * Pass parent as NULL, not my plan node, because we don't want
             * anything in this transient state linking into permanent state.
             * The only expression type that might wish to do so is a SubPlan,
             * and we already checked that there aren't any.
             *
             * Note that passing parent = NULL also disables JIT compilation
             * of the expressions, which is a win, because they're only going
             * to be used once under normal circumstances.
             */
            exprstatelist = ExecInitExprList(exprlist, std::ptr::null_mut());
        }

        /* parser should have checked all sublists are the same length */
        Assert!(list_length(exprstatelist) == (*(*slot).tts_tupleDescriptor).natts);

        /*
         * Compute the expressions and build a virtual result tuple. We
         * already did ExecClearTuple(slot).
         */
        values = (*slot).tts_values;
        isnull = (*slot).tts_isnull;

        resind = 0;
        foreach!(lc, exprstatelist, {
            let estate: *mut ExprState = lfirst(current_cell!(lc)) as *mut ExprState;
            let attr: *mut CompactAttribute =
                TupleDescCompactAttr((*slot).tts_tupleDescriptor, resind);

            *values.offset(resind as isize) = ExecEvalExpr(
                estate,
                econtext,
                &mut *isnull.offset(resind as isize),
            );

            /*
             * We must force any R/W expanded datums to read-only state, in
             * case they are multiply referenced in the plan node's output
             * expressions, or in case we skip the output projection and the
             * output column is multiply referenced in higher plan nodes.
             */
            *values.offset(resind as isize) = MakeExpandedObjectReadOnly(
                *values.offset(resind as isize),
                *isnull.offset(resind as isize),
                (*attr).attlen,
            );

            resind += 1;
        });

        MemoryContextSwitchTo(oldContext);

        /*
         * And return the virtual tuple.
         */
        ExecStoreVirtualTuple(slot);
    }

    slot
}

/*
 * ValuesRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn ValuesRecheck(_node: *mut ValuesScanState, _slot: *mut TupleTableSlot) -> bool {
    /* nothing to check */
    true
}

/* ----------------------------------------------------------------
 *      ExecValuesScan(node)
 *
 *      Scans the values lists sequentially and returns the next qualifying
 *      tuple.
 *      We call the ExecScan() routine and pass it the appropriate
 *      access method functions.
 * ----------------------------------------------------------------
 */
unsafe fn ExecValuesScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut ValuesScanState = castNode!(ValuesScanState, T_ValuesScanState, pstate);

    ExecScan(
        &mut (*node).ss,
        Some(ValuesNext_access),
        Some(ValuesRecheck_recheck),
    )
}

/*
 * The C code casts ValuesNext/ValuesRecheck (which take ValuesScanState*) to
 * ExecScanAccessMtd/ExecScanRecheckMtd (which take ScanState*).  Since
 * ScanState is the first member of ValuesScanState, the pointer values are
 * identical; provide thin shim functions with the ScanState* signature.
 */
unsafe fn ValuesNext_access(node: *mut ScanState) -> *mut TupleTableSlot {
    ValuesNext(node as *mut ValuesScanState)
}

unsafe fn ValuesRecheck_recheck(node: *mut ScanState, slot: *mut TupleTableSlot) -> bool {
    ValuesRecheck(node as *mut ValuesScanState, slot)
}

/* ----------------------------------------------------------------
 *      ExecInitValuesScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitValuesScan(
    node: *mut ValuesScan,
    estate: *mut EState,
    _eflags: c_int,
) -> *mut ValuesScanState {
    let scanstate: *mut ValuesScanState;
    let tupdesc: TupleDesc;
    let mut i: c_int;
    let planstate: *mut PlanState;

    /*
     * ValuesScan should not have any children.
     */
    Assert!(outerPlan(node as *mut Plan).is_null());
    Assert!(innerPlan(node as *mut Plan).is_null());

    /*
     * create new ScanState for node
     */
    scanstate = makeNode!(ValuesScanState, T_ValuesScanState);
    (*scanstate).ss.ps.plan = node as *mut Plan;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecValuesScan);

    /*
     * Miscellaneous initialization
     */
    planstate = &mut (*scanstate).ss.ps;

    /*
     * Create expression contexts.  We need two, one for per-sublist
     * processing and one for execScan.c to use for quals and projections. We
     * cheat a little by using ExecAssignExprContext() to build both.
     */
    ExecAssignExprContext(estate, planstate);
    (*scanstate).rowcontext = (*planstate).ps_ExprContext;
    ExecAssignExprContext(estate, planstate);

    /*
     * Get info about values list, initialize scan slot with it.
     */
    tupdesc = ExecTypeFromExprList(linitial((*node).values_lists) as *mut List);
    ExecInitScanTupleSlot(estate, &mut (*scanstate).ss, tupdesc, &TTSOpsVirtual);

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*scanstate).ss.ps);
    ExecAssignScanProjectionInfo(&mut (*scanstate).ss);

    /*
     * initialize child expressions
     */
    (*scanstate).ss.ps.qual = ExecInitQual(
        (*node).scan.plan.qual,
        scanstate as *mut PlanState,
    );

    /*
     * Other node-specific setup
     */
    (*scanstate).curr_idx = -1;
    (*scanstate).array_len = list_length((*node).values_lists);

    /*
     * Convert the list of expression sublists into an array for easier
     * addressing at runtime.  Also, detect whether any sublists contain
     * SubPlans; for just those sublists, go ahead and do expression
     * initialization.  (This avoids problems with SubPlans wanting to connect
     * themselves up to the outer plan tree.  Notably, EXPLAIN won't see the
     * subplans otherwise; also we will have troubles with dangling pointers
     * and/or leaked resources if we try to handle SubPlans the same as
     * simpler expressions.)
     */
    (*scanstate).exprlists = palloc(
        (*scanstate).array_len as Size * std::mem::size_of::<*mut List>(),
    ) as *mut *mut List;
    (*scanstate).exprstatelists = palloc0(
        (*scanstate).array_len as Size * std::mem::size_of::<*mut List>(),
    ) as *mut *mut List;
    i = 0;
    foreach!(vtl, (*node).values_lists, {
        let exprs: *mut List = lfirst_node!(List, T_List, current_cell!(vtl));

        *(*scanstate).exprlists.offset(i as isize) = exprs;

        /*
         * We can avoid the cost of a contain_subplans() scan in the simple
         * case where there are no SubPlans anywhere.
         */
        if !(*estate).es_subplanstates.is_null() && contain_subplans(exprs as *mut Node) {
            let saved_jit_flags: c_int;

            /*
             * As these expressions are only used once, disable JIT for them.
             * This is worthwhile because it's common to insert significant
             * amounts of data via VALUES().  Note that this doesn't prevent
             * use of JIT *within* a subplan, since that's initialized
             * separately; this just affects the upper-level subexpressions.
             */
            saved_jit_flags = (*estate).es_jit_flags;
            (*estate).es_jit_flags = PGJIT_NONE;

            *(*scanstate).exprstatelists.offset(i as isize) =
                ExecInitExprList(exprs, &mut (*scanstate).ss.ps);

            (*estate).es_jit_flags = saved_jit_flags;
        }
        i += 1;
    });

    scanstate
}

/* ----------------------------------------------------------------
 *      ExecReScanValuesScan
 *
 *      Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanValuesScan(node: *mut ValuesScanState) {
    if !(*node).ss.ps.ps_ResultTupleSlot.is_null() {
        ExecClearTuple((*node).ss.ps.ps_ResultTupleSlot);
    }

    ExecScanReScan(&mut (*node).ss);

    (*node).curr_idx = -1;
}

/* ----------------------------------------------------------------
 *      Local stubs for not-yet-ported dependencies
 * ----------------------------------------------------------------
 */

/* utils/expandeddatum.h: MakeExpandedObjectReadOnly(d, isnull, typlen) */
unsafe fn MakeExpandedObjectReadOnly(d: Datum, _isnull: bool, _typlen: int16) -> Datum {
    d // TODO: utils/adt/expandeddatum.c - faithful R/W -> R/O conversion
}

/* executor/execExpr.c: ExecEvalExpr() */
unsafe fn ExecEvalExpr(
    _state: *mut ExprState,
    _econtext: *mut ExprContext,
    _isNull: *mut bool,
) -> Datum {
    crate::executor::executor::ExecEvalExpr(_state as _, _econtext as _, _isNull as _) as _
}

/* optimizer/clauses.c: contain_subplans() */
unsafe fn contain_subplans(_clause: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_subplans(_clause as _) as _
}

/* jit/jit.h: PGJIT_NONE */
const PGJIT_NONE: c_int = 0;
