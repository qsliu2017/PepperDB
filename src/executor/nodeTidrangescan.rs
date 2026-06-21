//! Routines to support TID range scans of relations.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translated 1:1 from:
//!   postgres/src/backend/executor/nodeTidrangescan.c
//!   postgres/src/include/executor/nodeTidrangescan.h

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::ptr;

use crate::{castNode, foreach, current_cell, makeNode, Assert, IsA};

use crate::executor::executor::{ExecScanAccessMtd, ExecScanRecheckMtd};

use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, PlanState, ScanState, TidRangeScanState, TupleTableSlot,
};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::plannodes::{Plan, TidRangeScan};
use crate::nodes::pg_list::{lappend, lfirst, List, ListCell};
use crate::nodes::primnodes::{Expr, OpExpr, Var};

use crate::storage::block::InvalidBlockNumber;
use crate::storage::itemptr::{
    ItemPointer, ItemPointerCompare, ItemPointerCopy, ItemPointerData, ItemPointerIsValid,
    ItemPointerSet,
};

use crate::access::sysattr::SelfItemPointerAttributeNumber;
use crate::c::PG_UINT16_MAX;

// ----------------------------------------------------------------
// Local stub types for not-yet-ported dependencies.
// ----------------------------------------------------------------

type TableScanDesc = *mut c_void;
type ScanDirection = c_int;

// ----------------------------------------------------------------
// Local stubs for unported helper functions / constants we call.
// ----------------------------------------------------------------

// pg_operator.h: well-known TID comparison operator OIDs.
const TIDLessOperator: Oid = 2799;
const TIDGreaterOperator: Oid = 2800;
const TIDLessEqOperator: Oid = 2801;
const TIDGreaterEqOperator: Oid = 2802;

unsafe fn get_leftop(_clause: *const Expr) -> *mut Node {
    unimplemented!() // TODO: nodes/nodeFuncs.h
}

unsafe fn get_rightop(_clause: *const Expr) -> *mut Node {
    unimplemented!() // TODO: nodes/nodeFuncs.h
}

unsafe fn ExecInitExpr(_node: *mut Expr, _parent: *mut PlanState) -> *mut ExprState {
    crate::executor::execExpr::ExecInitExpr(_node as _, _parent as _) as _
}

unsafe fn ExecEvalExprSwitchContext(
    _state: *mut ExprState,
    _econtext: *mut ExprContext,
    _isNull: *mut bool,
) -> Datum {
    crate::executor::executor::ExecEvalExprSwitchContext(_state as _, _econtext as _, _isNull as _) as _
}

// itemptr.h: increment/decrement an ItemPointer.  The result may not be a
// valid item pointer.
unsafe fn ItemPointerInc(_pointer: *mut ItemPointerData) {
    crate::storage::itemptr::ItemPointerInc(_pointer as _)
}

unsafe fn ItemPointerDec(_pointer: *mut ItemPointerData) {
    crate::storage::itemptr::ItemPointerDec(_pointer as _)
}

unsafe fn table_beginscan_tidrange(
    _rel: *mut c_void,
    _snapshot: *mut c_void,
    _mintid: *mut ItemPointerData,
    _maxtid: *mut ItemPointerData,
) -> TableScanDesc {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_rescan_tidrange(
    _sscan: TableScanDesc,
    _mintid: *mut ItemPointerData,
    _maxtid: *mut ItemPointerData,
) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_scan_getnextslot_tidrange(
    _sscan: TableScanDesc,
    _direction: ScanDirection,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_endscan(_scan: TableScanDesc) {
    crate::access::table::tableam::table_endscan(_scan as _)
}

unsafe fn table_slot_callbacks(_rel: *mut c_void) -> *const c_void {
    crate::access::table::tableam::table_slot_callbacks(_rel as _) as _
}

unsafe fn ExecScan(
    _node: *mut ScanState,
    _access_mtd: ExecScanAccessMtd,
    _recheck_mtd: ExecScanRecheckMtd,
) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/execScan.c
}

unsafe fn ExecScanReScan(_node: *mut ScanState) {
    crate::executor::execScan::ExecScanReScan(_node as _)
}

unsafe fn ExecClearTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    crate::executor::tuptable::ExecClearTuple(_slot as _) as _
}

unsafe fn ExecAssignExprContext(_estate: *mut EState, _ps: *mut PlanState) {
    unimplemented!() // TODO: executor/execUtils.c
}

unsafe fn ExecOpenScanRelation(
    _estate: *mut EState,
    _scanrelid: crate::c::Index,
    _eflags: c_int,
) -> *mut c_void {
    unimplemented!() // TODO: executor/execUtils.c
}

unsafe fn ExecInitScanTupleSlot(
    _estate: *mut EState,
    _scanstate: *mut ScanState,
    _tupdesc: *mut c_void,
    _tts_ops: *const c_void,
) {
    crate::executor::execTuples::ExecInitScanTupleSlot(_estate as _, _scanstate as _, _tupdesc as _, _tts_ops as _)
}

unsafe fn ExecInitResultTypeTL(_ps: *mut PlanState) {
    crate::executor::execTuples::ExecInitResultTypeTL(_ps as _)
}

unsafe fn ExecAssignScanProjectionInfo(_node: *mut ScanState) {
    crate::executor::execScan::ExecAssignScanProjectionInfo(_node as _)
}

unsafe fn ExecInitQual(_qual: *mut List, _parent: *mut PlanState) -> *mut ExprState {
    crate::executor::execExpr::ExecInitQual(_qual as _, _parent as _) as _
}

unsafe fn RelationGetDescr(_rel: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO: utils/rel.h
}

// ----------------------------------------------------------------
//
// It's sufficient to check varattno to identify the CTID variable, as any
// Var in the relation scan qual must be for our table.  (Even if it's a
// parameterized scan referencing some other table's CTID, the other table's
// Var would have become a Param by the time it gets here.)
// ----------------------------------------------------------------
unsafe fn IsCTIDVar(node: *mut Node) -> bool {
    !node.is_null()
        && IsA!(node, T_Var)
        && (*(node as *mut Var)).varattno == SelfItemPointerAttributeNumber
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum TidExprType {
    TIDEXPR_UPPER_BOUND,
    TIDEXPR_LOWER_BOUND,
}
use TidExprType::*;

/* Upper or lower range bound for scan */
#[repr(C)]
struct TidOpExpr {
    exprtype: TidExprType,        /* type of op; lower or upper */
    exprstate: *mut ExprState,    /* ExprState for a TID-yielding subexpr */
    inclusive: bool,              /* whether op is inclusive */
}

/*
 * For the given 'expr', build and return an appropriate TidOpExpr taking into
 * account the expr's operator and operand order.
 */
unsafe fn MakeTidOpExpr(expr: *mut OpExpr, tidstate: *mut TidRangeScanState) -> *mut TidOpExpr {
    let arg1: *mut Node = get_leftop(expr as *mut Expr);
    let arg2: *mut Node = get_rightop(expr as *mut Expr);
    let mut exprstate: *mut ExprState = ptr::null_mut();
    let mut invert: bool = false;
    let tidopexpr: *mut TidOpExpr;

    if IsCTIDVar(arg1) {
        exprstate = ExecInitExpr(arg2 as *mut Expr, &mut (*tidstate).ss.ps);
    } else if IsCTIDVar(arg2) {
        exprstate = ExecInitExpr(arg1 as *mut Expr, &mut (*tidstate).ss.ps);
        invert = true;
    } else {
        elog!(ERROR, "could not identify CTID variable");
    }

    tidopexpr = palloc(std::mem::size_of::<TidOpExpr>()) as *mut TidOpExpr;
    (*tidopexpr).inclusive = false; /* for now */

    if (*expr).opno == TIDLessEqOperator {
        (*tidopexpr).inclusive = true;
        /* fall through */
        (*tidopexpr).exprtype = if invert { TIDEXPR_LOWER_BOUND } else { TIDEXPR_UPPER_BOUND };
    } else if (*expr).opno == TIDLessOperator {
        (*tidopexpr).exprtype = if invert { TIDEXPR_LOWER_BOUND } else { TIDEXPR_UPPER_BOUND };
    } else if (*expr).opno == TIDGreaterEqOperator {
        (*tidopexpr).inclusive = true;
        /* fall through */
        (*tidopexpr).exprtype = if invert { TIDEXPR_UPPER_BOUND } else { TIDEXPR_LOWER_BOUND };
    } else if (*expr).opno == TIDGreaterOperator {
        (*tidopexpr).exprtype = if invert { TIDEXPR_UPPER_BOUND } else { TIDEXPR_LOWER_BOUND };
    } else {
        elog!(ERROR, "could not identify CTID operator");
    }

    (*tidopexpr).exprstate = exprstate;

    tidopexpr
}

/*
 * Extract the qual subexpressions that yield TIDs to search for,
 * and compile them into ExprStates if they're ordinary expressions.
 */
unsafe fn TidExprListCreate(tidrangestate: *mut TidRangeScanState) {
    let node: *mut TidRangeScan = (*tidrangestate).ss.ps.plan as *mut TidRangeScan;
    let mut tidexprs: *mut List = ptr::null_mut(); /* NIL */
    let mut l: *mut ListCell;

    foreach!(l, (*node).tidrangequals, {
        let opexpr: *mut OpExpr = lfirst(current_cell!(l)) as *mut OpExpr;
        let tidopexpr: *mut TidOpExpr;

        if !IsA!(opexpr, T_OpExpr) {
            elog!(ERROR, "could not identify CTID expression");
        }

        tidopexpr = MakeTidOpExpr(opexpr, tidrangestate);
        tidexprs = lappend(tidexprs, tidopexpr as *mut c_void);
    });

    (*tidrangestate).trss_tidexprs = tidexprs;
}

/* ----------------------------------------------------------------
 *		TidRangeEval
 *
 *		Compute and set node's block and offset range to scan by evaluating
 *		node->trss_tidexprs.  Returns false if we detect the range cannot
 *		contain any tuples.  Returns true if it's possible for the range to
 *		contain tuples.  We don't bother validating that trss_mintid is less
 *		than or equal to trss_maxtid, as the scan_set_tidrange() table AM
 *		function will handle that.
 * ----------------------------------------------------------------
 */
unsafe fn TidRangeEval(node: *mut TidRangeScanState) -> bool {
    let econtext: *mut ExprContext = (*node).ss.ps.ps_ExprContext;
    let mut lowerBound: ItemPointerData = std::mem::zeroed();
    let mut upperBound: ItemPointerData = std::mem::zeroed();
    let mut l: *mut ListCell;

    /*
     * Set the upper and lower bounds to the absolute limits of the range of
     * the ItemPointer type.  Below we'll try to narrow this range on either
     * side by looking at the TidOpExprs.
     */
    ItemPointerSet(&mut lowerBound, 0, 0);
    ItemPointerSet(&mut upperBound, InvalidBlockNumber, PG_UINT16_MAX);

    foreach!(l, (*node).trss_tidexprs, {
        let tidopexpr: *mut TidOpExpr = lfirst(current_cell!(l)) as *mut TidOpExpr;
        let itemptr: ItemPointer;
        let mut isNull: bool = false;

        /* Evaluate this bound. */
        itemptr = DatumGetPointer(ExecEvalExprSwitchContext(
            (*tidopexpr).exprstate,
            econtext,
            &mut isNull,
        )) as ItemPointer;

        /* If the bound is NULL, *nothing* matches the qual. */
        if isNull {
            return false;
        }

        if (*tidopexpr).exprtype == TIDEXPR_LOWER_BOUND {
            let mut lb: ItemPointerData = std::mem::zeroed();

            ItemPointerCopy(itemptr, &mut lb);

            /*
             * Normalize non-inclusive ranges to become inclusive.  The
             * resulting ItemPointer here may not be a valid item pointer.
             */
            if !(*tidopexpr).inclusive {
                ItemPointerInc(&mut lb);
            }

            /* Check if we can narrow the range using this qual */
            if ItemPointerCompare(&mut lb, &mut lowerBound) > 0 {
                ItemPointerCopy(&lb, &mut lowerBound);
            }
        } else if (*tidopexpr).exprtype == TIDEXPR_UPPER_BOUND {
            let mut ub: ItemPointerData = std::mem::zeroed();

            ItemPointerCopy(itemptr, &mut ub);

            /*
             * Normalize non-inclusive ranges to become inclusive.  The
             * resulting ItemPointer here may not be a valid item pointer.
             */
            if !(*tidopexpr).inclusive {
                ItemPointerDec(&mut ub);
            }

            /* Check if we can narrow the range using this qual */
            if ItemPointerCompare(&mut ub, &mut upperBound) < 0 {
                ItemPointerCopy(&ub, &mut upperBound);
            }
        }
    });

    ItemPointerCopy(&lowerBound, &mut (*node).trss_mintid);
    ItemPointerCopy(&upperBound, &mut (*node).trss_maxtid);

    true
}

/* ----------------------------------------------------------------
 *		TidRangeNext
 *
 *		Retrieve a tuple from the TidRangeScan node's currentRelation
 *		using the TIDs in the TidRangeScanState information.
 *
 * ----------------------------------------------------------------
 */
unsafe fn TidRangeNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut TidRangeScanState;
    let mut scandesc: TableScanDesc;
    let estate: *mut EState;
    let direction: ScanDirection;
    let slot: *mut TupleTableSlot;

    /*
     * extract necessary information from TID scan node
     */
    scandesc = (*node).ss.ss_currentScanDesc as TableScanDesc;
    estate = (*node).ss.ps.state;
    slot = (*node).ss.ss_ScanTupleSlot;
    direction = (*estate).es_direction as ScanDirection;

    if !(*node).trss_inScan {
        /* First time through, compute TID range to scan */
        if !TidRangeEval(node) {
            return ptr::null_mut();
        }

        if scandesc.is_null() {
            scandesc = table_beginscan_tidrange(
                (*node).ss.ss_currentRelation as *mut c_void,
                (*estate).es_snapshot as *mut c_void,
                &mut (*node).trss_mintid,
                &mut (*node).trss_maxtid,
            );
            (*node).ss.ss_currentScanDesc = scandesc as *mut _;
        } else {
            /* rescan with the updated TID range */
            table_rescan_tidrange(scandesc, &mut (*node).trss_mintid, &mut (*node).trss_maxtid);
        }

        (*node).trss_inScan = true;
    }

    /* Fetch the next tuple. */
    if !table_scan_getnextslot_tidrange(scandesc, direction, slot) {
        (*node).trss_inScan = false;
        ExecClearTuple(slot);
    }

    slot
}

/*
 * TidRangeRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn TidRangeRecheck(node: *mut ScanState, slot: *mut TupleTableSlot) -> bool {
    let node = node as *mut TidRangeScanState;

    if !TidRangeEval(node) {
        return false;
    }

    Assert!(ItemPointerIsValid(&(*slot).tts_tid));

    /* Recheck the ctid is still within range */
    if ItemPointerCompare(&mut (*slot).tts_tid, &mut (*node).trss_mintid) < 0
        || ItemPointerCompare(&mut (*slot).tts_tid, &mut (*node).trss_maxtid) > 0
    {
        return false;
    }

    true
}

/* ----------------------------------------------------------------
 *		ExecTidRangeScan(node)
 *
 *		Scans the relation using tids and returns the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for TID range scanning.
 * ----------------------------------------------------------------
 */
unsafe fn ExecTidRangeScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut TidRangeScanState = castNode!(TidRangeScanState, T_TidRangeScanState, pstate);

    ExecScan(&mut (*node).ss, Some(TidRangeNext), Some(TidRangeRecheck))
}

/* ----------------------------------------------------------------
 *		ExecReScanTidRangeScan(node)
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanTidRangeScan(node: *mut TidRangeScanState) {
    /* mark scan as not in progress, and tid range list as not computed yet */
    (*node).trss_inScan = false;

    /*
     * We must wait until TidRangeNext before calling table_rescan_tidrange.
     */
    ExecScanReScan(&mut (*node).ss);
}

/* ----------------------------------------------------------------
 *		ExecEndTidRangeScan
 *
 *		Releases any storage allocated through C routines.
 *		Returns nothing.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndTidRangeScan(node: *mut TidRangeScanState) {
    let scan: TableScanDesc = (*node).ss.ss_currentScanDesc as TableScanDesc;

    if !scan.is_null() {
        table_endscan(scan);
    }
}

/* ----------------------------------------------------------------
 *		ExecInitTidRangeScan
 *
 *		Initializes the tid range scan's state information, creates
 *		scan keys, and opens the scan relation.
 *
 *		Parameters:
 *		  node: TidRangeScan node produced by the planner.
 *		  estate: the execution state initialized in InitPlan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitTidRangeScan(
    node: *mut TidRangeScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut TidRangeScanState {
    let tidrangestate: *mut TidRangeScanState;
    let currentRelation: *mut c_void;

    /*
     * create state structure
     */
    tidrangestate = makeNode!(TidRangeScanState, T_TidRangeScanState);
    (*tidrangestate).ss.ps.plan = node as *mut Plan;
    (*tidrangestate).ss.ps.state = estate;
    (*tidrangestate).ss.ps.ExecProcNode = Some(ExecTidRangeScan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*tidrangestate).ss.ps);

    /*
     * mark scan as not in progress, and TID range as not computed yet
     */
    (*tidrangestate).trss_inScan = false;

    /*
     * open the scan relation
     */
    currentRelation = ExecOpenScanRelation(estate, (*node).scan.scanrelid, eflags);

    (*tidrangestate).ss.ss_currentRelation = currentRelation as *mut _;
    (*tidrangestate).ss.ss_currentScanDesc = ptr::null_mut(); /* no table scan here */

    /*
     * get the scan type from the relation descriptor.
     */
    ExecInitScanTupleSlot(
        estate,
        &mut (*tidrangestate).ss,
        RelationGetDescr(currentRelation),
        table_slot_callbacks(currentRelation),
    );

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*tidrangestate).ss.ps);
    ExecAssignScanProjectionInfo(&mut (*tidrangestate).ss);

    /*
     * initialize child expressions
     */
    (*tidrangestate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual, tidrangestate as *mut PlanState);

    TidExprListCreate(tidrangestate);

    /*
     * all done.
     */
    tidrangestate
}
