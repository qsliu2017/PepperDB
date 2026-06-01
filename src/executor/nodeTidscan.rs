//! Routines to support direct tid scans of relations.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translated 1:1 from:
//!   postgres/src/backend/executor/nodeTidscan.c
//!   postgres/src/include/executor/nodeTidscan.h
//!
//! INTERFACE ROUTINES
//!		ExecTidScan			scans a relation using tids
//!		ExecInitTidScan		creates and initializes state info.
//!		ExecReScanTidScan	rescans the tid relation.
//!		ExecEndTidScan		releases all storage.

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::ptr;

use crate::{castNode, current_cell, foreach, makeNode, Assert, IsA};

use crate::executor::executor::{ExecScanAccessMtd, ExecScanRecheckMtd};

use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, PlanState, ScanState, TidScanState, TupleTableSlot,
};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{lappend, lfirst, linitial, list_length, lsecond, List, ListCell};
use crate::nodes::plannodes::{Plan, TidScan};
use crate::nodes::primnodes::{CurrentOfExpr, Expr, ScalarArrayOpExpr, Var};

use crate::access::sdir::{ScanDirection, ScanDirectionIsBackward};
use crate::access::sysattr::SelfItemPointerAttributeNumber;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::{
    ItemPointer, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
};
use crate::storage::off::OffsetNumber;

// ----------------------------------------------------------------
// Local stub types for not-yet-ported dependencies.
// ----------------------------------------------------------------

type TableScanDesc = *mut c_void;
type Snapshot = *mut c_void;
type Relation = *mut c_void;
type ArrayType = *mut c_void;

// pg_type.h
const TIDOID: Oid = 27;

// ----------------------------------------------------------------
// Local stubs for unported helper functions we call.
// ----------------------------------------------------------------

unsafe fn is_opclause(_clause: *const c_void) -> bool {
    unimplemented!() // TODO: nodes/nodeFuncs.h
}

unsafe fn get_leftop(_clause: *const Expr) -> *mut Node {
    unimplemented!() // TODO: nodes/nodeFuncs.h
}

unsafe fn get_rightop(_clause: *const Expr) -> *mut Node {
    unimplemented!() // TODO: nodes/nodeFuncs.h
}

unsafe fn ExecInitExpr(_node: *mut Expr, _parent: *mut PlanState) -> *mut ExprState {
    unimplemented!() // TODO: executor/execExpr.c
}

unsafe fn ExecEvalExprSwitchContext(
    _state: *mut ExprState,
    _econtext: *mut ExprContext,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: executor/executor.h
}

unsafe fn execCurrentOf(
    _cexpr: *mut CurrentOfExpr,
    _econtext: *mut ExprContext,
    _table_oid: Oid,
    _current_tid: *mut ItemPointerData,
) -> bool {
    unimplemented!() // TODO: executor/execCurrent.c
}

unsafe fn DatumGetArrayTypeP(_x: Datum) -> ArrayType {
    unimplemented!() // TODO: utils/array.h
}

unsafe fn deconstruct_array_builtin(
    _array: ArrayType,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!() // TODO: utils/arrayfuncs.c
}

unsafe fn table_beginscan_tid(_rel: Relation, _snapshot: Snapshot) -> TableScanDesc {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_endscan(_scan: TableScanDesc) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_rescan(_scan: TableScanDesc, _key: *mut c_void) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_tuple_tid_valid(_scan: TableScanDesc, _tid: ItemPointer) -> bool {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_tuple_get_latest_tid(_scan: TableScanDesc, _tid: ItemPointer) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_tuple_fetch_row_version(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_slot_callbacks(_rel: Relation) -> *const c_void {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn RelationGetRelid(_rel: Relation) -> Oid {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn RelationGetDescr(_rel: Relation) -> *mut c_void {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn ExecScan(
    _node: *mut ScanState,
    _access_mtd: ExecScanAccessMtd,
    _recheck_mtd: ExecScanRecheckMtd,
) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/execScan.c
}

unsafe fn ExecScanReScan(_node: *mut ScanState) {
    unimplemented!() // TODO: executor/execScan.c
}

unsafe fn ExecClearTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/tuptable.h
}

unsafe fn ExecAssignExprContext(_estate: *mut EState, _ps: *mut PlanState) {
    unimplemented!() // TODO: executor/execUtils.c
}

unsafe fn ExecOpenScanRelation(
    _estate: *mut EState,
    _scanrelid: crate::c::Index,
    _eflags: c_int,
) -> Relation {
    unimplemented!() // TODO: executor/execUtils.c
}

unsafe fn ExecInitScanTupleSlot(
    _estate: *mut EState,
    _scanstate: *mut ScanState,
    _tupdesc: *mut c_void,
    _tts_ops: *const c_void,
) {
    unimplemented!() // TODO: executor/execTuples.c
}

unsafe fn ExecInitResultTypeTL(_ps: *mut PlanState) {
    unimplemented!() // TODO: executor/execTuples.c
}

unsafe fn ExecAssignScanProjectionInfo(_node: *mut ScanState) {
    unimplemented!() // TODO: executor/execScan.c
}

unsafe fn ExecInitQual(_qual: *mut List, _parent: *mut PlanState) -> *mut ExprState {
    unimplemented!() // TODO: executor/execExpr.c
}

// qunique.h: de-duplicate a pre-sorted array in place, returning the new length.
unsafe fn qunique(
    _array: *mut c_void,
    _elements: usize,
    _width: usize,
    _compare: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
) -> usize {
    unimplemented!() // TODO: lib/qunique.h
}

unsafe fn bsearch_itemptr(
    _key: *const c_void,
    _base: *const c_void,
    _nmemb: usize,
    _size: usize,
    _compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
) -> *mut c_void {
    unimplemented!() // TODO: libc bsearch
}

unsafe fn qsort_itemptr(
    _base: *mut c_void,
    _nmemb: usize,
    _size: usize,
    _compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
) {
    unimplemented!() // TODO: libc qsort
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

/* one element in tss_tidexprs */
#[repr(C)]
struct TidExpr {
    exprstate: *mut ExprState,  /* ExprState for a TID-yielding subexpr */
    isarray: bool,              /* if true, it yields tid[] not just tid */
    cexpr: *mut CurrentOfExpr,  /* alternatively, we can have CURRENT OF */
}

/*
 * Extract the qual subexpressions that yield TIDs to search for,
 * and compile them into ExprStates if they're ordinary expressions.
 *
 * CURRENT OF is a special case that we can't compile usefully;
 * just drop it into the TidExpr list as-is.
 */
unsafe fn TidExprListCreate(tidstate: *mut TidScanState) {
    let node: *mut TidScan = (*tidstate).ss.ps.plan as *mut TidScan;
    let mut l: *mut ListCell;

    (*tidstate).tss_tidexprs = ptr::null_mut(); /* NIL */
    (*tidstate).tss_isCurrentOf = false;

    foreach!(l, (*node).tidquals, {
        let expr: *mut Expr = lfirst(current_cell!(l)) as *mut Expr;
        let tidexpr: *mut TidExpr = palloc0(std::mem::size_of::<TidExpr>()) as *mut TidExpr;

        if is_opclause(expr as *const c_void) {
            let arg1: *mut Node;
            let arg2: *mut Node;

            arg1 = get_leftop(expr);
            arg2 = get_rightop(expr);
            if IsCTIDVar(arg1) {
                (*tidexpr).exprstate = ExecInitExpr(arg2 as *mut Expr, &mut (*tidstate).ss.ps);
            } else if IsCTIDVar(arg2) {
                (*tidexpr).exprstate = ExecInitExpr(arg1 as *mut Expr, &mut (*tidstate).ss.ps);
            } else {
                elog!(ERROR, "could not identify CTID variable");
            }
            (*tidexpr).isarray = false;
        } else if !expr.is_null() && IsA!(expr, T_ScalarArrayOpExpr) {
            let saex: *mut ScalarArrayOpExpr = expr as *mut ScalarArrayOpExpr;

            Assert!(IsCTIDVar(linitial((*saex).args) as *mut Node));
            (*tidexpr).exprstate =
                ExecInitExpr(lsecond((*saex).args) as *mut Expr, &mut (*tidstate).ss.ps);
            (*tidexpr).isarray = true;
        } else if !expr.is_null() && IsA!(expr, T_CurrentOfExpr) {
            let cexpr: *mut CurrentOfExpr = expr as *mut CurrentOfExpr;

            (*tidexpr).cexpr = cexpr;
            (*tidstate).tss_isCurrentOf = true;
        } else {
            elog!(ERROR, "could not identify CTID expression");
        }

        (*tidstate).tss_tidexprs = lappend((*tidstate).tss_tidexprs, tidexpr as *mut c_void);
    });

    /* CurrentOfExpr could never appear OR'd with something else */
    Assert!(list_length((*tidstate).tss_tidexprs) == 1 || !(*tidstate).tss_isCurrentOf);
}

/*
 * Compute the list of TIDs to be visited, by evaluating the expressions
 * for them.
 *
 * (The result is actually an array, not a list.)
 */
unsafe fn TidListEval(tidstate: *mut TidScanState) {
    let econtext: *mut ExprContext = (*tidstate).ss.ps.ps_ExprContext;
    let scan: TableScanDesc;
    let mut tidList: *mut ItemPointerData;
    let mut numAllocTids: c_int;
    let mut numTids: c_int;
    let mut l: *mut ListCell;

    /*
     * Start scan on-demand - initializing a scan isn't free (e.g. heap stats
     * the size of the table), so it makes sense to delay that until needed -
     * the node might never get executed.
     */
    if (*tidstate).ss.ss_currentScanDesc.is_null() {
        (*tidstate).ss.ss_currentScanDesc = table_beginscan_tid(
            (*tidstate).ss.ss_currentRelation as Relation,
            (*(*tidstate).ss.ps.state).es_snapshot as Snapshot,
        ) as *mut _;
    }
    scan = (*tidstate).ss.ss_currentScanDesc as TableScanDesc;

    /*
     * We initialize the array with enough slots for the case that all quals
     * are simple OpExprs or CurrentOfExprs.  If there are any
     * ScalarArrayOpExprs, we may have to enlarge the array.
     */
    numAllocTids = list_length((*tidstate).tss_tidexprs);
    tidList = palloc(numAllocTids as usize * std::mem::size_of::<ItemPointerData>())
        as *mut ItemPointerData;
    numTids = 0;

    foreach!(l, (*tidstate).tss_tidexprs, {
        let tidexpr: *mut TidExpr = lfirst(current_cell!(l)) as *mut TidExpr;
        let itemptr: ItemPointer;
        let mut isNull: bool = false;

        if !(*tidexpr).exprstate.is_null() && !(*tidexpr).isarray {
            itemptr = DatumGetPointer(ExecEvalExprSwitchContext(
                (*tidexpr).exprstate,
                econtext,
                &mut isNull,
            )) as ItemPointer;
            if isNull {
                continue;
            }

            /*
             * We silently discard any TIDs that the AM considers invalid
             * (E.g. for heap, they could be out of range at the time of scan
             * start.  Since we hold at least AccessShareLock on the table, it
             * won't be possible for someone to truncate away the blocks we
             * intend to visit.).
             */
            if !table_tuple_tid_valid(scan, itemptr) {
                continue;
            }

            if numTids >= numAllocTids {
                numAllocTids *= 2;
                tidList = repalloc(
                    tidList as *mut c_void,
                    numAllocTids as usize * std::mem::size_of::<ItemPointerData>(),
                ) as *mut ItemPointerData;
            }
            *tidList.offset(numTids as isize) = *itemptr;
            numTids += 1;
        } else if !(*tidexpr).exprstate.is_null() && (*tidexpr).isarray {
            let arraydatum: Datum;
            let itemarray: ArrayType;
            let mut ipdatums: *mut Datum = ptr::null_mut();
            let mut ipnulls: *mut bool = ptr::null_mut();
            let mut ndatums: c_int = 0;
            let mut i: c_int;

            arraydatum = ExecEvalExprSwitchContext((*tidexpr).exprstate, econtext, &mut isNull);
            if isNull {
                continue;
            }
            itemarray = DatumGetArrayTypeP(arraydatum);
            deconstruct_array_builtin(
                itemarray,
                TIDOID,
                &mut ipdatums,
                &mut ipnulls,
                &mut ndatums,
            );
            if numTids + ndatums > numAllocTids {
                numAllocTids = numTids + ndatums;
                tidList = repalloc(
                    tidList as *mut c_void,
                    numAllocTids as usize * std::mem::size_of::<ItemPointerData>(),
                ) as *mut ItemPointerData;
            }
            i = 0;
            while i < ndatums {
                if *ipnulls.offset(i as isize) {
                    i += 1;
                    continue;
                }

                let itemptr2: ItemPointer =
                    DatumGetPointer(*ipdatums.offset(i as isize)) as ItemPointer;

                if !table_tuple_tid_valid(scan, itemptr2) {
                    i += 1;
                    continue;
                }

                *tidList.offset(numTids as isize) = *itemptr2;
                numTids += 1;
                i += 1;
            }
            pfree(ipdatums as *mut c_void);
            pfree(ipnulls as *mut c_void);
        } else {
            let mut cursor_tid: ItemPointerData = std::mem::zeroed();

            Assert!(!(*tidexpr).cexpr.is_null());
            if execCurrentOf(
                (*tidexpr).cexpr,
                econtext,
                RelationGetRelid((*tidstate).ss.ss_currentRelation as Relation),
                &mut cursor_tid,
            ) {
                if numTids >= numAllocTids {
                    numAllocTids *= 2;
                    tidList = repalloc(
                        tidList as *mut c_void,
                        numAllocTids as usize * std::mem::size_of::<ItemPointerData>(),
                    ) as *mut ItemPointerData;
                }
                *tidList.offset(numTids as isize) = cursor_tid;
                numTids += 1;
            }
        }
    });

    /*
     * Sort the array of TIDs into order, and eliminate duplicates.
     * Eliminating duplicates is necessary since we want OR semantics across
     * the list.  Sorting makes it easier to detect duplicates, and as a bonus
     * ensures that we will visit the heap in the most efficient way.
     */
    if numTids > 1 {
        /* CurrentOfExpr could never appear OR'd with something else */
        Assert!(!(*tidstate).tss_isCurrentOf);

        qsort_itemptr(
            tidList as *mut c_void,
            numTids as usize,
            std::mem::size_of::<ItemPointerData>(),
            itemptr_comparator,
        );
        numTids = qunique(
            tidList as *mut c_void,
            numTids as usize,
            std::mem::size_of::<ItemPointerData>(),
            itemptr_comparator,
        ) as c_int;
    }

    (*tidstate).tss_TidList = tidList;
    (*tidstate).tss_NumTids = numTids;
    (*tidstate).tss_TidPtr = -1;
}

/*
 * qsort comparator for ItemPointerData items
 */
unsafe extern "C" fn itemptr_comparator(a: *const c_void, b: *const c_void) -> c_int {
    let ipa: *const ItemPointerData = a as *const ItemPointerData;
    let ipb: *const ItemPointerData = b as *const ItemPointerData;
    let ba: BlockNumber = ItemPointerGetBlockNumber(ipa);
    let bb: BlockNumber = ItemPointerGetBlockNumber(ipb);
    let oa: OffsetNumber = ItemPointerGetOffsetNumber(ipa);
    let ob: OffsetNumber = ItemPointerGetOffsetNumber(ipb);

    if ba < bb {
        return -1;
    }
    if ba > bb {
        return 1;
    }
    if oa < ob {
        return -1;
    }
    if oa > ob {
        return 1;
    }
    0
}

/* ----------------------------------------------------------------
 *		TidNext
 *
 *		Retrieve a tuple from the TidScan node's currentRelation
 *		using the tids in the TidScanState information.
 *
 * ----------------------------------------------------------------
 */
unsafe fn TidNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut TidScanState;
    let estate: *mut EState;
    let direction: ScanDirection;
    let snapshot: Snapshot;
    let scan: TableScanDesc;
    let heapRelation: Relation;
    let slot: *mut TupleTableSlot;
    let tidList: *mut ItemPointerData;
    let numTids: c_int;
    let bBackward: bool;

    /*
     * extract necessary information from tid scan node
     */
    estate = (*node).ss.ps.state;
    direction = (*estate).es_direction as ScanDirection;
    snapshot = (*estate).es_snapshot as Snapshot;
    heapRelation = (*node).ss.ss_currentRelation as Relation;
    slot = (*node).ss.ss_ScanTupleSlot;

    /*
     * First time through, compute the list of TIDs to be visited
     */
    if (*node).tss_TidList.is_null() {
        TidListEval(node);
    }

    scan = (*node).ss.ss_currentScanDesc as TableScanDesc;
    tidList = (*node).tss_TidList;
    numTids = (*node).tss_NumTids;

    /*
     * Initialize or advance scan position, depending on direction.
     */
    bBackward = ScanDirectionIsBackward(direction);
    if bBackward {
        if (*node).tss_TidPtr < 0 {
            /* initialize for backward scan */
            (*node).tss_TidPtr = numTids - 1;
        } else {
            (*node).tss_TidPtr -= 1;
        }
    } else {
        if (*node).tss_TidPtr < 0 {
            /* initialize for forward scan */
            (*node).tss_TidPtr = 0;
        } else {
            (*node).tss_TidPtr += 1;
        }
    }

    while (*node).tss_TidPtr >= 0 && (*node).tss_TidPtr < numTids {
        let mut tid: ItemPointerData = *tidList.offset((*node).tss_TidPtr as isize);

        /*
         * For WHERE CURRENT OF, the tuple retrieved from the cursor might
         * since have been updated; if so, we should fetch the version that is
         * current according to our snapshot.
         */
        if (*node).tss_isCurrentOf {
            table_tuple_get_latest_tid(scan, &mut tid);
        }

        if table_tuple_fetch_row_version(heapRelation, &mut tid, snapshot, slot) {
            return slot;
        }

        /* Bad TID or failed snapshot qual; try next */
        if bBackward {
            (*node).tss_TidPtr -= 1;
        } else {
            (*node).tss_TidPtr += 1;
        }

        CHECK_FOR_INTERRUPTS();
    }

    /*
     * if we get here it means the tid scan failed so we are at the end of the
     * scan..
     */
    ExecClearTuple(slot)
}

/*
 * TidRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn TidRecheck(node: *mut ScanState, slot: *mut TupleTableSlot) -> bool {
    let node = node as *mut TidScanState;
    let match_: ItemPointer;

    /* WHERE CURRENT OF always intends to resolve to the latest tuple */
    if (*node).tss_isCurrentOf {
        return true;
    }

    if (*node).tss_TidList.is_null() {
        TidListEval(node);
    }

    /*
     * Binary search the TidList to see if this ctid is mentioned and return
     * true if it is.
     */
    match_ = bsearch_itemptr(
        &(*slot).tts_tid as *const ItemPointerData as *const c_void,
        (*node).tss_TidList as *const c_void,
        (*node).tss_NumTids as usize,
        std::mem::size_of::<ItemPointerData>(),
        itemptr_comparator,
    ) as ItemPointer;
    !match_.is_null()
}

/* ----------------------------------------------------------------
 *		ExecTidScan(node)
 *
 *		Scans the relation using tids and returns
 *		   the next qualifying tuple in the direction specified.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 *		  -- tss_TidPtr is -1.
 * ----------------------------------------------------------------
 */
unsafe fn ExecTidScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut TidScanState = castNode!(TidScanState, T_TidScanState, pstate);

    ExecScan(&mut (*node).ss, Some(TidNext), Some(TidRecheck))
}

/* ----------------------------------------------------------------
 *		ExecReScanTidScan(node)
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanTidScan(node: *mut TidScanState) {
    if !(*node).tss_TidList.is_null() {
        pfree((*node).tss_TidList as *mut c_void);
    }
    (*node).tss_TidList = ptr::null_mut();
    (*node).tss_NumTids = 0;
    (*node).tss_TidPtr = -1;

    /* not really necessary, but seems good form */
    if !(*node).ss.ss_currentScanDesc.is_null() {
        table_rescan((*node).ss.ss_currentScanDesc as TableScanDesc, ptr::null_mut());
    }

    ExecScanReScan(&mut (*node).ss);
}

/* ----------------------------------------------------------------
 *		ExecEndTidScan
 *
 *		Releases any storage allocated through C routines.
 *		Returns nothing.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndTidScan(node: *mut TidScanState) {
    if !(*node).ss.ss_currentScanDesc.is_null() {
        table_endscan((*node).ss.ss_currentScanDesc as TableScanDesc);
    }
}

/* ----------------------------------------------------------------
 *		ExecInitTidScan
 *
 *		Initializes the tid scan's state information, creates
 *		scan keys, and opens the base and tid relations.
 *
 *		Parameters:
 *		  node: TidScan node produced by the planner.
 *		  estate: the execution state initialized in InitPlan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitTidScan(
    node: *mut TidScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut TidScanState {
    let tidstate: *mut TidScanState;
    let currentRelation: Relation;

    /*
     * create state structure
     */
    tidstate = makeNode!(TidScanState, T_TidScanState);
    (*tidstate).ss.ps.plan = node as *mut Plan;
    (*tidstate).ss.ps.state = estate;
    (*tidstate).ss.ps.ExecProcNode = Some(ExecTidScan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*tidstate).ss.ps);

    /*
     * mark tid list as not computed yet
     */
    (*tidstate).tss_TidList = ptr::null_mut();
    (*tidstate).tss_NumTids = 0;
    (*tidstate).tss_TidPtr = -1;

    /*
     * open the scan relation
     */
    currentRelation = ExecOpenScanRelation(estate, (*node).scan.scanrelid, eflags);

    (*tidstate).ss.ss_currentRelation = currentRelation as *mut _;
    (*tidstate).ss.ss_currentScanDesc = ptr::null_mut(); /* no heap scan here */

    /*
     * get the scan type from the relation descriptor.
     */
    ExecInitScanTupleSlot(
        estate,
        &mut (*tidstate).ss,
        RelationGetDescr(currentRelation),
        table_slot_callbacks(currentRelation),
    );

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*tidstate).ss.ps);
    ExecAssignScanProjectionInfo(&mut (*tidstate).ss);

    /*
     * initialize child expressions
     */
    (*tidstate).ss.ps.qual = ExecInitQual((*node).scan.plan.qual, tidstate as *mut PlanState);

    TidExprListCreate(tidstate);

    /*
     * all done.
     */
    tidstate
}
