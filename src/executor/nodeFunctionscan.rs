//! nodeFunctionscan.c
//!   Support routines for scanning RangeFunctions (functions in rangetable).
//!
//! postgres source: src/backend/executor/nodeFunctionscan.c
//! companion header: src/include/executor/nodeFunctionscan.h
//!
//! INTERFACE ROUTINES
//!     ExecFunctionScan        scans a function.
//!     ExecFunctionNext        retrieve next tuple in sequential order.
//!     ExecInitFunctionScan    creates and initializes a functionscan node.
//!     ExecEndFunctionScan     releases any storage allocated.
//!     ExecReScanFunctionScan  rescans the function

use crate::prelude::*;
use crate::access::attnum::AttrNumber;

use std::ffi::{c_int, c_void};

use crate::access::sdir::{ScanDirection, ScanDirectionIsForward};
use crate::access::common::tupdesc::{
    BuildDescFromLists, CreateTemplateTupleDesc, CreateTupleDescCopy, TupleDesc,
    TupleDescCopyEntry, TupleDescInitEntry, TupleDescInitEntryCollation,
};
use crate::catalog::pg_type_d::{INT8OID, RECORDOID};
use crate::nodes::bitmapset::{bms_overlap, Bitmapset};
use crate::nodes::execnodes::{
    EState, ExprContext, PlanState, ScanState, SetExprState, Tuplestorestate, TupleTableSlot,
    FunctionScanState,
};
use crate::nodes::primnodes::Expr;
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::parsenodes::RangeTblFunction;
use crate::nodes::pg_list::{lfirst, list_length, List, ListCell};
use crate::nodes::plannodes::{FunctionScan, Plan};

use crate::executor::executor::{
    ExecScanAccessMtd, ExecScanRecheckMtd, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::executor::{
    ExecInitExtraTupleSlot, ExecInitQual, ExecInitResultTypeTL, ExecInitScanTupleSlot,
    ExecInitTableFunctionResult, ExecMakeTableFunctionResult,
};
use crate::executor::execScan::{
    ExecAssignScanProjectionInfo, ExecScan, ExecScanReScan,
};
use crate::executor::execTuples::{ExecStoreVirtualTuple, TTSOpsMinimalTuple};
use crate::executor::execUtils::ExecAssignExprContext;
use crate::executor::tuptable::{slot_getallattrs, ExecClearTuple, TupIsNull};

use crate::utils::memutils::ALLOCSET_DEFAULT_SIZES;

use crate::{castNode, foreach, current_cell, makeNode, Assert, AllocSetContextCreate};

/* outerPlan / innerPlan accessors for FunctionScan plan node */
macro_rules! outerPlan {
    ($node:expr) => {
        (*($node as *mut FunctionScan)).scan.plan.lefttree
    };
}
macro_rules! innerPlan {
    ($node:expr) => {
        (*($node as *mut FunctionScan)).scan.plan.righttree
    };
}

/*
 * Runtime data for each function being scanned.
 */
#[repr(C)]
struct FunctionScanPerFuncState {
    setexpr: *mut SetExprState, /* state of the expression being evaluated */
    tupdesc: TupleDesc,         /* desc of the function result type */
    colcount: c_int,            /* expected number of result columns */
    tstore: *mut Tuplestorestate, /* holds the function result set */
    rowcount: int64,            /* # of rows in result set, -1 if not known */
    func_slot: *mut TupleTableSlot, /* function result slot (or NULL) */
}

/* ----------------------------------------------------------------
 *                      Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *      FunctionNext
 *
 *      This is a workhorse for ExecFunctionScan
 * ----------------------------------------------------------------
 */
unsafe fn FunctionNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut FunctionScanState;

    let estate: *mut EState;
    let direction: ScanDirection;
    let scanslot: *mut TupleTableSlot;
    let mut alldone: bool;
    let oldpos: int64;
    let mut funcno: c_int;
    let mut att: c_int;

    let funcstates = (*node).funcstates as *mut FunctionScanPerFuncState;

    /*
     * get information from the estate and scan state
     */
    estate = (*node).ss.ps.state;
    direction = (*estate).es_direction;
    scanslot = (*node).ss.ss_ScanTupleSlot;

    if (*node).simple {
        /*
         * Fast path for the trivial case: the function return type and scan
         * result type are the same, so we fetch the function result straight
         * into the scan result slot. No need to update ordinality or
         * rowcounts either.
         */
        let mut tstore = (*funcstates.offset(0)).tstore;

        /*
         * If first time through, read all tuples from function and put them
         * in a tuplestore. Subsequent calls just fetch tuples from
         * tuplestore.
         */
        if tstore.is_null() {
            tstore = ExecMakeTableFunctionResult(
                (*funcstates.offset(0)).setexpr,
                (*node).ss.ps.ps_ExprContext,
                (*node).argcontext as *mut _,
                (*funcstates.offset(0)).tupdesc,
                ((*node).eflags & EXEC_FLAG_BACKWARD) != 0,
            );
            (*funcstates.offset(0)).tstore = tstore;

            /*
             * paranoia - cope if the function, which may have constructed the
             * tuplestore itself, didn't leave it pointing at the start. This
             * call is fast, so the overhead shouldn't be an issue.
             */
            tuplestore_rescan(tstore);
        }

        /*
         * Get the next tuple from tuplestore.
         */
        let _ = tuplestore_gettupleslot(
            tstore,
            ScanDirectionIsForward(direction),
            false,
            scanslot,
        );
        return scanslot;
    }

    /*
     * Increment or decrement ordinal counter before checking for end-of-data,
     * so that we can move off either end of the result by 1 (and no more than
     * 1) without losing correct count.  See PortalRunSelect for why we can
     * assume that we won't be called repeatedly in the end-of-data state.
     */
    oldpos = (*node).ordinal;
    if ScanDirectionIsForward(direction) {
        (*node).ordinal += 1;
    } else {
        (*node).ordinal -= 1;
    }

    /*
     * Main loop over functions.
     *
     * We fetch the function results into func_slots (which match the function
     * return types), and then copy the values to scanslot (which matches the
     * scan result type), setting the ordinal column (if any) as well.
     */
    ExecClearTuple(scanslot);
    att = 0;
    alldone = true;
    funcno = 0;
    while funcno < (*node).nfuncs {
        let fs = funcstates.offset(funcno as isize);
        let mut i: c_int;

        /*
         * If first time through, read all tuples from function and put them
         * in a tuplestore. Subsequent calls just fetch tuples from
         * tuplestore.
         */
        if (*fs).tstore.is_null() {
            (*fs).tstore = ExecMakeTableFunctionResult(
                (*fs).setexpr,
                (*node).ss.ps.ps_ExprContext,
                (*node).argcontext as *mut _,
                (*fs).tupdesc,
                ((*node).eflags & EXEC_FLAG_BACKWARD) != 0,
            );

            /*
             * paranoia - cope if the function, which may have constructed the
             * tuplestore itself, didn't leave it pointing at the start. This
             * call is fast, so the overhead shouldn't be an issue.
             */
            tuplestore_rescan((*fs).tstore);
        }

        /*
         * Get the next tuple from tuplestore.
         *
         * If we have a rowcount for the function, and we know the previous
         * read position was out of bounds, don't try the read. This allows
         * backward scan to work when there are mixed row counts present.
         */
        if (*fs).rowcount != -1 && (*fs).rowcount < oldpos {
            ExecClearTuple((*fs).func_slot);
        } else {
            let _ = tuplestore_gettupleslot(
                (*fs).tstore,
                ScanDirectionIsForward(direction),
                false,
                (*fs).func_slot,
            );
        }

        if TupIsNull((*fs).func_slot) {
            /*
             * If we ran out of data for this function in the forward
             * direction then we now know how many rows it returned. We need
             * to know this in order to handle backwards scans. The row count
             * we store is actually 1+ the actual number, because we have to
             * position the tuplestore 1 off its end sometimes.
             */
            if ScanDirectionIsForward(direction) && (*fs).rowcount == -1 {
                (*fs).rowcount = (*node).ordinal;
            }

            /*
             * populate the result cols with nulls
             */
            i = 0;
            while i < (*fs).colcount {
                *(*scanslot).tts_values.offset(att as isize) = 0 as Datum;
                *(*scanslot).tts_isnull.offset(att as isize) = true;
                att += 1;
                i += 1;
            }
        } else {
            /*
             * we have a result, so just copy it to the result cols.
             */
            slot_getallattrs((*fs).func_slot);

            i = 0;
            while i < (*fs).colcount {
                *(*scanslot).tts_values.offset(att as isize) =
                    *(*(*fs).func_slot).tts_values.offset(i as isize);
                *(*scanslot).tts_isnull.offset(att as isize) =
                    *(*(*fs).func_slot).tts_isnull.offset(i as isize);
                att += 1;
                i += 1;
            }

            /*
             * We're not done until every function result is exhausted; we pad
             * the shorter results with nulls until then.
             */
            alldone = false;
        }

        funcno += 1;
    }

    /*
     * ordinal col is always last, per spec.
     */
    if (*node).ordinality {
        *(*scanslot).tts_values.offset(att as isize) = Int64GetDatumFast((*node).ordinal);
        *(*scanslot).tts_isnull.offset(att as isize) = false;
    }

    /*
     * If alldone, we just return the previously-cleared scanslot.  Otherwise,
     * finish creating the virtual tuple.
     */
    if !alldone {
        ExecStoreVirtualTuple(scanslot);
    }

    scanslot
}

/*
 * FunctionRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn FunctionRecheck(_node: *mut ScanState, _slot: *mut TupleTableSlot) -> bool {
    /* nothing to check */
    true
}

/* ----------------------------------------------------------------
 *      ExecFunctionScan(node)
 *
 *      Scans the function sequentially and returns the next qualifying
 *      tuple.
 *      We call the ExecScan() routine and pass it the appropriate
 *      access method functions.
 * ----------------------------------------------------------------
 */
unsafe fn ExecFunctionScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut FunctionScanState = castNode!(FunctionScanState, T_FunctionScanState, pstate);

    ExecScan(
        &raw mut (*node).ss,
        Some(FunctionNext),
        Some(FunctionRecheck),
    )
}

/* ----------------------------------------------------------------
 *      ExecInitFunctionScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitFunctionScan(
    node: *mut FunctionScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut FunctionScanState {
    let scanstate: *mut FunctionScanState;
    let nfuncs: c_int = list_length((*node).functions);
    let scan_tupdesc: TupleDesc;
    let mut i: c_int;
    let mut natts: c_int;
    let lc: *mut crate::nodes::pg_list::ListCell;

    /* check for unsupported flags */
    Assert!((eflags & EXEC_FLAG_MARK) == 0);

    /*
     * FunctionScan should not have any children.
     */
    Assert!(outerPlan!(node).is_null());
    Assert!(innerPlan!(node).is_null());

    /*
     * create new ScanState for node
     */
    scanstate = makeNode!(FunctionScanState, T_FunctionScanState);
    (*scanstate).ss.ps.plan = node as *mut Plan;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecFunctionScan);
    (*scanstate).eflags = eflags;

    /*
     * are we adding an ordinality column?
     */
    (*scanstate).ordinality = (*node).funcordinality;

    (*scanstate).nfuncs = nfuncs;
    if nfuncs == 1 && !(*node).funcordinality {
        (*scanstate).simple = true;
    } else {
        (*scanstate).simple = false;
    }

    /*
     * Ordinal 0 represents the "before the first row" position.
     *
     * We need to track ordinal position even when not adding an ordinality
     * column to the result, in order to handle backwards scanning properly
     * with multiple functions with different result sizes. (We can't position
     * any individual function's tuplestore any more than 1 place beyond its
     * end, so when scanning backwards, we need to know when to start
     * including the function in the scan again.)
     */
    (*scanstate).ordinal = 0;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &raw mut (*scanstate).ss.ps);

    let funcstates = palloc(
        nfuncs as Size * std::mem::size_of::<FunctionScanPerFuncState>(),
    ) as *mut FunctionScanPerFuncState;
    (*scanstate).funcstates =
        funcstates as *mut crate::nodes::execnodes::FunctionScanPerFuncState;

    natts = 0;
    i = 0;
    foreach!(lc, (*node).functions, {
        let rtfunc = lfirst(current_cell!(lc)) as *mut RangeTblFunction;
        let funcexpr = (*rtfunc).funcexpr;
        let colcount = (*rtfunc).funccolcount;
        let fs = funcstates.offset(i as isize);
        let tupdesc: TupleDesc;

        (*fs).setexpr = ExecInitTableFunctionResult(
            funcexpr as *mut Expr,
            (*scanstate).ss.ps.ps_ExprContext,
            &raw mut (*scanstate).ss.ps,
        );

        /*
         * Don't allocate the tuplestores; the actual calls to the functions
         * do that.  NULL means that we have not called the function yet (or
         * need to call it again after a rescan).
         */
        (*fs).tstore = std::ptr::null_mut();
        (*fs).rowcount = -1;

        /*
         * Now build a tupdesc showing the result type we expect from the
         * function.  If we have a coldeflist then that takes priority (note
         * the parser enforces that there is one if the function's nominal
         * output type is RECORD).  Otherwise use get_expr_result_type.
         *
         * Note that if the function returns a named composite type, that may
         * now contain more or different columns than it did when the plan was
         * made.  For both that and the RECORD case, we need to check tuple
         * compatibility.  ExecMakeTableFunctionResult handles some of this,
         * and CheckVarSlotCompatibility provides a backstop.
         */
        if !(*rtfunc).funccolnames.is_null() {
            tupdesc = BuildDescFromLists(
                (*rtfunc).funccolnames,
                (*rtfunc).funccoltypes,
                (*rtfunc).funccoltypmods,
                (*rtfunc).funccolcollations,
            );

            /*
             * For RECORD results, make sure a typmod has been assigned.  (The
             * function should do this for itself, but let's cover things in
             * case it doesn't.)
             */
            BlessTupleDesc(tupdesc);
        } else {
            let functypclass: TypeFuncClass;
            let mut funcrettype: Oid = 0;
            let mut tupdesc_out: TupleDesc = std::ptr::null_mut();

            functypclass = get_expr_result_type(
                funcexpr,
                &raw mut funcrettype,
                &raw mut tupdesc_out,
            );

            if functypclass == TYPEFUNC_COMPOSITE
                || functypclass == TYPEFUNC_COMPOSITE_DOMAIN
            {
                /* Composite data type, e.g. a table's row type */
                Assert!(!tupdesc_out.is_null());
                /* Must copy it out of typcache for safety */
                tupdesc = CreateTupleDescCopy(tupdesc_out);
            } else if functypclass == TYPEFUNC_SCALAR {
                /* Base data type, i.e. scalar */
                tupdesc = CreateTemplateTupleDesc(1);
                TupleDescInitEntry(
                    tupdesc,
                    1 as AttrNumber,
                    std::ptr::null(), /* don't care about the name here */
                    funcrettype,
                    -1,
                    0,
                );
                TupleDescInitEntryCollation(tupdesc, 1 as AttrNumber, exprCollation(funcexpr));
            } else {
                /* crummy error message, but parser should have caught this */
                elog!(ERROR, "function in FROM has unsupported return type");
                unreachable!();
            }
        }

        (*fs).tupdesc = tupdesc;
        (*fs).colcount = colcount;

        /*
         * We only need separate slots for the function results if we are
         * doing ordinality or multiple functions; otherwise, we'll fetch
         * function results directly into the scan slot.
         */
        if !(*scanstate).simple {
            (*fs).func_slot =
                ExecInitExtraTupleSlot(estate, (*fs).tupdesc, &TTSOpsMinimalTuple);
        } else {
            (*fs).func_slot = std::ptr::null_mut();
        }

        natts += colcount;
        i += 1;
    });

    /*
     * Create the combined TupleDesc
     *
     * If there is just one function without ordinality, the scan result
     * tupdesc is the same as the function result tupdesc --- except that we
     * may stuff new names into it below, so drop any rowtype label.
     */
    if (*scanstate).simple {
        scan_tupdesc = CreateTupleDescCopy((*funcstates.offset(0)).tupdesc);
        (*scan_tupdesc).tdtypeid = RECORDOID;
        (*scan_tupdesc).tdtypmod = -1;
    } else {
        let mut attno: AttrNumber = 0;

        if (*node).funcordinality {
            natts += 1;
        }

        scan_tupdesc = CreateTemplateTupleDesc(natts);

        i = 0;
        while i < nfuncs {
            let tupdesc = (*funcstates.offset(i as isize)).tupdesc;
            let colcount = (*funcstates.offset(i as isize)).colcount;
            let mut j: c_int;

            j = 1;
            while j <= colcount {
                attno += 1;
                TupleDescCopyEntry(scan_tupdesc, attno, tupdesc, j as AttrNumber);
                j += 1;
            }
            i += 1;
        }

        /* If doing ordinality, add a column of type "bigint" at the end */
        if (*node).funcordinality {
            attno += 1;
            TupleDescInitEntry(
                scan_tupdesc,
                attno,
                std::ptr::null(), /* don't care about the name here */
                INT8OID,
                -1,
                0,
            );
        }

        Assert!(attno as c_int == natts);
    }

    /*
     * Initialize scan slot and type.
     */
    ExecInitScanTupleSlot(estate, &raw mut (*scanstate).ss, scan_tupdesc, &TTSOpsMinimalTuple);

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTypeTL(&raw mut (*scanstate).ss.ps);
    ExecAssignScanProjectionInfo(&raw mut (*scanstate).ss);

    /*
     * initialize child expressions
     */
    (*scanstate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual, scanstate as *mut PlanState);

    /*
     * Create a memory context that ExecMakeTableFunctionResult can use to
     * evaluate function arguments in.  We can't use the per-tuple context for
     * this because it gets reset too often; but we don't want to leak
     * evaluation results into the query-lifespan context either.  We just
     * need one context, because we evaluate each function separately.
     */
    (*scanstate).argcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        "Table function arguments",
        ALLOCSET_DEFAULT_SIZES
    ) as *mut _;

    scanstate
}

/* ----------------------------------------------------------------
 *      ExecEndFunctionScan
 *
 *      frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndFunctionScan(node: *mut FunctionScanState) {
    let mut i: c_int;

    let funcstates = (*node).funcstates as *mut FunctionScanPerFuncState;

    /*
     * Release slots and tuplestore resources
     */
    i = 0;
    while i < (*node).nfuncs {
        let fs = funcstates.offset(i as isize);

        if !(*fs).tstore.is_null() {
            tuplestore_end((*funcstates.offset(i as isize)).tstore);
            (*fs).tstore = std::ptr::null_mut();
        }
        i += 1;
    }
}

/* ----------------------------------------------------------------
 *      ExecReScanFunctionScan
 *
 *      Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanFunctionScan(node: *mut FunctionScanState) {
    let scan: *mut FunctionScan = (*node).ss.ps.plan as *mut FunctionScan;
    let mut i: c_int;
    let chgparam: *mut Bitmapset = (*node).ss.ps.chgParam;

    let funcstates = (*node).funcstates as *mut FunctionScanPerFuncState;

    if !(*node).ss.ps.ps_ResultTupleSlot.is_null() {
        ExecClearTuple((*node).ss.ps.ps_ResultTupleSlot);
    }
    i = 0;
    while i < (*node).nfuncs {
        let fs = funcstates.offset(i as isize);

        if !(*fs).func_slot.is_null() {
            ExecClearTuple((*fs).func_slot);
        }
        i += 1;
    }

    ExecScanReScan(&raw mut (*node).ss);

    /*
     * Here we have a choice whether to drop the tuplestores (and recompute
     * the function outputs) or just rescan them.  We must recompute if an
     * expression contains changed parameters, else we rescan.
     *
     * XXX maybe we should recompute if the function is volatile?  But in
     * general the executor doesn't conditionalize its actions on that.
     */
    if !chgparam.is_null() {
        let lc: *mut crate::nodes::pg_list::ListCell;

        i = 0;
        foreach!(lc, (*scan).functions, {
            let rtfunc = lfirst(current_cell!(lc)) as *mut RangeTblFunction;

            if bms_overlap(chgparam, (*rtfunc).funcparams) {
                if !(*funcstates.offset(i as isize)).tstore.is_null() {
                    tuplestore_end((*funcstates.offset(i as isize)).tstore);
                    (*funcstates.offset(i as isize)).tstore = std::ptr::null_mut();
                }
                (*funcstates.offset(i as isize)).rowcount = -1;
            }
            i += 1;
        });
    }

    /* Reset ordinality counter */
    (*node).ordinal = 0;

    /* Make sure we rewind any remaining tuplestores */
    i = 0;
    while i < (*node).nfuncs {
        if !(*funcstates.offset(i as isize)).tstore.is_null() {
            tuplestore_rescan((*funcstates.offset(i as isize)).tstore);
        }
        i += 1;
    }
}

/*
 * Int64GetDatumFast -- on 64-bit pass-by-value platforms this is the same as
 * Int64GetDatum (postgres.h).
 */
#[inline]
unsafe fn Int64GetDatumFast(X: int64) -> Datum {
    Int64GetDatum(X)
}

/* ----------------------------------------------------------------
 *      Local stubs for as-yet-unported helpers
 * ----------------------------------------------------------------
 */

/* TypeFuncClass -- from funcapi.h */
type TypeFuncClass = c_int;
#[allow(non_upper_case_globals)]
const TYPEFUNC_SCALAR: TypeFuncClass = 0; /* scalar result type */
#[allow(non_upper_case_globals)]
const TYPEFUNC_COMPOSITE: TypeFuncClass = 1; /* determinable rowtype result */
#[allow(non_upper_case_globals)]
const TYPEFUNC_COMPOSITE_DOMAIN: TypeFuncClass = 2; /* domain over determinable rowtype result */

unsafe fn get_expr_result_type(
    _expr: *mut Node,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}

unsafe fn BlessTupleDesc(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}

unsafe fn exprCollation(_expr: *const Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}

unsafe fn tuplestore_rescan(_state: *mut Tuplestorestate) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

unsafe fn tuplestore_end(_state: *mut Tuplestorestate) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}
