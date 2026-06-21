//! Support routines for sample scans of relations (table sampling).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translated 1:1 from:
//!   postgres/src/backend/executor/nodeSamplescan.c
//!   postgres/src/include/executor/nodeSamplescan.h

use crate::prelude::*;

use std::ffi::c_int;
use std::ptr;

use crate::{castNode, makeNode, foreach, current_cell, Assert, DirectFunctionCall1};

use crate::access::hash::hashfunc::hashfloat8;
use crate::access::tsmapi::{GetTsmRoutine, TsmRoutine};
use crate::common::pg_prng::{pg_global_prng_state, pg_prng_uint32};
use crate::executor::executor::{
    ExecAssignExprContext, ExecAssignScanProjectionInfo, ExecEvalExprSwitchContext, ExecInitExpr,
    ExecInitExprList, ExecInitQual, ExecInitResultTypeTL, ExecInitScanTupleSlot, ExecOpenScanRelation,
    ExecScan, ExecScanAccessMtd, ExecScanRecheckMtd, ExecScanReScan,
};
use crate::executor::tuptable::ExecClearTuple;
use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, PlanState, SampleScanState, ScanState, TupleTableSlot,
};
use crate::nodes::nodes::NodeTag;
use crate::nodes::parsenodes::TableSampleClause;
use crate::nodes::pg_list::{lfirst, list_length, List};
use crate::nodes::plannodes::SampleScan;
use crate::utils::rel::RelationGetDescr;

// outerPlan/innerPlan (plannodes.h): the left/right child of a Plan node.
#[allow(unused_macros)]
macro_rules! outerPlan {
    ($node:expr) => {
        (*$node).scan.plan.lefttree
    };
}
use outerPlan;

#[allow(unused_macros)]
macro_rules! innerPlan {
    ($node:expr) => {
        (*$node).scan.plan.righttree
    };
}
use innerPlan;

// ----------------------------------------------------------------
// Local stub types for not-yet-ported dependencies.
// ----------------------------------------------------------------

type TableScanDesc = *mut c_void;

// ----------------------------------------------------------------
// Local stubs for unported helper functions we call.
// (access/tableam.h)
// ----------------------------------------------------------------

unsafe fn table_slot_callbacks(_rel: *mut c_void) -> *const c_void {
    crate::access::table::tableam::table_slot_callbacks(_rel as _) as _
}

unsafe fn table_endscan(_scan: TableScanDesc) {
    crate::access::table::tableam::table_endscan(_scan as _)
}

unsafe fn table_beginscan_sampling(
    _rel: *mut c_void,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut c_void,
    _use_bulkread: bool,
    _allow_strat: bool,
    _use_pagemode: bool,
) -> TableScanDesc {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_rescan_set_params(
    _scan: TableScanDesc,
    _key: *mut c_void,
    _use_bulkread: bool,
    _allow_strat: bool,
    _use_pagemode: bool,
) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_scan_sample_next_block(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
) -> bool {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_scan_sample_next_tuple(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: access/tableam.h
}

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SampleNext
 *
 *		This is a workhorse for ExecSampleScan
 * ----------------------------------------------------------------
 */
unsafe fn SampleNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut SampleScanState;

    /*
     * if this is first call within a scan, initialize
     */
    if !(*node).begun {
        tablesample_init(node);
    }

    /*
     * get the next tuple, and store it in our result slot
     */
    tablesample_getnext(node)
}

/*
 * SampleRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn SampleRecheck(_node: *mut ScanState, _slot: *mut TupleTableSlot) -> bool {
    /*
     * No need to recheck for SampleScan, since like SeqScan we don't pass any
     * checkable keys to heap_beginscan.
     */
    true
}

/* ----------------------------------------------------------------
 *		ExecSampleScan(node)
 *
 *		Scans the relation using the sampling method and returns
 *		the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
unsafe fn ExecSampleScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SampleScanState = castNode!(SampleScanState, T_SampleScanState, pstate);

    ExecScan(
        &mut (*node).ss,
        Some(SampleNext),
        Some(SampleRecheck),
    )
}

/* ----------------------------------------------------------------
 *		ExecInitSampleScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitSampleScan(
    node: *mut SampleScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut SampleScanState {
    let scanstate: *mut SampleScanState;
    let tsc: *mut TableSampleClause = (*node).tablesample;
    let tsm: *mut TsmRoutine;

    Assert!(outerPlan!(node).is_null());
    Assert!(innerPlan!(node).is_null());

    /*
     * create state structure
     */
    scanstate = makeNode!(SampleScanState, T_SampleScanState);
    (*scanstate).ss.ps.plan = node as *mut _;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecSampleScan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*scanstate).ss.ps);

    /*
     * open the scan relation
     */
    (*scanstate).ss.ss_currentRelation =
        ExecOpenScanRelation(estate, (*node).scan.scanrelid, eflags);

    /* we won't set up the HeapScanDesc till later */
    (*scanstate).ss.ss_currentScanDesc = ptr::null_mut();

    /* and create slot with appropriate rowtype */
    ExecInitScanTupleSlot(
        estate,
        &mut (*scanstate).ss,
        RelationGetDescr((*scanstate).ss.ss_currentRelation),
        table_slot_callbacks((*scanstate).ss.ss_currentRelation as *mut c_void) as *const _,
    );

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*scanstate).ss.ps);
    ExecAssignScanProjectionInfo(&mut (*scanstate).ss);

    /*
     * initialize child expressions
     */
    (*scanstate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual, scanstate as *mut PlanState);

    (*scanstate).args = ExecInitExprList((*tsc).args, scanstate as *mut PlanState);
    (*scanstate).repeatable =
        ExecInitExpr((*tsc).repeatable as *mut _, scanstate as *mut PlanState);

    /*
     * If we don't have a REPEATABLE clause, select a random seed.  We want to
     * do this just once, since the seed shouldn't change over rescans.
     */
    if (*tsc).repeatable.is_null() {
        (*scanstate).seed = pg_prng_uint32(&raw mut pg_global_prng_state);
    }

    /*
     * Finally, initialize the TABLESAMPLE method handler.
     */
    tsm = GetTsmRoutine((*tsc).tsmhandler);
    (*scanstate).tsmroutine = tsm;
    (*scanstate).tsm_state = ptr::null_mut();

    if let Some(initfn) = (*tsm).InitSampleScan {
        initfn(scanstate, eflags);
    }

    /* We'll do BeginSampleScan later; we can't evaluate params yet */
    (*scanstate).begun = false;

    scanstate
}

/* ----------------------------------------------------------------
 *		ExecEndSampleScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndSampleScan(node: *mut SampleScanState) {
    /*
     * Tell sampling function that we finished the scan.
     */
    if let Some(endfn) = (*(*node).tsmroutine).EndSampleScan {
        endfn(node);
    }

    /*
     * close heap scan
     */
    if !(*node).ss.ss_currentScanDesc.is_null() {
        table_endscan((*node).ss.ss_currentScanDesc as TableScanDesc);
    }
}

/* ----------------------------------------------------------------
 *		ExecReScanSampleScan
 *
 *		Rescans the relation.
 *
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanSampleScan(node: *mut SampleScanState) {
    /* Remember we need to do BeginSampleScan again (if we did it at all) */
    (*node).begun = false;
    (*node).done = false;
    (*node).haveblock = false;
    (*node).donetuples = 0;

    ExecScanReScan(&mut (*node).ss);
}

/*
 * Initialize the TABLESAMPLE method: evaluate params and call BeginSampleScan.
 */
unsafe fn tablesample_init(scanstate: *mut SampleScanState) {
    let tsm: *mut TsmRoutine = (*scanstate).tsmroutine;
    let econtext: *mut ExprContext = (*scanstate).ss.ps.ps_ExprContext;
    let params: *mut Datum;
    let datum: Datum;
    let mut isnull: bool = false;
    let seed: uint32;
    let allow_sync: bool;
    let mut i: c_int;

    (*scanstate).donetuples = 0;
    params = palloc(list_length((*scanstate).args) as usize * std::mem::size_of::<Datum>()) as *mut Datum;

    i = 0;
    foreach!(arg, (*scanstate).args, {
        let argstate: *mut ExprState = lfirst(current_cell!(arg)) as *mut ExprState;

        *params.offset(i as isize) =
            ExecEvalExprSwitchContext(argstate, econtext, &mut isnull);
        if isnull {
            ereport!(
                ERROR,
                "TABLESAMPLE parameter cannot be null"
            );
        }
        i += 1;
    });

    if !(*scanstate).repeatable.is_null() {
        datum = ExecEvalExprSwitchContext((*scanstate).repeatable, econtext, &mut isnull);
        if isnull {
            ereport!(
                ERROR,
                "TABLESAMPLE REPEATABLE parameter cannot be null"
            );
        }

        /*
         * The REPEATABLE parameter has been coerced to float8 by the parser.
         * The reason for using float8 at the SQL level is that it will
         * produce unsurprising results both for users used to databases that
         * accept only integers in the REPEATABLE clause and for those who
         * might expect that REPEATABLE works like setseed() (a float in the
         * range from -1 to 1).
         *
         * We use hashfloat8() to convert the supplied value into a suitable
         * seed.  For regression-testing purposes, that has the convenient
         * property that REPEATABLE(0) gives a machine-independent result.
         */
        seed = DatumGetUInt32(DirectFunctionCall1!(hashfloat8, datum));
    } else {
        /* Use the seed selected by ExecInitSampleScan */
        seed = (*scanstate).seed;
    }

    /* Set default values for params that BeginSampleScan can adjust */
    (*scanstate).use_bulkread = true;
    (*scanstate).use_pagemode = true;

    /* Let tablesample method do its thing */
    ((*tsm).BeginSampleScan.expect("BeginSampleScan not set"))(
        scanstate,
        params,
        list_length((*scanstate).args),
        seed,
    );

    /* We'll use syncscan if there's no NextSampleBlock function */
    allow_sync = (*tsm).NextSampleBlock.is_none();

    /* Now we can create or reset the HeapScanDesc */
    if (*scanstate).ss.ss_currentScanDesc.is_null() {
        (*scanstate).ss.ss_currentScanDesc = table_beginscan_sampling(
            (*scanstate).ss.ss_currentRelation as *mut c_void,
            (*(*scanstate).ss.ps.state).es_snapshot as *mut c_void,
            0,
            ptr::null_mut(),
            (*scanstate).use_bulkread,
            allow_sync,
            (*scanstate).use_pagemode,
        ) as *mut _;
    } else {
        table_rescan_set_params(
            (*scanstate).ss.ss_currentScanDesc as TableScanDesc,
            ptr::null_mut(),
            (*scanstate).use_bulkread,
            allow_sync,
            (*scanstate).use_pagemode,
        );
    }

    pfree(params as *mut c_void);

    /* And we're initialized. */
    (*scanstate).begun = true;
}

/*
 * Get next tuple from TABLESAMPLE method.
 */
unsafe fn tablesample_getnext(scanstate: *mut SampleScanState) -> *mut TupleTableSlot {
    let scan: TableScanDesc = (*scanstate).ss.ss_currentScanDesc as TableScanDesc;
    let slot: *mut TupleTableSlot = (*scanstate).ss.ss_ScanTupleSlot;

    ExecClearTuple(slot);

    if (*scanstate).done {
        return ptr::null_mut();
    }

    loop {
        if !(*scanstate).haveblock {
            if !table_scan_sample_next_block(scan, scanstate) {
                (*scanstate).haveblock = false;
                (*scanstate).done = true;

                /* exhausted relation */
                return ptr::null_mut();
            }

            (*scanstate).haveblock = true;
        }

        if !table_scan_sample_next_tuple(scan, scanstate, slot) {
            /*
             * If we get here, it means we've exhausted the items on this page
             * and it's time to move to the next.
             */
            (*scanstate).haveblock = false;
            continue;
        }

        /* Found visible tuple, return it. */
        break;
    }

    (*scanstate).donetuples += 1;

    slot
}
