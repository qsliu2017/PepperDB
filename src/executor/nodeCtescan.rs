//! routines to handle CteScan nodes.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! src/backend/executor/nodeCtescan.c
//! src/include/executor/nodeCtescan.h

use crate::prelude::*;

use std::ffi::c_int;
use std::ptr;

use crate::nodes::execnodes::{
    CteScanState, EState, ExprContext, ParamExecData, PlanState, ScanState, Tuplestorestate,
    TupleTableSlot,
};
use crate::nodes::plannodes::{CteScan, Plan};
use crate::executor::executor::{ExecScanAccessMtd, ExecScanRecheckMtd};
use crate::{castNode, makeNode};

// ----------------------------------------------------------------
//		CteScanNext
//
//		This is a workhorse for ExecCteScan
// ----------------------------------------------------------------
unsafe fn CteScanNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut CteScanState;
    let estate: *mut EState;
    let dir: ScanDirection;
    let forward: bool;
    let tuplestorestate: *mut Tuplestorestate;
    let mut eof_tuplestore: bool;
    let slot: *mut TupleTableSlot;

    /*
     * get state info from node
     */
    estate = (*node).ss.ps.state;
    dir = (*estate).es_direction;
    forward = ScanDirectionIsForward!(dir);
    tuplestorestate = (*(*node).leader).cte_table;
    tuplestore_select_read_pointer(tuplestorestate, (*node).readptr);
    slot = (*node).ss.ss_ScanTupleSlot;

    /*
     * If we are not at the end of the tuplestore, or are going backwards, try
     * to fetch a tuple from tuplestore.
     */
    eof_tuplestore = tuplestore_ateof(tuplestorestate);

    if !forward && eof_tuplestore {
        if !(*(*node).leader).eof_cte {
            /*
             * When reversing direction at tuplestore EOF, the first
             * gettupleslot call will fetch the last-added tuple; but we want
             * to return the one before that, if possible. So do an extra
             * fetch.
             */
            if !tuplestore_advance(tuplestorestate, forward) {
                return ptr::null_mut(); /* the tuplestore must be empty */
            }
        }
        eof_tuplestore = false;
    }

    /*
     * If we can fetch another tuple from the tuplestore, return it.
     *
     * Note: we have to use copy=true in the tuplestore_gettupleslot call,
     * because we are sharing the tuplestore with other nodes that might write
     * into the tuplestore before we get called again.
     */
    if !eof_tuplestore {
        if tuplestore_gettupleslot(tuplestorestate, forward, true, slot) {
            return slot;
        }
        if forward {
            eof_tuplestore = true;
        }
    }

    /*
     * If necessary, try to fetch another row from the CTE query.
     *
     * Note: the eof_cte state variable exists to short-circuit further calls
     * of the CTE plan.  It's not optional, unfortunately, because some plan
     * node types are not robust about being called again when they've already
     * returned NULL.
     */
    if eof_tuplestore && !(*(*node).leader).eof_cte {
        let cteslot: *mut TupleTableSlot;

        /*
         * We can only get here with forward==true, so no need to worry about
         * which direction the subplan will go.
         */
        cteslot = ExecProcNode((*node).cteplanstate);
        if TupIsNull!(cteslot) {
            (*(*node).leader).eof_cte = true;
            return ptr::null_mut();
        }

        /*
         * There are corner cases where the subplan could change which
         * tuplestore read pointer is active, so be sure to reselect ours
         * before storing the tuple we got.
         */
        tuplestore_select_read_pointer(tuplestorestate, (*node).readptr);

        /*
         * Append a copy of the returned tuple to tuplestore.  NOTE: because
         * our read pointer is certainly in EOF state, its read position will
         * move forward over the added tuple.  This is what we want.  Also,
         * any other readers will *not* move past the new tuple, which is what
         * they want.
         */
        tuplestore_puttupleslot(tuplestorestate, cteslot);

        /*
         * We MUST copy the CTE query's output tuple into our own slot. This
         * is because other CteScan nodes might advance the CTE query before
         * we are called again, and our output tuple must stay stable over
         * that.
         */
        return ExecCopySlot(slot, cteslot);
    }

    /*
     * Nothing left ...
     */
    ExecClearTuple(slot)
}

/*
 * CteScanRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn CteScanRecheck(_node: *mut ScanState, _slot: *mut TupleTableSlot) -> bool {
    /* nothing to check */
    true
}

// ----------------------------------------------------------------
//		ExecCteScan(node)
//
//		Scans the CTE sequentially and returns the next qualifying tuple.
//		We call the ExecScan() routine and pass it the appropriate
//		access method functions.
// ----------------------------------------------------------------
unsafe fn ExecCteScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut CteScanState = castNode!(CteScanState, T_CteScanState, pstate);

    ExecScan(
        &mut (*node).ss,
        Some(CteScanNext),
        Some(CteScanRecheck),
    )
}

// ----------------------------------------------------------------
//		ExecInitCteScan
// ----------------------------------------------------------------
pub unsafe fn ExecInitCteScan(
    node: *mut CteScan,
    estate: *mut EState,
    mut eflags: c_int,
) -> *mut CteScanState {
    let scanstate: *mut CteScanState;
    let prmdata: *mut ParamExecData;

    /* check for unsupported flags */
    Assert!(eflags & EXEC_FLAG_MARK == 0);

    /*
     * For the moment we have to force the tuplestore to allow REWIND, because
     * we might be asked to rescan the CTE even though upper levels didn't
     * tell us to be prepared to do it efficiently.  Annoying, since this
     * prevents truncation of the tuplestore.  XXX FIXME
     *
     * Note: if we are in an EPQ recheck plan tree, it's likely that no access
     * to the tuplestore is needed at all, making this even more annoying.
     * It's not worth improving that as long as all the read pointers would
     * have REWIND anyway, but if we ever improve this logic then that aspect
     * should be considered too.
     */
    eflags |= EXEC_FLAG_REWIND;

    /*
     * CteScan should not have any children.
     */
    Assert!(outerPlan!(node).is_null());
    Assert!(innerPlan!(node).is_null());

    /*
     * create new CteScanState for node
     */
    scanstate = makeNode!(CteScanState, T_CteScanState);
    (*scanstate).ss.ps.plan = node as *mut Plan;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecCteScan);
    (*scanstate).eflags = eflags;
    (*scanstate).cte_table = ptr::null_mut();
    (*scanstate).eof_cte = false;

    /*
     * Find the already-initialized plan for the CTE query.
     */
    (*scanstate).cteplanstate = list_nth(
        (*estate).es_subplanstates,
        (*node).ctePlanId - 1,
    ) as *mut PlanState;

    /*
     * The Param slot associated with the CTE query is used to hold a pointer
     * to the CteState of the first CteScan node that initializes for this
     * CTE.  This node will be the one that holds the shared state for all the
     * CTEs, particularly the shared tuplestore.
     */
    prmdata = &mut (*(*estate).es_param_exec_vals.add((*node).cteParam as usize));
    Assert!((*prmdata).execPlan.is_null());
    Assert!(!(*prmdata).isnull);
    (*scanstate).leader = castNode!(
        CteScanState,
        T_CteScanState,
        DatumGetPointer((*prmdata).value)
    );
    if (*scanstate).leader.is_null() {
        /* I am the leader */
        (*prmdata).value = PointerGetDatum(scanstate as *const c_void);
        (*scanstate).leader = scanstate;
        (*scanstate).cte_table = tuplestore_begin_heap(true, false, work_mem);
        tuplestore_set_eflags((*scanstate).cte_table, (*scanstate).eflags);
        (*scanstate).readptr = 0;
    } else {
        /* Not the leader */
        /* Create my own read pointer, and ensure it is at start */
        (*scanstate).readptr = tuplestore_alloc_read_pointer(
            (*(*scanstate).leader).cte_table,
            (*scanstate).eflags,
        );
        tuplestore_select_read_pointer(
            (*(*scanstate).leader).cte_table,
            (*scanstate).readptr,
        );
        tuplestore_rescan((*(*scanstate).leader).cte_table);
    }

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*scanstate).ss.ps);

    /*
     * The scan tuple type (ie, the rowtype we expect to find in the work
     * table) is the same as the result rowtype of the CTE query.
     */
    ExecInitScanTupleSlot(
        estate,
        &mut (*scanstate).ss,
        ExecGetResultType((*scanstate).cteplanstate),
        &TTSOpsMinimalTuple,
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

    scanstate
}

// ----------------------------------------------------------------
//		ExecEndCteScan
//
//		frees any storage allocated through C routines.
// ----------------------------------------------------------------
pub unsafe fn ExecEndCteScan(node: *mut CteScanState) {
    /*
     * If I am the leader, free the tuplestore.
     */
    if (*node).leader == node {
        tuplestore_end((*node).cte_table);
        (*node).cte_table = ptr::null_mut();
    }
}

// ----------------------------------------------------------------
//		ExecReScanCteScan
//
//		Rescans the relation.
// ----------------------------------------------------------------
pub unsafe fn ExecReScanCteScan(node: *mut CteScanState) {
    let tuplestorestate: *mut Tuplestorestate = (*(*node).leader).cte_table;

    if !(*node).ss.ps.ps_ResultTupleSlot.is_null() {
        ExecClearTuple((*node).ss.ps.ps_ResultTupleSlot);
    }

    ExecScanReScan(&mut (*node).ss);

    /*
     * Clear the tuplestore if a new scan of the underlying CTE is required.
     * This implicitly resets all the tuplestore's read pointers.  Note that
     * multiple CTE nodes might redundantly clear the tuplestore; that's OK,
     * and not unduly expensive.  We'll stop taking this path as soon as
     * somebody has attempted to read something from the underlying CTE
     * (thereby causing its chgParam to be cleared).
     */
    if !(*(*(*node).leader).cteplanstate).chgParam.is_null() {
        tuplestore_clear(tuplestorestate);
        (*(*node).leader).eof_cte = false;
    } else {
        /*
         * Else, just rewind my own pointer.  Either the underlying CTE
         * doesn't need a rescan (and we can re-read what's in the tuplestore
         * now), or somebody else already took care of it.
         */
        tuplestore_select_read_pointer(tuplestorestate, (*node).readptr);
        tuplestore_rescan(tuplestorestate);
    }
}

// ----------------------------------------------------------------
// Local stubs for not-yet-ported dependencies
// ----------------------------------------------------------------

// ScanDirection (access/sdir.h)
type ScanDirection = c_int;

// EXEC_FLAG_* (executor/executor.h)
const EXEC_FLAG_MARK: c_int = 0x0008;
const EXEC_FLAG_REWIND: c_int = 0x0002;

#[allow(non_upper_case_globals)]
static mut work_mem: c_int = 4096; // TODO: utils/guc.c

// TTSOpsMinimalTuple (executor/tuptable.h)
#[allow(non_upper_case_globals)]
static TTSOpsMinimalTuple: TupleTableSlotOps = TupleTableSlotOps {};
#[allow(non_camel_case_types)]
pub struct TupleTableSlotOps {}

#[allow(unused_macros)]
macro_rules! ScanDirectionIsForward {
    ($dir:expr) => {
        $dir as c_int > 0
    };
}
use ScanDirectionIsForward;

#[allow(unused_macros)]
macro_rules! TupIsNull {
    ($slot:expr) => {
        $slot.is_null() || (*$slot).tts_flags & TTS_FLAG_EMPTY != 0
    };
}
use TupIsNull;
const TTS_FLAG_EMPTY: u16 = 1 << 1;

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

unsafe fn tuplestore_select_read_pointer(_state: *mut Tuplestorestate, _ptr: c_int) {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_ateof(_state: *mut Tuplestorestate) -> bool {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_advance(_state: *mut Tuplestorestate, _forward: bool) -> bool {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_puttupleslot(_state: *mut Tuplestorestate, _slot: *mut TupleTableSlot) {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_begin_heap(
    _randomAccess: bool,
    _interXact: bool,
    _maxKBytes: c_int,
) -> *mut Tuplestorestate {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_set_eflags(_state: *mut Tuplestorestate, _eflags: c_int) {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_alloc_read_pointer(_state: *mut Tuplestorestate, _eflags: c_int) -> c_int {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_rescan(_state: *mut Tuplestorestate) {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_clear(_state: *mut Tuplestorestate) {
    unimplemented!() // TODO: utils/tuplestore.c
}
unsafe fn tuplestore_end(_state: *mut Tuplestorestate) {
    unimplemented!() // TODO: utils/tuplestore.c
}

unsafe fn ExecProcNode(_node: *mut PlanState) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/execProcnode.c
}
unsafe fn ExecCopySlot(
    _dstslot: *mut TupleTableSlot,
    _srcslot: *mut TupleTableSlot,
) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn ExecClearTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn ExecScan(
    _node: *mut crate::nodes::execnodes::ScanState,
    _accessMtd: ExecScanAccessMtd,
    _recheckMtd: ExecScanRecheckMtd,
) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/execScan.c
}
unsafe fn ExecScanReScan(_node: *mut crate::nodes::execnodes::ScanState) {
    unimplemented!() // TODO: executor/execScan.c
}
unsafe fn ExecAssignExprContext(_estate: *mut EState, _planstate: *mut PlanState) {
    unimplemented!() // TODO: executor/execUtils.c
}
unsafe fn ExecInitScanTupleSlot(
    _estate: *mut EState,
    _scanstate: *mut crate::nodes::execnodes::ScanState,
    _tupledesc: *mut TupleDesc,
    _tts_ops: *const TupleTableSlotOps,
) {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn ExecGetResultType(_planstate: *mut PlanState) -> *mut TupleDesc {
    unimplemented!() // TODO: executor/execUtils.c
}
unsafe fn ExecInitResultTypeTL(_planstate: *mut PlanState) {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn ExecAssignScanProjectionInfo(_node: *mut crate::nodes::execnodes::ScanState) {
    unimplemented!() // TODO: executor/execScan.c
}
unsafe fn ExecInitQual(
    _qual: *mut crate::nodes::pg_list::List,
    _parent: *mut PlanState,
) -> *mut crate::nodes::execnodes::ExprState {
    unimplemented!() // TODO: executor/execExpr.c
}
unsafe fn list_nth(_list: *const crate::nodes::pg_list::List, _n: c_int) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: nodes/list.c
}

// TupleDesc (access/tupdesc.h) - referenced via stub signatures
type TupleDesc = crate::access::common::tupdesc::TupleDescData;

#[allow(unused_imports)]
use ExprContext as _ExprContextUnused;
