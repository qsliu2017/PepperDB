//! Routines to support scans of foreign tables
//!
//! src/backend/executor/nodeForeignscan.c
//! src/include/executor/nodeForeignscan.h
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! INTERFACE ROUTINES
//!     ExecForeignScan         scans a foreign table.
//!     ExecInitForeignScan     creates and initializes state info.
//!     ExecReScanForeignScan   rescans the foreign relation.
//!     ExecEndForeignScan      releases any resources allocated.

use crate::prelude::*;

use std::ffi::{c_int, c_void};

use crate::executor::executor::{
    ExecAssignExprContext, ExecAssignScanProjectionInfoWithVarno, ExecInitNode, ExecInitQual,
    ExecInitResultTypeTL, ExecInitScanTupleSlot, ExecOpenScanRelation, ExecQual, ExecReScan,
    ExecScan, ExecScanAccessMtd, ExecScanRecheckMtd, ExecScanReScan, ExecTypeFromTL, ExecEndNode,
    ResetExprContext, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::execTuples::TTSOpsHeapTuple;
use crate::executor::tuptable::TupIsNull;
use crate::access::common::tupdesc::{CreateTupleDescCopy, TupleDesc};
use crate::nodes::execnodes::{
    outerPlanState, AsyncRequest, EState, ExprContext, ForeignScanState, PlanState, ResultRelInfo,
    TupleTableSlot,
};
use crate::nodes::nodes::CmdType::CMD_SELECT;
use crate::nodes::pg_list::NIL;
use crate::nodes::plannodes::{outerPlan, ForeignScan, Plan};
use crate::nodes::primnodes::INDEX_VAR;
use crate::utils::rel::{RelationGetDescr, RelationGetRelid};
use crate::{castNode, makeNode, Assert};

// ----------------------------------------------------------------
//      Local stubs for not-yet-ported dependencies
// ----------------------------------------------------------------

// foreign/fdwapi.h -- the real FdwRoutine carrying the FDW vtable.  The
// ForeignScanState.fdwroutine field is typed as an opaque execnodes::FdwRoutine
// to avoid including fdwapi.h there, so we reach the function pointers through
// this real definition (see crate::foreign::fdwapi).
use crate::foreign::fdwapi::FdwRoutine;

// foreign/fdwapi.h
unsafe fn GetFdwRoutineForRelation(_relation: Relation, _makecopy: bool) -> *mut FdwRoutine {
    crate::foreign::foreign::GetFdwRoutineForRelation(_relation as _, _makecopy as _) as _
}
unsafe fn GetFdwRoutineByServerId(_serverid: Oid) -> *mut FdwRoutine {
    crate::foreign::foreign::GetFdwRoutineByServerId(_serverid as _) as _
}

// utils/rel.h
type Relation = crate::utils::rel::Relation;

// access/parallel.h -- ParallelContext / ParallelWorkerContext are not yet
// ported (only an opaque placeholder exists in nodes/extensible.h).  Mirror the
// fields used here so the storage/shm_toc.h calls below typecheck faithfully.
#[repr(C)]
pub struct ParallelContext {
    pub estimator: shm_toc_estimator,
    pub toc: *mut shm_toc,
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct ParallelWorkerContext {
    pub toc: *mut shm_toc,
    _opaque: [u8; 0],
}

// storage/shm_toc.h
#[repr(C)]
pub struct shm_toc {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct shm_toc_estimator {
    _opaque: [u8; 0],
}
unsafe fn shm_toc_estimate_chunk(_e: *mut shm_toc_estimator, _sz: Size) {
    unimplemented!()
}
unsafe fn shm_toc_estimate_keys(_e: *mut shm_toc_estimator, _cnt: Size) {
    unimplemented!()
}
unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _nbytes: Size) -> *mut c_void {
    crate::storage::ipc::shm_toc::shm_toc_allocate(_toc as _, _nbytes as _) as _
}
unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: uint64, _address: *mut c_void) {
    crate::storage::ipc::shm_toc::shm_toc_insert(_toc as _, _key as _, _address as _)
}
unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: uint64, _noError: bool) -> *mut c_void {
    crate::storage::ipc::shm_toc::shm_toc_lookup(_toc as _, _key as _, _noError as _) as _
}

// ----------------------------------------------------------------
//      ForeignNext
//
//      This is a workhorse for ExecForeignScan
// ----------------------------------------------------------------
unsafe fn ForeignNext(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    let slot: *mut TupleTableSlot;
    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let econtext = (*node).ss.ps.ps_ExprContext;
    let oldcontext: MemoryContext;

    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    /* Call the Iterate function in short-lived context */
    oldcontext = MemoryContextSwitchTo((*econtext).ecxt_per_tuple_memory);
    if (*plan).operation != CMD_SELECT {
        /*
         * direct modifications cannot be re-evaluated, so shouldn't get here
         * during EvalPlanQual processing
         */
        Assert!((*(*node).ss.ps.state).es_epq_active.is_null());

        slot = ((*fdwroutine).IterateDirectModify.unwrap())(node as *mut _) as *mut _;
    } else {
        slot = ((*fdwroutine).IterateForeignScan.unwrap())(node as *mut _) as *mut _;
    }
    MemoryContextSwitchTo(oldcontext);

    /*
     * Insert valid value into tableoid, the only actually-useful system
     * column.
     */
    if (*plan).fsSystemCol && !TupIsNull(slot) {
        (*slot).tts_tableOid = RelationGetRelid((*node).ss.ss_currentRelation);
    }

    slot
}

/*
 * ForeignRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn ForeignRecheck(node: *mut ForeignScanState, slot: *mut TupleTableSlot) -> bool {
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;
    let econtext: *mut ExprContext;

    /*
     * extract necessary information from foreign scan node
     */
    econtext = (*node).ss.ps.ps_ExprContext;

    /* Does the tuple meet the remote qual condition? */
    (*econtext).ecxt_scantuple = slot;

    ResetExprContext(econtext);

    /*
     * If an outer join is pushed down, RecheckForeignScan may need to store a
     * different tuple in the slot, because a different set of columns may go
     * to NULL upon recheck.  Otherwise, it shouldn't need to change the slot
     * contents, just return true or false to indicate whether the quals still
     * pass.  For simple cases, setting fdw_recheck_quals may be easier than
     * providing this callback.
     */
    if (*fdwroutine).RecheckForeignScan.is_some()
        && !((*fdwroutine).RecheckForeignScan.unwrap())(node as *mut _, slot as *mut _)
    {
        return false;
    }

    ExecQual((*node).fdw_recheck_quals, econtext)
}

/* ----------------------------------------------------------------
 *      ExecForeignScan(node)
 *
 *      Fetches the next tuple from the FDW, checks local quals, and
 *      returns it.
 *      We call the ExecScan() routine and pass it the appropriate
 *      access method functions.
 * ----------------------------------------------------------------
 */
unsafe fn ExecForeignScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut ForeignScanState = castNode!(ForeignScanState, T_ForeignScanState, pstate);
    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let estate = (*node).ss.ps.state;

    /*
     * Ignore direct modifications when EvalPlanQual is active --- they are
     * irrelevant for EvalPlanQual rechecking
     */
    if !(*estate).es_epq_active.is_null() && (*plan).operation != CMD_SELECT {
        return std::ptr::null_mut();
    }

    ExecScan(
        &raw mut (*node).ss,
        Some(ForeignNext_access),
        Some(ForeignRecheck_recheck),
    )
}

/* ----------------------------------------------------------------
 *      ExecInitForeignScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitForeignScan(
    node: *mut ForeignScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut ForeignScanState {
    let scanstate: *mut ForeignScanState;
    let mut currentRelation: Relation = std::ptr::null_mut();
    let scanrelid: Index = (*node).scan.scanrelid;
    let tlistvarno: c_int;
    let fdwroutine: *mut FdwRoutine;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * create state structure
     */
    scanstate = makeNode!(ForeignScanState, T_ForeignScanState);
    (*scanstate).ss.ps.plan = node as *mut Plan;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecForeignScan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &raw mut (*scanstate).ss.ps);

    /*
     * open the scan relation, if any; also acquire function pointers from the
     * FDW's handler
     */
    if scanrelid > 0 {
        currentRelation = ExecOpenScanRelation(estate, scanrelid, eflags);
        (*scanstate).ss.ss_currentRelation = currentRelation;
        fdwroutine = GetFdwRoutineForRelation(currentRelation, true);
    } else {
        /* We can't use the relcache, so get fdwroutine the hard way */
        fdwroutine = GetFdwRoutineByServerId((*node).fs_server);
    }

    /*
     * Determine the scan tuple type.  If the FDW provided a targetlist
     * describing the scan tuples, use that; else use base relation's rowtype.
     */
    if (*node).fdw_scan_tlist != NIL || currentRelation.is_null() {
        let scan_tupdesc: TupleDesc;

        scan_tupdesc = ExecTypeFromTL((*node).fdw_scan_tlist);
        ExecInitScanTupleSlot(estate, &raw mut (*scanstate).ss, scan_tupdesc, &TTSOpsHeapTuple);
        /* Node's targetlist will contain Vars with varno = INDEX_VAR */
        tlistvarno = INDEX_VAR;
    } else {
        let scan_tupdesc: TupleDesc;

        /* don't trust FDWs to return tuples fulfilling NOT NULL constraints */
        scan_tupdesc = CreateTupleDescCopy(RelationGetDescr(currentRelation));
        ExecInitScanTupleSlot(estate, &raw mut (*scanstate).ss, scan_tupdesc, &TTSOpsHeapTuple);
        /* Node's targetlist will contain Vars with varno = scanrelid */
        tlistvarno = scanrelid as c_int;
    }

    /* Don't know what an FDW might return */
    (*scanstate).ss.ps.scanopsfixed = false;
    (*scanstate).ss.ps.scanopsset = true;

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTypeTL(&raw mut (*scanstate).ss.ps);
    ExecAssignScanProjectionInfoWithVarno(&raw mut (*scanstate).ss, tlistvarno);

    /*
     * initialize child expressions
     */
    (*scanstate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual, scanstate as *mut PlanState);
    (*scanstate).fdw_recheck_quals =
        ExecInitQual((*node).fdw_recheck_quals, scanstate as *mut PlanState);

    /*
     * Determine whether to scan the foreign relation asynchronously or not;
     * this has to be kept in sync with the code in ExecInitAppend().
     */
    (*scanstate).ss.ps.async_capable =
        (*(node as *mut Plan)).async_capable && (*estate).es_epq_active.is_null();

    /*
     * Initialize FDW-related state.
     */
    (*scanstate).fdwroutine = fdwroutine as *mut crate::nodes::execnodes::FdwRoutine;
    (*scanstate).fdw_state = std::ptr::null_mut();

    /*
     * For the FDW's convenience, look up the modification target relation's
     * ResultRelInfo.  The ModifyTable node should have initialized it for us,
     * see ExecInitModifyTable.
     *
     * Don't try to look up the ResultRelInfo when EvalPlanQual is active,
     * though.  Direct modifications cannot be re-evaluated as part of
     * EvalPlanQual.  The lookup wouldn't work anyway because during
     * EvalPlanQual processing, EvalPlanQual only initializes the subtree
     * under the ModifyTable, and doesn't run ExecInitModifyTable.
     */
    if (*node).resultRelation > 0 && (*estate).es_epq_active.is_null() {
        if (*estate).es_result_relations.is_null()
            || (*(*estate).es_result_relations.add(((*node).resultRelation - 1) as usize)).is_null()
        {
            elog!(ERROR, "result relation not initialized");
        }
        (*scanstate).resultRelInfo =
            *(*estate).es_result_relations.add(((*node).resultRelation - 1) as usize);
    }

    /* Initialize any outer plan. */
    if !outerPlan(node as *mut Plan).is_null() {
        (*scanstate).ss.ps.lefttree =
            ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);
    }

    /*
     * Tell the FDW to initialize the scan.
     */
    if (*node).operation != CMD_SELECT {
        /*
         * Direct modifications cannot be re-evaluated by EvalPlanQual, so
         * don't bother preparing the FDW.
         *
         * In case of an inherited UPDATE/DELETE with foreign targets there
         * can be direct-modify ForeignScan nodes in the EvalPlanQual subtree,
         * so we need to ignore such ForeignScan nodes during EvalPlanQual
         * processing.  See also ExecForeignScan/ExecReScanForeignScan.
         */
        if (*estate).es_epq_active.is_null() {
            ((*fdwroutine).BeginDirectModify.unwrap())(scanstate as *mut _, eflags);
        }
    } else {
        ((*fdwroutine).BeginForeignScan.unwrap())(scanstate as *mut _, eflags);
    }

    scanstate
}

/* ----------------------------------------------------------------
 *      ExecEndForeignScan
 *
 *      frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndForeignScan(node: *mut ForeignScanState) {
    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let estate = (*node).ss.ps.state;
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    /* Let the FDW shut down */
    if (*plan).operation != CMD_SELECT {
        if (*estate).es_epq_active.is_null() {
            ((*fdwroutine).EndDirectModify.unwrap())(node as *mut _);
        }
    } else {
        ((*fdwroutine).EndForeignScan.unwrap())(node as *mut _);
    }

    /* Shut down any outer plan. */
    if !outerPlanState(node as *mut PlanState).is_null() {
        ExecEndNode(outerPlanState(node as *mut PlanState));
    }
}

/* ----------------------------------------------------------------
 *      ExecReScanForeignScan
 *
 *      Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanForeignScan(node: *mut ForeignScanState) {
    let plan = (*node).ss.ps.plan as *mut ForeignScan;
    let estate = (*node).ss.ps.state;
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    /*
     * Ignore direct modifications when EvalPlanQual is active --- they are
     * irrelevant for EvalPlanQual rechecking
     */
    if !(*estate).es_epq_active.is_null() && (*plan).operation != CMD_SELECT {
        return;
    }

    ((*fdwroutine).ReScanForeignScan.unwrap())(node as *mut _);

    /*
     * If chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.  outerPlan may also be NULL, in which case there is
     * nothing to rescan at all.
     */
    if !outerPlan.is_null() && (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }

    ExecScanReScan(&raw mut (*node).ss);
}

/* ----------------------------------------------------------------
 *      ExecForeignScanEstimate
 *
 *      Informs size of the parallel coordination information, if any
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecForeignScanEstimate(node: *mut ForeignScanState, pcxt: *mut ParallelContext) {
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    if (*fdwroutine).EstimateDSMForeignScan.is_some() {
        (*node).pscan_len = ((*fdwroutine).EstimateDSMForeignScan.unwrap())(node as *mut _, pcxt as *mut _);
        shm_toc_estimate_chunk(&raw mut (*pcxt).estimator, (*node).pscan_len);
        shm_toc_estimate_keys(&raw mut (*pcxt).estimator, 1);
    }
}

/* ----------------------------------------------------------------
 *      ExecForeignScanInitializeDSM
 *
 *      Initialize the parallel coordination information
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecForeignScanInitializeDSM(node: *mut ForeignScanState, pcxt: *mut ParallelContext) {
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    if (*fdwroutine).InitializeDSMForeignScan.is_some() {
        let plan_node_id: c_int = (*(*node).ss.ps.plan).plan_node_id;
        let coordinate: *mut c_void;

        coordinate = shm_toc_allocate((*pcxt).toc, (*node).pscan_len);
        ((*fdwroutine).InitializeDSMForeignScan.unwrap())(node as *mut _, pcxt as *mut _, coordinate);
        shm_toc_insert((*pcxt).toc, plan_node_id as uint64, coordinate);
    }
}

/* ----------------------------------------------------------------
 *      ExecForeignScanReInitializeDSM
 *
 *      Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecForeignScanReInitializeDSM(
    node: *mut ForeignScanState,
    pcxt: *mut ParallelContext,
) {
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    if (*fdwroutine).ReInitializeDSMForeignScan.is_some() {
        let plan_node_id: c_int = (*(*node).ss.ps.plan).plan_node_id;
        let coordinate: *mut c_void;

        coordinate = shm_toc_lookup((*pcxt).toc, plan_node_id as uint64, false);
        ((*fdwroutine).ReInitializeDSMForeignScan.unwrap())(node as *mut _, pcxt as *mut _, coordinate);
    }
}

/* ----------------------------------------------------------------
 *      ExecForeignScanInitializeWorker
 *
 *      Initialization according to the parallel coordination information
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecForeignScanInitializeWorker(
    node: *mut ForeignScanState,
    pwcxt: *mut ParallelWorkerContext,
) {
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    if (*fdwroutine).InitializeWorkerForeignScan.is_some() {
        let plan_node_id: c_int = (*(*node).ss.ps.plan).plan_node_id;
        let coordinate: *mut c_void;

        coordinate = shm_toc_lookup((*pwcxt).toc, plan_node_id as uint64, false);
        ((*fdwroutine).InitializeWorkerForeignScan.unwrap())(node as *mut _, (*pwcxt).toc as *mut _, coordinate);
    }
}

/* ----------------------------------------------------------------
 *      ExecShutdownForeignScan
 *
 *      Gives FDW chance to stop asynchronous resource consumption
 *      and release any resources still held.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecShutdownForeignScan(node: *mut ForeignScanState) {
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    if (*fdwroutine).ShutdownForeignScan.is_some() {
        ((*fdwroutine).ShutdownForeignScan.unwrap())(node as *mut _);
    }
}

/* ----------------------------------------------------------------
 *      ExecAsyncForeignScanRequest
 *
 *      Asynchronously request a tuple from a designed async-capable node
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecAsyncForeignScanRequest(areq: *mut AsyncRequest) {
    let node = (*areq).requestee as *mut ForeignScanState;
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    Assert!((*fdwroutine).ForeignAsyncRequest.is_some());
    ((*fdwroutine).ForeignAsyncRequest.unwrap())(areq as *mut _);
}

/* ----------------------------------------------------------------
 *      ExecAsyncForeignScanConfigureWait
 *
 *      In async mode, configure for a wait
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecAsyncForeignScanConfigureWait(areq: *mut AsyncRequest) {
    let node = (*areq).requestee as *mut ForeignScanState;
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    Assert!((*fdwroutine).ForeignAsyncConfigureWait.is_some());
    ((*fdwroutine).ForeignAsyncConfigureWait.unwrap())(areq as *mut _);
}

/* ----------------------------------------------------------------
 *      ExecAsyncForeignScanNotify
 *
 *      Callback invoked when a relevant event has occurred
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecAsyncForeignScanNotify(areq: *mut AsyncRequest) {
    let node = (*areq).requestee as *mut ForeignScanState;
    let fdwroutine = (*node).fdwroutine as *mut FdwRoutine;

    Assert!((*fdwroutine).ForeignAsyncNotify.is_some());
    ((*fdwroutine).ForeignAsyncNotify.unwrap())(areq as *mut _);
}

// ----------------------------------------------------------------
//      ExecScan callback trampolines
//
// ExecScan's callbacks take a `*mut ScanState`; ForeignScanState begins with a
// ScanState (`ss`) as its first field, so the pointer is reinterpreted here.
// ----------------------------------------------------------------
unsafe fn ForeignNext_access(
    node: *mut crate::nodes::execnodes::ScanState,
) -> *mut TupleTableSlot {
    let node = node as *mut ForeignScanState;
    ForeignNext(node)
}

unsafe fn ForeignRecheck_recheck(
    node: *mut crate::nodes::execnodes::ScanState,
    slot: *mut TupleTableSlot,
) -> bool {
    let node = node as *mut ForeignScanState;
    ForeignRecheck(node, slot)
}
