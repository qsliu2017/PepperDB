//! Translation of postgres/src/backend/executor/nodeCustom.c
//!                + postgres/src/include/executor/nodeCustom.h
//!
//! Routines to handle execution of custom scan node.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::common::tupdesc::TupleDesc;
use crate::executor::execScan::ExecAssignScanProjectionInfoWithVarno;
use crate::executor::execTuples::TTSOpsVirtual;
use crate::executor::executor::{
    ExecAssignExprContext, ExecInitQual, ExecInitResultTupleSlotTL, ExecInitScanTupleSlot,
    ExecOpenScanRelation, ExecTypeFromTL,
};
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::nodes::execnodes::{
    CustomScanState, EState, PlanState, ScanState, TupleTableSlot, TupleTableSlotOps,
};
use crate::nodes::extensible::{ParallelContext, shm_toc};
use crate::nodes::pg_list::NIL;
use crate::nodes::plannodes::CustomScan;
use crate::nodes::primnodes::INDEX_VAR;
use crate::utils::rel::{Relation, RelationGetDescr};
use crate::{castNode, Assert};
use core::ffi::{c_int, c_void};
use core::ptr;

use crate::nodes::nodes::NodeTag::T_CustomScanState;

/* errcodes.h classifications (errcode() shim ignores the value). */
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;

// ----------------------------------------------------------------
//	Opaque parallel-execution types (access/parallel.h not yet ported)
// ----------------------------------------------------------------

/// TODO(pg-port): access/parallel.h
#[repr(C)]
pub struct ParallelWorkerContext {
    _opaque: [u8; 0],
}

pub unsafe fn ExecInitCustomScan(
    cscan: *mut CustomScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut CustomScanState {
    let css: *mut CustomScanState;
    let mut slotOps: *const TupleTableSlotOps;
    let mut scan_rel: Relation = ptr::null_mut();
    let scanrelid: Index = (*cscan).scan.scanrelid;
    let tlistvarno: c_int;

    /*
     * Allocate the CustomScanState object.  We let the custom scan provider
     * do the palloc, in case it wants to make a larger object that embeds
     * CustomScanState as the first field.  It must set the node tag and the
     * methods field correctly at this time.  Other standard fields should be
     * set to zero.
     */
    css = castNode!(
        CustomScanState,
        T_CustomScanState,
        ((*(*cscan).methods).CreateCustomScanState.unwrap())(cscan)
    );

    /* ensure flags is filled correctly */
    (*css).flags = (*cscan).flags;

    /* fill up fields of ScanState */
    (*css).ss.ps.plan = &mut (*cscan).scan.plan;
    (*css).ss.ps.state = estate;
    (*css).ss.ps.ExecProcNode = Some(ExecCustomScan);

    /* create expression context for node */
    ExecAssignExprContext(estate, &mut (*css).ss.ps);

    /*
     * open the scan relation, if any
     */
    if scanrelid > 0 {
        scan_rel = ExecOpenScanRelation(estate, scanrelid, eflags);
        (*css).ss.ss_currentRelation = scan_rel;
    }

    /*
     * Use a custom slot if specified in CustomScanState or use virtual slot
     * otherwise.
     */
    slotOps = (*css).slotOps;
    if slotOps.is_null() {
        slotOps = &TTSOpsVirtual;
    }

    /*
     * Determine the scan tuple type.  If the custom scan provider provided a
     * targetlist describing the scan tuples, use that; else use base
     * relation's rowtype.
     */
    if (*cscan).custom_scan_tlist != NIL || scan_rel.is_null() {
        let scan_tupdesc: TupleDesc;

        scan_tupdesc = ExecTypeFromTL((*cscan).custom_scan_tlist);
        ExecInitScanTupleSlot(estate, &mut (*css).ss, scan_tupdesc, slotOps);
        /* Node's targetlist will contain Vars with varno = INDEX_VAR */
        tlistvarno = INDEX_VAR;
    } else {
        ExecInitScanTupleSlot(estate, &mut (*css).ss, RelationGetDescr(scan_rel), slotOps);
        /* Node's targetlist will contain Vars with varno = scanrelid */
        tlistvarno = scanrelid as c_int;
    }

    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlotTL(&mut (*css).ss.ps, &TTSOpsVirtual);
    ExecAssignScanProjectionInfoWithVarno(&mut (*css).ss, tlistvarno);

    /* initialize child expressions */
    (*css).ss.ps.qual = ExecInitQual((*cscan).scan.plan.qual, css as *mut PlanState);

    /*
     * The callback of custom-scan provider applies the final initialization
     * of the custom-scan-state node according to its logic.
     */
    ((*(*css).methods).BeginCustomScan.unwrap())(css, estate, eflags);

    css
}

unsafe fn ExecCustomScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut CustomScanState = castNode!(CustomScanState, T_CustomScanState, pstate);

    CHECK_FOR_INTERRUPTS();

    Assert!((*(*node).methods).ExecCustomScan.is_some());
    ((*(*node).methods).ExecCustomScan.unwrap())(node)
}

pub unsafe fn ExecEndCustomScan(node: *mut CustomScanState) {
    Assert!((*(*node).methods).EndCustomScan.is_some());
    ((*(*node).methods).EndCustomScan.unwrap())(node);
}

pub unsafe fn ExecReScanCustomScan(node: *mut CustomScanState) {
    Assert!((*(*node).methods).ReScanCustomScan.is_some());
    ((*(*node).methods).ReScanCustomScan.unwrap())(node);
}

pub unsafe fn ExecCustomMarkPos(node: *mut CustomScanState) {
    if (*(*node).methods).MarkPosCustomScan.is_none() {
        ereport!(
            ERROR,
            "custom scan does not support MarkPos"
        );
    }
    ((*(*node).methods).MarkPosCustomScan.unwrap())(node);
}

pub unsafe fn ExecCustomRestrPos(node: *mut CustomScanState) {
    if (*(*node).methods).RestrPosCustomScan.is_none() {
        ereport!(
            ERROR,
            "custom scan does not support MarkPos"
        );
    }
    ((*(*node).methods).RestrPosCustomScan.unwrap())(node);
}

pub unsafe fn ExecCustomScanEstimate(node: *mut CustomScanState, pcxt: *mut ParallelContext) {
    let methods = (*node).methods;

    if (*methods).EstimateDSMCustomScan.is_some() {
        (*node).pscan_len = ((*methods).EstimateDSMCustomScan.unwrap())(node, pcxt);
        shm_toc_estimate_chunk(pcxt, (*node).pscan_len);
        shm_toc_estimate_keys(pcxt, 1);
    }
}

pub unsafe fn ExecCustomScanInitializeDSM(node: *mut CustomScanState, pcxt: *mut ParallelContext) {
    let methods = (*node).methods;

    if (*methods).InitializeDSMCustomScan.is_some() {
        let plan_node_id: c_int = (*(*node).ss.ps.plan).plan_node_id;
        let coordinate: *mut c_void;

        coordinate = shm_toc_allocate(parallel_context_toc(pcxt), (*node).pscan_len);
        ((*methods).InitializeDSMCustomScan.unwrap())(node, pcxt, coordinate);
        shm_toc_insert(parallel_context_toc(pcxt), plan_node_id as uint64, coordinate);
    }
}

pub unsafe fn ExecCustomScanReInitializeDSM(
    node: *mut CustomScanState,
    pcxt: *mut ParallelContext,
) {
    let methods = (*node).methods;

    if (*methods).ReInitializeDSMCustomScan.is_some() {
        let plan_node_id: c_int = (*(*node).ss.ps.plan).plan_node_id;
        let coordinate: *mut c_void;

        coordinate = shm_toc_lookup(parallel_context_toc(pcxt), plan_node_id as uint64, false);
        ((*methods).ReInitializeDSMCustomScan.unwrap())(node, pcxt, coordinate);
    }
}

pub unsafe fn ExecCustomScanInitializeWorker(
    node: *mut CustomScanState,
    pwcxt: *mut ParallelWorkerContext,
) {
    let methods = (*node).methods;

    if (*methods).InitializeWorkerCustomScan.is_some() {
        let plan_node_id: c_int = (*(*node).ss.ps.plan).plan_node_id;
        let coordinate: *mut c_void;

        coordinate = shm_toc_lookup(
            parallel_worker_context_toc(pwcxt),
            plan_node_id as uint64,
            false,
        );
        ((*methods).InitializeWorkerCustomScan.unwrap())(
            node,
            parallel_worker_context_toc(pwcxt),
            coordinate,
        );
    }
}

pub unsafe fn ExecShutdownCustomScan(node: *mut CustomScanState) {
    let methods = (*node).methods;

    if (*methods).ShutdownCustomScan.is_some() {
        ((*methods).ShutdownCustomScan.unwrap())(node);
    }
}

// ----------------------------------------------------------------
//	Local stubs for helpers whose deps are not yet ported.
//	(access/parallel.h ParallelContext/ParallelWorkerContext are opaque;
//	 their estimator/toc members are not yet available.)
// ----------------------------------------------------------------

unsafe fn shm_toc_estimate_chunk(_pcxt: *mut ParallelContext, _sz: Size) {
    unimplemented!() // TODO: storage/ipc/shm_toc.c (needs pcxt->estimator)
}

unsafe fn shm_toc_estimate_keys(_pcxt: *mut ParallelContext, _cnt: Size) {
    unimplemented!() // TODO: storage/ipc/shm_toc.c (needs pcxt->estimator)
}

unsafe fn parallel_context_toc(_pcxt: *mut ParallelContext) -> *mut shm_toc {
    unimplemented!() // TODO: access/parallel.c (pcxt->toc)
}

unsafe fn parallel_worker_context_toc(_pwcxt: *mut ParallelWorkerContext) -> *mut shm_toc {
    unimplemented!() // TODO: access/parallel.c (pwcxt->toc)
}

unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _nbytes: Size) -> *mut c_void {
    unimplemented!() // TODO: storage/ipc/shm_toc.c
}

unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: uint64, _address: *mut c_void) {
    unimplemented!() // TODO: storage/ipc/shm_toc.c
}

unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: uint64, _noError: bool) -> *mut c_void {
    unimplemented!() // TODO: storage/ipc/shm_toc.c
}
