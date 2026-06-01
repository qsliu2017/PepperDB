//! Support routines for sequential scans of relations.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Translated 1:1 from:
//!   postgres/src/backend/executor/nodeSeqscan.c
//!   postgres/src/include/executor/nodeSeqscan.h
//!
//! INTERFACE ROUTINES
//!		ExecSeqScan				sequentially scans a relation.
//!		ExecSeqNext				retrieve next tuple in sequential order.
//!		ExecInitSeqScan			creates and initializes a seqscan node.
//!		ExecEndSeqScan			releases any storage allocated.
//!		ExecReScanSeqScan		rescans the relation
//!
//!		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
//!		ExecSeqScanInitializeDSM initialize DSM for parallel scan
//!		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
//!		ExecSeqScanInitializeWorker attach to DSM info in parallel worker

use crate::prelude::*;
use crate::executor::executor::{ExecScanAccessMtd, ExecScanRecheckMtd};

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

use std::ffi::c_int;
use std::ptr;

use crate::{castNode, makeNode, IsA};

use crate::nodes::execnodes::{
    EState, PlanState, ScanState, SeqScanState, TupleTableSlot,
};
use crate::nodes::nodes::NodeTag;
use crate::nodes::plannodes::SeqScan;

// ----------------------------------------------------------------
// Local stub types for not-yet-ported dependencies.
// ----------------------------------------------------------------

type TableScanDesc = *mut c_void;
type ScanDirection = c_int;
type ParallelTableScanDesc = *mut c_void;
type ParallelContext = c_void;
type ParallelWorkerContext = c_void;

// Function-pointer method types used by the ExecScan family.

// ----------------------------------------------------------------
// Local stubs for unported helper functions we call.
// ----------------------------------------------------------------

unsafe fn table_beginscan(
    _rel: *mut c_void,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut c_void,
) -> TableScanDesc {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_scan_getnextslot(
    _sscan: TableScanDesc,
    _direction: ScanDirection,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_endscan(_scan: TableScanDesc) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_rescan(_scan: TableScanDesc, _key: *mut c_void) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_slot_callbacks(_rel: *mut c_void) -> *const c_void {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_parallelscan_estimate(_rel: *mut c_void, _snapshot: *mut c_void) -> crate::c::Size {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_parallelscan_initialize(
    _rel: *mut c_void,
    _pscan: ParallelTableScanDesc,
    _snapshot: *mut c_void,
) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_beginscan_parallel(
    _rel: *mut c_void,
    _pscan: ParallelTableScanDesc,
) -> TableScanDesc {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn table_parallelscan_reinitialize(_rel: *mut c_void, _pscan: ParallelTableScanDesc) {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn ExecScanExtended(
    _node: *mut ScanState,
    _access_mtd: ExecScanAccessMtd,
    _recheck_mtd: ExecScanRecheckMtd,
    _epqstate: *mut c_void,
    _qual: *mut c_void,
    _projinfo: *mut c_void,
) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/execScan.h
}

unsafe fn ExecScan(
    _node: *mut ScanState,
    _access_mtd: ExecScanAccessMtd,
    _recheck_mtd: ExecScanRecheckMtd,
) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor/execScan.h
}

unsafe fn ExecScanReScan(_node: *mut ScanState) {
    unimplemented!() // TODO: executor/execScan.c
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
    unimplemented!() // TODO: executor/execTuples.c
}

unsafe fn ExecInitResultTypeTL(_ps: *mut PlanState) {
    unimplemented!() // TODO: executor/execTuples.c
}

unsafe fn ExecAssignScanProjectionInfo(_node: *mut ScanState) {
    unimplemented!() // TODO: executor/execScan.c
}

unsafe fn ExecInitQual(_qual: *mut c_void, _parent: *mut PlanState) -> *mut c_void {
    unimplemented!() // TODO: executor/execExpr.c
}

unsafe fn RelationGetDescr(_rel: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn shm_toc_estimate_chunk(_estimator: *mut c_void, _size: crate::c::Size) {
    unimplemented!() // TODO: storage/shm_toc.h
}

unsafe fn shm_toc_estimate_keys(_estimator: *mut c_void, _cnt: crate::c::Size) {
    unimplemented!() // TODO: storage/shm_toc.h
}

unsafe fn shm_toc_allocate(_toc: *mut c_void, _nbytes: crate::c::Size) -> *mut c_void {
    unimplemented!() // TODO: storage/shm_toc.h
}

unsafe fn shm_toc_insert(_toc: *mut c_void, _key: u64, _address: *mut c_void) {
    unimplemented!() // TODO: storage/shm_toc.h
}

unsafe fn shm_toc_lookup(_toc: *mut c_void, _key: u64, _noError: bool) -> *mut c_void {
    unimplemented!() // TODO: storage/shm_toc.h
}

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
unsafe fn SeqNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut SeqScanState;
    let scandesc: TableScanDesc;
    let estate: *mut EState;
    let direction: ScanDirection;
    let slot: *mut TupleTableSlot;

    /*
     * get information from the estate and scan state
     */
    scandesc = (*node).ss.ss_currentScanDesc as TableScanDesc;
    estate = (*node).ss.ps.state;
    direction = (*estate).es_direction as ScanDirection;
    slot = (*node).ss.ss_ScanTupleSlot;

    let scandesc = if scandesc.is_null() {
        /*
         * We reach here if the scan is not parallel, or if we're serially
         * executing a scan that was planned to be parallel.
         */
        let sd = table_beginscan(
            (*node).ss.ss_currentRelation as *mut c_void,
            (*estate).es_snapshot as *mut c_void,
            0,
            ptr::null_mut(),
        );
        (*node).ss.ss_currentScanDesc = sd as *mut _;
        sd
    } else {
        scandesc
    };

    /*
     * get the next tuple from the table
     */
    if table_scan_getnextslot(scandesc, direction, slot) {
        return slot;
    }
    ptr::null_mut()
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn SeqRecheck(_node: *mut ScanState, _slot: *mut TupleTableSlot) -> bool {
    /*
     * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
     * (and this is very bad) - so, here we do not check are keys ok or not.
     */
    true
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple. This variant is used when there is no es_epq_active, no qual
 *		and no projection.  Passing const-NULLs for these to ExecScanExtended
 *		allows the compiler to eliminate the additional code that would
 *		ordinarily be required for the evaluation of these.
 * ----------------------------------------------------------------
 */
unsafe fn ExecSeqScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SeqScanState = castNode!(SeqScanState, T_SeqScanState, pstate);

    Assert!((*(*pstate).state).es_epq_active.is_null());
    Assert!((*pstate).qual.is_null());
    Assert!((*pstate).ps_ProjInfo.is_null());

    ExecScanExtended(
        &mut (*node).ss,
        Some(SeqNext),
        Some(SeqRecheck),
        ptr::null_mut(),
        ptr::null_mut(),
        ptr::null_mut(),
    )
}

/*
 * Variant of ExecSeqScan() but when qual evaluation is required.
 */
unsafe fn ExecSeqScanWithQual(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SeqScanState = castNode!(SeqScanState, T_SeqScanState, pstate);

    Assert!((*(*pstate).state).es_epq_active.is_null());
    Assert!(!(*pstate).qual.is_null());
    Assert!((*pstate).ps_ProjInfo.is_null());

    ExecScanExtended(
        &mut (*node).ss,
        Some(SeqNext),
        Some(SeqRecheck),
        ptr::null_mut(),
        (*pstate).qual as *mut c_void,
        ptr::null_mut(),
    )
}

/*
 * Variant of ExecSeqScan() but when projection is required.
 */
unsafe fn ExecSeqScanWithProject(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SeqScanState = castNode!(SeqScanState, T_SeqScanState, pstate);

    Assert!((*(*pstate).state).es_epq_active.is_null());
    Assert!((*pstate).qual.is_null());
    Assert!(!(*pstate).ps_ProjInfo.is_null());

    ExecScanExtended(
        &mut (*node).ss,
        Some(SeqNext),
        Some(SeqRecheck),
        ptr::null_mut(),
        ptr::null_mut(),
        (*pstate).ps_ProjInfo as *mut c_void,
    )
}

/*
 * Variant of ExecSeqScan() but when qual evaluation and projection are
 * required.
 */
unsafe fn ExecSeqScanWithQualProject(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SeqScanState = castNode!(SeqScanState, T_SeqScanState, pstate);

    Assert!((*(*pstate).state).es_epq_active.is_null());
    Assert!(!(*pstate).qual.is_null());
    Assert!(!(*pstate).ps_ProjInfo.is_null());

    ExecScanExtended(
        &mut (*node).ss,
        Some(SeqNext),
        Some(SeqRecheck),
        ptr::null_mut(),
        (*pstate).qual as *mut c_void,
        (*pstate).ps_ProjInfo as *mut c_void,
    )
}

/*
 * Variant of ExecSeqScan for when EPQ evaluation is required.  We don't
 * bother adding variants of this for with/without qual and projection as
 * EPQ doesn't seem as exciting a case to optimize for.
 */
unsafe fn ExecSeqScanEPQ(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SeqScanState = castNode!(SeqScanState, T_SeqScanState, pstate);

    ExecScan(
        &mut (*node).ss,
        Some(SeqNext),
        Some(SeqRecheck),
    )
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitSeqScan(
    node: *mut SeqScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut SeqScanState {
    let scanstate: *mut SeqScanState;

    /*
     * Once upon a time it was possible to have an outerPlan of a SeqScan, but
     * not any more.
     */
    Assert!(outerPlan!(node).is_null());
    Assert!(innerPlan!(node).is_null());

    /*
     * create state structure
     */
    scanstate = makeNode!(SeqScanState, T_SeqScanState);
    (*scanstate).ss.ps.plan = node as *mut _;
    (*scanstate).ss.ps.state = estate;

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
        ExecOpenScanRelation(estate, (*node).scan.scanrelid, eflags) as *mut _;

    /* and create slot with the appropriate rowtype */
    ExecInitScanTupleSlot(
        estate,
        &mut (*scanstate).ss,
        RelationGetDescr((*scanstate).ss.ss_currentRelation as *mut c_void),
        table_slot_callbacks((*scanstate).ss.ss_currentRelation as *mut c_void),
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
        ExecInitQual((*node).scan.plan.qual as *mut c_void, scanstate as *mut PlanState) as *mut _;

    /*
     * When EvalPlanQual() is not in use, assign ExecProcNode for this node
     * based on the presence of qual and projection. Each ExecSeqScan*()
     * variant is optimized for the specific combination of these conditions.
     */
    if !(*(*scanstate).ss.ps.state).es_epq_active.is_null() {
        (*scanstate).ss.ps.ExecProcNode = Some(ExecSeqScanEPQ);
    } else if (*scanstate).ss.ps.qual.is_null() {
        if (*scanstate).ss.ps.ps_ProjInfo.is_null() {
            (*scanstate).ss.ps.ExecProcNode = Some(ExecSeqScan);
        } else {
            (*scanstate).ss.ps.ExecProcNode = Some(ExecSeqScanWithProject);
        }
    } else {
        if (*scanstate).ss.ps.ps_ProjInfo.is_null() {
            (*scanstate).ss.ps.ExecProcNode = Some(ExecSeqScanWithQual);
        } else {
            (*scanstate).ss.ps.ExecProcNode = Some(ExecSeqScanWithQualProject);
        }
    }

    scanstate
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndSeqScan(node: *mut SeqScanState) {
    let scanDesc: TableScanDesc;

    /*
     * get information from node
     */
    scanDesc = (*node).ss.ss_currentScanDesc as TableScanDesc;

    /*
     * close heap scan
     */
    if !scanDesc.is_null() {
        table_endscan(scanDesc);
    }
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanSeqScan(node: *mut SeqScanState) {
    let scan: TableScanDesc;

    scan = (*node).ss.ss_currentScanDesc as TableScanDesc;

    if !scan.is_null() {
        table_rescan(scan, /* scan desc */
                     ptr::null_mut()); /* new scan keys */
    }

    ExecScanReScan(node as *mut ScanState);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSeqScanEstimate(node: *mut SeqScanState, pcxt: *mut ParallelContext) {
    let estate: *mut EState = (*node).ss.ps.state;

    (*node).pscan_len = table_parallelscan_estimate(
        (*node).ss.ss_currentRelation as *mut c_void,
        (*estate).es_snapshot as *mut c_void,
    );
    shm_toc_estimate_chunk(estimator_of(pcxt), (*node).pscan_len);
    shm_toc_estimate_keys(estimator_of(pcxt), 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSeqScanInitializeDSM(node: *mut SeqScanState, pcxt: *mut ParallelContext) {
    let estate: *mut EState = (*node).ss.ps.state;
    let pscan: ParallelTableScanDesc;

    pscan = shm_toc_allocate(toc_of(pcxt), (*node).pscan_len) as ParallelTableScanDesc;
    table_parallelscan_initialize(
        (*node).ss.ss_currentRelation as *mut c_void,
        pscan,
        (*estate).es_snapshot as *mut c_void,
    );
    shm_toc_insert(
        toc_of(pcxt),
        (*(*node).ss.ps.plan).plan_node_id as u64,
        pscan,
    );
    (*node).ss.ss_currentScanDesc =
        table_beginscan_parallel((*node).ss.ss_currentRelation as *mut c_void, pscan) as *mut _;
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSeqScanReInitializeDSM(node: *mut SeqScanState, _pcxt: *mut ParallelContext) {
    let pscan: ParallelTableScanDesc;

    pscan = rs_parallel_of((*node).ss.ss_currentScanDesc as TableScanDesc);
    table_parallelscan_reinitialize((*node).ss.ss_currentRelation as *mut c_void, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecSeqScanInitializeWorker(
    node: *mut SeqScanState,
    pwcxt: *mut ParallelWorkerContext,
) {
    let pscan: ParallelTableScanDesc;

    pscan = shm_toc_lookup(
        toc_of(pwcxt),
        (*(*node).ss.ps.plan).plan_node_id as u64,
        false,
    ) as ParallelTableScanDesc;
    (*node).ss.ss_currentScanDesc =
        table_beginscan_parallel((*node).ss.ss_currentRelation as *mut c_void, pscan) as *mut _;
}

// ----------------------------------------------------------------
// Field-access helper stubs for opaque parallel/scan structs.
// (pcxt->estimator, pcxt->toc, scandesc->rs_parallel)
// ----------------------------------------------------------------

unsafe fn estimator_of(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (ParallelContext.estimator)
}

unsafe fn toc_of(_cxt: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (ParallelContext.toc / ParallelWorkerContext.toc)
}

unsafe fn rs_parallel_of(_scan: TableScanDesc) -> ParallelTableScanDesc {
    unimplemented!() // TODO: access/relscan.h (TableScanDescData.rs_parallel)
}
