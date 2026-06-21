//! src/backend/executor/nodeBitmapHeapscan.c
//!
//! nodeBitmapHeapscan.c
//!	  Routines to support bitmapped scans of relations
//!
//! NOTE: it is critical that this plan type only be used with MVCC-compliant
//! snapshots (ie, regular snapshots, not SnapshotAny or one of the other
//! special snapshots).  The reason is that since index and heap scans are
//! decoupled, there can be no assurance that the index tuple prompting a
//! visit to a particular heap TID still exists when the visit is made.
//! Therefore the tuple might not exist anymore either (which is OK because
//! heap_fetch will cope) --- but worse, the tuple slot could have been
//! re-used for a newer tuple.  With an MVCC snapshot the newer tuple is
//! certain to fail the time qual and so it will not be mistakenly returned,
//! but with anything else we might return a tuple that doesn't meet the
//! required index qual conditions.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/executor/nodeBitmapHeapscan.c
//!
//! INTERFACE ROUTINES
//!		ExecBitmapHeapScan			scans a relation using bitmap info
//!		ExecBitmapHeapNext			workhorse for above
//!		ExecInitBitmapHeapScan		creates and initializes state info.
//!		ExecReScanBitmapHeapScan	prepares to rescan the plan.
//!		ExecEndBitmapHeapScan		releases all storage.

use crate::prelude::*;
use crate::nodes::tidbitmap::TIDBitmap;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::executor::executor::{EXEC_FLAG_MARK, EXEC_FLAG_BACKWARD};

use std::ffi::{c_int, c_void};

use crate::c::{Size, uint64};

use crate::executor::executor::{ExecScanAccessMtd, ExecScanRecheckMtd};
use crate::nodes::execnodes::{
    outerPlanState, BitmapHeapScanInstrumentation, BitmapHeapScanState, EState, ExprContext,
    ExprState, ParallelBitmapHeapState, PlanState, ScanState, SharedBitmapHeapInstrumentation,
    TupleTableSlot,
};
use crate::nodes::execnodes::SharedBitmapState::*;
use crate::nodes::plannodes::{BitmapHeapScan, Plan};

use crate::{castNode, makeNode, Assert};

// ----- local stubs for not-yet-ported dependencies -----

#[allow(non_camel_case_types)]
#[allow(non_camel_case_types)]
type TBMIterator = crate::nodes::tidbitmap::TBMIterator;
#[allow(non_camel_case_types)]
type dsa_area = c_void;
#[allow(non_camel_case_types)]
type dsa_pointer = crate::c::Size;
#[allow(non_camel_case_types)]
type TableScanDesc = *mut c_void;
#[allow(non_camel_case_types)]
type ParallelContext = c_void;
#[allow(non_camel_case_types)]
type ParallelWorkerContext = c_void;

const InvalidDsaPointer: dsa_pointer = 0;

unsafe fn MultiExecProcNode(_node: *mut PlanState) -> *mut c_void {
    crate::executor::execProcnode::MultiExecProcNode(_node as _) as _
}
unsafe fn IsA_TIDBitmap(_tbm: *mut TIDBitmap) -> bool {
    unimplemented!() // TODO: src/backend/nodes/tidbitmap.c
}
unsafe fn tbm_prepare_shared_iterate(_tbm: *mut TIDBitmap) -> dsa_pointer {
    crate::nodes::tidbitmap::tbm_prepare_shared_iterate(_tbm as _) as _
}
unsafe fn tbm_begin_iterate(
    _tbm: *mut TIDBitmap,
    _dsa: *mut dsa_area,
    _dsp: dsa_pointer,
) -> TBMIterator {
    crate::nodes::tidbitmap::tbm_begin_iterate(_tbm as _, _dsa as _, _dsp as _)
}
unsafe fn tbm_exhausted(_iterator: *mut TBMIterator) -> bool {
    unimplemented!()
}
unsafe fn tbm_end_iterate(_iterator: *mut TBMIterator) {
    crate::nodes::tidbitmap::tbm_end_iterate(_iterator as _)
}
unsafe fn tbm_free(_tbm: *mut TIDBitmap) {
    crate::nodes::tidbitmap::tbm_free(_tbm as _)
}
unsafe fn tbm_free_shared_area(_dsa: *mut dsa_area, _dsp: dsa_pointer) {
    crate::nodes::tidbitmap::tbm_free_shared_area(_dsa as _, _dsp as _)
}
unsafe fn table_beginscan_bm(
    _rel: *mut c_void,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut c_void,
) -> TableScanDesc {
    unimplemented!() // TODO: src/include/access/tableam.h
}
unsafe fn table_scan_bitmap_next_tuple(
    _scan: TableScanDesc,
    _slot: *mut TupleTableSlot,
    _recheck: *mut bool,
    _lossy_pages: *mut uint64,
    _exact_pages: *mut uint64,
) -> bool {
    unimplemented!() // TODO: src/include/access/tableam.h
}
unsafe fn table_rescan(_scan: TableScanDesc, _key: *mut c_void) {
    unimplemented!() // TODO: src/include/access/tableam.h
}
unsafe fn table_endscan(_scan: TableScanDesc) {
    crate::access::table::tableam::table_endscan(_scan as _)
}
unsafe fn table_slot_callbacks(_rel: *mut c_void) -> *const c_void {
    crate::access::table::tableam::table_slot_callbacks(_rel as _) as _
}
unsafe fn ExecQualAndReset(_state: *mut ExprState, _econtext: *mut ExprContext) -> bool {
    crate::executor::executor::ExecQualAndReset(_state as _, _econtext as _) as _
}
unsafe fn ExecClearTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    crate::executor::tuptable::ExecClearTuple(_slot as _) as _
}
unsafe fn ExecScan(
    _node: *mut ScanState,
    _accessMtd: ExecScanAccessMtd,
    _recheckMtd: ExecScanRecheckMtd,
) -> *mut TupleTableSlot {
    unimplemented!() // TODO: src/backend/executor/execScan.c
}
unsafe fn ExecScanReScan(_node: *mut ScanState) {
    crate::executor::execScan::ExecScanReScan(_node as _)
}
unsafe fn ExecReScan(_node: *mut PlanState) {
    crate::executor::execAmi::ExecReScan(_node as _)
}
unsafe fn ExecEndNode(_node: *mut PlanState) {
    crate::executor::execProcnode::ExecEndNode(_node as _)
}
unsafe fn ExecInitNode(_node: *mut Plan, _estate: *mut EState, _eflags: c_int) -> *mut PlanState {
    crate::executor::execProcnode::ExecInitNode(_node as _, _estate as _, _eflags as _) as _
}
unsafe fn ExecAssignExprContext(_estate: *mut EState, _planstate: *mut PlanState) {
    unimplemented!() // TODO: src/backend/executor/execUtils.c
}
unsafe fn ExecOpenScanRelation(
    _estate: *mut EState,
    _scanrelid: crate::c::Index,
    _eflags: c_int,
) -> *mut c_void {
    unimplemented!() // TODO: src/backend/executor/execUtils.c
}
unsafe fn ExecInitScanTupleSlot(
    _estate: *mut EState,
    _scanstate: *mut ScanState,
    _tupledesc: *mut c_void,
    _tts_ops: *const c_void,
) {
    crate::executor::execTuples::ExecInitScanTupleSlot(_estate as _, _scanstate as _, _tupledesc as _, _tts_ops as _)
}
unsafe fn ExecInitResultTypeTL(_planstate: *mut PlanState) {
    crate::executor::execTuples::ExecInitResultTypeTL(_planstate as _)
}
unsafe fn ExecAssignScanProjectionInfo(_node: *mut ScanState) {
    crate::executor::execScan::ExecAssignScanProjectionInfo(_node as _)
}
unsafe fn ExecInitQual(_qual: *mut c_void, _parent: *mut PlanState) -> *mut ExprState {
    crate::executor::execExpr::ExecInitQual(_qual as _, _parent as _) as _
}
unsafe fn RelationGetDescr(_rel: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO: src/include/utils/rel.h
}
unsafe fn IsMVCCSnapshot(_snapshot: *mut c_void) -> bool {
    unimplemented!() // TODO: src/include/utils/snapmgr.h
}
unsafe fn IsParallelWorker() -> bool {
    unimplemented!() // TODO: src/include/miscadmin.h
}
unsafe fn SpinLockAcquire(_lock: *mut crate::nodes::execnodes::slock_t) {
    unimplemented!() // TODO: src/include/storage/spin.h
}
unsafe fn SpinLockRelease(_lock: *mut crate::nodes::execnodes::slock_t) {
    unimplemented!() // TODO: src/include/storage/spin.h
}
unsafe fn SpinLockInit(_lock: *mut crate::nodes::execnodes::slock_t) {
    unimplemented!() // TODO: src/include/storage/spin.h
}
unsafe fn ConditionVariableBroadcast(_cv: *mut crate::nodes::execnodes::ConditionVariable) {
    crate::storage::lmgr::condition_variable::ConditionVariableBroadcast(_cv as _)
}
unsafe fn ConditionVariableInit(_cv: *mut crate::nodes::execnodes::ConditionVariable) {
    unimplemented!() // TODO: src/backend/storage/lmgr/condition_variable.c
}
unsafe fn ConditionVariableSleep(
    _cv: *mut crate::nodes::execnodes::ConditionVariable,
    _wait_event_info: u32,
) {
    crate::storage::lmgr::condition_variable::ConditionVariableSleep(_cv as _, _wait_event_info as _)
}
unsafe fn ConditionVariableCancelSleep() -> bool {
    crate::storage::lmgr::condition_variable::ConditionVariableCancelSleep() as _
}
unsafe fn shm_toc_estimate_chunk(_estimator: *mut c_void, _size: Size) {
    unimplemented!()
}
unsafe fn shm_toc_estimate_keys(_estimator: *mut c_void, _keys: Size) {
    unimplemented!()
}
unsafe fn shm_toc_allocate(_toc: *mut c_void, _nbytes: Size) -> *mut c_void {
    crate::storage::ipc::shm_toc::shm_toc_allocate(_toc as _, _nbytes as _) as _
}
unsafe fn shm_toc_insert(_toc: *mut c_void, _key: u64, _address: *mut c_void) {
    crate::storage::ipc::shm_toc::shm_toc_insert(_toc as _, _key as _, _address as _)
}
unsafe fn shm_toc_lookup(_toc: *mut c_void, _key: u64, _noError: bool) -> *mut c_void {
    crate::storage::ipc::shm_toc::shm_toc_lookup(_toc as _, _key as _, _noError as _) as _
}
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2 // TODO: src/backend/utils/misc/guc.c (faithful add_size with overflow check)
}
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2 // TODO: src/backend/utils/misc/guc.c (faithful mul_size with overflow check)
}

const WAIT_EVENT_PARALLEL_BITMAP_SCAN: u32 = 0; // TODO: src/backend/utils/activity/wait_event.h

// Field accessors on the (opaque) TableScanDesc; faithful to scan->st.rs_tbmiterator.
// The relscan struct is not yet ported, so we route through a stub helper.
unsafe fn scandesc_tbmiterator(_scan: TableScanDesc) -> *mut TBMIterator {
    unimplemented!() // TODO: src/include/access/relscan.h (scan->st.rs_tbmiterator)
}

unsafe fn DsaPointerIsValid(p: dsa_pointer) -> bool {
    p != InvalidDsaPointer
}

unsafe fn InstrCountFiltered2(node: *mut BitmapHeapScanState, delta: u64) {
    // faithful to InstrCountFiltered2() macro in src/include/executor/executor.h
    let instrument = (*node).ss.ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered2 += delta as f64;
    }
}

extern "C" {
    static ParallelWorkerNumber: c_int;
}

/*
 * Do the underlying index scan, build the bitmap, set up the parallel state
 * needed for parallel workers to iterate through the bitmap, and set up the
 * underlying table scan descriptor.
 */
unsafe fn BitmapTableScanSetup(node: *mut BitmapHeapScanState) {
    let mut tbmiterator: TBMIterator = std::mem::zeroed();
    let pstate: *mut ParallelBitmapHeapState = (*node).pstate;
    let dsa: *mut dsa_area = (*(*node).ss.ps.state).es_query_dsa as *mut dsa_area;

    if pstate.is_null() {
        (*node).tbm = MultiExecProcNode(outerPlanState(node as *mut PlanState)) as *mut TIDBitmap;

        if (*node).tbm.is_null() || !IsA_TIDBitmap((*node).tbm) {
            elog!(ERROR, "unrecognized result from subplan");
        }
    } else if BitmapShouldInitializeSharedState(pstate) {
        /*
         * The leader will immediately come out of the function, but others
         * will be blocked until leader populates the TBM and wakes them up.
         */
        (*node).tbm = MultiExecProcNode(outerPlanState(node as *mut PlanState)) as *mut TIDBitmap;
        if (*node).tbm.is_null() || !IsA_TIDBitmap((*node).tbm) {
            elog!(ERROR, "unrecognized result from subplan");
        }

        /*
         * Prepare to iterate over the TBM. This will return the dsa_pointer
         * of the iterator state which will be used by multiple processes to
         * iterate jointly.
         */
        (*pstate).tbmiterator = tbm_prepare_shared_iterate((*node).tbm);

        /* We have initialized the shared state so wake up others. */
        BitmapDoneInitializingSharedState(pstate);
    }

    tbmiterator = tbm_begin_iterate(
        (*node).tbm,
        dsa,
        if !pstate.is_null() {
            (*pstate).tbmiterator
        } else {
            InvalidDsaPointer
        },
    );

    /*
     * If this is the first scan of the underlying table, create the table
     * scan descriptor and begin the scan.
     */
    if (*node).ss.ss_currentScanDesc.is_null() {
        (*node).ss.ss_currentScanDesc = table_beginscan_bm(
            (*node).ss.ss_currentRelation as *mut c_void,
            (*(*node).ss.ps.state).es_snapshot as *mut c_void,
            0,
            std::ptr::null_mut(),
        ) as *mut _;
    }

    *scandesc_tbmiterator((*node).ss.ss_currentScanDesc as TableScanDesc) = tbmiterator;
    (*node).initialized = true;
}

/* ----------------------------------------------------------------
 *		BitmapHeapNext
 *
 *		Retrieve next tuple from the BitmapHeapScan node's currentRelation
 * ----------------------------------------------------------------
 */
unsafe fn BitmapHeapNext(node: *mut ScanState) -> *mut TupleTableSlot {
    let node = node as *mut BitmapHeapScanState;
    let econtext: *mut ExprContext = (*node).ss.ps.ps_ExprContext;
    let slot: *mut TupleTableSlot = (*node).ss.ss_ScanTupleSlot;

    /*
     * If we haven't yet performed the underlying index scan, do it, and begin
     * the iteration over the bitmap.
     */
    if !(*node).initialized {
        BitmapTableScanSetup(node);
    }

    while table_scan_bitmap_next_tuple(
        (*node).ss.ss_currentScanDesc as TableScanDesc,
        slot,
        &raw mut (*node).recheck,
        &raw mut (*node).stats.lossy_pages,
        &raw mut (*node).stats.exact_pages,
    ) {
        /*
         * Continuing in previously obtained page.
         */
        CHECK_FOR_INTERRUPTS();

        /*
         * If we are using lossy info, we have to recheck the qual conditions
         * at every tuple.
         */
        if (*node).recheck {
            (*econtext).ecxt_scantuple = slot;
            if !ExecQualAndReset((*node).bitmapqualorig, econtext) {
                /* Fails recheck, so drop it and loop back for another */
                InstrCountFiltered2(node, 1);
                ExecClearTuple(slot);
                continue;
            }
        }

        /* OK to return this tuple */
        return slot;
    }

    /*
     * if we get here it means we are at the end of the scan..
     */
    ExecClearTuple(slot)
}

/*
 *	BitmapDoneInitializingSharedState - Shared state is initialized
 *
 *	By this time the leader has already populated the TBM and initialized the
 *	shared state so wake up other processes.
 */
#[inline]
unsafe fn BitmapDoneInitializingSharedState(pstate: *mut ParallelBitmapHeapState) {
    SpinLockAcquire(&raw mut (*pstate).mutex);
    (*pstate).state = BM_FINISHED;
    SpinLockRelease(&raw mut (*pstate).mutex);
    ConditionVariableBroadcast(&raw mut (*pstate).cv);
}

/*
 * BitmapHeapRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
unsafe fn BitmapHeapRecheck(node: *mut ScanState, slot: *mut TupleTableSlot) -> bool {
    let node = node as *mut BitmapHeapScanState;
    let econtext: *mut ExprContext;

    /*
     * extract necessary information from index scan node
     */
    econtext = (*node).ss.ps.ps_ExprContext;

    /* Does the tuple meet the original qual conditions? */
    (*econtext).ecxt_scantuple = slot;
    ExecQualAndReset((*node).bitmapqualorig, econtext)
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
unsafe fn ExecBitmapHeapScan(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut BitmapHeapScanState = castNode!(BitmapHeapScanState, T_BitmapHeapScanState, pstate);

    ExecScan(
        &raw mut (*node).ss,
        Some(BitmapHeapNext),
        Some(BitmapHeapRecheck),
    )
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanBitmapHeapScan(node: *mut BitmapHeapScanState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    let scan: TableScanDesc = (*node).ss.ss_currentScanDesc as TableScanDesc;

    if !scan.is_null() {
        /*
         * End iteration on iterators saved in scan descriptor if they have
         * not already been cleaned up.
         */
        if !tbm_exhausted(scandesc_tbmiterator(scan)) {
            tbm_end_iterate(scandesc_tbmiterator(scan));
        }

        /* rescan to release any page pin */
        table_rescan((*node).ss.ss_currentScanDesc as TableScanDesc, std::ptr::null_mut());
    }

    /* release bitmaps and buffers if any */
    if !(*node).tbm.is_null() {
        tbm_free((*node).tbm);
    }
    (*node).tbm = std::ptr::null_mut();
    (*node).initialized = false;
    (*node).recheck = true;

    ExecScanReScan(&raw mut (*node).ss);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapHeapScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndBitmapHeapScan(node: *mut BitmapHeapScanState) {
    let scanDesc: TableScanDesc;

    /*
     * When ending a parallel worker, copy the statistics gathered by the
     * worker back into shared memory so that it can be picked up by the main
     * process to report in EXPLAIN ANALYZE.
     */
    if !(*node).sinstrument.is_null() && IsParallelWorker() {
        let si: *mut BitmapHeapScanInstrumentation;

        Assert!(ParallelWorkerNumber <= (*(*node).sinstrument).num_workers);
        si = (*(*node).sinstrument)
            .sinstrument
            .as_mut_ptr()
            .add(ParallelWorkerNumber as usize);

        /*
         * Here we accumulate the stats rather than performing memcpy on
         * node->stats into si.  When a Gather/GatherMerge node finishes it
         * will perform planner shutdown on the workers.  On rescan it will
         * spin up new workers which will have a new BitmapHeapScanState and
         * zeroed stats.
         */
        (*si).exact_pages += (*node).stats.exact_pages;
        (*si).lossy_pages += (*node).stats.lossy_pages;
    }

    /*
     * extract information from the node
     */
    scanDesc = (*node).ss.ss_currentScanDesc as TableScanDesc;

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));

    if !scanDesc.is_null() {
        /*
         * End iteration on iterators saved in scan descriptor if they have
         * not already been cleaned up.
         */
        if !tbm_exhausted(scandesc_tbmiterator(scanDesc)) {
            tbm_end_iterate(scandesc_tbmiterator(scanDesc));
        }

        /*
         * close table scan
         */
        table_endscan(scanDesc);
    }

    /*
     * release bitmaps and buffers if any
     */
    if !(*node).tbm.is_null() {
        tbm_free((*node).tbm);
    }
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapHeapScan
 *
 *		Initializes the scan's state information.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitBitmapHeapScan(
    node: *mut BitmapHeapScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut BitmapHeapScanState {
    let scanstate: *mut BitmapHeapScanState;
    let currentRelation: *mut c_void;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * Assert caller didn't ask for an unsafe snapshot --- see comments at
     * head of file.
     */
    Assert!(IsMVCCSnapshot((*estate).es_snapshot as *mut c_void));

    /*
     * create state structure
     */
    scanstate = makeNode!(BitmapHeapScanState, T_BitmapHeapScanState);
    (*scanstate).ss.ps.plan = node as *mut Plan;
    (*scanstate).ss.ps.state = estate;
    (*scanstate).ss.ps.ExecProcNode = Some(ExecBitmapHeapScan);

    (*scanstate).tbm = std::ptr::null_mut();

    /* Zero the statistics counters */
    std::ptr::write_bytes(
        &raw mut (*scanstate).stats as *mut u8,
        0,
        std::mem::size_of::<BitmapHeapScanInstrumentation>(),
    );

    (*scanstate).initialized = false;
    (*scanstate).pstate = std::ptr::null_mut();
    (*scanstate).recheck = true;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &raw mut (*scanstate).ss.ps);

    /*
     * open the scan relation
     */
    currentRelation = ExecOpenScanRelation(estate, (*node).scan.scanrelid, eflags);

    /*
     * initialize child nodes
     */
    *outerPlanState_lvalue(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * get the scan type from the relation descriptor.
     */
    ExecInitScanTupleSlot(
        estate,
        &raw mut (*scanstate).ss,
        RelationGetDescr(currentRelation),
        table_slot_callbacks(currentRelation),
    );

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&raw mut (*scanstate).ss.ps);
    ExecAssignScanProjectionInfo(&raw mut (*scanstate).ss);

    /*
     * initialize child expressions
     */
    (*scanstate).ss.ps.qual =
        ExecInitQual((*node).scan.plan.qual as *mut c_void, scanstate as *mut PlanState);
    (*scanstate).bitmapqualorig =
        ExecInitQual((*node).bitmapqualorig as *mut c_void, scanstate as *mut PlanState);

    (*scanstate).ss.ss_currentRelation = currentRelation as *mut _;

    /*
     * all done.
     */
    scanstate
}

/*----------------
 *		BitmapShouldInitializeSharedState
 *
 *		The first process to come here and see the state to the BM_INITIAL
 *		will become the leader for the parallel bitmap scan and will be
 *		responsible for populating the TIDBitmap.  The other processes will
 *		be blocked by the condition variable until the leader wakes them up.
 * ---------------
 */
unsafe fn BitmapShouldInitializeSharedState(pstate: *mut ParallelBitmapHeapState) -> bool {
    let mut state;

    loop {
        SpinLockAcquire(&raw mut (*pstate).mutex);
        state = (*pstate).state;
        if (*pstate).state == BM_INITIAL {
            (*pstate).state = BM_INPROGRESS;
        }
        SpinLockRelease(&raw mut (*pstate).mutex);

        /* Exit if bitmap is done, or if we're the leader. */
        if state != BM_INPROGRESS {
            break;
        }

        /* Wait for the leader to wake us up. */
        ConditionVariableSleep(&raw mut (*pstate).cv, WAIT_EVENT_PARALLEL_BITMAP_SCAN);
    }

    ConditionVariableCancelSleep();

    state == BM_INITIAL
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapHeapEstimate(node: *mut BitmapHeapScanState, pcxt: *mut ParallelContext) {
    let mut size: Size;

    size = MAXALIGN(std::mem::size_of::<ParallelBitmapHeapState>());

    /* account for instrumentation, if required */
    if !(*node).ss.ps.instrument.is_null() && pcxt_nworkers(pcxt) > 0 {
        size = add_size(
            size,
            offset_of_sinstrument(),
        );
        size = add_size(
            size,
            mul_size(
                pcxt_nworkers(pcxt) as Size,
                std::mem::size_of::<BitmapHeapScanInstrumentation>(),
            ),
        );
    }

    shm_toc_estimate_chunk(pcxt_estimator(pcxt), size);
    shm_toc_estimate_keys(pcxt_estimator(pcxt), 1);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapInitializeDSM
 *
 *		Set up a parallel bitmap heap scan descriptor.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapHeapInitializeDSM(
    node: *mut BitmapHeapScanState,
    pcxt: *mut ParallelContext,
) {
    let pstate: *mut ParallelBitmapHeapState;
    let mut sinstrument: *mut SharedBitmapHeapInstrumentation = std::ptr::null_mut();
    let dsa: *mut dsa_area = (*(*node).ss.ps.state).es_query_dsa as *mut dsa_area;
    let mut ptr: *mut u8;
    let mut size: Size;

    /* If there's no DSA, there are no workers; initialize nothing. */
    if dsa.is_null() {
        return;
    }

    size = MAXALIGN(std::mem::size_of::<ParallelBitmapHeapState>());
    if !(*node).ss.ps.instrument.is_null() && pcxt_nworkers(pcxt) > 0 {
        size = add_size(size, offset_of_sinstrument());
        size = add_size(
            size,
            mul_size(
                pcxt_nworkers(pcxt) as Size,
                std::mem::size_of::<BitmapHeapScanInstrumentation>(),
            ),
        );
    }

    ptr = shm_toc_allocate(pcxt_toc(pcxt), size) as *mut u8;
    pstate = ptr as *mut ParallelBitmapHeapState;
    ptr = ptr.add(MAXALIGN(std::mem::size_of::<ParallelBitmapHeapState>()));
    if !(*node).ss.ps.instrument.is_null() && pcxt_nworkers(pcxt) > 0 {
        sinstrument = ptr as *mut SharedBitmapHeapInstrumentation;
    }

    (*pstate).tbmiterator = 0;

    /* Initialize the mutex */
    SpinLockInit(&raw mut (*pstate).mutex);
    (*pstate).state = BM_INITIAL;

    ConditionVariableInit(&raw mut (*pstate).cv);

    if !sinstrument.is_null() {
        (*sinstrument).num_workers = pcxt_nworkers(pcxt);

        /* ensure any unfilled slots will contain zeroes */
        std::ptr::write_bytes(
            (*sinstrument).sinstrument.as_mut_ptr() as *mut u8,
            0,
            pcxt_nworkers(pcxt) as usize * std::mem::size_of::<BitmapHeapScanInstrumentation>(),
        );
    }

    shm_toc_insert(
        pcxt_toc(pcxt),
        (*(*node).ss.ps.plan).plan_node_id as u64,
        pstate as *mut c_void,
    );
    (*node).pstate = pstate;
    (*node).sinstrument = sinstrument;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapHeapReInitializeDSM(
    node: *mut BitmapHeapScanState,
    _pcxt: *mut ParallelContext,
) {
    let pstate: *mut ParallelBitmapHeapState = (*node).pstate;
    let dsa: *mut dsa_area = (*(*node).ss.ps.state).es_query_dsa as *mut dsa_area;

    /* If there's no DSA, there are no workers; do nothing. */
    if dsa.is_null() {
        return;
    }

    (*pstate).state = BM_INITIAL;

    if DsaPointerIsValid((*pstate).tbmiterator) {
        tbm_free_shared_area(dsa, (*pstate).tbmiterator);
    }

    (*pstate).tbmiterator = InvalidDsaPointer;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapHeapInitializeWorker(
    node: *mut BitmapHeapScanState,
    pwcxt: *mut ParallelWorkerContext,
) {
    let mut ptr: *mut u8;

    Assert!(!(*(*node).ss.ps.state).es_query_dsa.is_null());

    ptr = shm_toc_lookup(
        pwcxt_toc(pwcxt),
        (*(*node).ss.ps.plan).plan_node_id as u64,
        false,
    ) as *mut u8;

    (*node).pstate = ptr as *mut ParallelBitmapHeapState;
    ptr = ptr.add(MAXALIGN(std::mem::size_of::<ParallelBitmapHeapState>()));

    if !(*node).ss.ps.instrument.is_null() {
        (*node).sinstrument = ptr as *mut SharedBitmapHeapInstrumentation;
    }
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapRetrieveInstrumentation
 *
 *		Transfer bitmap heap scan statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapHeapRetrieveInstrumentation(node: *mut BitmapHeapScanState) {
    let sinstrument: *mut SharedBitmapHeapInstrumentation = (*node).sinstrument;
    let size: Size;

    if sinstrument.is_null() {
        return;
    }

    size = offset_of_sinstrument()
        + (*sinstrument).num_workers as Size
            * std::mem::size_of::<BitmapHeapScanInstrumentation>();

    (*node).sinstrument = palloc(size) as *mut SharedBitmapHeapInstrumentation;
    std::ptr::copy_nonoverlapping(
        sinstrument as *const u8,
        (*node).sinstrument as *mut u8,
        size,
    );
}

// ----- local lvalue/accessor helpers for fields on not-yet-ported opaque types -----

/// `outerPlan(node)`: the outer (left) child plan of a BitmapHeapScan plan node.
unsafe fn outerPlan(node: *mut BitmapHeapScan) -> *mut Plan {
    (*node).scan.plan.lefttree
}

/// `outerPlanState(node) = ...`: lvalue access to the outer child PlanState.
unsafe fn outerPlanState_lvalue(node: *mut BitmapHeapScanState) -> *mut *mut PlanState {
    &raw mut (*node).ss.ps.lefttree
}

/// offsetof(SharedBitmapHeapInstrumentation, sinstrument)
unsafe fn offset_of_sinstrument() -> Size {
    let base = std::ptr::null::<SharedBitmapHeapInstrumentation>();
    (&raw const (*base).sinstrument) as usize - base as usize
}

// ParallelContext / ParallelWorkerContext field accessors (structs not yet ported).
unsafe fn pcxt_nworkers(_pcxt: *mut ParallelContext) -> c_int {
    unimplemented!() // TODO: src/include/access/parallel.h (pcxt->nworkers)
}
unsafe fn pcxt_estimator(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: src/include/access/parallel.h (&pcxt->estimator)
}
unsafe fn pcxt_toc(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: src/include/access/parallel.h (pcxt->toc)
}
unsafe fn pwcxt_toc(_pwcxt: *mut ParallelWorkerContext) -> *mut c_void {
    unimplemented!() // TODO: src/include/access/parallel.h (pwcxt->toc)
}
