//! nodeBitmapIndexscan.c
//!   Routines to support bitmapped index scans of relations
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/executor/nodeBitmapIndexscan.c
//!   src/include/executor/nodeBitmapIndexscan.h
//!
//! INTERFACE ROUTINES
//!     MultiExecBitmapIndexScan    scans a relation using index.
//!     ExecInitBitmapIndexScan     creates and initializes state info.
//!     ExecReScanBitmapIndexScan   prepares to rescan the plan.
//!     ExecEndBitmapIndexScan      releases all storage.

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::mem::size_of;
use std::ptr;

use crate::executor::executor::{
    exec_rt_fetch, ExecAssignExprContext, ExecReScan, EXEC_FLAG_BACKWARD,
    EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_MARK,
};
use crate::executor::execUtils::ResetExprContext;
use crate::executor::instrument::{InstrStartNode, InstrStopNode};
use crate::miscadmin::{work_mem, CHECK_FOR_INTERRUPTS};
use crate::nodes::execnodes::{
    BitmapIndexScanState, EState, ExprContext, IndexArrayKeyInfo,
    IndexRuntimeKeyInfo, IndexScanInstrumentation, PlanState, ScanKeyData,
    SharedIndexScanInstrumentation, TupleTableSlot,
};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::plannodes::{BitmapIndexScan, Plan};
use crate::nodes::tidbitmap::{tbm_create, TIDBitmap};
use crate::storage::lockdefs::{LOCKMODE, NoLock};
use crate::utils::rel::Relation;
use crate::{makeNode, Assert};

// ----------------------------------------------------------------
// Local stub types for not-yet-ported dependencies.
// ----------------------------------------------------------------

type IndexScanDesc = *mut c_void;
type ParallelContext = c_void;
type ParallelWorkerContext = c_void;

// ----------------------------------------------------------------
// Local stubs for unported helper functions we call.
// ----------------------------------------------------------------

unsafe fn index_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    crate::access::index::indexam::index_open(_relationId as _, _lockmode as _) as _
}

unsafe fn index_close(_relation: Relation, _lockmode: LOCKMODE) {
    crate::access::index::indexam::index_close(_relation as _, _lockmode as _)
}

unsafe fn index_beginscan_bitmap(
    _indexRelation: Relation,
    _snapshot: *mut c_void,
    _instrument: *mut IndexScanInstrumentation,
    _nkeys: c_int,
) -> IndexScanDesc {
    crate::access::index::indexam::index_beginscan_bitmap(_indexRelation as _, _snapshot as _, _instrument as _, _nkeys as _) as _
}

unsafe fn index_rescan(
    _scan: IndexScanDesc,
    _keys: *mut ScanKeyData,
    _nkeys: c_int,
    _orderbys: *mut ScanKeyData,
    _norderbys: c_int,
) {
    crate::access::index::indexam::index_rescan(_scan as _, _keys as _, _nkeys as _, _orderbys as _, _norderbys as _)
}

unsafe fn index_endscan(_scan: IndexScanDesc) {
    crate::access::index::indexam::index_endscan(_scan as _)
}

unsafe fn index_getbitmap(_scan: IndexScanDesc, _bitmap: *mut TIDBitmap) -> i64 {
    crate::access::index::indexam::index_getbitmap(_scan as _, _bitmap as _) as _
}

unsafe fn ExecIndexBuildScanKeys(
    _planstate: *mut PlanState,
    _index: Relation,
    _quals: *mut crate::nodes::pg_list::List,
    _isorderby: bool,
    _scanKeys: *mut *mut ScanKeyData,
    _numScanKeys: *mut c_int,
    _runtimeKeys: *mut *mut IndexRuntimeKeyInfo,
    _numRuntimeKeys: *mut c_int,
    _arrayKeys: *mut *mut IndexArrayKeyInfo,
    _numArrayKeys: *mut c_int,
) {
    crate::executor::nodeIndexscan::ExecIndexBuildScanKeys(_planstate as _, _index as _, _quals as _, _isorderby as _, _scanKeys as _, _numScanKeys as _, _runtimeKeys as _, _numRuntimeKeys as _, _arrayKeys as _, _numArrayKeys as _)
}

unsafe fn ExecIndexEvalRuntimeKeys(
    _econtext: *mut ExprContext,
    _runtimeKeys: *mut IndexRuntimeKeyInfo,
    _numRuntimeKeys: c_int,
) {
    crate::executor::nodeIndexscan::ExecIndexEvalRuntimeKeys(_econtext as _, _runtimeKeys as _, _numRuntimeKeys as _)
}

unsafe fn ExecIndexEvalArrayKeys(
    _econtext: *mut ExprContext,
    _arrayKeys: *mut IndexArrayKeyInfo,
    _numArrayKeys: c_int,
) -> bool {
    crate::executor::nodeIndexscan::ExecIndexEvalArrayKeys(_econtext as _, _arrayKeys as _, _numArrayKeys as _) as _
}

unsafe fn ExecIndexAdvanceArrayKeys(
    _arrayKeys: *mut IndexArrayKeyInfo,
    _numArrayKeys: c_int,
) -> bool {
    crate::executor::nodeIndexscan::ExecIndexAdvanceArrayKeys(_arrayKeys as _, _numArrayKeys as _) as _
}

unsafe fn IsParallelWorker() -> bool {
    unimplemented!() // TODO: miscadmin.h
}

unsafe fn shm_toc_estimate_chunk(_estimator: *mut c_void, _size: Size) {
    unimplemented!()
}

unsafe fn shm_toc_estimate_keys(_estimator: *mut c_void, _nkeys: Size) {
    unimplemented!()
}

unsafe fn shm_toc_allocate(_toc: *mut c_void, _nbytes: Size) -> *mut c_void {
    crate::storage::ipc::shm_toc::shm_toc_allocate(_toc as _, _nbytes as _) as _
}

unsafe fn shm_toc_insert(_toc: *mut c_void, _key: u64, _address: *mut c_void) {
    crate::storage::ipc::shm_toc::shm_toc_insert(_toc as _, _key as _, _address as _)
}

unsafe fn shm_toc_lookup(
    _toc: *mut c_void,
    _key: u64,
    _noError: bool,
) -> *mut c_void {
    crate::storage::ipc::shm_toc::shm_toc_lookup(_toc as _, _key as _, _noError as _) as _
}

// Global stubs (would be `extern` globals in C).
#[allow(non_upper_case_globals)]
static mut ParallelWorkerNumber: c_int = 0;

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScan
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
unsafe fn ExecBitmapIndexScan(_pstate: *mut PlanState) -> *mut TupleTableSlot {
    elog!(
        ERROR,
        "BitmapIndexScan node does not support ExecProcNode call convention"
    );
    #[allow(unreachable_code)]
    ptr::null_mut()
}

/* ----------------------------------------------------------------
 *		MultiExecBitmapIndexScan(node)
 * ----------------------------------------------------------------
 */
pub unsafe fn MultiExecBitmapIndexScan(node: *mut BitmapIndexScanState) -> *mut Node {
    let tbm: *mut TIDBitmap;
    let scandesc: IndexScanDesc;
    let mut nTuples: f64 = 0.0;
    let mut doscan: bool;

    /* must provide our own instrumentation support */
    if !(*node).ss.ps.instrument.is_null() {
        InstrStartNode((*node).ss.ps.instrument);
    }

    /*
     * extract necessary information from index scan node
     */
    scandesc = (*node).biss_ScanDesc as IndexScanDesc;

    /*
     * If we have runtime keys and they've not already been set up, do it now.
     * Array keys are also treated as runtime keys; note that if ExecReScan
     * returns with biss_RuntimeKeysReady still false, then there is an empty
     * array key so we should do nothing.
     */
    if !(*node).biss_RuntimeKeysReady
        && ((*node).biss_NumRuntimeKeys != 0 || (*node).biss_NumArrayKeys != 0)
    {
        ExecReScan(node as *mut PlanState);
        doscan = (*node).biss_RuntimeKeysReady;
    } else {
        doscan = true;
    }

    /*
     * Prepare the result bitmap.  Normally we just create a new one to pass
     * back; however, our parent node is allowed to store a pre-made one into
     * node->biss_result, in which case we just OR our tuple IDs into the
     * existing bitmap.  (This saves needing explicit UNION steps.)
     */
    if !(*node).biss_result.is_null() {
        tbm = (*node).biss_result;
        (*node).biss_result = ptr::null_mut(); /* reset for next time */
    } else {
        /* XXX should we use less than work_mem for this? */
        tbm = tbm_create(
            work_mem as Size * 1024 as Size,
            if (*((*node).ss.ps.plan as *mut BitmapIndexScan)).isshared {
                (*(*node).ss.ps.state).es_query_dsa as *mut _
            } else {
                ptr::null_mut()
            },
        );
    }

    /*
     * Get TIDs from index and insert into bitmap
     */
    while doscan {
        nTuples += index_getbitmap(scandesc, tbm) as f64;

        CHECK_FOR_INTERRUPTS();

        doscan = ExecIndexAdvanceArrayKeys(
            (*node).biss_ArrayKeys,
            (*node).biss_NumArrayKeys,
        );
        if doscan {
            /* reset index scan */
            index_rescan(
                (*node).biss_ScanDesc as IndexScanDesc,
                (*node).biss_ScanKeys,
                (*node).biss_NumScanKeys,
                ptr::null_mut(),
                0,
            );
        }
    }

    /* must provide our own instrumentation support */
    if !(*node).ss.ps.instrument.is_null() {
        InstrStopNode((*node).ss.ps.instrument, nTuples);
    }

    tbm as *mut Node
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanBitmapIndexScan(node: *mut BitmapIndexScanState) {
    let econtext: *mut ExprContext = (*node).biss_RuntimeContext;

    /*
     * Reset the runtime-key context so we don't leak memory as each outer
     * tuple is scanned.  Note this assumes that we will recalculate *all*
     * runtime keys on each call.
     */
    if !econtext.is_null() {
        ResetExprContext(econtext);
    }

    /*
     * If we are doing runtime key calculations (ie, any of the index key
     * values weren't simple Consts), compute the new key values.
     *
     * Array keys are also treated as runtime keys; note that if we return
     * with biss_RuntimeKeysReady still false, then there is an empty array
     * key so no index scan is needed.
     */
    if (*node).biss_NumRuntimeKeys != 0 {
        ExecIndexEvalRuntimeKeys(
            econtext,
            (*node).biss_RuntimeKeys,
            (*node).biss_NumRuntimeKeys,
        );
    }
    if (*node).biss_NumArrayKeys != 0 {
        (*node).biss_RuntimeKeysReady = ExecIndexEvalArrayKeys(
            econtext,
            (*node).biss_ArrayKeys,
            (*node).biss_NumArrayKeys,
        );
    } else {
        (*node).biss_RuntimeKeysReady = true;
    }

    /* reset index scan */
    if (*node).biss_RuntimeKeysReady {
        index_rescan(
            (*node).biss_ScanDesc as IndexScanDesc,
            (*node).biss_ScanKeys,
            (*node).biss_NumScanKeys,
            ptr::null_mut(),
            0,
        );
    }
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapIndexScan
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndBitmapIndexScan(node: *mut BitmapIndexScanState) {
    let indexRelationDesc: Relation;
    let indexScanDesc: IndexScanDesc;

    /*
     * extract information from the node
     */
    indexRelationDesc = (*node).biss_RelationDesc;
    indexScanDesc = (*node).biss_ScanDesc as IndexScanDesc;

    /*
     * When ending a parallel worker, copy the statistics gathered by the
     * worker back into shared memory so that it can be picked up by the main
     * process to report in EXPLAIN ANALYZE
     */
    if !(*node).biss_SharedInfo.is_null() && IsParallelWorker() {
        let winstrument: *mut IndexScanInstrumentation;

        Assert!(ParallelWorkerNumber <= num_workers((*node).biss_SharedInfo));
        winstrument = winstrument_ptr((*node).biss_SharedInfo, ParallelWorkerNumber);

        /*
         * We have to accumulate the stats rather than performing a memcpy.
         * When a Gather/GatherMerge node finishes it will perform planner
         * shutdown on the workers.  On rescan it will spin up new workers
         * which will have a new BitmapIndexScanState and zeroed stats.
         */
        add_nsearches(winstrument, get_nsearches(&raw mut (*node).biss_Instrument));
    }

    /*
     * close the index relation (no-op if we didn't open it)
     */
    if !indexScanDesc.is_null() {
        index_endscan(indexScanDesc);
    }
    if !indexRelationDesc.is_null() {
        index_close(indexRelationDesc, NoLock);
    }
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapIndexScan
 *
 *		Initializes the index scan's state information.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitBitmapIndexScan(
    node: *mut BitmapIndexScan,
    estate: *mut EState,
    eflags: c_int,
) -> *mut BitmapIndexScanState {
    let indexstate: *mut BitmapIndexScanState;
    let lockmode: LOCKMODE;

    /* check for unsupported flags */
    Assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    /*
     * create state structure
     */
    indexstate = makeNode!(BitmapIndexScanState, T_BitmapIndexScanState);
    (*indexstate).ss.ps.plan = node as *mut Plan;
    (*indexstate).ss.ps.state = estate;
    (*indexstate).ss.ps.ExecProcNode = Some(ExecBitmapIndexScan);

    /* normally we don't make the result bitmap till runtime */
    (*indexstate).biss_result = ptr::null_mut();

    /*
     * We do not open or lock the base relation here.  We assume that an
     * ancestor BitmapHeapScan node is holding AccessShareLock (or better) on
     * the heap relation throughout the execution of the plan tree.
     */

    (*indexstate).ss.ss_currentRelation = ptr::null_mut();
    (*indexstate).ss.ss_currentScanDesc = ptr::null_mut();

    /*
     * Miscellaneous initialization
     *
     * We do not need a standard exprcontext for this node, though we may
     * decide below to create a runtime-key exprcontext
     */

    /*
     * initialize child expressions
     *
     * We don't need to initialize targetlist or qual since neither are used.
     *
     * Note: we don't initialize all of the indexqual expression, only the
     * sub-parts corresponding to runtime keys (see below).
     */

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if eflags & EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return indexstate;
    }

    /* Open the index relation. */
    lockmode = (*exec_rt_fetch((*node).scan.scanrelid, estate)).rellockmode;
    (*indexstate).biss_RelationDesc = index_open((*node).indexid, lockmode);

    /*
     * Initialize index-specific scan state
     */
    (*indexstate).biss_RuntimeKeysReady = false;
    (*indexstate).biss_RuntimeKeys = ptr::null_mut();
    (*indexstate).biss_NumRuntimeKeys = 0;

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys(
        indexstate as *mut PlanState,
        (*indexstate).biss_RelationDesc,
        (*node).indexqual,
        false,
        &raw mut (*indexstate).biss_ScanKeys,
        &raw mut (*indexstate).biss_NumScanKeys,
        &raw mut (*indexstate).biss_RuntimeKeys,
        &raw mut (*indexstate).biss_NumRuntimeKeys,
        &raw mut (*indexstate).biss_ArrayKeys,
        &raw mut (*indexstate).biss_NumArrayKeys,
    );

    /*
     * If we have runtime keys or array keys, we need an ExprContext to
     * evaluate them. We could just create a "standard" plan node exprcontext,
     * but to keep the code looking similar to nodeIndexscan.c, it seems
     * better to stick with the approach of using a separate ExprContext.
     */
    if (*indexstate).biss_NumRuntimeKeys != 0 || (*indexstate).biss_NumArrayKeys != 0 {
        let stdecontext: *mut ExprContext = (*indexstate).ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &raw mut (*indexstate).ss.ps);
        (*indexstate).biss_RuntimeContext = (*indexstate).ss.ps.ps_ExprContext;
        (*indexstate).ss.ps.ps_ExprContext = stdecontext;
    } else {
        (*indexstate).biss_RuntimeContext = ptr::null_mut();
    }

    /*
     * Initialize scan descriptor.
     */
    (*indexstate).biss_ScanDesc = index_beginscan_bitmap(
        (*indexstate).biss_RelationDesc,
        (*estate).es_snapshot as *mut c_void,
        &raw mut (*indexstate).biss_Instrument,
        (*indexstate).biss_NumScanKeys,
    ) as *mut _;

    /*
     * If no run-time keys to calculate, go ahead and pass the scankeys to the
     * index AM.
     */
    if (*indexstate).biss_NumRuntimeKeys == 0 && (*indexstate).biss_NumArrayKeys == 0 {
        index_rescan(
            (*indexstate).biss_ScanDesc as IndexScanDesc,
            (*indexstate).biss_ScanKeys,
            (*indexstate).biss_NumScanKeys,
            ptr::null_mut(),
            0,
        );
    }

    /*
     * all done.
     */
    indexstate
}

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapIndexScanEstimate(
    node: *mut BitmapIndexScanState,
    pcxt: *mut ParallelContext,
) {
    let size: Size;

    /*
     * Parallel bitmap index scans are not supported, but we still need to
     * store the scan's instrumentation in DSM during parallel query
     */
    if (*node).ss.ps.instrument.is_null() || pcxt_nworkers(pcxt) == 0 {
        return;
    }

    size = offsetof_winstrument()
        + pcxt_nworkers(pcxt) as Size * size_of::<IndexScanInstrumentation>();
    shm_toc_estimate_chunk(pcxt_estimator(pcxt), size);
    shm_toc_estimate_keys(pcxt_estimator(pcxt), 1);
}

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScanInitializeDSM
 *
 *		Set up bitmap index scan shared instrumentation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapIndexScanInitializeDSM(
    node: *mut BitmapIndexScanState,
    pcxt: *mut ParallelContext,
) {
    let size: Size;

    /* don't need this if not instrumenting or no workers */
    if (*node).ss.ps.instrument.is_null() || pcxt_nworkers(pcxt) == 0 {
        return;
    }

    size = offsetof_winstrument()
        + pcxt_nworkers(pcxt) as Size * size_of::<IndexScanInstrumentation>();
    (*node).biss_SharedInfo =
        shm_toc_allocate(pcxt_toc(pcxt), size) as *mut SharedIndexScanInstrumentation;
    shm_toc_insert(
        pcxt_toc(pcxt),
        (*(*node).ss.ps.plan).plan_node_id as u64,
        (*node).biss_SharedInfo as *mut c_void,
    );

    /* Each per-worker area must start out as zeroes */
    ptr::write_bytes((*node).biss_SharedInfo as *mut u8, 0, size);
    set_num_workers((*node).biss_SharedInfo, pcxt_nworkers(pcxt));
}

/* ----------------------------------------------------------------
 *		ExecBitmapIndexScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapIndexScanInitializeWorker(
    node: *mut BitmapIndexScanState,
    pwcxt: *mut ParallelWorkerContext,
) {
    /* don't need this if not instrumenting */
    if (*node).ss.ps.instrument.is_null() {
        return;
    }

    (*node).biss_SharedInfo = shm_toc_lookup(
        pwcxt_toc(pwcxt),
        (*(*node).ss.ps.plan).plan_node_id as u64,
        false,
    ) as *mut SharedIndexScanInstrumentation;
}

/* ----------------------------------------------------------------
 * ExecBitmapIndexScanRetrieveInstrumentation
 *
 *		Transfer bitmap index scan statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecBitmapIndexScanRetrieveInstrumentation(node: *mut BitmapIndexScanState) {
    let SharedInfo: *mut SharedIndexScanInstrumentation = (*node).biss_SharedInfo;
    let size: usize;

    if SharedInfo.is_null() {
        return;
    }

    /* Create a copy of SharedInfo in backend-local memory */
    size = offsetof_winstrument()
        + get_num_workers(SharedInfo) as usize * size_of::<IndexScanInstrumentation>();
    (*node).biss_SharedInfo = palloc(size) as *mut SharedIndexScanInstrumentation;
    ptr::copy_nonoverlapping(
        SharedInfo as *const u8,
        (*node).biss_SharedInfo as *mut u8,
        size,
    );
}

// ----------------------------------------------------------------
// Local stub accessors for not-yet-ported opaque types.
// (SharedIndexScanInstrumentation/IndexScanInstrumentation are opaque in
// nodes/execnodes.rs; ParallelContext/ParallelWorkerContext fields live in
// access/parallel.h which is not yet ported.)
// ----------------------------------------------------------------

unsafe fn num_workers(_si: *mut SharedIndexScanInstrumentation) -> c_int {
    unimplemented!() // TODO: access/genam.h (SharedIndexScanInstrumentation.num_workers)
}

unsafe fn get_num_workers(_si: *mut SharedIndexScanInstrumentation) -> c_int {
    unimplemented!() // TODO: access/genam.h (SharedIndexScanInstrumentation.num_workers)
}

unsafe fn set_num_workers(_si: *mut SharedIndexScanInstrumentation, _n: c_int) {
    unimplemented!() // TODO: access/genam.h (SharedIndexScanInstrumentation.num_workers)
}

unsafe fn winstrument_ptr(
    _si: *mut SharedIndexScanInstrumentation,
    _idx: c_int,
) -> *mut IndexScanInstrumentation {
    unimplemented!() // TODO: access/genam.h (SharedIndexScanInstrumentation.winstrument[])
}

unsafe fn add_nsearches(_dst: *mut IndexScanInstrumentation, _n: u64) {
    unimplemented!() // TODO: access/genam.h (IndexScanInstrumentation.nsearches)
}

unsafe fn get_nsearches(_src: *mut IndexScanInstrumentation) -> u64 {
    unimplemented!() // TODO: access/genam.h (IndexScanInstrumentation.nsearches)
}

unsafe fn offsetof_winstrument() -> Size {
    unimplemented!() // TODO: offsetof(SharedIndexScanInstrumentation, winstrument)
}

unsafe fn pcxt_nworkers(_pcxt: *mut ParallelContext) -> c_int {
    unimplemented!() // TODO: access/parallel.h (ParallelContext.nworkers)
}

unsafe fn pcxt_estimator(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (ParallelContext.estimator)
}

unsafe fn pcxt_toc(_pcxt: *mut ParallelContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (ParallelContext.toc)
}

unsafe fn pwcxt_toc(_pwcxt: *mut ParallelWorkerContext) -> *mut c_void {
    unimplemented!() // TODO: access/parallel.h (ParallelWorkerContext.toc)
}
