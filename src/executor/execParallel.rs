/*-------------------------------------------------------------------------
 *
 * execParallel.c
 *	  Support routines for parallel execution.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This file contains routines that are intended to support setting up,
 * using, and tearing down a ParallelContext from within the PostgreSQL
 * executor.  The ParallelContext machinery will handle starting the
 * workers and ensuring that their state generally matches that of the
 * leader; see src/backend/access/transam/README.parallel for details.
 * However, we must save and restore relevant executor state, such as
 * any ParamListInfo associated with the query, buffer/WAL usage info, and
 * the actual plan to be passed down to the worker.
 *
 * IDENTIFICATION
 *	  src/backend/executor/execParallel.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::CStr;
use core::mem::size_of;
use std::ptr;

use crate::access::sdir::ForwardScanDirection;
use crate::access::transam::parallel::{
    BackgroundWorkerHandle, CreateParallelContext, DestroyParallelContext, InitializeParallelDSM,
    LaunchParallelWorkers, ParallelContext, ParallelWorkerContext, ParallelWorkerNumber,
    ReinitializeParallelDSM, WaitForParallelWorkersToFinish, dsm_segment, shm_mq, shm_mq_handle,
    shm_toc, PGPROC,
};
use crate::executor::execdesc::{CreateQueryDesc, FreeQueryDesc, QueryDesc};
use crate::executor::execMain::{ExecutorEnd, ExecutorFinish, ExecutorRun, ExecutorStart};
use crate::executor::executor::{ExecSetTupleBound, GetPerTupleExprContext};
use crate::executor::instrument::{
    BufferUsage, InstrAccumParallelQuery, InstrAggNode, InstrEndLoop, InstrEndParallelQuery,
    InstrInit, InstrStartParallelQuery, Instrumentation, WalUsage, WorkerInstrumentation,
};
use crate::executor::nodeSubplan::ExecSetParamPlanMulti;
use crate::executor::tqueue::TupleQueueReader;
use crate::jit::jit::{InstrJitAgg, JitInstrumentation, PGJIT_NONE};
use crate::nodes::bitmapset::{bms_is_empty, bms_next_member, bms_num_members, Bitmapset};
use crate::nodes::copyfuncs::copyObjectImpl;
use crate::nodes::execnodes::{
    dsa_area, dsa_pointer, EState, ParallelExecutorInfo, PlanState, SharedJitInstrumentation,
};
use crate::nodes::nodeFuncs::planstate_tree_walker;
use crate::nodes::nodes::{nodeTag, CmdType, NodeTag};
use crate::nodes::outfuncs::nodeToString;
use crate::nodes::params::{ParamExecData, ParamListInfo};
use crate::nodes::plannodes::{Plan, PlannedStmt};
use crate::nodes::primnodes::TargetEntry;
use crate::nodes::read::stringToNode;
use crate::nodes::pg_list::{lappend, list_nth_oid};
use crate::storage::lmgr::lwlock::LWTRANCHE_PARALLEL_QUERY_DSA;
use crate::tcop::dest::DestReceiver;
use crate::tcop::postgres::debug_query_string;
use crate::utils::activity::backend_status::{
    pgstat_get_my_plan_id, pgstat_get_my_query_id, pgstat_report_activity, STATE_RUNNING,
};
use crate::utils::adt::datum::{datumEstimateSpace, datumRestore, datumSerialize};
use crate::utils::cache::lsyscache::get_typlenbyval;
use crate::storage::ipc::shmem::{add_size, mul_size};
use crate::utils::time::snapmgr::GetActiveSnapshot;
use crate::utils::snapshot::InvalidSnapshot;

use crate::{foreach, current_cell, lfirst_node, makeNode, Assert};
use crate::c::MAXALIGN;

/* ----------------------------------------------------------------
 *		local helper stubs for not-yet-ported dependencies
 * ---------------------------------------------------------------- */

/* utils/dsa.h: dsa_pointer used here is execnodes' Size-width alias. */
const InvalidDsaPointer: dsa_pointer = 0;
fn DsaPointerIsValid(x: dsa_pointer) -> bool {
    x != InvalidDsaPointer
}

/* storage/proc.h: this backend's PGPROC pointer (TODO: storage/proc.c). */
static mut MyProc: *mut PGPROC = ptr::null_mut();

/* miscadmin.h: AmParallelWorkerProcess() / IsParallelWorker(). */
unsafe fn IsParallelWorker() -> bool {
    ParallelWorkerNumber >= 0
}

/* storage/shm_toc.h (TODO: storage/ipc/shm_toc.c -- type identity differs). */
unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _nbytes: Size) -> *mut c_void {
    unimplemented!() // TODO: storage/ipc/shm_toc.c
}
unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: u64, _address: *mut c_void) {
    unimplemented!() // TODO: storage/ipc/shm_toc.c
}
unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: u64, _noError: bool) -> *mut c_void {
    unimplemented!() // TODO: storage/ipc/shm_toc.c
}
unsafe fn shm_toc_estimate_chunk(_e: *mut c_void, _sz: Size) {
    unimplemented!() // TODO: storage/ipc/shm_toc.c
}
unsafe fn shm_toc_estimate_keys(_e: *mut c_void, _cnt: Size) {
    unimplemented!() // TODO: storage/ipc/shm_toc.c
}

/* storage/shm_mq.h (TODO: storage/ipc/shm_mq.c -- type identity differs). */
unsafe fn shm_mq_create(_address: *mut c_void, _size: Size) -> *mut shm_mq {
    unimplemented!() // TODO: storage/ipc/shm_mq.c
}
unsafe fn shm_mq_set_receiver(_mq: *mut shm_mq, _proc: *mut PGPROC) {
    unimplemented!() // TODO: storage/ipc/shm_mq.c
}
unsafe fn shm_mq_set_sender(_mq: *mut shm_mq, _proc: *mut PGPROC) {
    unimplemented!() // TODO: storage/ipc/shm_mq.c
}
unsafe fn shm_mq_attach(
    _mq: *mut shm_mq,
    _seg: *mut dsm_segment,
    _handle: *mut BackgroundWorkerHandle,
) -> *mut shm_mq_handle {
    unimplemented!() // TODO: storage/ipc/shm_mq.c
}
unsafe fn shm_mq_detach(_mqh: *mut shm_mq_handle) {
    unimplemented!() // TODO: storage/ipc/shm_mq.c
}
unsafe fn shm_mq_set_handle(_mqh: *mut shm_mq_handle, _handle: *mut BackgroundWorkerHandle) {
    unimplemented!() // TODO: storage/ipc/shm_mq.c
}

/* utils/dsa.h (TODO: utils/mmgr/dsa.c -- dsm_segment/dsa_pointer identity differs). */
unsafe fn dsa_allocate(_area: *mut dsa_area, _size: Size) -> dsa_pointer {
    unimplemented!() // TODO: utils/mmgr/dsa.c
}
unsafe fn dsa_free(_area: *mut dsa_area, _dp: dsa_pointer) {
    unimplemented!() // TODO: utils/mmgr/dsa.c
}
unsafe fn dsa_get_address(_area: *mut dsa_area, _dp: dsa_pointer) -> *mut c_void {
    unimplemented!() // TODO: utils/mmgr/dsa.c
}
unsafe fn dsa_minimum_size() -> Size {
    unimplemented!() // TODO: utils/mmgr/dsa.c
}
unsafe fn dsa_create_in_place(
    _place: *mut c_void,
    _size: Size,
    _tranche_id: c_int,
    _segment: *mut dsm_segment,
) -> *mut dsa_area {
    unimplemented!() // TODO: utils/mmgr/dsa.c
}
unsafe fn dsa_attach_in_place(_place: *mut c_void, _segment: *mut dsm_segment) -> *mut dsa_area {
    unimplemented!() // TODO: utils/mmgr/dsa.c
}
unsafe fn dsa_detach(_area: *mut dsa_area) {
    unimplemented!() // TODO: utils/mmgr/dsa.c
}

/* nodes/params.h (TODO: nodes/params.c). */
unsafe fn EstimateParamListSpace(_paramLI: ParamListInfo) -> Size {
    unimplemented!() // TODO: nodes/params.c
}
unsafe fn SerializeParamList(_paramLI: ParamListInfo, _start_address: *mut *mut c_char) {
    unimplemented!() // TODO: nodes/params.c
}
unsafe fn RestoreParamList(_start_address: *mut *mut c_char) -> ParamListInfo {
    unimplemented!() // TODO: nodes/params.c
}

/* executor/tqueue.h (TODO: executor/tqueue.c). */
unsafe fn CreateTupleQueueReader(_handle: *mut shm_mq_handle) -> *mut TupleQueueReader {
    unimplemented!() // TODO: executor/tqueue.c
}
unsafe fn DestroyTupleQueueReader(_reader: *mut TupleQueueReader) {
    unimplemented!() // TODO: executor/tqueue.c
}
unsafe fn CreateTupleQueueDestReceiver(_handle: *mut shm_mq_handle) -> *mut DestReceiver {
    unimplemented!() // TODO: executor/tqueue.c
}

/* access/parallel.h: silence "unused import" by referencing here. */
#[allow(dead_code)]
unsafe fn _ref_launch(pcxt: *mut ParallelContext) {
    LaunchParallelWorkers(pcxt);
}

/*
 * Per-node-type estimate/init/reinit/worker/retrieve helpers; each lives in
 * the corresponding nodeXXX.c file.  They are stubbed here as DSM-using plan
 * node entry points (TODO: their respective nodeXXX.c).
 */
macro_rules! node_stub2 {
    ($name:ident, $ty:ty) => {
        unsafe fn $name(_node: *mut $ty, _pcxt: *mut ParallelContext) {
            unimplemented!() // TODO: nodeXXX.c
        }
    };
}
macro_rules! node_stub_w {
    ($name:ident, $ty:ty) => {
        unsafe fn $name(_node: *mut $ty, _pwcxt: *mut ParallelWorkerContext) {
            unimplemented!() // TODO: nodeXXX.c
        }
    };
}
macro_rules! node_stub1 {
    ($name:ident, $ty:ty) => {
        unsafe fn $name(_node: *mut $ty) {
            unimplemented!() // TODO: nodeXXX.c
        }
    };
}

/* Opaque per-node PlanState supertypes (real defs live in their nodeXXX.c). */
type SeqScanState = PlanState;
type IndexScanState = PlanState;
type IndexOnlyScanState = PlanState;
type BitmapIndexScanState = PlanState;
type ForeignScanState = PlanState;
type AppendState = PlanState;
type CustomScanState = PlanState;
type BitmapHeapScanState = PlanState;
type HashJoinState = PlanState;
type HashState = PlanState;
type SortState = PlanState;
type IncrementalSortState = PlanState;
type AggState = PlanState;
type MemoizeState = PlanState;

node_stub2!(ExecSeqScanEstimate, SeqScanState);
node_stub2!(ExecIndexScanEstimate, IndexScanState);
node_stub2!(ExecIndexOnlyScanEstimate, IndexOnlyScanState);
node_stub2!(ExecBitmapIndexScanEstimate, BitmapIndexScanState);
node_stub2!(ExecForeignScanEstimate, ForeignScanState);
node_stub2!(ExecAppendEstimate, AppendState);
node_stub2!(ExecCustomScanEstimate, CustomScanState);
node_stub2!(ExecBitmapHeapEstimate, BitmapHeapScanState);
node_stub2!(ExecHashJoinEstimate, HashJoinState);
node_stub2!(ExecHashEstimate, HashState);
node_stub2!(ExecSortEstimate, SortState);
node_stub2!(ExecIncrementalSortEstimate, IncrementalSortState);
node_stub2!(ExecAggEstimate, AggState);
node_stub2!(ExecMemoizeEstimate, MemoizeState);

node_stub2!(ExecSeqScanInitializeDSM, SeqScanState);
node_stub2!(ExecIndexScanInitializeDSM, IndexScanState);
node_stub2!(ExecIndexOnlyScanInitializeDSM, IndexOnlyScanState);
node_stub2!(ExecBitmapIndexScanInitializeDSM, BitmapIndexScanState);
node_stub2!(ExecForeignScanInitializeDSM, ForeignScanState);
node_stub2!(ExecAppendInitializeDSM, AppendState);
node_stub2!(ExecCustomScanInitializeDSM, CustomScanState);
node_stub2!(ExecBitmapHeapInitializeDSM, BitmapHeapScanState);
node_stub2!(ExecHashJoinInitializeDSM, HashJoinState);
node_stub2!(ExecHashInitializeDSM, HashState);
node_stub2!(ExecSortInitializeDSM, SortState);
node_stub2!(ExecIncrementalSortInitializeDSM, IncrementalSortState);
node_stub2!(ExecAggInitializeDSM, AggState);
node_stub2!(ExecMemoizeInitializeDSM, MemoizeState);

node_stub2!(ExecSeqScanReInitializeDSM, SeqScanState);
node_stub2!(ExecIndexScanReInitializeDSM, IndexScanState);
node_stub2!(ExecIndexOnlyScanReInitializeDSM, IndexOnlyScanState);
node_stub2!(ExecForeignScanReInitializeDSM, ForeignScanState);
node_stub2!(ExecAppendReInitializeDSM, AppendState);
node_stub2!(ExecCustomScanReInitializeDSM, CustomScanState);
node_stub2!(ExecBitmapHeapReInitializeDSM, BitmapHeapScanState);
node_stub2!(ExecHashJoinReInitializeDSM, HashJoinState);

node_stub1!(ExecIndexScanRetrieveInstrumentation, IndexScanState);
node_stub1!(ExecIndexOnlyScanRetrieveInstrumentation, IndexOnlyScanState);
node_stub1!(ExecBitmapIndexScanRetrieveInstrumentation, BitmapIndexScanState);
node_stub1!(ExecSortRetrieveInstrumentation, SortState);
node_stub1!(ExecIncrementalSortRetrieveInstrumentation, IncrementalSortState);
node_stub1!(ExecHashRetrieveInstrumentation, HashState);
node_stub1!(ExecAggRetrieveInstrumentation, AggState);
node_stub1!(ExecMemoizeRetrieveInstrumentation, MemoizeState);
node_stub1!(ExecBitmapHeapRetrieveInstrumentation, BitmapHeapScanState);

node_stub_w!(ExecSeqScanInitializeWorker, SeqScanState);
node_stub_w!(ExecIndexScanInitializeWorker, IndexScanState);
node_stub_w!(ExecIndexOnlyScanInitializeWorker, IndexOnlyScanState);
node_stub_w!(ExecBitmapIndexScanInitializeWorker, BitmapIndexScanState);
node_stub_w!(ExecForeignScanInitializeWorker, ForeignScanState);
node_stub_w!(ExecAppendInitializeWorker, AppendState);
node_stub_w!(ExecCustomScanInitializeWorker, CustomScanState);
node_stub_w!(ExecBitmapHeapInitializeWorker, BitmapHeapScanState);
node_stub_w!(ExecHashJoinInitializeWorker, HashJoinState);
node_stub_w!(ExecHashInitializeWorker, HashState);
node_stub_w!(ExecSortInitializeWorker, SortState);
node_stub_w!(ExecIncrementalSortInitializeWorker, IncrementalSortState);
node_stub_w!(ExecAggInitializeWorker, AggState);
node_stub_w!(ExecMemoizeInitializeWorker, MemoizeState);

/*
 * Magic numbers for parallel executor communication.  We use constants
 * greater than any 32-bit integer here so that values < 2^32 can be used
 * by individual parallel nodes to store their own state.
 */
const PARALLEL_KEY_EXECUTOR_FIXED: u64 = 0xE000000000000001;
const PARALLEL_KEY_PLANNEDSTMT: u64 = 0xE000000000000002;
const PARALLEL_KEY_PARAMLISTINFO: u64 = 0xE000000000000003;
const PARALLEL_KEY_BUFFER_USAGE: u64 = 0xE000000000000004;
const PARALLEL_KEY_TUPLE_QUEUE: u64 = 0xE000000000000005;
const PARALLEL_KEY_INSTRUMENTATION: u64 = 0xE000000000000006;
const PARALLEL_KEY_DSA: u64 = 0xE000000000000007;
const PARALLEL_KEY_QUERY_TEXT: u64 = 0xE000000000000008;
const PARALLEL_KEY_JIT_INSTRUMENTATION: u64 = 0xE000000000000009;
const PARALLEL_KEY_WAL_USAGE: u64 = 0xE00000000000000A;

const PARALLEL_TUPLE_QUEUE_SIZE: Size = 65536;

/*
 * Fixed-size random stuff that we need to pass to parallel workers.
 */
#[repr(C)]
struct FixedParallelExecutorState {
    tuples_needed: i64, /* tuple bound, see ExecSetTupleBound */
    param_exec: dsa_pointer,
    eflags: c_int,
    jit_flags: c_int,
}

/*
 * DSM structure for accumulating per-PlanState instrumentation.
 *
 * instrument_options: Same meaning here as in instrument.c.
 *
 * instrument_offset: Offset, relative to the start of this structure,
 * of the first Instrumentation object.  This will depend on the length of
 * the plan_node_id array.
 *
 * num_workers: Number of workers.
 *
 * num_plan_nodes: Number of plan nodes.
 *
 * plan_node_id: Array of plan nodes for which we are gathering instrumentation
 * from parallel workers.  The length of this array is given by num_plan_nodes.
 */
#[repr(C)]
pub struct SharedExecutorInstrumentation {
    instrument_options: c_int,
    instrument_offset: c_int,
    num_workers: c_int,
    num_plan_nodes: c_int,
    plan_node_id: [c_int; 0], /* FLEXIBLE_ARRAY_MEMBER */
    /* array of num_plan_nodes * num_workers Instrumentation objects follows */
}

/* GetInstrumentationArray(sei) */
unsafe fn GetInstrumentationArray(
    sei: *mut SharedExecutorInstrumentation,
) -> *mut Instrumentation {
    ((sei as *mut c_char).add((*sei).instrument_offset as usize)) as *mut Instrumentation
}

/* string.h */
extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

/*
 * Concrete layout of ParallelExecutorInfo (executor/execParallel.h).  The
 * canonical typedef in nodes/execnodes.rs is opaque (used only by pointer);
 * we cast that pointer to this struct to read/write fields.
 */
#[repr(C)]
struct ParallelExecutorInfoFull {
    planstate: *mut PlanState, /* plan subtree we're running in parallel */
    pcxt: *mut ParallelContext, /* parallel context we're using */
    buffer_usage: *mut BufferUsage, /* points to bufusage area in DSM */
    wal_usage: *mut WalUsage,  /* walusage area in DSM */
    instrumentation: *mut SharedExecutorInstrumentation, /* optional */
    jit_instrumentation: *mut SharedJitInstrumentation, /* optional */
    area: *mut dsa_area,       /* points to DSA area in DSM */
    param_exec: dsa_pointer,   /* serialized PARAM_EXEC parameters */
    finished: bool,            /* set true by ExecParallelFinish */
    /* These two arrays have pcxt->nworkers_launched entries: */
    tqueue: *mut *mut shm_mq_handle, /* tuple queues for worker output */
    reader: *mut *mut TupleQueueReader, /* tuple reader/writer support */
}

/* Context object for ExecParallelEstimate. */
#[repr(C)]
struct ExecParallelEstimateContext {
    pcxt: *mut ParallelContext,
    nnodes: c_int,
}

/* Context object for ExecParallelInitializeDSM. */
#[repr(C)]
struct ExecParallelInitializeDSMContext {
    pcxt: *mut ParallelContext,
    instrumentation: *mut SharedExecutorInstrumentation,
    nnodes: c_int,
}

/*
 * Create a serialized representation of the plan to be sent to each worker.
 */
unsafe fn ExecSerializePlan(mut plan: *mut Plan, estate: *mut EState) -> *mut c_char {
    let pstmt: *mut PlannedStmt;

    /* We can't scribble on the original plan, so make a copy. */
    plan = copyObjectImpl(plan as *const c_void) as *mut Plan;

    /*
     * The worker will start its own copy of the executor, and that copy will
     * insert a junk filter if the toplevel node has any resjunk entries. We
     * don't want that to happen, because while resjunk columns shouldn't be
     * sent back to the user, here the tuples are coming back to another
     * backend which may very well need them.  So mutate the target list
     * accordingly.  This is sort of a hack; there might be better ways to do
     * this...
     */
    foreach!(lc, (*plan).targetlist, {
        let tle: *mut TargetEntry =
            lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(lc));

        (*tle).resjunk = false;
    });

    /*
     * Create a dummy PlannedStmt.  Most of the fields don't need to be valid
     * for our purposes, but the worker will need at least a minimal
     * PlannedStmt to start the executor.
     */
    pstmt = makeNode!(PlannedStmt, T_PlannedStmt);
    (*pstmt).commandType = CmdType::CMD_SELECT;
    (*pstmt).queryId = pgstat_get_my_query_id();
    (*pstmt).planId = pgstat_get_my_plan_id();
    (*pstmt).hasReturning = false;
    (*pstmt).hasModifyingCTE = false;
    (*pstmt).canSetTag = true;
    (*pstmt).transientPlan = false;
    (*pstmt).dependsOnRole = false;
    (*pstmt).parallelModeNeeded = false;
    (*pstmt).planTree = plan;
    (*pstmt).partPruneInfos = (*estate).es_part_prune_infos;
    (*pstmt).rtable = (*estate).es_range_table;
    (*pstmt).unprunableRelids = (*estate).es_unpruned_relids;
    (*pstmt).permInfos = (*estate).es_rteperminfos;
    (*pstmt).resultRelations = ptr::null_mut();
    (*pstmt).appendRelations = ptr::null_mut();

    /*
     * Transfer only parallel-safe subplans, leaving a NULL "hole" in the list
     * for unsafe ones (so that the list indexes of the safe ones are
     * preserved).  This positively ensures that the worker won't try to run,
     * or even do ExecInitNode on, an unsafe subplan.  That's important to
     * protect, eg, non-parallel-aware FDWs from getting into trouble.
     */
    (*pstmt).subplans = ptr::null_mut();
    foreach!(lc, (*(*estate).es_plannedstmt).subplans, {
        let mut subplan: *mut Plan =
            crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut Plan;

        if !subplan.is_null() && !(*subplan).parallel_safe {
            subplan = ptr::null_mut();
        }
        (*pstmt).subplans = lappend((*pstmt).subplans, subplan as *mut c_void);
    });

    (*pstmt).rewindPlanIDs = ptr::null_mut();
    (*pstmt).rowMarks = ptr::null_mut();
    (*pstmt).relationOids = ptr::null_mut();
    (*pstmt).invalItems = ptr::null_mut(); /* workers can't replan anyway... */
    (*pstmt).paramExecTypes = (*(*estate).es_plannedstmt).paramExecTypes;
    (*pstmt).utilityStmt = ptr::null_mut();
    (*pstmt).stmt_location = -1;
    (*pstmt).stmt_len = -1;

    /* Return serialized copy of our dummy PlannedStmt. */
    nodeToString(pstmt as *const c_void)
}

/*
 * Parallel-aware plan nodes (and occasionally others) may need some state
 * which is shared across all parallel workers.  Before we size the DSM, give
 * them a chance to call shm_toc_estimate_chunk or shm_toc_estimate_keys on
 * &pcxt->estimator.
 *
 * While we're at it, count the number of PlanState nodes in the tree, so
 * we know how many Instrumentation structures we need.
 */
unsafe fn ExecParallelEstimate(planstate: *mut PlanState, context: *mut c_void) -> bool {
    let e = context as *mut ExecParallelEstimateContext;

    if planstate.is_null() {
        return false;
    }

    /* Count this node. */
    (*e).nnodes += 1;

    match nodeTag(planstate as *const PlanState) {
        NodeTag::T_SeqScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecSeqScanEstimate(planstate as *mut SeqScanState, (*e).pcxt);
            }
        }
        NodeTag::T_IndexScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIndexScanEstimate(planstate as *mut IndexScanState, (*e).pcxt);
        }
        NodeTag::T_IndexOnlyScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIndexOnlyScanEstimate(planstate as *mut IndexOnlyScanState, (*e).pcxt);
        }
        NodeTag::T_BitmapIndexScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecBitmapIndexScanEstimate(planstate as *mut BitmapIndexScanState, (*e).pcxt);
        }
        NodeTag::T_ForeignScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecForeignScanEstimate(planstate as *mut ForeignScanState, (*e).pcxt);
            }
        }
        NodeTag::T_AppendState => {
            if (*(*planstate).plan).parallel_aware {
                ExecAppendEstimate(planstate as *mut AppendState, (*e).pcxt);
            }
        }
        NodeTag::T_CustomScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecCustomScanEstimate(planstate as *mut CustomScanState, (*e).pcxt);
            }
        }
        NodeTag::T_BitmapHeapScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecBitmapHeapEstimate(planstate as *mut BitmapHeapScanState, (*e).pcxt);
            }
        }
        NodeTag::T_HashJoinState => {
            if (*(*planstate).plan).parallel_aware {
                ExecHashJoinEstimate(planstate as *mut HashJoinState, (*e).pcxt);
            }
        }
        NodeTag::T_HashState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecHashEstimate(planstate as *mut HashState, (*e).pcxt);
        }
        NodeTag::T_SortState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecSortEstimate(planstate as *mut SortState, (*e).pcxt);
        }
        NodeTag::T_IncrementalSortState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIncrementalSortEstimate(planstate as *mut IncrementalSortState, (*e).pcxt);
        }
        NodeTag::T_AggState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecAggEstimate(planstate as *mut AggState, (*e).pcxt);
        }
        NodeTag::T_MemoizeState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecMemoizeEstimate(planstate as *mut MemoizeState, (*e).pcxt);
        }
        _ => {}
    }

    planstate_tree_walker(
        planstate,
        Some(ExecParallelEstimate as unsafe fn(*mut PlanState, *mut c_void) -> bool),
        context,
    )
}

/*
 * Estimate the amount of space required to serialize the indicated parameters.
 */
unsafe fn EstimateParamExecSpace(estate: *mut EState, params: *mut Bitmapset) -> Size {
    let mut paramid: c_int;
    let mut sz: Size = size_of::<c_int>();

    paramid = -1;
    loop {
        paramid = bms_next_member(params, paramid);
        if paramid < 0 {
            break;
        }
        let typeOid: Oid;
        let mut typLen: i16 = 0;
        let mut typByVal: bool = false;
        let prm: *mut ParamExecData;

        prm = &mut *(*estate).es_param_exec_vals.add(paramid as usize);
        typeOid = list_nth_oid((*(*estate).es_plannedstmt).paramExecTypes, paramid);

        sz = add_size(sz, size_of::<c_int>()); /* space for paramid */

        /* space for datum/isnull */
        if OidIsValid(typeOid) {
            get_typlenbyval(typeOid, &mut typLen, &mut typByVal);
        } else {
            /* If no type OID, assume by-value, like copyParamList does. */
            typLen = size_of::<Datum>() as i16;
            typByVal = true;
        }
        sz = add_size(
            sz,
            datumEstimateSpace((*prm).value, (*prm).isnull, typByVal, typLen as c_int),
        );
    }
    sz
}

/*
 * Serialize specified PARAM_EXEC parameters.
 *
 * We write the number of parameters first, as a 4-byte integer, and then
 * write details for each parameter in turn.  The details for each parameter
 * consist of a 4-byte paramid (location of param in execution time internal
 * parameter array) and then the datum as serialized by datumSerialize().
 */
unsafe fn SerializeParamExecParams(
    estate: *mut EState,
    params: *mut Bitmapset,
    area: *mut dsa_area,
) -> dsa_pointer {
    let size: Size;
    let nparams: c_int;
    let mut paramid: c_int;
    let mut prm: *mut ParamExecData;
    let handle: dsa_pointer;
    let mut start_address: *mut c_char;

    /* Allocate enough space for the current parameter values. */
    size = EstimateParamExecSpace(estate, params);
    handle = dsa_allocate(area, size);
    start_address = dsa_get_address(area, handle) as *mut c_char;

    /* First write the number of parameters as a 4-byte integer. */
    nparams = bms_num_members(params);
    ptr::copy_nonoverlapping(
        &nparams as *const c_int as *const c_char,
        start_address,
        size_of::<c_int>(),
    );
    start_address = start_address.add(size_of::<c_int>());

    /* Write details for each parameter in turn. */
    paramid = -1;
    loop {
        paramid = bms_next_member(params, paramid);
        if paramid < 0 {
            break;
        }
        let typeOid: Oid;
        let mut typLen: i16 = 0;
        let mut typByVal: bool = false;

        prm = &mut *(*estate).es_param_exec_vals.add(paramid as usize);
        typeOid = list_nth_oid((*(*estate).es_plannedstmt).paramExecTypes, paramid);

        /* Write paramid. */
        ptr::copy_nonoverlapping(
            &paramid as *const c_int as *const c_char,
            start_address,
            size_of::<c_int>(),
        );
        start_address = start_address.add(size_of::<c_int>());

        /* Write datum/isnull */
        if OidIsValid(typeOid) {
            get_typlenbyval(typeOid, &mut typLen, &mut typByVal);
        } else {
            /* If no type OID, assume by-value, like copyParamList does. */
            typLen = size_of::<Datum>() as i16;
            typByVal = true;
        }
        datumSerialize(
            (*prm).value,
            (*prm).isnull,
            typByVal,
            typLen as c_int,
            &mut start_address,
        );
    }

    handle
}

/*
 * Restore specified PARAM_EXEC parameters.
 */
unsafe fn RestoreParamExecParams(mut start_address: *mut c_char, estate: *mut EState) {
    let mut nparams: c_int = 0;
    let mut i: c_int;
    let mut paramid: c_int = 0;

    ptr::copy_nonoverlapping(
        start_address,
        &mut nparams as *mut c_int as *mut c_char,
        size_of::<c_int>(),
    );
    start_address = start_address.add(size_of::<c_int>());

    i = 0;
    while i < nparams {
        let prm: *mut ParamExecData;

        /* Read paramid */
        ptr::copy_nonoverlapping(
            start_address,
            &mut paramid as *mut c_int as *mut c_char,
            size_of::<c_int>(),
        );
        start_address = start_address.add(size_of::<c_int>());
        prm = &mut *(*estate).es_param_exec_vals.add(paramid as usize);

        /* Read datum/isnull. */
        (*prm).value = datumRestore(&mut start_address, &mut (*prm).isnull);
        (*prm).execPlan = ptr::null_mut();

        i += 1;
    }
}

/*
 * Initialize the dynamic shared memory segment that will be used to control
 * parallel execution.
 */
unsafe fn ExecParallelInitializeDSM(planstate: *mut PlanState, context: *mut c_void) -> bool {
    let d = context as *mut ExecParallelInitializeDSMContext;

    if planstate.is_null() {
        return false;
    }

    /* If instrumentation is enabled, initialize slot for this node. */
    if !(*d).instrumentation.is_null() {
        *(*(*d).instrumentation)
            .plan_node_id
            .as_mut_ptr()
            .add((*d).nnodes as usize) = (*(*planstate).plan).plan_node_id;
    }

    /* Count this node. */
    (*d).nnodes += 1;

    /*
     * Call initializers for DSM-using plan nodes.
     *
     * Most plan nodes won't do anything here, but plan nodes that allocated
     * DSM may need to initialize shared state in the DSM before parallel
     * workers are launched.  They can allocate the space they previously
     * estimated using shm_toc_allocate, and add the keys they previously
     * estimated using shm_toc_insert, in each case targeting pcxt->toc.
     */
    match nodeTag(planstate as *const PlanState) {
        NodeTag::T_SeqScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecSeqScanInitializeDSM(planstate as *mut SeqScanState, (*d).pcxt);
            }
        }
        NodeTag::T_IndexScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIndexScanInitializeDSM(planstate as *mut IndexScanState, (*d).pcxt);
        }
        NodeTag::T_IndexOnlyScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIndexOnlyScanInitializeDSM(planstate as *mut IndexOnlyScanState, (*d).pcxt);
        }
        NodeTag::T_BitmapIndexScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecBitmapIndexScanInitializeDSM(planstate as *mut BitmapIndexScanState, (*d).pcxt);
        }
        NodeTag::T_ForeignScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecForeignScanInitializeDSM(planstate as *mut ForeignScanState, (*d).pcxt);
            }
        }
        NodeTag::T_AppendState => {
            if (*(*planstate).plan).parallel_aware {
                ExecAppendInitializeDSM(planstate as *mut AppendState, (*d).pcxt);
            }
        }
        NodeTag::T_CustomScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecCustomScanInitializeDSM(planstate as *mut CustomScanState, (*d).pcxt);
            }
        }
        NodeTag::T_BitmapHeapScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecBitmapHeapInitializeDSM(planstate as *mut BitmapHeapScanState, (*d).pcxt);
            }
        }
        NodeTag::T_HashJoinState => {
            if (*(*planstate).plan).parallel_aware {
                ExecHashJoinInitializeDSM(planstate as *mut HashJoinState, (*d).pcxt);
            }
        }
        NodeTag::T_HashState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecHashInitializeDSM(planstate as *mut HashState, (*d).pcxt);
        }
        NodeTag::T_SortState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecSortInitializeDSM(planstate as *mut SortState, (*d).pcxt);
        }
        NodeTag::T_IncrementalSortState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIncrementalSortInitializeDSM(planstate as *mut IncrementalSortState, (*d).pcxt);
        }
        NodeTag::T_AggState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecAggInitializeDSM(planstate as *mut AggState, (*d).pcxt);
        }
        NodeTag::T_MemoizeState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecMemoizeInitializeDSM(planstate as *mut MemoizeState, (*d).pcxt);
        }
        _ => {}
    }

    planstate_tree_walker(
        planstate,
        Some(ExecParallelInitializeDSM as unsafe fn(*mut PlanState, *mut c_void) -> bool),
        context,
    )
}

/*
 * It sets up the response queues for backend workers to return tuples
 * to the main backend and start the workers.
 */
unsafe fn ExecParallelSetupTupleQueues(
    pcxt: *mut ParallelContext,
    reinitialize: bool,
) -> *mut *mut shm_mq_handle {
    let responseq: *mut *mut shm_mq_handle;
    let tqueuespace: *mut c_char;
    let mut i: c_int;

    /* Skip this if no workers. */
    if (*pcxt).nworkers == 0 {
        return ptr::null_mut();
    }

    /* Allocate memory for shared memory queue handles. */
    responseq = palloc((*pcxt).nworkers as usize * size_of::<*mut shm_mq_handle>())
        as *mut *mut shm_mq_handle;

    /*
     * If not reinitializing, allocate space from the DSM for the queues;
     * otherwise, find the already allocated space.
     */
    if !reinitialize {
        tqueuespace = shm_toc_allocate(
            (*pcxt).toc,
            mul_size(PARALLEL_TUPLE_QUEUE_SIZE, (*pcxt).nworkers as Size),
        ) as *mut c_char;
    } else {
        tqueuespace = shm_toc_lookup((*pcxt).toc, PARALLEL_KEY_TUPLE_QUEUE, false) as *mut c_char;
    }

    /* Create the queues, and become the receiver for each. */
    i = 0;
    while i < (*pcxt).nworkers {
        let mq: *mut shm_mq;

        mq = shm_mq_create(
            tqueuespace.add((i as Size) * PARALLEL_TUPLE_QUEUE_SIZE) as *mut c_void,
            PARALLEL_TUPLE_QUEUE_SIZE,
        );

        shm_mq_set_receiver(mq, MyProc);
        *responseq.add(i as usize) = shm_mq_attach(mq, (*pcxt).seg, ptr::null_mut());

        i += 1;
    }

    /* Add array of queues to shm_toc, so others can find it. */
    if !reinitialize {
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_TUPLE_QUEUE, tqueuespace as *mut c_void);
    }

    /* Return array of handles. */
    responseq
}

/*
 * Sets up the required infrastructure for backend workers to perform
 * execution and return results to the main backend.
 */
pub unsafe fn ExecInitParallelPlan(
    planstate: *mut PlanState,
    estate: *mut EState,
    sendParams: *mut Bitmapset,
    nworkers: c_int,
    tuples_needed: i64,
) -> *mut ParallelExecutorInfo {
    let pei: *mut ParallelExecutorInfo;
    let pcxt: *mut ParallelContext;
    let mut e: ExecParallelEstimateContext = core::mem::zeroed();
    let mut d: ExecParallelInitializeDSMContext = core::mem::zeroed();
    let fpes: *mut FixedParallelExecutorState;
    let pstmt_data: *mut c_char;
    let pstmt_space: *mut c_char;
    let mut paramlistinfo_space: *mut c_char;
    let bufusage_space: *mut BufferUsage;
    let walusage_space: *mut WalUsage;
    let mut instrumentation: *mut SharedExecutorInstrumentation = ptr::null_mut();
    let mut jit_instrumentation: *mut SharedJitInstrumentation = ptr::null_mut();
    let pstmt_len: c_int;
    let paramlistinfo_len: c_int;
    let mut instrumentation_len: c_int = 0;
    let mut jit_instrumentation_len: c_int = 0;
    let mut instrument_offset: c_int = 0;
    let dsa_minsize: Size = dsa_minimum_size();
    let query_string: *mut c_char;
    let query_len: c_int;

    /*
     * Force any initplan outputs that we're going to pass to workers to be
     * evaluated, if they weren't already.
     *
     * For simplicity, we use the EState's per-output-tuple ExprContext here.
     * That risks intra-query memory leakage, since we might pass through here
     * many times before that ExprContext gets reset; but ExecSetParamPlan
     * doesn't normally leak any memory in the context (see its comments), so
     * it doesn't seem worth complicating this function's API to pass it a
     * shorter-lived ExprContext.  This might need to change someday.
     */
    ExecSetParamPlanMulti(sendParams, GetPerTupleExprContext(estate));

    /* Allocate object for return value. */
    pei = palloc0(size_of::<ParallelExecutorInfoFull>()) as *mut ParallelExecutorInfo;
    let peif = pei as *mut ParallelExecutorInfoFull;
    (*peif).finished = false;
    (*peif).planstate = planstate;

    /* Fix up and serialize plan to be sent to workers. */
    pstmt_data = ExecSerializePlan((*planstate).plan, estate);

    /* Create a parallel context. */
    pcxt = CreateParallelContext(
        c"postgres".as_ptr(),
        c"ParallelQueryMain".as_ptr(),
        nworkers,
    );
    (*peif).pcxt = pcxt;

    /*
     * Before telling the parallel context to create a dynamic shared memory
     * segment, we need to figure out how big it should be.  Estimate space
     * for the various things we need to store.
     */

    /* Estimate space for fixed-size state. */
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator as *mut _ as *mut c_void,
        size_of::<FixedParallelExecutorState>(),
    );
    shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

    /* Estimate space for query text. */
    query_len = strlen((*estate).es_sourceText) as c_int;
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator as *mut _ as *mut c_void,
        (query_len + 1) as Size,
    );
    shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

    /* Estimate space for serialized PlannedStmt. */
    pstmt_len = strlen(pstmt_data) as c_int + 1;
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator as *mut _ as *mut c_void,
        pstmt_len as Size,
    );
    shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

    /* Estimate space for serialized ParamListInfo. */
    paramlistinfo_len = EstimateParamListSpace((*estate).es_param_list_info) as c_int;
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator as *mut _ as *mut c_void,
        paramlistinfo_len as Size,
    );
    shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

    /*
     * Estimate space for BufferUsage.
     *
     * If EXPLAIN is not in use and there are no extensions loaded that care,
     * we could skip this.  But we have no way of knowing whether anyone's
     * looking at pgBufferUsage, so do it unconditionally.
     */
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator as *mut _ as *mut c_void,
        mul_size(size_of::<BufferUsage>(), (*pcxt).nworkers as Size),
    );
    shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

    /*
     * Same thing for WalUsage.
     */
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator as *mut _ as *mut c_void,
        mul_size(size_of::<WalUsage>(), (*pcxt).nworkers as Size),
    );
    shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

    /* Estimate space for tuple queues. */
    shm_toc_estimate_chunk(
        &mut (*pcxt).estimator as *mut _ as *mut c_void,
        mul_size(PARALLEL_TUPLE_QUEUE_SIZE, (*pcxt).nworkers as Size),
    );
    shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

    /*
     * Give parallel-aware nodes a chance to add to the estimates, and get a
     * count of how many PlanState nodes there are.
     */
    e.pcxt = pcxt;
    e.nnodes = 0;
    ExecParallelEstimate(planstate, &mut e as *mut _ as *mut c_void);

    /* Estimate space for instrumentation, if required. */
    if (*estate).es_instrument != 0 {
        instrumentation_len = (core::mem::offset_of!(SharedExecutorInstrumentation, plan_node_id)
            + size_of::<c_int>() * e.nnodes as usize) as c_int;
        instrumentation_len = MAXALIGN(instrumentation_len as usize) as c_int;
        instrument_offset = instrumentation_len;
        instrumentation_len += mul_size(
            size_of::<Instrumentation>(),
            mul_size(e.nnodes as Size, nworkers as Size),
        ) as c_int;
        shm_toc_estimate_chunk(
            &mut (*pcxt).estimator as *mut _ as *mut c_void,
            instrumentation_len as Size,
        );
        shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

        /* Estimate space for JIT instrumentation, if required. */
        if (*estate).es_jit_flags != PGJIT_NONE {
            jit_instrumentation_len = (core::mem::offset_of!(SharedJitInstrumentation, jit_instr)
                + size_of::<JitInstrumentation>() * nworkers as usize)
                as c_int;
            shm_toc_estimate_chunk(
                &mut (*pcxt).estimator as *mut _ as *mut c_void,
                jit_instrumentation_len as Size,
            );
            shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);
        }
    }

    /* Estimate space for DSA area. */
    shm_toc_estimate_chunk(&mut (*pcxt).estimator as *mut _ as *mut c_void, dsa_minsize);
    shm_toc_estimate_keys(&mut (*pcxt).estimator as *mut _ as *mut c_void, 1);

    /*
     * InitializeParallelDSM() passes the active snapshot to the parallel
     * worker, which uses it to set es_snapshot.  Make sure we don't set
     * es_snapshot differently in the child.
     */
    Assert!(GetActiveSnapshot() == (*estate).es_snapshot);

    /* Everyone's had a chance to ask for space, so now create the DSM. */
    InitializeParallelDSM(pcxt);

    /*
     * OK, now we have a dynamic shared memory segment, and it should be big
     * enough to store all of the data we estimated we would want to put into
     * it, plus whatever general stuff (not specifically executor-related) the
     * ParallelContext itself needs to store there.  None of the space we
     * asked for has been allocated or initialized yet, though, so do that.
     */

    /* Store fixed-size state. */
    fpes = shm_toc_allocate((*pcxt).toc, size_of::<FixedParallelExecutorState>())
        as *mut FixedParallelExecutorState;
    (*fpes).tuples_needed = tuples_needed;
    (*fpes).param_exec = InvalidDsaPointer;
    (*fpes).eflags = (*estate).es_top_eflags;
    (*fpes).jit_flags = (*estate).es_jit_flags;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_EXECUTOR_FIXED, fpes as *mut c_void);

    /* Store query string */
    query_string = shm_toc_allocate((*pcxt).toc, (query_len + 1) as Size) as *mut c_char;
    ptr::copy_nonoverlapping(
        (*estate).es_sourceText,
        query_string,
        (query_len + 1) as usize,
    );
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_QUERY_TEXT, query_string as *mut c_void);

    /* Store serialized PlannedStmt. */
    pstmt_space = shm_toc_allocate((*pcxt).toc, pstmt_len as Size) as *mut c_char;
    ptr::copy_nonoverlapping(pstmt_data, pstmt_space, pstmt_len as usize);
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_PLANNEDSTMT, pstmt_space as *mut c_void);

    /* Store serialized ParamListInfo. */
    paramlistinfo_space = shm_toc_allocate((*pcxt).toc, paramlistinfo_len as Size) as *mut c_char;
    shm_toc_insert(
        (*pcxt).toc,
        PARALLEL_KEY_PARAMLISTINFO,
        paramlistinfo_space as *mut c_void,
    );
    SerializeParamList((*estate).es_param_list_info, &mut paramlistinfo_space);

    /* Allocate space for each worker's BufferUsage; no need to initialize. */
    bufusage_space = shm_toc_allocate(
        (*pcxt).toc,
        mul_size(size_of::<BufferUsage>(), (*pcxt).nworkers as Size),
    ) as *mut BufferUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_BUFFER_USAGE, bufusage_space as *mut c_void);
    (*peif).buffer_usage = bufusage_space;

    /* Same for WalUsage. */
    walusage_space = shm_toc_allocate(
        (*pcxt).toc,
        mul_size(size_of::<WalUsage>(), (*pcxt).nworkers as Size),
    ) as *mut WalUsage;
    shm_toc_insert((*pcxt).toc, PARALLEL_KEY_WAL_USAGE, walusage_space as *mut c_void);
    (*peif).wal_usage = walusage_space;

    /* Set up the tuple queues that the workers will write into. */
    (*peif).tqueue = ExecParallelSetupTupleQueues(pcxt, false);

    /* We don't need the TupleQueueReaders yet, though. */
    (*peif).reader = ptr::null_mut();

    /*
     * If instrumentation options were supplied, allocate space for the data.
     * It only gets partially initialized here; the rest happens during
     * ExecParallelInitializeDSM.
     */
    if (*estate).es_instrument != 0 {
        let instrument: *mut Instrumentation;
        let mut i: c_int;

        instrumentation =
            shm_toc_allocate((*pcxt).toc, instrumentation_len as Size) as *mut SharedExecutorInstrumentation;
        (*instrumentation).instrument_options = (*estate).es_instrument;
        (*instrumentation).instrument_offset = instrument_offset;
        (*instrumentation).num_workers = nworkers;
        (*instrumentation).num_plan_nodes = e.nnodes;
        instrument = GetInstrumentationArray(instrumentation);
        i = 0;
        while i < nworkers * e.nnodes {
            InstrInit(instrument.add(i as usize), (*estate).es_instrument);
            i += 1;
        }
        shm_toc_insert(
            (*pcxt).toc,
            PARALLEL_KEY_INSTRUMENTATION,
            instrumentation as *mut c_void,
        );
        (*peif).instrumentation = instrumentation;

        if (*estate).es_jit_flags != PGJIT_NONE {
            jit_instrumentation = shm_toc_allocate((*pcxt).toc, jit_instrumentation_len as Size)
                as *mut SharedJitInstrumentation;
            (*jit_instrumentation).num_workers = nworkers;
            ptr::write_bytes(
                (*jit_instrumentation).jit_instr.as_mut_ptr(),
                0,
                nworkers as usize,
            );
            shm_toc_insert(
                (*pcxt).toc,
                PARALLEL_KEY_JIT_INSTRUMENTATION,
                jit_instrumentation as *mut c_void,
            );
            (*peif).jit_instrumentation = jit_instrumentation;
        }
    }

    /*
     * Create a DSA area that can be used by the leader and all workers.
     * (However, if we failed to create a DSM and are using private memory
     * instead, then skip this.)
     */
    if !(*pcxt).seg.is_null() {
        let area_space: *mut c_char;

        area_space = shm_toc_allocate((*pcxt).toc, dsa_minsize) as *mut c_char;
        shm_toc_insert((*pcxt).toc, PARALLEL_KEY_DSA, area_space as *mut c_void);
        (*peif).area = dsa_create_in_place(
            area_space as *mut c_void,
            dsa_minsize,
            LWTRANCHE_PARALLEL_QUERY_DSA as c_int,
            (*pcxt).seg,
        );

        /*
         * Serialize parameters, if any, using DSA storage.  We don't dare use
         * the main parallel query DSM for this because we might relaunch
         * workers after the values have changed (and thus the amount of
         * storage required has changed).
         */
        if !bms_is_empty(sendParams) {
            (*peif).param_exec = SerializeParamExecParams(estate, sendParams, (*peif).area);
            (*fpes).param_exec = (*peif).param_exec;
        }
    }

    /*
     * Give parallel-aware nodes a chance to initialize their shared data.
     * This also initializes the elements of instrumentation->ps_instrument,
     * if it exists.
     */
    d.pcxt = pcxt;
    d.instrumentation = instrumentation;
    d.nnodes = 0;

    /* Install our DSA area while initializing the plan. */
    (*estate).es_query_dsa = (*peif).area;
    ExecParallelInitializeDSM(planstate, &mut d as *mut _ as *mut c_void);
    (*estate).es_query_dsa = ptr::null_mut();

    /*
     * Make sure that the world hasn't shifted under our feet.  This could
     * probably just be an Assert(), but let's be conservative for now.
     */
    if e.nnodes != d.nnodes {
        elog!(ERROR, "inconsistent count of PlanState nodes");
    }

    /* OK, we're ready to rock and roll. */
    pei
}

/*
 * Set up tuple queue readers to read the results of a parallel subplan.
 *
 * This is separate from ExecInitParallelPlan() because we can launch the
 * worker processes and let them start doing something before we do this.
 */
pub unsafe fn ExecParallelCreateReaders(pei: *mut ParallelExecutorInfo) {
    let peif = pei as *mut ParallelExecutorInfoFull;
    let nworkers: c_int = (*(*peif).pcxt).nworkers_launched;
    let mut i: c_int;

    Assert!((*peif).reader.is_null());

    if nworkers > 0 {
        (*peif).reader =
            palloc(nworkers as usize * size_of::<*mut TupleQueueReader>()) as *mut *mut TupleQueueReader;

        i = 0;
        while i < nworkers {
            shm_mq_set_handle(
                *(*peif).tqueue.add(i as usize),
                (*(*(*peif).pcxt).worker.add(i as usize)).bgwhandle,
            );
            *(*peif).reader.add(i as usize) =
                CreateTupleQueueReader(*(*peif).tqueue.add(i as usize));
            i += 1;
        }
    }
}

/*
 * Re-initialize the parallel executor shared memory state before launching
 * a fresh batch of workers.
 */
pub unsafe fn ExecParallelReinitialize(
    planstate: *mut PlanState,
    pei: *mut ParallelExecutorInfo,
    sendParams: *mut Bitmapset,
) {
    let peif = pei as *mut ParallelExecutorInfoFull;
    let estate: *mut EState = (*planstate).state;
    let fpes: *mut FixedParallelExecutorState;

    /* Old workers must already be shut down */
    Assert!((*peif).finished);

    /*
     * Force any initplan outputs that we're going to pass to workers to be
     * evaluated, if they weren't already (see comments in
     * ExecInitParallelPlan).
     */
    ExecSetParamPlanMulti(sendParams, GetPerTupleExprContext(estate));

    ReinitializeParallelDSM((*peif).pcxt);
    (*peif).tqueue = ExecParallelSetupTupleQueues((*peif).pcxt, true);
    (*peif).reader = ptr::null_mut();
    (*peif).finished = false;

    fpes = shm_toc_lookup((*(*peif).pcxt).toc, PARALLEL_KEY_EXECUTOR_FIXED, false)
        as *mut FixedParallelExecutorState;

    /* Free any serialized parameters from the last round. */
    if DsaPointerIsValid((*fpes).param_exec) {
        dsa_free((*peif).area, (*fpes).param_exec);
        (*fpes).param_exec = InvalidDsaPointer;
    }

    /* Serialize current parameter values if required. */
    if !bms_is_empty(sendParams) {
        (*peif).param_exec = SerializeParamExecParams(estate, sendParams, (*peif).area);
        (*fpes).param_exec = (*peif).param_exec;
    }

    /* Traverse plan tree and let each child node reset associated state. */
    (*estate).es_query_dsa = (*peif).area;
    ExecParallelReInitializeDSM(planstate, (*peif).pcxt as *mut c_void);
    (*estate).es_query_dsa = ptr::null_mut();
}

/*
 * Traverse plan tree to reinitialize per-node dynamic shared memory state
 */
unsafe fn ExecParallelReInitializeDSM(planstate: *mut PlanState, context: *mut c_void) -> bool {
    let pcxt = context as *mut ParallelContext;

    if planstate.is_null() {
        return false;
    }

    /*
     * Call reinitializers for DSM-using plan nodes.
     */
    match nodeTag(planstate as *const PlanState) {
        NodeTag::T_SeqScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecSeqScanReInitializeDSM(planstate as *mut SeqScanState, pcxt);
            }
        }
        NodeTag::T_IndexScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecIndexScanReInitializeDSM(planstate as *mut IndexScanState, pcxt);
            }
        }
        NodeTag::T_IndexOnlyScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecIndexOnlyScanReInitializeDSM(planstate as *mut IndexOnlyScanState, pcxt);
            }
        }
        NodeTag::T_ForeignScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecForeignScanReInitializeDSM(planstate as *mut ForeignScanState, pcxt);
            }
        }
        NodeTag::T_AppendState => {
            if (*(*planstate).plan).parallel_aware {
                ExecAppendReInitializeDSM(planstate as *mut AppendState, pcxt);
            }
        }
        NodeTag::T_CustomScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecCustomScanReInitializeDSM(planstate as *mut CustomScanState, pcxt);
            }
        }
        NodeTag::T_BitmapHeapScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecBitmapHeapReInitializeDSM(planstate as *mut BitmapHeapScanState, pcxt);
            }
        }
        NodeTag::T_HashJoinState => {
            if (*(*planstate).plan).parallel_aware {
                ExecHashJoinReInitializeDSM(planstate as *mut HashJoinState, pcxt);
            }
        }
        NodeTag::T_BitmapIndexScanState
        | NodeTag::T_HashState
        | NodeTag::T_SortState
        | NodeTag::T_IncrementalSortState
        | NodeTag::T_MemoizeState => {
            /* these nodes have DSM state, but no reinitialization is required */
        }

        _ => {}
    }

    planstate_tree_walker(
        planstate,
        Some(ExecParallelReInitializeDSM as unsafe fn(*mut PlanState, *mut c_void) -> bool),
        context,
    )
}

/*
 * Copy instrumentation information about this node and its descendants from
 * dynamic shared memory.
 */
unsafe fn ExecParallelRetrieveInstrumentation(
    planstate: *mut PlanState,
    context: *mut c_void,
) -> bool {
    let instrumentation = context as *mut SharedExecutorInstrumentation;
    let mut instrument: *mut Instrumentation;
    let mut i: c_int;
    let mut n: c_int;
    let ibytes: c_int;
    let plan_node_id: c_int = (*(*planstate).plan).plan_node_id;
    let oldcontext: MemoryContext;

    /* Find the instrumentation for this node. */
    i = 0;
    while i < (*instrumentation).num_plan_nodes {
        if *(*instrumentation).plan_node_id.as_ptr().add(i as usize) == plan_node_id {
            break;
        }
        i += 1;
    }
    if i >= (*instrumentation).num_plan_nodes {
        elog!(ERROR, "plan node {} not found", plan_node_id);
    }

    /* Accumulate the statistics from all workers. */
    instrument = GetInstrumentationArray(instrumentation);
    instrument = instrument.add((i * (*instrumentation).num_workers) as usize);
    n = 0;
    while n < (*instrumentation).num_workers {
        InstrAggNode((*planstate).instrument, instrument.add(n as usize));
        n += 1;
    }

    /*
     * Also store the per-worker detail.
     *
     * Worker instrumentation should be allocated in the same context as the
     * regular instrumentation information, which is the per-query context.
     * Switch into per-query memory context.
     */
    oldcontext = MemoryContextSwitchTo((*(*planstate).state).es_query_cxt);
    ibytes = mul_size((*instrumentation).num_workers as Size, size_of::<Instrumentation>()) as c_int;
    (*planstate).worker_instrument = palloc(
        ibytes as usize + core::mem::offset_of!(WorkerInstrumentation, instrument),
    ) as *mut WorkerInstrumentation;
    MemoryContextSwitchTo(oldcontext);

    (*(*planstate).worker_instrument).num_workers = (*instrumentation).num_workers;
    ptr::copy_nonoverlapping(
        instrument as *const c_char,
        (*(*planstate).worker_instrument).instrument.as_mut_ptr() as *mut c_char,
        ibytes as usize,
    );

    /* Perform any node-type-specific work that needs to be done. */
    match nodeTag(planstate as *const PlanState) {
        NodeTag::T_IndexScanState => {
            ExecIndexScanRetrieveInstrumentation(planstate as *mut IndexScanState);
        }
        NodeTag::T_IndexOnlyScanState => {
            ExecIndexOnlyScanRetrieveInstrumentation(planstate as *mut IndexOnlyScanState);
        }
        NodeTag::T_BitmapIndexScanState => {
            ExecBitmapIndexScanRetrieveInstrumentation(planstate as *mut BitmapIndexScanState);
        }
        NodeTag::T_SortState => {
            ExecSortRetrieveInstrumentation(planstate as *mut SortState);
        }
        NodeTag::T_IncrementalSortState => {
            ExecIncrementalSortRetrieveInstrumentation(planstate as *mut IncrementalSortState);
        }
        NodeTag::T_HashState => {
            ExecHashRetrieveInstrumentation(planstate as *mut HashState);
        }
        NodeTag::T_AggState => {
            ExecAggRetrieveInstrumentation(planstate as *mut AggState);
        }
        NodeTag::T_MemoizeState => {
            ExecMemoizeRetrieveInstrumentation(planstate as *mut MemoizeState);
        }
        NodeTag::T_BitmapHeapScanState => {
            ExecBitmapHeapRetrieveInstrumentation(planstate as *mut BitmapHeapScanState);
        }
        _ => {}
    }

    planstate_tree_walker(
        planstate,
        Some(ExecParallelRetrieveInstrumentation as unsafe fn(*mut PlanState, *mut c_void) -> bool),
        context,
    )
}

/*
 * Add up the workers' JIT instrumentation from dynamic shared memory.
 */
unsafe fn ExecParallelRetrieveJitInstrumentation(
    planstate: *mut PlanState,
    shared_jit: *mut SharedJitInstrumentation,
) {
    let combined: *mut JitInstrumentation;
    let ibytes: c_int;

    let mut n: c_int;

    /*
     * Accumulate worker JIT instrumentation into the combined JIT
     * instrumentation, allocating it if required.
     */
    if (*(*planstate).state).es_jit_worker_instr.is_null() {
        (*(*planstate).state).es_jit_worker_instr = MemoryContextAllocZero(
            (*(*planstate).state).es_query_cxt,
            size_of::<JitInstrumentation>(),
        ) as *mut JitInstrumentation;
    }
    combined = (*(*planstate).state).es_jit_worker_instr;

    /* Accumulate all the workers' instrumentations. */
    n = 0;
    while n < (*shared_jit).num_workers {
        InstrJitAgg(combined, (*shared_jit).jit_instr.as_mut_ptr().add(n as usize));
        n += 1;
    }

    /*
     * Store the per-worker detail.
     *
     * Similar to ExecParallelRetrieveInstrumentation(), allocate the
     * instrumentation in per-query context.
     */
    ibytes = (core::mem::offset_of!(SharedJitInstrumentation, jit_instr)
        + mul_size((*shared_jit).num_workers as Size, size_of::<JitInstrumentation>()))
        as c_int;
    (*planstate).worker_jit_instrument =
        MemoryContextAlloc((*(*planstate).state).es_query_cxt, ibytes as Size)
            as *mut SharedJitInstrumentation;

    ptr::copy_nonoverlapping(
        shared_jit as *const c_char,
        (*planstate).worker_jit_instrument as *mut c_char,
        ibytes as usize,
    );
}

/*
 * Finish parallel execution.  We wait for parallel workers to finish, and
 * accumulate their buffer/WAL usage.
 */
pub unsafe fn ExecParallelFinish(pei: *mut ParallelExecutorInfo) {
    let peif = pei as *mut ParallelExecutorInfoFull;
    let nworkers: c_int = (*(*peif).pcxt).nworkers_launched;
    let mut i: c_int;

    /* Make this be a no-op if called twice in a row. */
    if (*peif).finished {
        return;
    }

    /*
     * Detach from tuple queues ASAP, so that any still-active workers will
     * notice that no further results are wanted.
     */
    if !(*peif).tqueue.is_null() {
        i = 0;
        while i < nworkers {
            shm_mq_detach(*(*peif).tqueue.add(i as usize));
            i += 1;
        }
        pfree((*peif).tqueue as *mut c_void);
        (*peif).tqueue = ptr::null_mut();
    }

    /*
     * While we're waiting for the workers to finish, let's get rid of the
     * tuple queue readers.  (Any other local cleanup could be done here too.)
     */
    if !(*peif).reader.is_null() {
        i = 0;
        while i < nworkers {
            DestroyTupleQueueReader(*(*peif).reader.add(i as usize));
            i += 1;
        }
        pfree((*peif).reader as *mut c_void);
        (*peif).reader = ptr::null_mut();
    }

    /* Now wait for the workers to finish. */
    WaitForParallelWorkersToFinish((*peif).pcxt);

    /*
     * Next, accumulate buffer/WAL usage.  (This must wait for the workers to
     * finish, or we might get incomplete data.)
     */
    i = 0;
    while i < nworkers {
        InstrAccumParallelQuery(
            (*peif).buffer_usage.add(i as usize),
            (*peif).wal_usage.add(i as usize),
        );
        i += 1;
    }

    (*peif).finished = true;
}

/*
 * Accumulate instrumentation, and then clean up whatever ParallelExecutorInfo
 * resources still exist after ExecParallelFinish.  We separate these
 * routines because someone might want to examine the contents of the DSM
 * after ExecParallelFinish and before calling this routine.
 */
pub unsafe fn ExecParallelCleanup(pei: *mut ParallelExecutorInfo) {
    let peif = pei as *mut ParallelExecutorInfoFull;

    /* Accumulate instrumentation, if any. */
    if !(*peif).instrumentation.is_null() {
        ExecParallelRetrieveInstrumentation(
            (*peif).planstate,
            (*peif).instrumentation as *mut c_void,
        );
    }

    /* Accumulate JIT instrumentation, if any. */
    if !(*peif).jit_instrumentation.is_null() {
        ExecParallelRetrieveJitInstrumentation((*peif).planstate, (*peif).jit_instrumentation);
    }

    /* Free any serialized parameters. */
    if DsaPointerIsValid((*peif).param_exec) {
        dsa_free((*peif).area, (*peif).param_exec);
        (*peif).param_exec = InvalidDsaPointer;
    }
    if !(*peif).area.is_null() {
        dsa_detach((*peif).area);
        (*peif).area = ptr::null_mut();
    }
    if !(*peif).pcxt.is_null() {
        DestroyParallelContext((*peif).pcxt);
        (*peif).pcxt = ptr::null_mut();
    }
    pfree(pei as *mut c_void);
}

/*
 * Create a DestReceiver to write tuples we produce to the shm_mq designated
 * for that purpose.
 */
unsafe fn ExecParallelGetReceiver(seg: *mut dsm_segment, toc: *mut shm_toc) -> *mut DestReceiver {
    let mut mqspace: *mut c_char;
    let mq: *mut shm_mq;

    mqspace = shm_toc_lookup(toc, PARALLEL_KEY_TUPLE_QUEUE, false) as *mut c_char;
    mqspace = mqspace.add(ParallelWorkerNumber as usize * PARALLEL_TUPLE_QUEUE_SIZE);
    mq = mqspace as *mut shm_mq;
    shm_mq_set_sender(mq, MyProc);
    CreateTupleQueueDestReceiver(shm_mq_attach(mq, seg, ptr::null_mut()))
}

/*
 * Create a QueryDesc for the PlannedStmt we are to execute, and return it.
 */
unsafe fn ExecParallelGetQueryDesc(
    toc: *mut shm_toc,
    receiver: *mut DestReceiver,
    instrument_options: c_int,
) -> *mut QueryDesc {
    let pstmtspace: *mut c_char;
    let mut paramspace: *mut c_char;
    let pstmt: *mut PlannedStmt;
    let paramLI: ParamListInfo;
    let queryString: *mut c_char;

    /* Get the query string from shared memory */
    queryString = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, false) as *mut c_char;

    /* Reconstruct leader-supplied PlannedStmt. */
    pstmtspace = shm_toc_lookup(toc, PARALLEL_KEY_PLANNEDSTMT, false) as *mut c_char;
    pstmt = stringToNode(pstmtspace) as *mut PlannedStmt;

    /* Reconstruct ParamListInfo. */
    paramspace = shm_toc_lookup(toc, PARALLEL_KEY_PARAMLISTINFO, false) as *mut c_char;
    paramLI = RestoreParamList(&mut paramspace);

    /* Create a QueryDesc for the query. */
    CreateQueryDesc(
        pstmt,
        queryString,
        GetActiveSnapshot(),
        InvalidSnapshot,
        receiver,
        paramLI,
        ptr::null_mut(),
        instrument_options,
    )
}

/*
 * Copy instrumentation information from this node and its descendants into
 * dynamic shared memory, so that the parallel leader can retrieve it.
 */
unsafe fn ExecParallelReportInstrumentation(
    planstate: *mut PlanState,
    context: *mut c_void,
) -> bool {
    let instrumentation = context as *mut SharedExecutorInstrumentation;
    let mut i: c_int;
    let plan_node_id: c_int = (*(*planstate).plan).plan_node_id;
    let mut instrument: *mut Instrumentation;

    InstrEndLoop((*planstate).instrument);

    /*
     * If we shuffled the plan_node_id values in ps_instrument into sorted
     * order, we could use binary search here.  This might matter someday if
     * we're pushing down sufficiently large plan trees.  For now, do it the
     * slow, dumb way.
     */
    i = 0;
    while i < (*instrumentation).num_plan_nodes {
        if *(*instrumentation).plan_node_id.as_ptr().add(i as usize) == plan_node_id {
            break;
        }
        i += 1;
    }
    if i >= (*instrumentation).num_plan_nodes {
        elog!(ERROR, "plan node {} not found", plan_node_id);
    }

    /*
     * Add our statistics to the per-node, per-worker totals.  It's possible
     * that this could happen more than once if we relaunched workers.
     */
    instrument = GetInstrumentationArray(instrumentation);
    instrument = instrument.add((i * (*instrumentation).num_workers) as usize);
    Assert!(IsParallelWorker());
    Assert!(ParallelWorkerNumber < (*instrumentation).num_workers);
    InstrAggNode(
        instrument.add(ParallelWorkerNumber as usize),
        (*planstate).instrument,
    );

    planstate_tree_walker(
        planstate,
        Some(ExecParallelReportInstrumentation as unsafe fn(*mut PlanState, *mut c_void) -> bool),
        context,
    )
}

/*
 * Initialize the PlanState and its descendants with the information
 * retrieved from shared memory.  This has to be done once the PlanState
 * is allocated and initialized by executor; that is, after ExecutorStart().
 */
unsafe fn ExecParallelInitializeWorker(planstate: *mut PlanState, context: *mut c_void) -> bool {
    let pwcxt = context as *mut ParallelWorkerContext;

    if planstate.is_null() {
        return false;
    }

    match nodeTag(planstate as *const PlanState) {
        NodeTag::T_SeqScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecSeqScanInitializeWorker(planstate as *mut SeqScanState, pwcxt);
            }
        }
        NodeTag::T_IndexScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIndexScanInitializeWorker(planstate as *mut IndexScanState, pwcxt);
        }
        NodeTag::T_IndexOnlyScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIndexOnlyScanInitializeWorker(planstate as *mut IndexOnlyScanState, pwcxt);
        }
        NodeTag::T_BitmapIndexScanState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecBitmapIndexScanInitializeWorker(planstate as *mut BitmapIndexScanState, pwcxt);
        }
        NodeTag::T_ForeignScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecForeignScanInitializeWorker(planstate as *mut ForeignScanState, pwcxt);
            }
        }
        NodeTag::T_AppendState => {
            if (*(*planstate).plan).parallel_aware {
                ExecAppendInitializeWorker(planstate as *mut AppendState, pwcxt);
            }
        }
        NodeTag::T_CustomScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecCustomScanInitializeWorker(planstate as *mut CustomScanState, pwcxt);
            }
        }
        NodeTag::T_BitmapHeapScanState => {
            if (*(*planstate).plan).parallel_aware {
                ExecBitmapHeapInitializeWorker(planstate as *mut BitmapHeapScanState, pwcxt);
            }
        }
        NodeTag::T_HashJoinState => {
            if (*(*planstate).plan).parallel_aware {
                ExecHashJoinInitializeWorker(planstate as *mut HashJoinState, pwcxt);
            }
        }
        NodeTag::T_HashState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecHashInitializeWorker(planstate as *mut HashState, pwcxt);
        }
        NodeTag::T_SortState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecSortInitializeWorker(planstate as *mut SortState, pwcxt);
        }
        NodeTag::T_IncrementalSortState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecIncrementalSortInitializeWorker(planstate as *mut IncrementalSortState, pwcxt);
        }
        NodeTag::T_AggState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecAggInitializeWorker(planstate as *mut AggState, pwcxt);
        }
        NodeTag::T_MemoizeState => {
            /* even when not parallel-aware, for EXPLAIN ANALYZE */
            ExecMemoizeInitializeWorker(planstate as *mut MemoizeState, pwcxt);
        }
        _ => {}
    }

    planstate_tree_walker(
        planstate,
        Some(ExecParallelInitializeWorker as unsafe fn(*mut PlanState, *mut c_void) -> bool),
        context,
    )
}

/*
 * Main entrypoint for parallel query worker processes.
 *
 * We reach this function from ParallelWorkerMain, so the setup necessary to
 * create a sensible parallel environment has already been done;
 * ParallelWorkerMain worries about stuff like the transaction state, combo
 * CID mappings, and GUC values, so we don't need to deal with any of that
 * here.
 *
 * Our job is to deal with concerns specific to the executor.  The parallel
 * group leader will have stored a serialized PlannedStmt, and it's our job
 * to execute that plan and write the resulting tuples to the appropriate
 * tuple queue.  Various bits of supporting information that we need in order
 * to do this are also stored in the dsm_segment and can be accessed through
 * the shm_toc.
 */
pub unsafe fn ParallelQueryMain(seg: *mut dsm_segment, toc: *mut shm_toc) {
    let fpes: *mut FixedParallelExecutorState;
    let buffer_usage: *mut BufferUsage;
    let wal_usage: *mut WalUsage;
    let receiver: *mut DestReceiver;
    let queryDesc: *mut QueryDesc;
    let instrumentation: *mut SharedExecutorInstrumentation;
    let jit_instrumentation: *mut SharedJitInstrumentation;
    let mut instrument_options: c_int = 0;
    let area_space: *mut c_void;
    let area: *mut dsa_area;
    let mut pwcxt: ParallelWorkerContext = core::mem::zeroed();

    /* Get fixed-size state. */
    fpes = shm_toc_lookup(toc, PARALLEL_KEY_EXECUTOR_FIXED, false)
        as *mut FixedParallelExecutorState;

    /* Set up DestReceiver, SharedExecutorInstrumentation, and QueryDesc. */
    receiver = ExecParallelGetReceiver(seg, toc);
    instrumentation =
        shm_toc_lookup(toc, PARALLEL_KEY_INSTRUMENTATION, true) as *mut SharedExecutorInstrumentation;
    if !instrumentation.is_null() {
        instrument_options = (*instrumentation).instrument_options;
    }
    jit_instrumentation =
        shm_toc_lookup(toc, PARALLEL_KEY_JIT_INSTRUMENTATION, true) as *mut SharedJitInstrumentation;
    queryDesc = ExecParallelGetQueryDesc(toc, receiver, instrument_options);

    /* Setting debug_query_string for individual workers */
    debug_query_string = (*queryDesc).sourceText;

    /* Report workers' query for monitoring purposes */
    pgstat_report_activity(STATE_RUNNING, debug_query_string);

    /* Attach to the dynamic shared memory area. */
    area_space = shm_toc_lookup(toc, PARALLEL_KEY_DSA, false);
    area = dsa_attach_in_place(area_space, seg);

    /* Start up the executor */
    (*(*queryDesc).plannedstmt).jitFlags = (*fpes).jit_flags;
    ExecutorStart(queryDesc, (*fpes).eflags);

    /* Special executor initialization steps for parallel workers */
    (*(*(*queryDesc).planstate).state).es_query_dsa = area;
    if DsaPointerIsValid((*fpes).param_exec) {
        let paramexec_space: *mut c_char;

        paramexec_space = dsa_get_address(area, (*fpes).param_exec) as *mut c_char;
        RestoreParamExecParams(paramexec_space, (*queryDesc).estate);
    }
    pwcxt.toc = toc;
    pwcxt.seg = seg;
    ExecParallelInitializeWorker((*queryDesc).planstate, &mut pwcxt as *mut _ as *mut c_void);

    /* Pass down any tuple bound */
    ExecSetTupleBound((*fpes).tuples_needed, (*queryDesc).planstate);

    /*
     * Prepare to track buffer/WAL usage during query execution.
     *
     * We do this after starting up the executor to match what happens in the
     * leader, which also doesn't count buffer accesses and WAL activity that
     * occur during executor startup.
     */
    InstrStartParallelQuery();

    /*
     * Run the plan.  If we specified a tuple bound, be careful not to demand
     * more tuples than that.
     */
    ExecutorRun(
        queryDesc,
        ForwardScanDirection,
        if (*fpes).tuples_needed < 0 {
            0_u64
        } else {
            (*fpes).tuples_needed as u64
        },
    );

    /* Shut down the executor */
    ExecutorFinish(queryDesc);

    /* Report buffer/WAL usage during parallel execution. */
    buffer_usage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false) as *mut BufferUsage;
    wal_usage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false) as *mut WalUsage;
    InstrEndParallelQuery(
        buffer_usage.add(ParallelWorkerNumber as usize),
        wal_usage.add(ParallelWorkerNumber as usize),
    );

    /* Report instrumentation data if any instrumentation options are set. */
    if !instrumentation.is_null() {
        ExecParallelReportInstrumentation(
            (*queryDesc).planstate,
            instrumentation as *mut c_void,
        );
    }

    /* Report JIT instrumentation data if any */
    if !(*(*queryDesc).estate).es_jit.is_null() && !jit_instrumentation.is_null() {
        Assert!(ParallelWorkerNumber < (*jit_instrumentation).num_workers);
        *(*jit_instrumentation).jit_instr.as_mut_ptr().add(ParallelWorkerNumber as usize) =
            (*(*(*queryDesc).estate).es_jit).instr;
    }

    /* Must do this after capturing instrumentation. */
    ExecutorEnd(queryDesc);

    /* Cleanup. */
    dsa_detach(area);
    FreeQueryDesc(queryDesc);
    (*receiver).rDestroy.unwrap()(receiver);
}
