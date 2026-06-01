/*-------------------------------------------------------------------------
 *
 * nodeGatherMerge.c
 *		Scan a plan in multiple workers, and do order-preserving merge.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeGatherMerge.c
 *	  (companion header: src/include/executor/nodeGatherMerge.h)
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use std::ffi::{c_int, c_void};
use std::ptr;

use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::MinimalTuple;
use crate::executor::execTuples::{ExecStoreMinimalTuple, TTSOpsMinimalTuple};
use crate::executor::executor::{
    ExecAssignExprContext, ExecConditionalAssignProjectionInfo, ExecEndNode, ExecGetResultType,
    ExecInitExtraTupleSlot, ExecInitNode, ExecInitResultTypeTL, ExecProcNode, ExecProject,
    ExecReScan, ResetExprContext,
};
use crate::executor::tqueue::TupleQueueReaderNext;
use crate::executor::tuptable::{slot_getattr, ExecClearTuple, TupIsNull, TupleTableSlot};
use crate::access::common::heaptuple::heap_copy_minimal_tuple;
use crate::lib::binaryheap::{
    binaryheap, binaryheap_add_unordered, binaryheap_allocate, binaryheap_build, binaryheap_empty,
    binaryheap_first, binaryheap_remove_first, binaryheap_replace_first, binaryheap_reset,
};
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::nodes::bitmapset::{bms_add_member, Bitmapset};
use crate::nodes::execnodes::{
    dsa_area, outerPlanState, EState, ExprContext, GatherMergeState, ParallelExecutorInfo,
    PlanState, TupleQueueReader,
};
use crate::nodes::extensible::ParallelContext;
use crate::nodes::plannodes::{GatherMerge, Plan};
use crate::nodes::primnodes::OUTER_VAR;
use crate::optimizer::optimizer::parallel_leader_participation;
use crate::utils::sort::sortsupport::{
    ApplySortComparator, PrepareSortSupportFromOrderingOp, SortSupport, SortSupportData,
};
use crate::utils::palloc::CurrentMemoryContext;

use crate::{castNode, makeNode, Assert};

/*
 * When we read tuples from workers, it's a good idea to read several at once
 * for efficiency when possible: this minimizes context-switching overhead.
 * But reading too many at a time wastes memory without improving performance.
 * We'll read up to MAX_TUPLE_STORE tuples (in addition to the first one).
 */
const MAX_TUPLE_STORE: c_int = 10;

/*
 * Pending-tuple array for each worker.  This holds additional tuples that
 * we were able to fetch from the worker, but can't process yet.  In addition,
 * this struct holds the "done" flag indicating the worker is known to have
 * no more tuples.  (We do not use this struct for the leader; we don't keep
 * any pending tuples for the leader, and the need_to_scan_locally flag serves
 * as its "done" indicator.)
 *
 * NB: the canonical GMReaderTupleBuffer is private to nodeGatherMerge.c.  The
 * execnodes.h placeholder is opaque, so we define the real layout locally and
 * cast gm_state->gm_tuple_buffers to it.
 */
#[repr(C)]
struct GMReaderTupleBuffer {
    tuple: *mut MinimalTuple,  /* array of length MAX_TUPLE_STORE */
    nTuples: c_int,            /* number of tuples currently stored */
    readCounter: c_int,        /* index of next tuple to extract */
    done: bool,                /* true if reader is known exhausted */
}

/* ----------------------------------------------------------------
 *		local plan-tree accessor stubs
 * ---------------------------------------------------------------- */

/* innerPlan(node) / outerPlan(node) on a Plan node (nodes/plannodes.h) */
unsafe fn innerPlan(node: *mut GatherMerge) -> *mut Plan {
    (*node).plan.righttree
}
unsafe fn outerPlan(node: *mut GatherMerge) -> *mut Plan {
    (*node).plan.lefttree
}

/* ----------------------------------------------------------------
 *		local helper stubs for not-yet-ported dependencies
 * ---------------------------------------------------------------- */

/* executor/execParallel.h (TODO: backend/executor/execParallel.c) */
unsafe fn ExecInitParallelPlan(
    _planstate: *mut PlanState,
    _estate: *mut EState,
    _sendParams: *mut Bitmapset,
    _nworkers: c_int,
    _tuples_needed: i64,
) -> *mut ParallelExecutorInfo {
    unimplemented!() // TODO: executor/execParallel.c
}
unsafe fn ExecParallelReinitialize(
    _planstate: *mut PlanState,
    _pei: *mut ParallelExecutorInfo,
    _sendParams: *mut Bitmapset,
) {
    unimplemented!() // TODO: executor/execParallel.c
}
unsafe fn ExecParallelCreateReaders(_pei: *mut ParallelExecutorInfo) {
    unimplemented!() // TODO: executor/execParallel.c
}
unsafe fn ExecParallelFinish(_pei: *mut ParallelExecutorInfo) {
    unimplemented!() // TODO: executor/execParallel.c
}
unsafe fn ExecParallelCleanup(_pei: *mut ParallelExecutorInfo) {
    unimplemented!() // TODO: executor/execParallel.c
}

/* parallel.h (TODO: access/parallel.c) */
unsafe fn LaunchParallelWorkers(_pcxt: *mut ParallelContext) {
    unimplemented!() // TODO: access/parallel.c
}

/*
 * Accessors for the opaque ParallelContext / ParallelExecutorInfo structs.
 * These mirror struct-field reads in the C code; until those structs gain
 * real layouts we stub the accessors.
 */
unsafe fn pei_pcxt(_pei: *mut ParallelExecutorInfo) -> *mut ParallelContext {
    unimplemented!() // TODO: executor/execParallel.h -> pei->pcxt
}
unsafe fn pei_area(_pei: *mut ParallelExecutorInfo) -> *mut dsa_area {
    unimplemented!() // TODO: executor/execParallel.h -> pei->area
}
unsafe fn pei_reader(_pei: *mut ParallelExecutorInfo) -> *mut *mut TupleQueueReader {
    unimplemented!() // TODO: executor/execParallel.h -> pei->reader
}
unsafe fn pcxt_nworkers_launched(_pcxt: *mut ParallelContext) -> c_int {
    unimplemented!() // TODO: access/parallel.h -> pcxt->nworkers_launched
}
unsafe fn pcxt_nworkers_to_launch(_pcxt: *mut ParallelContext) -> c_int {
    unimplemented!() // TODO: access/parallel.h -> pcxt->nworkers_to_launch
}

/*
 * INVERT_COMPARE_RESULT (c.h): swaps the sign of a 3-way comparison.  The
 * sortsupport.rs copy is private, so we inline our own.
 */
#[inline]
fn INVERT_COMPARE_RESULT(var: c_int) -> c_int {
    -var
}

/* ----------------------------------------------------------------
 *		ExecInitGather
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitGatherMerge(
    node: *mut GatherMerge,
    estate: *mut EState,
    eflags: c_int,
) -> *mut GatherMergeState {
    let gm_state: *mut GatherMergeState;
    let outerNode: *mut Plan;
    let tupDesc: TupleDesc;

    /* Gather merge node doesn't have innerPlan node. */
    Assert!(innerPlan(node).is_null());

    /*
     * create state structure
     */
    gm_state = makeNode!(GatherMergeState, T_GatherMergeState);
    (*gm_state).ps.plan = node as *mut Plan;
    (*gm_state).ps.state = estate;
    (*gm_state).ps.ExecProcNode = Some(ExecGatherMerge);

    (*gm_state).initialized = false;
    (*gm_state).gm_initialized = false;
    (*gm_state).tuples_needed = -1;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*gm_state).ps);

    /*
     * GatherMerge doesn't support checking a qual (it's always more efficient
     * to do it in the child node).
     */
    Assert!((*node).plan.qual.is_null());

    /*
     * now initialize outer plan
     */
    outerNode = outerPlan(node);
    *(&mut (*gm_state).ps.lefttree) = ExecInitNode(outerNode, estate, eflags);

    /*
     * Leader may access ExecProcNode result directly (if
     * need_to_scan_locally), or from workers via tuple queue.  So we can't
     * trivially rely on the slot type being fixed for expressions evaluated
     * within this node.
     */
    (*gm_state).ps.outeropsset = true;
    (*gm_state).ps.outeropsfixed = false;

    /*
     * Store the tuple descriptor into gather merge state, so we can use it
     * while initializing the gather merge slots.
     */
    tupDesc = ExecGetResultType(outerPlanState(&mut (*gm_state).ps));
    (*gm_state).tupDesc = tupDesc;

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*gm_state).ps);
    ExecConditionalAssignProjectionInfo(&mut (*gm_state).ps, tupDesc, OUTER_VAR);

    /*
     * Without projections result slot type is not trivially known, see
     * comment above.
     */
    if (*gm_state).ps.ps_ProjInfo.is_null() {
        (*gm_state).ps.resultopsset = true;
        (*gm_state).ps.resultopsfixed = false;
    }

    /*
     * initialize sort-key information
     */
    if (*node).numCols != 0 {
        let mut i: c_int;

        (*gm_state).gm_nkeys = (*node).numCols;
        (*gm_state).gm_sortkeys =
            palloc0(size_of::<SortSupportData>() * (*node).numCols as usize) as SortSupport;

        i = 0;
        while i < (*node).numCols {
            let sortKey: SortSupport = (*gm_state).gm_sortkeys.offset(i as isize);

            (*sortKey).ssup_cxt = CurrentMemoryContext;
            (*sortKey).ssup_collation = *(*node).collations.offset(i as isize);
            (*sortKey).ssup_nulls_first = *(*node).nullsFirst.offset(i as isize);
            (*sortKey).ssup_attno = *(*node).sortColIdx.offset(i as isize);

            /*
             * We don't perform abbreviated key conversion here, for the same
             * reasons that it isn't used in MergeAppend
             */
            (*sortKey).abbreviate = false;

            PrepareSortSupportFromOrderingOp(*(*node).sortOperators.offset(i as isize), sortKey);

            i += 1;
        }
    }

    /* Now allocate the workspace for gather merge */
    gather_merge_setup(gm_state);

    gm_state
}

/* ----------------------------------------------------------------
 *		ExecGatherMerge(node)
 *
 *		Scans the relation via multiple workers and returns
 *		the next qualifying tuple.
 * ----------------------------------------------------------------
 */
unsafe fn ExecGatherMerge(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut GatherMergeState = castNode!(GatherMergeState, T_GatherMergeState, pstate);
    let slot: *mut TupleTableSlot;
    let econtext: *mut ExprContext;

    CHECK_FOR_INTERRUPTS();

    /*
     * As with Gather, we don't launch workers until this node is actually
     * executed.
     */
    if !(*node).initialized {
        let estate: *mut EState = (*node).ps.state;
        let gm: *mut GatherMerge = castNode!(GatherMerge, T_GatherMerge, (*node).ps.plan);

        /*
         * Sometimes we might have to run without parallelism; but if parallel
         * mode is active then we can try to fire up some workers.
         */
        if (*gm).num_workers > 0 && (*estate).es_use_parallel_mode {
            let pcxt: *mut ParallelContext;

            /* Initialize, or re-initialize, shared state needed by workers. */
            if (*node).pei.is_null() {
                (*node).pei = ExecInitParallelPlan(
                    outerPlanState(&mut (*node).ps),
                    estate,
                    (*gm).initParam,
                    (*gm).num_workers,
                    (*node).tuples_needed,
                );
            } else {
                ExecParallelReinitialize(
                    outerPlanState(&mut (*node).ps),
                    (*node).pei,
                    (*gm).initParam,
                );
            }

            /* Try to launch workers. */
            pcxt = pei_pcxt((*node).pei);
            LaunchParallelWorkers(pcxt);
            /* We save # workers launched for the benefit of EXPLAIN */
            (*node).nworkers_launched = pcxt_nworkers_launched(pcxt);

            /*
             * Count number of workers originally wanted and actually
             * launched.
             */
            (*estate).es_parallel_workers_to_launch += pcxt_nworkers_to_launch(pcxt);
            (*estate).es_parallel_workers_launched += pcxt_nworkers_launched(pcxt);

            /* Set up tuple queue readers to read the results. */
            if pcxt_nworkers_launched(pcxt) > 0 {
                ExecParallelCreateReaders((*node).pei);
                /* Make a working array showing the active readers */
                (*node).nreaders = pcxt_nworkers_launched(pcxt);
                (*node).reader = palloc(
                    (*node).nreaders as usize * size_of::<*mut TupleQueueReader>(),
                ) as *mut *mut TupleQueueReader;
                ptr::copy_nonoverlapping(
                    pei_reader((*node).pei),
                    (*node).reader,
                    (*node).nreaders as usize,
                );
            } else {
                /* No workers?	Then never mind. */
                (*node).nreaders = 0;
                (*node).reader = ptr::null_mut();
            }
        }

        /* allow leader to participate if enabled or no choice */
        if parallel_leader_participation || (*node).nreaders == 0 {
            (*node).need_to_scan_locally = true;
        }
        (*node).initialized = true;
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    econtext = (*node).ps.ps_ExprContext;
    ResetExprContext(econtext);

    /*
     * Get next tuple, either from one of our workers, or by running the plan
     * ourselves.
     */
    slot = gather_merge_getnext(node);
    if TupIsNull(slot) {
        return ptr::null_mut();
    }

    /* If no projection is required, we're done. */
    if (*node).ps.ps_ProjInfo.is_null() {
        return slot;
    }

    /*
     * Form the result tuple using ExecProject(), and return it.
     */
    (*econtext).ecxt_outertuple = slot;
    ExecProject((*node).ps.ps_ProjInfo)
}

/* ----------------------------------------------------------------
 *		ExecEndGatherMerge
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndGatherMerge(node: *mut GatherMergeState) {
    ExecEndNode(outerPlanState(&mut (*node).ps)); /* let children clean up first */
    ExecShutdownGatherMerge(node);
}

/* ----------------------------------------------------------------
 *		ExecShutdownGatherMerge
 *
 *		Destroy the setup for parallel workers including parallel context.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecShutdownGatherMerge(node: *mut GatherMergeState) {
    ExecShutdownGatherMergeWorkers(node);

    /* Now destroy the parallel context. */
    if !(*node).pei.is_null() {
        ExecParallelCleanup((*node).pei);
        (*node).pei = ptr::null_mut();
    }
}

/* ----------------------------------------------------------------
 *		ExecShutdownGatherMergeWorkers
 *
 *		Stop all the parallel workers.
 * ----------------------------------------------------------------
 */
unsafe fn ExecShutdownGatherMergeWorkers(node: *mut GatherMergeState) {
    if !(*node).pei.is_null() {
        ExecParallelFinish((*node).pei);
    }

    /* Flush local copy of reader array */
    if !(*node).reader.is_null() {
        pfree((*node).reader as *mut c_void);
    }
    (*node).reader = ptr::null_mut();
}

/* ----------------------------------------------------------------
 *		ExecReScanGatherMerge
 *
 *		Prepare to re-scan the result of a GatherMerge.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanGatherMerge(node: *mut GatherMergeState) {
    let gm: *mut GatherMerge = (*node).ps.plan as *mut GatherMerge;
    let outerPlan: *mut PlanState = outerPlanState(&mut (*node).ps);

    /* Make sure any existing workers are gracefully shut down */
    ExecShutdownGatherMergeWorkers(node);

    /* Free any unused tuples, so we don't leak memory across rescans */
    gather_merge_clear_tuples(node);

    /* Mark node so that shared state will be rebuilt at next call */
    (*node).initialized = false;
    (*node).gm_initialized = false;

    /*
     * Set child node's chgParam to tell it that the next scan might deliver a
     * different set of rows within the leader process.  (The overall rowset
     * shouldn't change, but the leader process's subset might; hence nodes
     * between here and the parallel table scan node mustn't optimize on the
     * assumption of an unchanging rowset.)
     */
    if (*gm).rescan_param >= 0 {
        (*outerPlan).chgParam = bms_add_member((*outerPlan).chgParam, (*gm).rescan_param);
    }

    /*
     * If chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.  Note: because this does nothing if we have a
     * rescan_param, it's currently guaranteed that parallel-aware child nodes
     * will not see a ReScan call until after they get a ReInitializeDSM call.
     * That ordering might not be something to rely on, though.  A good rule
     * of thumb is that ReInitializeDSM should reset only shared state, ReScan
     * should reset only local state, and anything that depends on both of
     * those steps being finished must wait until the first ExecProcNode call.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
}

/*
 * Set up the data structures that we'll need for Gather Merge.
 *
 * We allocate these once on the basis of gm->num_workers, which is an
 * upper bound for the number of workers we'll actually have.  During
 * a rescan, we reset the structures to empty.  This approach simplifies
 * not leaking memory across rescans.
 *
 * In the gm_slots[] array, index 0 is for the leader, and indexes 1 to n
 * are for workers.  The values placed into gm_heap correspond to indexes
 * in gm_slots[].  The gm_tuple_buffers[] array, however, is indexed from
 * 0 to n-1; it has no entry for the leader.
 */
unsafe fn gather_merge_setup(gm_state: *mut GatherMergeState) {
    let gm: *mut GatherMerge = castNode!(GatherMerge, T_GatherMerge, (*gm_state).ps.plan);
    let nreaders: c_int = (*gm).num_workers;
    let mut i: c_int;

    /*
     * Allocate gm_slots for the number of workers + one more slot for leader.
     * Slot 0 is always for the leader.  Leader always calls ExecProcNode() to
     * read the tuple, and then stores it directly into its gm_slots entry.
     * For other slots, code below will call ExecInitExtraTupleSlot() to
     * create a slot for the worker's results.  Note that during any single
     * scan, we might have fewer than num_workers available workers, in which
     * case the extra array entries go unused.
     */
    (*gm_state).gm_slots = palloc0(
        (nreaders + 1) as usize * size_of::<*mut TupleTableSlot>(),
    ) as *mut *mut TupleTableSlot;

    /* Allocate the tuple slot and tuple array for each worker */
    let gm_tuple_buffers = palloc0(
        nreaders as usize * size_of::<GMReaderTupleBuffer>(),
    ) as *mut GMReaderTupleBuffer;
    (*gm_state).gm_tuple_buffers =
        gm_tuple_buffers as *mut crate::nodes::execnodes::GMReaderTupleBuffer;

    i = 0;
    while i < nreaders {
        /* Allocate the tuple array with length MAX_TUPLE_STORE */
        (*gm_tuple_buffers.offset(i as isize)).tuple =
            palloc0(size_of::<MinimalTuple>() * MAX_TUPLE_STORE as usize) as *mut MinimalTuple;

        /* Initialize tuple slot for worker */
        *(*gm_state).gm_slots.offset((i + 1) as isize) = ExecInitExtraTupleSlot(
            (*gm_state).ps.state,
            (*gm_state).tupDesc,
            &TTSOpsMinimalTuple,
        );

        i += 1;
    }

    /* Allocate the resources for the merge */
    (*gm_state).gm_heap = binaryheap_allocate(
        nreaders + 1,
        heap_compare_slots,
        gm_state as *mut c_void,
    );
}

/*
 * Initialize the Gather Merge.
 *
 * Reset data structures to ensure they're empty.  Then pull at least one
 * tuple from leader + each worker (or set its "done" indicator), and set up
 * the heap.
 */
unsafe fn gather_merge_init(gm_state: *mut GatherMergeState) {
    let nreaders: c_int = (*gm_state).nreaders;
    let mut nowait: bool = true;
    let mut i: c_int;

    let gm_tuple_buffers =
        (*gm_state).gm_tuple_buffers as *mut GMReaderTupleBuffer;

    /* Assert that gather_merge_setup made enough space */
    Assert!(
        nreaders
            <= (*castNode!(GatherMerge, T_GatherMerge, (*gm_state).ps.plan)).num_workers
    );

    /* Reset leader's tuple slot to empty */
    *(*gm_state).gm_slots.offset(0) = ptr::null_mut();

    /* Reset the tuple slot and tuple array for each worker */
    i = 0;
    while i < nreaders {
        /* Reset tuple array to empty */
        (*gm_tuple_buffers.offset(i as isize)).nTuples = 0;
        (*gm_tuple_buffers.offset(i as isize)).readCounter = 0;
        /* Reset done flag to not-done */
        (*gm_tuple_buffers.offset(i as isize)).done = false;
        /* Ensure output slot is empty */
        ExecClearTuple(*(*gm_state).gm_slots.offset((i + 1) as isize));
        i += 1;
    }

    /* Reset binary heap to empty */
    binaryheap_reset((*gm_state).gm_heap);

    /*
     * First, try to read a tuple from each worker (including leader) in
     * nowait mode.  After this, if not all workers were able to produce a
     * tuple (or a "done" indication), then re-read from remaining workers,
     * this time using wait mode.  Add all live readers (those producing at
     * least one tuple) to the heap.
     */
    'reread: loop {
        i = 0;
        while i <= nreaders {
            CHECK_FOR_INTERRUPTS();

            /* skip this source if already known done */
            let skip = if i == 0 {
                (*gm_state).need_to_scan_locally
            } else {
                !(*gm_tuple_buffers.offset((i - 1) as isize)).done
            };
            if skip {
                if TupIsNull(*(*gm_state).gm_slots.offset(i as isize)) {
                    /* Don't have a tuple yet, try to get one */
                    if gather_merge_readnext(gm_state, i, nowait) {
                        binaryheap_add_unordered((*gm_state).gm_heap, Int32GetDatum(i));
                    }
                } else {
                    /*
                     * We already got at least one tuple from this worker, but
                     * might as well see if it has any more ready by now.
                     */
                    load_tuple_array(gm_state, i);
                }
            }
            i += 1;
        }

        /* need not recheck leader, since nowait doesn't matter for it */
        let mut do_reread = false;
        i = 1;
        while i <= nreaders {
            if !(*gm_tuple_buffers.offset((i - 1) as isize)).done
                && TupIsNull(*(*gm_state).gm_slots.offset(i as isize))
            {
                nowait = false;
                do_reread = true;
                break;
            }
            i += 1;
        }
        if do_reread {
            continue 'reread;
        }
        break;
    }

    /* Now heapify the heap. */
    binaryheap_build((*gm_state).gm_heap);

    (*gm_state).gm_initialized = true;
}

/*
 * Clear out the tuple table slot, and any unused pending tuples,
 * for each gather merge input.
 */
unsafe fn gather_merge_clear_tuples(gm_state: *mut GatherMergeState) {
    let mut i: c_int;

    let gm_tuple_buffers =
        (*gm_state).gm_tuple_buffers as *mut GMReaderTupleBuffer;

    i = 0;
    while i < (*gm_state).nreaders {
        let tuple_buffer: *mut GMReaderTupleBuffer = gm_tuple_buffers.offset(i as isize);

        while (*tuple_buffer).readCounter < (*tuple_buffer).nTuples {
            pfree(*(*tuple_buffer).tuple.offset((*tuple_buffer).readCounter as isize) as *mut c_void);
            (*tuple_buffer).readCounter += 1;
        }

        ExecClearTuple(*(*gm_state).gm_slots.offset((i + 1) as isize));
        i += 1;
    }
}

/*
 * Read the next tuple for gather merge.
 *
 * Fetch the sorted tuple out of the heap.
 */
unsafe fn gather_merge_getnext(gm_state: *mut GatherMergeState) -> *mut TupleTableSlot {
    let i: c_int;

    if !(*gm_state).gm_initialized {
        /*
         * First time through: pull the first tuple from each participant, and
         * set up the heap.
         */
        gather_merge_init(gm_state);
    } else {
        /*
         * Otherwise, pull the next tuple from whichever participant we
         * returned from last time, and reinsert that participant's index into
         * the heap, because it might now compare differently against the
         * other elements of the heap.
         */
        let j: c_int = DatumGetInt32(binaryheap_first((*gm_state).gm_heap));

        if gather_merge_readnext(gm_state, j, false) {
            binaryheap_replace_first((*gm_state).gm_heap, Int32GetDatum(j));
        } else {
            /* reader exhausted, remove it from heap */
            binaryheap_remove_first((*gm_state).gm_heap);
        }
    }

    if binaryheap_empty((*gm_state).gm_heap) {
        /* All the queues are exhausted, and so is the heap */
        gather_merge_clear_tuples(gm_state);
        ptr::null_mut()
    } else {
        /* Return next tuple from whichever participant has the leading one */
        i = DatumGetInt32(binaryheap_first((*gm_state).gm_heap));
        *(*gm_state).gm_slots.offset(i as isize)
    }
}

/*
 * Read tuple(s) for given reader in nowait mode, and load into its tuple
 * array, until we have MAX_TUPLE_STORE of them or would have to block.
 */
unsafe fn load_tuple_array(gm_state: *mut GatherMergeState, reader: c_int) {
    let tuple_buffer: *mut GMReaderTupleBuffer;
    let mut i: c_int;

    /* Don't do anything if this is the leader. */
    if reader == 0 {
        return;
    }

    let gm_tuple_buffers =
        (*gm_state).gm_tuple_buffers as *mut GMReaderTupleBuffer;
    tuple_buffer = gm_tuple_buffers.offset((reader - 1) as isize);

    /* If there's nothing in the array, reset the counters to zero. */
    if (*tuple_buffer).nTuples == (*tuple_buffer).readCounter {
        (*tuple_buffer).readCounter = 0;
        (*tuple_buffer).nTuples = 0;
    }

    /* Try to fill additional slots in the array. */
    i = (*tuple_buffer).nTuples;
    while i < MAX_TUPLE_STORE {
        let tuple: MinimalTuple;

        tuple = gm_readnext_tuple(gm_state, reader, true, &mut (*tuple_buffer).done);
        if tuple.is_null() {
            break;
        }
        *(*tuple_buffer).tuple.offset(i as isize) = tuple;
        (*tuple_buffer).nTuples += 1;
        i += 1;
    }
}

/*
 * Store the next tuple for a given reader into the appropriate slot.
 *
 * Returns true if successful, false if not (either reader is exhausted,
 * or we didn't want to wait for a tuple).  Sets done flag if reader
 * is found to be exhausted.
 */
unsafe fn gather_merge_readnext(
    gm_state: *mut GatherMergeState,
    reader: c_int,
    nowait: bool,
) -> bool {
    let tuple_buffer: *mut GMReaderTupleBuffer;
    let tup: MinimalTuple;

    /*
     * If we're being asked to generate a tuple from the leader, then we just
     * call ExecProcNode as normal to produce one.
     */
    if reader == 0 {
        if (*gm_state).need_to_scan_locally {
            let outerPlan: *mut PlanState = outerPlanState(&mut (*gm_state).ps);
            let outerTupleSlot: *mut TupleTableSlot;
            let estate: *mut EState = (*gm_state).ps.state;

            /* Install our DSA area while executing the plan. */
            (*estate).es_query_dsa = if !(*gm_state).pei.is_null() {
                pei_area((*gm_state).pei)
            } else {
                ptr::null_mut()
            };
            outerTupleSlot = ExecProcNode(outerPlan);
            (*estate).es_query_dsa = ptr::null_mut();

            if !TupIsNull(outerTupleSlot) {
                *(*gm_state).gm_slots.offset(0) = outerTupleSlot;
                return true;
            }
            /* need_to_scan_locally serves as "done" flag for leader */
            (*gm_state).need_to_scan_locally = false;
        }
        return false;
    }

    let gm_tuple_buffers =
        (*gm_state).gm_tuple_buffers as *mut GMReaderTupleBuffer;

    /* Otherwise, check the state of the relevant tuple buffer. */
    tuple_buffer = gm_tuple_buffers.offset((reader - 1) as isize);

    if (*tuple_buffer).nTuples > (*tuple_buffer).readCounter {
        /* Return any tuple previously read that is still buffered. */
        tup = *(*tuple_buffer).tuple.offset((*tuple_buffer).readCounter as isize);
        (*tuple_buffer).readCounter += 1;
    } else if (*tuple_buffer).done {
        /* Reader is known to be exhausted. */
        return false;
    } else {
        /* Read and buffer next tuple. */
        tup = gm_readnext_tuple(gm_state, reader, nowait, &mut (*tuple_buffer).done);
        if tup.is_null() {
            return false;
        }

        /*
         * Attempt to read more tuples in nowait mode and store them in the
         * pending-tuple array for the reader.
         */
        load_tuple_array(gm_state, reader);
    }

    Assert!(!tup.is_null());

    /* Build the TupleTableSlot for the given tuple */
    ExecStoreMinimalTuple(
        tup,                                          /* tuple to store */
        *(*gm_state).gm_slots.offset(reader as isize), /* slot in which to store the tuple */
        true,                                         /* pfree tuple when done with it */
    );

    true
}

/*
 * Attempt to read a tuple from given worker.
 */
unsafe fn gm_readnext_tuple(
    gm_state: *mut GatherMergeState,
    nreader: c_int,
    nowait: bool,
    done: *mut bool,
) -> MinimalTuple {
    let reader: *mut TupleQueueReader;
    let tup: MinimalTuple;

    /* Check for async events, particularly messages from workers. */
    CHECK_FOR_INTERRUPTS();

    /*
     * Attempt to read a tuple.
     *
     * Note that TupleQueueReaderNext will just return NULL for a worker which
     * fails to initialize.  We'll treat that worker as having produced no
     * tuples; WaitForParallelWorkersToFinish will error out when we get
     * there.
     */
    reader = *(*gm_state).reader.offset((nreader - 1) as isize);
    tup = TupleQueueReaderNext(reader, nowait, done);

    /*
     * Since we'll be buffering these across multiple calls, we need to make a
     * copy.
     */
    if !tup.is_null() {
        heap_copy_minimal_tuple(tup, 0)
    } else {
        ptr::null_mut()
    }
}

/*
 * We have one slot for each item in the heap array.  We use SlotNumber
 * to store slot indexes.  This doesn't actually provide any formal
 * type-safety, but it makes the code more self-documenting.
 */
type SlotNumber = int32;

/*
 * Compare the tuples in the two given slots.
 */
unsafe fn heap_compare_slots(a: Datum, b: Datum, arg: *mut c_void) -> c_int {
    let node: *mut GatherMergeState = arg as *mut GatherMergeState;
    let slot1: SlotNumber = DatumGetInt32(a);
    let slot2: SlotNumber = DatumGetInt32(b);

    let s1: *mut TupleTableSlot = *(*node).gm_slots.offset(slot1 as isize);
    let s2: *mut TupleTableSlot = *(*node).gm_slots.offset(slot2 as isize);
    let mut nkey: c_int;

    Assert!(!TupIsNull(s1));
    Assert!(!TupIsNull(s2));

    nkey = 0;
    while nkey < (*node).gm_nkeys {
        let sortKey: SortSupport = (*node).gm_sortkeys.offset(nkey as isize);
        let attno: AttrNumber = (*sortKey).ssup_attno;
        let datum1: Datum;
        let datum2: Datum;
        let mut isNull1: bool = false;
        let mut isNull2: bool = false;
        let mut compare: c_int;

        datum1 = slot_getattr(s1, attno as c_int, &mut isNull1);
        datum2 = slot_getattr(s2, attno as c_int, &mut isNull2);

        compare = ApplySortComparator(datum1, isNull1, datum2, isNull2, sortKey);
        if compare != 0 {
            compare = INVERT_COMPARE_RESULT(compare);
            return compare;
        }
        nkey += 1;
    }
    0
}
