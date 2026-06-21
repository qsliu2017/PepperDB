/*-------------------------------------------------------------------------
 *
 * nodeGather.c
 *	  Support routines for scanning a plan via multiple workers.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * A Gather executor launches parallel workers to run multiple copies of a
 * plan.  It can also run the plan itself, if the workers are not available
 * or have not started up yet.  It then merges all of the results it produces
 * and the results from the workers into a single output stream.  Therefore,
 * it will normally be used with a plan where running multiple copies of the
 * same plan does not produce duplicate output, such as parallel-aware
 * SeqScan.
 *
 * Alternatively, a Gather node can be configured to use just one worker
 * and the single-copy flag can be set.  In this case, the Gather node will
 * run the plan in one worker and will not execute the plan itself.  In
 * this case, it simply returns whatever tuples were returned by the worker.
 * If a worker cannot be obtained, then it will run the plan itself and
 * return the results.  Therefore, a plan used with a single-copy Gather
 * node need not be parallel-aware.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeGather.c
 *	  (companion header: src/include/executor/nodeGather.h)
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use std::ffi::c_int;
use std::ptr;

use crate::access::htup_details::{HeapTupleIsValid, MinimalTuple};
use crate::executor::execTuples::{ExecStoreMinimalTuple, TTSOpsMinimalTuple};
use crate::executor::executor::{
    ExecAssignExprContext, ExecConditionalAssignProjectionInfo, ExecEndNode, ExecGetResultType,
    ExecInitExtraTupleSlot, ExecInitNode, ExecInitResultTypeTL, ExecProcNode, ExecProject,
    ExecReScan, ResetExprContext,
};
use crate::executor::tqueue::TupleQueueReaderNext;
use crate::executor::tuptable::{ExecClearTuple, TupIsNull};
use crate::miscadmin::{CHECK_FOR_INTERRUPTS, Latch, MyLatch};
use crate::nodes::bitmapset::bms_add_member;
use crate::nodes::execnodes::{
    dsa_area, outerPlanState, EState, ExprContext, GatherState, ParallelExecutorInfo, PlanState,
    TupleQueueReader,
};
use crate::nodes::extensible::ParallelContext;
use crate::nodes::plannodes::{Gather, Plan};
use crate::nodes::primnodes::OUTER_VAR;
use crate::optimizer::optimizer::parallel_leader_participation;

use crate::access::common::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;

use crate::{castNode, makeNode, Assert};

/* ----------------------------------------------------------------
 *		local helper stubs for not-yet-ported dependencies
 * ---------------------------------------------------------------- */

/* latch.h flags (TODO: utils/latch.h) */
const WL_LATCH_SET: c_int = 1 << 0;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;
/* wait_event.h (TODO: utils/wait_event.h) */
const WAIT_EVENT_EXECUTE_GATHER: u32 = 0;

unsafe fn WaitLatch(_latch: *mut Latch, _wakeEvents: c_int, _timeout: i64, _wait_event_info: u32) -> c_int {
    crate::storage::ipc::latch::WaitLatch(_latch as _, _wakeEvents as _, _timeout as _, _wait_event_info as _) as _
}
unsafe fn ResetLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::ResetLatch(_latch as _)
}

/* executor/execParallel.h (TODO: backend/executor/execParallel.c) */
unsafe fn ExecInitParallelPlan(
    _planstate: *mut PlanState,
    _estate: *mut EState,
    _sendParams: *mut crate::nodes::bitmapset::Bitmapset,
    _nworkers: c_int,
    _tuples_needed: i64,
) -> *mut ParallelExecutorInfo {
    unimplemented!() // TODO: executor/execParallel.c
}
unsafe fn ExecParallelReinitialize(
    _planstate: *mut PlanState,
    _pei: *mut ParallelExecutorInfo,
    _sendParams: *mut crate::nodes::bitmapset::Bitmapset,
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
    crate::access::transam::parallel::LaunchParallelWorkers(_pcxt as _)
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

/* ----------------------------------------------------------------
 *		ExecInitGather
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitGather(node: *mut Gather, estate: *mut EState, eflags: c_int) -> *mut GatherState {
    let gatherstate: *mut GatherState;
    let outerNode: *mut Plan;
    let tupDesc: TupleDesc;

    /* Gather node doesn't have innerPlan node. */
    Assert!(innerPlan(node).is_null());

    /*
     * create state structure
     */
    gatherstate = makeNode!(GatherState, T_GatherState);
    (*gatherstate).ps.plan = node as *mut Plan;
    (*gatherstate).ps.state = estate;
    (*gatherstate).ps.ExecProcNode = Some(ExecGather);

    (*gatherstate).initialized = false;
    (*gatherstate).need_to_scan_locally =
        !(*node).single_copy && parallel_leader_participation;
    (*gatherstate).tuples_needed = -1;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &mut (*gatherstate).ps);

    /*
     * now initialize outer plan
     */
    outerNode = outerPlan(node);
    *(&mut (*gatherstate).ps.lefttree) = ExecInitNode(outerNode, estate, eflags);
    tupDesc = ExecGetResultType(outerPlanState(&mut (*gatherstate).ps));

    /*
     * Leader may access ExecProcNode result directly (if
     * need_to_scan_locally), or from workers via tuple queue.  So we can't
     * trivially rely on the slot type being fixed for expressions evaluated
     * within this node.
     */
    (*gatherstate).ps.outeropsset = true;
    (*gatherstate).ps.outeropsfixed = false;

    /*
     * Initialize result type and projection.
     */
    ExecInitResultTypeTL(&mut (*gatherstate).ps);
    ExecConditionalAssignProjectionInfo(&mut (*gatherstate).ps, tupDesc, OUTER_VAR);

    /*
     * Without projections result slot type is not trivially known, see
     * comment above.
     */
    if (*gatherstate).ps.ps_ProjInfo.is_null() {
        (*gatherstate).ps.resultopsset = true;
        (*gatherstate).ps.resultopsfixed = false;
    }

    /*
     * Initialize funnel slot to same tuple descriptor as outer plan.
     */
    (*gatherstate).funnel_slot =
        ExecInitExtraTupleSlot(estate, tupDesc, &TTSOpsMinimalTuple);

    /*
     * Gather doesn't support checking a qual (it's always more efficient to
     * do it in the child node).
     */
    Assert!((*node).plan.qual.is_null());

    gatherstate
}

/* ----------------------------------------------------------------
 *		ExecGather(node)
 *
 *		Scans the relation via multiple workers and returns
 *		the next qualifying tuple.
 * ----------------------------------------------------------------
 */
unsafe fn ExecGather(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut GatherState = castNode!(GatherState, T_GatherState, pstate);
    let slot: *mut TupleTableSlot;
    let econtext: *mut ExprContext;

    CHECK_FOR_INTERRUPTS();

    /*
     * Initialize the parallel context and workers on first execution. We do
     * this on first execution rather than during node initialization, as it
     * needs to allocate a large dynamic segment, so it is better to do it
     * only if it is really needed.
     */
    if !(*node).initialized {
        let estate: *mut EState = (*node).ps.state;
        let gather: *mut Gather = (*node).ps.plan as *mut Gather;

        /*
         * Sometimes we might have to run without parallelism; but if parallel
         * mode is active then we can try to fire up some workers.
         */
        if (*gather).num_workers > 0 && (*estate).es_use_parallel_mode {
            let pcxt: *mut ParallelContext;

            /* Initialize, or re-initialize, shared state needed by workers. */
            if (*node).pei.is_null() {
                (*node).pei = ExecInitParallelPlan(
                    outerPlanState(&mut (*node).ps),
                    estate,
                    (*gather).initParam,
                    (*gather).num_workers,
                    (*node).tuples_needed,
                );
            } else {
                ExecParallelReinitialize(
                    outerPlanState(&mut (*node).ps),
                    (*node).pei,
                    (*gather).initParam,
                );
            }

            /*
             * Register backend workers. We might not get as many as we
             * requested, or indeed any at all.
             */
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
            (*node).nextreader = 0;
        }

        /* Run plan locally if no workers or enabled and not single-copy. */
        (*node).need_to_scan_locally = ((*node).nreaders == 0)
            || (!(*gather).single_copy && parallel_leader_participation);
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
    slot = gather_getnext(node);
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
 *		ExecEndGather
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndGather(node: *mut GatherState) {
    ExecEndNode(outerPlanState(&mut (*node).ps)); /* let children clean up first */
    ExecShutdownGather(node);
}

/*
 * Read the next tuple.  We might fetch a tuple from one of the tuple queues
 * using gather_readnext, or if no tuple queue contains a tuple and the
 * single_copy flag is not set, we might generate one locally instead.
 */
unsafe fn gather_getnext(gatherstate: *mut GatherState) -> *mut TupleTableSlot {
    let outerPlan: *mut PlanState = outerPlanState(&mut (*gatherstate).ps);
    let mut outerTupleSlot: *mut TupleTableSlot;
    let fslot: *mut TupleTableSlot = (*gatherstate).funnel_slot;
    let mut tup: MinimalTuple;

    while (*gatherstate).nreaders > 0 || (*gatherstate).need_to_scan_locally {
        CHECK_FOR_INTERRUPTS();

        if (*gatherstate).nreaders > 0 {
            tup = gather_readnext(gatherstate);

            if HeapTupleIsValid(tup as crate::access::htup_details::HeapTuple) {
                ExecStoreMinimalTuple(
                    tup,   /* tuple to store */
                    fslot, /* slot to store the tuple */
                    false, /* don't pfree tuple  */
                );
                return fslot;
            }
        }

        if (*gatherstate).need_to_scan_locally {
            let estate: *mut EState = (*gatherstate).ps.state;

            /* Install our DSA area while executing the plan. */
            (*estate).es_query_dsa = if !(*gatherstate).pei.is_null() {
                pei_area((*gatherstate).pei)
            } else {
                ptr::null_mut()
            };
            outerTupleSlot = ExecProcNode(outerPlan);
            (*estate).es_query_dsa = ptr::null_mut();

            if !TupIsNull(outerTupleSlot) {
                return outerTupleSlot;
            }

            (*gatherstate).need_to_scan_locally = false;
        }
    }

    ExecClearTuple(fslot)
}

/*
 * Attempt to read a tuple from one of our parallel workers.
 */
unsafe fn gather_readnext(gatherstate: *mut GatherState) -> MinimalTuple {
    let mut nvisited: c_int = 0;

    loop {
        let reader: *mut TupleQueueReader;
        let tup: MinimalTuple;
        let mut readerdone: bool = false;

        /* Check for async events, particularly messages from workers. */
        CHECK_FOR_INTERRUPTS();

        /*
         * Attempt to read a tuple, but don't block if none is available.
         *
         * Note that TupleQueueReaderNext will just return NULL for a worker
         * which fails to initialize.  We'll treat that worker as having
         * produced no tuples; WaitForParallelWorkersToFinish will error out
         * when we get there.
         */
        Assert!((*gatherstate).nextreader < (*gatherstate).nreaders);
        reader = *(*gatherstate).reader.offset((*gatherstate).nextreader as isize);
        tup = TupleQueueReaderNext(reader, true, &mut readerdone);

        /*
         * If this reader is done, remove it from our working array of active
         * readers.  If all readers are done, we're outta here.
         */
        if readerdone {
            Assert!(tup.is_null());
            (*gatherstate).nreaders -= 1;
            if (*gatherstate).nreaders == 0 {
                ExecShutdownGatherWorkers(gatherstate);
                return ptr::null_mut();
            }
            ptr::copy(
                (*gatherstate).reader.offset(((*gatherstate).nextreader + 1) as isize),
                (*gatherstate).reader.offset((*gatherstate).nextreader as isize),
                ((*gatherstate).nreaders - (*gatherstate).nextreader) as usize,
            );
            if (*gatherstate).nextreader >= (*gatherstate).nreaders {
                (*gatherstate).nextreader = 0;
            }
            continue;
        }

        /* If we got a tuple, return it. */
        if !tup.is_null() {
            return tup;
        }

        /*
         * Advance nextreader pointer in round-robin fashion.  Note that we
         * only reach this code if we weren't able to get a tuple from the
         * current worker.  We used to advance the nextreader pointer after
         * every tuple, but it turns out to be much more efficient to keep
         * reading from the same queue until that would require blocking.
         */
        (*gatherstate).nextreader += 1;
        if (*gatherstate).nextreader >= (*gatherstate).nreaders {
            (*gatherstate).nextreader = 0;
        }

        /* Have we visited every (surviving) TupleQueueReader? */
        nvisited += 1;
        if nvisited >= (*gatherstate).nreaders {
            /*
             * If (still) running plan locally, return NULL so caller can
             * generate another tuple from the local copy of the plan.
             */
            if (*gatherstate).need_to_scan_locally {
                return ptr::null_mut();
            }

            /* Nothing to do except wait for developments. */
            WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
                0,
                WAIT_EVENT_EXECUTE_GATHER,
            );
            ResetLatch(MyLatch);
            nvisited = 0;
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecShutdownGatherWorkers
 *
 *		Stop all the parallel workers.
 * ----------------------------------------------------------------
 */
unsafe fn ExecShutdownGatherWorkers(node: *mut GatherState) {
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
 *		ExecShutdownGather
 *
 *		Destroy the setup for parallel workers including parallel context.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecShutdownGather(node: *mut GatherState) {
    ExecShutdownGatherWorkers(node);

    /* Now destroy the parallel context. */
    if !(*node).pei.is_null() {
        ExecParallelCleanup((*node).pei);
        (*node).pei = ptr::null_mut();
    }
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanGather
 *
 *		Prepare to re-scan the result of a Gather.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanGather(node: *mut GatherState) {
    let gather: *mut Gather = (*node).ps.plan as *mut Gather;
    let outerPlan: *mut PlanState = outerPlanState(&mut (*node).ps);

    /* Make sure any existing workers are gracefully shut down */
    ExecShutdownGatherWorkers(node);

    /* Mark node so that shared state will be rebuilt at next call */
    (*node).initialized = false;

    /*
     * Set child node's chgParam to tell it that the next scan might deliver a
     * different set of rows within the leader process.  (The overall rowset
     * shouldn't change, but the leader process's subset might; hence nodes
     * between here and the parallel table scan node mustn't optimize on the
     * assumption of an unchanging rowset.)
     */
    if (*gather).rescan_param >= 0 {
        (*outerPlan).chgParam =
            bms_add_member((*outerPlan).chgParam, (*gather).rescan_param);
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

/* ----------------------------------------------------------------
 *		local plan-tree accessor stubs
 * ---------------------------------------------------------------- */

/* innerPlan(node) / outerPlan(node) on a Plan node (nodes/plannodes.h) */
unsafe fn innerPlan(node: *mut Gather) -> *mut Plan {
    (*node).plan.righttree
}
unsafe fn outerPlan(node: *mut Gather) -> *mut Plan {
    (*node).plan.lefttree
}
