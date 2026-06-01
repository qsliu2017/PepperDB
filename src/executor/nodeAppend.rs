//! src/backend/executor/nodeAppend.c
//!
//! routines to handle append nodes.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/executor/nodeAppend.c

/* INTERFACE ROUTINES
 *		ExecInitAppend	- initialize the append node
 *		ExecAppend		- retrieve the next tuple from the node
 *		ExecEndAppend	- shut down the append node
 *		ExecReScanAppend - rescan the append node
 *
 *	 NOTES
 *		Each append node contains a list of one or more subplans which
 *		must be iteratively processed (forwards or backwards).
 *		Tuples are retrieved by executing the 'whichplan'th subplan
 *		until the subplan stops returning tuples, at which point that
 *		plan is shut down and the next started up.
 *
 *		Append nodes don't make use of their left and right
 *		subtrees, rather they maintain a list of subplans so
 *		a typical append node looks like this in the plan tree:
 *
 *				   ...
 *				   /
 *				Append -------+------+------+--- nil
 *				/	\		  |		 |		|
 *			  nil	nil		 ...    ...    ...
 *								 subplans
 *
 *		Append nodes are currently used for unions, and to support
 *		inheritance queries, where several relations need to be scanned.
 *		For example, in our standard person/student/employee/student-emp
 *		example, where student and employee inherit from person
 *		and student-emp inherits from student and employee, the
 *		query:
 *
 *				select name from person
 *
 *		generates the plan:
 *
 *				  |
 *				Append -------+-------+--------+--------+
 *				/	\		  |		  |		   |		|
 *			  nil	nil		 Scan	 Scan	  Scan	   Scan
 *							  |		  |		   |		|
 *							person employee student student-emp
 */

use crate::prelude::*;

use std::ffi::{c_int, c_void};

// Node/plan/exec types referenced by this unit.
use crate::nodes::nodes::NodeTag;
use crate::nodes::pg_list::List;
use crate::nodes::plannodes::{Append, Plan};
use crate::nodes::execnodes::{AppendState, AsyncRequest, EState, PlanState};
use crate::executor::tuptable::{TupleTableSlot, TupleTableSlotOps};

// Interrupt check (a function in miscadmin).
use crate::miscadmin::CHECK_FOR_INTERRUPTS;

// #[macro_export] node macros live at the crate root.
use crate::{castNode, makeNode, Assert, IsA};

/* Shared state for parallel-aware Append. */
#[repr(C)]
pub struct ParallelAppendState {
    pub pa_lock: LWLock,        /* mutual exclusion to choose next subplan */
    pub pa_next_plan: c_int,    /* next plan to choose by any worker */

    /*
     * pa_finished[i] should be true if no more workers should select subplan
     * i.  for a non-partial plan, this should be set to true as soon as a
     * worker selects the plan; for a partial plan, it remains false until
     * some worker executes the plan to completion.
     */
    pub pa_finished: [bool; FLEXIBLE_ARRAY_MEMBER],
}

const INVALID_SUBPLAN_INDEX: c_int = -1;
const EVENT_BUFFER_SIZE: c_int = 16;

/* ----------------------------------------------------------------
 *		ExecInitAppend
 *
 *		Begin all of the subscans of the append node.
 *
 *	   (This is potentially wasteful, since the entire result of the
 *		append node may not be scanned, but this way all of the
 *		structures get allocated in the executor's top level memory
 *		block instead of that of the call to ExecAppend.)
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitAppend(
    node: *mut Append,
    estate: *mut EState,
    eflags: c_int,
) -> *mut AppendState {
    let appendstate: *mut AppendState = makeNode!(AppendState, T_AppendState);
    let appendplanstates: *mut *mut PlanState;
    let appendops: *const TupleTableSlotOps;
    let mut validsubplans: *mut Bitmapset;
    let mut asyncplans: *mut Bitmapset;
    let nplans: c_int;
    let mut nasyncplans: c_int;
    let mut firstvalid: c_int;
    let mut i: c_int;
    let mut j: c_int;

    /* check for unsupported flags */
    Assert!(eflags & EXEC_FLAG_MARK == 0);

    /*
     * create new AppendState for our append node
     */
    (*appendstate).ps.plan = node as *mut Plan;
    (*appendstate).ps.state = estate;
    (*appendstate).ps.ExecProcNode = Some(ExecAppend);

    /* Let choose_next_subplan_* function handle setting the first subplan */
    (*appendstate).as_whichplan = INVALID_SUBPLAN_INDEX;
    (*appendstate).as_syncdone = false;
    (*appendstate).as_begun = false;

    /* If run-time partition pruning is enabled, then set that up now */
    if (*node).part_prune_index >= 0 {
        let prunestate: *mut PartitionPruneState;

        /*
         * Set up pruning data structure.  This also initializes the set of
         * subplans to initialize (validsubplans) by taking into account the
         * result of performing initial pruning if any.
         */
        prunestate = ExecInitPartitionExecPruning(
            &mut (*appendstate).ps,
            list_length((*node).appendplans),
            (*node).part_prune_index,
            (*node).apprelids,
            &mut validsubplans,
        );
        (*appendstate).as_prune_state = prunestate;
        nplans = bms_num_members(validsubplans);

        /*
         * When no run-time pruning is required and there's at least one
         * subplan, we can fill as_valid_subplans immediately, preventing
         * later calls to ExecFindMatchingSubPlans.
         */
        if !(*prunestate).do_exec_prune && nplans > 0 {
            (*appendstate).as_valid_subplans =
                bms_add_range(std::ptr::null_mut(), 0, nplans - 1);
            (*appendstate).as_valid_subplans_identified = true;
        }
    } else {
        nplans = list_length((*node).appendplans);

        /*
         * When run-time partition pruning is not enabled we can just mark all
         * subplans as valid; they must also all be initialized.
         */
        Assert!(nplans > 0);
        validsubplans = bms_add_range(std::ptr::null_mut(), 0, nplans - 1);
        (*appendstate).as_valid_subplans = validsubplans;
        (*appendstate).as_valid_subplans_identified = true;
        (*appendstate).as_prune_state = std::ptr::null_mut();
    }

    appendplanstates =
        palloc(nplans as usize * std::mem::size_of::<*mut PlanState>()) as *mut *mut PlanState;

    /*
     * call ExecInitNode on each of the valid plans to be executed and save
     * the results into the appendplanstates array.
     *
     * While at it, find out the first valid partial plan.
     */
    j = 0;
    asyncplans = std::ptr::null_mut();
    nasyncplans = 0;
    firstvalid = nplans;
    i = -1;
    loop {
        i = bms_next_member(validsubplans, i);
        if i < 0 {
            break;
        }
        let initNode: *mut Plan = list_nth((*node).appendplans, i) as *mut Plan;

        /*
         * Record async subplans.  When executing EvalPlanQual, we treat them
         * as sync ones; don't do this when initializing an EvalPlanQual plan
         * tree.
         */
        if (*initNode).async_capable && (*estate).es_epq_active.is_null() {
            asyncplans = bms_add_member(asyncplans, j);
            nasyncplans += 1;
        }

        /*
         * Record the lowest appendplans index which is a valid partial plan.
         */
        if i >= (*node).first_partial_plan && j < firstvalid {
            firstvalid = j;
        }

        *appendplanstates.offset(j as isize) = ExecInitNode(initNode, estate, eflags);
        j += 1;
    }

    (*appendstate).as_first_partial_plan = firstvalid;
    (*appendstate).appendplans = appendplanstates;
    (*appendstate).as_nplans = nplans;

    /*
     * Initialize Append's result tuple type and slot.  If the child plans all
     * produce the same fixed slot type, we can use that slot type; otherwise
     * make a virtual slot.  (Note that the result slot itself is used only to
     * return a null tuple at end of execution; real tuples are returned to
     * the caller in the children's own result slots.  What we are doing here
     * is allowing the parent plan node to optimize if the Append will return
     * only one kind of slot.)
     */
    appendops = ExecGetCommonSlotOps(appendplanstates, j);
    if !appendops.is_null() {
        ExecInitResultTupleSlotTL(&mut (*appendstate).ps, appendops);
    } else {
        ExecInitResultTupleSlotTL(&mut (*appendstate).ps, &TTSOpsVirtual);
        /* show that the output slot type is not fixed */
        (*appendstate).ps.resultopsset = true;
        (*appendstate).ps.resultopsfixed = false;
    }

    /* Initialize async state */
    (*appendstate).as_asyncplans = asyncplans;
    (*appendstate).as_nasyncplans = nasyncplans;
    (*appendstate).as_asyncrequests = std::ptr::null_mut();
    (*appendstate).as_asyncresults = std::ptr::null_mut();
    (*appendstate).as_nasyncresults = 0;
    (*appendstate).as_nasyncremain = 0;
    (*appendstate).as_needrequest = std::ptr::null_mut();
    (*appendstate).as_eventset = std::ptr::null_mut();
    (*appendstate).as_valid_asyncplans = std::ptr::null_mut();

    if nasyncplans > 0 {
        (*appendstate).as_asyncrequests = palloc0(
            nplans as usize * std::mem::size_of::<*mut AsyncRequest>(),
        ) as *mut *mut AsyncRequest;

        i = -1;
        loop {
            i = bms_next_member(asyncplans, i);
            if i < 0 {
                break;
            }
            let areq: *mut AsyncRequest;

            areq = palloc(std::mem::size_of::<AsyncRequest>()) as *mut AsyncRequest;
            (*areq).requestor = appendstate as *mut PlanState;
            (*areq).requestee = *appendplanstates.offset(i as isize);
            (*areq).request_index = i;
            (*areq).callback_pending = false;
            (*areq).request_complete = false;
            (*areq).result = std::ptr::null_mut();

            *(*appendstate).as_asyncrequests.offset(i as isize) = areq;
        }

        (*appendstate).as_asyncresults = palloc0(
            nasyncplans as usize * std::mem::size_of::<*mut TupleTableSlot>(),
        ) as *mut *mut TupleTableSlot;

        if (*appendstate).as_valid_subplans_identified {
            classify_matching_subplans(appendstate);
        }
    }

    /*
     * Miscellaneous initialization
     */

    (*appendstate).ps.ps_ProjInfo = std::ptr::null_mut();

    /* For parallel query, this will be overridden later. */
    (*appendstate).choose_next_subplan = Some(choose_next_subplan_locally);

    appendstate
}

/* ----------------------------------------------------------------
 *	   ExecAppend
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
unsafe extern "C" fn ExecAppend(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut AppendState = castNode!(AppendState, T_AppendState, pstate);
    let mut result: *mut TupleTableSlot;

    /*
     * If this is the first call after Init or ReScan, we need to do the
     * initialization work.
     */
    if !(*node).as_begun {
        Assert!((*node).as_whichplan == INVALID_SUBPLAN_INDEX);
        Assert!(!(*node).as_syncdone);

        /* Nothing to do if there are no subplans */
        if (*node).as_nplans == 0 {
            return ExecClearTuple((*node).ps.ps_ResultTupleSlot);
        }

        /* If there are any async subplans, begin executing them. */
        if (*node).as_nasyncplans > 0 {
            ExecAppendAsyncBegin(node);
        }

        /*
         * If no sync subplan has been chosen, we must choose one before
         * proceeding.
         */
        if !((*node).choose_next_subplan.unwrap())(node) && (*node).as_nasyncremain == 0 {
            return ExecClearTuple((*node).ps.ps_ResultTupleSlot);
        }

        Assert!(
            (*node).as_syncdone
                || ((*node).as_whichplan >= 0 && (*node).as_whichplan < (*node).as_nplans),
        );

        /* And we're initialized. */
        (*node).as_begun = true;
    }

    loop {
        let subnode: *mut PlanState;

        CHECK_FOR_INTERRUPTS();

        /*
         * try to get a tuple from an async subplan if any
         */
        if (*node).as_syncdone || !bms_is_empty((*node).as_needrequest) {
            if ExecAppendAsyncGetNext(node, &mut result) {
                return result;
            }
            Assert!(!(*node).as_syncdone);
            Assert!(bms_is_empty((*node).as_needrequest));
        }

        /*
         * figure out which sync subplan we are currently processing
         */
        Assert!((*node).as_whichplan >= 0 && (*node).as_whichplan < (*node).as_nplans);
        subnode = *(*node).appendplans.offset((*node).as_whichplan as isize);

        /*
         * get a tuple from the subplan
         */
        result = ExecProcNode(subnode);

        if !TupIsNull(result) {
            /*
             * If the subplan gave us something then return it as-is. We do
             * NOT make use of the result slot that was set up in
             * ExecInitAppend; there's no need for it.
             */
            return result;
        }

        /*
         * wait or poll for async events if any. We do this before checking
         * for the end of iteration, because it might drain the remaining
         * async subplans.
         */
        if (*node).as_nasyncremain > 0 {
            ExecAppendAsyncEventWait(node);
        }

        /* choose new sync subplan; if no sync/async subplans, we're done */
        if !((*node).choose_next_subplan.unwrap())(node) && (*node).as_nasyncremain == 0 {
            return ExecClearTuple((*node).ps.ps_ResultTupleSlot);
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecEndAppend
 *
 *		Shuts down the subscans of the append node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndAppend(node: *mut AppendState) {
    let appendplans: *mut *mut PlanState;
    let nplans: c_int;
    let mut i: c_int;

    /*
     * get information from the node
     */
    appendplans = (*node).appendplans;
    nplans = (*node).as_nplans;

    /*
     * shut down each of the subscans
     */
    i = 0;
    while i < nplans {
        ExecEndNode(*appendplans.offset(i as isize));
        i += 1;
    }
}

pub unsafe fn ExecReScanAppend(node: *mut AppendState) {
    let nasyncplans: c_int = (*node).as_nasyncplans;
    let mut i: c_int;

    /*
     * If any PARAM_EXEC Params used in pruning expressions have changed, then
     * we'd better unset the valid subplans so that they are reselected for
     * the new parameter values.
     */
    if !(*node).as_prune_state.is_null()
        && bms_overlap(
            (*node).ps.chgParam,
            (*(*node).as_prune_state).execparamids,
        )
    {
        (*node).as_valid_subplans_identified = false;
        bms_free((*node).as_valid_subplans);
        (*node).as_valid_subplans = std::ptr::null_mut();
        bms_free((*node).as_valid_asyncplans);
        (*node).as_valid_asyncplans = std::ptr::null_mut();
    }

    i = 0;
    while i < (*node).as_nplans {
        let subnode: *mut PlanState = *(*node).appendplans.offset(i as isize);

        /*
         * ExecReScan doesn't know about my subplans, so I have to do
         * changed-parameter signaling myself.
         */
        if !(*node).ps.chgParam.is_null() {
            UpdateChangedParamSet(subnode, (*node).ps.chgParam);
        }

        /*
         * If chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode or by first ExecAsyncRequest.
         */
        if (*subnode).chgParam.is_null() {
            ExecReScan(subnode);
        }
        i += 1;
    }

    /* Reset async state */
    if nasyncplans > 0 {
        i = -1;
        loop {
            i = bms_next_member((*node).as_asyncplans, i);
            if i < 0 {
                break;
            }
            let areq: *mut AsyncRequest = *(*node).as_asyncrequests.offset(i as isize);

            (*areq).callback_pending = false;
            (*areq).request_complete = false;
            (*areq).result = std::ptr::null_mut();
        }

        (*node).as_nasyncresults = 0;
        (*node).as_nasyncremain = 0;
        bms_free((*node).as_needrequest);
        (*node).as_needrequest = std::ptr::null_mut();
    }

    /* Let choose_next_subplan_* function handle setting the first subplan */
    (*node).as_whichplan = INVALID_SUBPLAN_INDEX;
    (*node).as_syncdone = false;
    (*node).as_begun = false;
}

/* ----------------------------------------------------------------
 *						Parallel Append Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecAppendEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecAppendEstimate(node: *mut AppendState, pcxt: *mut ParallelContext) {
    (*node).pstate_len = add_size(
        core::mem::offset_of!(ParallelAppendState, pa_finished),
        std::mem::size_of::<bool>() * (*node).as_nplans as usize,
    );

    shm_toc_estimate_chunk(&mut (*pcxt).estimator, (*node).pstate_len);
    shm_toc_estimate_keys(&mut (*pcxt).estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecAppendInitializeDSM
 *
 *		Set up shared state for Parallel Append.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecAppendInitializeDSM(node: *mut AppendState, pcxt: *mut ParallelContext) {
    let pstate: *mut ParallelAppendState;

    pstate = shm_toc_allocate((*pcxt).toc, (*node).pstate_len) as *mut ParallelAppendState;
    memset(pstate as *mut c_void, 0, (*node).pstate_len);
    LWLockInitialize(&mut (*pstate).pa_lock, LWTRANCHE_PARALLEL_APPEND);
    shm_toc_insert(
        (*pcxt).toc,
        (*(*node).ps.plan).plan_node_id,
        pstate as *mut c_void,
    );

    (*node).as_pstate = pstate;
    (*node).choose_next_subplan = Some(choose_next_subplan_for_leader);
}

/* ----------------------------------------------------------------
 *		ExecAppendReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecAppendReInitializeDSM(node: *mut AppendState, _pcxt: *mut ParallelContext) {
    let pstate: *mut ParallelAppendState = (*node).as_pstate;

    (*pstate).pa_next_plan = 0;
    memset(
        (*pstate).pa_finished.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of::<bool>() * (*node).as_nplans as usize,
    );
}

/* ----------------------------------------------------------------
 *		ExecAppendInitializeWorker
 *
 *		Copy relevant information from TOC into planstate, and initialize
 *		whatever is required to choose and execute the optimal subplan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecAppendInitializeWorker(
    node: *mut AppendState,
    pwcxt: *mut ParallelWorkerContext,
) {
    (*node).as_pstate = shm_toc_lookup(
        (*pwcxt).toc,
        (*(*node).ps.plan).plan_node_id,
        false,
    ) as *mut ParallelAppendState;
    (*node).choose_next_subplan = Some(choose_next_subplan_for_worker);
}

/* ----------------------------------------------------------------
 *		choose_next_subplan_locally
 *
 *		Choose next sync subplan for a non-parallel-aware Append,
 *		returning false if there are no more.
 * ----------------------------------------------------------------
 */
unsafe extern "C" fn choose_next_subplan_locally(node: *mut AppendState) -> bool {
    let mut whichplan: c_int = (*node).as_whichplan;
    let nextplan: c_int;

    /* We should never be called when there are no subplans */
    Assert!((*node).as_nplans > 0);

    /* Nothing to do if syncdone */
    if (*node).as_syncdone {
        return false;
    }

    /*
     * If first call then have the bms member function choose the first valid
     * sync subplan by initializing whichplan to -1.  If there happen to be no
     * valid sync subplans then the bms member function will handle that by
     * returning a negative number which will allow us to exit returning a
     * false value.
     */
    if whichplan == INVALID_SUBPLAN_INDEX {
        if (*node).as_nasyncplans > 0 {
            /* We'd have filled as_valid_subplans already */
            Assert!((*node).as_valid_subplans_identified);
        } else if !(*node).as_valid_subplans_identified {
            (*node).as_valid_subplans =
                ExecFindMatchingSubPlans((*node).as_prune_state, false, std::ptr::null_mut());
            (*node).as_valid_subplans_identified = true;
        }

        whichplan = -1;
    }

    /* Ensure whichplan is within the expected range */
    Assert!(whichplan >= -1 && whichplan <= (*node).as_nplans);

    if ScanDirectionIsForward((*(*node).ps.state).es_direction) {
        nextplan = bms_next_member((*node).as_valid_subplans, whichplan);
    } else {
        nextplan = bms_prev_member((*node).as_valid_subplans, whichplan);
    }

    if nextplan < 0 {
        /* Set as_syncdone if in async mode */
        if (*node).as_nasyncplans > 0 {
            (*node).as_syncdone = true;
        }
        return false;
    }

    (*node).as_whichplan = nextplan;

    true
}

/* ----------------------------------------------------------------
 *		choose_next_subplan_for_leader
 *
 *      Try to pick a plan which doesn't commit us to doing much
 *      work locally, so that as much work as possible is done in
 *      the workers.  Cheapest subplans are at the end.
 * ----------------------------------------------------------------
 */
unsafe extern "C" fn choose_next_subplan_for_leader(node: *mut AppendState) -> bool {
    let pstate: *mut ParallelAppendState = (*node).as_pstate;

    /* Backward scan is not supported by parallel-aware plans */
    Assert!(ScanDirectionIsForward((*(*node).ps.state).es_direction));

    /* We should never be called when there are no subplans */
    Assert!((*node).as_nplans > 0);

    LWLockAcquire(&mut (*pstate).pa_lock, LW_EXCLUSIVE);

    if (*node).as_whichplan != INVALID_SUBPLAN_INDEX {
        /* Mark just-completed subplan as finished. */
        *(*(*node).as_pstate)
            .pa_finished
            .as_mut_ptr()
            .offset((*node).as_whichplan as isize) = true;
    } else {
        /* Start with last subplan. */
        (*node).as_whichplan = (*node).as_nplans - 1;

        /*
         * If we've yet to determine the valid subplans then do so now.  If
         * run-time pruning is disabled then the valid subplans will always be
         * set to all subplans.
         */
        if !(*node).as_valid_subplans_identified {
            (*node).as_valid_subplans =
                ExecFindMatchingSubPlans((*node).as_prune_state, false, std::ptr::null_mut());
            (*node).as_valid_subplans_identified = true;

            /*
             * Mark each invalid plan as finished to allow the loop below to
             * select the first valid subplan.
             */
            mark_invalid_subplans_as_finished(node);
        }
    }

    /* Loop until we find a subplan to execute. */
    while *(*pstate).pa_finished.as_ptr().offset((*node).as_whichplan as isize) {
        if (*node).as_whichplan == 0 {
            (*pstate).pa_next_plan = INVALID_SUBPLAN_INDEX;
            (*node).as_whichplan = INVALID_SUBPLAN_INDEX;
            LWLockRelease(&mut (*pstate).pa_lock);
            return false;
        }

        /*
         * We needn't pay attention to as_valid_subplans here as all invalid
         * plans have been marked as finished.
         */
        (*node).as_whichplan -= 1;
    }

    /* If non-partial, immediately mark as finished. */
    if (*node).as_whichplan < (*node).as_first_partial_plan {
        *(*(*node).as_pstate)
            .pa_finished
            .as_mut_ptr()
            .offset((*node).as_whichplan as isize) = true;
    }

    LWLockRelease(&mut (*pstate).pa_lock);

    true
}

/* ----------------------------------------------------------------
 *		choose_next_subplan_for_worker
 *
 *		Choose next subplan for a parallel-aware Append, returning
 *		false if there are no more.
 *
 *		We start from the first plan and advance through the list;
 *		when we get back to the end, we loop back to the first
 *		partial plan.  This assigns the non-partial plans first in
 *		order of descending cost and then spreads out the workers
 *		as evenly as possible across the remaining partial plans.
 * ----------------------------------------------------------------
 */
unsafe extern "C" fn choose_next_subplan_for_worker(node: *mut AppendState) -> bool {
    let pstate: *mut ParallelAppendState = (*node).as_pstate;

    /* Backward scan is not supported by parallel-aware plans */
    Assert!(ScanDirectionIsForward((*(*node).ps.state).es_direction));

    /* We should never be called when there are no subplans */
    Assert!((*node).as_nplans > 0);

    LWLockAcquire(&mut (*pstate).pa_lock, LW_EXCLUSIVE);

    /* Mark just-completed subplan as finished. */
    if (*node).as_whichplan != INVALID_SUBPLAN_INDEX {
        *(*(*node).as_pstate)
            .pa_finished
            .as_mut_ptr()
            .offset((*node).as_whichplan as isize) = true;
    }
    /*
     * If we've yet to determine the valid subplans then do so now.  If
     * run-time pruning is disabled then the valid subplans will always be set
     * to all subplans.
     */
    else if !(*node).as_valid_subplans_identified {
        (*node).as_valid_subplans =
            ExecFindMatchingSubPlans((*node).as_prune_state, false, std::ptr::null_mut());
        (*node).as_valid_subplans_identified = true;

        mark_invalid_subplans_as_finished(node);
    }

    /* If all the plans are already done, we have nothing to do */
    if (*pstate).pa_next_plan == INVALID_SUBPLAN_INDEX {
        LWLockRelease(&mut (*pstate).pa_lock);
        return false;
    }

    /* Save the plan from which we are starting the search. */
    (*node).as_whichplan = (*pstate).pa_next_plan;

    /* Loop until we find a valid subplan to execute. */
    while *(*pstate).pa_finished.as_ptr().offset((*pstate).pa_next_plan as isize) {
        let mut nextplan: c_int;

        nextplan = bms_next_member((*node).as_valid_subplans, (*pstate).pa_next_plan);
        if nextplan >= 0 {
            /* Advance to the next valid plan. */
            (*pstate).pa_next_plan = nextplan;
        } else if (*node).as_whichplan > (*node).as_first_partial_plan {
            /*
             * Try looping back to the first valid partial plan, if there is
             * one.  If there isn't, arrange to bail out below.
             */
            nextplan = bms_next_member(
                (*node).as_valid_subplans,
                (*node).as_first_partial_plan - 1,
            );
            (*pstate).pa_next_plan = if nextplan < 0 {
                (*node).as_whichplan
            } else {
                nextplan
            };
        } else {
            /*
             * At last plan, and either there are no partial plans or we've
             * tried them all.  Arrange to bail out.
             */
            (*pstate).pa_next_plan = (*node).as_whichplan;
        }

        if (*pstate).pa_next_plan == (*node).as_whichplan {
            /* We've tried everything! */
            (*pstate).pa_next_plan = INVALID_SUBPLAN_INDEX;
            LWLockRelease(&mut (*pstate).pa_lock);
            return false;
        }
    }

    /* Pick the plan we found, and advance pa_next_plan one more time. */
    (*node).as_whichplan = (*pstate).pa_next_plan;
    (*pstate).pa_next_plan = bms_next_member((*node).as_valid_subplans, (*pstate).pa_next_plan);

    /*
     * If there are no more valid plans then try setting the next plan to the
     * first valid partial plan.
     */
    if (*pstate).pa_next_plan < 0 {
        let nextplan: c_int = bms_next_member(
            (*node).as_valid_subplans,
            (*node).as_first_partial_plan - 1,
        );

        if nextplan >= 0 {
            (*pstate).pa_next_plan = nextplan;
        } else {
            /*
             * There are no valid partial plans, and we already chose the last
             * non-partial plan; so flag that there's nothing more for our
             * fellow workers to do.
             */
            (*pstate).pa_next_plan = INVALID_SUBPLAN_INDEX;
        }
    }

    /* If non-partial, immediately mark as finished. */
    if (*node).as_whichplan < (*node).as_first_partial_plan {
        *(*(*node).as_pstate)
            .pa_finished
            .as_mut_ptr()
            .offset((*node).as_whichplan as isize) = true;
    }

    LWLockRelease(&mut (*pstate).pa_lock);

    true
}

/*
 * mark_invalid_subplans_as_finished
 *		Marks the ParallelAppendState's pa_finished as true for each invalid
 *		subplan.
 *
 * This function should only be called for parallel Append with run-time
 * pruning enabled.
 */
unsafe fn mark_invalid_subplans_as_finished(node: *mut AppendState) {
    let mut i: c_int;

    /* Only valid to call this while in parallel Append mode */
    Assert!(!(*node).as_pstate.is_null());

    /* Shouldn't have been called when run-time pruning is not enabled */
    Assert!(!(*node).as_prune_state.is_null());

    /* Nothing to do if all plans are valid */
    if bms_num_members((*node).as_valid_subplans) == (*node).as_nplans {
        return;
    }

    /* Mark all non-valid plans as finished */
    i = 0;
    while i < (*node).as_nplans {
        if !bms_is_member(i, (*node).as_valid_subplans) {
            *(*(*node).as_pstate).pa_finished.as_mut_ptr().offset(i as isize) = true;
        }
        i += 1;
    }
}

/* ----------------------------------------------------------------
 *						Asynchronous Append Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecAppendAsyncBegin
 *
 *		Begin executing designed async-capable subplans.
 * ----------------------------------------------------------------
 */
unsafe fn ExecAppendAsyncBegin(node: *mut AppendState) {
    let mut i: c_int;

    /* Backward scan is not supported by async-aware Appends. */
    Assert!(ScanDirectionIsForward((*(*node).ps.state).es_direction));

    /* We should never be called when there are no subplans */
    Assert!((*node).as_nplans > 0);

    /* We should never be called when there are no async subplans. */
    Assert!((*node).as_nasyncplans > 0);

    /* If we've yet to determine the valid subplans then do so now. */
    if !(*node).as_valid_subplans_identified {
        (*node).as_valid_subplans =
            ExecFindMatchingSubPlans((*node).as_prune_state, false, std::ptr::null_mut());
        (*node).as_valid_subplans_identified = true;

        classify_matching_subplans(node);
    }

    /* Initialize state variables. */
    (*node).as_syncdone = bms_is_empty((*node).as_valid_subplans);
    (*node).as_nasyncremain = bms_num_members((*node).as_valid_asyncplans);

    /* Nothing to do if there are no valid async subplans. */
    if (*node).as_nasyncremain == 0 {
        return;
    }

    /* Make a request for each of the valid async subplans. */
    i = -1;
    loop {
        i = bms_next_member((*node).as_valid_asyncplans, i);
        if i < 0 {
            break;
        }
        let areq: *mut AsyncRequest = *(*node).as_asyncrequests.offset(i as isize);

        Assert!((*areq).request_index == i);
        Assert!(!(*areq).callback_pending);

        /* Do the actual work. */
        ExecAsyncRequest(areq);
    }
}

/* ----------------------------------------------------------------
 *		ExecAppendAsyncGetNext
 *
 *		Get the next tuple from any of the asynchronous subplans.
 * ----------------------------------------------------------------
 */
unsafe fn ExecAppendAsyncGetNext(
    node: *mut AppendState,
    result: *mut *mut TupleTableSlot,
) -> bool {
    *result = std::ptr::null_mut();

    /* We should never be called when there are no valid async subplans. */
    Assert!((*node).as_nasyncremain > 0);

    /* Request a tuple asynchronously. */
    if ExecAppendAsyncRequest(node, result) {
        return true;
    }

    while (*node).as_nasyncremain > 0 {
        CHECK_FOR_INTERRUPTS();

        /* Wait or poll for async events. */
        ExecAppendAsyncEventWait(node);

        /* Request a tuple asynchronously. */
        if ExecAppendAsyncRequest(node, result) {
            return true;
        }

        /* Break from loop if there's any sync subplan that isn't complete. */
        if !(*node).as_syncdone {
            break;
        }
    }

    /*
     * If all sync subplans are complete, we're totally done scanning the
     * given node.  Otherwise, we're done with the asynchronous stuff but must
     * continue scanning the sync subplans.
     */
    if (*node).as_syncdone {
        Assert!((*node).as_nasyncremain == 0);
        *result = ExecClearTuple((*node).ps.ps_ResultTupleSlot);
        return true;
    }

    false
}

/* ----------------------------------------------------------------
 *		ExecAppendAsyncRequest
 *
 *		Request a tuple asynchronously.
 * ----------------------------------------------------------------
 */
unsafe fn ExecAppendAsyncRequest(
    node: *mut AppendState,
    result: *mut *mut TupleTableSlot,
) -> bool {
    let needrequest: *mut Bitmapset;
    let mut i: c_int;

    /* Nothing to do if there are no async subplans needing a new request. */
    if bms_is_empty((*node).as_needrequest) {
        Assert!((*node).as_nasyncresults == 0);
        return false;
    }

    /*
     * If there are any asynchronously-generated results that have not yet
     * been returned, we have nothing to do; just return one of them.
     */
    if (*node).as_nasyncresults > 0 {
        (*node).as_nasyncresults -= 1;
        *result = *(*node).as_asyncresults.offset((*node).as_nasyncresults as isize);
        return true;
    }

    /* Make a new request for each of the async subplans that need it. */
    needrequest = (*node).as_needrequest;
    (*node).as_needrequest = std::ptr::null_mut();
    i = -1;
    loop {
        i = bms_next_member(needrequest, i);
        if i < 0 {
            break;
        }
        let areq: *mut AsyncRequest = *(*node).as_asyncrequests.offset(i as isize);

        /* Do the actual work. */
        ExecAsyncRequest(areq);
    }
    bms_free(needrequest);

    /* Return one of the asynchronously-generated results if any. */
    if (*node).as_nasyncresults > 0 {
        (*node).as_nasyncresults -= 1;
        *result = *(*node).as_asyncresults.offset((*node).as_nasyncresults as isize);
        return true;
    }

    false
}

/* ----------------------------------------------------------------
 *		ExecAppendAsyncEventWait
 *
 *		Wait or poll for file descriptor events and fire callbacks.
 * ----------------------------------------------------------------
 */
unsafe fn ExecAppendAsyncEventWait(node: *mut AppendState) {
    let mut nevents: c_int = (*node).as_nasyncplans + 2;
    let timeout: std::ffi::c_long = if (*node).as_syncdone { -1 } else { 0 };
    let mut occurred_event: [WaitEvent; EVENT_BUFFER_SIZE as usize] =
        std::mem::zeroed();
    let noccurred: c_int;
    let mut i: c_int;

    /* We should never be called when there are no valid async subplans. */
    Assert!((*node).as_nasyncremain > 0);

    Assert!((*node).as_eventset.is_null());
    (*node).as_eventset = CreateWaitEventSet(CurrentResourceOwner, nevents);
    AddWaitEventToSet(
        (*node).as_eventset,
        WL_EXIT_ON_PM_DEATH,
        PGINVALID_SOCKET,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );

    /* Give each waiting subplan a chance to add an event. */
    i = -1;
    loop {
        i = bms_next_member((*node).as_asyncplans, i);
        if i < 0 {
            break;
        }
        let areq: *mut AsyncRequest = *(*node).as_asyncrequests.offset(i as isize);

        if (*areq).callback_pending {
            ExecAsyncConfigureWait(areq);
        }
    }

    /*
     * No need for further processing if none of the subplans configured any
     * events.
     */
    if GetNumRegisteredWaitEvents((*node).as_eventset) == 1 {
        FreeWaitEventSet((*node).as_eventset);
        (*node).as_eventset = std::ptr::null_mut();
        return;
    }

    /*
     * Add the process latch to the set, so that we wake up to process the
     * standard interrupts with CHECK_FOR_INTERRUPTS().
     *
     * NOTE: For historical reasons, it's important that this is added to the
     * WaitEventSet after the ExecAsyncConfigureWait() calls.  Namely,
     * postgres_fdw calls "GetNumRegisteredWaitEvents(set) == 1" to check if
     * any other events are in the set.  That's a poor design, it's
     * questionable for postgres_fdw to be doing that in the first place, but
     * we cannot change it now.  The pattern has possibly been copied to other
     * extensions too.
     */
    AddWaitEventToSet(
        (*node).as_eventset,
        WL_LATCH_SET,
        PGINVALID_SOCKET,
        MyLatch,
        std::ptr::null_mut(),
    );

    /* Return at most EVENT_BUFFER_SIZE events in one call. */
    if nevents > EVENT_BUFFER_SIZE {
        nevents = EVENT_BUFFER_SIZE;
    }

    /*
     * If the timeout is -1, wait until at least one event occurs.  If the
     * timeout is 0, poll for events, but do not wait at all.
     */
    noccurred = WaitEventSetWait(
        (*node).as_eventset,
        timeout,
        occurred_event.as_mut_ptr(),
        nevents,
        WAIT_EVENT_APPEND_READY,
    );
    FreeWaitEventSet((*node).as_eventset);
    (*node).as_eventset = std::ptr::null_mut();
    if noccurred == 0 {
        return;
    }

    /* Deliver notifications. */
    i = 0;
    while i < noccurred {
        let w: *mut WaitEvent = &mut occurred_event[i as usize];

        /*
         * Each waiting subplan should have registered its wait event with
         * user_data pointing back to its AsyncRequest.
         */
        if (*w).events & WL_SOCKET_READABLE != 0 {
            let areq: *mut AsyncRequest = (*w).user_data as *mut AsyncRequest;

            if (*areq).callback_pending {
                /*
                 * Mark it as no longer needing a callback.  We must do this
                 * before dispatching the callback in case the callback resets
                 * the flag.
                 */
                (*areq).callback_pending = false;

                /* Do the actual work. */
                ExecAsyncNotify(areq);
            }
        }

        /* Handle standard interrupts */
        if (*w).events & WL_LATCH_SET != 0 {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }
        i += 1;
    }
}

/* ----------------------------------------------------------------
 *		ExecAsyncAppendResponse
 *
 *		Receive a response from an asynchronous request we made.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecAsyncAppendResponse(areq: *mut AsyncRequest) {
    let node: *mut AppendState = (*areq).requestor as *mut AppendState;
    let slot: *mut TupleTableSlot = (*areq).result;

    /* The result should be a TupleTableSlot or NULL. */
    Assert!(slot.is_null() || IsA!(slot, T_TupleTableSlot));

    /* Nothing to do if the request is pending. */
    if !(*areq).request_complete {
        /* The request would have been pending for a callback. */
        Assert!((*areq).callback_pending);
        return;
    }

    /* If the result is NULL or an empty slot, there's nothing more to do. */
    if TupIsNull(slot) {
        /* The ending subplan wouldn't have been pending for a callback. */
        Assert!(!(*areq).callback_pending);
        (*node).as_nasyncremain -= 1;
        return;
    }

    /* Save result so we can return it. */
    Assert!((*node).as_nasyncresults < (*node).as_nasyncplans);
    *(*node).as_asyncresults.offset((*node).as_nasyncresults as isize) = slot;
    (*node).as_nasyncresults += 1;

    /*
     * Mark the subplan that returned a result as ready for a new request.  We
     * don't launch another one here immediately because it might complete.
     */
    (*node).as_needrequest = bms_add_member((*node).as_needrequest, (*areq).request_index);
}

/* ----------------------------------------------------------------
 *		classify_matching_subplans
 *
 *		Classify the node's as_valid_subplans into sync ones and
 *		async ones, adjust it to contain sync ones only, and save
 *		async ones in the node's as_valid_asyncplans.
 * ----------------------------------------------------------------
 */
unsafe fn classify_matching_subplans(node: *mut AppendState) {
    let valid_asyncplans: *mut Bitmapset;

    Assert!((*node).as_valid_subplans_identified);
    Assert!((*node).as_valid_asyncplans.is_null());

    /* Nothing to do if there are no valid subplans. */
    if bms_is_empty((*node).as_valid_subplans) {
        (*node).as_syncdone = true;
        (*node).as_nasyncremain = 0;
        return;
    }

    /* Nothing to do if there are no valid async subplans. */
    if !bms_overlap((*node).as_valid_subplans, (*node).as_asyncplans) {
        (*node).as_nasyncremain = 0;
        return;
    }

    /* Get valid async subplans. */
    valid_asyncplans = bms_intersect((*node).as_asyncplans, (*node).as_valid_subplans);

    /* Adjust the valid subplans to contain sync subplans only. */
    (*node).as_valid_subplans = bms_del_members((*node).as_valid_subplans, valid_asyncplans);

    /* Save valid async subplans. */
    (*node).as_valid_asyncplans = valid_asyncplans;
}

// ---- Local stubs for unported helpers ----

unsafe fn ExecInitPartitionExecPruning(
    _planstate: *mut PlanState,
    _n_total_subplans: c_int,
    _part_prune_index: c_int,
    _relids: *mut Bitmapset,
    _initially_valid_subplans: *mut *mut Bitmapset,
) -> *mut PartitionPruneState {
    unimplemented!() // TODO: execPartition.c
}

unsafe fn ExecFindMatchingSubPlans(
    _prunestate: *mut PartitionPruneState,
    _initial_prune: bool,
    _validsubplan_rtis: *mut *mut Bitmapset,
) -> *mut Bitmapset {
    unimplemented!() // TODO: execPartition.c
}

unsafe fn ExecGetCommonSlotOps(
    _planstates: *mut *mut PlanState,
    _nplans: c_int,
) -> *const TupleTableSlotOps {
    unimplemented!() // TODO: execUtils.c
}

unsafe fn ExecInitResultTupleSlotTL(_planstate: *mut PlanState, _tts_ops: *const TupleTableSlotOps) {
    unimplemented!() // TODO: execTuples.c
}

unsafe fn ExecInitNode(_node: *mut Plan, _estate: *mut EState, _eflags: c_int) -> *mut PlanState {
    unimplemented!() // TODO: execProcnode.c
}

unsafe fn ExecEndNode(_node: *mut PlanState) {
    unimplemented!() // TODO: execProcnode.c
}

unsafe fn ExecReScan(_node: *mut PlanState) {
    unimplemented!() // TODO: execAmi.c
}

unsafe fn UpdateChangedParamSet(_node: *mut PlanState, _newchg: *mut Bitmapset) {
    unimplemented!() // TODO: execUtils.c
}

unsafe fn ExecAsyncRequest(_areq: *mut AsyncRequest) {
    unimplemented!() // TODO: execAsync.c
}

unsafe fn ExecAsyncConfigureWait(_areq: *mut AsyncRequest) {
    unimplemented!() // TODO: execAsync.c
}

unsafe fn ExecAsyncNotify(_areq: *mut AsyncRequest) {
    unimplemented!() // TODO: execAsync.c
}

unsafe fn ExecClearTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot {
    unimplemented!() // TODO: execTuples.c
}

unsafe fn ExecProcNode(_node: *mut PlanState) -> *mut TupleTableSlot {
    unimplemented!() // TODO: executor.h
}

unsafe fn TupIsNull(_slot: *mut TupleTableSlot) -> bool {
    unimplemented!() // TODO: tuptable.h
}

unsafe fn bms_num_members(_a: *const Bitmapset) -> c_int {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_add_range(_a: *mut Bitmapset, _lower: c_int, _upper: c_int) -> *mut Bitmapset {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_add_member(_a: *mut Bitmapset, _x: c_int) -> *mut Bitmapset {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_next_member(_a: *const Bitmapset, _prevbit: c_int) -> c_int {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_prev_member(_a: *const Bitmapset, _prevbit: c_int) -> c_int {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_is_empty(_a: *const Bitmapset) -> bool {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_is_member(_x: c_int, _a: *const Bitmapset) -> bool {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_overlap(_a: *const Bitmapset, _b: *const Bitmapset) -> bool {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_intersect(_a: *const Bitmapset, _b: *const Bitmapset) -> *mut Bitmapset {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_del_members(_a: *mut Bitmapset, _b: *const Bitmapset) -> *mut Bitmapset {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn bms_free(_a: *mut Bitmapset) {
    unimplemented!() // TODO: bitmapset.c
}

unsafe fn ScanDirectionIsForward(_direction: ScanDirection) -> bool {
    unimplemented!() // TODO: sdir.h
}

unsafe fn add_size(_s1: Size, _s2: Size) -> Size {
    unimplemented!() // TODO: shmem.c
}

unsafe fn shm_toc_estimate_chunk(_e: *mut shm_toc_estimator, _sz: Size) {
    unimplemented!() // TODO: shm_toc.c
}

unsafe fn shm_toc_estimate_keys(_e: *mut shm_toc_estimator, _cnt: Size) {
    unimplemented!() // TODO: shm_toc.c
}

unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _nbytes: Size) -> *mut c_void {
    unimplemented!() // TODO: shm_toc.c
}

unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: u64, _address: *mut c_void) {
    unimplemented!() // TODO: shm_toc.c
}

unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: u64, _noError: bool) -> *mut c_void {
    unimplemented!() // TODO: shm_toc.c
}

unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) {
    unimplemented!() // TODO: lwlock.c
}

unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: LWLockMode) -> bool {
    unimplemented!() // TODO: lwlock.c
}

unsafe fn LWLockRelease(_lock: *mut LWLock) {
    unimplemented!() // TODO: lwlock.c
}

unsafe fn CreateWaitEventSet(_resowner: *mut ResourceOwnerData, _nevents: c_int) -> *mut WaitEventSet {
    unimplemented!() // TODO: waiteventset.c
}

unsafe fn AddWaitEventToSet(
    _set: *mut WaitEventSet,
    _events: u32,
    _fd: pgsocket,
    _latch: *mut Latch,
    _user_data: *mut c_void,
) -> c_int {
    unimplemented!() // TODO: waiteventset.c
}

unsafe fn GetNumRegisteredWaitEvents(_set: *mut WaitEventSet) -> c_int {
    unimplemented!() // TODO: waiteventset.c
}

unsafe fn FreeWaitEventSet(_set: *mut WaitEventSet) {
    unimplemented!() // TODO: waiteventset.c
}

unsafe fn WaitEventSetWait(
    _set: *mut WaitEventSet,
    _timeout: std::ffi::c_long,
    _occurred_events: *mut WaitEvent,
    _nevents: c_int,
    _wait_event_info: u32,
) -> c_int {
    unimplemented!() // TODO: waiteventset.c
}

unsafe fn ResetLatch(_latch: *mut Latch) {
    unimplemented!() // TODO: latch.c
}

unsafe fn list_length(_l: *const List) -> c_int {
    unimplemented!() // TODO: pg_list.h
}

unsafe fn list_nth(_list: *const List, _n: c_int) -> *mut c_void {
    unimplemented!() // TODO: list.c
}

unsafe fn memset(_s: *mut c_void, _c: c_int, _n: Size) -> *mut c_void {
    unimplemented!() // TODO: string.h
}

// ---- Local stub types for unported deps ----

pub type Bitmapset = c_void; // TODO: nodes/bitmapset.h
pub type PartitionPruneState = c_void; // TODO: executor/execPartition.h
pub type LWLock = c_void; // TODO: storage/lwlock.h
pub type LWLockMode = c_int; // TODO: storage/lwlock.h
pub type ParallelContext = c_void; // TODO: access/parallel.h
pub type ParallelWorkerContext = c_void; // TODO: access/parallel.h
pub type shm_toc = c_void; // TODO: storage/shm_toc.h
pub type shm_toc_estimator = c_void; // TODO: storage/shm_toc.h
pub type ResourceOwnerData = c_void; // TODO: utils/resowner.h
pub type WaitEventSet = c_void; // TODO: storage/waiteventset.h
pub type Latch = c_void; // TODO: storage/latch.h
pub type pgsocket = c_int; // TODO: port.h
pub type ScanDirection = c_int; // TODO: access/sdir.h

#[repr(C)]
pub struct WaitEvent {
    pub pos: c_int,
    pub events: u32,
    pub fd: pgsocket,
    pub user_data: *mut c_void,
}

// ---- Local stub constants for unported deps ----

const EXEC_FLAG_MARK: c_int = 0x0008; // TODO: executor/executor.h
const LWTRANCHE_PARALLEL_APPEND: c_int = 0; // TODO: storage/lwlock.h
const LW_EXCLUSIVE: LWLockMode = 0; // TODO: storage/lwlock.h
const WL_LATCH_SET: u32 = 1 << 0; // TODO: storage/latch.h
const WL_SOCKET_READABLE: u32 = 1 << 1; // TODO: storage/latch.h
const WL_EXIT_ON_PM_DEATH: u32 = 1 << 5; // TODO: storage/latch.h
const PGINVALID_SOCKET: pgsocket = -1; // TODO: port.h
const WAIT_EVENT_APPEND_READY: u32 = 0; // TODO: utils/wait_event.h

extern "C" {
    static mut MyLatch: *mut Latch; // TODO: storage/latch.h
    static mut CurrentResourceOwner: *mut ResourceOwnerData; // TODO: utils/resowner.h
    static TTSOpsVirtual: TupleTableSlotOps; // TODO: executor/tuptable.h
}
