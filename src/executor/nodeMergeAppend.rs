/*-------------------------------------------------------------------------
 *
 * nodeMergeAppend.rs
 *	  routines to handle MergeAppend nodes.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeMergeAppend.c
 *	  src/include/executor/nodeMergeAppend.h
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitMergeAppend		- initialize the MergeAppend node
 *		ExecMergeAppend			- retrieve the next tuple from the node
 *		ExecEndMergeAppend		- shut down the MergeAppend node
 *		ExecReScanMergeAppend	- rescan the MergeAppend node
 *
 *	 NOTES
 *		A MergeAppend node contains a list of one or more subplans.
 *		These are each expected to deliver tuples that are sorted according
 *		to a common sort key.  The MergeAppend node merges these streams
 *		to produce output sorted the same way.
 *
 *		MergeAppend nodes don't make use of their left and right
 *		subtrees, rather they maintain a list of subplans so
 *		a typical MergeAppend node looks like this in the plan tree:
 *
 *				   ...
 *				   /
 *				MergeAppend---+------+------+--- nil
 *				/	\		  |		 |		|
 *			  nil	nil		 ...    ...    ...
 *								 subplans
 */

// #include "postgres.h"
use crate::prelude::*;

use std::ffi::c_int;
use std::ffi::c_void;

use crate::c::int32;

use crate::{castNode, makeNode, Assert};

use crate::nodes::execnodes::{
    EState, MergeAppendState, PartitionPruneState, PlanState, TupleTableSlot,
};
use crate::nodes::plannodes::{MergeAppend, Plan};
use crate::nodes::pg_list::list_length;
use crate::nodes::pg_list::list_nth;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::T_MergeAppendState;

use crate::utils::sort::sortsupport::{
    ApplySortComparator, PrepareSortSupportFromOrderingOp, SortSupport, SortSupportData,
};

use crate::lib::binaryheap::{
    binaryheap, binaryheap_add_unordered, binaryheap_allocate, binaryheap_build,
    binaryheap_empty, binaryheap_first, binaryheap_remove_first, binaryheap_replace_first,
    binaryheap_reset, bh_node_type,
};

use crate::nodes::bitmapset::{
    bms_add_range, bms_free, bms_next_member, bms_num_members, bms_overlap,
};

use crate::executor::executor::{
    ExecEndNode, ExecGetCommonSlotOps, ExecInitNode, ExecInitResultTupleSlotTL, ExecProcNode,
    ExecReScan, UpdateChangedParamSet,
};
use crate::executor::tuptable::{
    slot_getattr, ExecClearTuple, TupIsNull, TupleTableSlotOps,
};
use crate::executor::execTuples::TTSOpsVirtual;

use crate::executor::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::access::attnum::AttrNumber;

/*
 * We have one slot for each item in the heap array.  We use SlotNumber
 * to store slot indexes.  This doesn't actually provide any formal
 * type-safety, but it makes the code more self-documenting.
 */
type SlotNumber = int32;

/* ---- local stubs for not-yet-ported helpers ---- */

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

/*
 * PartitionPruneState is opaque in this port; provide stub field accessors so
 * the merge logic can compile faithfully against do_exec_prune / execparamids.
 */
unsafe fn PartitionPruneState_do_exec_prune(_prunestate: *mut PartitionPruneState) -> bool {
    unimplemented!() // TODO: execnodes.h PartitionPruneState
}

unsafe fn PartitionPruneState_execparamids(
    _prunestate: *mut PartitionPruneState,
) -> *mut Bitmapset {
    unimplemented!() // TODO: execnodes.h PartitionPruneState
}

/* ----------------------------------------------------------------
 *		ExecInitMergeAppend
 *
 *		Begin all of the subscans of the MergeAppend node.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitMergeAppend(
    node: *mut MergeAppend,
    estate: *mut EState,
    eflags: c_int,
) -> *mut MergeAppendState {
    let mergestate: *mut MergeAppendState = makeNode!(MergeAppendState, T_MergeAppendState);
    let mergeplanstates: *mut *mut PlanState;
    let mergeops: *const TupleTableSlotOps;
    let mut validsubplans: *mut Bitmapset;
    let nplans: c_int;
    let mut i: c_int;
    let mut j: c_int;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * create new MergeAppendState for our node
     */
    (*mergestate).ps.plan = node as *mut Plan;
    (*mergestate).ps.state = estate;
    (*mergestate).ps.ExecProcNode = Some(ExecMergeAppend);

    /* If run-time partition pruning is enabled, then set that up now */
    if (*node).part_prune_index >= 0 {
        let prunestate: *mut PartitionPruneState;

        /*
         * Set up pruning data structure.  This also initializes the set of
         * subplans to initialize (validsubplans) by taking into account the
         * result of performing initial pruning if any.
         */
        validsubplans = std::ptr::null_mut();
        prunestate = ExecInitPartitionExecPruning(
            &raw mut (*mergestate).ps,
            list_length((*node).mergeplans),
            (*node).part_prune_index,
            (*node).apprelids,
            &raw mut validsubplans,
        );
        (*mergestate).ms_prune_state = prunestate;
        nplans = bms_num_members(validsubplans);

        /*
         * When no run-time pruning is required and there's at least one
         * subplan, we can fill ms_valid_subplans immediately, preventing
         * later calls to ExecFindMatchingSubPlans.
         */
        if !PartitionPruneState_do_exec_prune(prunestate) && nplans > 0 {
            (*mergestate).ms_valid_subplans =
                bms_add_range(std::ptr::null_mut(), 0, nplans - 1);
        }
    } else {
        nplans = list_length((*node).mergeplans);

        /*
         * When run-time partition pruning is not enabled we can just mark all
         * subplans as valid; they must also all be initialized.
         */
        Assert!(nplans > 0);
        validsubplans = bms_add_range(std::ptr::null_mut(), 0, nplans - 1);
        (*mergestate).ms_valid_subplans = validsubplans;
        (*mergestate).ms_prune_state = std::ptr::null_mut();
    }

    mergeplanstates =
        palloc(nplans as usize * std::mem::size_of::<*mut PlanState>()) as *mut *mut PlanState;
    (*mergestate).mergeplans = mergeplanstates;
    (*mergestate).ms_nplans = nplans;

    (*mergestate).ms_slots = palloc0(
        std::mem::size_of::<*mut TupleTableSlot>() * nplans as usize,
    ) as *mut *mut TupleTableSlot;
    (*mergestate).ms_heap = binaryheap_allocate(
        nplans,
        heap_compare_slots,
        mergestate as *mut c_void,
    );

    /*
     * call ExecInitNode on each of the valid plans to be executed and save
     * the results into the mergeplanstates array.
     */
    j = 0;
    i = -1;
    loop {
        i = bms_next_member(validsubplans, i);
        if i < 0 {
            break;
        }
        let initNode = list_nth((*node).mergeplans, i) as *mut Plan;

        *mergeplanstates.offset(j as isize) = ExecInitNode(initNode, estate, eflags);
        j += 1;
    }

    /*
     * Initialize MergeAppend's result tuple type and slot.  If the child
     * plans all produce the same fixed slot type, we can use that slot type;
     * otherwise make a virtual slot.  (Note that the result slot itself is
     * used only to return a null tuple at end of execution; real tuples are
     * returned to the caller in the children's own result slots.  What we are
     * doing here is allowing the parent plan node to optimize if the
     * MergeAppend will return only one kind of slot.)
     */
    mergeops = ExecGetCommonSlotOps(mergeplanstates, j);
    if !mergeops.is_null() {
        ExecInitResultTupleSlotTL(&raw mut (*mergestate).ps, mergeops);
    } else {
        ExecInitResultTupleSlotTL(
            &raw mut (*mergestate).ps,
            &TTSOpsVirtual as *const TupleTableSlotOps,
        );
        /* show that the output slot type is not fixed */
        (*mergestate).ps.resultopsset = true;
        (*mergestate).ps.resultopsfixed = false;
    }

    /*
     * Miscellaneous initialization
     */
    (*mergestate).ps.ps_ProjInfo = std::ptr::null_mut();

    /*
     * initialize sort-key information
     */
    (*mergestate).ms_nkeys = (*node).numCols;
    (*mergestate).ms_sortkeys = palloc0(
        std::mem::size_of::<SortSupportData>() * (*node).numCols as usize,
    ) as SortSupport;

    i = 0;
    while i < (*node).numCols {
        let sortKey: SortSupport = (*mergestate).ms_sortkeys.offset(i as isize);

        (*sortKey).ssup_cxt = CurrentMemoryContext;
        (*sortKey).ssup_collation = *(*node).collations.offset(i as isize);
        (*sortKey).ssup_nulls_first = *(*node).nullsFirst.offset(i as isize);
        (*sortKey).ssup_attno = *(*node).sortColIdx.offset(i as isize);

        /*
         * It isn't feasible to perform abbreviated key conversion, since
         * tuples are pulled into mergestate's binary heap as needed.  It
         * would likely be counter-productive to convert tuples into an
         * abbreviated representation as they're pulled up, so opt out of that
         * additional optimization entirely.
         */
        (*sortKey).abbreviate = false;

        PrepareSortSupportFromOrderingOp(*(*node).sortOperators.offset(i as isize), sortKey);

        i += 1;
    }

    /*
     * initialize to show we have not run the subplans yet
     */
    (*mergestate).ms_initialized = false;

    mergestate
}

/* ----------------------------------------------------------------
 *	   ExecMergeAppend
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
unsafe fn ExecMergeAppend(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut MergeAppendState = castNode!(MergeAppendState, T_MergeAppendState, pstate);
    let result: *mut TupleTableSlot;
    let mut i: SlotNumber;

    CHECK_FOR_INTERRUPTS();

    if !(*node).ms_initialized {
        /* Nothing to do if all subplans were pruned */
        if (*node).ms_nplans == 0 {
            return ExecClearTuple((*node).ps.ps_ResultTupleSlot);
        }

        /*
         * If we've yet to determine the valid subplans then do so now.  If
         * run-time pruning is disabled then the valid subplans will always be
         * set to all subplans.
         */
        if (*node).ms_valid_subplans.is_null() {
            (*node).ms_valid_subplans = ExecFindMatchingSubPlans(
                (*node).ms_prune_state,
                false,
                std::ptr::null_mut(),
            );
        }

        /*
         * First time through: pull the first tuple from each valid subplan,
         * and set up the heap.
         */
        i = -1;
        loop {
            i = bms_next_member((*node).ms_valid_subplans, i);
            if i < 0 {
                break;
            }
            *(*node).ms_slots.offset(i as isize) =
                ExecProcNode(*(*node).mergeplans.offset(i as isize));
            if !TupIsNull(*(*node).ms_slots.offset(i as isize)) {
                binaryheap_add_unordered((*node).ms_heap, Int32GetDatum(i) as bh_node_type);
            }
        }
        binaryheap_build((*node).ms_heap);
        (*node).ms_initialized = true;
    } else {
        /*
         * Otherwise, pull the next tuple from whichever subplan we returned
         * from last time, and reinsert the subplan index into the heap,
         * because it might now compare differently against the existing
         * elements of the heap.  (We could perhaps simplify the logic a bit
         * by doing this before returning from the prior call, but it's better
         * to not pull tuples until necessary.)
         */
        i = DatumGetInt32(binaryheap_first((*node).ms_heap) as Datum);
        *(*node).ms_slots.offset(i as isize) =
            ExecProcNode(*(*node).mergeplans.offset(i as isize));
        if !TupIsNull(*(*node).ms_slots.offset(i as isize)) {
            binaryheap_replace_first((*node).ms_heap, Int32GetDatum(i) as bh_node_type);
        } else {
            let _ = binaryheap_remove_first((*node).ms_heap);
        }
    }

    if binaryheap_empty((*node).ms_heap) {
        /* All the subplans are exhausted, and so is the heap */
        result = ExecClearTuple((*node).ps.ps_ResultTupleSlot);
    } else {
        i = DatumGetInt32(binaryheap_first((*node).ms_heap) as Datum);
        result = *(*node).ms_slots.offset(i as isize);
    }

    result
}

/*
 * Compare the tuples in the two given slots.
 */
unsafe fn heap_compare_slots(a: Datum, b: Datum, arg: *mut c_void) -> c_int {
    let node = arg as *mut MergeAppendState;
    let slot1: SlotNumber = DatumGetInt32(a);
    let slot2: SlotNumber = DatumGetInt32(b);

    let s1: *mut TupleTableSlot = *(*node).ms_slots.offset(slot1 as isize);
    let s2: *mut TupleTableSlot = *(*node).ms_slots.offset(slot2 as isize);
    let mut nkey: c_int;

    Assert!(!TupIsNull(s1));
    Assert!(!TupIsNull(s2));

    nkey = 0;
    while nkey < (*node).ms_nkeys {
        let sortKey: SortSupport = (*node).ms_sortkeys.offset(nkey as isize);
        let attno: AttrNumber = (*sortKey).ssup_attno;
        let datum1: Datum;
        let datum2: Datum;
        let mut isNull1: bool = false;
        let mut isNull2: bool = false;
        let mut compare: c_int;

        datum1 = slot_getattr(s1, attno as c_int, &raw mut isNull1);
        datum2 = slot_getattr(s2, attno as c_int, &raw mut isNull2);

        compare = ApplySortComparator(datum1, isNull1, datum2, isNull2, sortKey);
        if compare != 0 {
            /* INVERT_COMPARE_RESULT(compare); */
            compare = -compare;
            return compare;
        }

        nkey += 1;
    }
    0
}

/* ----------------------------------------------------------------
 *		ExecEndMergeAppend
 *
 *		Shuts down the subscans of the MergeAppend node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndMergeAppend(node: *mut MergeAppendState) {
    let mergeplans: *mut *mut PlanState;
    let nplans: c_int;
    let mut i: c_int;

    /*
     * get information from the node
     */
    mergeplans = (*node).mergeplans;
    nplans = (*node).ms_nplans;

    /*
     * shut down each of the subscans
     */
    i = 0;
    while i < nplans {
        ExecEndNode(*mergeplans.offset(i as isize));
        i += 1;
    }
}

pub unsafe fn ExecReScanMergeAppend(node: *mut MergeAppendState) {
    let mut i: c_int;

    /*
     * If any PARAM_EXEC Params used in pruning expressions have changed, then
     * we'd better unset the valid subplans so that they are reselected for
     * the new parameter values.
     */
    if !(*node).ms_prune_state.is_null()
        && bms_overlap(
            (*node).ps.chgParam,
            PartitionPruneState_execparamids((*node).ms_prune_state),
        )
    {
        bms_free((*node).ms_valid_subplans);
        (*node).ms_valid_subplans = std::ptr::null_mut();
    }

    i = 0;
    while i < (*node).ms_nplans {
        let subnode: *mut PlanState = *(*node).mergeplans.offset(i as isize);

        /*
         * ExecReScan doesn't know about my subplans, so I have to do
         * changed-parameter signaling myself.
         */
        if !(*node).ps.chgParam.is_null() {
            UpdateChangedParamSet(subnode, (*node).ps.chgParam);
        }

        /*
         * If chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (*subnode).chgParam.is_null() {
            ExecReScan(subnode);
        }

        i += 1;
    }
    binaryheap_reset((*node).ms_heap);
    (*node).ms_initialized = false;
}
