//! nodeSetOp.c - Routines to handle INTERSECT and EXCEPT selection.
//!
//! The input of a SetOp node consists of two relations (outer and inner) with
//! identical column sets.  In SETOP_SORTED mode the node performs a merge join
//! on the grouping columns, counting how many tuples from each input match.
//! In SETOP_HASHED mode the outer relation is read into a hash table counting
//! group sizes, then the inner relation increments matching group counts, and
//! finally the hash table is scanned to emit the SQL-spec'd output.

use crate::prelude::*;

use std::ffi::c_int;
use std::ptr::null_mut;

use crate::nodes::execnodes::{
    innerPlanState, outerPlanState, EState, ExprContext, PlanState, SetOpState,
    SetOpStatePerInput, TupleHashIterator,
};
use crate::nodes::nodes::{
    SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL, SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL,
    SETOP_HASHED, T_SetOpState,
};
use crate::nodes::plannodes::{innerPlan, outerPlan, Plan, SetOp};

use crate::access::attnum::AttrNumber;

use crate::executor::execGrouping::{
    execTuplesHashPrepare, BuildTupleHashTable, LookupTupleHashEntry, ResetTupleHashTable,
    TupleHashEntry, TupleHashEntryGetAdditional, TupleHashEntryGetTuple, TupleHashTable,
};
use crate::executor::execTuples::TTSOpsMinimalTuple;
use crate::executor::execTuples::ExecStoreMinimalTuple;
use crate::executor::executor::{
    ExecAssignExprContext, ExecEndNode, ExecGetCommonChildSlotOps, ExecGetResultType,
    ExecInitExtraTupleSlot, ExecInitNode, ExecInitResultTupleSlotTL, ExecProcNode, ExecReScan,
    ResetExprContext, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND,
};
use crate::executor::tuptable::{
    slot_getallattrs, ExecClearTuple, ExecCopySlotMinimalTuple, TupIsNull, TupleTableSlot,
};

use crate::utils::sort::sortsupport::{
    ApplySortComparator, PrepareSortSupportFromOrderingOp, SortSupport, SortSupportData,
};

use crate::miscadmin::CHECK_FOR_INTERRUPTS;

use crate::{castNode, makeNode, AllocSetContextCreate, Assert};

/*
 * SetOpStatePerGroupData - per-group working state
 *
 * In SETOP_SORTED mode, we need only one of these structs, and it's just a
 * local in setop_retrieve_sorted.  In SETOP_HASHED mode, the hash table
 * contains one of these for each tuple group.
 */
#[repr(C)]
pub struct SetOpStatePerGroupData {
    pub numLeft: int64,  /* number of left-input dups in group */
    pub numRight: int64, /* number of right-input dups in group */
}

pub type SetOpStatePerGroup = *mut SetOpStatePerGroupData;

// ---- not-yet-ported helpers (stubbed locally) ----

/// ResetTupleHashIterator - begin iterating over the hash table.
/// TODO(pg-port): provided by execGrouping.c / simplehash iterator macros.
unsafe fn ResetTupleHashIterator(hashtable: TupleHashTable, iter: *mut TupleHashIterator) {
    let _ = (hashtable, iter);
    unimplemented!("ResetTupleHashIterator: simplehash iterator not ported")
}

/// ScanTupleHashTable - return the next entry, or NULL when exhausted.
/// TODO(pg-port): provided by execGrouping.c / simplehash iterator macros.
unsafe fn ScanTupleHashTable(
    hashtable: TupleHashTable,
    iter: *mut TupleHashIterator,
) -> TupleHashEntry {
    let _ = (hashtable, iter);
    unimplemented!("ScanTupleHashTable: simplehash iterator not ported")
}

/*
 * Initialize the hash table to empty.
 */
unsafe fn build_hash_table(setopstate: *mut SetOpState) {
    let node: *mut SetOp = (*setopstate).ps.plan as *mut SetOp;
    let econtext: *mut ExprContext = (*setopstate).ps.ps_ExprContext;
    let desc = ExecGetResultType(outerPlanState(setopstate as *mut PlanState));

    Assert!((*node).strategy == SETOP_HASHED);
    Assert!((*node).numGroups > 0);

    /*
     * If both child plans deliver the same fixed tuple slot type, we can tell
     * BuildTupleHashTable to expect that slot type as input.  Otherwise,
     * we'll pass NULL denoting that any slot type is possible.
     */
    (*setopstate).hashtable = BuildTupleHashTable(
        &mut (*setopstate).ps as *mut PlanState as *mut _,
        desc as *mut _,
        ExecGetCommonChildSlotOps(&mut (*setopstate).ps as *mut PlanState),
        (*node).numCols,
        (*node).cmpColIdx,
        (*setopstate).eqfuncoids,
        (*setopstate).hashfunctions as *mut _,
        (*node).cmpCollations,
        (*node).numGroups,
        core::mem::size_of::<SetOpStatePerGroupData>(),
        (*(*setopstate).ps.state).es_query_cxt as *mut _,
        (*setopstate).tableContext as *mut _,
        (*econtext).ecxt_per_tuple_memory as *mut _,
        false,
    ) as *mut _;
}

/*
 * We've completed processing a tuple group.  Decide how many copies (if any)
 * of its representative row to emit, and store the count into numOutput.
 * This logic is straight from the SQL92 specification.
 */
unsafe fn set_output_count(setopstate: *mut SetOpState, pergroup: SetOpStatePerGroup) {
    let plannode: *mut SetOp = (*setopstate).ps.plan as *mut SetOp;

    match (*plannode).cmd {
        SETOPCMD_INTERSECT => {
            if (*pergroup).numLeft > 0 && (*pergroup).numRight > 0 {
                (*setopstate).numOutput = 1;
            } else {
                (*setopstate).numOutput = 0;
            }
        }
        SETOPCMD_INTERSECT_ALL => {
            (*setopstate).numOutput = if (*pergroup).numLeft < (*pergroup).numRight {
                (*pergroup).numLeft
            } else {
                (*pergroup).numRight
            };
        }
        SETOPCMD_EXCEPT => {
            if (*pergroup).numLeft > 0 && (*pergroup).numRight == 0 {
                (*setopstate).numOutput = 1;
            } else {
                (*setopstate).numOutput = 0;
            }
        }
        SETOPCMD_EXCEPT_ALL => {
            (*setopstate).numOutput = if (*pergroup).numLeft < (*pergroup).numRight {
                0
            } else {
                (*pergroup).numLeft - (*pergroup).numRight
            };
        }
        #[allow(unreachable_patterns)]
        _ => {
            elog!(ERROR, "unrecognized set op: {}", (*plannode).cmd as c_int);
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecSetOp
 * ----------------------------------------------------------------
 */
unsafe fn ExecSetOp(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut SetOpState = castNode!(SetOpState, T_SetOpState, pstate);
    let plannode: *mut SetOp = (*node).ps.plan as *mut SetOp;
    let resultTupleSlot: *mut TupleTableSlot = (*node).ps.ps_ResultTupleSlot;

    CHECK_FOR_INTERRUPTS();

    /*
     * If the previously-returned tuple needs to be returned more than once,
     * keep returning it.
     */
    if (*node).numOutput > 0 {
        (*node).numOutput -= 1;
        return resultTupleSlot;
    }

    /* Otherwise, we're done if we are out of groups */
    if (*node).setop_done {
        return null_mut();
    }

    /* Fetch the next tuple group according to the correct strategy */
    if (*plannode).strategy == SETOP_HASHED {
        if !(*node).table_filled {
            setop_fill_hash_table(node);
        }
        setop_retrieve_hash_table(node)
    } else {
        setop_retrieve_sorted(node)
    }
}

/*
 * ExecSetOp for non-hashed case
 */
unsafe fn setop_retrieve_sorted(setopstate: *mut SetOpState) -> *mut TupleTableSlot {
    let outerPlan: *mut PlanState;
    let innerPlan: *mut PlanState;
    let resultTupleSlot: *mut TupleTableSlot;

    /*
     * get state info from node
     */
    outerPlan = outerPlanState(setopstate as *mut PlanState);
    innerPlan = innerPlanState(setopstate as *mut PlanState);
    resultTupleSlot = (*setopstate).ps.ps_ResultTupleSlot;

    /*
     * If first time through, establish the invariant that setop_load_group
     * expects: each side's nextTupleSlot is the next output from the child
     * plan, or empty if there is no more output from it.
     */
    if (*setopstate).need_init {
        (*setopstate).need_init = false;

        (*setopstate).leftInput.nextTupleSlot = ExecProcNode(outerPlan);

        /*
         * If the outer relation is empty, then we will emit nothing, and we
         * don't need to read the inner relation at all.
         */
        if TupIsNull((*setopstate).leftInput.nextTupleSlot) {
            (*setopstate).setop_done = true;
            return null_mut();
        }

        (*setopstate).rightInput.nextTupleSlot = ExecProcNode(innerPlan);

        /* Set flags that we've not completed either side's group */
        (*setopstate).leftInput.needGroup = true;
        (*setopstate).rightInput.needGroup = true;
    }

    /*
     * We loop retrieving groups until we find one we should return
     */
    while !(*setopstate).setop_done {
        let cmpresult: c_int;
        let mut pergroup: SetOpStatePerGroupData =
            SetOpStatePerGroupData { numLeft: 0, numRight: 0 };

        /*
         * Fetch the rest of the current outer group, if we didn't already.
         */
        if (*setopstate).leftInput.needGroup {
            setop_load_group(&mut (*setopstate).leftInput, outerPlan, setopstate);
        }

        /*
         * If no more outer groups, we're done, and don't need to look at any
         * more of the inner relation.
         */
        if (*setopstate).leftInput.numTuples == 0 {
            (*setopstate).setop_done = true;
            break;
        }

        /*
         * Fetch the rest of the current inner group, if we didn't already.
         */
        if (*setopstate).rightInput.needGroup {
            setop_load_group(&mut (*setopstate).rightInput, innerPlan, setopstate);
        }

        /*
         * Determine whether we have matching groups on both sides (this is
         * basically like the core logic of a merge join).
         */
        if (*setopstate).rightInput.numTuples == 0 {
            cmpresult = -1; /* as though left input is lesser */
        } else {
            cmpresult = setop_compare_slots(
                (*setopstate).leftInput.firstTupleSlot,
                (*setopstate).rightInput.firstTupleSlot,
                setopstate,
            );
        }

        if cmpresult < 0 {
            /* Left group is first, and has no right matches */
            pergroup.numLeft = (*setopstate).leftInput.numTuples;
            pergroup.numRight = 0;
            /* We'll need another left group next time */
            (*setopstate).leftInput.needGroup = true;
        } else if cmpresult == 0 {
            /* We have matching groups */
            pergroup.numLeft = (*setopstate).leftInput.numTuples;
            pergroup.numRight = (*setopstate).rightInput.numTuples;
            /* We'll need to read from both sides next time */
            (*setopstate).leftInput.needGroup = true;
            (*setopstate).rightInput.needGroup = true;
        } else {
            /* Right group has no left matches, so we can ignore it */
            (*setopstate).rightInput.needGroup = true;
            continue;
        }

        /*
         * Done scanning these input tuple groups.  See if we should emit any
         * copies of result tuple, and if so return the first copy.  (Note
         * that the result tuple is the same as the left input's firstTuple
         * slot.)
         */
        set_output_count(setopstate, &mut pergroup);

        if (*setopstate).numOutput > 0 {
            (*setopstate).numOutput -= 1;
            return resultTupleSlot;
        }
    }

    /* No more groups */
    ExecClearTuple(resultTupleSlot);
    null_mut()
}

/*
 * Load next group of tuples from one child plan or the other.
 *
 * On entry, we've already read the first tuple of the next group
 * (if there is one) into input->nextTupleSlot.  This invariant
 * is maintained on exit.
 */
unsafe fn setop_load_group(
    input: *mut SetOpStatePerInput,
    inputPlan: *mut PlanState,
    setopstate: *mut SetOpState,
) {
    (*input).needGroup = false;

    /* If we've exhausted this child plan, report an empty group */
    if TupIsNull((*input).nextTupleSlot) {
        ExecClearTuple((*input).firstTupleSlot);
        (*input).numTuples = 0;
        return;
    }

    /* Make a local copy of the first tuple for comparisons */
    ExecStoreMinimalTuple(
        ExecCopySlotMinimalTuple((*input).nextTupleSlot),
        (*input).firstTupleSlot,
        true,
    );
    /* and count it */
    (*input).numTuples = 1;

    /* Scan till we find the end-of-group */
    loop {
        let cmpresult: c_int;

        /* Get next input tuple, if there is one */
        (*input).nextTupleSlot = ExecProcNode(inputPlan);
        if TupIsNull((*input).nextTupleSlot) {
            break;
        }

        /* There is; does it belong to same group as firstTuple? */
        cmpresult = setop_compare_slots((*input).firstTupleSlot, (*input).nextTupleSlot, setopstate);
        Assert!(cmpresult <= 0); /* else input is mis-sorted */
        if cmpresult != 0 {
            break;
        }

        /* Still in same group, so count this tuple */
        (*input).numTuples += 1;
    }
}

/*
 * Compare the tuples in the two given slots.
 */
unsafe fn setop_compare_slots(
    s1: *mut TupleTableSlot,
    s2: *mut TupleTableSlot,
    setopstate: *mut SetOpState,
) -> c_int {
    /* We'll often need to fetch all the columns, so just do it */
    slot_getallattrs(s1);
    slot_getallattrs(s2);
    for nkey in 0..(*setopstate).numCols {
        let sortKey: SortSupport = (*setopstate).sortKeys.offset(nkey as isize);
        let attno: AttrNumber = (*sortKey).ssup_attno;
        let datum1: Datum = *(*s1).tts_values.offset((attno - 1) as isize);
        let datum2: Datum = *(*s2).tts_values.offset((attno - 1) as isize);
        let isNull1: bool = *(*s1).tts_isnull.offset((attno - 1) as isize);
        let isNull2: bool = *(*s2).tts_isnull.offset((attno - 1) as isize);

        let compare: c_int =
            ApplySortComparator(datum1, isNull1, datum2, isNull2, sortKey);
        if compare != 0 {
            return compare;
        }
    }
    0
}

/*
 * ExecSetOp for hashed case: phase 1, read inputs and build hash table
 */
unsafe fn setop_fill_hash_table(setopstate: *mut SetOpState) {
    let outerPlan: *mut PlanState;
    let innerPlan: *mut PlanState;
    let econtext: *mut ExprContext = (*setopstate).ps.ps_ExprContext;
    let mut have_tuples: bool = false;

    /*
     * get state info from node
     */
    outerPlan = outerPlanState(setopstate as *mut PlanState);
    innerPlan = innerPlanState(setopstate as *mut PlanState);

    /*
     * Process each outer-plan tuple, and then fetch the next one, until we
     * exhaust the outer plan.
     */
    loop {
        let outerslot: *mut TupleTableSlot;
        let hashtable: TupleHashTable = (*setopstate).hashtable as *mut _;
        let entry: TupleHashEntry;
        let pergroup: SetOpStatePerGroup;
        let mut isnew: bool = false;

        outerslot = ExecProcNode(outerPlan);
        if TupIsNull(outerslot) {
            break;
        }
        have_tuples = true;

        /* Find or build hashtable entry for this tuple's group */
        entry = LookupTupleHashEntry(hashtable, outerslot, &mut isnew, null_mut());

        pergroup = TupleHashEntryGetAdditional(hashtable, entry) as SetOpStatePerGroup;
        /* If new tuple group, initialize counts to zero */
        if isnew {
            (*pergroup).numLeft = 0;
            (*pergroup).numRight = 0;
        }

        /* Advance the counts */
        (*pergroup).numLeft += 1;

        /* Must reset expression context after each hashtable lookup */
        ResetExprContext(econtext);
    }

    /*
     * If the outer relation is empty, then we will emit nothing, and we don't
     * need to read the inner relation at all.
     */
    if have_tuples {
        /*
         * Process each inner-plan tuple, and then fetch the next one, until
         * we exhaust the inner plan.
         */
        loop {
            let innerslot: *mut TupleTableSlot;
            let hashtable: TupleHashTable = (*setopstate).hashtable as *mut _;
            let entry: TupleHashEntry;

            innerslot = ExecProcNode(innerPlan);
            if TupIsNull(innerslot) {
                break;
            }

            /* For tuples not seen previously, do not make hashtable entry */
            entry = LookupTupleHashEntry(hashtable, innerslot, null_mut(), null_mut());

            /* Advance the counts if entry is already present */
            if !entry.is_null() {
                let pergroup: SetOpStatePerGroup =
                    TupleHashEntryGetAdditional(hashtable, entry) as SetOpStatePerGroup;

                (*pergroup).numRight += 1;
            }

            /* Must reset expression context after each hashtable lookup */
            ResetExprContext(econtext);
        }
    }

    (*setopstate).table_filled = true;
    /* Initialize to walk the hash table */
    ResetTupleHashIterator((*setopstate).hashtable as *mut _, &mut (*setopstate).hashiter);
}

/*
 * ExecSetOp for hashed case: phase 2, retrieving groups from hash table
 */
unsafe fn setop_retrieve_hash_table(setopstate: *mut SetOpState) -> *mut TupleTableSlot {
    let mut entry: TupleHashEntry;
    let resultTupleSlot: *mut TupleTableSlot;

    /*
     * get state info from node
     */
    resultTupleSlot = (*setopstate).ps.ps_ResultTupleSlot;

    /*
     * We loop retrieving groups until we find one we should return
     */
    while !(*setopstate).setop_done {
        let hashtable: TupleHashTable = (*setopstate).hashtable as *mut _;
        let pergroup: SetOpStatePerGroup;

        CHECK_FOR_INTERRUPTS();

        /*
         * Find the next entry in the hash table
         */
        entry = ScanTupleHashTable(hashtable, &mut (*setopstate).hashiter);
        if entry.is_null() {
            /* No more entries in hashtable, so done */
            (*setopstate).setop_done = true;
            return null_mut();
        }

        /*
         * See if we should emit any copies of this tuple, and if so return
         * the first copy.
         */
        pergroup = TupleHashEntryGetAdditional(hashtable, entry) as SetOpStatePerGroup;
        set_output_count(setopstate, pergroup);

        if (*setopstate).numOutput > 0 {
            (*setopstate).numOutput -= 1;
            return ExecStoreMinimalTuple(
                TupleHashEntryGetTuple(entry),
                resultTupleSlot,
                false,
            );
        }
    }

    /* No more groups */
    ExecClearTuple(resultTupleSlot);
    null_mut()
}

/* ----------------------------------------------------------------
 *		ExecInitSetOp
 *
 *		This initializes the setop node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitSetOp(node: *mut SetOp, estate: *mut EState, mut eflags: c_int) -> *mut SetOpState {
    let setopstate: *mut SetOpState;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * create state structure
     */
    setopstate = makeNode!(SetOpState, T_SetOpState);
    (*setopstate).ps.plan = node as *mut Plan;
    (*setopstate).ps.state = estate;
    (*setopstate).ps.ExecProcNode = Some(ExecSetOp);

    (*setopstate).setop_done = false;
    (*setopstate).numOutput = 0;
    (*setopstate).numCols = (*node).numCols;
    (*setopstate).need_init = true;

    /*
     * create expression context
     */
    ExecAssignExprContext(estate, &mut (*setopstate).ps as *mut PlanState);

    /*
     * If hashing, we also need a longer-lived context to store the hash
     * table.  The table can't just be kept in the per-query context because
     * we want to be able to throw it away in ExecReScanSetOp.
     */
    if (*node).strategy == SETOP_HASHED {
        (*setopstate).tableContext = AllocSetContextCreate!(
            CurrentMemoryContext,
            "SetOp hash table",
            ALLOCSET_DEFAULT_SIZES
        );
    }

    /*
     * initialize child nodes
     *
     * If we are hashing then the child plans do not need to handle REWIND
     * efficiently; see ExecReScanSetOp.
     */
    if (*node).strategy == SETOP_HASHED {
        eflags &= !EXEC_FLAG_REWIND;
    }
    (*(setopstate as *mut PlanState)).lefttree =
        ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);
    (*(setopstate as *mut PlanState)).righttree =
        ExecInitNode(innerPlan(node as *mut Plan), estate, eflags);

    /*
     * Initialize locally-allocated slots.  In hashed mode, we just need a
     * result slot.  In sorted mode, we need one first-tuple-of-group slot for
     * each input; we use the result slot for the left input's slot and create
     * another for the right input.  (Note: the nextTupleSlot slots are not
     * ours, but just point to the last slot returned by the input plan node.)
     */
    ExecInitResultTupleSlotTL(&mut (*setopstate).ps as *mut PlanState, &TTSOpsMinimalTuple);
    if (*node).strategy != SETOP_HASHED {
        (*setopstate).leftInput.firstTupleSlot = (*setopstate).ps.ps_ResultTupleSlot;
        (*setopstate).rightInput.firstTupleSlot = ExecInitExtraTupleSlot(
            estate,
            (*setopstate).ps.ps_ResultTupleDesc,
            &TTSOpsMinimalTuple,
        );
    }

    /* Setop nodes do no projections. */
    (*setopstate).ps.ps_ProjInfo = null_mut();

    /*
     * Precompute fmgr lookup data for inner loop.  We need equality and
     * hashing functions to do it by hashing, while for sorting we need
     * SortSupport data.
     */
    if (*node).strategy == SETOP_HASHED {
        execTuplesHashPrepare(
            (*node).numCols,
            (*node).cmpOperators,
            &mut (*setopstate).eqfuncoids,
            core::ptr::addr_of_mut!((*setopstate).hashfunctions) as *mut _,
        );
    } else {
        let nkeys: c_int = (*node).numCols;

        (*setopstate).sortKeys = palloc0(nkeys as usize * core::mem::size_of::<SortSupportData>())
            as SortSupport;
        for i in 0..nkeys {
            let sortKey: SortSupport = (*setopstate).sortKeys.offset(i as isize);

            (*sortKey).ssup_cxt = CurrentMemoryContext;
            (*sortKey).ssup_collation = *(*node).cmpCollations.offset(i as isize);
            (*sortKey).ssup_nulls_first = *(*node).cmpNullsFirst.offset(i as isize);
            (*sortKey).ssup_attno = *(*node).cmpColIdx.offset(i as isize);
            /* abbreviated key conversion is not useful here */
            (*sortKey).abbreviate = false;

            PrepareSortSupportFromOrderingOp(*(*node).cmpOperators.offset(i as isize), sortKey);
        }
    }

    /* Create a hash table if needed */
    if (*node).strategy == SETOP_HASHED {
        build_hash_table(setopstate);
        (*setopstate).table_filled = false;
    }

    setopstate
}

/* ----------------------------------------------------------------
 *		ExecEndSetOp
 *
 *		This shuts down the subplans and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndSetOp(node: *mut SetOpState) {
    /* free subsidiary stuff including hashtable */
    if !(*node).tableContext.is_null() {
        MemoryContextDelete((*node).tableContext);
    }

    ExecEndNode(outerPlanState(node as *mut PlanState));
    ExecEndNode(innerPlanState(node as *mut PlanState));
}

pub unsafe fn ExecReScanSetOp(node: *mut SetOpState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);
    let innerPlan: *mut PlanState = innerPlanState(node as *mut PlanState);

    ExecClearTuple((*node).ps.ps_ResultTupleSlot);
    (*node).setop_done = false;
    (*node).numOutput = 0;

    if (*((*node).ps.plan as *mut SetOp)).strategy == SETOP_HASHED {
        /*
         * In the hashed case, if we haven't yet built the hash table then we
         * can just return; nothing done yet, so nothing to undo. If subnode's
         * chgParam is not NULL then it will be re-scanned by ExecProcNode,
         * else no reason to re-scan it at all.
         */
        if !(*node).table_filled {
            return;
        }

        /*
         * If we do have the hash table and the subplans do not have any
         * parameter changes, then we can just rescan the existing hash table;
         * no need to build it again.
         */
        if (*outerPlan).chgParam.is_null() && (*innerPlan).chgParam.is_null() {
            ResetTupleHashIterator((*node).hashtable as *mut _, &mut (*node).hashiter);
            return;
        }

        /* Release any hashtable storage */
        if !(*node).tableContext.is_null() {
            MemoryContextReset((*node).tableContext);
        }

        /* And rebuild an empty hashtable */
        ResetTupleHashTable((*node).hashtable as *mut _);
        (*node).table_filled = false;
    } else {
        /* Need to re-read first input from each side */
        (*node).need_init = true;
    }

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (*outerPlan).chgParam.is_null() {
        ExecReScan(outerPlan);
    }
    if (*innerPlan).chgParam.is_null() {
        ExecReScan(innerPlan);
    }
}
