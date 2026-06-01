//! nodeRecursiveunion.c - routines to handle RecursiveUnion nodes.
//!
//! To implement UNION (without ALL), we need a hashtable that stores tuples
//! already seen.  The hash key is computed from the grouping columns.
//!
//! src/backend/executor/nodeRecursiveunion.c
//! Companion header: src/include/executor/nodeRecursiveunion.h

use crate::prelude::*;

use std::ffi::c_int;
use std::ptr::null_mut;

use crate::nodes::execnodes::{
    innerPlanState, outerPlanState, EState, ParamExecData, PlanState, RecursiveUnionState,
    Tuplestorestate,
};
use crate::nodes::nodes::T_RecursiveUnionState;
use crate::nodes::plannodes::{innerPlan, outerPlan, Plan, RecursiveUnion};
use crate::nodes::pg_list::NIL;

use crate::nodes::bitmapset::{bms_add_member, Bitmapset};

use crate::executor::execGrouping::{
    execTuplesHashPrepare, BuildTupleHashTable, LookupTupleHashEntry, ResetTupleHashTable,
};
use crate::executor::executor::{
    ExecEndNode, ExecGetCommonChildSlotOps, ExecGetResultType, ExecInitNode,
    ExecInitResultTypeTL, ExecProcNode, ExecReScan, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::tuptable::{TupIsNull, TupleTableSlot};

use crate::access::common::tupdesc::TupleDesc;

use crate::miscadmin::{work_mem, CHECK_FOR_INTERRUPTS};

use crate::{castNode, makeNode, AllocSetContextCreate, Assert};

// ---- not-yet-ported helpers (stubbed locally) ----

/// tuplestore_begin_heap - create a new tuplestore.
/// TODO(pg-port): utils/sort/tuplestore.c.
unsafe fn tuplestore_begin_heap(
    randomAccess: bool,
    interXact: bool,
    maxKBytes: c_int,
) -> *mut Tuplestorestate {
    let _ = (randomAccess, interXact, maxKBytes);
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/// tuplestore_puttupleslot - store a tuple slot into the tuplestore.
/// TODO(pg-port): utils/sort/tuplestore.c.
unsafe fn tuplestore_puttupleslot(state: *mut Tuplestorestate, slot: *mut TupleTableSlot) {
    let _ = (state, slot);
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/// tuplestore_clear - clear out all tuples but keep the tuplestore.
/// TODO(pg-port): utils/sort/tuplestore.c.
unsafe fn tuplestore_clear(state: *mut Tuplestorestate) {
    let _ = state;
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/// tuplestore_end - release a tuplestore.
/// TODO(pg-port): utils/sort/tuplestore.c.
unsafe fn tuplestore_end(state: *mut Tuplestorestate) {
    let _ = state;
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

/*
 * Initialize the hash table to empty.
 */
unsafe fn build_hash_table(rustate: *mut RecursiveUnionState) {
    let node = (*rustate).ps.plan as *mut RecursiveUnion;
    let desc: TupleDesc = ExecGetResultType(outerPlanState(rustate as *mut PlanState));

    Assert!((*node).numCols > 0);
    Assert!((*node).numGroups > 0);

    /*
     * If both child plans deliver the same fixed tuple slot type, we can tell
     * BuildTupleHashTable to expect that slot type as input.  Otherwise,
     * we'll pass NULL denoting that any slot type is possible.
     */
    // Cross-module stub divergence: BuildTupleHashTable (execGrouping) uses its
    // own PlanState/TupleDesc/FmgrInfo/MemoryContext/SlotOps stub types; cast.
    (*rustate).hashtable = BuildTupleHashTable(
        &raw mut (*rustate).ps as *mut _,
        desc as *mut _,
        ExecGetCommonChildSlotOps(&mut (*rustate).ps) as *const _,
        (*node).numCols,
        (*node).dupColIdx,
        (*rustate).eqfuncoids,
        (*rustate).hashfunctions as *mut _,
        (*node).dupCollations,
        (*node).numGroups,
        0,
        (*(*rustate).ps.state).es_query_cxt as *mut _,
        (*rustate).tableContext as *mut _,
        (*rustate).tempContext as *mut _,
        false,
    ) as *mut _;
}

/* ----------------------------------------------------------------
 *		ExecRecursiveUnion(node)
 *
 *		Scans the recursive query sequentially and returns the next
 *		qualifying tuple.
 *
 * 1. evaluate non recursive term and assign the result to RT
 *
 * 2. execute recursive terms
 *
 * 2.1 WT := RT
 * 2.2 while WT is not empty repeat 2.3 to 2.6. if WT is empty returns RT
 * 2.3 replace the name of recursive term with WT
 * 2.4 evaluate the recursive term and store into WT
 * 2.5 append WT to RT
 * 2.6 go back to 2.2
 * ----------------------------------------------------------------
 */
unsafe fn ExecRecursiveUnion(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node = castNode!(RecursiveUnionState, T_RecursiveUnionState, pstate);
    let outerPlan = outerPlanState(node as *mut PlanState);
    let innerPlan = innerPlanState(node as *mut PlanState);
    let plan = (*node).ps.plan as *mut RecursiveUnion;
    let mut slot: *mut TupleTableSlot;
    let mut isnew: bool = false;

    CHECK_FOR_INTERRUPTS();

    /* 1. Evaluate non-recursive term */
    if !(*node).recursing {
        loop {
            slot = ExecProcNode(outerPlan);
            if TupIsNull(slot) {
                break;
            }
            if (*plan).numCols > 0 {
                /* Find or build hashtable entry for this tuple's group */
                LookupTupleHashEntry((*node).hashtable as *mut _, slot, &mut isnew, null_mut());
                /* Must reset temp context after each hashtable lookup */
                MemoryContextReset((*node).tempContext);
                /* Ignore tuple if already seen */
                if !isnew {
                    continue;
                }
            }
            /* Each non-duplicate tuple goes to the working table ... */
            tuplestore_puttupleslot((*node).working_table, slot);
            /* ... and to the caller */
            return slot;
        }
        (*node).recursing = true;
    }

    /* 2. Execute recursive term */
    loop {
        slot = ExecProcNode(innerPlan);
        if TupIsNull(slot) {
            /* Done if there's nothing in the intermediate table */
            if (*node).intermediate_empty {
                break;
            }

            /*
             * Now we let the intermediate table become the work table.  We
             * need a fresh intermediate table, so delete the tuples from the
             * current working table and use that as the new intermediate
             * table.  This saves a round of free/malloc from creating a new
             * tuple store.
             */
            tuplestore_clear((*node).working_table);

            let swaptemp = (*node).working_table;
            (*node).working_table = (*node).intermediate_table;
            (*node).intermediate_table = swaptemp;

            /* mark the intermediate table as empty */
            (*node).intermediate_empty = true;

            /* reset the recursive term */
            (*innerPlan).chgParam =
                bms_add_member((*innerPlan).chgParam, (*plan).wtParam);

            /* and continue fetching from recursive term */
            continue;
        }

        if (*plan).numCols > 0 {
            /* Find or build hashtable entry for this tuple's group */
            LookupTupleHashEntry((*node).hashtable as *mut _, slot, &mut isnew, null_mut());
            /* Must reset temp context after each hashtable lookup */
            MemoryContextReset((*node).tempContext);
            /* Ignore tuple if already seen */
            if !isnew {
                continue;
            }
        }

        /* Else, tuple is good; stash it in intermediate table ... */
        (*node).intermediate_empty = false;
        tuplestore_puttupleslot((*node).intermediate_table, slot);
        /* ... and return it */
        return slot;
    }

    null_mut()
}

/* ----------------------------------------------------------------
 *		ExecInitRecursiveUnion
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitRecursiveUnion(
    node: *mut RecursiveUnion,
    estate: *mut EState,
    eflags: c_int,
) -> *mut RecursiveUnionState {
    let prmdata: *mut ParamExecData;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * create state structure
     */
    let rustate: *mut RecursiveUnionState = makeNode!(RecursiveUnionState, T_RecursiveUnionState);
    (*rustate).ps.plan = node as *mut Plan;
    (*rustate).ps.state = estate;
    (*rustate).ps.ExecProcNode = Some(ExecRecursiveUnion);

    (*rustate).eqfuncoids = null_mut();
    (*rustate).hashfunctions = null_mut();
    (*rustate).hashtable = null_mut();
    (*rustate).tempContext = null_mut();
    (*rustate).tableContext = null_mut();

    /* initialize processing state */
    (*rustate).recursing = false;
    (*rustate).intermediate_empty = true;
    (*rustate).working_table = tuplestore_begin_heap(false, false, work_mem);
    (*rustate).intermediate_table = tuplestore_begin_heap(false, false, work_mem);

    /*
     * If hashing, we need a per-tuple memory context for comparisons, and a
     * longer-lived context to store the hash table.  The table can't just be
     * kept in the per-query context because we want to be able to throw it
     * away when rescanning.
     */
    if (*node).numCols > 0 {
        (*rustate).tempContext = AllocSetContextCreate!(
            CurrentMemoryContext,
            "RecursiveUnion",
            ALLOCSET_DEFAULT_SIZES
        );
        (*rustate).tableContext = AllocSetContextCreate!(
            CurrentMemoryContext,
            "RecursiveUnion hash table",
            ALLOCSET_DEFAULT_SIZES
        );
    }

    /*
     * Make the state structure available to descendant WorkTableScan nodes
     * via the Param slot reserved for it.
     */
    prmdata = &mut (*(*estate).es_param_exec_vals.offset((*node).wtParam as isize));
    Assert!((*prmdata).execPlan.is_null());
    (*prmdata).value = PointerGetDatum(rustate as *const c_void);
    (*prmdata).isnull = false;

    /*
     * Miscellaneous initialization
     *
     * RecursiveUnion plans don't have expression contexts because they never
     * call ExecQual or ExecProject.
     */
    Assert!((*node).plan.qual == NIL);

    /*
     * RecursiveUnion nodes still have Result slots, which hold pointers to
     * tuples, so we have to initialize them.
     */
    ExecInitResultTypeTL(&mut (*rustate).ps);

    /*
     * Initialize result tuple type.  (Note: we have to set up the result type
     * before initializing child nodes, because nodeWorktablescan.c expects it
     * to be valid.)
     */
    (*rustate).ps.ps_ProjInfo = null_mut();

    /*
     * initialize child nodes
     */
    (*(rustate as *mut PlanState)).lefttree =
        ExecInitNode(outerPlan(node as *mut Plan), estate, eflags);
    (*(rustate as *mut PlanState)).righttree =
        ExecInitNode(innerPlan(node as *mut Plan), estate, eflags);

    /*
     * If hashing, precompute fmgr lookup data for inner loop, and create the
     * hash table.
     */
    if (*node).numCols > 0 {
        execTuplesHashPrepare(
            (*node).numCols,
            (*node).dupOperators,
            &raw mut (*rustate).eqfuncoids as *mut _,
            &raw mut (*rustate).hashfunctions as *mut _,
        );
        build_hash_table(rustate);
    }

    rustate
}

/* ----------------------------------------------------------------
 *		ExecEndRecursiveUnion
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndRecursiveUnion(node: *mut RecursiveUnionState) {
    /* Release tuplestores */
    tuplestore_end((*node).working_table);
    tuplestore_end((*node).intermediate_table);

    /* free subsidiary stuff including hashtable */
    if !(*node).tempContext.is_null() {
        MemoryContextDelete((*node).tempContext);
    }
    if !(*node).tableContext.is_null() {
        MemoryContextDelete((*node).tableContext);
    }

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));
    ExecEndNode(innerPlanState(node as *mut PlanState));
}

/* ----------------------------------------------------------------
 *		ExecReScanRecursiveUnion
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanRecursiveUnion(node: *mut RecursiveUnionState) {
    let outerPlan = outerPlanState(node as *mut PlanState);
    let innerPlan = innerPlanState(node as *mut PlanState);
    let plan = (*node).ps.plan as *mut RecursiveUnion;

    /*
     * Set recursive term's chgParam to tell it that we'll modify the working
     * table and therefore it has to rescan.
     */
    (*innerPlan).chgParam = bms_add_member((*innerPlan).chgParam, (*plan).wtParam);

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.  Because of above, we only have to do this to the
     * non-recursive term.
     */
    if (*outerPlan).chgParam == null_mut::<Bitmapset>() {
        ExecReScan(outerPlan);
    }

    /* Release any hashtable storage */
    if !(*node).tableContext.is_null() {
        MemoryContextReset((*node).tableContext);
    }

    /* Empty hashtable if needed */
    if (*plan).numCols > 0 {
        ResetTupleHashTable((*node).hashtable as *mut _);
    }

    /* reset processing state */
    (*node).recursing = false;
    (*node).intermediate_empty = true;
    tuplestore_clear((*node).working_table);
    tuplestore_clear((*node).intermediate_table);
}
