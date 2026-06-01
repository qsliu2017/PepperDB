//! nodeMaterial.c - Routines to handle materialization nodes.
//!
//! INTERFACE ROUTINES
//!     ExecMaterial        - materialize the result of a subplan
//!     ExecInitMaterial    - initialize node and subnodes
//!     ExecEndMaterial     - shutdown node and subnodes

use crate::prelude::*;

use std::ptr::null_mut;

use crate::access::sdir::{ScanDirection, ScanDirectionIsForward};
use crate::nodes::execnodes::{
    outerPlanState, EState, MaterialState, PlanState, Tuplestorestate,
};
use crate::nodes::plannodes::{outerPlan, Material, Plan};

use crate::executor::execTuples::TTSOpsMinimalTuple;
use crate::executor::execUtils::ExecCreateScanSlotFromOuterPlan;
use crate::executor::executor::{
    ExecEndNode, ExecInitNode, ExecInitResultTupleSlotTL, ExecProcNode,
    ExecReScan, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND,
};
use crate::executor::tuptable::{
    ExecClearTuple, ExecCopySlot, TupIsNull, TupleTableSlot,
};
use crate::miscadmin::{work_mem, CHECK_FOR_INTERRUPTS};

use crate::{castNode, makeNode, Assert};

/* ----------------------------------------------------------------
 *		ExecMaterial
 *
 *		As long as we are at the end of the data collected in the tuplestore,
 *		we collect one new row from the subplan on each call, and stash it
 *		aside in the tuplestore before returning it.  The tuplestore is
 *		only read if we are asked to scan backwards, rescan, or mark/restore.
 *
 * ----------------------------------------------------------------
 */
unsafe fn ExecMaterial(pstate: *mut PlanState) -> *mut TupleTableSlot {
    let node: *mut MaterialState =
        castNode!(MaterialState, T_MaterialState, pstate);
    let estate: *mut EState;
    let dir: ScanDirection;
    let forward: bool;
    let mut tuplestorestate: *mut Tuplestorestate;
    let mut eof_tuplestore: bool;
    let slot: *mut TupleTableSlot;

    CHECK_FOR_INTERRUPTS();

    /*
     * get state info from node
     */
    estate = (*node).ss.ps.state;
    dir = (*estate).es_direction;
    forward = ScanDirectionIsForward(dir);
    tuplestorestate = (*node).tuplestorestate;

    /*
     * If first time through, and we need a tuplestore, initialize it.
     */
    if tuplestorestate.is_null() && (*node).eflags != 0 {
        tuplestorestate = tuplestore_begin_heap(true, false, work_mem);
        tuplestore_set_eflags(tuplestorestate, (*node).eflags);
        if (*node).eflags & EXEC_FLAG_MARK != 0 {
            /*
             * Allocate a second read pointer to serve as the mark. We know it
             * must have index 1, so needn't store that.
             */
            let ptrno: c_int /* PG_USED_FOR_ASSERTS_ONLY */;

            ptrno =
                tuplestore_alloc_read_pointer(tuplestorestate, (*node).eflags);
            Assert!(ptrno == 1);
        }
        (*node).tuplestorestate = tuplestorestate;
    }

    /*
     * If we are not at the end of the tuplestore, or are going backwards, try
     * to fetch a tuple from tuplestore.
     */
    eof_tuplestore =
        tuplestorestate.is_null() || tuplestore_ateof(tuplestorestate);

    if !forward && eof_tuplestore {
        if !(*node).eof_underlying {
            /*
             * When reversing direction at tuplestore EOF, the first
             * gettupleslot call will fetch the last-added tuple; but we want
             * to return the one before that, if possible. So do an extra
             * fetch.
             */
            if !tuplestore_advance(tuplestorestate, forward) {
                return null_mut(); /* the tuplestore must be empty */
            }
        }
        eof_tuplestore = false;
    }

    /*
     * If we can fetch another tuple from the tuplestore, return it.
     */
    slot = (*node).ss.ps.ps_ResultTupleSlot;
    if !eof_tuplestore {
        if tuplestore_gettupleslot(tuplestorestate, forward, false, slot) {
            return slot;
        }
        if forward {
            eof_tuplestore = true;
        }
    }

    /*
     * If necessary, try to fetch another row from the subplan.
     *
     * Note: the eof_underlying state variable exists to short-circuit further
     * subplan calls.  It's not optional, unfortunately, because some plan
     * node types are not robust about being called again when they've already
     * returned NULL.
     */
    if eof_tuplestore && !(*node).eof_underlying {
        let outerNode: *mut PlanState;
        let outerslot: *mut TupleTableSlot;

        /*
         * We can only get here with forward==true, so no need to worry about
         * which direction the subplan will go.
         */
        outerNode = outerPlanState(node as *mut PlanState);
        outerslot = ExecProcNode(outerNode);
        if TupIsNull(outerslot) {
            (*node).eof_underlying = true;
            return null_mut();
        }

        /*
         * Append a copy of the returned tuple to tuplestore.  NOTE: because
         * the tuplestore is certainly in EOF state, its read position will
         * move forward over the added tuple.  This is what we want.
         */
        if !tuplestorestate.is_null() {
            tuplestore_puttupleslot(tuplestorestate, outerslot);
        }

        ExecCopySlot(slot, outerslot);
        return slot;
    }

    /*
     * Nothing left ...
     */
    ExecClearTuple(slot)
}

/* ----------------------------------------------------------------
 *		ExecInitMaterial
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitMaterial(
    node: *mut Material,
    estate: *mut EState,
    mut eflags: c_int,
) -> *mut MaterialState {
    let matstate: *mut MaterialState;
    let outerPlan: *mut Plan;

    /*
     * create state structure
     */
    matstate = makeNode!(MaterialState, T_MaterialState);
    (*matstate).ss.ps.plan = node as *mut Plan;
    (*matstate).ss.ps.state = estate;
    (*matstate).ss.ps.ExecProcNode = Some(ExecMaterial);

    /*
     * We must have a tuplestore buffering the subplan output to do backward
     * scan or mark/restore.  We also prefer to materialize the subplan output
     * if we might be called on to rewind and replay it many times. However,
     * if none of these cases apply, we can skip storing the data.
     */
    (*matstate).eflags = eflags
        & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    /*
     * Tuplestore's interpretation of the flag bits is subtly different from
     * the general executor meaning: it doesn't think BACKWARD necessarily
     * means "backwards all the way to start".  If told to support BACKWARD we
     * must include REWIND in the tuplestore eflags, else tuplestore_trim
     * might throw away too much.
     */
    if eflags & EXEC_FLAG_BACKWARD != 0 {
        (*matstate).eflags |= EXEC_FLAG_REWIND;
    }

    (*matstate).eof_underlying = false;
    (*matstate).tuplestorestate = null_mut();

    /*
     * Miscellaneous initialization
     *
     * Materialization nodes don't need ExprContexts because they never call
     * ExecQual or ExecProject.
     */

    /*
     * initialize child nodes
     *
     * We shield the child node from the need to support REWIND, BACKWARD, or
     * MARK/RESTORE.
     */
    eflags &= !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    outerPlan = crate::nodes::plannodes::outerPlan(node as *mut Plan);
    /* outerPlanState(matstate) = ExecInitNode(...) */
    (*matstate).ss.ps.lefttree = ExecInitNode(outerPlan, estate, eflags);

    /*
     * Initialize result type and slot. No need to initialize projection info
     * because this node doesn't do projections.
     *
     * material nodes only return tuples from their materialized relation.
     */
    ExecInitResultTupleSlotTL(
        &mut (*matstate).ss.ps,
        &TTSOpsMinimalTuple,
    );
    (*matstate).ss.ps.ps_ProjInfo = null_mut();

    /*
     * initialize tuple type.
     */
    ExecCreateScanSlotFromOuterPlan(
        estate,
        &mut (*matstate).ss as *mut _ as *mut c_void,
        &TTSOpsMinimalTuple,
    );

    matstate
}

/* ----------------------------------------------------------------
 *		ExecEndMaterial
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndMaterial(node: *mut MaterialState) {
    /*
     * Release tuplestore resources
     */
    if !(*node).tuplestorestate.is_null() {
        tuplestore_end((*node).tuplestorestate);
    }
    (*node).tuplestorestate = null_mut();

    /*
     * shut down the subplan
     */
    ExecEndNode(outerPlanState(node as *mut PlanState));
}

/* ----------------------------------------------------------------
 *		ExecMaterialMarkPos
 *
 *		Calls tuplestore to save the current position in the stored file.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecMaterialMarkPos(node: *mut MaterialState) {
    Assert!((*node).eflags & EXEC_FLAG_MARK != 0);

    /*
     * if we haven't materialized yet, just return.
     */
    if (*node).tuplestorestate.is_null() {
        return;
    }

    /*
     * copy the active read pointer to the mark.
     */
    tuplestore_copy_read_pointer((*node).tuplestorestate, 0, 1);

    /*
     * since we may have advanced the mark, try to truncate the tuplestore.
     */
    tuplestore_trim((*node).tuplestorestate);
}

/* ----------------------------------------------------------------
 *		ExecMaterialRestrPos
 *
 *		Calls tuplestore to restore the last saved file position.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecMaterialRestrPos(node: *mut MaterialState) {
    Assert!((*node).eflags & EXEC_FLAG_MARK != 0);

    /*
     * if we haven't materialized yet, just return.
     */
    if (*node).tuplestorestate.is_null() {
        return;
    }

    /*
     * copy the mark to the active read pointer.
     */
    tuplestore_copy_read_pointer((*node).tuplestorestate, 1, 0);
}

/* ----------------------------------------------------------------
 *		ExecReScanMaterial
 *
 *		Rescans the materialized relation.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecReScanMaterial(node: *mut MaterialState) {
    let outerPlan: *mut PlanState = outerPlanState(node as *mut PlanState);

    ExecClearTuple((*node).ss.ps.ps_ResultTupleSlot);

    if (*node).eflags != 0 {
        /*
         * If we haven't materialized yet, just return. If outerplan's
         * chgParam is not NULL then it will be re-scanned by ExecProcNode,
         * else no reason to re-scan it at all.
         */
        if (*node).tuplestorestate.is_null() {
            return;
        }

        /*
         * If subnode is to be rescanned then we forget previous stored
         * results; we have to re-read the subplan and re-store.  Also, if we
         * told tuplestore it needn't support rescan, we lose and must
         * re-read.  (This last should not happen in common cases; else our
         * caller lied by not passing EXEC_FLAG_REWIND to us.)
         *
         * Otherwise we can just rewind and rescan the stored output. The
         * state of the subnode does not change.
         */
        if !(*outerPlan).chgParam.is_null()
            || ((*node).eflags & EXEC_FLAG_REWIND) == 0
        {
            tuplestore_end((*node).tuplestorestate);
            (*node).tuplestorestate = null_mut();
            if (*outerPlan).chgParam.is_null() {
                ExecReScan(outerPlan);
            }
            (*node).eof_underlying = false;
        } else {
            tuplestore_rescan((*node).tuplestorestate);
        }
    } else {
        /* In this case we are just passing on the subquery's output */

        /*
         * if chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (*outerPlan).chgParam.is_null() {
            ExecReScan(outerPlan);
        }
        (*node).eof_underlying = false;
    }
}

/* ----------------------------------------------------------------
 *		Local stubs for not-yet-ported tuplestore routines
 *		(src/backend/utils/sort/tuplestore.c)
 * ----------------------------------------------------------------
 */

// TODO: not ported - tuplestore_begin_heap
unsafe fn tuplestore_begin_heap(
    _randomAccess: bool,
    _interXact: bool,
    _maxKBytes: c_int,
) -> *mut Tuplestorestate {
    unimplemented!()
}

// TODO: not ported - tuplestore_set_eflags
unsafe fn tuplestore_set_eflags(_state: *mut Tuplestorestate, _eflags: c_int) {
    unimplemented!()
}

// TODO: not ported - tuplestore_alloc_read_pointer
unsafe fn tuplestore_alloc_read_pointer(
    _state: *mut Tuplestorestate,
    _eflags: c_int,
) -> c_int {
    unimplemented!()
}

// TODO: not ported - tuplestore_ateof
unsafe fn tuplestore_ateof(_state: *mut Tuplestorestate) -> bool {
    unimplemented!()
}

// TODO: not ported - tuplestore_advance
unsafe fn tuplestore_advance(
    _state: *mut Tuplestorestate,
    _forward: bool,
) -> bool {
    unimplemented!()
}

// TODO: not ported - tuplestore_gettupleslot
unsafe fn tuplestore_gettupleslot(
    _state: *mut Tuplestorestate,
    _forward: bool,
    _copy: bool,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

// TODO: not ported - tuplestore_puttupleslot
unsafe fn tuplestore_puttupleslot(
    _state: *mut Tuplestorestate,
    _slot: *mut TupleTableSlot,
) {
    unimplemented!()
}

// TODO: not ported - tuplestore_end
unsafe fn tuplestore_end(_state: *mut Tuplestorestate) {
    unimplemented!()
}

// TODO: not ported - tuplestore_copy_read_pointer
unsafe fn tuplestore_copy_read_pointer(
    _state: *mut Tuplestorestate,
    _srcptr: c_int,
    _destptr: c_int,
) {
    unimplemented!()
}

// TODO: not ported - tuplestore_trim
unsafe fn tuplestore_trim(_state: *mut Tuplestorestate) {
    unimplemented!()
}

// TODO: not ported - tuplestore_rescan
unsafe fn tuplestore_rescan(_state: *mut Tuplestorestate) {
    unimplemented!()
}
