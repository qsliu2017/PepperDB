//! executor/execScan.c - generalized relation scan support (ExecScan et al.)

use crate::prelude::*;

use crate::executor::executor::{
    EvalPlanQualFetchRowMark, ExecConditionalAssignProjectionInfo, ExecProject, ExecQual,
    ExecScanAccessMtd, ExecScanRecheckMtd, ResetExprContext,
};
use crate::executor::tuptable::{TupIsNull, TupleTableSlot};
use crate::executor::tuptable::ExecClearTuple;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::nodes::bitmapset::{bms_is_member, bms_next_member, Bitmapset};
use crate::nodes::execnodes::{
    EPQState, ExprContext, ExprState, ProjectionInfo, ScanState,
};
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::nodes::plannodes::{CustomScan, ForeignScan, Plan, Scan};

/*
 * Macros for inline access to certain instrumentation counters.
 *
 *   InstrCountFiltered1(node, delta) -- accumulate `delta` into the node's
 *   per-tuple "filtered by scanqual/joinqual" counter, but only when
 *   instrumentation is active for the node.
 *
 * The InstrCountFiltered1 C macro (execnodes.h) is not yet provided centrally
 * (deferred until the Instrumentation layout landed); the Instrumentation
 * struct now has the real `nfiltered1` field, so we provide the equivalent
 * here, faithful to:
 *
 *   #define InstrCountFiltered1(node, delta) \
 *       do { \
 *           if (((PlanState *)(node))->instrument) \
 *               ((PlanState *)(node))->instrument->nfiltered1 += (delta); \
 *       } while(0)
 */
#[inline]
unsafe fn InstrCountFiltered1(node: *mut ScanState, delta: f64) {
    let instrument = (*node).ps.instrument;
    if !instrument.is_null() {
        (*instrument).nfiltered1 += delta;
    }
}

/* ----------------------------------------------------------------
 * ExecScanFetch -- check interrupts & fetch next potential tuple
 *
 * This routine substitutes a test tuple if inside an EvalPlanQual recheck.
 * Otherwise, it simply executes the access method's next-tuple routine.
 *
 * (static pg_attribute_always_inline in executor/execScan.h, pulled in by
 * execScan.c via #include "executor/execScan.h".)
 * ----------------------------------------------------------------
 */
#[inline]
pub unsafe fn ExecScanFetch(
    node: *mut ScanState,
    epqstate: *mut EPQState,
    accessMtd: ExecScanAccessMtd,
    recheckMtd: ExecScanRecheckMtd,
) -> *mut TupleTableSlot {
    CHECK_FOR_INTERRUPTS();

    if !epqstate.is_null() {
        /*
         * We are inside an EvalPlanQual recheck.  Return the test tuple if
         * one is available, after rechecking any access-method-specific
         * conditions.
         */
        let scanrelid: Index = (*((*node).ps.plan as *mut Scan)).scanrelid;

        if scanrelid == 0 {
            /*
             * This is a ForeignScan or CustomScan which has pushed down a
             * join to the remote side.  If it is a descendant node in the EPQ
             * recheck plan tree, run the recheck method function.  Otherwise,
             * run the access method function below.
             */
            if bms_is_member((*epqstate).epqParam, (*(*node).ps.plan).extParam) {
                /*
                 * The recheck method is responsible not only for rechecking
                 * the scan/join quals but also for storing the correct tuple
                 * in the slot.
                 */
                let slot: *mut TupleTableSlot = (*node).ss_ScanTupleSlot;

                if !(recheckMtd.expect("recheckMtd"))(node, slot) {
                    ExecClearTuple(slot); /* would not be returned by scan */
                }
                return slot;
            }
        } else if *(*epqstate).relsubs_done.add((scanrelid - 1) as usize) {
            /*
             * Return empty slot, as either there is no EPQ tuple for this rel
             * or we already returned it.
             */
            let slot: *mut TupleTableSlot = (*node).ss_ScanTupleSlot;

            return ExecClearTuple(slot);
        } else if !(*(*epqstate).relsubs_slot.add((scanrelid - 1) as usize)).is_null() {
            /*
             * Return replacement tuple provided by the EPQ caller.
             */
            let slot: *mut TupleTableSlot = *(*epqstate).relsubs_slot.add((scanrelid - 1) as usize);

            Assert!((*(*epqstate).relsubs_rowmark.add((scanrelid - 1) as usize)).is_null());

            /* Mark to remember that we shouldn't return it again */
            *(*epqstate).relsubs_done.add((scanrelid - 1) as usize) = true;

            /* Return empty slot if we haven't got a test tuple */
            if TupIsNull(slot) {
                return null_mut();
            }

            /* Check if it meets the access-method conditions */
            if !(recheckMtd.expect("recheckMtd"))(node, slot) {
                return ExecClearTuple(slot); /* would not be returned by scan */
            }
            return slot;
        } else if !(*(*epqstate).relsubs_rowmark.add((scanrelid - 1) as usize)).is_null() {
            /*
             * Fetch and return replacement tuple using a non-locking rowmark.
             */
            let slot: *mut TupleTableSlot = (*node).ss_ScanTupleSlot;

            /* Mark to remember that we shouldn't return more */
            *(*epqstate).relsubs_done.add((scanrelid - 1) as usize) = true;

            if !EvalPlanQualFetchRowMark(epqstate, scanrelid as u32, slot) {
                return null_mut();
            }

            /* Return empty slot if we haven't got a test tuple */
            if TupIsNull(slot) {
                return null_mut();
            }

            /* Check if it meets the access-method conditions */
            if !(recheckMtd.expect("recheckMtd"))(node, slot) {
                return ExecClearTuple(slot); /* would not be returned by scan */
            }
            return slot;
        }
    }

    /*
     * Run the node-type-specific access method function to get the next tuple
     */
    (accessMtd.expect("accessMtd"))(node)
}

/* ----------------------------------------------------------------
 * ExecScanExtended
 *		Scans the relation using the specified 'access method' and returns the
 *		next tuple.  Optionally checks the tuple against 'qual' and applies
 *		'projInfo' if provided.
 *
 * (static pg_attribute_always_inline in executor/execScan.h.)
 * ----------------------------------------------------------------
 */
#[inline]
pub unsafe fn ExecScanExtended(
    node: *mut ScanState,
    accessMtd: ExecScanAccessMtd,
    recheckMtd: ExecScanRecheckMtd,
    epqstate: *mut EPQState,
    qual: *mut ExprState,
    projInfo: *mut ProjectionInfo,
) -> *mut TupleTableSlot {
    let econtext: *mut ExprContext = (*node).ps.ps_ExprContext;

    /* interrupt checks are in ExecScanFetch */

    /*
     * If we have neither a qual to check nor a projection to do, just skip
     * all the overhead and return the raw scan tuple.
     */
    if qual.is_null() && projInfo.is_null() {
        ResetExprContext(econtext);
        return ExecScanFetch(node, epqstate, accessMtd, recheckMtd);
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(econtext);

    /*
     * get a tuple from the access method.  Loop until we obtain a tuple that
     * passes the qualification.
     */
    loop {
        let slot: *mut TupleTableSlot = ExecScanFetch(node, epqstate, accessMtd, recheckMtd);

        /*
         * if the slot returned by the accessMtd contains NULL, then it means
         * there is nothing more to scan so we just return an empty slot,
         * being careful to use the projection result slot so it has correct
         * tupleDesc.
         */
        if TupIsNull(slot) {
            if !projInfo.is_null() {
                return ExecClearTuple((*projInfo).pi_state.resultslot);
            } else {
                return slot;
            }
        }

        /*
         * place the current tuple into the expr context
         */
        (*econtext).ecxt_scantuple = slot;

        /*
         * check that the current tuple satisfies the qual-clause
         *
         * check for non-null qual here to avoid a function call to ExecQual()
         * when the qual is null ... saves only a few cycles, but they add up
         * ...
         */
        if qual.is_null() || ExecQual(qual, econtext) {
            /*
             * Found a satisfactory scan tuple.
             */
            if !projInfo.is_null() {
                /*
                 * Form a projection tuple, store it in the result tuple slot
                 * and return it.
                 */
                return ExecProject(projInfo);
            } else {
                /*
                 * Here, we aren't projecting, so just return scan tuple.
                 */
                return slot;
            }
        } else {
            InstrCountFiltered1(node, 1.0);
        }

        /*
         * Tuple fails qual, so free per-tuple memory and try again.
         */
        ResetExprContext(econtext);
    }
}

/* ----------------------------------------------------------------
 *		ExecScan
 *
 *		Scans the relation using the 'access method' indicated and
 *		returns the next qualifying tuple.
 *		The access method returns the next tuple and ExecScan() is
 *		responsible for checking the tuple returned against the qual-clause.
 *
 *		A 'recheck method' must also be provided that can check an
 *		arbitrary tuple of the relation against any qual conditions
 *		that are implemented internal to the access method.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecScan(
    node: *mut ScanState,
    accessMtd: ExecScanAccessMtd, /* function returning a tuple */
    recheckMtd: ExecScanRecheckMtd,
) -> *mut TupleTableSlot {
    let epqstate: *mut EPQState = (*(*node).ps.state).es_epq_active;
    let qual: *mut ExprState = (*node).ps.qual;
    let projInfo: *mut ProjectionInfo = (*node).ps.ps_ProjInfo;

    ExecScanExtended(node, accessMtd, recheckMtd, epqstate, qual, projInfo)
}

/*
 * ExecAssignScanProjectionInfo
 *		Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * The scan slot's descriptor must have been set already.
 */
pub unsafe fn ExecAssignScanProjectionInfo(node: *mut ScanState) {
    let scan: *mut Scan = (*node).ps.plan as *mut Scan;
    let tupdesc = (*(*node).ss_ScanTupleSlot).tts_tupleDescriptor;

    ExecConditionalAssignProjectionInfo(&mut (*node).ps, tupdesc, (*scan).scanrelid as c_int);
}

/*
 * ExecAssignScanProjectionInfoWithVarno
 *		As above, but caller can specify varno expected in Vars in the tlist.
 */
pub unsafe fn ExecAssignScanProjectionInfoWithVarno(node: *mut ScanState, varno: c_int) {
    let tupdesc = (*(*node).ss_ScanTupleSlot).tts_tupleDescriptor;

    ExecConditionalAssignProjectionInfo(&mut (*node).ps, tupdesc, varno);
}

/*
 * ExecScanReScan
 *
 * This must be called within the ReScan function of any plan node type
 * that uses ExecScan().
 */
pub unsafe fn ExecScanReScan(node: *mut ScanState) {
    let estate = (*node).ps.state;

    /*
     * We must clear the scan tuple so that observers (e.g., execCurrent.c)
     * can tell that this plan node is not positioned on a tuple.
     */
    ExecClearTuple((*node).ss_ScanTupleSlot);

    /*
     * Rescan EvalPlanQual tuple(s) if we're inside an EvalPlanQual recheck.
     * But don't lose the "blocked" status of blocked target relations.
     */
    if !(*estate).es_epq_active.is_null() {
        let epqstate: *mut EPQState = (*estate).es_epq_active;
        let scanrelid: Index = (*((*node).ps.plan as *mut Scan)).scanrelid;

        if scanrelid > 0 {
            *(*epqstate).relsubs_done.add((scanrelid - 1) as usize) =
                *(*epqstate).relsubs_blocked.add((scanrelid - 1) as usize);
        } else {
            let relids: *mut Bitmapset;
            let mut rtindex: c_int = -1;

            /*
             * If an FDW or custom scan provider has replaced the join with a
             * scan, there are multiple RTIs; reset the relsubs_done flag for
             * all of them.
             */
            if nodeTag((*node).ps.plan) == NodeTag::T_ForeignScan {
                relids = (*((*node).ps.plan as *mut ForeignScan)).fs_base_relids;
            } else if nodeTag((*node).ps.plan) == NodeTag::T_CustomScan {
                relids = (*((*node).ps.plan as *mut CustomScan)).custom_relids;
            } else {
                elog!(
                    ERROR,
                    "unexpected scan node: {}",
                    nodeTag((*node).ps.plan) as c_int
                );
                unreachable!()
            }

            loop {
                rtindex = bms_next_member(relids, rtindex);
                if rtindex < 0 {
                    break;
                }
                Assert!(rtindex > 0);
                *(*epqstate).relsubs_done.add((rtindex - 1) as usize) =
                    *(*epqstate).relsubs_blocked.add((rtindex - 1) as usize);
            }
        }
    }
}
