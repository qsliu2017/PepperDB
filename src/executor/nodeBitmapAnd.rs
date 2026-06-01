//! nodeBitmapAnd.c - routines to handle BitmapAnd nodes.
//
// BitmapAnd nodes don't make use of their left and right subtrees, rather they
// maintain a list of subplans, much like Append nodes.  The logic is much
// simpler than Append, however, since we needn't cope with forward/backward
// execution.
//
// INTERFACE ROUTINES
//   ExecInitBitmapAnd  - initialize the BitmapAnd node
//   MultiExecBitmapAnd - retrieve the result bitmap from the node
//   ExecEndBitmapAnd   - shut down the BitmapAnd node
//   ExecReScanBitmapAnd - rescan the BitmapAnd node

use crate::prelude::*;

use crate::executor::executor::{
    ExecEndNode, ExecInitNode, ExecReScan, MultiExecProcNode, UpdateChangedParamSet,
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::instrument::{InstrStartNode, InstrStopNode};
use crate::nodes::execnodes::{BitmapAndState, EState, PlanState, TupleTableSlot};
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::pg_list::{lfirst, list_length};
use crate::nodes::plannodes::{BitmapAnd, Plan};
use crate::nodes::tidbitmap::{tbm_free, tbm_intersect, tbm_is_empty, TIDBitmap};
use crate::{current_cell, foreach, makeNode, Assert};

/* ----------------------------------------------------------------
 *		ExecBitmapAnd
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
unsafe fn ExecBitmapAnd(_pstate: *mut PlanState) -> *mut TupleTableSlot {
    elog!(
        ERROR,
        "BitmapAnd node does not support ExecProcNode call convention"
    );
    #[allow(unreachable_code)]
    null_mut()
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapAnd
 *
 *		Begin all of the subscans of the BitmapAnd node.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitBitmapAnd(
    node: *mut BitmapAnd,
    estate: *mut EState,
    eflags: c_int,
) -> *mut BitmapAndState {
    let bitmapandstate: *mut BitmapAndState = makeNode!(BitmapAndState, T_BitmapAndState);
    let bitmapplanstates: *mut *mut PlanState;
    let nplans: c_int;
    let mut i: c_int;

    /* check for unsupported flags */
    Assert!((eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)) == 0);

    /*
     * Set up empty vector of subplan states
     */
    nplans = list_length((*node).bitmapplans);

    bitmapplanstates =
        palloc0(nplans as usize * core::mem::size_of::<*mut PlanState>()) as *mut *mut PlanState;

    /*
     * create new BitmapAndState for our BitmapAnd node
     */
    (*bitmapandstate).ps.plan = node as *mut Plan;
    (*bitmapandstate).ps.state = estate;
    (*bitmapandstate).ps.ExecProcNode = Some(ExecBitmapAnd);
    (*bitmapandstate).bitmapplans = bitmapplanstates;
    (*bitmapandstate).nplans = nplans;

    /*
     * call ExecInitNode on each of the plans to be executed and save the
     * results into the array "bitmapplanstates".
     */
    i = 0;
    foreach!(l, (*node).bitmapplans, {
        let initNode = lfirst(current_cell!(l)) as *mut Plan;
        *bitmapplanstates.add(i as usize) = ExecInitNode(initNode, estate, eflags);
        i += 1;
    });

    /*
     * Miscellaneous initialization
     *
     * BitmapAnd plans don't have expression contexts because they never call
     * ExecQual or ExecProject.  They don't need any tuple slots either.
     */

    bitmapandstate
}

/* ----------------------------------------------------------------
 *	   MultiExecBitmapAnd
 * ----------------------------------------------------------------
 */
pub unsafe fn MultiExecBitmapAnd(node: *mut BitmapAndState) -> *mut Node {
    let bitmapplans: *mut *mut PlanState;
    let nplans: c_int;
    let mut i: c_int;
    let mut result: *mut TIDBitmap = null_mut();

    /* must provide our own instrumentation support */
    if !(*node).ps.instrument.is_null() {
        InstrStartNode((*node).ps.instrument);
    }

    /*
     * get information from the node
     */
    bitmapplans = (*node).bitmapplans;
    nplans = (*node).nplans;

    /*
     * Scan all the subplans and AND their result bitmaps
     */
    i = 0;
    while i < nplans {
        let subnode: *mut PlanState = *bitmapplans.add(i as usize);
        let subresult: *mut TIDBitmap;

        subresult = MultiExecProcNode(subnode) as *mut TIDBitmap;

        // C: !IsA(subresult, TIDBitmap).  TODO(pg-port): there is no T_TIDBitmap
        // in the ported NodeTag enum yet; tidbitmap.rs leaves the tag as
        // T_Invalid (see its module note).  Match that placeholder here.
        if subresult.is_null() || nodeTag(subresult) != NodeTag::T_Invalid {
            elog!(ERROR, "unrecognized result from subplan");
        }

        if result.is_null() {
            result = subresult; /* first subplan */
        } else {
            tbm_intersect(result, subresult);
            tbm_free(subresult);
        }

        /*
         * If at any stage we have a completely empty bitmap, we can fall out
         * without evaluating the remaining subplans, since ANDing them can no
         * longer change the result.  (Note: the fact that indxpath.c orders
         * the subplans by selectivity should make this case more likely to
         * occur.)
         */
        if tbm_is_empty(result) {
            break;
        }

        i += 1;
    }

    if result.is_null() {
        elog!(ERROR, "BitmapAnd doesn't support zero inputs");
    }

    /* must provide our own instrumentation support */
    if !(*node).ps.instrument.is_null() {
        InstrStopNode((*node).ps.instrument, 0.0 /* XXX */);
    }

    result as *mut Node
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapAnd
 *
 *		Shuts down the subscans of the BitmapAnd node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndBitmapAnd(node: *mut BitmapAndState) {
    let bitmapplans: *mut *mut PlanState;
    let nplans: c_int;
    let mut i: c_int;

    /*
     * get information from the node
     */
    bitmapplans = (*node).bitmapplans;
    nplans = (*node).nplans;

    /*
     * shut down each of the subscans (that we've initialized)
     */
    i = 0;
    while i < nplans {
        if !(*bitmapplans.add(i as usize)).is_null() {
            ExecEndNode(*bitmapplans.add(i as usize));
        }
        i += 1;
    }
}

pub unsafe fn ExecReScanBitmapAnd(node: *mut BitmapAndState) {
    let mut i: c_int;

    i = 0;
    while i < (*node).nplans {
        let subnode: *mut PlanState = *(*node).bitmapplans.add(i as usize);

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
}
