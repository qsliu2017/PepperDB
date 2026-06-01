//! nodeBitmapOr.c - routines to handle BitmapOr nodes.
//
// BitmapOr nodes don't make use of their left and right subtrees, rather they
// maintain a list of subplans, much like Append nodes.  The logic is much
// simpler than Append, however, since we needn't cope with forward/backward
// execution.
//
// INTERFACE ROUTINES
//   ExecInitBitmapOr   - initialize the BitmapOr node
//   MultiExecBitmapOr  - retrieve the result bitmap from the node
//   ExecEndBitmapOr    - shut down the BitmapOr node
//   ExecReScanBitmapOr - rescan the BitmapOr node

use crate::prelude::*;

use crate::executor::executor::{
    ExecEndNode, ExecInitNode, ExecReScan, MultiExecProcNode, UpdateChangedParamSet,
    EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK,
};
use crate::executor::instrument::{InstrStartNode, InstrStopNode};
use crate::miscadmin::work_mem;
use crate::nodes::execnodes::{
    BitmapIndexScanState, BitmapOrState, EState, PlanState, TupleTableSlot,
};
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::pg_list::{lfirst, list_length};
use crate::nodes::plannodes::{BitmapOr, Plan};
use crate::nodes::tidbitmap::{tbm_create, tbm_free, tbm_union, TIDBitmap};
use crate::{castNode, current_cell, foreach, makeNode, Assert, IsA};

/* ----------------------------------------------------------------
 *		ExecBitmapOr
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
unsafe fn ExecBitmapOr(_pstate: *mut PlanState) -> *mut TupleTableSlot {
    elog!(
        ERROR,
        "BitmapOr node does not support ExecProcNode call convention"
    );
    #[allow(unreachable_code)]
    null_mut()
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapOr
 *
 *		Begin all of the subscans of the BitmapOr node.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecInitBitmapOr(
    node: *mut BitmapOr,
    estate: *mut EState,
    eflags: c_int,
) -> *mut BitmapOrState {
    let bitmaporstate: *mut BitmapOrState = makeNode!(BitmapOrState, T_BitmapOrState);
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
     * create new BitmapOrState for our BitmapOr node
     */
    (*bitmaporstate).ps.plan = node as *mut Plan;
    (*bitmaporstate).ps.state = estate;
    (*bitmaporstate).ps.ExecProcNode = Some(ExecBitmapOr);
    (*bitmaporstate).bitmapplans = bitmapplanstates;
    (*bitmaporstate).nplans = nplans;

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
     * BitmapOr plans don't have expression contexts because they never call
     * ExecQual or ExecProject.  They don't need any tuple slots either.
     */

    bitmaporstate
}

/* ----------------------------------------------------------------
 *	   MultiExecBitmapOr
 * ----------------------------------------------------------------
 */
pub unsafe fn MultiExecBitmapOr(node: *mut BitmapOrState) -> *mut Node {
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
     * Scan all the subplans and OR their result bitmaps
     */
    i = 0;
    while i < nplans {
        let subnode: *mut PlanState = *bitmapplans.add(i as usize);
        let subresult: *mut TIDBitmap;

        /*
         * We can special-case BitmapIndexScan children to avoid an explicit
         * tbm_union step for each child: just pass down the current result
         * bitmap and let the child OR directly into it.
         */
        if IsA!(subnode, T_BitmapIndexScanState) {
            if result.is_null() {
                /* first subplan */
                /* XXX should we use less than work_mem for this? */
                result = tbm_create(
                    work_mem as Size * 1024 as Size,
                    if (*(castNode!(BitmapOr, T_BitmapOr, (*node).ps.plan))).isshared {
                        (*(*node).ps.state).es_query_dsa as *mut _
                    } else {
                        null_mut()
                    },
                );
            }

            (*(subnode as *mut BitmapIndexScanState)).biss_result = result as *mut _;

            subresult = MultiExecProcNode(subnode) as *mut TIDBitmap;

            if subresult != result {
                elog!(ERROR, "unrecognized result from subplan");
            }
        } else {
            /* standard implementation */
            subresult = MultiExecProcNode(subnode) as *mut TIDBitmap;

            // C: !IsA(subresult, TIDBitmap).  TODO(pg-port): there is no
            // T_TIDBitmap in the ported NodeTag enum yet; tidbitmap.rs leaves
            // the tag as T_Invalid (see its module note).  Match that here.
            if subresult.is_null() || nodeTag(subresult) != NodeTag::T_Invalid {
                elog!(ERROR, "unrecognized result from subplan");
            }

            if result.is_null() {
                result = subresult; /* first subplan */
            } else {
                tbm_union(result, subresult);
                tbm_free(subresult);
            }
        }

        i += 1;
    }

    /* We could return an empty result set here? */
    if result.is_null() {
        elog!(ERROR, "BitmapOr doesn't support zero inputs");
    }

    /* must provide our own instrumentation support */
    if !(*node).ps.instrument.is_null() {
        InstrStopNode((*node).ps.instrument, 0.0 /* XXX */);
    }

    result as *mut Node
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapOr
 *
 *		Shuts down the subscans of the BitmapOr node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
pub unsafe fn ExecEndBitmapOr(node: *mut BitmapOrState) {
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

pub unsafe fn ExecReScanBitmapOr(node: *mut BitmapOrState) {
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
