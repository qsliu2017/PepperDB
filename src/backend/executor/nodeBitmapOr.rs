//! BitmapOr node. Translated from backend/executor/nodeBitmapOr.c (disposition:
//! full for the serial path).
//!
//! A BitmapOr holds a list of bitmap subplans (like Append) and unions their result
//! bitmaps. Driven via `MultiExecProcNode` (yields a `TIDBitmap`); its
//! `ExecProcNode` arm is an error. PG special-cases a BitmapIndexScan child: it
//! pre-stashes the accumulator bitmap into the child so the child ORs directly into
//! it, avoiding an explicit `tbm_union`. We reproduce that by passing the
//! accumulator down to a BitmapIndexScan child as its result bitmap; other child
//! kinds union normally.

use std::sync::Arc;

use crate::backend::executor::execProcnode::{exec_end_node, multi_exec_proc_node, PlanStateNode};
use crate::backend::executor::nodeBitmapIndexscan::multi_exec_bitmap_index_scan;
use crate::backend::nodes::tidbitmap::{tbm_create, tbm_free, tbm_union, TIDBitmap};
use crate::nodes::execnodes::PlanState;
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::BitmapOr;
use crate::shared_state::SharedState;

/// Run-state for a BitmapOr: the PG `BitmapOrState` (an empty [`PlanState`]) plus
/// the initialized child subplan states.
pub struct BitmapOrRun<'rel> {
    pub ps: PlanState,
    pub bitmapplans: Vec<PlanStateNode<'rel>>,
}

/// PG `ExecInitBitmapOr`: init each bitmap subplan. No exprcontext or tuple slots.
pub fn exec_init_bitmap_or<'rel>(
    node: &BitmapOr,
    bitmapplans: Vec<PlanStateNode<'rel>>,
) -> Box<BitmapOrRun<'rel>> {
    let ps = PlanState {
        plan: Some(Node::BitmapOr(Box::new(node.clone()))),
        ..PlanState::default()
    };
    Box::new(BitmapOrRun { ps, bitmapplans })
}

/// PG `MultiExecBitmapOr`: union the child bitmaps. A BitmapIndexScan child ORs
/// directly into the accumulator (no explicit union); other children union in.
pub async fn multi_exec_bitmap_or(
    shared: &Arc<SharedState>,
    run: &mut BitmapOrRun<'_>,
) -> Box<TIDBitmap> {
    let mut result: Option<Box<TIDBitmap>> = None;
    for child in &mut run.bitmapplans {
        if let PlanStateNode::BitmapIndexScan(child_run) = child {
            // First child: create the accumulator; thereafter reuse it. The child
            // ORs its TIDs straight into the bitmap we hand it.
            let acc = result.take().unwrap_or_else(|| tbm_create(4 * 1024 * 1024));
            result = Some(multi_exec_bitmap_index_scan(shared, child_run, Some(acc)).await);
        } else {
            let subresult = Box::pin(multi_exec_proc_node(shared, child, None)).await;
            match result {
                None => result = Some(subresult),
                Some(ref mut acc) => {
                    tbm_union(acc, &subresult);
                    tbm_free(subresult);
                }
            }
        }
    }
    result.unwrap_or_else(|| unimplemented!("BitmapOr doesn't support zero inputs"))
}

/// PG `ExecEndBitmapOr`: shut down each child subplan.
pub fn exec_end_bitmap_or(shared: Option<&Arc<SharedState>>, run: &mut BitmapOrRun<'_>) {
    for child in &mut run.bitmapplans {
        exec_end_node(shared, child);
    }
}
