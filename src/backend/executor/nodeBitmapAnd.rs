//! BitmapAnd node. Translated from backend/executor/nodeBitmapAnd.c (disposition:
//! full for the serial path).
//!
//! A BitmapAnd does not use lefttree/righttree; it holds a list of bitmap subplans
//! (much like Append) and intersects their result bitmaps. Like the other bitmap
//! producers it is driven via `MultiExecProcNode` (yields a `TIDBitmap`, not a
//! slot); its `ExecProcNode` arm is an error. The first child's bitmap becomes the
//! accumulator; each subsequent child's bitmap is `tbm_intersect`ed in. An empty
//! intermediate result short-circuits the remaining children.

use std::sync::Arc;

use crate::backend::executor::execProcnode::{exec_end_node, multi_exec_proc_node, PlanStateNode};
use crate::backend::nodes::tidbitmap::{tbm_free, tbm_intersect, tbm_is_empty, TIDBitmap};
use crate::nodes::execnodes::PlanState;
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::BitmapAnd;
use crate::shared_state::SharedState;

/// Run-state for a BitmapAnd: the PG `BitmapAndState` (an empty [`PlanState`]; this
/// node has no exprcontext/slots) plus the initialized child subplan states.
pub struct BitmapAndRun<'rel> {
    pub ps: PlanState,
    pub bitmapplans: Vec<PlanStateNode<'rel>>,
}

/// PG `ExecInitBitmapAnd`: init each bitmap subplan. The node needs no exprcontext
/// or tuple slots (it never calls ExecQual or ExecProject).
pub fn exec_init_bitmap_and<'rel>(
    node: &BitmapAnd,
    bitmapplans: Vec<PlanStateNode<'rel>>,
) -> Box<BitmapAndRun<'rel>> {
    let ps = PlanState {
        plan: Some(Node::BitmapAnd(Box::new(node.clone()))),
        ..PlanState::default()
    };
    Box::new(BitmapAndRun { ps, bitmapplans })
}

/// PG `MultiExecBitmapAnd`: intersect the child bitmaps. The first child's bitmap
/// is the accumulator; each subsequent child is intersected in and then freed.
pub async fn multi_exec_bitmap_and(
    shared: &Arc<SharedState>,
    run: &mut BitmapAndRun<'_>,
) -> Box<TIDBitmap> {
    let mut result: Option<Box<TIDBitmap>> = None;
    for child in &mut run.bitmapplans {
        let subresult = Box::pin(multi_exec_proc_node(shared, child, None)).await;
        match result {
            None => result = Some(subresult),
            Some(ref mut acc) => {
                tbm_intersect(acc, &subresult);
                tbm_free(subresult);
            }
        }
        // A completely empty intermediate result can't change under more ANDs.
        if result.as_deref().is_some_and(tbm_is_empty) {
            break;
        }
    }
    result.unwrap_or_else(|| unimplemented!("BitmapAnd doesn't support zero inputs"))
}

/// PG `ExecEndBitmapAnd`: shut down each child subplan.
pub fn exec_end_bitmap_and(shared: Option<&Arc<SharedState>>, run: &mut BitmapAndRun<'_>) {
    for child in &mut run.bitmapplans {
        exec_end_node(shared, child);
    }
}
