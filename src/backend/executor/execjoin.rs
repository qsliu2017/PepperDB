//! Shared join-node machinery used by nodeNestloop / nodeHashjoin / nodeMergejoin.
//!
//! A join node evaluates its joinqual + (optional) otherqual against
//! `ecxt_outertuple`/`ecxt_innertuple` and projects the join targetlist (whose
//! Vars are OUTER_VAR/INNER_VAR after setrefs) into an owned result slot. This
//! module owns the projection + qual run helpers so each join node shares one
//! implementation (the C `ExecQual`/`ExecProject` over a JoinState).

use crate::backend::executor::execExpr::exec_qual;
use crate::backend::executor::execTuples::exec_store_virtual_tuple;
use crate::executor::tuptable::ExecClearTuple;
use crate::nodes::execnodes::{ExprContext, ExprState, ProjectionInfo, TupleTableSlot};

/// The owned projection + qual state a join node carries (PG keeps these in the
/// JoinState's PlanState; here they ride the node's run-state). `outer_slot`/
/// `inner_slot` are the owned virtual slots the node fills per join pair and points
/// `ecxt_outertuple`/`ecxt_innertuple` at while the joinqual + projection run.
pub struct JoinProj {
    pub projection: Box<ProjectionInfo>,
    /// the join node's own qual (qpqual): all non-join clauses (M7 inner joins
    /// carry none, so this is usually `None`).
    pub otherqual: Option<Box<ExprState>>,
    pub outer_slot: Box<TupleTableSlot>,
    pub inner_slot: Box<TupleTableSlot>,
}

/// PG `ExecQual` over a join qual: `None` (empty) is always-true. The econtext's
/// `ecxt_outertuple`/`ecxt_innertuple` must already point at the current join pair.
pub fn run_join_qual(qual: Option<&mut ExprState>, econtext: &mut ExprContext) -> bool {
    exec_qual(qual, econtext)
}

/// PG `ExecProject` over a join projection: clear the result slot, run the
/// projection interpreter (it deposits each target via EEOP_ASSIGN_OUTER/INNER_VAR
/// / ASSIGN_TMP reading the econtext join slots), store the virtual tuple, and
/// return a borrow of the result slot.
pub fn project_join<'p>(
    proj: &'p mut ProjectionInfo,
    econtext: &mut ExprContext,
) -> Option<&'p mut TupleTableSlot> {
    {
        let slot = proj
            .state
            .resultslot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("project_join: projection has no result slot"));
        ExecClearTuple(slot);
    }
    let evalfunc = proj
        .state
        .evalfunc
        .unwrap_or_else(|| unimplemented!("project_join: projection not ready"));
    let mut is_null = false;
    let _ = evalfunc(&mut proj.state, econtext, &mut is_null);
    let slot = proj
        .state
        .resultslot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("project_join: projection lost its result slot"));
    exec_store_virtual_tuple(slot);
    proj.state.resultslot.as_deref_mut()
}
