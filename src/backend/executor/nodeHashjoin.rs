//! Hash-join node. Translated from backend/executor/nodeHashjoin.c
//! (disposition: full for the M7 single-batch INNER hashjoin; the multi-batch
//! state machine, outer-join unmatched emission, and parallel hashjoin are clean
//! grow guards).
//!
//! `ExecInitHashJoin` initializes the outer (lefttree) child and the Hash inner
//! (righttree) child, compiles the outer hashkey + the hashclauses (re-checked per
//! probe) + the joinqual, and builds the result projection. `ExecHashJoin` is the
//! hybrid state machine collapsed to the single-batch case:
//!  1. BUILD: `MultiExecHash` drains the Hash child into the in-memory table.
//!  2. PROBE: for each outer tuple, compute its hash value, look up the bucket, and
//!     for every inner row in that bucket set ecxt_outer/innertuple and re-check the
//!     hashclauses + joinqual; emit the projected join row for each match.
//!
//! Slot ownership / Send: the table (owned by the Hash child) holds the inner rows;
//! the hashjoin owns the outer/inner join slots + result projection. `ecxt_outer/
//! innertuple` point at the owned join slots while the clauses + projection run via
//! EEOP_OUTER_VAR/INNER_VAR. No lock/RefCell across the child `.await`.

use std::sync::Arc;

use crate::backend::executor::execExpr::{exec_build_projection_info, exec_init_expr, exec_init_qual};
use crate::backend::executor::execProcnode::{exec_end_node, exec_proc_node, result_type_of, PlanStateNode};
use crate::backend::executor::execTuples::{exec_store_virtual_tuple, exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::backend::executor::nodeHash::{compute_hash, multi_exec_hash};
use crate::backend::executor::nodeUnique::snapshot_slot;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, ExprContext, ExprState, HashJoinState, JoinState, PlanState, ProjectionInfo};
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::plannodes::HashJoin;
use crate::shared_state::SharedState;

use super::execjoin::{project_join, run_join_qual, JoinProj};

/// Run-state pairing the PG `HashJoinState` with its outer child + Hash inner child,
/// the compiled outer hashkey + hashclauses + joinqual, the result projection, and
/// the probe cursor.
pub struct HashJoinRun<'rel> {
    pub state: Box<HashJoinState>,
    pub outer: Box<PlanStateNode<'rel>>,
    /// the inner (build) side: a `PlanStateNode::Hash`.
    pub inner: Box<PlanStateNode<'rel>>,
    pub econtext: Box<ExprContext>,
    pub proj: JoinProj,
    /// the hashclauses (re-checked per probe -- a hash collision yields a bucket
    /// hit whose clause must still be verified) + the joinqual.
    pub hashclauses: Option<Box<ExprState>>,
    /// the outer-side hash key expressions (read the outer tuple via OUTER_VAR).
    pub outer_hashkeys: Vec<Box<ExprState>>,
    /// whether the inner hash table has been built.
    pub built: bool,
    /// the current outer tuple needs a new fetch (probe cursor exhausted).
    pub need_new_outer: bool,
    /// the bucket row indexes matching the current outer tuple, and the cursor.
    pub probe_matches: Vec<usize>,
    pub probe_cursor: usize,
}

/// PG `ExecInitHashJoin`: build the HashJoinState over initialized children.
pub fn exec_init_hash_join<'rel>(
    node: &HashJoin,
    estate: &mut EState<'rel>,
    eflags: i32,
    outer: PlanStateNode<'rel>,
    inner: PlanStateNode<'rel>,
) -> Box<HashJoinRun<'rel>> {
    let _ = (estate, eflags);
    crate::assert!(
        node.join.jointype == JoinType::INNER,
        "ExecHashJoin: only INNER join emit is reachable this milestone"
    );
    crate::assert!(
        matches!(&inner, PlanStateNode::Hash(_)),
        "ExecInitHashJoin: inner child must be a Hash node"
    );

    let outer_desc = result_type_of(&outer)
        .unwrap_or_else(|| unimplemented!("ExecInitHashJoin: outer child has no result descriptor"));
    // The inner (Hash) child has no result rowtype; its build rows are the Hash
    // child's rowtype. Use the hashjoin result tlist for the inner slot width by
    // sizing it generously from the result desc (the inner snapshot arrays carry
    // their own length). We size the inner slot from the Hash child's rowtype.
    let inner_desc = hash_child_desc(&inner)
        .unwrap_or_else(|| unimplemented!("ExecInitHashJoin: inner Hash child has no rowtype"));

    let result_desc = exec_type_from_tl(&node.join.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);
    let projection: Box<ProjectionInfo> = {
        let mut transient = ExprContext::default();
        exec_build_projection_info(&node.join.plan.targetlist, &mut transient, result_slot, None, None)
    };

    let hashclauses = exec_init_qual(&node.hashclauses, None);
    let joinqual = exec_init_qual(&node.join.joinqual, None);
    let otherqual = exec_init_qual(&node.join.plan.qual, None);

    let outer_hashkeys: Vec<Box<ExprState>> = node
        .hashkeys
        .iter()
        .map(|k| exec_init_expr(Some(k), None).unwrap_or_else(|| unimplemented!("ExecInitHashJoin: empty outer hashkey")))
        .collect();

    let mut ps = PlanState {
        plan: Some(Node::HashJoin(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_result_tuple_desc = Some(result_desc);

    let js = JoinState {
        ps,
        jointype: Some(node.join.jointype),
        single_match: node.join.inner_unique || node.join.jointype == JoinType::SEMI,
        joinqual,
    };

    Box::new(HashJoinRun {
        state: Box::new(HashJoinState {
            js,
            ..HashJoinState::default()
        }),
        outer: Box::new(outer),
        inner: Box::new(inner),
        econtext: Box::new(ExprContext::default()),
        proj: JoinProj {
            projection,
            otherqual,
            outer_slot: make_tuple_table_slot(Some(outer_desc), &TTS_OPS_VIRTUAL),
            inner_slot: make_tuple_table_slot(Some(inner_desc), &TTS_OPS_VIRTUAL),
        },
        hashclauses,
        outer_hashkeys,
        built: false,
        need_new_outer: true,
        probe_matches: Vec::new(),
        probe_cursor: 0,
    })
}

/// PG `ExecHashJoin`: build (once) then probe. Returns the next qualifying join
/// tuple's projected slot, or `None` at end of join.
pub async fn exec_hash_join<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut HashJoinRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    // BUILD: drive the Hash child to build the in-memory table.
    if !run.built {
        if let PlanStateNode::Hash(h) = run.inner.as_mut() {
            Box::pin(multi_exec_hash(shared, h)).await;
        } else {
            unimplemented!("ExecHashJoin: inner child is not a Hash node");
        }
        run.built = true;
    }

    loop {
        // Fetch a new outer tuple and compute its bucket of candidate inner rows.
        if run.need_new_outer {
            let snapped = match Box::pin(exec_proc_node(shared, &mut run.outer)).await {
                None => return None,
                Some(slot) => snapshot_slot(slot),
            };
            store_into(&mut run.proj.outer_slot, &snapped.0, &snapped.1);

            run.econtext.ecxt_outertuple = Some(std::mem::replace(
                &mut run.proj.outer_slot,
                make_tuple_table_slot(None, &TTS_OPS_VIRTUAL),
            ));
            let hashval = compute_hash(&mut run.outer_hashkeys, &mut run.econtext);
            if let Some(s) = run.econtext.ecxt_outertuple.take() {
                run.proj.outer_slot = s;
            }

            // NULL outer key matches nothing -> empty bucket.
            run.probe_matches = hashval.map_or_else(Vec::new, |h| probe_bucket(run, h));
            run.probe_cursor = 0;
            run.need_new_outer = false;
        }

        // Walk the bucket candidates for the current outer tuple.
        if run.probe_cursor >= run.probe_matches.len() {
            run.need_new_outer = true;
            continue;
        }
        let row_idx = run.probe_matches[run.probe_cursor];
        run.probe_cursor += 1;

        let (vals, nulls) = inner_row(run, row_idx);
        store_into(&mut run.proj.inner_slot, &vals, &nulls);

        run.econtext.ecxt_outertuple = Some(std::mem::replace(
            &mut run.proj.outer_slot,
            make_tuple_table_slot(None, &TTS_OPS_VIRTUAL),
        ));
        run.econtext.ecxt_innertuple = Some(std::mem::replace(
            &mut run.proj.inner_slot,
            make_tuple_table_slot(None, &TTS_OPS_VIRTUAL),
        ));

        // Re-check the hashclauses (collisions) + joinqual + otherqual.
        let passes = run_join_qual(run.hashclauses.as_deref_mut(), &mut run.econtext)
            && run_join_qual(run.state.js.joinqual.as_deref_mut(), &mut run.econtext)
            && run_join_qual(run.proj.otherqual.as_deref_mut(), &mut run.econtext);

        if passes {
            if run.state.js.single_match {
                run.need_new_outer = true;
            }
            project_join(&mut run.proj.projection, &mut run.econtext);
            restore_slots(run);
            return run.proj.projection.state.resultslot.as_deref_mut();
        }
        restore_slots(run);
    }
}

/// The deformed inner row at `idx` in the built hash table.
fn inner_row(run: &HashJoinRun<'_>, idx: usize) -> (Vec<crate::postgres::Datum>, Vec<bool>) {
    let PlanStateNode::Hash(h) = run.inner.as_ref() else {
        unimplemented!("inner_row: inner child is not a Hash node");
    };
    let table = h.table.as_ref().unwrap_or_else(|| unreachable!("table built"));
    table.rows[idx].clone()
}

/// The bucket row indexes that hashed to `hashval` in the built table.
fn probe_bucket(run: &HashJoinRun<'_>, hashval: u64) -> Vec<usize> {
    let PlanStateNode::Hash(h) = run.inner.as_ref() else {
        unimplemented!("probe_bucket: inner child is not a Hash node");
    };
    let table = h.table.as_ref().unwrap_or_else(|| unreachable!("table built"));
    table.buckets.get(&hashval).cloned().unwrap_or_default()
}

/// The Hash inner child's input rowtype (the build rows' shape).
fn hash_child_desc(inner: &PlanStateNode<'_>) -> Option<crate::access::tupdesc::TupleDesc> {
    let PlanStateNode::Hash(h) = inner else { return None };
    result_type_of(&h.child)
}

/// Restore the outer/inner join slots from the econtext into the projection holder.
fn restore_slots(run: &mut HashJoinRun<'_>) {
    if let Some(s) = run.econtext.ecxt_outertuple.take() {
        run.proj.outer_slot = s;
    }
    if let Some(s) = run.econtext.ecxt_innertuple.take() {
        run.proj.inner_slot = s;
    }
}

/// Store deformed `(values, isnull)` into a virtual slot.
fn store_into(slot: &mut TupleTableSlot, values: &[crate::postgres::Datum], isnull: &[bool]) {
    ExecClearTuple(slot);
    let n = values.len();
    slot.values[..n].copy_from_slice(values);
    slot.isnull[..n].copy_from_slice(isnull);
    exec_store_virtual_tuple(slot);
}

/// PG `ExecEndHashJoin`: tear down the outer + inner (Hash) children.
pub fn exec_end_hash_join(shared: Option<&Arc<SharedState>>, run: &mut HashJoinRun<'_>) {
    exec_end_node(shared, &mut run.outer);
    exec_end_node(shared, &mut run.inner);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::nodeHash::exec_init_hash;
    use crate::backend::executor::nodeNestloop::join_test_support::{
        eq_joinqual, int_desc, join_tlist, join_var, plan_with, source, INT4_EQ,
    };
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt};
    use crate::nodes::plannodes::{Hash, Join};
    use crate::postgres_ext::InvalidOid;

    /// Build a HashJoin over a `Hash` inner. The HashJoin's `hashkeys` (outer probe)
    /// reference the outer child via OUTER_VAR; the Hash node's own `hashkeys`
    /// reference its child via OUTER_VAR (set_hash_refs); the hashclauses are
    /// `OUTER_VAR.1 = INNER_VAR.1` (the build/probe key columns).
    fn hashjoin_node() -> HashJoin {
        let hash_node = Hash {
            plan: plan_with(Vec::new(), Vec::new()),
            // Hash sits over its single child -> OUTER_VAR position of b.y (col 1).
            hashkeys: vec![join_var(true, 1)],
            skew_table: InvalidOid,
            skew_column: 0,
            skew_inherit: false,
            rows_total: 0.0,
        };
        let join = Join {
            plan: {
                let mut p = plan_with(join_tlist(&[(true, 1), (false, 2)]), Vec::new());
                p.righttree = Some(Node::Hash(Box::new(hash_node)));
                p
            },
            jointype: JoinType::INNER,
            inner_unique: false,
            joinqual: Vec::new(),
        };
        HashJoin {
            join,
            hashclauses: vec![eq_joinqual(1, 1)],
            hashoperators: vec![INT4_EQ],
            hashcollations: vec![InvalidOid],
            // outer probe key: a.x (outer col 1).
            hashkeys: vec![join_var(true, 1)],
        }
    }

    /// Init a HashJoin run over the given outer/inner tuple sources (the inner is
    /// wrapped in a Hash node carrying the inner build key).
    fn init_run(
        outer: PlanStateNode<'static>,
        inner_src: PlanStateNode<'static>,
    ) -> Box<HashJoinRun<'static>> {
        let node = hashjoin_node();
        let Node::Hash(hash_plan) = node.join.plan.righttree.as_ref().expect("hash inner") else {
            unreachable!("inner is a Hash node");
        };
        let mut estate = EState::default();
        let hash_run = exec_init_hash(hash_plan, &mut estate, inner_src);
        let inner = PlanStateNode::Hash(hash_run);
        exec_init_hash_join(&node, &mut estate, 0, outer, inner)
    }

    async fn drain_join(run: &mut HashJoinRun<'static>) -> Vec<(i32, i32)> {
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_hash_join(None, run)).await else { break };
            let a = DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null");
            let b = DatumGetInt32_opt(slot_getattr(slot, 2)).expect("non-null");
            out.push((a, b));
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hashjoin_inner_equijoin() {
        let a = int_desc(1);
        let b = int_desc(2);
        let outer = source(&a, &[&[1], &[2], &[3]]);
        let inner = source(&b, &[&[2, 20], &[3, 30], &[3, 31], &[5, 50]]);
        let mut run = init_run(outer, inner);
        let mut got = drain_join(&mut run).await;
        got.sort_unstable();
        assert_eq!(got, vec![(2, 20), (3, 30), (3, 31)]);
        exec_end_hash_join(None, &mut run);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hashjoin_no_matches() {
        let a = int_desc(1);
        let b = int_desc(2);
        let outer = source(&a, &[&[1], &[9]]);
        let inner = source(&b, &[&[2, 20], &[3, 30]]);
        let mut run = init_run(outer, inner);
        assert!(drain_join(&mut run).await.is_empty());
        exec_end_hash_join(None, &mut run);
    }
}
