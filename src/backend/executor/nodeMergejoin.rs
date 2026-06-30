//! Merge-join node. Translated from backend/executor/nodeMergejoin.c
//! (disposition: full for the M7 INNER merge join over two sorted inputs; the
//! outer-join fill paths, the EXEC-state-machine mark/restore micro-states, and
//! the constant-false short-circuit are collapsed into the equal-key run product
//! described below; outer-join emission is a clean grow guard).
//!
//! `ExecInitMergeJoin` initializes the outer (lefttree) + inner (righttree)
//! children, compiles the per-clause merge keys + comparator, the joinqual, and the
//! result projection. `ExecMergeJoin` materializes both (already sorted) inputs and
//! runs the classic sort-merge: advance the cursor on the side with the smaller
//! merge key; when the keys are equal, emit the cartesian product of the equal-key
//! run on each side (the mark/restore PG does on the inner is realized here by
//! replaying the inner equal-key run for each outer row of the run). Each emitted
//! pair is re-checked against the joinqual before projection.
//!
//! Slot ownership / Send: the run-state owns both materialized inputs (deformed
//! `(values, isnull)` rows) and the join slots; `ecxt_outer/innertuple` point at
//! the owned join slots while the joinqual + projection run. No lock/RefCell across
//! the child `.await` (rules.md s5/s10).

use std::sync::Arc;

use crate::backend::executor::execExpr::{exec_build_projection_info, exec_init_expr, exec_init_qual};
use crate::backend::executor::execProcnode::{exec_end_node, exec_proc_node, result_type_of, PlanStateNode};
use crate::backend::executor::execTuples::{exec_store_virtual_tuple, exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::backend::executor::nodeUnique::snapshot_slot;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, ExprContext, ExprState, JoinState, MergeJoinState, PlanState, ProjectionInfo};
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::plannodes::MergeJoin;
use crate::nodes::primnodes::OpExpr;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::utils::sortsupport::{PrepareSortSupportFromOrderingOp, SortComparator, SortSupportData};

use super::execjoin::{project_join, run_join_qual, JoinProj};

type Row = (Vec<Datum>, Vec<bool>);

/// A compiled merge key: the outer + inner key expressions and the 3-way datum
/// comparator (ASC, NULLS LAST for the M7 paths) for that key's type.
struct MergeKey {
    outer_key: Box<ExprState>,
    inner_key: Box<ExprState>,
    comparator: SortComparator,
    collation: Oid,
}

/// Run-state pairing the PG `MergeJoinState` with its children, the merge keys, the
/// result projection, and the materialized + cursor state.
pub struct MergeJoinRun<'rel> {
    pub state: Box<MergeJoinState>,
    pub outer: Box<PlanStateNode<'rel>>,
    pub inner: Box<PlanStateNode<'rel>>,
    pub econtext: Box<ExprContext>,
    pub proj: JoinProj,
    keys: Vec<MergeKey>,
    /// both inputs materialized (already sorted by the merge keys); `None` until the
    /// first call drains them.
    outer_rows: Option<Vec<Row>>,
    inner_rows: Option<Vec<Row>>,
    /// the (outer_idx, inner_idx) matching pairs precomputed by the sort-merge, and
    /// the cursor into them. Built once after materialization.
    pairs: Vec<(usize, usize)>,
    pair_cursor: usize,
}

/// PG `ExecInitMergeJoin`: build the MergeJoinState over initialized children.
pub fn exec_init_merge_join<'rel>(
    node: &MergeJoin,
    estate: &mut EState<'rel>,
    eflags: i32,
    outer: PlanStateNode<'rel>,
    inner: PlanStateNode<'rel>,
) -> Box<MergeJoinRun<'rel>> {
    let _ = (estate, eflags);
    crate::assert!(
        node.join.jointype == JoinType::INNER,
        "ExecMergeJoin: only INNER join emit is reachable this milestone"
    );

    let outer_desc = result_type_of(&outer)
        .unwrap_or_else(|| unimplemented!("ExecInitMergeJoin: outer child has no result descriptor"));
    let inner_desc = result_type_of(&inner)
        .unwrap_or_else(|| unimplemented!("ExecInitMergeJoin: inner child has no result descriptor"));

    let result_desc = exec_type_from_tl(&node.join.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);
    let projection: Box<ProjectionInfo> = {
        let mut transient = ExprContext::default();
        exec_build_projection_info(&node.join.plan.targetlist, &mut transient, result_slot, None, None)
    };

    let keys = node
        .mergeclauses
        .iter()
        .map(init_merge_key)
        .collect();

    let joinqual = exec_init_qual(&node.join.joinqual, None);
    let otherqual = exec_init_qual(&node.join.plan.qual, None);

    let mut ps = PlanState {
        plan: Some(Node::MergeJoin(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_result_tuple_desc = Some(result_desc);

    let js = JoinState {
        ps,
        jointype: Some(node.join.jointype),
        single_match: node.join.inner_unique || node.join.jointype == JoinType::SEMI,
        joinqual,
    };

    Box::new(MergeJoinRun {
        state: Box::new(MergeJoinState {
            js,
            mj_num_clauses: i32::try_from(node.mergeclauses.len()).unwrap_or(0),
            ..MergeJoinState::default()
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
        keys,
        outer_rows: None,
        inner_rows: None,
        pairs: Vec::new(),
        pair_cursor: 0,
    })
}

/// Compile one mergeclause (a switched `=` OpExpr: outer key left, inner key right)
/// into its key expressions + datum comparator.
fn init_merge_key(clause: &Node) -> MergeKey {
    let Node::OpExpr(op) = clause else {
        unimplemented!("ExecInitMergeJoin: non-OpExpr merge clause");
    };
    let OpExpr { args, inputcollid, .. } = op.as_ref();
    crate::assert!(args.len() == 2, "merge clause is binary");
    let outer_key = exec_init_expr(Some(&args[0]), None)
        .unwrap_or_else(|| unimplemented!("ExecInitMergeJoin: empty outer merge key"));
    let inner_key = exec_init_expr(Some(&args[1]), None)
        .unwrap_or_else(|| unimplemented!("ExecInitMergeJoin: empty inner merge key"));
    let keytype = crate::nodes::nodeFuncs::exprType(&args[0]);
    let comparator = comparator_for_type(keytype);
    MergeKey { outer_key, inner_key, comparator, collation: *inputcollid }
}

/// Resolve a 3-way ASC comparator for a merge-key type via the type's btree LT
/// ordering operator (the seeded int2/int4/int8/oid/text/date/numeric set).
fn comparator_for_type(keytype: Oid) -> SortComparator {
    let lt_op = match keytype {
        Oid::INT2OID => 95,     // int2lt
        Oid::INT4OID => 97,     // int4lt
        Oid::INT8OID => 412,    // int8lt
        Oid::OIDOID => 609,     // oidlt
        Oid::TEXTOID => 664,    // text_lt
        Oid::DATEOID => 1095,   // date_lt
        Oid::NUMERICOID => 1754, // numeric_lt
        other => unimplemented!("ExecInitMergeJoin: no merge comparator for type {other}"),
    };
    let mut ssup = blank_ssup();
    PrepareSortSupportFromOrderingOp(Oid::new(lt_op), &mut ssup);
    ssup.comparator
        .unwrap_or_else(|| unreachable!("ordering op resolved a comparator"))
}

/// PG `ExecMergeJoin`: materialize both sorted inputs (once), precompute the
/// sort-merge matching pairs (the equal-key run product), then serve one qualifying
/// join tuple per call. Returns the next projected slot, or `None` at end of join.
pub async fn exec_merge_join<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut MergeJoinRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    if run.outer_rows.is_none() {
        let outer_rows = drain(shared, &mut run.outer).await;
        let inner_rows = drain(shared, &mut run.inner).await;
        run.pairs = merge_pairs(&mut run.keys, &outer_rows, &inner_rows);
        run.outer_rows = Some(outer_rows);
        run.inner_rows = Some(inner_rows);
    }

    loop {
        if run.pair_cursor >= run.pairs.len() {
            return None;
        }
        let (oi, ii) = run.pairs[run.pair_cursor];
        run.pair_cursor += 1;

        let outer = run.outer_rows.as_ref().unwrap_or_else(|| unreachable!())[oi].clone();
        let inner = run.inner_rows.as_ref().unwrap_or_else(|| unreachable!())[ii].clone();
        store_into(&mut run.proj.outer_slot, &outer.0, &outer.1);
        store_into(&mut run.proj.inner_slot, &inner.0, &inner.1);
        load_econtext(run);

        // Re-check the joinqual + otherqual (non-merge clauses) before projecting.
        let passes = run_join_qual(run.state.js.joinqual.as_deref_mut(), &mut run.econtext)
            && run_join_qual(run.proj.otherqual.as_deref_mut(), &mut run.econtext);
        if passes {
            project_join(&mut run.proj.projection, &mut run.econtext);
            restore_slots(run);
            return run.proj.projection.state.resultslot.as_deref_mut();
        }
        restore_slots(run);
    }
}

/// Precompute the matching (outer_idx, inner_idx) pairs by sort-merge: advance the
/// smaller-key side; on an equal key, emit the cartesian product of the equal-key
/// run on each side (the mark/restore product), then skip past both runs.
fn merge_pairs(keys: &mut [MergeKey], outer: &[Row], inner: &[Row]) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();
    let (mut oi, mut ii) = (0usize, 0usize);
    while oi < outer.len() && ii < inner.len() {
        let c = cmp_pair_keys(keys, &outer[oi], &inner[ii]);
        match c.cmp(&0) {
            std::cmp::Ordering::Less => oi += 1,
            std::cmp::Ordering::Greater => ii += 1,
            std::cmp::Ordering::Equal => {
                // Delimit the equal-key run on each side, then emit the product.
                let mut oend = oi + 1;
                while oend < outer.len() && cmp_outer_keys(keys, &outer[oi], &outer[oend]) == 0 {
                    oend += 1;
                }
                let mut iend = ii + 1;
                while iend < inner.len() && cmp_inner_keys(keys, &inner[ii], &inner[iend]) == 0 {
                    iend += 1;
                }
                for o in oi..oend {
                    for i in ii..iend {
                        pairs.push((o, i));
                    }
                }
                oi = oend;
                ii = iend;
            }
        }
    }
    pairs
}

/// 3-way compare an outer row's keys against an inner row's keys.
fn cmp_pair_keys(keys: &mut [MergeKey], outer: &Row, inner: &Row) -> i32 {
    for k in keys.iter_mut() {
        let lv = eval_outer(k, outer);
        let rv = eval_inner(k, inner);
        let cmp = cmp_datums(k.comparator, k.collation, lv, rv);
        if cmp != 0 {
            return cmp;
        }
    }
    0
}

/// 3-way compare two outer rows' keys (for delimiting an outer equal-key run).
fn cmp_outer_keys(keys: &mut [MergeKey], a: &Row, b: &Row) -> i32 {
    for k in keys.iter_mut() {
        let lv = eval_outer(k, a);
        let rv = eval_outer(k, b);
        let cmp = cmp_datums(k.comparator, k.collation, lv, rv);
        if cmp != 0 {
            return cmp;
        }
    }
    0
}

/// 3-way compare two inner rows' keys (for delimiting an inner equal-key run).
fn cmp_inner_keys(keys: &mut [MergeKey], a: &Row, b: &Row) -> i32 {
    for k in keys.iter_mut() {
        let lv = eval_inner(k, a);
        let rv = eval_inner(k, b);
        let cmp = cmp_datums(k.comparator, k.collation, lv, rv);
        if cmp != 0 {
            return cmp;
        }
    }
    0
}

fn cmp_datums(comparator: SortComparator, collation: Oid, l: (Datum, bool), r: (Datum, bool)) -> i32 {
    match (l.1, r.1) {
        (true, true) => 0,
        (true, false) => 1,  // NULLS LAST (ASC)
        (false, true) => -1,
        (false, false) => {
            let mut ssup = blank_ssup();
            ssup.ssup_collation = collation;
            ssup.comparator = Some(comparator);
            comparator(l.0, r.0, &ssup)
        }
    }
}

/// Evaluate a merge key's outer-side expr (an OUTER_VAR Var) over an outer row.
fn eval_outer(key: &mut MergeKey, row: &Row) -> (Datum, bool) {
    let mut ec = ExprContext { ecxt_outertuple: Some(slot_from_row(row)), ..ExprContext::default() };
    let mut isnull = false;
    let evalfunc = key.outer_key.evalfunc.unwrap_or_else(|| unimplemented!("merge key not ready"));
    let datum = evalfunc(&mut key.outer_key, &mut ec, &mut isnull);
    (datum, isnull)
}

/// Evaluate a merge key's inner-side expr (an INNER_VAR Var) over an inner row.
fn eval_inner(key: &mut MergeKey, row: &Row) -> (Datum, bool) {
    let mut ec = ExprContext { ecxt_innertuple: Some(slot_from_row(row)), ..ExprContext::default() };
    let mut isnull = false;
    let evalfunc = key.inner_key.evalfunc.unwrap_or_else(|| unimplemented!("merge key not ready"));
    let datum = evalfunc(&mut key.inner_key, &mut ec, &mut isnull);
    (datum, isnull)
}

/// Build a virtual slot whose value/null arrays carry a materialized row, sized to
/// the row width (the Var opcode reads `slot.values[attnum]` directly). Returns a
/// `Box` because `ExprContext::ecxt_{outer,inner}tuple` hold `Option<Box<_>>`.
#[allow(clippy::unnecessary_box_returns, reason = "ExprContext stores the slot as Box<TupleTableSlot>")]
fn slot_from_row(row: &Row) -> Box<TupleTableSlot> {
    let mut slot = make_tuple_table_slot(None, &TTS_OPS_VIRTUAL);
    let n = row.0.len();
    slot.values.clone_from(&row.0);
    slot.isnull.clone_from(&row.1);
    slot.nvalid = i16::try_from(n).unwrap_or(i16::MAX);
    slot.flags.remove(crate::executor::tuptable::TtsFlags::EMPTY);
    slot
}

fn blank_ssup() -> SortSupportData {
    SortSupportData {
        ssup_cxt: crate::utils::palloc::MemoryContext::default(),
        ssup_collation: crate::postgres_ext::InvalidOid,
        ssup_reverse: false,
        ssup_nulls_first: false,
        ssup_attno: 0,
        ssup_extra: core::ptr::null_mut(),
        comparator: None,
        abbreviate: false,
        abbrev_converter: None,
        abbrev_abort: None,
        abbrev_full_comparator: None,
    }
}

/// Set ecxt_outer/innertuple to the join slots (moved out of the projection holder).
fn load_econtext(run: &mut MergeJoinRun<'_>) {
    run.econtext.ecxt_outertuple = Some(std::mem::replace(
        &mut run.proj.outer_slot,
        make_tuple_table_slot(None, &TTS_OPS_VIRTUAL),
    ));
    run.econtext.ecxt_innertuple = Some(std::mem::replace(
        &mut run.proj.inner_slot,
        make_tuple_table_slot(None, &TTS_OPS_VIRTUAL),
    ));
}

/// Restore the join slots from the econtext into the projection holder.
fn restore_slots(run: &mut MergeJoinRun<'_>) {
    if let Some(s) = run.econtext.ecxt_outertuple.take() {
        run.proj.outer_slot = s;
    }
    if let Some(s) = run.econtext.ecxt_innertuple.take() {
        run.proj.inner_slot = s;
    }
}

/// Drain a child plan-state into deformed rows.
async fn drain(shared: Option<&Arc<SharedState>>, child: &mut PlanStateNode<'_>) -> Vec<Row> {
    let mut rows = Vec::new();
    while let Some(slot) = Box::pin(exec_proc_node(shared, child)).await {
        rows.push(snapshot_slot(slot));
    }
    rows
}

/// Store deformed `(values, isnull)` into a virtual slot.
fn store_into(slot: &mut TupleTableSlot, values: &[Datum], isnull: &[bool]) {
    ExecClearTuple(slot);
    let n = values.len();
    slot.values[..n].copy_from_slice(values);
    slot.isnull[..n].copy_from_slice(isnull);
    exec_store_virtual_tuple(slot);
}

/// PG `ExecEndMergeJoin`: tear down the child subtrees.
pub fn exec_end_merge_join(shared: Option<&Arc<SharedState>>, run: &mut MergeJoinRun<'_>) {
    exec_end_node(shared, &mut run.outer);
    exec_end_node(shared, &mut run.inner);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::nodeNestloop::join_test_support::{
        eq_joinqual, int_desc, join_tlist, plan_with, source,
    };
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt};
    use crate::nodes::plannodes::Join;
    use crate::postgres_ext::InvalidOid;

    fn mergejoin_node() -> MergeJoin {
        let join = Join {
            plan: plan_with(join_tlist(&[(true, 1), (false, 2)]), Vec::new()),
            jointype: JoinType::INNER,
            inner_unique: false,
            joinqual: Vec::new(),
        };
        MergeJoin {
            join,
            skip_mark_restore: false,
            // mergeclause: a.x = b.y (OUTER_VAR.1 = INNER_VAR.1).
            mergeclauses: vec![eq_joinqual(1, 1)],
            merge_families: vec![Oid::new(1976)],
            merge_collations: vec![InvalidOid],
            merge_reversals: vec![false],
            merge_nulls_first: vec![false],
        }
    }

    async fn drain_join(run: &mut MergeJoinRun<'static>) -> Vec<(i32, i32)> {
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_merge_join(None, run)).await else { break };
            let a = DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null");
            let b = DatumGetInt32_opt(slot_getattr(slot, 2)).expect("non-null");
            out.push((a, b));
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mergejoin_inner_equijoin_sorted_inputs() {
        let a = int_desc(1);
        let b = int_desc(2);
        // Both inputs sorted ascending by the join key.
        let outer = source(&a, &[&[1], &[2], &[3]]);
        let inner = source(&b, &[&[2, 20], &[3, 30], &[3, 31], &[5, 50]]);
        let mut estate = EState::default();
        let mut run = exec_init_merge_join(&mergejoin_node(), &mut estate, 0, outer, inner);
        let got = drain_join(&mut run).await;
        // The equal-key run product: (3,30) and (3,31) for outer 3.
        assert_eq!(got, vec![(2, 20), (3, 30), (3, 31)]);
        exec_end_merge_join(None, &mut run);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mergejoin_duplicate_keys_both_sides() {
        let a = int_desc(1);
        let b = int_desc(2);
        // Outer has two 3's; inner has two 3's -> 2x2 = 4 matched pairs.
        let outer = source(&a, &[&[3], &[3]]);
        let inner = source(&b, &[&[3, 30], &[3, 31]]);
        let mut estate = EState::default();
        let mut run = exec_init_merge_join(&mergejoin_node(), &mut estate, 0, outer, inner);
        let got = drain_join(&mut run).await;
        assert_eq!(got, vec![(3, 30), (3, 31), (3, 30), (3, 31)]);
        exec_end_merge_join(None, &mut run);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mergejoin_no_matches() {
        let a = int_desc(1);
        let b = int_desc(2);
        let outer = source(&a, &[&[1], &[4]]);
        let inner = source(&b, &[&[2, 20], &[3, 30]]);
        let mut estate = EState::default();
        let mut run = exec_init_merge_join(&mergejoin_node(), &mut estate, 0, outer, inner);
        assert!(drain_join(&mut run).await.is_empty());
        exec_end_merge_join(None, &mut run);
    }
}
