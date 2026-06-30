//! Nested-loop join executor. Translated from
//! backend/executor/nodeNestloop.c (disposition: full for the M7 INNER nestloop;
//! the LEFT/ANTI unmatched-outer emission + nestloop-param passing are clean grow
//! guards).
//!
//! `ExecInitNestLoop` initializes the outer (lefttree) and inner (righttree)
//! children, builds the joinqual + result projection, and the owned outer/inner
//! join slots. `ExecNestLoop` is PG's main loop: for each outer tuple, scan the
//! inner from the beginning, and for every (outer,inner) pair that passes the
//! joinqual, project the join targetlist and return the result.
//!
//! In PG the inner child is `ExecReScan`-ed per outer row. This port materializes
//! the inner child ONCE into an owned row buffer on the first call (a plain
//! nestloop rescans the whole inner per outer row, which is what this models) and
//! replays it from the buffer for each subsequent outer row -- correct for an
//! inner join with no nestloop params. Parameterized inner (nestParams) grows
//! later (rules.md s4).
//!
//! Slot ownership / Send: the run-state OWNS the outer/inner join slots, the
//! result slot, and the materialized inner buffer (`Vec` of owned value/null
//! arrays). `ecxt_outertuple`/`ecxt_innertuple` are set to the owned join slots
//! before the joinqual + projection read them via EEOP_OUTER_VAR/INNER_VAR. No
//! lock/RefCell is held across the child `.await` (rules.md s5/s10).

use std::sync::Arc;

use crate::backend::executor::execExpr::{exec_build_projection_info, exec_init_qual};
use crate::backend::executor::execProcnode::{exec_end_node, exec_proc_node, result_type_of, PlanStateNode};
use crate::backend::executor::execTuples::{exec_store_virtual_tuple, exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::backend::executor::nodeUnique::snapshot_slot;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, ExprContext, JoinState, NestLoopState, PlanState, ProjectionInfo};
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::plannodes::NestLoop;
use crate::shared_state::SharedState;

use super::execjoin::{project_join, run_join_qual, JoinProj};

/// Run-state pairing the PG `NestLoopState` with its outer/inner child plan-states,
/// the owned join exprcontext + result projection, and the materialized inner
/// buffer. The C node holds the children via `ps.lefttree`/`ps.righttree` and the
/// projection/econtext in the PlanState; here they live in the wrapper (island).
pub struct NestLoopRun<'rel> {
    pub state: Box<NestLoopState>,
    pub outer: Box<PlanStateNode<'rel>>,
    pub inner: Box<PlanStateNode<'rel>>,
    /// the join exprcontext (holds ecxt_outertuple/ecxt_innertuple while the
    /// joinqual + projection run). Owned `Box` (Send).
    pub econtext: Box<ExprContext>,
    /// the joinqual + result projection, owned here.
    pub proj: JoinProj,
    /// the inner child materialized once: each row's deformed (values, isnull).
    /// `None` until the first call drains the inner child.
    pub inner_rows: Option<Vec<(Vec<crate::postgres::Datum>, Vec<bool>)>>,
    /// cursor into `inner_rows` for the current outer tuple.
    pub inner_cursor: usize,
}

/// PG `ExecInitNestLoop`: build the NestLoopState over initialized children. The
/// outer child gets REWIND (cheap rescans help); the inner is materialized here so
/// it needs no special eflags. Builds the result slot/type/projection from the
/// join targetlist and the joinqual.
pub fn exec_init_nest_loop<'rel>(
    node: &NestLoop,
    estate: &mut EState<'rel>,
    eflags: i32,
    outer: PlanStateNode<'rel>,
    inner: PlanStateNode<'rel>,
) -> Box<NestLoopRun<'rel>> {
    let _ = (estate, eflags);
    crate::assert!(node.nest_params.is_empty(), "ExecInitNestLoop: nestloop params not yet reachable");

    let outer_desc = result_type_of(&outer)
        .unwrap_or_else(|| unimplemented!("ExecInitNestLoop: outer child has no result descriptor"));
    let inner_desc = result_type_of(&inner)
        .unwrap_or_else(|| unimplemented!("ExecInitNestLoop: inner child has no result descriptor"));

    let result_desc = exec_type_from_tl(&node.join.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);

    let projection: Box<ProjectionInfo> = {
        let mut transient = ExprContext::default();
        exec_build_projection_info(&node.join.plan.targetlist, &mut transient, result_slot, None, None)
    };

    let joinqual = exec_init_qual(&node.join.joinqual, None);
    let otherqual = exec_init_qual(&node.join.plan.qual, None);

    let mut ps = PlanState {
        plan: Some(Node::NestLoop(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_result_tuple_desc = Some(result_desc);

    let js = JoinState {
        ps,
        jointype: Some(node.join.jointype),
        single_match: node.join.inner_unique || node.join.jointype == JoinType::SEMI,
        joinqual,
    };

    crate::assert!(
        node.join.jointype == JoinType::INNER,
        "ExecNestLoop: only INNER join emit is reachable this milestone"
    );

    Box::new(NestLoopRun {
        state: Box::new(NestLoopState {
            js,
            nl_need_new_outer: true,
            nl_matched_outer: false,
            nl_null_inner_tuple_slot: None,
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
        inner_rows: None,
        inner_cursor: 0,
    })
}

/// PG `ExecNestLoop`: the nested-loop main loop. Returns a borrow of the projected
/// result slot for the next qualifying join tuple, or `None` at end of join.
pub async fn exec_nest_loop<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut NestLoopRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    // Materialize the inner child once (the whole inner is replayed per outer row).
    if run.inner_rows.is_none() {
        let mut rows = Vec::new();
        while let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.inner)).await {
            rows.push(snapshot_slot(slot));
        }
        run.inner_rows = Some(rows);
    }

    loop {
        // If we need a new outer tuple, fetch it and rewind the inner cursor.
        if run.state.nl_need_new_outer {
            let snapped = match Box::pin(exec_proc_node(shared, &mut run.outer)).await {
                None => return None, // no more outer tuples -> join complete
                Some(slot) => snapshot_slot(slot),
            };
            store_into(&mut run.proj.outer_slot, &snapped.0, &snapped.1);
            run.state.nl_need_new_outer = false;
            run.state.nl_matched_outer = false;
            run.inner_cursor = 0;
        }

        // Try the next inner tuple for the current outer.
        let inner_rows = run
            .inner_rows
            .as_ref()
            .unwrap_or_else(|| unreachable!("inner materialized above"));
        if run.inner_cursor >= inner_rows.len() {
            // No more inner tuples -> need a new outer tuple next time around.
            run.state.nl_need_new_outer = true;
            continue;
        }
        let (vals, nulls) = inner_rows[run.inner_cursor].clone();
        run.inner_cursor += 1;
        store_into(&mut run.proj.inner_slot, &vals, &nulls);

        // Point the econtext at the owned outer/inner slots and test the joinqual.
        run.econtext.ecxt_outertuple = Some(std::mem::replace(
            &mut run.proj.outer_slot,
            make_tuple_table_slot(None, &TTS_OPS_VIRTUAL),
        ));
        run.econtext.ecxt_innertuple = Some(std::mem::replace(
            &mut run.proj.inner_slot,
            make_tuple_table_slot(None, &TTS_OPS_VIRTUAL),
        ));

        let passes = run_join_qual(run.state.js.joinqual.as_deref_mut(), &mut run.econtext)
            && run_join_qual(run.proj.otherqual.as_deref_mut(), &mut run.econtext);

        if passes {
            run.state.nl_matched_outer = true;
            if run.state.js.single_match {
                run.state.nl_need_new_outer = true;
            }
            // Project while the join slots are on the econtext, then restore them;
            // the result slot is owned separately (run.proj.projection), so the
            // final borrow is taken after the restore to avoid an aliasing clash.
            project_join(&mut run.proj.projection, &mut run.econtext);
            restore_slots(run);
            return run.proj.projection.state.resultslot.as_deref_mut();
        }

        restore_slots(run);
    }
}

/// Restore the outer/inner join slots back into the projection holder from the
/// econtext (they were moved out to set ecxt_outertuple/ecxt_innertuple).
fn restore_slots(run: &mut NestLoopRun<'_>) {
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

/// PG `ExecEndNestLoop`: tear down the child subtrees.
pub fn exec_end_nest_loop(shared: Option<&Arc<SharedState>>, run: &mut NestLoopRun<'_>) {
    exec_end_node(shared, &mut run.outer);
    exec_end_node(shared, &mut run.inner);
}

#[cfg(test)]
pub(crate) mod join_test_support {
    //! Shared helpers for the join-node unit tests (nestloop / hashjoin /
    //! mergejoin): build synthetic outer/inner tuple sources, OUTER_VAR/INNER_VAR
    //! Vars (simulating post-setrefs), an int4 `=` joinqual, and a join targetlist.
    use std::sync::Arc;

    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot, TTS_OPS_VIRTUAL};
    use crate::backend::nodes::makefuncs::{make_opclause, make_target_entry, make_var};
    use crate::executor::tuptable::TupleTableSlot;
    use crate::nodes::nodes::Node;
    use crate::nodes::plannodes::Plan;
    use crate::nodes::primnodes::{INNER_VAR, OUTER_VAR};
    use crate::postgres::Int32GetDatum;
    use crate::postgres_ext::{InvalidOid, Oid};

    pub const INT4OID: Oid = Oid::new(23);
    pub const INT4_EQ: Oid = Oid::new(96);
    pub const INT4EQ_FN: Oid = Oid::new(65);

    /// An n-column int4 rowtype (cols named c1..cn).
    pub fn int_desc(ncols: usize) -> TupleDesc {
        let mut d = TupleDescData::create_template(ncols as i32);
        for i in 0..ncols {
            d.init_builtin_entry((i + 1) as i16, &format!("c{}", i + 1), INT4OID, -1, 0);
            d.init_entry_collation((i + 1) as i16, InvalidOid);
        }
        Arc::new(d)
    }

    /// A tuple source of int4 rows (each row a slice of column values).
    pub fn source(desc: &TupleDesc, rows: &[&[i32]]) -> PlanStateNode<'static> {
        let slots: Vec<Box<TupleTableSlot>> = rows
            .iter()
            .map(|r| {
                let mut slot = make_tuple_table_slot(Some(Arc::clone(desc)), &TTS_OPS_VIRTUAL);
                for (i, &v) in r.iter().enumerate() {
                    slot.values[i] = Int32GetDatum(v);
                    slot.isnull[i] = false;
                }
                exec_store_virtual_tuple(&mut slot);
                slot
            })
            .collect();
        PlanStateNode::test_tuple_source(Arc::clone(desc), slots)
    }

    /// A Var referencing the outer (OUTER_VAR) or inner (INNER_VAR) child output by
    /// position (1-based), as setrefs would have stamped it.
    pub fn join_var(outer: bool, attno: i16) -> Node {
        let varno = if outer { OUTER_VAR } else { INNER_VAR };
        Node::Var(Box::new(make_var(varno, attno, INT4OID, -1, InvalidOid, 0)))
    }

    /// The `=` joinqual OpExpr: `OUTER_VAR.outer_att = INNER_VAR.inner_att` (int4eq).
    pub fn eq_joinqual(outer_att: i16, inner_att: i16) -> Node {
        let Node::OpExpr(mut op) = make_opclause(
            INT4_EQ,
            crate::postgres_ext::Oid::new(16), // bool
            false,
            Some(join_var(true, outer_att)),
            Some(join_var(false, inner_att)),
            InvalidOid,
            InvalidOid,
        ) else {
            unreachable!("make_opclause yields an OpExpr");
        };
        op.opfuncid = INT4EQ_FN;
        Node::OpExpr(op)
    }

    /// A join targetlist: one entry per `(outer, attno)` projecting that child Var.
    pub fn join_tlist(cols: &[(bool, i16)]) -> Vec<Node> {
        cols.iter()
            .enumerate()
            .map(|(i, &(outer, attno))| {
                let v = join_var(outer, attno);
                Node::TargetEntry(Box::new(make_target_entry(Some(v), (i + 1) as i16, None, false)))
            })
            .collect()
    }

    /// A bare Plan carrying a targetlist (+ optional qual), no children/costs.
    pub fn plan_with(targetlist: Vec<Node>, qual: Vec<Node>) -> Plan {
        Plan {
            disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
            parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
            targetlist, qual, lefttree: None, righttree: None, init_plan: Vec::new(),
            ext_param: None, all_param: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::join_test_support::*;
    use super::*;
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt};
    use crate::nodes::nodes::JoinType;
    use crate::nodes::plannodes::{Join, NestLoop};

    fn nestloop_node() -> NestLoop {
        // outer a(c1), inner b(c1,c2); join a.c1 = b.c1; project (a.c1, b.c2).
        let join = Join {
            plan: plan_with(join_tlist(&[(true, 1), (false, 2)]), Vec::new()),
            jointype: JoinType::INNER,
            inner_unique: false,
            joinqual: vec![eq_joinqual(1, 1)],
        };
        NestLoop { join, nest_params: Vec::new() }
    }

    async fn drain_join(run: &mut NestLoopRun<'static>) -> Vec<(i32, i32)> {
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_nest_loop(None, run)).await else { break };
            let a = DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null");
            let b = DatumGetInt32_opt(slot_getattr(slot, 2)).expect("non-null");
            out.push((a, b));
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nestloop_inner_equijoin() {
        let a = int_desc(1);
        let b = int_desc(2);
        // a: x in {1,2,3}; b: (y,z) in {(2,20),(3,30),(3,31),(5,50)}.
        let outer = source(&a, &[&[1], &[2], &[3]]);
        let inner = source(&b, &[&[2, 20], &[3, 30], &[3, 31], &[5, 50]]);
        let mut estate = EState::default();
        let mut run = exec_init_nest_loop(&nestloop_node(), &mut estate, 0, outer, inner);
        let mut got = drain_join(&mut run).await;
        got.sort_unstable();
        // matches: (2,20), (3,30), (3,31).
        assert_eq!(got, vec![(2, 20), (3, 30), (3, 31)]);
        exec_end_nest_loop(None, &mut run);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nestloop_no_matches() {
        let a = int_desc(1);
        let b = int_desc(2);
        let outer = source(&a, &[&[1], &[9]]);
        let inner = source(&b, &[&[2, 20], &[3, 30]]);
        let mut estate = EState::default();
        let mut run = exec_init_nest_loop(&nestloop_node(), &mut estate, 0, outer, inner);
        assert!(drain_join(&mut run).await.is_empty());
        exec_end_nest_loop(None, &mut run);
    }
}
