//! SetOp node executor (INTERSECT / EXCEPT [ALL]). Translated from
//! backend/executor/nodeSetOp.c (disposition: full leaf for the M12 two-input set
//! difference/intersection; PG's hashed strategy + the streaming sorted merge are
//! collapsed here to a buffer-and-group pass that is order-independent and yields
//! the spec output).
//!
//! PG 18.4's SetOp has two inputs (outer = left, inner = right) with identical
//! column sets and counts, per group, how many tuples arrive from each side. The
//! SQL spec output is then, per distinct group with L left dups and R right dups:
//!   INTERSECT      -> emit the row once if L>0 and R>0
//!   INTERSECT ALL  -> emit min(L, R) copies
//!   EXCEPT         -> emit the row once if L>0 and R==0
//!   EXCEPT ALL     -> emit max(L - R, 0) copies
//! SetOp does no qual checking nor projection; the output row is a copy of the
//! first-arriving tuple in each group (we keep the left side's copy, falling back to
//! the right for INTERSECT groups present only via the right -- which never emit).
//!
//! Async coloring: pulling a child reaches the table AM, so `ExecSetOp` is `async`
//! (rules.md s5). The buffered groups are owned `Vec`s (Send).

use std::sync::Arc;

use crate::backend::executor::execProcnode::{
    exec_end_node, exec_proc_node, result_type_of, PlanStateNode,
};
use crate::backend::executor::nodeGroup::grouping_equal;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, PlanState, ScanState};
use crate::nodes::nodes::{Node, SetOpCmd};
use crate::nodes::plannodes::SetOp;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// A buffered output group: the representative row + how many copies to emit.
struct OutGroup {
    values: Vec<Datum>,
    isnull: Vec<bool>,
    emit: i64,
}

/// Run-state pairing the PG `SetOpState` shell with the two child plan-states and
/// the lazily computed output groups.
pub struct SetOpRun<'rel> {
    pub ss: Box<ScanState>,
    pub cmd: SetOpCmd,
    /// the grouping columns (all output columns) and their types, for equality.
    pub key_cols: Vec<i16>,
    pub key_types: Vec<Oid>,
    /// outer (left) child.
    pub left: Box<PlanStateNode<'rel>>,
    /// inner (right) child.
    pub right: Box<PlanStateNode<'rel>>,
    /// computed output groups (filled on first call); `None` until then.
    out: Option<Vec<OutGroup>>,
    /// next output group + remaining copies cursor.
    cur: usize,
}

/// PG `ExecInitSetOp`: build the SetOpState over already-initialized children. The
/// grouping columns are every output column (a SetOp compares the whole row); the
/// result rowtype is the outer child's.
pub fn exec_init_setop<'rel>(
    node: &SetOp,
    estate: &mut EState<'rel>,
    left: PlanStateNode<'rel>,
    right: PlanStateNode<'rel>,
) -> Box<SetOpRun<'rel>> {
    let _ = estate;
    let outer_desc = result_type_of(&left)
        .unwrap_or_else(|| unimplemented!("ExecInitSetOp: outer child has no result descriptor"));

    // Grouping columns: PG passes cmpColIdx over the duplicate-check columns; for a
    // set operation these are all output columns. Read the column types off the desc.
    let ncols = outer_desc.natts as usize;
    let key_cols: Vec<i16> = (1..=ncols as i16).collect();
    let key_types: Vec<Oid> = (0..ncols).map(|i| outer_desc.attr(i).atttypid).collect();

    let result_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    let mut ps = PlanState {
        plan: Some(Node::SetOp(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_result_tuple_desc = Some(Arc::clone(&outer_desc));
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.scandesc = Some(Arc::clone(&outer_desc));
    ps.ps_proj_info = None;

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: None,
    };

    Box::new(SetOpRun {
        ss: Box::new(ss),
        cmd: node.cmd,
        key_cols,
        key_types,
        left: Box::new(left),
        right: Box::new(right),
        out: None,
        cur: 0,
    })
}

/// Drain a child into owned (values, isnull) rows.
async fn drain_child(
    shared: Option<&Arc<SharedState>>,
    child: &mut PlanStateNode<'_>,
) -> Vec<(Vec<Datum>, Vec<bool>)> {
    use crate::executor::tuptable::slot_getallattrs;
    let mut rows = Vec::new();
    loop {
        match Box::pin(exec_proc_node(shared, child)).await {
            None => break,
            Some(s) => {
                slot_getallattrs(s);
                let n = s.nvalid.max(0) as usize;
                rows.push((s.values[..n].to_vec(), s.isnull[..n].to_vec()));
            }
        }
    }
    rows
}

impl SetOpRun<'_> {
    /// Buffer both inputs, group identical rows, count per side, and compute the
    /// per-group emit count under the SetOp command. Groups are kept in left-input
    /// first-seen order (PG returns the first-arriving tuple of each group).
    fn build_groups(&mut self, left: Vec<(Vec<Datum>, Vec<bool>)>, right: Vec<(Vec<Datum>, Vec<bool>)>) {
        let key_cols = &self.key_cols;
        let key_types = &self.key_types;
        // Per group: representative row, left count, right count.
        let mut groups: Vec<(Vec<Datum>, Vec<bool>, i64, i64)> = Vec::new();

        for (v, n) in left {
            match groups
                .iter()
                .position(|g| grouping_equal(key_cols, key_types, &v, &n, &g.0, &g.1))
            {
                Some(i) => groups[i].2 += 1,
                None => groups.push((v, n, 1, 0)),
            }
        }
        for (v, n) in right {
            match groups
                .iter()
                .position(|g| grouping_equal(key_cols, key_types, &v, &n, &g.0, &g.1))
            {
                Some(i) => groups[i].3 += 1,
                None => groups.push((v, n, 0, 1)),
            }
        }

        let mut out = Vec::new();
        for (values, isnull, num_left, num_right) in groups {
            let emit = match self.cmd {
                SetOpCmd::INTERSECT => i64::from(num_left > 0 && num_right > 0),
                SetOpCmd::INTERSECT_ALL => num_left.min(num_right),
                SetOpCmd::EXCEPT => i64::from(num_left > 0 && num_right == 0),
                SetOpCmd::EXCEPT_ALL => (num_left - num_right).max(0),
            };
            if emit > 0 {
                out.push(OutGroup { values, isnull, emit });
            }
        }
        self.out = Some(out);
    }
}

/// PG `ExecSetOp`: on the first call, buffer + group both inputs; thereafter emit
/// the computed output one row at a time (with multiplicity for the ALL variants).
pub async fn exec_setop<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut SetOpRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    if run.out.is_none() {
        let left = drain_child(shared, &mut run.left).await;
        let right = drain_child(shared, &mut run.right).await;
        run.build_groups(left, right);
    }

    // Advance over groups, decrementing each group's remaining emit count.
    let out = run.out.as_mut().unwrap_or_else(|| unreachable!("groups built"));
    while run.cur < out.len() {
        if out[run.cur].emit <= 0 {
            run.cur += 1;
            continue;
        }
        out[run.cur].emit -= 1;
        let (values, isnull) = (out[run.cur].values.clone(), out[run.cur].isnull.clone());
        let slot = run
            .ss
            .ps
            .ps_result_tuple_slot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecSetOp: no result slot"));
        ExecClearTuple(slot);
        let n = values.len();
        slot.values[..n].copy_from_slice(&values);
        slot.isnull[..n].copy_from_slice(&isnull);
        crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
        return run.ss.ps.ps_result_tuple_slot.as_deref_mut();
    }
    None
}

/// PG `ExecEndSetOp`: tear down both children.
pub fn exec_end_setop(shared: Option<&Arc<SharedState>>, run: &mut SetOpRun<'_>) {
    exec_end_node(shared, &mut run.left);
    exec_end_node(shared, &mut run.right);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt, TTSOpsVirtual};
    use crate::nodes::plannodes::{Plan, SetOp};
    use crate::nodes::nodes::SetOpStrategy;
    use crate::postgres::Int32GetDatum;

    const INT4OID: Oid = Oid::new(23);
    const INVALID: Oid = crate::postgres_ext::InvalidOid;

    fn int4_desc() -> TupleDesc {
        let mut d = TupleDescData::create_template(1);
        d.init_builtin_entry(1, "a", INT4OID, -1, 0);
        d.init_entry_collation(1, INVALID);
        Arc::new(d)
    }

    fn source(desc: &TupleDesc, vals: Vec<i32>) -> PlanStateNode<'static> {
        let slots: Vec<Box<TupleTableSlot>> = vals
            .into_iter()
            .map(|v| {
                let mut slot = make_tuple_table_slot(Some(Arc::clone(desc)), &TTSOpsVirtual);
                slot.values[0] = Int32GetDatum(v);
                slot.isnull[0] = false;
                exec_store_virtual_tuple(&mut slot);
                slot
            })
            .collect();
        PlanStateNode::test_tuple_source(Arc::clone(desc), slots)
    }

    fn setop_node(cmd: SetOpCmd) -> SetOp {
        SetOp {
            plan: Plan {
                disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
                parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
                targetlist: Vec::new(), qual: Vec::new(), lefttree: None, righttree: None,
                init_plan: Vec::new(), ext_param: None, all_param: None,
            },
            cmd,
            strategy: SetOpStrategy::SORTED,
            num_cols: 1,
            cmp_col_idx: vec![1],
            cmp_operators: vec![Oid::new(96)],
            cmp_collations: vec![INVALID],
            cmp_nulls_first: vec![false],
            num_groups: 0,
        }
    }

    async fn run(cmd: SetOpCmd, l: Vec<i32>, r: Vec<i32>) -> Vec<i32> {
        let desc = int4_desc();
        let mut estate = EState::default();
        let mut run = exec_init_setop(&setop_node(cmd), &mut estate, source(&desc, l), source(&desc, r));
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_setop(None, &mut run)).await else { break };
            out.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
        }
        exec_end_setop(None, &mut run);
        out.sort_unstable();
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn intersect_distinct() {
        assert_eq!(run(SetOpCmd::INTERSECT, vec![1, 2, 2, 3], vec![2, 3, 3, 4]).await, vec![2, 3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn intersect_all_min_count() {
        // group 2: left 2, right 1 -> min 1; group 3: left 1, right 2 -> min 1.
        assert_eq!(run(SetOpCmd::INTERSECT_ALL, vec![2, 2, 3], vec![2, 3, 3]).await, vec![2, 3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn except_distinct() {
        assert_eq!(run(SetOpCmd::EXCEPT, vec![1, 2, 2, 3], vec![2, 4]).await, vec![1, 3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn except_all_count_difference() {
        // left: 2x2, 1x3, 1x1 ; right: 1x2 -> 2:max(2-1,0)=1, 3:1, 1:1.
        assert_eq!(run(SetOpCmd::EXCEPT_ALL, vec![1, 2, 2, 3], vec![2]).await, vec![1, 2, 3]);
    }
}
