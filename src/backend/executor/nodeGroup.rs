//! Group node executor. Translated from
//! backend/executor/nodeGroup.c (disposition: full for the M5 GROUP BY over
//! sorted input; the HAVING qual + non-trivial projection arms are clean grow
//! guards).
//!
//! The Group node's child delivers tuples SORTED by the grouping columns, so group
//! boundaries are found by comparing adjacent tuples on those columns. Group
//! returns one row per group (the first tuple of the group). PG runs the node's
//! qual (HAVING) + projection on that first tuple; the M5 path has no HAVING and a
//! passthrough projection (the grouping columns), so the first group tuple is
//! returned as-is.
//!
//! Async coloring: the child drive reaches the table AM, so `ExecGroup` is `async`
//! (rules.md s5). No guard across the child `.await`.
//!
//! `grouping_equal` (the adjacent-tuple key comparison) is shared with nodeUnique.

use std::sync::Arc;

use crate::backend::executor::execProcnode::{exec_end_node, exec_proc_node, result_type_of, PlanStateNode};
use crate::backend::executor::nodeUnique::snapshot_slot;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, GroupState, PlanState, ScanState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::Group;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// Run-state pairing the PG `GroupState` with its child plan-state. The
/// ScanTupleSlot holds the (copied) first tuple of the current group (PG's
/// `firsttupleslot`); the result slot serves it out.
pub struct GroupRun<'rel> {
    pub state: Box<GroupState>,
    pub child: Box<PlanStateNode<'rel>>,
    /// grouping column positions (1-based) + their types, for the boundary test.
    pub key_cols: Vec<i16>,
    pub key_types: Vec<Oid>,
    /// the first tuple of the current group, as an owned snapshot (PG keeps it in
    /// `ss_ScanTupleSlot`); `None` before the first group is read.
    pub first_tuple: Option<(Vec<Datum>, Vec<bool>)>,
}

/// PG `ExecInitGroup`: build the GroupState over an initialized child. Scan +
/// result slots are virtual of the child's rowtype. M5 has no HAVING; projection
/// is passthrough of the first group tuple.
pub fn exec_init_group<'rel>(
    node: &Group,
    estate: &mut EState<'rel>,
    child: PlanStateNode<'rel>,
) -> Box<GroupRun<'rel>> {
    crate::assert!(node.plan.qual.is_empty(), "ExecInitGroup: HAVING qual not yet reachable");
    let _ = estate;

    let outer_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitGroup: child has no result descriptor"));

    let scan_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );
    let result_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    let key_cols = node.grp_col_idx.clone();
    let key_types = key_cols
        .iter()
        .map(|&c| outer_desc.attr((c - 1) as usize).atttypid)
        .collect();

    let mut ps = PlanState {
        plan: Some(Node::Group(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_result_tuple_desc = Some(Arc::clone(&outer_desc));
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.scandesc = Some(outer_desc);
    ps.ps_proj_info = None;

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(GroupRun {
        state: Box::new(GroupState { ss, eqfunction: None, grp_done: false }),
        child: Box::new(child),
        key_cols,
        key_types,
        first_tuple: None,
    })
}

/// PG `ExecGroup`: return one tuple per group (the first tuple of each group).
/// Scans over remaining group members (equal on the grouping columns) and stops at
/// the first tuple of the next group, which becomes the next group's first tuple.
pub async fn exec_group<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut GroupRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    if run.state.grp_done {
        return None;
    }

    // First time through: acquire the first input tuple.
    if run.first_tuple.is_none() {
        let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await else {
            run.state.grp_done = true;
            return None;
        };
        run.first_tuple = Some(snapshot_slot(slot));
        return Some(emit_first(run));
    }

    // Scan over the rest of the current group, then emit the next group's first.
    loop {
        let copied = {
            let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await else {
                run.state.grp_done = true;
                return None;
            };
            snapshot_slot(slot)
        };

        let first = run.first_tuple.as_ref().unwrap_or_else(|| unreachable!("first set"));
        if grouping_equal(&run.key_cols, &run.key_types, &copied.0, &copied.1, &first.0, &first.1) {
            // Same group -> skip.
            continue;
        }

        // First tuple of the next group.
        run.first_tuple = Some(copied);
        return Some(emit_first(run));
    }
}

/// Project the current group's first tuple into the result slot (passthrough; M5
/// has no HAVING/non-trivial projection) and return a borrow of it.
fn emit_first<'r>(run: &'r mut GroupRun<'_>) -> &'r mut TupleTableSlot {
    let (values, isnull) = run.first_tuple.as_ref().unwrap_or_else(|| unreachable!("first set"));
    let n = values.len();
    let slot = run
        .state
        .ss
        .ps
        .ps_result_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecGroup: no result slot"));
    ExecClearTuple(slot);
    slot.values[..n].copy_from_slice(values);
    slot.isnull[..n].copy_from_slice(isnull);
    crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
    slot
}

/// PG `ExecEndGroup`: tear down the child.
pub fn exec_end_group(shared: Option<&Arc<SharedState>>, run: &mut GroupRun<'_>) {
    exec_end_node(shared, &mut run.child);
}

/// PG `ExecReScanGroup`: reset the group state. The child rescan is the caller's
/// responsibility.
pub fn exec_rescan_group(run: &mut GroupRun<'_>) {
    run.state.grp_done = false;
    run.first_tuple = None;
    if let Some(slot) = run.state.ss.ss_scan_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
}

/// Adjacent-tuple grouping equality on the key columns. Two rows are "equal" when
/// every key column compares equal under the column type's btree comparator, with
/// NULLs treated as equal to each other (PG `execTuplesMatch` semantics over the
/// grouping/uniq operators -- which are the type's `=` operators). Shared by
/// nodeUnique + nodeGroup.
pub fn grouping_equal(
    key_cols: &[i16],
    key_types: &[Oid],
    av: &[Datum],
    an: &[bool],
    bv: &[Datum],
    bn: &[bool],
) -> bool {
    for (i, &col) in key_cols.iter().enumerate() {
        let idx = (col - 1) as usize;
        let (a_null, b_null) = (an[idx], bn[idx]);
        if a_null || b_null {
            if a_null != b_null {
                return false; // one NULL, one not -> distinct
            }
            continue; // both NULL -> equal on this column
        }
        if type_cmp(key_types[i], av[idx], bv[idx]) != 0 {
            return false;
        }
    }
    true
}

/// 3-way compare two non-null datums of `typeid` via the type's btree comparator.
/// Mirrors execExpr's `type_cmp_proc` map (the seeded int/float/numeric/date
/// btree opclasses); the general type-cache path grows later.
fn type_cmp(typeid: Oid, a: Datum, b: Datum) -> i32 {
    let proc = match typeid {
        Oid::INT4OID => Oid::F_BTINT4CMP,
        Oid::INT2OID => Oid::F_BTINT2CMP,
        Oid::INT8OID => Oid::F_BTINT8CMP,
        Oid::OIDOID => Oid::F_BTOIDCMP,
        Oid::TEXTOID => Oid::F_BTTEXTCMP,
        Oid::FLOAT4OID => Oid::F_BTFLOAT4CMP,
        Oid::FLOAT8OID => Oid::F_BTFLOAT8CMP,
        Oid::NUMERICOID => Oid::F_NUMERIC_CMP,
        Oid::DATEOID => Oid::F_DATE_CMP,
        Oid::TIMESTAMPOID => Oid::F_TIMESTAMP_CMP,
        _ => {
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("grouping_equal: no comparison function for type {}", typeid.get())
            );
            unreachable!("elog!(ERROR) raises")
        }
    };
    let r = crate::fmgr::OidFunctionCall2(proc, a, b).unwrap_or_else(|| {
        crate::elog!(crate::utils::elog::ERROR, "grouping comparator returned NULL");
        unreachable!("elog!(ERROR) raises")
    });
    crate::postgres::DatumGetInt32(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt, TTSOpsVirtual};
    use crate::nodes::plannodes::{Group, Plan};
    use crate::postgres::Int32GetDatum;
    use std::sync::Arc;

    const INT4OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(23);
    const INVALID: crate::postgres_ext::Oid = crate::postgres_ext::InvalidOid;

    fn int4_desc(n: usize) -> TupleDesc {
        let mut d = TupleDescData::create_template(n as i32);
        for (i, name) in ["a", "b"].iter().take(n).enumerate() {
            d.init_builtin_entry((i + 1) as i16, name, INT4OID, -1, 0);
            d.init_entry_collation((i + 1) as i16, INVALID);
        }
        Arc::new(d)
    }

    fn make_source(desc: &TupleDesc, rows: Vec<Vec<i32>>) -> PlanStateNode<'static> {
        let slots: Vec<Box<TupleTableSlot>> = rows
            .into_iter()
            .map(|r| {
                let mut slot = make_tuple_table_slot(Some(Arc::clone(desc)), &TTSOpsVirtual);
                for (i, v) in r.iter().enumerate() {
                    slot.values[i] = Int32GetDatum(*v);
                    slot.isnull[i] = false;
                }
                exec_store_virtual_tuple(&mut slot);
                slot
            })
            .collect();
        PlanStateNode::test_tuple_source(Arc::clone(desc), slots)
    }

    fn group_node(num_cols: i32, colidx: Vec<i16>) -> Group {
        Group {
            plan: Plan {
                disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
                parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
                targetlist: Vec::new(), qual: Vec::new(), lefttree: None, righttree: None,
                init_plan: Vec::new(), ext_param: None, all_param: None,
            },
            num_cols,
            grp_col_idx: colidx,
            grp_operators: vec![crate::postgres_ext::Oid::new(96); num_cols as usize], // int4eq
            grp_collations: vec![INVALID; num_cols as usize],
        }
    }

    async fn run_group(node: Group, child: PlanStateNode<'static>, ncols: usize) -> Vec<Vec<i32>> {
        let mut estate = EState::default();
        let mut run = exec_init_group(&node, &mut estate, child);
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_group(None, &mut run)).await else { break };
            let row = (1..=ncols as i32)
                .map(|a| DatumGetInt32_opt(slot_getattr(slot, a)).expect("non-null"))
                .collect();
            out.push(row);
        }
        exec_end_group(None, &mut run);
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_boundaries_over_sorted_input() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![vec![1], vec![1], vec![2], vec![2], vec![2], vec![3]]);
        // one row per group = the first tuple of each group.
        assert_eq!(run_group(group_node(1, vec![1]), child, 1).await, vec![vec![1], vec![2], vec![3]]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_single_group() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![vec![5], vec![5], vec![5]]);
        assert_eq!(run_group(group_node(1, vec![1]), child, 1).await, vec![vec![5]]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_all_distinct() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![vec![1], vec![2], vec![3]]);
        assert_eq!(run_group(group_node(1, vec![1]), child, 1).await, vec![vec![1], vec![2], vec![3]]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_two_key_columns() {
        let desc = int4_desc(2);
        let child = make_source(&desc, vec![vec![1, 1], vec![1, 1], vec![1, 2], vec![2, 2]]);
        assert_eq!(
            run_group(group_node(2, vec![1, 2]), child, 2).await,
            vec![vec![1, 1], vec![1, 2], vec![2, 2]]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn group_empty_input() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![]);
        assert!(run_group(group_node(1, vec![1]), child, 1).await.is_empty());
    }
}
