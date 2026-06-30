//! Unique node executor. Translated from
//! backend/executor/nodeUnique.c (disposition: full for the M5 DISTINCT over
//! sorted input).
//!
//! Unique filters duplicates from a stream of SORTED tuples: the first row of each
//! group is returned, and subsequent rows that match the previous returned row on
//! the uniq columns are skipped. It does no projection or qual. Equality on the
//! key columns is computed by `grouping_equal` (the btree cmp of each key column's
//! type, NULLs equal -- PG's `execTuplesMatchPrepare` over the uniqOperators).
//!
//! Async coloring: the child drive reaches the table AM, so `ExecUnique` is
//! `async` (rules.md s5). No guard across the child `.await`.

use std::sync::Arc;

use crate::backend::executor::execProcnode::{exec_end_node, exec_proc_node, result_type_of, PlanStateNode};
use crate::backend::executor::nodeGroup::grouping_equal;
use crate::executor::tuptable::{tts_empty, ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, PlanState, UniqueState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::Unique;
use crate::shared_state::SharedState;

/// Run-state pairing the PG `UniqueState` with its child plan-state. The
/// result/"previous returned" slot is the node-owned `ps_result_tuple_slot`; PG
/// keeps the last returned tuple there (the duplicate-detection reference).
pub struct UniqueRun<'rel> {
    pub state: Box<UniqueState>,
    pub child: Box<PlanStateNode<'rel>>,
    /// the uniq column positions (1-based) + their types, resolved from the plan +
    /// child rowtype, for the equality test.
    pub key_cols: Vec<i16>,
    pub key_types: Vec<crate::postgres_ext::Oid>,
}

/// PG `ExecInitUnique`: build the UniqueState over an initialized child. The
/// result slot is a virtual slot of the child's rowtype. Unique does no projection.
pub fn exec_init_unique<'rel>(
    node: &Unique,
    estate: &mut EState<'rel>,
    child: PlanStateNode<'rel>,
) -> Box<UniqueRun<'rel>> {
    let _ = estate;
    let outer_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitUnique: child has no result descriptor"));

    let result_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    let key_cols = node.uniq_col_idx.clone();
    let key_types = key_cols
        .iter()
        .map(|&c| outer_desc.attr((c - 1) as usize).atttypid)
        .collect();

    let mut ps = PlanState {
        plan: Some(Node::Unique(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_result_tuple_desc = Some(outer_desc);
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.ps_proj_info = None;

    Box::new(UniqueRun {
        state: Box::new(UniqueState { ps, eqfunction: None }),
        child: Box::new(child),
        key_cols,
        key_types,
    })
}

/// PG `ExecUnique`: return the next non-duplicate row. The first row of each group
/// is returned (and saved as the comparison reference); later rows equal to it on
/// the key columns are skipped.
pub async fn exec_unique<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut UniqueRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    loop {
        let copied = {
            let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await else {
                // end of subplan.
                let result = run.state.ps.ps_result_tuple_slot.as_mut()
                    .unwrap_or_else(|| unimplemented!("ExecUnique: no result slot"));
                ExecClearTuple(result);
                return None;
            };
            snapshot_slot(slot)
        };

        // Always return the first tuple of the stream (result slot empty).
        let first = run
            .state
            .ps
            .ps_result_tuple_slot
            .as_ref()
            .is_none_or(|s| tts_empty(s));

        if !first {
            // Equal to the previously returned row on the key columns -> skip.
            let prev = run.state.ps.ps_result_tuple_slot.as_ref()
                .unwrap_or_else(|| unimplemented!("ExecUnique: no result slot"));
            if grouping_equal(&run.key_cols, &run.key_types, &copied.0, &copied.1, &prev.values, &prev.isnull) {
                continue;
            }
        }

        // New distinct tuple: store it in the result slot and return it.
        let result = run.state.ps.ps_result_tuple_slot.as_mut()
            .unwrap_or_else(|| unimplemented!("ExecUnique: no result slot"));
        ExecClearTuple(result);
        let n = copied.0.len();
        result.values[..n].copy_from_slice(&copied.0);
        result.isnull[..n].copy_from_slice(&copied.1);
        crate::backend::executor::execTuples::exec_store_virtual_tuple(result);
        return Some(result);
    }
}

/// Deform a child slot into owned (values, isnull) vectors (the child reuses its
/// slot; the snapshot lets us compare/store without holding the child borrow).
pub(crate) fn snapshot_slot(slot: &mut TupleTableSlot) -> (Vec<crate::postgres::Datum>, Vec<bool>) {
    use crate::executor::tuptable::slot_getallattrs;
    slot_getallattrs(slot);
    let n = slot.nvalid.max(0) as usize;
    (slot.values[..n].to_vec(), slot.isnull[..n].to_vec())
}

/// PG `ExecEndUnique`: tear down the child.
pub fn exec_end_unique(shared: Option<&Arc<SharedState>>, run: &mut UniqueRun<'_>) {
    exec_end_node(shared, &mut run.child);
}

/// PG `ExecReScanUnique`: clear the reference row so the first input tuple is
/// returned again. The child rescan is the caller's responsibility.
pub fn exec_rescan_unique(run: &mut UniqueRun<'_>) {
    if let Some(slot) = run.state.ps.ps_result_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt, TTSOpsVirtual};
    use crate::nodes::plannodes::{Plan, Unique};
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

    fn unique_node(num_cols: i32, colidx: Vec<i16>) -> Unique {
        Unique {
            plan: Plan {
                disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
                parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
                targetlist: Vec::new(), qual: Vec::new(), lefttree: None, righttree: None,
                init_plan: Vec::new(), ext_param: None, all_param: None,
            },
            num_cols,
            uniq_col_idx: colidx,
            uniq_operators: vec![crate::postgres_ext::Oid::new(96); num_cols as usize], // int4eq
            uniq_collations: vec![INVALID; num_cols as usize],
        }
    }

    async fn run_unique(node: Unique, child: PlanStateNode<'static>, ncols: usize) -> Vec<Vec<i32>> {
        let mut estate = EState::default();
        let mut run = exec_init_unique(&node, &mut estate, child);
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_unique(None, &mut run)).await else { break };
            let row = (1..=ncols as i32)
                .map(|a| DatumGetInt32_opt(slot_getattr(slot, a)).expect("non-null"))
                .collect();
            out.push(row);
        }
        exec_end_unique(None, &mut run);
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distinct_over_sorted_dups() {
        let desc = int4_desc(1);
        // sorted input with duplicates.
        let child = make_source(&desc, vec![vec![1], vec![1], vec![2], vec![3], vec![3], vec![3]]);
        assert_eq!(run_unique(unique_node(1, vec![1]), child, 1).await, vec![vec![1], vec![2], vec![3]]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distinct_no_dups_passthrough() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![vec![1], vec![2], vec![3]]);
        assert_eq!(run_unique(unique_node(1, vec![1]), child, 1).await, vec![vec![1], vec![2], vec![3]]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distinct_two_key_columns() {
        let desc = int4_desc(2);
        let child = make_source(&desc, vec![vec![1, 1], vec![1, 1], vec![1, 2], vec![2, 2]]);
        assert_eq!(
            run_unique(unique_node(2, vec![1, 2]), child, 2).await,
            vec![vec![1, 1], vec![1, 2], vec![2, 2]]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn distinct_empty_input() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![]);
        assert!(run_unique(unique_node(1, vec![1]), child, 1).await.is_empty());
    }
}
