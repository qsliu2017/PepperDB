//! Append node executor. Translated from backend/executor/nodeAppend.c
//! (disposition: full leaf for the M12 in-order concatenation; the async-append /
//! partition-pruning / parallel-Append machinery of PG is staged).
//!
//! `ExecAppend` runs each subplan in turn, concatenating their output. It is the
//! UNION ALL executor (the planner stacks an Append over the branch subplans). The
//! subplans are held in the run-state's `subplans` vector (PG `appendplans`); the
//! node walks them left to right, exhausting each before advancing.
//!
//! Async coloring: pulling a subplan reaches the table AM, so `ExecAppend` is
//! `async` (rules.md s5). The subplan states are owned `Box`es (Send); no guard
//! across a child `.await`.

use std::sync::Arc;

use crate::backend::executor::execProcnode::{
    exec_end_node, exec_proc_node, result_type_of, PlanStateNode,
};
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, PlanState, ScanState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::Append;
use crate::shared_state::SharedState;

/// Run-state pairing the PG `AppendState` shell (held as a `ScanState` for the
/// result slot/desc) with the owned child subplan states and the current cursor.
pub struct AppendRun<'rel> {
    pub ss: Box<ScanState>,
    /// the subplan states, run left to right (PG `appendplans`).
    pub subplans: Vec<PlanStateNode<'rel>>,
    /// index of the subplan currently being drained.
    pub which: usize,
}

/// PG `ExecInitAppend`: build the AppendState over already-initialized children.
/// The result rowtype is the first subplan's rowtype (the branches share a rowtype,
/// reconciled by the planner). Append does no projection; it forwards child tuples.
pub fn exec_init_append<'rel>(
    node: &Append,
    estate: &mut EState<'rel>,
    children: Vec<PlanStateNode<'rel>>,
) -> Box<AppendRun<'rel>> {
    let _ = (node, estate);
    let outer_desc = children
        .first()
        .and_then(result_type_of)
        .unwrap_or_else(|| unimplemented!("ExecInitAppend: an Append needs at least one subplan"));

    let result_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    let mut ps = PlanState {
        plan: Some(Node::Append(Box::new(node.clone()))),
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

    Box::new(AppendRun {
        ss: Box::new(ss),
        subplans: children,
        which: 0,
    })
}

/// PG `ExecAppend`: return the next tuple, advancing through subplans in order.
pub async fn exec_append<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut AppendRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    while run.which < run.subplans.len() {
        // Pull from the current subplan; snapshot it into the result slot so the
        // returned borrow is over the node-owned slot (PG copies into ps_ResultTupleSlot).
        let copied = match Box::pin(exec_proc_node(shared, &mut run.subplans[run.which])).await {
            None => None,
            Some(s) => {
                use crate::executor::tuptable::slot_getallattrs;
                slot_getallattrs(s);
                let n = s.nvalid.max(0) as usize;
                Some((s.values[..n].to_vec(), s.isnull[..n].to_vec()))
            }
        };
        match copied {
            None => {
                // Current subplan exhausted -> advance.
                run.which += 1;
            }
            Some((values, isnull)) => {
                let slot = run
                    .ss
                    .ps
                    .ps_result_tuple_slot
                    .as_mut()
                    .unwrap_or_else(|| unimplemented!("ExecAppend: no result slot"));
                ExecClearTuple(slot);
                let n = values.len();
                slot.values[..n].copy_from_slice(&values);
                slot.isnull[..n].copy_from_slice(&isnull);
                crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
                return run.ss.ps.ps_result_tuple_slot.as_deref_mut();
            }
        }
    }

    None
}

/// PG `ExecEndAppend`: tear down every subplan.
pub fn exec_end_append(shared: Option<&Arc<SharedState>>, run: &mut AppendRun<'_>) {
    for sub in &mut run.subplans {
        exec_end_node(shared, sub);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt, TTSOpsVirtual};
    use crate::nodes::plannodes::Plan;
    use crate::postgres::Int32GetDatum;

    const INT4OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(23);
    const INVALID: crate::postgres_ext::Oid = crate::postgres_ext::InvalidOid;

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

    fn append_node() -> Append {
        Append {
            plan: Plan {
                disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
                parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
                targetlist: Vec::new(), qual: Vec::new(), lefttree: None, righttree: None,
                init_plan: Vec::new(), ext_param: None, all_param: None,
            },
            apprelids: None,
            appendplans: Vec::new(),
            nasyncplans: 0,
            first_partial_plan: 0,
            part_prune_index: -1,
        }
    }

    async fn drain(run: &mut AppendRun<'static>) -> Vec<i32> {
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_append(None, run)).await else { break };
            out.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn append_concatenates_in_order() {
        let desc = int4_desc();
        let children = vec![source(&desc, vec![1, 2]), source(&desc, vec![3]), source(&desc, vec![4, 5])];
        let mut estate = EState::default();
        let mut run = exec_init_append(&append_node(), &mut estate, children);
        assert_eq!(drain(&mut run).await, vec![1, 2, 3, 4, 5]);
        exec_end_append(None, &mut run);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn append_keeps_duplicates() {
        // UNION ALL semantics: no dedup across branches.
        let desc = int4_desc();
        let children = vec![source(&desc, vec![1, 1]), source(&desc, vec![1])];
        let mut estate = EState::default();
        let mut run = exec_init_append(&append_node(), &mut estate, children);
        assert_eq!(drain(&mut run).await, vec![1, 1, 1]);
        exec_end_append(None, &mut run);
    }
}
