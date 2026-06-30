//! CteScan node executor. Translated from backend/executor/nodeCtescan.c
//! (disposition: full leaf for the M12 materialize-once CTE scan; PG's shared
//! CteState leader/reader split -- where one CteScan runs the CTE plan into a
//! tuplestore and sibling scans read the same store via a second read pointer -- is
//! simplified here so each CteScan owns and runs its own copy of the CTE subplan).
//!
//! PG threads the CTE plan through `EState.es_param_exec_vals[cteParam]` to a
//! leader CteScan; this port keeps no es_subplanstates registry yet, so the planner
//! embeds the CTE subplan as the CteScan's `lefttree` and each scan materializes it
//! independently (a CTE referenced twice runs twice -- correct, not yet shared).
//!
//! Async coloring: running the CTE subplan reaches the table AM, so `ExecCteScan` is
//! `async` (rules.md s5). The materialized rows are owned `Vec`s (Send).

use std::sync::Arc;

use crate::backend::executor::execProcnode::{
    exec_end_node, exec_proc_node, result_type_of, PlanStateNode,
};
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, PlanState, ScanState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::CteScan;
use crate::postgres::Datum;
use crate::shared_state::SharedState;

/// Run-state pairing the PG `CteScanState` shell with the CTE subplan state and the
/// materialized rows (filled on the first scan).
pub struct CteScanRun<'rel> {
    pub ss: Box<ScanState>,
    /// the CTE subplan (PG runs this once into the shared CteState tuplestore).
    pub cteplan: Box<PlanStateNode<'rel>>,
    /// materialized CTE output (lazily filled).
    rows: Option<Vec<(Vec<Datum>, Vec<bool>)>>,
    /// read cursor into `rows`.
    cur: usize,
}

/// PG `ExecInitCteScan`: build the CteScanState over the initialized CTE subplan.
/// The result rowtype is the CTE subplan's rowtype.
pub fn exec_init_cte_scan<'rel>(
    node: &CteScan,
    estate: &mut EState<'rel>,
    cteplan: PlanStateNode<'rel>,
) -> Box<CteScanRun<'rel>> {
    let _ = estate;
    let outer_desc = result_type_of(&cteplan)
        .unwrap_or_else(|| unimplemented!("ExecInitCteScan: CTE subplan has no result descriptor"));

    let result_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    let mut ps = PlanState {
        plan: Some(Node::CteScan(Box::new(node.clone()))),
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

    Box::new(CteScanRun {
        ss: Box::new(ss),
        cteplan: Box::new(cteplan),
        rows: None,
        cur: 0,
    })
}

/// PG `ExecCteScan`: on the first call run the CTE subplan to completion into the
/// materialized buffer; thereafter serve rows from the buffer.
pub async fn exec_cte_scan<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut CteScanRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    if run.rows.is_none() {
        use crate::executor::tuptable::slot_getallattrs;
        let mut rows = Vec::new();
        loop {
            match Box::pin(exec_proc_node(shared, &mut run.cteplan)).await {
                None => break,
                Some(s) => {
                    slot_getallattrs(s);
                    let n = s.nvalid.max(0) as usize;
                    rows.push((s.values[..n].to_vec(), s.isnull[..n].to_vec()));
                }
            }
        }
        run.rows = Some(rows);
    }

    let rows = run.rows.as_ref().unwrap_or_else(|| unreachable!("rows materialized"));
    if run.cur >= rows.len() {
        return None;
    }
    let (values, isnull) = (rows[run.cur].0.clone(), rows[run.cur].1.clone());
    run.cur += 1;

    let slot = run
        .ss
        .ps
        .ps_result_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecCteScan: no result slot"));
    ExecClearTuple(slot);
    let n = values.len();
    slot.values[..n].copy_from_slice(&values);
    slot.isnull[..n].copy_from_slice(&isnull);
    crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
    run.ss.ps.ps_result_tuple_slot.as_deref_mut()
}

/// PG `ExecEndCteScan`: tear down the CTE subplan.
pub fn exec_end_cte_scan(shared: Option<&Arc<SharedState>>, run: &mut CteScanRun<'_>) {
    exec_end_node(shared, &mut run.cteplan);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt, TTSOpsVirtual};
    use crate::nodes::plannodes::{Plan, Scan};
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

    fn ctescan_node() -> CteScan {
        CteScan {
            scan: Scan {
                plan: Plan {
                    disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
                    parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
                    targetlist: Vec::new(), qual: Vec::new(), lefttree: None, righttree: None,
                    init_plan: Vec::new(), ext_param: None, all_param: None,
                },
                scanrelid: 1,
            },
            cte_plan_id: 0,
            cte_param: 0,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ctescan_materializes_and_serves() {
        let desc = int4_desc();
        let mut estate = EState::default();
        let mut run = exec_init_cte_scan(&ctescan_node(), &mut estate, source(&desc, vec![5, 6, 7]));
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_cte_scan(None, &mut run)).await else { break };
            out.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
        }
        assert_eq!(out, vec![5, 6, 7]);
        exec_end_cte_scan(None, &mut run);
    }
}
