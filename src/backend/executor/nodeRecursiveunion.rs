//! RecursiveUnion node executor. Translated from
//! backend/executor/nodeRecursiveunion.c (disposition: full leaf for the M12
//! recursive CTE; PG's hashed-dedup via BuildTupleHashTable is replaced by a
//! linear group scan, and the cross-node working-table handoff -- PG does it
//! through `es_param_exec_vals[wtParam]` + `chgParam` -- is done here via a shared
//! `WorkTableRef` registered on the EState).
//!
//! Algorithm (PG's, verbatim in structure):
//!   1. Evaluate the non-recursive term (outer) into the working table + result.
//!   2. Repeat: read the recursive term (inner, which scans the working table via a
//!      WorkTableScan), stash new rows into the intermediate table + return them;
//!      when the inner term is exhausted, swap working <- intermediate and continue
//!      until the intermediate table comes up empty.
//!
//! UNION (numCols>0) deduplicates against all rows seen; UNION ALL keeps duplicates.
//!
//! Async coloring: pulling a child reaches the table AM, so `ExecRecursiveUnion` is
//! `async` (rules.md s5). The working/intermediate tables are owned `Vec`s shared
//! with the WorkTableScan through an `Arc<Mutex<..>>` (Send); no lock is held across
//! a child `.await`.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::backend::executor::execProcnode::{
    exec_end_node, exec_proc_node, result_type_of, PlanStateNode,
};
use crate::backend::executor::nodeGroup::grouping_equal;
use crate::executor::tuptable::{ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, PlanState, ScanState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::RecursiveUnion;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// A buffered tuple row (deformed column datums + null flags).
pub type Row = (Vec<Datum>, Vec<bool>);

/// The current working table a recursive term's WorkTableScan reads. RecursiveUnion
/// fills it each iteration; the WorkTableScan clones rows out on each scan. Shared
/// (Send) so the worktable handle can live on both nodes (rules.md s8/per-task-Send).
pub type WorkTableRef = Arc<Mutex<Vec<Row>>>;

/// Run-state pairing the PG `RecursiveUnionState` shell with the two child
/// plan-states, the shared working table + the intermediate table.
pub struct RecursiveUnionRun<'rel> {
    pub ss: Box<ScanState>,
    /// non-recursive term (PG outer).
    pub left: Box<PlanStateNode<'rel>>,
    /// recursive term (PG inner); reads the working table via a WorkTableScan.
    pub right: Box<PlanStateNode<'rel>>,
    /// > 0 -> UNION (dedup on all columns); 0 -> UNION ALL.
    pub num_cols: i32,
    pub key_cols: Vec<i16>,
    pub key_types: Vec<Oid>,
    /// the shared current working table (WT) the recursive term scans.
    pub working_table: WorkTableRef,
    /// the next working table being built (PG intermediate_table).
    intermediate: Vec<Row>,
    /// false until the non-recursive term is drained.
    recursing: bool,
    /// rows seen so far, for UNION dedup.
    seen: Vec<Row>,
    /// true once intermediate had a row this round.
    intermediate_nonempty: bool,
}

/// PG `ExecInitRecursiveUnion`: register the shared working table on the EState
/// (keyed by `wt_param`) BEFORE the inner subtree is initialized, so the recursive
/// term's WorkTableScan can pick it up. The caller initializes the children around
/// this; `exec_init_node` registers the handle, inits the inner child, then builds
/// the run-state.
pub fn make_worktable_ref() -> WorkTableRef {
    Arc::new(Mutex::new(Vec::new()))
}

/// Build the RecursiveUnionState run over already-initialized children + the shared
/// working table. The result rowtype is the non-recursive term's rowtype.
pub fn exec_init_recursive_union<'rel>(
    node: &RecursiveUnion,
    estate: &mut EState<'rel>,
    left: PlanStateNode<'rel>,
    right: PlanStateNode<'rel>,
    working_table: WorkTableRef,
) -> Box<RecursiveUnionRun<'rel>> {
    let _ = estate;
    let outer_desc = result_type_of(&left)
        .unwrap_or_else(|| unimplemented!("ExecInitRecursiveUnion: non-recursive term has no rowtype"));

    let ncols = outer_desc.natts as usize;
    let key_cols: Vec<i16> = (1..=ncols as i16).collect();
    let key_types: Vec<Oid> = (0..ncols).map(|i| outer_desc.attr(i).atttypid).collect();

    let result_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    let mut ps = PlanState {
        plan: Some(Node::RecursiveUnion(Box::new(node.clone()))),
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

    Box::new(RecursiveUnionRun {
        ss: Box::new(ss),
        left: Box::new(left),
        right: Box::new(right),
        num_cols: node.num_cols,
        key_cols,
        key_types,
        working_table,
        intermediate: Vec::new(),
        recursing: false,
        seen: Vec::new(),
        intermediate_nonempty: false,
    })
}

impl RecursiveUnionRun<'_> {
    /// UNION dedup: returns true if `row` is new (recording it); false if already
    /// seen. For UNION ALL (num_cols == 0) every row is "new".
    fn is_new(&mut self, values: &[Datum], isnull: &[bool]) -> bool {
        if self.num_cols == 0 {
            return true;
        }
        let dup = self
            .seen
            .iter()
            .any(|s| grouping_equal(&self.key_cols, &self.key_types, values, isnull, &s.0, &s.1));
        if dup {
            return false;
        }
        self.seen.push((values.to_vec(), isnull.to_vec()));
        true
    }

    /// Store a row into the result slot and return a borrow of it.
    fn emit(&mut self, values: &[Datum], isnull: &[bool]) -> Option<&mut TupleTableSlot> {
        let slot = self
            .ss
            .ps
            .ps_result_tuple_slot
            .as_mut()
            .unwrap_or_else(|| unimplemented!("ExecRecursiveUnion: no result slot"));
        ExecClearTuple(slot);
        let n = values.len();
        slot.values[..n].copy_from_slice(values);
        slot.isnull[..n].copy_from_slice(isnull);
        crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
        self.ss.ps.ps_result_tuple_slot.as_deref_mut()
    }
}

/// Snapshot a child's reused slot into an owned row.
fn snapshot(s: &mut TupleTableSlot) -> Row {
    use crate::executor::tuptable::slot_getallattrs;
    slot_getallattrs(s);
    let n = s.nvalid.max(0) as usize;
    (s.values[..n].to_vec(), s.isnull[..n].to_vec())
}

/// PG `ExecRecursiveUnion`: return the next qualifying tuple.
pub async fn exec_recursive_union<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut RecursiveUnionRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    // 1. Non-recursive term: drain it into the working table + return each new row.
    if !run.recursing {
        loop {
            let row = match Box::pin(exec_proc_node(shared, &mut run.left)).await {
                None => break,
                Some(s) => snapshot(s),
            };
            if !run.is_new(&row.0, &row.1) {
                continue;
            }
            run.working_table.lock().push(row.clone());
            return run.emit(&row.0, &row.1);
        }
        run.recursing = true;
    }

    // 2. Recursive term: read the inner plan (scans WT), stash new rows into the
    //    intermediate table + return them. When the inner term is exhausted, swap
    //    intermediate -> working and continue until the intermediate stays empty.
    loop {
        let pulled = Box::pin(exec_proc_node(shared, &mut run.right)).await;
        match pulled {
            None => {
                // The recursive term is exhausted for this working table.
                if !run.intermediate_nonempty {
                    return None; // nothing produced -> done.
                }
                // intermediate becomes the new working table.
                let next = std::mem::take(&mut run.intermediate);
                {
                    let mut wt = run.working_table.lock();
                    wt.clear();
                    wt.extend(next);
                }
                run.intermediate_nonempty = false;
                // Re-run the recursive term over the fresh working table. The inner
                // WorkTableScan re-reads the shared handle on its next init/rescan;
                // here the inner node is stateful, so reset it for a fresh pass.
                crate::backend::executor::execProcnode::exec_rescan_node(&mut run.right);
            }
            Some(s) => {
                let row = snapshot(s);
                if !run.is_new(&row.0, &row.1) {
                    continue;
                }
                run.intermediate_nonempty = true;
                run.intermediate.push(row.clone());
                return run.emit(&row.0, &row.1);
            }
        }
    }
}

/// PG `ExecEndRecursiveUnion`: tear down both children.
pub fn exec_end_recursive_union(shared: Option<&Arc<SharedState>>, run: &mut RecursiveUnionRun<'_>) {
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
    use crate::nodes::plannodes::{Plan, RecursiveUnion};
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

    fn empty_plan() -> Plan {
        Plan {
            disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
            parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
            targetlist: Vec::new(), qual: Vec::new(), lefttree: None, righttree: None,
            init_plan: Vec::new(), ext_param: None, all_param: None,
        }
    }

    fn ru_node(num_cols: i32) -> RecursiveUnion {
        RecursiveUnion {
            plan: empty_plan(),
            wt_param: 0,
            num_cols,
            dup_col_idx: if num_cols > 0 { vec![1] } else { Vec::new() },
            dup_operators: if num_cols > 0 { vec![Oid::new(96)] } else { Vec::new() },
            dup_collations: if num_cols > 0 { vec![INVALID] } else { Vec::new() },
            num_groups: 0,
        }
    }

    /// Recursive union whose recursive term immediately yields nothing (an empty
    /// TupleSource). Only the non-recursive seed rows come out, and the recursion
    /// terminates on the first empty round. Verifies the base-case + bounded
    /// termination; the genuine incrementing 1..5 is exercised at the wire level.
    #[tokio::test(flavor = "multi_thread")]
    async fn recursive_union_base_only_terminates() {
        let desc = int4_desc();
        let wt = make_worktable_ref();
        let left = source(&desc, vec![1, 2, 3]);
        // Empty recursive term -> the loop ends after one empty round.
        let right = PlanStateNode::test_tuple_source(Arc::clone(&desc), Vec::new());
        let mut estate = EState::default();
        let mut run = exec_init_recursive_union(&ru_node(0), &mut estate, left, right, wt);
        let mut out = Vec::new();
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 1000, "recursive union did not terminate");
            let Some(slot) = Box::pin(exec_recursive_union(None, &mut run)).await else { break };
            out.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
        }
        assert_eq!(out, vec![1, 2, 3]);
        exec_end_recursive_union(None, &mut run);
    }
}
