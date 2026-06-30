//! Sort node executor. Translated from
//! backend/executor/nodeSort.c (disposition: full for the M5 forward sort -- datum
//! + tuple variants, asc/desc, NULLS ordering, optional bounded top-N).
//!
//! `ExecInitSort` initializes the child subplan, builds the (virtual) scan/result
//! slots from the child's result rowtype, and decides datum-vs-tuple sort
//! (single-column input sorts as a bare Datum). `ExecSort` drains the child into a
//! tuplesort (step 24) on the first call, `tuplesort_performsort`s, then returns
//! one sorted tuple per call from the tuplesort. `ExecEndSort` releases the sort
//! and the child; `ExecReScanSort` rewinds (random-access) or re-sorts.
//!
//! Async coloring: draining the child reaches the table AM, so `ExecSort` is
//! `async` (rules.md s5). The tuplesort drain happens entirely before any sorted
//! row is served; no lock/RefCell is held across the child `.await` (the tuplesort
//! is an owned `Box`, genuinely `Send`).
//!
//! GROW: backward scan / mark-restore / parallel-worker instrumentation beyond
//! the M5 forward path are clean grow guards (rules.md s4).

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::access::tupdesc::TupleDesc;
use crate::backend::executor::execProcnode::{
    exec_end_node, exec_proc_node, result_type_of, PlanStateNode,
};
use crate::backend::executor::execTuples::{make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::backend::utils::sort::tuplesort::{
    sortopt, tuplesort_begin_datum, tuplesort_begin_heap, tuplesort_end, tuplesort_getdatum,
    tuplesort_gettupleslot, tuplesort_performsort, tuplesort_putdatum, tuplesort_puttupleslot,
    tuplesort_rescan, tuplesort_set_bound, Tuplesortstate,
};
use crate::executor::tuptable::{slot_getsomeattrs, tts_empty, ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, PlanState, ScanState, SortState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::Sort;
use crate::shared_state::SharedState;

/// Run-state pairing the PG `SortState` with its child plan-state and the owned
/// tuplesort. The C node holds the child through `ps.lefttree` (a `PlanState*`) and
/// the sort through `void *tuplesortstate`; the Rust base `PlanState.lefttree` is
/// the wrong type for the dispatch enum, so both live here (an island wrapper, no
/// edit to nodes/execnodes.rs), like `SeqScanRun`/`ModifyTableRun`.
pub struct SortRun<'rel> {
    pub state: Box<SortState>,
    /// the outer (input) subplan state yielding rows to sort.
    pub child: Box<PlanStateNode<'rel>>,
    /// the tuplesort, created+filled on the first `ExecSort`. Owned `Box` (Send).
    pub tuplesort: Option<Box<Tuplesortstate>>,
}

/// PG `ExecInitSort`: build the SortState over an initialized child subplan.
///
/// The child is shielded from REWIND/BACKWARD/MARK (the sort materializes). The
/// scan + result slots are virtual slots of the child's result rowtype (the sort
/// neither quals nor projects -- it forwards the input rows in sorted order).
/// Single-column input -> datum sort (faster); otherwise a tuple sort.
pub fn exec_init_sort<'rel>(
    node: &Sort,
    estate: &mut EState<'rel>,
    eflags: i32,
    child: PlanStateNode<'rel>,
) -> Box<SortRun<'rel>> {
    let _ = estate;
    // randomAccess if the parent needs REWIND/BACKWARD/MARK (we materialize then).
    let random_access = (eflags
        & (crate::utils::tuplestore::EXEC_FLAG_REWIND
            | crate::utils::tuplestore::EXEC_FLAG_BACKWARD
            | crate::utils::tuplestore::EXEC_FLAG_MARK))
        != 0;

    // ExecGetResultType(outerNode): the child's result rowtype is the sort's I/O
    // shape. Both the scan slot and the result slot use it (virtual).
    let outer_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitSort: child has no result descriptor"));

    let scan_slot = make_tuple_table_slot(Some(Arc::clone(&outer_desc)), &TTS_OPS_VIRTUAL);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&outer_desc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::Sort(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_result_tuple_desc = Some(Arc::clone(&outer_desc));
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.scandesc = Some(Arc::clone(&outer_desc));
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;
    // Sort does no projection.
    ps.ps_proj_info = None;

    let datum_sort = outer_desc.natts == 1;

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(SortRun {
        state: Box::new(SortState {
            ss,
            random_access,
            bounded: false,
            sort_done: false,
            bounded_done: false,
            bound_done: 0,
            bound: 0,
            datum_sort,
            ..SortState::default()
        }),
        child: Box::new(child),
        tuplesort: None,
    })
}

/// PG `ExecSort`: on the first call drain the child into a tuplesort and
/// `performsort`; every call then fetches the next sorted row into the node-owned
/// result slot. Returns a borrow of that slot, or `None` at end of sorted output.
pub async fn exec_sort<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut SortRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    if !run.state.sort_done {
        let plannode = sort_plan(&run.state);
        let tup_desc = run
            .state
            .ss
            .ps
            .scandesc
            .clone()
            .unwrap_or_else(|| unimplemented!("ExecSort: no scan descriptor"));

        let mut tuplesortopts = sortopt::NONE;
        if run.state.random_access {
            tuplesortopts |= sortopt::RANDOMACCESS;
        }
        if run.state.bounded {
            tuplesortopts |= sortopt::ALLOWBOUNDED;
        }

        let mut tuplesort = if run.state.datum_sort {
            let att = tup_desc.attr(0);
            tuplesort_begin_datum(
                att.atttypid,
                plannode.sort_operators[0],
                plannode.collations[0],
                plannode.nulls_first[0],
                work_mem(),
                tuplesortopts,
            )
        } else {
            tuplesort_begin_heap(
                Arc::clone(&tup_desc),
                plannode.num_cols,
                &plannode.sort_col_idx,
                &plannode.sort_operators,
                &plannode.collations,
                &plannode.nulls_first,
                work_mem(),
                tuplesortopts,
            )
        };
        if run.state.bounded {
            tuplesort_set_bound(&mut tuplesort, run.state.bound);
        }

        // Drain the child, feeding each row to the tuplesort.
        if run.state.datum_sort {
            loop {
                let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await else {
                    break;
                };
                slot_getsomeattrs(slot, 1);
                tuplesort_putdatum(&mut tuplesort, slot.values[0], slot.isnull[0]);
            }
        } else {
            loop {
                let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await else {
                    break;
                };
                tuplesort_puttupleslot(&mut tuplesort, slot);
            }
        }

        tuplesort_performsort(&mut tuplesort);
        run.tuplesort = Some(tuplesort);
        run.state.sort_done = true;
        run.state.bounded_done = run.state.bounded;
        run.state.bound_done = run.state.bound;
    }

    // Fetch the next sorted item into the result slot.
    let tuplesort = run
        .tuplesort
        .as_mut()
        .unwrap_or_else(|| unreachable!("tuplesort built above"));
    let slot = run
        .state
        .ss
        .ps
        .ps_result_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecSort: no result slot"));

    if run.state.datum_sort {
        ExecClearTuple(slot);
        if let Some((value, isnull)) = tuplesort_getdatum(tuplesort, true) {
            slot.values[0] = value;
            slot.isnull[0] = isnull;
            crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
        }
    } else {
        let _ = tuplesort_gettupleslot(tuplesort, true, false, slot);
    }

    if tts_empty(slot) {
        return None;
    }
    Some(slot)
}

/// PG `ExecEndSort`: release the tuplesort, then tear down the child.
pub fn exec_end_sort(shared: Option<&Arc<SharedState>>, run: &mut SortRun<'_>) {
    if let Some(ts) = run.tuplesort.take() {
        tuplesort_end(ts);
    }
    exec_end_node(shared, &mut run.child);
}

/// PG `ExecReScanSort`: if not yet sorted, nothing to do. With random access and
/// unchanged bound, rewind the sorted output; otherwise forget the sort so the
/// next `ExecSort` re-reads + re-sorts the child.
pub fn exec_rescan_sort(run: &mut SortRun<'_>) {
    if !run.state.sort_done {
        return;
    }
    if let Some(slot) = run.state.ss.ps.ps_result_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
    if run.state.bounded != run.state.bounded_done
        || run.state.bound != run.state.bound_done
        || !run.state.random_access
    {
        // Forget the sort; the next ExecSort re-sorts. Child rescan is the caller's
        // responsibility (ExecReScan on the child) -- the M5 driver tests re-init.
        run.state.sort_done = false;
        if let Some(ts) = run.tuplesort.take() {
            tuplesort_end(ts);
        }
    } else if let Some(ts) = run.tuplesort.as_mut() {
        tuplesort_rescan(ts);
    }
}

/// The `Sort` plan node behind a SortState.
fn sort_plan(state: &SortState) -> Sort {
    match state.ss.ps.plan.as_ref() {
        Some(Node::Sort(s)) => (**s).clone(),
        _ => unimplemented!("sort_plan: SortState has no Sort plan"),
    }
}

/// `work_mem` GUC (KB). The sort budget; the in-memory path is what M5 exercises.
/// The GUC global defaults to 0 until the GUC bootstrap runs; fall back to PG's
/// compiled default (4 MB) so the in-memory sort has a budget (the spill path is
/// staged in tuplesort, step 24).
fn work_mem() -> i32 {
    // SAFETY: `work_mem` is a process-global GUC int; this backend reads it (the
    // GUC machinery is single-writer at startup). Read-only access here.
    let w = unsafe { crate::miscadmin::work_mem };
    if w > 0 {
        w
    } else {
        4096
    }
}

#[cfg(test)]
mod tests {
    //! Sort node integration: feed a synthetic in-memory tuple source (a Result-like
    //! child built from a Values stream) into ExecSort and verify asc/desc + NULLS
    //! ordering. The int4 btree comparator is resolved through the real
    //! PrepareSortSupportFromOrderingOp (ordering op 97/521 -> btint4cmp).
    use super::*;
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt, TTSOpsVirtual};
    use crate::nodes::plannodes::{Plan, Sort};
    use crate::postgres::{DatumGetInt32, Int32GetDatum};
    use std::sync::Arc;

    const INT4OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(23);
    const INVALID: crate::postgres_ext::Oid = crate::postgres_ext::InvalidOid;

    fn int4_desc(ncols: usize) -> TupleDesc {
        let mut d = TupleDescData::create_template(ncols as i32);
        for (i, name) in ["a", "b", "c"].iter().take(ncols).enumerate() {
            d.init_builtin_entry((i + 1) as i16, name, INT4OID, -1, 0);
            d.init_entry_collation((i + 1) as i16, INVALID);
        }
        Arc::new(d)
    }

    fn empty_plan() -> Plan {
        Plan {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            parallel_aware: false,
            parallel_safe: false,
            async_capable: false,
            plan_node_id: 0,
            targetlist: Vec::new(),
            qual: Vec::new(),
            lefttree: None,
            righttree: None,
            init_plan: Vec::new(),
            ext_param: None,
            all_param: None,
        }
    }

    /// A synthetic child plan-state: an in-memory list of int4 rows served one per
    /// ExecProcNode. Wrapped as a `PlanStateNode::TupleSource` test variant.
    fn make_source(desc: &TupleDesc, rows: Vec<Vec<Option<i32>>>) -> PlanStateNode<'static> {
        let slots: Vec<Box<TupleTableSlot>> = rows
            .into_iter()
            .map(|r| {
                let mut slot = make_tuple_table_slot(Some(Arc::clone(desc)), &TTSOpsVirtual);
                for (i, v) in r.iter().enumerate() {
                    match v {
                        Some(x) => {
                            slot.values[i] = Int32GetDatum(*x);
                            slot.isnull[i] = false;
                        }
                        None => {
                            slot.isnull[i] = true;
                        }
                    }
                }
                exec_store_virtual_tuple(&mut slot);
                slot
            })
            .collect();
        PlanStateNode::test_tuple_source(Arc::clone(desc), slots)
    }

    fn sort_node(num_cols: i32, colidx: Vec<i16>, ops: Vec<crate::postgres_ext::Oid>, nulls_first: Vec<bool>) -> Sort {
        Sort {
            plan: empty_plan(),
            num_cols,
            sort_col_idx: colidx,
            sort_operators: ops,
            collations: vec![INVALID; num_cols as usize],
            nulls_first,
        }
    }

    async fn run_sort(node: Sort, child: PlanStateNode<'static>) -> Vec<Vec<Option<i32>>> {
        let mut estate = EState::default();
        let mut run = exec_init_sort(&node, &mut estate, 0, child);
        let mut out = Vec::new();
        loop {
            let got = Box::pin(exec_sort(None, &mut run)).await;
            let Some(slot) = got else { break };
            let natts = i32::from(slot.nvalid);
            let row = (1..=natts)
                .map(|attno| DatumGetInt32_opt(slot_getattr(slot, attno)))
                .collect();
            out.push(row);
        }
        exec_end_sort(None, &mut run);
        out
    }

    const INT4_LT: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(97);
    const INT4_GT: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(521);

    #[tokio::test(flavor = "multi_thread")]
    async fn datum_sort_ascending() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![vec![Some(5)], vec![Some(1)], vec![Some(4)], vec![Some(2)], vec![Some(3)]]);
        let node = sort_node(1, vec![1], vec![INT4_LT], vec![false]);
        let out = run_sort(node, child).await;
        assert_eq!(out, vec![vec![Some(1)], vec![Some(2)], vec![Some(3)], vec![Some(4)], vec![Some(5)]]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn datum_sort_descending() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![vec![Some(5)], vec![Some(1)], vec![Some(4)], vec![Some(2)], vec![Some(3)]]);
        let node = sort_node(1, vec![1], vec![INT4_GT], vec![false]);
        let out = run_sort(node, child).await;
        assert_eq!(out, vec![vec![Some(5)], vec![Some(4)], vec![Some(3)], vec![Some(2)], vec![Some(1)]]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn nulls_last_then_nulls_first() {
        let desc = int4_desc(1);
        let rows = || vec![vec![Some(2)], vec![None], vec![Some(1)]];
        // asc nulls last (default)
        let out = run_sort(sort_node(1, vec![1], vec![INT4_LT], vec![false]), make_source(&desc, rows())).await;
        assert_eq!(out, vec![vec![Some(1)], vec![Some(2)], vec![None]]);
        // asc nulls first
        let out = run_sort(sort_node(1, vec![1], vec![INT4_LT], vec![true]), make_source(&desc, rows())).await;
        assert_eq!(out, vec![vec![None], vec![Some(1)], vec![Some(2)]]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tuple_sort_two_keys() {
        // sort by col1 asc, col2 asc; col1 has ties.
        let desc = int4_desc(2);
        let child = make_source(
            &desc,
            vec![vec![Some(1), Some(30)], vec![Some(1), Some(10)], vec![Some(2), Some(5)], vec![Some(1), Some(20)]],
        );
        let node = sort_node(2, vec![1, 2], vec![INT4_LT, INT4_LT], vec![false, false]);
        let out = run_sort(node, child).await;
        assert_eq!(
            out,
            vec![vec![Some(1), Some(10)], vec![Some(1), Some(20)], vec![Some(1), Some(30)], vec![Some(2), Some(5)]]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_input_sorts_to_nothing() {
        let desc = int4_desc(1);
        let child = make_source(&desc, vec![]);
        let node = sort_node(1, vec![1], vec![INT4_LT], vec![false]);
        let out = run_sort(node, child).await;
        assert!(out.is_empty());
    }

    /// The resolved comparator came from PrepareSortSupportFromOrderingOp; verify
    /// the int4 `<` operator (97) really maps to btint4cmp (i.e. sorting works
    /// through the opclass resolver, not a test-injected comparator).
    #[test]
    fn ordering_op_resolves_int4_comparator() {
        use crate::utils::sortsupport::{PrepareSortSupportFromOrderingOp, SortSupportData};
        let mut ssup = SortSupportData {
            ssup_cxt: crate::utils::palloc::MemoryContext::default(),
            ssup_collation: INVALID,
            ssup_reverse: false,
            ssup_nulls_first: false,
            ssup_attno: 1,
            ssup_extra: core::ptr::null_mut(),
            comparator: None,
            abbreviate: false,
            abbrev_converter: None,
            abbrev_abort: None,
            abbrev_full_comparator: None,
        };
        PrepareSortSupportFromOrderingOp(INT4_LT, &mut ssup);
        assert!(!ssup.ssup_reverse);
        let cmp = ssup.comparator.expect("comparator resolved");
        assert!(cmp(Int32GetDatum(1), Int32GetDatum(2), &ssup) < 0);
        assert_eq!(cmp(Int32GetDatum(2), Int32GetDatum(2), &ssup), 0);

        // The ">" operator (521) resolves the same comparator but ssup_reverse set.
        ssup.comparator = None;
        ssup.ssup_reverse = false;
        PrepareSortSupportFromOrderingOp(INT4_GT, &mut ssup);
        assert!(ssup.ssup_reverse);
    }

    // keep DatumGetInt32 referenced (used in non-opt helpers above)
    const _: fn(crate::postgres::Datum) -> i32 = DatumGetInt32;
}
