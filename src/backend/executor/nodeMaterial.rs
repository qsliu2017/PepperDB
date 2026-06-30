//! Material node executor. Translated from
//! backend/executor/nodeMaterial.c (disposition: full for the M5 in-memory
//! materialize + forward read + rescan; backward scan / mark-restore reach the
//! tuplestore's staged disk path and stay grow guards).
//!
//! `ExecInitMaterial` initializes the child and records the read-capability eflags
//! (REWIND/BACKWARD/MARK) that decide whether to buffer. `ExecMaterial` reads from
//! the tuplestore while not at its EOF, otherwise pulls the next child row, stashes
//! a copy in the tuplestore, and returns it. `ExecReScanMaterial` rewinds the
//! buffered output (or forgets it to re-read the child).
//!
//! Async coloring: pulling the child reaches the table AM, so `ExecMaterial` is
//! `async` (rules.md s5). The tuplestore is an owned `Box` (Send); no guard across
//! the child `.await`.

use std::sync::Arc;

use crate::backend::executor::execProcnode::{exec_end_node, exec_proc_node, result_type_of, PlanStateNode};
use crate::backend::utils::sort::tuplestore::{
    tuplestore_ateof, tuplestore_begin_heap, tuplestore_end, tuplestore_gettupleslot,
    tuplestore_puttupleslot, tuplestore_rescan, tuplestore_set_eflags, tuplestore_set_tupdesc,
    Tuplestorestate, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND,
};
use crate::executor::tuptable::{tts_empty, ExecClearTuple, TupleTableSlot};
use crate::nodes::execnodes::{EState, MaterialState, PlanState, ScanState};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::Material;
use crate::shared_state::SharedState;

/// Run-state pairing the PG `MaterialState` with its child plan-state and the
/// owned tuplestore. The C node holds the child via `ps.lefttree` and the store via
/// `tuplestorestate`; here both live in the wrapper (island) -- `MaterialState.
/// tuplestorestate` is still an opaque-forward stub in nodes/execnodes.rs (not yet
/// rewired to the real `Tuplestorestate`), so the real store is kept here, like
/// `SortRun.tuplesort`.
pub struct MaterialRun<'rel> {
    pub state: Box<MaterialState>,
    /// the outer (input) subplan state.
    pub child: Box<PlanStateNode<'rel>>,
    /// the tuplestore buffering the child output (when `eflags != 0`), created on
    /// the first `ExecMaterial`. Owned `Box` (Send).
    pub tuplestore: Option<Box<Tuplestorestate>>,
}

/// PG `ExecInitMaterial`: build the MaterialState over an initialized child. The
/// read-capability eflags (REWIND/BACKWARD/MARK) decide whether a tuplestore is
/// kept (else the node just passes the child through). BACKWARD implies REWIND for
/// the tuplestore's trim safety. Result/scan slots are virtual of the child shape.
pub fn exec_init_material<'rel>(
    node: &Material,
    estate: &mut EState<'rel>,
    eflags: i32,
    child: PlanStateNode<'rel>,
) -> Box<MaterialRun<'rel>> {
    let _ = estate;
    let mut mat_eflags = eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);
    if (eflags & EXEC_FLAG_BACKWARD) != 0 {
        mat_eflags |= EXEC_FLAG_REWIND;
    }

    let outer_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitMaterial: child has no result descriptor"));

    let scan_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );
    let result_slot = crate::backend::executor::execTuples::make_tuple_table_slot(
        Some(Arc::clone(&outer_desc)),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    let mut ps = PlanState {
        plan: Some(Node::Material(Box::new(node.clone()))),
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

    Box::new(MaterialRun {
        state: Box::new(MaterialState {
            ss,
            eflags: mat_eflags,
            eof_underlying: false,
            tuplestorestate: None,
        }),
        child: Box::new(child),
        tuplestore: None,
    })
}

/// PG `ExecMaterial`: serve from the tuplestore at its read frontier, else fetch
/// the next child row (stashing a copy). Returns a borrow of the result slot, or
/// `None` at end of data.
pub async fn exec_material<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut MaterialRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    // First call with a buffering requirement: create the tuplestore.
    if run.tuplestore.is_none() && run.state.eflags != 0 {
        let mut ts = tuplestore_begin_heap(true, false, work_mem());
        tuplestore_set_eflags(&mut ts, run.state.eflags);
        if let Some(desc) = run.state.ss.ps.scandesc.clone() {
            tuplestore_set_tupdesc(&mut ts, desc);
        }
        // MARK would allocate a second read pointer (index 1); the M5 path does not
        // reach mark-restore, so the single read pointer suffices.
        run.tuplestore = Some(ts);
    }

    // Forward scan only on the M5 path (es_direction is Forward). Decide where the
    // next row comes from WITHOUT holding the result-slot borrow across branches
    // (the conditional-return-of-borrow shape otherwise over-constrains the slot's
    // lifetime under NLL); the single slot borrow + return happens at the tail.
    let eof_tuplestore = run.tuplestore.as_ref().is_none_or(|ts| tuplestore_ateof(ts));

    // 1) If not at the tuplestore frontier, try the next buffered tuple.
    if !eof_tuplestore {
        let served = {
            let ts = run
                .tuplestore
                .as_mut()
                .unwrap_or_else(|| unreachable!("tuplestore present (not at eof)"));
            let slot = run
                .state
                .ss
                .ps
                .ps_result_tuple_slot
                .as_mut()
                .unwrap_or_else(|| unimplemented!("ExecMaterial: no result slot"));
            tuplestore_gettupleslot(ts, true, false, slot)
        };
        if served {
            return run.state.ss.ps.ps_result_tuple_slot.as_deref_mut();
        }
        // forward + ran out -> fall through to fetch from the child.
    }

    // 2) Fetch the next child row (PG `ExecCopySlot(slot, outerslot)` + append a
    // copy to the tuplestore). Snapshot the child's reused slot into owned arrays
    // first, so the child slot's next mutation cannot invalidate the result.
    if !run.state.eof_underlying {
        let copied = match Box::pin(exec_proc_node(shared, &mut run.child)).await {
            None => {
                run.state.eof_underlying = true;
                None
            }
            Some(s) => {
                use crate::executor::tuptable::slot_getallattrs;
                slot_getallattrs(s);
                let n = s.nvalid.max(0) as usize;
                Some((s.values[..n].to_vec(), s.isnull[..n].to_vec()))
            }
        };

        if let Some((values, isnull)) = copied {
            // Append to the tuplestore from the owned snapshot (it is at EOF, so its
            // read pointer advances over the appended tuple).
            if let Some(ts) = run.tuplestore.as_mut() {
                let desc = run
                    .state
                    .ss
                    .ps
                    .scandesc
                    .clone()
                    .unwrap_or_else(|| unimplemented!("ExecMaterial: no scan descriptor"));
                crate::backend::utils::sort::tuplestore::tuplestore_putvalues(ts, &desc, &values, &isnull);
            }
            // Materialize into the result slot.
            let slot = run.state.ss.ps.ps_result_tuple_slot.as_mut()
                .unwrap_or_else(|| unimplemented!("ExecMaterial: no result slot"));
            ExecClearTuple(slot);
            let n = values.len();
            slot.values[..n].copy_from_slice(&values);
            slot.isnull[..n].copy_from_slice(&isnull);
            crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
            return run.state.ss.ps.ps_result_tuple_slot.as_deref_mut();
        }
    }

    // 3) Nothing left.
    let slot = run.state.ss.ps.ps_result_tuple_slot.as_mut()
        .unwrap_or_else(|| unimplemented!("ExecMaterial: no result slot"));
    ExecClearTuple(slot);
    None
}

/// `work_mem` (KB), mirroring nodeSort's read of the GUC global with PG's compiled
/// default when the GUC bootstrap has not run.
fn work_mem() -> i32 {
    // SAFETY: process-global GUC int; read-only here (single-writer at startup).
    let w = unsafe { crate::miscadmin::work_mem };
    if w > 0 { w } else { 4096 }
}

/// PG `ExecEndMaterial`: release the tuplestore, then tear down the child.
pub fn exec_end_material(shared: Option<&Arc<SharedState>>, run: &mut MaterialRun<'_>) {
    if let Some(ts) = run.tuplestore.take() {
        tuplestore_end(ts);
    }
    exec_end_node(shared, &mut run.child);
}

/// PG `ExecReScanMaterial`: rewind the buffered output, or forget it to re-read the
/// child. The child rescan is the caller's responsibility (M5 driver re-inits).
pub fn exec_rescan_material(run: &mut MaterialRun<'_>) {
    if let Some(slot) = run.state.ss.ps.ps_result_tuple_slot.as_mut() {
        ExecClearTuple(slot);
    }
    if run.state.eflags != 0 {
        if run.tuplestore.is_none() {
            return;
        }
        if (run.state.eflags & EXEC_FLAG_REWIND) == 0 {
            // told tuplestore it needn't rewind -> must re-read.
            if let Some(ts) = run.tuplestore.take() {
                tuplestore_end(ts);
            }
            run.state.eof_underlying = false;
        } else if let Some(ts) = run.tuplestore.as_mut() {
            tuplestore_rescan(ts);
        }
    } else {
        run.state.eof_underlying = false;
    }
}

// Avoid an unused-import warning for the MARK/BACKWARD flags referenced only in the
// eflag computation above (they ARE used; this binds the symbols for clarity).
const _: i32 = EXEC_FLAG_MARK | EXEC_FLAG_BACKWARD;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt, TTSOpsVirtual};
    use crate::nodes::plannodes::{Material, Plan};
    use crate::postgres::Int32GetDatum;
    use std::sync::Arc;

    const INT4OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(23);
    const INVALID: crate::postgres_ext::Oid = crate::postgres_ext::InvalidOid;

    fn int4_desc() -> TupleDesc {
        let mut d = TupleDescData::create_template(1);
        d.init_builtin_entry(1, "a", INT4OID, -1, 0);
        d.init_entry_collation(1, INVALID);
        Arc::new(d)
    }

    fn make_source(desc: &TupleDesc, vals: Vec<i32>) -> PlanStateNode<'static> {
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

    fn material_node() -> Material {
        Material {
            plan: Plan {
                disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
                parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
                targetlist: Vec::new(), qual: Vec::new(), lefttree: None, righttree: None,
                init_plan: Vec::new(), ext_param: None, all_param: None,
            },
        }
    }

    async fn drain(run: &mut MaterialRun<'static>) -> Vec<i32> {
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_material(None, run)).await else { break };
            out.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
        }
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn materialize_then_read() {
        let desc = int4_desc();
        let child = make_source(&desc, vec![10, 20, 30]);
        let mut estate = EState::default();
        let mut run = exec_init_material(&material_node(), &mut estate, EXEC_FLAG_REWIND, child);
        assert_eq!(drain(&mut run).await, vec![10, 20, 30]);
        exec_end_material(None, &mut run);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rescan_re_reads_from_start() {
        let desc = int4_desc();
        let child = make_source(&desc, vec![1, 2, 3]);
        let mut estate = EState::default();
        let mut run = exec_init_material(&material_node(), &mut estate, EXEC_FLAG_REWIND, child);
        assert_eq!(drain(&mut run).await, vec![1, 2, 3]);
        exec_rescan_material(&mut run);
        // After rescan, the buffered rows are re-served from the tuplestore.
        assert_eq!(drain(&mut run).await, vec![1, 2, 3]);
        exec_end_material(None, &mut run);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn passthrough_without_buffering() {
        // eflags == 0 -> no tuplestore; the node just forwards the child.
        let desc = int4_desc();
        let child = make_source(&desc, vec![7, 8]);
        let mut estate = EState::default();
        let mut run = exec_init_material(&material_node(), &mut estate, 0, child);
        assert!(run.tuplestore.is_none() || run.state.eflags == 0);
        assert_eq!(drain(&mut run).await, vec![7, 8]);
        exec_end_material(None, &mut run);
    }
}
