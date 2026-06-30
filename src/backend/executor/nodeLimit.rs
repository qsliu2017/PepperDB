//! Limit node executor. Translated from
//! backend/executor/nodeLimit.c (disposition: full for the M5 forward
//! LIMIT/OFFSET state machine; WITH TIES and backward scan are clean grow guards).
//!
//! `ExecInitLimit` initializes the child, compiles the limit/offset expressions,
//! and sets the result slot ops to the child's. `ExecLimit` runs the LIMIT/OFFSET
//! state machine: skip OFFSET rows, then pass through up to COUNT rows, then stop.
//! The limit/offset exprs are (re)evaluated at startup/rescan (`recompute_limits`).
//!
//! Async coloring: `ExecLimit` drives the child (which reaches the table AM), so it
//! is `async` (rules.md s5). No lock held across the child `.await`.
//!
//! GROW: backward scan, WITH TIES (the WINDOWEND_TIES / last_slot equality path),
//! and `ExecSetTupleBound` pushdown to the child are grow guards (rules.md s4) --
//! the M5 forward LIMIT_OPTION_COUNT path is complete.

use std::sync::Arc;

use crate::backend::executor::execProcnode::{exec_end_node, exec_proc_node, result_type_of, PlanStateNode};
use crate::nodes::execnodes::{EState, LimitState, LimitStateCond, PlanState};
use crate::nodes::nodes::Node;
use crate::nodes::nodes::LimitOption;
use crate::nodes::plannodes::Limit;
use crate::postgres::DatumGetInt64;
use crate::shared_state::SharedState;

/// Run-state pairing the PG `LimitState` with its child plan-state. The child plus
/// the most-recent passed-through slot (a deep copy, since the child reuses its
/// slot) live here; the C node holds these via `ps.lefttree` + `subSlot`.
pub struct LimitRun<'rel> {
    pub state: Box<LimitState>,
    /// the outer (input) subplan state.
    pub child: Box<PlanStateNode<'rel>>,
    /// the most recent row the window has accepted (an owned copy of the child's
    /// reused slot; PG keeps a `subSlot` pointer into the child's slot, valid until
    /// the next child fetch -- here we copy it so the borrow does not outlive it).
    pub sub_slot: Option<Box<crate::nodes::execnodes::TupleTableSlot>>,
}

/// PG `ExecInitLimit`: build the LimitState over an initialized child. Compiles
/// the limit/offset expressions (int8 exprs) and a result slot of the child's
/// rowtype. Limit does no qual/projection.
pub fn exec_init_limit<'rel>(
    node: &Limit,
    estate: &mut EState<'rel>,
    child: PlanStateNode<'rel>,
) -> Box<LimitRun<'rel>> {
    crate::assert!(
        node.limit_option == LimitOption::COUNT,
        "ExecInitLimit: WITH TIES not yet reachable"
    );

    let outer_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitLimit: child has no result descriptor"));

    let mut ps = PlanState {
        plan: Some(Node::Limit(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_expr_context = Some(crate::backend::executor::execUtils::create_expr_context(estate));
    ps.ps_result_tuple_desc = Some(outer_desc);
    ps.ps_proj_info = None;

    // ExecInitExpr on the limit/offset exprs.
    let limit_offset =
        crate::backend::executor::execExpr::exec_init_expr(node.limit_offset.as_ref(), None);
    let limit_count =
        crate::backend::executor::execExpr::exec_init_expr(node.limit_count.as_ref(), None);

    let state = LimitState {
        ps,
        limit_offset,
        limit_count,
        limit_option: node.limit_option,
        offset: 0,
        count: 0,
        no_count: false,
        lstate: LimitStateCond::INITIAL,
        position: 0,
        sub_slot: None,
        eqfunction: None,
        last_slot: None,
    };

    Box::new(LimitRun {
        state: Box::new(state),
        child: Box::new(child),
        sub_slot: None,
    })
}

/// PG `ExecLimit`: the LIMIT/OFFSET state machine (M5 forward subset). Returns a
/// borrow of the current in-window row (held in `sub_slot`), or `None` past the
/// window / on an empty subplan.
pub async fn exec_limit<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut LimitRun<'_>,
) -> Option<&'r mut crate::nodes::execnodes::TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();

    loop {
        match run.state.lstate {
            LimitStateCond::INITIAL => {
                recompute_limits(run);
                // FALL THRU to RESCAN.
                run.state.lstate = LimitStateCond::RESCAN;
            }
            LimitStateCond::RESCAN => {
                // Empty window: behave like an empty subplan.
                if run.state.count <= 0 && !run.state.no_count {
                    run.state.lstate = LimitStateCond::EMPTY;
                    return None;
                }
                // Fetch rows until position > offset (skip the OFFSET rows).
                loop {
                    let snap = match Box::pin(exec_proc_node(shared, &mut run.child)).await {
                        None => {
                            run.state.lstate = LimitStateCond::EMPTY;
                            return None;
                        }
                        Some(slot) => snapshot_slot(slot),
                    };
                    store_sub_slot(run, snap);
                    run.state.position += 1;
                    if run.state.position > run.state.offset {
                        break;
                    }
                }
                run.state.lstate = LimitStateCond::INWINDOW;
                return run.sub_slot.as_deref_mut();
            }
            LimitStateCond::INWINDOW => {
                // Forward: check for stepping off the end of the window.
                if !run.state.no_count
                    && run.state.position - run.state.offset >= run.state.count
                {
                    run.state.lstate = LimitStateCond::WINDOWEND;
                    return None;
                }
                let snap = match Box::pin(exec_proc_node(shared, &mut run.child)).await {
                    None => {
                        run.state.lstate = LimitStateCond::SUBPLANEOF;
                        return None;
                    }
                    Some(slot) => snapshot_slot(slot),
                };
                store_sub_slot(run, snap);
                run.state.position += 1;
                return run.sub_slot.as_deref_mut();
            }
            LimitStateCond::EMPTY
            | LimitStateCond::SUBPLANEOF
            | LimitStateCond::WINDOWEND => return None,
            other => unimplemented!("ExecLimit: state {other:?} not yet reachable (backward/WITH TIES)"),
        }
    }
}

/// Deform a child slot into an owned snapshot (values, isnull, desc). The child
/// reuses its slot; snapshotting frees the child borrow before we store into the
/// node-owned `sub_slot` (PG holds a pointer into the child slot, valid only until
/// the next fetch -- the owned-slot model copies so the returned borrow survives).
fn snapshot_slot(
    src: &mut crate::nodes::execnodes::TupleTableSlot,
) -> (Vec<crate::postgres::Datum>, Vec<bool>, Option<crate::access::tupdesc::TupleDesc>) {
    use crate::executor::tuptable::slot_getallattrs;
    slot_getallattrs(src);
    let n = src.nvalid.max(0) as usize;
    (src.values[..n].to_vec(), src.isnull[..n].to_vec(), src.tupleDescriptor.clone())
}

/// Store a snapshot into the node-owned `sub_slot`.
fn store_sub_slot(
    run: &mut LimitRun<'_>,
    snap: (Vec<crate::postgres::Datum>, Vec<bool>, Option<crate::access::tupdesc::TupleDesc>),
) {
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot, TTS_OPS_VIRTUAL};
    let (values, isnull, desc) = snap;
    let dst = run
        .sub_slot
        .get_or_insert_with(|| make_tuple_table_slot(desc, &TTS_OPS_VIRTUAL));
    crate::executor::tuptable::ExecClearTuple(dst);
    let n = values.len();
    dst.values[..n].copy_from_slice(&values);
    dst.isnull[..n].copy_from_slice(&isnull);
    exec_store_virtual_tuple(dst);
}

/// PG `recompute_limits`: evaluate the limit/offset exprs and reset position. NULL
/// offset means 0; NULL count means LIMIT ALL (`no_count`). Negative values raise.
fn recompute_limits(run: &mut LimitRun<'_>) {
    let econtext = run
        .state
        .ps
        .ps_expr_context
        .as_mut()
        .unwrap_or_else(|| unimplemented!("recompute_limits: no exprcontext"));

    // OFFSET: NULL expr or NULL value -> 0; negative raises.
    run.state.offset = run.state.limit_offset.as_mut().map_or(0, |expr| {
        eval_int8(expr, econtext).map_or(0, |v| {
            if v < 0 {
                crate::elog!(crate::utils::elog::ERROR, "OFFSET must not be negative");
            }
            v
        })
    });

    // COUNT: NULL expr or NULL value -> LIMIT ALL (no_count); negative raises.
    match run.state.limit_count.as_mut().and_then(|expr| eval_int8(expr, econtext)) {
        None => {
            run.state.count = 0;
            run.state.no_count = true;
        }
        Some(v) => {
            if v < 0 {
                crate::elog!(crate::utils::elog::ERROR, "LIMIT must not be negative");
            }
            run.state.count = v;
            run.state.no_count = false;
        }
    }

    run.state.position = 0;
    run.state.lstate = LimitStateCond::RESCAN;
}

/// Evaluate an int8 expr through its compiled `evalfunc`, returning the i64 value
/// or `None` for SQL NULL. The limit/offset exprs are by-value Const int8s; the
/// const path holds no arena allocation, so the direct evalfunc call is sound
/// (ResetExprContext is a no-op; rules.md s6.4) -- no lock/RefCell across it.
fn eval_int8(
    expr: &mut crate::nodes::execnodes::ExprState,
    econtext: &mut crate::nodes::execnodes::ExprContext,
) -> Option<i64> {
    let evalfunc = expr
        .evalfunc
        .unwrap_or_else(|| unimplemented!("eval_int8: expr not ready"));
    let mut is_null = false;
    let v = evalfunc(expr, econtext, &mut is_null);
    (!is_null).then(|| DatumGetInt64(v))
}

/// PG `ExecEndLimit`: tear down the child.
pub fn exec_end_limit(shared: Option<&Arc<SharedState>>, run: &mut LimitRun<'_>) {
    exec_end_node(shared, &mut run.child);
}

/// PG `ExecReScanLimit`: recompute limits + reset the state machine. The child
/// rescan is the caller's responsibility (M5 driver tests re-init).
pub fn exec_rescan_limit(run: &mut LimitRun<'_>) {
    recompute_limits(run);
    run.state.lstate = LimitStateCond::INITIAL;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::backend::nodes::makefuncs::make_const;
    use crate::executor::tuptable::{slot_getattr, DatumGetInt32_opt, TTSOpsVirtual, TupleTableSlot};
    use crate::nodes::plannodes::Plan;
    use crate::postgres::{Int32GetDatum, Int64GetDatum};
    use std::sync::Arc;

    const INT4OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(23);
    const INT8OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid::new(20);
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

    fn int8_const(v: i64) -> Node {
        Node::Const(Box::new(make_const(INT8OID, -1, INVALID, 8, Int64GetDatum(v), false, true)))
    }

    fn empty_plan() -> Plan {
        Plan {
            disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
            parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
            targetlist: Vec::new(), qual: Vec::new(), lefttree: None, righttree: None,
            init_plan: Vec::new(), ext_param: None, all_param: None,
        }
    }

    fn limit_node(offset: Option<Node>, count: Option<Node>) -> Limit {
        Limit {
            plan: empty_plan(),
            limit_offset: offset,
            limit_count: count,
            limit_option: LimitOption::COUNT,
            uniq_num_cols: 0,
            uniq_col_idx: Vec::new(),
            uniq_operators: Vec::new(),
            uniq_collations: Vec::new(),
        }
    }

    async fn run_limit(node: Limit, child: PlanStateNode<'static>) -> Vec<i32> {
        let mut estate = EState::default();
        let mut run = exec_init_limit(&node, &mut estate, child);
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_limit(None, &mut run)).await else { break };
            out.push(DatumGetInt32_opt(slot_getattr(slot, 1)).expect("non-null"));
        }
        exec_end_limit(None, &mut run);
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn limit_2_offset_1_over_5_rows() {
        let desc = int4_desc();
        let child = make_source(&desc, vec![10, 20, 30, 40, 50]);
        let node = limit_node(Some(int8_const(1)), Some(int8_const(2)));
        assert_eq!(run_limit(node, child).await, vec![20, 30]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn limit_only() {
        let desc = int4_desc();
        let child = make_source(&desc, vec![1, 2, 3, 4, 5]);
        let node = limit_node(None, Some(int8_const(3)));
        assert_eq!(run_limit(node, child).await, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offset_only_limit_all() {
        let desc = int4_desc();
        let child = make_source(&desc, vec![1, 2, 3, 4, 5]);
        let node = limit_node(Some(int8_const(2)), None);
        assert_eq!(run_limit(node, child).await, vec![3, 4, 5]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn limit_zero_returns_nothing() {
        let desc = int4_desc();
        let child = make_source(&desc, vec![1, 2, 3]);
        let node = limit_node(None, Some(int8_const(0)));
        assert!(run_limit(node, child).await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offset_past_end_returns_nothing() {
        let desc = int4_desc();
        let child = make_source(&desc, vec![1, 2]);
        let node = limit_node(Some(int8_const(5)), Some(int8_const(3)));
        assert!(run_limit(node, child).await.is_empty());
    }
}
