//! Aggregate node executor. Translated from backend/executor/nodeAgg.c
//! (disposition: full for the M5 aggregate path -- AGG_PLAIN + AGG_SORTED +
//! AGG_HASHED over the count/sum/min/max aggregates; the heavy paths are clean
//! grow guards).
//!
//! `ExecInitAgg` resolves each `Aggref` in the targetlist against pg_aggregate
//! (via the AGGFNOID syscache: transfn / finalfn / transtype / initval) and binds
//! the transition function through fmgr. `ExecAgg` runs one of three strategies:
//!
//!  - AGG_PLAIN: no GROUP BY -- one group over the whole input. Drain the child,
//!    advancing every aggregate's transition state per row, then finalize once and
//!    emit a single result row (`SELECT count(*) FROM t`).
//!  - AGG_SORTED: input is pre-sorted by the grouping columns (a Sort below).
//!    Advance within a group; at a group boundary, finalize + emit the group, then
//!    re-seed the state from the first row of the next group.
//!  - AGG_HASHED: hash the grouping key -> per-group transition state. Build the
//!    hash table by draining the child, then emit one finalized row per entry.
//!
//! Per-group transition state is an owned `(Datum, bool)` vector (one slot per
//! aggregate), advanced by `OidFunctionCall{1,2}` on the transfn OID -- the
//! transition value flows by value through fmgr (int8/int4 are pass-by-value here,
//! so PG's in-place AggCheckCallContext path is ifdef-ed out). The state and the
//! resolved `AggInfo`s are owned and `Send`; no raw pointers, no lock across the
//! child `.await` (rules.md s5/s10).
//!
//! GROW/STAGE: grouping sets / DISTINCT-in-agg / ORDER BY-in-agg / FILTER /
//! ordered-set / partial(parallel) agg are clean `not_yet_reachable` guards. The
//! Aggref input is restricted to a single plain `Var` (or no arg for count(*));
//! arbitrary input expressions grow with the expr engine.

use std::collections::HashMap;
use std::sync::Arc;

use crate::backend::executor::execProcnode::{
    exec_end_node, exec_proc_node, result_type_of, PlanStateNode,
};
use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::backend::executor::nodeGroup::grouping_equal;
use crate::backend::executor::nodeUnique::snapshot_slot;
use crate::executor::tuptable::{slot_getattr, ExecClearTuple, TupleTableSlot};
use crate::fmgr::{OidFunctionCall1, OidFunctionCall2};
use crate::nodes::execnodes::{AggState, EState, PlanState, ScanState};
use crate::nodes::nodes::{AggStrategy, Node};
use crate::nodes::plannodes::Agg;
use crate::nodes::primnodes::Aggref;
use crate::postgres::{Datum, Int64GetDatum};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// Resolved per-aggregate info (PG's AggStatePerTrans + AggStatePerAgg, collapsed
/// to the M5 single-transition / single-arg shape). Owned + `Send`.
pub struct AggInfo {
    /// transition function OID (e.g. int8inc / int4_sum / int4larger).
    pub transfn: Oid,
    /// number of explicit transition inputs (0 = count(*), 1 = count(x)/sum/min/max
    /// fed one value besides the running state).
    pub num_input: i32,
    /// final function OID, or `InvalidOid` (most M5 aggs have none).
    pub finalfn: Oid,
    /// input column position (1-based) in the child tuple, or 0 for count(*).
    pub input_attno: i16,
    /// the transition type's initial value as a Datum, or `None` (start NULL).
    pub init_value: Option<Datum>,
}

/// One aggregate's running transition state for one group. Owned `(value, isnull)`.
#[derive(Clone, Copy)]
struct PerGroupState {
    value: Datum,
    is_null: bool,
}

/// Run-state pairing the PG `AggState` with its child plan-state and the resolved
/// aggregate metadata. The C node holds the child via `ps.lefttree` and the
/// per-agg/per-trans/per-group arrays via `void *`; the Rust island wrapper holds
/// them here (like SortRun/GroupRun).
pub struct AggRun<'rel> {
    pub state: Box<AggState>,
    pub child: Box<PlanStateNode<'rel>>,
    pub strategy: AggStrategy,
    /// resolved aggregate metadata, one per targetlist Aggref (indexed by aggno).
    pub aggs: Vec<AggInfo>,
    /// grouping column positions (1-based) + their types (SORTED/HASHED).
    pub key_cols: Vec<i16>,
    pub key_types: Vec<Oid>,
    /// PLAIN/SORTED: per-aggregate transition state for the current group.
    cur_group: Vec<PerGroupState>,
    /// SORTED: the first tuple of the current group (group-key representative).
    first_tuple: Option<(Vec<Datum>, Vec<bool>)>,
    /// HASHED: filled hash table (built on first call), drained one entry per call.
    hash_groups: Option<Vec<HashGroup>>,
    hash_cursor: usize,
    /// whether the input has been fully consumed (PLAIN/HASHED build done).
    input_done: bool,
    /// PLAIN: whether the single result row has been emitted.
    emitted: bool,
}

/// HASHED: one hash-table entry -- the grouping-key representative tuple plus the
/// per-aggregate transition state.
struct HashGroup {
    key_row: (Vec<Datum>, Vec<bool>),
    pergroup: Vec<PerGroupState>,
}

/// PG `ExecInitAgg`: build the AggState over an initialized child. Resolves each
/// targetlist `Aggref` against pg_aggregate (AGGFNOID) and seeds the result slot
/// from the Agg node's targetlist rowtype (which the planner set as the child's
/// result desc -- the M5 passthrough+agg shape).
pub fn exec_init_agg<'rel>(
    node: &Agg,
    estate: &mut EState<'rel>,
    child: PlanStateNode<'rel>,
) -> Box<AggRun<'rel>> {
    let _ = estate;
    crate::assert!(node.grouping_sets.is_empty(), "ExecInitAgg: GROUPING SETS not yet reachable");
    crate::assert!(node.plan.qual.is_empty(), "ExecInitAgg: HAVING qual not yet reachable");

    let outer_desc = result_type_of(&child)
        .unwrap_or_else(|| unimplemented!("ExecInitAgg: child has no result descriptor"));

    // Resolve each Aggref in the targetlist (in resno order) into an AggInfo.
    let mut aggs: Vec<AggInfo> = Vec::new();
    for te in &node.plan.targetlist {
        let Node::TargetEntry(te) = te else { continue };
        if let Some(Node::Aggref(aggref)) = te.expr.as_ref() {
            aggs.push(resolve_aggref(aggref));
        }
    }

    // The result rowtype: PG projects the Agg targetlist -- one attribute per
    // non-junk TargetEntry, typed by the entry's expression (the Aggref's aggtype
    // for an aggregate, the Var's vartype for a grouping column). Build it here
    // (the planner's ExecAssignResultTypeFromTL analog); the descriptor is virtual
    // and the value layout is what the wire path reads.
    let result_desc =
        crate::backend::executor::execTuples::exec_clean_type_from_tl(&node.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);

    let key_cols = node.grp_col_idx.clone();
    let key_types: Vec<Oid> = key_cols
        .iter()
        .map(|&c| outer_desc.attr((c - 1) as usize).atttypid)
        .collect();

    let mut ps = PlanState {
        plan: Some(Node::Agg(Box::new(node.clone()))),
        ..PlanState::default()
    };
    ps.ps_result_tuple_desc = Some(Arc::clone(&result_desc));
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.scandesc = Some(Arc::clone(&outer_desc));

    let ss = ScanState { ps, ss_current_relation: None, ss_current_scan_desc: None, ss_scan_tuple_slot: None };

    let numaggs = i32::try_from(aggs.len()).unwrap_or(0);
    let cur_group = init_pergroup(&aggs);
    Box::new(AggRun {
        state: Box::new(AggState { ss, numaggs, ..AggState::default() }),
        child: Box::new(child),
        strategy: node.aggstrategy,
        aggs,
        key_cols,
        key_types,
        cur_group,
        first_tuple: None,
        hash_groups: None,
        hash_cursor: 0,
        input_done: false,
        emitted: false,
    })
}

/// Resolve an `Aggref` against pg_aggregate (AGGFNOID syscache, warm) into the
/// transition/final metadata nodeAgg drives. The input column is the Aggref's sole
/// argument when it is a plain `Var` (sum/min/max/count(x)); count(*) has no arg.
fn resolve_aggref(aggref: &Aggref) -> AggInfo {
    crate::assert!(aggref.aggdistinct.is_empty(), "ExecInitAgg: DISTINCT agg not yet reachable");
    crate::assert!(aggref.aggorder.is_empty(), "ExecInitAgg: ORDER BY in agg not yet reachable");
    crate::assert!(aggref.aggfilter.is_none(), "ExecInitAgg: FILTER not yet reachable");

    let (transfn, finalfn, transtype, init_value) = lookup_aggregate(aggref.aggfnoid);

    // The transition input: count(*) has none (aggstar); count(x)/sum/min/max have
    // a single arg that the M5 planner emits as a plain Var over the child tuple.
    let (num_input, input_attno) = if aggref.aggstar || aggref.args.is_empty() {
        (0, 0)
    } else {
        let attno = match aggref.args.first() {
            Some(Node::TargetEntry(te)) => match te.expr.as_ref() {
                Some(Node::Var(v)) => v.varattno,
                _ => unimplemented!("ExecInitAgg: non-Var aggregate input not yet reachable"),
            },
            Some(Node::Var(v)) => v.varattno,
            _ => unimplemented!("ExecInitAgg: unexpected aggregate argument shape"),
        };
        (1, attno)
    };
    let _ = transtype;

    AggInfo { transfn, num_input, finalfn, input_attno, init_value }
}

/// Read `(transfn, finalfn, transtype, initval-as-Datum)` for an aggregate from a
/// warm AGGFNOID syscache hit. The initial value, when present, is parsed from its
/// catalog text into the transition type's Datum (M5: count's int8 `0`).
fn lookup_aggregate(aggfnoid: Oid) -> (Oid, Oid, Oid, Option<Datum>) {
    use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache};
    use crate::catalog::pg_aggregate::{Form_pg_aggregate, FormData_pg_aggregate};
    use crate::postgres::ObjectIdGetDatum;
    use crate::utils::syscache::SysCacheIdentifier;

    let Some(tuple) =
        search_sys_cache(SysCacheIdentifier::AGGFNOID, &[ObjectIdGetDatum(aggfnoid)])
    else {
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("cache lookup failed for aggregate {} (AGGFNOID not warm)", aggfnoid.get())
        );
        unreachable!("elog!(ERROR) raises")
    };

    // SAFETY: a held AGGFNOID hit -> a pg_aggregate row; the fixed part covers
    // transfn/finalfn/transtype (the varlen initval tail follows).
    let (transfn, finalfn, transtype) = {
        #[allow(
            clippy::cast_ptr_alignment,
            reason = "GETSTRUCT reinterprets the MAXALIGN'd tuple body as the Form struct, like lsyscache"
        )]
        let pa: Form_pg_aggregate =
            crate::access::htup_details::GETSTRUCT(unsafe { &*tuple }).cast::<FormData_pg_aggregate>();
        let pa = unsafe { &*pa };
        (pa.aggtransfn, pa.aggfinalfn, pa.aggtranstype)
    };
    release_sys_cache(tuple);

    // The initval (varlen text) is not read from the tuple's varlen tail (the
    // SysCacheGetAttr path stages); the compiled-in seed map carries it. M5 only
    // needs count's int8 `0`; everything else starts NULL.
    let init_value = agg_initval(aggfnoid, transtype);
    (transfn, finalfn, transtype, init_value)
}

/// The compiled-in initial transition value for an aggregate (the catalog
/// `agginitval`, parsed into the transition type's Datum). M5: count's int8 `0`;
/// all other seeded aggregates start with a NULL transition value.
fn agg_initval(aggfnoid: Oid, transtype: Oid) -> Option<Datum> {
    use crate::catalog::genbki::INT8OID;
    // count() = 2803, count(any) = 2147 -> int8 initval 0.
    if (aggfnoid.get() == 2803 || aggfnoid.get() == 2147) && transtype == INT8OID {
        return Some(Int64GetDatum(0));
    }
    None
}

/// Fresh per-aggregate transition state seeded from each aggregate's initval.
fn init_pergroup(aggs: &[AggInfo]) -> Vec<PerGroupState> {
    aggs.iter()
        .map(|a| {
            a.init_value.map_or(PerGroupState { value: Datum(0), is_null: true }, |v| {
                PerGroupState { value: v, is_null: false }
            })
        })
        .collect()
}

/// Advance every aggregate's transition state with one input row.
fn advance_aggregates(aggs: &[AggInfo], pergroup: &mut [PerGroupState], slot: &mut TupleTableSlot) {
    for (info, st) in aggs.iter().zip(pergroup.iter_mut()) {
        advance_transition(info, st, slot);
    }
}

/// Advance one aggregate's transition value: `state = transfn(state[, input])`.
/// `count(*)` (num_input 0) calls the 1-arg transfn (int8inc) on the state alone;
/// otherwise the input column is read and the 2-arg transfn is called. The result
/// becomes the new transition value (by-value; PG's in-place path is ifdef-ed out).
fn advance_transition(info: &AggInfo, st: &mut PerGroupState, slot: &mut TupleTableSlot) {
    if info.num_input == 0 {
        // count(*): increment the running int8 state (never NULL after init).
        let result = OidFunctionCall1(info.transfn, st.value)
            .unwrap_or_else(|| unreachable!("count transfn returns non-NULL"));
        st.value = result;
        st.is_null = false;
        return;
    }

    // One-input aggregate (count(x)/sum/min/max). Read the input column; a NULL
    // input is skipped (PG's strict-on-input semantics: the transfn is not invoked
    // and the state is unchanged).
    let input = slot_getattr(slot, i32::from(info.input_attno));
    let Some(input_val) = input else {
        return; // NULL input -> ignore this row
    };

    if st.is_null {
        // First non-null input seeds the state. min/max/count(x) (no initval): the
        // state becomes the input value directly (PG's "first call" semantics for a
        // NULL transition value with a strict transfn). sum's transfn handles the
        // NULL state itself, but we mirror the same seed for the non-numeric ones.
        // For sum (int4_sum/int2_sum), calling the transfn with a NULL state is the
        // faithful path; it returns the first value. Always go through the transfn
        // when it can accept a NULL state; here we route min/max (strict 2-arg
        // comparators) by seeding directly and routing sum through its transfn.
        if info.finalfn == crate::postgres_ext::InvalidOid && is_strict_minmax(info.transfn) {
            st.value = input_val;
            st.is_null = false;
            return;
        }
        // sum-style: invoke the transfn, which seeds from a NULL state. We model a
        // NULL state as Datum(0) is wrong for sum; instead call the 2-arg transfn
        // and let it produce the first value (int4_sum checks ARGISNULL(0)).
        let result = call_transition_nullable(info.transfn, None, input_val);
        match result {
            Some(v) => { st.value = v; st.is_null = false; }
            None => { st.is_null = true; }
        }
        return;
    }

    let result = OidFunctionCall2(info.transfn, st.value, input_val)
        .unwrap_or_else(|| unreachable!("transfn returns non-NULL once state is non-null"));
    st.value = result;
    st.is_null = false;
}

/// Whether a transfn is a strict min/max comparator (int*larger/smaller,
/// numeric/text larger/smaller) -- those seed the state from the first input
/// directly rather than through a NULL-state transfn call.
fn is_strict_minmax(transfn: Oid) -> bool {
    use crate::utils::fmgroids as f;
    matches!(
        transfn,
        x if x == f::F_INT4LARGER || x == f::F_INT4SMALLER
            || x == f::F_INT2LARGER || x == f::F_INT2SMALLER
            || x == f::F_INT8LARGER || x == f::F_INT8SMALLER
            || x == f::F_NUMERIC_LARGER || x == f::F_NUMERIC_SMALLER
            || x == f::F_TEXT_LARGER || x == f::F_TEXT_SMALLER
    )
}

/// Call a 2-arg transition function whose first arg (the running state) may be
/// NULL, returning the result (or None if the result is NULL). Used to seed
/// sum-style aggregates from a NULL transition value (int4_sum/int2_sum read
/// `PG_ARGISNULL(0)`). The fcinfo is built directly to set the isnull flag.
fn call_transition_nullable(transfn: Oid, state: Option<Datum>, input: Datum) -> Option<Datum> {
    use crate::fmgr::{fmgr_info, FmgrInfo, FunctionCallInfoBaseData, FunctionCallInvoke};
    use crate::postgres::NullableDatum;
    use crate::postgres_ext::InvalidOid;

    let mut lookup = FmgrInfo {
        fn_addr: None, oid: InvalidOid, nargs: 0, strict: false, retset: false,
        stats: 0, extra: 0, mcxt: (), expr: None,
    };
    fmgr_info(transfn, &mut lookup);

    let mut fcinfo = FunctionCallInfoBaseData {
        flinfo: Some(Box::new(lookup)),
        context: None, resultinfo: None, fncollation: InvalidOid,
        isnull: false, nargs: 2,
        args: vec![
            NullableDatum { value: state.unwrap_or(Datum(0)), isnull: state.is_none() },
            NullableDatum { value: input, isnull: false },
        ],
    };
    let result = FunctionCallInvoke(&mut fcinfo);
    if fcinfo.isnull { None } else { Some(result) }
}

/// Finalize one aggregate: apply the final function if present, else the
/// transition value is the result. Returns `(value, isnull)`.
fn finalize_aggregate(info: &AggInfo, st: &PerGroupState) -> (Datum, bool) {
    if info.finalfn == crate::postgres_ext::InvalidOid {
        return (st.value, st.is_null);
    }
    if st.is_null {
        // A NULL transition value: most final fns are strict -> NULL result.
        return (Datum(0), true);
    }
    OidFunctionCall1(info.finalfn, st.value).map_or((Datum(0), true), |v| (v, false))
}

/// PG `ExecAgg`: return the next aggregated row. Dispatches on the strategy.
pub async fn exec_agg<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut AggRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    crate::miscadmin::check_for_interrupts();
    match run.strategy {
        AggStrategy::PLAIN => exec_agg_plain(shared, run).await,
        AggStrategy::SORTED => exec_agg_sorted(shared, run).await,
        AggStrategy::HASHED => exec_agg_hashed(shared, run).await,
        AggStrategy::MIXED => unimplemented!("ExecAgg: AGG_MIXED (grouping sets) not yet reachable"),
    }
}

/// AGG_PLAIN: one group over the whole input. Drain, finalize, emit one row.
async fn exec_agg_plain<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut AggRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    if run.emitted {
        return None;
    }
    // Drain the child, advancing the single group's transition state per row.
    loop {
        let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await else { break };
        advance_aggregates(&run.aggs, &mut run.cur_group, slot);
    }
    run.input_done = true;
    run.emitted = true;

    // Plain agg with no input rows still emits one row (count(*) -> 0).
    let pergroup = run.cur_group.clone();
    Some(project_group(run, None, &pergroup))
}

/// AGG_SORTED: emit one finalized row per group boundary over sorted input.
async fn exec_agg_sorted<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut AggRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    if run.input_done {
        return None;
    }

    // First call: read the first input tuple to open the first group.
    if run.first_tuple.is_none() {
        let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await else {
            run.input_done = true;
            return None;
        };
        let snap = snapshot_slot(slot);
        run.cur_group = init_pergroup(&run.aggs);
        advance_from_snapshot(&run.aggs, &mut run.cur_group, &snap);
        run.first_tuple = Some(snap);
    }

    // Advance over the rest of the current group; stop at the next group's first
    // tuple, finalize+emit the current group, and re-seed from that next tuple.
    loop {
        let next =
            Box::pin(exec_proc_node(shared, &mut run.child)).await.map(snapshot_slot);

        match next {
            None => {
                // End of input: emit the final group.
                run.input_done = true;
                let first = run.first_tuple.take();
                let pergroup = run.cur_group.clone();
                return Some(project_group(run, first.as_ref(), &pergroup));
            }
            Some(snap) => {
                let same = {
                    let first = run.first_tuple.as_ref().unwrap_or_else(|| unreachable!("first set"));
                    grouping_equal(&run.key_cols, &run.key_types, &snap.0, &snap.1, &first.0, &first.1)
                };
                if same {
                    advance_from_snapshot(&run.aggs, &mut run.cur_group, &snap);
                    continue;
                }
                // Group boundary: finalize the current group, then re-seed.
                let first = run.first_tuple.replace(snap.clone());
                let pergroup = std::mem::replace(&mut run.cur_group, init_pergroup(&run.aggs));
                advance_from_snapshot(&run.aggs, &mut run.cur_group, &snap);
                return Some(project_group(run, first.as_ref(), &pergroup));
            }
        }
    }
}

/// AGG_HASHED: build a hash table of grouping-key -> per-group state on the first
/// call, then emit one finalized row per entry.
async fn exec_agg_hashed<'r>(
    shared: Option<&Arc<SharedState>>,
    run: &'r mut AggRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    if run.hash_groups.is_none() {
        // Build phase: drain the child, hashing each row to its group.
        let mut order: Vec<HashKey> = Vec::new();
        let mut groups: HashMap<HashKey, HashGroup> = HashMap::new();
        loop {
            let Some(slot) = Box::pin(exec_proc_node(shared, &mut run.child)).await else { break };
            let snap = snapshot_slot(slot);
            let key = hash_key(&run.key_cols, &snap);
            let entry = groups.entry(key.clone()).or_insert_with(|| {
                order.push(key.clone());
                HashGroup { key_row: snap.clone(), pergroup: init_pergroup(&run.aggs) }
            });
            advance_from_snapshot(&run.aggs, &mut entry.pergroup, &snap);
        }
        // Emit in first-seen order (deterministic for tests).
        let built: Vec<HashGroup> =
            order.into_iter().map(|k| groups.remove(&k).unwrap_or_else(|| unreachable!("key present"))).collect();
        run.hash_groups = Some(built);
        run.hash_cursor = 0;
        run.input_done = true;
    }

    let groups = run.hash_groups.as_ref().unwrap_or_else(|| unreachable!("hash built"));
    if run.hash_cursor >= groups.len() {
        return None;
    }
    let idx = run.hash_cursor;
    run.hash_cursor += 1;
    let key_row = groups[idx].key_row.clone();
    let pergroup = groups[idx].pergroup.clone();
    Some(project_group(run, Some(&key_row), &pergroup))
}

/// A hashable grouping key: the key columns' datums (as raw bits) + null flags.
/// Sufficient for the by-value M5 key types (int2/int4/int8/oid); pass-by-ref key
/// types grow with a proper hash (rules.md s4).
type HashKey = Vec<(usize, bool)>;

fn hash_key(key_cols: &[i16], snap: &(Vec<Datum>, Vec<bool>)) -> HashKey {
    key_cols
        .iter()
        .map(|&c| {
            let idx = (c - 1) as usize;
            (snap.0[idx].0, snap.1[idx])
        })
        .collect()
}

/// Advance the per-group state from a snapshot row (the SORTED/HASHED path keeps
/// rows as owned snapshots, not live slots). Re-stores the snapshot into a scratch
/// slot so `slot_getattr` can read the aggregate input column.
fn advance_from_snapshot(aggs: &[AggInfo], pergroup: &mut [PerGroupState], snap: &(Vec<Datum>, Vec<bool>)) {
    for (info, st) in aggs.iter().zip(pergroup.iter_mut()) {
        if info.num_input == 0 {
            let result = OidFunctionCall1(info.transfn, st.value)
                .unwrap_or_else(|| unreachable!("count transfn returns non-NULL"));
            st.value = result;
            st.is_null = false;
            continue;
        }
        let idx = (info.input_attno - 1) as usize;
        if snap.1[idx] {
            continue; // NULL input
        }
        let input_val = snap.0[idx];
        if st.is_null {
            if info.finalfn == crate::postgres_ext::InvalidOid && is_strict_minmax(info.transfn) {
                st.value = input_val;
                st.is_null = false;
            } else {
                match call_transition_nullable(info.transfn, None, input_val) {
                    Some(v) => { st.value = v; st.is_null = false; }
                    None => { st.is_null = true; }
                }
            }
            continue;
        }
        let result = OidFunctionCall2(info.transfn, st.value, input_val)
            .unwrap_or_else(|| unreachable!("transfn non-NULL once state non-null"));
        st.value = result;
        st.is_null = false;
    }
}

/// Build the result tuple for one finalized group: walk the Agg targetlist, filling
/// each non-junk TargetEntry from either the finalized aggregate value (Aggref) or
/// the group-key representative row (Var). `group_row` is the representative input
/// tuple (the group's first row) for reading grouping Vars; `None` for PLAIN with
/// no rows (only Aggref columns can appear then).
fn project_group<'r>(
    run: &'r mut AggRun<'_>,
    group_row: Option<&(Vec<Datum>, Vec<bool>)>,
    pergroup: &[PerGroupState],
) -> &'r mut TupleTableSlot {
    // Finalize every aggregate up front (positional aggno order).
    let finals: Vec<(Datum, bool)> =
        run.aggs.iter().zip(pergroup.iter()).map(|(i, s)| finalize_aggregate(i, s)).collect();

    // Walk the targetlist, producing one output attribute per non-junk entry.
    let targetlist: Vec<Node> = match run.state.ss.ps.plan.as_ref() {
        Some(Node::Agg(a)) => a.plan.targetlist.clone(),
        _ => unreachable!("AggState plan is an Agg"),
    };

    let mut out_values: Vec<Datum> = Vec::new();
    let mut out_nulls: Vec<bool> = Vec::new();
    let mut aggno = 0usize;
    for te in &targetlist {
        let Node::TargetEntry(te) = te else { continue };
        if te.resjunk {
            continue;
        }
        match te.expr.as_ref() {
            Some(Node::Aggref(_)) => {
                let (v, n) = finals.get(aggno).copied().unwrap_or((Datum(0), true));
                aggno += 1;
                out_values.push(v);
                out_nulls.push(n);
            }
            Some(Node::Var(v)) => {
                let idx = (v.varattno - 1) as usize;
                let (val, null) =
                    group_row.map_or((Datum(0), true), |row| (row.0[idx], row.1[idx]));
                out_values.push(val);
                out_nulls.push(null);
            }
            _ => unimplemented!("ExecAgg: non-Var/Aggref targetlist entry not yet reachable"),
        }
    }

    let slot = run
        .state
        .ss
        .ps
        .ps_result_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("ExecAgg: no result slot"));
    ExecClearTuple(slot);
    let n = out_values.len();
    slot.values[..n].copy_from_slice(&out_values);
    slot.isnull[..n].copy_from_slice(&out_nulls);
    exec_store_virtual_tuple(slot);
    slot
}

/// PG `ExecEndAgg`: tear down the child.
pub fn exec_end_agg(shared: Option<&Arc<SharedState>>, run: &mut AggRun<'_>) {
    exec_end_node(shared, &mut run.child);
}

/// PG `ExecReScanAgg`: reset the aggregate state. The child rescan is the caller's
/// responsibility.
pub fn exec_rescan_agg(run: &mut AggRun<'_>) {
    run.cur_group = init_pergroup(&run.aggs);
    run.first_tuple = None;
    run.hash_groups = None;
    run.hash_cursor = 0;
    run.input_done = false;
    run.emitted = false;
}

#[cfg(test)]
mod tests {
    //! Aggregate executor integration: count/sum/min/max over a TupleSource child,
    //! with the aggregate catalog warmed by a real `bootstrap_catalogs` initdb on a
    //! tempdir cluster (so the AGGFNOID syscache resolves each Aggref's transfn).
    //! Covers AGG_PLAIN (whole-table), AGG_SORTED (grouped over pre-sorted input),
    //! and AGG_HASHED (grouped via hash). Plans are hand-built (the planner's Agg
    //! createplan lands at step 26).
    use super::*;
    use std::sync::Arc;

    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::executor::execTuples::{exec_store_virtual_tuple, make_tuple_table_slot};
    use crate::executor::tuptable::{slot_getattr, TTSOpsVirtual, TupleTableSlot};
    use crate::nodes::nodes::{AggSplit, AggStrategy};
    use crate::nodes::plannodes::{Agg, Plan};
    use crate::nodes::primnodes::{Aggref, TargetEntry};
    use crate::postgres::{DatumGetInt32, DatumGetInt64, Int32GetDatum};
    use crate::shared_state::{SharedState, SharedStateConfig};

    const INT4OID: Oid = Oid::new(23);
    const INT8OID: Oid = Oid::new(20);
    const INVALID: Oid = crate::postgres_ext::InvalidOid;
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    /// Run an async body on a multi-thread runtime with a large worker stack. The
    /// `bootstrap_catalogs` initdb path is deeply nested (boxed/pinned in PG's boot
    /// session), overflowing the default test thread stack; the wire tests get a big
    /// stack from the supervisor, so the node-level tests build one explicitly.
    fn run_big_stack<Fut>(f: impl FnOnce() -> Fut + Send + 'static)
    where
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_stack_size(64 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap();
        // spawn onto a worker thread (which has the large stack); block_on runs on
        // the current test thread, so the work itself must be on a worker.
        let handle = rt.spawn(async move { f().await });
        rt.block_on(handle).unwrap();
    }

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-agg25b-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 256,
            ..Default::default()
        })
    }

    /// initdb (warms AGGFNOID + the M3/M4 caches) then run `f`, all inside one
    /// backend session + relcache/catcache scope stack (the warm catcache must
    /// outlive the run). Delegates to the proven tcop initdb harness.
    async fn with_initdb<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + 'static,
        T: Send + 'static,
    {
        crate::backend::tcop::postgres::bootstrap_then(shared, f).await
    }

    fn int4_desc(ncols: usize) -> TupleDesc {
        let mut d = TupleDescData::create_template(ncols as i32);
        for (i, name) in ["a", "b"].iter().take(ncols).enumerate() {
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

    fn empty_plan(targetlist: Vec<Node>) -> Plan {
        Plan {
            disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0, plan_width: 0,
            parallel_aware: false, parallel_safe: false, async_capable: false, plan_node_id: 0,
            targetlist, qual: Vec::new(), lefttree: None, righttree: None,
            init_plan: Vec::new(), ext_param: None, all_param: None,
        }
    }

    fn aggref(aggfnoid: Oid, rettype: Oid, arg_attno: Option<i16>, aggno: i32) -> Aggref {
        let args = arg_attno.map_or_else(Vec::new, |attno| {
            vec![Node::Var(Box::new(crate::backend::nodes::makefuncs::make_var(
                1, attno, INT4OID, -1, INVALID, 0,
            )))]
        });
        Aggref {
            aggfnoid, aggtype: rettype, aggcollid: INVALID, inputcollid: INVALID,
            aggtranstype: INVALID, aggargtypes: Vec::new(), aggdirectargs: Vec::new(),
            args, aggorder: Vec::new(), aggdistinct: Vec::new(), aggfilter: None,
            aggstar: arg_attno.is_none(), aggvariadic: false, aggkind: b'n' as i8,
            aggpresorted: false, agglevelsup: 0, aggsplit: AggSplit::SIMPLE, aggno,
            aggtransno: aggno, location: -1,
        }
    }

    fn te(expr: Node, resno: i16) -> Node {
        Node::TargetEntry(Box::new(TargetEntry {
            expr: Some(expr), resno, resname: None, ressortgroupref: 0,
            resorigtbl: INVALID, resorigcol: 0, resjunk: false,
        }))
    }

    fn agg_node(strategy: AggStrategy, targetlist: Vec<Node>, grp_cols: Vec<i16>) -> Agg {
        let n = grp_cols.len() as i32;
        Agg {
            plan: empty_plan(targetlist),
            aggstrategy: strategy, aggsplit: AggSplit::SIMPLE, num_cols: n,
            grp_col_idx: grp_cols, grp_operators: vec![Oid::new(96); n as usize],
            grp_collations: vec![INVALID; n as usize], num_groups: 0, transition_space: 0,
            agg_params: None, grouping_sets: Vec::new(), chain: Vec::new(),
        }
    }

    async fn run_agg(node: Agg, child: PlanStateNode<'static>, ncols: usize) -> Vec<Vec<i64>> {
        let mut estate = EState::default();
        let mut run = exec_init_agg(&node, &mut estate, child);
        let mut out = Vec::new();
        loop {
            let Some(slot) = Box::pin(exec_agg(None, &mut run)).await else { break };
            // int8 result (count/sum) or int4 (group key/min/max) -- read raw bits
            // (i64::MIN sentinel for NULL); test columns interpret per position.
            let row = (1..=ncols as i32)
                .map(|a| slot_getattr(slot, a).map_or(i64::MIN, |d| d.0 as i64))
                .collect();
            out.push(row);
        }
        exec_end_agg(None, &mut run);
        out
    }

    /// SELECT count(*) FROM t (AGG_PLAIN) -> the row count.
    #[test]
    fn plain_count_star() {
        run_big_stack(|| async {
            let got = with_initdb(new_shared(), |_s| async move {
                let desc = int4_desc(1);
                let child = make_source(&desc, vec![vec![1], vec![2], vec![3], vec![4]]);
                // count() = aggfnoid 2803, returns int8.
                let tlist = vec![te(Node::Aggref(Box::new(aggref(Oid::new(2803), INT8OID, None, 0))), 1)];
                run_agg(agg_node(AggStrategy::PLAIN, tlist, vec![]), child, 1).await
            })
            .await;
            assert_eq!(got, vec![vec![4i64]], "count(*) over 4 rows");
        });
    }

    /// count(*) over an empty input still emits one row (0).
    #[test]
    fn plain_count_star_empty() {
        run_big_stack(|| async {
            let got = with_initdb(new_shared(), |_s| async move {
                let desc = int4_desc(1);
                let child = make_source(&desc, vec![]);
                let tlist = vec![te(Node::Aggref(Box::new(aggref(Oid::new(2803), INT8OID, None, 0))), 1)];
                run_agg(agg_node(AggStrategy::PLAIN, tlist, vec![]), child, 1).await
            })
            .await;
            assert_eq!(got, vec![vec![0i64]], "count(*) over empty input is 0");
        });
    }

    /// SELECT count(*), sum(a) FROM t (AGG_PLAIN) -> count + sum.
    #[test]
    fn plain_count_and_sum() {
        run_big_stack(|| async {
            let got = with_initdb(new_shared(), |_s| async move {
                let desc = int4_desc(1);
                let child = make_source(&desc, vec![vec![10], vec![20], vec![30]]);
                // sum(int4) = aggfnoid 2108 (-> int8), input column a (attno 1).
                let tlist = vec![
                    te(Node::Aggref(Box::new(aggref(Oid::new(2803), INT8OID, None, 0))), 1),
                    te(Node::Aggref(Box::new(aggref(Oid::new(2108), INT8OID, Some(1), 1))), 2),
                ];
                run_agg(agg_node(AggStrategy::PLAIN, tlist, vec![]), child, 2).await
            })
            .await;
            assert_eq!(got, vec![vec![3i64, 60i64]], "count=3, sum=60");
        });
    }

    /// SELECT min(a), max(a) FROM t (AGG_PLAIN) -> min + max (int4).
    #[test]
    fn plain_min_max() {
        run_big_stack(|| async {
            let got = with_initdb(new_shared(), |_s| async move {
                let desc = int4_desc(1);
                let child = make_source(&desc, vec![vec![5], vec![2], vec![9], vec![3]]);
                // min(int4) = 2132, max(int4) = 2116, both -> int4.
                let tlist = vec![
                    te(Node::Aggref(Box::new(aggref(Oid::new(2132), INT4OID, Some(1), 0))), 1),
                    te(Node::Aggref(Box::new(aggref(Oid::new(2116), INT4OID, Some(1), 1))), 2),
                ];
                run_agg(agg_node(AggStrategy::PLAIN, tlist, vec![]), child, 2).await
            })
            .await;
            // int4 datums: read low 32 bits.
            let min = DatumGetInt32(Datum(got[0][0] as usize));
            let max = DatumGetInt32(Datum(got[0][1] as usize));
            assert_eq!((min, max), (2, 9), "min=2, max=9");
        });
    }

    /// SELECT a, count(*) FROM t GROUP BY a, over input pre-sorted by a (AGG_SORTED).
    #[test]
    fn sorted_grouped_count() {
        run_big_stack(|| async {
            let got = with_initdb(new_shared(), |_s| async move {
                let desc = int4_desc(1);
                // pre-sorted by a: groups {1: x2, 2: x3, 3: x1}.
                let child = make_source(&desc, vec![vec![1], vec![1], vec![2], vec![2], vec![2], vec![3]]);
                // targetlist: a (Var), count(*) (Aggref) -> result cols (a int4, count int8).
                let tlist = vec![
                    te(Node::Var(Box::new(crate::backend::nodes::makefuncs::make_var(1, 1, INT4OID, -1, INVALID, 0))), 1),
                    te(Node::Aggref(Box::new(aggref(Oid::new(2803), INT8OID, None, 0))), 2),
                ];
                run_agg(agg_node(AggStrategy::SORTED, tlist, vec![1]), child, 2).await
            })
            .await;
            // (group-key a as int4, count as int8) per group.
            let decoded: Vec<(i32, i64)> = got
                .iter()
                .map(|r| (DatumGetInt32(Datum(r[0] as usize)), DatumGetInt64(Datum(r[1] as usize))))
                .collect();
            assert_eq!(decoded, vec![(1, 2), (2, 3), (3, 1)], "per-group counts");
        });
    }

    /// SELECT a, count(*) FROM t GROUP BY a via the hash strategy (AGG_HASHED).
    /// Input is NOT pre-sorted; the hash table groups it.
    #[test]
    fn hashed_grouped_count() {
        run_big_stack(|| async {
            let got = with_initdb(new_shared(), |_s| async move {
                let desc = int4_desc(1);
                let child = make_source(&desc, vec![vec![2], vec![1], vec![2], vec![3], vec![1], vec![2]]);
                let tlist = vec![
                    te(Node::Var(Box::new(crate::backend::nodes::makefuncs::make_var(1, 1, INT4OID, -1, INVALID, 0))), 1),
                    te(Node::Aggref(Box::new(aggref(Oid::new(2803), INT8OID, None, 0))), 2),
                ];
                run_agg(agg_node(AggStrategy::HASHED, tlist, vec![1]), child, 2).await
            })
            .await;
            let mut decoded: Vec<(i32, i64)> = got
                .iter()
                .map(|r| (DatumGetInt32(Datum(r[0] as usize)), DatumGetInt64(Datum(r[1] as usize))))
                .collect();
            decoded.sort_unstable();
            assert_eq!(decoded, vec![(1, 2), (2, 3), (3, 1)], "hashed per-group counts");
        });
    }
}
