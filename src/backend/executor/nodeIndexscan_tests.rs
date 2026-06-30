//! M6 executor integration: a hand-built single-column int4 btree index over a
//! heap, driven through the full executor (`standard_executor_start_indexed` ->
//! run -> end). Builds the IndexScan / IndexOnlyScan plan nodes directly (the
//! planner's index-path selection is step 28), the index via `btbuild` (so the
//! index tuples carry real heap TIDs for `index_fetch_heap`), and verifies:
//!   * `a = 2` point lookup returns the matching row via the index,
//!   * `a > 1` range scan returns the rows > 1 in index order,
//!   * an index-only scan returns the indexed column (heap-fetched on M6, since
//!     the VM is stubbed),
//!   * `ExecReScan` re-runs the scan.

#![allow(clippy::large_futures, reason = "test bodies chain the full executor async stack; run via block_on")]
#![allow(clippy::default_trait_access, reason = "IndexInfo opaque-forward fields constructed via Default")]
#![allow(clippy::field_reassign_with_default, reason = "EState built incrementally in the rescan test for clarity")]
#![allow(clippy::items_after_statements, reason = "test-local async helper fn defined where it is used")]
#![allow(clippy::let_and_return, reason = "named binding documents the collected rows")]

use std::sync::{Arc, Mutex};

use crate::access::sdir::ScanDirection;
use crate::backend::access::transam::xact::{
    CommandCounterIncrement, GetCurrentCommandId, StartTransactionCommand,
};
use crate::backend::access::common::heaptuple::heap_form_tuple;
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::executor::execMain::{
    standard_executor_end, standard_executor_run, standard_executor_start_indexed,
};
use crate::backend::nodes::makefuncs::{make_const, make_opclause, make_target_entry, make_var};
use crate::backend::utils::cache::relcache::{
    index_init_opclass_support, relation_init_index_access_info,
};
use crate::backend::utils::time::snapmgr::{GetTransactionSnapshot, PushActiveSnapshot};
use crate::catalog::pg_class::{
    FormData_pg_class, RELKIND_INDEX, RELKIND_RELATION, RELPERSISTENCE_PERMANENT,
};
use crate::catalog::pg_index::FormData_pg_index;
use crate::common::relpath::ForkNumber;
use crate::executor::execdesc::QueryDesc;
use crate::executor::instrument::InstrumentOption;
use crate::executor::tuptable::{slot_getattr, TupleTableSlot};
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::plannodes::{IndexOnlyScan, IndexScan, Plan, PlannedStmt, Scan};
use crate::nodes::primnodes::INDEX_VAR;
use crate::postgres::{Datum, DatumGetInt32, Int32GetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::relfilelocator::RelFileLocator;
use crate::tcop::dest::{CommandDest, DestReceiver};
use crate::utils::rel::{LockInfoData, LockRelId, RelationData};

const INT4OID: Oid = Oid::new(23);
const INT4_BTREE_OPS_OID: Oid = Oid::new(1978);
// int4 btree comparison operators.
const INT4_EQ: Oid = Oid::new(96);
const INT4_GT: Oid = Oid::new(521);

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn new_shared() -> Arc<SharedState> {
    use crate::shared_state::SharedStateConfig;
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-exec27a-{}-{}", std::process::id(), n));
    let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
    let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
    SharedState::new(SharedStateConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        nbuffers: 256,
        ..Default::default()
    })
}

async fn in_all_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
where
    F: FnOnce(Arc<SharedState>) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    use crate::backend::access::transam::xact::xact_scope;
    use crate::backend::access::transam::xloginsert::with_insertion;
    use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};
    let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
    let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");
    crate::session::scope(
        sess,
        crate::backend::utils::resowner::resowner::scope(
            owner,
            xact_scope(snapmgr_scope(combocid_scope(with_insertion(f(shared))))),
        ),
    )
    .await
}

fn rloc(rel: u32) -> RelFileLocator {
    RelFileLocator { spcOid: Oid::new(1663), dbOid: Oid::new(90000), relNumber: Oid::new(82000 + rel) }
}

fn one_int4_desc() -> crate::access::tupdesc::TupleDesc {
    use crate::access::tupdesc::TupleDescData;
    let mut d = TupleDescData::create_template(1);
    d.init_builtin_entry(1, "a", INT4OID, -1, 0);
    Arc::new(d)
}

fn make_relation(
    locator: RelFileLocator,
    tupdesc: crate::access::tupdesc::TupleDesc,
    relkind: i8,
) -> Arc<RelationData> {
    use std::sync::atomic::Ordering;
    // SAFETY: FormData_pg_class is repr(C) POD; all-zero is valid.
    let mut form: Box<FormData_pg_class> = Box::new(unsafe { core::mem::zeroed() });
    form.relkind = relkind;
    form.relpersistence = RELPERSISTENCE_PERMANENT;
    form.relnatts = tupdesc.natts as i16;
    form.relam = Oid::new(403);
    let mut rel = RelationData::blank();
    rel.rd_locator = locator;
    rel.rd_refcnt.store(1, Ordering::Relaxed);
    rel.rd_isvalid.store(true, Ordering::Relaxed);
    rel.rd_rel = Some(form);
    rel.rd_att = Some(tupdesc);
    rel.rd_id = locator.relNumber;
    rel.rd_lockInfo = LockInfoData {
        lockRelId: LockRelId { relId: locator.relNumber, dbId: locator.dbOid },
    };
    Arc::new(rel)
}

fn init_index_support(index: &mut RelationData, nkeyatts: i16) {
    // SAFETY: FormData_pg_index is repr(C) POD; zero then patch the fixed fields.
    let mut idx: Box<FormData_pg_index> = Box::new(unsafe { core::mem::zeroed() });
    idx.indnatts = nkeyatts;
    idx.indnkeyatts = nkeyatts;
    idx.indisunique = false;
    idx.indnullsnotdistinct = false;
    index.rd_index = Some(idx);
    relation_init_index_access_info(index);
    let opclasses = vec![INT4_BTREE_OPS_OID; nkeyatts as usize];
    let collations = vec![InvalidOid; nkeyatts as usize];
    let indoption = vec![0i16; nkeyatts as usize];
    index_init_opclass_support(index, &opclasses, &collations, &indoption);
}

async fn create_main_fork(shared: &Arc<SharedState>, locator: RelFileLocator) {
    let mut smgr = crate::storage::smgr::SmgrRelation::open(
        locator,
        crate::storage::procnumber::INVALID_PROC_NUMBER,
    );
    smgr.create(shared, ForkNumber::MAIN_FORKNUM, false).await;
}

async fn insert_row1(shared: &Arc<SharedState>, relation: &Arc<RelationData>, a: i32) {
    let desc = relation.rd_att.clone().unwrap();
    let mut tuple = heap_form_tuple(&desc, &[Int32GetDatum(a)], &[false]);
    let cid = GetCurrentCommandId(true);
    heap_insert(shared, relation, &mut tuple, cid, 0).await;
}

fn index_info(nkeys: i32) -> crate::nodes::execnodes::IndexInfo {
    crate::nodes::execnodes::IndexInfo {
        num_index_attrs: nkeys,
        num_index_key_attrs: nkeys,
        index_attr_numbers: (1..=nkeys).map(|i| i as i16).collect(),
        expressions: Vec::new(),
        expressions_state: Vec::new(),
        predicate: Vec::new(),
        predicate_state: None,
        exclusion_ops: Vec::new(),
        exclusion_procs: Vec::new(),
        exclusion_strats: Vec::new(),
        unique_ops: Vec::new(),
        unique_procs: Vec::new(),
        unique_strats: Vec::new(),
        unique: false,
        nulls_not_distinct: false,
        ready_for_inserts: true,
        checked_unchanged: false,
        index_unchanged: false,
        concurrent: false,
        broken_hot_chain: false,
        summarizing: false,
        without_overlaps: false,
        parallel_workers: 0,
        am: Oid::new(403),
        am_cache: Default::default(),
        context: Default::default(),
    }
}

fn txn_snapshot(shared: &Arc<SharedState>) -> crate::utils::snapshot::Snapshot {
    let mut snap = GetTransactionSnapshot(shared);
    Arc::make_mut(snap.as_mut().expect("a transaction snapshot")).curcid =
        GetCurrentCommandId(false);
    snap
}

/// An empty `Plan` body (the scan node's plan), projecting `targetlist`.
fn scan_plan_body(targetlist: Vec<Node>) -> Plan {
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
        targetlist,
        qual: Vec::new(),
        lefttree: None,
        righttree: None,
        init_plan: Vec::new(),
        ext_param: None,
        all_param: None,
    }
}

/// A targetlist projecting heap column `a` (attno 1, varno 1).
fn project_col_a(desc: &crate::access::tupdesc::TupleDesc) -> Vec<Node> {
    let att = desc.attr(0);
    let var = make_var(1, 1, att.atttypid, att.atttypmod, att.attcollation, 0);
    let tle = make_target_entry(Some(Node::Var(Box::new(var))), 1, Some("a".to_string()), false);
    vec![Node::TargetEntry(Box::new(tle))]
}

/// The proc OID backing a btree comparison operator (so `indexqualorig` compiles).
fn op_proc(op: Oid) -> Oid {
    match op {
        INT4_EQ => Oid::new(65),  // int4eq
        INT4_GT => Oid::new(147), // int4gt
        _ => unreachable!("test uses only int4 = / >"),
    }
}

/// An indexqual `a <op> <value>`: OpExpr(op, Var(INDEX_VAR, attno 1), Const(value)).
fn index_qual(op: Oid, value: i32) -> Node {
    let leftvar = make_var(INDEX_VAR, 1, INT4OID, -1, InvalidOid, 0);
    let rightconst = make_const(INT4OID, -1, InvalidOid, 4, Int32GetDatum(value), false, true);
    let mut clause = make_opclause(
        op,
        Oid::new(16), // bool result type
        false,
        Some(Node::Var(Box::new(leftvar))),
        Some(Node::Const(Box::new(rightconst))),
        InvalidOid,
        InvalidOid,
    );
    if let Node::OpExpr(op_expr) = &mut clause {
        op_expr.opfuncid = op_proc(op);
    }
    clause
}

fn planned_stmt(plan_tree: Node) -> PlannedStmt {
    PlannedStmt {
        command_type: CmdType::SELECT,
        query_id: 0,
        plan_id: 0,
        has_returning: false,
        has_modifying_cte: false,
        can_set_tag: true,
        transient_plan: false,
        depends_on_role: false,
        parallel_mode_needed: false,
        jit_flags: 0,
        plan_tree,
        part_prune_infos: Vec::new(),
        rtable: vec![Node::Const(Box::new(make_const(
            INT4OID, -1, InvalidOid, 4, Int32GetDatum(0), false, true,
        )))],
        unprunable_relids: None,
        perm_infos: Vec::new(),
        result_relations: Vec::new(),
        append_relations: Vec::new(),
        subplans: Vec::new(),
        rewind_plan_ids: None,
        row_marks: Vec::new(),
        relation_oids: Vec::new(),
        inval_items: Vec::new(),
        param_exec_types: Vec::new(),
        utility_stmt: None,
        stmt_location: -1,
        stmt_len: 0,
    }
}

#[allow(deprecated)]
fn query_desc(stmt: PlannedStmt, snap: crate::utils::snapshot::Snapshot) -> (QueryDesc<'static>, Arc<Mutex<Collected>>) {
    let sink = Arc::new(Mutex::new(Collected::default()));
    let dest: Box<dyn DestReceiver> = Box::new(CollectingDest { sink: Arc::clone(&sink) });
    let qd = QueryDesc {
        operation: stmt.command_type,
        plannedstmt: Some(Box::new(stmt)),
        sourceText: String::new(),
        snapshot: Some(Box::new(snap)),
        crosscheck_snapshot: None,
        dest: Some(dest),
        params: None,
        queryEnv: None,
        instrument_options: InstrumentOption::empty(),
        tupDesc: None,
        estate: None,
        planstate: None,
        already_executed: false,
        totaltime: None,
    };
    (qd, sink)
}

#[derive(Default)]
struct Collected {
    rows: Vec<Vec<i32>>,
}
struct CollectingDest {
    sink: Arc<Mutex<Collected>>,
}
impl DestReceiver for CollectingDest {
    fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool {
        let natts = i32::from(slot.nvalid);
        let row = (1..=natts)
            .map(|attno| DatumGetInt32(slot_getattr(slot, attno).unwrap_or(Datum(0))))
            .collect();
        self.sink.lock().unwrap().rows.push(row);
        true
    }
    fn r_startup(&mut self, _op: CmdType, _ti: crate::access::tupdesc::TupleDesc) {}
    fn r_shutdown(&mut self) {}
    fn mydest(&self) -> CommandDest {
        CommandDest::DestNone
    }
}

/// Run a SELECT QueryDesc to completion over a heap + index, returning each row's
/// single int4 column.
async fn run_plan(
    shared: &Arc<SharedState>,
    heap: &Arc<RelationData>,
    index: &Arc<RelationData>,
    stmt: PlannedStmt,
) -> Vec<Vec<i32>> {
    let snap = txn_snapshot(shared);
    let range_table_rels: Vec<Option<&RelationData>> = vec![Some(&**heap)];
    let index_rels: Vec<Option<&RelationData>> = vec![Some(&**index)];
    let snapshot_ref = snap.as_deref();
    let (mut qd, sink) = query_desc(stmt, snap.clone());

    standard_executor_start_indexed(&mut qd, &range_table_rels, &index_rels, snapshot_ref, 0);
    standard_executor_run(Some(shared), &mut qd, ScanDirection::Forward, 0).await;
    standard_executor_end(Some(shared), &mut qd);
    drop(qd);

    let out = sink.lock().unwrap().rows.clone();
    out
}

/// Set up a heap t(a int) with `vals`, build a btree index on `a`, return both.
async fn setup_heap_index(
    shared: &Arc<SharedState>,
    rel_n: u32,
    vals: &[i32],
) -> (Arc<RelationData>, Arc<RelationData>) {
    StartTransactionCommand(shared).await;
    PushActiveSnapshot(GetTransactionSnapshot(shared));
    let hloc = rloc(rel_n);
    let iloc = rloc(rel_n + 100);
    create_main_fork(shared, hloc).await;
    create_main_fork(shared, iloc).await;
    let heap = make_relation(hloc, one_int4_desc(), RELKIND_RELATION);
    let mut index = make_relation(iloc, one_int4_desc(), RELKIND_INDEX);
    init_index_support(Arc::get_mut(&mut index).unwrap(), 1);

    for &v in vals {
        insert_row1(shared, &heap, v).await;
    }
    CommandCounterIncrement();
    PushActiveSnapshot(txn_snapshot(shared));
    let ii = index_info(1);
    crate::backend::access::nbtree::nbtree::btbuild(shared, &heap, &index, &ii).await;
    (heap, index)
}

fn index_scan_stmt(desc: &crate::access::tupdesc::TupleDesc, indexid: Oid, op: Oid, value: i32) -> PlannedStmt {
    let qual = index_qual(op, value);
    let node = IndexScan {
        scan: Scan { plan: scan_plan_body(project_col_a(desc)), scanrelid: 1 },
        indexid,
        indexqual: vec![qual.clone()],
        indexqualorig: vec![qual],
        indexorderby: Vec::new(),
        indexorderbyorig: Vec::new(),
        indexorderbyops: Vec::new(),
        indexorderdir: ScanDirection::Forward,
    };
    planned_stmt(Node::IndexScan(Box::new(node)))
}

#[tokio::test(flavor = "multi_thread")]
async fn index_scan_point_lookup_eq() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        let (heap, index) = setup_heap_index(&shared, 1, &[30, 10, 50, 20, 40]).await;
        let desc = one_int4_desc();
        // a = 20 -> one row.
        let rows = run_plan(&shared, &heap, &index, index_scan_stmt(&desc, index.rd_id, INT4_EQ, 20)).await;
        assert_eq!(rows, vec![vec![20]]);
        // a = 25 -> no rows.
        let rows = run_plan(&shared, &heap, &index, index_scan_stmt(&desc, index.rd_id, INT4_EQ, 25)).await;
        assert!(rows.is_empty());
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn index_scan_range_gt_in_order() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        let (heap, index) = setup_heap_index(&shared, 2, &[30, 10, 50, 20, 40]).await;
        let desc = one_int4_desc();
        // a > 20 -> 30, 40, 50 in index order.
        let rows = run_plan(&shared, &heap, &index, index_scan_stmt(&desc, index.rd_id, INT4_GT, 20)).await;
        assert_eq!(rows, vec![vec![30], vec![40], vec![50]]);
        // a > 1 -> all rows in order.
        let rows = run_plan(&shared, &heap, &index, index_scan_stmt(&desc, index.rd_id, INT4_GT, 1)).await;
        assert_eq!(rows, vec![vec![10], vec![20], vec![30], vec![40], vec![50]]);
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn index_only_scan_returns_indexed_column() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        let (heap, index) = setup_heap_index(&shared, 3, &[30, 10, 50, 20, 40]).await;
        let desc = one_int4_desc();
        // indextlist = the index's column a (varno INDEX_VAR, attno 1).
        let att = desc.attr(0);
        let ivar = make_var(INDEX_VAR, 1, att.atttypid, att.atttypmod, att.attcollation, 0);
        let itle = make_target_entry(Some(Node::Var(Box::new(ivar))), 1, Some("a".to_string()), false);
        let indextlist = vec![Node::TargetEntry(Box::new(itle))];
        // The output targetlist reads the scan slot (varno 1, attno 1).
        let ovar = make_var(1, 1, att.atttypid, att.atttypmod, att.attcollation, 0);
        let otle = make_target_entry(Some(Node::Var(Box::new(ovar))), 1, Some("a".to_string()), false);

        let qual = index_qual(INT4_GT, 20);
        let node = IndexOnlyScan {
            scan: Scan { plan: scan_plan_body(vec![Node::TargetEntry(Box::new(otle))]), scanrelid: 1 },
            indexid: index.rd_id,
            indexqual: vec![qual.clone()],
            recheckqual: vec![qual],
            indexorderby: Vec::new(),
            indextlist,
            indexorderdir: ScanDirection::Forward,
        };
        let rows = run_plan(&shared, &heap, &index, planned_stmt(Node::IndexOnlyScan(Box::new(node)))).await;
        assert_eq!(rows, vec![vec![30], vec![40], vec![50]]);
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn index_scan_rescan_reruns() {
    use crate::backend::executor::execAmi::exec_rescan;
    use crate::backend::executor::execProcnode::{exec_init_node, exec_proc_node, PlanStateNode};
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        let (heap, index) = Box::pin(setup_heap_index(&shared, 4, &[30, 10, 50, 20, 40])).await;
        let desc = one_int4_desc();
        let stmt = index_scan_stmt(&desc, index.rd_id, INT4_GT, 20);

        // Drive the node directly so we can ExecReScan it mid-life.
        let snap = txn_snapshot(&shared);
        let range_table_rels: Vec<Option<&RelationData>> = vec![Some(&*heap)];
        let index_rels: Vec<Option<&RelationData>> = vec![Some(&*index)];
        let mut estate = crate::nodes::execnodes::EState::default();
        estate.es_range_table_rels = &range_table_rels;
        estate.es_index_rels = &index_rels;
        estate.es_snapshot_ref = snap.as_deref();

        let Node::IndexScan(_) = &stmt.plan_tree else { unreachable!() };
        let mut node: PlanStateNode = exec_init_node(Some(&stmt.plan_tree), &mut estate, 0).expect("init");

        async fn collect(n: &mut PlanStateNode<'_>, sh: &Arc<SharedState>) -> Vec<i32> {
            let mut out = Vec::new();
            while let Some(slot) = exec_proc_node(Some(sh), n).await {
                out.push(DatumGetInt32(slot_getattr(slot, 1).unwrap()));
            }
            out
        }

        let first = Box::pin(collect(&mut node, &shared)).await;
        assert_eq!(first, vec![30, 40, 50]);

        exec_rescan(&shared, &mut node);
        let second = Box::pin(collect(&mut node, &shared)).await;
        assert_eq!(second, vec![30, 40, 50]);

        crate::backend::executor::execProcnode::exec_end_node(Some(&shared), &mut node);
    }))
    .await;
}
