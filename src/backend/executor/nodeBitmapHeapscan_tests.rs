//! M6 bitmap-scan integration: a hand-built single-column int4 btree index over a
//! heap, driven through the executor as a BitmapHeapScan over a BitmapIndexScan
//! (and BitmapAnd / BitmapOr of two BitmapIndexScans). The bitmap index scan
//! collects matching heap TIDs into a TIDBitmap; the bitmap heap scan iterates it
//! and heap-fetches each tuple. Verifies:
//!   * `a = 2`   -> the matching row,
//!   * `a > 1`   -> the rows > 1 (bitmap, heap-fetched, ascending block order),
//!   * BitmapAnd(`a > 1`, `a < 4`) -> the intersection,
//!   * BitmapOr(`a = 1`, `a = 4`)  -> the union.
//!
//! The planner's bitmap-path selection is a later milestone; the plans are built by
//! hand here (as in nodeIndexscan_tests).

#![allow(clippy::large_futures, reason = "test bodies chain the full executor async stack; run via block_on")]
#![allow(clippy::default_trait_access, reason = "IndexInfo opaque-forward fields constructed via Default")]
#![allow(clippy::field_reassign_with_default, reason = "EState built incrementally for clarity")]
#![allow(clippy::items_after_statements, reason = "test-local async helper fn defined where it is used")]

use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::backend::access::transam::xact::{
    CommandCounterIncrement, GetCurrentCommandId, StartTransactionCommand,
};
use crate::backend::access::common::heaptuple::heap_form_tuple;
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::executor::execProcnode::{exec_end_node, exec_init_node, exec_proc_node, PlanStateNode};
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
use crate::executor::tuptable::{slot_getattr, TupleTableSlot};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::{BitmapAnd, BitmapHeapScan, BitmapIndexScan, BitmapOr, Plan, Scan};
use crate::nodes::primnodes::INDEX_VAR;
use crate::postgres::{Datum, DatumGetInt32, Int32GetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::rel::{LockInfoData, LockRelId, RelationData};

const INT4OID: Oid = Oid(23);
const INT4_BTREE_OPS_OID: Oid = Oid(1978);
const INT4_EQ: Oid = Oid(96);
const INT4_LT: Oid = Oid(97);
const INT4_GT: Oid = Oid(521);

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn new_shared() -> Arc<SharedState> {
    use crate::shared_state::SharedStateConfig;
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-exec27b-{}-{}", std::process::id(), n));
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
    RelFileLocator { spcOid: Oid(1663), dbOid: Oid(90000), relNumber: Oid(83000 + rel) }
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
    form.relam = Oid(403);
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
        am: Oid(403),
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

fn scan_plan_body(targetlist: Vec<Node>, lefttree: Option<Node>) -> Plan {
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
        lefttree,
        righttree: None,
        init_plan: Vec::new(),
        ext_param: None,
        all_param: None,
    }
}

fn project_col_a(desc: &crate::access::tupdesc::TupleDesc) -> Vec<Node> {
    let att = desc.attr(0);
    let var = make_var(1, 1, att.atttypid, att.atttypmod, att.attcollation, 0);
    let tle = make_target_entry(Some(Node::Var(Box::new(var))), 1, Some("a".to_string()), false);
    vec![Node::TargetEntry(Box::new(tle))]
}

fn op_proc(op: Oid) -> Oid {
    match op {
        INT4_EQ => Oid(65),  // int4eq
        INT4_LT => Oid(66),  // int4lt
        INT4_GT => Oid(147), // int4gt
        _ => unreachable!("test uses only int4 = / < / >"),
    }
}

fn index_qual(op: Oid, value: i32) -> Node {
    let leftvar = make_var(INDEX_VAR, 1, INT4OID, -1, InvalidOid, 0);
    let rightconst = make_const(INT4OID, -1, InvalidOid, 4, Int32GetDatum(value), false, true);
    let mut clause = make_opclause(
        op,
        Oid(16),
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

/// A bitmapqualorig clause reading the HEAP scan slot (varno 1) -- the recheck qual.
fn heap_qual(op: Oid, value: i32) -> Node {
    let leftvar = make_var(1, 1, INT4OID, -1, InvalidOid, 0);
    let rightconst = make_const(INT4OID, -1, InvalidOid, 4, Int32GetDatum(value), false, true);
    let mut clause = make_opclause(
        op,
        Oid(16),
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

/// A BitmapIndexScan node for `a <op> value` against `indexid`.
fn bitmap_index_scan(op: Oid, value: i32, indexid: Oid) -> Node {
    let qual = index_qual(op, value);
    Node::BitmapIndexScan(Box::new(BitmapIndexScan {
        scan: Scan { plan: scan_plan_body(Vec::new(), None), scanrelid: 1 },
        indexid,
        isshared: false,
        indexqual: vec![qual.clone()],
        indexqualorig: vec![qual],
    }))
}

/// A BitmapHeapScan over `child`, projecting column a, rechecking `bitmapqualorig`.
fn bitmap_heap_scan(
    desc: &crate::access::tupdesc::TupleDesc,
    child: Node,
    bitmapqualorig: Vec<Node>,
) -> Node {
    Node::BitmapHeapScan(Box::new(BitmapHeapScan {
        scan: Scan {
            plan: scan_plan_body(project_col_a(desc), Some(child)),
            scanrelid: 1,
        },
        bitmapqualorig,
    }))
}

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

/// Run a hand-built bitmap plan tree to completion, returning each row's int4 `a`.
async fn run_bitmap_plan(
    shared: &Arc<SharedState>,
    heap: &Arc<RelationData>,
    index: &Arc<RelationData>,
    plan_tree: Node,
) -> Vec<i32> {
    let snap = txn_snapshot(shared);
    let range_table_rels: Vec<Option<&RelationData>> = vec![Some(&**heap)];
    let index_rels: Vec<Option<&RelationData>> = vec![Some(&**index)];
    let mut estate = crate::nodes::execnodes::EState::default();
    estate.es_range_table_rels = &range_table_rels;
    estate.es_index_rels = &index_rels;
    estate.es_snapshot_ref = snap.as_deref();

    let mut node: PlanStateNode = exec_init_node(Some(&plan_tree), &mut estate, 0).expect("init");

    let mut out = Vec::<i32>::new();
    while let Some(slot) = exec_proc_node(Some(shared), &mut node).await {
        out.push(DatumGetInt32(slot_getattr(slot, 1).unwrap_or(Datum(0))));
    }
    exec_end_node(Some(shared), &mut node);
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn bitmap_scan_point_lookup_eq() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        let (heap, index) = Box::pin(setup_heap_index(&shared, 1, &[3, 1, 5, 2, 4])).await;
        let desc = one_int4_desc();
        let child = bitmap_index_scan(INT4_EQ, 2, index.rd_id);
        let plan = bitmap_heap_scan(&desc, child, vec![heap_qual(INT4_EQ, 2)]);
        let rows = Box::pin(run_bitmap_plan(&shared, &heap, &index, plan)).await;
        assert_eq!(rows, vec![2]);

        // a = 9 -> empty.
        let child = bitmap_index_scan(INT4_EQ, 9, index.rd_id);
        let plan = bitmap_heap_scan(&desc, child, vec![heap_qual(INT4_EQ, 9)]);
        let rows = Box::pin(run_bitmap_plan(&shared, &heap, &index, plan)).await;
        assert!(rows.is_empty());
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bitmap_scan_range_gt() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        let (heap, index) = Box::pin(setup_heap_index(&shared, 2, &[3, 1, 5, 2, 4])).await;
        let desc = one_int4_desc();
        // a > 1 -> 2,3,4,5 (heap-fetched; bitmap is page/offset ordered).
        let child = bitmap_index_scan(INT4_GT, 1, index.rd_id);
        let plan = bitmap_heap_scan(&desc, child, vec![heap_qual(INT4_GT, 1)]);
        let mut rows = Box::pin(run_bitmap_plan(&shared, &heap, &index, plan)).await;
        rows.sort_unstable();
        assert_eq!(rows, vec![2, 3, 4, 5]);
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bitmap_and_intersection() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        let (heap, index) = Box::pin(setup_heap_index(&shared, 3, &[3, 1, 5, 2, 4])).await;
        let desc = one_int4_desc();
        // (a > 1) AND (a < 4) -> {2, 3}.
        let and = Node::BitmapAnd(Box::new(BitmapAnd {
            plan: scan_plan_body(Vec::new(), None),
            bitmapplans: vec![
                bitmap_index_scan(INT4_GT, 1, index.rd_id),
                bitmap_index_scan(INT4_LT, 4, index.rd_id),
            ],
        }));
        let plan = bitmap_heap_scan(
            &desc,
            and,
            vec![heap_qual(INT4_GT, 1), heap_qual(INT4_LT, 4)],
        );
        let mut rows = Box::pin(run_bitmap_plan(&shared, &heap, &index, plan)).await;
        rows.sort_unstable();
        assert_eq!(rows, vec![2, 3]);
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bitmap_or_union() {
    let shared = new_shared();
    Box::pin(in_all_scopes(shared.clone(), |shared| async move {
        let (heap, index) = Box::pin(setup_heap_index(&shared, 4, &[3, 1, 5, 2, 4])).await;
        let desc = one_int4_desc();
        // (a = 1) OR (a = 4) -> {1, 4}.
        let or = Node::BitmapOr(Box::new(BitmapOr {
            plan: scan_plan_body(Vec::new(), None),
            isshared: false,
            bitmapplans: vec![
                bitmap_index_scan(INT4_EQ, 1, index.rd_id),
                bitmap_index_scan(INT4_EQ, 4, index.rd_id),
            ],
        }));
        // No recheck qual: BitmapOr of exact index scans yields exact pages.
        let plan = bitmap_heap_scan(&desc, or, Vec::new());
        let mut rows = Box::pin(run_bitmap_plan(&shared, &heap, &index, plan)).await;
        rows.sort_unstable();
        assert_eq!(rows, vec![1, 4]);
    }))
    .await;
}
