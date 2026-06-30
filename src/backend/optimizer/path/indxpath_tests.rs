//! M6 planner integration: with a btree index present, the planner chooses an index
//! (IndexScan / BitmapHeapScan) over a sequential scan when a WHERE clause matches
//! the index, by cost; without a matching clause it keeps the seqscan. Runs the full
//! pipeline (raw_parser -> analyze -> rewrite -> standard_planner) over an initdb'd
//! tempdir cluster with a real `t(a int)` table and a btree index on `a` (created via
//! the DefineIndex CREATE INDEX driver).

#![allow(clippy::large_futures, reason = "test bodies chain the full async catalog/planner stack")]

use std::sync::Arc;

use crate::backend::tcop::postgres::bootstrap_then;
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::PlannedStmt;
use crate::postgres_ext::Oid;
use crate::shared_state::{SharedState, SharedStateConfig};

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn new_shared() -> Arc<SharedState> {
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-indxpath-{}-{}", std::process::id(), n));
    let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
    let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
    SharedState::new(SharedStateConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        nbuffers: 256,
        ..Default::default()
    })
}

/// Create a user table `t(a int)` and return its OID (mirrors the analyze tests).
async fn create_table_t(shared: &Arc<SharedState>) -> Oid {
    use crate::access::tupdesc::TupleDescData;
    use crate::backend::catalog::heap::heap_create_with_catalog;
    use crate::catalog::genbki::INT4OID;
    use crate::catalog::pg_class::{RELKIND_RELATION, RELPERSISTENCE_PERMANENT};
    use crate::catalog::pg_namespace::PG_PUBLIC_NAMESPACE;

    let mut td = TupleDescData::create_template(1);
    td.init_builtin_entry(1, "a", INT4OID, -1, 0);
    let tupdesc = Arc::new(td);

    let relid = heap_create_with_catalog(
        shared,
        "t",
        PG_PUBLIC_NAMESPACE,
        crate::common::relpath::DEFAULTTABLESPACE_OID,
        Oid::new(0),
        Oid::new(0),
        Oid::new(10),
        Oid::new(2),
        tupdesc,
        RELKIND_RELATION,
        RELPERSISTENCE_PERMANENT,
        false,
    )
    .await;
    crate::backend::access::transam::xact::CommandCounterIncrement();
    refresh_active_snapshot(shared);
    relid
}

fn refresh_active_snapshot(shared: &Arc<SharedState>) {
    use crate::backend::access::transam::xact::GetCurrentCommandId;
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };
    PopActiveSnapshot();
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(s) = snap.as_mut() {
        Arc::make_mut(s).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);
}

/// Build the IndexStmt for `CREATE INDEX i ON t (a)` and run DefineIndex.
async fn create_index_on_t(shared: &Arc<SharedState>) {
    use crate::backend::commands::indexcmds::define_index;
    use crate::backend::parser::parser::make_index_elem;
    use crate::nodes::makefuncs::makeRangeVar;
    use crate::nodes::parsenodes::{IndexStmt, SortByDir};

    let elem = make_index_elem("a".to_owned(), SortByDir::DEFAULT);
    let stmt = IndexStmt {
        idxname: Some("i".to_owned()),
        relation: Some(Box::new(makeRangeVar(None, Some("t".to_owned()), -1))),
        accessMethod: None,
        tableSpace: None,
        indexParams: vec![elem],
        indexIncludingParams: Vec::new(),
        options: Vec::new(),
        whereClause: None,
        excludeOpNames: Vec::new(),
        idxcomment: None,
        indexOid: Oid::new(0),
        oldNumber: Oid::new(0),
        oldCreateSubid: crate::c::SubTransactionId(0),
        oldFirstRelfilelocatorSubid: crate::c::SubTransactionId(0),
        unique: false,
        nulls_not_distinct: false,
        primary: false,
        isconstraint: false,
        iswithoutoverlaps: false,
        deferrable: false,
        initdeferred: false,
        transformed: false,
        concurrent: false,
        if_not_exists: false,
        reset_default_tblspc: false,
    };
    define_index(shared, &stmt).await;
    crate::backend::access::transam::xact::CommandCounterIncrement();
    refresh_active_snapshot(shared);
}

/// Run one SQL string through the front pipeline + planner, returning the PlannedStmt.
async fn plan_query(shared: &Arc<SharedState>, sql: &str) -> PlannedStmt {
    use crate::backend::optimizer::plan::planner::standard_planner;
    use crate::backend::parser::analyze::parse_analyze_fixedparams_async;
    use crate::backend::parser::parser::raw_parser;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;

    let mut list = raw_parser(sql, RawParseMode::Default);
    assert_eq!(list.len(), 1, "single statement");
    let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
    let rs: RawStmt = *rs;
    let analyzed = parse_analyze_fixedparams_async(shared, &rs, sql, &[], 0).await;
    let mut rewritten = query_rewrite(*analyzed);
    assert_eq!(rewritten.len(), 1);
    let mut query = rewritten.remove(0);
    standard_planner(&mut query, sql, 0, None)
}

/// The top scan plan node tag (the scan under any upper stage). For these queries
/// (no GROUP/ORDER/LIMIT) the top plan node is the scan itself.
fn scan_node_kind(plan: &PlannedStmt) -> &'static str {
    match &plan.plan_tree {
        Node::SeqScan(_) => "SeqScan",
        Node::IndexScan(_) => "IndexScan",
        Node::IndexOnlyScan(_) => "IndexOnlyScan",
        Node::BitmapHeapScan(_) => "BitmapHeapScan",
        other => panic!("unexpected top plan node: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_plan_chosen_over_seqscan() {
    let shared = new_shared();
    bootstrap_then(shared, |shared| async move {
        // bootstrap_then commits the initdb transaction; open a fresh one for the
        // table/index creation + planning, and push the active snapshot they read.
        use crate::backend::access::transam::xact::{GetCurrentCommandId, StartTransactionCommand};
        use crate::backend::utils::time::snapmgr::{GetTransactionSnapshot, PushActiveSnapshot};
        StartTransactionCommand(&shared).await;
        let mut snap = GetTransactionSnapshot(&shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);

        let relid = create_table_t(&shared).await;

        // Before the index exists: a WHERE qual still plans a SeqScan (no index).
        let before = plan_query(&shared, "SELECT * FROM t WHERE a = 20").await;
        assert_eq!(scan_node_kind(&before), "SeqScan", "no index -> SeqScan");

        // Create the index on t(a).
        create_index_on_t(&shared).await;

        // Simulate a populated, analyzed table (index_update_stats sets these after a
        // real build / ANALYZE) so the cost-based choice is exercised deterministically:
        // a selective qual over a large table must prefer the index. A tiny table is
        // genuinely cheaper to seqscan, so the choice only bites with real volume.
        crate::backend::utils::cache::relcache::update_relation_stats(relid, 1000, 226_000.0);
        refresh_active_snapshot(&shared);

        // A selective equality qual now chooses an index path (IndexScan or the
        // bitmap form), NOT a SeqScan.
        let eq = plan_query(&shared, "SELECT * FROM t WHERE a = 20").await;
        let eq_kind = scan_node_kind(&eq);
        assert!(
            eq_kind == "IndexScan" || eq_kind == "BitmapHeapScan",
            "a = 20 should plan an index scan, got {eq_kind}"
        );

        // A selective range qual likewise chooses an index path.
        let gt = plan_query(&shared, "SELECT * FROM t WHERE a > 100").await;
        let gt_kind = scan_node_kind(&gt);
        assert!(
            gt_kind == "IndexScan" || gt_kind == "BitmapHeapScan",
            "a > 100 should plan an index scan, got {gt_kind}"
        );

        // No WHERE clause: nothing matches the index, so the seqscan stays.
        let all = plan_query(&shared, "SELECT * FROM t").await;
        assert_eq!(scan_node_kind(&all), "SeqScan", "no qual -> SeqScan");
    })
    .await;
}
