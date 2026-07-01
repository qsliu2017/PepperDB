//! Integration tests for CLUSTER / VACUUM FULL (step 47).
//!
//! Each test stands up a real foundation `SharedState` over a tempdir + the full
//! per-task scope stack, runs initdb, then drives real SQL through the
//! parse -> analyze -> rewrite -> process_utility pipeline. The assertions check the
//! milestone bar: VACUUM FULL shrinks the heap (relpages/reltuples drop, the
//! relfilenode changes -- a genuinely new physical file) while the live rows stay
//! correct + visible + found by index; CLUSTER physically orders the heap by an
//! index and marks the index indisclustered; and a rewrite loses/duplicates no rows.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_ptr_alignment,
    clippy::type_complexity,
    reason = "tests"
)]

use std::sync::Arc;

use futures_util::FutureExt;

use crate::postgres_ext::Oid;
use crate::shared_state::{SharedState, SharedStateConfig};

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const DB_OID: Oid = Oid::new(90000);

fn new_shared() -> Arc<SharedState> {
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-cluster-{}-{}", std::process::id(), n));
    let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
    let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
    SharedState::new(SharedStateConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        nbuffers: 256,
        ..Default::default()
    })
}

async fn in_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
where
    F: FnOnce(Arc<SharedState>) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    use crate::backend::access::transam::xloginsert::with_insertion;
    use crate::backend::catalog::indexing::scope_async as catalog_index_scope;
    use crate::backend::commands::trigger::after_trigger_scope;
    use crate::backend::utils::cache::catcache::scope_async as catcache_scope;
    use crate::backend::utils::cache::relcache::scope_async as relcache_scope;
    use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};

    let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
    sess.set_database_id(DB_OID);
    sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);
    let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");

    let body = Box::pin(catalog_index_scope(Box::pin(relcache_scope(Box::pin(f(shared))))));
    let body = Box::pin(catcache_scope(body));
    let body = Box::pin(with_insertion(body));
    let body = Box::pin(after_trigger_scope(body));
    let body = Box::pin(combocid_scope(body));
    let body = Box::pin(snapmgr_scope(body));
    let body = Box::pin(crate::backend::access::transam::xact::xact_scope(body));
    crate::session::scope(sess, crate::backend::utils::resowner::resowner::scope(owner, body)).await
}

async fn init_db(shared: &Arc<SharedState>) {
    use crate::backend::access::transam::xact::{
        CommitTransactionCommand, GetCurrentCommandId, GetTopTransactionIdIfAny,
        IsTransactionOrTransactionBlock, StartTransactionCommand,
    };
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };

    StartTransactionCommand(shared).await;
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(s) = snap.as_mut() {
        Arc::make_mut(s).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);
    crate::backend::bootstrap::bootstrap::bootstrap_catalogs(shared).await;
    crate::backend::access::transam::xact::CommandCounterIncrement();
    PopActiveSnapshot();
    let committed = GetTopTransactionIdIfAny();
    CommitTransactionCommand(shared).await;
    if !IsTransactionOrTransactionBlock() {
        crate::backend::tcop::postgres::publish_committed_xid(shared, committed);
    }
    crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
}

async fn run_sql(shared: &Arc<SharedState>, sql: &str) -> Result<Vec<Vec<(i32, bool)>>, String> {
    use crate::backend::access::transam::xact::{
        CommitTransactionCommand, GetCurrentCommandId, GetTopTransactionIdIfAny,
        IsTransactionOrTransactionBlock, StartTransactionCommand,
    };
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };

    install_error_capture_hook();
    clear_last_error();

    StartTransactionCommand(shared).await;
    crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(s) = snap.as_mut() {
        Arc::make_mut(s).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);

    let shared2 = Arc::clone(shared);
    let sql2 = sql.to_string();
    let fut = async move { run_sql_inner(&shared2, &sql2).await };
    match std::panic::AssertUnwindSafe(fut).catch_unwind().await {
        Ok(rows) => {
            PopActiveSnapshot();
            let committed = GetTopTransactionIdIfAny();
            CommitTransactionCommand(shared).await;
            if !IsTransactionOrTransactionBlock() {
                crate::backend::tcop::postgres::publish_committed_xid(shared, committed);
            }
            Ok(rows)
        }
        Err(payload) => {
            let msg = describe_panic(payload.as_ref());
            crate::utils::elog::flush_error_state();
            crate::backend::access::transam::xact::AbortCurrentTransaction(shared).await;
            Err(msg)
        }
    }
}

fn describe_panic(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(ed) = payload.downcast_ref::<crate::utils::elog::ErrorData>() {
        return ed.message.clone().unwrap_or_default();
    }
    if let Some(ed) = payload.downcast_ref::<Box<crate::utils::elog::ErrorData>>() {
        return ed.message.clone().unwrap_or_default();
    }
    if let Some(s) = payload.downcast_ref::<&str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    last_error().unwrap_or_else(|| "unknown error".to_string())
}

static LAST_ERROR_SLOT: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
static HOOK_INSTALLED: std::sync::Once = std::sync::Once::new();

fn clear_last_error() {
    *LAST_ERROR_SLOT.lock().unwrap() = None;
}
fn last_error() -> Option<String> {
    LAST_ERROR_SLOT.lock().unwrap().clone()
}
fn install_error_capture_hook() {
    HOOK_INSTALLED.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info.payload();
            if let Some(ed) = payload.downcast_ref::<crate::utils::elog::ErrorData>() {
                *LAST_ERROR_SLOT.lock().unwrap() = ed.message.clone();
                return;
            }
            prev(info);
        }));
    });
}

async fn run_sql_inner(shared: &Arc<SharedState>, sql: &str) -> Vec<Vec<(i32, bool)>> {
    use crate::backend::optimizer::plan::planner::standard_planner;
    use crate::backend::parser::analyze::parse_analyze_fixedparams_async;
    use crate::backend::parser::parser::raw_parser;
    use crate::backend::rewrite::rewriteHandler::query_rewrite;
    use crate::nodes::nodes::{CmdType, Node};
    use crate::nodes::parsenodes::RawStmt;
    use crate::parser::parser::RawParseMode;

    let mut list = raw_parser(sql, RawParseMode::Default);
    assert_eq!(list.len(), 1, "one statement per run_sql");
    let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
    let rs: RawStmt = *rs;
    let analyzed = parse_analyze_fixedparams_async(shared, &rs, sql, &[], 0).await;

    let mut query = if matches!(
        analyzed.commandType,
        CmdType::INSERT | CmdType::UPDATE | CmdType::DELETE | CmdType::MERGE
    ) {
        *analyzed
    } else {
        let mut rewritten = query_rewrite(*analyzed);
        assert_eq!(rewritten.len(), 1);
        rewritten.remove(0)
    };

    if query.commandType == CmdType::UTILITY {
        let plan = crate::backend::tcop::postgres::wrap_utility_stmt(&query);
        let mut dest =
            crate::backend::tcop::dest::create_dest_receiver(crate::tcop::dest::CommandDest::DestNone);
        crate::backend::tcop::utility::process_utility(
            shared,
            &plan,
            sql,
            crate::tcop::utility::ProcessUtilityContext::Toplevel,
            dest.as_mut(),
            None,
        )
        .await;
        return Vec::new();
    }

    let plan = standard_planner(&mut query, sql, 0, None);
    let sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Vec<(i32, bool)>>::new()));
    let receiver: Box<dyn crate::tcop::dest::DestReceiver> =
        Box::new(RowSink { sink: Arc::clone(&sink) });
    crate::backend::tcop::postgres::execute_plan_into(shared, &plan, sql, None, receiver, 0).await;
    let rows = sink.lock().unwrap().clone();
    drop(sink);
    rows
}

struct RowSink {
    sink: Arc<std::sync::Mutex<Vec<Vec<(i32, bool)>>>>,
}
impl crate::tcop::dest::DestReceiver for RowSink {
    fn receive_slot(&mut self, slot: &mut crate::executor::tuptable::TupleTableSlot) -> bool {
        let natts = i32::from(slot.nvalid);
        let row = (1..=natts)
            .map(|attno| {
                let v = crate::executor::tuptable::slot_getattr(slot, attno);
                (v.map_or(0, crate::postgres::DatumGetInt32), v.is_none())
            })
            .collect();
        self.sink.lock().unwrap().push(row);
        true
    }
    fn r_startup(&mut self, _op: crate::nodes::nodes::CmdType, _td: crate::access::tupdesc::TupleDesc) {}
    fn r_shutdown(&mut self) {}
    fn mydest(&self) -> crate::tcop::dest::CommandDest {
        crate::tcop::dest::CommandDest::DestNone
    }
}

// ---------------------------------------------------------------------------
// catalog readers (each in its own read transaction)
// ---------------------------------------------------------------------------

async fn relid_of(shared: &Arc<SharedState>, name: &str) -> Oid {
    with_read_txn(shared, |shared| async move {
        crate::backend::catalog::namespace::range_var_get_relid(&shared, None, name)
            .await
            .unwrap_or(crate::postgres_ext::InvalidOid)
    })
    .await
}

/// pg_class (relfilenode, relpages, reltuples) for `relid`.
async fn pg_class_phys(shared: &Arc<SharedState>, relid: Oid) -> (Oid, i32, f32) {
    with_read_txn(shared, |shared| async move {
        use crate::access::htup_details::GETSTRUCT;
        use crate::catalog::pg_class::{self as pc, FormData_pg_class, RelationRelationId};
        let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
            &shared,
            RelationRelationId,
            pc::Anum_pg_class_oid,
            relid,
        )
        .await;
        let mut out = (crate::postgres_ext::InvalidOid, 0, 0.0);
        for row in &rows {
            let p = GETSTRUCT(&row.tuple).cast::<FormData_pg_class>();
            if unsafe { (*p).oid } == relid {
                out = unsafe { ((*p).relfilenode, (*p).relpages, (*p).reltuples) };
            }
        }
        for row in rows {
            crate::backend::access::common::heaptuple::heap_freetuple(row.tuple);
        }
        out
    })
    .await
}

/// Whether some index of `heap_relid` is marked clustered (indisclustered stand-in,
/// the index registry -- pg_index is not an on-disk catalog in this port).
fn any_index_clustered(heap_relid: Oid) -> bool {
    crate::backend::catalog::indexing::relation_get_index_list(heap_relid)
        .iter()
        .any(|ri| ri.indisclustered)
}

/// The main-fork block count of relation `relid` (via a fresh read transaction).
async fn heap_nblocks(shared: &Arc<SharedState>, relid: Oid) -> u32 {
    with_read_txn(shared, |shared| async move {
        use crate::common::relpath::ForkNumber;
        crate::backend::utils::cache::relcache::relation_build_desc(&shared, relid).await;
        let rel = crate::backend::utils::cache::relcache::relation_id_get_relation(relid).unwrap();
        let smgr_ptr = rel.smgr();
        let smgr = unsafe { &mut *smgr_ptr };
        let n = smgr.nblocks(&shared, ForkNumber::MAIN_FORKNUM).await;
        crate::backend::utils::cache::relcache::relation_close(rel);
        n
    })
    .await
}

async fn with_read_txn<F, Fut, T>(shared: &Arc<SharedState>, f: F) -> T
where
    F: FnOnce(Arc<SharedState>) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    use crate::backend::access::transam::xact::{
        CommitTransactionCommand, GetCurrentCommandId, StartTransactionCommand,
    };
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };
    StartTransactionCommand(shared).await;
    crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(sn) = snap.as_mut() {
        Arc::make_mut(sn).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);
    let out = f(Arc::clone(shared)).await;
    PopActiveSnapshot();
    CommitTransactionCommand(shared).await;
    out
}

fn set_of(rows: &[Vec<(i32, bool)>]) -> std::collections::HashSet<i32> {
    rows.iter().map(|r| r[0].0).collect()
}
fn order_of(rows: &[Vec<(i32, bool)>]) -> Vec<i32> {
    rows.iter().map(|r| r[0].0).collect()
}

// ===========================================================================
//  Tests
// ===========================================================================

/// VACUUM FULL shrinks a table: insert many, delete most, VACUUM FULL, then the
/// relation is physically smaller (relpages + reltuples drop), the live rows remain
/// correct + visible, the relfilenode changed (a new physical file), and a re-insert
/// works.
#[tokio::test(flavor = "multi_thread")]
async fn vacuum_full_shrinks_and_preserves_live_rows() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;

        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        for i in 1..=12 {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({i})")).await.unwrap();
        }
        // Delete most rows, leaving 11 and 12.
        run_sql(&shared, "DELETE FROM t WHERE a <= 10").await.unwrap();

        let relid = relid_of(&shared, "t").await;
        let (filenode_before, _pages_before, _tuples_before) = pg_class_phys(&shared, relid).await;
        // The heap before VACUUM FULL holds 12 slots (2 live + 10 dead), 1+ pages.
        let nblocks_before = heap_nblocks(&shared, relid).await;

        // VACUUM FULL rewrites the heap.
        run_sql(&shared, "VACUUM FULL t").await.unwrap();

        let (filenode_after, pages_after, tuples_after) = pg_class_phys(&shared, relid).await;
        let nblocks_after = heap_nblocks(&shared, relid).await;

        // reltuples is now exactly the 2 live rows (the dead space is gone, not just
        // marked free); pg_class.relpages matches the compacted heap's block count.
        assert_eq!(tuples_after as i64, 2, "exactly the 2 live rows remain");
        assert_eq!(pages_after, nblocks_after as i32, "relpages tracks the new heap size");
        assert!(
            nblocks_after <= nblocks_before,
            "the rewritten heap is no larger: {nblocks_before} -> {nblocks_after} blocks"
        );

        // The relfilenode changed: a genuinely new physical file.
        assert_ne!(
            filenode_after, filenode_before,
            "VACUUM FULL swaps in a new relfilenode ({filenode_before:?} -> {filenode_after:?})"
        );

        // Live rows are intact + visible.
        let live = set_of(&run_sql(&shared, "SELECT a FROM t").await.unwrap());
        assert_eq!(live, [11, 12].into_iter().collect(), "live rows preserved by VACUUM FULL");

        // A re-insert into the rewritten heap works + reads back.
        run_sql(&shared, "INSERT INTO t VALUES (99)").await.unwrap();
        let after_ins = set_of(&run_sql(&shared, "SELECT a FROM t").await.unwrap());
        assert_eq!(after_ins, [11, 12, 99].into_iter().collect(), "re-insert works after rewrite");
    }))
    .await;
}

/// VACUUM FULL over an indexed table: the index is rebuilt so an index scan still
/// finds the live rows after the rewrite.
#[tokio::test(flavor = "multi_thread")]
async fn vacuum_full_rebuilds_indexes() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;

        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        run_sql(&shared, "CREATE INDEX t_a_idx ON t (a)").await.unwrap();
        for i in 1..=6 {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({i})")).await.unwrap();
        }

        run_sql(&shared, "VACUUM FULL t").await.unwrap();

        // An index scan (WHERE a = k) still finds each row after the rebuild.
        for k in 1..=6 {
            let hit = run_sql(&shared, &format!("SELECT a FROM t WHERE a = {k}")).await.unwrap();
            assert_eq!(hit.len(), 1, "index scan finds a = {k} after VACUUM FULL rebuild");
            assert_eq!(hit[0][0].0, k);
        }
        // A full scan returns exactly the live rows.
        let all = set_of(&run_sql(&shared, "SELECT a FROM t").await.unwrap());
        assert_eq!(all, (1..=6).collect(), "all rows preserved after indexed VACUUM FULL");
    }))
    .await;
}

/// CLUSTER t USING idx: insert out-of-order rows, cluster, then a physical (seqscan)
/// order of the heap follows the index order, and the index is marked indisclustered.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_orders_heap_by_index() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;

        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        run_sql(&shared, "CREATE INDEX t_a_idx ON t (a)").await.unwrap();
        // Insert out of order.
        for v in [5, 1, 4, 2, 3] {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({v})")).await.unwrap();
        }

        let relid = relid_of(&shared, "t").await;
        assert!(!any_index_clustered(relid), "not clustered before CLUSTER");

        run_sql(&shared, "CLUSTER t USING t_a_idx").await.unwrap();

        // A plain SELECT (seqscan) now returns the rows in physical == index order.
        let order = order_of(&run_sql(&shared, "SELECT a FROM t").await.unwrap());
        assert_eq!(order, vec![1, 2, 3, 4, 5], "heap physically ordered by the cluster index");

        // The index is marked clustered.
        assert!(any_index_clustered(relid), "indisclustered set after CLUSTER");

        // The relfilenode changed (a rewrite happened).
        let (_fn, _pg, rt) = pg_class_phys(&shared, relid).await;
        assert_eq!(rt as i64, 5, "all 5 rows preserved by CLUSTER");
    }))
    .await;
}

/// A rewrite preserves every live row exactly -- no loss, no duplicate -- and the
/// relfilenode changes.
#[tokio::test(flavor = "multi_thread")]
async fn rewrite_preserves_rows_exactly() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;

        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        for i in 1..=8 {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({i})")).await.unwrap();
        }
        let before = order_of(&run_sql(&shared, "SELECT a FROM t").await.unwrap());
        let relid = relid_of(&shared, "t").await;
        let (fn_before, _, _) = pg_class_phys(&shared, relid).await;

        run_sql(&shared, "VACUUM FULL t").await.unwrap();

        let after = set_of(&run_sql(&shared, "SELECT a FROM t").await.unwrap());
        assert_eq!(after, before.iter().copied().collect(), "no row lost or duplicated");
        assert_eq!(after.len(), 8, "exactly 8 distinct rows");
        let (fn_after, _, _) = pg_class_phys(&shared, relid).await;
        assert_ne!(fn_after, fn_before, "relfilenode changed by the rewrite");
    }))
    .await;
}

/// CLUSTER with no relation (re-cluster every marked table) is a clean staged error.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_all_tables_is_staged() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        let err = run_sql(&shared, "CLUSTER").await.unwrap_err();
        assert!(err.contains("CLUSTER"), "staged bare-CLUSTER error, got: {err}");
    }))
    .await;
}
