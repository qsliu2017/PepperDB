//! Wire-level / full-SQL-path tests for set operations (UNION/INTERSECT/EXCEPT)
//! and CTEs (WITH / WITH RECURSIVE) -- step 43 planner glue.
//!
//! Each test stands up a real foundation `SharedState` over a tempdir, runs initdb,
//! creates + populates real tables, then drives `SELECT ... UNION ...` / `WITH ...`
//! through the full parse -> analyze -> rewrite -> plan -> execute pipeline and
//! asserts the returned rows. The harness mirrors `commands/fk_tests.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::type_complexity, reason = "tests")]

use std::sync::Arc;

use futures_util::FutureExt;

use crate::postgres_ext::Oid;
use crate::shared_state::{SharedState, SharedStateConfig};

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

const DB_OID: Oid = Oid::new(90000);

fn new_shared() -> Arc<SharedState> {
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-setop-{}-{}", std::process::id(), n));
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

/// Run one SQL statement as its own autocommit transaction; returns Ok(rows) (each
/// row a Vec of (i32, isnull)) or Err(message) on ereport(ERROR).
async fn run_sql(shared: &Arc<SharedState>, sql: &str) -> Result<Vec<Vec<(i32, bool)>>, String> {
    use crate::backend::access::transam::xact::{
        CommitTransactionCommand, GetCurrentCommandId, GetTopTransactionIdIfAny,
        IsTransactionOrTransactionBlock, StartTransactionCommand,
    };
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };

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
    "unknown error".to_string()
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
        let mut dest = crate::backend::tcop::dest::create_dest_receiver(
            crate::tcop::dest::CommandDest::DestNone,
        );
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
    let receiver: Box<dyn crate::tcop::dest::DestReceiver> = Box::new(RowSink { sink: Arc::clone(&sink) });
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

/// Pull the single-column i32 values out of a row set (non-null), sorted, for
/// order-independent comparison.
fn col0_sorted(rows: &[Vec<(i32, bool)>]) -> Vec<i32> {
    let mut v: Vec<i32> = rows.iter().map(|r| r[0].0).collect();
    v.sort_unstable();
    v
}

async fn seed(shared: &Arc<SharedState>) {
    init_db(shared).await;
    run_sql(shared, "CREATE TABLE t (a int)").await.unwrap();
    run_sql(shared, "CREATE TABLE u (a int)").await.unwrap();
    // t = {1,2,3,3}, u = {3,4,5}
    run_sql(shared, "INSERT INTO t VALUES (1)").await.unwrap();
    run_sql(shared, "INSERT INTO t VALUES (2)").await.unwrap();
    run_sql(shared, "INSERT INTO t VALUES (3)").await.unwrap();
    run_sql(shared, "INSERT INTO t VALUES (3)").await.unwrap();
    run_sql(shared, "INSERT INTO u VALUES (3)").await.unwrap();
    run_sql(shared, "INSERT INTO u VALUES (4)").await.unwrap();
    run_sql(shared, "INSERT INTO u VALUES (5)").await.unwrap();
}

// ===========================================================================
//  Set-operation tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn union_deduplicates() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        let rows = run_sql(&shared, "SELECT a FROM t UNION SELECT a FROM u").await.unwrap();
        // {1,2,3} U {3,4,5} = {1,2,3,4,5} (dups removed)
        assert_eq!(col0_sorted(&rows), vec![1, 2, 3, 4, 5]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn union_all_keeps_duplicates() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        let rows = run_sql(&shared, "SELECT a FROM t UNION ALL SELECT a FROM u").await.unwrap();
        // t = {1,2,3,3}, u = {3,4,5} -> 7 rows, multiset preserved
        assert_eq!(col0_sorted(&rows), vec![1, 2, 3, 3, 3, 4, 5]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn intersect_distinct() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        let rows = run_sql(&shared, "SELECT a FROM t INTERSECT SELECT a FROM u").await.unwrap();
        // common: {3}
        assert_eq!(col0_sorted(&rows), vec![3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn except_distinct() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        let rows = run_sql(&shared, "SELECT a FROM t EXCEPT SELECT a FROM u").await.unwrap();
        // {1,2,3} - {3,4,5} = {1,2}
        assert_eq!(col0_sorted(&rows), vec![1, 2]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn intersect_all_min_multiplicity() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE a (x int)").await.unwrap();
        run_sql(&shared, "CREATE TABLE b (x int)").await.unwrap();
        // a = {3,3,3}, b = {3,3} -> INTERSECT ALL = min(3,2)=2 copies of 3
        for _ in 0..3 {
            run_sql(&shared, "INSERT INTO a VALUES (3)").await.unwrap();
        }
        for _ in 0..2 {
            run_sql(&shared, "INSERT INTO b VALUES (3)").await.unwrap();
        }
        let rows = run_sql(&shared, "SELECT x FROM a INTERSECT ALL SELECT x FROM b").await.unwrap();
        assert_eq!(col0_sorted(&rows), vec![3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn except_all_count_difference() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE a (x int)").await.unwrap();
        run_sql(&shared, "CREATE TABLE b (x int)").await.unwrap();
        // a = {3,3,3}, b = {3} -> EXCEPT ALL = max(3-1,0)=2 copies of 3
        for _ in 0..3 {
            run_sql(&shared, "INSERT INTO a VALUES (3)").await.unwrap();
        }
        run_sql(&shared, "INSERT INTO b VALUES (3)").await.unwrap();
        let rows = run_sql(&shared, "SELECT x FROM a EXCEPT ALL SELECT x FROM b").await.unwrap();
        assert_eq!(col0_sorted(&rows), vec![3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn precedence_intersect_binds_tighter_than_union() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        // `t UNION u INTERSECT u` == `t UNION (u INTERSECT u)` = {1,2,3} U {3,4,5} = {1,2,3,4,5}.
        // (If it wrongly bound as (t UNION u) INTERSECT u, the result would be {3,4,5}.)
        let rows = run_sql(
            &shared,
            "SELECT a FROM t UNION SELECT a FROM u INTERSECT SELECT a FROM u",
        )
        .await
        .unwrap();
        assert_eq!(col0_sorted(&rows), vec![1, 2, 3, 4, 5]);
    })
    .await;
}

// ===========================================================================
//  CTE tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cte_nonrecursive_simple() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        // WITH c AS (SELECT a FROM t) SELECT * FROM c -> the t rows {1,2,3,3}.
        let rows = run_sql(&shared, "WITH c AS (SELECT a FROM t) SELECT * FROM c").await.unwrap();
        assert_eq!(col0_sorted(&rows), vec![1, 2, 3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_referenced_twice() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        // A CTE referenced twice (via UNION ALL of two scans of the same CTE): each
        // reference materializes the CTE, so the result is the CTE multiset doubled.
        let rows = run_sql(
            &shared,
            "WITH c AS (SELECT a FROM t) SELECT * FROM c UNION ALL SELECT * FROM c",
        )
        .await
        .unwrap();
        // t = {1,2,3,3} -> twice = {1,1,2,2,3,3,3,3}
        assert_eq!(col0_sorted(&rows), vec![1, 1, 2, 2, 3, 3, 3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_recursive_counter_one_to_five() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        // WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 5)
        // SELECT n FROM t -> 1,2,3,4,5.
        let rows = run_sql(
            &shared,
            "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 5) SELECT n FROM t",
        )
        .await
        .unwrap();
        assert_eq!(col0_sorted(&rows), vec![1, 2, 3, 4, 5]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_recursive_tree_path_terminates() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        // A small terminating tree/chain traversal: each step doubles, bounded by
        // n < 20. The working set is 1,2,4,8,16; the 16<20 step emits 32 (and 32<20 is
        // false, so 32 produces nothing) -- the recursion terminates at 1,2,4,8,16,32.
        let rows = run_sql(
            &shared,
            "WITH RECURSIVE p(n) AS (SELECT 1 UNION ALL SELECT n * 2 FROM p WHERE n < 20) SELECT n FROM p",
        )
        .await
        .unwrap();
        assert_eq!(col0_sorted(&rows), vec![1, 2, 4, 8, 16, 32]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_recursive_two_relation_term_staged_cleanly() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        // A two-relation recursive term (edge JOIN reach) exceeds the milestone's
        // single-self-reference shape. It must STAGE cleanly (a clear error), never
        // hang or return wrong rows.
        run_sql(&shared, "CREATE TABLE edge (src int, dst int)").await.unwrap();
        run_sql(&shared, "INSERT INTO edge VALUES (1, 2)").await.unwrap();
        let res = run_sql(
            &shared,
            "WITH RECURSIVE reach(n) AS (\
                 SELECT 1 \
                 UNION ALL \
                 SELECT edge.dst FROM edge, reach WHERE edge.src = reach.n AND reach.n < 100\
             ) SELECT n FROM reach",
        )
        .await;
        assert!(
            res.is_err(),
            "two-relation recursive term must stage cleanly (got rows: {res:?})"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cte_recursive_declared_but_not_self_referential_executes() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        // WITH RECURSIVE merely PERMITS self-reference; a body that never references
        // itself is a plain (non-recursive) CTE and must execute, not panic.
        let rows = run_sql(
            &shared,
            "WITH RECURSIVE a AS (SELECT 1 UNION ALL SELECT 2) SELECT * FROM a",
        )
        .await
        .unwrap();
        assert_eq!(col0_sorted(&rows), vec![1, 2]);
    })
    .await;
}

// ===========================================================================
//  Subquery / SubLink tests (M12, step 44). Seed: t = {1,2,3,3}, u = {3,4,5}.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn exists_correlated_filters() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        // EXISTS over a correlated subquery: keep t rows whose `a` is present in u.
        let rows = run_sql(
            &shared,
            "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.a)",
        )
        .await
        .unwrap();
        // Only a=3 exists in u; t has two 3s.
        assert_eq!(col0_sorted(&rows), vec![3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn not_exists_correlated_filters() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        let rows = run_sql(
            &shared,
            "SELECT a FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.a = t.a)",
        )
        .await
        .unwrap();
        // t rows NOT in u: 1, 2.
        assert_eq!(col0_sorted(&rows), vec![1, 2]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn in_subquery() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        let rows = run_sql(&shared, "SELECT a FROM t WHERE a IN (SELECT a FROM u)").await.unwrap();
        // a in u = {3,4,5} -> only 3 (twice).
        assert_eq!(col0_sorted(&rows), vec![3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn not_in_subquery_no_nulls() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        let rows = run_sql(&shared, "SELECT a FROM t WHERE a NOT IN (SELECT a FROM u)").await.unwrap();
        // t not in u -> 1, 2.
        assert_eq!(col0_sorted(&rows), vec![1, 2]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn not_in_subquery_with_null_is_three_valued() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE n (a int)").await.unwrap();
        run_sql(&shared, "CREATE TABLE m (a int)").await.unwrap();
        run_sql(&shared, "INSERT INTO n VALUES (1)").await.unwrap();
        run_sql(&shared, "INSERT INTO n VALUES (2)").await.unwrap();
        run_sql(&shared, "INSERT INTO m VALUES (2)").await.unwrap();
        // m also contains a NULL -> `a NOT IN (m)` is never TRUE (NULL/UNKNOWN), so
        // no rows qualify (SQL three-valued semantics).
        run_sql(&shared, "INSERT INTO m VALUES (NULL)").await.unwrap();
        let rows = run_sql(&shared, "SELECT a FROM n WHERE a NOT IN (SELECT a FROM m)").await.unwrap();
        assert!(rows.is_empty(), "NOT IN over a NULL-containing subquery yields no rows, got {rows:?}");
        // The positive IN still works: only 2 is present.
        let rows2 = run_sql(&shared, "SELECT a FROM n WHERE a IN (SELECT a FROM m)").await.unwrap();
        assert_eq!(col0_sorted(&rows2), vec![2]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn eq_any_subquery() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        let rows = run_sql(&shared, "SELECT a FROM t WHERE a = ANY (SELECT a FROM u)").await.unwrap();
        assert_eq!(col0_sorted(&rows), vec![3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn gt_all_subquery() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE t (a int)").await.unwrap();
        run_sql(&shared, "CREATE TABLE u (a int)").await.unwrap();
        for v in [1, 5, 9] {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({v})")).await.unwrap();
        }
        for v in [2, 3, 4] {
            run_sql(&shared, &format!("INSERT INTO u VALUES ({v})")).await.unwrap();
        }
        // a > ALL (2,3,4) -> a > 4 -> {5,9}.
        let rows = run_sql(&shared, "SELECT a FROM t WHERE a > ALL (SELECT a FROM u)").await.unwrap();
        assert_eq!(col0_sorted(&rows), vec![5, 9]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_scalar_subquery_in_target_list() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE t (a int)").await.unwrap();
        run_sql(&shared, "CREATE TABLE u (a int, b int)").await.unwrap();
        for v in [1, 2] {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({v})")).await.unwrap();
        }
        // u: (1,10),(1,20),(2,30)
        run_sql(&shared, "INSERT INTO u VALUES (1, 10)").await.unwrap();
        run_sql(&shared, "INSERT INTO u VALUES (1, 20)").await.unwrap();
        run_sql(&shared, "INSERT INTO u VALUES (2, 30)").await.unwrap();
        // For each t.a, the max(u.b) where u.a = t.a: a=1 -> 20, a=2 -> 30.
        let rows = run_sql(
            &shared,
            "SELECT a, (SELECT max(b) FROM u WHERE u.a = t.a) FROM t",
        )
        .await
        .unwrap();
        let mut got: Vec<(i32, i32)> = rows.iter().map(|r| (r[0].0, r[1].0)).collect();
        got.sort_unstable();
        assert_eq!(got, vec![(1, 20), (2, 30)]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn uncorrelated_scalar_initplan_in_where() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        init_db(&shared).await;
        run_sql(&shared, "CREATE TABLE t (a int)").await.unwrap();
        for v in [1, 2, 3, 4] {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({v})")).await.unwrap();
        }
        // min(a) over {1,2,3,4} = 1; a > 1 -> {2,3,4}. The uncorrelated scalar
        // sub-select is an InitPlan: run once, its result cached for every outer row.
        let rows = run_sql(&shared, "SELECT a FROM t WHERE a > (SELECT min(a) FROM t)").await.unwrap();
        assert_eq!(col0_sorted(&rows), vec![2, 3, 4]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn in_value_list_non_subquery_executes() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        // `x IN (expr_list)` parses + analyzes to an OR-chain of `=`, planned via
        // the OR-clause path (make_sub_restrictinfos). t = {1,2,3,3}, so IN (1,3)
        // yields {1,3,3}.
        let rows = run_sql(&shared, "SELECT a FROM t WHERE a IN (1, 3)").await.unwrap();
        assert_eq!(col0_sorted(&rows), vec![1, 3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_sublink_stages_cleanly() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        // A sub-select nested inside another sub-select would discard the inner
        // SubPlan (silently-wrong rows). It must stage cleanly (catchable error),
        // and the transaction must recover.
        let res = run_sql(
            &shared,
            "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS (SELECT 1 FROM t WHERE t.a = u.a))",
        )
        .await;
        assert!(res.is_err(), "nested sub-select must stage cleanly, got {res:?}");
        // The transaction recovered: a plain single-level sublink still works.
        let rows = run_sql(
            &shared,
            "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = t.a)",
        )
        .await
        .unwrap();
        assert_eq!(col0_sorted(&rows), vec![3, 3]);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_having_stages_cleanly() {
    let shared = new_shared();
    in_scopes(Arc::clone(&shared), |shared| async move {
        seed(&shared).await;
        // A correlation reference in a sub-select's HAVING would be misclassified as a
        // run-once InitPlan (stale for every outer row). HAVING is not yet in the
        // grammar, so this currently stages at parse time; the replace_correlation_vars
        // guard (subselect.rs) catches it once HAVING parses. Either way it must stage
        // cleanly (catchable error), never return silently-wrong rows.
        let res = run_sql(
            &shared,
            "SELECT a FROM t WHERE (SELECT count(*) FROM u GROUP BY u.a HAVING count(*) > t.a) > 0",
        )
        .await;
        assert!(res.is_err(), "correlated HAVING must stage cleanly, got {res:?}");
        // The transaction recovered: a plain uncorrelated scalar sub-select still works.
        let rows = run_sql(&shared, "SELECT a FROM t WHERE a > (SELECT min(a) FROM u)").await.unwrap();
        // min(u)=3; a>3 -> none of t={1,2,3,3}.
        assert!(rows.is_empty(), "post-recovery query works, got {rows:?}");
    })
    .await;
}
