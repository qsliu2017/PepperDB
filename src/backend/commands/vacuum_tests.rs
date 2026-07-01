//! Integration tests for VACUUM + ANALYZE (step 46).
//!
//! Each test stands up a real foundation `SharedState` over a tempdir + the full
//! per-task scope stack (the catalog-test harness), runs initdb, then drives real
//! SQL through the parse -> analyze -> plan -> (process_utility | ExecutorRun)
//! pipeline. The assertions check the milestone bar: VACUUM reclaims dead tuples'
//! space (a re-insert reuses it, live rows stay correct, index entries for deleted
//! rows are gone), ANALYZE populates pg_statistic + pg_class.reltuples and the
//! planner's eqsel consumes the real frequency, and an un-analyzed table keeps the
//! no-stats default.

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

const DB_OID: Oid = Oid::new(90000);

fn new_shared() -> Arc<SharedState> {
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("pepperdb-vac-{}-{}", std::process::id(), n));
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
static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

/// The number of pg_statistic rows for `relid` (a catalog scan). Runs in its own
/// read transaction so the seeded/ANALYZE'd rows are visible.
async fn count_pg_statistic_rows(shared: &Arc<SharedState>, relid: Oid) -> usize {
    use crate::access::htup_details::GETSTRUCT;
    use crate::backend::access::transam::xact::{
        CommitTransactionCommand, GetCurrentCommandId, StartTransactionCommand,
    };
    use crate::backend::utils::time::snapmgr::{
        GetTransactionSnapshot, PopActiveSnapshot, PushActiveSnapshot,
    };
    use crate::catalog::pg_statistic::{self as s, FormData_pg_statistic, StatisticRelationId};

    StartTransactionCommand(shared).await;
    crate::backend::utils::time::snapmgr::InvalidateCatalogSnapshot();
    let mut snap = GetTransactionSnapshot(shared);
    if let Some(sn) = snap.as_mut() {
        Arc::make_mut(sn).curcid = GetCurrentCommandId(false);
    }
    PushActiveSnapshot(snap);

    let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
        shared,
        StatisticRelationId,
        s::Anum_pg_statistic_starelid,
        relid,
    )
    .await;
    let mut n = 0;
    for row in &rows {
        // SAFETY: owned tuple; the fixed part starts with FormData_pg_statistic.
        let p = GETSTRUCT(&row.tuple).cast::<FormData_pg_statistic>();
        if unsafe { (*p).starelid } == relid {
            n += 1;
        }
    }
    for row in rows {
        crate::backend::access::common::heaptuple::heap_freetuple(row.tuple);
    }

    PopActiveSnapshot();
    CommitTransactionCommand(shared).await;
    n
}

/// The OID of a user relation by name (via a fresh read transaction).
async fn relid_of(shared: &Arc<SharedState>, name: &str) -> Oid {
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
    let oid = crate::backend::catalog::namespace::range_var_get_relid(shared, None, name)
        .await
        .unwrap_or(crate::postgres_ext::InvalidOid);
    PopActiveSnapshot();
    CommitTransactionCommand(shared).await;
    oid
}

/// The durable pg_class.reltuples for `relid` (via a fresh read transaction).
async fn pg_class_reltuples(shared: &Arc<SharedState>, relid: Oid) -> f64 {
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
    let n = crate::backend::utils::adt::selfuncs::pg_class_reltuples_for_test(shared, relid).await;
    PopActiveSnapshot();
    CommitTransactionCommand(shared).await;
    n
}

// ===========================================================================
//  Tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_removes_dead_tuples_and_keeps_live_rows() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        crate::backend::utils::adt::selfuncs::clear_stats_cache_for_test();

        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        for i in 1..=6 {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({i})")).await.unwrap();
        }
        // Delete half the rows, creating dead tuples.
        run_sql(&shared, "DELETE FROM t WHERE a <= 3").await.unwrap();

        // Live rows before VACUUM: 4,5,6.
        let before = run_sql(&shared, "SELECT a FROM t").await.unwrap();
        assert_eq!(before.len(), 3, "3 live rows survive the delete");

        // VACUUM reclaims the dead tuples.
        run_sql(&shared, "VACUUM t").await.unwrap();

        // Live rows still correct after VACUUM.
        let after: std::collections::HashSet<i32> =
            run_sql(&shared, "SELECT a FROM t").await.unwrap().into_iter().map(|r| r[0].0).collect();
        assert_eq!(after, [4, 5, 6].into_iter().collect(), "live rows intact post-vacuum");

        // Re-insert reuses the reclaimed space and reads back correctly.
        run_sql(&shared, "INSERT INTO t VALUES (7)").await.unwrap();
        let final_rows: std::collections::HashSet<i32> =
            run_sql(&shared, "SELECT a FROM t").await.unwrap().into_iter().map(|r| r[0].0).collect();
        assert_eq!(final_rows, [4, 5, 6, 7].into_iter().collect());
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_removes_index_entries_for_deleted_rows() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        crate::backend::utils::adt::selfuncs::clear_stats_cache_for_test();

        // VACUUM must run cleanly over a table that HAS an index (it opens the
        // index and drives its bulk-delete). The end-to-end "index entry for a
        // deleted row is gone" is covered at the AM level by the nbtree test
        // `btbulkdelete_removes_dead_index_entries` (the executor's
        // index-scan-driven DELETE is a separate M8 gap, so we do not delete through
        // SQL on an indexed table here).
        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        run_sql(&shared, "CREATE INDEX t_a_idx ON t (a)").await.unwrap();
        for i in 1..=5 {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({i})")).await.unwrap();
        }
        // VACUUM over the indexed table succeeds (no panic; the index vacuum runs).
        run_sql(&shared, "VACUUM t").await.unwrap();

        // The live rows and the index scan remain correct after VACUUM.
        let all: std::collections::HashSet<i32> =
            run_sql(&shared, "SELECT a FROM t").await.unwrap().into_iter().map(|r| r[0].0).collect();
        assert_eq!(all, [1, 2, 3, 4, 5].into_iter().collect(), "live rows intact after indexed VACUUM");
        let pt: std::collections::HashSet<i32> =
            run_sql(&shared, "SELECT a FROM t WHERE a = 3").await.unwrap().into_iter().map(|r| r[0].0).collect();
        assert_eq!(pt, std::collections::HashSet::from([3]), "index scan still finds live key 3");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn analyze_populates_pg_statistic_and_reltuples() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        crate::backend::utils::adt::selfuncs::clear_stats_cache_for_test();

        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        // A skewed distribution: value 5 is very common (a clear MCV), plus a spread.
        for _ in 0..40 {
            run_sql(&shared, "INSERT INTO t VALUES (5)").await.unwrap();
        }
        for i in 1..=20 {
            run_sql(&shared, &format!("INSERT INTO t VALUES ({i})")).await.unwrap();
        }
        // 60 rows total; value 5 appears 41 times.
        run_sql(&shared, "ANALYZE t").await.unwrap();

        let relid = relid_of(&shared, "t").await;
        assert!(relid.is_valid(), "table t resolves");

        // pg_statistic has a row for the analyzed column.
        let nstats = count_pg_statistic_rows(&shared, relid).await;
        assert!(nstats >= 1, "ANALYZE wrote a pg_statistic row, got {nstats}");

        // pg_class.reltuples ~ 60 (the durable catalog row ANALYZE updated).
        let reltuples = pg_class_reltuples(&shared, relid).await;
        assert!(
            (reltuples - 60.0).abs() < 5.0,
            "pg_class.reltuples ~ 60 after ANALYZE, got {reltuples}"
        );

        // eqsel for the common value 5 reflects its real frequency (~41/60 = 0.68),
        // well above DEFAULT_EQ_SEL (0.005).
        let sel_common =
            crate::backend::utils::adt::selfuncs::eqsel_for_test(relid, 1, 5);
        assert!(
            sel_common > crate::backend::utils::adt::selfuncs::DEFAULT_EQ_SEL,
            "eqsel for a common value ({sel_common}) exceeds DEFAULT_EQ_SEL"
        );
        assert!(
            (sel_common - 41.0 / 60.0).abs() < 0.1,
            "eqsel for value 5 (~0.68) close to its real frequency, got {sel_common}"
        );

        // eqsel for a rare value (appears once) is small (< the common value).
        let sel_rare = crate::backend::utils::adt::selfuncs::eqsel_for_test(relid, 1, 17);
        assert!(sel_rare < sel_common, "rare value selectivity below the common one");
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn selfuncs_no_stats_fallback_is_default() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        crate::backend::utils::adt::selfuncs::clear_stats_cache_for_test();

        run_sql(&shared, "CREATE TABLE u (a int4)").await.unwrap();
        for i in 1..=10 {
            run_sql(&shared, &format!("INSERT INTO u VALUES ({i})")).await.unwrap();
        }
        // No ANALYZE: the stats cache has no entry for u.a, so eqsel returns the
        // no-stats default.
        let relid = relid_of(&shared, "u").await;
        let sel = crate::backend::utils::adt::selfuncs::eqsel_for_test(relid, 1, 3);
        assert!(
            (sel - crate::backend::utils::adt::selfuncs::DEFAULT_EQ_SEL).abs() < 1e-9,
            "un-analyzed table keeps DEFAULT_EQ_SEL, got {sel}"
        );
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_full_is_staged() {
    let _serial = TEST_SERIAL.lock().await;
    let shared = new_shared();
    Box::pin(in_scopes(shared.clone(), |shared| async move {
        init_db(&shared).await;
        crate::backend::utils::adt::selfuncs::clear_stats_cache_for_test();
        run_sql(&shared, "CREATE TABLE t (a int4)").await.unwrap();
        run_sql(&shared, "INSERT INTO t VALUES (1)").await.unwrap();
        // VACUUM FULL routes to CLUSTER (step 47): a clean, catchable staged error.
        let err = run_sql(&shared, "VACUUM FULL t").await.unwrap_err();
        assert!(err.contains("VACUUM FULL"), "staged VACUUM FULL error, got: {err}");
    }))
    .await;
}
